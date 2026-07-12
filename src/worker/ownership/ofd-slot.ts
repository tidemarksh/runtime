import type { ProcessHandle } from "../types.js";
import {
  deleteRegularFileOfdOffsetSnapshot,
  recordRegularFileOfdOffsetSnapshot,
  writeRegularFileOfdOffsetState,
} from "../regular-file-ofd-offset-state.js";
import {
  FD_ENTRY_BYTES,
  FD_ENTRY_OFD_ID_OFFSET,
} from "../../layout/fd.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_REFCOUNT_OFFSET,
  OFD_STATUS_FLAGS_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../layout/ofd.js";
import {
  networkConnectionOfds,
  restoreNetworkOfdSlotWatermarks,
} from "./socket-control.js";

interface FdEntryLike {
  fd?: number;
  ofdId: number;
}

interface OfdSlotSnapshotLike {
  ofdId: number;
  slot: Uint8Array;
}

interface StatusMessageLike {
  vmPtr: number;
  fdEntries?: readonly ({ fd: number } & FdEntryLike)[];
  ofdSlots?: readonly OfdSlotSnapshotLike[];
  syscallNr?: bigint;
  fdTraceSyscallNr?: bigint;
  fdTraceArg0OfdId?: number;
  fdTraceArg0OfdType?: number;
  fdTraceArg0OfdOffset?: bigint;
  fdTraceArg1?: bigint;
  fdTraceResult?: bigint;
  trustRegularFileOfdOffsets?: boolean;
  trustRegularFileOffsetOfdIds?: ReadonlySet<number>;
  recordRegularFileOfdOffsets?: boolean;
  allowRegularFileIdentityReplacement?: boolean;
}

const SUPERVISOR_RESTORE_SYSCALL_NONE = 0xffffffffffffffffn;
const SYS_FCNTL = 25n;
const SYS_IOCTL = 29n;
const SYS_READ = 63n;
const SYS_WRITE = 64n;
const SYS_READV = 65n;
const SYS_WRITEV = 66n;
const F_SETFL = 4n;
const OPEN_ACCESS_MODE_MASK = 0x3;

interface OfdSlotCandidate {
  type: number;
  hostFd: number;
  ino: number;
  offset: bigint;
}

function readOfdSlotCandidate(slot: Uint8Array): OfdSlotCandidate {
  const view = new DataView(slot.buffer, slot.byteOffset, slot.byteLength);
  return {
    type: view.getInt32(OFD_TYPE_OFFSET, true),
    hostFd: view.getInt32(OFD_HOST_FD_OFFSET, true),
    ino: view.getUint32(OFD_INO_OFFSET, true),
    offset: view.getBigInt64(OFD_OFFSET_OFFSET, true),
  };
}

function requireSupervisorRestoreActionSkip(handle: ProcessHandle): number {
  const skip = handle.kernel.supervisor_fd_entry_restore_action_skip;
  if (!skip) {
    throw new Error("kernel supervisor ABI missing supervisor_fd_entry_restore_action_skip");
  }
  return skip.call(handle.kernel);
}

function requireSupervisorFdEntryGuestWriteAction(
  handle: ProcessHandle,
): (vmPtr: number, fd: number, candidateOfdId: number, explicitOfdId: number) => number {
  const decide = handle.kernel.supervisor_fd_entry_guest_write_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_fd_entry_guest_write_action");
  }
  return decide.bind(handle.kernel);
}

function requireSupervisorOfdSlotWriteAction(
  handle: ProcessHandle,
): (
  ofdId: number,
  candidateType: number,
  candidateHostFd: number,
  candidateIno: number,
  candidateOffset: bigint | number,
) => number {
  const decide = handle.kernel.supervisor_ofd_slot_write_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_ofd_slot_write_action");
  }
  return decide.bind(handle.kernel);
}

function requireSupervisorOfdSlotRestoreAction(
  handle: ProcessHandle,
): (
  ofdId: number,
  candidateType: number,
  candidateHostFd: number,
  candidateIno: number,
  candidateOffset: bigint | number,
  syscallNr: bigint | number,
  flags: number,
) => number {
  const decide = handle.kernel.supervisor_ofd_slot_restore_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_ofd_slot_restore_action");
  }
  return decide.bind(handle.kernel);
}

function supervisorOfdSlotRestoreFlags(
  handle: ProcessHandle,
  referencedByFdEntry: boolean,
  trustRegularFileOfdOffsets: boolean,
): number {
  let flags = 0;
  if (referencedByFdEntry) {
    const flag = handle.kernel.supervisor_ofd_slot_restore_flag_referenced_by_fd_entry;
    if (!flag) {
      throw new Error("kernel supervisor ABI missing supervisor_ofd_slot_restore_flag_referenced_by_fd_entry");
    }
    flags |= flag.call(handle.kernel);
  }
  if (trustRegularFileOfdOffsets) {
    const flag = handle.kernel.supervisor_ofd_slot_restore_flag_trust_regular_file_offset;
    if (!flag) {
      throw new Error("kernel supervisor ABI missing supervisor_ofd_slot_restore_flag_trust_regular_file_offset");
    }
    flags |= flag.call(handle.kernel);
  }
  return flags;
}

function ofdSnapshotReferencedByFdEntry(
  fdEntries: readonly FdEntryLike[] | undefined,
  ofdId: number,
): boolean {
  return (fdEntries ?? []).some((entry) => entry.ofdId === ofdId);
}

function candidateOfdIdFromFdEntry(entry: Uint8Array): number {
  return new DataView(
    entry.buffer,
    entry.byteOffset,
    entry.byteLength,
  ).getUint32(FD_ENTRY_OFD_ID_OFFSET, true);
}

function ofdSlotWriteShouldBeSkipped(
  ofdId: number,
  candidateSlot: Uint8Array,
  decide: ReturnType<typeof requireSupervisorOfdSlotWriteAction>,
  skipAction: number,
): boolean {
  const candidateView = new DataView(candidateSlot.buffer, candidateSlot.byteOffset, candidateSlot.byteLength);
  return decide(
    ofdId,
    candidateView.getInt32(OFD_TYPE_OFFSET, true),
    candidateView.getInt32(OFD_HOST_FD_OFFSET, true),
    candidateView.getUint32(OFD_INO_OFFSET, true),
    candidateView.getBigInt64(OFD_OFFSET_OFFSET, true),
  ) === skipAction;
}

function ofdSnapshotWouldClobberExistingSlot(
  handle: ProcessHandle,
  fdEntries: readonly FdEntryLike[] | undefined,
  snapshot: OfdSlotSnapshotLike,
  slotBytes: number,
  decide: ReturnType<typeof requireSupervisorOfdSlotRestoreAction>,
  skipAction: number,
  allowRegularFileIdentityReplacement: boolean,
  trustRegularFileOfdOffsets: boolean,
  syscallNr: bigint | number,
): boolean {
  const targetOffset = handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes;
  if (targetOffset < 0 || targetOffset + slotBytes > handle.memory.buffer.byteLength) {
    return true;
  }
  const candidate = readOfdSlotCandidate(snapshot.slot);
  const referencedByFdEntry = allowRegularFileIdentityReplacement
    && ofdSnapshotReferencedByFdEntry(fdEntries, snapshot.ofdId);
  const flags = supervisorOfdSlotRestoreFlags(
    handle,
    referencedByFdEntry,
    trustRegularFileOfdOffsets,
  );
  return decide(
    snapshot.ofdId,
    candidate.type,
    candidate.hostFd,
    candidate.ino,
    candidate.offset,
    syscallNr,
    flags,
  ) === skipAction;
}

export function maskRegularFileFdEntryRollbackGuestWrite(
  handle: ProcessHandle,
  msg: StatusMessageLike,
  guestPtr: number,
  bytes: Uint8Array,
): Uint8Array {
  const processPtr = msg.vmPtr === 0 ? 0 : handle.kernel.thread_process_ptr?.(msg.vmPtr);
  if (processPtr === undefined || processPtr <= 0) {
    return bytes;
  }
  const decide = requireSupervisorFdEntryGuestWriteAction(handle);
  const skipAction = requireSupervisorRestoreActionSkip(handle);
  const maxFds = handle.kernel.abi_max_fds();
  const fdTableStart = processPtr;
  const fdTableEnd = fdTableStart + maxFds * FD_ENTRY_BYTES;
  const writeStart = guestPtr;
  const writeEnd = guestPtr + bytes.length;
  if (writeEnd <= fdTableStart || writeStart >= fdTableEnd) {
    return bytes;
  }

  let masked: Uint8Array | null = null;
  const firstFd = Math.max(0, Math.floor((Math.max(writeStart, fdTableStart) - fdTableStart) / FD_ENTRY_BYTES));
  const lastFd = Math.min(
    maxFds - 1,
    Math.floor((Math.min(writeEnd - 1, fdTableEnd - 1) - fdTableStart) / FD_ENTRY_BYTES),
  );
  const memory = new Uint8Array(handle.memory.buffer);
  for (let fd = firstFd; fd <= lastFd; fd += 1) {
    const entryStart = fdTableStart + fd * FD_ENTRY_BYTES;
    const entryEnd = entryStart + FD_ENTRY_BYTES;
    const overlapStart = Math.max(writeStart, entryStart);
    const overlapEnd = Math.min(writeEnd, entryEnd);
    if (overlapStart >= overlapEnd) {
      continue;
    }
    const currentEntry = memory.subarray(
      handle.guestBase + entryStart,
      handle.guestBase + entryEnd,
    ).slice();
    const candidateEntry = currentEntry.slice();
    candidateEntry.set(
      bytes.subarray(overlapStart - writeStart, overlapEnd - writeStart),
      overlapStart - entryStart,
    );
    const candidateOfdId = candidateOfdIdFromFdEntry(candidateEntry);
    const explicitFdEntry = msg.fdEntries?.find((entry) => entry.fd === fd);
    if (
      decide(msg.vmPtr, fd, candidateOfdId, explicitFdEntry?.ofdId ?? 0) !== skipAction
    ) {
      continue;
    }
    if (masked === null) {
      masked = bytes.slice();
    }
    masked.set(
      currentEntry.subarray(overlapStart - entryStart, overlapEnd - entryStart),
      overlapStart - writeStart,
    );
  }
  return masked ?? bytes;
}

export function maskIncompatibleOfdSlotKernelWrite(
  handle: ProcessHandle,
  ptr: number,
  bytes: Uint8Array,
): Uint8Array {
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return bytes;
  }
  const tableStart = handle.kernelState.ofdTablePtr;
  const tableEnd = tableStart + tableBytes;
  const writeStart = ptr;
  const writeEnd = ptr + bytes.length;
  if (writeEnd <= tableStart || writeStart >= tableEnd) {
    return bytes;
  }

  let masked: Uint8Array | null = null;
  const decide = requireSupervisorOfdSlotWriteAction(handle);
  const skipAction = requireSupervisorRestoreActionSkip(handle);
  const firstOfdId = Math.max(1, Math.floor((Math.max(writeStart, tableStart) - tableStart) / slotBytes));
  const lastOfdId = Math.min(
    maxFds - 1,
    Math.floor((Math.min(writeEnd - 1, tableEnd - 1) - tableStart) / slotBytes),
  );
  for (let ofdId = firstOfdId; ofdId <= lastOfdId; ofdId += 1) {
    const slotStart = tableStart + ofdId * slotBytes;
    const slotEnd = slotStart + slotBytes;
    const overlapStart = Math.max(writeStart, slotStart);
    const overlapEnd = Math.min(writeEnd, slotEnd);
    if (overlapStart >= overlapEnd) {
      continue;
    }
    const currentSlot = new Uint8Array(handle.memory.buffer, slotStart, slotBytes).slice();
    const candidateSlot = currentSlot.slice();
    candidateSlot.set(
      bytes.subarray(overlapStart - writeStart, overlapEnd - writeStart),
      overlapStart - slotStart,
    );
    if (!ofdSlotWriteShouldBeSkipped(ofdId, candidateSlot, decide, skipAction)) {
      continue;
    }
    if (masked === null) {
      masked = bytes.slice();
    }
    masked.set(
      currentSlot.subarray(overlapStart - slotStart, overlapEnd - slotStart),
      overlapStart - writeStart,
    );
  }
  return masked ?? bytes;
}

function recordRegularFileOfdOffsets(
  handle: ProcessHandle,
  msg: StatusMessageLike,
  slotBytes: number,
  appliedOfdIds: ReadonlySet<number>,
): void {
  const fileType = handle.kernel.abi_fd_type_file?.();
  if (fileType === undefined || !msg.ofdSlots || msg.ofdSlots.length === 0) {
    return;
  }
  for (const snapshot of msg.ofdSlots) {
    if (
      snapshot.ofdId <= 0
      || snapshot.ofdId >= handle.kernel.abi_max_fds()
      || snapshot.slot.length !== slotBytes
      || !appliedOfdIds.has(snapshot.ofdId)
    ) {
      continue;
    }
    const view = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
    if (view.getInt32(OFD_TYPE_OFFSET, true) !== fileType) {
      continue;
    }
    recordRegularFileOfdOffsetSnapshot(handle, snapshot.ofdId, {
      ino: view.getUint32(OFD_INO_OFFSET, true),
      hostFd: view.getInt32(OFD_HOST_FD_OFFSET, true),
      offset: view.getBigInt64(OFD_OFFSET_OFFSET, true),
    });
  }
}

function regularFileOfdIdFromAppliedSlot(
  handle: ProcessHandle,
  snapshot: OfdSlotSnapshotLike,
  slot: Uint8Array,
): number | null {
  const fileType = handle.kernel.abi_fd_type_file?.();
  if (fileType === undefined) {
    return null;
  }
  const candidate = readOfdSlotCandidate(slot);
  return candidate.type === fileType ? snapshot.ofdId : null;
}

function applyEventfdTraceOffset(
  handle: ProcessHandle,
  msg: StatusMessageLike,
  slotBytes: number,
): boolean {
  const eventfdType = handle.kernel.abi_fd_type_eventfd?.();
  if (
    eventfdType === undefined
    || (
      msg.fdTraceSyscallNr !== SYS_READ
      && msg.fdTraceSyscallNr !== SYS_WRITE
      && msg.fdTraceSyscallNr !== SYS_READV
      && msg.fdTraceSyscallNr !== SYS_WRITEV
    )
    || msg.fdTraceArg0OfdType !== eventfdType
    || msg.fdTraceArg0OfdId === undefined
    || msg.fdTraceArg0OfdOffset === undefined
    || msg.fdTraceResult === undefined
    || msg.fdTraceResult < 0n
  ) {
    return false;
  }
  const ofdId = msg.fdTraceArg0OfdId;
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return false;
  }
  const offset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
  if (offset < 0 || offset + slotBytes > handle.memory.buffer.byteLength) {
    return false;
  }
  const view = new DataView(handle.memory.buffer, offset, slotBytes);
  if (view.getInt32(OFD_TYPE_OFFSET, true) !== eventfdType) {
    return false;
  }
  view.setBigInt64(OFD_OFFSET_OFFSET, msg.fdTraceArg0OfdOffset, true);
  return true;
}

function isLiveEpollDependentOfdType(handle: ProcessHandle, type: number): boolean {
  return type === handle.kernel.abi_fd_type_epoll?.()
    || type === handle.kernel.abi_fd_type_eventfd?.()
    || type === handle.kernel.abi_fd_type_timerfd?.();
}

function statusMessageCanChangeOfdStatusFlags(
  msg: StatusMessageLike,
  ofdId: number,
): boolean {
  if (
    msg.fdTraceArg0OfdId !== ofdId
    || msg.fdTraceResult === undefined
    || msg.fdTraceResult < 0n
  ) {
    return false;
  }
  if (msg.fdTraceSyscallNr === SYS_FCNTL && msg.fdTraceArg1 === F_SETFL) {
    return true;
  }
  return msg.fdTraceSyscallNr === SYS_IOCTL;
}

function preserveLiveEpollDependentStatusFlags(
  handle: ProcessHandle,
  msg: StatusMessageLike,
  snapshot: OfdSlotSnapshotLike,
  slotBytes: number,
): Uint8Array {
  const targetOffset = handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes;
  if (targetOffset < 0 || targetOffset + slotBytes > handle.memory.buffer.byteLength) {
    return snapshot.slot;
  }
  const currentView = new DataView(handle.memory.buffer, targetOffset, slotBytes);
  const snapshotView = new DataView(
    snapshot.slot.buffer,
    snapshot.slot.byteOffset,
    snapshot.slot.byteLength,
  );
  const currentType = currentView.getInt32(OFD_TYPE_OFFSET, true);
  const snapshotType = snapshotView.getInt32(OFD_TYPE_OFFSET, true);
  if (
    currentType !== snapshotType
    || !isLiveEpollDependentOfdType(handle, currentType)
    || statusMessageCanChangeOfdStatusFlags(msg, snapshot.ofdId)
  ) {
    return snapshot.slot;
  }
  const currentFlags = currentView.getInt32(OFD_STATUS_FLAGS_OFFSET, true);
  const snapshotFlags = snapshotView.getInt32(OFD_STATUS_FLAGS_OFFSET, true);
  if (currentFlags === snapshotFlags) {
    return snapshot.slot;
  }
  const merged = snapshot.slot.slice();
  new DataView(merged.buffer, merged.byteOffset, merged.byteLength)
    .setInt32(OFD_STATUS_FLAGS_OFFSET, currentFlags, true);
  return merged;
}

function preserveLiveOfdRefcount(
  handle: ProcessHandle,
  msg: StatusMessageLike,
  snapshot: OfdSlotSnapshotLike,
  slotBytes: number,
): Uint8Array {
  const targetOffset = handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes;
  if (targetOffset < 0 || targetOffset + slotBytes > handle.memory.buffer.byteLength) {
    return snapshot.slot;
  }
  const current = new DataView(handle.memory.buffer, targetOffset, slotBytes);
  const candidate = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
  const currentType = current.getInt32(OFD_TYPE_OFFSET, true);
  if (currentType === 0 || currentType !== candidate.getInt32(OFD_TYPE_OFFSET, true)) {
    return snapshot.slot;
  }
  const candidateRefcount = candidate.getInt32(OFD_REFCOUNT_OFFSET, true);
  const processHeldRefcount = handle.kernel.supervisor_process_held_ofd_refcount;
  if (!processHeldRefcount) {
    throw new Error("kernel supervisor ABI missing process-held OFD refcount export");
  }
  const heldRefcount = Math.max(0, processHeldRefcount.call(handle.kernel, msg.vmPtr, snapshot.ofdId));
  if (candidateRefcount >= heldRefcount) {
    return snapshot.slot;
  }
  const merged = snapshot.slot.slice();
  new DataView(merged.buffer, merged.byteOffset, merged.byteLength)
    .setInt32(OFD_REFCOUNT_OFFSET, heldRefcount, true);
  return merged;
}

function preserveFcntlSetflAccessMode(
  handle: ProcessHandle,
  msg: StatusMessageLike,
  snapshot: OfdSlotSnapshotLike,
  slotBytes: number,
): Uint8Array {
  if (
    msg.fdTraceSyscallNr !== SYS_FCNTL
    || msg.fdTraceArg1 !== F_SETFL
    || msg.fdTraceArg0OfdId !== snapshot.ofdId
    || msg.fdTraceResult === undefined
    || msg.fdTraceResult < 0n
  ) {
    return snapshot.slot;
  }
  const targetOffset = handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes;
  if (targetOffset < 0 || targetOffset + slotBytes > handle.memory.buffer.byteLength) {
    return snapshot.slot;
  }
  const currentView = new DataView(handle.memory.buffer, targetOffset, slotBytes);
  const snapshotView = new DataView(
    snapshot.slot.buffer,
    snapshot.slot.byteOffset,
    snapshot.slot.byteLength,
  );
  if (currentView.getInt32(OFD_TYPE_OFFSET, true) !== snapshotView.getInt32(OFD_TYPE_OFFSET, true)) {
    return snapshot.slot;
  }
  const currentFlags = currentView.getInt32(OFD_STATUS_FLAGS_OFFSET, true);
  const snapshotFlags = snapshotView.getInt32(OFD_STATUS_FLAGS_OFFSET, true);
  const currentAccessMode = currentFlags & OPEN_ACCESS_MODE_MASK;
  if ((snapshotFlags & OPEN_ACCESS_MODE_MASK) === currentAccessMode) {
    return snapshot.slot;
  }
  const merged = snapshot.slot.slice();
  new DataView(merged.buffer, merged.byteOffset, merged.byteLength)
    .setInt32(
      OFD_STATUS_FLAGS_OFFSET,
      (snapshotFlags & ~OPEN_ACCESS_MODE_MASK) | currentAccessMode,
      true,
    );
  return merged;
}

export function applyThreadEventfdTraceOffset(
  handle: ProcessHandle,
  msg: StatusMessageLike,
): boolean {
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return false;
  }
  return applyEventfdTraceOffset(handle, msg, slotBytes);
}

export function applyThreadOfdSlots(
  handle: ProcessHandle,
  msg: StatusMessageLike,
  appendStatusSocketDebug?: (handle: ProcessHandle, event: string) => void,
): void {
  restoreNetworkOfdSlotWatermarks(handle, appendStatusSocketDebug);
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
  }
  if (msg.ofdSlots && msg.ofdSlots.length > 0) {
    const protectedNetworkOfds = new Set(networkConnectionOfds(handle).values());
    const memory = new Uint8Array(handle.memory.buffer);
    const decide = requireSupervisorOfdSlotRestoreAction(handle);
    const skipAction = requireSupervisorRestoreActionSkip(handle);
    const syscallNr = msg.syscallNr ?? SUPERVISOR_RESTORE_SYSCALL_NONE;
    const trustRegularFileOfdOffsets = msg.trustRegularFileOfdOffsets === true;
    const allowRegularFileIdentityReplacement = msg.allowRegularFileIdentityReplacement === true;
    const appliedRegularFileOfdIds: number[] = [];
    for (const snapshot of msg.ofdSlots) {
      if (
        snapshot.ofdId <= 0
        || snapshot.ofdId >= maxFds
        || snapshot.slot.length !== slotBytes
      ) {
        continue;
      }
      if (protectedNetworkOfds.has(snapshot.ofdId)) {
        continue;
      }
      if (
        ofdSnapshotWouldClobberExistingSlot(
          handle,
          msg.fdEntries,
          snapshot,
          slotBytes,
          decide,
          skipAction,
          allowRegularFileIdentityReplacement,
          trustRegularFileOfdOffsets
            || msg.trustRegularFileOffsetOfdIds?.has(snapshot.ofdId) === true,
          syscallNr,
        )
      ) {
        continue;
      }
      const refcountMergedSlot = preserveLiveOfdRefcount(handle, msg, snapshot, slotBytes);
      const statusMergedSlot = preserveFcntlSetflAccessMode(
        handle,
        msg,
        { ...snapshot, slot: refcountMergedSlot },
        slotBytes,
      );
      const slot = preserveLiveEpollDependentStatusFlags(
        handle,
        msg,
        { ...snapshot, slot: statusMergedSlot },
        slotBytes,
      );
      memory.set(slot, handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes);
      const regularFileOfdId = regularFileOfdIdFromAppliedSlot(handle, snapshot, slot);
      if (regularFileOfdId !== null) {
        writeRegularFileOfdOffsetState(
          handle,
          regularFileOfdId,
          slotBytes,
          new DataView(slot.buffer, slot.byteOffset, slot.byteLength)
            .getBigInt64(OFD_OFFSET_OFFSET, true),
        );
        appliedRegularFileOfdIds.push(regularFileOfdId);
      }
    }
    if (msg.recordRegularFileOfdOffsets !== false) {
      recordRegularFileOfdOffsets(handle, msg, slotBytes, new Set(appliedRegularFileOfdIds));
    } else {
      for (const ofdId of appliedRegularFileOfdIds) {
        deleteRegularFileOfdOffsetSnapshot(handle, ofdId);
      }
    }
  }
  applyEventfdTraceOffset(handle, msg, slotBytes);
  restoreNetworkOfdSlotWatermarks(handle, appendStatusSocketDebug);
}
