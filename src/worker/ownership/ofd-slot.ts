import type { ProcessHandle } from "../types.js";
import {
  FD_ENTRY_BYTES,
  FD_ENTRY_OFD_ID_OFFSET,
} from "../../layout/fd.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
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
  trustRegularFileOfdOffsets?: boolean;
}

const SUPERVISOR_RESTORE_SYSCALL_NONE = 0xffffffffffffffffn;

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
  trustRegularFileOfdOffsets: boolean,
  syscallNr: bigint | number,
): boolean {
  const targetOffset = handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes;
  if (targetOffset < 0 || targetOffset + slotBytes > handle.memory.buffer.byteLength) {
    return true;
  }
  const candidate = readOfdSlotCandidate(snapshot.slot);
  const flags = supervisorOfdSlotRestoreFlags(
    handle,
    ofdSnapshotReferencedByFdEntry(fdEntries, snapshot.ofdId),
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
    ) {
      continue;
    }
    const view = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
    if (view.getInt32(OFD_TYPE_OFFSET, true) !== fileType) {
      continue;
    }
    const offsets = handle.regularFileOfdOffsets ?? new Map<number, { ino: number; hostFd: number; offset: bigint }>();
    offsets.set(snapshot.ofdId, {
      ino: view.getUint32(OFD_INO_OFFSET, true),
      hostFd: view.getInt32(OFD_HOST_FD_OFFSET, true),
      offset: view.getBigInt64(OFD_OFFSET_OFFSET, true),
    });
    handle.regularFileOfdOffsets = offsets;
  }
}

export function applyThreadOfdSlots(
  handle: ProcessHandle,
  msg: StatusMessageLike,
  appendStatusSocketDebug?: (handle: ProcessHandle, event: string) => void,
): void {
  restoreNetworkOfdSlotWatermarks(handle, appendStatusSocketDebug);
  if (!msg.ofdSlots || msg.ofdSlots.length === 0) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const protectedNetworkOfds = new Set(networkConnectionOfds(handle).values());
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  const decide = requireSupervisorOfdSlotRestoreAction(handle);
  const skipAction = requireSupervisorRestoreActionSkip(handle);
  const syscallNr = msg.syscallNr ?? SUPERVISOR_RESTORE_SYSCALL_NONE;
  const trustRegularFileOfdOffsets = msg.trustRegularFileOfdOffsets === true;
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
        trustRegularFileOfdOffsets,
        syscallNr,
      )
    ) {
      continue;
    }
    memory.set(snapshot.slot, handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes);
  }
  restoreNetworkOfdSlotWatermarks(handle, appendStatusSocketDebug);
  recordRegularFileOfdOffsets(handle, msg, slotBytes);
}
