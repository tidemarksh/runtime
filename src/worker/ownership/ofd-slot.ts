import type { ProcessHandle } from "../types.js";
import {
  networkConnectionOfds,
  restoreNetworkOfdSlotWatermarks,
} from "./socket-control.js";

const OFD_TYPE_OFFSET = 0;
const OFD_HOST_FD_OFFSET = 4;
const OFD_REFCOUNT_OFFSET = 12;
const OFD_OFFSET_OFFSET = 16;
const OFD_INO_OFFSET = 24;

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
}

const FD_ENTRY_BYTES = 8;
const FD_ENTRY_OFD_ID_OFFSET = 0;
const maxRegularFileOffsetByHandle = new WeakMap<ProcessHandle, Map<number, bigint>>();

export function explicitPipeFdEntryWouldRollBack(
  handle: ProcessHandle,
  explicitOfdId: number | undefined,
  currentOfdId: number,
  candidateOfdId: number,
): boolean {
  const maxFds = handle.kernel.abi_max_fds();
  if (
    explicitOfdId === undefined
    || currentOfdId <= 0
    || currentOfdId >= maxFds
    || candidateOfdId === currentOfdId
    || explicitOfdId !== currentOfdId
  ) {
    return false;
  }
  const currentType = handle.kernel.kernel_ofd_fd_type(currentOfdId);
  return currentType === handle.kernel.abi_fd_type_pipe()
    || currentType === handle.kernel.abi_fd_type_pty_master?.()
    || currentType === handle.kernel.abi_fd_type_pty_slave?.();
}

export function regularFileFdEntryWouldRollBack(
  handle: ProcessHandle,
  fileType: number,
  currentOfdId: number,
  candidateOfdId: number,
): boolean {
  const maxFds = handle.kernel.abi_max_fds();
  if (
    currentOfdId <= 0
    || candidateOfdId <= 0
    || currentOfdId >= maxFds
    || candidateOfdId >= maxFds
    || currentOfdId === candidateOfdId
  ) {
    return false;
  }
  return handle.kernel.kernel_ofd_fd_type(currentOfdId) === fileType
    && handle.kernel.kernel_ofd_fd_type(candidateOfdId) === fileType
    && handle.kernel.kernel_ofd_host_fd(currentOfdId) === handle.kernel.kernel_ofd_host_fd(candidateOfdId)
    && handle.kernel.kernel_ofd_ino(currentOfdId) === handle.kernel.kernel_ofd_ino(candidateOfdId)
    && handle.kernel.kernel_ofd_refcount(currentOfdId) > 0
    && handle.kernel.kernel_ofd_offset(candidateOfdId) < handle.kernel.kernel_ofd_offset(currentOfdId);
}

export function ofdSlotWriteWouldClobberIdentityOrRollBack(
  handle: ProcessHandle,
  currentSlot: Uint8Array,
  candidateSlot: Uint8Array,
): boolean {
  const currentView = new DataView(currentSlot.buffer, currentSlot.byteOffset, currentSlot.byteLength);
  const candidateView = new DataView(candidateSlot.buffer, candidateSlot.byteOffset, candidateSlot.byteLength);
  const currentType = currentView.getInt32(OFD_TYPE_OFFSET, true);
  const candidateType = candidateView.getInt32(OFD_TYPE_OFFSET, true);
  if (currentType === 0) {
    return false;
  }
  if (currentView.getInt32(OFD_REFCOUNT_OFFSET, true) <= 0) {
    return false;
  }
  if (candidateType === 0 || candidateType !== currentType) {
    return true;
  }
  if (
    currentView.getInt32(OFD_HOST_FD_OFFSET, true) !== candidateView.getInt32(OFD_HOST_FD_OFFSET, true)
    || currentView.getUint32(OFD_INO_OFFSET, true) !== candidateView.getUint32(OFD_INO_OFFSET, true)
  ) {
    return true;
  }
  return currentType === handle.kernel.abi_fd_type_file?.()
    && candidateView.getBigInt64(OFD_OFFSET_OFFSET, true) < currentView.getBigInt64(OFD_OFFSET_OFFSET, true);
}

export function ofdSnapshotWouldClobberExistingSlot(
  handle: ProcessHandle,
  fdEntries: readonly FdEntryLike[] | undefined,
  snapshot: OfdSlotSnapshotLike,
  slotBytes: number,
): boolean {
  const targetOffset = handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes;
  if (targetOffset < 0 || targetOffset + slotBytes > handle.memory.buffer.byteLength) {
    return true;
  }
  if ((fdEntries ?? []).some((entry) => entry.ofdId === snapshot.ofdId)) {
    return false;
  }
  const targetView = new DataView(handle.memory.buffer, targetOffset, slotBytes);
  const targetType = targetView.getInt32(OFD_TYPE_OFFSET, true);
  if (targetType === 0 || targetView.getInt32(OFD_REFCOUNT_OFFSET, true) <= 0) {
    return false;
  }
  const sourceView = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
  const sourceType = sourceView.getInt32(OFD_TYPE_OFFSET, true);
  if (sourceType === 0 || sourceType !== targetType) {
    return true;
  }
  return sourceView.getInt32(OFD_HOST_FD_OFFSET, true) !== targetView.getInt32(OFD_HOST_FD_OFFSET, true)
    || sourceView.getUint32(OFD_INO_OFFSET, true) !== targetView.getUint32(OFD_INO_OFFSET, true);
}

export function maskRegularFileFdEntryRollbackGuestWrite(
  handle: ProcessHandle,
  msg: StatusMessageLike,
  guestPtr: number,
  bytes: Uint8Array,
): Uint8Array {
  const fileType = handle.kernel.abi_fd_type_file?.();
  const processPtr = msg.vmPtr === 0 ? 0 : handle.kernel.thread_process_ptr?.(msg.vmPtr);
  if (fileType === undefined || processPtr === undefined || processPtr <= 0) {
    return bytes;
  }
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
    const currentOfdId = new DataView(
      currentEntry.buffer,
      currentEntry.byteOffset,
      currentEntry.byteLength,
    ).getUint32(FD_ENTRY_OFD_ID_OFFSET, true);
    const candidateOfdId = new DataView(
      candidateEntry.buffer,
      candidateEntry.byteOffset,
      candidateEntry.byteLength,
    ).getUint32(FD_ENTRY_OFD_ID_OFFSET, true);
    const explicitFdEntry = msg.fdEntries?.find((entry) => entry.fd === fd);
    if (
      !regularFileFdEntryWouldRollBack(handle, fileType, currentOfdId, candidateOfdId)
      && !explicitPipeFdEntryWouldRollBack(handle, explicitFdEntry?.ofdId, currentOfdId, candidateOfdId)
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
    if (!ofdSlotWriteWouldClobberIdentityOrRollBack(handle, currentSlot, candidateSlot)) {
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

export function ofdSnapshotWouldRollBackRegularFileOffset(
  handle: ProcessHandle,
  msg: StatusMessageLike,
  snapshot: OfdSlotSnapshotLike,
  slotBytes: number,
): boolean {
  const fileType = handle.kernel.abi_fd_type_file?.();
  if (fileType === undefined) {
    return false;
  }
  const sourceView = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
  const sourceType = sourceView.getInt32(OFD_TYPE_OFFSET, true);
  if (sourceType !== fileType) {
    return false;
  }
  const targetType = handle.kernel.kernel_ofd_fd_type(snapshot.ofdId);
  if (targetType !== fileType) {
    return false;
  }
  const targetOffset = handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes;
  const targetView = new DataView(handle.memory.buffer, targetOffset, slotBytes);
  const targetRefcount = targetView.getInt32(OFD_REFCOUNT_OFFSET, true);
  if (targetRefcount <= 0) {
    return false;
  }
  const sourceHostFd = sourceView.getInt32(OFD_HOST_FD_OFFSET, true);
  const targetHostFd = targetView.getInt32(OFD_HOST_FD_OFFSET, true);
  const sourceIno = sourceView.getUint32(OFD_INO_OFFSET, true);
  const targetIno = targetView.getUint32(OFD_INO_OFFSET, true);
  if (sourceHostFd !== targetHostFd || sourceIno !== targetIno) {
    return false;
  }
  const sourceOffset = sourceView.getBigInt64(OFD_OFFSET_OFFSET, true);
  const targetCurrentOffset = targetView.getBigInt64(OFD_OFFSET_OFFSET, true);
  let maxOffsets = maxRegularFileOffsetByHandle.get(handle);
  if (maxOffsets === undefined) {
    maxOffsets = new Map();
    maxRegularFileOffsetByHandle.set(handle, maxOffsets);
  }
  const maxSeenOffset = maxOffsets.get(snapshot.ofdId) ?? targetCurrentOffset;
  const nextMaxOffset = sourceOffset > targetCurrentOffset ? sourceOffset : targetCurrentOffset;
  if (nextMaxOffset > maxSeenOffset) {
    maxOffsets.set(snapshot.ofdId, nextMaxOffset);
  }
  if (sourceOffset < maxSeenOffset) {
    return true;
  }
  if (sourceOffset >= targetCurrentOffset) {
    return false;
  }
  const syscallNr = msg.syscallNr;
  const lseekNr = handle.kernel.abi_sys_lseek?.();
  return syscallNr === undefined || syscallNr !== BigInt(lseekNr ?? 62);
}

export function recordRegularFileOfdOffsets(
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
    if (ofdSnapshotWouldClobberExistingSlot(handle, msg.fdEntries, snapshot, slotBytes)) {
      continue;
    }
    if (ofdSnapshotWouldRollBackRegularFileOffset(handle, msg, snapshot, slotBytes)) {
      continue;
    }
    memory.set(snapshot.slot, handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes);
  }
  restoreNetworkOfdSlotWatermarks(handle, appendStatusSocketDebug);
  recordRegularFileOfdOffsets(handle, msg, slotBytes);
}
