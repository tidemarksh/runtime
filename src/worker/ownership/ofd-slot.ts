import type { ProcessHandle } from "../types.js";

const OFD_TYPE_OFFSET = 0;
const OFD_HOST_FD_OFFSET = 4;
const OFD_REFCOUNT_OFFSET = 12;
const OFD_OFFSET_OFFSET = 16;
const OFD_INO_OFFSET = 24;

interface FdEntryLike {
  ofdId: number;
}

interface OfdSlotSnapshotLike {
  ofdId: number;
  slot: Uint8Array;
}

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
