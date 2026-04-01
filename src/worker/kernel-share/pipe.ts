import type { KernelSyncHandle, ProcessHandle } from "../types.js";

export function snapshotLocalPipeSlots(
  handle: ProcessHandle,
  ofdIds: Iterable<number>,
): Map<number, Uint8Array> {
  const maxPipes = handle.kernel.abi_max_pipes();
  const regionBytes = handle.kernel.kernel_pipe_region_bytes();
  const slotBytes = regionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid pipe region layout: bytes=${regionBytes} maxPipes=${maxPipes}`);
  }

  const snapshots = new Map<number, Uint8Array>();
  for (const ofdId of ofdIds) {
    if (handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()) {
      continue;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    if (pipeSlot < 0 || pipeSlot >= maxPipes || snapshots.has(pipeSlot)) {
      continue;
    }
    const slotOffset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
    snapshots.set(pipeSlot, new Uint8Array(handle.memory.buffer, slotOffset, slotBytes).slice());
  }
  return snapshots;
}

export function restoreLocalPipeSlots(
  handle: KernelSyncHandle,
  preservedPipeSlots: ReadonlyMap<number, Uint8Array> | null,
): void {
  if (!preservedPipeSlots || preservedPipeSlots.size === 0) {
    return;
  }
  const maxPipes = handle.kernel.abi_max_pipes();
  const regionBytes = handle.kernel.kernel_pipe_region_bytes();
  const slotBytes = regionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid pipe region layout: bytes=${regionBytes} maxPipes=${maxPipes}`);
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  for (const [pipeSlot, slot] of preservedPipeSlots) {
    bytes.set(slot, handle.kernelState.pipeBase + pipeSlot * slotBytes);
  }
}

export function formatPipeCounterDebug(handle: ProcessHandle, fd: number): string {
  const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
  if (ofdId === 0 || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()) {
    return `${fd}:ofd=0`;
  }
  const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
  const base = handle.kernelState.pipeBase + pipeSlot * (32 + 4096);
  const i32 = new Int32Array(handle.memory.buffer);
  const u32 = new Uint32Array(handle.memory.buffer);
  return [
    `${fd}:ofd=${ofdId}`,
    `slot=${pipeSlot}`,
    `ro=${Atomics.load(i32, (base + 8) >>> 2)}`,
    `wo=${Atomics.load(i32, (base + 12) >>> 2)}`,
    `head=${Atomics.load(u32, base >>> 2)}`,
    `tail=${Atomics.load(u32, (base + 4) >>> 2)}`,
  ].join(":");
}

export function formatOpenFdSummary(handle: ProcessHandle, limit = 16): string {
  return Array.from({ length: limit }, (_value, fd) => {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId === 0) {
      return null;
    }
    return `${fd}->${ofdId}/${handle.kernel.process_fd_flags(handle.vmPtr, fd)}/${handle.kernel.kernel_ofd_fd_type(ofdId)}/${handle.kernel.kernel_ofd_ino(ofdId)}/${handle.kernel.kernel_ofd_host_fd(ofdId)}`;
  }).filter(Boolean).join(" ");
}
