import type { KernelSyncHandle } from "../types.js";
import {
  clearKernelRegion,
  copyKernelRegion,
  kernelWorkerOwnsEpollState,
  kernelWorkerOwnsPipeState,
  kernelWorkerOwnsSocketState,
} from "./common.js";
import { clearMemfsState, syncMemfsState } from "./memfs.js";

export function copySelectedOfdState(
  source: KernelSyncHandle,
  target: KernelSyncHandle,
  ofdIds: Iterable<number>,
): void {
  const maxFds = source.kernel.abi_max_fds();
  const tableBytes = source.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }

  for (const ofdId of ofdIds) {
    if (ofdId <= 0 || ofdId >= maxFds) {
      continue;
    }
    const slotOffset = source.kernelState.ofdTablePtr + ofdId * slotBytes;
    copyKernelRegion(source, slotOffset, target, target.kernelState.ofdTablePtr + ofdId * slotBytes, slotBytes);
  }
}

export function clearEpollState(target: KernelSyncHandle): void {
  const tableBytes = target.kernel.kernel_epoll_table_bytes();
  const instanceBytes = target.kernel.kernel_epoll_instance_bytes();
  const tableEntries = tableBytes >>> 2;
  const table = new Uint32Array(target.memory.buffer, target.kernelState.epollTablePtr, tableEntries);
  const instancePtrs = [...table].filter((ptr) => ptr !== 0);

  clearKernelRegion(target, target.kernelState.epollTablePtr, tableBytes);
  for (const ptr of instancePtrs) {
    clearKernelRegion(target, ptr, instanceBytes);
  }
}

export function syncEpollState(source: KernelSyncHandle, target: KernelSyncHandle): void {
  const tableBytes = source.kernel.kernel_epoll_table_bytes();
  const instanceBytes = source.kernel.kernel_epoll_instance_bytes();
  const tableEntries = tableBytes >>> 2;
  const sourceTable = new Uint32Array(source.memory.buffer, source.kernelState.epollTablePtr, tableEntries);
  const targetTable = new Uint32Array(target.memory.buffer, target.kernelState.epollTablePtr, tableEntries);
  const targetPtrs = [...targetTable];
  for (let i = 0; i < tableEntries; i += 1) {
    const sourcePtr = sourceTable[i] ?? 0;
    const targetPtr = targetPtrs[i] ?? 0;
    if (sourcePtr === 0) {
      targetTable[i] = 0;
      if (targetPtr !== 0) {
        clearKernelRegion(target, targetPtr, instanceBytes);
      }
      continue;
    }
    const instancePtr = targetPtr !== 0 ? targetPtr : sourcePtr;
    targetTable[i] = instancePtr;
    copyKernelRegion(source, sourcePtr, target, instancePtr, instanceBytes);
  }
}

export function copySocketState(source: KernelSyncHandle, target: KernelSyncHandle): void {
  copyKernelRegion(
    source,
    source.kernelState.socketTablePtr,
    target,
    target.kernelState.socketTablePtr,
    source.kernel.kernel_socket_table_bytes(),
  );
  copyKernelRegion(
    source,
    source.kernelState.netBufBase,
    target,
    target.kernelState.netBufBase,
    source.kernel.kernel_net_buf_pool_bytes(),
  );
}

export function discardCopiedSharedKernelObjects(
  handle: KernelSyncHandle,
  preservedOfdSlots: ReadonlyMap<number, Uint8Array> | null = null,
  options: { clearMemfs?: boolean } = {},
): void {
  const clearMemfsObjects = options.clearMemfs ?? true;
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  clearKernelRegion(handle, handle.kernelState.ofdTablePtr, tableBytes);
  if (preservedOfdSlots && preservedOfdSlots.size > 0 && Number.isInteger(slotBytes) && slotBytes > 0) {
    const bytes = new Uint8Array(handle.memory.buffer);
    for (const [ofdId, slot] of preservedOfdSlots) {
      bytes.set(slot, handle.kernelState.ofdTablePtr + ofdId * slotBytes);
    }
  }
  if (clearMemfsObjects) {
    clearMemfsState(handle);
  }
  clearKernelRegion(
    handle,
    handle.kernelState.pipeBase,
    handle.kernel.kernel_pipe_region_bytes(),
  );
  clearEpollState(handle);
  clearKernelRegion(
    handle,
    handle.kernelState.socketTablePtr,
    handle.kernel.kernel_socket_table_bytes(),
  );
  clearKernelRegion(
    handle,
    handle.kernelState.netBufBase,
    handle.kernel.kernel_net_buf_pool_bytes(),
  );
}

export function restoreLocalOfdSlots(
  handle: KernelSyncHandle,
  preservedOfdSlots: ReadonlyMap<number, Uint8Array> | null,
): void {
  if (!preservedOfdSlots || preservedOfdSlots.size === 0) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  for (const [ofdId, slot] of preservedOfdSlots) {
    bytes.set(slot, handle.kernelState.ofdTablePtr + ofdId * slotBytes);
  }
}

export function copyLegacySharedKernelObjectsLocal(
  source: KernelSyncHandle,
  target: KernelSyncHandle,
  sharedOfdIds: Iterable<number>,
  options: { includeMemfs?: boolean } = {},
): void {
  if (source === target) {
    return;
  }
  const includeMemfs = options.includeMemfs ?? true;
  const includePipe = !kernelWorkerOwnsPipeState(source) && !kernelWorkerOwnsPipeState(target);
  const includeEpoll = !kernelWorkerOwnsEpollState(source) && !kernelWorkerOwnsEpollState(target);
  const includeSocket = !kernelWorkerOwnsSocketState(source)
    && !kernelWorkerOwnsSocketState(target);

  copySelectedOfdState(source, target, sharedOfdIds);
  if (includeMemfs) {
    syncMemfsState(source, target);
  }
  if (includePipe) {
    copyKernelRegion(
      source,
      source.kernelState.pipeBase,
      target,
      target.kernelState.pipeBase,
      source.kernel.kernel_pipe_region_bytes(),
    );
  }
  if (includeSocket) {
    copySocketState(source, target);
  }
  if (includeEpoll) {
    syncEpollState(source, target);
  }
  target.kernelState = {
    ...target.kernelState,
    socketUsed: includeSocket ? source.kernelState.socketUsed : target.kernelState.socketUsed,
    pipeUsed: includePipe ? source.kernelState.pipeUsed : target.kernelState.pipeUsed,
    netBufUsed: includeSocket ? source.kernelState.netBufUsed : target.kernelState.netBufUsed,
  };
}
