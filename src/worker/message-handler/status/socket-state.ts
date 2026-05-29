import { attachKernelRuntimeState } from "../../../abi.js";
import { FS_RING_TOTAL_BYTES } from "../../../layout/fs-ring.js";
import {
  forceNetworkClosedFds,
} from "../../network-socket-close.js";
import type { ProcessHandle } from "../../types.js";
import type { ThreadWorkerStatusMessage } from "../types.js";
import {
  appendStatusSocketDebug,
  formatBytesAscii,
  pipeSlotLifecycleDebugEnabled,
  recordStatusSocketMetaTransition,
  STATUS_SOCKET_DEBUG_LOG_ENABLED,
} from "./debug.js";
import {
  applyThreadMemfsKernelWriteToForkShareMembers,
} from "./memfs-kernel-write.js";
import {
  maskIncompatibleOfdSlotKernelWrite,
  maskRegularFileFdEntryRollbackGuestWrite,
} from "../../ownership/ofd-slot.js";
import type { WorkerMessageHandlerDeps } from "../types.js";
import {
  deriveNetBufUsedFromSocketSlots,
  closedSocketRecvBufferIndices,
  netBufUsedMaskFromBufferIndices,
  networkAuthoritativeFds,
  preserveClosedNetworkRecvBuffers,
  preserveNetworkRecvBuffers,
  preserveNetworkRecvForKernelMemoryWrite,
  preservedSocketRecvBufferIndices,
  preserveUsedNetBufferHeads,
  readNetworkRecvBufferDebug,
  restoreMergedNetBufferHeads,
  restoreNetworkRecvBufferProducers,
} from "./network-recv.js";

interface KernelMemoryWriteOptions {
  skipFsRingWrites?: boolean;
}

interface PreservedNetworkSocketSlot {
  offset: number;
  data: Uint8Array;
  socketMask: number;
}

function socketTablePtrs(handle: ProcessHandle): number[] {
  const ptrs = [handle.kernelState.socketTablePtr];
  const activePtr = handle.kernel.kernel_socket_table_ptr?.();
  if (activePtr !== undefined && !ptrs.includes(activePtr)) {
    ptrs.push(activePtr);
  }
  return ptrs;
}

function activeSocketTablePtr(handle: ProcessHandle): number {
  return handle.kernel.kernel_socket_table_ptr?.() ?? handle.kernelState.socketTablePtr;
}

export function applyThreadGuestMemoryWrites(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  appendRuntimeLifecycleEvent?: WorkerMessageHandlerDeps["appendRuntimeLifecycleEvent"],
): void {
  if (!msg.guestMemoryWrites || msg.guestMemoryWrites.length === 0) {
    return;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  for (const write of msg.guestMemoryWrites) {
    const start = handle.guestBase + write.guestPtr;
    if (
      !Number.isFinite(start)
      || start < handle.guestBase
      || write.bytes.length === 0
      || start + write.bytes.length > memory.byteLength
    ) {
      continue;
    }
    const writeBytes = maskRegularFileFdEntryRollbackGuestWrite(handle, msg, write.guestPtr, write.bytes);
    memory.set(writeBytes, start);
    if (pipeSlotLifecycleDebugEnabled()) {
      appendRuntimeLifecycleEvent?.(
        handle.runtimeId,
        `guest-write-status:${handle.pid}:ptr=0x${write.guestPtr.toString(16)}`
          + `:len=${write.bytes.length}:data=${JSON.stringify(formatBytesAscii(write.bytes))}`,
      );
    }
  }
}

export function applyThreadKernelMemoryWrites(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  appendRuntimeLifecycleEvent?: WorkerMessageHandlerDeps["appendRuntimeLifecycleEvent"],
  options?: KernelMemoryWriteOptions,
): void {
  if (!msg.kernelMemoryWrites || msg.kernelMemoryWrites.length === 0) {
    return;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  for (const write of msg.kernelMemoryWrites) {
    if (
      !Number.isFinite(write.ptr)
      || write.ptr < 0
      || write.bytes.length === 0
      || write.ptr + write.bytes.length > memory.byteLength
    ) {
      continue;
    }
    if (options?.skipFsRingWrites === true && isFsRingKernelMemoryWrite(handle, msg, write.ptr, write.bytes.length)) {
      if (pipeSlotLifecycleDebugEnabled()) {
        appendRuntimeLifecycleEvent?.(
          handle.runtimeId,
          `kernel-write-status-skip-fs-ring:${handle.pid}:ptr=0x${write.ptr.toString(16)}:len=${write.bytes.length}`,
        );
      }
      continue;
    }
    const restoreNetworkRecv = preserveNetworkRecvForKernelMemoryWrite(handle, msg, write.ptr, write.bytes.length);
    const writeBytes = maskIncompatibleOfdSlotKernelWrite(handle, write.ptr, write.bytes);
    memory.set(writeBytes, write.ptr);
    restoreNetworkRecv?.();
    applyThreadMemfsKernelWriteToForkShareMembers(handle, write.ptr, write.bytes);
    if (pipeSlotLifecycleDebugEnabled()) {
      appendRuntimeLifecycleEvent?.(
        handle.runtimeId,
        `kernel-write-status:${handle.pid}:ptr=0x${write.ptr.toString(16)}:len=${write.bytes.length}`,
      );
    }
  }
}

function isFsRingKernelMemoryWrite(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  ptr: number,
  length: number,
): boolean {
  if (msg.status !== handle.kernel.abi_status_fs() || handle.kernel.thread_fs_ring_base === undefined) {
    return false;
  }
  const ringBase = handle.kernel.thread_fs_ring_base(msg.vmPtr);
  if (ringBase <= 0) {
    return false;
  }
  const ringEnd = ringBase + FS_RING_TOTAL_BYTES;
  return ptr < ringEnd && ptr + length > ringBase;
}

export function applyThreadSocketState(handle: ProcessHandle, msg: ThreadWorkerStatusMessage): void {
  if (!msg.socketState) {
    return;
  }
  const currentSocketUsed = handle.kernelState.socketUsed | handle.kernel.kernel_socket_used();
  const currentNetBufUsed = handle.kernelState.netBufUsed
    | handle.kernel.kernel_net_buf_used()
    | deriveNetBufUsedFromSocketSlots(handle, currentSocketUsed);
  const debugFds = networkAuthoritativeFds(handle);
  const socketDebugEntry = (fd: number, source: string) => {
    const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(msg.vmPtr, fd) ?? -1;
    const state = socketIdx >= 0 ? handle.kernel.thread_socket_state?.(socketIdx) ?? -1 : -1;
    const type = socketIdx >= 0 ? handle.kernel.thread_socket_type?.(socketIdx) ?? -1 : -1;
    const ext = socketIdx >= 0 ? handle.kernel.thread_socket_external_peer?.(socketIdx) ?? -1 : -1;
    const buf = socketIdx >= 0 ? handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1 : -1;
    const recv = socketIdx >= 0 ? handle.kernel.thread_socket_recv_available?.(socketIdx) ?? -1 : -1;
    recordStatusSocketMetaTransition(handle, source, fd, socketIdx, state, type, ext, buf, recv);
    return {
      fd,
      sock: socketIdx,
      state,
      type,
      ext,
      buf,
      recv,
      ht: readNetworkRecvBufferDebug(handle, socketIdx),
      avail: handle.kernel.thread_socket_outbound_available?.(msg.vmPtr, fd) ?? -1,
    };
  };
  const before = STATUS_SOCKET_DEBUG_LOG_ENABLED && debugFds.size > 0
    ? [...debugFds].map((fd) => socketDebugEntry(fd, "status-before"))
    : [];
  const pendingOutboundFds = [...debugFds].filter((fd) =>
    (handle.kernel.thread_socket_outbound_available?.(msg.vmPtr, fd) ?? 0) > 0
  );
  if (pendingOutboundFds.length > 0) {
    appendStatusSocketDebug(
      handle,
      `socket-state-skip:${handle.pid}:reason=pending-outbound:fds=${pendingOutboundFds.join(".")}:before=${JSON.stringify(before)}`,
    );
    return;
  }
  const socketTableBytes = handle.kernel.kernel_socket_table_bytes();
  const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes();
  const maxNetBuffers = Math.max(1, Number(handle.kernel.abi_max_sockets?.() ?? 32) * 2);
  const netBufSlotBytes = Math.floor(netBufPoolBytes / maxNetBuffers);
  const netBufPoolSizeValid = msg.socketState.netBufPool === undefined
    || msg.socketState.netBufPool.length === netBufPoolBytes;
  const netBufSlotsSizeValid = msg.socketState.netBufPool !== undefined
    || (msg.socketState.netBufSlots ?? []).every((slot) => (
      slot.bufferIndex >= 0
      && slot.bufferIndex < maxNetBuffers
      && slot.bytes.length === netBufSlotBytes
    ));
  if (
    msg.socketState.socketTable.length !== socketTableBytes
    || !netBufPoolSizeValid
    || !netBufSlotsSizeValid
  ) {
    if (debugFds.size > 0) {
      appendStatusSocketDebug(handle, `socket-state-skip:${handle.pid}:reason=size:socket=${msg.socketState.socketTable.length}/${socketTableBytes}:net=${msg.socketState.netBufPool?.length ?? -1}/${netBufPoolBytes}:slots=${msg.socketState.netBufSlots?.length ?? 0}`);
    }
    return;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  const preservedSocketSlots = preserveNetworkSocketSlots(handle, msg.vmPtr, socketTableBytes);
  const preservedSocketUsed = preservedSocketSlots.reduce((mask, slot) => mask | slot.socketMask, 0);
  const preservedSocketRecvBuffers = [
    ...preservedSocketRecvBufferIndices(preservedSocketSlots),
    ...closedSocketRecvBufferIndices(handle),
  ];
  const preservedSocketNetBufUsed = netBufUsedMaskFromBufferIndices(preservedSocketRecvBuffers);
  const preservedRecvBuffers = preserveNetworkRecvBuffers(handle, msg.vmPtr, netBufPoolBytes);
  const preservedNetBufferHeads = preserveUsedNetBufferHeads(
    handle,
    netBufPoolBytes,
    currentNetBufUsed | msg.socketState.netBufUsed | preservedSocketNetBufUsed,
    preservedSocketRecvBuffers,
  ).concat(preserveClosedNetworkRecvBuffers(handle, netBufPoolBytes));
  memory.set(msg.socketState.socketTable, handle.kernelState.socketTablePtr);
  if (msg.socketState.netBufPool !== undefined) {
    memory.set(msg.socketState.netBufPool, handle.kernelState.netBufBase);
  } else if (netBufSlotBytes > 0) {
    for (const slot of msg.socketState.netBufSlots ?? []) {
      memory.set(slot.bytes, handle.kernelState.netBufBase + slot.bufferIndex * netBufSlotBytes);
    }
  }
  restoreNetworkSocketSlots(memory, socketTablePtrs(handle), preservedSocketSlots);
  const mergedSocketUsed = msg.socketState.socketUsed | currentSocketUsed | preservedSocketUsed;
  const derivedNetBufUsed = deriveNetBufUsedFromSocketSlots(handle, mergedSocketUsed);
  restoreNetworkRecvBufferProducers(memory, handle.kernelState.netBufBase, preservedRecvBuffers);
  restoreMergedNetBufferHeads(memory, handle.kernelState.netBufBase, preservedNetBufferHeads);
  closeNetworkClosedFdPeers(handle, msg.vmPtr);
  handle.kernelState = {
    ...handle.kernelState,
    socketUsed: mergedSocketUsed,
    netBufUsed: msg.socketState.netBufUsed | currentNetBufUsed | derivedNetBufUsed | preservedSocketNetBufUsed,
  };
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  if (debugFds.size > 0) {
    const after = [...debugFds].map((fd) => socketDebugEntry(fd, "status-after"));
    appendStatusSocketDebug(handle, `socket-state-apply:${handle.pid}:socketUsed=0x${msg.socketState.socketUsed.toString(16)}:netBufUsed=0x${msg.socketState.netBufUsed.toString(16)}:derivedNetBufUsed=0x${derivedNetBufUsed.toString(16)}:before=${JSON.stringify(before)}:after=${JSON.stringify(after)}`);
  }
}

function closeNetworkClosedFdPeers(handle: ProcessHandle, vmPtr: number): void {
  forceNetworkClosedFds(handle, vmPtr);
}

function preserveNetworkSocketSlots(
  handle: ProcessHandle,
  vmPtr: number,
  socketTableBytes: number,
): PreservedNetworkSocketSlot[] {
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const socketSlotBytes = maxSockets > 0 ? socketTableBytes / maxSockets : 0;
  if (!Number.isInteger(socketSlotBytes) || socketSlotBytes <= 0) {
    return [];
  }
  const preserved: PreservedNetworkSocketSlot[] = [];
  const preservedOffsets = new Set<number>();
  const memory = new Uint8Array(handle.memory.buffer);
  const preserveSocketIdx = (socketIdx: number): void => {
    if (socketIdx < 0 || socketIdx >= maxSockets) {
      return;
    }
    const offset = socketIdx * socketSlotBytes;
    if (preservedOffsets.has(offset)) {
      return;
    }
    const absolute = activeSocketTablePtr(handle) + offset;
    preservedOffsets.add(offset);
    preserved.push({
      offset,
      data: memory.slice(absolute, absolute + socketSlotBytes),
      socketMask: 1 << socketIdx,
    });
  };
  for (const fd of networkAuthoritativeFds(handle)) {
    const closedSlot = handle.networkClosedSocketSlots.get(fd);
    if (closedSlot) {
      if (!preservedOffsets.has(closedSlot.offset)) {
        preservedOffsets.add(closedSlot.offset);
        preserved.push({
          offset: closedSlot.offset,
          data: closedSlot.data,
          socketMask: 1 << Math.floor(closedSlot.offset / socketSlotBytes),
        });
      }
      continue;
    }
    handle.networkSocketSlotWatermarks ??= new Map();
    const watermarkedSlot = handle.networkSocketSlotWatermarks.get(fd);
    if (watermarkedSlot && !preservedOffsets.has(watermarkedSlot.offset)) {
      preservedOffsets.add(watermarkedSlot.offset);
      preserved.push({
        offset: watermarkedSlot.offset,
        data: watermarkedSlot.data,
        socketMask: 1 << Math.floor(watermarkedSlot.offset / socketSlotBytes),
      });
    }
    const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd);
    if (socketIdx === undefined || socketIdx < 0 || socketIdx >= maxSockets) {
      handle.networkClosedFds.delete(fd);
      continue;
    }
    preserveSocketIdx(socketIdx);
    preserveSocketIdx(handle.kernel.thread_socket_peer_idx?.(socketIdx) ?? -1);
  }
  return preserved;
}

function restoreNetworkSocketSlots(
  memory: Uint8Array,
  socketTablePtrs: number[],
  preserved: PreservedNetworkSocketSlot[],
): void {
  for (const slot of preserved) {
    for (const socketTablePtr of socketTablePtrs) {
      memory.set(slot.data, socketTablePtr + slot.offset);
    }
  }
}
