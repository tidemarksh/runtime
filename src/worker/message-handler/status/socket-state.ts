import {
  attachKernelRuntimeState,
  kernelSocketStateBytes,
  kernelSocketStatePtr,
} from "../../../abi.js";
import { FS_RING_TOTAL_BYTES } from "../../../layout/fs-ring.js";
import { kernelNetBufferCount } from "../../../kernel-socket-layout.js";
import {
  forceNetworkClosedFds,
} from "../../network-socket-close.js";
import {
  kernelSocketOutboundAvailable,
  preserveNetworkSocketSlots,
  restoreNetworkSocketSlots,
  runtimeNetworkFds,
  socketTablePtrs,
} from "../../ownership/socket-control.js";
import type { ProcessHandle } from "../../types.js";
import type { ThreadWorkerStatusMessage } from "../types.js";
import {
  appendStatusSocketDebug,
  formatBytesAscii,
  pipeSlotLifecycleDebugEnabled,
  recordStatusSocketMetaTransition,
  STATUS_SOCKET_DEBUG_LOG_ENABLED,
} from "./debug.js";
import { isProcessMemfsDirty } from "../../process-memfs-state.js";
import {
  maskIncompatibleOfdSlotKernelWrite,
  maskRegularFileFdEntryRollbackGuestWrite,
} from "../../ownership/ofd-slot.js";
import {
  formatGuestMemoryWriteWatchOverlap,
} from "../../guest-memory-writes.js";
import { guestWriteWatchRegions } from "../../guest-write-watch-state.js";
import { workerDebugLog } from "../../worker-debug-log.js";
import type { WorkerMessageHandlerDeps } from "../types.js";
import { supervisorProcessStatusIsExecveSpawn } from "../../supervisor-status.js";
import {
  deriveNetBufUsedFromSocketSlots,
  closedSocketRecvBufferIndices,
  netBufUsedMaskFromBufferIndices,
  preserveClosedNetworkRecvBuffers,
  preserveNetworkRecvBuffers,
  preserveNetworkRecvForKernelMemoryWrite,
  preservedSocketRecvBufferIndices,
  preserveUsedNetBufferHeads,
  readNetworkRecvBufferDebug,
  reconcileNetworkRecvBufferConsumers,
  restoreMergedNetBufferHeads,
  restoreNetworkRecvBufferProducers,
} from "./network-recv.js";

interface KernelMemoryWriteOptions {
  skipFsRingWrites?: boolean;
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
    const overlap = formatGuestMemoryWriteWatchOverlap(
      "status-guest-write",
      guestWriteWatchRegions(handle),
      write.guestPtr,
      writeBytes,
    );
    if (overlap !== null) {
      workerDebugLog(handle).push(overlap);
      if (workerDebugLog(handle).length > 4096) {
        workerDebugLog(handle).splice(0, workerDebugLog(handle).length - 4096);
      }
    }
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
    if (isDirtyMemfsKernelMemoryWrite(handle, msg, write.ptr, write.bytes.length)) {
      if (pipeSlotLifecycleDebugEnabled()) {
        appendRuntimeLifecycleEvent?.(
          handle.runtimeId,
          `kernel-write-status-skip-dirty-memfs:${handle.pid}:ptr=0x${write.ptr.toString(16)}:len=${write.bytes.length}`,
        );
      }
      continue;
    }
    const restoreNetworkRecv = preserveNetworkRecvForKernelMemoryWrite(handle, msg, write.ptr, write.bytes.length);
    const writeBytes = maskIncompatibleOfdSlotKernelWrite(handle, write.ptr, write.bytes);
    memory.set(writeBytes, write.ptr);
    restoreNetworkRecv?.();
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

function isDirtyMemfsKernelMemoryWrite(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  ptr: number,
  length: number,
): boolean {
  if (!isProcessMemfsDirty(handle) || length <= 0) {
    return false;
  }
  const faultStatus = handle.kernel.abi_status_fault?.();
  if (
    msg.status !== faultStatus
    && !supervisorProcessStatusIsExecveSpawn(handle.kernel, msg.status)
  ) {
    return false;
  }
  const regions = [
    { start: handle.kernelState.memfsPtr, length: handle.kernelState.memfsHeaderBytes },
    { start: handle.kernel.kernel_memfs_inodes_ptr?.() ?? 0, length: handle.kernelState.memfsInodesBytes },
    { start: handle.kernel.kernel_memfs_first_child_ptr?.() ?? 0, length: handle.kernelState.memfsFirstChildBytes },
    { start: handle.kernel.kernel_memfs_dir_entries_ptr?.() ?? 0, length: handle.kernelState.memfsDirEntriesBytes },
    { start: handle.kernel.kernel_memfs_data_ptr?.() ?? 0, length: handle.kernelState.memfsDataBytes },
  ];
  return regions.some((region) => (
    region.start > 0
    && region.length > 0
    && ptr < region.start + region.length
    && ptr + length > region.start
  ));
}

export function applyThreadSocketState(handle: ProcessHandle, msg: ThreadWorkerStatusMessage): void {
  if (!msg.socketState) {
    return;
  }
  const debugFds = runtimeNetworkFds(handle);
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
      avail: kernelSocketOutboundAvailable(handle, msg.vmPtr, fd),
    };
  };
  const before = STATUS_SOCKET_DEBUG_LOG_ENABLED && debugFds.size > 0
    ? [...debugFds].map((fd) => socketDebugEntry(fd, "status-before"))
    : [];
  const pendingOutboundFds = [...debugFds].filter((fd) =>
    kernelSocketOutboundAvailable(handle, msg.vmPtr, fd) > 0
  );
  if (pendingOutboundFds.length > 0) {
    appendStatusSocketDebug(
      handle,
      `socket-state-skip:${handle.pid}:reason=pending-outbound:fds=${pendingOutboundFds.join(".")}:before=${JSON.stringify(before)}`,
    );
    return;
  }
  const socketTableBytes = handle.kernel.kernel_socket_table_bytes();
  const socketStateBytes = kernelSocketStateBytes(handle.kernel, handle.kernelState);
  const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes();
  const maxNetBuffers = kernelNetBufferCount(handle.kernel);
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
    || (
      msg.socketState.socketState !== undefined
      && msg.socketState.socketState.length !== socketStateBytes
    )
    || !netBufPoolSizeValid
    || !netBufSlotsSizeValid
  ) {
    if (debugFds.size > 0) {
      appendStatusSocketDebug(handle, `socket-state-skip:${handle.pid}:reason=size:socket=${msg.socketState.socketTable.length}/${socketTableBytes}:full=${msg.socketState.socketState?.length ?? -1}/${socketStateBytes}:net=${msg.socketState.netBufPool?.length ?? -1}/${netBufPoolBytes}:slots=${msg.socketState.netBufSlots?.length ?? 0}`);
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
    msg.socketState.netBufUsed | preservedSocketNetBufUsed,
    preservedSocketRecvBuffers,
  ).concat(preserveClosedNetworkRecvBuffers(handle, netBufPoolBytes));
  if (msg.socketState.socketState !== undefined) {
    memory.set(msg.socketState.socketState, kernelSocketStatePtr(handle.kernelState));
  } else {
    memory.set(msg.socketState.socketTable, handle.kernelState.socketTablePtr);
  }
  if (msg.socketState.netBufPool !== undefined) {
    memory.set(msg.socketState.netBufPool, handle.kernelState.netBufBase);
  } else if (netBufSlotBytes > 0) {
    for (const slot of msg.socketState.netBufSlots ?? []) {
      memory.set(slot.bytes, handle.kernelState.netBufBase + slot.bufferIndex * netBufSlotBytes);
    }
  }
  reconcileNetworkRecvBufferConsumers(handle, msg.vmPtr, preservedRecvBuffers);
  restoreNetworkSocketSlots(memory, socketTablePtrs(handle), preservedSocketSlots);
  const mergedSocketUsed = msg.socketState.socketUsed | preservedSocketUsed;
  const derivedNetBufUsed = deriveNetBufUsedFromSocketSlots(handle, mergedSocketUsed);
  restoreNetworkRecvBufferProducers(memory, handle.kernelState.netBufBase, preservedRecvBuffers);
  restoreMergedNetBufferHeads(memory, handle.kernelState.netBufBase, preservedNetBufferHeads);
  forceNetworkClosedFds(handle, msg.vmPtr);
  handle.kernelState = {
    ...handle.kernelState,
    socketUsed: mergedSocketUsed,
    netBufUsed: msg.socketState.netBufUsed | derivedNetBufUsed | preservedSocketNetBufUsed,
  };
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  if (debugFds.size > 0) {
    const after = [...debugFds].map((fd) => socketDebugEntry(fd, "status-after"));
    const incomingFds = (msg.fdEntries ?? [])
      .filter((entry) => debugFds.has(entry.fd))
      .map((entry) => `${entry.fd}/${entry.ofdId}/${entry.generation ?? -1}`)
      .join(".") || "none";
    appendStatusSocketDebug(handle, `socket-state-apply:${handle.pid}:socketUsed=0x${msg.socketState.socketUsed.toString(16)}:netBufUsed=0x${msg.socketState.netBufUsed.toString(16)}:derivedNetBufUsed=0x${derivedNetBufUsed.toString(16)}:incomingFds=${incomingFds}:preservedSlots=${preservedSocketSlots.length}:preservedRecv=${preservedRecvBuffers.length}:preservedHeads=${preservedNetBufferHeads.length}:before=${JSON.stringify(before)}:after=${JSON.stringify(after)}`);
  }
}
