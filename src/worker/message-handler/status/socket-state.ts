import { attachKernelRuntimeState } from "../../../abi.js";
import { FS_RING_TOTAL_BYTES } from "../../../layout/fs-ring.js";
import {
  NET_BUFFER_HEAD_OFFSET,
  NET_BUFFER_HEADER_BYTES,
  NET_BUFFER_TAIL_OFFSET,
} from "../../../layout/net-buffer.js";
import { SOCKET_STATE_SHUTDOWN } from "../../../layout/socket.js";
import {
  forceNetworkClosedFds,
  networkSocketSlotRecvBufferIndex,
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

const SOCK_STATE_SHUTDOWN = SOCKET_STATE_SHUTDOWN;

interface KernelMemoryWriteOptions {
  skipFsRingWrites?: boolean;
}

interface PreservedNetworkRecvBuffer {
  offset: number;
  head: number;
  tail: number;
  data: Uint8Array;
  capacity: number;
  preserveProducer: boolean;
  authoritativeData: boolean;
  forceHead?: boolean;
  forceTail?: boolean;
}

interface PreservedNetBufferHead {
  offset: number;
  head: number;
  tail?: number;
  data?: Uint8Array;
  restoreData?: boolean;
  capacity: number;
  forceHead?: boolean;
  forceTail?: boolean;
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

function readNetworkRecvBufferDebug(
  handle: ProcessHandle,
  socketIdx: number,
  netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes(),
): string {
  if (socketIdx < 0) {
    return "-1/-1";
  }
  const recvBufIdx = handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1;
  if (recvBufIdx < 0) {
    return "-1/-1";
  }
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return "-1/-1";
  }
  const absolute = handle.kernelState.netBufBase + recvBufIdx * netBufferBytes;
  const view = new DataView(handle.memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
  return `${view.getUint32(NET_BUFFER_HEAD_OFFSET, true)}/${view.getUint32(NET_BUFFER_TAIL_OFFSET, true)}`;
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

function preserveNetworkRecvForKernelMemoryWrite(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  ptr: number,
  length: number,
): (() => void) | null {
  if (networkAuthoritativeFds(handle).size === 0) {
    return null;
  }
  const netBufBase = handle.kernelState.netBufBase;
  const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes?.() ?? 0;
  const netBufEnd = netBufBase + netBufPoolBytes;
  if (netBufBase <= 0 || netBufPoolBytes <= 0 || ptr >= netBufEnd || ptr + length <= netBufBase) {
    return null;
  }
  const currentNetBufUsed = handle.kernelState.netBufUsed
    | handle.kernel.kernel_net_buf_used()
    | deriveNetBufUsedFromSocketSlots(handle, handle.kernelState.socketUsed | handle.kernel.kernel_socket_used());
  const preservedRecvBuffers = preserveNetworkRecvBuffers(handle, msg.vmPtr, netBufPoolBytes);
  const preservedNetBufferHeads = preserveUsedNetBufferHeads(
    handle,
    netBufPoolBytes,
    currentNetBufUsed,
  ).concat(preserveClosedNetworkRecvBuffers(handle, netBufPoolBytes));
  if (preservedRecvBuffers.length === 0 && preservedNetBufferHeads.length === 0) {
    return null;
  }
  return () => {
    const memory = new Uint8Array(handle.memory.buffer);
    restoreNetworkRecvBufferProducers(memory, handle.kernelState.netBufBase, preservedRecvBuffers);
    restoreMergedNetBufferHeads(memory, handle.kernelState.netBufBase, preservedNetBufferHeads);
  };
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

export function deriveNetBufUsedFromSocketSlots(handle: ProcessHandle, socketUsed: number): bigint {
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  let netBufUsed = 0n;
  for (let socketIdx = 0; socketIdx < maxSockets; socketIdx += 1) {
    if ((socketUsed & (1 << socketIdx)) === 0) {
      continue;
    }
    const recvBufIdx = handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1;
    if (recvBufIdx >= 0 && recvBufIdx < netBufferCount) {
      netBufUsed |= 1n << BigInt(recvBufIdx);
    }
  }
  return netBufUsed;
}

export function networkAuthoritativeFds(handle: ProcessHandle): Set<number> {
  return new Set([
    ...(handle.networkConnections?.keys() ?? []),
    ...(handle.networkPendingConnectFds ?? []),
    ...(handle.networkClosedFds ?? []),
    ...(handle.networkClosedSocketSlots?.keys() ?? []),
    ...(handle.networkSocketSlotWatermarks?.keys() ?? []),
    ...(handle.networkRecvPayloadWatermarks?.keys() ?? []),
  ]);
}

function closeNetworkClosedFdPeers(handle: ProcessHandle, vmPtr: number): void {
  forceNetworkClosedFds(handle, vmPtr);
}

function counterAhead(sourceValue: number, targetValue: number, capacity: number): boolean {
  if (sourceValue === targetValue) {
    return false;
  }
  if (!Number.isFinite(capacity) || capacity <= 0) {
    return sourceValue > targetValue;
  }
  return sourceValue > targetValue
    || (
      sourceValue < targetValue
      && targetValue > capacity / 2
      && sourceValue < capacity / 2
    );
}

function rememberNetworkRecvTail(handle: ProcessHandle, fd: number, tail: number, capacity: number): number {
  const previous = handle.networkRecvTailWatermarks.get(fd) ?? 0;
  const next = counterAhead(tail, previous, capacity) ? tail : previous;
  handle.networkRecvTailWatermarks.set(fd, next);
  return next;
}

function socketReadHeadFloor(
  handle: ProcessHandle,
  socketIdx: number,
  tail: number,
  capacity?: number,
): number {
  const readBytes = handle.kernel.thread_socket_read_bytes?.(socketIdx);
  if (readBytes === undefined) {
    return 0;
  }
  const numeric = typeof readBytes === "bigint" ? Number(readBytes) : readBytes;
  if (!Number.isSafeInteger(numeric) || numeric <= 0) {
    return 0;
  }
  if (capacity !== undefined && Number.isFinite(capacity) && capacity > 0) {
    return numeric % capacity;
  }
  return Math.min(numeric, tail);
}

function networkRecvPayloadWatermark(
  handle: ProcessHandle,
  fd: number,
  socketIdx: number,
  currentTail: number,
  capacity: number,
): { head: number; tail: number; data: Uint8Array } | null {
  handle.networkRecvPayloadWatermarks ??= new Map();
  const payload = handle.networkRecvPayloadWatermarks.get(fd);
  if (payload === undefined || payload.capacity !== capacity) {
    return null;
  }
  if (payload.readBytes !== undefined && payload.availableBytes !== undefined && socketIdx >= 0) {
    const readBytesRaw = handle.kernel.thread_socket_read_bytes?.(socketIdx);
    const readBytes = typeof readBytesRaw === "bigint" ? Number(readBytesRaw) : readBytesRaw;
    const availableBytes = handle.kernel.thread_socket_recv_available?.(socketIdx);
    if (
      Number.isSafeInteger(readBytes)
      && Number.isSafeInteger(availableBytes)
      && availableBytes !== undefined
      && availableBytes >= 0
    ) {
      const payloadProgress = payload.deliveredBytes ?? (payload.readBytes + payload.availableBytes);
      const currentProgress = (readBytes as number) + availableBytes;
      const readProgress = (readBytes as number) - payload.readBytes;
      const deliveredRemaining = payload.deliveredBytes === undefined
        ? undefined
        : payload.deliveredBytes - (readBytes as number);
      const payloadCoversReadProgress = deliveredRemaining === undefined
        ? readProgress >= 0 && readProgress < payload.availableBytes
        : readProgress >= 0
          && readProgress <= payload.availableBytes
          && deliveredRemaining >= 0
          && deliveredRemaining <= payload.availableBytes - readProgress;
      if (payloadCoversReadProgress) {
        const restoredHead = readProgress > 0
          ? (payload.head + readProgress) % payload.capacity
          : payload.head;
        const restoredTail = deliveredRemaining === undefined
          ? payload.tail
          : (restoredHead + deliveredRemaining) % payload.capacity;
        appendStatusSocketDebug(
          handle,
          `net-payload-watermark-use-progress:${handle.pid}:fd=${fd}:source=status:payload=${payloadProgress}:current=${currentProgress}:phead=${payload.head}:rhead=${restoredHead}:ptail=${payload.tail}:rtail=${restoredTail}:ctail=${currentTail}`,
        );
        return {
          head: restoredHead,
          tail: restoredTail,
          data: payload.data,
        };
      }
      if (currentProgress > payloadProgress) {
        appendStatusSocketDebug(
          handle,
          `net-payload-watermark-skip-progress:${handle.pid}:fd=${fd}:source=status:payload=${payloadProgress}:current=${currentProgress}:ptail=${payload.tail}:ctail=${currentTail}`,
        );
        return null;
      }
      return null;
    }
  }
  const payloadTailAhead = (() => {
    if (payload.tail === currentTail) {
      return true;
    }
    if (!Number.isFinite(capacity) || capacity <= 0) {
      return payload.tail > currentTail;
    }
    const distance = (payload.tail - currentTail + capacity) % capacity;
    return distance > 0 && distance < capacity / 2;
  })();
  if (payloadTailAhead) {
    appendStatusSocketDebug(
      handle,
      `net-payload-watermark-use:${handle.pid}:fd=${fd}:source=status:ptail=${payload.tail}:ctail=${currentTail}:cap=${capacity}`,
    );
    return {
      head: payload.head,
      tail: payload.tail,
      data: payload.data,
    };
  }
  return null;
}

function mergeSocketReadHeadFloor(
  handle: ProcessHandle,
  socketIdx: number,
  head: number,
  tail: number,
  capacity: number,
): number {
  const available = handle.kernel.thread_socket_recv_available?.(socketIdx);
  if (available === 0 && head === tail) {
    return head;
  }
  const floor = socketReadHeadFloor(handle, socketIdx, tail, capacity);
  return counterAhead(floor, head, capacity) ? floor : head;
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

export function preserveNetworkRecvBuffers(
  handle: ProcessHandle,
  vmPtr: number,
  netBufPoolBytes: number,
): PreservedNetworkRecvBuffer[] {
  const authoritativeFds = networkAuthoritativeFds(handle);
  if (authoritativeFds.size === 0) {
    return [];
  }
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return [];
  }
  const preserved: PreservedNetworkRecvBuffer[] = [];
  const preservedOffsets = new Set<number>();
  const memory = new Uint8Array(handle.memory.buffer);
  const preserveWatermarkedRecvBuffer = (fd: number): boolean => {
    handle.networkRecvPayloadWatermarks ??= new Map();
    const payload = handle.networkRecvPayloadWatermarks.get(fd);
    if (payload === undefined || payload.recvBufIdx < 0 || payload.recvBufIdx >= netBufferCount) {
      return false;
    }
    const offset = payload.recvBufIdx * netBufferBytes;
    if (preservedOffsets.has(offset)) {
      return true;
    }
    handle.networkRecvHeadWatermarks.set(fd, payload.head);
    const watermarkHead = payload.head;
    handle.networkRecvTailWatermarks.set(fd, payload.tail);
    const watermarkTail = payload.tail;
    preservedOffsets.add(offset);
    preserved.push({
      offset,
      head: watermarkHead,
      tail: watermarkTail,
      data: payload.data,
      capacity: payload.capacity,
      preserveProducer: true,
      authoritativeData: true,
      forceHead: true,
      forceTail: true,
    });
    return true;
  };
  const preserveSocketRecvBuffer = (socketIdx: number, fd: number | null): void => {
    const recvBufIdx = handle.kernel.thread_socket_recv_buf_idx?.(socketIdx);
    if (recvBufIdx === undefined || recvBufIdx < 0 || recvBufIdx >= netBufferCount) {
      if (fd !== null) {
        preserveWatermarkedRecvBuffer(fd);
      }
      return;
    }
    const offset = recvBufIdx * netBufferBytes;
    if (preservedOffsets.has(offset)) {
      return;
    }
    const absolute = handle.kernelState.netBufBase + offset;
    const bytes = memory.slice(absolute, absolute + netBufferBytes);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const currentHead = view.getUint32(NET_BUFFER_HEAD_OFFSET, true);
    const currentTail = view.getUint32(NET_BUFFER_TAIL_OFFSET, true);
    const capacity = netBufferBytes - NET_BUFFER_HEADER_BYTES;
    const payload = fd === null ? null : networkRecvPayloadWatermark(handle, fd, socketIdx, currentTail, capacity);
    const terminalRecv = fd !== null
      && handle.kernel.thread_socket_state?.(socketIdx) === SOCK_STATE_SHUTDOWN
      && (handle.kernel.thread_socket_external_peer?.(socketIdx) ?? 0) === 0;
    const preservedTail = payload?.tail ?? currentTail;
    const mergedHead = terminalRecv
      ? currentHead
      : payload?.head ?? mergeSocketReadHeadFloor(
        handle,
        socketIdx,
        currentHead,
        currentTail,
        capacity,
      );
    const watermarkHead = fd === null ? mergedHead : mergedHead;
    if (fd !== null) {
      handle.networkRecvHeadWatermarks.set(fd, watermarkHead);
    }
    const watermarkTail = (() => {
      if (fd === null) {
        return currentTail;
      }
      if (payload !== null) {
        handle.networkRecvTailWatermarks.set(fd, preservedTail);
        return preservedTail;
      }
      if (terminalRecv) {
        handle.networkRecvTailWatermarks.set(fd, watermarkHead);
        return watermarkHead;
      }
      return rememberNetworkRecvTail(handle, fd, preservedTail, capacity);
    })();
    preservedOffsets.add(offset);
    preserved.push({
      offset,
      head: watermarkHead,
      tail: watermarkTail,
      data: payload?.data ?? bytes.slice(NET_BUFFER_HEADER_BYTES),
      capacity,
      preserveProducer: fd !== null,
      authoritativeData: payload !== null,
      forceHead: fd !== null,
      forceTail: payload !== null || terminalRecv,
    });
  };
  for (const fd of authoritativeFds) {
    const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd);
    if (socketIdx === undefined || socketIdx < 0) {
      if (!preserveWatermarkedRecvBuffer(fd)) {
        handle.networkClosedFds.delete(fd);
      }
      continue;
    }
    preserveSocketRecvBuffer(socketIdx, fd);
    const peerIdx = handle.kernel.thread_socket_peer_idx?.(socketIdx) ?? -1;
    if (peerIdx >= 0) {
      preserveSocketRecvBuffer(peerIdx, null);
    }
  }
  return preserved;
}

export function preserveUsedNetBufferHeads(
  handle: ProcessHandle,
  netBufPoolBytes: number,
  netBufUsed: bigint,
  extraBufferIndices: Iterable<number> = [],
): PreservedNetBufferHead[] {
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return [];
  }
  const preserved: PreservedNetBufferHead[] = [];
  const memory = new Uint8Array(handle.memory.buffer);
  const bufferIndices = new Set<number>();
  const socketReadFloors = socketReadHeadFloorsByBuffer(handle, netBufPoolBytes);
  for (let bufferIndex = 0; bufferIndex < netBufferCount; bufferIndex += 1) {
    if ((netBufUsed & (1n << BigInt(bufferIndex))) === 0n) {
      continue;
    }
    bufferIndices.add(bufferIndex);
  }
  for (const bufferIndex of extraBufferIndices) {
    if (bufferIndex >= 0 && bufferIndex < netBufferCount) {
      bufferIndices.add(bufferIndex);
    }
  }
  for (const bufferIndex of bufferIndices) {
    const offset = bufferIndex * netBufferBytes;
    const absolute = handle.kernelState.netBufBase + offset;
    const view = new DataView(memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
    const currentHead = view.getUint32(NET_BUFFER_HEAD_OFFSET, true);
    const currentTail = view.getUint32(NET_BUFFER_TAIL_OFFSET, true);
    const socketReadFloor = socketReadFloors.get(bufferIndex) ?? 0;
    preserved.push({
      offset,
      head: counterAhead(socketReadFloor, currentHead, netBufferBytes - NET_BUFFER_HEADER_BYTES)
        ? socketReadFloor
        : currentHead,
      tail: currentTail,
      data: memory.slice(absolute + NET_BUFFER_HEADER_BYTES, absolute + netBufferBytes),
      capacity: netBufferBytes - NET_BUFFER_HEADER_BYTES,
    });
  }
  return preserved;
}

function socketReadHeadFloorsByBuffer(
  handle: ProcessHandle,
  netBufPoolBytes: number,
): Map<number, number> {
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  const floors = new Map<number, number>();
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return floors;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  for (let socketIdx = 0; socketIdx < maxSockets; socketIdx += 1) {
    const recvBufIdx = handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1;
    if (recvBufIdx < 0 || recvBufIdx >= netBufferCount) {
      continue;
    }
    const absolute = handle.kernelState.netBufBase + recvBufIdx * netBufferBytes;
    const view = new DataView(memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
    const tail = view.getUint32(NET_BUFFER_TAIL_OFFSET, true);
    const floor = socketReadHeadFloor(handle, socketIdx, tail, netBufferBytes - NET_BUFFER_HEADER_BYTES);
    const previous = floors.get(recvBufIdx) ?? 0;
    if (counterAhead(floor, previous, netBufferBytes - NET_BUFFER_HEADER_BYTES)) {
      floors.set(recvBufIdx, floor);
    }
  }
  return floors;
}

function preservedSocketRecvBufferIndices(preserved: PreservedNetworkSocketSlot[]): number[] {
  const indices: number[] = [];
  for (const slot of preserved) {
    const recvBufIdx = networkSocketSlotRecvBufferIndex(slot);
    if (recvBufIdx >= 0) {
      indices.push(recvBufIdx);
    }
  }
  return indices;
}

function closedSocketRecvBufferIndices(handle: ProcessHandle): number[] {
  const indices: number[] = [];
  for (const slot of handle.networkClosedSocketSlots.values()) {
    const recvBufIdx = networkSocketSlotRecvBufferIndex(slot);
    if (recvBufIdx >= 0) {
      indices.push(recvBufIdx);
    }
  }
  return indices;
}

export function preserveClosedNetworkRecvBuffers(
  handle: ProcessHandle,
  netBufPoolBytes: number,
): PreservedNetBufferHead[] {
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return [];
  }
  const preserved: PreservedNetBufferHead[] = [];
  const memory = new Uint8Array(handle.memory.buffer);
  for (const recvBufIdx of closedSocketRecvBufferIndices(handle)) {
    if (recvBufIdx < 0 || recvBufIdx >= netBufferCount) {
      continue;
    }
    const offset = recvBufIdx * netBufferBytes;
    const absolute = handle.kernelState.netBufBase + offset;
    const view = new DataView(memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
    preserved.push({
      offset,
      head: view.getUint32(NET_BUFFER_HEAD_OFFSET, true),
      tail: view.getUint32(NET_BUFFER_TAIL_OFFSET, true),
      data: memory.slice(absolute + NET_BUFFER_HEADER_BYTES, absolute + netBufferBytes),
      capacity: netBufferBytes - NET_BUFFER_HEADER_BYTES,
      forceHead: true,
      forceTail: true,
    });
  }
  return preserved;
}

function netBufUsedMaskFromBufferIndices(indices: Iterable<number>): bigint {
  let mask = 0n;
  for (const index of indices) {
    if (index >= 0) {
      mask |= 1n << BigInt(index);
    }
  }
  return mask;
}

export function restoreNetworkRecvBufferProducers(
  memory: Uint8Array,
  netBufBase: number,
  preserved: PreservedNetworkRecvBuffer[],
): void {
  for (const buffer of preserved) {
    const absolute = netBufBase + buffer.offset;
    const view = new DataView(memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
    const incomingHead = view.getUint32(NET_BUFFER_HEAD_OFFSET, true);
    const incomingTail = view.getUint32(NET_BUFFER_TAIL_OFFSET, true);
    const mergedHead = buffer.forceHead === true
      ? buffer.head
      : counterAhead(incomingHead, buffer.head, buffer.capacity)
      ? incomingHead
      : buffer.head;
    const tailDistanceFromHead = (tail: number): number => (
      tail - mergedHead + buffer.capacity
    ) % buffer.capacity;
    const preservedTailAhead = buffer.forceTail === true
      ? true
      : buffer.authoritativeData
      ? tailDistanceFromHead(buffer.tail) > tailDistanceFromHead(incomingTail)
      : counterAhead(buffer.tail, incomingTail, buffer.capacity);
    if (
      preservedTailAhead
      || (buffer.authoritativeData && buffer.tail === incomingTail)
      || (buffer.forceHead === true && incomingHead !== buffer.head)
    ) {
      memory.set(buffer.data, absolute + NET_BUFFER_HEADER_BYTES);
    }
    view.setUint32(NET_BUFFER_HEAD_OFFSET, mergedHead, true);
    view.setUint32(
      NET_BUFFER_TAIL_OFFSET,
      buffer.forceTail === true || preservedTailAhead ? buffer.tail : incomingTail,
      true,
    );
  }
}

export function restoreMergedNetBufferHeads(
  memory: Uint8Array,
  netBufBase: number,
  preserved: PreservedNetBufferHead[],
): void {
  for (const buffer of preserved) {
    const absolute = netBufBase + buffer.offset;
    const view = new DataView(memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
    const incomingHead = view.getUint32(NET_BUFFER_HEAD_OFFSET, true);
    if (buffer.forceHead === true) {
      if (incomingHead !== buffer.head && buffer.data !== undefined) {
        memory.set(buffer.data, absolute + NET_BUFFER_HEADER_BYTES);
      }
      view.setUint32(NET_BUFFER_HEAD_OFFSET, buffer.head, true);
    } else if (counterAhead(buffer.head, incomingHead, buffer.capacity)) {
      view.setUint32(NET_BUFFER_HEAD_OFFSET, buffer.head, true);
    }
    if (buffer.tail !== undefined) {
      const incomingTail = view.getUint32(NET_BUFFER_TAIL_OFFSET, true);
      const incomingTailPlausible = incomingTail < buffer.capacity
        && buffer.head < buffer.capacity;
      if (!incomingTailPlausible) {
        if (buffer.data !== undefined) {
          memory.set(buffer.data, absolute + NET_BUFFER_HEADER_BYTES);
        }
        view.setUint32(NET_BUFFER_HEAD_OFFSET, buffer.head, true);
        view.setUint32(NET_BUFFER_TAIL_OFFSET, buffer.tail, true);
        continue;
      }
      const tailDistanceFromHead = (tail: number): number => (
        tail - buffer.head + buffer.capacity
      ) % buffer.capacity;
      const preservedTailAhead = buffer.forceTail === true
        ? true
        : buffer.restoreData
        ? tailDistanceFromHead(buffer.tail) > tailDistanceFromHead(incomingTail)
        : counterAhead(buffer.tail, incomingTail, buffer.capacity);
      if (buffer.forceTail === true || preservedTailAhead) {
        if (buffer.restoreData && buffer.data !== undefined) {
          memory.set(buffer.data, absolute + NET_BUFFER_HEADER_BYTES);
        }
        view.setUint32(NET_BUFFER_TAIL_OFFSET, buffer.tail, true);
      }
    }
  }
}
