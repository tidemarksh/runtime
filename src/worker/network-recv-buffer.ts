import { kernelSocketCount, requireKernelSocketCount } from "../kernel-socket-layout.js";
import {
  NET_BUFFER_HEAD_OFFSET,
  NET_BUFFER_HEADER_BYTES,
  NET_BUFFER_TAIL_OFFSET,
} from "../layout/net-buffer.js";
import { SOCKET_STATE_SHUTDOWN } from "../layout/socket.js";
import { networkSocketSlotRecvBufferIndex } from "./network-socket-close.js";
import {
  kernelSocketExternalPeer,
  kernelSocketIndexForFd,
  kernelSocketPeerIndex,
  kernelSocketReadBytes,
  kernelSocketRecvAvailable,
  kernelSocketRecvBufferIndex,
  kernelSocketState,
  runtimeNetworkFds,
} from "./ownership/socket-control.js";
import {
  networkClosedFds,
  networkClosedSocketSlots,
  networkRecvHeadWatermarks,
  networkRecvPayloadWatermarks,
  networkRecvTailWatermarks,
} from "./network-socket-state.js";
import type { ProcessHandle } from "./types.js";

const SOCK_STATE_SHUTDOWN = SOCKET_STATE_SHUTDOWN;

export interface NetworkRecvBufferDebugOptions {
  source: string;
  append(handle: ProcessHandle, event: string): void;
}

export interface PreservedNetworkRecvBuffer {
  offset: number;
  head: number;
  tail: number;
  data: Uint8Array;
  capacity: number;
  preserveProducer: boolean;
  payloadData: boolean;
  forceHead?: boolean;
  forceTail?: boolean;
}

export interface PreservedNetBufferHead {
  offset: number;
  head: number;
  tail?: number;
  data?: Uint8Array;
  restoreData?: boolean;
  capacity: number;
  forceHead?: boolean;
  forceTail?: boolean;
}

export interface RestoreMergedNetBufferHeadsOptions {
  restoreDataOnEqualTail?: boolean;
  restorePlainDataOnTail?: boolean;
}

export function readNetworkRecvBufferDebug(
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
  const maxSockets = kernelSocketCount(handle.kernel);
  if (maxSockets === undefined) {
    return "-1/-1";
  }
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return "-1/-1";
  }
  const absolute = handle.kernelState.netBufBase + recvBufIdx * netBufferBytes;
  const view = new DataView(handle.memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
  return `${view.getUint32(NET_BUFFER_HEAD_OFFSET, true)}/${view.getUint32(NET_BUFFER_TAIL_OFFSET, true)}`;
}

export function deriveNetBufUsedFromSocketSlots(handle: ProcessHandle, socketUsed: number): bigint {
  const maxSockets = requireKernelSocketCount(handle.kernel);
  const netBufferCount = maxSockets * 2;
  let netBufUsed = 0n;
  for (let socketIdx = 0; socketIdx < maxSockets; socketIdx += 1) {
    if ((socketUsed & (1 << socketIdx)) === 0) {
      continue;
    }
    const recvBufIdx = kernelSocketRecvBufferIndex(handle, socketIdx);
    if (recvBufIdx >= 0 && recvBufIdx < netBufferCount) {
      netBufUsed |= 1n << BigInt(recvBufIdx);
    }
  }
  return netBufUsed;
}

export function counterAhead(sourceValue: number, targetValue: number, capacity: number): boolean {
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

export function rememberNetworkRecvTail(handle: ProcessHandle, fd: number, tail: number, capacity: number): number {
  const previous = networkRecvTailWatermarks(handle).get(fd) ?? 0;
  const next = counterAhead(tail, previous, capacity) ? tail : previous;
  networkRecvTailWatermarks(handle).set(fd, next);
  return next;
}

export function socketReadHeadFloor(
  handle: ProcessHandle,
  socketIdx: number,
  tail: number,
  capacity?: number,
): number {
  const readBytes = kernelSocketReadBytes(handle, socketIdx);
  const numeric = typeof readBytes === "bigint" ? Number(readBytes) : readBytes;
  if (!Number.isSafeInteger(numeric) || numeric <= 0) {
    return 0;
  }
  if (capacity !== undefined && Number.isFinite(capacity) && capacity > 0) {
    return numeric % capacity;
  }
  return Math.min(numeric, tail);
}

export function networkRecvPayloadWatermark(
  handle: ProcessHandle,
  fd: number,
  socketIdx: number,
  currentTail: number,
  capacity: number,
  debug?: NetworkRecvBufferDebugOptions,
): { head: number; tail: number; data: Uint8Array } | null {
  const payload = networkRecvPayloadWatermarks(handle).get(fd);
  if (payload === undefined || payload.capacity !== capacity) {
    return null;
  }
  if (payload.readBytes !== undefined && payload.availableBytes !== undefined && socketIdx >= 0) {
    const readBytesRaw = kernelSocketReadBytes(handle, socketIdx);
    const readBytes = typeof readBytesRaw === "bigint" ? Number(readBytesRaw) : readBytesRaw;
    const availableBytes = kernelSocketRecvAvailable(handle, socketIdx);
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
        debug?.append(
          handle,
          `net-payload-watermark-use-progress:${handle.pid}:fd=${fd}:source=${debug.source}:payload=${payloadProgress}:current=${currentProgress}:phead=${payload.head}:rhead=${restoredHead}:ptail=${payload.tail}:rtail=${restoredTail}:ctail=${currentTail}`,
        );
        return {
          head: restoredHead,
          tail: restoredTail,
          data: payload.data,
        };
      }
      if (currentProgress > payloadProgress) {
        debug?.append(
          handle,
          `net-payload-watermark-skip-progress:${handle.pid}:fd=${fd}:source=${debug.source}:payload=${payloadProgress}:current=${currentProgress}:ptail=${payload.tail}:ctail=${currentTail}`,
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
    debug?.append(
      handle,
      `net-payload-watermark-use:${handle.pid}:fd=${fd}:source=${debug.source}:ptail=${payload.tail}:ctail=${currentTail}:cap=${capacity}`,
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
  const available = kernelSocketRecvAvailable(handle, socketIdx);
  if (available === 0 && head === tail) {
    return head;
  }
  const floor = socketReadHeadFloor(handle, socketIdx, tail, capacity);
  return counterAhead(floor, head, capacity) ? floor : head;
}

export function preserveNetworkRecvBuffers(
  handle: ProcessHandle,
  vmPtr: number,
  netBufPoolBytes: number,
  debug?: NetworkRecvBufferDebugOptions,
  excludedFds: ReadonlySet<number> = new Set(),
): PreservedNetworkRecvBuffer[] {
  const runtimeFds = runtimeNetworkFds(handle);
  if (runtimeFds.size === 0) {
    return [];
  }
  const maxSockets = requireKernelSocketCount(handle.kernel);
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return [];
  }
  const preserved: PreservedNetworkRecvBuffer[] = [];
  const preservedOffsets = new Set<number>();
  const memory = new Uint8Array(handle.memory.buffer);
  const preserveWatermarkedRecvBuffer = (fd: number): boolean => {
    const payload = networkRecvPayloadWatermarks(handle).get(fd);
    if (payload === undefined || payload.recvBufIdx < 0 || payload.recvBufIdx >= netBufferCount) {
      return false;
    }
    const offset = payload.recvBufIdx * netBufferBytes;
    if (preservedOffsets.has(offset)) {
      return true;
    }
    networkRecvHeadWatermarks(handle).set(fd, payload.head);
    const watermarkHead = payload.head;
    networkRecvTailWatermarks(handle).set(fd, payload.tail);
    const watermarkTail = payload.tail;
    preservedOffsets.add(offset);
    preserved.push({
      offset,
      head: watermarkHead,
      tail: watermarkTail,
      data: payload.data,
      capacity: payload.capacity,
      preserveProducer: true,
      payloadData: true,
      forceHead: true,
      forceTail: true,
    });
    return true;
  };
  const preserveSocketRecvBuffer = (socketIdx: number, fd: number | null): void => {
    const recvBufIdx = kernelSocketRecvBufferIndex(handle, socketIdx);
    if (recvBufIdx < 0 || recvBufIdx >= netBufferCount) {
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
    const payload = fd === null ? null : networkRecvPayloadWatermark(handle, fd, socketIdx, currentTail, capacity, debug);
    const terminalRecv = fd !== null
      && kernelSocketState(handle, socketIdx) === SOCK_STATE_SHUTDOWN
      && kernelSocketExternalPeer(handle, socketIdx) === 0;
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
      networkRecvHeadWatermarks(handle).set(fd, watermarkHead);
    }
    const watermarkTail = (() => {
      if (fd === null) {
        return currentTail;
      }
      if (payload !== null) {
        networkRecvTailWatermarks(handle).set(fd, preservedTail);
        return preservedTail;
      }
      if (terminalRecv) {
        networkRecvTailWatermarks(handle).set(fd, watermarkHead);
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
      payloadData: payload !== null,
      forceHead: fd !== null,
      forceTail: payload !== null || terminalRecv,
    });
  };
  for (const fd of runtimeFds) {
    if (excludedFds.has(fd)) {
      continue;
    }
    const socketIdx = kernelSocketIndexForFd(handle, vmPtr, fd);
    if (socketIdx < 0) {
      if (!preserveWatermarkedRecvBuffer(fd)) {
        networkClosedFds(handle).delete(fd);
      }
      continue;
    }
    preserveSocketRecvBuffer(socketIdx, fd);
    const peerIdx = kernelSocketPeerIndex(handle, socketIdx);
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
  const maxSockets = requireKernelSocketCount(handle.kernel);
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

export function socketReadHeadFloorsByBuffer(
  handle: ProcessHandle,
  netBufPoolBytes: number,
): Map<number, number> {
  const floors = new Map<number, number>();
  const maxSockets = requireKernelSocketCount(handle.kernel);
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return floors;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  for (let socketIdx = 0; socketIdx < maxSockets; socketIdx += 1) {
    const recvBufIdx = kernelSocketRecvBufferIndex(handle, socketIdx);
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

export function preservedSocketRecvBufferIndices(preserved: Iterable<{ data: Uint8Array }>): number[] {
  const indices: number[] = [];
  for (const slot of preserved) {
    const recvBufIdx = networkSocketSlotRecvBufferIndex(slot);
    if (recvBufIdx >= 0) {
      indices.push(recvBufIdx);
    }
  }
  return indices;
}

export function closedSocketRecvBufferIndices(handle: ProcessHandle): number[] {
  const indices: number[] = [];
  for (const slot of networkClosedSocketSlots(handle).values()) {
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
  excludedFds: ReadonlySet<number> = new Set(),
): PreservedNetBufferHead[] {
  const maxSockets = requireKernelSocketCount(handle.kernel);
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return [];
  }
  const preserved: PreservedNetBufferHead[] = [];
  const memory = new Uint8Array(handle.memory.buffer);
  for (const [fd, slot] of networkClosedSocketSlots(handle)) {
    if (excludedFds.has(fd)) {
      continue;
    }
    const recvBufIdx = networkSocketSlotRecvBufferIndex(slot);
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

export function netBufUsedMaskFromBufferIndices(indices: Iterable<number>): bigint {
  let mask = 0n;
  for (const index of indices) {
    if (index >= 0) {
      mask |= 1n << BigInt(index);
    }
  }
  return mask;
}

export function preserveNetworkRecvHeadWatermarks(
  handle: ProcessHandle,
  netBufPoolBytes: number,
  debug?: NetworkRecvBufferDebugOptions,
): PreservedNetBufferHead[] {
  const fds = [...runtimeNetworkFds(handle)];
  if (fds.length === 0) {
    return [];
  }
  const maxSockets = requireKernelSocketCount(handle.kernel);
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return [];
  }
  const preserved: PreservedNetBufferHead[] = [];
  const memory = new Uint8Array(handle.memory.buffer);
  for (const fd of fds) {
    const socketIdx = kernelSocketIndexForFd(handle, handle.vmPtr, fd);
    if (socketIdx < 0) {
      networkClosedFds(handle)?.delete(fd);
      continue;
    }
    const recvBufIdx = kernelSocketRecvBufferIndex(handle, socketIdx);
    if (recvBufIdx < 0 || recvBufIdx >= netBufferCount) {
      continue;
    }
    const offset = recvBufIdx * netBufferBytes;
    const absolute = handle.kernelState.netBufBase + offset;
    const view = new DataView(memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
    const currentHead = view.getUint32(NET_BUFFER_HEAD_OFFSET, true);
    const currentTail = view.getUint32(NET_BUFFER_TAIL_OFFSET, true);
    const capacity = netBufferBytes - NET_BUFFER_HEADER_BYTES;
    const payload = networkRecvPayloadWatermark(handle, fd, socketIdx, currentTail, capacity, debug);
    const terminalRecv = kernelSocketState(handle, socketIdx) === SOCK_STATE_SHUTDOWN
      && kernelSocketExternalPeer(handle, socketIdx) === 0;
    const preservedTail = payload?.tail ?? currentTail;
    const mergedHead = (() => {
      if (terminalRecv) {
        return currentHead;
      }
      if (payload !== null) {
        return payload.head;
      }
      return mergeSocketReadHeadFloor(handle, socketIdx, currentHead, currentTail, capacity);
    })();
    preserved.push({
      offset,
      head: mergedHead,
      tail: (() => {
        if (payload !== null) {
          networkRecvTailWatermarks(handle).set(fd, preservedTail);
          return preservedTail;
        }
        if (terminalRecv) {
          networkRecvTailWatermarks(handle).set(fd, mergedHead);
          return mergedHead;
        }
        return rememberNetworkRecvTail(handle, fd, preservedTail, capacity);
      })(),
      data: payload?.data ?? memory.slice(absolute + NET_BUFFER_HEADER_BYTES, absolute + netBufferBytes),
      restoreData: payload !== null,
      capacity,
      forceHead: true,
      forceTail: payload !== null || terminalRecv,
    });
    networkRecvHeadWatermarks(handle).set(fd, mergedHead);
  }
  return preserved;
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
      : buffer.payloadData
      ? tailDistanceFromHead(buffer.tail) > tailDistanceFromHead(incomingTail)
      : counterAhead(buffer.tail, incomingTail, buffer.capacity);
    if (
      preservedTailAhead
      || (buffer.payloadData && buffer.tail === incomingTail)
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
  options: RestoreMergedNetBufferHeadsOptions = {},
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
      const restoreDataOnEqualTail = options.restoreDataOnEqualTail === true;
      const preservedTailCoversIncoming = buffer.tail === incomingTail || preservedTailAhead;
      if (
        restoreDataOnEqualTail
        && buffer.restoreData
        && buffer.data !== undefined
        && preservedTailCoversIncoming
      ) {
        memory.set(buffer.data, absolute + NET_BUFFER_HEADER_BYTES);
      }
      if (buffer.forceTail === true || preservedTailAhead) {
        if (
          buffer.data !== undefined
          && (
            (buffer.restoreData && !restoreDataOnEqualTail)
            || (!buffer.restoreData && options.restorePlainDataOnTail === true)
          )
        ) {
          memory.set(buffer.data, absolute + NET_BUFFER_HEADER_BYTES);
        }
        view.setUint32(NET_BUFFER_TAIL_OFFSET, buffer.tail, true);
      }
    }
  }
}
