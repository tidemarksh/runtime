import { readKernelRuntimeState } from "../abi.js";
import { publishKernelRuntimeState } from "../bridge/runtime-state.js";
import { SOCKET_STATE_CONNECTED, SOCKET_STATE_SHUTDOWN } from "../layout/socket.js";
import type { RuntimeNetworkConnection, RuntimeNetworkEndpoint } from "../bridge/network.js";
import { ensureMemoryCapacity, ensureKernelMemoryCapacity, kernelScratchBase, writeKernelScratch } from "./kernel-scratch.js";
import { kernelWorkerOwnsPipeState } from "./ownership/pipe-control.js";
import { forceNetworkClosedFd, restoreNetworkSocketSlotSnapshot, snapshotNetworkSocketSlot } from "./network-socket-close.js";
import { createTransferDebugStats, type ProcessHandle } from "./types.js";

interface WorkerProcessIoDeps {
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  drainProcessOutput(handle: ProcessHandle): boolean;
  failProcess(handle: ProcessHandle, error: Error): void;
  flushStdin(handle: ProcessHandle): boolean;
  getActiveHandle?(handle: ProcessHandle): ProcessHandle | undefined;
  isActiveHandle(handle: ProcessHandle): boolean;
  notifyHostIoProgress(handle: ProcessHandle): void;
  pumpProcess(handle: ProcessHandle): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  requestSocketStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const NETWORK_BRIDGE_CHUNK_SIZE = 240 * 1024;
const NETWORK_FD_TRACE_LOG_ENABLED = ((globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.TIDEMARK_WORKER_DEBUG_FD_TRACE ?? "") === "1";
const NETWORK_LIFECYCLE_LOG_ENABLED = ((globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.TIDEMARK_WORKER_DEBUG_NETWORK ?? "") === "1";

function logNetworkDebug(event: string): void {
  if (!NETWORK_LIFECYCLE_LOG_ENABLED) {
    return;
  }
  console.error(`[tidemark:network] ${event}`);
}

function appendNetworkLifecycle(handle: ProcessHandle, event: string): void {
  if (!NETWORK_LIFECYCLE_LOG_ENABLED) {
    return;
  }
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
  depsAppendNetworkLifecycle(handle, event);
}

let depsAppendNetworkLifecycle: (handle: ProcessHandle, event: string) => void = () => {};

const processIoSocketMetaDebug = new WeakMap<ProcessHandle, Map<number, string>>();

function recordProcessIoSocketMetaTransition(
  handle: ProcessHandle,
  fd: number,
  socketIdx: number | undefined,
  state: number | string,
  type: number | string,
  ext: number | string,
  buf: number | string,
  recv: number | string,
): void {
  if (!NETWORK_LIFECYCLE_LOG_ENABLED) {
    return;
  }
  let byFd = processIoSocketMetaDebug.get(handle);
  if (byFd === undefined) {
    byFd = new Map();
    processIoSocketMetaDebug.set(handle, byFd);
  }
  const next = `${socketIdx ?? "n/a"}/${state}/${type}/${ext}/${buf}/${recv}`;
  const previous = byFd.get(fd);
  if (previous === next) {
    return;
  }
  byFd.set(fd, next);
  appendNetworkLifecycle(handle, `socket-meta-transition:${handle.pid}:source=process-io:fd=${fd}:prev=${previous ?? "none"}:next=${next}`);
}

function formatNetworkFdTrace(handle: ProcessHandle): string {
  if (!NETWORK_FD_TRACE_LOG_ENABLED) {
    return "";
  }
  const len = handle.kernel.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return "";
  }
  const sampleToPid = (sample: bigint | number | undefined): number => {
    if (sample === undefined) {
      return 0;
    }
    return Number(BigInt.asIntN(32, BigInt(sample) & 0xffffffffn));
  };
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const nr = Number(handle.kernel.debug_fd_syscall_trace_nr?.(index) ?? -1n);
    if (![45, 46, 57, 63, 64, 66, 68, 70, 72, 73, 198, 203, 206, 207, 208, 209, 211, 212].includes(nr)) {
      continue;
    }
    const arg1 = Number(handle.kernel.debug_fd_syscall_trace_arg1?.(index) ?? 0n);
    const arg2 = Number(handle.kernel.debug_fd_syscall_trace_arg2?.(index) ?? 0n);
    let text = "";
    const absoluteArg1 = handle.guestBase + arg1;
    if (
      nr === 64
      && arg1 > 0
      && arg2 > 0
      && arg2 <= 512
      && absoluteArg1 + arg2 <= handle.memory.buffer.byteLength
    ) {
      text = `:text=${JSON.stringify(decoder.decode(new Uint8Array(handle.memory.buffer, absoluteArg1, arg2)))}`;
    }
    entries.push([
      `${index}:${nr}`,
      `pid=${sampleToPid(handle.kernel.debug_fd_syscall_trace_pid_sample0?.(index))}`,
      `a0=${handle.kernel.debug_fd_syscall_trace_arg0?.(index)?.toString() ?? "n/a"}`,
      `a0ofd=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? "n/a"}`,
      `a0ty=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? "n/a"}`,
      `a1=${arg1 || handle.kernel.debug_fd_syscall_trace_arg1?.(index)?.toString() || "n/a"}`,
      `a2=${arg2 || handle.kernel.debug_fd_syscall_trace_arg2?.(index)?.toString() || "n/a"}`,
      `a2ofd=${handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? "n/a"}`,
      `a2ty=${handle.kernel.debug_fd_syscall_trace_arg2_ofd_type?.(index) ?? "n/a"}`,
      `ret=${handle.kernel.debug_fd_syscall_trace_result?.(index)?.toString() ?? "n/a"}`,
      `st=${handle.kernel.debug_fd_syscall_trace_status?.(index) ?? "n/a"}`,
    ].join(":") + text);
  }
  return entries.slice(-96).join(",");
}

function isPromiseLike<T>(value: T | PromiseLike<T>): value is PromiseLike<T> {
  return value !== null && value !== undefined && typeof (value as { then?: unknown }).then === "function";
}

function copyBytes(bytes: Uint8Array<ArrayBufferLike>): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(bytes.length);
  out.set(bytes);
  return out;
}

function ownedBytes(bytes: Uint8Array): Uint8Array<ArrayBuffer> {
  if (bytes.buffer instanceof ArrayBuffer) {
    return bytes as Uint8Array<ArrayBuffer>;
  }
  return copyBytes(bytes);
}

export function createWorkerProcessIoHelpers(deps: WorkerProcessIoDeps) {
  depsAppendNetworkLifecycle = (handle, event) => {
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
  };
  const pendingNetworkInbound = new WeakMap<RuntimeNetworkConnection, Uint8Array<ArrayBuffer> | null>();
  const pendingNetworkReads = new WeakMap<RuntimeNetworkConnection, Promise<void>>();
  const pendingNetworkEof = new WeakSet<RuntimeNetworkConnection>();
  const networkPeerEof = new WeakSet<RuntimeNetworkConnection>();
  const suppressNetworkDrainAfterInbound = new WeakSet<RuntimeNetworkConnection>();
  const retryNetworkReadAfterPending = new WeakSet<RuntimeNetworkConnection>();
  const networkDrainMisses = new WeakMap<RuntimeNetworkConnection, number>();
  const networkEofDeferrals = new WeakMap<RuntimeNetworkConnection, number>();
  const observedSocketReads = new WeakMap<ProcessHandle, Map<number, { calls: number; bytes: bigint }>>();
  const pendingNetworkConnects = new WeakMap<ProcessHandle, Map<number, {
    fd: number;
    connection: RuntimeNetworkConnection;
  }>>();

  function networkConnectionOfds(handle: ProcessHandle): Map<number, number> {
    handle.networkConnectionOfds ??= new Map();
    return handle.networkConnectionOfds;
  }

  function requireSupervisorSocketCommitExternalConnectResult(handle: ProcessHandle) {
    const commit = handle.kernel.supervisor_socket_commit_external_connect_result;
    if (typeof commit !== "function") {
      throw new Error("kernel supervisor ABI missing supervisor_socket_commit_external_connect_result");
    }
    return commit;
  }

  function requireSupervisorSocketPrepareExternalPeer(handle: ProcessHandle) {
    const prepare = handle.kernel.supervisor_socket_prepare_external_peer;
    if (typeof prepare !== "function") {
      throw new Error("kernel supervisor ABI missing supervisor_socket_prepare_external_peer");
    }
    return prepare;
  }

  function requireSupervisorSocketCloseExternalPeer(handle: ProcessHandle) {
    const close = handle.kernel.supervisor_socket_close_external_peer;
    if (typeof close !== "function") {
      throw new Error("kernel supervisor ABI missing supervisor_socket_close_external_peer");
    }
    return close;
  }

  function requireSupervisorSocketBridgeActiveFd(handle: ProcessHandle) {
    const activeFd = handle.kernel.supervisor_socket_bridge_active_fd;
    if (typeof activeFd !== "function") {
      throw new Error("kernel supervisor ABI missing supervisor_socket_bridge_active_fd");
    }
    return activeFd;
  }

  function requireThreadSocketIdxForFd(handle: ProcessHandle) {
    const socketIdxForFd = handle.kernel.thread_socket_idx_for_fd;
    if (typeof socketIdxForFd !== "function") {
      throw new Error("kernel supervisor ABI missing thread_socket_idx_for_fd");
    }
    return socketIdxForFd;
  }

  function requireThreadSocketRecvAvailable(handle: ProcessHandle) {
    const recvAvailable = handle.kernel.thread_socket_recv_available;
    if (typeof recvAvailable !== "function") {
      throw new Error("kernel supervisor ABI missing thread_socket_recv_available");
    }
    return recvAvailable;
  }

  function requireThreadSocketRecvBufIdx(handle: ProcessHandle) {
    const recvBufIdx = handle.kernel.thread_socket_recv_buf_idx;
    if (typeof recvBufIdx !== "function") {
      throw new Error("kernel supervisor ABI missing thread_socket_recv_buf_idx");
    }
    return recvBufIdx;
  }

  function requireThreadSocketReadBytes(handle: ProcessHandle) {
    const readBytes = handle.kernel.thread_socket_read_bytes;
    if (typeof readBytes !== "function") {
      throw new Error("kernel supervisor ABI missing thread_socket_read_bytes");
    }
    return readBytes;
  }

  function requireAbiMaxSockets(handle: ProcessHandle): number {
    const maxSockets = handle.kernel.abi_max_sockets;
    if (typeof maxSockets !== "function") {
      throw new Error("kernel supervisor ABI missing abi_max_sockets");
    }
    return maxSockets();
  }

  function socketIdxForFd(handle: ProcessHandle, vmPtr: number, fd: number): number {
    return requireThreadSocketIdxForFd(handle)(vmPtr, fd);
  }

  function socketRecvAvailable(handle: ProcessHandle, socketIdx: number): number {
    return requireThreadSocketRecvAvailable(handle)(socketIdx);
  }

  function socketRecvBufIdx(handle: ProcessHandle, socketIdx: number): number {
    return requireThreadSocketRecvBufIdx(handle)(socketIdx);
  }

  function socketReadBytesSnapshot(handle: ProcessHandle, socketIdx: number): number | undefined {
    const raw = requireThreadSocketReadBytes(handle)(socketIdx);
    const readBytes = typeof raw === "bigint" ? Number(raw) : raw;
    return Number.isSafeInteger(readBytes) ? readBytes as number : undefined;
  }

  function recordNetworkPendingInbound(handle: ProcessHandle, bytes: Uint8Array<ArrayBuffer> | null): void {
    if (bytes !== null && bytes.length > handle.transferDebugStats.networkPendingInboundBytesMax) {
      handle.transferDebugStats.networkPendingInboundBytesMax = bytes.length;
    }
  }

  function setPendingNetworkInbound(
    handle: ProcessHandle,
    connection: RuntimeNetworkConnection,
    bytes: Uint8Array<ArrayBuffer> | null,
  ): void {
    recordNetworkPendingInbound(handle, bytes);
    pendingNetworkInbound.set(connection, bytes);
  }

  function recordNetworkBridgeRead(handle: ProcessHandle, value: Uint8Array | null | undefined): void {
    if (value === undefined || value === null) {
      return;
    }
    handle.transferDebugStats.networkBridgeReadCalls += 1;
    handle.transferDebugStats.networkBridgeReadBytes += value.length;
  }

  function recordGuestSocketReadProgress(handle: ProcessHandle, socketIdx: number | undefined): void {
    if (socketIdx === undefined || socketIdx < 0) {
      return;
    }
    const calls = handle.kernel.thread_socket_read_count?.(socketIdx);
    const bytes = handle.kernel.thread_socket_read_bytes?.(socketIdx);
    if (calls === undefined || bytes === undefined) {
      return;
    }
    let bySocket = observedSocketReads.get(handle);
    if (bySocket === undefined) {
      bySocket = new Map();
      observedSocketReads.set(handle, bySocket);
    }
    const current = { calls, bytes: BigInt(bytes) };
    const previous = bySocket.get(socketIdx);
    bySocket.set(socketIdx, current);
    if (previous === undefined) {
      return;
    }
    if (current.calls > previous.calls) {
      handle.transferDebugStats.networkGuestSocketReadCalls += current.calls - previous.calls;
    }
    if (current.bytes > previous.bytes) {
      handle.transferDebugStats.networkGuestSocketReadBytes += Number(current.bytes - previous.bytes);
    }
  }

  function recordGuestSocketReadProgressForFd(handle: ProcessHandle, vmPtr: number, fd: number): void {
    recordGuestSocketReadProgress(handle, handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd));
  }

  function ensureNetworkBookkeeping(handle: ProcessHandle): void {
    handle.transferDebugStats ??= createTransferDebugStats();
    handle.networkConnections ??= new Map();
    handle.networkPendingConnectFds ??= new Set();
    handle.networkClosedFds ??= new Set();
    handle.networkClosedSocketSlots ??= new Map();
    handle.networkSocketSlotWatermarks ??= new Map();
    handle.networkRecvHeadWatermarks ??= new Map();
    handle.networkRecvTailWatermarks ??= new Map();
    handle.networkRecvPayloadWatermarks ??= new Map();
  }

  function rememberNetworkOfdSlot(handle: ProcessHandle, ofdId: number): void {
    const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
    if (ofdId <= 0 || ofdId >= maxFds) {
      return;
    }
    const tableBytes = handle.kernel.kernel_ofd_table_bytes?.() ?? 0;
    const slotBytes = tableBytes / maxFds;
    if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
      return;
    }
    if (handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_socket?.()) {
      return;
    }
    const offset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
    handle.networkOfdSlotWatermarks ??= new Map();
    handle.networkOfdSlotWatermarks.set(ofdId, new Uint8Array(handle.memory.buffer).slice(offset, offset + slotBytes));
  }

  function clearNetworkRecvWatermarks(handle: ProcessHandle, fd: number): void {
    ensureNetworkBookkeeping(handle);
    if (NETWORK_LIFECYCLE_LOG_ENABLED && handle.networkRecvPayloadWatermarks?.has(fd)) {
      deps.appendWorkerDebugLog(handle, `net-watermark-clear:${handle.pid}:fd=${fd}`);
    }
    handle.networkRecvHeadWatermarks.delete(fd);
    handle.networkRecvTailWatermarks.delete(fd);
    handle.networkRecvPayloadWatermarks ??= new Map();
    handle.networkRecvPayloadWatermarks.delete(fd);
    handle.networkSocketSlotWatermarks ??= new Map();
    handle.networkSocketSlotWatermarks.delete(fd);
  }

  function moveNetworkRecvWatermarks(handle: ProcessHandle, fromFd: number, toFd: number): void {
    if (fromFd === toFd) {
      return;
    }
    const head = handle.networkRecvHeadWatermarks.get(fromFd);
    const tail = handle.networkRecvTailWatermarks.get(fromFd);
    handle.networkRecvPayloadWatermarks ??= new Map();
    const payload = handle.networkRecvPayloadWatermarks.get(fromFd);
    handle.networkSocketSlotWatermarks ??= new Map();
    const socketSlot = handle.networkSocketSlotWatermarks.get(fromFd);
    clearNetworkRecvWatermarks(handle, fromFd);
    if (head !== undefined) {
      handle.networkRecvHeadWatermarks.set(toFd, head);
    }
    if (tail !== undefined) {
      handle.networkRecvTailWatermarks.set(toFd, tail);
    }
    if (payload !== undefined) {
      handle.networkRecvPayloadWatermarks.set(toFd, payload);
    }
    if (socketSlot !== undefined) {
      handle.networkSocketSlotWatermarks.set(toFd, socketSlot);
    }
  }

  function copyNetworkRecvWatermarks(source: ProcessHandle, target: ProcessHandle, fd: number): void {
    const head = source.networkRecvHeadWatermarks.get(fd);
    if (head !== undefined && !target.networkRecvHeadWatermarks.has(fd)) {
      target.networkRecvHeadWatermarks.set(fd, head);
    }
    const tail = source.networkRecvTailWatermarks.get(fd);
    if (tail !== undefined && !target.networkRecvTailWatermarks.has(fd)) {
      target.networkRecvTailWatermarks.set(fd, tail);
    }
    const payload = source.networkRecvPayloadWatermarks.get(fd);
    const targetPayload = target.networkRecvPayloadWatermarks.get(fd);
    const payloadProgress = (snapshot: typeof payload): number => {
      if (snapshot === undefined) {
        return -1;
      }
      if (snapshot.deliveredBytes !== undefined) {
        return snapshot.deliveredBytes;
      }
      if (snapshot.readBytes !== undefined && snapshot.availableBytes !== undefined) {
        return snapshot.readBytes + snapshot.availableBytes;
      }
      return snapshot.tail;
    };
    if (
      payload !== undefined
      && (
        targetPayload === undefined
        || counterAhead(payload.tail, targetPayload.tail, payload.capacity)
        || payloadProgress(payload) > payloadProgress(targetPayload)
      )
    ) {
      target.networkRecvPayloadWatermarks.set(fd, {
        recvBufIdx: payload.recvBufIdx,
        head: payload.head,
        tail: payload.tail,
        capacity: payload.capacity,
        readBytes: payload.readBytes,
        availableBytes: payload.availableBytes,
        deliveredBytes: payload.deliveredBytes,
        data: payload.data.slice(),
      });
    }
    const socketSlot = source.networkSocketSlotWatermarks.get(fd);
    if (socketSlot !== undefined && !target.networkSocketSlotWatermarks.has(fd)) {
      target.networkSocketSlotWatermarks.set(fd, {
        offset: socketSlot.offset,
        data: socketSlot.data.slice(),
      });
    }
  }

  function rememberNetworkRecvPayload(handle: ProcessHandle, vmPtr: number, fd: number, writtenBytes?: number): void {
    const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd) ?? -1;
    handle.networkRecvPayloadWatermarks ??= new Map();
    if (socketIdx < 0) {
      return;
    }
    const recvBufIdx = handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1;
    if (recvBufIdx < 0) {
      return;
    }
    const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes?.();
    if (netBufPoolBytes === undefined) {
      return;
    }
    const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
    const netBufferCount = maxSockets * 2;
    const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
    if (!Number.isInteger(netBufferBytes) || netBufferBytes <= 16) {
      return;
    }
    const absolute = handle.kernelState.netBufBase + recvBufIdx * netBufferBytes;
    const memory = new Uint8Array(handle.memory.buffer);
    const view = new DataView(memory.buffer, absolute, 16);
    const head = view.getUint32(0, true);
    const tail = view.getUint32(4, true);
    const capacity = netBufferBytes - 16;
    const readBytesRaw = handle.kernel.thread_socket_read_bytes?.(socketIdx);
    const readBytes = typeof readBytesRaw === "bigint" ? Number(readBytesRaw) : readBytesRaw;
    const availableBytes = handle.kernel.thread_socket_recv_available?.(socketIdx);
    const safeReadBytes = Number.isSafeInteger(readBytes) ? readBytes as number : undefined;
    const safeAvailableBytes = Number.isSafeInteger(availableBytes) && availableBytes !== undefined && availableBytes >= 0
      ? availableBytes
      : undefined;
    const observedDeliveredBytes = safeReadBytes !== undefined && safeAvailableBytes !== undefined
      ? safeReadBytes + safeAvailableBytes
      : undefined;
    const previousPayload = handle.networkRecvPayloadWatermarks.get(fd);
    const previousDeliveredBytes = previousPayload === undefined
      ? undefined
      : networkRecvPayloadDeliveredBytes(previousPayload);
    const deliveredBytes = previousDeliveredBytes !== undefined && writtenBytes !== undefined && writtenBytes > 0
      ? previousDeliveredBytes + writtenBytes
      : observedDeliveredBytes;
    const socketSlot = snapshotNetworkSocketSlot(handle, vmPtr, fd);
    if (socketSlot !== null) {
      handle.networkSocketSlotWatermarks ??= new Map();
      handle.networkSocketSlotWatermarks.set(fd, socketSlot);
    }
    handle.networkRecvHeadWatermarks.set(fd, head);
    handle.networkRecvTailWatermarks.set(fd, tail);
    handle.networkRecvPayloadWatermarks.set(fd, {
      recvBufIdx,
      head,
      tail,
      capacity,
      readBytes: safeReadBytes,
      availableBytes: safeAvailableBytes,
      deliveredBytes,
      data: memory.slice(absolute + 16, absolute + netBufferBytes),
    });
  }

  function networkRecvPayloadDeliveredBytes(payload: {
    readBytes?: number;
    availableBytes?: number;
    deliveredBytes?: number;
  }): number | undefined {
    if (payload.deliveredBytes !== undefined) {
      return payload.deliveredBytes;
    }
    if (payload.readBytes !== undefined && payload.availableBytes !== undefined) {
      return payload.readBytes + payload.availableBytes;
    }
    return undefined;
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

  function restoreBufferedNetworkRecvFromWatermark(
    handle: ProcessHandle,
    vmPtr: number,
    fd: number,
    options: { allowPartialReadProgress?: boolean } = {},
  ): boolean {
    const payload = handle.networkRecvPayloadWatermarks.get(fd);
    if (!payload) {
      if (NETWORK_LIFECYCLE_LOG_ENABLED) {
        deps.appendWorkerDebugLog(handle, `net-watermark-unread:${handle.pid}:fd=${fd}:missing`);
      }
      return false;
    }
    let socketIdx = socketIdxForFd(handle, vmPtr, fd);
    if (socketIdx < 0) {
      const socketSlot = handle.networkSocketSlotWatermarks.get(fd);
      if (!socketSlot) {
        return false;
      }
      restoreNetworkSocketSlotSnapshot(handle, socketSlot);
      socketIdx = socketIdxForFd(handle, vmPtr, fd);
      if (socketIdx < 0) {
        return false;
      }
    }
    let recvBufIdx = socketRecvBufIdx(handle, socketIdx);
    if (recvBufIdx !== payload.recvBufIdx) {
      const socketSlot = handle.networkSocketSlotWatermarks.get(fd);
      if (!socketSlot) {
        return false;
      }
      restoreNetworkSocketSlotSnapshot(handle, socketSlot);
      recvBufIdx = socketRecvBufIdx(handle, socketIdx);
      if (recvBufIdx !== payload.recvBufIdx) {
        return false;
      }
    }
    let partialReadProgress: number | null = null;
    let partialReadTail: number | null = null;
    let progressConfirmsUnreadPayload = false;
    if (
      payload.readBytes !== undefined
      && payload.availableBytes !== undefined
    ) {
      const readBytes = socketReadBytesSnapshot(handle, socketIdx);
      const deliveredBytes = networkRecvPayloadDeliveredBytes(payload);
      if (
        Number.isSafeInteger(readBytes)
      ) {
        const availableBytes = socketRecvAvailable(handle, socketIdx);
        const readProgress = (readBytes as number) - payload.readBytes;
        const deliveredRemaining = deliveredBytes === undefined
          ? undefined
          : deliveredBytes - (readBytes as number);
        const payloadStillCoversReadProgress = deliveredRemaining === undefined
          ? readProgress >= 0 && readProgress < payload.availableBytes
          : readProgress >= 0
            && readProgress <= payload.availableBytes
            && deliveredRemaining >= 0
            && deliveredRemaining <= payload.availableBytes - readProgress;
        if (
          deliveredBytes !== undefined
          && Number.isSafeInteger(availableBytes)
          && availableBytes !== undefined
          && availableBytes >= 0
          && (readBytes as number) + availableBytes > deliveredBytes
          && !payloadStillCoversReadProgress
        ) {
          appendNetworkLifecycle(
            handle,
            `net-watermark-restore-skip-progress:${handle.pid}:fd=${fd}:payload=${deliveredBytes}:current=${(readBytes as number) + availableBytes}:ptail=${payload.tail}`,
          );
          return false;
        }
        if (
          !payloadStillCoversReadProgress
          && readProgress >= payload.availableBytes
        ) {
          return false;
        }
        progressConfirmsUnreadPayload = deliveredBytes === undefined && readProgress >= 0;
        if (
          readProgress > 0
          && (
            !options.allowPartialReadProgress
            || deliveredBytes === undefined
          )
        ) {
          return false;
        }
        if (payloadStillCoversReadProgress && (readProgress > 0 || deliveredRemaining !== undefined)) {
          partialReadProgress = readProgress;
          partialReadTail = deliveredRemaining === undefined
            ? payload.tail
            : (payload.head + readProgress + deliveredRemaining) % payload.capacity;
        }
        if (
          deliveredBytes === undefined
          && readProgress > 0
          && Number.isSafeInteger(availableBytes)
          && availableBytes !== undefined
          && availableBytes >= 0
          && (readBytes as number) + availableBytes > payload.readBytes + payload.availableBytes
        ) {
          appendNetworkLifecycle(
            handle,
            `net-watermark-restore-skip-progress:${handle.pid}:fd=${fd}:payload=${payload.readBytes + payload.availableBytes}:current=${(readBytes as number) + availableBytes}:ptail=${payload.tail}`,
          );
          return false;
        }
        if (
          readProgress > 0
          && !(
            Number.isSafeInteger(availableBytes)
            && availableBytes !== undefined
            && availableBytes >= 0
          )
        ) {
          progressConfirmsUnreadPayload = false;
        }
      }
    }
    const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes();
    const maxSockets = requireAbiMaxSockets(handle);
    const netBufferCount = maxSockets * 2;
    const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
    if (!Number.isInteger(netBufferBytes) || netBufferBytes <= 16 || payload.data.length !== netBufferBytes - 16) {
      return false;
    }
    const absolute = handle.kernelState.netBufBase + recvBufIdx * netBufferBytes;
    const memory = new Uint8Array(handle.memory.buffer);
    const view = new DataView(memory.buffer, absolute, 16);
    const currentHead = view.getUint32(0, true);
    const currentTail = view.getUint32(4, true);
    if (
      partialReadProgress !== null
      && payload.availableBytes !== undefined
      && (
        partialReadProgress < payload.availableBytes
        || partialReadTail !== null
      )
    ) {
      const restoredHead = (payload.head + partialReadProgress) % payload.capacity;
      appendNetworkLifecycle(
        handle,
        `net-watermark-restore-partial:${handle.pid}:fd=${fd}:head=${currentHead}:restoredHead=${restoredHead}:tail=${currentTail}:ptail=${payload.tail}:rtail=${partialReadTail ?? payload.tail}:progress=${partialReadProgress}:avail=${payload.availableBytes}`,
      );
      memory.set(payload.data, absolute + 16);
      view.setUint32(0, restoredHead, true);
      view.setUint32(4, partialReadTail ?? payload.tail, true);
      return true;
    }
    if (progressConfirmsUnreadPayload) {
      appendNetworkLifecycle(
        handle,
        `net-watermark-restore-progress:${handle.pid}:fd=${fd}:head=${currentHead}:tail=${currentTail}:phead=${payload.head}:ptail=${payload.tail}`,
      );
      memory.set(payload.data, absolute + 16);
      view.setUint32(0, payload.head, true);
      view.setUint32(4, payload.tail, true);
      return true;
    }
    if (!counterAhead(payload.tail, currentHead, payload.capacity)) {
      return false;
    }
    if (counterAhead(payload.tail, currentTail, payload.capacity) || currentTail === currentHead) {
      appendNetworkLifecycle(
        handle,
        `net-watermark-restore-use:${handle.pid}:fd=${fd}:head=${currentHead}:tail=${currentTail}:ptail=${payload.tail}:cap=${payload.capacity}`,
      );
      memory.set(payload.data, absolute + 16);
      view.setUint32(4, payload.tail, true);
    }
    return true;
  }

  function hasUnreadNetworkRecvWatermark(
    handle: ProcessHandle,
    vmPtr: number,
    fd: number,
  ): boolean {
    const payload = handle.networkRecvPayloadWatermarks.get(fd);
    if (!payload) {
      return false;
    }
    const socketIdx = socketIdxForFd(handle, vmPtr, fd);
    if (socketIdx < 0) {
      return false;
    }
    const safeReadBytes = socketReadBytesSnapshot(handle, socketIdx);
    if (
      payload.readBytes !== undefined
      && payload.availableBytes !== undefined
    ) {
      if (safeReadBytes === undefined) {
        if (NETWORK_LIFECYCLE_LOG_ENABLED) {
          deps.appendWorkerDebugLog(
            handle,
            `net-watermark-unread:${handle.pid}:fd=${fd}:read=unknown:base=${payload.readBytes}:avail=${payload.availableBytes}:unread=0`,
          );
        }
        return false;
      }
      const unread = safeReadBytes - payload.readBytes < payload.availableBytes;
      if (NETWORK_LIFECYCLE_LOG_ENABLED) {
        deps.appendWorkerDebugLog(
          handle,
          `net-watermark-unread:${handle.pid}:fd=${fd}:read=${safeReadBytes}:base=${payload.readBytes}:avail=${payload.availableBytes}:unread=${unread ? 1 : 0}`,
        );
      }
      return unread;
    }
    const recvBufIdx = socketRecvBufIdx(handle, socketIdx);
    if (recvBufIdx !== payload.recvBufIdx) {
      return false;
    }
    const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes();
    const maxSockets = requireAbiMaxSockets(handle);
    const netBufferCount = maxSockets * 2;
    const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
    if (!Number.isInteger(netBufferBytes) || netBufferBytes <= 16) {
      return false;
    }
    const absolute = handle.kernelState.netBufBase + recvBufIdx * netBufferBytes;
    const view = new DataView(handle.memory.buffer, absolute, 16);
    const currentHead = view.getUint32(0, true);
    return counterAhead(payload.tail, currentHead, payload.capacity);
  }

  function hasBufferedNetworkRecv(handle: ProcessHandle, vmPtr: number, fd: number): boolean {
    const payload = handle.networkRecvPayloadWatermarks.get(fd);
    if (payload && handle.networkConnections.has(fd)) {
      const socketIdx = socketIdxForFd(handle, vmPtr, fd);
      const safeReadBytes = socketIdx >= 0 ? socketReadBytesSnapshot(handle, socketIdx) : undefined;
      if (
        safeReadBytes !== undefined
        && payload.readBytes !== undefined
        && payload.availableBytes !== undefined
        && safeReadBytes - payload.readBytes < payload.availableBytes
      ) {
        restoreBufferedNetworkRecvFromWatermark(handle, vmPtr, fd);
        appendNetworkLifecycle(handle, `net-buffered:${handle.pid}:fd=${fd}:payload-watermark`);
        return true;
      }
    }
    const socketIdx = socketIdxForFd(handle, vmPtr, fd);
    if (socketIdx >= 0) {
      const available = socketRecvAvailable(handle, socketIdx);
      const safeReadBytes = socketReadBytesSnapshot(handle, socketIdx);
      if (payload && safeReadBytes !== undefined) {
        const deliveredBytes = networkRecvPayloadDeliveredBytes(payload);
        if (deliveredBytes !== undefined && safeReadBytes >= deliveredBytes) {
          restoreBufferedNetworkRecvFromWatermark(handle, vmPtr, fd, { allowPartialReadProgress: true });
          const restoredAvailable = socketRecvAvailable(handle, socketIdx);
          if (restoredAvailable <= 0) {
            appendNetworkLifecycle(
              handle,
              `net-buffered:${handle.pid}:fd=${fd}:false:socket=${socketIdx}:available=${restoredAvailable}:payload=delivered`,
            );
            clearNetworkRecvWatermarks(handle, fd);
            return false;
          }
        }
      }
      if (available > 0) {
        appendNetworkLifecycle(handle, `net-buffered:${handle.pid}:fd=${fd}:available=${available}`);
        return true;
      }
      if (restoreBufferedNetworkRecvFromWatermark(handle, vmPtr, fd)) {
        appendNetworkLifecycle(handle, `net-buffered:${handle.pid}:fd=${fd}:restore-watermark`);
        return true;
      }
      if (hasUnreadNetworkRecvWatermark(handle, vmPtr, fd)) {
        appendNetworkLifecycle(handle, `net-buffered:${handle.pid}:fd=${fd}:unread-watermark`);
        return true;
      }
      appendNetworkLifecycle(
        handle,
        `net-buffered:${handle.pid}:fd=${fd}:false:socket=${socketIdx}:available=${available}:payload=${handle.networkRecvPayloadWatermarks.has(fd) ? 1 : 0}`,
      );
      clearNetworkRecvWatermarks(handle, fd);
      return false;
    }
    appendNetworkLifecycle(handle, `net-buffered:${handle.pid}:fd=${fd}:false:socket=${socketIdx}`);
    clearNetworkRecvWatermarks(handle, fd);
    return false;
  }

  function hasCurrentNetworkRecvAvailable(handle: ProcessHandle, vmPtr: number, fd: number): boolean {
    const socketIdx = socketIdxForFd(handle, vmPtr, fd);
    if (socketIdx < 0) {
      return false;
    }
    return socketRecvAvailable(handle, socketIdx) > 0;
  }

  function clearCurrentNetworkRecvBuffer(handle: ProcessHandle, vmPtr: number, fd: number): void {
    const socketIdx = socketIdxForFd(handle, vmPtr, fd);
    if (socketIdx < 0) {
      return;
    }
    const recvBufIdx = socketRecvBufIdx(handle, socketIdx);
    if (recvBufIdx < 0) {
      return;
    }
    const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes?.();
    const maxSockets = handle.kernel.abi_max_sockets?.();
    if (netBufPoolBytes === undefined || maxSockets === undefined) {
      return;
    }
    const netBufferCount = maxSockets * 2;
    const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
    if (!Number.isInteger(netBufferBytes) || netBufferBytes <= 16 || recvBufIdx >= netBufferCount) {
      return;
    }
    const absolute = handle.kernelState.netBufBase + recvBufIdx * netBufferBytes;
    const view = new DataView(handle.memory.buffer, absolute, 16);
    const head = view.getUint32(0, true);
    view.setUint32(4, head, true);
    handle.networkRecvHeadWatermarks.set(fd, head);
    handle.networkRecvTailWatermarks.set(fd, head);
  }

  function queueNetworkReadPromise(
    handle: ProcessHandle,
    vmPtr: number,
    fd: number,
    connection: RuntimeNetworkConnection,
    read: PromiseLike<Uint8Array | null | undefined>,
  ): void {
    if (pendingNetworkReads.has(connection)) {
      deps.appendWorkerDebugLog(handle, `net-read-skip:${handle.pid}:vm=0x${vmPtr.toString(16)}`);
      return;
    }
    handle.transferDebugStats.networkBridgeReadQueued += 1;
    deps.appendWorkerDebugLog(handle, `net-read-queue:${handle.pid}:vm=0x${vmPtr.toString(16)}`);
    appendNetworkLifecycle(handle, `net-read-queue:${handle.pid}:0x${vmPtr.toString(16)}`);
    const pending = Promise.resolve(read)
      .then((value) => {
        deps.appendWorkerDebugLog(
          handle,
          `net-read-resolve:${handle.pid}:vm=0x${vmPtr.toString(16)}:${
            value === undefined ? "undefined" : value === null ? "null" : `n=${value.length}`
          }`,
        );
        appendNetworkLifecycle(
          handle,
          `net-read-resolve:${handle.pid}:0x${vmPtr.toString(16)}:${
            value === undefined ? "undefined" : value === null ? "null" : `n=${value.length}`
          }`,
        );
        if (value !== undefined) {
          recordNetworkBridgeRead(handle, value);
          if (value === null) {
            const pendingInbound = pendingNetworkInbound.get(connection);
            if (pendingInbound && pendingInbound.length > 0) {
              pendingNetworkEof.add(connection);
              return;
            }
            const activeHandle = resolveActiveNetworkHandle(handle, vmPtr, fd, connection);
            if (!activeHandle) {
              pendingNetworkEof.add(connection);
              return;
            }
          }
          setPendingNetworkInbound(handle, connection, value === null ? null : ownedBytes(value));
          void (async () => {
            const activeHandle = resolveActiveNetworkHandle(handle, vmPtr, fd, connection);
            if (!activeHandle) {
              return;
            }
            const pendingSocketSync = activeHandle.socketSyncPromise;
            if (pendingSocketSync != null) {
              deps.appendWorkerDebugLog(activeHandle, `net-read-await-socket-sync:${activeHandle.pid}:vm=0x${vmPtr.toString(16)}`);
              await pendingSocketSync;
            }
            const syncedHandle = resolveActiveNetworkHandle(activeHandle, vmPtr, fd, connection);
            if (!syncedHandle) {
              return;
            }
            const changed = await pumpNetworkConnections(syncedHandle, vmPtr);
            if (changed) {
              await deps.requestSocketStateSync(syncedHandle, true);
              await deps.requestPipeStateSync(syncedHandle, false);
            }
            deps.notifyHostIoProgress(syncedHandle);
            deps.pumpProcess(syncedHandle);
          })().catch((error) => {
            const activeHandle = deps.getActiveHandle?.(handle) ?? handle;
            if (deps.isActiveHandle(activeHandle)) {
              deps.failProcess(activeHandle, error instanceof Error ? error : new Error(String(error)));
            }
          });
        }
      })
      .catch((error) => {
        if (deps.isActiveHandle(handle)) {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        }
      })
      .finally(() => {
        const retryAfterPending = retryNetworkReadAfterPending.has(connection);
        retryNetworkReadAfterPending.delete(connection);
        pendingNetworkReads.delete(connection);
        if (retryAfterPending) {
          void (async () => {
            const activeHandle = resolveActiveNetworkHandle(handle, vmPtr, fd, connection);
            if (!activeHandle) {
              return;
            }
            const changed = await pumpNetworkConnections(activeHandle, vmPtr);
            if (changed) {
              await deps.requestSocketStateSync(activeHandle, true);
              await deps.requestPipeStateSync(activeHandle, false);
            }
            deps.notifyHostIoProgress(activeHandle);
            deps.pumpProcess(activeHandle);
          })().catch((error) => {
            const activeHandle = deps.getActiveHandle?.(handle) ?? handle;
            if (deps.isActiveHandle(activeHandle)) {
              deps.failProcess(activeHandle, error instanceof Error ? error : new Error(String(error)));
            }
          });
        }
      });
    pendingNetworkReads.set(connection, pending);
  }

  function readConnectionNow(
    handle: ProcessHandle,
    vmPtr: number,
    fd: number,
    connection: RuntimeNetworkConnection,
    maxBytes: number,
  ): Uint8Array | null | undefined {
    if (pendingNetworkReads.has(connection)) {
      return undefined;
    }
    const result = connection.read(maxBytes);
    if (!isPromiseLike(result)) {
      recordNetworkBridgeRead(handle, result);
      return result;
    }
    queueNetworkReadPromise(handle, vmPtr, fd, connection, result);
    return undefined;
  }

  function resolveActiveNetworkHandle(
    handle: ProcessHandle,
    vmPtr: number,
    fd: number,
    connection: RuntimeNetworkConnection,
  ): ProcessHandle | null {
    if (deps.isActiveHandle(handle)) {
      return handle;
    }
    const activeHandle = deps.getActiveHandle?.(handle);
    if (!activeHandle || !deps.isActiveHandle(activeHandle)) {
      return null;
    }
    ensureNetworkBookkeeping(activeHandle);
    activeHandle.networkBridge ??= handle.networkBridge;
    activeHandle.networkClosedFds.delete(fd);
    activeHandle.networkClosedSocketSlots.delete(fd);
    activeHandle.networkPendingConnectFds.delete(fd);
    clearNetworkRecvWatermarks(activeHandle, fd);
    copyNetworkRecvWatermarks(handle, activeHandle, fd);
    if (activeHandle.networkConnections.get(fd) !== connection) {
      activeHandle.networkConnections.set(fd, connection);
    }
    const ofdId = activeHandle.kernel.process_fd_ofd_id(vmPtr, fd);
    networkConnectionOfds(activeHandle).set(fd, ofdId);
    rememberNetworkOfdSlot(activeHandle, ofdId);
    deps.appendWorkerDebugLog(activeHandle, `net-read-adopt-active:${activeHandle.pid}:fd=${fd}:vm=0x${vmPtr.toString(16)}`);
    return activeHandle;
  }

  function scheduleQueuedStdinFlush(handle: ProcessHandle): void {
    if (handle.stdinFlushPromise !== null) {
      return;
    }
    handle.stdinFlushPromise = (async () => {
      const pendingPipeSync = handle.pipeSyncPromise;
      if (pendingPipeSync !== null) {
        await pendingPipeSync;
      }
      if (!deps.isActiveHandle(handle)) {
        return;
      }
      const changed = deps.flushStdin(handle);
      if (!changed) {
        return;
      }
      await deps.requestPipeStateSync(handle, true);
    })().catch((error) => {
      if (deps.isActiveHandle(handle)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    }).finally(() => {
      handle.stdinFlushPromise = null;
      if (deps.isActiveHandle(handle) && handle.stdinQueue.length > 0) {
        scheduleQueuedStdinFlush(handle);
      }
    });
  }

  function writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void {
    deps.writeProcessStdin(handle, data);
    if (handle.pipeSyncPromise !== null) {
      scheduleQueuedStdinFlush(handle);
      return;
    }
    const changed = deps.flushStdin(handle);
    if (!changed) {
      if (kernelWorkerOwnsPipeState(handle)) {
        void deps.requestPipeStateSync(handle, true).catch((error) => {
          if (deps.isActiveHandle(handle)) {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          }
        });
      }
      return;
    }
    void deps.requestPipeStateSync(handle, true).catch((error) => {
      if (deps.isActiveHandle(handle)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  async function injectHttpRequest(
    handle: ProcessHandle,
    port: number,
    request: string,
  ): Promise<string> {
    const requestBytes = encoder.encode(request);
    const scratch = writeKernelScratch(handle.kernel, requestBytes, 0x20000);
    const chunkSize = 64 * 1024;
    const responseChunks: Uint8Array[] = [];

    ensureKernelMemoryCapacity(handle.kernel, scratch + chunkSize);
    const connId = handle.kernel.process_inject_http(
      handle.vmPtr,
      port,
      scratch,
      requestBytes.length,
    );
    if (connId < 0) {
      throw new Error(`no listening process on port ${port}`);
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);

    const deadline = Date.now() + 10_000;
    try {
      for (;;) {
        deps.notifyHostIoProgress(handle);
        deps.drainProcessOutput(handle);

        const n = handle.kernel.process_read_http_response(
          handle.vmPtr,
          connId,
          scratch,
          chunkSize,
        );
        if (n < 0) {
          break;
        }
        if (n > 0) {
          responseChunks.push(new Uint8Array(handle.memory.buffer.slice(scratch, scratch + n)));
          continue;
        }
        if (Date.now() >= deadline) {
          throw new Error(`timed out waiting for HTTP response on port ${port}`);
        }
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    } finally {
      handle.kernel.process_close_http_connection(handle.vmPtr, connId);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    }

    const total = responseChunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const merged = new Uint8Array(total);
    let offset = 0;
    for (const chunk of responseChunks) {
      merged.set(chunk, offset);
      offset += chunk.length;
    }
    return decoder.decode(merged);
  }

  function readConnectEndpoint(handle: ProcessHandle, vmPtr: number): {
    fd: number;
    endpoint: RuntimeNetworkEndpoint;
  } | null {
    const scratch = kernelScratchBase(handle.kernel) + 0x20000;
    ensureMemoryCapacity(handle.kernel.memory ?? handle.memory, scratch + 28);
    const endpointFromBytes = (fd: number, bytes: Uint8Array, addrLen: number): {
      fd: number;
      endpoint: RuntimeNetworkEndpoint;
    } | null => {
      if (addrLen < 8) {
        return null;
      }
      const family = bytes[0] | (bytes[1] << 8);
      const port = (bytes[2] << 8) | bytes[3];
      if (family === 2 && addrLen >= 8) {
        return {
          fd,
          endpoint: {
            family: "inet",
            address: `${bytes[4]}.${bytes[5]}.${bytes[6]}.${bytes[7]}`,
            port,
          },
        };
      }
      if (family === 10 && addrLen >= 24) {
        const groups: string[] = [];
        for (let offset = 8; offset < 24; offset += 2) {
          groups.push(((bytes[offset] << 8) | bytes[offset + 1]).toString(16));
        }
        return {
          fd,
          endpoint: {
            family: "inet6",
            address: groups.join(":"),
            port,
          },
        };
      }
      return null;
    };
    const endpointFromScratch = (fd: number, addrLen: number) => endpointFromBytes(
      fd,
      new Uint8Array(handle.memory.buffer, scratch, Math.max(0, addrLen)),
      addrLen,
    );

    const readPeerAddr = handle.kernel.supervisor_socket_connect_peer_addr;
    const readConnectFd = handle.kernel.supervisor_socket_connect_fd;
    if (readPeerAddr && readConnectFd) {
      const addrLen = readPeerAddr(vmPtr, scratch, 28);
      const endpoint = endpointFromScratch(readConnectFd(vmPtr), addrLen);
      if (endpoint) {
        return endpoint;
      }
    }

    const readPeerAddrForFd = handle.kernel.supervisor_socket_connect_peer_addr_for_fd;
    if (readPeerAddrForFd) {
      const maxFds = handle.kernel.abi_max_fds();
      const socketFdType = handle.kernel.abi_fd_type_socket();
      for (let fd = 0; fd < maxFds; fd += 1) {
        const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
        if (ofdId <= 0 || handle.kernel.kernel_ofd_fd_type(ofdId) !== socketFdType) {
          continue;
        }
        const addrLen = readPeerAddrForFd(vmPtr, fd, scratch, 28);
        const endpoint = endpointFromScratch(fd, addrLen);
        if (endpoint) {
          return endpoint;
        }
      }
    }

    return null;
  }

  async function resolveNetworkConnect(handle: ProcessHandle, vmPtr: number): Promise<bigint> {
    ensureNetworkBookkeeping(handle);
    if (!handle.networkBridge) {
      deps.appendWorkerDebugLog(handle, `net-connect-no-bridge:${handle.pid}`);
      return BigInt(-handle.kernel.abi_econnrefused());
    }
    const connect = readConnectEndpoint(handle, vmPtr);
    if (!connect) {
      const blockedNr = handle.kernel.thread_blocked_syscall_nr?.(vmPtr)?.toString() ?? "n/a";
      const arg0 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0)?.toString() ?? "n/a";
      const arg1 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 1)?.toString() ?? "n/a";
      const arg2 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 2)?.toString() ?? "n/a";
      deps.appendWorkerDebugLog(
        handle,
        `net-connect-missing-endpoint:${handle.pid}:nr=${blockedNr}:args=${arg0}/${arg1}/${arg2}`,
      );
      return BigInt(-handle.kernel.abi_econnrefused());
    }
    handle.networkPendingConnectFds.add(connect.fd);
    try {
      const result = await handle.networkBridge.connect({
        pid: handle.pid,
        fd: connect.fd,
        endpoint: connect.endpoint,
      });
      const errno = result?.errno;
      if (errno === undefined || errno === 0) {
        if (result?.connection) {
          let pending = pendingNetworkConnects.get(handle);
          if (!pending) {
            pending = new Map();
            pendingNetworkConnects.set(handle, pending);
          }
          pending.set(vmPtr, {
            fd: connect.fd,
            connection: result.connection,
          });
        } else {
          handle.networkPendingConnectFds.delete(connect.fd);
        }
        deps.appendWorkerDebugLog(
          handle,
          `net-connect:${handle.pid}:fd=${connect.fd}:${connect.endpoint.family}:${connect.endpoint.address}:${connect.endpoint.port}:ok`,
        );
        logNetworkDebug(`connect pid=${handle.pid} fd=${connect.fd} ${connect.endpoint.family} ${connect.endpoint.address}:${connect.endpoint.port} ok connection=${result?.connection ? 1 : 0}`);
        return 0n;
      }
      deps.appendWorkerDebugLog(
        handle,
        `net-connect:${handle.pid}:fd=${connect.fd}:${connect.endpoint.family}:${connect.endpoint.address}:${connect.endpoint.port}:errno=${errno}`,
      );
      logNetworkDebug(`connect pid=${handle.pid} fd=${connect.fd} ${connect.endpoint.family} ${connect.endpoint.address}:${connect.endpoint.port} errno=${errno}`);
      handle.networkPendingConnectFds.delete(connect.fd);
      return BigInt(errno < 0 ? errno : -errno);
    } catch {
      deps.appendWorkerDebugLog(
        handle,
        `net-connect:${handle.pid}:fd=${connect.fd}:${connect.endpoint.family}:${connect.endpoint.address}:${connect.endpoint.port}:throw`,
      );
      logNetworkDebug(`connect pid=${handle.pid} fd=${connect.fd} ${connect.endpoint.family} ${connect.endpoint.address}:${connect.endpoint.port} throw`);
      handle.networkPendingConnectFds.delete(connect.fd);
      return BigInt(-handle.kernel.abi_econnrefused());
    }
  }

  async function commitNetworkConnect(handle: ProcessHandle, vmPtr: number, connectResult?: bigint): Promise<void> {
    ensureNetworkBookkeeping(handle);
    const pending = pendingNetworkConnects.get(handle);
    const connect = pending?.get(vmPtr);
    if (!pending || !connect) {
      return;
    }
    pending.delete(vmPtr);
    if (pending.size === 0) {
      pendingNetworkConnects.delete(handle);
    }
    handle.networkPendingConnectFds.delete(connect.fd);

    const result = BigInt.asIntN(64, connectResult ?? handle.kernel.thread_a0(vmPtr));
    const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, connect.fd);
    const supervisorCommitResult = requireSupervisorSocketCommitExternalConnectResult(handle)(
      vmPtr,
      connect.fd,
      result,
    );
    const markExternalPeerResult = supervisorCommitResult === 1 ? 0 : -1;
    const status = handle.kernel.thread_status(vmPtr);
    const shouldCommit = supervisorCommitResult === 1;
    if (shouldCommit) {
      const existing = handle.networkConnections.get(connect.fd);
      if (existing && existing !== connect.connection) {
        await existing.close?.();
        pendingNetworkInbound.delete(existing);
        pendingNetworkReads.delete(existing);
      }
      handle.networkClosedFds.delete(connect.fd);
      handle.networkClosedSocketSlots.delete(connect.fd);
      clearCurrentNetworkRecvBuffer(handle, vmPtr, connect.fd);
      clearNetworkRecvWatermarks(handle, connect.fd);
      handle.networkConnections.set(connect.fd, connect.connection);
      const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, connect.fd);
      networkConnectionOfds(handle).set(connect.fd, ofdId);
      rememberNetworkOfdSlot(handle, ofdId);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      const peerIdx = socketIdx !== undefined && socketIdx >= 0
        ? handle.kernel.thread_socket_peer_idx?.(socketIdx)
        : undefined;
      const socketState = socketIdx !== undefined && socketIdx >= 0
        ? handle.kernel.thread_socket_state?.(socketIdx)
        : undefined;
      const externalPeer = socketIdx !== undefined && socketIdx >= 0
        ? handle.kernel.thread_socket_external_peer?.(socketIdx)
        : undefined;
      logNetworkDebug(`connect-commit pid=${handle.pid} fd=${connect.fd} result=${result.toString()} status=${status} mark=${markExternalPeerResult ?? "n/a"} sock=${socketIdx ?? "n/a"} peer=${peerIdx ?? "n/a"} state=${socketState ?? "n/a"} external=${externalPeer ?? "n/a"}`);
      return;
    }
    logNetworkDebug(`connect-drop pid=${handle.pid} fd=${connect.fd} result=${result.toString()} status=${status} mark=${markExternalPeerResult ?? "n/a"}`);
    if (handle.networkConnections.get(connect.fd) === connect.connection) {
      handle.networkConnections.delete(connect.fd);
    }
    await connect.connection.close?.();
  }

  async function pumpNetworkConnections(handle: ProcessHandle, vmPtr = handle.vmPtr): Promise<boolean> {
    const pumpStartedAtMs = Date.now();
    ensureNetworkBookkeeping(handle);
    handle.transferDebugStats.networkPumpCalls += 1;
    const drainOutbound = handle.kernel.thread_socket_drain_outbound;
    const injectInbound = handle.kernel.thread_socket_inject_inbound;
    const prepareExternalPeer = requireSupervisorSocketPrepareExternalPeer(handle);
    const closeExternalPeer = requireSupervisorSocketCloseExternalPeer(handle);
    let changed = false;
    const finishPump = (): boolean => {
      if (changed) {
        handle.transferDebugStats.networkPumpChangedCalls += 1;
      }
      handle.transferDebugStats.networkPumpElapsedMs += Date.now() - pumpStartedAtMs;
      return changed;
    };
    if (!drainOutbound || !injectInbound || handle.networkConnections.size === 0) {
      return finishPump();
    }
    const scratch = kernelScratchBase(handle.kernel) + 0x24000;
    let chunkSize = NETWORK_BRIDGE_CHUNK_SIZE;
    try {
      ensureMemoryCapacity(handle.kernel.memory ?? handle.memory, scratch + chunkSize);
    } catch (error) {
      if (!(error instanceof RangeError)) {
        throw error;
      }
      chunkSize = 16 * 1024;
      ensureMemoryCapacity(handle.kernel.memory ?? handle.memory, scratch + chunkSize);
    }
    const memory = new Uint8Array(handle.memory.buffer);
    const closeNetworkConnection = async (
      fd: number,
      connection: RuntimeNetworkConnection,
      reason: string,
      forceFd = true,
    ): Promise<void> => {
      const beforeClose = formatSocketStateForFd(fd);
      if (forceFd) {
        forceNetworkClosedFd(handle, vmPtr, fd);
        snapshotClosedNetworkSocketSlot(fd);
      }
      const afterClose = formatSocketStateForFd(fd);
      pendingNetworkInbound.delete(connection);
      pendingNetworkReads.delete(connection);
      pendingNetworkEof.delete(connection);
      networkPeerEof.delete(connection);
      suppressNetworkDrainAfterInbound.delete(connection);
      networkDrainMisses.delete(connection);
      networkEofDeferrals.delete(connection);
      networkConnectionOfds(handle).delete(fd);
      handle.networkConnections.delete(fd);
      if (!hasPendingConnectForFd(fd)) {
        handle.networkPendingConnectFds.delete(fd);
      }
      clearNetworkRecvWatermarks(handle, fd);
      if (forceFd) {
        handle.networkClosedFds.add(fd);
      }
      changed = true;
      await connection.close?.();
      deps.appendWorkerDebugLog(handle, `net-close:${handle.pid}:fd=${fd}:${reason}:before=${beforeClose}:after=${afterClose}`);
      appendNetworkLifecycle(handle, `net-close:${handle.pid}:fd=${fd}:${reason}:force=${forceFd ? 1 : 0}:before=${beforeClose}:after=${afterClose}`);
      logNetworkDebug(`close pid=${handle.pid} fd=${fd} ${reason}`);
      const fdTrace = formatNetworkFdTrace(handle);
      if (fdTrace) {
        logNetworkDebug(`fd-trace pid=${handle.pid} ${fdTrace}`);
      }
      changed = true;
    };
    const closeNetworkPeerEof = async (
      fd: number,
      connection: RuntimeNetworkConnection,
      reason: string,
    ): Promise<void> => {
      if (networkPeerEof.has(connection)) {
        return;
      }
      const beforeClose = formatSocketStateForFd(fd);
      closeExternalPeer?.(vmPtr, fd);
      clearCurrentNetworkRecvBuffer(handle, vmPtr, fd);
      const closedSlot = snapshotNetworkSocketSlot(handle, vmPtr, fd);
      if (closedSlot !== null) {
        handle.networkClosedSocketSlots.set(fd, closedSlot);
      }
      const afterClose = formatSocketStateForFd(fd);
      pendingNetworkInbound.delete(connection);
      pendingNetworkReads.delete(connection);
      pendingNetworkEof.delete(connection);
      suppressNetworkDrainAfterInbound.delete(connection);
      networkDrainMisses.delete(connection);
      networkEofDeferrals.delete(connection);
      networkPeerEof.add(connection);
      changed = true;
      deps.appendWorkerDebugLog(handle, `net-peer-eof:${handle.pid}:fd=${fd}:${reason}:before=${beforeClose}:after=${afterClose}`);
      appendNetworkLifecycle(handle, `net-peer-eof:${handle.pid}:fd=${fd}:${reason}:before=${beforeClose}:after=${afterClose}`);
      await connection.close?.();
    };
    const enotsock = handle.kernel.abi_enotsock?.() ?? -88;
    const enotconn = handle.kernel.abi_enotconn?.() ?? -107;

    const findFdForOfd = (ofdId: number, preferredFd: number): {
      fd: number | null;
      supervisor: boolean;
    } => {
      if (ofdId <= 0) {
        return { fd: null, supervisor: false };
      }
      const supervisorActiveFd = requireSupervisorSocketBridgeActiveFd(handle)(
        vmPtr,
        preferredFd,
        ofdId,
        hasPendingConnectForFd(preferredFd) ? 1 : 0,
      );
      return {
        fd: supervisorActiveFd >= 0 ? supervisorActiveFd : null,
        supervisor: true,
      };
    };

    const rekeyNetworkConnection = (
      fromFd: number,
      toFd: number,
      ofdId: number,
      connection: RuntimeNetworkConnection,
    ): void => {
      const ofds = networkConnectionOfds(handle);
      if (fromFd === toFd) {
        ofds?.set(toFd, ofdId);
        deps.appendWorkerDebugLog(handle, `net-rekey:${handle.pid}:fd=${fromFd}:ofd=${ofdId}`);
        changed = true;
        return;
      }
      handle.networkConnections.delete(fromFd);
      handle.networkConnections.set(toFd, connection);
      ofds?.delete(fromFd);
      ofds?.set(toFd, ofdId);
      moveNetworkRecvWatermarks(handle, fromFd, toFd);
      deps.appendWorkerDebugLog(handle, `net-rekey:${handle.pid}:fd=${fromFd}->${toFd}:ofd=${ofdId}`);
      changed = true;
    };

    const hasPendingConnectForFd = (fd: number): boolean => {
      const pending = pendingNetworkConnects.get(handle);
      if (!pending) {
        return false;
      }
      for (const connect of pending.values()) {
        if (connect.fd === fd) {
          return true;
        }
      }
      return false;
    };

    const socketDrainCandidates = (connectedFd: number): number[] => {
      const candidates = [connectedFd];
      const connectedOfd = handle.kernel.process_fd_ofd_id(vmPtr, connectedFd);
      if (connectedOfd <= 0) {
        return candidates;
      }
      const maxFds = handle.kernel.abi_max_fds();
      for (let fd = 0; fd < maxFds; fd += 1) {
        if (fd === connectedFd) {
          continue;
        }
        const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
        if (
          ofdId > 0
          && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_socket()
          && ofdId === connectedOfd
        ) {
          candidates.push(fd);
        }
      }
      return candidates;
    };

    const pendingOutboundAvailable = (connectedFd: number): number => {
      let total = 0;
      for (const fd of socketDrainCandidates(connectedFd)) {
        const available = handle.kernel.thread_socket_outbound_available?.(vmPtr, fd);
        if (available !== undefined && available > 0) {
          total += available;
        }
      }
      return total;
    };

    const hasPendingInbound = (connection: RuntimeNetworkConnection): boolean =>
      pendingNetworkInbound.has(connection) || pendingNetworkReads.has(connection);

    const formatSocketStateForFd = (fd: number): string => {
      const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd);
      const peerIdx = socketIdx !== undefined && socketIdx >= 0
        ? handle.kernel.thread_socket_peer_idx?.(socketIdx)
        : undefined;
      const state = socketIdx !== undefined && socketIdx >= 0 ? handle.kernel.thread_socket_state?.(socketIdx) ?? "n/a" : "n/a";
      const type = socketIdx !== undefined && socketIdx >= 0 ? handle.kernel.thread_socket_type?.(socketIdx) ?? "n/a" : "n/a";
      const ext = socketIdx !== undefined && socketIdx >= 0 ? handle.kernel.thread_socket_external_peer?.(socketIdx) ?? "n/a" : "n/a";
      const buf = socketIdx !== undefined && socketIdx >= 0 ? handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? "n/a" : "n/a";
      const recv = socketIdx !== undefined && socketIdx >= 0 ? handle.kernel.thread_socket_recv_available?.(socketIdx) ?? "n/a" : "n/a";
      const readDebug = socketIdx !== undefined && socketIdx >= 0
        ? [
          handle.kernel.thread_socket_read_count?.(socketIdx) ?? -1,
          handle.kernel.thread_socket_read_bytes?.(socketIdx)?.toString() ?? "-1",
          handle.kernel.thread_socket_read_last_fd?.(socketIdx) ?? -1,
          handle.kernel.thread_socket_read_last_result?.(socketIdx)?.toString() ?? "-1",
          handle.kernel.thread_socket_read_last_before?.(socketIdx) ?? -1,
          handle.kernel.thread_socket_read_last_after?.(socketIdx) ?? -1,
        ].join("/")
        : "-1";
      recordProcessIoSocketMetaTransition(handle, fd, socketIdx, state, type, ext, buf, recv);
      return [
        `sock=${socketIdx ?? "n/a"}`,
        `state=${state}`,
        `type=${type}`,
        `ext=${ext}`,
        `buf=${buf}`,
        `recv=${recv}`,
        `read=${readDebug}`,
        `peer=${peerIdx ?? "n/a"}`,
        `peer_buf=${peerIdx !== undefined && peerIdx >= 0 ? handle.kernel.thread_socket_recv_buf_idx?.(peerIdx) ?? "n/a" : "n/a"}`,
        `peer_recv=${peerIdx !== undefined && peerIdx >= 0 ? handle.kernel.thread_socket_recv_available?.(peerIdx) ?? "n/a" : "n/a"}`,
      ].join(":");
    };

    const snapshotClosedNetworkSocketSlot = (fd: number): void => {
      const slot = snapshotNetworkSocketSlot(handle, vmPtr, fd);
      if (slot === null) {
        return;
      }
      handle.networkClosedSocketSlots.set(fd, slot);
    };

    const repairExternalPeerForFd = (fd: number, reason: string): boolean => {
      const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd);
      const socketState = socketIdx !== undefined && socketIdx >= 0
        ? handle.kernel.thread_socket_state?.(socketIdx)
        : undefined;
      if (socketState === SOCKET_STATE_CONNECTED || socketState === SOCKET_STATE_SHUTDOWN) {
        return false;
      }
      const before = formatSocketStateForFd(fd);
      const markResult = prepareExternalPeer?.(vmPtr, fd);
      if (markResult !== 0) {
        return false;
      }
      const after = formatSocketStateForFd(fd);
      deps.appendWorkerDebugLog(handle, `net-peer-repair:${handle.pid}:fd=${fd}:${reason}:before=${before}:after=${after}`);
      appendNetworkLifecycle(handle, `net-peer-repair:${handle.pid}:fd=${fd}:${reason}:before=${before}:after=${after}`);
      changed = true;
      return true;
    };

    const pumpInbound = async (
      fd: number,
      connection: RuntimeNetworkConnection,
    ): Promise<"closed" | "idle" | "blocked" | "changed"> => {
      for (;;) {
        if (networkPeerEof.has(connection)) {
          return "idle";
        }
        if (pendingNetworkEof.has(connection)) {
          const pendingInbound = pendingNetworkInbound.get(connection);
          if (!pendingInbound || pendingInbound.length === 0) {
            const restoredEofWatermark = restoreBufferedNetworkRecvFromWatermark(
              handle,
              vmPtr,
              fd,
              { allowPartialReadProgress: true },
            );
            if (restoredEofWatermark || hasCurrentNetworkRecvAvailable(handle, vmPtr, fd)) {
              return "idle";
            }
            const eofDeferrals = networkEofDeferrals.get(connection) ?? 0;
            if (suppressNetworkDrainAfterInbound.has(connection) && eofDeferrals < 1) {
              networkEofDeferrals.set(connection, eofDeferrals + 1);
              return "idle";
            }
            pendingNetworkEof.delete(connection);
            networkEofDeferrals.delete(connection);
            clearNetworkRecvWatermarks(handle, fd);
            await closeNetworkPeerEof(fd, connection, "eof");
            return "closed";
          }
        }
        const inbound = pendingNetworkInbound.has(connection)
          ? pendingNetworkInbound.get(connection) ?? null
          : readConnectionNow(handle, vmPtr, fd, connection, chunkSize);
        if (inbound === null) {
          const restoredEofWatermark = restoreBufferedNetworkRecvFromWatermark(
            handle,
            vmPtr,
            fd,
            { allowPartialReadProgress: true },
          );
          if (
            restoredEofWatermark
            || suppressNetworkDrainAfterInbound.has(connection)
            || hasCurrentNetworkRecvAvailable(handle, vmPtr, fd)
          ) {
            pendingNetworkEof.add(connection);
            networkEofDeferrals.set(connection, 0);
            return "idle";
          }
          clearNetworkRecvWatermarks(handle, fd);
          await closeNetworkPeerEof(fd, connection, "eof");
          return "closed";
        }
        if (inbound === undefined || inbound.length === 0) {
          return "idle";
        }
        const owned = ownedBytes(inbound);
        const injection = owned.length > chunkSize ? owned.subarray(0, chunkSize) : owned;
        deps.appendWorkerDebugLog(handle, `net-inbound:${handle.pid}:fd=${fd}:n=${injection.length}:pending=${owned.length - injection.length}:before=${formatSocketStateForFd(fd)}`);
        appendNetworkLifecycle(handle, `net-inbound:${handle.pid}:fd=${fd}:n=${injection.length}:pending=${owned.length - injection.length}:before=${formatSocketStateForFd(fd)}`);
        memory.set(injection, scratch);
        const inboundSocketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd);
        const inboundSocketState = inboundSocketIdx !== undefined && inboundSocketIdx >= 0
          ? handle.kernel.thread_socket_state?.(inboundSocketIdx)
          : undefined;
        if (inboundSocketState !== SOCKET_STATE_SHUTDOWN) {
          prepareExternalPeer?.(vmPtr, fd);
        }
        let written = injectInbound(vmPtr, fd, scratch, injection.length);
        if (
          written === enotconn
          && inboundSocketState !== SOCKET_STATE_SHUTDOWN
          && prepareExternalPeer?.(vmPtr, fd) === 0
        ) {
            written = injectInbound(vmPtr, fd, scratch, injection.length);
        }
        deps.appendWorkerDebugLog(handle, `net-inject-result:${handle.pid}:fd=${fd}:written=${written}:after=${formatSocketStateForFd(fd)}`);
        appendNetworkLifecycle(handle, `net-inject-result:${handle.pid}:fd=${fd}:written=${written}:after=${formatSocketStateForFd(fd)}`);
        if (written > 0) {
          handle.transferDebugStats.networkInboundInjectCalls += 1;
          handle.transferDebugStats.networkInboundInjectedBytes += written;
          rememberNetworkOfdSlot(handle, handle.kernel.process_fd_ofd_id(vmPtr, fd));
          rememberNetworkRecvPayload(handle, vmPtr, fd, written);
          networkDrainMisses.delete(connection);
          networkEofDeferrals.delete(connection);
          suppressNetworkDrainAfterInbound.add(connection);
          deps.appendWorkerDebugLog(handle, `net-inject:${handle.pid}:fd=${fd}:n=${written}`);
          logNetworkDebug(`inject pid=${handle.pid} fd=${fd} n=${written}`);
          changed = true;
        }
        if (written > 0 && written < owned.length) {
          setPendingNetworkInbound(handle, connection, owned.subarray(written));
          appendNetworkLifecycle(handle, `net-inject-partial:${handle.pid}:fd=${fd}:written=${written}:pending=${owned.length - written}`);
          logNetworkDebug(`inject-partial pid=${handle.pid} fd=${fd} written=${written} pending=${owned.length - written}`);
          return "changed";
        }
        if (written === enotsock || written === enotconn) {
          deps.appendWorkerDebugLog(handle, `net-inject:${handle.pid}:fd=${fd}:closed=${written}:pending=${owned.length}`);
          appendNetworkLifecycle(handle, `net-inject-closed:${handle.pid}:fd=${fd}:written=${written}:pending=${owned.length}`);
          logNetworkDebug(`inject-closed pid=${handle.pid} fd=${fd} written=${written} pending=${owned.length}`);
          await closeNetworkConnection(
            fd,
            connection,
            written === enotsock ? "inject-not-socket" : "inject-not-connected",
            !hasPendingConnectForFd(fd),
          );
          return "closed";
        }
        if (written <= 0) {
          handle.transferDebugStats.networkInboundBlockedCalls += 1;
          deps.appendWorkerDebugLog(handle, `net-inject:${handle.pid}:fd=${fd}:blocked=${written}:pending=${owned.length}`);
          appendNetworkLifecycle(handle, `net-inject-blocked:${handle.pid}:fd=${fd}:written=${written}:pending=${owned.length}`);
          logNetworkDebug(`inject-blocked pid=${handle.pid} fd=${fd} written=${written} pending=${owned.length}`);
          setPendingNetworkInbound(handle, connection, owned);
          return "blocked";
        }
        pendingNetworkInbound.delete(connection);
      }
    };

    for (const [fd, connection] of [...handle.networkConnections]) {
      let activeFd = fd;
      const expectedOfd = networkConnectionOfds(handle).get(fd);
      const currentOfd = handle.kernel.process_fd_ofd_id(vmPtr, fd);
      if (expectedOfd !== undefined && currentOfd !== expectedOfd) {
        const replacement = findFdForOfd(expectedOfd, fd);
        if (replacement.fd === null) {
          if (
            !replacement.supervisor
            && (
              currentOfd > 0
              && handle.kernel.kernel_ofd_fd_type(currentOfd) === handle.kernel.abi_fd_type_socket()
              && !hasPendingConnectForFd(fd)
            )
          ) {
            rekeyNetworkConnection(fd, fd, currentOfd, connection);
            activeFd = fd;
          } else {
            await closeNetworkConnection(fd, connection, `stale-ofd:${expectedOfd}->${currentOfd}`, false);
            continue;
          }
        }
        if (replacement.fd !== null) {
          rekeyNetworkConnection(fd, replacement.fd, expectedOfd, connection);
          activeFd = replacement.fd;
        }
      }
      recordGuestSocketReadProgressForFd(handle, vmPtr, activeFd);
      const initialInbound = await pumpInbound(activeFd, connection);
      recordGuestSocketReadProgressForFd(handle, vmPtr, activeFd);
      if (initialInbound === "closed") {
        continue;
      }
      if (initialInbound === "changed") {
        continue;
      }
      if (networkPeerEof.has(connection)) {
        continue;
      }
      if (suppressNetworkDrainAfterInbound.has(connection)) {
        const bufferedInbound = hasBufferedNetworkRecv(handle, vmPtr, activeFd);
        const pendingRead = pendingNetworkReads.has(connection);
        const pendingInbound = pendingNetworkInbound.has(connection) || pendingNetworkEof.has(connection);
        if (bufferedInbound || pendingRead || pendingInbound) {
          if (!bufferedInbound) {
            repairExternalPeerForFd(activeFd, "buffered-inbound");
          }
          deps.appendWorkerDebugLog(handle, `net-drain-suppressed:${handle.pid}:fd=${activeFd}`);
          continue;
        }
        const outboundAvailable = pendingOutboundAvailable(activeFd);
        if (outboundAvailable > 0) {
          deps.appendWorkerDebugLog(handle, `net-drain-unsuppressed:${handle.pid}:fd=${activeFd}:out=${outboundAvailable}`);
        }
        suppressNetworkDrainAfterInbound.delete(connection);
      }
      for (const drainFd of socketDrainCandidates(activeFd)) {
        for (;;) {
          const available = handle.kernel.thread_socket_outbound_available?.(vmPtr, drainFd);
          const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, drainFd);
          const peerIdx = socketIdx !== undefined && socketIdx >= 0
            ? handle.kernel.thread_socket_peer_idx?.(socketIdx)
            : undefined;
          const socketState = socketIdx !== undefined && socketIdx >= 0
            ? handle.kernel.thread_socket_state?.(socketIdx)
            : undefined;
          const externalPeer = socketIdx !== undefined && socketIdx >= 0
            ? handle.kernel.thread_socket_external_peer?.(socketIdx)
            : undefined;
          const socketRecvBuf = socketIdx !== undefined && socketIdx >= 0
            ? handle.kernel.thread_socket_recv_buf_idx?.(socketIdx)
            : undefined;
          const socketRecvAvail = socketIdx !== undefined && socketIdx >= 0
            ? handle.kernel.thread_socket_recv_available?.(socketIdx)
            : undefined;
          const peerRecvBuf = peerIdx !== undefined && peerIdx >= 0
            ? handle.kernel.thread_socket_recv_buf_idx?.(peerIdx)
            : undefined;
          const peerRecvAvail = peerIdx !== undefined && peerIdx >= 0
            ? handle.kernel.thread_socket_recv_available?.(peerIdx)
            : undefined;
          if (drainFd === activeFd) {
            const markResult = socketState === SOCKET_STATE_SHUTDOWN
              ? undefined
              : prepareExternalPeer?.(vmPtr, activeFd);
            const repairedExternalPeer = markResult === 0
              && (
                (socketState !== undefined && socketState !== SOCKET_STATE_CONNECTED)
                || (externalPeer !== undefined && externalPeer !== 1)
              );
            if (repairedExternalPeer) {
              changed = true;
              logNetworkDebug(`mark-repair pid=${handle.pid} fd=${activeFd} state=${socketState ?? "n/a"} external=${externalPeer ?? "n/a"}`);
            } else if (markResult !== undefined && markResult !== 0) {
              const state = readKernelRuntimeState(handle.kernel);
              logNetworkDebug(`mark pid=${handle.pid} fd=${activeFd} result=${markResult} state=${socketState ?? "n/a"} external=${externalPeer ?? "n/a"} socketUsed=0x${state.socketUsed.toString(16)} netBufUsed=0x${state.netBufUsed.toString(16)}`);
            }
          }
          let n = drainOutbound(vmPtr, drainFd, scratch, chunkSize);
          if (
            drainFd === activeFd
            && n === enotconn
            && socketState !== SOCKET_STATE_SHUTDOWN
            && prepareExternalPeer?.(vmPtr, activeFd) === 0
          ) {
            n = drainOutbound(vmPtr, drainFd, scratch, chunkSize);
          }
          if (n <= 0) {
            deps.appendWorkerDebugLog(handle, `net-drain:${handle.pid}:fd=${drainFd}:n=${n}:avail=${available ?? "n/a"}:sock=${socketIdx ?? "n/a"}:peer=${peerIdx ?? "n/a"}`);
            appendNetworkLifecycle(handle, `net-drain-empty:${handle.pid}:fd=${drainFd}:n=${n}:avail=${available ?? "n/a"}:sock=${socketIdx ?? "n/a"}:peer=${peerIdx ?? "n/a"}`);
            logNetworkDebug(`drain pid=${handle.pid} fd=${drainFd} n=${n} avail=${available ?? "n/a"} sock=${socketIdx ?? "n/a"} peer=${peerIdx ?? "n/a"} state=${socketState ?? "n/a"} external=${externalPeer ?? "n/a"} sockRecv=${socketRecvBuf ?? "n/a"}:${socketRecvAvail ?? "n/a"} peerRecv=${peerRecvBuf ?? "n/a"}:${peerRecvAvail ?? "n/a"}`);
            const fdTrace = drainFd === activeFd ? formatNetworkFdTrace(handle) : "";
            if (fdTrace) {
              logNetworkDebug(`fd-trace pid=${handle.pid} ${fdTrace}`);
            }
            if (drainFd === activeFd && (n === enotsock || n === enotconn) && !hasPendingInbound(connection)) {
              if (hasPendingConnectForFd(activeFd)) {
                await closeNetworkConnection(activeFd, connection, n === enotsock ? "pending-connect-not-socket" : "pending-connect-not-connected", false);
                break;
              }
              if (n === enotconn) {
                const misses = (networkDrainMisses.get(connection) ?? 0) + 1;
                networkDrainMisses.set(connection, misses);
                if (misses <= 4) {
                  logNetworkDebug(`drain-defer pid=${handle.pid} fd=${activeFd} misses=${misses}`);
                  break;
                }
              }
              await closeNetworkConnection(activeFd, connection, n === enotsock ? "not-socket" : "not-connected");
            }
            break;
          }
          const outbound = new Uint8Array(handle.memory.buffer.slice(scratch, scratch + n));
          await connection.write(outbound);
          handle.transferDebugStats.networkOutboundDrainCalls += 1;
          handle.transferDebugStats.networkOutboundDrainedBytes += n;
          await Promise.resolve();
          if (pendingNetworkReads.has(connection)) {
            retryNetworkReadAfterPending.add(connection);
          }
          networkDrainMisses.delete(connection);
          deps.appendWorkerDebugLog(handle, `net-drain:${handle.pid}:fd=${drainFd}:n=${n}:write`);
          appendNetworkLifecycle(handle, `net-drain-write:${handle.pid}:fd=${drainFd}:n=${n}`);
          logNetworkDebug(`drain pid=${handle.pid} fd=${drainFd} n=${n} write`);
          changed = true;
          break;
        }
      }
      await pumpInbound(activeFd, connection);
      recordGuestSocketReadProgressForFd(handle, vmPtr, activeFd);
    }
    return finishPump();
  }

  return {
    commitNetworkConnect,
    injectHttpRequest,
    pumpNetworkConnections,
    resolveNetworkConnect,
    writeProcessStdin,
  };
}
