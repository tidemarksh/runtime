import { attachKernelRuntimeState, readKernelRuntimeState, type KernelRuntimeState } from "../abi.js";
import {
  NET_BUFFER_HEAD_OFFSET,
  NET_BUFFER_HEADER_BYTES,
  NET_BUFFER_TAIL_OFFSET,
} from "../layout/net-buffer.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_TYPE_OFFSET,
} from "../layout/ofd.js";
import {
  PIPE_GENERATION_OFFSET as PIPE_DEBUG_GENERATION_OFFSET,
  PIPE_HEAD_OFFSET as PIPE_DEBUG_HEAD_OFFSET,
  PIPE_HEADER_BYTES as PIPE_DEBUG_HEADER_BYTES,
  PIPE_READ_OPEN_OFFSET as PIPE_DEBUG_READ_OPEN_OFFSET,
  PIPE_SLOT_MODE_CONTROL,
  PIPE_SLOT_MODE_READ,
  PIPE_SLOT_MODE_WRITE,
  PIPE_TAIL_OFFSET as PIPE_DEBUG_TAIL_OFFSET,
  PIPE_WRITE_OPEN_OFFSET as PIPE_DEBUG_WRITE_OPEN_OFFSET,
} from "../layout/pipe.js";
import { SOCKET_STATE_SHUTDOWN } from "../layout/socket.js";
import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPipeSlotSnapshot,
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPidStateSnapshot,
  KernelWorkerSocketStateSnapshot,
} from "../messages.js";
import {
  copyLegacySharedKernelObjectsLocal,
  kernelShareOtherMembers,
  reconcileKernelSharePipeControl,
  restoreLocalOfdSlots,
} from "./kernel-share.js";
import {
  mergeKernelShareOfdStateInto,
  pruneStaleKernelShareOfdIds,
  reconcileForkSharedOfdRefcounts,
  snapshotKernelShareOfdSlots,
  syncKernelShareOfdReservations,
} from "./fork-shared.js";
import {
  activeKernelShareOfdIds,
  collectHeldEpollOfdIds,
  collectProcessHeldOfds,
  restoreIncompatibleLocalOfdSlots,
  restoreRuntimeNetworkOfdSlots,
  snapshotLocalOfdSlots,
  snapshotRuntimeNetworkOfdSlots,
  staleKernelShareOfdIds,
  syncPipeOfdGenerationsForSlot,
} from "./ownership/ofd-table.js";
import {
  isRegularFileOfd,
  sourceForkSharedOfdPeerState,
  sourceOfdCanPublishForkSharedState,
  targetHasIncompatibleForkSharedOfd,
  targetHoldsSyncedRegularFileOfd,
  trackHeldForkSharedOfds,
} from "./ownership/ofd-peer.js";
import {
  ofdIdsFromFdEntries,
  restoreProcessFdEntries,
  snapshotProcessFdEntries,
} from "./ownership/fd-table.js";
import {
  restoreProcessState,
  restoreProcessStates,
  snapshotProcessState,
  snapshotProcessStates,
} from "./ownership/process-state.js";
import {
  composeHandlePipeUsed,
  expandPipePeerOfdIds,
  hasPipePeerInOtherKernelShareMember,
  isProcessHostStdioPipeOfd,
  liveProcessPipeUsedBits,
  preserveHostStdioPipeControlForStep,
} from "./ownership/pipe-control.js";
import {
  collectRuntimeNetworkSocketOfdIds,
  hasRuntimeNetworkSocketState,
  runtimeNetworkFds,
} from "./ownership/socket-control.js";
import type { ProcessHandle } from "./types.js";
import {
  forceNetworkClosedFds,
  networkSocketSlotRecvBufferIndex,
  restoreNetworkSocketSlotSnapshot,
  snapshotNetworkSocketSlot,
} from "./network-socket-close.js";

const lastForkSharedActiveOfdIdsByHandle = new WeakMap<ProcessHandle, Set<number>>();
const SOCKET_SYNC_DEBUG_LOG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_WORKER_DEBUG_LOG ?? "") === "1"
  || ((globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_WORKER_DEBUG_NETWORK ?? "") === "1";
const FORK_SHARED_MEMFS_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_PUBLICATION_DEBUG ?? "") === "1";
const SOCK_STATE_SHUTDOWN = SOCKET_STATE_SHUTDOWN;

function int32AtomicView(buffer: ArrayBufferLike): Int32Array | null {
  return buffer instanceof SharedArrayBuffer ? new Int32Array(buffer) : null;
}

function loadPipeU32(view: DataView, atomicView: Int32Array | null, byteOffset: number): number {
  if (atomicView !== null && byteOffset % 4 === 0) {
    return Atomics.load(atomicView, byteOffset / 4) >>> 0;
  }
  return view.getUint32(byteOffset, true);
}

function loadPipeI32(view: DataView, atomicView: Int32Array | null, byteOffset: number): number {
  if (atomicView !== null && byteOffset % 4 === 0) {
    return Atomics.load(atomicView, byteOffset / 4);
  }
  return view.getInt32(byteOffset, true);
}

interface NetworkBridgeSocketStateSnapshot {
  handle: ProcessHandle;
  connectionFds: number[];
  pendingConnectFds: number[];
  closedFds: number[];
  fdStats: Array<{
    fd: number;
    state: number;
    type: number;
    ext: number;
    recvAvailable: number;
    readCount: number;
  }>;
  socketTable: Uint8Array;
  netBufPoolBytes: number;
  netBufSlots: Array<{
    bufferIndex: number;
    bytes: Uint8Array;
  }>;
  ofdSlots: Array<{
    ofdId: number;
    bytes: Uint8Array;
  }>;
  socketUsed: number;
  netBufUsed: bigint;
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

interface PreservedRuntimeNetworkKernelState {
  handle: ProcessHandle;
  vmPtr: number;
  ofdSlots: Map<number, Uint8Array>;
  fdEntries: KernelWorkerFdEntrySnapshot[];
}

function appendSocketSyncDebug(handle: ProcessHandle, event: string): void {
  if (!SOCKET_SYNC_DEBUG_LOG_ENABLED) {
    return;
  }
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
}

function appendForkSharedMemfsDebug(handle: ProcessHandle, event: string): void {
  if (!FORK_SHARED_MEMFS_DEBUG_ENABLED) {
    return;
  }
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
}

const networkSocketMetaDebug = new WeakMap<ProcessHandle, Map<number, string>>();

function recordNetworkSocketMetaTransition(
  handle: ProcessHandle,
  source: string,
  fd: number,
  socketIdx: number,
  state: number,
  type: number,
  ext: number,
  buf: number,
  recv: number,
): void {
  if (!SOCKET_SYNC_DEBUG_LOG_ENABLED) {
    return;
  }
  let byFd = networkSocketMetaDebug.get(handle);
  if (byFd === undefined) {
    byFd = new Map();
    networkSocketMetaDebug.set(handle, byFd);
  }
  const next = `${socketIdx}/${state}/${type}/${ext}/${buf}/${recv}`;
  const previous = byFd.get(fd);
  if (previous === next) {
    return;
  }
  byFd.set(fd, next);
  appendSocketSyncDebug(handle, `socket-meta-transition:${handle.pid}:source=${source}:fd=${fd}:prev=${previous ?? "none"}:next=${next}`);
}

function formatPipeSnapshotDebug(
  label: string,
  snapshots: readonly KernelWorkerPipeSlotSnapshot[],
  limit = 8,
): string {
  if (snapshots.length === 0) {
    return `${label}=none`;
  }
  return `${label}=${snapshots.slice(0, limit).map((snapshot) => {
    const view = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
    const data = snapshot.slot.subarray(PIPE_DEBUG_HEADER_BYTES, PIPE_DEBUG_HEADER_BYTES + 24);
    const ascii = [...data]
      .map((byte) => byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : ".")
      .join("");
    return [
      `slot${snapshot.pipeSlot}`,
      `m${snapshot.mode ?? -1}`,
      `f${snapshot.fresh ? 1 : 0}`,
      `h${view.getUint32(PIPE_DEBUG_HEAD_OFFSET, true)}`,
      `t${view.getUint32(PIPE_DEBUG_TAIL_OFFSET, true)}`,
      `ro${view.getInt32(PIPE_DEBUG_READ_OPEN_OFFSET, true)}`,
      `wo${view.getInt32(PIPE_DEBUG_WRITE_OPEN_OFFSET, true)}`,
      `g${view.getUint32(PIPE_DEBUG_GENERATION_OFFSET, true)}`,
      `d=${JSON.stringify(ascii)}`,
    ].join("/");
  }).join(",")}${snapshots.length > limit ? `,+${snapshots.length - limit}` : ""}`;
}

function formatFdSnapshotDebug(
  label: string,
  snapshots: readonly KernelWorkerFdEntrySnapshot[],
  fds: readonly number[] = [0, 1, 2],
): string {
  const byFd = new Map(snapshots.map((snapshot) => [snapshot.fd, snapshot]));
  return `${label}=${fds.map((fd) => {
    const snapshot = byFd.get(fd);
    return snapshot === undefined
      ? `${fd}->missing`
      : `${fd}->${snapshot.ofdId}/${snapshot.fdFlags}`;
  }).join(",")}`;
}

function formatOfdSnapshotDebug(
  label: string,
  snapshots: readonly KernelWorkerOfdSlotSnapshot[],
  limit = 12,
): string {
  if (snapshots.length === 0) {
    return `${label}=none`;
  }
  return `${label}=${snapshots.slice(0, limit).map((snapshot) => {
    const view = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
    const offset = snapshot.slot.byteLength >= OFD_OFFSET_OFFSET + 8
      ? view.getBigUint64(OFD_OFFSET_OFFSET, true)
      : 0n;
    return [
      `ofd${snapshot.ofdId}`,
      `t${view.getUint32(OFD_TYPE_OFFSET, true)}`,
      `h${view.getInt32(OFD_HOST_FD_OFFSET, true)}`,
      `o${offset.toString()}`,
      `i${view.getUint32(OFD_INO_OFFSET, true)}`,
    ].join("/");
  }).join(",")}${snapshots.length > limit ? `,+${snapshots.length - limit}` : ""}`;
}

function formatOfdIdList(ofdIds: readonly number[]): string {
  return ofdIds.length > 0 ? ofdIds.join(".") : "none";
}

function readNetworkRecvBufferDebug(handle: ProcessHandle, socketIdx: number): string {
  if (socketIdx < 0) {
    return "-1/-1";
  }
  const recvBufIdx = handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1;
  if (recvBufIdx < 0) {
    return "-1/-1";
  }
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? handle.kernel.kernel_net_buf_pool_bytes() / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return "-1/-1";
  }
  const absolute = handle.kernelState.netBufBase + recvBufIdx * netBufferBytes;
  const view = new DataView(handle.memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
  return `${view.getUint32(NET_BUFFER_HEAD_OFFSET, true)}/${view.getUint32(NET_BUFFER_TAIL_OFFSET, true)}`;
}

function mergePipeSlotSnapshotsIntoHandle(
  target: ProcessHandle,
  snapshots: readonly KernelWorkerPipeSlotSnapshot[],
): void {
  if (snapshots.length === 0) {
    return;
  }
  const pipeRegionBytes = target.kernel.kernel_pipe_region_bytes();
  const maxPipes = target.kernel.abi_max_pipes();
  const slotBytes = pipeRegionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= PIPE_DEBUG_HEADER_BYTES) {
    return;
  }
  const pipeBufferBytes = slotBytes - PIPE_DEBUG_HEADER_BYTES;
  const counterAhead = (sourceValue: number, targetValue: number): boolean =>
    sourceValue > targetValue
    || (
      sourceValue < targetValue
      && targetValue > pipeBufferBytes / 2
      && sourceValue < pipeBufferBytes / 2
    );
  const generationAhead = (sourceValue: number, targetValue: number): boolean =>
    targetValue === 0
    || sourceValue > targetValue
    || (
      sourceValue < targetValue
      && targetValue > 0x80000000
      && sourceValue < 0x80000000
    );
  const memory = new Uint8Array(target.memory.buffer);
  const view = new DataView(target.memory.buffer);
  for (const snapshot of snapshots) {
    if (
      snapshot.pipeSlot <= 0
      || snapshot.pipeSlot >= maxPipes
      || snapshot.slot.length !== slotBytes
    ) {
      continue;
    }
    const mode = snapshot.mode ?? (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE | PIPE_SLOT_MODE_CONTROL);
    const sourceView = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
    const sourceHead = sourceView.getUint32(PIPE_DEBUG_HEAD_OFFSET, true);
    const sourceTail = sourceView.getUint32(PIPE_DEBUG_TAIL_OFFSET, true);
    const sourceReadOpen = sourceView.getInt32(PIPE_DEBUG_READ_OPEN_OFFSET, true);
    const sourceWriteOpen = sourceView.getInt32(PIPE_DEBUG_WRITE_OPEN_OFFSET, true);
    const targetOffset = target.kernelState.pipeBase + snapshot.pipeSlot * slotBytes;
    let targetHead = view.getUint32(targetOffset + PIPE_DEBUG_HEAD_OFFSET, true);
    let targetTail = view.getUint32(targetOffset + PIPE_DEBUG_TAIL_OFFSET, true);
    const targetReadOpen = view.getInt32(targetOffset + PIPE_DEBUG_READ_OPEN_OFFSET, true);
    const targetWriteOpen = view.getInt32(targetOffset + PIPE_DEBUG_WRITE_OPEN_OFFSET, true);
    const sourceGeneration = sourceView.getUint32(PIPE_DEBUG_GENERATION_OFFSET, true);
    const targetGeneration = view.getUint32(targetOffset + PIPE_DEBUG_GENERATION_OFFSET, true);
    if (
      sourceGeneration !== 0
      && sourceGeneration !== targetGeneration
    ) {
      if (
        sourceHead === 0
        && sourceTail === 0
        && targetHead !== targetTail
        && snapshot.fresh !== true
        && !generationAhead(sourceGeneration, targetGeneration)
      ) {
        continue;
      }
      if (
        snapshot.fresh === true
        || generationAhead(sourceGeneration, targetGeneration)
        || (targetReadOpen <= 0 && targetWriteOpen <= 0)
      ) {
        memory.set(snapshot.slot, targetOffset);
      }
      syncPipeOfdGenerationsForSlot(target, snapshot.pipeSlot);
      continue;
    }
    if (
      snapshot.fresh === true
      && sourceHead === 0
      && sourceTail === 0
      && targetHead === targetTail
      && sourceReadOpen > 0
      && sourceWriteOpen > 0
      && (mode & (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE)) === (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE)
    ) {
      memory.set(snapshot.slot, targetOffset);
      syncPipeOfdGenerationsForSlot(target, snapshot.pipeSlot);
      continue;
    }
    if ((mode & PIPE_SLOT_MODE_CONTROL) !== 0) {
      view.setInt32(
        targetOffset + PIPE_DEBUG_READ_OPEN_OFFSET,
        sourceReadOpen,
        true,
      );
      view.setInt32(
        targetOffset + PIPE_DEBUG_WRITE_OPEN_OFFSET,
        sourceWriteOpen,
        true,
      );
    }
    if (
      (mode & PIPE_SLOT_MODE_READ) !== 0
      && (mode & PIPE_SLOT_MODE_WRITE) === 0
      && sourceHead !== 0
      && sourceHead === sourceTail
      && targetTail !== sourceTail
      && counterAhead(sourceTail, targetTail)
    ) {
      view.setUint32(targetOffset + PIPE_DEBUG_HEAD_OFFSET, sourceHead, true);
      view.setUint32(targetOffset + PIPE_DEBUG_TAIL_OFFSET, sourceTail, true);
      targetHead = sourceHead;
      targetTail = sourceTail;
    }
    if (
      (mode & PIPE_SLOT_MODE_WRITE) !== 0
      && (mode & PIPE_SLOT_MODE_READ) === 0
      && sourceHead === 0
      && sourceTail !== 0
      && targetHead !== 0
      && (targetHead === targetTail || targetTail === 0)
      && counterAhead(sourceTail, targetTail)
    ) {
      view.setUint32(targetOffset + PIPE_DEBUG_HEAD_OFFSET, 0, true);
      targetHead = 0;
    }
    let readHeadAdvanced = false;
    if ((mode & PIPE_SLOT_MODE_READ) !== 0) {
      if (
        counterAhead(sourceHead, targetHead)
        && (sourceHead !== sourceTail || targetTail === sourceTail)
      ) {
        view.setUint32(targetOffset + PIPE_DEBUG_HEAD_OFFSET, sourceHead, true);
        targetHead = sourceHead;
        readHeadAdvanced = true;
      }
      if (counterAhead(sourceTail, targetTail) || (readHeadAdvanced && sourceHead !== sourceTail)) {
        view.setUint32(targetOffset + PIPE_DEBUG_TAIL_OFFSET, sourceTail, true);
        memory.set(
          snapshot.slot.subarray(PIPE_DEBUG_HEADER_BYTES, slotBytes),
          targetOffset + PIPE_DEBUG_HEADER_BYTES,
        );
        targetTail = sourceTail;
      } else if (sourceTail === targetTail && sourceHead !== sourceTail) {
        if (!((mode & PIPE_SLOT_MODE_READ) !== 0 && counterAhead(targetHead, sourceHead))) {
          memory.set(
            snapshot.slot.subarray(PIPE_DEBUG_HEADER_BYTES, slotBytes),
            targetOffset + PIPE_DEBUG_HEADER_BYTES,
          );
        }
      }
    }
    if ((mode & PIPE_SLOT_MODE_WRITE) !== 0) {
      if (counterAhead(sourceTail, targetTail)) {
        view.setUint32(targetOffset + PIPE_DEBUG_TAIL_OFFSET, sourceTail, true);
        memory.set(
          snapshot.slot.subarray(PIPE_DEBUG_HEADER_BYTES, slotBytes),
          targetOffset + PIPE_DEBUG_HEADER_BYTES,
        );
      } else if (sourceTail === targetTail && sourceHead !== sourceTail) {
        if (!((mode & PIPE_SLOT_MODE_READ) !== 0 && counterAhead(targetHead, sourceHead))) {
          memory.set(
            snapshot.slot.subarray(PIPE_DEBUG_HEADER_BYTES, slotBytes),
            targetOffset + PIPE_DEBUG_HEADER_BYTES,
          );
        }
      }
    }
    syncPipeOfdGenerationsForSlot(target, snapshot.pipeSlot);
  }
}

function readOptionalTraceU64(value: bigint | number | undefined): bigint | null {
  if (value === undefined) {
    return null;
  }
  return typeof value === "bigint" ? value : BigInt(value);
}

function collectFreshPipe2Slots(
  handle: ProcessHandle,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[],
): Set<number> {
  const pipe2Syscall = readOptionalTraceU64(handle.kernel.abi_sys_pipe2?.());
  const traceLen = handle.kernel.debug_fd_syscall_trace_len?.() ?? 0;
  const freshOfdIds = new Set<number>();
  if (pipe2Syscall === null || traceLen <= 0) {
    return new Set();
  }
  for (let index = 0; index < traceLen; index += 1) {
    if (readOptionalTraceU64(handle.kernel.debug_fd_syscall_trace_nr?.(index)) !== pipe2Syscall) {
      continue;
    }
    const result = readOptionalTraceU64(handle.kernel.debug_fd_syscall_trace_result?.(index));
    if (result !== null && BigInt.asIntN(64, result) !== 0n) {
      continue;
    }
    const arg0OfdId = handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0;
    const arg2OfdId = handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? 0;
    if (arg0OfdId > 0) {
      freshOfdIds.add(arg0OfdId);
    }
    if (arg2OfdId > 0) {
      freshOfdIds.add(arg2OfdId);
    }
  }
  if (freshOfdIds.size === 0) {
    return new Set();
  }

  const liveOfdIds = new Set(
    fdEntries
      .map((entry) => entry.ofdId)
      .filter((ofdId) => ofdId > 0),
  );
  const freshPipeSlots = new Set<number>();
  for (const ofdId of freshOfdIds) {
    if (
      !liveOfdIds.has(ofdId)
      || ofdId >= handle.kernel.abi_max_fds()
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      continue;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    if (pipeSlot > 0 && pipeSlot < handle.kernel.abi_max_pipes()) {
      freshPipeSlots.add(pipeSlot);
    }
  }
  return freshPipeSlots;
}

function snapshotFreshDuplexPipeSlots(
  handle: ProcessHandle,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[],
  pendingSnapshots: readonly KernelWorkerPipeSlotSnapshot[],
): KernelWorkerPipeSlotSnapshot[] {
  const maxPipes = handle.kernel.abi_max_pipes();
  const pipeRegionBytes = handle.kernel.kernel_pipe_region_bytes();
  const slotBytes = pipeRegionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= PIPE_DEBUG_HEADER_BYTES) {
    return [];
  }
  const pendingNonEmptyFullSlots = new Set(
    pendingSnapshots
      .filter((snapshot) => {
        const mode = snapshot.mode ?? (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE | PIPE_SLOT_MODE_CONTROL);
        if ((mode & (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE)) !== (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE)) {
          return false;
        }
        const snapshotView = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
        return snapshotView.getUint32(PIPE_DEBUG_HEAD_OFFSET, true)
          !== snapshotView.getUint32(PIPE_DEBUG_TAIL_OFFSET, true);
      })
      .map((snapshot) => snapshot.pipeSlot),
  );
  const freshPipe2Slots = collectFreshPipe2Slots(handle, fdEntries);
  const slotModes = new Map<number, number>();
  for (const entry of fdEntries) {
    const ofdId = entry.ofdId;
    if (
      ofdId <= 0
      || ofdId >= handle.kernel.abi_max_fds()
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      continue;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    if (
      pipeSlot <= 0
      || pipeSlot >= maxPipes
      || (pendingNonEmptyFullSlots.has(pipeSlot) && !freshPipe2Slots.has(pipeSlot))
    ) {
      continue;
    }
    const mode = handle.kernel.kernel_ofd_host_fd(ofdId) >= 0
      ? PIPE_SLOT_MODE_READ
      : PIPE_SLOT_MODE_WRITE;
    slotModes.set(pipeSlot, (slotModes.get(pipeSlot) ?? 0) | mode);
  }
  const memory = new Uint8Array(handle.memory.buffer);
  const view = new DataView(handle.memory.buffer);
  const atomicView = int32AtomicView(handle.memory.buffer);
  const snapshots: KernelWorkerPipeSlotSnapshot[] = [];
  for (const [pipeSlot, mode] of slotModes) {
    if ((mode & (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE)) !== (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE)) {
      continue;
    }
    const offset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
    const head = loadPipeU32(view, atomicView, offset + PIPE_DEBUG_HEAD_OFFSET);
    const tail = loadPipeU32(view, atomicView, offset + PIPE_DEBUG_TAIL_OFFSET);
    const readOpen = loadPipeI32(view, atomicView, offset + PIPE_DEBUG_READ_OPEN_OFFSET);
    const writeOpen = loadPipeI32(view, atomicView, offset + PIPE_DEBUG_WRITE_OPEN_OFFSET);
    const generation = loadPipeU32(view, atomicView, offset + PIPE_DEBUG_GENERATION_OFFSET);
    if (head !== tail || (head === 0 && !freshPipe2Slots.has(pipeSlot)) || readOpen <= 0 || writeOpen <= 0) {
      continue;
    }
    const slot = new Uint8Array(slotBytes);
    const slotView = new DataView(slot.buffer);
    slotView.setInt32(PIPE_DEBUG_READ_OPEN_OFFSET, readOpen, true);
    slotView.setInt32(PIPE_DEBUG_WRITE_OPEN_OFFSET, writeOpen, true);
    slotView.setUint32(PIPE_DEBUG_GENERATION_OFFSET, generation, true);
    memory.set(slot, offset);
    snapshots.push({
      pipeSlot,
      slot,
      mode: PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE,
      fresh: true,
    });
  }
  return snapshots;
}

function snapshotNonEmptyPipeSlotsForOfds(
  handle: ProcessHandle,
  ofdIds: readonly number[],
  pendingSnapshots: readonly KernelWorkerPipeSlotSnapshot[],
): KernelWorkerPipeSlotSnapshot[] {
  const maxPipes = handle.kernel.abi_max_pipes();
  const pipeRegionBytes = handle.kernel.kernel_pipe_region_bytes();
  const slotBytes = pipeRegionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= PIPE_DEBUG_HEADER_BYTES) {
    return [];
  }
  const pendingSlots = new Set(pendingSnapshots.map((snapshot) => snapshot.pipeSlot));
  const pipeSlotModes = new Map<number, number>();
  for (const ofdId of ofdIds) {
    if (
      ofdId <= 0
      || ofdId >= handle.kernel.abi_max_fds()
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      continue;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    if (pipeSlot > 0 && pipeSlot < maxPipes && !pendingSlots.has(pipeSlot)) {
      const mode = handle.kernel.kernel_ofd_host_fd(ofdId) >= 0
        ? PIPE_SLOT_MODE_READ
        : PIPE_SLOT_MODE_WRITE;
      pipeSlotModes.set(pipeSlot, (pipeSlotModes.get(pipeSlot) ?? 0) | mode);
    }
  }
  const memory = new Uint8Array(handle.memory.buffer);
  const view = new DataView(handle.memory.buffer);
  const atomicView = int32AtomicView(handle.memory.buffer);
  const snapshots: KernelWorkerPipeSlotSnapshot[] = [];
  for (const [pipeSlot, mode] of pipeSlotModes) {
    const offset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
    const head = loadPipeU32(view, atomicView, offset + PIPE_DEBUG_HEAD_OFFSET);
    const tail = loadPipeU32(view, atomicView, offset + PIPE_DEBUG_TAIL_OFFSET);
    if (head === tail) {
      continue;
    }
    snapshots.push({
      pipeSlot,
      slot: memory.slice(offset, offset + slotBytes),
      mode,
    });
  }
  return snapshots;
}

function snapshotNonEmptyHostOutputPipeSlots(
  handle: ProcessHandle,
  pendingSnapshots: readonly KernelWorkerPipeSlotSnapshot[],
): KernelWorkerPipeSlotSnapshot[] {
  const maxPipes = handle.kernel.abi_max_pipes();
  const pipeRegionBytes = handle.kernel.kernel_pipe_region_bytes();
  const slotBytes = pipeRegionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= PIPE_DEBUG_HEADER_BYTES) {
    return [];
  }
  const pendingSlots = new Set(pendingSnapshots.map((snapshot) => snapshot.pipeSlot));
  const memory = new Uint8Array(handle.memory.buffer);
  const view = new DataView(handle.memory.buffer);
  const atomicView = int32AtomicView(handle.memory.buffer);
  const snapshots: KernelWorkerPipeSlotSnapshot[] = [];
  const slots = [
    handle.hostStdio.stdoutPipe,
    handle.hostStdio.stderrPipe,
  ].filter((pipeSlot, index, array) => (
    pipeSlot > 0
    && pipeSlot < maxPipes
    && array.indexOf(pipeSlot) === index
    && !pendingSlots.has(pipeSlot)
  ));
  for (const pipeSlot of slots) {
    const offset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
    const head = loadPipeU32(view, atomicView, offset + PIPE_DEBUG_HEAD_OFFSET);
    const tail = loadPipeU32(view, atomicView, offset + PIPE_DEBUG_TAIL_OFFSET);
    if (head === tail) {
      continue;
    }
    snapshots.push({
      pipeSlot,
      slot: memory.slice(offset, offset + slotBytes),
      mode: PIPE_SLOT_MODE_WRITE,
    });
  }
  return snapshots;
}

function formatNetworkSocketDebug(handle: ProcessHandle): string {
  const fds = [...new Set([
    ...(handle.networkConnections?.keys() ?? []),
    ...(handle.networkPendingConnectFds ?? []),
    ...(handle.networkClosedFds ?? []),
    ...(handle.networkClosedSocketSlots?.keys() ?? []),
    ...(handle.networkSocketSlotWatermarks?.keys() ?? []),
    ...(handle.networkRecvPayloadWatermarks?.keys() ?? []),
  ])];
  return fds
    .sort((left, right) => left - right)
    .map((fd) => {
      const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(handle.vmPtr, fd) ?? -1;
      const peerIdx = socketIdx >= 0 ? handle.kernel.thread_socket_peer_idx?.(socketIdx) ?? -1 : -1;
      const state = socketIdx >= 0 ? handle.kernel.thread_socket_state?.(socketIdx) ?? -1 : -1;
      const type = socketIdx >= 0 ? handle.kernel.thread_socket_type?.(socketIdx) ?? -1 : -1;
      const ext = socketIdx >= 0 ? handle.kernel.thread_socket_external_peer?.(socketIdx) ?? -1 : -1;
      const buf = socketIdx >= 0 ? handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1 : -1;
      const recv = socketIdx >= 0 ? handle.kernel.thread_socket_recv_available?.(socketIdx) ?? -1 : -1;
      recordNetworkSocketMetaTransition(handle, "sync-format", fd, socketIdx, state, type, ext, buf, recv);
      return [
        `fd=${fd}`,
        `sock=${socketIdx}`,
        `state=${state}`,
        `type=${type}`,
        `ext=${ext}`,
        `buf=${buf}`,
        `recv=${recv}`,
        `ht=${readNetworkRecvBufferDebug(handle, socketIdx)}`,
        `read=${socketIdx >= 0 ? [
          handle.kernel.thread_socket_read_count?.(socketIdx) ?? -1,
          handle.kernel.thread_socket_read_bytes?.(socketIdx)?.toString() ?? "-1",
          handle.kernel.thread_socket_read_last_fd?.(socketIdx) ?? -1,
          handle.kernel.thread_socket_read_last_result?.(socketIdx)?.toString() ?? "-1",
          handle.kernel.thread_socket_read_last_before?.(socketIdx) ?? -1,
          handle.kernel.thread_socket_read_last_after?.(socketIdx) ?? -1,
        ].join("/") : "-1"}`,
        `peer=${peerIdx}`,
        `peer_type=${peerIdx >= 0 ? handle.kernel.thread_socket_type?.(peerIdx) ?? -1 : -1}`,
        `peer_ext=${peerIdx >= 0 ? handle.kernel.thread_socket_external_peer?.(peerIdx) ?? -1 : -1}`,
        `peer_buf=${peerIdx >= 0 ? handle.kernel.thread_socket_recv_buf_idx?.(peerIdx) ?? -1 : -1}`,
        `peer_recv=${peerIdx >= 0 ? handle.kernel.thread_socket_recv_available?.(peerIdx) ?? -1 : -1}`,
      ].join(":");
    })
    .join("|");
}

function networkSocketFdStats(handle: ProcessHandle, fd: number): {
  fd: number;
  state: number;
  type: number;
  ext: number;
  recvAvailable: number;
  readCount: number;
} {
  const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(handle.vmPtr, fd) ?? -1;
  return {
    fd,
    state: socketIdx >= 0 ? handle.kernel.thread_socket_state?.(socketIdx) ?? -1 : -1,
    type: socketIdx >= 0 ? handle.kernel.thread_socket_type?.(socketIdx) ?? -1 : -1,
    ext: socketIdx >= 0 ? handle.kernel.thread_socket_external_peer?.(socketIdx) ?? -1 : -1,
    recvAvailable: socketIdx >= 0 ? handle.kernel.thread_socket_recv_available?.(socketIdx) ?? -1 : -1,
    readCount: socketIdx >= 0 ? handle.kernel.thread_socket_read_count?.(socketIdx) ?? 0 : 0,
  };
}

function closeNetworkClosedFdPeers(handle: ProcessHandle): void {
  forceNetworkClosedFds(handle);
}

function restoreClosedNetworkSocketSlots(handle: ProcessHandle): void {
  if ((handle.networkClosedSocketSlots?.size ?? 0) === 0) {
    return;
  }
  for (const slot of handle.networkClosedSocketSlots?.values() ?? []) {
    restoreNetworkSocketSlotSnapshot(handle, slot);
  }
}

function preserveNetworkSocketSlots(handle: ProcessHandle): PreservedNetworkSocketSlot[] {
  const fds = [...runtimeNetworkFds(handle)];
  if (fds.length === 0) {
    return [];
  }
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const socketTableBytes = handle.kernel.kernel_socket_table_bytes?.() ?? 0;
  const socketSlotBytes = maxSockets > 0 ? socketTableBytes / maxSockets : 0;
  if (!Number.isInteger(socketSlotBytes) || socketSlotBytes <= 0) {
    return [];
  }
  const preserved: PreservedNetworkSocketSlot[] = [];
  const preservedOffsets = new Set<number>();
  const memory = new Uint8Array(handle.memory.buffer);
  const socketTablePtr = handle.kernel.kernel_socket_table_ptr?.() ?? handle.kernelState.socketTablePtr;
  const preserveSocketIdx = (socketIdx: number): void => {
    if (socketIdx < 0 || socketIdx >= maxSockets) {
      return;
    }
    const offset = socketIdx * socketSlotBytes;
    if (preservedOffsets.has(offset)) {
      return;
    }
    preservedOffsets.add(offset);
    preserved.push({
      offset,
      data: memory.slice(socketTablePtr + offset, socketTablePtr + offset + socketSlotBytes),
      socketMask: 1 << socketIdx,
    });
  };
  for (const fd of fds) {
    const closedSlot = handle.networkClosedSocketSlots?.get(fd);
    if (closedSlot) {
      if (!preservedOffsets.has(closedSlot.offset)) {
        preservedOffsets.add(closedSlot.offset);
        preserved.push({
          ...closedSlot,
          socketMask: 1 << Math.floor(closedSlot.offset / socketSlotBytes),
        });
      }
      continue;
    }
    const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(handle.vmPtr, fd) ?? -1;
    if (socketIdx >= 0) {
      preserveSocketIdx(socketIdx);
      preserveSocketIdx(handle.kernel.thread_socket_peer_idx?.(socketIdx) ?? -1);
      continue;
    }
    const watermarkedSlot = handle.networkSocketSlotWatermarks?.get(fd);
    if (
      (handle.networkRecvPayloadWatermarks?.has(fd) ?? false)
      && watermarkedSlot
      && !preservedOffsets.has(watermarkedSlot.offset)
    ) {
      preservedOffsets.add(watermarkedSlot.offset);
      preserved.push({
        ...watermarkedSlot,
        socketMask: 1 << Math.floor(watermarkedSlot.offset / socketSlotBytes),
      });
      continue;
    }
    const slot = snapshotNetworkSocketSlot(handle, handle.vmPtr, fd);
    if (slot && !preservedOffsets.has(slot.offset)) {
      preservedOffsets.add(slot.offset);
      preserved.push({
        ...slot,
        socketMask: 1 << Math.floor(slot.offset / socketSlotBytes),
      });
    }
  }
  return preserved;
}

function restoreNetworkSocketSlots(handle: ProcessHandle, preserved: PreservedNetworkSocketSlot[]): void {
  for (const slot of preserved) {
    restoreNetworkSocketSlotSnapshot(handle, slot);
  }
}

function currentNetBufUsed(handle: ProcessHandle): bigint {
  return handle.kernelState.netBufUsed | handle.kernel.kernel_net_buf_used();
}

function currentSocketUsed(handle: ProcessHandle): number {
  return handle.kernelState.socketUsed | handle.kernel.kernel_socket_used();
}

function netBufferSlotBytesForHandle(handle: ProcessHandle): number {
  const maxBuffers = Math.max(1, Number(handle.kernel.abi_max_sockets?.() ?? 32) * 2);
  const poolBytes = handle.kernel.kernel_net_buf_pool_bytes();
  const slotBytes = Math.floor(poolBytes / maxBuffers);
  return Number.isInteger(slotBytes) && slotBytes > 0 ? slotBytes : 0;
}

function snapshotUsedNetBufferSlots(
  memory: Uint8Array,
  netBufBase: number,
  slotBytes: number,
  maxBuffers: number,
  netBufUsed: bigint,
): Array<{ bufferIndex: number; bytes: Uint8Array }> {
  if (slotBytes <= 0) {
    return [];
  }
  const slots: Array<{ bufferIndex: number; bytes: Uint8Array }> = [];
  for (let bufferIndex = 0; bufferIndex < maxBuffers; bufferIndex += 1) {
    if ((netBufUsed & (1n << BigInt(bufferIndex))) === 0n) {
      continue;
    }
    const offset = netBufBase + bufferIndex * slotBytes;
    slots.push({
      bufferIndex,
      bytes: memory.slice(offset, offset + slotBytes),
    });
  }
  return slots;
}

function restoreNetBufferSlots(
  memory: Uint8Array,
  netBufBase: number,
  slotBytes: number,
  maxBuffers: number,
  slots: Array<{ bufferIndex: number; bytes: Uint8Array }>,
): void {
  if (slotBytes <= 0) {
    return;
  }
  for (const slot of slots) {
    if (slot.bufferIndex < 0 || slot.bufferIndex >= maxBuffers || slot.bytes.length !== slotBytes) {
      continue;
    }
    memory.set(slot.bytes, netBufBase + slot.bufferIndex * slotBytes);
  }
}

function socketSnapshotNetBufPoolBytes(handle: ProcessHandle, snapshot: KernelWorkerSocketStateSnapshot): number {
  return snapshot.netBufPool?.length ?? handle.kernel.kernel_net_buf_pool_bytes();
}

function applyKernelWorkerNetBuffers(
  handle: ProcessHandle,
  memory: Uint8Array,
  snapshot: KernelWorkerSocketStateSnapshot,
): void {
  if (snapshot.netBufPool) {
    memory.set(snapshot.netBufPool, handle.kernelState.netBufBase);
    return;
  }
  restoreNetBufferSlots(
    memory,
    handle.kernelState.netBufBase,
    netBufferSlotBytesForHandle(handle),
    Math.max(1, Number(handle.kernel.abi_max_sockets?.() ?? 32) * 2),
    snapshot.netBufSlots ?? [],
  );
}

function networkBridgeOutboundAvailable(handle: ProcessHandle): boolean {
  const fds = [...new Set([
    ...(handle.networkConnections?.keys() ?? []),
    ...(handle.networkPendingConnectFds ?? []),
  ])];
  return fds.some((fd) => (handle.kernel.thread_socket_outbound_available?.(handle.vmPtr, fd) ?? 0) > 0);
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
        appendSocketSyncDebug(
          handle,
          `net-payload-watermark-use-progress:${handle.pid}:fd=${fd}:source=sync:payload=${payloadProgress}:current=${currentProgress}:phead=${payload.head}:rhead=${restoredHead}:ptail=${payload.tail}:rtail=${restoredTail}:ctail=${currentTail}`,
        );
        return {
          head: restoredHead,
          tail: restoredTail,
          data: payload.data,
        };
      }
      if (currentProgress > payloadProgress) {
        appendSocketSyncDebug(
          handle,
          `net-payload-watermark-skip-progress:${handle.pid}:fd=${fd}:source=sync:payload=${payloadProgress}:current=${currentProgress}:ptail=${payload.tail}:ctail=${currentTail}`,
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
    appendSocketSyncDebug(
      handle,
      `net-payload-watermark-use:${handle.pid}:fd=${fd}:source=sync:ptail=${payload.tail}:ctail=${currentTail}:cap=${capacity}`,
    );
    return {
      head: payload.head,
      tail: payload.tail,
      data: payload.data,
    };
  }
  return null;
}

function preserveUsedNetBufferHeads(
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

function netBufUsedMaskFromBufferIndices(indices: Iterable<number>): bigint {
  let mask = 0n;
  for (const index of indices) {
    if (index >= 0) {
      mask |= 1n << BigInt(index);
    }
  }
  return mask;
}

function preserveNetworkRecvHeadWatermarks(
  handle: ProcessHandle,
  netBufPoolBytes: number,
): PreservedNetBufferHead[] {
  const fds = [...runtimeNetworkFds(handle)];
  if (fds.length === 0) {
    return [];
  }
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return [];
  }
  const preserved: PreservedNetBufferHead[] = [];
  const memory = new Uint8Array(handle.memory.buffer);
  for (const fd of fds) {
    const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(handle.vmPtr, fd) ?? -1;
    if (socketIdx < 0) {
      handle.networkClosedFds?.delete(fd);
      continue;
    }
    const recvBufIdx = handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1;
    if (recvBufIdx < 0 || recvBufIdx >= netBufferCount) {
      continue;
    }
    const offset = recvBufIdx * netBufferBytes;
    const absolute = handle.kernelState.netBufBase + offset;
    const view = new DataView(memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
    const currentHead = view.getUint32(NET_BUFFER_HEAD_OFFSET, true);
    const currentTail = view.getUint32(NET_BUFFER_TAIL_OFFSET, true);
    const capacity = netBufferBytes - NET_BUFFER_HEADER_BYTES;
    const payload = networkRecvPayloadWatermark(handle, fd, socketIdx, currentTail, capacity);
    const terminalRecv = handle.kernel.thread_socket_state?.(socketIdx) === SOCK_STATE_SHUTDOWN
      && (handle.kernel.thread_socket_external_peer?.(socketIdx) ?? 0) === 0;
    const preservedTail = payload?.tail ?? currentTail;
    const mergedHead = (() => {
      if (terminalRecv) {
        return currentHead;
      }
      if (payload !== null) {
        return payload.head;
      }
      const available = handle.kernel.thread_socket_recv_available?.(socketIdx);
      if (available === 0 && currentHead === currentTail) {
        return currentHead;
      }
      const floor = socketReadHeadFloor(handle, socketIdx, currentTail, capacity);
      return counterAhead(floor, currentHead, capacity)
        ? floor
        : currentHead;
    })();
    preserved.push({
      offset,
      head: mergedHead,
      tail: (() => {
        if (payload !== null) {
          handle.networkRecvTailWatermarks.set(fd, preservedTail);
          return preservedTail;
        }
        if (terminalRecv) {
          handle.networkRecvTailWatermarks.set(fd, mergedHead);
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
    handle.networkRecvHeadWatermarks.set(fd, mergedHead);
  }
  return preserved;
}

function restoreMergedNetBufferHeads(
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
      const preservedTailCoversIncoming = buffer.tail === incomingTail
        || preservedTailAhead;
      if (buffer.restoreData && buffer.data !== undefined && preservedTailCoversIncoming) {
        memory.set(buffer.data, absolute + NET_BUFFER_HEADER_BYTES);
      }
      if (buffer.forceTail === true || preservedTailAhead) {
        if (!buffer.restoreData && buffer.data !== undefined) {
          memory.set(buffer.data, absolute + NET_BUFFER_HEADER_BYTES);
        }
        view.setUint32(NET_BUFFER_TAIL_OFFSET, buffer.tail, true);
      }
    }
  }
}

function snapshotNetworkBridgeSocketState(
  handles: Iterable<ProcessHandle>,
): NetworkBridgeSocketStateSnapshot[] {
  const snapshots: NetworkBridgeSocketStateSnapshot[] = [];
  for (const handle of handles) {
    if (
      (handle.networkConnections?.size ?? 0) === 0
      && (handle.networkPendingConnectFds?.size ?? 0) === 0
      && (handle.networkClosedFds?.size ?? 0) === 0
      && (handle.networkClosedSocketSlots?.size ?? 0) === 0
      && (handle.networkSocketSlotWatermarks?.size ?? 0) === 0
      && (handle.networkRecvPayloadWatermarks?.size ?? 0) === 0
    ) {
      continue;
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    const memory = new Uint8Array(handle.memory.buffer);
    const socketTableBytes = handle.kernel.kernel_socket_table_bytes();
    const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes();
    const netBufUsed = currentNetBufUsed(handle);
    const maxNetBuffers = Math.max(1, Number(handle.kernel.abi_max_sockets?.() ?? 32) * 2);
    const netBufferSlotBytes = netBufferSlotBytesForHandle(handle);
    const ofdSlotBytes = Math.floor(handle.kernel.kernel_ofd_table_bytes() / handle.kernel.abi_max_fds());
    const ofdSlots: NetworkBridgeSocketStateSnapshot["ofdSlots"] = [];
    const connectionFds = [...(handle.networkConnections?.keys() ?? [])];
    const pendingConnectFds = [...(handle.networkPendingConnectFds ?? [])];
    const closedFds = [...(handle.networkClosedFds ?? [])];
    const closedSlotFds = [...(handle.networkClosedSocketSlots?.keys() ?? [])];
    const watermarkedSocketFds = [...(handle.networkSocketSlotWatermarks?.keys() ?? [])];
    const watermarkedRecvFds = [...(handle.networkRecvPayloadWatermarks?.keys() ?? [])];
    const fdStats = [...new Set([
      ...connectionFds,
      ...pendingConnectFds,
      ...closedFds,
      ...closedSlotFds,
      ...watermarkedSocketFds,
      ...watermarkedRecvFds,
    ])]
      .map((fd) => networkSocketFdStats(handle, fd));
    for (let ofdId = 1; ofdId < handle.kernel.abi_max_fds(); ofdId += 1) {
      if (
        ofdSlotBytes <= 0
        || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_socket()
      ) {
        continue;
      }
      const offset = handle.kernelState.ofdTablePtr + ofdId * ofdSlotBytes;
      ofdSlots.push({
        ofdId,
        bytes: memory.slice(offset, offset + ofdSlotBytes),
      });
    }
    snapshots.push({
      handle,
      connectionFds,
      pendingConnectFds,
      closedFds,
      fdStats,
      socketTable: memory.slice(
        handle.kernelState.socketTablePtr,
        handle.kernelState.socketTablePtr + socketTableBytes,
      ),
      netBufPoolBytes,
      netBufSlots: snapshotUsedNetBufferSlots(
        memory,
        handle.kernelState.netBufBase,
        netBufferSlotBytes,
        maxNetBuffers,
        netBufUsed,
      ),
      ofdSlots,
      socketUsed: handle.kernelState.socketUsed,
      netBufUsed,
    });
  }
  return snapshots;
}

function snapshotSocketStateForKernelWorker(handle: ProcessHandle): KernelWorkerSocketStateSnapshot {
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  const memory = new Uint8Array(handle.memory.buffer);
  const socketTableBytes = handle.kernel.kernel_socket_table_bytes();
  const netBufUsed = currentNetBufUsed(handle);
  const maxNetBuffers = Math.max(1, Number(handle.kernel.abi_max_sockets?.() ?? 32) * 2);
  const netBufferSlotBytes = netBufferSlotBytesForHandle(handle);
  return {
    socketTable: memory.slice(
      handle.kernelState.socketTablePtr,
      handle.kernelState.socketTablePtr + socketTableBytes,
    ),
    netBufSlots: snapshotUsedNetBufferSlots(
      memory,
      handle.kernelState.netBufBase,
      netBufferSlotBytes,
      maxNetBuffers,
      netBufUsed,
    ),
    socketUsed: currentSocketUsed(handle),
    netBufUsed,
  };
}

export function applySocketStateFromKernelWorker(
  handle: ProcessHandle,
  snapshot: KernelWorkerSocketStateSnapshot,
): void {
  if (networkBridgeOutboundAvailable(handle)) {
    appendSocketSyncDebug(handle, `socket-state-skip:${handle.pid}:reason=pending-outbound:current=${formatNetworkSocketDebug(handle)}`);
    return;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  const netBufUsed = currentNetBufUsed(handle) | snapshot.netBufUsed;
  const socketUsed = currentSocketUsed(handle) | snapshot.socketUsed;
  const preservedSocketSlots = preserveNetworkSocketSlots(handle);
  const preservedSocketUsed = preservedSocketSlots.reduce((mask, slot) => mask | slot.socketMask, 0);
  const preservedSocketRecvBuffers = preservedSocketRecvBufferIndices(preservedSocketSlots);
  const preservedSocketNetBufUsed = netBufUsedMaskFromBufferIndices(preservedSocketRecvBuffers);
  const netBufPoolBytes = socketSnapshotNetBufPoolBytes(handle, snapshot);
  const preservedNetBufferHeads = [
    ...preserveUsedNetBufferHeads(
      handle,
      netBufPoolBytes,
      netBufUsed | preservedSocketNetBufUsed,
      preservedSocketRecvBuffers,
    ),
    ...preserveNetworkRecvHeadWatermarks(handle, netBufPoolBytes),
  ];
  memory.set(snapshot.socketTable, handle.kernelState.socketTablePtr);
  applyKernelWorkerNetBuffers(handle, memory, snapshot);
  restoreNetworkSocketSlots(handle, preservedSocketSlots);
  restoreMergedNetBufferHeads(memory, handle.kernelState.netBufBase, preservedNetBufferHeads);
  restoreClosedNetworkSocketSlots(handle);
  closeNetworkClosedFdPeers(handle);
  handle.kernelState = {
    ...handle.kernelState,
    socketUsed: socketUsed | preservedSocketUsed,
    netBufUsed: netBufUsed | preservedSocketNetBufUsed,
  };
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

function restoreNetworkBridgeSocketState(
  snapshots: NetworkBridgeSocketStateSnapshot[],
): void {
  for (const snapshot of snapshots) {
    const { handle } = snapshot;
    if (snapshot.connectionFds.some((fd) => !(handle.networkConnections?.has(fd) ?? false))) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      continue;
    }
    const pendingConnectFdsStillAuthoritative = snapshot.pendingConnectFds.every((fd) => handle.networkPendingConnectFds?.has(fd) ?? false);
    if (!pendingConnectFdsStillAuthoritative) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      continue;
    }
    const closedFdsStillAuthoritative = snapshot.closedFds.every((fd) => handle.networkClosedFds.has(fd));
    if (!closedFdsStillAuthoritative) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      continue;
    }
    const progressed = networkBridgeSocketSnapshotProgressed(snapshot);
    if (progressed) {
      appendSocketSyncDebug(
        handle,
        `socket-restore-merge-progress:${handle.pid}:snapshot=${snapshot.fdStats.map((stat) => `${stat.fd}/${stat.state}/${stat.type}/${stat.ext}/${stat.recvAvailable}/${stat.readCount}`).join(",")}:current=${formatNetworkSocketDebug(handle)}`,
      );
    }
    if (networkBridgeOutboundAvailable(handle)) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      appendSocketSyncDebug(handle, `socket-restore-skip-pending-outbound:${handle.pid}:current=${formatNetworkSocketDebug(handle)}`);
      continue;
    }
    const memory = new Uint8Array(handle.memory.buffer);
    const ofdSlotBytes = Math.floor(handle.kernel.kernel_ofd_table_bytes() / handle.kernel.abi_max_fds());
    const netBufUsed = currentNetBufUsed(handle) | snapshot.netBufUsed;
    const socketUsed = currentSocketUsed(handle) | snapshot.socketUsed;
    const preservedSocketSlots = preserveNetworkSocketSlots(handle);
    const preservedSocketUsed = preservedSocketSlots.reduce((mask, slot) => mask | slot.socketMask, 0);
    const preservedSocketRecvBuffers = preservedSocketRecvBufferIndices(preservedSocketSlots);
    const preservedSocketNetBufUsed = netBufUsedMaskFromBufferIndices(preservedSocketRecvBuffers);
    const netBufPoolBytes = snapshot.netBufPoolBytes;
    const preservedNetBufferHeads = [
      ...preserveUsedNetBufferHeads(
        handle,
        netBufPoolBytes,
        netBufUsed | preservedSocketNetBufUsed,
        preservedSocketRecvBuffers,
      ),
      ...preserveNetworkRecvHeadWatermarks(handle, netBufPoolBytes),
    ];
    memory.set(snapshot.socketTable, handle.kernelState.socketTablePtr);
    restoreNetBufferSlots(
      memory,
      handle.kernelState.netBufBase,
      netBufferSlotBytesForHandle(handle),
      Math.max(1, Number(handle.kernel.abi_max_sockets?.() ?? 32) * 2),
      snapshot.netBufSlots,
    );
    restoreNetworkSocketSlots(handle, preservedSocketSlots);
    restoreMergedNetBufferHeads(memory, handle.kernelState.netBufBase, preservedNetBufferHeads);
    restoreClosedNetworkSocketSlots(handle);
    closeNetworkClosedFdPeers(handle);
    for (const ofdSlot of snapshot.ofdSlots) {
      if (ofdSlot.ofdId <= 0 || ofdSlotBytes <= 0) {
        continue;
      }
      memory.set(ofdSlot.bytes, handle.kernelState.ofdTablePtr + ofdSlot.ofdId * ofdSlotBytes);
    }
    handle.kernelState = {
      ...handle.kernelState,
      socketUsed: socketUsed | preservedSocketUsed,
      netBufUsed: netBufUsed | preservedSocketNetBufUsed,
    };
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
  }
}

function networkBridgeSocketSnapshotProgressed(snapshot: NetworkBridgeSocketStateSnapshot): boolean {
  const { handle } = snapshot;
  return snapshot.fdStats.some((stat) => {
    const current = networkSocketFdStats(handle, stat.fd);
    return current.recvAvailable !== stat.recvAvailable
      || current.readCount !== stat.readCount
      || current.state !== stat.state;
  });
}

function repairHandleVmPidAfterKernelStateAttach(
  handle: ProcessHandle,
  vmPtr: number,
  pidBeforeSync: number,
): void {
  if (vmPtr === 0 || pidBeforeSync !== handle.pid) {
    return;
  }
  const pidAfterSync = handle.kernel.process_current_pid(vmPtr);
  if (pidAfterSync <= 0 || pidAfterSync === handle.pid) {
    return;
  }
  handle.kernel.process_rewrite_child_pid(vmPtr, pidAfterSync, handle.pid, handle.pid + 1);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

function snapshotRuntimeNetworkFdEntries(
  handle: ProcessHandle,
  vmPtr: number,
  runtimeNetworkSocketOfdIds: ReadonlySet<number>,
): KernelWorkerFdEntrySnapshot[] {
  if (vmPtr === 0) {
    return [];
  }
  const networkFds = runtimeNetworkFds(handle);
  if (
    runtimeNetworkSocketOfdIds.size === 0
    && networkFds.size === 0
    && (handle.networkOfdSlotWatermarks?.size ?? 0) === 0
  ) {
    return [];
  }
  const connectionOfds = handle.networkConnectionOfds ?? new Map<number, number>();
  return snapshotProcessFdEntries(handle, vmPtr)
    .filter((entry) => {
      if (entry.ofdId <= 0) {
        return false;
      }
      return runtimeNetworkSocketOfdIds.has(entry.ofdId)
        || networkFds.has(entry.fd)
        || connectionOfds.get(entry.fd) === entry.ofdId
        || (handle.networkOfdSlotWatermarks?.has(entry.ofdId) ?? false);
    });
}

function snapshotRuntimeNetworkKernelState(
  handle: ProcessHandle,
  vmPtr = handle.vmPtr,
): PreservedRuntimeNetworkKernelState {
  const runtimeNetworkSocketOfdIds = collectRuntimeNetworkSocketOfdIds([handle]);
  appendSocketSyncDebug(
    handle,
    `runtime-net-kernel-snapshot:${handle.pid}:vm=0x${vmPtr.toString(16)}:ofds=${formatOfdIdList([...runtimeNetworkSocketOfdIds].sort((left, right) => left - right))}`,
  );
  return {
    handle,
    vmPtr,
    ofdSlots: snapshotRuntimeNetworkOfdSlots(handle, runtimeNetworkSocketOfdIds),
    fdEntries: snapshotRuntimeNetworkFdEntries(handle, vmPtr, runtimeNetworkSocketOfdIds),
  };
}

function restoreRuntimeNetworkKernelState(
  snapshot: PreservedRuntimeNetworkKernelState,
): void {
  appendSocketSyncDebug(
    snapshot.handle,
    `runtime-net-kernel-restore:${snapshot.handle.pid}:vm=0x${snapshot.vmPtr.toString(16)}:ofdSlots=${formatOfdIdList([...snapshot.ofdSlots.keys()].sort((left, right) => left - right))}:fdEntries=${snapshot.fdEntries.map((entry) => `${entry.fd}->${entry.ofdId}`).join(".") || "none"}`,
  );
  restoreRuntimeNetworkOfdSlots(snapshot.handle, snapshot.ofdSlots);
  restoreProcessFdEntries(snapshot.handle, snapshot.vmPtr, snapshot.fdEntries);
  snapshot.handle.kernelState = readKernelRuntimeState(snapshot.handle.kernel);
  attachKernelRuntimeState(snapshot.handle.kernel, snapshot.handle.kernelState);
}

export function snapshotProcessFdOfdStateForKernelWorker(
  handle: ProcessHandle,
  vmPtr: number,
): {
  fdEntries: KernelWorkerFdEntrySnapshot[];
  ofdSlots: KernelWorkerOfdSlotSnapshot[];
} {
  const fdEntries = snapshotProcessFdEntries(handle, vmPtr);
  const ofdIds = [...new Set([
    ...ofdIdsFromFdEntries(handle, fdEntries),
    ...collectProcessHeldOfds(handle),
  ])];
  const ofdSlots = [...snapshotKernelShareOfdSlots(handle, ofdIds)]
    .map(([ofdId, slot]) => ({ ofdId, slot }));
  return { fdEntries, ofdSlots };
}

function collectTracedForkSharedOfdIds(
  handle: ProcessHandle,
  sourceVmPtr: number,
): number[] {
  const len = handle.kernel.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return [];
  }
  const traced = new Set<number>();
  const sampleToPid = (sample: bigint | number | undefined): number => {
    if (sample === undefined) {
      return 0;
    }
    return Number(BigInt.asIntN(32, BigInt(sample) & 0xffffffffn));
  };
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
    if (traceVmPtr !== 0 && sourceVmPtr !== 0 && traceVmPtr !== sourceVmPtr) {
      continue;
    }
    const tracePid = sampleToPid(handle.kernel.debug_fd_syscall_trace_pid_sample0?.(index));
    if (tracePid > 0 && tracePid !== handle.pid) {
      continue;
    }
    const arg0OfdId = handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0;
    const arg2OfdId = handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? 0;
    if (arg0OfdId > 0) {
      traced.add(arg0OfdId);
    }
    if (arg2OfdId > 0) {
      traced.add(arg2OfdId);
    }
  }
  return [...traced];
}

export function adoptKernelState(
  current: KernelRuntimeState,
  incoming: KernelRuntimeState,
): KernelRuntimeState {
  return {
    heapBase: incoming.heapBase,
    heapPtr: Math.max(current.heapPtr, incoming.heapPtr),
    ofdTablePtr: current.ofdTablePtr,
    memfsPtr: current.memfsPtr,
    memfsHeaderBytes: current.memfsHeaderBytes > 0 ? current.memfsHeaderBytes : incoming.memfsHeaderBytes,
    memfsInodesBytes: current.memfsInodesBytes > 0 ? current.memfsInodesBytes : incoming.memfsInodesBytes,
    memfsFirstChildBytes: current.memfsFirstChildBytes > 0
      ? current.memfsFirstChildBytes
      : incoming.memfsFirstChildBytes,
    memfsDirEntriesBytes: current.memfsDirEntriesBytes > 0
      ? current.memfsDirEntriesBytes
      : incoming.memfsDirEntriesBytes,
    memfsDataBytes: current.memfsDataBytes > 0 ? current.memfsDataBytes : incoming.memfsDataBytes,
    memStatePtr: current.memStatePtr,
    epollTablePtr: current.epollTablePtr,
    socketTablePtr: current.socketTablePtr,
    socketUsed: incoming.socketUsed,
    pipeBase: current.pipeBase,
    pipeUsed: incoming.pipeUsed,
    netBufBase: current.netBufBase,
    netBufUsed: incoming.netBufUsed,
    sigSlotsPtr: current.sigSlotsPtr,
    processPendingPtr: current.processPendingPtr,
    processRuntimeStatePtr: current.processRuntimeStatePtr,
    timeStatePtr: current.timeStatePtr,
  };
}

export function adoptKernelStateForHandle(
  handle: ProcessHandle,
  incoming: KernelRuntimeState,
): KernelRuntimeState {
  const current = handle.kernelState;
  const adopted = adoptKernelState(current, incoming);
  const pipeUsed = adopted.pipeUsed | liveProcessPipeUsedBits(handle);
  if (
    (handle.networkConnections?.size ?? 0) === 0
    && (handle.networkPendingConnectFds?.size ?? 0) === 0
    && (handle.networkClosedFds?.size ?? 0) === 0
    && (handle.networkClosedSocketSlots?.size ?? 0) === 0
  ) {
    return {
      ...adopted,
      pipeUsed,
    };
  }
  return {
    ...adopted,
    pipeUsed,
    socketUsed: current.socketUsed | incoming.socketUsed,
    netBufUsed: current.netBufUsed | incoming.netBufUsed,
  };
}

export function blockedStatusUsesForkSharedStateSync(handle: ProcessHandle, status: number): boolean {
  return status === handle.kernel.abi_status_pipe_wait()
    || status === handle.kernel.abi_status_socket_recv()
    || status === handle.kernel.abi_status_socket_send()
    || status === handle.kernel.abi_status_flock_wait();
}

export function syncForkSharedStateLocally(
  handle: ProcessHandle,
  targets: ProcessHandle[],
  options: { includeMemfs?: boolean; trackHeldOfds?: boolean; ofdIds?: number[] } = {},
  collectTrackableForkSharedOfds: (handle: ProcessHandle) => Set<number>,
): void {
  if (options.trackHeldOfds === true) {
    for (const ofdId of collectTrackableForkSharedOfds(handle)) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
    }
  }
  const activeOfdIds = activeKernelShareOfdIds(handle);
  const allowedOfdIdSet = options.ofdIds === undefined ? null : new Set(options.ofdIds);
  const staleOfdIds = staleKernelShareOfdIds(handle);
  syncKernelShareOfdReservations(handle);
  const syncOfdIds = [...new Set(
    allowedOfdIdSet === null
      ? activeOfdIds
      : activeOfdIds.filter((ofdId) => allowedOfdIdSet.has(ofdId)),
  )].sort((left, right) => left - right);
  mergeKernelShareOfdStateInto(handle, syncOfdIds);
  reconcileForkSharedOfdRefcounts(handle, syncOfdIds);
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState(handle.kernelShare.members);
  const processStateSnapshots = snapshotProcessStates(targets);
  try {
    for (const target of targets) {
      copyLegacySharedKernelObjectsLocal(handle, target, syncOfdIds, options);
      attachKernelRuntimeState(target.kernel, target.kernelState);
    }
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
    restoreProcessStates(processStateSnapshots);
  }
  pruneStaleKernelShareOfdIds(handle, staleOfdIds);
}

function isQuiescentForkSharedSyncTarget(target: ProcessHandle): boolean {
  return target.runningWorker === null
    && target.readyWorkers.length === 0
    && target.blockedWorkers.size > 0
    && !hasPendingBlockedRpc(target)
    && !target.exitInProgress;
}

function hasPendingBlockedRpc(handle: ProcessHandle): boolean {
  return [...handle.blockedWorkers.values()].some((blocked) => blocked.rpcPending);
}

export async function syncForkSharedStateViaKernelWorker(
  handle: ProcessHandle,
  targets: ProcessHandle[],
  options: {
    includeMemfs?: boolean;
    trackHeldOfds?: boolean;
    deltaOfdIds?: boolean;
    memfsOnly?: boolean;
    preserveSourceRefcounts?: boolean;
    preservePublishedRegularFileData?: boolean;
    sourceVmPtr?: number;
    ofdIds?: number[];
  } = {},
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    throw new Error("syncForkSharedStateViaKernelWorker requires kernel-worker ownership");
  }
  const quiescentTargets = targets.filter(isQuiescentForkSharedSyncTarget);
  let syncTargets = options.includeMemfs === true
    ? quiescentTargets.filter((target) => !hasRuntimeNetworkSocketState(target))
    : quiescentTargets;
  const memfsOnly = options.memfsOnly === true;

  if (!memfsOnly && options.trackHeldOfds === true) {
    trackHeldForkSharedOfds(handle, options.sourceVmPtr ?? handle.vmPtr);
  }
  const syncableForkSharedOfd = (ofdId: number): boolean => !isProcessHostStdioPipeOfd(handle, ofdId);
  const sourceVmPtr = options.sourceVmPtr ?? handle.vmPtr;
  const sourceFdEntries = memfsOnly ? [] : snapshotProcessFdEntries(handle, sourceVmPtr);
  const sourceFdOfdIds = memfsOnly
    ? []
    : ofdIdsFromFdEntries(handle, sourceFdEntries).filter(syncableForkSharedOfd);
  const sourceHeldOfdIds = memfsOnly
    ? new Set<number>()
    : new Set([...collectProcessHeldOfds(handle)].filter(syncableForkSharedOfd));
  const sourcePublishedOfdIds = new Set<number>();
  const sourceConflictingOfdIds = new Set<number>();
  const allowedOfdIdSet = options.ofdIds === undefined ? null : new Set(options.ofdIds);
  if (!memfsOnly) {
    for (const ofdId of new Set([...sourceHeldOfdIds, ...sourceFdOfdIds])) {
      if (!handle.kernelShare.sharedOfdIds.has(ofdId)) {
        continue;
      }
      if (sourceOfdCanPublishForkSharedState(handle, ofdId)) {
        sourcePublishedOfdIds.add(ofdId);
      } else {
        sourceConflictingOfdIds.add(ofdId);
      }
    }
  }
  const activeOfdIds = memfsOnly
    ? []
    : activeKernelShareOfdIds(handle).filter((ofdId) =>
        syncableForkSharedOfd(ofdId)
        && !sourceConflictingOfdIds.has(ofdId)
        && (allowedOfdIdSet === null || allowedOfdIdSet.has(ofdId))
      );
  const preservedSourceFdOfdSlots = snapshotLocalOfdSlots(
    handle,
    sourceFdOfdIds.filter((ofdId) => !isRegularFileOfd(handle, ofdId)),
  );
  const sourceActiveOfdIds = activeOfdIds
    .filter((ofdId) => sourceHeldOfdIds.has(ofdId) || sourceFdOfdIds.includes(ofdId));
  const staleOfdIds = memfsOnly ? [] : staleKernelShareOfdIds(handle);
  if (!memfsOnly) {
    syncKernelShareOfdReservations(handle);
  }
  const runtimeNetworkSocketOfdIds = collectRuntimeNetworkSocketOfdIds(handle.kernelShare.members);
  const preservedRuntimeNetworkKernelStates = [...handle.kernelShare.members].map((member) =>
    snapshotRuntimeNetworkKernelState(member, member === handle ? sourceVmPtr : member.vmPtr)
  );
  const omitRuntimeNetworkSocketOfds = (ofdIds: number[]): number[] => {
    if (runtimeNetworkSocketOfdIds.size === 0) {
      return ofdIds;
    }
    return ofdIds.filter((ofdId) => !runtimeNetworkSocketOfdIds.has(ofdId));
  };
  const publishableSourceHeldOfdIds = [...sourceHeldOfdIds].filter((ofdId) =>
    syncableForkSharedOfd(ofdId)
    && !sourceConflictingOfdIds.has(ofdId)
    && (allowedOfdIdSet === null || allowedOfdIdSet.has(ofdId))
  );
  const publishableSourceFdOfdIds = sourceFdOfdIds.filter((ofdId) =>
    syncableForkSharedOfd(ofdId)
    && !sourceConflictingOfdIds.has(ofdId)
    && (allowedOfdIdSet === null || allowedOfdIdSet.has(ofdId))
  );
  const sourceFdOfdIdSet = new Set(sourceFdOfdIds);
  const trackedSourceOfdIds = options.trackHeldOfds === true
    ? [...new Set([...sourceHeldOfdIds, ...sourceFdOfdIds])]
        .filter((ofdId) =>
          (handle.kernelShare.sharedOfdIds.has(ofdId) || sourceFdOfdIdSet.has(ofdId))
          && syncableForkSharedOfd(ofdId)
          && handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_none()
          && (sourceFdOfdIdSet.has(ofdId) || !sourceForkSharedOfdPeerState(handle, ofdId).incompatiblePeer)
          && (allowedOfdIdSet === null || allowedOfdIdSet.has(ofdId))
        )
    : [];
  let syncOfdIds = [...new Set([...publishableSourceHeldOfdIds, ...publishableSourceFdOfdIds, ...activeOfdIds])]
    .sort((left, right) => left - right);
  if (options.deltaOfdIds) {
    const sourceForkSharedFdOfdIds = sourceFdOfdIds
      .filter((ofdId) => {
        if (allowedOfdIdSet !== null && !allowedOfdIdSet.has(ofdId)) {
          return false;
        }
        if (sourcePublishedOfdIds.has(ofdId)) {
          return true;
        }
        if (sourceHeldOfdIds.has(ofdId) && hasPipePeerInOtherKernelShareMember(handle, ofdId)) {
          handle.kernelShare.sharedOfdIds.add(ofdId);
          return true;
        }
        return false;
      });
    const tracedOfdIds = collectTracedForkSharedOfdIds(handle, sourceVmPtr)
      .filter((ofdId) => {
        if (allowedOfdIdSet !== null && !allowedOfdIdSet.has(ofdId)) {
          return false;
        }
        if (sourcePublishedOfdIds.has(ofdId)) {
          return true;
        }
        if (sourceHeldOfdIds.has(ofdId) && hasPipePeerInOtherKernelShareMember(handle, ofdId)) {
          handle.kernelShare.sharedOfdIds.add(ofdId);
          return true;
        }
        return false;
      });
    syncKernelShareOfdReservations(handle);
    const deltaOfdIdSet = new Set([
      ...sourceActiveOfdIds,
      ...sourceForkSharedFdOfdIds,
      ...activeOfdIds,
      ...tracedOfdIds,
      ...trackedSourceOfdIds,
    ]);
    const staleOfdIdSet = new Set(staleOfdIds);
    for (const ofdId of expandPipePeerOfdIds(handle, deltaOfdIdSet)) {
      if (staleOfdIdSet.has(ofdId) || !syncableForkSharedOfd(ofdId)) {
        continue;
      }
      handle.kernelShare.sharedOfdIds.add(ofdId);
      deltaOfdIdSet.add(ofdId);
    }
    const deltaOfdIds = omitRuntimeNetworkSocketOfds([...deltaOfdIdSet])
      .filter(syncableForkSharedOfd)
      .sort((left, right) => left - right);
    deltaOfdIds.sort((left, right) => left - right);
    appendSocketSyncDebug(
      handle,
      [
        `fork-shared-delta:${handle.pid}`,
        `src=0x${sourceVmPtr.toString(16)}`,
        `source_fd=${formatOfdIdList(sourceFdOfdIds)}`,
        `source_shared_fd=${formatOfdIdList(sourceForkSharedFdOfdIds)}`,
        `source_held=${formatOfdIdList([...sourceHeldOfdIds].sort((left, right) => left - right))}`,
        `source_active=${formatOfdIdList(sourceActiveOfdIds)}`,
        `tracked_source=${formatOfdIdList(trackedSourceOfdIds)}`,
        `active=${formatOfdIdList(activeOfdIds)}`,
        `stale=${formatOfdIdList(staleOfdIds)}`,
        `traced=${formatOfdIdList(tracedOfdIds)}`,
        `runtime_network_socket=${formatOfdIdList([...runtimeNetworkSocketOfdIds].sort((left, right) => left - right))}`,
        `sync=${formatOfdIdList(deltaOfdIds)}`,
      ].join(":"),
    );
    if (deltaOfdIds.length === 0 && !memfsOnly) {
      lastForkSharedActiveOfdIdsByHandle.set(handle, new Set(sourceActiveOfdIds));
      return;
    }
    syncOfdIds = deltaOfdIds;
  } else {
    syncOfdIds = omitRuntimeNetworkSocketOfds(syncOfdIds).filter(syncableForkSharedOfd);
  }
  if (!memfsOnly && syncOfdIds.length > 0) {
    const syncOfdIdSet = new Set(syncOfdIds);
    syncTargets = syncTargets.filter(
      (target) => !targetHoldsSyncedRegularFileOfd(target, syncOfdIdSet),
    );
  }
  if (!memfsOnly && syncOfdIds.length > 0) {
    syncTargets = syncTargets.filter(
      (target) => !targetHasIncompatibleForkSharedOfd(handle, target, syncOfdIds),
    );
  }
  const processStateSnapshots = snapshotProcessStates(syncTargets);
  const mergeSourceOfdIds = omitRuntimeNetworkSocketOfds(
    sourceActiveOfdIds.filter((ofdId) => !sourceFdOfdIds.includes(ofdId)),
  );
  mergeKernelShareOfdStateInto(handle, mergeSourceOfdIds);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  const snapshotOfdIds = syncOfdIds;
  const ofdSlots: KernelWorkerOfdSlotSnapshot[] = [...snapshotKernelShareOfdSlots(handle, snapshotOfdIds)]
    .map(([ofdId, slot]) => ({ ofdId, slot }));
  const targetPids = syncTargets.map((target) => target.pid).filter((pid) => pid > 0);
  const targetKernelStates: KernelWorkerPidStateSnapshot[] = syncTargets
    .filter((target) => target.pid > 0)
    .map((target) => ({
      pid: target.pid,
      vmPtr: target.vmPtr,
      kernelState: target.kernelState,
    }));
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState(handle.kernelShare.members);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "sync-fork-shared-state",
      pid: handle.pid,
      vmPtr: options.sourceVmPtr ?? handle.vmPtr,
      targetPids,
      targetKernelStates,
      ofdIds: syncOfdIds,
      ofdSlots,
      fdEntries: sourceFdEntries,
      includeMemfs: options.includeMemfs ?? true,
      preserveSourceRefcounts: options.preserveSourceRefcounts,
      preservePublishedRegularFileData: options.preservePublishedRegularFileData,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "fork-shared-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-fork-shared-state: ${response.type}`);
  }
  if (response.forkSharedDebug) {
    appendForkSharedMemfsDebug(handle, `fork-shared-memfs:${handle.pid}:${response.forkSharedDebug}`);
  }
  for (const ofdId of syncOfdIds) {
    handle.kernelShare.sharedOfdIds.add(ofdId);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  handle.kernelState = {
    ...handle.kernelState,
    pipeUsed: composeHandlePipeUsed(handle, handle.kernelState.pipeUsed),
  };
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  restoreLocalOfdSlots(handle, preservedSourceFdOfdSlots);
  for (const snapshot of preservedRuntimeNetworkKernelStates) {
    restoreRuntimeNetworkKernelState(snapshot);
  }
  preserveHostStdioPipeControlForStep(handle, handle.vmPtr);
  for (const member of kernelShareOtherMembers(handle)) {
    if (member.runningWorker !== null || member.exitInProgress) {
      continue;
    }
    preserveHostStdioPipeControlForStep(member, member.vmPtr);
  }
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  lastForkSharedActiveOfdIdsByHandle.set(handle, new Set(sourceActiveOfdIds));
  pruneStaleKernelShareOfdIds(handle, staleOfdIds);
  if (!memfsOnly) {
    syncKernelShareOfdReservations(handle);
  }
  restoreProcessStates(processStateSnapshots);
  const heldAfterSync = collectProcessHeldOfds(handle);
  const unheldActiveOfdIds = activeKernelShareOfdIds(handle)
    .filter((ofdId) =>
      !heldAfterSync.has(ofdId)
      && (allowedOfdIdSet === null || allowedOfdIdSet.has(ofdId))
    );
  if (unheldActiveOfdIds.length > 0) {
    await rehydrateForkSharedStateViaKernelWorker([handle], unheldActiveOfdIds, {
      includeMemfs: false,
    });
  }
}

export async function rehydrateForkSharedStateViaKernelWorker(
  targets: ProcessHandle[],
  ofdIds: number[],
  options: { procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean } = {},
): Promise<void> {
  if (targets.length === 0) {
    return;
  }
  const owner = targets.find((target) => target.kernelWorkerRequest && target.pid > 0);
  if (!owner?.kernelWorkerRequest) {
    return;
  }
  const targetPids = [...new Set(targets.map((target) => target.pid).filter((pid) => pid > 0))];
  const targetKernelStates: KernelWorkerPidStateSnapshot[] = targets
    .filter((target) => target.pid > 0)
    .map((target) => {
      target.kernelState = readKernelRuntimeState(target.kernel);
      attachKernelRuntimeState(target.kernel, target.kernelState);
      return {
        pid: target.pid,
        vmPtr: target.vmPtr,
        kernelState: target.kernelState,
      };
    });
  if (targetPids.length === 0) {
    return;
  }
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState(targets);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await owner.kernelWorkerRequest({
      type: "rehydrate-fork-shared-state",
      targetPids,
      targetKernelStates,
      ofdIds,
      procSelfExeTargets: options.procSelfExeTargets,
      includeMemfs: options.includeMemfs,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "fork-shared-state-rehydrated") {
    throw new Error(
      `unexpected kernel-worker response for rehydrate-fork-shared-state: ${response.type}`,
    );
  }
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
}

export async function syncBlockedForkSharedStateViaKernelWorker(
  handle: ProcessHandle,
  sourceVmPtr = handle.vmPtr,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }
  const activeSync = handle.kernelShare.forkSharedSyncPromise ?? handle.forkSharedSyncPromise;
  if (activeSync !== null) {
    await activeSync;
  }
  const syncTargets = kernelShareOtherMembers(handle)
    .filter((member) => (
      member.runningWorker === null
      && !member.exitInProgress
      && !hasPendingBlockedRpc(member)
    ));
  const syncPromise = syncForkSharedStateViaKernelWorker(
    handle,
    syncTargets,
    {
      includeMemfs: false,
      deltaOfdIds: true,
      trackHeldOfds: true,
      sourceVmPtr,
    },
  ).finally(() => {
    if (handle.forkSharedSyncPromise === syncPromise) {
      handle.forkSharedSyncPromise = null;
    }
    if (handle.kernelShare.forkSharedSyncPromise === syncPromise) {
      handle.kernelShare.forkSharedSyncPromise = null;
    }
  });
  handle.forkSharedSyncPromise = syncPromise;
  handle.kernelShare.forkSharedSyncPromise = syncPromise;
  await syncPromise;
}

export async function syncPipeStateViaKernelWorker(handle: ProcessHandle): Promise<bigint> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return handle.kernelState.pipeUsed;
  }
  const processStateSnapshot = snapshotProcessState(handle);
  const fdEntries = snapshotProcessFdEntries(handle, handle.vmPtr);
  const fdEntryOfdIds = ofdIdsFromFdEntries(handle, fdEntries);
  const localPipeOfdIds = [...new Set([
    ...collectProcessHeldOfds(handle),
    ...fdEntryOfdIds,
  ])];
  const preservedFdOfdSlots = snapshotLocalOfdSlots(
    handle,
    fdEntryOfdIds.filter((ofdId) => !isRegularFileOfd(handle, ofdId)),
  );
  const runtimeNetworkSocketOfdIds = collectRuntimeNetworkSocketOfdIds([handle]);
  const preservedRuntimeNetworkSocketOfdSlots = snapshotRuntimeNetworkOfdSlots(
    handle,
    runtimeNetworkSocketOfdIds,
  );
  const preservedRuntimeNetworkFdEntries = snapshotRuntimeNetworkFdEntries(
    handle,
    handle.vmPtr,
    runtimeNetworkSocketOfdIds,
  );
  const preservedRuntimeNetworkKernelStates = [...handle.kernelShare.members].map((member) =>
    snapshotRuntimeNetworkKernelState(member)
  );
  const pipeOfdIds = [...new Set([
    ...collectProcessHeldOfds(handle),
    ...fdEntryOfdIds,
    ...activeKernelShareOfdIds(handle),
  ])]
    .filter((ofdId) => {
      const fdType = handle.kernel.kernel_ofd_fd_type(ofdId);
      return fdType === handle.kernel.abi_fd_type_pipe()
        || fdType === handle.kernel.abi_fd_type_pty_master?.()
        || fdType === handle.kernel.abi_fd_type_pty_slave?.();
    });
  const preservedLocalPipeOfdSlots = snapshotLocalOfdSlots(handle, pipeOfdIds.filter((ofdId) => (
    fdEntryOfdIds.includes(ofdId)
  )));
  const ofdSlots = [...snapshotKernelShareOfdSlots(handle, pipeOfdIds)]
    .map(([ofdId, slot]) => ({ ofdId, slot }));
  const pendingPipeSlots = handle.pendingPipeSlotSnapshots.slice();
  const hostOutputPipeSlots = snapshotNonEmptyHostOutputPipeSlots(handle, pendingPipeSlots);
  const pendingAndHostPipeSlots = [...pendingPipeSlots, ...hostOutputPipeSlots];
  const pipeSlots = [
    ...pendingPipeSlots,
    ...hostOutputPipeSlots,
    ...snapshotNonEmptyPipeSlotsForOfds(handle, localPipeOfdIds, pendingAndHostPipeSlots),
    ...snapshotFreshDuplexPipeSlots(handle, fdEntries, pendingAndHostPipeSlots),
  ];
  appendSocketSyncDebug(
    handle,
    `pipe-sync-request-slots:${handle.pid}:${formatPipeSnapshotDebug("slots", pipeSlots)}`,
  );
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState(handle.kernelShare.members);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  const applyFdEntries = handle.runningWorker === null
    && handle.readyWorkers.length === 0
    && handle.workers.size === 0
    && handle.blockedWorkers.size === 0;
  appendSocketSyncDebug(
    handle,
    [
      `pipe-sync-request:${handle.pid}`,
      `vm=0x${handle.vmPtr.toString(16)}`,
      `applyFd=${applyFdEntries ? 1 : 0}`,
      `running=${handle.runningWorker === null ? 0 : 1}`,
      `ready=${handle.readyWorkers.length}`,
      `workers=${handle.workers.size}`,
      `blocked=${handle.blockedWorkers.size}`,
      `stdioOwner=${handle.stdioOwner?.pid ?? 0}`,
      formatFdSnapshotDebug("fds", fdEntries),
      `pipeOfds=${formatOfdIdList(pipeOfdIds)}`,
      `localPipeOfds=${formatOfdIdList(localPipeOfdIds)}`,
      formatOfdSnapshotDebug("ofds", ofdSlots),
    ].join(":"),
  );
  try {
    response = await handle.kernelWorkerRequest({
      type: "sync-pipe-state",
      pid: handle.pid,
      vmPtr: handle.vmPtr,
      fdEntries,
      applyFdEntries,
      ofdSlots,
      pipeSlots,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "pipe-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-pipe-state: ${response.type}`);
  }
  appendSocketSyncDebug(
    handle,
    `pipe-sync-response-slots:${handle.pid}:${formatPipeSnapshotDebug("slots", response.pipeSlots ?? [])}`,
  );
  if (pendingPipeSlots.length > 0) {
    handle.pendingPipeSlotSnapshots.splice(0, pendingPipeSlots.length);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  if (response.pipeSlots && response.pipeSlots.length > 0) {
    mergePipeSlotSnapshotsIntoHandle(handle, response.pipeSlots);
    for (const member of kernelShareOtherMembers(handle)) {
      if (member.runningWorker !== null || member.exitInProgress) {
        continue;
      }
      mergePipeSlotSnapshotsIntoHandle(member, response.pipeSlots);
    }
  }
  if (hostOutputPipeSlots.length > 0) {
    mergePipeSlotSnapshotsIntoHandle(handle, hostOutputPipeSlots);
  }
  preserveHostStdioPipeControlForStep(handle, handle.vmPtr);
  if (pipeSlots.length > 0) {
    for (const member of kernelShareOtherMembers(handle)) {
      if (member.runningWorker !== null || member.exitInProgress) {
        continue;
      }
      mergePipeSlotSnapshotsIntoHandle(member, pipeSlots);
      preserveHostStdioPipeControlForStep(member, member.vmPtr);
    }
  }
  restoreLocalOfdSlots(handle, preservedFdOfdSlots);
  restoreRuntimeNetworkOfdSlots(handle, preservedRuntimeNetworkSocketOfdSlots);
  restoreProcessFdEntries(handle, handle.vmPtr, preservedRuntimeNetworkFdEntries);
  for (const snapshot of preservedRuntimeNetworkKernelStates) {
    restoreRuntimeNetworkKernelState(snapshot);
  }
  restoreIncompatibleLocalOfdSlots(handle, preservedLocalPipeOfdSlots);
  reconcileKernelSharePipeControl(handle);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  restoreProcessState(handle, processStateSnapshot, { preserveCurrentFdTable: true });
  return handle.kernelState.pipeUsed;
}

export async function syncEpollStateViaKernelWorker(handle: ProcessHandle): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }
  if (collectHeldEpollOfdIds(handle).length === 0) {
    return;
  }

  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "sync-epoll-state",
      pid: handle.pid,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "epoll-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-epoll-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

export async function syncOfdStateViaKernelWorker(handle: ProcessHandle, ofdIds: number[]): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0 || ofdIds.length === 0) {
    return;
  }

  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "sync-ofd-state",
      pid: handle.pid,
      ofdIds,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "ofd-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-ofd-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

export async function syncSocketStateViaKernelWorker(handle: ProcessHandle): Promise<{
  socketUsed: number;
  netBufUsed: bigint;
}> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return {
      socketUsed: handle.kernelState.socketUsed,
      netBufUsed: handle.kernelState.netBufUsed,
    };
  }

  const pidBeforeSync = handle.vmPtr === 0
    ? 0
    : handle.kernel.process_current_pid(handle.vmPtr);
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  appendSocketSyncDebug(handle, `socket-sync-start:${handle.pid}:${formatNetworkSocketDebug(handle)}`);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "sync-socket-state",
      pid: handle.pid,
      kernelState: handle.kernelState,
      socketState: snapshotSocketStateForKernelWorker(handle),
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
    appendSocketSyncDebug(handle, `socket-sync-finally-restore:${handle.pid}:${formatNetworkSocketDebug(handle)}`);
  }
  if (response.type !== "socket-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-socket-state: ${response.type}`);
  }
  const localNetworkProgress = networkSocketSnapshots.some(networkBridgeSocketSnapshotProgressed);
  handle.kernelState = response.socketState && !localNetworkProgress
    ? adoptKernelState(handle.kernelState, response.kernelState)
    : adoptKernelStateForHandle(handle, response.kernelState);
  if (response.socketState && !localNetworkProgress) {
    applySocketStateFromKernelWorker(handle, response.socketState);
    appendSocketSyncDebug(
      handle,
      `socket-sync-response-apply:${handle.pid}:${formatNetworkSocketDebug(handle)}`,
    );
  } else if (response.socketState) {
    appendSocketSyncDebug(handle, `socket-sync-response-skip-progress:${handle.pid}:${formatNetworkSocketDebug(handle)}`);
  }
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  appendSocketSyncDebug(handle, `socket-sync-restore:${handle.pid}:${formatNetworkSocketDebug(handle)}`);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  repairHandleVmPidAfterKernelStateAttach(handle, handle.vmPtr, pidBeforeSync);
  return {
    socketUsed: handle.kernelState.socketUsed,
    netBufUsed: handle.kernelState.netBufUsed,
  };
}

export async function releaseHostStdioViaKernelWorker(handle: ProcessHandle): Promise<bigint> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    handle.kernel.process_release_host_stdio(handle.vmPtr);
    return handle.kernelState.pipeUsed;
  }

  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "release-host-stdio",
      pid: handle.pid,
      vmPtr: handle.vmPtr,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "host-stdio-released") {
    throw new Error(`unexpected kernel-worker response for release-host-stdio: ${response.type}`);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  return handle.kernelState.pipeUsed;
}
