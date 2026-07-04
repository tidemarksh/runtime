import { attachKernelRuntimeState, readKernelRuntimeState } from "../../abi.js";
import { kernelNetBufferCount } from "../../kernel-socket-layout.js";
import {
  NET_BUFFER_HEAD_OFFSET,
  NET_BUFFER_HEADER_BYTES,
  NET_BUFFER_TAIL_OFFSET,
} from "../../layout/net-buffer.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../layout/ofd.js";
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
} from "../../layout/pipe.js";
import { SOCKET_STATE_SHUTDOWN } from "../../layout/socket.js";
import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPipeSlotSnapshot,
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPidStateSnapshot,
  KernelWorkerSocketStateSnapshot,
} from "../../messages.js";
import {
  copyLegacySharedKernelObjectsLocal,
  kernelShareOtherMembers,
  reconcileKernelSharePipeControl,
  restoreLocalOfdSlots,
} from "../kernel-share.js";
import {
  mergeKernelShareOfdStateInto,
  pruneStaleKernelShareOfdIds,
  reconcileForkSharedOfdRefcounts,
  snapshotKernelShareOfdSlots,
  syncKernelShareOfdReservations,
} from "../fork-shared.js";
import {
  restoreRuntimeNetworkOfdSlots,
  snapshotRuntimeNetworkOfdSlots,
  syncPipeOfdGenerationsForSlot,
} from "../ownership/ofd-table.js";
import {
  supervisorOfdPipeSlotModeMap,
  supervisorOfdPipeSlots,
} from "../ownership/pipe-slot.js";
import {
  restoreProcessFdEntries,
  snapshotProcessFdEntries,
} from "../ownership/fd-table.js";
import {
  collectRuntimeNetworkSocketOfdIds,
  kernelSocketOutboundAvailable,
  preserveNetworkSocketSlots,
  restoreNetworkSocketSlotsForHandle,
  runtimeNetworkFds,
} from "../ownership/socket-control.js";
import type { ProcessHandle } from "../types.js";
import {
  forceNetworkClosedFds,
  networkSocketSlotRecvBufferIndex,
  restoreNetworkSocketSlotSnapshot,
} from "../network-socket-close.js";
import {
  netBufUsedMaskFromBufferIndices,
  preserveNetworkRecvHeadWatermarks as preserveNetworkRecvHeadWatermarksCommon,
  preserveUsedNetBufferHeads,
  preservedSocketRecvBufferIndices,
  readNetworkRecvBufferDebug,
  restoreMergedNetBufferHeads as restoreMergedNetBufferHeadsCommon,
  type PreservedNetBufferHead,
} from "../network-recv-buffer.js";
import {
  adoptKernelState,
  adoptKernelStateForHandle,
  repairHandleVmPidAfterKernelStateAttach,
} from "../kernel-state.js";
import { appendForkSharedMemfsDebug } from "./memfs.js";
import { describeSyncCause, type SyncCause } from "../contracts/sync.js";

export const lastForkSharedActiveOfdIdsByHandle = new WeakMap<ProcessHandle, Set<number>>();
export const SOCK_STATE_SHUTDOWN = SOCKET_STATE_SHUTDOWN;

export function formatSyncCause(cause: SyncCause): string {
  return `cause=${describeSyncCause(cause)}`;
}

export const socketSyncDebugLogEnabled = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_WORKER_DEBUG_LOG ?? "") === "1"
  || ((globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_WORKER_DEBUG_NETWORK ?? "") === "1";

export function appendSocketSyncDebug(handle: ProcessHandle, event: string): void {
  if (!socketSyncDebugLogEnabled) {
    return;
  }
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
}

export function int32AtomicView(buffer: ArrayBufferLike): Int32Array | null {
  return buffer instanceof SharedArrayBuffer ? new Int32Array(buffer) : null;
}

export function loadPipeU32(view: DataView, atomicView: Int32Array | null, byteOffset: number): number {
  if (atomicView !== null && byteOffset % 4 === 0) {
    return Atomics.load(atomicView, byteOffset / 4) >>> 0;
  }
  return view.getUint32(byteOffset, true);
}

export function loadPipeI32(view: DataView, atomicView: Int32Array | null, byteOffset: number): number {
  if (atomicView !== null && byteOffset % 4 === 0) {
    return Atomics.load(atomicView, byteOffset / 4);
  }
  return view.getInt32(byteOffset, true);
}

export function formatPipeSnapshotDebug(
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

export interface NetworkBridgeSocketStateSnapshot {
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

export interface PreservedRuntimeNetworkKernelState {
  handle: ProcessHandle;
  vmPtr: number;
  ofdSlots: Map<number, Uint8Array>;
  fdEntries: KernelWorkerFdEntrySnapshot[];
}

const networkSocketMetaDebug = new WeakMap<ProcessHandle, Map<number, string>>();

export function recordNetworkSocketMetaTransition(
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
  if (!socketSyncDebugLogEnabled) {
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

export function formatFdSnapshotDebug(
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

export function formatOfdSnapshotDebug(
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

export function formatOfdIdList(ofdIds: readonly number[]): string {
  return ofdIds.length > 0 ? ofdIds.join(".") : "none";
}

export function mergePipeSlotSnapshotsIntoHandle(
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

export function readOptionalTraceU64(value: bigint | number | undefined): bigint | null {
  if (value === undefined) {
    return null;
  }
  return typeof value === "bigint" ? value : BigInt(value);
}

export function collectFreshPipe2Slots(
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
    if (!liveOfdIds.has(ofdId) || ofdId >= handle.kernel.abi_max_fds()) {
      continue;
    }
    for (const pipeSlot of supervisorOfdPipeSlots(handle, ofdId)) {
      freshPipeSlots.add(pipeSlot);
    }
  }
  return freshPipeSlots;
}

export function snapshotFreshDuplexPipeSlots(
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
    for (const [pipeSlot, mode] of supervisorOfdPipeSlotModeMap(handle, ofdId)) {
      if (
        pipeSlot <= 0
        || pipeSlot >= maxPipes
        || (pendingNonEmptyFullSlots.has(pipeSlot) && !freshPipe2Slots.has(pipeSlot))
      ) {
        continue;
      }
      slotModes.set(pipeSlot, (slotModes.get(pipeSlot) ?? 0) | mode);
    }
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

export function snapshotNonEmptyPipeSlotsForOfds(
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
    for (const [pipeSlot, mode] of supervisorOfdPipeSlotModeMap(handle, ofdId)) {
      if (pipeSlot > 0 && pipeSlot < maxPipes && !pendingSlots.has(pipeSlot)) {
        pipeSlotModes.set(pipeSlot, (pipeSlotModes.get(pipeSlot) ?? 0) | mode);
      }
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

export function snapshotNonEmptyHostOutputPipeSlots(
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

export function formatNetworkSocketDebug(handle: ProcessHandle): string {
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

export function networkSocketFdStats(handle: ProcessHandle, fd: number): {
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

export function closeNetworkClosedFdPeers(handle: ProcessHandle): void {
  forceNetworkClosedFds(handle);
}

export function restoreClosedNetworkSocketSlots(handle: ProcessHandle): void {
  if ((handle.networkClosedSocketSlots?.size ?? 0) === 0) {
    return;
  }
  for (const slot of handle.networkClosedSocketSlots?.values() ?? []) {
    restoreNetworkSocketSlotSnapshot(handle, slot);
  }
}

export function currentNetBufUsed(handle: ProcessHandle): bigint {
  return handle.kernelState.netBufUsed | handle.kernel.kernel_net_buf_used();
}

export function currentSocketUsed(handle: ProcessHandle): number {
  return handle.kernelState.socketUsed | handle.kernel.kernel_socket_used();
}

export function netBufferSlotBytesForHandle(handle: ProcessHandle): number {
  const maxBuffers = kernelNetBufferCount(handle.kernel);
  const poolBytes = handle.kernel.kernel_net_buf_pool_bytes();
  const slotBytes = Math.floor(poolBytes / maxBuffers);
  return Number.isInteger(slotBytes) && slotBytes > 0 ? slotBytes : 0;
}

export function snapshotUsedNetBufferSlots(
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

export function restoreNetBufferSlots(
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

export function socketSnapshotNetBufPoolBytes(handle: ProcessHandle, snapshot: KernelWorkerSocketStateSnapshot): number {
  return snapshot.netBufPool?.length ?? handle.kernel.kernel_net_buf_pool_bytes();
}

export function applyKernelWorkerNetBuffers(
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
    kernelNetBufferCount(handle.kernel),
    snapshot.netBufSlots ?? [],
  );
}

export function networkBridgeOutboundAvailable(handle: ProcessHandle): boolean {
  const fds = [...new Set([
    ...(handle.networkConnections?.keys() ?? []),
    ...(handle.networkPendingConnectFds ?? []),
  ])];
  return fds.some((fd) => kernelSocketOutboundAvailable(handle, handle.vmPtr, fd) > 0);
}

const syncNetworkRecvDebug = {
  source: "sync",
  append: appendSocketSyncDebug,
} as const;

export function preserveNetworkRecvHeadWatermarks(
  handle: ProcessHandle,
  netBufPoolBytes: number,
): PreservedNetBufferHead[] {
  return preserveNetworkRecvHeadWatermarksCommon(handle, netBufPoolBytes, syncNetworkRecvDebug);
}

export function restoreMergedNetBufferHeads(
  memory: Uint8Array,
  netBufBase: number,
  preserved: PreservedNetBufferHead[],
): void {
  restoreMergedNetBufferHeadsCommon(
    memory,
    netBufBase,
    preserved,
    { restoreDataOnEqualTail: true, restorePlainDataOnTail: true },
  );
}

export function snapshotNetworkBridgeSocketState(
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
    const maxNetBuffers = kernelNetBufferCount(handle.kernel);
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
    for (const ofdId of collectRuntimeNetworkSocketOfdIds([handle])) {
      if (ofdSlotBytes <= 0 || ofdId <= 0) {
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

export function snapshotSocketStateForKernelWorker(handle: ProcessHandle): KernelWorkerSocketStateSnapshot {
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  const memory = new Uint8Array(handle.memory.buffer);
  const socketTableBytes = handle.kernel.kernel_socket_table_bytes();
  const netBufUsed = currentNetBufUsed(handle);
  const maxNetBuffers = kernelNetBufferCount(handle.kernel);
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
  const preservedSocketSlots = preserveNetworkSocketSlots(
    handle,
    handle.vmPtr,
    handle.kernel.kernel_socket_table_bytes(),
    { pruneMissingClosedFds: false, preserveWatermarkedSlots: "recv-payload" },
  );
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
  restoreNetworkSocketSlotsForHandle(handle, preservedSocketSlots);
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

export function restoreNetworkBridgeSocketState(
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
    const preservedSocketSlots = preserveNetworkSocketSlots(
      handle,
      handle.vmPtr,
      handle.kernel.kernel_socket_table_bytes(),
      { pruneMissingClosedFds: false, preserveWatermarkedSlots: "recv-payload" },
    );
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
      kernelNetBufferCount(handle.kernel),
      snapshot.netBufSlots,
    );
    restoreNetworkSocketSlotsForHandle(handle, preservedSocketSlots);
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

export function networkBridgeSocketSnapshotProgressed(snapshot: NetworkBridgeSocketStateSnapshot): boolean {
  const { handle } = snapshot;
  return snapshot.fdStats.some((stat) => {
    const current = networkSocketFdStats(handle, stat.fd);
    return current.recvAvailable !== stat.recvAvailable
      || current.readCount !== stat.readCount
      || current.state !== stat.state;
  });
}

export function snapshotRuntimeNetworkFdEntries(
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

export function snapshotRuntimeNetworkKernelState(
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

export function restoreRuntimeNetworkKernelState(
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

export {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  NET_BUFFER_HEAD_OFFSET,
  NET_BUFFER_HEADER_BYTES,
  NET_BUFFER_TAIL_OFFSET,
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_TYPE_OFFSET,
  PIPE_DEBUG_GENERATION_OFFSET,
  PIPE_DEBUG_HEAD_OFFSET,
  PIPE_DEBUG_HEADER_BYTES,
  PIPE_DEBUG_READ_OPEN_OFFSET,
  PIPE_SLOT_MODE_CONTROL,
  PIPE_SLOT_MODE_READ,
  PIPE_SLOT_MODE_WRITE,
  PIPE_DEBUG_TAIL_OFFSET,
  PIPE_DEBUG_WRITE_OPEN_OFFSET,
  SOCKET_STATE_SHUTDOWN,
  copyLegacySharedKernelObjectsLocal,
  kernelShareOtherMembers,
  reconcileKernelSharePipeControl,
  restoreLocalOfdSlots,
  mergeKernelShareOfdStateInto,
  pruneStaleKernelShareOfdIds,
  reconcileForkSharedOfdRefcounts,
  snapshotKernelShareOfdSlots,
  syncKernelShareOfdReservations,
  forceNetworkClosedFds,
  networkSocketSlotRecvBufferIndex,
  restoreNetworkSocketSlotSnapshot,
  adoptKernelState,
  adoptKernelStateForHandle,
  repairHandleVmPidAfterKernelStateAttach,
  appendForkSharedMemfsDebug,
  describeSyncCause,
};

export type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPipeSlotSnapshot,
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPidStateSnapshot,
  KernelWorkerSocketStateSnapshot,
  ProcessHandle,
  SyncCause,
};
