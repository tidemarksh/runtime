import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import {
  activeKernelShareOfdIds,
  collectProcessHeldOfds,
  hasPlausibleOfdState,
} from "../ownership/ofd-table.js";
import { ensureKernelMemoryCapacity, kernelScratchBase } from "../kernel-scratch.js";
import { adoptKernelStateForHandle } from "../sync.js";
import {
  createTransferDebugStats,
  type ProcessHandle,
  type ThreadWorkerState,
} from "../types.js";
import type { KernelWorkerPidBytesSnapshot } from "../../messages.js";
import {
  FS_OP_READ,
  FS_OP_WRITE,
  FS_RING_COMPLETE_HEAD_OFFSET,
  FS_RING_COMPLETE_TAIL_OFFSET,
  FS_RING_REQUEST_OPCODE_OFFSET,
  FS_RING_REQUEST_VM_PTR_OFFSET,
  FS_RING_RESPONSE_RESULT_OFFSET,
  FS_RING_RESPONSE_VM_PTR_OFFSET,
  FS_RING_SUBMIT_HEAD_OFFSET,
  FS_RING_SUBMIT_TAIL_OFFSET,
  FS_RING_TOTAL_BYTES,
  fsRingCompletionEntryOffset,
  fsRingRequestEntryOffset,
} from "../../layout/fs-ring.js";
import {
  completedKernelRpcOutcome,
  type KernelRpcResumeEffect,
  type KernelRpcResumeOutcome,
} from "./outcomes.js";
import { expectKernelWorkerResponse } from "./transport.js";

const FS_DEBUG_PATH_BUF_LEN = 4096;
const FS_LIFECYCLE_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_LIFECYCLE_DEBUG ?? "") === "1";

interface PendingFsContext {
  opcode: number;
  effects: number;
  targetIno: number;
  path: string | null;
  secondaryPath: string | null;
}

interface FsFdStateSnapshot {
  vmPtr: number;
  fdEntries: Array<{ fd: number; ofdId: number; fdFlags: number }>;
  ofdTablePtr: number;
  ofdTable: Uint8Array | null;
}

interface FsRingProgressSnapshot {
  vmPtr: number;
  ringBase: number;
  submitHead: number;
  submitTail: number;
  completeHead: number;
  completeTail: number;
}

export interface WorkerKernelRpcFsDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void;
  blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  kickBlockedWorkers(handle: ProcessHandle): void;
  markRuntimeFsDirty(handle: ProcessHandle): void;
  kernelShareOtherMembers(handle: ProcessHandle): ProcessHandle[];
  syncForkSharedStateLocallyAfterFs(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options?: { includeMemfs?: boolean; trackHeldOfds?: boolean; ofdIds?: number[] },
  ): void;
  syncDirtyMemfsPeerViaKernelWorker(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options?: {
      includeMemfs?: boolean;
      trackHeldOfds?: boolean;
      deltaOfdIds?: boolean;
      memfsOnly?: boolean;
      preserveSourceRefcounts?: boolean;
      preservePublishedRegularFileData?: boolean;
      sourceVmPtr?: number;
      ofdIds?: number[];
    },
  ): Promise<void>;
  syncSharedOfdStateAfterFs(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options: { sourceVmPtr: number },
  ): Promise<void>;
  publishMemfsAfterFs(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options: { preservePublishedRegularFileData: boolean; sourceVmPtr: number },
  ): Promise<void>;
  rehydrateForkSharedStateViaKernelWorker(
    targets: ProcessHandle[],
    ofdIds: number[],
    options?: { procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean },
  ): Promise<void>;
  publishRuntimeKernelState(handle: ProcessHandle): void;
}

export function createWorkerKernelRpcFsHelpers(deps: WorkerKernelRpcFsDeps) {
  const fsResumeInFlight = new WeakMap<ProcessHandle, Map<string, Promise<KernelRpcResumeOutcome>>>();
  const decoder = new TextDecoder();

function decodeKernelBytes(memory: WebAssembly.Memory, ptr: number, length: number): string {
  const bytes = new Uint8Array(length);
  bytes.set(new Uint8Array(memory.buffer, ptr, length));
  return decoder.decode(bytes);
}

function readPendingFsPath(handle: ProcessHandle, vmPtr: number, secondary = false): string | null {
  const absolutePath = secondary
    ? handle.kernel.thread_pending_fs_secondary_absolute_path
    : handle.kernel.thread_pending_fs_absolute_path;
  if (!absolutePath) {
    return null;
  }
  const bufPtr = kernelScratchBase(handle.kernel) + 0x8000 + (secondary ? FS_DEBUG_PATH_BUF_LEN : 0);
  ensureKernelMemoryCapacity(handle.kernel, bufPtr + FS_DEBUG_PATH_BUF_LEN);
  const length = Number(absolutePath(vmPtr, bufPtr, FS_DEBUG_PATH_BUF_LEN) ?? 0n);
  if (length <= 0 || bufPtr + length > handle.kernel.memory.buffer.byteLength) {
    return null;
  }
  return decodeKernelBytes(handle.kernel.memory, bufPtr, length);
}

function readPendingFsContext(handle: ProcessHandle, vmPtr: number): PendingFsContext {
  return {
    opcode: handle.kernel.thread_pending_fs_opcode(vmPtr),
    effects: handle.kernel.thread_pending_fs_effects(vmPtr),
    targetIno: handle.kernel.thread_pending_fs_target_ino(vmPtr),
    path: readPendingFsPath(handle, vmPtr),
    secondaryPath: readPendingFsPath(handle, vmPtr, true),
  };
}

function formatPendingFsDebugSuffix(context: PendingFsContext): string {
  const pathSuffix = context.path ? `:path=${JSON.stringify(context.path)}` : "";
  const secondaryPathSuffix = context.secondaryPath
    ? `:path2=${JSON.stringify(context.secondaryPath)}`
    : "";
  return `:op=${context.opcode}:fx=${context.effects}:ino=${context.targetIno}${pathSuffix}${secondaryPathSuffix}`;
}

function appendFsLifecycleEvent(
  handle: ProcessHandle,
  phase: string,
  vmPtr: number,
  detail = "",
): void {
  const event = `fs-${phase}:${handle.pid}:0x${vmPtr.toString(16)}${detail}`;
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    event,
  );
  if (FS_LIFECYCLE_DEBUG_ENABLED) {
    console.error(`[tidemark-fs] ${event}`);
  }
}

function formatFsRingDebug(handle: ProcessHandle, vmPtr: number): string {
  const progress = snapshotFsRingProgress(handle, vmPtr);
  if (progress === null) {
    return "";
  }
  const {
    ringBase,
    submitHead,
    submitTail,
    completeHead,
    completeTail,
  } = progress;
  const view = new DataView(handle.memory.buffer);
  const requests: string[] = [];
  for (let pos = submitTail; pos < submitHead && requests.length < 4; pos += 1) {
    const entry = fsRingRequestEntryOffset(ringBase, pos);
    const opcode = view.getUint32(entry + FS_RING_REQUEST_OPCODE_OFFSET, true);
    const requestVmPtr = view.getUint32(entry + FS_RING_REQUEST_VM_PTR_OFFSET, true);
    requests.push(`${pos}:op=${opcode}/vm=0x${requestVmPtr.toString(16)}`);
  }
  const completions: string[] = [];
  for (let pos = completeTail; pos < completeHead && completions.length < 4; pos += 1) {
    const entry = fsRingCompletionEntryOffset(ringBase, pos);
    const result = view.getBigInt64(entry + FS_RING_RESPONSE_RESULT_OFFSET, true);
    const completionVmPtr = view.getUint32(entry + FS_RING_RESPONSE_VM_PTR_OFFSET, true);
    completions.push(`${pos}:rv=${result.toString()}/vm=0x${completionVmPtr.toString(16)}`);
  }
  return [
    `:ring=0x${ringBase.toString(16)}`,
    `:sq=${submitTail}->${submitHead}[${requests.join("|")}]`,
    `:cq=${completeTail}->${completeHead}[${completions.join("|")}]`,
  ].join("");
}

function fsRingHasPendingRequests(handle: ProcessHandle, vmPtr: number): boolean {
  const progress = snapshotFsRingProgress(handle, vmPtr);
  return progress !== null && progress.submitHead !== progress.submitTail;
}

function fsRingHasPendingRequestForVm(handle: ProcessHandle, vmPtr: number): boolean {
  const progress = snapshotFsRingProgress(handle, vmPtr);
  if (progress === null) {
    return false;
  }
  const view = new DataView(handle.memory.buffer);
  const { ringBase, submitHead, submitTail } = progress;
  for (let pos = submitTail; pos < submitHead; pos += 1) {
    const entry = fsRingRequestEntryOffset(ringBase, pos);
    const requestVmPtr = view.getUint32(entry + FS_RING_REQUEST_VM_PTR_OFFSET, true);
    if (requestVmPtr === vmPtr) {
      return true;
    }
  }
  return false;
}

function fsRingHasPendingCompletionForVm(handle: ProcessHandle, vmPtr: number): boolean {
  const progress = snapshotFsRingProgress(handle, vmPtr);
  if (progress === null) {
    return false;
  }
  const view = new DataView(handle.memory.buffer);
  const { ringBase, completeHead, completeTail } = progress;
  for (let pos = completeTail; pos < completeHead; pos += 1) {
    const entry = fsRingCompletionEntryOffset(ringBase, pos);
    const completionVmPtr = view.getUint32(entry + FS_RING_RESPONSE_VM_PTR_OFFSET, true);
    if (completionVmPtr === vmPtr) {
      return true;
    }
  }
  return false;
}

function snapshotFsRingProgress(handle: ProcessHandle, vmPtr: number): FsRingProgressSnapshot | null {
  let ringBase = 0;
  try {
    ringBase = handle.kernel.thread_fs_ring_base?.(vmPtr) ?? 0;
  } catch {
    return null;
  }
  if (ringBase <= 0 || ringBase + FS_RING_TOTAL_BYTES > handle.memory.buffer.byteLength) {
    return null;
  }
  const view = new DataView(handle.memory.buffer);
  return {
    vmPtr,
    ringBase,
    submitHead: view.getUint32(ringBase + FS_RING_SUBMIT_HEAD_OFFSET, true),
    submitTail: view.getUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, true),
    completeHead: view.getUint32(ringBase + FS_RING_COMPLETE_HEAD_OFFSET, true),
    completeTail: view.getUint32(ringBase + FS_RING_COMPLETE_TAIL_OFFSET, true),
  };
}

function counterBehind(current: number, target: number): boolean {
  const distance = (target - current) >>> 0;
  return distance !== 0 && distance < 0x80000000;
}

function restoreFsRingProgress(handle: ProcessHandle, snapshot: FsRingProgressSnapshot | null): void {
  if (!snapshot) {
    return;
  }
  const ringBase = handle.kernel.thread_fs_ring_base?.(snapshot.vmPtr) ?? 0;
  if (
    ringBase !== snapshot.ringBase
    || ringBase <= 0
    || ringBase + FS_RING_COMPLETE_TAIL_OFFSET + 4 > handle.memory.buffer.byteLength
  ) {
    return;
  }
  const view = new DataView(handle.memory.buffer);
  const advance = (offset: number, target: number): void => {
    const current = view.getUint32(ringBase + offset, true);
    if (counterBehind(current, target)) {
      view.setUint32(ringBase + offset, target, true);
    }
  };
  advance(FS_RING_SUBMIT_HEAD_OFFSET, snapshot.submitHead);
  advance(FS_RING_SUBMIT_TAIL_OFFSET, snapshot.submitTail);
  advance(FS_RING_COMPLETE_HEAD_OFFSET, snapshot.completeHead);
  advance(FS_RING_COMPLETE_TAIL_OFFSET, snapshot.completeTail);
}

function fsResumeKey(handle: ProcessHandle, vmPtr: number): string {
  const pendingFs = readPendingFsContext(handle, vmPtr);
  const submitHead = handle.kernel.thread_fs_ring_submit_head?.(vmPtr) ?? 0;
  const submitTail = handle.kernel.thread_fs_ring_submit_tail?.(vmPtr) ?? 0;
  return [
    vmPtr,
    submitHead,
    submitTail,
    pendingFs.opcode,
    pendingFs.effects,
    pendingFs.targetIno,
    pendingFs.path ?? "",
    pendingFs.secondaryPath ?? "",
  ].join(":");
}

function normalizeFsResult(result: bigint | number): bigint {
  return typeof result === "bigint" ? result : BigInt(result);
}

function isPackageIndexTransferPath(path: string | null): boolean {
  return path?.includes("_Packages.xz") === true || path?.endsWith("/Packages") === true;
}

function recordFsTransferStats(
  handle: ProcessHandle,
  pendingFs: PendingFsContext,
  fsResult: bigint | number,
  fsSucceeded: boolean,
  fsEffects: { memfsMutation: number; memfsFileData: number; sharedOfdState: number },
  viaKernelWorker: boolean,
  memfsPublishReady: boolean,
): void {
  handle.transferDebugStats ??= createTransferDebugStats();
  const stats = handle.transferDebugStats;
  stats.fsResumeCalls += 1;
  if (viaKernelWorker) {
    stats.fsKernelWorkerCalls += 1;
  } else {
    stats.fsLocalCalls += 1;
  }
  if (!fsSucceeded) {
    return;
  }

  if ((pendingFs.effects & fsEffects.memfsMutation) !== 0) {
    stats.memfsMutationOps += 1;
  }
  if ((pendingFs.effects & fsEffects.memfsFileData) !== 0) {
    stats.memfsFileDataOps += 1;
  }
  if (memfsPublishReady) {
    stats.pageCachePublishCandidates += 1;
  }

  const byteCount = Number(BigInt.asIntN(64, normalizeFsResult(fsResult)));
  if (!Number.isFinite(byteCount) || byteCount <= 0) {
    return;
  }
  if (pendingFs.opcode === FS_OP_READ) {
    stats.fsReadCalls += 1;
    stats.fsReadBytes += byteCount;
  } else if (pendingFs.opcode === FS_OP_WRITE) {
    stats.fsWriteCalls += 1;
    stats.fsWriteBytes += byteCount;
    if (isPackageIndexTransferPath(pendingFs.path)) {
      stats.packagePathWriteCalls += 1;
      stats.packagePathWriteBytes += byteCount;
    }
  }
}

function fsResultIsPending(handle: ProcessHandle, vmPtr: number, fsResult: bigint | number): boolean {
  const result = Number(BigInt.asIntN(64, normalizeFsResult(fsResult)));
  const eagain = handle.kernel.abi_eagain?.() ?? 11;
  const fsStatus = handle.kernel.abi_status_fs?.() ?? -1;
  return result === eagain
    && handle.kernel.thread_status?.(vmPtr) === fsStatus;
}

function clearStaleFsStatusAfterResume(handle: ProcessHandle, vmPtr: number, fsResult: bigint | number): void {
  const result = Number(BigInt.asIntN(64, normalizeFsResult(fsResult)));
  const eagain = handle.kernel.abi_eagain?.() ?? 11;
  const fsStatus = handle.kernel.abi_status_fs?.() ?? -1;
  if (
    result === eagain
    || handle.kernel.thread_status?.(vmPtr) !== fsStatus
    || fsRingHasPendingRequestForVm(handle, vmPtr)
    || fsRingHasPendingCompletionForVm(handle, vmPtr)
  ) {
    return;
  }
  const before = formatFsRingDebug(handle, vmPtr);
  const clearedResult = handle.kernel.thread_resume_fs(vmPtr);
  appendFsLifecycleEvent(
    handle,
    "clear-stale-status",
    vmPtr,
    `:rv=${clearedResult.toString()}:status=${handle.kernel.thread_status(vmPtr)}${before}`,
  );
}

function snapshotFsFdState(handle: ProcessHandle, vmPtr: number): FsFdStateSnapshot | null {
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  if (vmPtr === 0 || maxFds <= 0 || !handle.kernel.process_fd_ofd_id || !handle.kernel.process_fd_flags) {
    return null;
  }
  const fdEntries: FsFdStateSnapshot["fdEntries"] = [];
  for (let fd = 0; fd < maxFds; fd += 1) {
    fdEntries.push({
      fd,
      ofdId: handle.kernel.process_fd_ofd_id(vmPtr, fd),
      fdFlags: handle.kernel.process_fd_flags(vmPtr, fd),
    });
  }
  const tableBytes = handle.kernel.kernel_ofd_table_bytes?.() ?? 0;
  const ofdTablePtr = handle.kernelState.ofdTablePtr;
  let ofdTable: Uint8Array | null = null;
  if (tableBytes > 0 && ofdTablePtr > 0) {
    const memory = new Uint8Array(handle.memory.buffer);
    if (ofdTablePtr + tableBytes <= memory.byteLength) {
      ofdTable = memory.slice(ofdTablePtr, ofdTablePtr + tableBytes);
    }
  }
  return { vmPtr, fdEntries, ofdTablePtr, ofdTable };
}

function restoreFsFdState(handle: ProcessHandle, snapshot: FsFdStateSnapshot | null): void {
  if (!snapshot || snapshot.vmPtr === 0 || !handle.kernel.process_set_fd_entry) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds?.() ?? snapshot.fdEntries.length;
  const tableBytes = handle.kernel.kernel_ofd_table_bytes?.() ?? 0;
  const ofdSlotBytes = maxFds > 0 ? tableBytes / maxFds : 0;
  const canRestoreOfdSlot = snapshot.ofdTable !== null
    && snapshot.ofdTablePtr > 0
    && Number.isInteger(ofdSlotBytes)
    && ofdSlotBytes > 0;
  const memory = canRestoreOfdSlot ? new Uint8Array(handle.memory.buffer) : null;
  for (const entry of snapshot.fdEntries) {
    if (entry.fd < 0 || entry.fd >= maxFds) {
      continue;
    }
    const currentOfdId = handle.kernel.process_fd_ofd_id?.(snapshot.vmPtr, entry.fd) ?? entry.ofdId;
    const currentFdFlags = handle.kernel.process_fd_flags?.(snapshot.vmPtr, entry.fd) ?? entry.fdFlags;
    if (currentOfdId === entry.ofdId && currentFdFlags === entry.fdFlags) {
      continue;
    }
    handle.kernel.process_set_fd_entry(snapshot.vmPtr, entry.fd, entry.ofdId, entry.fdFlags);
    const currentOfdType = entry.ofdId > 0
      ? handle.kernel.kernel_ofd_fd_type?.(entry.ofdId) ?? 0
      : 0;
    if (
      canRestoreOfdSlot
      && memory
      && snapshot.ofdTable
      && entry.ofdId > 0
      && entry.ofdId < maxFds
      && currentOfdType === 0
    ) {
      const sourceOffset = entry.ofdId * ofdSlotBytes;
      const targetOffset = snapshot.ofdTablePtr + sourceOffset;
      if (
        sourceOffset + ofdSlotBytes <= snapshot.ofdTable.length
        && targetOffset + ofdSlotBytes <= memory.byteLength
      ) {
        memory.set(
          snapshot.ofdTable.subarray(sourceOffset, sourceOffset + ofdSlotBytes),
          targetOffset,
        );
      }
    }
  }
}

async function resumeFsViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  fsEffects: { memfsMutation: number; memfsFileData: number; sharedOfdState: number },
): Promise<KernelRpcResumeOutcome> {
  let byVmPtr = fsResumeInFlight.get(handle);
  const key = fsResumeKey(handle, vmPtr);
  const existing = byVmPtr?.get(key);
  if (existing) {
    deps.appendWorkerDebugLog(handle, `fs-join:0x${vmPtr.toString(16)}`);
    appendFsLifecycleEvent(handle, "join", vmPtr, "");
    await existing;
    return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr));
  }
  if (byVmPtr === undefined) {
    byVmPtr = new Map();
    fsResumeInFlight.set(handle, byVmPtr);
  }
  const resume = resumeFsViaKernelWorkerInner(handle, worker, vmPtr, fsEffects);
  byVmPtr.set(key, resume);
  try {
    return await resume;
  } finally {
    if (byVmPtr.get(key) === resume) {
      byVmPtr.delete(key);
    }
    if (byVmPtr.size === 0) {
      fsResumeInFlight.delete(handle);
    }
  }
}

async function resumeFsViaKernelWorkerInner(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  fsEffects: { memfsMutation: number; memfsFileData: number; sharedOfdState: number },
): Promise<KernelRpcResumeOutcome> {
  const effects: KernelRpcResumeEffect[] = [];
  const pendingFs = readPendingFsContext(handle, vmPtr);
  const fsDebugSuffix = formatPendingFsDebugSuffix(pendingFs);
  const pendingRingRequest = pendingFs.opcode === 0 && fsRingHasPendingRequests(handle, vmPtr);
  const memfsStateMayChange = (pendingFs.effects & (fsEffects.memfsMutation | fsEffects.memfsFileData)) !== 0;
  const memfsPublishMayBeReady = (pendingFs.effects & fsEffects.memfsMutation) !== 0
    || (
      (pendingFs.effects & fsEffects.memfsFileData) !== 0
      && pendingFs.opcode === FS_OP_WRITE
    );
  const ofdStateMayChange = (pendingFs.effects & fsEffects.sharedOfdState) !== 0;
  const preservePublishedRegularFileData = (pendingFs.effects & fsEffects.memfsMutation) !== 0
    && (pendingFs.effects & fsEffects.memfsFileData) === 0;
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    deps.appendWorkerDebugLog(handle, `fs-start:0x${vmPtr.toString(16)}:local${fsDebugSuffix}`);
    appendFsLifecycleEvent(handle, "start", vmPtr, `:local${fsDebugSuffix}${formatFsRingDebug(handle, vmPtr)}`);
    handle.kernel.thread_process_fs(vmPtr);
    const fsResult = normalizeFsResult(handle.kernel.thread_resume_fs(vmPtr));
    const fsSucceeded = Number(BigInt.asIntN(64, fsResult)) >= 0;
    const memfsMutated = fsSucceeded && (memfsStateMayChange || pendingRingRequest);
    const memfsPublishReady = fsSucceeded && memfsPublishMayBeReady;
    const ofdStateChanged = fsSucceeded && (ofdStateMayChange || pendingRingRequest);
    recordFsTransferStats(handle, pendingFs, fsResult, fsSucceeded, fsEffects, false, memfsPublishReady);
    const postFsRingProgress = snapshotFsRingProgress(handle, vmPtr);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    if (memfsMutated) {
      handle.processMemfsDirty = true;
      if (memfsPublishReady) {
        handle.transferDebugStats.pageCachePublishCalls += 1;
        handle.transferDebugStats.forkSharedMemfsSyncCalls += 1;
        effects.push({
          kind: "sync-fork-shared-state-local-after-fs",
          targets: deps.kernelShareOtherMembers(handle),
        });
      }
      effects.push({ kind: "mark-runtime-fs-dirty", handle });
    } else if (ofdStateChanged) {
      effects.push({
        kind: "sync-fork-shared-state-local-after-fs",
        targets: deps.kernelShareOtherMembers(handle),
        options: { includeMemfs: false },
      });
    }
    restoreFsRingProgress(handle, postFsRingProgress);
    clearStaleFsStatusAfterResume(handle, vmPtr, fsResult);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    effects.push({ kind: "kick-blocked-workers" });
    deps.appendWorkerDebugLog(handle, `fs-done:0x${vmPtr.toString(16)}:local:rv=${fsResult.toString()}`);
    appendFsLifecycleEvent(
      handle,
      "done",
      vmPtr,
      `:local:rv=${fsResult.toString()}:status=${handle.kernel.thread_status(vmPtr)}${formatFsRingDebug(handle, vmPtr)}`,
    );
    if (fsResultIsPending(handle, vmPtr, fsResult)) {
      appendFsLifecycleEvent(
        handle,
        "pending",
        vmPtr,
        `:local:rv=${fsResult.toString()}${formatFsRingDebug(handle, vmPtr)}`,
      );
      const status = handle.kernel.abi_status_fs?.() ?? -1;
      effects.push({ kind: "block-worker", status });
      return { kind: "blocked", status, effects };
    }
    effects.push({ kind: "enqueue-worker" });
    return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr), effects);
  }

  try {
    deps.appendWorkerDebugLog(handle, `fs-start:0x${vmPtr.toString(16)}${fsDebugSuffix}`);
    appendFsLifecycleEvent(handle, "start", vmPtr, `${fsDebugSuffix}${formatFsRingDebug(handle, vmPtr)}`);
    const heldOfdIds = collectProcessHeldOfds(handle);
    const missingSharedOfdIds = activeKernelShareOfdIds(handle)
      .filter((ofdId) => !heldOfdIds.has(ofdId) || !hasPlausibleOfdState(handle, ofdId));
    if (missingSharedOfdIds.length > 0) {
      await deps.rehydrateForkSharedStateViaKernelWorker([handle], missingSharedOfdIds, {
        includeMemfs: false,
      });
    }
    const dirtyMemfsPeers = deps.kernelShareOtherMembers(handle)
      .filter((member) => (
        member.processMemfsDirty
        && member.runningWorker === null
        && !member.exitInProgress
        && member.kernelWorkerRequest
        && member.pid > 0
      ));
    for (const peer of dirtyMemfsPeers) {
      const targets = [handle, ...deps.kernelShareOtherMembers(peer)]
        .filter((member, index, members) => (
          member !== peer
          && !member.exitInProgress
          && member.runningWorker === null
          && members.indexOf(member) === index
        ));
      peer.transferDebugStats.forkSharedMemfsSyncCalls += 1;
      await deps.syncDirtyMemfsPeerViaKernelWorker(peer, targets, {
        includeMemfs: true,
        memfsOnly: true,
        preservePublishedRegularFileData: true,
        sourceVmPtr: peer.vmPtr,
      });
      peer.processMemfsDirty = false;
    }
    deps.assertProcessStandardFdIntegrity(handle, "resume-fs-before");
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    const processFsResponse = await handle.kernelWorkerRequest({
      type: "process-fs",
      pid: handle.pid,
      vmPtr,
      kernelState: handle.kernelState,
    });
    const fsProcessedResponse = expectKernelWorkerResponse(
      processFsResponse,
      "fs-processed",
      "process-fs",
    );
    if (fsProcessedResponse.fsDebug) {
      appendFsLifecycleEvent(handle, "processed", vmPtr, `:${fsProcessedResponse.fsDebug}`);
    }
    handle.kernelState = adoptKernelStateForHandle(handle, fsProcessedResponse.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    const fsResult = normalizeFsResult(fsProcessedResponse.result);
    const fsSucceeded = Number(BigInt.asIntN(64, fsResult)) >= 0;
    const memfsMutated = fsProcessedResponse.memfsMutated || (fsSucceeded && (memfsStateMayChange || pendingRingRequest));
    const memfsPublishReady = fsSucceeded && memfsPublishMayBeReady;
    const ofdStateChanged = fsProcessedResponse.ofdStateChanged || (fsSucceeded && (ofdStateMayChange || pendingRingRequest));
    recordFsTransferStats(handle, pendingFs, fsResult, fsSucceeded, fsEffects, true, memfsPublishReady);
    const postFsRingProgress = snapshotFsRingProgress(handle, vmPtr);
    const postFsFdState = snapshotFsFdState(handle, vmPtr);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    const currentPid = handle.kernel.process_current_pid?.(vmPtr) ?? handle.pid;
    const transientProcessOwnerMismatch = currentPid > 0 && currentPid !== handle.pid;
    if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
      return { kind: "process-gone", effects };
    }
    deps.assertProcessStandardFdIntegrity(handle, "resume-fs");
    const forkShareMembers = deps.kernelShareOtherMembers(handle);
    const syncTargets = forkShareMembers
      .filter((member) => member.runningWorker === null);
    if (ofdStateChanged && !transientProcessOwnerMismatch && forkShareMembers.length > 0) {
      effects.push({
        kind: "sync-shared-ofd-state-after-fs",
        targets: syncTargets,
        sourceVmPtr: vmPtr,
      });
    }
    restoreFsFdState(handle, postFsFdState);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    if (memfsMutated) {
      handle.processMemfsDirty = true;
      if (!transientProcessOwnerMismatch) {
        if (memfsPublishReady) {
          appendFsLifecycleEvent(
            handle,
            "publish-memfs",
            vmPtr,
            `${fsDebugSuffix}:targets=${syncTargets.length}:members=${forkShareMembers.length}`,
          );
          // MemFS publication must not fan out OFD slots: during fork/exec
          // a child may not hold the parent's pipe OFDs, and copying those
          // slots from the child clears the parent's standard streams.
          handle.transferDebugStats.pageCachePublishCalls += 1;
          handle.transferDebugStats.forkSharedMemfsSyncCalls += 1;
          effects.push({
            kind: "publish-memfs-after-fs",
            targets: syncTargets,
            preservePublishedRegularFileData,
            sourceVmPtr: vmPtr,
          });
          if (forkShareMembers.length > 0) {
            for (const member of forkShareMembers) {
              effects.push({ kind: "mark-runtime-fs-dirty", handle: member });
            }
          }
        }
      }
      effects.push({ kind: "mark-runtime-fs-dirty", handle });
    }
    restoreFsFdState(handle, postFsFdState);
    restoreFsRingProgress(handle, postFsRingProgress);
    clearStaleFsStatusAfterResume(handle, vmPtr, fsResult);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    effects.push(
      { kind: "publish-runtime-kernel-state" },
      { kind: "kick-blocked-workers" },
    );
    deps.appendWorkerDebugLog(handle, `fs-done:0x${vmPtr.toString(16)}:rv=${fsResult.toString()}`);
    appendFsLifecycleEvent(
      handle,
      "done",
      vmPtr,
      `:rv=${fsResult.toString()}:status=${handle.kernel.thread_status?.(vmPtr) ?? "n/a"}${formatFsRingDebug(handle, vmPtr)}`,
    );
    if (fsResultIsPending(handle, vmPtr, fsResult)) {
      appendFsLifecycleEvent(
        handle,
        "pending",
        vmPtr,
        `:rv=${fsResult.toString()}${formatFsRingDebug(handle, vmPtr)}`,
      );
      const status = handle.kernel.abi_status_fs();
      effects.push({ kind: "block-worker", status });
      return { kind: "blocked", status, effects };
    }
    effects.push({ kind: "enqueue-worker" });
    return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr), effects);
  } catch (error) {
    deps.appendWorkerDebugLog(handle, `fs-err:0x${vmPtr.toString(16)}`);
    appendFsLifecycleEvent(handle, "err", vmPtr, formatFsRingDebug(handle, vmPtr));
    if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
      return { kind: "process-gone", effects };
    }
    deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    return { kind: "process-gone", effects };
  }
}



  async function resumeFsViaKernelWorkerOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    fsEffects: { memfsMutation: number; memfsFileData: number; sharedOfdState: number },
  ): Promise<KernelRpcResumeOutcome> {
    return resumeFsViaKernelWorker(handle, worker, vmPtr, fsEffects);
  }

  return {
    resumeFsViaKernelWorker,
    resumeFsViaKernelWorkerOutcome,
  };
}
