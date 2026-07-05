import { hostExecutionState } from "../host-execution-state.js";
import { processHasWorker, processWorkers } from "../process-worker-state.js";
import { guestWriteWatchRegions } from "../guest-write-watch-state.js";
import { workerDebugLog } from "../worker-debug-log.js";
import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import {
  collectProcessHeldOfds,
  hasPlausibleOfdState,
} from "../ownership/ofd-table.js";
import {
  applyThreadFdEntries,
  supervisorProcessFdFlagsForFd,
  supervisorProcessOfdForFd,
} from "../ownership/fd-table.js";
import { supervisorOfdFdType } from "../ownership/ofd-metadata.js";
import { applyThreadOfdSlots } from "../ownership/ofd-slot.js";
import {
  appendPendingGuestMemoryWrites,
  formatGuestMemoryWriteWatchOverlap,
} from "../guest-memory-writes.js";
import { ensureKernelMemoryCapacity, kernelScratchBase } from "../kernel-scratch.js";
import { adoptKernelStateForHandle } from "../kernel-state.js";
import {
  forgetMemfsDeletedPath,
  reapplyMemfsDeletedPaths,
  rememberMemfsDeletedPath,
  removeLocalMemfsPath,
  resolveLocalMemfsPath,
} from "../memfs-tombstones.js";
import {
  isProcessMemfsDirty,
  setProcessMemfsDirty,
  setProcessMemfsPublishPending,
} from "../process-memfs-state.js";
import { processExitInProgress } from "../process-lifecycle-state.js";
import {
  type ProcessHandle,
  type ThreadWorkerState,
} from "../types.js";
import { transferDebugStats } from "../worker-debug-stats.js";
import {
  clearThreadOwnerAsyncStatus,
  getThreadOwnerAsyncStatus,
  getThreadWaitingInWorkerStatus,
  setThreadOwnerAsyncStatus,
} from "../thread-host-state.js";
import { maybeThreadActivityState, getThreadActivityState } from "../thread-activity-state.js";
import { setPendingKernelStateForNextStep } from "../thread-step-payload.js";
import type {
  GuestMemoryWriteSnapshot,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPidBytesSnapshot,
} from "../../messages.js";
import type { SyncCause } from "../contracts/sync.js";
import {
  FS_OP_CLOSE,
  FS_OP_EXECVE,
  FS_OP_FSYNC,
  FS_OP_FSTAT,
  FS_OP_GETDENTS,
  FS_OP_LSEEK,
  FS_OP_MKDIR,
  FS_OP_MMAP,
  FS_OP_OPENAT,
  FS_OP_READ,
  FS_OP_RENAME,
  FS_OP_TRUNCATE,
  FS_OP_UNLINK,
  FS_OP_WRITE,
  FS_RING_COMPLETE_HEAD_OFFSET,
  FS_RING_COMPLETE_TAIL_OFFSET,
  FS_RING_REQUEST_FD_OFFSET,
  FS_RING_REQUEST_FLAGS_OFFSET,
  FS_RING_REQUEST_OPCODE_OFFSET,
  FS_RING_REQUEST_OFFSET_OFFSET,
  FS_RING_REQUEST_BUF_PTR_OFFSET,
  FS_RING_REQUEST_LEN_OFFSET,
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
  type KernelRpcResumeOutcome,
} from "./outcomes.js";
import { submitSupervisorShadowStatusEvent } from "../supervisor-shadow.js";
import { supervisorProcessStatusIsExecveSpawn } from "../supervisor-status.js";
import { requireKernelWorkerOwnership } from "../kernel-worker-ownership.js";
import { expectKernelWorkerResponse } from "./transport.js";

const FS_DEBUG_PATH_BUF_LEN = 4096;
const FS_LIFECYCLE_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_LIFECYCLE_DEBUG ?? "") === "1";

export interface KernelRpcFsEffects {
  memfsMutation: number;
  memfsFileData: number;
  sharedOfdState: number;
}

const KERNEL_RPC_FS_EFFECTS: KernelRpcFsEffects = {
  memfsMutation: 1 << 0,
  sharedOfdState: 1 << 1,
  memfsFileData: 1 << 2,
};

function supervisorNumber(value: bigint | number | undefined, name: string): number {
  const numeric = typeof value === "bigint" ? Number(value) : value;
  if (typeof numeric !== "number" || !Number.isSafeInteger(numeric)) {
    throw new RangeError(`invalid ${name} supervisor export value: ${String(value)}`);
  }
  return numeric;
}

function supervisorNumericExport(handle: ProcessHandle, exportName: string): number {
  const read = (handle.kernel as unknown as Record<string, unknown>)[exportName];
  if (typeof read !== "function") {
    throw new Error(`missing ${exportName} export`);
  }
  return supervisorNumber((read as () => bigint | number).call(handle.kernel), exportName);
}

interface PendingFsContext {
  opcode: number;
  effects: number;
  targetIno: number;
  path: string | null;
  secondaryPath: string | null;
}

interface FsFdStateSnapshot {
  vmPtr: number;
  fdEntries: Array<{ fd: number; ofdId: number; fdFlags: number; generation?: number }>;
  ofdTablePtr: number;
  ofdTable: Uint8Array | null;
}

interface FilteredFsFdState {
  fdEntries?: KernelWorkerFdEntrySnapshot[];
  ofdSlots?: KernelWorkerOfdSlotSnapshot[];
  skippedFdEntries: number;
  skippedOfdSlots: number;
}

interface FsRingProgressSnapshot {
  vmPtr: number;
  ringBase: number;
  submitHead: number;
  submitTail: number;
  completeHead: number;
  completeTail: number;
}

interface PendingRegularFileFsRequest {
  fd: number;
  ofdId: number;
}

interface FsGuestMemoryWriteOptions {
  queueThreadWorkerWrites?: boolean;
}

interface SupervisorFsCompletionDecision {
  memfsMutated: boolean;
  memfsPublishReady: boolean;
  ofdStateChanged: boolean;
  regularFileRequest: boolean;
}

interface SupervisorFsResponseDisposition {
  adoptKernelState: boolean;
  applyGuestMemoryWrites: boolean;
  queueThreadWorkerWrites: boolean;
  applyLocalMetadata: boolean;
  clearTombstones: boolean;
  fdResponseMayClearLiveFds: boolean;
  storePendingKernelState: boolean;
  resolvedToExecveSpawn: boolean;
  processOwnerMatches: boolean;
}

interface SupervisorFsLocalMetadataActions {
  tombstonePrimary: boolean;
  forgetPrimary: boolean;
  forgetSecondary: boolean;
}

export interface WorkerKernelRpcFsDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleExecveSpawn?(handle: ProcessHandle, worker: Worker, thread: ThreadWorkerState): Promise<void>;
  markRuntimeFsDirty(handle: ProcessHandle): void;
  notifyHostEffectProgress(handle: ProcessHandle): void;
  kernelShareOtherMembers(handle: ProcessHandle): ProcessHandle[];
  syncDirtyMemfsPeerViaKernelWorker(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options: {
      cause: SyncCause;
      includeMemfs?: boolean;
      trackHeldOfds?: boolean;
      deltaOfdIds?: boolean;
      memfsOnly?: boolean;
      preserveSourceRefcounts?: boolean;
      sourceVmPtr?: number;
      ofdIds?: number[];
    },
  ): Promise<void>;
  syncSharedOfdStateAfterFs(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options: { sourceVmPtr: number },
  ): Promise<void>;
  rehydrateForkSharedStateViaKernelWorker(
    targets: ProcessHandle[],
    ofdIds: number[],
    options: { cause: SyncCause; procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean },
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

function submitSupervisorFsReadyEvent(
  handle: ProcessHandle,
  vmPtr: number,
  status: number | undefined,
): void {
  const okStatus = handle.kernel.abi_status_ok?.();
  if (okStatus === undefined || status !== okStatus) {
    return;
  }
  const result = submitSupervisorShadowStatusEvent(handle, {
    eventKind: supervisorNumericExport(handle, "supervisor_thread_event_kind_backing_store_ready"),
    vmPtr,
    pid: handle.pid,
    tid: handle.kernel.thread_tid?.(vmPtr) ?? 0,
    status,
    syncEffects: 0,
    blockedReason: 0,
    pc: handle.kernel.thread_pc?.(vmPtr) ?? 0n,
    sp: handle.kernel.thread_sp?.(vmPtr) ?? 0n,
  });
  if (result !== undefined && result < 0) {
    deps.appendWorkerDebugLog(handle, `supervisor-fs-ready-event:submit-error:${result}`);
  }
}

function formatPendingFsDebugSuffix(context: PendingFsContext): string {
  const pathSuffix = context.path ? `:path=${JSON.stringify(context.path)}` : "";
  const secondaryPathSuffix = context.secondaryPath
    ? `:path2=${JSON.stringify(context.secondaryPath)}`
    : "";
  return `:op=${context.opcode}:fx=${context.effects}:ino=${context.targetIno}${pathSuffix}${secondaryPathSuffix}`;
}

function applyLocalMetadataMutationAfterFs(
  handle: ProcessHandle,
  vmPtr: number,
  pendingFs: PendingFsContext,
  actions: SupervisorFsLocalMetadataActions,
): void {
  if (!actions.tombstonePrimary || !pendingFs.path) {
    return;
  }
  rememberMemfsDeletedPath(handle, pendingFs.path);
  const removed = removeLocalMemfsPath(handle, pendingFs.path);
  appendFsLifecycleEvent(
    handle,
    "local-metadata",
    vmPtr,
    `:op=UNLINK:path=${JSON.stringify(pendingFs.path)}:removed=${removed ? 1 : 0}:post_ino=${resolveLocalMemfsPath(handle, pendingFs.path)}`,
  );
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  for (const member of deps.kernelShareOtherMembers(handle)) {
    rememberMemfsDeletedPath(member, pendingFs.path);
    const memberRemoved = removeLocalMemfsPath(member, pendingFs.path);
    appendFsLifecycleEvent(
      member,
      "local-metadata-peer",
      member.vmPtr,
      `:source=${handle.pid}:op=UNLINK:path=${JSON.stringify(pendingFs.path)}:removed=${memberRemoved ? 1 : 0}:post_ino=${resolveLocalMemfsPath(member, pendingFs.path)}`,
    );
    member.kernelState = readKernelRuntimeState(member.kernel);
    attachKernelRuntimeState(member.kernel, member.kernelState);
  }
}

function clearSupersededMemfsDeletedPath(
  handle: ProcessHandle,
  pendingFs: PendingFsContext,
  actions: SupervisorFsLocalMetadataActions,
): void {
  if (actions.forgetPrimary && pendingFs.path) {
    forgetMemfsDeletedPath(handle, pendingFs.path);
  }
  if (actions.forgetSecondary && pendingFs.secondaryPath) {
    forgetMemfsDeletedPath(handle, pendingFs.secondaryPath);
  }
}

function reapplyLocalMemfsDeletedPathTombstones(
  handle: ProcessHandle,
  vmPtr: number,
  phase: string,
): void {
  const results = reapplyMemfsDeletedPaths(handle);
  if (results.length === 0) {
    return;
  }
  for (const result of results) {
    appendFsLifecycleEvent(
      handle,
      "local-metadata-tombstone",
      vmPtr,
      `:${phase}:path=${JSON.stringify(result.path)}:removed=${result.removed ? 1 : 0}:post_ino=${result.postIno}`,
    );
  }
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
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

function formatFdEntrySummary(
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
): string {
  const nonzeroEntries = (fdEntries ?? [])
    .filter((entry) => entry.ofdId !== 0)
    .slice(0, 16)
    .map((entry) => `${entry.fd}->${entry.ofdId}/${entry.fdFlags}`);
  return nonzeroEntries.length > 0 ? nonzeroEntries.join(",") : "none";
}

function formatRuntimeFdSummary(handle: ProcessHandle, vmPtr: number): string {
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  if (vmPtr === 0 || maxFds <= 0) {
    return "none";
  }
  const entries: string[] = [];
  for (let fd = 0; fd < maxFds && entries.length < 16; fd += 1) {
    const ofdId = supervisorProcessOfdForFd(handle, vmPtr, fd);
    if (ofdId === 0) {
      continue;
    }
    entries.push(`${fd}->${ofdId}/${supervisorProcessFdFlagsForFd(handle, vmPtr, fd)}`);
  }
  return entries.length > 0 ? entries.join(",") : "none";
}

function fsOpName(opcode: number): string {
  switch (opcode) {
    case FS_OP_READ:
      return "READ";
    case FS_OP_WRITE:
      return "WRITE";
    case FS_OP_OPENAT:
      return "OPENAT";
    case FS_OP_CLOSE:
      return "CLOSE";
    case FS_OP_FSTAT:
      return "FSTAT";
    case FS_OP_MKDIR:
      return "MKDIR";
    case FS_OP_UNLINK:
      return "UNLINK";
    case FS_OP_RENAME:
      return "RENAME";
    case FS_OP_GETDENTS:
      return "GETDENTS";
    case FS_OP_TRUNCATE:
      return "TRUNCATE";
    case FS_OP_LSEEK:
      return "LSEEK";
    case FS_OP_MMAP:
      return "MMAP";
    case FS_OP_EXECVE:
      return "EXECVE";
    case FS_OP_FSYNC:
      return "FSYNC";
    default:
      return `OP${opcode}`;
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
    const fd = view.getInt32(entry + FS_RING_REQUEST_FD_OFFSET, true);
    const offset = view.getBigUint64(entry + FS_RING_REQUEST_OFFSET_OFFSET, true);
    const len = view.getBigUint64(entry + FS_RING_REQUEST_LEN_OFFSET, true);
    const bufPtr = view.getBigUint64(entry + FS_RING_REQUEST_BUF_PTR_OFFSET, true);
    const flags = view.getUint32(entry + FS_RING_REQUEST_FLAGS_OFFSET, true);
    const requestVmPtr = view.getUint32(entry + FS_RING_REQUEST_VM_PTR_OFFSET, true);
    requests.push([
      `${pos}:op=${opcode}/${fsOpName(opcode)}`,
      `fd=${fd}`,
      `off=${offset.toString()}`,
      `len=${len.toString()}`,
      `buf=0x${bufPtr.toString(16)}`,
      `flags=${flags}`,
      `vm=0x${requestVmPtr.toString(16)}`,
    ].join("/"));
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

function pendingFsRequestFd(handle: ProcessHandle, vmPtr: number): number | null {
  const progress = snapshotFsRingProgress(handle, vmPtr);
  if (progress === null) {
    return null;
  }
  const view = new DataView(handle.memory.buffer);
  const { ringBase, submitHead, submitTail } = progress;
  for (let pos = submitTail; pos < submitHead; pos += 1) {
    const entry = fsRingRequestEntryOffset(ringBase, pos);
    const requestVmPtr = view.getUint32(entry + FS_RING_REQUEST_VM_PTR_OFFSET, true);
    if (requestVmPtr === vmPtr) {
      return view.getInt32(entry + FS_RING_REQUEST_FD_OFFSET, true);
    }
  }
  return null;
}

function pendingFsSharedOfdIds(handle: ProcessHandle, vmPtr: number): number[] {
  const fd = pendingFsRequestFd(handle, vmPtr);
  if (fd === null || fd < 0 || fd >= handle.kernel.abi_max_fds()) {
    return [];
  }
  const ofdId = supervisorProcessOfdForFd(handle, vmPtr, fd);
  if (
    ofdId > 0
    && ofdId < handle.kernel.abi_max_fds()
    && handle.kernelShare.sharedOfdIds.has(ofdId)
  ) {
    return [ofdId];
  }
  return [];
}

function hasLiveWorkerState(
  handle: ProcessHandle,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): boolean {
  if (
    (hostExecutionState(handle).runningWorker !== null && hostExecutionState(handle).runningWorker !== undefined)
    || (hostExecutionState(handle).vforkReadyWorkers?.length ?? 0) > 0
    || (hostExecutionState(handle).hostBlockedWorkers?.size ?? 0) > 0
  ) {
    return true;
  }
  for (const worker of processWorkers(handle)) {
    const thread = threadWorkerStates.get(worker);
    if (getThreadWaitingInWorkerStatus(thread) !== null) {
      return true;
    }
  }
  return false;
}

function supervisorPendingRingRequest(
  handle: ProcessHandle,
  vmPtr: number,
): boolean {
  const pendingRingRequest = handle.kernel.supervisor_fs_pending_ring_request;
  if (typeof pendingRingRequest !== "function") {
    throw new Error("missing supervisor_fs_pending_ring_request export");
  }
  return pendingRingRequest.call(handle.kernel, vmPtr) !== 0;
}

function supervisorPendingRegularFileFsRequest(
  handle: ProcessHandle,
  vmPtr: number,
): PendingRegularFileFsRequest | null {
  const pendingFd = handle.kernel.supervisor_fs_pending_regular_file_fd;
  const pendingOfd = handle.kernel.supervisor_fs_pending_regular_file_ofd;
  if (typeof pendingFd !== "function" || typeof pendingOfd !== "function") {
    throw new Error("missing supervisor pending regular-file FS request exports");
  }
  const fd = pendingFd.call(handle.kernel, vmPtr);
  const ofdId = pendingOfd.call(handle.kernel, vmPtr);
  return fd >= 0 && ofdId > 0 ? { fd, ofdId } : null;
}

function supervisorFsCompletionDecision(
  handle: ProcessHandle,
  vmPtr: number,
  pendingFs: PendingFsContext,
  fsResult: bigint | number,
  options: {
    hostMemfsMutated?: boolean;
    hostOfdStateChanged?: boolean;
    pendingRingRequest: boolean;
    regularFileRequest: boolean;
  },
): SupervisorFsCompletionDecision {
  const decide = handle.kernel.supervisor_fs_completion_decision;
  if (typeof decide !== "function") {
    throw new Error("missing supervisor_fs_completion_decision export");
  }
  const flags = decide.call(
    handle.kernel,
    vmPtr,
    pendingFs.opcode,
    pendingFs.effects,
    fsResult,
    options.hostMemfsMutated === true ? 1 : 0,
    options.hostOfdStateChanged === true ? 1 : 0,
    options.pendingRingRequest ? 1 : 0,
    options.regularFileRequest ? 1 : 0,
  );
  const memfsDirtyFlag = supervisorNumericExport(handle, "supervisor_fs_completion_flag_memfs_dirty");
  const memfsPublishReadyFlag = supervisorNumericExport(handle, "supervisor_fs_completion_flag_memfs_publish_ready");
  const ofdStateChangedFlag = supervisorNumericExport(handle, "supervisor_fs_completion_flag_ofd_state_changed");
  const regularFileRequestFlag = supervisorNumericExport(handle, "supervisor_fs_completion_flag_regular_file_request");
  return {
    memfsMutated: (flags & memfsDirtyFlag) !== 0,
    memfsPublishReady: (flags & memfsPublishReadyFlag) !== 0,
    ofdStateChanged: (flags & ofdStateChangedFlag) !== 0,
    regularFileRequest: (flags & regularFileRequestFlag) !== 0,
  };
}

function supervisorFsResponseFlag(handle: ProcessHandle, name: keyof typeof responseFlagAccessors): number {
  return supervisorNumericExport(handle, responseFlagAccessors[name]);
}

const responseFlagAccessors = {
  adoptKernelState: "supervisor_fs_response_flag_adopt_kernel_state",
  applyGuestMemoryWrites: "supervisor_fs_response_flag_apply_guest_memory_writes",
  queueThreadWorkerWrites: "supervisor_fs_response_flag_queue_thread_worker_writes",
  applyLocalMetadata: "supervisor_fs_response_flag_apply_local_metadata",
  clearTombstones: "supervisor_fs_response_flag_clear_tombstones",
  fdResponseMayClearLiveFds: "supervisor_fs_response_flag_fd_response_may_clear_live_fds",
  storePendingKernelState: "supervisor_fs_response_flag_store_pending_kernel_state",
  resolvedToExecveSpawn: "supervisor_fs_response_flag_resolved_to_execve_spawn",
  processOwnerMatches: "supervisor_fs_response_flag_process_owner_matches",
} as const;

function supervisorFsResponseDisposition(
  handle: ProcessHandle,
  vmPtr: number,
  pendingFs: PendingFsContext,
  fsResult: bigint | number,
  threadState: ThreadWorkerState | undefined,
): SupervisorFsResponseDisposition {
  const decide = handle.kernel.supervisor_fs_response_disposition;
  if (typeof decide !== "function") {
    throw new Error("missing supervisor_fs_response_disposition export");
  }
  const flags = decide.call(
    handle.kernel,
    vmPtr,
    handle.pid,
    pendingFs.opcode,
    fsResult,
    getThreadOwnerAsyncStatus(threadState) ?? 0,
    maybeThreadActivityState(threadState)?.lastStatus ?? 0,
  );
  return {
    adoptKernelState: (flags & supervisorFsResponseFlag(handle, "adoptKernelState")) !== 0,
    applyGuestMemoryWrites: (flags & supervisorFsResponseFlag(handle, "applyGuestMemoryWrites")) !== 0,
    queueThreadWorkerWrites: (flags & supervisorFsResponseFlag(handle, "queueThreadWorkerWrites")) !== 0,
    applyLocalMetadata: (flags & supervisorFsResponseFlag(handle, "applyLocalMetadata")) !== 0,
    clearTombstones: (flags & supervisorFsResponseFlag(handle, "clearTombstones")) !== 0,
    fdResponseMayClearLiveFds: (flags & supervisorFsResponseFlag(handle, "fdResponseMayClearLiveFds")) !== 0,
    storePendingKernelState: (flags & supervisorFsResponseFlag(handle, "storePendingKernelState")) !== 0,
    resolvedToExecveSpawn: (flags & supervisorFsResponseFlag(handle, "resolvedToExecveSpawn")) !== 0,
    processOwnerMatches: (flags & supervisorFsResponseFlag(handle, "processOwnerMatches")) !== 0,
  };
}

function findSupervisorExecveSpawnStatus(
  handle: ProcessHandle,
  candidates: ReadonlyArray<number | null | undefined>,
): number | undefined {
  for (const status of candidates) {
    if (
      status !== null
      && status !== undefined
      && supervisorProcessStatusIsExecveSpawn(handle.kernel, status)
    ) {
      return status;
    }
  }
  return undefined;
}

function supervisorFsLocalMetadataActions(
  handle: ProcessHandle,
  pendingFs: PendingFsContext,
  fsResult: bigint | number,
): SupervisorFsLocalMetadataActions {
  const decide = handle.kernel.supervisor_fs_local_metadata_action;
  if (typeof decide !== "function") {
    throw new Error("missing supervisor_fs_local_metadata_action export");
  }
  const action = decide.call(
    handle.kernel,
    pendingFs.opcode,
    pendingFs.effects,
    fsResult,
    pendingFs.path ? 1 : 0,
    pendingFs.secondaryPath ? 1 : 0,
  );
  const none = supervisorNumericExport(handle, "supervisor_fs_local_metadata_action_none");
  const tombstonePrimary = supervisorNumericExport(handle, "supervisor_fs_local_metadata_action_tombstone_primary");
  const forgetPrimary = supervisorNumericExport(handle, "supervisor_fs_local_metadata_action_forget_primary");
  const forgetSecondary = supervisorNumericExport(handle, "supervisor_fs_local_metadata_action_forget_secondary");
  const known = none | tombstonePrimary | forgetPrimary | forgetSecondary;
  if ((action & ~known) !== 0) {
    throw new Error(`unknown supervisor fs local metadata action: ${action}`);
  }
  return {
    tombstonePrimary: (action & tombstonePrimary) !== 0,
    forgetPrimary: (action & forgetPrimary) !== 0,
    forgetSecondary: (action & forgetSecondary) !== 0,
  };
}

function filterFsResponseToRegularFileRequest(
  request: PendingRegularFileFsRequest,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[] | undefined,
): {
  fdEntries?: KernelWorkerFdEntrySnapshot[];
  ofdSlots?: KernelWorkerOfdSlotSnapshot[];
} {
  const requestFdEntries = (fdEntries ?? [])
    .filter((entry) => entry.fd === request.fd && entry.ofdId === request.ofdId);
  const requestOfdIds = new Set<number>([request.ofdId]);
  for (const entry of requestFdEntries) {
    requestOfdIds.add(entry.ofdId);
  }
  const requestOfdSlots = (ofdSlots ?? [])
    .filter((snapshot) => requestOfdIds.has(snapshot.ofdId));
  return {
    fdEntries: requestFdEntries.length > 0 ? requestFdEntries : undefined,
    ofdSlots: requestOfdSlots.length > 0 ? requestOfdSlots : undefined,
  };
}

function fdEntryMatches(entry: { ofdId: number; fdFlags: number }, ofdId: number, fdFlags: number): boolean {
  return entry.ofdId === ofdId && entry.fdFlags === fdFlags;
}

function fdGenerationMatches(
  baselineGeneration: number | undefined,
  currentGeneration: number | undefined,
  responseGeneration: number | undefined,
): boolean {
  if (baselineGeneration === undefined || currentGeneration === undefined) {
    return true;
  }
  return currentGeneration === baselineGeneration
    || (responseGeneration !== undefined && currentGeneration === responseGeneration);
}

function byteRangesEqual(
  left: Uint8Array,
  right: Uint8Array,
): boolean {
  if (left.byteLength !== right.byteLength) {
    return false;
  }
  for (let offset = 0; offset < left.byteLength; offset += 1) {
    if (left[offset] !== right[offset]) {
      return false;
    }
  }
  return true;
}

function baselineOfdSlot(
  baseline: FsFdStateSnapshot,
  ofdId: number,
  slotBytes: number,
): Uint8Array | null {
  if (baseline.ofdTable === null || ofdId <= 0 || slotBytes <= 0) {
    return null;
  }
  const sourceOffset = ofdId * slotBytes;
  if (sourceOffset < 0 || sourceOffset + slotBytes > baseline.ofdTable.byteLength) {
    return null;
  }
  return baseline.ofdTable.subarray(sourceOffset, sourceOffset + slotBytes);
}

function currentOfdSlot(
  handle: ProcessHandle,
  ofdId: number,
  slotBytes: number,
): Uint8Array | null {
  const targetOffset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
  if (
    ofdId <= 0
    || slotBytes <= 0
    || targetOffset < 0
    || targetOffset + slotBytes > handle.memory.buffer.byteLength
  ) {
    return null;
  }
  return new Uint8Array(handle.memory.buffer, targetOffset, slotBytes);
}

function filterStaleFsResponseFdState(
  handle: ProcessHandle,
  vmPtr: number,
  baseline: FsFdStateSnapshot | null,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[] | undefined,
): FilteredFsFdState {
  if (baseline === null || baseline.vmPtr !== vmPtr) {
    return {
      fdEntries: fdEntries === undefined ? undefined : [...fdEntries],
      ofdSlots: ofdSlots === undefined ? undefined : [...ofdSlots],
      skippedFdEntries: 0,
      skippedOfdSlots: 0,
    };
  }

  const baselineFdEntries = new Map<number, {
    ofdId: number;
    fdFlags: number;
    generation?: number;
  }>();
  for (const entry of baseline.fdEntries) {
    baselineFdEntries.set(entry.fd, entry);
  }

  const keptFdEntries: KernelWorkerFdEntrySnapshot[] = [];
  const staleOfdIds = new Set<number>();
  let skippedFdEntries = 0;
  for (const entry of fdEntries ?? []) {
    const baselineEntry = baselineFdEntries.get(entry.fd);
    if (baselineEntry === undefined) {
      keptFdEntries.push(entry);
      continue;
    }
    const currentOfdId = supervisorProcessOfdForFd(handle, vmPtr, entry.fd);
    const currentFdFlags = supervisorProcessFdFlagsForFd(handle, vmPtr, entry.fd);
    const currentGeneration = handle.kernel.process_fd_generation?.(vmPtr, entry.fd);
    if (!fdGenerationMatches(baselineEntry.generation, currentGeneration, entry.generation)) {
      if (entry.ofdId > 0) {
        staleOfdIds.add(entry.ofdId);
      }
      skippedFdEntries += 1;
      continue;
    }
    if (
      fdEntryMatches(baselineEntry, currentOfdId, currentFdFlags)
      || fdEntryMatches(entry, currentOfdId, currentFdFlags)
    ) {
      keptFdEntries.push(entry);
      continue;
    }
    skippedFdEntries += 1;
    if (entry.ofdId > 0) {
      staleOfdIds.add(entry.ofdId);
    }
  }

  const maxFds = handle.kernel.abi_max_fds?.() ?? baseline.fdEntries.length;
  const tableBytes = handle.kernel.kernel_ofd_table_bytes?.() ?? 0;
  const slotBytes = maxFds > 0 ? tableBytes / maxFds : 0;
  const canFilterOfdSlots = baseline.ofdTable !== null
    && Number.isInteger(slotBytes)
    && slotBytes > 0;
  const keptOfdSlots: KernelWorkerOfdSlotSnapshot[] = [];
  let skippedOfdSlots = 0;
  for (const snapshot of ofdSlots ?? []) {
    if (staleOfdIds.has(snapshot.ofdId)) {
      skippedOfdSlots += 1;
      continue;
    }
    if (!canFilterOfdSlots || snapshot.slot.byteLength !== slotBytes) {
      keptOfdSlots.push(snapshot);
      continue;
    }
    const baselineSlot = baselineOfdSlot(baseline, snapshot.ofdId, slotBytes);
    const currentSlot = currentOfdSlot(handle, snapshot.ofdId, slotBytes);
    if (
      baselineSlot === null
      || currentSlot === null
      || byteRangesEqual(currentSlot, baselineSlot)
      || byteRangesEqual(currentSlot, snapshot.slot)
    ) {
      keptOfdSlots.push(snapshot);
      continue;
    }
    skippedOfdSlots += 1;
  }

  return {
    fdEntries: keptFdEntries.length > 0 ? keptFdEntries : undefined,
    ofdSlots: keptOfdSlots.length > 0 ? keptOfdSlots : undefined,
    skippedFdEntries,
    skippedOfdSlots,
  };
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

function formatFsErrorDetail(error: unknown): string {
  const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
  return `:err=${JSON.stringify(detail.slice(0, 1000))}`;
}

function isPackageIndexTransferPath(path: string | null): boolean {
  return path?.includes("_Packages.xz") === true || path?.endsWith("/Packages") === true;
}

function recordFsTransferStats(
  handle: ProcessHandle,
  pendingFs: PendingFsContext,
  fsResult: bigint | number,
  fsSucceeded: boolean,
  fsEffects: KernelRpcFsEffects,
  viaKernelWorker: boolean,
  memfsPublishReady: boolean,
): void {
  const stats = transferDebugStats(handle);
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
  if (vmPtr === 0 || maxFds <= 0) {
    return null;
  }
  const fdEntries: FsFdStateSnapshot["fdEntries"] = [];
  for (let fd = 0; fd < maxFds; fd += 1) {
    fdEntries.push({
      fd,
      ofdId: supervisorProcessOfdForFd(handle, vmPtr, fd),
      fdFlags: supervisorProcessFdFlagsForFd(handle, vmPtr, fd),
      generation: handle.kernel.process_fd_generation?.(vmPtr, fd),
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
    const currentOfdId = supervisorProcessOfdForFd(handle, snapshot.vmPtr, entry.fd);
    const currentFdFlags = supervisorProcessFdFlagsForFd(handle, snapshot.vmPtr, entry.fd);
    if (currentOfdId === entry.ofdId && currentFdFlags === entry.fdFlags) {
      continue;
    }
    handle.kernel.process_set_fd_entry(snapshot.vmPtr, entry.fd, entry.ofdId, entry.fdFlags);
    const currentOfdType = entry.ofdId > 0
      ? supervisorOfdFdType(handle, entry.ofdId)
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

function applyFsGuestMemoryWrites(
  handle: ProcessHandle,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
  writes: GuestMemoryWriteSnapshot[] | undefined,
  options?: FsGuestMemoryWriteOptions,
): void {
  if (!writes || writes.length === 0) {
    return;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  const pendingWrites: GuestMemoryWriteSnapshot[] = [];
  const queueThreadWorkerWrites = options?.queueThreadWorkerWrites ?? true;
  for (const write of writes) {
    const start = handle.guestBase + write.guestPtr;
    if (
      !Number.isFinite(start)
      || start < handle.guestBase
      || write.bytes.length === 0
      || start + write.bytes.length > memory.byteLength
    ) {
      continue;
    }
    const overlap = formatGuestMemoryWriteWatchOverlap(
      "fs-guest-write",
      guestWriteWatchRegions(handle),
      write.guestPtr,
      write.bytes,
    );
    if (overlap !== null) {
      workerDebugLog(handle).push(overlap);
      if (workerDebugLog(handle).length > 4096) {
        workerDebugLog(handle).splice(0, workerDebugLog(handle).length - 4096);
      }
    }
    memory.set(write.bytes, start);
    pendingWrites.push({
      guestPtr: write.guestPtr,
      bytes: memory.slice(start, start + write.bytes.length),
    });
  }
  if (!queueThreadWorkerWrites || pendingWrites.length === 0) {
    return;
  }
  for (const worker of processWorkers(handle)) {
    const thread = threadWorkerStates.get(worker);
    if (!thread) {
      continue;
    }
    appendPendingGuestMemoryWrites(thread, pendingWrites);
  }
}

async function resumeFsViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  fsEffects: KernelRpcFsEffects = KERNEL_RPC_FS_EFFECTS,
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

async function completeFsResumeFromSupervisorDecision(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  fsResult: bigint,
  options: {
    source: "local" | "kernel-worker";
    applyPostFsWake(): void | Promise<void>;
    finalStatus?: number;
    forceExecveSpawn?: boolean;
  },
): Promise<KernelRpcResumeOutcome> {
  const sourcePrefix = options.source === "local" ? ":local" : "";
  const finalStatus = options.finalStatus ?? handle.kernel.thread_status?.(vmPtr);
  deps.appendWorkerDebugLog(
    handle,
    `fs-done:0x${vmPtr.toString(16)}${sourcePrefix}:rv=${fsResult.toString()}`,
  );
  appendFsLifecycleEvent(
    handle,
    "done",
    vmPtr,
    `${sourcePrefix}:rv=${fsResult.toString()}:status=${finalStatus ?? "n/a"}${formatFsRingDebug(handle, vmPtr)}`,
  );
  if (fsResultIsPending(handle, vmPtr, fsResult)) {
    appendFsLifecycleEvent(
      handle,
      "pending",
      vmPtr,
      `${sourcePrefix}:rv=${fsResult.toString()}${formatFsRingDebug(handle, vmPtr)}`,
    );
    await options.applyPostFsWake();
    const status = handle.kernel.abi_status_fs?.() ?? -1;
    return { kind: "blocked", status };
  }
  const resolvedToExecveSpawn = options.forceExecveSpawn === true
    || (finalStatus !== undefined && supervisorProcessStatusIsExecveSpawn(handle.kernel, finalStatus));
  if (resolvedToExecveSpawn && finalStatus !== undefined) {
    await options.applyPostFsWake();
    await handleFsExecveSpawn(handle, worker, vmPtr, finalStatus);
    return completedKernelRpcOutcome();
  }
  await options.applyPostFsWake();
  submitSupervisorFsReadyEvent(handle, vmPtr, finalStatus);
  deps.enqueueWorker(handle, worker);
  return completedKernelRpcOutcome(finalStatus);
}

async function handleFsExecveSpawn(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  status: number,
): Promise<void> {
  const thread = deps.threadWorkerStates.get(worker);
  if (!thread || thread.vmPtr !== vmPtr) {
    return;
  }
  if (!deps.handleExecveSpawn) {
    throw new Error("kernel-rpc fs helper missing execve spawn host executor");
  }
  setThreadOwnerAsyncStatus(thread, status);
  try {
    await deps.handleExecveSpawn(handle, worker, thread);
  } finally {
    if (processHasWorker(handle, worker) && deps.threadWorkerStates.get(worker) === thread) {
      clearThreadOwnerAsyncStatus(thread);
    }
  }
}

async function resumeFsViaKernelWorkerInner(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  fsEffects: KernelRpcFsEffects,
): Promise<KernelRpcResumeOutcome> {
  const pendingFs = readPendingFsContext(handle, vmPtr);
  const fsDebugSuffix = formatPendingFsDebugSuffix(pendingFs);
  const pendingRingRequest = supervisorPendingRingRequest(handle, vmPtr);
  const regularFilePendingFs = supervisorPendingRegularFileFsRequest(handle, vmPtr);
  const regularFileOnlyPendingFs = regularFilePendingFs !== null;
  const kernelWorkerRequest = requireKernelWorkerOwnership(handle, "resumeFsViaKernelWorker");

  try {
    deps.appendWorkerDebugLog(handle, `fs-start:0x${vmPtr.toString(16)}${fsDebugSuffix}`);
    appendFsLifecycleEvent(handle, "start", vmPtr, `${fsDebugSuffix}${formatFsRingDebug(handle, vmPtr)}`);
    const heldOfdIds = collectProcessHeldOfds(handle);
    const missingSharedOfdIds = pendingFsSharedOfdIds(handle, vmPtr)
      .filter((ofdId) => !heldOfdIds.has(ofdId) || !hasPlausibleOfdState(handle, ofdId));
    if (missingSharedOfdIds.length > 0) {
      await deps.rehydrateForkSharedStateViaKernelWorker([handle], missingSharedOfdIds, {
        cause: { kind: "syscall", nr: -1n, name: "fs-rehydrate-shared-ofd" },
        includeMemfs: false,
      });
    }
    const dirtyMemfsPeers = deps.kernelShareOtherMembers(handle)
      .filter((member) => (
        isProcessMemfsDirty(member)
        && hasLiveWorkerState(member, deps.threadWorkerStates)
        && hostExecutionState(member).runningWorker === null
        && !processExitInProgress(member)
        && member.kernelWorkerRequest
        && member.pid > 0
      ));
    for (const peer of dirtyMemfsPeers) {
      const targets = [handle, ...deps.kernelShareOtherMembers(peer)]
        .filter((member, index, members) => (
          member !== peer
          && !processExitInProgress(member)
          && hostExecutionState(member).runningWorker === null
          && members.indexOf(member) === index
        ));
      transferDebugStats(peer).forkSharedMemfsSyncCalls += 1;
      await deps.syncDirtyMemfsPeerViaKernelWorker(peer, targets, {
        cause: { kind: "syscall", nr: -1n, name: "fs-dirty-memfs-peer" },
        includeMemfs: true,
        memfsOnly: true,
        sourceVmPtr: peer.vmPtr,
      });
      setProcessMemfsDirty(peer, false);
    }
    deps.assertProcessStandardFdIntegrity(handle, "resume-fs-before");
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    const requestFsFdState = snapshotFsFdState(handle, vmPtr);
    const processFsResponse = await kernelWorkerRequest({
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
    if (fsProcessedResponse.fdEntries) {
      appendFsLifecycleEvent(
        handle,
        "fd-response",
        vmPtr,
        `:fds=${formatFdEntrySummary(fsProcessedResponse.fdEntries)}`,
      );
    }
    const fsResult = normalizeFsResult(fsProcessedResponse.result);
    const fsSucceeded = Number(BigInt.asIntN(64, fsResult)) >= 0;
    const threadState = deps.threadWorkerStates.get(worker);
    const fsResponseDisposition = supervisorFsResponseDisposition(
      handle,
      vmPtr,
      pendingFs,
      fsResult,
      threadState,
    );
    const localMetadataActions = supervisorFsLocalMetadataActions(handle, pendingFs, fsResult);
    const responseCurrentPid = handle.kernel.process_current_pid?.(vmPtr) ?? handle.pid;
    if (fsResponseDisposition.adoptKernelState) {
      handle.kernelState = adoptKernelStateForHandle(handle, fsProcessedResponse.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
    }
    if (fsResponseDisposition.applyGuestMemoryWrites) {
      applyFsGuestMemoryWrites(
        handle,
        deps.threadWorkerStates,
        fsProcessedResponse.guestMemoryWrites,
        {
          queueThreadWorkerWrites: fsResponseDisposition.queueThreadWorkerWrites,
        },
      );
    }
    if (fsResponseDisposition.storePendingKernelState) {
      appendFsLifecycleEvent(
        handle,
        "child-state-skipped",
        vmPtr,
        `:rv=${fsResult.toString()}:pid=${responseCurrentPid}${fsDebugSuffix}`,
      );
    }
    if (fsResponseDisposition.applyLocalMetadata) {
      applyLocalMetadataMutationAfterFs(handle, vmPtr, pendingFs, localMetadataActions);
    }
    if (fsResponseDisposition.clearTombstones) {
      clearSupersededMemfsDeletedPath(handle, pendingFs, localMetadataActions);
      reapplyLocalMemfsDeletedPathTombstones(handle, vmPtr, "worker-after-adopt");
    }
    const responseFdState = filterStaleFsResponseFdState(
      handle,
      vmPtr,
      requestFsFdState,
      fsProcessedResponse.fdEntries,
      fsProcessedResponse.ofdSlots,
    );
    if (responseFdState.skippedFdEntries > 0 || responseFdState.skippedOfdSlots > 0) {
      appendFsLifecycleEvent(
        handle,
        "fd-stale-skipped",
        vmPtr,
        `:fds=${responseFdState.skippedFdEntries}:ofds=${responseFdState.skippedOfdSlots}`,
      );
    }
    if (
      fsResponseDisposition.adoptKernelState
      && !regularFileOnlyPendingFs
      && (responseFdState.ofdSlots || responseFdState.fdEntries)
    ) {
      applyThreadOfdSlots(handle, {
        vmPtr,
        fdEntries: responseFdState.fdEntries,
        ofdSlots: responseFdState.ofdSlots,
      });
      applyThreadFdEntries(handle, vmPtr, responseFdState.fdEntries, {
        preserveLiveEpollDependentDescriptors: !fsResponseDisposition.fdResponseMayClearLiveFds,
        preserveLiveRegularFileDescriptors: !fsResponseDisposition.fdResponseMayClearLiveFds,
      });
      appendFsLifecycleEvent(
        handle,
        "fd-applied",
        vmPtr,
        `:fds=${formatRuntimeFdSummary(handle, vmPtr)}`,
      );
    } else if (fsResponseDisposition.adoptKernelState && regularFilePendingFs) {
      const requestFsState = filterFsResponseToRegularFileRequest(
        regularFilePendingFs,
        responseFdState.fdEntries,
        responseFdState.ofdSlots,
      );
      if (requestFsState.fdEntries || requestFsState.ofdSlots) {
        applyThreadOfdSlots(handle, {
          vmPtr,
          fdEntries: requestFsState.fdEntries,
          ofdSlots: requestFsState.ofdSlots,
          trustRegularFileOfdOffsets: true,
        });
        applyThreadFdEntries(handle, vmPtr, requestFsState.fdEntries);
        appendFsLifecycleEvent(
          handle,
          "fd-apply-partial",
          vmPtr,
          `:fd=${regularFilePendingFs.fd}:ofd=${regularFilePendingFs.ofdId}:fds=${formatRuntimeFdSummary(handle, vmPtr)}`,
        );
      } else {
        appendFsLifecycleEvent(
          handle,
          "fd-apply-skipped",
          vmPtr,
          `:fd=${regularFilePendingFs.fd}:ofd=${regularFilePendingFs.ofdId}:fds=${formatFdEntrySummary(fsProcessedResponse.fdEntries)}`,
        );
      }
    } else if (regularFileOnlyPendingFs) {
      appendFsLifecycleEvent(
        handle,
        "fd-apply-skipped",
        vmPtr,
        `:fds=${formatFdEntrySummary(fsProcessedResponse.fdEntries)}`,
      );
    }
    const fsCompletion = supervisorFsCompletionDecision(handle, vmPtr, pendingFs, fsResult, {
      hostMemfsMutated: fsProcessedResponse.memfsMutated,
      hostOfdStateChanged: fsProcessedResponse.ofdStateChanged,
      pendingRingRequest,
      regularFileRequest: regularFileOnlyPendingFs,
    });
    const memfsMutated = fsCompletion.memfsMutated;
    const memfsPublishReady = fsCompletion.memfsPublishReady;
    const ofdStateChanged = fsCompletion.ofdStateChanged;
    recordFsTransferStats(handle, pendingFs, fsResult, fsSucceeded, fsEffects, true, memfsPublishReady);
    const postFsRingProgress = snapshotFsRingProgress(handle, vmPtr);
    const postFsFdState = snapshotFsFdState(handle, vmPtr);
    if (fsResponseDisposition.adoptKernelState) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
    }
    if (!processHasWorker(handle, worker) || !threadState) {
      return { kind: "process-gone" };
    }
    if (fsResponseDisposition.storePendingKernelState) {
      setPendingKernelStateForNextStep(threadState, fsProcessedResponse.kernelState);
    }
    const statusAfterFs = findSupervisorExecveSpawnStatus(handle, [
      handle.kernel.thread_status?.(vmPtr),
      getThreadOwnerAsyncStatus(threadState),
      getThreadActivityState(threadState).lastStatus,
    ]);
    if (
      fsResponseDisposition.resolvedToExecveSpawn
      && statusAfterFs !== undefined
    ) {
      restoreFsFdState(handle, postFsFdState);
      restoreFsRingProgress(handle, postFsRingProgress);
      clearStaleFsStatusAfterResume(handle, vmPtr, fsResult);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      return completeFsResumeFromSupervisorDecision(handle, worker, vmPtr, fsResult, {
        source: "kernel-worker",
        finalStatus: statusAfterFs,
        forceExecveSpawn: true,
        applyPostFsWake: () => {
          deps.publishRuntimeKernelState(handle);
          deps.notifyHostEffectProgress(handle);
        },
      });
    }
    if (fsResponseDisposition.processOwnerMatches) {
      deps.assertProcessStandardFdIntegrity(handle, "resume-fs");
    }
    const forkShareMembers = deps.kernelShareOtherMembers(handle);
    const syncTargets = forkShareMembers
      .filter((member) => hostExecutionState(member).runningWorker === null);
    const syncSharedOfdAfterFs = ofdStateChanged
      && !regularFileOnlyPendingFs
      && fsResponseDisposition.processOwnerMatches
      && forkShareMembers.length > 0;
    restoreFsFdState(handle, postFsFdState);
    if (fsResponseDisposition.adoptKernelState) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
    }
    const dirtyMemfsHandles: ProcessHandle[] = [];
    if (memfsMutated) {
      setProcessMemfsDirty(handle, true);
      setProcessMemfsPublishPending(handle, true);
      if (fsResponseDisposition.processOwnerMatches) {
        if (memfsPublishReady && forkShareMembers.length > 0) {
          for (const member of forkShareMembers) {
            dirtyMemfsHandles.push(member);
          }
        }
      }
      dirtyMemfsHandles.push(handle);
    }
    restoreFsFdState(handle, postFsFdState);
    restoreFsRingProgress(handle, postFsRingProgress);
    if (fsResponseDisposition.adoptKernelState) {
      clearStaleFsStatusAfterResume(handle, vmPtr, fsResult);
    }
    if (fsResponseDisposition.adoptKernelState) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
    }
    let postFsWakeApplied = false;
    const applyPostFsWake = async () => {
      if (postFsWakeApplied) {
        return;
      }
      postFsWakeApplied = true;
      if (syncSharedOfdAfterFs) {
        await deps.syncSharedOfdStateAfterFs(handle, syncTargets, { sourceVmPtr: vmPtr });
      }
      for (const dirtyHandle of dirtyMemfsHandles) {
        deps.markRuntimeFsDirty(dirtyHandle);
      }
      deps.publishRuntimeKernelState(handle);
      deps.notifyHostEffectProgress(handle);
    };
    return completeFsResumeFromSupervisorDecision(handle, worker, vmPtr, fsResult, {
      source: "kernel-worker",
      applyPostFsWake,
    });
  } catch (error) {
    const errorDetail = formatFsErrorDetail(error);
    deps.appendWorkerDebugLog(handle, `fs-err:0x${vmPtr.toString(16)}${errorDetail}`);
    appendFsLifecycleEvent(handle, "err", vmPtr, `${formatFsRingDebug(handle, vmPtr)}${errorDetail}`);
    if (!processHasWorker(handle, worker) || !deps.threadWorkerStates.has(worker)) {
      return { kind: "process-gone" };
    }
    deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    return { kind: "process-gone" };
  }
}

  async function resumeFsViaKernelWorkerOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    fsEffects: KernelRpcFsEffects = KERNEL_RPC_FS_EFFECTS,
  ): Promise<KernelRpcResumeOutcome> {
    return resumeFsViaKernelWorker(handle, worker, vmPtr, fsEffects);
  }

  return {
    resumeFsViaKernelWorker,
    resumeFsViaKernelWorkerOutcome,
  };
}
