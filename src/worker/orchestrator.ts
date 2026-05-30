/**
 * Process and thread-worker orchestration.
 */

import type { FsSnapshotEntry } from "../fs/types.js";
import type {
  KernelWorkerPidBytesSnapshot,
  ThreadWorkerOutbound,
} from "../messages.js";
import {
  formatPipeCounterDebug,
} from "./kernel-share.js";
import { kernelWorkerOwnsPipeState } from "./ownership/pipe-control.js";
import {
  captureVforkSnapshot,
} from "./fork-shared.js";
import { collectTrackableForkSharedOfds } from "./ownership/ofd-peer.js";
import {
  drainHiddenProcessOutput,
  drainProcessOutput,
  flushStdin,
  writeProcessStdin as bufferProcessStdin,
} from "./stdio.js";
import { createWorkerProcessIoHelpers } from "./process-io.js";
import type {
  CreateProcessOptions,
  ProcessHandle,
} from "./types.js";
import { createWorkerProcessFactory } from "./create-process.js";
import {
  rehydrateForkSharedStateViaKernelWorker as rehydrateForkSharedStateViaKernelWorkerImpl,
  syncBlockedForkSharedStateViaKernelWorker as syncBlockedForkSharedStateViaKernelWorkerImpl,
  syncForkSharedStateLocally as syncForkSharedStateLocallyImpl,
  syncForkSharedStateViaKernelWorker as syncForkSharedStateViaKernelWorkerImpl,
  syncPipeStateViaKernelWorker as syncPipeStateViaKernelWorkerImpl,
  syncSocketStateViaKernelWorker as syncSocketStateViaKernelWorkerImpl,
} from "./sync.js";
import { createWorkerDestroyHelpers } from "./destroy.js";
import { createWorkerExitHelpers } from "./exit.js";
import { createWorkerForkHandlers } from "./fork.js";
import {
  clearVforkSnapshot,
  createWorkerKernelRpcHelpers,
  resolveProcessCwdInoViaKernelWorker,
} from "./kernel-rpc.js";
import { createWorkerLifecycleHelpers } from "./lifecycle.js";
import { createWorkerMessageHandler } from "./message-handler.js";
import { createWorkerRuntimeContext } from "./runtime-context.js";
import { createWorkerScheduler } from "./scheduler.js";
import {
  hasActiveChildProcess as hasActiveChildProcessImpl,
  pumpActiveChildProcesses as pumpActiveChildProcessesImpl,
} from "./child-processes.js";

interface WorkerOrchestratorDeps {
  ensureCompiledModule(wasmBytes: BufferSource): Promise<WebAssembly.Module>;
}

export interface WorkerOrchestrator {
  clearRuntimeFsState(runtimeId: number): void;
  clearRuntimeFailureSnapshot(runtimeId: number): void;
  createProcess(
    wasmBytes: BufferSource,
    elfData: Uint8Array,
    options?: CreateProcessOptions,
  ): Promise<ProcessHandle>;
  debugRuntimeExecutionStats(runtimeId: number): ReturnType<typeof debugRuntimeExecutionStatsCtx>;
  debugRuntimeLastFailureSnapshot(runtimeId: number): ReturnType<typeof debugRuntimeLastFailureSnapshotCtx>;
  debugRuntimeProcessHandles(runtimeId: number): string;
  debugRuntimeWorkerLogFocus(runtimeId: number): ReturnType<typeof debugRuntimeWorkerLogFocusCtx>;
  destroyProcessTree(rootPid: number, runtimeId: number, signal?: number): void;
  destroyRuntimeProcesses(runtimeId: number, signal?: number): void;
  flushRuntimeFsStateFromDirtyHandle(runtimeId: number): { version: number; entries: FsSnapshotEntry[] } | null;
  getRuntimeFsState(runtimeId: number): { version: number; entries: FsSnapshotEntry[] } | null;
  injectHttpRequest(handle: ProcessHandle, port: number, request: string): Promise<string>;
  mirrorRuntimeFsEntry(runtimeId: number, entry: FsSnapshotEntry): void;
  primeRuntimeFsState(runtimeId: number, entries: FsSnapshotEntry[]): number;
  publishRuntimeFsState(runtimeId: number, entries: FsSnapshotEntry[]): number;
  replaceRuntimeFsSnapshot(runtimeId: number, entries: FsSnapshotEntry[]): void;
  writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void;
}

let ensureCompiledModuleImpl: WorkerOrchestratorDeps["ensureCompiledModule"] | null = null;
const workerRuntimeContext = createWorkerRuntimeContext({
  formatPipeCounterDebug,
});
const {
  appendRuntimeLifecycleEvent: appendRuntimeLifecycleEventCtx,
  assertProcessStandardFdIntegrity: assertProcessStandardFdIntegrityCtx,
  captureRuntimeFailureSnapshot: captureRuntimeFailureSnapshotCtx,
  clearRuntimeFsState: clearRuntimeFsStateCtx,
  clearRuntimeFailureSnapshot: clearRuntimeFailureSnapshotCtx,
  debugRuntimeExecutionStats: debugRuntimeExecutionStatsCtx,
  debugRuntimeLastFailureSnapshot: debugRuntimeLastFailureSnapshotCtx,
  debugRuntimeProcessHandles: debugRuntimeProcessHandlesCtx,
  debugRuntimeWorkerLogFocus: debugRuntimeWorkerLogFocusCtx,
  flushRuntimeFsStateFromDirtyHandle: flushRuntimeFsStateFromDirtyHandleCtx,
  formatAllLiveThreadDebug: formatAllLiveThreadDebugCtx,
  getRuntimeFsState: getRuntimeFsStateCtx,
  markRuntimeFsDirty: markRuntimeFsDirtyCtx,
  mirrorRuntimeFsEntry: mirrorRuntimeFsEntryCtx,
  processHandles,
  primeRuntimeFsState: primeRuntimeFsStateCtx,
  publishRuntimeFsState: publishRuntimeFsStateCtx,
  replaceRuntimeFsSnapshot: replaceRuntimeFsSnapshotCtx,
  runtimeFsDirtyHandles,
  snapshotProcessFs: snapshotProcessFsCtx,
  threadWorkerStates,
} = workerRuntimeContext;
let workerKernelRpcHelpers: ReturnType<typeof createWorkerKernelRpcHelpers>;
let workerLifecycleHelpers: ReturnType<typeof createWorkerLifecycleHelpers>;

async function completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number): Promise<void> {
  return workerLifecycleHelpers.completeProcessExit(handle, exitCode, termSignal);
}

function failProcess(handle: ProcessHandle, error: Error): void {
  workerLifecycleHelpers.failProcess(handle, error);
}

function resumeVforkParentAfterChildExit(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  exitCode: number,
  termSignal: number,
): boolean {
  return workerLifecycleHelpers.resumeVforkParentAfterChildExit(
    handle,
    worker,
    vmPtr,
    exitCode,
    termSignal,
  );
}

function statusDefersOwnedStateSync(handle: ProcessHandle, status: number): boolean {
  return workerLifecycleHelpers.statusDefersOwnedStateSync(handle, status);
}

const workerScheduler = createWorkerScheduler({
  appendRuntimeLifecycleEvent,
  threadWorkerStates,
  completeProcessExit,
  failProcess,
  hasActiveChildProcess: (parentPid) => hasActiveChildProcessImpl(processHandles, parentPid, threadWorkerStates),
  pumpActiveChildProcesses: (parentPid) => (
    pumpActiveChildProcessesImpl(processHandles, parentPid, pumpProcess, kickBlockedWorkers, threadWorkerStates)
  ),
  pumpNetworkConnections,
  resumeFsViaKernelWorker,
  resumeBlockedSyscallLocally: (handle, worker, vmPtr, blockedStatus, timeoutExpired) => (
    workerKernelRpcHelpers.resumeBlockedSyscallLocally(
      handle,
      worker,
      vmPtr,
      blockedStatus,
      timeoutExpired,
    )
  ),
  resumeVforkParentAfterChildExit,
  resumeWait4ViaKernelWorker: (handle, worker, vmPtr) => (
    workerKernelRpcHelpers.resumeWait4ViaKernelWorker(handle, worker, vmPtr)
  ),
  handleWorkerMessage,
});
const {
  appendWorkerDebugLog,
  attachThreadWorker,
  blockWorker,
  clearBlockedWorker,
  enqueueWorker,
  handleBlockedRetryResult,
  kickBlockedWorkers,
  pumpProcess,
  pruneDetachedProcessWorkers,
  requestEpollDependentOfdStateSync,
  requestEpollStateSync,
  requestPipeStateSync,
  requestSocketStateSync,
  wakePipeAndEpollWaitersInKernelShare,
} = workerScheduler;
workerKernelRpcHelpers = createWorkerKernelRpcHelpers({
  processHandles,
  threadWorkerStates,
  appendRuntimeLifecycleEvent,
  appendWorkerDebugLog,
  assertProcessStandardFdIntegrity,
  blockWorker,
  enqueueWorker,
  failProcess,
  handleBlockedRetryResult,
  kickBlockedWorkers,
  markRuntimeFsDirty,
  requestPipeStateSync,
  syncBlockedForkSharedStateViaKernelWorker,
  syncForkSharedStateLocally,
  syncForkSharedStateViaKernelWorker,
  rehydrateForkSharedStateViaKernelWorker,
  syncPipeStateViaKernelWorker,
  syncSocketStateViaKernelWorker,
  collectTrackableForkSharedOfds,
});
const {
  canonicalizeActiveVforkChildViaKernelWorker,
  canonicalizeChildLifecycleViaKernelWorker,
  initializeProcessLifecycleViaKernelWorker,
  recordChildExitViaKernelWorker,
  registerProcessWithKernelWorker,
  releaseHostStdioViaKernelWorker,
  resumeConnectViaKernelWorker,
  resumeFsViaKernelWorker: resumeFsViaKernelWorkerImpl,
  resumeVforkExecveParentViaKernelWorker,
  resumeVforkParentViaKernelWorker,
  syncProcessMemfsViaKernelWorker,
  syncProcessLifecycleViaKernelWorker,
  unregisterProcessWithKernelWorker,
} = workerKernelRpcHelpers;
const workerForkHandlers = createWorkerForkHandlers({
  processHandles,
  appendRuntimeLifecycleEvent,
  appendWorkerDebugLog,
  assertProcessStandardFdIntegrity,
  attachThreadWorker,
  clearVforkSnapshot,
  enqueueWorker,
  failProcess,
  initializeProcessLifecycleViaKernelWorker,
  kickBlockedWorkers,
  recordChildExitViaKernelWorker,
  requestPipeStateSync,
  rehydrateForkSharedStateViaKernelWorker,
  registerProcessWithKernelWorker,
  resumeVforkExecveParentViaKernelWorker,
  syncForkSharedStateLocally,
  syncForkSharedStateViaKernelWorker,
  syncProcessMemfsViaKernelWorker,
  canonicalizeChildLifecycleViaKernelWorker,
  bootStrapMemoryBytes: 64 * 1024 * 1024,
  wasmPageSize: 65536,
  wasmMaxPages: 64512,
});
const {
  handleExecveSpawn,
} = workerForkHandlers;
let handleWorkerMessageImpl: (handle: ProcessHandle, worker: Worker, msg: ThreadWorkerOutbound) => void;
const workerDestroyHelpers = createWorkerDestroyHelpers({
  processHandles,
  threadWorkerStates,
  runtimeFsDirtyHandles,
  appendRuntimeLifecycleEvent,
  clearBlockedWorker,
  clearVforkSnapshot,
  unregisterProcessWithKernelWorker,
});
const {
  destroyProcessHandle,
  destroyProcessTree,
  destroyRuntimeProcesses,
} = workerDestroyHelpers;
const workerExitHelpers = createWorkerExitHelpers({
  processHandles,
  appendRuntimeLifecycleEvent,
  appendWorkerDebugLog,
  captureRuntimeFailureSnapshot: captureRuntimeFailureSnapshotCtx,
  debugRuntimeProcessHandles,
  drainHiddenProcessOutput,
  drainProcessOutput,
  destroyProcessHandle,
  formatAllLiveThreadDebug,
  kickBlockedWorkers,
  publishRuntimeFsState: publishRuntimeFsStateCtx,
  recordChildExitViaKernelWorker,
  releaseSharedHostStdio,
  requestPipeStateSync,
  snapshotProcessFs: snapshotProcessFsCtx,
  syncForkSharedStateViaKernelWorker,
  unregisterProcessWithKernelWorker,
});
workerLifecycleHelpers = createWorkerLifecycleHelpers({
  appendRuntimeLifecycleEvent,
  drainProcessOutput,
  enqueueWorker,
  getProcessByPid: (pid) => processHandles.get(pid),
  kickBlockedWorkers,
  recordChildExitViaKernelWorker,
  releaseSharedHostStdio,
  requestEpollStateSync,
  requestPipeStateSync,
  requestSocketStateSync,
  resumeVforkParentViaKernelWorker,
  syncForkSharedStateViaKernelWorker,
  workerExitCompleteProcessExit: workerExitHelpers.completeProcessExit,
  workerExitFailProcess: workerExitHelpers.failProcess,
  workerExitPrepublishParentWaitStatus: workerExitHelpers.prepublishParentWaitStatus,
});
const WASM_PAGE_SIZE = 65536;
const WASM_MAX_PAGES = 64512;
const BOOTSTRAP_MEMORY_BYTES = 64 * 1024 * 1024;
const FS_EFFECT_MEMFS_MUTATION = 1 << 0;
const FS_EFFECT_SHARED_OFD_STATE = 1 << 1;
const FS_EFFECT_MEMFS_FILE_DATA = 1 << 2;
const SYNC_EFFECT_PIPE_STATE = 1 << 0;
const SYNC_EFFECT_SOCKET_STATE = 1 << 1;
const SYNC_EFFECT_EPOLL_STATE = 1 << 2;
const SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE = 1 << 3;
const SYNC_EFFECT_PROCESS_LIFECYCLE = 1 << 4;
const SYNC_EFFECT_SIGNAL_WAITERS = 1 << 5;
const SYNC_EFFECT_FORK_SHARED_OFD_STATE = 1 << 6;
const SYNC_EFFECT_PROCESS_MEMFS = 1 << 7;
const STATUS_OK_COMPLETED_SYNC_EFFECTS = SYNC_EFFECT_PIPE_STATE
  | SYNC_EFFECT_SOCKET_STATE
  | SYNC_EFFECT_EPOLL_STATE
  | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
  | SYNC_EFFECT_PROCESS_LIFECYCLE;

async function ensureCompiledModule(wasmBytes: BufferSource): Promise<WebAssembly.Module> {
  if (!ensureCompiledModuleImpl) {
    throw new Error("worker orchestrator is not initialized");
  }
  return ensureCompiledModuleImpl(wasmBytes);
}

let resolveNetworkConnectImpl: (handle: ProcessHandle, vmPtr: number) => Promise<bigint>;
let pumpNetworkConnectionsImpl: (handle: ProcessHandle, vmPtr?: number) => Promise<boolean>;
let commitNetworkConnectImpl: (handle: ProcessHandle, vmPtr: number, result?: bigint) => Promise<void>;

function resolveNetworkConnect(handle: ProcessHandle, vmPtr: number): Promise<bigint> {
  return resolveNetworkConnectImpl(handle, vmPtr);
}

function pumpNetworkConnections(handle: ProcessHandle, vmPtr?: number): Promise<boolean> {
  return pumpNetworkConnectionsImpl(handle, vmPtr);
}

function commitNetworkConnect(handle: ProcessHandle, vmPtr: number, result?: bigint): Promise<void> {
  return commitNetworkConnectImpl(handle, vmPtr, result);
}

handleWorkerMessageImpl = createWorkerMessageHandler({
  processHandles,
  threadWorkerStates,
  statusOkCompletedSyncEffects: STATUS_OK_COMPLETED_SYNC_EFFECTS,
  syncEffectPipeState: SYNC_EFFECT_PIPE_STATE,
  syncEffectSocketState: SYNC_EFFECT_SOCKET_STATE,
  syncEffectEpollState: SYNC_EFFECT_EPOLL_STATE,
  syncEffectEpollDependentOfdState: SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE,
  syncEffectProcessLifecycle: SYNC_EFFECT_PROCESS_LIFECYCLE,
  syncEffectSignalWaiters: SYNC_EFFECT_SIGNAL_WAITERS,
  syncEffectForkSharedOfdState: SYNC_EFFECT_FORK_SHARED_OFD_STATE,
  syncEffectProcessMemfs: SYNC_EFFECT_PROCESS_MEMFS,
  appendRuntimeLifecycleEvent,
  appendWorkerDebugLog,
  assertProcessStandardFdIntegrity,
  attachThreadWorker,
  blockWorker,
  canonicalizeActiveVforkChildViaKernelWorker,
  captureVforkSnapshot,
  clearVforkSnapshot,
  clearBlockedWorker,
  completeProcessExit,
  enqueueWorker,
  failProcess,
  handleExecveSpawn,
  kickBlockedWorkers,
  markRuntimeFsDirty,
  pumpNetworkConnections,
  commitNetworkConnect,
  pumpProcess,
  pruneDetachedProcessWorkers,
  requestEpollDependentOfdStateSync,
  requestEpollStateSync,
  requestPipeStateSync,
  requestSocketStateSync,
  resolveNetworkConnect,
  resumeConnectViaKernelWorker,
  resumeFsViaKernelWorker,
  resumeVforkParentAfterChildExit,
  statusDefersOwnedStateSync,
  syncBlockedForkSharedStateViaKernelWorker,
  syncForkSharedStateViaKernelWorker,
  syncProcessMemfsViaKernelWorker,
  syncProcessLifecycleViaKernelWorker,
});
const workerProcessFactory = createWorkerProcessFactory({
  processHandles,
  attachThreadWorker,
  assertProcessStandardFdIntegrity,
  ensureCompiledModule,
  getRuntimeFsEntries: (runtimeId) => getRuntimeFsState(runtimeId)?.entries ?? [],
  initializeProcessLifecycleViaKernelWorker,
  registerProcessWithKernelWorker,
  resolveProcessCwdInoViaKernelWorker,
  bootstrapMemoryBytes: BOOTSTRAP_MEMORY_BYTES,
  wasmPageSize: WASM_PAGE_SIZE,
  wasmMaxPages: WASM_MAX_PAGES,
});
const workerProcessIoHelpers = createWorkerProcessIoHelpers({
  appendRuntimeLifecycleEvent,
  appendWorkerDebugLog,
  drainProcessOutput,
  failProcess,
  flushStdin,
  getActiveHandle: (handle) => processHandles.get(handle.pid),
  isActiveHandle: (handle) => handle.pid <= 0 || processHandles.get(handle.pid) === handle,
  kickBlockedWorkers,
  pumpProcess,
  requestPipeStateSync,
  requestSocketStateSync,
  writeProcessStdin: bufferProcessStdin,
});
resolveNetworkConnectImpl = workerProcessIoHelpers.resolveNetworkConnect;
pumpNetworkConnectionsImpl = workerProcessIoHelpers.pumpNetworkConnections;
commitNetworkConnectImpl = workerProcessIoHelpers.commitNetworkConnect;

export function primeRuntimeFsState(runtimeId: number, entries: FsSnapshotEntry[]): number {
  return primeRuntimeFsStateCtx(runtimeId, entries);
}

export function publishRuntimeFsState(runtimeId: number, entries: FsSnapshotEntry[]): number {
  return publishRuntimeFsStateCtx(runtimeId, entries);
}

export function getRuntimeFsState(
  runtimeId: number,
): { version: number; entries: FsSnapshotEntry[] } | null {
  return getRuntimeFsStateCtx(runtimeId);
}

export function flushRuntimeFsStateFromDirtyHandle(
  runtimeId: number,
): { version: number; entries: FsSnapshotEntry[] } | null {
  return flushRuntimeFsStateFromDirtyHandleCtx(runtimeId);
}

export function clearRuntimeFsState(runtimeId: number): void {
  clearRuntimeFsStateCtx(runtimeId);
}

export function clearRuntimeFailureSnapshot(runtimeId: number): void {
  clearRuntimeFailureSnapshotCtx(runtimeId);
}

export async function createProcess(
  wasmBytes: BufferSource,
  elfData: Uint8Array,
  options: CreateProcessOptions = {},
): Promise<ProcessHandle> {
  return workerProcessFactory.createProcess(wasmBytes, elfData, options);
}

export function writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void {
  workerProcessIoHelpers.writeProcessStdin(handle, data);
}

export async function injectHttpRequest(
  handle: ProcessHandle,
  port: number,
  request: string,
): Promise<string> {
  return workerProcessIoHelpers.injectHttpRequest(handle, port, request);
}

export function debugRuntimeProcessHandles(runtimeId: number): string {
  return debugRuntimeProcessHandlesCtx(runtimeId);
}

export function debugRuntimeExecutionStats(runtimeId: number) {
  return debugRuntimeExecutionStatsCtx(runtimeId);
}

export function debugRuntimeLastFailureSnapshot(runtimeId: number) {
  return debugRuntimeLastFailureSnapshotCtx(runtimeId);
}

export function debugRuntimeWorkerLogFocus(runtimeId: number) {
  return debugRuntimeWorkerLogFocusCtx(runtimeId);
}

function appendRuntimeLifecycleEvent(runtimeId: number, event: string): void {
  appendRuntimeLifecycleEventCtx(runtimeId, event);
}

function formatAllLiveThreadDebug(handle: ProcessHandle): string {
  return formatAllLiveThreadDebugCtx(handle);
}

function assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void {
  assertProcessStandardFdIntegrityCtx(handle, context);
}

export function mirrorRuntimeFsEntry(runtimeId: number, entry: FsSnapshotEntry): void {
  mirrorRuntimeFsEntryCtx(runtimeId, entry);
}

export function replaceRuntimeFsSnapshot(runtimeId: number, entries: FsSnapshotEntry[]): void {
  replaceRuntimeFsSnapshotCtx(runtimeId, entries);
}

function markRuntimeFsDirty(handle: ProcessHandle): void {
  markRuntimeFsDirtyCtx(handle);
}

async function resumeFsViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
): Promise<void> {
  await resumeFsViaKernelWorkerImpl(handle, worker, vmPtr, {
    memfsMutation: FS_EFFECT_MEMFS_MUTATION,
    memfsFileData: FS_EFFECT_MEMFS_FILE_DATA,
    sharedOfdState: FS_EFFECT_SHARED_OFD_STATE,
  });
}

function syncForkSharedStateLocally(
  handle: ProcessHandle,
  targets: ProcessHandle[],
  options: { includeMemfs?: boolean; trackHeldOfds?: boolean; ofdIds?: number[] } = {},
): void {
  syncForkSharedStateLocallyImpl(
    handle,
    targets,
    { ...options, cause: { kind: "syscall", nr: -1n, name: "fork-shared-local-sync" } },
    collectTrackableForkSharedOfds,
  );
}

async function syncForkSharedStateViaKernelWorker(
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
    syncForkSharedStateLocally(handle, targets, options);
    return;
  }

  if (options.trackHeldOfds === true && options.deltaOfdIds !== true) {
    for (const ofdId of collectTrackableForkSharedOfds(handle)) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
    }
  }
  await syncForkSharedStateViaKernelWorkerImpl(
    handle,
    targets,
    { ...options, cause: { kind: "syscall", nr: -1n, name: "fork-shared-state-sync" } },
  );
}

async function rehydrateForkSharedStateViaKernelWorker(
  targets: ProcessHandle[],
  ofdIds: number[],
  options: { procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean } = {},
): Promise<void> {
  await rehydrateForkSharedStateViaKernelWorkerImpl(targets, ofdIds, {
    ...options,
    cause: { kind: "syscall", nr: -1n, name: "fork-shared-rehydrate" },
  });
}

async function syncBlockedForkSharedStateViaKernelWorker(
  handle: ProcessHandle,
  sourceVmPtr?: number,
): Promise<void> {
  await syncBlockedForkSharedStateViaKernelWorkerImpl(
    handle,
    sourceVmPtr,
    { kind: "blocked-rpc", status: handle.kernel.abi_status_pipe_wait() },
  );
}

async function syncPipeStateViaKernelWorker(handle: ProcessHandle): Promise<bigint> {
  return syncPipeStateViaKernelWorkerImpl(handle, { kind: "syscall", nr: -1n, name: "pipe-state-sync" });
}

async function syncSocketStateViaKernelWorker(handle: ProcessHandle): Promise<{
  socketUsed: number;
  netBufUsed: bigint;
}> {
  return syncSocketStateViaKernelWorkerImpl(handle, { kind: "syscall", nr: -1n, name: "socket-state-sync" });
}

async function releaseSharedHostStdio(handle: ProcessHandle): Promise<void> {
  const hadLocalHostStdio = handle.stdinPipe !== 0 || handle.stdoutPipe !== 0 || handle.stderrPipe !== 0;
  if (!hadLocalHostStdio) {
    return;
  }
  const shared = handle.hostStdio;
  if (shared.refCount > 0) {
    shared.refCount -= 1;
  }
  handle.stdinPipe = 0;
  handle.stdoutPipe = 0;
  handle.stderrPipe = 0;
  if (shared.refCount > 0) {
    return;
  }
  if (handle.stdioOwner !== null && !handle.stdioOwner.exitInProgress) {
    return;
  }
  const pipeUsed = await releaseHostStdioViaKernelWorker(
    handle,
    { kind: "syscall", nr: -1n, name: "release-host-stdio" },
  );
  if (kernelWorkerOwnsPipeState(handle)) {
    wakePipeAndEpollWaitersInKernelShare(handle, pipeUsed);
  }
}

function handleWorkerMessage(
  handle: ProcessHandle,
  worker: Worker,
  msg: ThreadWorkerOutbound,
): void {
  handleWorkerMessageImpl(handle, worker, msg);
}

const workerOrchestrator: WorkerOrchestrator = {
  clearRuntimeFsState,
  clearRuntimeFailureSnapshot,
  createProcess,
  debugRuntimeExecutionStats,
  debugRuntimeLastFailureSnapshot,
  debugRuntimeProcessHandles,
  debugRuntimeWorkerLogFocus,
  destroyProcessTree,
  destroyRuntimeProcesses,
  flushRuntimeFsStateFromDirtyHandle,
  getRuntimeFsState,
  injectHttpRequest,
  mirrorRuntimeFsEntry,
  primeRuntimeFsState,
  publishRuntimeFsState,
  replaceRuntimeFsSnapshot,
  writeProcessStdin,
};

export function createWorkerOrchestrator(deps: WorkerOrchestratorDeps): WorkerOrchestrator {
  ensureCompiledModuleImpl = deps.ensureCompiledModule;
  return workerOrchestrator;
}
