/**
 * Process and thread-worker orchestration.
 */

import type { FsSnapshotEntry } from "../../fs/types.js";
import type {
  KernelWorkerPidBytesSnapshot,
  ThreadWorkerOutbound,
} from "../../messages.js";
import {
  formatPipeCounterDebug,
} from "../kernel-share.js";
import {
  captureVforkSnapshot,
} from "../fork-shared.js";
import {
  drainHiddenProcessOutput,
  drainProcessOutput,
  flushStdin,
  settleVisibleProcessOutput,
  writeProcessStdin as bufferProcessStdin,
} from "../stdio.js";
import { createWorkerProcessIoHelpers } from "../process-io.js";
import type {
  CreateProcessOptions,
  ProcessHandle,
} from "../types.js";
import type {
  KernelRpcResumeOutcome,
} from "../kernel-rpc/outcomes.js";
import {
  applyKernelRpcResumeEffects as applyKernelRpcResumeEffectsImpl,
  continueBlockedRpcResumeAfterEffects,
} from "../kernel-rpc/effects.js";
import { createWorkerProcessFactory } from "../create-process.js";

const WASM_PAGE_SIZE = 65536;
const WASM_MAX_PAGES = 131072;
const BOOTSTRAP_MEMORY_BYTES = 64 * 1024 * 1024;
import {
  applySocketStateFromKernelWorker,
  releaseSharedHostStdioViaKernelWorker as releaseSharedHostStdioImpl,
  rehydrateForkSharedStateViaKernelWorker as rehydrateForkSharedStateViaKernelWorkerImpl,
  snapshotProcessFdOfdStateForKernelWorker,
  syncBlockedForkSharedStateViaKernelWorker as syncBlockedForkSharedStateViaKernelWorkerImpl,
  syncForkSharedStateLocally as syncForkSharedStateLocallyImpl,
  syncForkSharedStateForWorkerRuntime as syncForkSharedStateForWorkerRuntimeImpl,
  syncProcessMemfsViaKernelWorker as syncProcessMemfsViaKernelWorkerImpl,
  syncSocketStateViaKernelWorker as syncSocketStateViaKernelWorkerImpl,
  type SyncCause,
} from "../sync.js";
import { createWorkerDestroyHelpers } from "../destroy.js";
import { createWorkerExitHelpers } from "../exit.js";
import { createWorkerForkHandlers } from "../fork.js";
import {
  clearVforkSnapshot,
  createWorkerKernelRpcHelpers,
  resolveProcessCwdInoViaKernelWorker,
} from "../kernel-rpc.js";
import { createWorkerLifecycleHelpers } from "../lifecycle.js";
import { createWorkerMessageHandler } from "../message-handler.js";
import { createWorkerRuntimeContext } from "../runtime-context.js";
import { createWorkerScheduler } from "../scheduler.js";
import {
  hasActiveChildProcess as hasActiveChildProcessImpl,
  pumpActiveChildProcesses as pumpActiveChildProcessesImpl,
} from "../child-processes.js";
import { createWorkerOrchestratorFacade } from "./facade.js";
import type {
  WorkerOrchestrator,
  WorkerOrchestratorDeps,
} from "./types.js";

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

async function completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number, exitVmPtr?: number): Promise<void> {
  return workerLifecycleHelpers.completeProcessExit(handle, exitCode, termSignal, exitVmPtr);
}

function failProcess(handle: ProcessHandle, error: Error): void {
  captureRuntimeFailureSnapshotCtx(handle, 1, 0);
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
  pumpActiveChildProcesses: (parentPid) => {
    const notifyChildProcessProgress = (handle: ProcessHandle): void => kickBlockedWorkers(handle);
    return pumpActiveChildProcessesImpl(
      processHandles,
      parentPid,
      pumpProcess,
      notifyChildProcessProgress,
      threadWorkerStates,
    );
  },
  pumpNetworkConnections,
  resumeFsViaKernelWorker,
  resumeFsViaKernelWorkerOutcome,
  resumeBlockedSyscallOutcome,
  resumeVforkParentAfterChildExit,
  resumeWait4ViaKernelWorker,
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
  markRuntimeFsDirty,
  requestPipeStateSync,
  syncProcessMemfsViaKernelWorker: syncProcessMemfsViaKernelWorkerImpl,
  syncBlockedForkSharedStateViaKernelWorker,
  syncForkSharedStateLocally,
  syncForkSharedStateViaKernelWorker,
  rehydrateForkSharedStateViaKernelWorker,
  syncSocketStateViaKernelWorker,
  snapshotProcessFdOfdStateForKernelWorker,
  applySocketStateFromKernelWorker,
});
const {
  canonicalizeActiveVforkChildViaKernelWorker: canonicalizeActiveVforkChildViaKernelWorkerImpl,
  canonicalizeChildLifecycleViaKernelWorker,
  initializeProcessLifecycleViaKernelWorker,
  recordChildExitViaKernelWorker: recordChildExitViaKernelWorkerImpl,
  registerProcessWithKernelWorker,
  resumeConnectViaKernelWorker: resumeConnectViaKernelWorkerImpl,
  resumeFsViaKernelWorkerOutcome: resumeFsViaKernelWorkerOutcomeImpl,
  resumeFsViaKernelWorker: resumeFsViaKernelWorkerImpl,
  resumeVforkExecveParentViaKernelWorker: resumeVforkExecveParentViaKernelWorkerImpl,
  resumeVforkParentViaKernelWorker: resumeVforkParentViaKernelWorkerImpl,
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
  bootStrapMemoryBytes: BOOTSTRAP_MEMORY_BYTES,
  wasmPageSize: WASM_PAGE_SIZE,
  wasmMaxPages: WASM_MAX_PAGES,
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
  notifyLifecycleProgress: kickBlockedWorkers,
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
  resumeFsViaKernelWorkerOutcome,
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
  notifyHostIoProgress: kickBlockedWorkers,
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

const kernelRpcResumeEffectPorts = {
  blockWorker,
  enqueueWorker,
  handleBlockedRetryResult,
  handleExecveSpawn,
  kickBlockedWorkers,
  requestPipeStateSync,
  settleVisibleProcessOutput,
  syncBlockedForkSharedStateViaKernelWorker,
  syncSocketStateViaKernelWorker,
  threadWorkerStates,
};

const sharedHostStdioReleasePorts = {
  wakePipeAndEpollWaitersInKernelShare,
};

function applyKernelRpcResumeEffects(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  outcome: KernelRpcResumeOutcome,
): Promise<KernelRpcResumeOutcome> {
  return applyKernelRpcResumeEffectsImpl(
    handle,
    worker,
    vmPtr,
    outcome,
    kernelRpcResumeEffectPorts,
  );
}

async function resumeFsViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
): Promise<void> {
  const outcome = await resumeFsViaKernelWorkerImpl(handle, worker, vmPtr);
  await applyKernelRpcResumeEffects(handle, worker, vmPtr, outcome);
}

async function resumeFsViaKernelWorkerOutcome(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
): Promise<KernelRpcResumeOutcome> {
  const outcome = await resumeFsViaKernelWorkerOutcomeImpl(handle, worker, vmPtr);
  return applyKernelRpcResumeEffects(handle, worker, vmPtr, outcome);
}

async function resumeBlockedSyscallOutcome(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  blockedStatus: number,
  timeoutExpired?: boolean,
): Promise<KernelRpcResumeOutcome> {
  const outcome = await workerKernelRpcHelpers.resumeBlockedSyscallOutcome(
    handle,
    worker,
    vmPtr,
    blockedStatus,
    timeoutExpired,
  );
  const appliedOutcome = await applyKernelRpcResumeEffects(handle, worker, vmPtr, outcome);
  return continueBlockedRpcResumeAfterEffects(handle, worker, vmPtr, outcome, appliedOutcome, {
    threadWorkerStates,
  });
}

async function resumeWait4ViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
): Promise<KernelRpcResumeOutcome> {
  const outcome = await workerKernelRpcHelpers.resumeWait4ViaKernelWorker(handle, worker, vmPtr);
  return applyKernelRpcResumeEffects(handle, worker, vmPtr, outcome);
}

async function resumeConnectViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  result: bigint,
): Promise<KernelRpcResumeOutcome> {
  const outcome = await resumeConnectViaKernelWorkerImpl(handle, worker, vmPtr, result);
  return applyKernelRpcResumeEffects(handle, worker, vmPtr, outcome);
}

async function resumeVforkParentViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  waitStatus: number,
): Promise<KernelRpcResumeOutcome> {
  const outcome = await resumeVforkParentViaKernelWorkerImpl(handle, worker, vmPtr, waitStatus);
  return applyKernelRpcResumeEffects(handle, worker, vmPtr, outcome);
}

async function canonicalizeActiveVforkChildViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  provisionalChildPid: number,
): Promise<KernelRpcResumeOutcome> {
  const outcome = await canonicalizeActiveVforkChildViaKernelWorkerImpl(
    handle,
    worker,
    vmPtr,
    provisionalChildPid,
  );
  return applyKernelRpcResumeEffects(handle, worker, vmPtr, outcome);
}

async function resumeVforkExecveParentViaKernelWorker(
  handle: ProcessHandle,
  vmPtr: number,
  childPid: number,
): Promise<KernelRpcResumeOutcome> {
  const outcome = await resumeVforkExecveParentViaKernelWorkerImpl(handle, vmPtr, childPid);
  return applyKernelRpcResumeEffects(handle, {} as Worker, vmPtr, outcome);
}

async function recordChildExitViaKernelWorker(
  parent: ProcessHandle,
  childPid: number,
  waitStatus: number,
): Promise<void> {
  const outcome = await recordChildExitViaKernelWorkerImpl(parent, childPid, waitStatus);
  await applyKernelRpcResumeEffects(parent, {} as Worker, parent.vmPtr, outcome);
}

function syncForkSharedStateLocally(
  handle: ProcessHandle,
  targets: ProcessHandle[],
  options: { cause: SyncCause; includeMemfs?: boolean; trackHeldOfds?: boolean; ofdIds?: number[] },
): void {
  syncForkSharedStateLocallyImpl(handle, targets, options);
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
    sourceVmPtr?: number;
    ofdIds?: number[];
    cause: SyncCause;
  },
): Promise<void> {
  await syncForkSharedStateForWorkerRuntimeImpl(handle, targets, options);
}

async function rehydrateForkSharedStateViaKernelWorker(
  targets: ProcessHandle[],
  ofdIds: number[],
  options: { cause: SyncCause; procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean },
): Promise<void> {
  await rehydrateForkSharedStateViaKernelWorkerImpl(targets, ofdIds, options);
}

async function syncBlockedForkSharedStateViaKernelWorker(
  handle: ProcessHandle,
  sourceVmPtr: number | undefined,
  cause: SyncCause,
): Promise<void> {
  await syncBlockedForkSharedStateViaKernelWorkerImpl(handle, sourceVmPtr, cause);
}

async function syncSocketStateViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<{
  socketUsed: number;
  netBufUsed: bigint;
}> {
  return syncSocketStateViaKernelWorkerImpl(handle, cause);
}

async function releaseSharedHostStdio(handle: ProcessHandle): Promise<void> {
  await releaseSharedHostStdioImpl(handle, sharedHostStdioReleasePorts);
}

function handleWorkerMessage(
  handle: ProcessHandle,
  worker: Worker,
  msg: ThreadWorkerOutbound,
): void {
  handleWorkerMessageImpl(handle, worker, msg);
}

const workerOrchestrator: WorkerOrchestrator = createWorkerOrchestratorFacade({
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
});

export function createWorkerOrchestrator(deps: WorkerOrchestratorDeps): WorkerOrchestrator {
  ensureCompiledModuleImpl = deps.ensureCompiledModule;
  return workerOrchestrator;
}
