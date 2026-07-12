import type { ThreadWorkerOutbound } from "../../messages.js";
import type { KernelRpcResumeOutcome } from "../kernel-rpc/outcomes.js";
import type { SyncCause } from "../contracts/sync.js";
import type { AttachThreadWorkerRequest, ProcessHandle, ThreadWorkerState, VforkSnapshot } from "../types.js";
import type { WorkerMessageHandlerSyncEffects } from "./sync-effects.js";

export type ThreadWorkerStatusMessage = Extract<ThreadWorkerOutbound, { type: "status" }>;

export interface WorkerMessageHandlerDeps extends Partial<WorkerMessageHandlerSyncEffects> {
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void;
  attachThreadWorker(handle: ProcessHandle, request: AttachThreadWorkerRequest): void;
  blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void;
  canonicalizeActiveVforkChildViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    provisionalChildPid: number,
  ): Promise<KernelRpcResumeOutcome>;
  captureVforkSnapshot(
    handle: ProcessHandle,
    thread: ThreadWorkerState,
  ): VforkSnapshot;
  clearVforkSnapshot(handle: ProcessHandle): void;
  clearBlockedWorker(handle: ProcessHandle, worker: Worker): void;
  completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number, exitVmPtr?: number): Promise<void>;
  commitNetworkConnect(handle: ProcessHandle, vmPtr: number, result?: bigint): Promise<void>;
  drainHiddenProcessOutput(handle: ProcessHandle): boolean;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleExecveSpawn(handle: ProcessHandle, worker: Worker, thread: ThreadWorkerState): Promise<void>;
  notifyWorkerStatusProgress(handle: ProcessHandle): void;
  markRuntimeFsDirty(handle: ProcessHandle): void;
  parkSupervisorFutexWait(handle: ProcessHandle, worker: Worker, vmPtr: number, source: string): void;
  pumpProcess(handle: ProcessHandle): void;
  pruneDetachedProcessWorkers(handle: ProcessHandle, anchorVmPtr: number, currentWorker: Worker | null): void;
  pumpNetworkConnections(handle: ProcessHandle, vmPtr?: number): Promise<boolean>;
  requestEpollDependentOfdStateSync(
    handle: ProcessHandle,
    additionalOfdIds?: readonly number[],
    options?: { mode?: "push" | "pull" },
  ): Promise<void>;
  requestEpollStateSync(handle: ProcessHandle, retryWaiters?: boolean): Promise<void>;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  requestSocketStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  requestThreadWorkerBlockedResume(handle: ProcessHandle, worker: Worker): void;
  resolveNetworkConnect(handle: ProcessHandle, vmPtr: number): Promise<bigint>;
  resumeConnectViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    result: bigint,
  ): Promise<KernelRpcResumeOutcome>;
  resumeFsViaKernelWorker(handle: ProcessHandle, worker: Worker, vmPtr: number): Promise<void>;
  resumeFsViaKernelWorkerOutcome?(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
  ): Promise<KernelRpcResumeOutcome>;
  resumeVforkParentAfterChildExit(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    exitCode: number,
    termSignal: number,
  ): boolean;
  statusDefersOwnedStateSync(handle: ProcessHandle, status: number): boolean;
  syncBlockedForkSharedStateViaKernelWorker(handle: ProcessHandle, sourceVmPtr: number | undefined, cause: SyncCause): Promise<void>;
  syncForkSharedStateViaKernelWorker(
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
    },
  ): Promise<void>;
  syncProcessMemfsViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<void>;
  syncProcessLifecycleViaKernelWorker(handle: ProcessHandle, vmPtr: number): Promise<void>;
}
