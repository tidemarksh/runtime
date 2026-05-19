import type { ThreadWorkerOutbound } from "../../messages.js";
import type { AttachThreadWorkerRequest, ProcessHandle, ThreadWorkerState } from "../types.js";

export type ThreadWorkerStatusMessage = Extract<ThreadWorkerOutbound, { type: "status" }>;

export interface WorkerMessageHandlerDeps {
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  statusOkCompletedSyncEffects: number;
  syncEffectPipeState: number;
  syncEffectSocketState: number;
  syncEffectEpollState: number;
  syncEffectEpollDependentOfdState: number;
  syncEffectProcessLifecycle: number;
  syncEffectSignalWaiters: number;
  syncEffectForkSharedOfdState: number;
  syncEffectProcessMemfs: number;
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
  ): Promise<void>;
  captureVforkSnapshot(
    handle: ProcessHandle,
    thread: ThreadWorkerState,
  ): NonNullable<ProcessHandle["vforkSnapshot"]>;
  clearVforkSnapshot(handle: ProcessHandle): void;
  clearBlockedWorker(handle: ProcessHandle, worker: Worker): void;
  completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number): Promise<void>;
  commitNetworkConnect(handle: ProcessHandle, vmPtr: number, result?: bigint): Promise<void>;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleExecveSpawn(handle: ProcessHandle, worker: Worker, thread: ThreadWorkerState): Promise<void>;
  kickBlockedWorkers(handle: ProcessHandle): void;
  markRuntimeFsDirty(handle: ProcessHandle): void;
  pumpProcess(handle: ProcessHandle): void;
  pruneDetachedProcessWorkers(handle: ProcessHandle, anchorVmPtr: number, currentWorker: Worker | null): void;
  pumpNetworkConnections(handle: ProcessHandle, vmPtr?: number): Promise<boolean>;
  requestEpollDependentOfdStateSync(handle: ProcessHandle): Promise<void>;
  requestEpollStateSync(handle: ProcessHandle, retryWaiters?: boolean): Promise<void>;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  requestSocketStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  resolveNetworkConnect(handle: ProcessHandle, vmPtr: number): Promise<bigint>;
  resumeConnectViaKernelWorker(handle: ProcessHandle, worker: Worker, vmPtr: number, result: bigint): Promise<void>;
  resumeFsViaKernelWorker(handle: ProcessHandle, worker: Worker, vmPtr: number): Promise<void>;
  resumeVforkParentAfterChildExit(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    exitCode: number,
    termSignal: number,
  ): boolean;
  statusDefersOwnedStateSync(handle: ProcessHandle, status: number): boolean;
  syncBlockedForkSharedStateViaKernelWorker(handle: ProcessHandle, sourceVmPtr?: number): Promise<void>;
  syncForkSharedStateViaKernelWorker(
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
    },
  ): Promise<void>;
  syncProcessMemfsViaKernelWorker(handle: ProcessHandle): Promise<void>;
  syncProcessLifecycleViaKernelWorker(handle: ProcessHandle, vmPtr: number): Promise<void>;
}
