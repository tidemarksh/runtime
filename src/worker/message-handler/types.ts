import type { ThreadWorkerOutbound } from "../../messages.js";
import type { ProcessHandle, ThreadWorkerState } from "../types.js";

export type ThreadWorkerStatusMessage = Extract<ThreadWorkerOutbound, { type: "status" }>;

export interface WorkerMessageHandlerDeps {
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  legacyOkSyncEffects: number;
  syncEffectPipeState: number;
  syncEffectSocketState: number;
  syncEffectEpollState: number;
  syncEffectEpollDependentOfdState: number;
  syncEffectProcessLifecycle: number;
  syncEffectSignalWaiters: number;
  syncEffectForkSharedOfdState: number;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void;
  attachThreadWorker(handle: ProcessHandle, worker: Worker, vmPtr: number): void;
  blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void;
  canonicalizeActiveVforkChildViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    provisionalChildPid: number,
  ): Promise<void>;
  captureVforkSnapshot(
    handle: ProcessHandle,
    vmPtr: number,
  ): NonNullable<ProcessHandle["vforkSnapshot"]>;
  clearBlockedWorker(handle: ProcessHandle, worker: Worker): void;
  completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number): Promise<void>;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleExecveSpawn(handle: ProcessHandle, worker: Worker, thread: ThreadWorkerState): Promise<void>;
  kickBlockedWorkers(handle: ProcessHandle): void;
  pumpProcess(handle: ProcessHandle): void;
  requestEpollDependentOfdStateSync(handle: ProcessHandle): Promise<void>;
  requestEpollStateSync(handle: ProcessHandle, retryWaiters?: boolean): Promise<void>;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  requestSocketStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
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
  syncBlockedForkSharedStateViaKernelWorker(handle: ProcessHandle): Promise<void>;
  syncForkSharedStateViaKernelWorker(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options?: { includeMemfs?: boolean; trackHeldOfds?: boolean; preserveSourceRefcounts?: boolean },
  ): Promise<void>;
  syncProcessLifecycleViaKernelWorker(handle: ProcessHandle, vmPtr: number): Promise<void>;
}
