import type {
  AttachThreadWorkerRequest,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type {
  ThreadWorkerInbound,
  ThreadWorkerOutbound,
} from "../../messages.js";

export interface WorkerSchedulerDeps {
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number): Promise<void>;
  failProcess(handle: ProcessHandle, error: Error): void;
  hasActiveChildProcess(parentPid: number): boolean;
  pumpNetworkConnections(handle: ProcessHandle, vmPtr?: number): Promise<boolean>;
  resumeBlockedSyscallLocally(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    status: number,
    timeoutExpired?: boolean,
  ): Promise<void>;
  resumeVforkParentAfterChildExit(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    exitCode: number,
    termSignal: number,
  ): boolean;
  resumeWait4ViaKernelWorker(handle: ProcessHandle, worker: Worker, vmPtr: number): Promise<void>;
  handleWorkerMessage(handle: ProcessHandle, worker: Worker, msg: ThreadWorkerOutbound): void;
}

export interface WorkerScheduler {
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  attachThreadWorker(handle: ProcessHandle, request: AttachThreadWorkerRequest): void;
  blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void;
  clearBlockedWorker(handle: ProcessHandle, worker: Worker): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  handleBlockedRetryResult(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    status: number,
  ): void;
  kickBlockedWorkers(handle: ProcessHandle): void;
  pumpProcess(handle: ProcessHandle): void;
  pruneDetachedProcessWorkers(handle: ProcessHandle, anchorVmPtr: number, currentWorker: Worker | null): void;
  requestEpollDependentOfdStateSync(handle: ProcessHandle): Promise<void>;
  requestEpollStateSync(handle: ProcessHandle, retryWaiters?: boolean): Promise<void>;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  requestSocketStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  wakePipeAndEpollWaitersInKernelShare(handle: ProcessHandle, pipeUsed: bigint): void;
}

export interface WorkerSchedulerQueueHelpers {
  attachThreadWorker(handle: ProcessHandle, request: AttachThreadWorkerRequest): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  pumpProcess(handle: ProcessHandle): void;
}

export interface WorkerSchedulerStateSyncHelpers {
  requestEpollDependentOfdStateSync(handle: ProcessHandle): Promise<void>;
  requestEpollStateSync(handle: ProcessHandle, retryWaiters?: boolean): Promise<void>;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  requestSocketStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  wakePipeAndEpollWaitersInKernelShare(handle: ProcessHandle, pipeUsed: bigint): void;
}

export type { ThreadWorkerInbound };
