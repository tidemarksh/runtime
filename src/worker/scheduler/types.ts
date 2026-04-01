import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type {
  ThreadWorkerInbound,
  ThreadWorkerOutbound,
} from "../../messages.js";

export interface WorkerSchedulerDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number): Promise<void>;
  failProcess(handle: ProcessHandle, error: Error): void;
  resumeBlockedSyscallLocally(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    status: number,
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
  attachThreadWorker(handle: ProcessHandle, worker: Worker, vmPtr: number): void;
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
  requestEpollDependentOfdStateSync(handle: ProcessHandle): Promise<void>;
  requestEpollStateSync(handle: ProcessHandle, retryWaiters?: boolean): Promise<void>;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  requestSocketStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  wakePipeAndEpollWaitersInKernelShare(handle: ProcessHandle, pipeUsed: bigint): void;
}

export interface WorkerSchedulerQueueHelpers {
  attachThreadWorker(handle: ProcessHandle, worker: Worker, vmPtr: number): void;
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
