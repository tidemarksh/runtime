import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../../types.js";

export interface WorkerKernelRpcLifecycleDeps {
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  drainProcessOutput(handle: ProcessHandle): boolean;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleBlockedRetryResult(handle: ProcessHandle, worker: Worker, vmPtr: number, status: number): void;
  kickBlockedWorkers(handle: ProcessHandle): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  clearVforkSnapshot(handle: ProcessHandle): void;
}
