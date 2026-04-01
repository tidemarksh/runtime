import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../../types.js";

export interface WorkerKernelRpcLifecycleDeps {
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleBlockedRetryResult(handle: ProcessHandle, worker: Worker, vmPtr: number, status: number): void;
  kickBlockedWorkers(handle: ProcessHandle): void;
  clearVforkSnapshot(handle: ProcessHandle): void;
}
