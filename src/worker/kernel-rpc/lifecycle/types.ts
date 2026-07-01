import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerSocketStateSnapshot,
  SyncCause,
} from "../../contracts/index.js";
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
  kickBlockedWorkers(handle: ProcessHandle): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  syncSocketStateViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<{ socketUsed: number; netBufUsed: bigint }>;
  syncProcessMemfsViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<void>;
  snapshotProcessFdOfdStateForKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
  ): {
    fdEntries: KernelWorkerFdEntrySnapshot[];
    ofdSlots: KernelWorkerOfdSlotSnapshot[];
  };
  applySocketStateFromKernelWorker(handle: ProcessHandle, snapshot: KernelWorkerSocketStateSnapshot): void;
  clearVforkSnapshot(handle: ProcessHandle): void;
}
