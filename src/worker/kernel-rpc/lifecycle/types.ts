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
  failProcess(handle: ProcessHandle, error: Error): void;
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
