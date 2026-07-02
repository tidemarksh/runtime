import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";

export interface VforkExclusiveScheduler {
  hasActiveExclusiveVforkWorker(handle: ProcessHandle): boolean;
  canScheduleWorker(handle: ProcessHandle, worker: Worker): boolean;
}

export function createVforkExclusiveScheduler(
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): VforkExclusiveScheduler {
  function hasActiveExclusiveVforkWorker(handle: ProcessHandle): boolean {
    const worker = handle.vforkWorker;
    if (handle.vforkSnapshot === null || worker === null) {
      return false;
    }
    if (handle.runningWorker === worker) {
      return true;
    }
    if (handle.vforkReadyWorkers.includes(worker)) {
      return true;
    }
    if (handle.blockedWorkers.has(worker)) {
      return true;
    }
    const thread = threadWorkerStates.get(worker);
    return thread?.waitingInWorkerStatus !== null || thread?.ownerAsyncStatus !== null;
  }

  function canScheduleWorker(handle: ProcessHandle, worker: Worker): boolean {
    return !hasActiveExclusiveVforkWorker(handle) || handle.vforkWorker === worker;
  }

  return {
    hasActiveExclusiveVforkWorker,
    canScheduleWorker,
  };
}
