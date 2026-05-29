import {
  publishKernelRuntimeState,
} from "../../bridge/runtime-state.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";

export interface FutexSchedulerDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  formatChildExitRecords(handle: ProcessHandle): string;
  childExitRecordsAlreadyApplied(handle: ProcessHandle, recordsKey: string): boolean;
  applyLocalChildExitRecords(handle: ProcessHandle): void;
}

export interface FutexScheduler {
  applyChildExitRecordsForWorkerLocalWaits(handle: ProcessHandle): void;
}

export function createFutexScheduler(deps: FutexSchedulerDeps): FutexScheduler {
  function hasWorkerLocalFutexWait(handle: ProcessHandle): boolean {
    const futexWaitStatus = handle.kernel.abi_status_futex_wait();
    for (const worker of handle.workers) {
      const thread = deps.threadWorkerStates.get(worker);
      if (
        thread?.ownerPid === handle.pid
        && thread.waitingInWorkerStatus === futexWaitStatus
      ) {
        return true;
      }
    }
    return false;
  }

  function applyChildExitRecordsForWorkerLocalWaits(handle: ProcessHandle): void {
    if (handle.childExitSyncRecords.size === 0 || !hasWorkerLocalFutexWait(handle)) {
      return;
    }
    const recordsKey = deps.formatChildExitRecords(handle);
    if (deps.childExitRecordsAlreadyApplied(handle, recordsKey)) {
      return;
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `futex-wait-apply-child-records:${handle.pid}:${recordsKey}`,
    );
    deps.applyLocalChildExitRecords(handle);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
  }

  return {
    applyChildExitRecordsForWorkerLocalWaits,
  };
}
