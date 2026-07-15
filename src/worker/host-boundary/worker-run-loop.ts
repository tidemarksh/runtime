import { hostExecutionState } from "../host-execution-state.js";
import { processHasWorker } from "../process-worker-state.js";
import type {
  HostBlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import {
  type SupervisorBlockedWaitRetryFlags,
} from "../../supervisor-blocked-resume.js";
import { clearThreadRunningGeneration } from "../thread-host-state.js";
import {
  beginBlockedRpc as beginHostBlockedRpc,
  consumeBlockedRpcDeferredRetry,
  finishBlockedRpc as finishHostBlockedRpc,
} from "../host-blocked-state.js";
import { clearForkSharedSyncIfCurrent } from "../fork-shared-sync-state.js";
import { threadLatestStepAnswered } from "../thread-activity-state.js";

export function logNetworkDebug(_event: string): void {
  // Runtime diagnostics are kept in the per-process worker debug buffer.
}

export interface RunLoopSchedulerDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  retryHostBlockedWorker(handle: ProcessHandle, worker: Worker): void;
  clearHostBlockedRetryTimer(handle: ProcessHandle, worker: Worker): void;
  scheduleSupervisorBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    flags?: SupervisorBlockedWaitRetryFlags,
  ): void;
}

export interface RunLoopScheduler {
  beginBlockedRpc(blocked: HostBlockedWorkerState): number;
  finishBlockedRpc(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    rpcGeneration: number,
  ): HostBlockedWorkerState | null;
  observeForkSharedSyncCompletion(
    handle: ProcessHandle,
    worker: Worker,
    promise: Promise<void>,
  ): void;
  clearStaleRunningWorker(handle: ProcessHandle, blockedWorker: Worker): boolean;
}

export function createRunLoopScheduler(deps: RunLoopSchedulerDeps): RunLoopScheduler {
  const observedForkSharedSyncPromises = new WeakSet<Promise<void>>();

  function beginBlockedRpc(blocked: HostBlockedWorkerState): number {
    return beginHostBlockedRpc(blocked);
  }

  function finishBlockedRpc(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    rpcGeneration: number,
  ): HostBlockedWorkerState | null {
    const latestBlocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
    if (
      !latestBlocked
      || latestBlocked.vmPtr !== vmPtr
      || !finishHostBlockedRpc(latestBlocked, rpcGeneration)
    ) {
      return null;
    }
    if (consumeBlockedRpcDeferredRetry(latestBlocked)) {
      deps.clearHostBlockedRetryTimer(handle, worker);
      deps.scheduleSupervisorBlockedRetry(handle, worker, ["host-progress"]);
    }
    return latestBlocked;
  }

  function observeForkSharedSyncCompletion(
    handle: ProcessHandle,
    worker: Worker,
    promise: Promise<void>,
  ): void {
    if (observedForkSharedSyncPromises.has(promise)) {
      return;
    }
    observedForkSharedSyncPromises.add(promise);
    const onSettled = (): void => {
      clearForkSharedSyncIfCurrent(handle, promise);
      const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
      if (!blocked) {
        return;
      }
      deps.clearHostBlockedRetryTimer(handle, worker);
      deps.scheduleSupervisorBlockedRetry(
        handle,
        worker,
        ["host-progress"],
      );
    };
    promise.then(onSettled, onSettled);
  }

  function clearStaleRunningWorker(handle: ProcessHandle, blockedWorker: Worker): boolean {
    const runningWorker = hostExecutionState(handle).runningWorker;
    if (runningWorker === null || runningWorker === blockedWorker) {
      return false;
    }
    if (!processHasWorker(handle, runningWorker)) {
      hostExecutionState(handle).runningWorker = null;
      const detachedThread = deps.threadWorkerStates.get(runningWorker);
      if (detachedThread) {
        clearThreadRunningGeneration(detachedThread);
      }
      return true;
    }
    const runningThread = deps.threadWorkerStates.get(runningWorker);
    if (!runningThread) {
      hostExecutionState(handle).runningWorker = null;
      return true;
    }
    if (threadLatestStepAnswered(runningThread)) {
      hostExecutionState(handle).runningWorker = null;
      clearThreadRunningGeneration(runningThread);
      return true;
    }
    return false;
  }

  return {
    beginBlockedRpc,
    finishBlockedRpc,
    observeForkSharedSyncCompletion,
    clearStaleRunningWorker,
  };
}
