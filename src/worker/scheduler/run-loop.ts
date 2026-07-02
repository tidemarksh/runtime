import type {
  HostBlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import {
  SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_HOST_PROGRESS,
  type SupervisorBlockedWaitRetryFlags,
} from "../../supervisor-blocked-resume.js";

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
    blocked.rpcPending = true;
    blocked.rpcGeneration += 1;
    return blocked.rpcGeneration;
  }

  function finishBlockedRpc(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    rpcGeneration: number,
  ): HostBlockedWorkerState | null {
    const latestBlocked = handle.hostBlockedWorkers.get(worker);
    if (
      !latestBlocked
      || latestBlocked.vmPtr !== vmPtr
      || latestBlocked.rpcGeneration !== rpcGeneration
    ) {
      return null;
    }
    latestBlocked.rpcPending = false;
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
      if (handle.kernelShare.forkSharedSyncPromise === promise) {
        handle.kernelShare.forkSharedSyncPromise = null;
      }
      if (handle.forkSharedSyncPromise === promise) {
        handle.forkSharedSyncPromise = null;
      }
      const blocked = handle.hostBlockedWorkers.get(worker);
      if (!blocked) {
        return;
      }
      deps.clearHostBlockedRetryTimer(handle, worker);
      deps.scheduleSupervisorBlockedRetry(
        handle,
        worker,
        SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_HOST_PROGRESS,
      );
    };
    promise.then(onSettled, onSettled);
  }

  function clearStaleRunningWorker(handle: ProcessHandle, blockedWorker: Worker): boolean {
    const runningWorker = handle.runningWorker;
    if (runningWorker === null || runningWorker === blockedWorker) {
      return false;
    }
    if (!handle.workers.has(runningWorker)) {
      handle.runningWorker = null;
      const detachedThread = deps.threadWorkerStates.get(runningWorker);
      if (detachedThread) {
        detachedThread.runningGeneration = null;
      }
      return true;
    }
    const runningThread = deps.threadWorkerStates.get(runningWorker);
    if (!runningThread) {
      handle.runningWorker = null;
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
