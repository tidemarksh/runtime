import type {
  HostBlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";

export function logNetworkDebug(_event: string): void {
  // Runtime diagnostics are kept in the per-process worker debug buffer.
}

export interface RunLoopSchedulerDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  retryHostBlockedWorker(handle: ProcessHandle, worker: Worker): void;
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
  scheduleHostBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    delayMs?: number,
  ): void;
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

  function scheduleHostBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    delayMs = 10,
  ): void {
    const blocked = handle.hostBlockedWorkers.get(worker);
    if (!blocked || blocked.retryTimer !== null) {
      return;
    }
    blocked.retryTimer = setTimeout(() => {
      blocked.retryTimer = null;
      deps.retryHostBlockedWorker(handle, worker);
    }, delayMs);
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
      if (blocked.retryTimer !== null) {
        clearTimeout(blocked.retryTimer);
        blocked.retryTimer = null;
      }
      scheduleHostBlockedRetry(handle, worker, 0);
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
    scheduleHostBlockedRetry,
  };
}
