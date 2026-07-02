import type {
  BlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import { threadHasCanonicalFutexBlock } from "./futex.js";

export function logNetworkDebug(_event: string): void {
  // Runtime diagnostics are kept in the per-process worker debug buffer.
}

export interface RunLoopSchedulerDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void;
  retryBlockedWorker(handle: ProcessHandle, worker: Worker): void;
}

export interface RunLoopScheduler {
  beginBlockedRpc(blocked: BlockedWorkerState): number;
  finishBlockedRpc(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    rpcGeneration: number,
  ): BlockedWorkerState | null;
  observeForkSharedSyncCompletion(
    handle: ProcessHandle,
    worker: Worker,
    promise: Promise<void>,
  ): void;
  clearStaleRunningWorker(handle: ProcessHandle, blockedWorker: Worker): boolean;
  scheduleBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    delayMs?: number,
  ): void;
}

export function createRunLoopScheduler(deps: RunLoopSchedulerDeps): RunLoopScheduler {
  const observedForkSharedSyncPromises = new WeakSet<Promise<void>>();

  function statusBlocksInOwnerScheduler(handle: ProcessHandle, status: number): boolean {
    return status === handle.kernel.abi_status_epoll_wait()
      || status === handle.kernel.abi_status_fs()
      || status === handle.kernel.abi_status_pipe_wait()
      || status === handle.kernel.abi_status_wait4()
      || status === handle.kernel.abi_status_flock_wait();
  }

  function beginBlockedRpc(blocked: BlockedWorkerState): number {
    blocked.rpcPending = true;
    blocked.rpcGeneration += 1;
    return blocked.rpcGeneration;
  }

  function finishBlockedRpc(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    rpcGeneration: number,
  ): BlockedWorkerState | null {
    const latestBlocked = handle.blockedWorkers.get(worker);
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

  function scheduleBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    delayMs = 10,
  ): void {
    const blocked = handle.blockedWorkers.get(worker);
    if (!blocked || blocked.retryTimer !== null) {
      return;
    }
    blocked.retryTimer = setTimeout(() => {
      blocked.retryTimer = null;
      deps.retryBlockedWorker(handle, worker);
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
      const blocked = handle.blockedWorkers.get(worker);
      if (!blocked) {
        return;
      }
      if (blocked.retryTimer !== null) {
        clearTimeout(blocked.retryTimer);
        blocked.retryTimer = null;
      }
      scheduleBlockedRetry(handle, worker, 0);
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
    if (runningThread.waitingInWorkerStatus !== null) {
      handle.runningWorker = null;
      runningThread.runningGeneration = null;
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `running-worker-cleared:${handle.pid}:wait=${runningThread.waitingInWorkerStatus}`,
      );
      return true;
    }
    const runningStatus = handle.kernel.thread_status(runningThread.vmPtr);
    if (
      threadHasCanonicalFutexBlock(handle, runningThread.vmPtr)
      && runningThread.pendingGuestMemoryWrites.length === 0
      && runningStatus === handle.kernel.abi_status_ok()
      && handle.kernel.thread_process_ptr(runningThread.vmPtr) !== 0
    ) {
      handle.runningWorker = null;
      runningThread.runningGeneration = null;
      runningThread.waitingInWorkerStatus = handle.kernel.abi_status_futex_wait();
      runningThread.waitingInWorkerReconcileDeferred = false;
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `running-worker-cleared:${handle.pid}:recorded-futex-wait:0x${runningThread.vmPtr.toString(16)}`,
      );
      return true;
    }
    if (
      (
        runningThread.lastStatus === handle.kernel.abi_status_futex_wait()
        || threadHasCanonicalFutexBlock(handle, runningThread.vmPtr)
      )
      && runningThread.pendingGuestMemoryWrites.length === 0
      && runningStatus === handle.kernel.abi_status_ok()
      && handle.kernel.thread_process_ptr(runningThread.vmPtr) !== 0
      && runningThread.stepCount <= runningThread.statusCount
    ) {
      handle.runningWorker = null;
      runningThread.runningGeneration = null;
      runningThread.waitingInWorkerStatus = handle.kernel.abi_status_futex_wait();
      runningThread.waitingInWorkerReconcileDeferred = false;
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `running-worker-cleared:${handle.pid}:answered-futex-wait:0x${runningThread.vmPtr.toString(16)}:record=${threadHasCanonicalFutexBlock(handle, runningThread.vmPtr) ? 1 : 0}`,
      );
      return true;
    }
    if (
      runningThread.lastStatus === handle.kernel.abi_status_ok()
      && runningThread.pendingGuestMemoryWrites.length === 0
      && runningStatus === handle.kernel.abi_status_ok()
      && handle.kernel.thread_process_ptr(runningThread.vmPtr) !== 0
      && runningThread.stepCount <= runningThread.statusCount
    ) {
      handle.runningWorker = null;
      runningThread.runningGeneration = null;
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `running-worker-cleared:${handle.pid}:answered-ok:0x${runningThread.vmPtr.toString(16)}`,
      );
      return true;
    }
    const ownerBlockingUnansweredSteps = runningThread.stepCount - runningThread.statusCount;
    if (
      runningThread.lastStatus !== null
      && statusBlocksInOwnerScheduler(handle, runningThread.lastStatus)
      && runningThread.pendingGuestMemoryWrites.length === 0
      && runningStatus === handle.kernel.abi_status_ok()
      && handle.kernel.thread_process_ptr(runningThread.vmPtr) !== 0
      && ownerBlockingUnansweredSteps <= 0
    ) {
      handle.runningWorker = null;
      runningThread.runningGeneration = null;
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `running-worker-cleared:${handle.pid}:answered-blocking-status=${runningThread.lastStatus}:0x${runningThread.vmPtr.toString(16)}`,
      );
      return true;
    }
    if (handle.kernel.thread_process_ptr(runningThread.vmPtr) === 0) {
      handle.runningWorker = null;
      runningThread.runningGeneration = null;
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `running-worker-cleared:${handle.pid}:stale-process:0x${runningThread.vmPtr.toString(16)}`,
      );
      return true;
    }
    if (runningStatus === handle.kernel.abi_status_sleep()) {
      handle.runningWorker = null;
      runningThread.runningGeneration = null;
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `running-worker-cleared:${handle.pid}:sleep-supervisor-owned:0x${runningThread.vmPtr.toString(16)}`,
      );
      return true;
    }
    if (statusBlocksInOwnerScheduler(handle, runningStatus)) {
      handle.runningWorker = null;
      runningThread.runningGeneration = null;
      deps.blockWorker(handle, runningWorker, runningStatus, runningThread.vmPtr);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `running-worker-blocked:${handle.pid}:status=${runningStatus}`,
      );
      return true;
    }
    if (runningStatus !== handle.kernel.abi_status_ok()) {
      handle.runningWorker = null;
      runningThread.runningGeneration = null;
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `running-worker-cleared:${handle.pid}:status=${runningStatus}`,
      );
      return true;
    }
    return false;
  }

  return {
    beginBlockedRpc,
    finishBlockedRpc,
    observeForkSharedSyncCompletion,
    clearStaleRunningWorker,
    scheduleBlockedRetry,
  };
}
