import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../abi.js";
import {
  maybeUnrefTimer,
  releaseWorker,
} from "./pool.js";
import {
  drainHiddenProcessOutput,
  drainProcessOutput,
  flushStdin,
} from "./stdio.js";
import { collectLiveThreadVmPtrs, processHasLiveThreads } from "./live-threads.js";
import { createWorkerSchedulerQueueHelpers } from "./scheduler/queue.js";
import { createWorkerSchedulerStateSyncHelpers } from "./scheduler/state-sync.js";
import type {
  BlockedWorkerState,
  ProcessHandle,
} from "./types.js";
import type {
  WorkerScheduler,
  WorkerSchedulerDeps,
} from "./scheduler/types.js";

const WORKER_DEBUG_LOG_ENABLED = true;
const GUEST_TICKS_PER_SEC = 100_000_000n;
const GUEST_NSEC_PER_SEC = 1_000_000_000n;

export function createWorkerScheduler(deps: WorkerSchedulerDeps): WorkerScheduler {
  const { threadWorkerStates } = deps;
  const schedulerQueueHelpers = createWorkerSchedulerQueueHelpers({
    threadWorkerStates,
    appendWorkerDebugLog,
    failProcess: deps.failProcess,
    handleWorkerMessage: deps.handleWorkerMessage,
  });

  const {
    attachThreadWorker,
    enqueueWorker,
    pumpProcess,
  } = schedulerQueueHelpers;

  function hasActiveExclusiveVforkWorker(handle: ProcessHandle): boolean {
    const worker = handle.vforkWorker;
    if (handle.vforkSnapshot === null || worker === null) {
      return false;
    }
    if (handle.runningWorker === worker) {
      return true;
    }
    if (handle.readyWorkers.includes(worker)) {
      return true;
    }
    if (handle.blockedWorkers.has(worker)) {
      return true;
    }
    const thread = threadWorkerStates.get(worker);
    return thread?.waitingInWorkerStatus !== null;
  }

  function canScheduleWorker(handle: ProcessHandle, worker: Worker): boolean {
    return !hasActiveExclusiveVforkWorker(handle) || handle.vforkWorker === worker;
  }

  function clearBlockedWorker(handle: ProcessHandle, worker: Worker): void {
    const blocked = handle.blockedWorkers.get(worker);
    if (!blocked) {
      return;
    }
    if (blocked.retryTimer !== null) {
      clearTimeout(blocked.retryTimer);
    }
    handle.blockedWorkers.delete(worker);
  }

  function monotonicTicksCurrent(handle: ProcessHandle): bigint {
    const ticks = handle.kernel.monotonic_ticks_current?.();
    return ticks === undefined ? 0n : BigInt(ticks);
  }

  function sleepDurationToTicks(sec: bigint, nsec: bigint): bigint {
    const base = sec * GUEST_TICKS_PER_SEC;
    const sub = nsec * GUEST_TICKS_PER_SEC;
    const rounded = sub === 0n ? 0n : (sub + (GUEST_NSEC_PER_SEC - 1n)) / GUEST_NSEC_PER_SEC;
    return base + rounded;
  }

  function sleepDeadlineTicks(handle: ProcessHandle, vmPtr: number): bigint {
    const exportedDeadline = handle.kernel.thread_sleep_deadline_ticks?.(vmPtr);
    if (exportedDeadline !== undefined) {
      return BigInt(exportedDeadline);
    }
    const sec = handle.kernel.thread_sleep_seconds(vmPtr);
    const nsec = handle.kernel.thread_sleep_nanoseconds(vmPtr);
    return monotonicTicksCurrent(handle) + sleepDurationToTicks(sec, nsec);
  }

  function wakeBlockedSleepWorker(handle: ProcessHandle, worker: Worker, blocked: { vmPtr: number }): void {
    handle.kernel.thread_resume_sleep?.(blocked.vmPtr);
    clearBlockedWorker(handle, worker);
    enqueueWorker(handle, worker);
  }

  function wakeDueSleepWorkers(handle: ProcessHandle): boolean {
    const now = monotonicTicksCurrent(handle);
    let wokeAny = false;
    for (const [worker, blocked] of [...handle.blockedWorkers.entries()]) {
      if (
        blocked.status !== handle.kernel.abi_status_sleep()
        || blocked.sleepDeadlineTicks === null
        || blocked.sleepDeadlineTicks > now
      ) {
        continue;
      }
      wakeBlockedSleepWorker(handle, worker, blocked);
      wokeAny = true;
    }
    return wokeAny;
  }

  function maybeAdvanceGuestTimeToNextSleep(handle: ProcessHandle): boolean {
    if (handle.runningWorker !== null || handle.readyWorkers.length > 0) {
      return false;
    }
    let nextDeadline: bigint | null = null;
    for (const blocked of handle.blockedWorkers.values()) {
      if (blocked.status !== handle.kernel.abi_status_sleep() || blocked.sleepDeadlineTicks === null) {
        continue;
      }
      if (nextDeadline === null || blocked.sleepDeadlineTicks < nextDeadline) {
        nextDeadline = blocked.sleepDeadlineTicks;
      }
    }
    if (nextDeadline === null) {
      return false;
    }
    if (nextDeadline > monotonicTicksCurrent(handle)) {
      handle.kernel.sync_monotonic_ticks_floor?.(nextDeadline);
    }
    return wakeDueSleepWorkers(handle);
  }

  function clearStaleRunningWorker(handle: ProcessHandle, blockedWorker: Worker): boolean {
    const runningWorker = handle.runningWorker;
    if (runningWorker === null || runningWorker === blockedWorker) {
      return false;
    }
    if (!handle.workers.has(runningWorker)) {
      handle.runningWorker = null;
      return true;
    }
    const runningThread = threadWorkerStates.get(runningWorker);
    if (!runningThread) {
      handle.runningWorker = null;
      return true;
    }
    if (runningThread.waitingInWorkerStatus !== null) {
      handle.runningWorker = null;
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `running-worker-cleared:${handle.pid}:wait=${runningThread.waitingInWorkerStatus}`,
      );
      return true;
    }
    const runningStatus = handle.kernel.thread_status(runningThread.vmPtr);
    if (runningStatus !== handle.kernel.abi_status_ok()) {
      handle.runningWorker = null;
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `running-worker-cleared:${handle.pid}:status=${runningStatus}`,
      );
      return true;
    }
    return false;
  }

  function selectProcessAnchorVmPtr(
    handle: ProcessHandle,
    excludedWorker: Worker | null = null,
  ): number | null {
    const candidates: Worker[] = [];
    if (handle.runningWorker !== null) {
      candidates.push(handle.runningWorker);
    }
    candidates.push(...handle.readyWorkers, ...handle.workers);
    const seen = new Set<Worker>();
    for (const candidate of candidates) {
      if (candidate === excludedWorker || seen.has(candidate)) {
        continue;
      }
      seen.add(candidate);
      const vmPtr = threadWorkerStates.get(candidate)?.vmPtr;
      if (vmPtr !== undefined && vmPtr !== 0) {
        return vmPtr;
      }
    }
    return null;
  }

  function removeReadyWorker(handle: ProcessHandle, worker: Worker): void {
    for (let index = handle.readyWorkers.length - 1; index >= 0; index--) {
      if (handle.readyWorkers[index] === worker) {
        handle.readyWorkers.splice(index, 1);
      }
    }
  }

  function appendWorkerDebugLog(handle: ProcessHandle, event: string): void {
    if (!WORKER_DEBUG_LOG_ENABLED) {
      return;
    }
    handle.workerDebugLog.push(event);
    if (handle.workerDebugLog.length > 256) {
      handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 256);
    }
  }

  function shouldPreserveTransientWorker(handle: ProcessHandle, vmPtr: number): boolean {
    const status = handle.kernel.thread_status(vmPtr);
    return status === handle.kernel.abi_status_vfork_wait()
      || status === handle.kernel.abi_status_execve_spawn()
      || status === handle.kernel.abi_status_clone_spawn();
  }

  function pruneStaleProcessWorkers(
    handle: ProcessHandle,
    liveVmPtrs: Set<number>,
    currentWorker: Worker | null,
  ): void {
    if (liveVmPtrs.size === 0) {
      return;
    }

    for (const worker of [...handle.workers]) {
      if (worker === currentWorker) {
        continue;
      }
      const thread = threadWorkerStates.get(worker);
      if (!thread || liveVmPtrs.has(thread.vmPtr)) {
        continue;
      }
      if (shouldPreserveTransientWorker(handle, thread.vmPtr)) {
        appendWorkerDebugLog(
          handle,
          `prune-skip-transient:0x${thread.vmPtr.toString(16)}:${handle.kernel.thread_status(thread.vmPtr)}`,
        );
        continue;
      }
      clearBlockedWorker(handle, worker);
      removeReadyWorker(handle, worker);
      if (handle.runningWorker === worker) {
        handle.runningWorker = null;
      }
      if (handle.vforkWorker === worker) {
        handle.vforkWorker = null;
      }
      appendWorkerDebugLog(handle, `prune:0x${thread.vmPtr.toString(16)}`);
      handle.workers.delete(worker);
      threadWorkerStates.delete(worker);
      releaseWorker(worker);
    }
  }

  function pruneDetachedProcessWorkers(
    handle: ProcessHandle,
    anchorVmPtr: number,
    currentWorker: Worker | null,
  ): void {
    if (anchorVmPtr === 0 || handle.workers.size <= 1) {
      return;
    }
    if (handle.pendingChildSpawnRecords > 0) {
      appendWorkerDebugLog(handle, `prune-skip-spawn:${handle.pendingChildSpawnRecords}`);
      return;
    }
    const liveVmPtrs = collectLiveThreadVmPtrs(handle.kernel, anchorVmPtr);
    pruneStaleProcessWorkers(handle, liveVmPtrs, currentWorker);
  }

  function scheduleBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    delayMs = 10,
    timeoutExpired = false,
  ): void {
    const blocked = handle.blockedWorkers.get(worker);
    if (!blocked || blocked.retryTimer !== null) {
      return;
    }
    blocked.retryTimer = setTimeout(() => {
      blocked.retryTimer = null;
      blocked.timeoutExpired = timeoutExpired;
      retryBlockedWorker(handle, worker);
    }, delayMs);
    maybeUnrefTimer(blocked.retryTimer);
  }

  function socketWaitTimeoutMs(handle: ProcessHandle, status: number, vmPtr: number): number {
    if (
      status !== handle.kernel.abi_status_connect()
      && status !== handle.kernel.abi_status_socket_recv()
      && status !== handle.kernel.abi_status_socket_send()
    ) {
      return -1;
    }
    const timeout = handle.kernel.thread_socket_wait_timeout?.(vmPtr);
    return timeout === undefined ? -1 : Number(timeout);
  }

  function scheduleSocketBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    blocked: { timeoutDeadlineMs: number | null },
    timeoutMs: number,
  ): void {
    if (timeoutMs < 0) {
      blocked.timeoutDeadlineMs = null;
      scheduleBlockedRetry(handle, worker, 1);
      return;
    }
    const now = Date.now();
    if (blocked.timeoutDeadlineMs === null) {
      blocked.timeoutDeadlineMs = now + timeoutMs;
    }
    const remaining = blocked.timeoutDeadlineMs - now;
    if (remaining <= 0) {
      scheduleBlockedRetry(handle, worker, 0, true);
      return;
    }
    scheduleBlockedRetry(handle, worker, Math.min(10, remaining));
  }

  function blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void {
    clearBlockedWorker(handle, worker);
    const blocked: BlockedWorkerState = {
      vmPtr,
      status,
      retryTimer: null,
      timeoutExpired: false,
      timeoutDeadlineMs: null,
      sleepDeadlineTicks: null,
      rpcPending: false,
    };
    handle.blockedWorkers.set(worker, blocked);
    if (status === handle.kernel.abi_status_sleep()) {
      blocked.sleepDeadlineTicks = sleepDeadlineTicks(handle, vmPtr);
      return;
    }
    if (status === handle.kernel.abi_status_epoll_wait()) {
      const timeoutMs = Number(handle.kernel.thread_epoll_wait_timeout(vmPtr));
      if (timeoutMs >= 0) {
        scheduleBlockedRetry(handle, worker, Math.max(0, timeoutMs), true);
      } else if (handle.kernel.thread_process_thread_count(vmPtr) > 1) {
        scheduleBlockedRetry(handle, worker, 10);
      }
      return;
    }
    const socketTimeoutMs = socketWaitTimeoutMs(handle, status, vmPtr);
    if (socketTimeoutMs >= 0) {
      scheduleSocketBlockedRetry(handle, worker, blocked, socketTimeoutMs);
      return;
    }
    scheduleBlockedRetry(handle, worker, status === handle.kernel.abi_status_wait4() ? 10 : 1);
  }

  function handleBlockedRetryResult(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    status: number,
  ): void {
    const blocked = handle.blockedWorkers.get(worker);
    if (!blocked || blocked.vmPtr !== vmPtr) {
      return;
    }

    if (status === handle.kernel.abi_status_ok()) {
      clearBlockedWorker(handle, worker);
      enqueueWorker(handle, worker);
      return;
    }
    if (status === handle.kernel.abi_status_fault()) {
      clearBlockedWorker(handle, worker);
      const exitCode = handle.kernel.thread_exit_code(vmPtr);
      const termSignal = handle.kernel.thread_term_signal(vmPtr);
      if (deps.resumeVforkParentAfterChildExit(handle, worker, vmPtr, exitCode, termSignal)) {
        return;
      }
      const remainingThreads = handle.kernel.thread_process_thread_count(vmPtr);
      const hasLiveThreads = remainingThreads !== 0 && processHasLiveThreads(handle.kernel, vmPtr);
      appendWorkerDebugLog(handle, `fault:0x${vmPtr.toString(16)} remaining=${remainingThreads}`);
      if (handle.runningWorker === worker) {
        handle.runningWorker = null;
      }
      handle.workers.delete(worker);
      threadWorkerStates.delete(worker);
      releaseWorker(worker);
      if (remainingThreads === 0 || !hasLiveThreads) {
        void deps.completeProcessExit(handle, exitCode, termSignal).catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      } else {
        pumpProcess(handle);
      }
      return;
    }

    blocked.status = status;
    if (status === handle.kernel.abi_status_epoll_wait()) {
      const timeoutMs = Number(handle.kernel.thread_epoll_wait_timeout(vmPtr));
      if (timeoutMs >= 0) {
        scheduleBlockedRetry(handle, worker, Math.max(0, timeoutMs), true);
      } else if (handle.kernel.thread_process_thread_count(vmPtr) > 1) {
        scheduleBlockedRetry(handle, worker, 10);
      }
      return;
    }
    const socketTimeoutMs = socketWaitTimeoutMs(handle, status, vmPtr);
    if (socketTimeoutMs >= 0 || blocked.timeoutDeadlineMs !== null) {
      scheduleSocketBlockedRetry(handle, worker, blocked, socketTimeoutMs);
      return;
    }
    scheduleBlockedRetry(handle, worker, status === handle.kernel.abi_status_wait4() ? 10 : 1);
  }

  function retryBlockedWorker(handle: ProcessHandle, worker: Worker): void {
    const blocked = handle.blockedWorkers.get(worker);
    if (!blocked) {
      return;
    }
    if (blocked.status === handle.kernel.abi_status_sleep()) {
      const currentStatus = handle.kernel.thread_status(blocked.vmPtr);
      if (currentStatus !== blocked.status) {
        handleBlockedRetryResult(handle, worker, blocked.vmPtr, currentStatus);
        return;
      }
      if (blocked.sleepDeadlineTicks !== null && blocked.sleepDeadlineTicks > monotonicTicksCurrent(handle)) {
        return;
      }
      wakeBlockedSleepWorker(handle, worker, blocked);
      return;
    }
    const currentStatus = handle.kernel.thread_status(blocked.vmPtr);
    if (currentStatus !== blocked.status) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `wait4-retry-reconcile:${handle.pid}:0x${blocked.vmPtr.toString(16)}:blocked=${blocked.status}:local=${currentStatus}`,
      );
      handleBlockedRetryResult(handle, worker, blocked.vmPtr, currentStatus);
      return;
    }
    if (blocked.rpcPending) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-retry-skip:${handle.pid}:rpc-pending`);
      return;
    }
    clearStaleRunningWorker(handle, worker);
    if (handle.runningWorker !== null) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-retry-skip:${handle.pid}:running`);
      scheduleBlockedRetry(
        handle,
        worker,
        blocked.status === handle.kernel.abi_status_wait4() ? 10 : 1,
        blocked.timeoutExpired,
      );
      return;
    }
    if (!canScheduleWorker(handle, worker)) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-retry-skip:${handle.pid}:cannot-schedule`);
      scheduleBlockedRetry(
        handle,
        worker,
        blocked.status === handle.kernel.abi_status_wait4() ? 10 : 1,
        blocked.timeoutExpired,
      );
      return;
    }
    if (
      blocked.status === handle.kernel.abi_status_wait4()
      && (
        handle.pendingChildExitRecords > 0
        || handle.pendingChildSpawnRecords > 0
        || deps.hasActiveChildProcess(handle.pid)
      )
    ) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `wait4-retry-skip:${handle.pid}:gate:exit=${handle.pendingChildExitRecords}:spawn=${handle.pendingChildSpawnRecords}:active=${deps.hasActiveChildProcess(handle.pid) ? 1 : 0}`,
      );
      scheduleBlockedRetry(handle, worker, 50);
      return;
    }

    const anchorVmPtr = selectProcessAnchorVmPtr(handle, worker);
    if (anchorVmPtr !== null) {
      try {
        const liveVmPtrs = collectLiveThreadVmPtrs(handle.kernel, anchorVmPtr);
        pruneStaleProcessWorkers(handle, liveVmPtrs, null);
        if (!liveVmPtrs.has(blocked.vmPtr)) {
          clearBlockedWorker(handle, worker);
          return;
        }
      } catch {
        // If the candidate anchor is stale, fall back to the normal resume path.
      }
    }

    flushStdin(handle);
    drainProcessOutput(handle);

    if (
      blocked.status === handle.kernel.abi_status_epoll_wait()
      && handle.kernelWorkerRequest
      && handle.pid > 0
    ) {
      blocked.rpcPending = true;
      void requestEpollDependentOfdStateSync(handle)
        .then(() => requestEpollStateSync(handle, false))
        .then(() => {
          const latestBlocked = handle.blockedWorkers.get(worker);
          if (!latestBlocked || latestBlocked.vmPtr !== blocked.vmPtr) {
            return;
          }
          latestBlocked.rpcPending = false;
          if (handle.runningWorker !== null || !canScheduleWorker(handle, worker)) {
            scheduleBlockedRetry(handle, worker, 1, latestBlocked.timeoutExpired);
            return;
          }
          attachKernelRuntimeState(handle.kernel, handle.kernelState);
          if (latestBlocked.timeoutExpired) {
            handle.kernel.thread_resume_epoll_wait_timeout(latestBlocked.vmPtr);
            latestBlocked.timeoutExpired = false;
          } else {
            handle.kernel.thread_resume_blocked_syscall(latestBlocked.vmPtr);
          }
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          handleBlockedRetryResult(
            handle,
            worker,
            latestBlocked.vmPtr,
            handle.kernel.thread_status(latestBlocked.vmPtr),
          );
        })
        .catch((error) => {
          const latestBlocked = handle.blockedWorkers.get(worker);
          if (latestBlocked && latestBlocked.vmPtr === blocked.vmPtr) {
            latestBlocked.rpcPending = false;
          }
          if (handle.workers.has(worker) && threadWorkerStates.has(worker)) {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          }
        });
      return;
    }

    if (
      blocked.status === handle.kernel.abi_status_wait4()
      && handle.kernelWorkerRequest
      && handle.pid > 0
    ) {
      blocked.rpcPending = true;
      void deps.resumeWait4ViaKernelWorker(handle, worker, blocked.vmPtr);
      return;
    }

    if (
      handle.kernelWorkerRequest
      && handle.pid > 0
      && (
        blocked.status === handle.kernel.abi_status_pipe_wait()
        || blocked.status === handle.kernel.abi_status_connect()
        || blocked.status === handle.kernel.abi_status_socket_recv()
        || blocked.status === handle.kernel.abi_status_socket_send()
        || blocked.status === handle.kernel.abi_status_flock_wait()
      )
    ) {
      blocked.rpcPending = true;
      void deps.resumeBlockedSyscallLocally(
        handle,
        worker,
        blocked.vmPtr,
        blocked.status,
        blocked.timeoutExpired,
      );
      return;
    }

    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    if (blocked.status === handle.kernel.abi_status_epoll_wait() && blocked.timeoutExpired) {
      handle.kernel.thread_resume_epoll_wait_timeout(blocked.vmPtr);
      blocked.timeoutExpired = false;
    } else if (
      blocked.timeoutExpired
      && (
        blocked.status === handle.kernel.abi_status_connect()
        || blocked.status === handle.kernel.abi_status_socket_recv()
        || blocked.status === handle.kernel.abi_status_socket_send()
      )
      && handle.kernel.thread_resume_socket_wait_timeout
    ) {
      handle.kernel.thread_resume_socket_wait_timeout(blocked.vmPtr);
      blocked.timeoutExpired = false;
    } else if (blocked.status === handle.kernel.abi_status_wait4()) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-retry-local:${handle.pid}:0x${blocked.vmPtr.toString(16)}`);
      handle.kernel.thread_resume_wait4(blocked.vmPtr);
    } else {
      handle.kernel.thread_resume_blocked_syscall(blocked.vmPtr);
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    handleBlockedRetryResult(handle, worker, blocked.vmPtr, handle.kernel.thread_status(blocked.vmPtr));
  }

  const schedulerStateSyncHelpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    drainHiddenProcessOutput,
    drainProcessOutput,
    threadWorkerStates,
    retryBlockedWorker,
  });
  const {
    requestEpollDependentOfdStateSync,
    requestEpollStateSync,
    requestPipeStateSync,
    requestSocketStateSync,
    wakePipeAndEpollWaitersInKernelShare,
  } = schedulerStateSyncHelpers;

  function kickBlockedWorkers(handle: ProcessHandle): void {
    wakeDueSleepWorkers(handle);
    maybeAdvanceGuestTimeToNextSleep(handle);
    for (const worker of [...handle.blockedWorkers.keys()]) {
      retryBlockedWorker(handle, worker);
    }
  }

  return {
    appendWorkerDebugLog,
    attachThreadWorker,
    blockWorker,
    clearBlockedWorker,
    enqueueWorker,
    handleBlockedRetryResult,
    kickBlockedWorkers,
    pumpProcess,
    pruneDetachedProcessWorkers,
    requestEpollDependentOfdStateSync,
    requestEpollStateSync,
    requestPipeStateSync,
    requestSocketStateSync,
    wakePipeAndEpollWaitersInKernelShare,
  };
}
