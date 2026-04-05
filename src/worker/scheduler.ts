import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../abi.js";
import {
  maybeUnrefTimer,
  releaseWorker,
} from "./pool.js";
import {
  drainProcessOutput,
  flushStdin,
} from "./stdio.js";
import { createWorkerSchedulerQueueHelpers } from "./scheduler/queue.js";
import { createWorkerSchedulerStateSyncHelpers } from "./scheduler/state-sync.js";
import type {
  ProcessHandle,
} from "./types.js";
import type {
  WorkerScheduler,
  WorkerSchedulerDeps,
} from "./scheduler/types.js";

const WORKER_DEBUG_LOG_ENABLED = false;

export function createWorkerScheduler(deps: WorkerSchedulerDeps): WorkerScheduler {
  const { threadWorkerStates } = deps;
  const schedulerQueueHelpers = createWorkerSchedulerQueueHelpers({
    threadWorkerStates,
    appendWorkerDebugLog,
    handleWorkerMessage: deps.handleWorkerMessage,
  });

  const {
    attachThreadWorker,
    enqueueWorker,
    pumpProcess,
  } = schedulerQueueHelpers;

  function canScheduleWorker(handle: ProcessHandle, worker: Worker): boolean {
    return handle.vforkWorker === null || handle.vforkSnapshot === null || handle.vforkWorker === worker;
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

  function collectLiveThreadVmPtrs(handle: ProcessHandle, vmPtr: number): Set<number> {
    const liveVmPtrs = new Set<number>();
    for (let slot = 0; slot < handle.kernel.abi_max_threads(); slot++) {
      const liveVmPtr = handle.kernel.process_thread_vm_ptr(vmPtr, slot);
      if (liveVmPtr !== 0) {
        liveVmPtrs.add(liveVmPtr);
      }
    }
    return liveVmPtrs;
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
      clearBlockedWorker(handle, worker);
      removeReadyWorker(handle, worker);
      if (handle.vforkWorker === worker) {
        handle.vforkWorker = null;
      }
      appendWorkerDebugLog(handle, `prune:0x${thread.vmPtr.toString(16)}`);
      handle.workers.delete(worker);
      threadWorkerStates.delete(worker);
      releaseWorker(worker);
    }
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

  function blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void {
    clearBlockedWorker(handle, worker);
    handle.blockedWorkers.set(worker, {
      vmPtr,
      status,
      retryTimer: null,
      timeoutExpired: false,
      rpcPending: false,
    });
    if (status === handle.kernel.abi_status_epoll_wait()) {
      const timeoutMs = Number(handle.kernel.thread_epoll_wait_timeout(vmPtr));
      if (timeoutMs >= 0) {
        scheduleBlockedRetry(handle, worker, Math.max(1, timeoutMs), true);
      }
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
      appendWorkerDebugLog(handle, `fault:0x${vmPtr.toString(16)} remaining=${remainingThreads}`);
      handle.workers.delete(worker);
      threadWorkerStates.delete(worker);
      releaseWorker(worker);
      if (remainingThreads === 0) {
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
        scheduleBlockedRetry(handle, worker, Math.max(1, timeoutMs), true);
      }
      return;
    }
    scheduleBlockedRetry(handle, worker, status === handle.kernel.abi_status_wait4() ? 10 : 1);
  }

  function retryBlockedWorker(handle: ProcessHandle, worker: Worker): void {
    const blocked = handle.blockedWorkers.get(worker);
    if (!blocked || blocked.rpcPending || handle.runningWorker !== null || !canScheduleWorker(handle, worker)) {
      return;
    }

    const anchorVmPtr = selectProcessAnchorVmPtr(handle, worker);
    if (anchorVmPtr !== null) {
      try {
        const liveVmPtrs = collectLiveThreadVmPtrs(handle, anchorVmPtr);
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

    if (blocked.status === handle.kernel.abi_status_wait4() && handle.kernelWorkerRequest && handle.pid > 0) {
      blocked.rpcPending = true;
      void deps.resumeWait4ViaKernelWorker(handle, worker, blocked.vmPtr);
      return;
    }

    if (
      handle.kernelWorkerRequest
      && handle.pid > 0
      && (
        blocked.status === handle.kernel.abi_status_pipe_wait()
        || blocked.status === handle.kernel.abi_status_socket_recv()
        || blocked.status === handle.kernel.abi_status_socket_send()
        || blocked.status === handle.kernel.abi_status_flock_wait()
      )
    ) {
      blocked.rpcPending = true;
      void deps.resumeBlockedSyscallLocally(handle, worker, blocked.vmPtr, blocked.status);
      return;
    }

    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    if (blocked.status === handle.kernel.abi_status_epoll_wait() && blocked.timeoutExpired) {
      handle.kernel.thread_resume_epoll_wait_timeout(blocked.vmPtr);
      blocked.timeoutExpired = false;
    } else if (blocked.status === handle.kernel.abi_status_wait4()) {
      handle.kernel.thread_resume_wait4(blocked.vmPtr);
    } else {
      handle.kernel.thread_resume_blocked_syscall(blocked.vmPtr);
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    handleBlockedRetryResult(handle, worker, blocked.vmPtr, handle.kernel.thread_status(blocked.vmPtr));
  }

  const schedulerStateSyncHelpers = createWorkerSchedulerStateSyncHelpers({
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
    for (const worker of handle.blockedWorkers.keys()) {
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
    requestEpollDependentOfdStateSync,
    requestEpollStateSync,
    requestPipeStateSync,
    requestSocketStateSync,
    wakePipeAndEpollWaitersInKernelShare,
  };
}
