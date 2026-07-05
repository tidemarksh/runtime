import { hostExecutionState } from "../host-execution-state.js";
import {
  supervisorBlockedEpollWaitRetryRequired,
  supervisorBlockedPipeSyncBeforeRetryClear,
  supervisorBlockedStatusIsEpollWait,
  supervisorBlockedStatusIsPipeWait,
  supervisorBlockedStatusIsWait4,
  supervisorClearBlockedWaitNetworkPump,
  type SupervisorBlockedWaitRetryFlags,
} from "../../supervisor-blocked-resume.js";
import {
  drainHiddenProcessOutput,
  drainProcessOutput,
} from "../stdio.js";
import { appendWorkerDebugLog } from "./debug-log.js";
import { createPipePollScheduler } from "../host-boundary/blocked-poll.js";
import { createPruningScheduler } from "../host-boundary/worker-pruning.js";
import { createWorkerSchedulerQueueHelpers } from "../host-boundary/thread-worker-queue.js";
import { createRunLoopScheduler, logNetworkDebug } from "./run-loop.js";
import { createSocketWaitScheduler } from "../diagnostics/socket-wait.js";
import {
  createBlockedWaitDiagnostics,
} from "../host-boundary/blocked-wait-diagnostics.js";
import {
  createBlockedWaitTimers,
  type BlockedWaitTimers,
} from "../host-boundary/blocked-wait-timers.js";
import {
  createBlockedRetryCoordinator,
} from "../host-boundary/blocked-retry.js";
import { createWorkerSchedulerStateSyncHelpers } from "../host-boundary/state-sync.js";
import type {
  HostBlockedWorkerState,
  ProcessHandle,
} from "../types.js";
import { clearThreadRunningGeneration } from "../thread-host-state.js";
import type {
  WorkerScheduler,
  WorkerSchedulerDeps,
} from "./types.js";
import {
  copyHostBlockedEffectState,
  isBlockedRpcPending,
} from "../host-blocked-state.js";

export function createWorkerScheduler(deps: WorkerSchedulerDeps): WorkerScheduler {
  const { threadWorkerStates } = deps;
  const threadWorkerBlockedResumeRequests = new WeakSet<Worker>();
  let blockedWaitTimers: BlockedWaitTimers;
  let retryHostBlockedWorkerImpl: ((handle: ProcessHandle, worker: Worker) => void) | null = null;

  function retryHostBlockedWorker(handle: ProcessHandle, worker: Worker): void {
    if (retryHostBlockedWorkerImpl === null) {
      deps.failProcess(handle, new Error("blocked retry coordinator is not initialized"));
      return;
    }
    retryHostBlockedWorkerImpl(handle, worker);
  }

  const schedulerQueueHelpers = createWorkerSchedulerQueueHelpers({
    threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    appendWorkerDebugLog,
    blockWorker,
    clearHostBlockedRetryTimer,
    takeThreadWorkerBlockedResume,
    isSupervisorFutexWaiter,
    completeProcessExit: deps.completeProcessExit,
    failProcess: deps.failProcess,
    handleWorkerMessage: deps.handleWorkerMessage,
  });

  const {
    attachThreadWorker,
    enqueueWorker,
    pumpProcess,
  } = schedulerQueueHelpers;

  blockedWaitTimers = createBlockedWaitTimers({
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    appendWorkerDebugLog,
    failProcess: deps.failProcess,
    retryHostBlockedWorker,
    pumpProcess,
  });

  function clearBlockedWorker(
    handle: ProcessHandle,
    worker: Worker,
    options: {
      clearNetworkPumpBackoff?: boolean;
      clearPipeSyncBeforeRetry?: boolean;
    } = {},
  ): void {
    const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
    if (!blocked) {
      return;
    }
    if (
      (options.clearPipeSyncBeforeRetry ?? true)
      && supervisorBlockedStatusIsPipeWait(handle.kernel, blocked.status)
    ) {
      try {
        supervisorBlockedPipeSyncBeforeRetryClear(handle.kernel, blocked.vmPtr);
      } catch (error) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    }
    if (options.clearNetworkPumpBackoff ?? true) {
      try {
        supervisorClearBlockedWaitNetworkPump(handle.kernel, blocked.vmPtr);
      } catch (error) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    }
    clearHostBlockedRetryTimer(handle, worker);
    hostExecutionState(handle).hostBlockedWorkers.delete(worker);
  }

  function clearHostBlockedRetryTimer(handle: ProcessHandle, worker: Worker): void {
    blockedWaitTimers.clearHostBlockedRetryTimer(handle, worker);
  }

  function scheduleSupervisorBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    flags: SupervisorBlockedWaitRetryFlags = [],
  ): void {
    blockedWaitTimers.scheduleSupervisorBlockedRetry(handle, worker, flags);
  }

  function supervisorBlockedWaitActiveChildPid(handle: ProcessHandle, vmPtr: number): number {
    const readActiveChild = handle.kernel.supervisor_blocked_wait_active_child_pid;
    if (readActiveChild === undefined) {
      throw new Error("kernel supervisor ABI missing supervisor_blocked_wait_active_child_pid");
    }
    const childPid = readActiveChild.call(handle.kernel, vmPtr);
    if (childPid < 0) {
      throw new Error(`kernel supervisor active child selection failed: ${childPid}`);
    }
    return childPid;
  }

  function activeChildDebugFlag(handle: ProcessHandle, vmPtr: number): number {
    try {
      return supervisorBlockedWaitActiveChildPid(handle, vmPtr) > 0 ? 1 : 0;
    } catch {
      return -1;
    }
  }

  function requestThreadWorkerBlockedResume(_handle: ProcessHandle, worker: Worker): void {
    threadWorkerBlockedResumeRequests.add(worker);
  }

  function takeThreadWorkerBlockedResume(_handle: ProcessHandle, worker: Worker): boolean {
    if (!threadWorkerBlockedResumeRequests.has(worker)) {
      return false;
    }
    threadWorkerBlockedResumeRequests.delete(worker);
    return true;
  }

  function isSupervisorFutexWaiter(handle: ProcessHandle, worker: Worker): boolean {
    const thread = threadWorkerStates.get(worker);
    return thread !== undefined
      && handle.workers.has(worker)
      && handle.kernel.thread_status(thread.vmPtr) === handle.kernel.abi_status_futex_wait();
  }

  function parkSupervisorFutexWait(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    source: string,
  ): void {
    clearBlockedWorker(handle, worker);
    const readyIndex = hostExecutionState(handle).vforkReadyWorkers.indexOf(worker);
    if (readyIndex >= 0) {
      hostExecutionState(handle).vforkReadyWorkers.splice(readyIndex, 1);
    }
    if (hostExecutionState(handle).runningWorker === worker) {
      hostExecutionState(handle).runningWorker = null;
    }
    const thread = threadWorkerStates.get(worker);
    if (thread !== undefined && thread.vmPtr === vmPtr) {
      clearThreadRunningGeneration(thread);
    }
    appendWorkerDebugLog(
      handle,
      `${source}:0x${vmPtr.toString(16)}`,
    );
  }

  const {
    beginBlockedRpc,
    clearStaleRunningWorker,
    finishBlockedRpc,
    observeForkSharedSyncCompletion,
  } = createRunLoopScheduler({
    threadWorkerStates,
    retryHostBlockedWorker,
    clearHostBlockedRetryTimer,
    scheduleSupervisorBlockedRetry,
  });
  const {
    blockedOrCurrentSyscallNr,
    formatBlockedPollFdSet,
    formatFirstPipeWaitDebug,
    pollWaitTimeoutMs,
  } = createPipePollScheduler({
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    scheduleSupervisorBlockedRetry,
  });
  const {
    formatFirstSocketWaitDebug,
  } = createSocketWaitScheduler({
    appendWorkerDebugLog,
    logNetworkDebug,
  });
  const {
    appendBlockedStateLifecycle,
  } = createBlockedWaitDiagnostics({
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    activeChildDebugFlag,
    formatBlockedPollFdSet,
    formatFirstPipeWaitDebug,
  });

  const {
    pruneDetachedProcessWorkers,
    pruneStaleProcessWorkers,
    selectProcessAnchorVmPtr,
  } = createPruningScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    appendWorkerDebugLog,
    clearBlockedWorker,
  });

  function blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void {
    if (status === handle.kernel.abi_status_futex_wait()) {
      parkSupervisorFutexWait(handle, worker, vmPtr, "block-worker-futex-supervisor-owned");
      return;
    }
    const previousBlocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
    const sameBlockedThread = previousBlocked !== undefined
      && previousBlocked.vmPtr === vmPtr
      && previousBlocked.status === status;
    clearBlockedWorker(handle, worker, {
      clearNetworkPumpBackoff: !sameBlockedThread,
      clearPipeSyncBeforeRetry: !sameBlockedThread,
    });
    const blocked: HostBlockedWorkerState = {
      vmPtr,
      status,
    };
    if (sameBlockedThread && previousBlocked !== undefined) {
      copyHostBlockedEffectState(blocked, previousBlocked);
    }
    if (
      sameBlockedThread
      && previousBlocked !== undefined
      && isSupervisorTimerArmed(previousBlocked)
    ) {
      markSupervisorTimerArmed(blocked);
    }
    hostExecutionState(handle).hostBlockedWorkers.set(worker, blocked);
    const supervisorTimerArmed = armBlockedSupervisorTimerForBlocked(handle, blocked);
    if (supervisorTimerArmed) {
      return;
    }
    if (sameBlockedThread && isBlockedRpcPending(blocked)) {
      return;
    }
    if (supervisorBlockedStatusIsEpollWait(handle.kernel, status)) {
      if (supervisorBlockedEpollWaitRetryRequired(handle.kernel, vmPtr)) {
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
          [],
        );
      }
      return;
    }
    if (supervisorBlockedStatusIsWait4(handle.kernel, status)) {
      retryHostBlockedWorker(handle, worker);
      return;
    }
    scheduleSupervisorBlockedRetry(
      handle,
      worker,
      [],
    );
  }

  function markSupervisorTimerArmed(blocked: HostBlockedWorkerState): void {
    blockedWaitTimers.markSupervisorTimerArmed(blocked);
  }

  function isSupervisorTimerArmed(blocked: HostBlockedWorkerState): boolean {
    return blockedWaitTimers.isSupervisorTimerArmed(blocked);
  }

  function armBlockedSupervisorTimerForBlocked(
    handle: ProcessHandle,
    blocked: HostBlockedWorkerState,
  ): boolean {
    return blockedWaitTimers.armBlockedSupervisorTimerForBlocked(handle, blocked);
  }

  const schedulerStateSyncHelpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    drainHiddenProcessOutput,
    drainProcessOutput,
    pumpProcess,
    threadWorkerStates,
    retryHostBlockedWorker,
  });
  const {
    requestEpollDependentOfdStateSync,
    requestEpollStateSync,
    requestPipeStateSync,
    requestSocketStateSync,
    wakePipeAndEpollWaitersInKernelShare,
  } = schedulerStateSyncHelpers;

  const blockedRetryCoordinator = createBlockedRetryCoordinator({
    threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    appendWorkerDebugLog,
    failProcess: deps.failProcess,
    completeProcessExit: deps.completeProcessExit,
    pumpSupervisorSelectedChildProcess: deps.pumpSupervisorSelectedChildProcess,
    pumpNetworkConnections: deps.pumpNetworkConnections,
    executeHostBlockedSyscallResumeOutcome: deps.executeHostBlockedSyscallResumeOutcome,
    resumeFsViaKernelWorker: deps.resumeFsViaKernelWorker,
    resumeFsViaKernelWorkerOutcome: deps.resumeFsViaKernelWorkerOutcome,
    resumeVforkParentAfterChildExit: deps.resumeVforkParentAfterChildExit,
    resumeWait4ViaKernelWorker: deps.resumeWait4ViaKernelWorker,
    beginBlockedRpc,
    finishBlockedRpc,
    observeForkSharedSyncCompletion,
    clearStaleRunningWorker,
    clearBlockedWorker,
    scheduleSupervisorBlockedRetry,
    armBlockedSupervisorTimerForBlocked,
    isSupervisorTimerArmed,
    pumpProcess,
    enqueueWorker,
    selectProcessAnchorVmPtr,
    pruneStaleProcessWorkers,
    requestPipeStateSync,
    requestSocketStateSync,
    requestEpollStateSync,
    requestEpollDependentOfdStateSync,
    supervisorBlockedWaitActiveChildPid,
    blockedOrCurrentSyscallNr,
    formatBlockedPollFdSet,
    formatFirstPipeWaitDebug,
    formatFirstSocketWaitDebug,
    pollWaitTimeoutMs,
    appendBlockedStateLifecycle,
  });
  retryHostBlockedWorkerImpl = blockedRetryCoordinator.retryHostBlockedWorker;

  function notifySchedulerProgress(handle: ProcessHandle): void {
    for (const worker of [...hostExecutionState(handle).hostBlockedWorkers.keys()]) {
      retryHostBlockedWorker(handle, worker);
    }
  }

  return {
    appendWorkerDebugLog,
    attachThreadWorker,
    blockWorker,
    clearBlockedWorker,
    clearHostBlockedRetryTimer,
    parkSupervisorFutexWait,
    requestThreadWorkerBlockedResume,
    enqueueWorker,
    notifySchedulerProgress,
    pumpProcess,
    pruneDetachedProcessWorkers,
    requestEpollDependentOfdStateSync,
    requestEpollStateSync,
    requestPipeStateSync,
    requestSocketStateSync,
    wakePipeAndEpollWaitersInKernelShare,
  };
}
