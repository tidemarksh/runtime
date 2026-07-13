import { hostExecutionState } from "../host-execution-state.js";
import { processHasWorker, processWorkerList } from "../process-worker-state.js";
import {
  supervisorBlockedEpollWaitTimeoutMs,
  supervisorBlockedInitialAction,
  supervisorBlockedPipeSyncBeforeRetryClear,
  supervisorClearBlockedWaitNetworkPump,
  supervisorSchedulerBlockWorkerAction,
  supervisorSchedulerClearBlockedWorkerActions,
  supervisorSchedulerFutexWaiterAction,
  type SupervisorBlockedRetryActionFlag,
  type SupervisorBlockedWaitRetryFlags,
} from "../../supervisor-blocked-resume.js";
import {
  drainHiddenProcessOutput,
  drainProcessOutput,
} from "../stdio.js";
import { appendWorkerDebugLog } from "./worker-debug-log.js";
import {
  snapshotProcessPipeSlotsForThreadStep,
} from "../ownership/pipe-control.js";
import {
  appendPendingPipeSlotsForNextStep,
} from "../thread-step-payload.js";
import { createPipePollScheduler } from "./blocked-poll.js";
import { createPruningScheduler } from "./worker-pruning.js";
import { createWorkerSchedulerQueueHelpers } from "./thread-worker-queue.js";
import { createRunLoopScheduler, logNetworkDebug } from "./worker-run-loop.js";
import { createSocketWaitScheduler } from "../diagnostics/socket-wait.js";
import {
  createBlockedWaitDiagnostics,
} from "./blocked-wait-diagnostics.js";
import {
  createBlockedWaitTimers,
  type BlockedWaitTimers,
} from "./blocked-wait-timers.js";
import {
  createBlockedRetryCoordinator,
} from "./blocked-retry.js";
import { createWorkerSchedulerStateSyncHelpers } from "./state-sync.js";
import type {
  HostBlockedWorkerState,
  ProcessHandle,
} from "../types.js";
import { clearThreadRunningGeneration } from "../thread-host-state.js";
import type {
  WorkerScheduler,
  WorkerSchedulerDeps,
} from "./worker-scheduler-types.js";
import {
  copyHostBlockedEffectState,
  isEpollPassiveUntilWake,
  isEpollWakeRetryPending,
  isBlockedRpcPending,
  setEpollPassiveUntilWake,
} from "../host-blocked-state.js";
import { clearSupervisorHostTimer } from "../supervisor-host-op.js";

interface BlockWorkerOptions {
  deferInitialRetry?: boolean;
}

export function createWorkerScheduler(deps: WorkerSchedulerDeps): WorkerScheduler {
  const { threadWorkerStates } = deps;
  const threadWorkerBlockedResumeRequests = new WeakSet<Worker>();
  const threadWorkerBlockedTimeoutExpired = new WeakSet<Worker>();
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
    takeThreadWorkerBlockedTimeoutExpired,
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
    threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    appendWorkerDebugLog,
    failProcess: deps.failProcess,
    retryHostBlockedWorker,
    markThreadWorkerBlockedTimeoutExpired,
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
    const clearActions = supervisorSchedulerClearBlockedWorkerActions(
      handle.kernel,
      blocked.status,
      { clearPipeSyncBeforeRetry: options.clearPipeSyncBeforeRetry ?? true },
    );
    if (clearActions.clearPipeSyncBeforeRetry) {
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
    clearSupervisorHostTimer(handle, blocked.vmPtr);
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

  function requestThreadWorkerBlockedResume(handle: ProcessHandle, worker: Worker): void {
    threadWorkerBlockedResumeRequests.add(worker);
    const thread = threadWorkerStates.get(worker);
    if (thread === undefined) {
      return;
    }
    const pipeSlots = snapshotProcessPipeSlotsForThreadStep(handle, thread.vmPtr);
    if (pipeSlots.length === 0) {
      return;
    }
    appendPendingPipeSlotsForNextStep(thread, pipeSlots);
    appendWorkerDebugLog(
      handle,
      `blocked-resume-pipe-slots:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}:slots=${pipeSlots.length}`,
    );
  }

  function takeThreadWorkerBlockedResume(_handle: ProcessHandle, worker: Worker): boolean {
    if (!threadWorkerBlockedResumeRequests.has(worker)) {
      return false;
    }
    threadWorkerBlockedResumeRequests.delete(worker);
    return true;
  }

  function markThreadWorkerBlockedTimeoutExpired(handle: ProcessHandle, vmPtr: number): void {
    for (const worker of processWorkerList(handle)) {
      if (threadWorkerStates.get(worker)?.vmPtr === vmPtr) {
        threadWorkerBlockedTimeoutExpired.add(worker);
        return;
      }
    }
  }

  function takeThreadWorkerBlockedTimeoutExpired(
    _handle: ProcessHandle,
    worker: Worker,
  ): boolean {
    if (!threadWorkerBlockedTimeoutExpired.has(worker)) {
      return false;
    }
    threadWorkerBlockedTimeoutExpired.delete(worker);
    return true;
  }

  function isSupervisorFutexWaiter(handle: ProcessHandle, worker: Worker): boolean {
    const thread = threadWorkerStates.get(worker);
    return thread !== undefined
      && processHasWorker(handle, worker)
      && supervisorSchedulerFutexWaiterAction(
        handle.kernel,
        handle.kernel.thread_status(thread.vmPtr),
      ) === "waiter";
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

  function blockWorker(
    handle: ProcessHandle,
    worker: Worker,
    status: number,
    vmPtr: number,
    options: BlockWorkerOptions = {},
  ): void {
    if (supervisorSchedulerBlockWorkerAction(handle.kernel, status) === "park-futex") {
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
    if (status === handle.kernel.abi_status_wait4()) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `wait4-block-register:${handle.pid}:vm=0x${vmPtr.toString(16)}`
          + `:target=${handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0)?.toString() ?? "n/a"}`
          + `:result=${handle.kernel.thread_a0?.(vmPtr)?.toString() ?? "n/a"}`,
      );
    }
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
    const flags: SupervisorBlockedRetryActionFlag[] = [];
    if (isSupervisorTimerArmed(blocked)) {
      flags.push("supervisor-timer-armed");
    }
    if (options.deferInitialRetry || (sameBlockedThread && isBlockedRpcPending(blocked))) {
      flags.push("rpc-pending");
    }
    let initialAction;
    try {
      initialAction = supervisorBlockedInitialAction(handle.kernel, vmPtr, status, flags);
    } catch (error) {
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      return;
    }
    switch (initialAction) {
      case "none":
      case "defer-supervisor-timer":
      case "defer-rpc-pending":
        return;
      case "arm-wait-timer":
        if (armBlockedSupervisorTimerForBlocked(handle, blocked)) {
          return;
        }
        retryHostBlockedWorker(handle, worker);
        return;
      case "retry-now":
        retryHostBlockedWorker(handle, worker);
        return;
      case "schedule-retry":
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
          [],
        );
        return;
      default: {
        const exhaustive: never = initialAction;
        throw new Error(`unhandled supervisor blocked initial action: ${exhaustive}`);
      }
    }
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
    requestThreadWorkerBlockedResume,
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

  function shouldSkipProgressRetry(handle: ProcessHandle, worker: Worker, blocked: HostBlockedWorkerState): boolean {
    if (
      blocked.status !== handle.kernel.abi_status_epoll_wait()
      || supervisorBlockedEpollWaitTimeoutMs(handle.kernel, blocked.vmPtr) >= 0
    ) {
      return false;
    }
    clearStaleRunningWorker(handle, worker);
    if (isEpollPassiveUntilWake(blocked) && !isEpollWakeRetryPending(blocked)) {
      return true;
    }
    if (isEpollWakeRetryPending(blocked) && hostExecutionState(handle).runningWorker !== null) {
      setEpollPassiveUntilWake(blocked, false);
      return true;
    }
    return false;
  }

  function notifySchedulerProgress(handle: ProcessHandle): void {
    for (const [worker, blocked] of [...hostExecutionState(handle).hostBlockedWorkers]) {
      if (shouldSkipProgressRetry(handle, worker, blocked)) {
        continue;
      }
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
