import {
  supervisorBlockedEpollWaitRetryRequired,
  supervisorBlockedPipeSyncBeforeRetryClear,
  supervisorBlockedPipeSyncBeforeRetryRequested,
  supervisorBlockedPollWaitHasSocket,
  supervisorBlockedRetryAction,
  supervisorBlockedStatusIsEpollWait,
  supervisorBlockedStatusIsPipeWait,
  supervisorBlockedStatusIsWait4,
  supervisorClearBlockedWaitNetworkPump,
  supervisorCompleteBlockedWaitNetworkPump,
  type SupervisorBlockedRetryAction,
  type SupervisorBlockedRetryActionFlag,
  type SupervisorBlockedRetryActionFlags,
  type SupervisorBlockedWaitRetryFlags,
} from "../../supervisor-blocked-resume.js";
import {
  wakeRuntimeBridge,
} from "../../bridge/runtime-state.js";
import {
  releaseWorker,
} from "../pool.js";
import {
  drainHiddenProcessOutput,
  drainProcessOutput,
  flushStdin,
} from "../stdio.js";
import { kernelWorkerOwnsPipeState } from "../ownership/pipe-control.js";
import { describeForkSharedSyncPromise } from "../fork-shared-sync-debug.js";
import { collectLiveThreadVmPtrs } from "../live-threads.js";
import { appendWorkerDebugLog } from "./debug-log.js";
import { clearStaleForkSharedSyncGate } from "../fork-shared-sync-gate.js";
import { createPipePollScheduler } from "./pipe-poll.js";
import { createPruningScheduler } from "./pruning.js";
import { createWorkerSchedulerQueueHelpers } from "./queue.js";
import { createRunLoopScheduler, logNetworkDebug } from "./run-loop.js";
import { createSocketWaitScheduler } from "./socket-wait.js";
import {
  createBlockedWaitDiagnostics,
  formatBlockedEpollEntries,
  formatFaultThreadSummary,
  formatProcessThreadSlots,
  formatRunningRetryBlocker,
  formatThreadFdTableSummary,
} from "../host-boundary/blocked-wait-diagnostics.js";
import {
  createBlockedWaitTimers,
  type BlockedWaitTimers,
} from "../host-boundary/blocked-wait-timers.js";
import { executeKernelWorkerBlockedEpollRetry } from "../host-boundary/blocked-epoll-retry.js";
import { createWorkerSchedulerStateSyncHelpers } from "../host-boundary/state-sync.js";
import {
  readSupervisorFaultExitDecision,
  supervisorFaultExitKindProcessExit,
} from "../supervisor-shadow.js";
import type {
  HostBlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type {
  WorkerScheduler,
  WorkerSchedulerDeps,
} from "./types.js";
import type { KernelRpcResumeOutcome } from "../kernel-rpc/outcomes.js";

export function createWorkerScheduler(deps: WorkerSchedulerDeps): WorkerScheduler {
  const { threadWorkerStates } = deps;
  const threadWorkerBlockedResumeRequests = new WeakSet<Worker>();
  let blockedWaitTimers: BlockedWaitTimers;

  function consumeKernelRpcResumeOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    outcome: KernelRpcResumeOutcome,
  ): void {
    if (outcome.kind === "completed" && outcome.status !== undefined) {
      handleHostBlockedRetryResult(handle, worker, vmPtr, outcome.status);
    }
    switch (outcome.kind) {
      case "completed":
      case "queued-in-worker":
      case "process-gone":
      case "stale":
        return;
      case "blocked": {
        const blocked = handle.hostBlockedWorkers.get(worker);
        if (blocked && blocked.vmPtr === vmPtr) {
          blocked.status = outcome.status;
        }
        return;
      }
      default: {
        const exhaustive: never = outcome;
        throw new Error(`unhandled kernel-rpc outcome: ${JSON.stringify(exhaustive)}`);
      }
    }
  }

  function resumeBlockedSyscallViaResumeOutcome(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
    lifecycleEvent: string,
    afterOutcome?: (outcome: KernelRpcResumeOutcome) => void,
    requireKernelWorker = true,
  ): boolean {
    if (requireKernelWorker && (!handle.kernelWorkerRequest || handle.pid <= 0)) {
      return false;
    }
    const hostBlockedResume = deps.executeHostBlockedSyscallResumeOutcome;
    if (hostBlockedResume === undefined) {
      const resumeSurface = requireKernelWorker ? "kernel-worker" : "host-boundary";
      deps.failProcess(handle, new Error(`${resumeSurface} blocked syscall resume requires executeHostBlockedSyscallResumeOutcome`));
      return true;
    }
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, lifecycleEvent);
    blocked.rpcPending = true;
    void hostBlockedResume(
      handle,
      worker,
      blocked.vmPtr,
      blocked.status,
    ).then((outcome) => {
      consumeKernelRpcResumeOutcome(handle, worker, blocked.vmPtr, outcome);
      afterOutcome?.(outcome);
    }).catch((error) => {
      const latestBlocked = handle.hostBlockedWorkers.get(worker);
      if (latestBlocked && latestBlocked.vmPtr === blocked.vmPtr) {
        latestBlocked.rpcPending = false;
      }
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    });
    return true;
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

  function pumpProcessIfPipeWaitBlocked(handle: ProcessHandle, outcome: KernelRpcResumeOutcome): void {
    if (outcome.kind === "blocked" && supervisorBlockedStatusIsPipeWait(handle.kernel, outcome.status)) {
      pumpProcess(handle);
    }
  }

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
    const blocked = handle.hostBlockedWorkers.get(worker);
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
    handle.hostBlockedWorkers.delete(worker);
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

  function parkBehindSupervisorActiveChild(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
    readyCount: number | undefined,
  ): boolean {
    if (
      !supervisorBlockedStatusIsPipeWait(handle.kernel, blocked.status)
      && !supervisorBlockedStatusIsWait4(handle.kernel, blocked.status)
    ) {
      return false;
    }
    const childPid = supervisorBlockedWaitActiveChildPid(handle, blocked.vmPtr);
    if (childPid <= 0) {
      return false;
    }
    deps.pumpSupervisorSelectedChildProcess?.(childPid);
    const event = supervisorBlockedStatusIsPipeWait(handle.kernel, blocked.status)
      ? `pipe-retry-skip:${handle.pid}:active-child:${childPid}:ready=${readyCount ?? "n/a"}:${formatFirstPipeWaitDebug(handle, blocked.vmPtr)}:${formatBlockedPollFdSet(handle, blocked.vmPtr)}`
      : `wait4-retry-skip:${handle.pid}:active-child:${childPid}`;
    if (blocked.lastPipeRetryLifecycleEvent !== event) {
      blocked.lastPipeRetryLifecycleEvent = event;
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
    }
    scheduleSupervisorBlockedRetry(
      handle,
      worker,
      ["active-child"],
    );
    return true;
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

  function postNetworkPumpRetryActionFlags(
    handle: ProcessHandle,
  ): SupervisorBlockedRetryActionFlags {
    const flags: SupervisorBlockedRetryActionFlag[] = ["wait-timer-already-checked"];
    if (handle.runningWorker !== null) {
      flags.push("running-worker");
    }
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      flags.push("kernel-worker-available");
    }
    return flags;
  }

  function readPostNetworkPumpRetryAction(
    handle: ProcessHandle,
    latestBlocked: HostBlockedWorkerState,
  ): SupervisorBlockedRetryAction | null {
    try {
      return supervisorBlockedRetryAction(
        handle.kernel,
        latestBlocked.vmPtr,
        postNetworkPumpRetryActionFlags(handle),
      );
    } catch (error) {
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      return null;
    }
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
    const readyIndex = handle.vforkReadyWorkers.indexOf(worker);
    if (readyIndex >= 0) {
      handle.vforkReadyWorkers.splice(readyIndex, 1);
    }
    if (handle.runningWorker === worker) {
      handle.runningWorker = null;
    }
    const thread = threadWorkerStates.get(worker);
    if (thread !== undefined && thread.vmPtr === vmPtr) {
      thread.runningGeneration = null;
    }
    appendWorkerDebugLog(
      handle,
      `${source}:0x${vmPtr.toString(16)}`,
    );
  }

  function hasPendingCloneChildInitResume(handle: ProcessHandle): boolean {
    for (const worker of handle.workers) {
      const thread = threadWorkerStates.get(worker);
      if (
        thread !== undefined
        && !thread.initialized
        && thread.resumeWorkersOnReady.length > 0
      ) {
        return true;
      }
    }
    return false;
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

  function pumpNetworkForBlockedWait(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): boolean {
    appendWorkerDebugLog(handle, `net-pump-start:${handle.pid}:status=${blocked.status}`);
    logNetworkDebug(`pump-start pid=${handle.pid} status=${blocked.status}`);
    const rpcGeneration = beginBlockedRpc(blocked);
    void deps.pumpNetworkConnections(handle, blocked.vmPtr)
      .then(async (changed) => {
        if (changed) {
          await requestSocketStateSync(handle, true);
          await requestPipeStateSync(handle, false);
        }
        const latestBlocked = finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
        if (!latestBlocked) {
          return;
        }
        try {
          supervisorCompleteBlockedWaitNetworkPump(handle.kernel, latestBlocked.vmPtr, changed);
        } catch (error) {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          return;
        }
        appendWorkerDebugLog(handle, `net-pump-done:${handle.pid}:status=${blocked.status}`);
        logNetworkDebug(`pump-done pid=${handle.pid} status=${blocked.status}`);
        if (
          changed
          && supervisorBlockedStatusIsPipeWait(handle.kernel, latestBlocked.status)
          && supervisorBlockedPollWaitHasSocket(handle.kernel, latestBlocked.vmPtr)
        ) {
          const action = readPostNetworkPumpRetryAction(handle, latestBlocked);
          switch (action) {
            case null:
              return;
            case "defer-running-worker":
            case "defer-process-gate":
            case "defer-passive-wait":
              scheduleSupervisorBlockedRetry(
                handle,
                worker,
              );
              return;
            case "park-active-child":
              if (!parkBehindSupervisorActiveChild(
                handle,
                worker,
                latestBlocked,
                handle.kernel.thread_poll_wait_ready_count?.(latestBlocked.vmPtr),
              )) {
                deps.failProcess(
                  handle,
                  new Error(`kernel supervisor selected active-child retry for unsupported status ${latestBlocked.status}`),
                );
              }
              return;
            case "request-pipe-sync":
            case "request-pipe-sync-and-drain-output":
              executePipeSyncBeforeRetryAction(
                handle,
                worker,
                latestBlocked,
                action === "request-pipe-sync-and-drain-output",
              );
              return;
            case "schedule-pipe-sync-retry":
              scheduleSupervisorBlockedRetry(handle, worker);
              return;
            case "arm-wait-timer":
              if (armBlockedSupervisorTimerForBlocked(handle, latestBlocked)) {
                return;
              }
              break;
            case "resume-kernel-worker":
            case "resume-direct":
              break;
            default:
              deps.failProcess(
                handle,
                new Error(`kernel supervisor returned invalid post-network retry action: ${action}`),
              );
              return;
          }
          if (
            handle.kernelWorkerRequest
            && handle.pid > 0
          ) {
            resumeBlockedSyscallViaResumeOutcome(
              handle,
              worker,
              latestBlocked,
              `pipe-retry-rpc:${handle.pid}`,
              (outcome) => pumpProcessIfPipeWaitBlocked(handle, outcome),
            );
            return;
          }
          resumeBlockedSyscallViaResumeOutcome(
            handle,
            worker,
            latestBlocked,
            `pipe-retry-host-boundary:${handle.pid}`,
            (outcome) => pumpProcessIfPipeWaitBlocked(handle, outcome),
            false,
          );
          return;
        }
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
          changed
            ? []
            : ["host-progress"],
        );
      })
      .catch((error) => {
        finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    return true;
  }

  function appendEpollRetryResult(handle: ProcessHandle, vmPtr: number): void {
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `epoll-retry-result:${handle.pid}:vm=0x${vmPtr.toString(16)}`
        + `:status=${handle.kernel.thread_status(vmPtr)}`
        + `:a0=${handle.kernel.thread_a0?.(vmPtr)?.toString() ?? "n/a"}`,
    );
  }

  function appendWait4RetryResult(handle: ProcessHandle, vmPtr: number): void {
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `wait4-retry-result:${handle.pid}:result=${handle.kernel.thread_a0?.(vmPtr)?.toString() ?? "n/a"}`
        + `:status=${handle.kernel.thread_status(vmPtr)}`
        + `:a0=${handle.kernel.thread_a0?.(vmPtr)?.toString() ?? "n/a"}:records=supervisor`,
    );
  }

  function resumeEpollWait(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): void {
    if (resumeBlockedSyscallViaResumeOutcome(
      handle,
      worker,
      blocked,
      `epoll-retry-resume-rpc:${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}`,
      () => {
        if (handle.workers.has(worker) && threadWorkerStates.has(worker)) {
          appendEpollRetryResult(handle, blocked.vmPtr);
        }
      },
    )) {
      return;
    }

    resumeBlockedSyscallViaResumeOutcome(
      handle,
      worker,
      blocked,
      `epoll-retry-resume-host-boundary:${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}`,
      () => {
        if (handle.workers.has(worker) && threadWorkerStates.has(worker)) {
          appendEpollRetryResult(handle, blocked.vmPtr);
        }
      },
      false,
    );
  }

  function clearStaleHostBlockedWorker(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): boolean {
    const currentStatus = handle.kernel.thread_status(blocked.vmPtr);
    if (currentStatus === blocked.status) {
      return false;
    }
    if (
      currentStatus !== handle.kernel.abi_status_ok()
      && currentStatus !== handle.kernel.abi_status_fault()
    ) {
      return false;
    }
    if (supervisorBlockedStatusIsWait4(handle.kernel, blocked.status)) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `wait4-retry-stale:${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}:status=${currentStatus}`,
      );
    }
    handleHostBlockedRetryResult(handle, worker, blocked.vmPtr, currentStatus);
    return true;
  }

  function resumeWait4BlockedWorker(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): void {
    const forkSharedSyncPromise = handle.kernelShare.forkSharedSyncPromise;
    if (forkSharedSyncPromise !== null) {
      const staleGate = clearStaleForkSharedSyncGate(handle, forkSharedSyncPromise);
      if (staleGate !== null) {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-retry-cleared:${handle.pid}:${staleGate}`);
      }
    }

    if (clearStaleHostBlockedWorker(handle, worker, blocked)) {
      return;
    }

    resumeBlockedSyscallViaResumeOutcome(
      handle,
      worker,
      blocked,
      `wait4-retry-host-boundary:${handle.pid}:0x${blocked.vmPtr.toString(16)}:records=supervisor`,
      () => {
        if (handle.workers.has(worker) && threadWorkerStates.has(worker)) {
          appendWait4RetryResult(handle, blocked.vmPtr);
        }
      },
      false,
    );
  }

  function blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void {
    if (status === handle.kernel.abi_status_futex_wait()) {
      parkSupervisorFutexWait(handle, worker, vmPtr, "block-worker-futex-supervisor-owned");
      return;
    }
    const previousBlocked = handle.hostBlockedWorkers.get(worker);
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
      rpcPending: sameBlockedThread ? previousBlocked.rpcPending : false,
      rpcGeneration: sameBlockedThread ? previousBlocked.rpcGeneration : 0,
      lastPipeRetryLifecycleEvent: sameBlockedThread ? previousBlocked.lastPipeRetryLifecycleEvent : undefined,
      lastWait4RetryLifecycleEvent: sameBlockedThread ? previousBlocked.lastWait4RetryLifecycleEvent : undefined,
    };
    if (
      sameBlockedThread
      && previousBlocked !== undefined
      && isSupervisorTimerArmed(previousBlocked)
    ) {
      markSupervisorTimerArmed(blocked);
    }
    handle.hostBlockedWorkers.set(worker, blocked);
    const supervisorTimerArmed = armBlockedSupervisorTimerForBlocked(handle, blocked);
    if (supervisorTimerArmed) {
      return;
    }
    if (sameBlockedThread && blocked.rpcPending) {
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

  function handleHostBlockedRetryResult(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    status: number,
  ): void {
    const blocked = handle.hostBlockedWorkers.get(worker);
    if (!blocked || blocked.vmPtr !== vmPtr) {
      return;
    }

    if (status === handle.kernel.abi_status_ok()) {
      if (supervisorBlockedStatusIsEpollWait(handle.kernel, blocked.status)) {
        appendWorkerDebugLog(
          handle,
          `blocked-epoll-ready-fds:${handle.pid}:vm=0x${vmPtr.toString(16)}:${formatThreadFdTableSummary(handle, vmPtr)}`,
        );
      }
      if (supervisorBlockedStatusIsWait4(handle.kernel, blocked.status)) {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-resumed-supervisor:${handle.pid}`);
      }
      clearBlockedWorker(handle, worker);
      enqueueWorker(handle, worker);
      return;
    }
    if (status === handle.kernel.abi_status_fault()) {
      clearBlockedWorker(handle, worker);
      const supervisorExitDecision = readSupervisorFaultExitDecision(handle, vmPtr);
      if (supervisorExitDecision === undefined) {
        deps.failProcess(
          handle,
          new Error(`supervisor fault exit decision required for vm=0x${vmPtr.toString(16)}`),
        );
        return;
      }
      const exitCode = supervisorExitDecision.exitCode;
      const termSignal = supervisorExitDecision.termSignal;
      if (deps.resumeVforkParentAfterChildExit(handle, worker, vmPtr, exitCode, termSignal)) {
        return;
      }
      const currentPid = handle.kernel.process_current_pid(vmPtr);
      const remainingThreads = supervisorExitDecision.remainingThreads;
      const hasLiveThreads = supervisorExitDecision.liveThreads !== 0;
      const faultProcessPtr = handle.kernel.thread_process_ptr(vmPtr);
      const ownerPid = handle.kernel.process_current_pid(handle.vmPtr);
      const ownerProcessPtr = handle.kernel.thread_process_ptr(handle.vmPtr);
      const ownerThreadCount = handle.kernel.thread_process_thread_count(handle.vmPtr);
      const processTableDebug = `:fault_pid=${currentPid}:fault_process=0x${faultProcessPtr.toString(16)}`
        + `:fault_count=${remainingThreads}`
        + `:owner_pid=${ownerPid}:owner_process=0x${ownerProcessPtr.toString(16)}`
        + `:owner_count=${ownerThreadCount}:owner_threads=[${formatProcessThreadSlots(handle, handle.vmPtr)}]`;
      appendWorkerDebugLog(handle, `fault:0x${vmPtr.toString(16)} remaining=${remainingThreads}`);
      if (handle.runningWorker === worker) {
        handle.runningWorker = null;
      }
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `scheduler-fault-worker-delete:${handle.pid}:vm=0x${vmPtr.toString(16)}`
          + `:status=${status}:exit=${exitCode}:${termSignal}`
          + `:current=${handle.kernel.thread_status?.(vmPtr) ?? -1}`
          + `:remaining=${remainingThreads}:live=${hasLiveThreads ? 1 : 0}`
          + processTableDebug
          + `:${formatFaultThreadSummary(handle, "faultVm", vmPtr)}`
          + `:threads=[${formatProcessThreadSlots(handle, vmPtr)}]`,
      );
      handle.workers.delete(worker);
      threadWorkerStates.delete(worker);
      releaseWorker(worker);
      const shouldExitProcess = supervisorExitDecision.kind === supervisorFaultExitKindProcessExit(handle.kernel);
      if (shouldExitProcess) {
        const exitVmPtr = supervisorExitDecision.exitVmPtr;
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `complete-exit-request:scheduler-fault:${handle.pid}:vm=0x${vmPtr.toString(16)}`
            + `:exitVm=0x${exitVmPtr.toString(16)}`
            + `:decision=${supervisorExitDecision.seq.toString()}`
            + `:status=${status}:exit=${exitCode}:${termSignal}`
            + `:current=${handle.kernel.thread_status?.(vmPtr) ?? -1}:remaining=${remainingThreads}:live=${hasLiveThreads ? 1 : 0}`
            + processTableDebug
            + `:${formatFaultThreadSummary(handle, "faultVm", vmPtr)}`
            + `:${formatFaultThreadSummary(handle, "exitVm", exitVmPtr)}`,
        );
        void deps.completeProcessExit(handle, exitCode, termSignal, exitVmPtr).catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      } else {
        pumpProcess(handle);
      }
      return;
    }

    if (status === handle.kernel.abi_status_futex_wait()) {
      parkSupervisorFutexWait(handle, worker, vmPtr, "blocked-retry-futex-supervisor-owned");
      return;
    }

    blocked.status = status;
    const supervisorTimerArmed = armBlockedSupervisorTimerForBlocked(handle, blocked);
    if (supervisorTimerArmed) {
      return;
    }

    if (supervisorBlockedStatusIsEpollWait(handle.kernel, status)) {
      if (supervisorBlockedEpollWaitRetryRequired(handle.kernel, vmPtr)) {
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
          [],
        );
        pumpProcess(handle);
      }
      return;
    }
    scheduleSupervisorBlockedRetry(
      handle,
      worker,
      [],
    );
  }

  function blockedRetryActionFlags(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
    thread: ThreadWorkerState | undefined,
  ): SupervisorBlockedRetryActionFlags {
    const flags: SupervisorBlockedRetryActionFlag[] = [];
    if (isSupervisorTimerArmed(blocked)) {
      flags.push("supervisor-timer-armed");
    }
    if (blocked.rpcPending) {
      flags.push("rpc-pending");
    }
    if (thread?.ownerAsyncStatus !== null && thread?.ownerAsyncStatus !== undefined) {
      flags.push("owner-async");
    }
    if (hasPendingCloneChildInitResume(handle)) {
      flags.push("thread-init-pending");
    }
    if (handle.kernelShare.forkSharedSyncPromise !== null) {
      flags.push("fork-shared-sync-pending");
    }
    if (handle.runningWorker !== null) {
      flags.push("running-worker");
    }
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      flags.push("kernel-worker-available");
    }
    if (kernelWorkerOwnsPipeState(handle)) {
      flags.push("pipe-state-kernel-worker-owns");
    }
    if (handle.networkConnections.size > 0) {
      flags.push("network-connections-available");
    }
    return flags;
  }

  function readBlockedRetryAction(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): SupervisorBlockedRetryAction | null {
    const thread = threadWorkerStates.get(worker);
    try {
      return supervisorBlockedRetryAction(
        handle.kernel,
        blocked.vmPtr,
        blockedRetryActionFlags(handle, worker, blocked, thread),
      );
    } catch (error) {
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      return null;
    }
  }

  function runBlockedRetryHostCleanup(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): boolean {
    const anchorVmPtr = selectProcessAnchorVmPtr(handle, worker);
    if (anchorVmPtr !== null) {
      try {
        const liveVmPtrs = collectLiveThreadVmPtrs(handle.kernel, anchorVmPtr);
        pruneStaleProcessWorkers(handle, liveVmPtrs, null);
        if (!liveVmPtrs.has(blocked.vmPtr)) {
          clearBlockedWorker(handle, worker);
          return false;
        }
      } catch {
        // If the candidate anchor is stale, fall back to the normal resume path.
      }
    }

    flushStdin(handle);
    if (!kernelWorkerOwnsPipeState(handle)) {
      drainProcessOutput(handle);
    }
    return true;
  }

  function appendPipeRetryLifecycle(
    handle: ProcessHandle,
    blocked: HostBlockedWorkerState,
    blockedPollTimeoutMs: number,
    blockedPollReadyCount: number | undefined,
  ): void {
    if (supervisorBlockedStatusIsPipeWait(handle.kernel, blocked.status)) {
      const pipeSyncBeforeRetryRequested = supervisorBlockedPipeSyncBeforeRetryRequested(
        handle.kernel,
        blocked.vmPtr,
      );
      const event = `pipe-retry:${handle.pid}:a7=${blockedOrCurrentSyscallNr(handle, blocked.vmPtr)?.toString() ?? "n/a"}:a0=${handle.kernel.thread_a0(blocked.vmPtr).toString()}:timeout=${blockedPollTimeoutMs}:ready=${blockedPollReadyCount ?? "n/a"}:${formatFirstPipeWaitDebug(handle, blocked.vmPtr)}:${formatFirstSocketWaitDebug(handle, blocked.vmPtr)}:${formatBlockedPollFdSet(handle, blocked.vmPtr)}:syncRequested=${pipeSyncBeforeRetryRequested ? 1 : 0}`;
      if (blocked.lastPipeRetryLifecycleEvent !== event) {
        blocked.lastPipeRetryLifecycleEvent = event;
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
      }
    }
  }

  function executePipeSyncBeforeRetryAction(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
    shouldDrainOutput: boolean,
  ): void {
    const rpcGeneration = beginBlockedRpc(blocked);
    void requestPipeStateSync(handle, false)
      .then(() => {
        const latestBlocked = finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
        if (!latestBlocked) {
          return;
        }
        if (shouldDrainOutput || !kernelWorkerOwnsPipeState(handle)) {
          drainProcessOutput(handle);
        }
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
          ["host-progress"],
        );
      })
      .catch((error) => {
        finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
  }

  function executeFsRetry(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): void {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `fs-retry:${handle.pid}`);
      const thread = threadWorkerStates.get(worker);
      if (thread?.ownerAsyncStatus !== null && thread?.ownerAsyncStatus !== undefined) {
        scheduleSupervisorBlockedRetry(handle, worker);
        return;
      }
      if (thread) {
        thread.ownerAsyncStatus = blocked.status;
      }
      clearBlockedWorker(handle, worker);
      const resumeFs = deps.resumeFsViaKernelWorkerOutcome ?? deps.resumeFsViaKernelWorker;
      void resumeFs(handle, worker, blocked.vmPtr)
        .then((outcome) => {
          if (outcome !== undefined) {
            consumeKernelRpcResumeOutcome(handle, worker, blocked.vmPtr, outcome);
          }
        })
        .finally(() => {
          if (thread) {
            thread.ownerAsyncStatus = null;
          }
          pumpProcess(handle);
        })
        .catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
  }

  function executeKernelWorkerBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): void {
    executeKernelWorkerBlockedEpollRetry(
      {
        threadWorkerStates,
        appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
        failProcess: deps.failProcess,
        beginBlockedRpc,
        finishBlockedRpc,
        requestPipeStateSync,
        requestEpollStateSync,
        requestEpollDependentOfdStateSync,
        armBlockedSupervisorTimerForBlocked,
        clearStaleRunningWorker,
        scheduleSupervisorBlockedRetry,
        resumeEpollWait,
        formatBlockedEpollEntries,
        formatRunningRetryBlocker,
      },
      handle,
      worker,
      blocked,
    );
  }

  type BlockedRetryActionResult = "done" | "needs-host-cleanup" | "retry";

  function executeSupervisorBlockedRetryAction(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
    action: SupervisorBlockedRetryAction,
    hostCleanupDone: boolean,
    blockedPollTimeoutMs: number,
    blockedPollReadyCount: number | undefined,
  ): BlockedRetryActionResult {
    switch (action) {
      case "defer-supervisor-timer":
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `blocked-retry-skip-supervisor-timer:${handle.pid}:status=${blocked.status}:vm=0x${blocked.vmPtr.toString(16)}`,
        );
        wakeRuntimeBridge(handle.runtimeBridge);
        pumpProcess(handle);
        return "done";
      case "defer-rpc-pending":
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-retry-skip:${handle.pid}:rpc-pending`);
        return "done";
      case "defer-owner-async": {
        const thread = threadWorkerStates.get(worker);
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `blocked-retry-skip:${handle.pid}:owner-async:${thread?.ownerAsyncStatus ?? "unknown"}`,
        );
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
          [],
        );
        return "done";
      }
      case "defer-thread-init-pending":
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `epoll-retry-skip:${handle.pid}:clone-child-init-pending`,
        );
        wakeRuntimeBridge(handle.runtimeBridge);
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
          ["thread-init-pending"],
        );
        return "done";
      case "arm-wait-timer":
        try {
          if (!armBlockedSupervisorTimerForBlocked(handle, blocked)) {
            return "retry";
          }
        } catch (error) {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        }
        return "done";
      case "park-active-child":
        if (!parkBehindSupervisorActiveChild(
          handle,
          worker,
          blocked,
          blockedPollReadyCount,
        )) {
          deps.failProcess(
            handle,
            new Error(`kernel supervisor selected active-child retry for unsupported status ${blocked.status}`),
          );
        }
        return "done";
      case "resume-wait4":
        resumeWait4BlockedWorker(handle, worker, blocked);
        return "done";
      case "defer-fork-shared-sync": {
        const forkSharedSyncPromise = handle.kernelShare.forkSharedSyncPromise;
        if (forkSharedSyncPromise === null) {
          return "retry";
        }
        const staleGate = clearStaleForkSharedSyncGate(handle, forkSharedSyncPromise);
        if (staleGate !== null) {
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-retry-cleared:${handle.pid}:${staleGate}`);
          return "retry";
        }
        observeForkSharedSyncCompletion(handle, worker, forkSharedSyncPromise);
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `blocked-retry-skip:${handle.pid}:fork-shared-sync:${describeForkSharedSyncPromise(forkSharedSyncPromise)}`,
        );
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
          [],
        );
        return "done";
      }
      case "defer-running-worker": {
        if (supervisorBlockedStatusIsEpollWait(handle.kernel, blocked.status)) {
          wakeRuntimeBridge(handle.runtimeBridge);
        }
        const runningThread = handle.runningWorker === null
          ? undefined
          : threadWorkerStates.get(handle.runningWorker);
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `blocked-retry-skip:${handle.pid}:running=1`
            + `:${formatRunningRetryBlocker(handle, runningThread)}`,
        );
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
          [],
        );
        return "done";
      }
      case "defer-process-gate":
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-retry-skip:${handle.pid}:process-gate`);
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
        );
        return "done";
      case "pump-network":
        if (!pumpNetworkForBlockedWait(handle, worker, blocked)) {
          scheduleSupervisorBlockedRetry(handle, worker);
        }
        return "done";
      case "epoll-kernel-worker":
        if (!hostCleanupDone) {
          return "needs-host-cleanup";
        }
        executeKernelWorkerBlockedRetry(handle, worker, blocked);
        return "done";
      case "request-pipe-sync":
      case "request-pipe-sync-and-drain-output":
        if (!hostCleanupDone) {
          return "needs-host-cleanup";
        }
        appendPipeRetryLifecycle(handle, blocked, blockedPollTimeoutMs, blockedPollReadyCount);
        executePipeSyncBeforeRetryAction(
          handle,
          worker,
          blocked,
          action === "request-pipe-sync-and-drain-output",
        );
        return "done";
      case "schedule-pipe-sync-retry":
        appendPipeRetryLifecycle(handle, blocked, blockedPollTimeoutMs, blockedPollReadyCount);
        scheduleSupervisorBlockedRetry(handle, worker);
        return "done";
      case "resume-fs":
        if (!hostCleanupDone) {
          return "needs-host-cleanup";
        }
        executeFsRetry(handle, worker, blocked);
        return "done";
      case "defer-passive-wait":
        scheduleSupervisorBlockedRetry(handle, worker);
        return "done";
      case "resume-kernel-worker":
        if (!hostCleanupDone) {
          return "needs-host-cleanup";
        }
        appendPipeRetryLifecycle(handle, blocked, blockedPollTimeoutMs, blockedPollReadyCount);
        resumeBlockedSyscallViaResumeOutcome(
          handle,
          worker,
          blocked,
          `pipe-retry-rpc:${handle.pid}`,
          (outcome) => pumpProcessIfPipeWaitBlocked(handle, outcome),
        );
        return "done";
      case "resume-wait4-kernel-worker":
        if (!hostCleanupDone) {
          return "needs-host-cleanup";
        }
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-retry-rpc:${handle.pid}`);
        blocked.rpcPending = true;
        void deps.resumeWait4ViaKernelWorker(handle, worker, blocked.vmPtr)
          .then((outcome) => {
            consumeKernelRpcResumeOutcome(handle, worker, blocked.vmPtr, outcome);
          })
          .catch((error) => {
            const latestBlocked = handle.hostBlockedWorkers.get(worker);
            if (latestBlocked && latestBlocked.vmPtr === blocked.vmPtr) {
              latestBlocked.rpcPending = false;
            }
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return "done";
      case "unsupported":
        deps.failProcess(
          handle,
          new Error(`kernel supervisor rejected blocked status ${blocked.status} resume delegation`),
        );
        return "done";
      case "resume-epoll-direct":
        if (!hostCleanupDone) {
          return "needs-host-cleanup";
        }
        resumeEpollWait(handle, worker, blocked);
        return "done";
      case "resume-direct":
        if (!hostCleanupDone) {
          return "needs-host-cleanup";
        }
        appendPipeRetryLifecycle(handle, blocked, blockedPollTimeoutMs, blockedPollReadyCount);
        resumeBlockedSyscallViaResumeOutcome(
          handle,
          worker,
          blocked,
          supervisorBlockedStatusIsPipeWait(handle.kernel, blocked.status)
            ? `pipe-retry-host-boundary:${handle.pid}`
            : `blocked-retry-host-boundary:${handle.pid}:status=${blocked.status}`,
          (outcome) => pumpProcessIfPipeWaitBlocked(handle, outcome),
          false,
        );
        return "done";
      default: {
        const exhaustive: never = action;
        throw new Error(`unhandled supervisor blocked retry action: ${exhaustive}`);
      }
    }
  }

  function retryHostBlockedWorker(handle: ProcessHandle, worker: Worker): void {
    const blocked = handle.hostBlockedWorkers.get(worker);
    if (!blocked) {
      return;
    }
    appendBlockedStateLifecycle(handle, blocked);
    clearStaleRunningWorker(handle, worker);
    if (clearStaleHostBlockedWorker(handle, worker, blocked)) {
      return;
    }
    const blockedPollTimeoutMs = pollWaitTimeoutMs(handle, blocked.status, blocked.vmPtr);
    const blockedPollReadyCount = supervisorBlockedStatusIsPipeWait(handle.kernel, blocked.status)
      ? handle.kernel.thread_poll_wait_ready_count?.(blocked.vmPtr)
      : undefined;
    let action = readBlockedRetryAction(handle, worker, blocked);
    if (action === null) {
      return;
    }
    let result = executeSupervisorBlockedRetryAction(
      handle,
      worker,
      blocked,
      action,
      false,
      blockedPollTimeoutMs,
      blockedPollReadyCount,
    );
    if (result === "done") {
      return;
    }
    if (result === "retry") {
      retryHostBlockedWorker(handle, worker);
      return;
    }
    if (!runBlockedRetryHostCleanup(handle, worker, blocked)) {
      return;
    }
    result = executeSupervisorBlockedRetryAction(
      handle,
      worker,
      blocked,
      action,
      true,
      blockedPollTimeoutMs,
      blockedPollReadyCount,
    );
    if (result === "retry") {
      retryHostBlockedWorker(handle, worker);
      return;
    }
    if (result === "needs-host-cleanup") {
      deps.failProcess(
        handle,
        new Error(`kernel supervisor blocked retry action still requires host cleanup after cleanup: ${action}`),
      );
    }
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

  function notifySchedulerProgress(handle: ProcessHandle): void {
    for (const worker of [...handle.hostBlockedWorkers.keys()]) {
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
