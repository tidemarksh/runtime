import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import {
  resumeBlockedSyscallBySupervisor,
  supervisorBlockedPipeSyncBeforeRetryAction,
  supervisorBlockedPipeSyncBeforeRetryClear,
  supervisorBlockedPipeSyncBeforeRetryRequested,
  supervisorBlockedResumeDelegation,
  supervisorBlockedWaitTimerRequired,
  supervisorBlockedWaitRetryPlan,
  SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC,
  SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC_AND_DRAIN_OUTPUT,
  SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_SCHEDULE_RETRY,
  SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT,
  SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT,
  SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER,
  SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED,
  SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER,
  SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_ACTIVE_CHILD,
  SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_HOST_PROGRESS,
  SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE,
  SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_THREAD_INIT_PENDING,
  SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_ACTIVE_CHILD,
  SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PASSIVE_WAIT,
  type SupervisorBlockedWaitRetryFlags,
  type SupervisorBlockedWaitRetryReason,
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
import { clearStaleForkSharedSyncGate } from "./fork-shared-gate.js";
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
import { createWorkerSchedulerStateSyncHelpers } from "../host-boundary/state-sync.js";
import { createVforkExclusiveScheduler } from "./vfork-exclusive.js";
import {
  readSupervisorFaultExitDecision,
  submitSupervisorShadowStatusEvent,
  supervisorFaultExitKindProcessExit,
  SUPERVISOR_THREAD_EVENT_KIND_TIMER_FIRED,
} from "../supervisor-shadow.js";
import {
  armSupervisorBlockedRetryTimer,
  armSupervisorHostTimer,
  drainSupervisorArmHostTimerHostOp,
  drainSupervisorBlockedRetryHostOp,
} from "../supervisor-host-op.js";
import type {
  HostBlockedWorkerState,
  ProcessHandle,
} from "../types.js";
import type {
  WorkerScheduler,
  WorkerSchedulerDeps,
} from "./types.js";
import type { KernelRpcResumeOutcome } from "../kernel-rpc/outcomes.js";

export function createWorkerScheduler(deps: WorkerSchedulerDeps): WorkerScheduler {
  const { threadWorkerStates } = deps;
  const supervisorArmedBlockedTimers = new WeakSet<HostBlockedWorkerState>();
  const hostBlockedRetryTimers = new WeakMap<HostBlockedWorkerState, ReturnType<typeof setTimeout>>();
  const threadWorkerBlockedResumeRequests = new WeakSet<Worker>();

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

  function requireKernelWorkerBlockedResumeOutcome(
    handle: ProcessHandle,
  ): NonNullable<WorkerSchedulerDeps["executeHostBlockedSyscallResumeOutcome"]> | null {
    const hostBlockedResume = deps.executeHostBlockedSyscallResumeOutcome;
    if (hostBlockedResume !== undefined) {
      return hostBlockedResume;
    }
    deps.failProcess(handle, new Error("kernel-worker blocked syscall resume requires executeHostBlockedSyscallResumeOutcome"));
    return null;
  }

  function resumeBlockedSyscallViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
    lifecycleEvent: string,
    afterOutcome?: (outcome: KernelRpcResumeOutcome) => void,
  ): boolean {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return false;
    }
    const hostBlockedResume = requireKernelWorkerBlockedResumeOutcome(handle);
    if (hostBlockedResume === null) {
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

  function clearBlockedWorker(
    handle: ProcessHandle,
    worker: Worker,
    options: { clearPipeSyncBeforeRetry?: boolean } = {},
  ): void {
    const blocked = handle.hostBlockedWorkers.get(worker);
    if (!blocked) {
      return;
    }
    if (
      (options.clearPipeSyncBeforeRetry ?? true)
      && blocked.status === handle.kernel.abi_status_pipe_wait()
    ) {
      try {
        supervisorBlockedPipeSyncBeforeRetryClear(handle.kernel, blocked.vmPtr);
      } catch (error) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    }
    clearHostBlockedRetryTimer(handle, worker);
    handle.hostBlockedWorkers.delete(worker);
  }

  function clearHostBlockedRetryTimer(handle: ProcessHandle, worker: Worker): void {
    const blocked = handle.hostBlockedWorkers.get(worker);
    if (!blocked) {
      return;
    }
    const timer = hostBlockedRetryTimers.get(blocked);
    if (timer === undefined) {
      return;
    }
    clearTimeout(timer);
    hostBlockedRetryTimers.delete(blocked);
  }

  function submitBlockedRetryTimerFiredEvent(
    handle: ProcessHandle,
    blocked: HostBlockedWorkerState,
    delayMs: number,
    reason: SupervisorBlockedWaitRetryReason,
  ): void {
    const vmPtr = blocked.vmPtr;
    const status = handle.kernel.thread_status?.(vmPtr) ?? blocked.status;
    const result = submitSupervisorShadowStatusEvent(handle, {
      eventKind: handle.kernel.supervisor_thread_event_kind_timer_fired?.()
        ?? SUPERVISOR_THREAD_EVENT_KIND_TIMER_FIRED,
      sequence: blocked.rpcGeneration,
      vmPtr,
      pid: handle.pid,
      tid: handle.kernel.thread_tid?.(vmPtr) ?? 0,
      status,
      blockedReason: reason,
      pc: handle.kernel.thread_pc?.(vmPtr) ?? 0n,
      sp: handle.kernel.thread_sp?.(vmPtr) ?? 0n,
      detail: delayMs,
    });
    if (result !== undefined && result < 0) {
      appendWorkerDebugLog(handle, `supervisor-timer-fired-event:submit-error:${result}`);
    }
  }

  function scheduleHostBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    hostOp: NonNullable<ReturnType<typeof drainSupervisorBlockedRetryHostOp>>,
  ): void {
    const blocked = handle.hostBlockedWorkers.get(worker);
    if (!blocked || hostBlockedRetryTimers.has(blocked)) {
      return;
    }
    const timer = armSupervisorBlockedRetryTimer(handle, hostOp, {
      appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
      appendWorkerDebugLog,
      failProcess: deps.failProcess,
      onTimerFired: (delayMs, reason) => {
        if (hostBlockedRetryTimers.get(blocked) === timer) {
          hostBlockedRetryTimers.delete(blocked);
        }
        submitBlockedRetryTimerFiredEvent(handle, blocked, delayMs, reason as SupervisorBlockedWaitRetryReason);
        retryHostBlockedWorker(handle, worker);
      },
    });
    hostBlockedRetryTimers.set(blocked, timer);
  }

  function scheduleSupervisorBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    flags: SupervisorBlockedWaitRetryFlags = SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE,
  ): void {
    const blocked = handle.hostBlockedWorkers.get(worker);
    if (!blocked) {
      return;
    }
    const enqueueRetryTimer = handle.kernel.supervisor_enqueue_blocked_retry_timer;
    if (enqueueRetryTimer === undefined) {
      deps.failProcess(handle, new Error("kernel supervisor ABI missing supervisor_enqueue_blocked_retry_timer"));
      return;
    }
    const enqueueResult = enqueueRetryTimer(blocked.vmPtr, flags);
    if (enqueueResult < 0) {
      deps.failProcess(
        handle,
        new Error(
          `supervisor blocked retry timer enqueue failed: `
            + `pid=${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}:result=${enqueueResult}`,
        ),
      );
      return;
    }
    if (enqueueResult === 0) {
      deps.failProcess(
        handle,
        new Error(
          `supervisor blocked retry timer enqueue returned no HostOp: `
            + `pid=${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}`,
        ),
      );
      return;
    }
    const retryHostOp = drainSupervisorBlockedRetryHostOp(handle, blocked.vmPtr);
    if (retryHostOp === undefined) {
      deps.failProcess(handle, new Error("kernel supervisor ABI missing blocked retry HostOp exports"));
      return;
    }
    if (retryHostOp === null) {
      deps.failProcess(
        handle,
        new Error(
          `supervisor blocked retry timer enqueue returned no drainable HostOp: `
            + `pid=${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}`,
        ),
      );
      return;
    }
    scheduleHostBlockedRetry(handle, worker, retryHostOp);
  }

  function supervisorBlockedWaitActiveChildPid(handle: ProcessHandle, vmPtr: number): number {
    const readActiveChild = handle.kernel.supervisor_blocked_wait_active_child_pid;
    if (readActiveChild === undefined) {
      throw new Error("kernel supervisor ABI missing supervisor_blocked_wait_active_child_pid");
    }
    const childPid = readActiveChild(vmPtr);
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
      blocked.status !== handle.kernel.abi_status_pipe_wait()
      && blocked.status !== handle.kernel.abi_status_wait4()
    ) {
      return false;
    }
    const retryPlan = supervisorBlockedWaitRetryPlan(handle.kernel, blocked.vmPtr);
    if (retryPlan.reason !== SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_ACTIVE_CHILD) {
      return false;
    }
    const childPid = supervisorBlockedWaitActiveChildPid(handle, blocked.vmPtr);
    if (childPid > 0) {
      deps.pumpSupervisorSelectedChildProcess?.(childPid);
    }
    const event = blocked.status === handle.kernel.abi_status_pipe_wait()
      ? `pipe-retry-skip:${handle.pid}:active-child:${childPid}:ready=${readyCount ?? "n/a"}:${formatFirstPipeWaitDebug(handle, blocked.vmPtr)}:${formatBlockedPollFdSet(handle, blocked.vmPtr)}`
      : `wait4-retry-skip:${handle.pid}:active-child:${childPid}`;
    if (blocked.lastPipeRetryLifecycleEvent !== event) {
      blocked.lastPipeRetryLifecycleEvent = event;
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
    }
    scheduleSupervisorBlockedRetry(
      handle,
      worker,
      SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_ACTIVE_CHILD,
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

  function epollTimerfdDeadlineTicks(handle: ProcessHandle, vmPtr: number): bigint | null {
    const rawDeadline = handle.kernel.thread_epoll_timerfd_deadline_ticks?.(vmPtr);
    if (rawDeadline === undefined) {
      return null;
    }
    const deadline = BigInt(rawDeadline);
    return deadline === 0n ? null : deadline;
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
    canScheduleWorker,
  } = createVforkExclusiveScheduler({
    threadWorkerStates,
    isSupervisorFutexWaiter,
  });
  const {
    blockedOrCurrentSyscallNr,
    formatBlockedPollFdSet,
    formatFirstPipeWaitDebug,
    pollWaitTimeoutMs,
  } = createPipePollScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    scheduleSupervisorBlockedRetry,
  });
  const {
    formatFirstSocketWaitDebug,
    blockedWaitShouldPumpNetwork,
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

  function blockedEpollWatchesPipe(handle: ProcessHandle, vmPtr: number): boolean {
    const supervisorWatchesPipe = handle.kernel.supervisor_blocked_epoll_watches_pipe;
    if (supervisorWatchesPipe === undefined) {
      throw new Error("kernel supervisor ABI missing supervisor_blocked_epoll_watches_pipe");
    }
    return supervisorWatchesPipe(vmPtr) !== 0;
  }

  function collectBlockedEpollWatchedOfdIdsByType(
    handle: ProcessHandle,
    vmPtr: number,
    types: readonly number[],
  ): number[] {
    const supervisorWatchedOfd = handle.kernel.supervisor_blocked_epoll_watched_ofd;
    if (supervisorWatchedOfd === undefined) {
      throw new Error("kernel supervisor ABI missing supervisor_blocked_epoll_watched_ofd");
    }

    const maxFds = handle.kernel.abi_max_fds();
    const targetTypes = new Set(types);
    const ofdIds: number[] = [];
    for (let index = 0; index < maxFds; index += 1) {
      const ofdId = supervisorWatchedOfd(vmPtr, index);
      if (ofdId === 0) {
        break;
      }
      if (
        ofdId > 0
        && ofdId < maxFds
        && targetTypes.has(handle.kernel.kernel_ofd_fd_type(ofdId))
        && !ofdIds.includes(ofdId)
      ) {
        ofdIds.push(ofdId);
      }
    }
    return ofdIds;
  }

  function pumpNetworkForBlockedWait(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): boolean {
    if (!blockedWaitShouldPumpNetwork(handle, blocked.status, blocked.vmPtr)) {
      appendWorkerDebugLog(
        handle,
        `net-pump-skip:${handle.pid}:status=${blocked.status}:conns=${handle.networkConnections.size}`,
      );
      if (handle.networkConnections.size > 0) {
        logNetworkDebug(`pump-skip pid=${handle.pid} status=${blocked.status} conns=${handle.networkConnections.size}`);
      }
      return false;
    }

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
        latestBlocked.skipNextNetworkPump = !changed;
        appendWorkerDebugLog(handle, `net-pump-done:${handle.pid}:status=${blocked.status}`);
        logNetworkDebug(`pump-done pid=${handle.pid} status=${blocked.status}`);
        if (
          changed
          && latestBlocked.status === handle.kernel.abi_status_pipe_wait()
          && handle.kernel.thread_poll_wait_has_socket?.(latestBlocked.vmPtr) === 1
        ) {
          if (handle.runningWorker !== null || !canScheduleWorker(handle, worker)) {
            scheduleSupervisorBlockedRetry(handle, worker);
            return;
          }
          if (
            handle.kernelWorkerRequest
            && handle.pid > 0
          ) {
            resumeBlockedSyscallViaKernelWorker(
              handle,
              worker,
              latestBlocked,
              `pipe-retry-rpc:${handle.pid}`,
              (outcome) => {
                if (
                  outcome.kind === "blocked"
                  && outcome.status === handle.kernel.abi_status_pipe_wait()
                ) {
                  pumpProcess(handle);
                }
              },
            );
            return;
          }
          attachKernelRuntimeState(handle.kernel, handle.kernelState);
          resumeBlockedSyscallBySupervisor(handle.kernel, latestBlocked.vmPtr);
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          handleHostBlockedRetryResult(
            handle,
            worker,
            latestBlocked.vmPtr,
            handle.kernel.thread_status(latestBlocked.vmPtr),
          );
          return;
        }
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
          changed
            ? SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE
            : SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_HOST_PROGRESS,
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

  function resumeEpollWait(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): void {
    if (resumeBlockedSyscallViaKernelWorker(
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

    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    resumeBlockedSyscallBySupervisor(handle.kernel, blocked.vmPtr);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    appendEpollRetryResult(handle, blocked.vmPtr);
    handleHostBlockedRetryResult(handle, worker, blocked.vmPtr, handle.kernel.thread_status(blocked.vmPtr));
  }

  function clearStaleWait4BlockedWorker(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): boolean {
    const currentStatus = handle.kernel.thread_status(blocked.vmPtr);
    if (currentStatus === handle.kernel.abi_status_wait4()) {
      return false;
    }
    if (
      currentStatus !== handle.kernel.abi_status_ok()
      && currentStatus !== handle.kernel.abi_status_fault()
    ) {
      return false;
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `wait4-retry-stale:${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}:status=${currentStatus}`,
    );
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

    if (clearStaleWait4BlockedWorker(handle, worker, blocked)) {
      return;
    }

    let resumeDelegation: ReturnType<typeof supervisorBlockedResumeDelegation>;
    try {
      resumeDelegation = supervisorBlockedResumeDelegation(
        handle.kernel,
        blocked.vmPtr,
        !!handle.kernelWorkerRequest && handle.pid > 0,
      );
    } catch (error) {
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      return;
    }

    switch (resumeDelegation) {
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER:
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
        return;
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT:
        break;
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED:
        if (clearStaleWait4BlockedWorker(handle, worker, blocked)) {
          return;
        }
        deps.failProcess(
          handle,
          new Error(`kernel supervisor rejected wait4 resume delegation for pid ${handle.pid}`),
        );
        return;
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER:
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT:
        deps.failProcess(
          handle,
          new Error(`kernel supervisor returned invalid wait4 resume delegation ${resumeDelegation}`),
        );
        return;
      default: {
        const exhaustive: never = resumeDelegation;
        throw new Error(`unhandled supervisor wait4 resume delegation: ${exhaustive}`);
      }
    }

    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `wait4-retry-local:${handle.pid}:0x${blocked.vmPtr.toString(16)}:records=supervisor`,
    );
    const rawWait4Result = resumeBlockedSyscallBySupervisor(handle.kernel, blocked.vmPtr);
    const wait4Result = rawWait4Result ?? 0n;
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `wait4-retry-result:${handle.pid}:result=${wait4Result.toString()}:status=${handle.kernel.thread_status(blocked.vmPtr)}:a0=${handle.kernel.thread_a0?.(blocked.vmPtr)?.toString() ?? "n/a"}:records=supervisor`,
    );
    handleHostBlockedRetryResult(handle, worker, blocked.vmPtr, handle.kernel.thread_status(blocked.vmPtr));
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
    clearBlockedWorker(handle, worker, { clearPipeSyncBeforeRetry: !sameBlockedThread });
    const blocked: HostBlockedWorkerState = {
      vmPtr,
      status,
      rpcPending: sameBlockedThread ? previousBlocked.rpcPending : false,
      rpcGeneration: sameBlockedThread ? previousBlocked.rpcGeneration : 0,
      skipNextNetworkPump: sameBlockedThread ? previousBlocked.skipNextNetworkPump : false,
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
    if (status === handle.kernel.abi_status_epoll_wait()) {
      if (handle.kernel.thread_process_thread_count(vmPtr) > 1) {
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
          SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE,
        );
      }
      return;
    }
    const pollTimeoutMs = pollWaitTimeoutMs(handle, status, vmPtr);
    const pollReadyCount = status === handle.kernel.abi_status_pipe_wait()
      ? handle.kernel.thread_poll_wait_ready_count?.(vmPtr)
      : undefined;
    if (pollReadyCount !== undefined && pollReadyCount > 0) {
      scheduleSupervisorBlockedRetry(
        handle,
        worker,
        SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_HOST_PROGRESS,
      );
      return;
    }
    if (status === handle.kernel.abi_status_wait4()) {
      retryHostBlockedWorker(handle, worker);
      return;
    }
    if (
      status === handle.kernel.abi_status_pipe_wait()
      && kernelWorkerOwnsPipeState(handle)
      && (!handle.kernelWorkerRequest || handle.pid <= 0)
      && supervisorBlockedPipeSyncBeforeRetryRequested(handle.kernel, vmPtr)
      && pollTimeoutMs < 0
      && (pollReadyCount === undefined || pollReadyCount <= 0)
      && handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr) !== 1
      && handle.kernel.thread_blocked_pipe_write_ready?.(vmPtr) !== 1
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
      && !directPipeReadHasSynchronizedEof(handle, status, vmPtr)
    ) {
      return;
    }
    scheduleSupervisorBlockedRetry(
      handle,
      worker,
      SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE,
    );
  }

  function armBlockedSupervisorTimer(handle: ProcessHandle, vmPtr: number): boolean {
    let hostTimer = drainSupervisorArmHostTimerHostOp(handle, vmPtr);
    if (hostTimer === undefined) {
      throw new Error("kernel supervisor ABI missing arm host timer HostOp exports");
    }
    if (hostTimer === null) {
      const enqueueTimer = handle.kernel.supervisor_enqueue_blocked_wait_timer;
      if (enqueueTimer === undefined) {
        throw new Error("kernel supervisor ABI missing supervisor_enqueue_blocked_wait_timer");
      }
      const enqueueResult = enqueueTimer(vmPtr);
      if (enqueueResult < 0) {
        throw new Error(
          `supervisor blocked wait timer enqueue failed: `
            + `pid=${handle.pid}:vm=0x${vmPtr.toString(16)}:result=${enqueueResult}`,
        );
      }
      if (enqueueResult === 0) {
        if (supervisorBlockedWaitTimerRequired(handle.kernel, vmPtr)) {
          throw new Error(
            `supervisor blocked wait timer required but no HostOp was enqueued: `
              + `pid=${handle.pid}:vm=0x${vmPtr.toString(16)}`,
          );
        }
        return false;
      }
      hostTimer = drainSupervisorArmHostTimerHostOp(handle, vmPtr);
      if (hostTimer === undefined) {
        throw new Error("kernel supervisor ABI missing arm host timer HostOp exports");
      }
      if (hostTimer === null) {
        throw new Error(
          `supervisor blocked wait timer enqueue returned no HostOp: `
            + `pid=${handle.pid}:vm=0x${vmPtr.toString(16)}`,
        );
      }
    }
    if (hostTimer === null) {
      return false;
    }
    armSupervisorHostTimer(handle, hostTimer, {
      appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
      appendWorkerDebugLog,
      failProcess: deps.failProcess,
      pumpProcess,
    });
    return true;
  }

  function markSupervisorTimerArmed(blocked: HostBlockedWorkerState): void {
    supervisorArmedBlockedTimers.add(blocked);
  }

  function isSupervisorTimerArmed(blocked: HostBlockedWorkerState): boolean {
    return supervisorArmedBlockedTimers.has(blocked);
  }

  function armBlockedSupervisorTimerForBlocked(
    handle: ProcessHandle,
    blocked: HostBlockedWorkerState,
  ): boolean {
    if (isSupervisorTimerArmed(blocked)) {
      return true;
    }
    const supervisorTimerArmed = armBlockedSupervisorTimer(handle, blocked.vmPtr);
    if (supervisorTimerArmed) {
      markSupervisorTimerArmed(blocked);
    }
    return supervisorTimerArmed;
  }

  function directPipeReadHasSynchronizedEof(
    handle: ProcessHandle,
    status: number,
    vmPtr: number,
  ): boolean {
    if (status !== handle.kernel.abi_status_pipe_wait()) {
      return false;
    }
    const syscall = blockedOrCurrentSyscallNr(handle, vmPtr);
    const read = handle.kernel.abi_sys_read?.();
    const readv = handle.kernel.abi_sys_readv?.();
    const isDirectRead = syscall !== null
      && (
        (read !== undefined && syscall === BigInt(read))
        || (readv !== undefined && syscall === BigInt(readv))
      );
    if (!isDirectRead || handle.kernel.thread_poll_wait_has_socket?.(vmPtr) === 1) {
      return false;
    }
    const writeOpen = handle.kernel.thread_blocked_pipe_write_open?.(vmPtr);
    if (writeOpen === undefined || writeOpen > 0) {
      return false;
    }
    return (handle.kernel.thread_blocked_pipe_buffered_len?.(vmPtr) ?? 0) === 0;
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
      if (blocked.status === handle.kernel.abi_status_epoll_wait()) {
        appendWorkerDebugLog(
          handle,
          `blocked-epoll-ready-fds:${handle.pid}:vm=0x${vmPtr.toString(16)}:${formatThreadFdTableSummary(handle, vmPtr)}`,
        );
      }
      if (blocked.status === handle.kernel.abi_status_wait4()) {
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

    if (status === handle.kernel.abi_status_epoll_wait()) {
      if (handle.kernel.thread_process_thread_count(vmPtr) > 1) {
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
          SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE,
        );
        pumpProcess(handle);
      }
      return;
    }
    const pollReadyCount = status === handle.kernel.abi_status_pipe_wait()
      ? handle.kernel.thread_poll_wait_ready_count?.(vmPtr)
      : undefined;
    if (pollReadyCount !== undefined && pollReadyCount > 0) {
      scheduleSupervisorBlockedRetry(
        handle,
        worker,
        SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_HOST_PROGRESS,
      );
      return;
    }
    const retryPlan = supervisorBlockedWaitRetryPlan(handle.kernel, vmPtr);
    if (retryPlan.reason === SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PASSIVE_WAIT) {
      scheduleSupervisorBlockedRetry(handle, worker);
      return;
    }
    scheduleSupervisorBlockedRetry(
      handle,
      worker,
      SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE,
    );
  }

  function retryHostBlockedWorker(handle: ProcessHandle, worker: Worker): void {
    const blocked = handle.hostBlockedWorkers.get(worker);
    if (!blocked) {
      return;
    }
    if (isSupervisorTimerArmed(blocked)) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `blocked-retry-skip-supervisor-timer:${handle.pid}:status=${blocked.status}:vm=0x${blocked.vmPtr.toString(16)}`,
      );
      wakeRuntimeBridge(handle.runtimeBridge);
      pumpProcess(handle);
      return;
    }
    appendBlockedStateLifecycle(handle, blocked);
    if (blocked.rpcPending) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-retry-skip:${handle.pid}:rpc-pending`);
      return;
    }
    const thread = threadWorkerStates.get(worker);
    if (thread?.ownerAsyncStatus !== null && thread?.ownerAsyncStatus !== undefined) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `blocked-retry-skip:${handle.pid}:owner-async:${thread.ownerAsyncStatus}`,
      );
      scheduleSupervisorBlockedRetry(
        handle,
        worker,
        SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE,
      );
      return;
    }
    if (
      blocked.status === handle.kernel.abi_status_epoll_wait()
      && hasPendingCloneChildInitResume(handle)
    ) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `epoll-retry-skip:${handle.pid}:clone-child-init-pending`,
      );
      wakeRuntimeBridge(handle.runtimeBridge);
      scheduleSupervisorBlockedRetry(
        handle,
        worker,
        SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_THREAD_INIT_PENDING,
      );
      return;
    }
    const blockedPollTimeoutMs = pollWaitTimeoutMs(handle, blocked.status, blocked.vmPtr);
    const blockedPollReadyCount = blocked.status === handle.kernel.abi_status_pipe_wait()
      ? handle.kernel.thread_poll_wait_ready_count?.(blocked.vmPtr)
      : undefined;
    if (armBlockedSupervisorTimerForBlocked(handle, blocked)) {
      return;
    }
    if (parkBehindSupervisorActiveChild(
      handle,
      worker,
      blocked,
      blockedPollReadyCount,
    )) {
      return;
    }
    if (blocked.status === handle.kernel.abi_status_wait4()) {
      resumeWait4BlockedWorker(handle, worker, blocked);
      return;
    }
    const forkSharedSyncPromise = handle.kernelShare.forkSharedSyncPromise;
    if (
      forkSharedSyncPromise !== null
      && blockedPollTimeoutMs < 0
    ) {
      const staleGate = clearStaleForkSharedSyncGate(handle, forkSharedSyncPromise);
      if (staleGate !== null) {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-retry-cleared:${handle.pid}:${staleGate}`);
      } else {
        observeForkSharedSyncCompletion(handle, worker, forkSharedSyncPromise);
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `blocked-retry-skip:${handle.pid}:fork-shared-sync:${describeForkSharedSyncPromise(forkSharedSyncPromise)}`,
        );
        scheduleSupervisorBlockedRetry(
          handle,
          worker,
          SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE,
        );
        return;
      }
    }
    clearStaleRunningWorker(handle, worker);
    if (handle.runningWorker !== null) {
      if (blocked.status === handle.kernel.abi_status_epoll_wait()) {
        wakeRuntimeBridge(handle.runtimeBridge);
      }
      const runningThread = threadWorkerStates.get(handle.runningWorker);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `blocked-retry-skip:${handle.pid}:running=1`
          + `:${formatRunningRetryBlocker(handle, runningThread)}`,
      );
      scheduleSupervisorBlockedRetry(
        handle,
        worker,
        SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE,
      );
      return;
    }
    if (!canScheduleWorker(handle, worker)) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-retry-skip:${handle.pid}:cannot-schedule`);
      scheduleSupervisorBlockedRetry(
        handle,
        worker,
        SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE,
      );
      return;
    }
    if (blocked.skipNextNetworkPump) {
      blocked.skipNextNetworkPump = false;
    } else if (pumpNetworkForBlockedWait(handle, worker, blocked)) {
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
    if (!kernelWorkerOwnsPipeState(handle)) {
      drainProcessOutput(handle);
    }

    if (
      blocked.status === handle.kernel.abi_status_epoll_wait()
      && handle.kernelWorkerRequest
      && handle.pid > 0
    ) {
      const rpcGeneration = beginBlockedRpc(blocked);
      const epollRetryTimeoutMs = Number(handle.kernel.thread_epoll_wait_timeout(blocked.vmPtr));
      const epollRetryTimerfdDeadline = epollTimerfdDeadlineTicks(handle, blocked.vmPtr);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `epoll-retry-rpc:${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}`
          + `:timeout=${epollRetryTimeoutMs}`
          + `:timerfdDeadline=${epollRetryTimerfdDeadline?.toString() ?? "none"}`
          + `:epoll=${formatBlockedEpollEntries(handle, blocked.vmPtr)}`,
      );
      void (blockedEpollWatchesPipe(handle, blocked.vmPtr)
        ? requestPipeStateSync(handle, false)
        : Promise.resolve())
        .then(() => requestEpollStateSync(handle, false))
        .then(() => requestEpollDependentOfdStateSync(
          handle,
          collectBlockedEpollWatchedOfdIdsByType(
            handle,
            blocked.vmPtr,
            [
              handle.kernel.abi_fd_type_eventfd?.() ?? 9,
              handle.kernel.abi_fd_type_timerfd?.() ?? 10,
            ],
          ),
          { mode: "pull" },
        ))
        .then(() => {
          const latestBlocked = finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
          if (!latestBlocked) {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `epoll-retry-stale:${handle.pid}`);
            return;
          }
          if (Number(handle.kernel.thread_epoll_wait_timeout(latestBlocked.vmPtr)) < 0) {
            const deadlineTicks = epollTimerfdDeadlineTicks(handle, latestBlocked.vmPtr);
            if (deadlineTicks !== null) {
              deps.appendRuntimeLifecycleEvent(
                handle.runtimeId,
                `epoll-retry-deadline:${handle.pid}:vm=0x${latestBlocked.vmPtr.toString(16)}:ticks=${deadlineTicks.toString()}`,
              );
              if (!armBlockedSupervisorTimerForBlocked(handle, latestBlocked)) {
                throw new Error("kernel supervisor timer HostOp required for epoll timerfd wait");
              }
              return;
            }
            deps.appendRuntimeLifecycleEvent(
              handle.runtimeId,
              `epoll-retry-no-deadline:${handle.pid}:vm=0x${latestBlocked.vmPtr.toString(16)}`
                + `:timeout=${Number(handle.kernel.thread_epoll_wait_timeout(latestBlocked.vmPtr))}`,
            );
          }
          clearStaleRunningWorker(handle, worker);
          const runningWorkerBlocksResume = handle.runningWorker !== null;
          const canScheduleBlockedWorker = canScheduleWorker(handle, worker);
          if (runningWorkerBlocksResume || !canScheduleBlockedWorker) {
            if (runningWorkerBlocksResume) {
              wakeRuntimeBridge(handle.runtimeBridge);
            }
            const runningThread = handle.runningWorker === null
              ? undefined
              : threadWorkerStates.get(handle.runningWorker);
            deps.appendRuntimeLifecycleEvent(
              handle.runtimeId,
              `epoll-retry-skip:${handle.pid}:running=${handle.runningWorker !== null ? 1 : 0}:sched=${canScheduleBlockedWorker ? 1 : 0}`
                + `:${formatRunningRetryBlocker(handle, runningThread)}`,
            );
            scheduleSupervisorBlockedRetry(handle, worker);
            return;
          }
          resumeEpollWait(handle, worker, latestBlocked);
        })
        .catch((error) => {
          finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
          if (handle.workers.has(worker) && threadWorkerStates.has(worker)) {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          }
        });
      return;
    }

    if (blocked.status === handle.kernel.abi_status_pipe_wait()) {
      const pipeSyncBeforeRetryRequested = supervisorBlockedPipeSyncBeforeRetryRequested(
        handle.kernel,
        blocked.vmPtr,
      );
      const event = `pipe-retry:${handle.pid}:a7=${blockedOrCurrentSyscallNr(handle, blocked.vmPtr)?.toString() ?? "n/a"}:a0=${handle.kernel.thread_a0(blocked.vmPtr).toString()}:timeout=${blockedPollTimeoutMs}:ready=${blockedPollReadyCount ?? "n/a"}:${formatFirstPipeWaitDebug(handle, blocked.vmPtr)}:${formatFirstSocketWaitDebug(handle, blocked.vmPtr)}:${formatBlockedPollFdSet(handle, blocked.vmPtr)}:syncRequested=${pipeSyncBeforeRetryRequested ? 1 : 0}`;
      if (blocked.lastPipeRetryLifecycleEvent !== event) {
        blocked.lastPipeRetryLifecycleEvent = event;
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
      }
      const syncBeforeRetryAction = supervisorBlockedPipeSyncBeforeRetryAction(
        handle.kernel,
        blocked.vmPtr,
        kernelWorkerOwnsPipeState(handle) && !!handle.kernelWorkerRequest && handle.pid > 0,
      );
      if (
        syncBeforeRetryAction === SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC
        || syncBeforeRetryAction
          === SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC_AND_DRAIN_OUTPUT
      ) {
        const shouldDrainOutput = syncBeforeRetryAction
          === SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC_AND_DRAIN_OUTPUT;
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
              SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_HOST_PROGRESS,
            );
          })
          .catch((error) => {
            finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (
        syncBeforeRetryAction
          === SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_SCHEDULE_RETRY
      ) {
        scheduleSupervisorBlockedRetry(handle, worker);
        return;
      }
    }
    if (blocked.status === handle.kernel.abi_status_fs()) {
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
      return;
    }
    const retryPlan = supervisorBlockedWaitRetryPlan(handle.kernel, blocked.vmPtr);
    if (retryPlan.reason === SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PASSIVE_WAIT) {
      scheduleSupervisorBlockedRetry(handle, worker);
      return;
    }
    let resumeDelegation: ReturnType<typeof supervisorBlockedResumeDelegation>;
    try {
      resumeDelegation = supervisorBlockedResumeDelegation(
        handle.kernel,
        blocked.vmPtr,
        !!handle.kernelWorkerRequest && handle.pid > 0,
      );
    } catch (error) {
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      return;
    }

    switch (resumeDelegation) {
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER:
        resumeBlockedSyscallViaKernelWorker(
          handle,
          worker,
          blocked,
          `pipe-retry-rpc:${handle.pid}`,
          (outcome) => {
            if (
              outcome.kind === "blocked"
              && outcome.status === handle.kernel.abi_status_pipe_wait()
            ) {
              pumpProcess(handle);
            }
          }
        );
        return;
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER:
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
        return;
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED:
        deps.failProcess(
          handle,
          new Error(`kernel supervisor rejected blocked status ${blocked.status} resume delegation`),
        );
        return;
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT:
        resumeEpollWait(handle, worker, blocked);
        return;
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT:
        break;
      default: {
        const exhaustive: never = resumeDelegation;
        throw new Error(`unhandled supervisor blocked resume delegation: ${exhaustive}`);
      }
    }

    if (blocked.status === handle.kernel.abi_status_pipe_wait()) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-retry-local:${handle.pid}`);
    }
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    if (blocked.status === handle.kernel.abi_status_wait4()) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `wait4-retry-local:${handle.pid}:0x${blocked.vmPtr.toString(16)}:records=supervisor`,
      );
      const rawWait4Result = resumeBlockedSyscallBySupervisor(handle.kernel, blocked.vmPtr);
      const wait4Result = rawWait4Result ?? 0n;
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `wait4-retry-result:${handle.pid}:result=${wait4Result.toString()}:status=${handle.kernel.thread_status(blocked.vmPtr)}:a0=${handle.kernel.thread_a0?.(blocked.vmPtr)?.toString() ?? "n/a"}:records=supervisor`,
      );
    } else {
      resumeBlockedSyscallBySupervisor(handle.kernel, blocked.vmPtr);
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    handleHostBlockedRetryResult(handle, worker, blocked.vmPtr, handle.kernel.thread_status(blocked.vmPtr));
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
