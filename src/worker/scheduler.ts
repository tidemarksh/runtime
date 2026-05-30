import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../abi.js";
import {
  wakeRuntimeBridge,
} from "../bridge/runtime-state.js";
import {
  releaseWorker,
} from "./pool.js";
import {
  drainHiddenProcessOutput,
  drainProcessOutput,
  flushStdin,
} from "./stdio.js";
import { kernelWorkerOwnsPipeState } from "./ownership/pipe-control.js";
import { describeForkSharedSyncPromise } from "./fork-shared-sync-debug.js";
import { collectLiveThreadVmPtrs, processHasLiveThreads } from "./live-threads.js";
import { appendWorkerDebugLog } from "./scheduler/fairness.js";
import { createFutexScheduler } from "./scheduler/futex.js";
import { createPipePollScheduler } from "./scheduler/pipe-poll.js";
import { createPruningScheduler } from "./scheduler/pruning.js";
import { createWorkerSchedulerQueueHelpers } from "./scheduler/queue.js";
import { createRunLoopScheduler, logNetworkDebug } from "./scheduler/run-loop.js";
import { createSleepScheduler } from "./scheduler/sleep.js";
import { createSocketWaitScheduler } from "./scheduler/socket-wait.js";
import { createWorkerSchedulerStateSyncHelpers } from "./scheduler/state-sync.js";
import { createVforkExclusiveScheduler } from "./scheduler/vfork-exclusive.js";
import { createWait4Scheduler } from "./scheduler/wait4.js";
import type {
  BlockedWorkerState,
  ProcessHandle,
} from "./types.js";
import type {
  WorkerScheduler,
  WorkerSchedulerDeps,
} from "./scheduler/types.js";
import type { KernelRpcResumeOutcome } from "./kernel-rpc/outcomes.js";
export function createWorkerScheduler(deps: WorkerSchedulerDeps): WorkerScheduler {
  const { threadWorkerStates } = deps;
  const pipeChildExitResumeKeys = new WeakMap<ProcessHandle, Map<number, string>>();

  function consumeKernelRpcResumeOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    outcome: KernelRpcResumeOutcome,
  ): void {
    switch (outcome.kind) {
      case "completed":
      case "queued-in-worker":
      case "process-gone":
      case "stale":
        return;
      case "blocked": {
        const blocked = handle.blockedWorkers.get(worker);
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
  const appliedChildExitRecordKeys = new Map<string, Set<string>>();
  const schedulerQueueHelpers = createWorkerSchedulerQueueHelpers({
    threadWorkerStates,
    appendWorkerDebugLog,
    blockWorker,
    failProcess: deps.failProcess,
    handleWorkerMessage: deps.handleWorkerMessage,
  });

  const {
    attachThreadWorker,
    enqueueWorker,
    pumpProcess,
  } = schedulerQueueHelpers;

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

  const {
    beginBlockedRpc,
    clearStaleRunningWorker,
    finishBlockedRpc,
    observeForkSharedSyncCompletion,
    scheduleBlockedRetry,
  } = createRunLoopScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    retryBlockedWorker,
  });
  const {
    canScheduleWorker,
  } = createVforkExclusiveScheduler(threadWorkerStates);
  const {
    advanceGuestTimeByMs,
    maybeAdvanceGuestTimeToNextSleep,
    monotonicTicksCurrent,
    sleepDeadlineTicks,
    sleepRetryDelayMs,
    wakeBlockedSleepWorker,
    wakeDueSleepWorkers,
  } = createSleepScheduler({
    clearBlockedWorker,
    enqueueWorker,
  });
  const {
    blockedOrCurrentSyscallNr,
    formatBlockedPollFdSet,
    formatFirstPipeWaitDebug,
    pollWaitTimeoutMs,
    pipeWaitShouldParkBehindActiveChild,
    pipeWaitShouldRemainPassive,
    resumePipePollTimeoutLocally,
    schedulePipePollTimeoutRetry,
  } = createPipePollScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    advanceGuestTimeByMs,
    handleBlockedRetryResult,
    scheduleBlockedRetry,
    pipeChildExitRecordsAlreadyResumedForWait,
    hasActiveChildProcess: deps.hasActiveChildProcess,
  });
  const {
    formatFirstSocketWaitDebug,
    networkInboundAvailable,
    socketWaitTimeoutMs,
    blockedWaitShouldPumpNetwork,
    scheduleSocketBlockedRetry,
  } = createSocketWaitScheduler({
    appendWorkerDebugLog,
    logNetworkDebug,
    scheduleBlockedRetry,
  });
  const {
    pruneDetachedProcessWorkers,
    pruneStaleProcessWorkers,
    selectProcessAnchorVmPtr,
  } = createPruningScheduler({
    threadWorkerStates,
    appendWorkerDebugLog,
    clearBlockedWorker,
  });
  const {
    applyLocalChildExitRecords,
    childExitRecordsAlreadyApplied,
    formatChildExitRecords,
    markChildExitRecordsApplied,
  } = createWait4Scheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
  }, appliedChildExitRecordKeys);
  const {
    applyChildExitRecordsForWorkerLocalWaits,
  } = createFutexScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    formatChildExitRecords,
    childExitRecordsAlreadyApplied,
    applyLocalChildExitRecords,
  });

  function pipeChildExitRecordsResumeKeyForWait(handle: ProcessHandle, vmPtr: number): string {
    const childExitRecordsKey = formatChildExitRecords(handle);
    return childExitRecordsKey === ""
      ? ""
      : formatPipeChildExitResumeKey(handle, vmPtr, childExitRecordsKey);
  }

  function pipeChildExitRecordsAlreadyResumedForWait(
    handle: ProcessHandle,
    vmPtr: number,
    pipeChildExitResumeKey: string | null = pipeChildExitResumeKeyFor(handle, vmPtr),
  ): boolean {
    const childExitResumeKey = pipeChildExitRecordsResumeKeyForWait(handle, vmPtr);
    return childExitResumeKey === "" || pipeChildExitResumeKey === childExitResumeKey;
  }

  function formatPipeChildExitResumeKey(
    handle: ProcessHandle,
    vmPtr: number,
    recordsKey = formatChildExitRecords(handle),
  ): string {
    const syscall = blockedOrCurrentSyscallNr(handle, vmPtr);
    const blockedFd = handle.kernel.thread_blocked_fd?.(vmPtr);
    const blockedOfd = handle.kernel.thread_blocked_ofd_id?.(vmPtr);
    return [
      recordsKey,
      `sys=${syscall?.toString() ?? "n/a"}`,
      `bfd=${blockedFd ?? "n/a"}`,
      `ofd=${blockedOfd ?? "n/a"}`,
    ].join(":");
  }

  function pipeChildExitResumeKeyFor(handle: ProcessHandle, vmPtr: number): string | null {
    return pipeChildExitResumeKeys.get(handle)?.get(vmPtr) ?? null;
  }

  function setPipeChildExitResumeKey(handle: ProcessHandle, vmPtr: number, key: string): void {
    let keys = pipeChildExitResumeKeys.get(handle);
    if (keys === undefined) {
      keys = new Map();
      pipeChildExitResumeKeys.set(handle, keys);
    }
    keys.set(vmPtr, key);
  }

  function markPipeChildExitRecordsResumedForWait(
    handle: ProcessHandle,
    blocked: BlockedWorkerState,
  ): void {
    const recordsKey = formatChildExitRecords(handle);
    if (recordsKey === "") {
      return;
    }
    const resumeKey = formatPipeChildExitResumeKey(handle, blocked.vmPtr, recordsKey);
    blocked.pipeChildExitResumeKey = resumeKey;
    setPipeChildExitResumeKey(handle, blocked.vmPtr, resumeKey);
  }

  function formatBlockedFdEntries(handle: ProcessHandle, vmPtr: number, limit = 32): string {
    const entries: string[] = [];
    const maxFds = handle.kernel.abi_max_fds();
    for (let fd = 0; fd < Math.min(limit, maxFds); fd += 1) {
      const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
      if (ofdId === 0) {
        continue;
      }
      const hasValidOfd = ofdId > 0 && ofdId < maxFds;
      const type = hasValidOfd ? handle.kernel.kernel_ofd_fd_type(ofdId) : -1;
      const ino = hasValidOfd ? handle.kernel.kernel_ofd_ino(ofdId) : -1;
      const hostFd = hasValidOfd ? handle.kernel.kernel_ofd_host_fd(ofdId) : -1;
      const offset = hasValidOfd && handle.kernel.kernel_ofd_offset
        ? handle.kernel.kernel_ofd_offset(ofdId)
        : -1n;
      entries.push(`${fd}->${ofdId}/${type}/${ino}/${hostFd}/${offset.toString()}`);
    }
    return entries.join(" ") || "none";
  }

  function appendBlockedStateLifecycle(handle: ProcessHandle, blocked: BlockedWorkerState): void {
    const status = blocked.status;
    if (status !== handle.kernel.abi_status_pipe_wait() && status !== handle.kernel.abi_status_wait4()) {
      return;
    }
    if (handle.pid === 1) {
      return;
    }
    const parts = [
      `blocked-state:${handle.pid}:0x${blocked.vmPtr.toString(16)}:status=${status}`,
      `fds=${formatBlockedFdEntries(handle, blocked.vmPtr)}`,
      `records=${[...handle.childExitSyncRecords].map(([childPid, waitStatus]) => `${childPid}/${waitStatus}`).join(".") || "none"}`,
      `pending=${handle.pendingChildExitRecords}/${handle.pendingChildSpawnRecords}`,
      `active=${deps.hasActiveChildProcess(handle.pid) ? 1 : 0}`,
    ];
    if (status === handle.kernel.abi_status_pipe_wait()) {
      parts.push(
        `ofd=${handle.kernel.thread_blocked_ofd_id(blocked.vmPtr)}`,
        `ready=${handle.kernel.thread_poll_wait_ready_count?.(blocked.vmPtr) ?? -1}`,
        `pipeRevents=${handle.kernel.thread_poll_wait_first_pipe_revents?.(blocked.vmPtr) ?? -1}`,
        formatBlockedPollFdSet(handle, blocked.vmPtr),
        `eofPending=${handle.kernel.thread_pipe_read_eof_sync_pending?.(blocked.vmPtr) ?? -1}`,
        `hupPending=${handle.kernel.thread_poll_hup_sync_pending?.(blocked.vmPtr) ?? -1}`,
        `buf=${handle.kernel.thread_blocked_pipe_buffered_len?.(blocked.vmPtr) ?? -1}`,
        `writeOpen=${handle.kernel.thread_blocked_pipe_write_open?.(blocked.vmPtr) ?? -1}`,
        `readReady=${handle.kernel.thread_blocked_pipe_read_ready?.(blocked.vmPtr) ?? -1}`,
      );
    }
    const event = parts.join(":");
    if (status === handle.kernel.abi_status_pipe_wait()) {
      if (blocked.lastPipeRetryLifecycleEvent === event) {
        return;
      }
      blocked.lastPipeRetryLifecycleEvent = event;
    } else {
      if (blocked.lastWait4RetryLifecycleEvent === event) {
        return;
      }
      blocked.lastWait4RetryLifecycleEvent = event;
    }
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
  }

  function pumpNetworkForBlockedWait(
    handle: ProcessHandle,
    worker: Worker,
    blocked: BlockedWorkerState,
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
            scheduleBlockedRetry(handle, worker, 1, latestBlocked.timeoutExpired);
            return;
          }
          attachKernelRuntimeState(handle.kernel, handle.kernelState);
          handle.kernel.thread_resume_blocked_syscall(latestBlocked.vmPtr);
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          handleBlockedRetryResult(
            handle,
            worker,
            latestBlocked.vmPtr,
            handle.kernel.thread_status(latestBlocked.vmPtr),
          );
          return;
        }
        scheduleBlockedRetry(handle, worker, changed ? 1 : 0, latestBlocked.timeoutExpired);
      })
      .catch((error) => {
        finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    return true;
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
      rpcGeneration: 0,
      skipNextNetworkPump: false,
      pipeSyncedBeforePassive: false,
      pipeChildExitResumeKey: null,
      pipeEofChildExitDeferrals: 0,
    };
    handle.blockedWorkers.set(worker, blocked);
    if (status === handle.kernel.abi_status_sleep()) {
      blocked.sleepDeadlineTicks = sleepDeadlineTicks(handle, vmPtr);
      scheduleBlockedRetry(handle, worker, sleepRetryDelayMs(handle, blocked.sleepDeadlineTicks), true);
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
    const pollTimeoutMs = pollWaitTimeoutMs(handle, status, vmPtr);
    const pollReadyCount = status === handle.kernel.abi_status_pipe_wait()
      ? handle.kernel.thread_poll_wait_ready_count?.(vmPtr)
      : undefined;
    if (pollReadyCount !== undefined && pollReadyCount > 0) {
      blocked.timeoutDeadlineMs = null;
      scheduleBlockedRetry(handle, worker, 0);
      return;
    }
    if (
      (pollTimeoutMs >= 0 || blocked.timeoutDeadlineMs !== null)
      && (pollReadyCount === undefined || pollReadyCount <= 0)
    ) {
      if (
        status === handle.kernel.abi_status_pipe_wait()
        && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
      ) {
        schedulePipePollTimeoutRetry(handle, worker, blocked, pollTimeoutMs);
      } else {
        scheduleSocketBlockedRetry(handle, worker, blocked, pollTimeoutMs);
      }
      return;
    }
    const socketTimeoutMs = socketWaitTimeoutMs(handle, status, vmPtr);
    if (socketTimeoutMs >= 0) {
      scheduleSocketBlockedRetry(handle, worker, blocked, socketTimeoutMs);
      return;
    }
    if (status === handle.kernel.abi_status_wait4()) {
      retryBlockedWorker(handle, worker);
      return;
    }
    if (
      status === handle.kernel.abi_status_pipe_wait()
      && kernelWorkerOwnsPipeState(handle)
      && pollTimeoutMs < 0
      && (pollReadyCount === undefined || pollReadyCount <= 0)
      && handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr) !== 1
      && handle.kernel.thread_blocked_pipe_write_ready?.(vmPtr) !== 1
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
    ) {
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
      if (blocked.status === handle.kernel.abi_status_wait4()) {
        const reapedPid = Number(handle.kernel.thread_a0?.(vmPtr) ?? 0n);
        if (reapedPid > 0) {
          handle.childExitSyncRecords.delete(reapedPid);
        }
      }
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
      const currentPid = handle.kernel.process_current_pid(vmPtr);
      const faultStateLooksStale = currentPid === 0 || currentPid > 0xffff;
      const remainingThreads = faultStateLooksStale ? 0 : handle.kernel.thread_process_thread_count(vmPtr);
      const hasLiveThreads = !faultStateLooksStale
        && remainingThreads !== 0
        && processHasLiveThreads(handle.kernel, vmPtr, vmPtr);
      appendWorkerDebugLog(handle, `fault:0x${vmPtr.toString(16)} remaining=${remainingThreads}`);
      if (handle.runningWorker === worker) {
        handle.runningWorker = null;
      }
      handle.workers.delete(worker);
      threadWorkerStates.delete(worker);
      releaseWorker(worker);
      if (remainingThreads <= 1 || !hasLiveThreads) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `complete-exit-request:scheduler-fault:${handle.pid}:vm=0x${vmPtr.toString(16)}`
            + `:status=${status}:exit=${exitCode}:${termSignal}`
            + `:current=${handle.kernel.thread_status?.(vmPtr) ?? -1}:remaining=${remainingThreads}:live=${hasLiveThreads ? 1 : 0}`,
        );
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
    const pollTimeoutMs = pollWaitTimeoutMs(handle, status, vmPtr);
    const pollReadyCount = status === handle.kernel.abi_status_pipe_wait()
      ? handle.kernel.thread_poll_wait_ready_count?.(vmPtr)
      : undefined;
    if (pollReadyCount !== undefined && pollReadyCount > 0) {
      blocked.timeoutDeadlineMs = null;
      scheduleBlockedRetry(handle, worker, 0);
      return;
    }
    if (
      (pollTimeoutMs >= 0 || blocked.timeoutDeadlineMs !== null)
      && (pollReadyCount === undefined || pollReadyCount <= 0)
    ) {
      scheduleSocketBlockedRetry(handle, worker, blocked, pollTimeoutMs);
      return;
    }
    if (pipeWaitShouldRemainPassive(
      handle,
      status,
      vmPtr,
      pollTimeoutMs,
      blocked.timeoutDeadlineMs,
      pollReadyCount,
      pipeChildExitResumeKeyFor(handle, vmPtr),
    )) {
      scheduleBlockedRetry(handle, worker, 25);
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
        if (!blocked.timeoutExpired) {
          scheduleBlockedRetry(handle, worker, sleepRetryDelayMs(handle, blocked.sleepDeadlineTicks), true);
          return;
        }
        handle.kernel.sync_monotonic_ticks_floor?.(blocked.sleepDeadlineTicks);
      }
      wakeBlockedSleepWorker(handle, worker, blocked);
      return;
    }
    appendBlockedStateLifecycle(handle, blocked);
    if (blocked.rpcPending) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-retry-skip:${handle.pid}:rpc-pending`);
      return;
    }
    const thread = threadWorkerStates.get(worker);
    if (thread?.ownerAsyncStatus !== null && thread?.ownerAsyncStatus !== undefined) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `blocked-retry-skip:${handle.pid}:owner-async:${thread.ownerAsyncStatus}`,
      );
      scheduleBlockedRetry(
        handle,
        worker,
        blocked.status === handle.kernel.abi_status_wait4() ? 10 : 1,
        blocked.timeoutExpired,
      );
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
    const blockedPollTimeoutMs = pollWaitTimeoutMs(handle, blocked.status, blocked.vmPtr);
    if (
      blocked.status === handle.kernel.abi_status_pipe_wait()
      || blocked.status === handle.kernel.abi_status_wait4()
    ) {
      deps.pumpActiveChildProcesses?.(handle.pid);
    }
    const blockedPollReadyCount = blocked.status === handle.kernel.abi_status_pipe_wait()
      ? handle.kernel.thread_poll_wait_ready_count?.(blocked.vmPtr)
      : undefined;
    if (pipeWaitShouldParkBehindActiveChild(
      handle,
      blocked,
      blockedPollTimeoutMs,
      blockedPollReadyCount,
    )) {
      const event = `pipe-retry-skip:${handle.pid}:active-child:ready=${blockedPollReadyCount ?? "n/a"}:${formatFirstPipeWaitDebug(handle, blocked.vmPtr)}:${formatBlockedPollFdSet(handle, blocked.vmPtr)}`;
      if (blocked.lastPipeRetryLifecycleEvent !== event) {
        blocked.lastPipeRetryLifecycleEvent = event;
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
      }
      scheduleBlockedRetry(handle, worker, 5);
      return;
    }
    const forkSharedSyncPromise = handle.kernelShare.forkSharedSyncPromise;
    const wait4HasLocalExitRecord = blocked.status === handle.kernel.abi_status_wait4()
      && handle.childExitSyncRecords.size > 0;
    const pipeHasLocalExitRecord = blocked.status === handle.kernel.abi_status_pipe_wait()
      && handle.childExitSyncRecords.size > 0;
    if (
      forkSharedSyncPromise !== null
      && blockedPollTimeoutMs < 0
      && !wait4HasLocalExitRecord
      && !pipeHasLocalExitRecord
    ) {
      observeForkSharedSyncCompletion(handle, worker, forkSharedSyncPromise);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `wait4-retry-skip:${handle.pid}:fork-shared-sync:${describeForkSharedSyncPromise(forkSharedSyncPromise)}`,
      );
      scheduleBlockedRetry(
        handle,
        worker,
        blocked.status === handle.kernel.abi_status_wait4() ? 10 : 1,
        blocked.timeoutExpired,
      );
      return;
    }
    clearStaleRunningWorker(handle, worker);
    if (handle.runningWorker !== null) {
      if (wait4HasLocalExitRecord || pipeHasLocalExitRecord) {
        wakeRuntimeBridge(handle.runtimeBridge);
      }
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
        || (handle.childExitSyncRecords.size === 0 && deps.hasActiveChildProcess(handle.pid))
      )
    ) {
      const event = `wait4-retry-skip:${handle.pid}:gate:exit=${handle.pendingChildExitRecords}:spawn=${handle.pendingChildSpawnRecords}:active=${deps.hasActiveChildProcess(handle.pid) ? 1 : 0}`;
      if (blocked.lastWait4RetryLifecycleEvent !== event) {
        blocked.lastWait4RetryLifecycleEvent = event;
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
      }
      scheduleBlockedRetry(handle, worker, 50);
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
      void requestEpollDependentOfdStateSync(handle)
        .then(() => requestEpollStateSync(handle, false))
        .then(() => {
          const latestBlocked = finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
          if (!latestBlocked) {
            return;
          }
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
          finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
          if (handle.workers.has(worker) && threadWorkerStates.has(worker)) {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          }
        });
      return;
    }

    const pipeWaitChildExitRecordsKey = formatChildExitRecords(handle);
    if (
      blocked.status === handle.kernel.abi_status_pipe_wait()
      && pipeWaitChildExitRecordsKey !== ""
      && !childExitRecordsAlreadyApplied(handle, pipeWaitChildExitRecordsKey)
      && !pipeChildExitRecordsAlreadyResumedForWait(
        handle,
        blocked.vmPtr,
        blocked.pipeChildExitResumeKey,
      )
    ) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `pipe-retry-apply-child-records:${handle.pid}:${pipeWaitChildExitRecordsKey}`,
      );
      applyLocalChildExitRecords(handle);
      markChildExitRecordsApplied(handle, pipeWaitChildExitRecordsKey);
    }
    const pipeEofSyncPending = blocked.status === handle.kernel.abi_status_pipe_wait()
      && handle.kernel.thread_pipe_read_eof_sync_pending?.(blocked.vmPtr) === 1;
    const pipePollHupSyncPending = blocked.status === handle.kernel.abi_status_pipe_wait()
      && handle.kernel.thread_poll_hup_sync_pending?.(blocked.vmPtr) === 1;
    const pipeReadinessSyncPending = pipeEofSyncPending || pipePollHupSyncPending;
    if (
      blocked.status === handle.kernel.abi_status_pipe_wait()
      && blockedPollTimeoutMs >= 0
      && !blocked.timeoutExpired
      && (blockedPollReadyCount === undefined || blockedPollReadyCount === 0)
      && !pipeReadinessSyncPending
      && handle.kernel.thread_poll_wait_has_socket?.(blocked.vmPtr) !== 1
      && networkInboundAvailable(handle, blocked.vmPtr)
    ) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-retry-network-timeout:${handle.pid}`);
      blocked.timeoutExpired = true;
    }
    if (blocked.status === handle.kernel.abi_status_pipe_wait()) {
      const event = `pipe-retry:${handle.pid}:a7=${blockedOrCurrentSyscallNr(handle, blocked.vmPtr)?.toString() ?? "n/a"}:a0=${handle.kernel.thread_a0(blocked.vmPtr).toString()}:timeout=${blockedPollTimeoutMs}:expired=${blocked.timeoutExpired ? 1 : 0}:ready=${blockedPollReadyCount ?? "n/a"}:${formatFirstPipeWaitDebug(handle, blocked.vmPtr)}:${formatFirstSocketWaitDebug(handle, blocked.vmPtr)}:${formatBlockedPollFdSet(handle, blocked.vmPtr)}:synced=${blocked.pipeSyncedBeforePassive ? 1 : 0}`;
      if (blocked.lastPipeRetryLifecycleEvent !== event) {
        blocked.lastPipeRetryLifecycleEvent = event;
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
      }
    }
    if (
      blocked.status === handle.kernel.abi_status_pipe_wait()
      && blockedPollTimeoutMs >= 0
      && !blocked.timeoutExpired
      && (blockedPollReadyCount === undefined || blockedPollReadyCount === 0)
      && !pipeReadinessSyncPending
    ) {
      if (
        !blocked.pipeSyncedBeforePassive
        && kernelWorkerOwnsPipeState(handle)
        && handle.kernelWorkerRequest
        && handle.pid > 0
      ) {
        blocked.pipeSyncedBeforePassive = true;
        const rpcGeneration = beginBlockedRpc(blocked);
        void requestPipeStateSync(handle, false)
          .then(() => {
            const latestBlocked = finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
            if (!latestBlocked) {
              return;
            }
            if (!kernelWorkerOwnsPipeState(handle)) {
              drainProcessOutput(handle);
            }
            scheduleBlockedRetry(handle, worker, 0, latestBlocked.timeoutExpired);
          })
          .catch((error) => {
            finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (handle.kernel.thread_poll_wait_has_socket?.(blocked.vmPtr) !== 1) {
        schedulePipePollTimeoutRetry(
          handle,
          worker,
          blocked,
          blockedPollTimeoutMs,
        );
        return;
      }
      if (
        !blocked.pipeSyncedBeforePassive
        && !deps.hasActiveChildProcess(handle.pid)
        && kernelWorkerOwnsPipeState(handle)
        && handle.kernelWorkerRequest
        && handle.pid > 0
      ) {
        blocked.pipeSyncedBeforePassive = true;
        const rpcGeneration = beginBlockedRpc(blocked);
        void requestPipeStateSync(handle, false)
          .then(() => {
            const latestBlocked = finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
            if (!latestBlocked) {
              return;
            }
            drainProcessOutput(handle);
            scheduleBlockedRetry(handle, worker, 0, latestBlocked.timeoutExpired);
          })
          .catch((error) => {
            finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      scheduleSocketBlockedRetry(
        handle,
        worker,
        blocked,
        blockedPollTimeoutMs,
      );
      return;
    }
    if (
      blocked.status === handle.kernel.abi_status_pipe_wait()
      && blocked.timeoutExpired
      && blockedPollTimeoutMs >= 0
      && deps.hasActiveChildProcess(handle.pid)
      && handle.kernel.thread_resume_poll_wait_timeout
      && !pipeReadinessSyncPending
    ) {
      resumePipePollTimeoutLocally(handle, worker, blocked, blockedPollTimeoutMs);
      return;
    }
    if (
      blocked.status === handle.kernel.abi_status_pipe_wait()
      && blocked.timeoutExpired
      && blockedPollTimeoutMs >= 0
      && !blocked.pipeSyncedBeforePassive
      && kernelWorkerOwnsPipeState(handle)
      && handle.kernelWorkerRequest
      && handle.pid > 0
      && handle.kernel.thread_resume_poll_wait_timeout
      && !pipeReadinessSyncPending
    ) {
      blocked.pipeSyncedBeforePassive = true;
      void requestPipeStateSync(handle, false)
        .then(() => {
          const latestBlocked = handle.blockedWorkers.get(worker);
          if (!latestBlocked || latestBlocked.vmPtr !== blocked.vmPtr) {
            return;
          }
          drainProcessOutput(handle);
          resumePipePollTimeoutLocally(handle, worker, latestBlocked, blockedPollTimeoutMs);
        })
        .catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }
    if (
      blocked.status === handle.kernel.abi_status_pipe_wait()
      && !blocked.pipeSyncedBeforePassive
      && !deps.hasActiveChildProcess(handle.pid)
      && kernelWorkerOwnsPipeState(handle)
      && handle.kernelWorkerRequest
      && handle.pid > 0
    ) {
      blocked.pipeSyncedBeforePassive = true;
      const rpcGeneration = beginBlockedRpc(blocked);
      void requestPipeStateSync(handle, false)
        .then(() => {
          const latestBlocked = finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
          if (!latestBlocked) {
            return;
          }
          drainProcessOutput(handle);
          scheduleBlockedRetry(handle, worker, 0, latestBlocked.timeoutExpired);
        })
        .catch((error) => {
          finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }
    if (blocked.status === handle.kernel.abi_status_fs()) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `fs-retry:${handle.pid}`);
      const thread = threadWorkerStates.get(worker);
      if (thread?.ownerAsyncStatus !== null && thread?.ownerAsyncStatus !== undefined) {
        scheduleBlockedRetry(handle, worker, 1, blocked.timeoutExpired);
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
        })
        .catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }
    if (
      blocked.timeoutExpired
      && blocked.status === handle.kernel.abi_status_pipe_wait()
      && blockedPollTimeoutMs >= 0
      && handle.kernel.thread_resume_poll_wait_timeout
      && !pipeReadinessSyncPending
    ) {
      resumePipePollTimeoutLocally(handle, worker, blocked, blockedPollTimeoutMs);
      return;
    }
    if (pipeWaitShouldRemainPassive(
      handle,
      blocked.status,
      blocked.vmPtr,
      blockedPollTimeoutMs,
      blocked.timeoutDeadlineMs,
      blockedPollReadyCount,
      pipeChildExitResumeKeyFor(handle, blocked.vmPtr),
    )) {
      scheduleBlockedRetry(handle, worker, 25, blocked.timeoutExpired);
      return;
    }
    if (
      blocked.status === handle.kernel.abi_status_pipe_wait()
      && blockedPollReadyCount !== undefined
      && blockedPollReadyCount > 0
    ) {
      blocked.timeoutDeadlineMs = null;
    }

    const blockedSocketTimeoutMs = socketWaitTimeoutMs(handle, blocked.status, blocked.vmPtr);
    if (
      (blockedSocketTimeoutMs >= 0 || blocked.timeoutDeadlineMs !== null)
      && !blocked.timeoutExpired
    ) {
      scheduleSocketBlockedRetry(handle, worker, blocked, blockedSocketTimeoutMs);
      return;
    }

    if (
      blocked.timeoutExpired
      && blocked.status === handle.kernel.abi_status_pipe_wait()
      && blockedPollTimeoutMs >= 0
      && handle.kernel.thread_resume_poll_wait_timeout
      && !pipeReadinessSyncPending
    ) {
      resumePipePollTimeoutLocally(handle, worker, blocked, blockedPollTimeoutMs);
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
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-retry-rpc:${handle.pid}`);
      if (
        blocked.status === handle.kernel.abi_status_pipe_wait()
        && handle.childExitSyncRecords.size > 0
      ) {
        markPipeChildExitRecordsResumedForWait(handle, blocked);
      }
      const resumeBlockedRpc = deps.resumeBlockedSyscallOutcome ?? deps.resumeBlockedSyscallLocally;
      void resumeBlockedRpc(
        handle,
        worker,
        blocked.vmPtr,
        blocked.status,
        blocked.timeoutExpired,
      ).then((outcome) => {
        if (outcome !== undefined) {
          consumeKernelRpcResumeOutcome(handle, worker, blocked.vmPtr, outcome);
        }
      }).catch((error) => {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
      return;
    }

    if (blocked.status === handle.kernel.abi_status_pipe_wait()) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-retry-local:${handle.pid}`);
    }
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    if (blocked.status === handle.kernel.abi_status_epoll_wait() && blocked.timeoutExpired) {
      handle.kernel.thread_resume_epoll_wait_timeout(blocked.vmPtr);
      blocked.timeoutExpired = false;
    } else if (
      blocked.timeoutExpired
      && blocked.status === handle.kernel.abi_status_pipe_wait()
      && handle.kernel.thread_resume_poll_wait_timeout
      && !pipeReadinessSyncPending
    ) {
      resumePipePollTimeoutLocally(handle, worker, blocked, blockedPollTimeoutMs);
      return;
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
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `wait4-retry-local:${handle.pid}:0x${blocked.vmPtr.toString(16)}:records=${[...handle.childExitSyncRecords].map(([childPid, waitStatus]) => `${childPid}/${waitStatus}`).join(".") || "none"}`,
      );
      applyLocalChildExitRecords(handle);
      const rawWait4Result = handle.kernel.thread_resume_wait4(blocked.vmPtr);
      const wait4Result = rawWait4Result ?? 0n;
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `wait4-retry-result:${handle.pid}:result=${wait4Result.toString()}:status=${handle.kernel.thread_status(blocked.vmPtr)}:a0=${handle.kernel.thread_a0?.(blocked.vmPtr)?.toString() ?? "n/a"}:records=${[...handle.childExitSyncRecords].map(([childPid, waitStatus]) => `${childPid}/${waitStatus}`).join(".") || "none"}`,
      );
      if (wait4Result > 0n) {
        handle.childExitSyncRecords.delete(Number(wait4Result));
      }
    } else {
      if (
        blocked.status === handle.kernel.abi_status_pipe_wait()
        && handle.childExitSyncRecords.size > 0
      ) {
        markPipeChildExitRecordsResumedForWait(handle, blocked);
      }
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
    applyChildExitRecordsForWorkerLocalWaits(handle);
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
