import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../abi.js";
import {
  releaseWorker,
} from "./pool.js";
import {
  drainHiddenProcessOutput,
  drainProcessOutput,
  flushStdin,
} from "./stdio.js";
import { kernelWorkerOwnsPipeState } from "./kernel-share.js";
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

const WORKER_DEBUG_LOG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_WORKER_DEBUG_LOG ?? "") === "1";
function logNetworkDebug(_event: string): void {
  // Runtime diagnostics are kept in the per-process worker debug buffer.
}
const GUEST_TICKS_PER_SEC = 100_000_000n;
const GUEST_NSEC_PER_SEC = 1_000_000_000n;
const PIPE_SLOT_BYTES = 32 + 65_536;
const PIPE_OFF_HEAD = 0;
const PIPE_OFF_TAIL = 4;

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

  function advanceGuestTimeByMs(handle: ProcessHandle, timeoutMs: number): void {
    if (timeoutMs <= 0 || !Number.isFinite(timeoutMs)) {
      return;
    }
    const deltaTicks = BigInt(Math.ceil(timeoutMs)) * GUEST_TICKS_PER_SEC / 1000n;
    handle.kernel.sync_monotonic_ticks_floor?.(monotonicTicksCurrent(handle) + deltaTicks);
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

  function pollWaitTimeoutMs(handle: ProcessHandle, status: number, vmPtr: number): number {
    if (status !== handle.kernel.abi_status_pipe_wait()) {
      return -1;
    }
    const syscallNr = handle.kernel.thread_a7?.(vmPtr);
    if (syscallNr === undefined) {
      return -1;
    }
    const syscall = BigInt(syscallNr);
    const ppoll = handle.kernel.abi_sys_ppoll?.();
    const pselect6 = handle.kernel.abi_sys_pselect6?.();
    if (
      (ppoll === undefined || syscall !== BigInt(ppoll))
      && (pselect6 === undefined || syscall !== BigInt(pselect6))
    ) {
      return -1;
    }
    const timeout = handle.kernel.thread_poll_wait_timeout?.(vmPtr);
    return timeout === undefined ? -1 : Number(timeout);
  }

  function pipeWaitShouldRemainPassive(
    handle: ProcessHandle,
    status: number,
    vmPtr: number,
    pollTimeoutMs: number,
    timeoutDeadlineMs: number | null,
    readyCount: number | undefined,
  ): boolean {
    const syscallNr = handle.kernel.thread_a7?.(vmPtr);
    if (syscallNr === undefined) {
      return false;
    }
    const syscall = BigInt(syscallNr);
    const read = handle.kernel.abi_sys_read?.();
    const readv = handle.kernel.abi_sys_readv?.();
    const ppoll = handle.kernel.abi_sys_ppoll?.();
    const pselect6 = handle.kernel.abi_sys_pselect6?.();
    const canPark = (
      (read !== undefined && syscall === BigInt(read))
      || (readv !== undefined && syscall === BigInt(readv))
      || (ppoll !== undefined && syscall === BigInt(ppoll))
      || (pselect6 !== undefined && syscall === BigInt(pselect6))
    );
    const blockedReadIsReady = (
      ((read !== undefined && syscall === BigInt(read)) || (readv !== undefined && syscall === BigInt(readv)))
      && handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr) === 1
    );
    const blockedPollPipeIsReady = blockedPollPipeHasBufferedData(handle, vmPtr);
    return status === handle.kernel.abi_status_pipe_wait()
      && canPark
      && !blockedReadIsReady
      && !blockedPollPipeIsReady
      && pollTimeoutMs < 0
      && timeoutDeadlineMs === null
      && (readyCount === undefined || readyCount <= 0)
      && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) !== 1;
  }

  function blockedPollPipeHasBufferedData(handle: ProcessHandle, vmPtr: number): boolean {
    const packed = handle.kernel.thread_poll_wait_first_pipe_revents?.(vmPtr) ?? 0;
    const fd = packed & 0xff;
    const interest = (packed >>> 8) & 0xffff;
    const revents = (packed >>> 24) & 0xff;
    if (revents !== 0) {
      return true;
    }
    if ((interest & 0x001) === 0) {
      return false;
    }
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId === 0
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      return false;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
      return false;
    }
    const slotBase = handle.kernelState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
    const view = new DataView(handle.memory.buffer);
    const head = view.getUint32(slotBase + PIPE_OFF_HEAD, true);
    const tail = view.getUint32(slotBase + PIPE_OFF_TAIL, true);
    return head !== tail;
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

  function blockedWaitMayDependOnNetwork(handle: ProcessHandle, status: number, vmPtr: number): boolean {
    const pollWaitHasSocket = pollWaitTimeoutMs(handle, status, vmPtr) >= 0
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) === 1;
    const readyCount = handle.kernel.thread_poll_wait_ready_count?.(vmPtr);
    const socketRevents = handle.kernel.thread_poll_wait_first_socket_revents?.(vmPtr);
    appendWorkerDebugLog(
      handle,
      `net-wait:${handle.pid}:status=${status}:poll=${pollWaitTimeoutMs(handle, status, vmPtr)}:hasSocket=${pollWaitHasSocket ? 1 : 0}:ready=${readyCount ?? "n/a"}:sock=0x${(socketRevents ?? 0).toString(16)}:conns=${handle.networkConnections.size}`,
    );
    logNetworkDebug(`wait pid=${handle.pid} status=${status} poll=${pollWaitTimeoutMs(handle, status, vmPtr)} hasSocket=${pollWaitHasSocket ? 1 : 0} ready=${readyCount ?? "n/a"} sock=0x${(socketRevents ?? 0).toString(16)} conns=${handle.networkConnections.size}`);
    if (readyCount !== undefined && readyCount > 0) {
      return false;
    }
    if (pollWaitHasSocket) {
      return true;
    }
    return status === handle.kernel.abi_status_socket_recv()
      || status === handle.kernel.abi_status_socket_send()
      || status === handle.kernel.abi_status_connect();
  }

  function blockedWaitShouldPumpNetwork(handle: ProcessHandle, status: number, vmPtr: number): boolean {
    if (handle.networkConnections.size === 0) {
      return false;
    }
    if (blockedWaitMayDependOnNetwork(handle, status, vmPtr)) {
      return true;
    }
    const readyCount = status === handle.kernel.abi_status_pipe_wait()
      ? handle.kernel.thread_poll_wait_ready_count?.(vmPtr)
      : undefined;
    return status === handle.kernel.abi_status_pipe_wait()
      && (readyCount === undefined || readyCount <= 0);
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
    blocked.rpcPending = true;
    void deps.pumpNetworkConnections(handle, blocked.vmPtr)
      .then(async (changed) => {
        if (changed) {
          await requestSocketStateSync(handle, true);
        }
        const latestBlocked = handle.blockedWorkers.get(worker);
        if (!latestBlocked || latestBlocked.vmPtr !== blocked.vmPtr) {
          return;
        }
        latestBlocked.rpcPending = false;
        latestBlocked.skipNextNetworkPump = !changed;
        appendWorkerDebugLog(handle, `net-pump-done:${handle.pid}:status=${blocked.status}`);
        logNetworkDebug(`pump-done pid=${handle.pid} status=${blocked.status}`);
        scheduleBlockedRetry(handle, worker, changed ? 1 : 0, latestBlocked.timeoutExpired);
      })
      .catch((error) => {
        const latestBlocked = handle.blockedWorkers.get(worker);
        if (latestBlocked && latestBlocked.vmPtr === blocked.vmPtr) {
          latestBlocked.rpcPending = false;
        }
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
      skipNextNetworkPump: false,
      pipeSyncedBeforePassive: false,
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
    const socketTimeoutMs = socketWaitTimeoutMs(handle, status, vmPtr);
    if (socketTimeoutMs >= 0) {
      scheduleSocketBlockedRetry(handle, worker, blocked, socketTimeoutMs);
      return;
    }
    if (status === handle.kernel.abi_status_wait4()) {
      retryBlockedWorker(handle, worker);
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
    const blockedPollTimeoutMs = pollWaitTimeoutMs(handle, blocked.status, blocked.vmPtr);
    if (handle.kernelShare.forkSharedSyncPromise !== null && blockedPollTimeoutMs < 0) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-retry-skip:${handle.pid}:fork-shared-sync`);
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
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `wait4-retry-skip:${handle.pid}:gate:exit=${handle.pendingChildExitRecords}:spawn=${handle.pendingChildSpawnRecords}:active=${deps.hasActiveChildProcess(handle.pid) ? 1 : 0}`,
      );
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

    const blockedPollReadyCount = blocked.status === handle.kernel.abi_status_pipe_wait()
      ? handle.kernel.thread_poll_wait_ready_count?.(blocked.vmPtr)
      : undefined;
    if (blocked.status === handle.kernel.abi_status_pipe_wait()) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `pipe-retry:${handle.pid}:a7=${handle.kernel.thread_a7?.(blocked.vmPtr)?.toString() ?? "n/a"}:a0=${handle.kernel.thread_a0(blocked.vmPtr).toString()}:ready=${blockedPollReadyCount ?? "n/a"}:synced=${blocked.pipeSyncedBeforePassive ? 1 : 0}`,
      );
    }
    if (
      blocked.status === handle.kernel.abi_status_pipe_wait()
      && blockedPollTimeoutMs >= 0
      && !blocked.timeoutExpired
      && (blockedPollReadyCount === undefined || blockedPollReadyCount <= 0)
    ) {
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
      && !blocked.pipeSyncedBeforePassive
      && kernelWorkerOwnsPipeState(handle)
      && handle.kernelWorkerRequest
      && handle.pid > 0
    ) {
      blocked.pipeSyncedBeforePassive = true;
      blocked.rpcPending = true;
      void requestPipeStateSync(handle, false)
        .then(() => {
          const latestBlocked = handle.blockedWorkers.get(worker);
          if (!latestBlocked || latestBlocked.vmPtr !== blocked.vmPtr) {
            return;
          }
          latestBlocked.rpcPending = false;
          scheduleBlockedRetry(handle, worker, 0, latestBlocked.timeoutExpired);
        })
        .catch((error) => {
          const latestBlocked = handle.blockedWorkers.get(worker);
          if (latestBlocked && latestBlocked.vmPtr === blocked.vmPtr) {
            latestBlocked.rpcPending = false;
          }
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }
    if (pipeWaitShouldRemainPassive(
      handle,
      blocked.status,
      blocked.vmPtr,
      blockedPollTimeoutMs,
      blocked.timeoutDeadlineMs,
      blockedPollReadyCount,
    )) {
      scheduleBlockedRetry(handle, worker, 25, blocked.timeoutExpired);
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
    ) {
      advanceGuestTimeByMs(handle, blockedPollTimeoutMs);
      handle.kernel.thread_resume_poll_wait_timeout(blocked.vmPtr);
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
