import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../abi.js";
import {
  publishKernelRuntimeState,
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
import { kernelWorkerOwnsPipeState } from "./kernel-share.js";
import { describeForkSharedSyncPromise } from "./fork-shared-sync-debug.js";
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
const POLLFD_BYTES = 8;
const PIPE_SLOT_BYTES = 32 + 65_536;
const PIPE_OFF_HEAD = 0;
const PIPE_OFF_TAIL = 4;
const PIPE_OFF_READ_OPEN = 8;
const PIPE_OFF_WRITE_OPEN = 12;
const PIPE_DATA_START = 32;
const PIPE_BUF_SIZE = 65_536;

function pipeDebugPreviewEnabled(): boolean {
  return ((globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_PIPE_DEBUG_PREVIEW ?? "") === "1";
}

function formatPipeDebugAscii(bytes: Uint8Array, maxLen = 96): string {
  let out = "";
  for (const byte of bytes.subarray(0, maxLen)) {
    if (byte === 0x0a) {
      out += "\\n";
    } else if (byte === 0x0d) {
      out += "\\r";
    } else if (byte === 0x09) {
      out += "\\t";
    } else if (byte >= 0x20 && byte <= 0x7e && byte !== 0x5c && byte !== 0x22) {
      out += String.fromCharCode(byte);
    } else {
      out += `\\x${byte.toString(16).padStart(2, "0")}`;
    }
  }
  if (bytes.length > maxLen) {
    out += "...";
  }
  return out;
}

function formatPipeWaitDataDebug(handle: ProcessHandle, slotBase: number, head: number, tail: number): string {
  if (!pipeDebugPreviewEnabled()) {
    return "";
  }
  const len = tail >= head
    ? tail - head
    : PIPE_BUF_SIZE - head + tail;
  if (len <= 0 || len > PIPE_BUF_SIZE) {
    return `:data_len=${len}`;
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  const dataBase = slotBase + PIPE_DATA_START;
  const previewLen = Math.min(len, 96);
  const preview = new Uint8Array(previewLen);
  for (let i = 0; i < previewLen; i += 1) {
    preview[i] = bytes[dataBase + ((head + i) % PIPE_BUF_SIZE)];
  }
  return `:data_len=${len}:data_ascii=${JSON.stringify(formatPipeDebugAscii(preview))}`;
}

export function createWorkerScheduler(deps: WorkerSchedulerDeps): WorkerScheduler {
  const { threadWorkerStates } = deps;
  const observedForkSharedSyncPromises = new WeakSet<Promise<void>>();
  const pipeChildExitResumeKeys = new WeakMap<ProcessHandle, Map<number, string>>();
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
    return thread?.waitingInWorkerStatus !== null || thread?.ownerAsyncStatus !== null;
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

  function sleepRetryDelayMs(handle: ProcessHandle, deadlineTicks: bigint): number {
    const now = monotonicTicksCurrent(handle);
    if (deadlineTicks <= now) {
      return 0;
    }
    const remainingTicks = deadlineTicks - now;
    const delayMs = Number((remainingTicks * 1000n + (GUEST_TICKS_PER_SEC - 1n)) / GUEST_TICKS_PER_SEC);
    if (!Number.isFinite(delayMs)) {
      return 2_147_483_647;
    }
    return Math.max(0, Math.min(delayMs, 2_147_483_647));
  }

  function advanceGuestTimeByMs(handle: ProcessHandle, timeoutMs: number): void {
    if (timeoutMs <= 0 || !Number.isFinite(timeoutMs)) {
      return;
    }
    const deltaTicks = BigInt(Math.ceil(timeoutMs)) * GUEST_TICKS_PER_SEC / 1000n;
    handle.kernel.sync_monotonic_ticks_floor?.(monotonicTicksCurrent(handle) + deltaTicks);
  }

  function syscallMatches(
    syscallNr: bigint,
    abiSyscall: (() => bigint | number) | undefined,
  ): boolean {
    const value = abiSyscall?.();
    return value !== undefined && syscallNr === BigInt(value);
  }

  function queueGuestBufferWrite(
    handle: ProcessHandle,
    worker: Worker,
    ptr: bigint | number,
    length: bigint | number,
  ): void {
    const start = typeof ptr === "bigint" ? Number(ptr) : ptr;
    const requested = typeof length === "bigint" ? Number(length) : length;
    if (!Number.isFinite(start) || !Number.isFinite(requested) || start <= 0 || requested <= 0) {
      return;
    }
    const absolute = handle.guestBase + start;
    const writeLen = Math.min(requested, 65_536);
    const memory = new Uint8Array(handle.memory.buffer);
    if (absolute < 0 || absolute + writeLen > memory.byteLength) {
      return;
    }
    const thread = threadWorkerStates.get(worker);
    if (!thread) {
      return;
    }
    thread.pendingGuestMemoryWrites.push({
      guestPtr: start,
      bytes: memory.slice(absolute, absolute + writeLen),
    });
    if (thread.pendingGuestMemoryWrites.length > 32) {
      thread.pendingGuestMemoryWrites.splice(0, thread.pendingGuestMemoryWrites.length - 32);
    }
  }

  function queueBlockedPollWaitGuestMemoryWrites(
    handle: ProcessHandle,
    worker: Worker,
    syscallNr: bigint,
    arg0: bigint,
    arg1: bigint,
    arg2: bigint,
    arg3: bigint,
  ): void {
    const maxFds = Math.max(0, handle.kernel.abi_max_fds?.() ?? 1024);
    if (syscallMatches(syscallNr, handle.kernel.abi_sys_ppoll)) {
      const nfds = Math.min(Number(arg1), maxFds);
      if (Number.isFinite(nfds) && nfds > 0) {
        queueGuestBufferWrite(handle, worker, arg0, nfds * POLLFD_BYTES);
      }
      return;
    }
    if (syscallMatches(syscallNr, handle.kernel.abi_sys_pselect6)) {
      const nfds = Math.min(Number(arg0), maxFds);
      if (!Number.isFinite(nfds) || nfds <= 0) {
        return;
      }
      const setBytes = Math.floor((nfds + 63) / 64) * 8;
      queueGuestBufferWrite(handle, worker, arg1, setBytes);
      queueGuestBufferWrite(handle, worker, arg2, setBytes);
      queueGuestBufferWrite(handle, worker, arg3, setBytes);
    }
  }

  function resumePipePollTimeoutLocally(
    handle: ProcessHandle,
    worker: Worker,
    blocked: BlockedWorkerState,
    timeoutMs: number,
  ): void {
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-retry-timeout-local:${handle.pid}`);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    const blockedSyscallNr = handle.kernel.thread_blocked_syscall_nr?.(blocked.vmPtr) ?? 0n;
    const blockedArg0 = handle.kernel.thread_blocked_syscall_arg?.(blocked.vmPtr, 0) ?? 0n;
    const blockedArg1 = handle.kernel.thread_blocked_syscall_arg?.(blocked.vmPtr, 1) ?? 0n;
    const blockedArg2 = handle.kernel.thread_blocked_syscall_arg?.(blocked.vmPtr, 2) ?? 0n;
    const blockedArg3 = handle.kernel.thread_blocked_syscall_arg?.(blocked.vmPtr, 3) ?? 0n;
    advanceGuestTimeByMs(handle, timeoutMs);
    handle.kernel.thread_resume_poll_wait_timeout?.(blocked.vmPtr);
    queueBlockedPollWaitGuestMemoryWrites(
      handle,
      worker,
      BigInt(blockedSyscallNr),
      BigInt(blockedArg0),
      BigInt(blockedArg1),
      BigInt(blockedArg2),
      BigInt(blockedArg3),
    );
    blocked.timeoutExpired = false;
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    handleBlockedRetryResult(
      handle,
      worker,
      blocked.vmPtr,
      handle.kernel.thread_status(blocked.vmPtr),
    );
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
      scheduleBlockedRetry(handle, worker, 0, blocked.timeoutExpired);
    };
    promise.then(onSettled, onSettled);
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
    if (handle.workerDebugLog.length > 4096) {
      handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
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

  function blockedOrCurrentSyscallNr(handle: ProcessHandle, vmPtr: number): bigint | null {
    const blockedSyscall = handle.kernel.thread_blocked_syscall_nr?.(vmPtr);
    if (blockedSyscall !== undefined && BigInt(blockedSyscall) !== 0n) {
      return BigInt(blockedSyscall);
    }
    const currentSyscall = handle.kernel.thread_a7?.(vmPtr);
    return currentSyscall === undefined ? null : BigInt(currentSyscall);
  }

  function pollWaitTimeoutMs(handle: ProcessHandle, status: number, vmPtr: number): number {
    if (status !== handle.kernel.abi_status_pipe_wait()) {
      return -1;
    }
    const syscall = blockedOrCurrentSyscallNr(handle, vmPtr);
    if (syscall === null) {
      return -1;
    }
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
    pipeChildExitResumeKey: string | null,
  ): boolean {
    const syscall = blockedOrCurrentSyscallNr(handle, vmPtr);
    if (syscall === null) {
      return false;
    }
    const read = handle.kernel.abi_sys_read?.();
    const readv = handle.kernel.abi_sys_readv?.();
    const ppoll = handle.kernel.abi_sys_ppoll?.();
    const pselect6 = handle.kernel.abi_sys_pselect6?.();
    const isPollLike = (ppoll !== undefined && syscall === BigInt(ppoll))
      || (pselect6 !== undefined && syscall === BigInt(pselect6));
    const canPark = (
      (read !== undefined && syscall === BigInt(read))
      || (readv !== undefined && syscall === BigInt(readv))
      || isPollLike
    );
    const blockedReadIsReady = (
      ((read !== undefined && syscall === BigInt(read)) || (readv !== undefined && syscall === BigInt(readv)))
      && handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr) === 1
    );
    const blockedPollPipeIsReady = blockedPollPipeHasBufferedData(handle, vmPtr);
    const pollLikePipeHasNoWriter = isPollLike
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
      && !blockedPollPipeIsReady
      && pollPipeWriteOpenForThreadStep(handle, vmPtr) <= 0;
    const childExitRecordsAlreadyResumed = pipeChildExitRecordsAlreadyResumedForWait(
      handle,
      vmPtr,
      pipeChildExitResumeKey,
    );
    return status === handle.kernel.abi_status_pipe_wait()
      && canPark
      && childExitRecordsAlreadyResumed
      && !blockedReadIsReady
      && !blockedPollPipeIsReady
      && !pollLikePipeHasNoWriter
      && pollTimeoutMs < 0
      && timeoutDeadlineMs === null
      && (readyCount === undefined || readyCount === 0)
      && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) !== 1
      && handle.kernel.thread_poll_hup_sync_pending?.(vmPtr) !== 1;
  }

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

  function childExitRecordsAlreadyApplied(handle: ProcessHandle, recordsKey: string): boolean {
    return appliedChildExitRecordKeys.get(`${handle.runtimeId}:${handle.pid}`)?.has(recordsKey) ?? false;
  }

  function markChildExitRecordsApplied(handle: ProcessHandle, recordsKey: string): void {
    const processKey = `${handle.runtimeId}:${handle.pid}`;
    let records = appliedChildExitRecordKeys.get(processKey);
    if (records === undefined) {
      records = new Set();
      appliedChildExitRecordKeys.set(processKey, records);
    }
    records.add(recordsKey);
  }

  function pipeWaitHasImmediateWakeReason(
    handle: ProcessHandle,
    vmPtr: number,
    readyCount: number | undefined,
  ): boolean {
    return (readyCount !== undefined && readyCount !== 0)
      || handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr) === 1
      || handle.kernel.thread_blocked_pipe_write_ready?.(vmPtr) === 1
      || blockedPollPipeHasBufferedData(handle, vmPtr)
      || handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) === 1
      || handle.kernel.thread_poll_hup_sync_pending?.(vmPtr) === 1;
  }

  function pipeWaitShouldParkBehindActiveChild(
    handle: ProcessHandle,
    blocked: BlockedWorkerState,
    pollTimeoutMs: number,
    readyCount: number | undefined,
  ): boolean {
    return blocked.status === handle.kernel.abi_status_pipe_wait()
      && pollTimeoutMs < 0
      && blocked.timeoutDeadlineMs === null
      && !blocked.timeoutExpired
      && pipeChildExitRecordsAlreadyResumedForWait(handle, blocked.vmPtr)
      && handle.pendingChildExitRecords === 0
      && handle.pendingChildSpawnRecords === 0
      && handle.kernel.thread_poll_wait_has_socket?.(blocked.vmPtr) !== 1
      && deps.hasActiveChildProcess(handle.pid)
      && !pipeWaitHasImmediateWakeReason(handle, blocked.vmPtr, readyCount);
  }

  function blockedPollPipeHasBufferedData(handle: ProcessHandle, vmPtr: number): boolean {
    const rawMask = handle.kernel.thread_poll_wait_pipe_slots_mask?.(vmPtr);
    const mask = rawMask === undefined
      ? 0n
      : typeof rawMask === "bigint" ? rawMask : BigInt(rawMask);
    if (mask !== 0n) {
      const maxPipes = Math.min(handle.kernel.abi_max_pipes?.() ?? 64, 64);
      const view = new DataView(handle.memory.buffer);
      for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
        if ((mask & (1n << BigInt(pipeSlot))) === 0n) {
          continue;
        }
        const slotBase = handle.kernelState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
        const head = view.getUint32(slotBase + PIPE_OFF_HEAD, true);
        const tail = view.getUint32(slotBase + PIPE_OFF_TAIL, true);
        if (head !== tail) {
          return true;
        }
      }
    }
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

  function pollPipeSlotsForThreadStep(handle: ProcessHandle, vmPtr: number): Set<number> {
    const rawMask = handle.kernel.thread_poll_wait_pipe_slots_mask?.(vmPtr);
    const mask = rawMask === undefined
      ? 0n
      : typeof rawMask === "bigint" ? rawMask : BigInt(rawMask);
    const pipeSlots = new Set<number>();
    const maxPipes = Math.min(handle.kernel.abi_max_pipes?.() ?? 64, 64);
    for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
      if ((mask & (1n << BigInt(pipeSlot))) !== 0n) {
        pipeSlots.add(pipeSlot);
      }
    }
    if (pipeSlots.size > 0) {
      return pipeSlots;
    }
    const packed = handle.kernel.thread_poll_wait_first_pipe_revents?.(vmPtr);
    if (packed === undefined || packed === 0) {
      return pipeSlots;
    }
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, packed & 0xff);
    if (
      ofdId > 0
      && ofdId < handle.kernel.abi_max_fds()
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
    ) {
      const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
      if (pipeSlot > 0 && pipeSlot < maxPipes) {
        pipeSlots.add(pipeSlot);
      }
    }
    return pipeSlots;
  }

  function pollPipeWriteOpenForThreadStep(handle: ProcessHandle, vmPtr: number): number {
    let writeOpen = 0;
    const view = new DataView(handle.memory.buffer);
    for (const pipeSlot of pollPipeSlotsForThreadStep(handle, vmPtr)) {
      const slotBase = handle.kernelState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
      if (slotBase < 0 || slotBase + PIPE_OFF_WRITE_OPEN + 4 > view.byteLength) {
        continue;
      }
      writeOpen = Math.max(writeOpen, view.getInt32(slotBase + PIPE_OFF_WRITE_OPEN, true));
    }
    return writeOpen;
  }

  function formatPipeSlotHolders(handle: ProcessHandle, pipeSlot: number): string {
    if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
      return "n/a";
    }
    const holders: string[] = [];
    for (const member of handle.kernelShare.members) {
      if (member.vmPtr === 0 || member.exitInProgress) {
        continue;
      }
      const maxFds = member.kernel.abi_max_fds();
      for (let fd = 0; fd < maxFds; fd += 1) {
        const ofdId = member.kernel.process_fd_ofd_id(member.vmPtr, fd);
        if (
          ofdId <= 0
          || ofdId >= maxFds
          || member.kernel.kernel_ofd_fd_type(ofdId) !== member.kernel.abi_fd_type_pipe()
          || member.kernel.kernel_ofd_ino(ofdId) !== pipeSlot
        ) {
          continue;
        }
        const hostFd = member.kernel.kernel_ofd_host_fd(ofdId);
        holders.push(`${member.pid}/${fd}/${ofdId}/${hostFd >= 0 ? "r" : "w"}/h${hostFd}`);
      }
    }
    return holders.join(".") || "none";
  }

  function formatFirstPipeWaitDebug(handle: ProcessHandle, vmPtr: number): string {
    const packed = handle.kernel.thread_poll_wait_first_pipe_revents?.(vmPtr);
    if (packed === undefined) {
      return "pipe=n/a";
    }
    const fd = packed & 0xff;
    const interest = (packed >>> 8) & 0xffff;
    const revents = (packed >>> 24) & 0xff;
    try {
      const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
      if (
        ofdId <= 0
        || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
      ) {
        return `pipe=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}`;
      }
      const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
      if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
        return `pipe=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}/slot${pipeSlot}`;
      }
      const slotBase = handle.kernelState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
      const view = new DataView(handle.memory.buffer);
      const head = view.getUint32(slotBase + PIPE_OFF_HEAD, true);
      const tail = view.getUint32(slotBase + PIPE_OFF_TAIL, true);
      const readOpen = view.getInt32(slotBase + PIPE_OFF_READ_OPEN, true);
      const writeOpen = view.getInt32(slotBase + PIPE_OFF_WRITE_OPEN, true);
      return `pipe=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}/slot${pipeSlot}/h${head}/t${tail}/ro${readOpen}/wo${writeOpen}${formatPipeWaitDataDebug(handle, slotBase, head, tail)}/holders=${formatPipeSlotHolders(handle, pipeSlot)}`;
    } catch (error) {
      return `pipe=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/err=${JSON.stringify(error instanceof Error ? error.message : String(error))}`;
    }
  }

  function formatFirstSocketWaitDebug(handle: ProcessHandle, vmPtr: number): string {
    const packed = handle.kernel.thread_poll_wait_first_socket_revents?.(vmPtr);
    if (packed === undefined) {
      return "sock=n/a";
    }
    const fd = packed & 0xff;
    const interest = (packed >>> 8) & 0xffff;
    const revents = (packed >>> 24) & 0xff;
    try {
      const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
      if (
        ofdId <= 0
        || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_socket()
      ) {
        return `sock=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}`;
      }
      const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd) ?? -1;
      const state = socketIdx >= 0 ? handle.kernel.thread_socket_state?.(socketIdx) : undefined;
      const available = socketIdx >= 0 ? handle.kernel.thread_socket_recv_available?.(socketIdx) : undefined;
      const peerIdx = socketIdx >= 0 ? handle.kernel.thread_socket_peer_idx?.(socketIdx) : undefined;
      return `sock=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}/idx${socketIdx}/st${state ?? "n/a"}/avail${available ?? "n/a"}/peer${peerIdx ?? "n/a"}/conn${handle.networkConnections.has(fd) ? 1 : 0}`;
    } catch (error) {
      return `sock=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/err=${JSON.stringify(error instanceof Error ? error.message : String(error))}`;
    }
  }

  function formatPollFdTarget(handle: ProcessHandle, vmPtr: number, fd: number): string {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    const maxFds = handle.kernel.abi_max_fds();
    if (ofdId <= 0 || ofdId >= maxFds) {
      return `ofd${ofdId}`;
    }
    const type = handle.kernel.kernel_ofd_fd_type(ofdId);
    const ino = handle.kernel.kernel_ofd_ino(ofdId);
    const hostFd = handle.kernel.kernel_ofd_host_fd(ofdId);
    if (type !== handle.kernel.abi_fd_type_pipe()) {
      return `ofd${ofdId}/ty${type}/ino${ino}/host${hostFd}`;
    }
    if (ino <= 0 || ino >= handle.kernel.abi_max_pipes()) {
      return `ofd${ofdId}/ty${type}/slot${ino}/host${hostFd}`;
    }
    const slotBase = handle.kernelState.pipeBase + ino * PIPE_SLOT_BYTES;
    const view = new DataView(handle.memory.buffer);
    const head = view.getUint32(slotBase + PIPE_OFF_HEAD, true);
    const tail = view.getUint32(slotBase + PIPE_OFF_TAIL, true);
    const readOpen = view.getInt32(slotBase + PIPE_OFF_READ_OPEN, true);
    const writeOpen = view.getInt32(slotBase + PIPE_OFF_WRITE_OPEN, true);
    return `ofd${ofdId}/ty${type}/slot${ino}/h${head}/t${tail}/ro${readOpen}/wo${writeOpen}/host${hostFd}/holders=${formatPipeSlotHolders(handle, ino)}`;
  }

  function guestAbsoluteRange(handle: ProcessHandle, ptr: bigint, byteLength: number): number | null {
    const guestPtr = Number(ptr);
    if (!Number.isFinite(guestPtr) || guestPtr <= 0 || byteLength <= 0) {
      return null;
    }
    const absolute = handle.guestBase + guestPtr;
    if (absolute < 0 || absolute + byteLength > handle.memory.buffer.byteLength) {
      return null;
    }
    return absolute;
  }

  function formatBlockedPollFdSet(handle: ProcessHandle, vmPtr: number, limit = 16): string {
    const syscall = blockedOrCurrentSyscallNr(handle, vmPtr);
    if (syscall === null) {
      return "pollfds=n/a";
    }
    const ppoll = handle.kernel.abi_sys_ppoll?.();
    const pselect6 = handle.kernel.abi_sys_pselect6?.();
    const maxFds = handle.kernel.abi_max_fds();
    const view = new DataView(handle.memory.buffer);
    try {
      if (ppoll !== undefined && syscall === BigInt(ppoll)) {
        const fdsPtr = BigInt(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0) ?? 0n);
        const nfds = Number(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 1) ?? 0n);
        if (!Number.isFinite(nfds) || nfds <= 0) {
          return "pollfds=ppoll:none";
        }
        const count = Math.min(nfds, maxFds, limit);
        const absolute = guestAbsoluteRange(handle, fdsPtr, count * POLLFD_BYTES);
        if (absolute === null) {
          return `pollfds=ppoll:invalid:ptr=0x${fdsPtr.toString(16)}:nfds=${nfds}`;
        }
        const entries: string[] = [];
        for (let index = 0; index < count; index += 1) {
          const offset = absolute + index * POLLFD_BYTES;
          const fd = view.getInt32(offset, true);
          const events = view.getUint16(offset + 4, true);
          const revents = view.getUint16(offset + 6, true);
          if (fd < 0 || events === 0) {
            continue;
          }
          entries.push(`${index}=fd${fd}/ev0x${events.toString(16)}/rev0x${revents.toString(16)}/${formatPollFdTarget(handle, vmPtr, fd)}`);
        }
        return `pollfds=ppoll:${entries.join(",") || "none"}`;
      }
      if (pselect6 !== undefined && syscall === BigInt(pselect6)) {
        const nfds = Number(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0) ?? 0n);
        const readPtr = BigInt(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 1) ?? 0n);
        const writePtr = BigInt(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 2) ?? 0n);
        const exceptPtr = BigInt(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 3) ?? 0n);
        if (!Number.isFinite(nfds) || nfds <= 0) {
          return "pollfds=pselect:none";
        }
        const count = Math.min(nfds, maxFds, limit);
        const setBytes = Math.floor((Math.min(nfds, maxFds) + 63) / 64) * 8;
        const readBase = readPtr === 0n ? null : guestAbsoluteRange(handle, readPtr, setBytes);
        const writeBase = writePtr === 0n ? null : guestAbsoluteRange(handle, writePtr, setBytes);
        const exceptBase = exceptPtr === 0n ? null : guestAbsoluteRange(handle, exceptPtr, setBytes);
        const hasBit = (base: number | null, fd: number): boolean => {
          if (base === null) {
            return false;
          }
          return (view.getUint8(base + Math.floor(fd / 8)) & (1 << (fd % 8))) !== 0;
        };
        const entries: string[] = [];
        for (let fd = 0; fd < count; fd += 1) {
          const interest = `${hasBit(readBase, fd) ? "r" : ""}${hasBit(writeBase, fd) ? "w" : ""}${hasBit(exceptBase, fd) ? "e" : ""}`;
          if (interest === "") {
            continue;
          }
          entries.push(`fd${fd}/${interest}/${formatPollFdTarget(handle, vmPtr, fd)}`);
        }
        return `pollfds=pselect:${entries.join(",") || "none"}`;
      }
    } catch (error) {
      return `pollfds=err:${JSON.stringify(error instanceof Error ? error.message : String(error))}`;
    }
    return "pollfds=not-poll";
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

  function schedulePipePollTimeoutRetry(
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
    scheduleBlockedRetry(handle, worker, remaining);
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
    if (pollWaitHasSocket) {
      return true;
    }
    if (readyCount !== undefined && readyCount > 0) {
      return false;
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

  function networkInboundAvailable(handle: ProcessHandle, vmPtr: number): boolean {
    if (handle.networkConnections.size === 0) {
      return false;
    }
    for (const fd of handle.networkConnections.keys()) {
      const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd) ?? -1;
      if (socketIdx >= 0 && (handle.kernel.thread_socket_recv_available?.(socketIdx) ?? 0) > 0) {
        return true;
      }
    }
    return false;
  }

  function formatChildExitRecords(handle: ProcessHandle): string {
    return [...handle.childExitSyncRecords]
      .map(([childPid, waitStatus]) => `${childPid}/${waitStatus}`)
      .join(".");
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

  function applyLocalChildExitRecords(handle: ProcessHandle): void {
    if (handle.childExitSyncRecords.size === 0) {
      return;
    }
    const recordsKey = formatChildExitRecords(handle);
    if (childExitRecordsAlreadyApplied(handle, recordsKey)) {
      return;
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `wait4-apply-records:${handle.pid}:${recordsKey}`,
    );
    for (const [childPid, waitStatus] of handle.childExitSyncRecords) {
      if (handle.kernel.process_record_child_exit_for_parent && handle.pid > 0) {
        handle.kernel.process_record_child_exit_for_parent(handle.pid, childPid, waitStatus);
      } else {
        handle.kernel.process_record_child_exit(childPid, waitStatus);
      }
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    markChildExitRecordsApplied(handle, recordsKey);
  }

  function hasWorkerLocalFutexWait(handle: ProcessHandle): boolean {
    const futexWaitStatus = handle.kernel.abi_status_futex_wait();
    for (const worker of handle.workers) {
      const thread = threadWorkerStates.get(worker);
      if (
        thread?.ownerPid === handle.pid
        && thread.waitingInWorkerStatus === futexWaitStatus
      ) {
        return true;
      }
    }
    return false;
  }

  function applyChildExitRecordsForWorkerLocalWaits(handle: ProcessHandle): void {
    if (handle.childExitSyncRecords.size === 0 || !hasWorkerLocalFutexWait(handle)) {
      return;
    }
    const recordsKey = formatChildExitRecords(handle);
    if (childExitRecordsAlreadyApplied(handle, recordsKey)) {
      return;
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `futex-wait-apply-child-records:${handle.pid}:${recordsKey}`,
    );
    applyLocalChildExitRecords(handle);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
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
      void deps.resumeFsViaKernelWorker(handle, worker, blocked.vmPtr)
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
