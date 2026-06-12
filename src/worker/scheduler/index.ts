import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
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
import { collectLiveThreadVmPtrs, processHasLiveThreads } from "../live-threads.js";
import { appendWorkerDebugLog } from "./fairness.js";
import { clearStaleForkSharedSyncGate } from "./fork-shared-gate.js";
import { createFutexScheduler } from "./futex.js";
import { createPipePollScheduler } from "./pipe-poll.js";
import { createPruningScheduler } from "./pruning.js";
import { createWorkerSchedulerQueueHelpers } from "./queue.js";
import { createRunLoopScheduler, logNetworkDebug } from "./run-loop.js";
import { createSleepScheduler } from "./sleep.js";
import { createSocketWaitScheduler } from "./socket-wait.js";
import { createWorkerSchedulerStateSyncHelpers } from "./state-sync.js";
import { createVforkExclusiveScheduler } from "./vfork-exclusive.js";
import { createWait4Scheduler } from "./wait4.js";
import type {
  BlockedWorkerState,
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

  function formatProcessThreadSlots(handle: ProcessHandle, vmPtr: number): string {
    const slots: string[] = [];
    for (let slot = 0; slot < handle.kernel.abi_max_threads(); slot += 1) {
      const tid = handle.kernel.process_thread_tid(vmPtr, slot);
      const state = handle.kernel.process_thread_state(vmPtr, slot);
      const threadVmPtr = handle.kernel.process_thread_vm_ptr(vmPtr, slot);
      if (tid === 0 && state === 0 && threadVmPtr === 0) {
        continue;
      }
      slots.push(`${slot}:${tid}/${state}/0x${threadVmPtr.toString(16)}`);
    }
    return slots.join(",");
  }

  function formatFaultThreadSummary(handle: ProcessHandle, label: string, vmPtr: number): string {
    return `${label}=0x${vmPtr.toString(16)}`
      + `/status=${handle.kernel.thread_status?.(vmPtr) ?? -1}`
      + `/exit=${handle.kernel.thread_exit_code?.(vmPtr) ?? 0}:${handle.kernel.thread_term_signal?.(vmPtr) ?? 0}`
      + `/sig=${handle.kernel.thread_fault_signal?.(vmPtr) ?? 0}`
      + `/kind=${handle.kernel.thread_fault_kind?.(vmPtr) ?? 0}`
      + `/pc=0x${(handle.kernel.thread_fault_pc?.(vmPtr) ?? 0n).toString(16)}`
      + `/addr=0x${(handle.kernel.thread_fault_addr?.(vmPtr) ?? 0n).toString(16)}`;
  }

  function formatOfdType(handle: ProcessHandle, ofdId: number): string {
    try {
      const fdType = handle.kernel.kernel_ofd_fd_type(ofdId);
      if (fdType === handle.kernel.abi_fd_type_file?.()) {
        return "file";
      }
      if (fdType === handle.kernel.abi_fd_type_pipe?.()) {
        return "pipe";
      }
      if (fdType === handle.kernel.abi_fd_type_epoll?.()) {
        return "epoll";
      }
      if (fdType === handle.kernel.abi_fd_type_eventfd?.()) {
        return "eventfd";
      }
      if (fdType === handle.kernel.abi_fd_type_socket?.()) {
        return "socket";
      }
      return String(fdType);
    } catch {
      return "err";
    }
  }

  function formatThreadFdTableSummary(handle: ProcessHandle, vmPtr: number): string {
    if (vmPtr === 0) {
      return "fds=none";
    }
    const focusFds = new Set([9, 10, 11, 12]);
    const entries: string[] = [];
    const maxFds = handle.kernel.abi_max_fds();
    for (let fd = 0; fd < maxFds; fd += 1) {
      let ofdId = 0;
      try {
        ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
      } catch {
        entries.push(`${fd}=err`);
        continue;
      }
      if (ofdId === 0 && !focusFds.has(fd)) {
        continue;
      }
      const flags = handle.kernel.process_fd_flags?.(vmPtr, fd) ?? 0;
      if (ofdId === 0) {
        entries.push(`${fd}=0/fl${flags}`);
        continue;
      }
      let ino = "err";
      let refcount = "err";
      try {
        ino = String(handle.kernel.kernel_ofd_ino(ofdId));
      } catch {
        // keep diagnostic compact when an inherited harness lacks an export
      }
      try {
        refcount = String(handle.kernel.kernel_ofd_refcount(ofdId));
      } catch {
        // keep diagnostic compact when an inherited harness lacks an export
      }
      entries.push(`${fd}=${ofdId}/${formatOfdType(handle, ofdId)}/ino${ino}/ref${refcount}/fl${flags}`);
      if (entries.length >= 24) {
        entries.push("truncated");
        break;
      }
    }
    return `fds=${entries.join(",") || "empty"}`;
  }

  function faultExitVmPriority(handle: ProcessHandle, vmPtr: number): number {
    if ((handle.kernel.thread_fault_signal?.(vmPtr) ?? 0) !== 0) {
      return 4;
    }
    if ((handle.kernel.thread_fault_kind?.(vmPtr) ?? 0) !== 0) {
      return 3;
    }
    if (
      (handle.kernel.thread_fault_pc?.(vmPtr) ?? 0n) !== 0n
      || (handle.kernel.thread_fault_addr?.(vmPtr) ?? 0n) !== 0n
    ) {
      return 2;
    }
    if ((handle.kernel.thread_term_signal?.(vmPtr) ?? 0) !== 0) {
      return 1;
    }
    return 0;
  }

  function selectFaultExitVmPtr(handle: ProcessHandle, vmPtr: number): number {
    const candidates = new Set<number>([vmPtr]);
    if (handle.vmPtr !== 0) {
      candidates.add(handle.vmPtr);
      for (const liveVmPtr of collectLiveThreadVmPtrs(handle.kernel, handle.vmPtr)) {
        candidates.add(liveVmPtr);
      }
    }
    let selectedVmPtr = vmPtr;
    let selectedPriority = faultExitVmPriority(handle, selectedVmPtr);
    for (const candidate of candidates) {
      const priority = faultExitVmPriority(handle, candidate);
      if (priority > selectedPriority) {
        selectedVmPtr = candidate;
        selectedPriority = priority;
      }
    }
    return selectedVmPtr;
  }

  const appliedChildExitRecordKeys = new Map<string, Set<string>>();
  const schedulerQueueHelpers = createWorkerSchedulerQueueHelpers({
    threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    appendWorkerDebugLog,
    blockWorker,
    completeProcessExit: deps.completeProcessExit,
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

  function epollTimerfdDeadlineTicks(handle: ProcessHandle, vmPtr: number): bigint | null {
    const rawDeadline = handle.kernel.thread_epoll_timerfd_deadline_ticks?.(vmPtr);
    if (rawDeadline === undefined) {
      return null;
    }
    const deadline = BigInt(rawDeadline);
    return deadline === 0n ? null : deadline;
  }

  function markEpollTimeoutZeroExpired(handle: ProcessHandle, blocked: BlockedWorkerState): void {
    if (
      blocked.status === handle.kernel.abi_status_epoll_wait()
      && Number(handle.kernel.thread_epoll_wait_timeout(blocked.vmPtr)) === 0
    ) {
      blocked.timeoutExpired = true;
      blocked.epollTimerfdDeadlineTicks = null;
    }
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

  function formatRunningRetryBlocker(
    handle: ProcessHandle,
    runningThread: ThreadWorkerState | undefined,
  ): string {
    const runningVmPtr = runningThread?.vmPtr ?? 0;
    const runningStatus = runningVmPtr === 0 ? -1 : handle.kernel.thread_status(runningVmPtr);
    const activeStepVmPtr = handle.kernel.debug_thread_step_vm_ptr?.() ?? 0;
    const now = Date.now();
    const lastStepAge = runningThread?.lastStepAtMs == null ? "null" : String(now - runningThread.lastStepAtMs);
    const lastStatusAge = runningThread?.lastStatusAtMs == null ? "null" : String(now - runningThread.lastStatusAtMs);
    const processPtr = runningVmPtr === 0 ? 0 : handle.kernel.thread_process_ptr(runningVmPtr);
    return `runningVm=0x${runningVmPtr.toString(16)}:runningStatus=${runningStatus}`
      + `:runningLast=${runningThread?.lastStatus ?? "null"}:runningWait=${runningThread?.waitingInWorkerStatus ?? "null"}`
      + `:runningSteps=${runningThread === undefined ? "null" : `${runningThread.stepCount}/${runningThread.statusCount}`}`
      + `:pendingWrites=${runningThread?.pendingGuestMemoryWrites.length ?? "null"}`
      + `:lastStepAge=${lastStepAge}:lastStatusAge=${lastStatusAge}`
      + `:proc=0x${processPtr.toString(16)}`
      + `:initialized=${runningThread === undefined ? "null" : runningThread.initialized ? 1 : 0}`
      + `:ownerAsync=${runningThread?.ownerAsyncStatus ?? "null"}`
      + `:ready=${handle.readyWorkers.length}`
      + `:active=0x${activeStepVmPtr.toString(16)}`;
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
    blockWorker,
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
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
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

  function formatPipeSlotSummary(handle: ProcessHandle, pipeSlot: number): string {
    const maxPipes = handle.kernel.abi_max_pipes();
    const pipeRegionBytes = handle.kernel.kernel_pipe_region_bytes();
    const slotBytes = maxPipes > 0 ? Math.floor(pipeRegionBytes / maxPipes) : 0;
    const dataOffset = 32;
    if (
      pipeSlot <= 0
      || pipeSlot >= maxPipes
      || slotBytes <= dataOffset
      || handle.kernelState.pipeBase < 0
    ) {
      return "pipe-state=invalid";
    }
    const slotBase = handle.kernelState.pipeBase + pipeSlot * slotBytes;
    const view = new DataView(handle.memory.buffer);
    if (slotBase < 0 || slotBase + slotBytes > view.byteLength) {
      return "pipe-state=oob";
    }
    const head = view.getUint32(slotBase, true);
    const tail = view.getUint32(slotBase + 4, true);
    const readOpen = view.getInt32(slotBase + 8, true);
    const writeOpen = view.getInt32(slotBase + 12, true);
    const generation = view.getUint32(slotBase + 20, true);
    const capacity = slotBytes - dataOffset;
    const unread = tail >= head ? tail - head : capacity - head + tail;
    return `pipe-state=h${head}:t${tail}:ro${readOpen}:wo${writeOpen}:u${unread}:gen${generation}`;
  }

  function formatBlockedEpollEntries(handle: ProcessHandle, vmPtr: number, limit = 8): string {
    const maxFds = handle.kernel.abi_max_fds();
    const epollFd = handle.kernel.thread_blocked_fd?.(vmPtr)
      ?? Number(handle.kernel.thread_a0?.(vmPtr) ?? -1n);
    const epollOfdId = (handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0) > 0
      ? handle.kernel.thread_blocked_ofd_id(vmPtr)
      : handle.kernel.process_fd_ofd_id(vmPtr, epollFd);
    if (
      epollFd < 0
      || epollOfdId <= 0
      || epollOfdId >= maxFds
      || handle.kernel.kernel_ofd_fd_type(epollOfdId) !== handle.kernel.abi_fd_type_epoll()
    ) {
      return "none";
    }

    const tableBytes = handle.kernel.kernel_epoll_table_bytes();
    const instanceBytes = handle.kernel.kernel_epoll_instance_bytes();
    const epollIdx = handle.kernel.kernel_ofd_ino(epollOfdId);
    const tablePointerBytes = tableBytes % 8 === 0
      && Math.floor((instanceBytes - 4) / (tableBytes >>> 3)) >= 28
      ? 8
      : 4;
    const tableEntries = Math.floor(tableBytes / tablePointerBytes);
    const entryStride = tableEntries > 0 ? Math.floor((instanceBytes - 4) / tableEntries) : 0;
    const countOffset = entryStride * tableEntries;
    const tablePtr = handle.kernelState.epollTablePtr;
    const view = new DataView(handle.memory.buffer);
    if (
      tableEntries <= 0
      || entryStride < 28
      || epollIdx < 0
      || epollIdx >= tableEntries
      || tablePtr < 0
      || tablePtr + tableBytes > view.byteLength
    ) {
      return `invalid:fd=${epollFd}:ofd=${epollOfdId}:idx=${epollIdx}:stride=${entryStride}`;
    }

    const instancePtr = view.getUint32(tablePtr + epollIdx * tablePointerBytes, true);
    if (instancePtr === 0 || instancePtr + instanceBytes > view.byteLength) {
      return `empty:fd=${epollFd}:ofd=${epollOfdId}:idx=${epollIdx}:ptr=0x${instancePtr.toString(16)}`;
    }
    const rawCount = view.getUint32(instancePtr + countOffset, true);
    const count = Math.min(rawCount, tableEntries, limit);
    const entries: string[] = [];
    const eventfdType = handle.kernel.abi_fd_type_eventfd?.();
    const pipeType = handle.kernel.abi_fd_type_pipe();
    for (let i = 0; i < count; i += 1) {
      const offset = instancePtr + i * entryStride;
      const fd = view.getUint32(offset, true);
      const ofdId = view.getUint32(offset + 4, true);
      const events = view.getUint32(offset + 8, true);
      const data = view.getBigUint64(offset + 16, true);
      const last = view.getUint32(offset + 24, true);
      const hasValidOfd = ofdId > 0 && ofdId < maxFds;
      const type = hasValidOfd ? handle.kernel.kernel_ofd_fd_type(ofdId) : -1;
      const ino = hasValidOfd ? handle.kernel.kernel_ofd_ino(ofdId) : -1;
      const ofdOffset = hasValidOfd ? handle.kernel.kernel_ofd_offset(ofdId) : -1n;
      const readiness = type === eventfdType
        ? `eventfd=${ofdOffset.toString()}`
        : type === pipeType
          ? `pipe=${ino}:${formatPipeSlotSummary(handle, ino)}`
          : "obj=other";
      entries.push(
        `${i}:fd=${fd}:ofd=${ofdId}:ty=${type}:ino=${ino}`
          + `:ev=0x${events.toString(16)}:last=0x${last.toString(16)}`
          + `:data=0x${data.toString(16)}:${readiness}`,
      );
    }
    return `fd=${epollFd}:ofd=${epollOfdId}:idx=${epollIdx}:count=${rawCount}:entries=${entries.join("|") || "none"}`;
  }

  function blockedEpollWatchesPipe(handle: ProcessHandle, vmPtr: number): boolean {
    const maxFds = handle.kernel.abi_max_fds();
    const epollFd = handle.kernel.thread_blocked_fd?.(vmPtr)
      ?? Number(handle.kernel.thread_a0?.(vmPtr) ?? -1n);
    const epollOfdId = (handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0) > 0
      ? handle.kernel.thread_blocked_ofd_id(vmPtr)
      : handle.kernel.process_fd_ofd_id(vmPtr, epollFd);
    if (
      epollFd < 0
      || epollOfdId <= 0
      || epollOfdId >= maxFds
      || handle.kernel.kernel_ofd_fd_type(epollOfdId) !== handle.kernel.abi_fd_type_epoll()
    ) {
      return false;
    }

    const tableBytes = handle.kernel.kernel_epoll_table_bytes();
    const instanceBytes = handle.kernel.kernel_epoll_instance_bytes();
    const epollIdx = handle.kernel.kernel_ofd_ino(epollOfdId);
    const tablePointerBytes = tableBytes % 8 === 0
      && Math.floor((instanceBytes - 4) / (tableBytes >>> 3)) >= 28
      ? 8
      : 4;
    const tableEntries = Math.floor(tableBytes / tablePointerBytes);
    const entryStride = tableEntries > 0 ? Math.floor((instanceBytes - 4) / tableEntries) : 0;
    const countOffset = entryStride * tableEntries;
    const tablePtr = handle.kernelState.epollTablePtr;
    const view = new DataView(handle.memory.buffer);
    if (
      tableEntries <= 0
      || entryStride < 28
      || epollIdx < 0
      || epollIdx >= tableEntries
      || tablePtr < 0
      || tablePtr + tableBytes > view.byteLength
    ) {
      return false;
    }

    const instancePtr = view.getUint32(tablePtr + epollIdx * tablePointerBytes, true);
    if (instancePtr === 0 || instancePtr + instanceBytes > view.byteLength) {
      return false;
    }
    const rawCount = view.getUint32(instancePtr + countOffset, true);
    const count = Math.min(rawCount, tableEntries);
    for (let i = 0; i < count; i += 1) {
      const ofdId = view.getUint32(instancePtr + i * entryStride + 4, true);
      if (
        ofdId > 0
        && ofdId < maxFds
        && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
      ) {
        return true;
      }
    }
    return false;
  }

  function collectBlockedEpollWatchedOfdIdsByType(
    handle: ProcessHandle,
    vmPtr: number,
    types: readonly number[],
  ): number[] {
    const maxFds = handle.kernel.abi_max_fds();
    const targetTypes = new Set(types);
    const epollFd = handle.kernel.thread_blocked_fd?.(vmPtr)
      ?? Number(handle.kernel.thread_a0?.(vmPtr) ?? -1n);
    const epollOfdId = (handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0) > 0
      ? handle.kernel.thread_blocked_ofd_id(vmPtr)
      : handle.kernel.process_fd_ofd_id(vmPtr, epollFd);
    if (
      epollFd < 0
      || epollOfdId <= 0
      || epollOfdId >= maxFds
      || handle.kernel.kernel_ofd_fd_type(epollOfdId) !== handle.kernel.abi_fd_type_epoll()
    ) {
      return [];
    }

    const tableBytes = handle.kernel.kernel_epoll_table_bytes();
    const instanceBytes = handle.kernel.kernel_epoll_instance_bytes();
    const epollIdx = handle.kernel.kernel_ofd_ino(epollOfdId);
    const tablePointerBytes = tableBytes % 8 === 0
      && Math.floor((instanceBytes - 4) / (tableBytes >>> 3)) >= 28
      ? 8
      : 4;
    const tableEntries = Math.floor(tableBytes / tablePointerBytes);
    const entryStride = tableEntries > 0 ? Math.floor((instanceBytes - 4) / tableEntries) : 0;
    const countOffset = entryStride * tableEntries;
    const tablePtr = handle.kernelState.epollTablePtr;
    const view = new DataView(handle.memory.buffer);
    if (
      tableEntries <= 0
      || entryStride < 28
      || epollIdx < 0
      || epollIdx >= tableEntries
      || tablePtr < 0
      || tablePtr + tableBytes > view.byteLength
    ) {
      return [];
    }

    const instancePtr = view.getUint32(tablePtr + epollIdx * tablePointerBytes, true);
    if (instancePtr === 0 || instancePtr + instanceBytes > view.byteLength) {
      return [];
    }
    const rawCount = view.getUint32(instancePtr + countOffset, true);
    const count = Math.min(rawCount, tableEntries);
    const ofdIds: number[] = [];
    for (let i = 0; i < count; i += 1) {
      const ofdId = view.getUint32(instancePtr + i * entryStride + 4, true);
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
    const previousBlocked = handle.blockedWorkers.get(worker);
    const sameBlockedThread = previousBlocked !== undefined
      && previousBlocked.vmPtr === vmPtr
      && previousBlocked.status === status;
    clearBlockedWorker(handle, worker);
    const blocked: BlockedWorkerState = {
      vmPtr,
      status,
      retryTimer: null,
      timeoutExpired: false,
      timeoutDeadlineMs: null,
      sleepDeadlineTicks: null,
      epollTimerfdDeadlineTicks: sameBlockedThread ? previousBlocked.epollTimerfdDeadlineTicks : null,
      rpcPending: sameBlockedThread ? previousBlocked.rpcPending : false,
      rpcGeneration: sameBlockedThread ? previousBlocked.rpcGeneration : 0,
      skipNextNetworkPump: sameBlockedThread ? previousBlocked.skipNextNetworkPump : false,
      pipeSyncedBeforePassive: sameBlockedThread ? previousBlocked.pipeSyncedBeforePassive : false,
      pipeChildExitResumeKey: sameBlockedThread ? previousBlocked.pipeChildExitResumeKey : null,
      pipeEofChildExitDeferrals: sameBlockedThread ? previousBlocked.pipeEofChildExitDeferrals : 0,
      lastPipeRetryLifecycleEvent: sameBlockedThread ? previousBlocked.lastPipeRetryLifecycleEvent : undefined,
      lastWait4RetryLifecycleEvent: sameBlockedThread ? previousBlocked.lastWait4RetryLifecycleEvent : undefined,
    };
    handle.blockedWorkers.set(worker, blocked);
    if (sameBlockedThread && blocked.rpcPending) {
      return;
    }
    if (status === handle.kernel.abi_status_sleep()) {
      blocked.sleepDeadlineTicks = sleepDeadlineTicks(handle, vmPtr);
      scheduleBlockedRetry(handle, worker, sleepRetryDelayMs(handle, blocked.sleepDeadlineTicks), true);
      return;
    }
    if (status === handle.kernel.abi_status_epoll_wait()) {
      const timeoutMs = Number(handle.kernel.thread_epoll_wait_timeout(vmPtr));
      if (timeoutMs >= 0) {
        blocked.epollTimerfdDeadlineTicks = null;
        scheduleBlockedRetry(handle, worker, Math.max(0, timeoutMs), true);
      } else {
        blocked.epollTimerfdDeadlineTicks = epollTimerfdDeadlineTicks(handle, vmPtr);
        if (blocked.epollTimerfdDeadlineTicks !== null) {
          scheduleBlockedRetry(handle, worker, sleepRetryDelayMs(handle, blocked.epollTimerfdDeadlineTicks), true);
        } else if (handle.kernel.thread_process_thread_count(vmPtr) > 1) {
          scheduleBlockedRetry(handle, worker, 10);
        }
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
      && (!handle.kernelWorkerRequest || handle.pid <= 0)
      && blocked.pipeSyncedBeforePassive
      && pollTimeoutMs < 0
      && (pollReadyCount === undefined || pollReadyCount <= 0)
      && handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr) !== 1
      && handle.kernel.thread_blocked_pipe_write_ready?.(vmPtr) !== 1
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
      && !directPipeReadHasSynchronizedEof(handle, status, vmPtr)
    ) {
      return;
    }
    scheduleBlockedRetry(handle, worker, status === handle.kernel.abi_status_wait4() ? 10 : 1);
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
      if (blocked.status === handle.kernel.abi_status_epoll_wait()) {
        appendWorkerDebugLog(
          handle,
          `blocked-epoll-ready-fds:${handle.pid}:vm=0x${vmPtr.toString(16)}:${formatThreadFdTableSummary(handle, vmPtr)}`,
        );
      }
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
      if (remainingThreads <= 1 || !hasLiveThreads) {
        const exitVmPtr = selectFaultExitVmPtr(handle, vmPtr);
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `complete-exit-request:scheduler-fault:${handle.pid}:vm=0x${vmPtr.toString(16)}`
            + `:exitVm=0x${exitVmPtr.toString(16)}`
            + `:priority=${faultExitVmPriority(handle, exitVmPtr)}`
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

    blocked.status = status;
    if (status === handle.kernel.abi_status_epoll_wait()) {
      const timeoutMs = Number(handle.kernel.thread_epoll_wait_timeout(vmPtr));
      if (timeoutMs >= 0) {
        scheduleBlockedRetry(handle, worker, Math.max(0, timeoutMs), true);
      } else if (handle.kernel.thread_process_thread_count(vmPtr) > 1) {
        scheduleBlockedRetry(handle, worker, 10);
        pumpProcess(handle);
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
    if (
      blocked.status === handle.kernel.abi_status_epoll_wait()
      && hasPendingCloneChildInitResume(handle)
    ) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `epoll-retry-skip:${handle.pid}:clone-child-init-pending`,
      );
      wakeRuntimeBridge(handle.runtimeBridge);
      scheduleBlockedRetry(handle, worker, 10, blocked.timeoutExpired);
      return;
    }
    markEpollTimeoutZeroExpired(handle, blocked);
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
      const staleGate = clearStaleForkSharedSyncGate(handle, forkSharedSyncPromise);
      if (staleGate !== null) {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-retry-cleared:${handle.pid}:${staleGate}`);
      } else {
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
    }
    clearStaleRunningWorker(handle, worker);
    if (handle.runningWorker !== null) {
      if (blocked.status === handle.kernel.abi_status_epoll_wait()) {
        wakeRuntimeBridge(handle.runtimeBridge);
      }
      if (wait4HasLocalExitRecord || pipeHasLocalExitRecord) {
        wakeRuntimeBridge(handle.runtimeBridge);
      }
      const runningThread = threadWorkerStates.get(handle.runningWorker);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `wait4-retry-skip:${handle.pid}:running=1`
          + `:${formatRunningRetryBlocker(handle, runningThread)}`,
      );
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
      const epollRetryTimeoutMs = Number(handle.kernel.thread_epoll_wait_timeout(blocked.vmPtr));
      const epollRetryTimerfdDeadline = epollTimerfdDeadlineTicks(handle, blocked.vmPtr);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `epoll-retry-rpc:${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}`
          + `:expired=${blocked.timeoutExpired ? 1 : 0}:timeout=${epollRetryTimeoutMs}`
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
          markEpollTimeoutZeroExpired(handle, latestBlocked);
          if (
            !latestBlocked.timeoutExpired
            && latestBlocked.epollTimerfdDeadlineTicks == null
            && Number(handle.kernel.thread_epoll_wait_timeout(latestBlocked.vmPtr)) < 0
          ) {
            const deadlineTicks = epollTimerfdDeadlineTicks(handle, latestBlocked.vmPtr);
            if (deadlineTicks !== null) {
              latestBlocked.epollTimerfdDeadlineTicks = deadlineTicks;
              deps.appendRuntimeLifecycleEvent(
                handle.runtimeId,
                `epoll-retry-deadline:${handle.pid}:vm=0x${latestBlocked.vmPtr.toString(16)}:ticks=${deadlineTicks.toString()}`,
              );
              scheduleBlockedRetry(handle, worker, sleepRetryDelayMs(handle, deadlineTicks), true);
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
            scheduleBlockedRetry(handle, worker, 1, latestBlocked.timeoutExpired);
            return;
          }
          attachKernelRuntimeState(handle.kernel, handle.kernelState);
          if (latestBlocked.timeoutExpired && latestBlocked.epollTimerfdDeadlineTicks != null) {
            handle.kernel.sync_monotonic_ticks_floor?.(latestBlocked.epollTimerfdDeadlineTicks);
            latestBlocked.timeoutExpired = false;
            latestBlocked.epollTimerfdDeadlineTicks = null;
          }
          if (latestBlocked.timeoutExpired) {
            handle.kernel.thread_resume_epoll_wait_timeout(latestBlocked.vmPtr);
            latestBlocked.timeoutExpired = false;
          } else {
            handle.kernel.thread_resume_blocked_syscall(latestBlocked.vmPtr);
          }
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `epoll-retry-result:${handle.pid}:vm=0x${latestBlocked.vmPtr.toString(16)}`
              + `:status=${handle.kernel.thread_status(latestBlocked.vmPtr)}`
              + `:a0=${handle.kernel.thread_a0?.(latestBlocked.vmPtr)?.toString() ?? "n/a"}`,
          );
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
          pumpProcess(handle);
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
          if (
            outcome.kind === "blocked"
            && outcome.status === handle.kernel.abi_status_pipe_wait()
          ) {
            pumpProcess(handle);
          }
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
    if (
      blocked.status === handle.kernel.abi_status_epoll_wait()
      && blocked.timeoutExpired
      && blocked.epollTimerfdDeadlineTicks != null
    ) {
      handle.kernel.sync_monotonic_ticks_floor?.(blocked.epollTimerfdDeadlineTicks);
      blocked.timeoutExpired = false;
      blocked.epollTimerfdDeadlineTicks = null;
      handle.kernel.thread_resume_blocked_syscall(blocked.vmPtr);
    } else if (blocked.status === handle.kernel.abi_status_epoll_wait() && blocked.timeoutExpired) {
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
      applyLocalChildExitRecords(handle, { force: true });
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
