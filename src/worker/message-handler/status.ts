import {
  attachKernelRuntimeState,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import {
  drainProcessOutput,
  flushStdin,
} from "../stdio.js";
import {
  applyStatusPipeSlots,
  statusKernelWorkerOwnsPipeState,
} from "./status/pipe-slots.js";
import { processHasLiveThreads } from "../live-threads.js";
import { adoptKernelStateForHandle } from "../sync.js";
import {
  pruneStatusNetworkConnectionsFromFdEntries,
} from "./status/network-recv.js";
import { recordWorkerMessageDebugEvent, type ProcessHandle } from "../types.js";
import { createWorkerOwnedStateSyncHandler } from "./status/owned-state-sync.js";
import { createWorkerStatusTransitionHandler } from "./status/transitions.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "./types.js";
import { formatCompactDebugSuffix, readProcessPidField, WRITE_DEBUG_KEYS } from "../diagnostics/format.js";
import { createWorkerWaitInWorkerStatusHandler } from "./status/wait-in-worker.js";
import {
  appendStatusSocketDebug,
  formatLifecycleFdTrace,
  formatStatusFdEntries,
  pipeSlotLifecycleDebugEnabled,
  shouldRecordStatusFdLifecycle,
  STATUS_SOCKET_DEBUG_LOG_ENABLED,
} from "./status/debug.js";
import {
  hasSuspiciousLowStateDebug,
  incrementBoundedCounter,
  readLocalProcessPid,
} from "./status/route-state.js";
import {
  applyThreadGuestMemoryWrites,
  applyThreadKernelMemoryWrites,
  applyThreadSocketState,
} from "./status/socket-state.js";
import { handleOwnerAsyncStatusReturn } from "./status/owner-async.js";
import { applyStatusFdEntries } from "./status/fd-entries.js";
import { applyStatusOfdSlots } from "./status/ofd-slots.js";

interface ThreadRegisterSnapshot {
  vmPtr: number;
  pc: bigint;
  sp: bigint;
  s11: bigint;
}


export function createWorkerStatusMessageHandler(deps: WorkerMessageHandlerDeps) {
  const handleWaitInWorkerStatus = createWorkerWaitInWorkerStatusHandler(deps);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps);
  const handleStatusTransition = createWorkerStatusTransitionHandler(deps);

  return function handleWorkerStatusMessage(
    handle: ProcessHandle,
    worker: Worker,
    msg: ThreadWorkerStatusMessage,
  ): void {
    const thread = deps.threadWorkerStates.get(worker);
    if (!thread) {
      recordWorkerMessageDebugEvent(
        handle.workerMessageDebugStats,
        "statusHandlerNoThreadDrops",
        `status-no-thread:${handle.pid}:${msg.status}`,
      );
      return;
    }
    if (msg.generation !== undefined && msg.generation !== thread.generation) {
      if (handle.runningWorker === worker && thread.runningGeneration === msg.generation) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
      }
      deps.appendWorkerDebugLog(
        handle,
        `status-stale-generation:0x${thread.vmPtr.toString(16)}`
          + `:msg=${msg.generation}:current=${thread.generation}:status=${msg.status}`,
      );
      return;
    }
    const status = msg.status;
    const processPidField = readProcessPidField(msg.debug);
    const localProcessPid = readLocalProcessPid(handle, msg.vmPtr);
    const transientProcessOwnerMismatch = processPidField !== null
      ? processPidField !== handle.pid
      : localProcessPid !== null && localProcessPid !== handle.pid;
    const activeVforkWorker = handle.vforkSnapshot !== null && handle.vforkWorker === worker;
    const statusBelongsToHandle = !transientProcessOwnerMismatch || activeVforkWorker;
    if (handleOwnerAsyncStatusReturn({
      handle,
      worker,
      thread,
      msg,
      statusBelongsToHandle,
      deps,
    })) {
      return;
    }
    const previousWaitingStatus = thread.waitingInWorkerStatus;
    thread.waitingInWorkerStatus = null;
    if (status === handle.kernel.abi_status_execve_spawn()) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `status-execve-spawn-seen:${handle.pid}:0x${msg.vmPtr.toString(16)}`
          + `:owner=${thread.ownerPid}`
          + `:field=${processPidField ?? "n/a"}`
          + `:local=${localProcessPid ?? "n/a"}`
          + `:kind=${handle.kernel.thread_spawn_kind?.(msg.vmPtr) ?? -1}`
          + `:child=${handle.kernel.thread_spawn_child_pid?.(msg.vmPtr) ?? -1}`,
      );
    }
    if (statusBelongsToHandle) {
      applyStatusOfdSlots(handle, msg);
      applyStatusFdEntries(handle, msg);
      pruneStatusNetworkConnectionsFromFdEntries(handle, msg, deps);
    }
    if (STATUS_SOCKET_DEBUG_LOG_ENABLED && msg.fdEntries) {
      appendStatusSocketDebug(
        handle,
        `status-fds:${handle.pid}:0x${msg.vmPtr.toString(16)}:status=${status}:${formatStatusFdEntries(handle, msg)}`,
      );
    }
    if (shouldRecordStatusFdLifecycle(handle, status, msg)) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `status-fds:${handle.pid}:0x${msg.vmPtr.toString(16)}:status=${status}:${formatStatusFdEntries(handle, msg)}`,
      );
    }
    thread.statusCount += 1;
    recordWorkerMessageDebugEvent(handle.workerMessageDebugStats, "statusHandled");
    thread.statusCounts[String(status)] = (thread.statusCounts[String(status)] ?? 0) + 1;
    thread.lastStatus = status;
    const statusAtMs = Date.now();
    thread.lastStatusAtMs = statusAtMs;
    if (thread.runningGeneration === msg.generation && thread.lastStepAtMs !== null) {
      const stepWallMs = Math.max(0, statusAtMs - thread.lastStepAtMs);
      thread.lastStepDurationMs = stepWallMs;
      thread.totalStepWallMs += stepWallMs;
      thread.maxStepWallMs = Math.max(thread.maxStepWallMs, stepWallMs);
    }
    thread.lastStatusPc = msg.pc;
    thread.lastStatusSp = msg.sp ?? null;
    thread.lastStatusS11 = msg.s11 ?? null;
    if (msg.forkStackSnapshot) {
      thread.forkStackSnapshot = msg.forkStackSnapshot;
    }
    if (msg.processState) {
      thread.execveProcessState = msg.processState;
    }
    incrementBoundedCounter(thread.hotStatusPcs, `0x${msg.pc.toString(16)}`);
    const faultStatus = handle.kernel.abi_status_fault?.();
    const suspiciousLowStateDebug = hasSuspiciousLowStateDebug(msg.debug);
    deps.appendWorkerDebugLog(
      handle,
      `status-route:0x${thread.vmPtr.toString(16)}`
        + `:owner_pid=${thread.ownerPid}`
        + `:handle_pid=${handle.pid}`
        + `:generation=${msg.generation}`
        + `:status=${status}`,
    );
    if (!(status === faultStatus && suspiciousLowStateDebug && handle.lastDebugMessage)) {
      handle.lastDebugMessage = msg.debug;
    }
    deps.appendWorkerDebugLog(
      handle,
      `status:0x${thread.vmPtr.toString(16)}@${msg.status}${formatCompactDebugSuffix(msg.debug)}`,
    );
    if (
      status === handle.kernel.abi_status_ok()
      && (
        msg.pc < 0x1000n
        || (msg.sp ?? 0n) === 0n
        || (msg.s11 ?? 0n) === 0n
      )
    ) {
      deps.appendWorkerDebugLog(
        handle,
        `status_message_break:0x${thread.vmPtr.toString(16)}`
          + `:pc=0x${msg.pc.toString(16)}`
          + `:ra=0x${(msg.ra ?? 0n).toString(16)}`
          + `:sp=0x${(msg.sp ?? 0n).toString(16)}`
          + `:s11=0x${(msg.s11 ?? 0n).toString(16)}`,
      );
    }
    if (msg.debug && suspiciousLowStateDebug) {
      deps.appendWorkerDebugLog(
        handle,
        `status_debug:0x${thread.vmPtr.toString(16)}:${msg.debug}`,
      );
    }
    if (processPidField !== null && processPidField !== handle.pid) {
      deps.appendWorkerDebugLog(
        handle,
        `status_debug:0x${thread.vmPtr.toString(16)}:handle_pid=${handle.pid}:process_pid_field=${processPidField}`,
      );
    }
    const writeDebugSuffix = formatCompactDebugSuffix(msg.debug, WRITE_DEBUG_KEYS);
    if (writeDebugSuffix !== "") {
      deps.appendWorkerDebugLog(handle, `write:0x${thread.vmPtr.toString(16)}${writeDebugSuffix}`);
    }
    if (msg.fdTraceDebug) {
      deps.appendWorkerDebugLog(handle, `fdtrace:0x${thread.vmPtr.toString(16)}:${msg.fdTraceDebug}`);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `fdtrace:${handle.pid}:0x${thread.vmPtr.toString(16)}:${formatLifecycleFdTrace(msg.fdTraceDebug)}`,
      );
    }

    const isRunningWorker = handle.runningWorker === worker;
    if (!isRunningWorker) {
      recordWorkerMessageDebugEvent(
        handle.workerMessageDebugStats,
        "statusNotRunning",
        `status-not-running:${handle.pid}:0x${thread.vmPtr.toString(16)}:${status}`,
      );
    }
    const deferLocalPipeDrain = statusKernelWorkerOwnsPipeState(handle);
    const adoptPreSnapshots: ThreadRegisterSnapshot[] = [];
    for (const candidate of handle.workers) {
      const candidateThread = deps.threadWorkerStates.get(candidate);
      if (!candidateThread) {
        continue;
      }
      adoptPreSnapshots.push({
        vmPtr: candidateThread.vmPtr,
        pc: handle.kernel.thread_pc(candidateThread.vmPtr),
        sp: handle.kernel.thread_sp(candidateThread.vmPtr),
        s11: handle.kernel.thread_s11?.(candidateThread.vmPtr) ?? 0n,
      });
    }
    const shouldAdoptThreadKernelState = (
      status !== handle.kernel.abi_status_clone_spawn()
      && statusBelongsToHandle
    );
    if (shouldAdoptThreadKernelState) {
      handle.kernelState = adoptKernelStateForHandle(handle, msg.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      applyThreadKernelMemoryWrites(handle, msg, deps.appendRuntimeLifecycleEvent);
      applyThreadGuestMemoryWrites(handle, msg, deps.appendRuntimeLifecycleEvent);
      applyStatusPipeSlots(
        handle,
        msg.pipeSlots,
        pipeSlotLifecycleDebugEnabled() ? deps.appendRuntimeLifecycleEvent : undefined,
      );
      applyStatusOfdSlots(handle, msg);
      applyStatusFdEntries(handle, msg);
      applyThreadSocketState(handle, msg);
      for (const pre of adoptPreSnapshots) {
        const postPc = handle.kernel.thread_pc(pre.vmPtr);
        const postSp = handle.kernel.thread_sp(pre.vmPtr);
        const postS11 = handle.kernel.thread_s11?.(pre.vmPtr) ?? 0n;
        const becameBroken = (
          pre.pc >= 0x1000n
          && pre.sp !== 0n
          && pre.s11 !== 0n
          && (postPc < 0x1000n || postSp === 0n || postS11 === 0n)
        );
        if (!becameBroken) {
          continue;
        }
        deps.appendWorkerDebugLog(
          handle,
          `status_adopt_break:src=0x${thread.vmPtr.toString(16)} target=0x${pre.vmPtr.toString(16)} pre_pc=0x${pre.pc.toString(16)} pre_sp=0x${pre.sp.toString(16)} pre_s11=0x${pre.s11.toString(16)} post_pc=0x${postPc.toString(16)} post_sp=0x${postSp.toString(16)} post_s11=0x${postS11.toString(16)} incoming_sig_slots=0x${msg.kernelState.sigSlotsPtr.toString(16)} incoming_process_pending=0x${msg.kernelState.processPendingPtr.toString(16)} incoming_process_runtime=0x${msg.kernelState.processRuntimeStatePtr.toString(16)}`,
        );
      }
    } else if (status === handle.kernel.abi_status_fs()) {
      applyThreadKernelMemoryWrites(handle, msg, deps.appendRuntimeLifecycleEvent);
      applyThreadGuestMemoryWrites(handle, msg, deps.appendRuntimeLifecycleEvent);
    }
    if (msg.waitInWorker) {
      handleWaitInWorkerStatus(handle, worker, thread, msg, isRunningWorker);
      return;
    }
    if (previousWaitingStatus === handle.kernel.abi_status_futex_wait()) {
      deps.appendWorkerDebugLog(
        handle,
        `resume:0x${thread.vmPtr.toString(16)}@${previousWaitingStatus}->${status}${formatCompactDebugSuffix(msg.debug)}`,
      );
    }
    deps.clearBlockedWorker(handle, worker);
    const wroteToPipe = flushStdin(handle);
    const drainedPipe = deferLocalPipeDrain || statusKernelWorkerOwnsPipeState(handle)
      ? false
      : drainProcessOutput(handle);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState);
    if (isRunningWorker) {
      handle.runningWorker = null;
      thread.runningGeneration = null;
      recordWorkerMessageDebugEvent(
        handle.workerMessageDebugStats,
        "statusClearedRunning",
        `status-cleared-running:${handle.pid}:0x${thread.vmPtr.toString(16)}:${status}`,
      );
    }

    let transitionStatus = status;
    let transitionMsg = msg;
    if (status === faultStatus && handle.kernel.thread_status) {
      const currentStatus = handle.kernel.thread_status(thread.vmPtr);
      const currentStatusHasLiveThread = processHasLiveThreads(handle.kernel, thread.vmPtr);
      if (
        currentStatus !== status
        && currentStatus !== handle.kernel.abi_status_fault()
        && currentStatusHasLiveThread
        && !activeVforkWorker
      ) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `status-stale-fault-current:${handle.pid}:0x${thread.vmPtr.toString(16)}:${status}->${currentStatus}`,
        );
        deps.appendWorkerDebugLog(
          handle,
          `status-stale-fault-current:0x${thread.vmPtr.toString(16)}:${status}->${currentStatus}`,
        );
        transitionStatus = currentStatus;
        transitionMsg = {
          ...msg,
          status: currentStatus,
        };
      }
    }

    const completedSyncEffects = (
      transitionStatus === handle.kernel.abi_status_ok()
      && transitionMsg.syncEffects === undefined
    )
      ? deps.statusOkCompletedSyncEffects
      : (transitionMsg.syncEffects ?? 0);
    const continueAfterOwnedStateSync = (): void => {
      handleStatusTransition({
        handle,
        worker,
        thread,
        msg: transitionMsg,
        status: transitionStatus,
        wroteToPipe,
        drainedPipe,
        completedSyncEffects,
      });
    };
    if (maybeHandleOwnedStateSync({
      handle,
      worker,
      thread,
      status: transitionStatus,
      syscallNr: transitionMsg.syscallNr ?? null,
      rawSyncEffects: transitionMsg.syncEffects,
      completedSyncEffects,
      latestFdTrace: transitionMsg.fdTraceSyscallNr === undefined
        ? null
        : {
            nr: transitionMsg.fdTraceSyscallNr,
            arg0OfdId: transitionMsg.fdTraceArg0OfdId ?? 0,
            arg0OfdType: transitionMsg.fdTraceArg0OfdType ?? 0,
            arg1: transitionMsg.fdTraceArg1 ?? null,
            arg2OfdId: transitionMsg.fdTraceArg2OfdId ?? 0,
            arg2OfdType: transitionMsg.fdTraceArg2OfdType ?? 0,
            result: transitionMsg.fdTraceResult ?? null,
          },
      isRunningWorker,
      wroteToPipe,
      drainedPipe,
      continueAfterOwnedStateSync,
    })) {
      return;
    }
    continueAfterOwnedStateSync();
  };
}
