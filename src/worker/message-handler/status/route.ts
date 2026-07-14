import { hostExecutionState } from "../../host-execution-state.js";
import { processWorkers } from "../../process-worker-state.js";
import {
  attachKernelRuntimeState,
} from "../../../abi.js";
import { publishKernelRuntimeState } from "../../../bridge/runtime-state.js";
import {
  drainProcessOutput,
  flushStdin,
} from "../../stdio.js";
import {
  applyThreadPipeSlots,
} from "../../ownership/pipe-control.js";
import { reconcileKernelSharePipeControl } from "../../kernel-share/pipe.js";
import { processHasKernelWorkerOwnership } from "../../kernel-worker-ownership.js";
import { adoptKernelStateForHandle } from "../../sync.js";
import {
  pruneStatusNetworkConnectionsFromFdEntries,
} from "./network-recv.js";
import { recordWorkerMessageDebugEvent, type ProcessHandle } from "../../types.js";
import { createWorkerOwnedStateSyncHandler } from "./owned-state-sync.js";
import { createWorkerStatusTransitionHandler } from "./transitions.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "../types.js";
import { formatCompactDebugSuffix, readProcessPidField, WRITE_DEBUG_KEYS } from "../../diagnostics/format.js";
import { createWorkerWaitInWorkerStatusHandler } from "./wait-in-worker.js";
import {
  appendStatusSocketDebug,
  formatLifecycleFdTrace,
  formatStatusFdEntries,
  pipeSlotLifecycleDebugEnabled,
  shouldRecordStatusFdLifecycle,
  STATUS_SOCKET_DEBUG_LOG_ENABLED,
} from "./debug.js";
import {
  hasSuspiciousLowStateDebug,
  readLocalProcessPid,
  statusProcessBelongsToHandle,
  statusProcessIncarnationMatches,
} from "./route-state.js";
import {
  processLastDebugMessage,
  setProcessLastDebugMessage,
} from "../../process-debug-state.js";
import { workerMessageDebugStats } from "../../worker-debug-stats.js";
import { setThreadExecveProcessState } from "../../thread-execve-state.js";
import {
  getThreadActivityState,
  incrementBoundedActivityCounter,
  pushRecentStatusActivity,
} from "../../thread-activity-state.js";
import {
  applyThreadGuestMemoryWrites,
  applyThreadKernelMemoryWrites,
  applyThreadSocketState,
} from "./socket-state.js";
import { handleOwnerAsyncStatusReturn } from "./owner-async.js";
import {
  applyStatusFdEntries,
  markStatusFdChangesForNextStep,
} from "./fd-entries.js";
import {
  clearThreadRunningGeneration,
  getThreadGeneration,
  getThreadOwnerPid,
  getThreadRunningGeneration,
  clearThreadWaitingInWorkerStatus,
  getThreadOwnerAsyncStatus,
  getThreadWaitingInWorkerStatus,
} from "../../thread-host-state.js";
import {
  applyStatusEventfdTraceOffset,
  applyStatusOfdSlots,
} from "./ofd-slots.js";
import {
  canonicalFdTableVmPtrForThread,
  supervisorProcessFdFlagsForFd,
  supervisorProcessOfdForFd,
} from "../../ownership/fd-table.js";
import { resolveWorkerMessageHandlerSyncEffects } from "../sync-effects.js";
import {
  readSupervisorFaultExitDecision,
  submitSupervisorShadowStatusEvent,
  supervisorFaultExitKindProcessExit,
} from "../../supervisor-shadow.js";
import {
  supervisorProcessStatusIsCloneSpawn,
  supervisorProcessStatusIsExecveSpawn,
  supervisorProcessStatusRouteActions,
} from "../../supervisor-status.js";
import { clearSupervisorHostTimer } from "../../supervisor-host-op.js";
import {
  takeChangedFdEntriesWithBaselineSincePosted,
  takeChangedOfdSlotsWithBaselineSincePosted,
} from "../../thread-step-payload.js";

const CLONE_SPAWN_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_CLONE_SPAWN_DEBUG ?? "") === "1";
const FS_LIFECYCLE_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_LIFECYCLE_DEBUG ?? "") === "1";
const FD_CONTROL_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FD_CONTROL_DEBUG ?? "") === "1";

interface ThreadRegisterSnapshot {
  vmPtr: number;
  pc: bigint;
  sp: bigint;
  s11: bigint;
}

function appendFsStatusRouteTiming(
  deps: WorkerMessageHandlerDeps,
  handle: ProcessHandle,
  phase: string,
  vmPtr: number,
  detail = "",
): void {
  if (!FS_LIFECYCLE_DEBUG_ENABLED) {
    return;
  }
  const event = `fs-status-route-${phase}:${handle.pid}:0x${vmPtr.toString(16)}:t=${Date.now()}${detail}`;
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
  console.error(`[tidemark-fs] ${event}`);
}

function statusRouteFdTraceIsRelevant(msg: ThreadWorkerStatusMessage): boolean {
  return msg.syscallNr === 19n
    || msg.syscallNr === 20n
    || msg.syscallNr === 21n
    || msg.syscallNr === 94n
    || (msg.fdEntries ?? []).some((entry) => entry.fd >= 3 && entry.fd <= 5 && entry.ofdId !== 0);
}

function formatMessageFdFocus(msg: ThreadWorkerStatusMessage): string {
  return [3, 4, 5].map((fd) => {
    const entry = msg.fdEntries?.find((candidate) => candidate.fd === fd);
    return entry === undefined
      ? `${fd}=missing`
      : `${fd}=${entry.ofdId}/fl${entry.fdFlags}/gen${entry.generation ?? "n/a"}`;
  }).join(",");
}

function formatKernelFdFocus(handle: ProcessHandle, vmPtr: number): string {
  return [3, 4, 5].map((fd) => {
    const generation = handle.kernel.process_fd_generation?.(vmPtr, fd);
    return `${fd}=${supervisorProcessOfdForFd(handle, vmPtr, fd)}`
      + `/fl${supervisorProcessFdFlagsForFd(handle, vmPtr, fd)}`
      + `/gen${generation ?? "n/a"}`;
  }).join(",");
}

function appendStatusRouteFdDebug(
  phase: string,
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  detail = "",
): void {
  if (!FD_CONTROL_DEBUG_ENABLED || !statusRouteFdTraceIsRelevant(msg)) {
    return;
  }
  const canonicalVmPtr = canonicalFdTableVmPtrForThread(handle, msg.vmPtr);
  const canonicalSuffix = canonicalVmPtr === msg.vmPtr
    ? ""
    : `:canonical=0x${canonicalVmPtr.toString(16)}:canonicalLocal=${formatKernelFdFocus(handle, canonicalVmPtr)}`;
  console.error(
    "[tidemark-fd-route] "
      + `phase=${phase}:handle=${handle.pid}:vm=0x${msg.vmPtr.toString(16)}`
      + `:status=${msg.status}:syscall=${msg.syscallNr?.toString() ?? "n/a"}`
      + detail
      + `:msg=${formatMessageFdFocus(msg)}`
      + `:local=${formatKernelFdFocus(handle, msg.vmPtr)}`
      + canonicalSuffix,
  );
}

export function createWorkerStatusMessageHandler(deps: WorkerMessageHandlerDeps) {
  const syncEffects = resolveWorkerMessageHandlerSyncEffects(deps);
  const handleWaitInWorkerStatus = createWorkerWaitInWorkerStatusHandler(deps);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps);
  const handleStatusTransition = createWorkerStatusTransitionHandler(deps);

  return function handleWorkerStatusMessage(
    handle: ProcessHandle,
    worker: Worker,
    msg: ThreadWorkerStatusMessage,
  ): void {
    const handlerStartMs = FS_LIFECYCLE_DEBUG_ENABLED ? Date.now() : 0;
    const thread = deps.threadWorkerStates.get(worker);
    if (!thread) {
      if (CLONE_SPAWN_DEBUG_ENABLED && supervisorProcessStatusIsCloneSpawn(handle.kernel, msg.status)) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `clone-status-no-thread:${handle.pid}:0x${msg.vmPtr.toString(16)}`
            + `:status=${msg.status}:generation=${msg.generation ?? "n/a"}`,
        );
      }
      recordWorkerMessageDebugEvent(
        workerMessageDebugStats(handle),
        "statusHandlerNoThreadDrops",
        `status-no-thread:${handle.pid}:${msg.status}`,
      );
      return;
    }
    const threadGeneration = getThreadGeneration(thread);
    if (msg.generation !== undefined && msg.generation !== threadGeneration) {
      if (hostExecutionState(handle).runningWorker === worker && getThreadRunningGeneration(thread) === msg.generation) {
        hostExecutionState(handle).runningWorker = null;
        clearThreadRunningGeneration(thread);
      }
      deps.appendWorkerDebugLog(
        handle,
        `status-stale-generation:0x${thread.vmPtr.toString(16)}`
          + `:msg=${msg.generation}:current=${threadGeneration}:status=${msg.status}`,
      );
      if (CLONE_SPAWN_DEBUG_ENABLED && supervisorProcessStatusIsCloneSpawn(handle.kernel, msg.status)) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `clone-status-stale-generation:${handle.pid}:0x${thread.vmPtr.toString(16)}`
            + `:msg=${msg.generation}:current=${threadGeneration}`,
        );
      }
      return;
    }
    const status = msg.status;
    const pendingWaitingStatus = getThreadWaitingInWorkerStatus(thread);
    if (pendingWaitingStatus !== null && status !== pendingWaitingStatus) {
      clearSupervisorHostTimer(handle, thread.vmPtr);
    }
    const fsStatus = handle.kernel.abi_status_fs?.();
    const traceFsStatus = fsStatus !== undefined && status === fsStatus;
    const traceFsStatusTiming = traceFsStatus && FS_LIFECYCLE_DEBUG_ENABLED;
    if (traceFsStatusTiming) {
      appendFsStatusRouteTiming(deps, handle, "entry", msg.vmPtr);
    }
    const processPidField = readProcessPidField(msg.debug);
    const trustedProcessPidField = processPidField !== null && processPidField > 0
      ? processPidField
      : null;
    const localProcessPid = readLocalProcessPid(handle, msg.vmPtr);
    const localProcessPtr = handle.kernel.thread_process_ptr?.(msg.vmPtr) ?? 0;
    const statusProcessIdentityMatches = statusProcessIncarnationMatches(
      msg.processPtr,
      msg.processPid,
      localProcessPtr,
      localProcessPid,
    );
    const transientProcessOwnerMismatch = !statusProcessBelongsToHandle(
      handle.pid,
      localProcessPid,
      trustedProcessPidField,
    );
    const activeVforkWorker = hostExecutionState(handle).vforkSnapshot !== null && hostExecutionState(handle).vforkWorker === worker;
    const statusBelongsToHandle = !transientProcessOwnerMismatch || activeVforkWorker;
    const faultStatus = handle.kernel.abi_status_fault?.();
    const statusStateBelongsToHandle = statusBelongsToHandle
      && statusProcessIdentityMatches
      && !(activeVforkWorker && status === faultStatus);
    appendStatusRouteFdDebug(
      "pre-owner-async",
      handle,
      msg,
      `:processField=${processPidField ?? "n/a"}`
        + `:trustedField=${trustedProcessPidField ?? "n/a"}`
        + `:localPid=${localProcessPid ?? "n/a"}`
        + `:msgProcess=0x${(msg.processPtr ?? 0).toString(16)}/${msg.processPid ?? "n/a"}`
        + `:localProcess=0x${localProcessPtr.toString(16)}`
        + `:belongs=${statusStateBelongsToHandle ? 1 : 0}`
        + `:ownerAsync=${getThreadOwnerAsyncStatus(thread) ?? "null"}`,
    );
    const fdChanges = takeChangedFdEntriesWithBaselineSincePosted(thread, msg.fdEntries);
    const authoritativeFds = fdChanges?.authoritativeFds;
    const postedFdEntries = fdChanges?.postedFdEntries;
    const ofdChanges = takeChangedOfdSlotsWithBaselineSincePosted(thread, msg.ofdSlots);
    const authoritativeOfdIds = ofdChanges?.authoritativeOfdIds;
    const postedOfdSlots = ofdChanges?.postedOfdSlots;
    if (handleOwnerAsyncStatusReturn({
      handle,
      worker,
      thread,
      msg,
      statusBelongsToHandle: statusStateBelongsToHandle,
      authoritativeFds,
      postedFdEntries,
      authoritativeOfdIds,
      postedOfdSlots,
      deps,
    })) {
      appendStatusRouteFdDebug(
        "owner-async-return",
        handle,
        msg,
        `:processField=${processPidField ?? "n/a"}`
          + `:trustedField=${trustedProcessPidField ?? "n/a"}`
          + `:localPid=${localProcessPid ?? "n/a"}`
          + `:belongs=${statusStateBelongsToHandle ? 1 : 0}`
          + `:ownerAsync=${getThreadOwnerAsyncStatus(thread) ?? "null"}`,
      );
      if (CLONE_SPAWN_DEBUG_ENABLED && supervisorProcessStatusIsCloneSpawn(handle.kernel, status)) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `clone-status-owner-async:${handle.pid}:0x${thread.vmPtr.toString(16)}`
            + `:generation=${msg.generation ?? "n/a"}`
            + `:owner=${getThreadOwnerAsyncStatus(thread) ?? "null"}`
            + `:belongs=${statusStateBelongsToHandle ? 1 : 0}`,
        );
      }
      return;
    }
    const previousWaitingStatus = getThreadWaitingInWorkerStatus(thread);
    clearThreadWaitingInWorkerStatus(thread);
    if (supervisorProcessStatusIsExecveSpawn(handle.kernel, status)) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `status-execve-spawn-seen:${handle.pid}:0x${msg.vmPtr.toString(16)}`
          + `:owner=${getThreadOwnerPid(thread)}`
          + `:field=${processPidField ?? "n/a"}`
          + `:local=${localProcessPid ?? "n/a"}`
          + `:kind=${handle.kernel.thread_spawn_kind?.(msg.vmPtr) ?? -1}`
          + `:child=${handle.kernel.thread_spawn_child_pid?.(msg.vmPtr) ?? -1}`
          + `:pc=${handle.kernel.thread_pc?.(msg.vmPtr)?.toString(16) ?? "n/a"}`
          + `:a0=${handle.kernel.thread_a0?.(msg.vmPtr)?.toString() ?? "n/a"}`
          + `:a7=${handle.kernel.thread_a7?.(msg.vmPtr)?.toString() ?? "n/a"}`,
      );
    }
    const routeActions = supervisorProcessStatusRouteActions(handle.kernel, status, {
      statusStateBelongsToHandle,
      kernelWorkerAvailable: processHasKernelWorkerOwnership(handle),
    });
    appendStatusRouteFdDebug(
      "route-actions",
      handle,
      msg,
      `:belongs=${statusStateBelongsToHandle ? 1 : 0}`
        + `:applyFd=${routeActions.applyStatusFdState ? 1 : 0}`
        + `:adopt=${routeActions.adoptThreadKernelState ? 1 : 0}`
        + `:fsWrites=${routeActions.applyFsMemoryWrites ? 1 : 0}`,
    );
    const isRunningWorker = hostExecutionState(handle).runningWorker === worker;
    const shouldApplyDetachedEventfdTraceOffset = (
      !routeActions.applyStatusFdState
      && getThreadOwnerPid(thread) === handle.pid
      && thread.vmPtr === msg.vmPtr
    );
    const detachedEventfdTraceOffsetApplied = shouldApplyDetachedEventfdTraceOffset
      ? applyStatusEventfdTraceOffset(handle, msg)
      : false;
    if (
      STATUS_SOCKET_DEBUG_LOG_ENABLED
      && msg.fdTraceArg0OfdType === handle.kernel.abi_fd_type_eventfd?.()
    ) {
      appendStatusSocketDebug(
        handle,
        `status-route-eventfd:${handle.pid}:0x${msg.vmPtr.toString(16)}:status=${status}`
          + `:belongs=${statusStateBelongsToHandle ? 1 : 0}`
          + `:applyFd=${routeActions.applyStatusFdState ? 1 : 0}`
          + `:adopt=${routeActions.adoptThreadKernelState ? 1 : 0}`
          + `:traceApply=${detachedEventfdTraceOffsetApplied ? 1 : 0}`
          + `:fd0Ofd=${msg.fdTraceArg0OfdId ?? "n/a"}`
          + `:fd0Off=${msg.fdTraceArg0OfdOffset?.toString() ?? "n/a"}`
          + `:ret=${msg.fdTraceResult?.toString() ?? "n/a"}`,
      );
    }
    if (traceFsStatusTiming) {
      appendFsStatusRouteTiming(
        deps,
        handle,
        "route-actions",
        thread.vmPtr,
        `:adopt=${routeActions.adoptThreadKernelState ? 1 : 0}`
          + `:applyFd=${routeActions.applyStatusFdState ? 1 : 0}`
          + `:fsWrites=${routeActions.applyFsMemoryWrites ? 1 : 0}`
          + `:dt=${Date.now() - handlerStartMs}`,
      );
    }
    if (routeActions.applyStatusFdState) {
      const changedRegularFileOfdFds = applyStatusOfdSlots(handle, msg, {
        authoritativeOfdIds,
        postedFdEntries,
        postedOfdSlots,
      });
      markStatusFdChangesForNextStep(
        handle,
        deps.threadWorkerStates,
        [...new Set([
          ...changedRegularFileOfdFds,
          ...applyStatusFdEntries(handle, msg, authoritativeFds, postedFdEntries, postedOfdSlots),
        ])],
      );
      pruneStatusNetworkConnectionsFromFdEntries(handle, msg, deps);
      appendStatusRouteFdDebug("after-status-fd", handle, msg);
    }
    if (
      msg.blockedSyscallNr !== undefined
      && msg.blockedSyscallArgs !== undefined
      && (handle.kernel.thread_blocked_syscall_nr?.(thread.vmPtr) ?? 0n) === 0n
    ) {
      const restoreBlockedState = handle.kernel.thread_restore_blocked_syscall_state;
      if (!restoreBlockedState) {
        throw new Error("kernel supervisor ABI missing thread_restore_blocked_syscall_state");
      }
      const restoreResult = restoreBlockedState(
        thread.vmPtr,
        msg.blockedFd ?? Number(msg.blockedSyscallArgs[0]),
        msg.blockedHasOfdPin === true ? 1 : 0,
        msg.blockedSyscallNr,
        ...msg.blockedSyscallArgs,
      );
      if (restoreResult < 0) {
        throw new Error(`kernel supervisor rejected blocked syscall state restore: ${restoreResult}`);
      }
    }
    if (traceFsStatusTiming) {
      appendFsStatusRouteTiming(
        deps,
        handle,
        "after-status-fd",
        thread.vmPtr,
        `:dt=${Date.now() - handlerStartMs}`,
      );
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
    const activity = getThreadActivityState(thread);
    activity.statusCount += 1;
    recordWorkerMessageDebugEvent(workerMessageDebugStats(handle), "statusHandled");
    activity.statusCounts[String(status)] = (activity.statusCounts[String(status)] ?? 0) + 1;
    activity.lastStatus = status;
    if (CLONE_SPAWN_DEBUG_ENABLED && supervisorProcessStatusIsCloneSpawn(handle.kernel, status)) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `clone-status-route:${handle.pid}:0x${thread.vmPtr.toString(16)}`
          + `:generation=${msg.generation ?? "n/a"}`
          + `:count=${activity.statusCounts[String(status)] ?? 0}`,
      );
    }
    const statusAtMs = Date.now();
    activity.lastStatusAtMs = statusAtMs;
    pushRecentStatusActivity(activity, {
      status,
      pc: msg.pc,
      syscallNr: msg.syscallNr ?? null,
      fdTraceSyscallNr: msg.fdTraceSyscallNr ?? null,
      fdTraceArg0OfdId: msg.fdTraceArg0OfdId ?? null,
      fdTraceArg0OfdType: msg.fdTraceArg0OfdType ?? null,
      fdTraceArg0OfdOffset: msg.fdTraceArg0OfdOffset ?? null,
      fdTraceArg1: msg.fdTraceArg1 ?? null,
      fdTraceArg2OfdId: msg.fdTraceArg2OfdId ?? null,
      fdTraceArg2OfdType: msg.fdTraceArg2OfdType ?? null,
      fdTraceResult: msg.fdTraceResult ?? null,
      futexWaitAddr: msg.futexWaitAddr ?? null,
      futexBlockedAddr: msg.futexBlockedAddr ?? null,
      atMs: statusAtMs,
    });
    if (getThreadRunningGeneration(thread) === msg.generation && activity.lastStepAtMs !== null) {
      const stepWallMs = Math.max(0, statusAtMs - activity.lastStepAtMs);
      activity.lastStepDurationMs = stepWallMs;
      activity.totalStepWallMs += stepWallMs;
      activity.maxStepWallMs = Math.max(activity.maxStepWallMs, stepWallMs);
    }
    activity.lastStatusPc = msg.pc;
    activity.lastStatusSp = msg.sp ?? null;
    activity.lastStatusS11 = msg.s11 ?? null;
    if (msg.processState) {
      setThreadExecveProcessState(thread, msg.processState);
    }
    incrementBoundedActivityCounter(activity.hotStatusPcs, `0x${msg.pc.toString(16)}`);
    const suspiciousLowStateDebug = hasSuspiciousLowStateDebug(msg.debug);
    deps.appendWorkerDebugLog(
      handle,
      `status-route:0x${thread.vmPtr.toString(16)}`
        + `:owner_pid=${getThreadOwnerPid(thread)}`
        + `:handle_pid=${handle.pid}`
        + `:generation=${msg.generation}`
        + `:status=${status}`,
    );
    if (!(status === faultStatus && suspiciousLowStateDebug && processLastDebugMessage(handle))) {
      setProcessLastDebugMessage(handle, msg.debug);
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
    if (trustedProcessPidField !== null && trustedProcessPidField !== handle.pid) {
      deps.appendWorkerDebugLog(
        handle,
        `status_debug:0x${thread.vmPtr.toString(16)}:handle_pid=${handle.pid}:process_pid_field=${trustedProcessPidField}`,
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

    if (!isRunningWorker) {
      recordWorkerMessageDebugEvent(
        workerMessageDebugStats(handle),
        "statusNotRunning",
        `status-not-running:${handle.pid}:0x${thread.vmPtr.toString(16)}:${status}`,
      );
    }
    const adoptPreSnapshots: ThreadRegisterSnapshot[] = [];
    for (const candidate of processWorkers(handle)) {
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
    const shouldApplyDetachedPipeSlots = (
      !routeActions.adoptThreadKernelState
      && (msg.pipeSlots?.length ?? 0) > 0
      && !(activeVforkWorker && status === faultStatus)
    );
    let appliedPipeControlSnapshot = false;
    if (routeActions.adoptThreadKernelState) {
      handle.kernelState = adoptKernelStateForHandle(handle, msg.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      applyThreadKernelMemoryWrites(
        handle,
        msg,
        deps.appendRuntimeLifecycleEvent,
        { skipFsRingWrites: routeActions.skipFsRingWrites },
      );
      applyThreadGuestMemoryWrites(handle, msg, deps.appendRuntimeLifecycleEvent);
      applyThreadPipeSlots(
        handle,
        msg.pipeSlots,
        pipeSlotLifecycleDebugEnabled() ? deps.appendRuntimeLifecycleEvent : undefined,
      );
      appliedPipeControlSnapshot = (msg.pipeSlots ?? []).some((snapshot) => snapshot.controlChanged === true);
      const changedRegularFileOfdFds = applyStatusOfdSlots(handle, msg, {
        authoritativeOfdIds,
        postedFdEntries,
        postedOfdSlots,
      });
      markStatusFdChangesForNextStep(
        handle,
        deps.threadWorkerStates,
        [...new Set([
          ...changedRegularFileOfdFds,
          ...applyStatusFdEntries(handle, msg, authoritativeFds, postedFdEntries, postedOfdSlots),
        ])],
      );
      pruneStatusNetworkConnectionsFromFdEntries(handle, msg, deps);
      applyThreadSocketState(handle, msg);
      appendStatusRouteFdDebug("after-adopt", handle, msg);
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
    } else if (shouldApplyDetachedPipeSlots) {
      applyThreadPipeSlots(
        handle,
        msg.pipeSlots,
        pipeSlotLifecycleDebugEnabled() ? deps.appendRuntimeLifecycleEvent : undefined,
      );
      appliedPipeControlSnapshot = (msg.pipeSlots ?? []).some((snapshot) => snapshot.controlChanged === true);
    } else if (routeActions.applyFsMemoryWrites) {
      applyThreadKernelMemoryWrites(
        handle,
        msg,
        deps.appendRuntimeLifecycleEvent,
        { skipFsRingWrites: routeActions.skipFsRingWrites },
      );
      applyThreadGuestMemoryWrites(handle, msg, deps.appendRuntimeLifecycleEvent);
    }
    if (appliedPipeControlSnapshot) {
      reconcileKernelSharePipeControl(handle);
    }
    if (traceFsStatusTiming) {
      appendFsStatusRouteTiming(
        deps,
        handle,
        "after-state-apply",
        thread.vmPtr,
        `:dt=${Date.now() - handlerStartMs}`,
      );
    }
    const submitShadowThreadEvent = (
      shadowMsg: ThreadWorkerStatusMessage,
      shadowStatus: number,
      completedSyncEffects: number,
    ): void => {
      const shadowThreadEventResult = submitSupervisorShadowStatusEvent(handle, {
        sequence: activity.statusCount,
        vmPtr: shadowMsg.vmPtr,
        pid: handle.pid,
        tid: handle.kernel.thread_tid?.(shadowMsg.vmPtr) ?? 0,
        status: shadowStatus,
        syncEffects: completedSyncEffects,
        blockedReason: shadowMsg.syscallNr === undefined
          ? 0
          : Number(shadowMsg.syscallNr & 0xffff_ffffn),
        pc: shadowMsg.pc,
        sp: shadowMsg.sp ?? 0n,
      });
      if (shadowThreadEventResult !== undefined && shadowThreadEventResult < 0) {
        deps.appendWorkerDebugLog(handle, `supervisor-thread-event:submit-error:${shadowThreadEventResult}`);
      }
    };
    const completedSyncEffectsForMessage = (
      messageStatus: number,
      message: ThreadWorkerStatusMessage,
    ): number => {
      const effects = (
        messageStatus === handle.kernel.abi_status_ok()
        && message.syncEffects === undefined
      )
        ? syncEffects.statusOkCompletedSyncEffects
        : (message.syncEffects ?? 0);
      return (message.pipeSlots?.length ?? 0) > 0
        ? effects | syncEffects.syncEffectPipeState
        : effects;
    };
    const waitInWorkerRequiresLocalWaitHandling = msg.waitInWorker
      && status !== handle.kernel.abi_status_connect();
    if (waitInWorkerRequiresLocalWaitHandling) {
      const waitInWorkerSyncEffects = completedSyncEffectsForMessage(status, msg);
      submitShadowThreadEvent(msg, status, waitInWorkerSyncEffects);
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
    const drainedPipe = routeActions.deferLocalPipeDrain
      ? false
      : drainProcessOutput(handle);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState);
    if (traceFsStatusTiming) {
      appendFsStatusRouteTiming(
        deps,
        handle,
        "after-host-io",
        thread.vmPtr,
        `:wrote=${wroteToPipe ? 1 : 0}:drained=${drainedPipe ? 1 : 0}:dt=${Date.now() - handlerStartMs}`,
      );
    }
    if (isRunningWorker) {
      hostExecutionState(handle).runningWorker = null;
      clearThreadRunningGeneration(thread);
      recordWorkerMessageDebugEvent(
        workerMessageDebugStats(handle),
        "statusClearedRunning",
        `status-cleared-running:${handle.pid}:0x${thread.vmPtr.toString(16)}:${status}`,
      );
    }

    let transitionStatus = status;
    let transitionMsg = msg;
    if (status === faultStatus && handle.kernel.thread_status) {
      const currentStatus = handle.kernel.thread_status(thread.vmPtr);
      let supervisorAllowsCurrentStatus = false;
      if (
        currentStatus !== status
        && currentStatus !== handle.kernel.abi_status_fault()
        && !activeVforkWorker
      ) {
        const supervisorExitDecision = readSupervisorFaultExitDecision(handle, thread.vmPtr);
        supervisorAllowsCurrentStatus = supervisorExitDecision !== undefined
          && supervisorExitDecision.kind !== supervisorFaultExitKindProcessExit(handle.kernel)
          && supervisorExitDecision.liveThreads !== 0;
      }
      if (
        currentStatus !== status
        && currentStatus !== handle.kernel.abi_status_fault()
        && supervisorAllowsCurrentStatus
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

    const completedSyncEffects = completedSyncEffectsForMessage(transitionStatus, transitionMsg);
    submitShadowThreadEvent(transitionMsg, transitionStatus, completedSyncEffects);
    if (traceFsStatusTiming) {
      appendFsStatusRouteTiming(
        deps,
        handle,
        "before-transition",
        thread.vmPtr,
        `:dt=${Date.now() - handlerStartMs}`,
      );
    }
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
      completedSyncEffects,
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
