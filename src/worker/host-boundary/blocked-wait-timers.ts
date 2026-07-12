import { hostExecutionState } from "../host-execution-state.js";
import { processHasWorker } from "../process-worker-state.js";
import {
  supervisorBlockedEpollWaitTimeoutMs,
  supervisorBlockedWaitRetryFlags,
  supervisorBlockedWaitTimerRequired,
  type SupervisorBlockedWaitRetryFlags,
} from "../../supervisor-blocked-resume.js";
import {
  armSupervisorBlockedRetryTimer,
  armSupervisorHostTimer,
  drainSupervisorArmHostTimerHostOp,
  drainSupervisorBlockedRetryHostOp,
} from "../supervisor-host-op.js";
import {
  hasSupervisorShadowProtocol,
  submitSupervisorShadowStatusEvent,
} from "../supervisor-shadow.js";
import type {
  HostBlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import {
  getBlockedRpcGeneration,
  isEpollWakeRetryPending,
  setEpollPassiveUntilWake,
  setWaitTimerAlreadyChecked,
} from "../host-blocked-state.js";

export interface BlockedWaitTimersDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  retryHostBlockedWorker(handle: ProcessHandle, worker: Worker): void;
  markThreadWorkerBlockedTimeoutExpired(handle: ProcessHandle, vmPtr: number): void;
  pumpProcess(handle: ProcessHandle): void;
}

export interface BlockedWaitTimers {
  clearHostBlockedRetryTimer(handle: ProcessHandle, worker: Worker): void;
  scheduleSupervisorBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    flags?: SupervisorBlockedWaitRetryFlags,
  ): void;
  markSupervisorTimerArmed(blocked: HostBlockedWorkerState): void;
  isSupervisorTimerArmed(blocked: HostBlockedWorkerState): boolean;
  armBlockedSupervisorTimerForBlocked(
    handle: ProcessHandle,
    blocked: HostBlockedWorkerState,
  ): boolean;
}

function supervisorThreadEventKindTimerFired(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_thread_event_kind_timer_fired;
  if (exportFn === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_thread_event_kind_timer_fired");
  }
  return exportFn.call(handle.kernel);
}

export function createBlockedWaitTimers(deps: BlockedWaitTimersDeps): BlockedWaitTimers {
  const supervisorArmedBlockedTimers = new WeakSet<HostBlockedWorkerState>();
  const hostBlockedRetryTimers = new WeakMap<HostBlockedWorkerState, ReturnType<typeof setTimeout>>();

  function shouldDeferPendingEpollRetryForRunningWorker(
    handle: ProcessHandle,
    blocked: HostBlockedWorkerState,
  ): boolean {
    if (
      blocked.status !== handle.kernel.abi_status_epoll_wait()
      || !isEpollWakeRetryPending(blocked)
      || hostExecutionState(handle).runningWorker === null
      || supervisorBlockedEpollWaitTimeoutMs(handle.kernel, blocked.vmPtr) >= 0
    ) {
      return false;
    }
    setEpollPassiveUntilWake(blocked, false);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `blocked-retry-timer-defer-running:${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}`,
    );
    return true;
  }

  function restoreMissingEpollRetryTarget(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): boolean {
    if (
      blocked.status !== handle.kernel.abi_status_epoll_wait()
      || hostExecutionState(handle).hostBlockedWorkers.has(worker)
      || !processHasWorker(handle, worker)
    ) {
      return false;
    }
    const thread = deps.threadWorkerStates.get(worker);
    if (thread === undefined || thread.vmPtr !== blocked.vmPtr) {
      return false;
    }
    const currentStatus = handle.kernel.thread_status(blocked.vmPtr);
    if (currentStatus !== blocked.status) {
      return false;
    }
    hostExecutionState(handle).hostBlockedWorkers.set(worker, blocked);
    deps.appendWorkerDebugLog(
      handle,
      `supervisor-blocked-retry-target-restored:vm=0x${blocked.vmPtr.toString(16)}`
        + `:status=${blocked.status}`,
    );
    return true;
  }

  function clearHostBlockedRetryTimer(handle: ProcessHandle, worker: Worker): void {
    const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
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
    reason: number,
    hostOpSeq: bigint,
  ): void {
    if (!hasSupervisorShadowProtocol(handle.kernel)) {
      return;
    }
    const vmPtr = blocked.vmPtr;
    const status = handle.kernel.thread_status?.(vmPtr) ?? blocked.status;
    const eventKind = supervisorThreadEventKindTimerFired(handle);
    const sequence = getBlockedRpcGeneration(blocked);
    const result = submitSupervisorShadowStatusEvent(handle, {
      eventKind,
      sequence,
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
      deps.appendWorkerDebugLog(handle, `supervisor-timer-fired-event:submit-error:${result}`);
      return;
    }
    deps.appendWorkerDebugLog(
      handle,
      `supervisor-timer-fired-event:submit:vm=0x${vmPtr.toString(16)}`
        + `:seq=${sequence}:hostSeq=${hostOpSeq.toString()}:kind=${eventKind}:result=${result ?? "n/a"}`
        + `:status=${status}:reason=${reason}:delay=${delayMs}`,
    );
  }

  function scheduleHostBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    hostOp: NonNullable<ReturnType<typeof drainSupervisorBlockedRetryHostOp>>,
  ): void {
    const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
    if (!blocked || hostBlockedRetryTimers.has(blocked)) {
      return;
    }
    const timer = armSupervisorBlockedRetryTimer(handle, hostOp, {
      appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
      appendWorkerDebugLog: deps.appendWorkerDebugLog,
      failProcess: deps.failProcess,
      onTimerFired: (delayMs, reason, hostOpSeq) => {
        supervisorArmedBlockedTimers.delete(blocked);
        if (hostBlockedRetryTimers.get(blocked) === timer) {
          hostBlockedRetryTimers.delete(blocked);
        }
        submitBlockedRetryTimerFiredEvent(handle, blocked, delayMs, reason, hostOpSeq);
        if (shouldDeferPendingEpollRetryForRunningWorker(handle, blocked)) {
          return;
        }
        restoreMissingEpollRetryTarget(handle, worker, blocked);
        setWaitTimerAlreadyChecked(
          hostExecutionState(handle).hostBlockedWorkers.get(worker) ?? blocked,
          true,
        );
        deps.retryHostBlockedWorker(handle, worker);
      },
    });
    hostBlockedRetryTimers.set(blocked, timer);
  }

  function scheduleSupervisorBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    flags: SupervisorBlockedWaitRetryFlags = [],
  ): void {
    const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
    if (!blocked) {
      return;
    }
    const enqueueRetryTimer = handle.kernel.supervisor_enqueue_blocked_retry_timer;
    if (enqueueRetryTimer === undefined) {
      deps.failProcess(handle, new Error("kernel supervisor ABI missing supervisor_enqueue_blocked_retry_timer"));
      return;
    }
    const enqueueResult = enqueueRetryTimer.call(
      handle.kernel,
      blocked.vmPtr,
      supervisorBlockedWaitRetryFlags(handle.kernel, flags),
    );
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

  function armBlockedSupervisorTimer(handle: ProcessHandle, blocked: HostBlockedWorkerState): boolean {
    const vmPtr = blocked.vmPtr;
    let hostTimer = drainSupervisorArmHostTimerHostOp(handle, vmPtr);
    if (hostTimer === undefined) {
      throw new Error("kernel supervisor ABI missing arm host timer HostOp exports");
    }
    if (hostTimer === null) {
      const enqueueTimer = handle.kernel.supervisor_enqueue_blocked_wait_timer;
      if (enqueueTimer === undefined) {
        throw new Error("kernel supervisor ABI missing supervisor_enqueue_blocked_wait_timer");
      }
      const enqueueResult = enqueueTimer.call(handle.kernel, vmPtr);
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
      appendWorkerDebugLog: deps.appendWorkerDebugLog,
      failProcess: deps.failProcess,
      pumpProcess: (processHandle) => {
        supervisorArmedBlockedTimers.delete(blocked);
        deps.markThreadWorkerBlockedTimeoutExpired(processHandle, blocked.vmPtr);
        deps.pumpProcess(processHandle);
      },
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
    const supervisorTimerArmed = armBlockedSupervisorTimer(handle, blocked);
    if (supervisorTimerArmed) {
      markSupervisorTimerArmed(blocked);
    }
    return supervisorTimerArmed;
  }

  return {
    clearHostBlockedRetryTimer,
    scheduleSupervisorBlockedRetry,
    markSupervisorTimerArmed,
    isSupervisorTimerArmed,
    armBlockedSupervisorTimerForBlocked,
  };
}
