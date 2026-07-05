import { hostExecutionState } from "../host-execution-state.js";
import { processHasKernelWorkerOwnership } from "../kernel-worker-ownership.js";
import {
  deleteProcessWorker,
  processHasWorker,
  processWorkers,
} from "../process-worker-state.js";
import {
  wakeRuntimeBridge,
} from "../../bridge/runtime-state.js";
import {
  supervisorBlockedPipeSyncBeforeRetryCompletionActions,
  supervisorBlockedPipeSyncBeforeRetryRequested,
  supervisorBlockedPostNetworkPumpAction,
  supervisorBlockedRetryAction,
  supervisorBlockedRetryHostCleanupActions,
  supervisorBlockedRetryResultAction,
  supervisorBlockedRetryStatusActions,
  supervisorBlockedStaleRetryResultAction,
  supervisorCompleteBlockedWaitNetworkPump,
  type SupervisorBlockedPostNetworkPumpAction,
  type SupervisorBlockedRetryAction,
  type SupervisorBlockedRetryActionFlag,
  type SupervisorBlockedRetryActionFlags,
  type SupervisorBlockedStaleRetryResultAction,
  type SupervisorBlockedWaitRetryFlags,
} from "../../supervisor-blocked-resume.js";
import {
  releaseWorker,
} from "../pool.js";
import {
  drainProcessOutput,
  flushStdin,
} from "../stdio.js";
import { describeForkSharedSyncPromise } from "../fork-shared-sync-debug.js";
import { clearStaleForkSharedSyncGate } from "../fork-shared-sync-gate.js";
import { collectLiveThreadVmPtrs } from "../live-threads.js";
import { networkConnections } from "../network-socket-state.js";
import {
  readSupervisorFaultExitDecision,
  supervisorFaultExitKindProcessExit,
} from "../supervisor-shadow.js";
import { executeKernelWorkerBlockedEpollRetry } from "./blocked-epoll-retry.js";
import {
  formatBlockedEpollEntries,
  formatFaultThreadSummary,
  formatProcessThreadSlots,
  formatRunningRetryBlocker,
  formatThreadFdTableSummary,
} from "./blocked-wait-diagnostics.js";
import type { KernelRpcResumeOutcome } from "../kernel-rpc/outcomes.js";
import type {
  HostBlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import {
  clearThreadRunningGeneration,
  clearThreadOwnerAsyncStatus,
  getThreadOwnerAsyncStatus,
  isThreadInitialized,
  setThreadOwnerAsyncStatus,
  threadResumeWorkersOnReadyCount,
} from "../thread-host-state.js";
import {
  getLastPipeRetryLifecycleEvent,
  isBlockedRpcPending,
  setBlockedRpcPending,
  setLastPipeRetryLifecycleEvent,
} from "../host-blocked-state.js";
import { getProcessKernelShareForkSharedSync } from "../fork-shared-sync-state.js";

export interface BlockedRetryCoordinator {
  retryHostBlockedWorker(handle: ProcessHandle, worker: Worker): void;
}

export interface BlockedRetryCoordinatorDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number, exitVmPtr?: number): Promise<void>;
  pumpSupervisorSelectedChildProcess?(childPid: number): boolean;
  pumpNetworkConnections(handle: ProcessHandle, vmPtr?: number): Promise<boolean>;
  executeHostBlockedSyscallResumeOutcome?(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    status: number,
  ): Promise<KernelRpcResumeOutcome>;
  resumeFsViaKernelWorker(handle: ProcessHandle, worker: Worker, vmPtr: number): Promise<void>;
  resumeFsViaKernelWorkerOutcome?(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
  ): Promise<KernelRpcResumeOutcome>;
  resumeVforkParentAfterChildExit(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    exitCode: number,
    termSignal: number,
  ): boolean;
  resumeWait4ViaKernelWorker(handle: ProcessHandle, worker: Worker, vmPtr: number): Promise<KernelRpcResumeOutcome>;
  beginBlockedRpc(blocked: HostBlockedWorkerState): number;
  finishBlockedRpc(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    rpcGeneration: number,
  ): HostBlockedWorkerState | null;
  observeForkSharedSyncCompletion(handle: ProcessHandle, worker: Worker, promise: Promise<unknown>): void;
  clearStaleRunningWorker(handle: ProcessHandle, worker: Worker): void;
  clearBlockedWorker(
    handle: ProcessHandle,
    worker: Worker,
    options?: {
      clearNetworkPumpBackoff?: boolean;
      clearPipeSyncBeforeRetry?: boolean;
    },
  ): void;
  scheduleSupervisorBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    flags?: SupervisorBlockedWaitRetryFlags,
  ): void;
  armBlockedSupervisorTimerForBlocked(handle: ProcessHandle, blocked: HostBlockedWorkerState): boolean;
  isSupervisorTimerArmed(blocked: HostBlockedWorkerState): boolean;
  pumpProcess(handle: ProcessHandle): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  selectProcessAnchorVmPtr(handle: ProcessHandle, currentWorker: Worker | null): number | null;
  pruneStaleProcessWorkers(handle: ProcessHandle, liveVmPtrs: Set<number>, currentWorker: Worker | null): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  requestSocketStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  requestEpollStateSync(handle: ProcessHandle, retryWaiters?: boolean): Promise<void>;
  requestEpollDependentOfdStateSync(
    handle: ProcessHandle,
    additionalOfdIds?: readonly number[],
    options?: { mode?: "push" | "pull" },
  ): Promise<void>;
  supervisorBlockedWaitActiveChildPid(handle: ProcessHandle, vmPtr: number): number;
  blockedOrCurrentSyscallNr(handle: ProcessHandle, vmPtr: number): bigint | null;
  formatBlockedPollFdSet(handle: ProcessHandle, vmPtr: number): string;
  formatFirstPipeWaitDebug(handle: ProcessHandle, vmPtr: number): string;
  formatFirstSocketWaitDebug(handle: ProcessHandle, vmPtr: number): string;
  pollWaitTimeoutMs(handle: ProcessHandle, status: number, vmPtr: number): number;
  appendBlockedStateLifecycle(handle: ProcessHandle, blocked: HostBlockedWorkerState): void;
}

export function createBlockedRetryCoordinator(
  deps: BlockedRetryCoordinatorDeps,
): BlockedRetryCoordinator {
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
        const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
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
    if (requireKernelWorker && !processHasKernelWorkerOwnership(handle)) {
      return false;
    }
    const hostBlockedResume = deps.executeHostBlockedSyscallResumeOutcome;
    if (hostBlockedResume === undefined) {
      const resumeSurface = requireKernelWorker ? "kernel-worker" : "host-boundary";
      deps.failProcess(handle, new Error(`${resumeSurface} blocked syscall resume requires executeHostBlockedSyscallResumeOutcome`));
      return true;
    }
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, lifecycleEvent);
    setBlockedRpcPending(blocked, true);
    void hostBlockedResume(
      handle,
      worker,
      blocked.vmPtr,
      blocked.status,
    ).then((outcome) => {
      consumeKernelRpcResumeOutcome(handle, worker, blocked.vmPtr, outcome);
      afterOutcome?.(outcome);
    }).catch((error) => {
      const latestBlocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
      if (latestBlocked && latestBlocked.vmPtr === blocked.vmPtr) {
        setBlockedRpcPending(latestBlocked, false);
      }
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    });
    return true;
  }

  function pumpProcessIfPipeWaitBlocked(handle: ProcessHandle, outcome: KernelRpcResumeOutcome): void {
    if (
      outcome.kind === "blocked"
      && supervisorBlockedRetryStatusActions(handle.kernel, outcome.status).pumpBlockedOutcome
    ) {
      deps.pumpProcess(handle);
    }
  }

  function parkBehindSupervisorActiveChild(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
    readyCount: number | undefined,
  ): boolean {
    const statusActions = supervisorBlockedRetryStatusActions(handle.kernel, blocked.status);
    if (!statusActions.activeChildPipe && !statusActions.activeChildWait4) {
      return false;
    }
    const childPid = deps.supervisorBlockedWaitActiveChildPid(handle, blocked.vmPtr);
    if (childPid <= 0) {
      return false;
    }
    deps.pumpSupervisorSelectedChildProcess?.(childPid);
    const event = statusActions.activeChildPipe
      ? `pipe-retry-skip:${handle.pid}:active-child:${childPid}:ready=${readyCount ?? "n/a"}:${deps.formatFirstPipeWaitDebug(handle, blocked.vmPtr)}:${deps.formatBlockedPollFdSet(handle, blocked.vmPtr)}`
      : `wait4-retry-skip:${handle.pid}:active-child:${childPid}`;
    if (getLastPipeRetryLifecycleEvent(blocked) !== event) {
      setLastPipeRetryLifecycleEvent(blocked, event);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
    }
    deps.scheduleSupervisorBlockedRetry(
      handle,
      worker,
      ["active-child"],
    );
    return true;
  }

  function postNetworkPumpRetryActionFlags(
    handle: ProcessHandle,
  ): SupervisorBlockedRetryActionFlags {
    const flags: SupervisorBlockedRetryActionFlag[] = ["wait-timer-already-checked"];
    if (hostExecutionState(handle).runningWorker !== null) {
      flags.push("running-worker");
    }
    if (processHasKernelWorkerOwnership(handle)) {
      flags.push("kernel-worker-available");
    }
    return flags;
  }

  function readPostNetworkPumpAction(
    handle: ProcessHandle,
    latestBlocked: HostBlockedWorkerState,
    changed: boolean,
  ): SupervisorBlockedPostNetworkPumpAction | null {
    try {
      return supervisorBlockedPostNetworkPumpAction(
        handle.kernel,
        latestBlocked.vmPtr,
        changed,
        postNetworkPumpRetryActionFlags(handle),
      );
    } catch (error) {
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      return null;
    }
  }

  function executePostNetworkPumpAction(
    handle: ProcessHandle,
    worker: Worker,
    latestBlocked: HostBlockedWorkerState,
    changed: boolean,
  ): void {
    const action = readPostNetworkPumpAction(handle, latestBlocked, changed);
    switch (action) {
      case null:
        return;
      case "schedule-retry":
        deps.scheduleSupervisorBlockedRetry(handle, worker, []);
        return;
      case "schedule-retry-with-host-progress":
        deps.scheduleSupervisorBlockedRetry(handle, worker, ["host-progress"]);
        return;
      case "defer-running-worker":
      case "defer-process-gate":
      case "defer-passive-wait":
        deps.scheduleSupervisorBlockedRetry(handle, worker);
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
        deps.scheduleSupervisorBlockedRetry(handle, worker);
        return;
      case "arm-wait-timer":
        if (deps.armBlockedSupervisorTimerForBlocked(handle, latestBlocked)) {
          return;
        }
        resumeBlockedSyscallViaResumeOutcome(
          handle,
          worker,
          latestBlocked,
          processHasKernelWorkerOwnership(handle)
            ? `pipe-retry-rpc:${handle.pid}`
            : `pipe-retry-host-boundary:${handle.pid}`,
          (outcome) => pumpProcessIfPipeWaitBlocked(handle, outcome),
          processHasKernelWorkerOwnership(handle),
        );
        return;
      case "resume-kernel-worker":
        resumeBlockedSyscallViaResumeOutcome(
          handle,
          worker,
          latestBlocked,
          `pipe-retry-rpc:${handle.pid}`,
          (outcome) => pumpProcessIfPipeWaitBlocked(handle, outcome),
        );
        return;
      case "resume-direct":
        resumeBlockedSyscallViaResumeOutcome(
          handle,
          worker,
          latestBlocked,
          `pipe-retry-host-boundary:${handle.pid}`,
          (outcome) => pumpProcessIfPipeWaitBlocked(handle, outcome),
          false,
        );
        return;
      default:
        deps.failProcess(
          handle,
          new Error(`kernel supervisor returned invalid post-network retry action: ${action}`),
        );
    }
  }

  function pumpNetworkForBlockedWait(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): boolean {
    deps.appendWorkerDebugLog(handle, `net-pump-start:${handle.pid}:status=${blocked.status}`);
    const rpcGeneration = deps.beginBlockedRpc(blocked);
    void deps.pumpNetworkConnections(handle, blocked.vmPtr)
      .then(async (changed) => {
        if (changed) {
          await deps.requestSocketStateSync(handle, true);
          await deps.requestPipeStateSync(handle, false);
        }
        const latestBlocked = deps.finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
        if (!latestBlocked) {
          return;
        }
        try {
          supervisorCompleteBlockedWaitNetworkPump(handle.kernel, latestBlocked.vmPtr, changed);
        } catch (error) {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          return;
        }
        deps.appendWorkerDebugLog(handle, `net-pump-done:${handle.pid}:status=${blocked.status}`);
        executePostNetworkPumpAction(handle, worker, latestBlocked, changed);
      })
      .catch((error) => {
        deps.finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
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
        if (processHasWorker(handle, worker) && deps.threadWorkerStates.has(worker)) {
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
        if (processHasWorker(handle, worker) && deps.threadWorkerStates.has(worker)) {
          appendEpollRetryResult(handle, blocked.vmPtr);
        }
      },
      false,
    );
  }

  function readStaleRetryResultAction(
    handle: ProcessHandle,
    blocked: HostBlockedWorkerState,
  ): SupervisorBlockedStaleRetryResultAction | null {
    try {
      return supervisorBlockedStaleRetryResultAction(
        handle.kernel,
        blocked.vmPtr,
        blocked.status,
      );
    } catch (error) {
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      return null;
    }
  }

  function clearStaleHostBlockedWorker(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): boolean {
    const action = readStaleRetryResultAction(handle, blocked);
    if (action === null || action === "none") {
      return false;
    }
    const currentStatus = handle.kernel.thread_status(blocked.vmPtr);
    if (action === "handle-current-result-wait4") {
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
    const forkSharedSyncPromise = getProcessKernelShareForkSharedSync(handle);
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
        if (processHasWorker(handle, worker) && deps.threadWorkerStates.has(worker)) {
          appendWait4RetryResult(handle, blocked.vmPtr);
        }
      },
      false,
    );
  }

  function handleHostBlockedRetryResult(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    status: number,
  ): void {
    const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
    if (!blocked || blocked.vmPtr !== vmPtr) {
      return;
    }

    if (status === handle.kernel.abi_status_ok()) {
      const statusActions = supervisorBlockedRetryStatusActions(handle.kernel, blocked.status);
      if (statusActions.successLogEpollReady) {
        deps.appendWorkerDebugLog(
          handle,
          `blocked-epoll-ready-fds:${handle.pid}:vm=0x${vmPtr.toString(16)}:${formatThreadFdTableSummary(handle, vmPtr)}`,
        );
      }
      if (statusActions.successLogWait4Resumed) {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-resumed-supervisor:${handle.pid}`);
      }
      deps.clearBlockedWorker(handle, worker);
      deps.enqueueWorker(handle, worker);
      return;
    }
    if (status === handle.kernel.abi_status_fault()) {
      deps.clearBlockedWorker(handle, worker);
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
      deps.appendWorkerDebugLog(handle, `fault:0x${vmPtr.toString(16)} remaining=${remainingThreads}`);
      if (hostExecutionState(handle).runningWorker === worker) {
        hostExecutionState(handle).runningWorker = null;
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
      deleteProcessWorker(handle, worker);
      deps.threadWorkerStates.delete(worker);
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
        deps.pumpProcess(handle);
      }
      return;
    }

    blocked.status = status;
    const action = supervisorBlockedRetryResultAction(handle.kernel, vmPtr, status);
    switch (action) {
      case "none":
        return;
      case "park-futex": {
        deps.clearBlockedWorker(handle, worker);
        const readyIndex = hostExecutionState(handle).vforkReadyWorkers.indexOf(worker);
        if (readyIndex >= 0) {
          hostExecutionState(handle).vforkReadyWorkers.splice(readyIndex, 1);
        }
        if (hostExecutionState(handle).runningWorker === worker) {
          hostExecutionState(handle).runningWorker = null;
        }
        const thread = deps.threadWorkerStates.get(worker);
        if (thread !== undefined && thread.vmPtr === vmPtr) {
          clearThreadRunningGeneration(thread);
        }
        deps.appendWorkerDebugLog(
          handle,
          `blocked-retry-futex-supervisor-owned:0x${vmPtr.toString(16)}`,
        );
        return;
      }
      case "arm-wait-timer":
        if (deps.armBlockedSupervisorTimerForBlocked(handle, blocked)) {
          return;
        }
        deps.scheduleSupervisorBlockedRetry(
          handle,
          worker,
          [],
        );
        return;
      case "schedule-retry":
        deps.scheduleSupervisorBlockedRetry(
          handle,
          worker,
          [],
        );
        return;
      case "schedule-retry-and-pump":
        deps.scheduleSupervisorBlockedRetry(
          handle,
          worker,
          [],
        );
        deps.pumpProcess(handle);
        return;
      default: {
        const exhaustive: never = action;
        throw new Error(`unhandled supervisor blocked retry result action: ${exhaustive}`);
      }
    }
  }

  function blockedRetryActionFlags(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
    thread: ThreadWorkerState | undefined,
  ): SupervisorBlockedRetryActionFlags {
    const flags: SupervisorBlockedRetryActionFlag[] = [];
    if (deps.isSupervisorTimerArmed(blocked)) {
      flags.push("supervisor-timer-armed");
    }
    if (isBlockedRpcPending(blocked)) {
      flags.push("rpc-pending");
    }
    if (getThreadOwnerAsyncStatus(thread) !== null) {
      flags.push("owner-async");
    }
    if (hasPendingCloneChildInitResume(handle)) {
      flags.push("thread-init-pending");
    }
    if (getProcessKernelShareForkSharedSync(handle) !== null) {
      flags.push("fork-shared-sync-pending");
    }
    if (hostExecutionState(handle).runningWorker !== null) {
      flags.push("running-worker");
    }
    if (processHasKernelWorkerOwnership(handle)) {
      flags.push("kernel-worker-available");
    }
    if (processHasKernelWorkerOwnership(handle)) {
      flags.push("pipe-state-kernel-worker-owns");
    }
    if (networkConnections(handle).size > 0) {
      flags.push("network-connections-available");
    }
    return flags;
  }

  function hasPendingCloneChildInitResume(handle: ProcessHandle): boolean {
    for (const worker of processWorkers(handle)) {
      const thread = deps.threadWorkerStates.get(worker);
      if (
        thread !== undefined
        && !isThreadInitialized(thread)
        && threadResumeWorkersOnReadyCount(thread) > 0
      ) {
        return true;
      }
    }
    return false;
  }

  function readBlockedRetryAction(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): SupervisorBlockedRetryAction | null {
    const thread = deps.threadWorkerStates.get(worker);
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
    const anchorVmPtr = deps.selectProcessAnchorVmPtr(handle, worker);
    if (anchorVmPtr !== null) {
      try {
        const liveVmPtrs = collectLiveThreadVmPtrs(handle.kernel, anchorVmPtr);
        deps.pruneStaleProcessWorkers(handle, liveVmPtrs, null);
        if (!liveVmPtrs.has(blocked.vmPtr)) {
          deps.clearBlockedWorker(handle, worker);
          return false;
        }
      } catch {
        // If the candidate anchor is stale, fall back to the normal resume path.
      }
    }

    const cleanupActions = supervisorBlockedRetryHostCleanupActions(handle.kernel, {
      kernelWorkerAvailable: processHasKernelWorkerOwnership(handle),
    });
    if (cleanupActions.flushStdin) {
      flushStdin(handle);
    }
    if (cleanupActions.drainLocalPipe) {
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
    if (supervisorBlockedRetryStatusActions(handle.kernel, blocked.status).pipeRetryDiagnostics) {
      const pipeSyncBeforeRetryRequested = supervisorBlockedPipeSyncBeforeRetryRequested(
        handle.kernel,
        blocked.vmPtr,
      );
      const event = `pipe-retry:${handle.pid}:a7=${deps.blockedOrCurrentSyscallNr(handle, blocked.vmPtr)?.toString() ?? "n/a"}:a0=${handle.kernel.thread_a0(blocked.vmPtr).toString()}:timeout=${blockedPollTimeoutMs}:ready=${blockedPollReadyCount ?? "n/a"}:${deps.formatFirstPipeWaitDebug(handle, blocked.vmPtr)}:${deps.formatFirstSocketWaitDebug(handle, blocked.vmPtr)}:${deps.formatBlockedPollFdSet(handle, blocked.vmPtr)}:syncRequested=${pipeSyncBeforeRetryRequested ? 1 : 0}`;
      if (getLastPipeRetryLifecycleEvent(blocked) !== event) {
        setLastPipeRetryLifecycleEvent(blocked, event);
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
    const rpcGeneration = deps.beginBlockedRpc(blocked);
    void deps.requestPipeStateSync(handle, false)
      .then(() => {
        const latestBlocked = deps.finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
        if (!latestBlocked) {
          return;
        }
        const completionActions = supervisorBlockedPipeSyncBeforeRetryCompletionActions(handle.kernel, {
          requestedDrainOutput: shouldDrainOutput,
          kernelWorkerAvailable: processHasKernelWorkerOwnership(handle),
        });
        if (completionActions.drainLocalPipe) {
          drainProcessOutput(handle);
        }
        if (completionActions.scheduleRetryWithHostProgress) {
          deps.scheduleSupervisorBlockedRetry(
            handle,
            worker,
            ["host-progress"],
          );
        }
      })
      .catch((error) => {
        deps.finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
  }

  function executeFsRetry(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
  ): void {
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `fs-retry:${handle.pid}`);
    const thread = deps.threadWorkerStates.get(worker);
    if (getThreadOwnerAsyncStatus(thread) !== null) {
      deps.scheduleSupervisorBlockedRetry(handle, worker);
      return;
    }
    if (thread) {
      setThreadOwnerAsyncStatus(thread, blocked.status);
    }
    deps.clearBlockedWorker(handle, worker);
    const resumeFs = deps.resumeFsViaKernelWorkerOutcome ?? deps.resumeFsViaKernelWorker;
    void resumeFs(handle, worker, blocked.vmPtr)
      .then((outcome) => {
        if (outcome !== undefined) {
          consumeKernelRpcResumeOutcome(handle, worker, blocked.vmPtr, outcome);
        }
      })
      .finally(() => {
        if (thread) {
          clearThreadOwnerAsyncStatus(thread);
        }
        deps.pumpProcess(handle);
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
        threadWorkerStates: deps.threadWorkerStates,
        appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
        failProcess: deps.failProcess,
        beginBlockedRpc: deps.beginBlockedRpc,
        finishBlockedRpc: deps.finishBlockedRpc,
        requestPipeStateSync: deps.requestPipeStateSync,
        requestEpollStateSync: deps.requestEpollStateSync,
        requestEpollDependentOfdStateSync: deps.requestEpollDependentOfdStateSync,
        armBlockedSupervisorTimerForBlocked: deps.armBlockedSupervisorTimerForBlocked,
        clearStaleRunningWorker: deps.clearStaleRunningWorker,
        scheduleSupervisorBlockedRetry: deps.scheduleSupervisorBlockedRetry,
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
        deps.pumpProcess(handle);
        return "done";
      case "defer-rpc-pending":
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-retry-skip:${handle.pid}:rpc-pending`);
        return "done";
      case "defer-owner-async": {
        const thread = deps.threadWorkerStates.get(worker);
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `blocked-retry-skip:${handle.pid}:owner-async:${getThreadOwnerAsyncStatus(thread) ?? "unknown"}`,
        );
        deps.scheduleSupervisorBlockedRetry(
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
        deps.scheduleSupervisorBlockedRetry(
          handle,
          worker,
          ["thread-init-pending"],
        );
        return "done";
      case "arm-wait-timer":
        try {
          if (!deps.armBlockedSupervisorTimerForBlocked(handle, blocked)) {
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
        const forkSharedSyncPromise = getProcessKernelShareForkSharedSync(handle);
        if (forkSharedSyncPromise === null) {
          return "retry";
        }
        const staleGate = clearStaleForkSharedSyncGate(handle, forkSharedSyncPromise);
        if (staleGate !== null) {
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `wait4-retry-cleared:${handle.pid}:${staleGate}`);
          return "retry";
        }
        deps.observeForkSharedSyncCompletion(handle, worker, forkSharedSyncPromise);
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `blocked-retry-skip:${handle.pid}:fork-shared-sync:${describeForkSharedSyncPromise(forkSharedSyncPromise)}`,
        );
        deps.scheduleSupervisorBlockedRetry(
          handle,
          worker,
          [],
        );
        return "done";
      }
      case "defer-running-worker": {
        if (supervisorBlockedRetryStatusActions(handle.kernel, blocked.status).wakeRuntimeBridgeWhileRunning) {
          wakeRuntimeBridge(handle.runtimeBridge);
        }
        const runningWorker = hostExecutionState(handle).runningWorker;
        const runningThread = runningWorker === null
          ? undefined
          : deps.threadWorkerStates.get(runningWorker);
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `blocked-retry-skip:${handle.pid}:running=1`
            + `:${formatRunningRetryBlocker(handle, runningThread)}`,
        );
        deps.scheduleSupervisorBlockedRetry(
          handle,
          worker,
          [],
        );
        return "done";
      }
      case "defer-process-gate":
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-retry-skip:${handle.pid}:process-gate`);
        deps.scheduleSupervisorBlockedRetry(
          handle,
          worker,
        );
        return "done";
      case "pump-network":
        if (!pumpNetworkForBlockedWait(handle, worker, blocked)) {
          deps.scheduleSupervisorBlockedRetry(handle, worker);
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
        deps.scheduleSupervisorBlockedRetry(handle, worker);
        return "done";
      case "resume-fs":
        if (!hostCleanupDone) {
          return "needs-host-cleanup";
        }
        executeFsRetry(handle, worker, blocked);
        return "done";
      case "defer-passive-wait":
        deps.scheduleSupervisorBlockedRetry(handle, worker);
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
        setBlockedRpcPending(blocked, true);
        void deps.resumeWait4ViaKernelWorker(handle, worker, blocked.vmPtr)
          .then((outcome) => {
            consumeKernelRpcResumeOutcome(handle, worker, blocked.vmPtr, outcome);
          })
          .catch((error) => {
            const latestBlocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
            if (latestBlocked && latestBlocked.vmPtr === blocked.vmPtr) {
              setBlockedRpcPending(latestBlocked, false);
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
          supervisorBlockedRetryStatusActions(handle.kernel, blocked.status).pipeRetryDiagnostics
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
    const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
    if (!blocked) {
      return;
    }
    deps.appendBlockedStateLifecycle(handle, blocked);
    deps.clearStaleRunningWorker(handle, worker);
    if (clearStaleHostBlockedWorker(handle, worker, blocked)) {
      return;
    }
    const blockedPollTimeoutMs = deps.pollWaitTimeoutMs(handle, blocked.status, blocked.vmPtr);
    const blockedPollReadyCount = supervisorBlockedRetryStatusActions(
      handle.kernel,
      blocked.status,
    ).pipeRetryDiagnostics
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

  return {
    retryHostBlockedWorker,
  };
}
