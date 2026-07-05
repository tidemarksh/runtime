import { hostExecutionState } from "../host-execution-state.js";
import {
  addProcessWorker,
  deleteProcessWorker,
  processHasWorker,
  processWorkerCount,
} from "../process-worker-state.js";
import {
  setWorkerErrorHandler,
  setWorkerMessageErrorHandler,
  setWorkerMessageHandler,
} from "../../bridge/compat.js";
import { publishRuntimeBridgeEpoch } from "../../bridge/runtime-state.js";
import { createSupervisorResumeHostBoundary } from "./supervisor-resume.js";
import { clearStaleForkSharedSyncGate } from "../fork-shared-sync-gate.js";
import { guestWriteWatchRegions } from "../guest-write-watch-state.js";
import {
  clearForkSharedSyncIfCurrent,
  getProcessKernelShareForkSharedSync,
} from "../fork-shared-sync-state.js";
import { processExitInProgress } from "../process-lifecycle-state.js";
import type {
  AttachThreadWorkerRequest,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type { ThreadWorkerOutbound } from "../../messages.js";
import { kernelStackSlotForWorker } from "../pool.js";
import {
  readSupervisorFaultExitDecision,
  supervisorFaultExitKindProcessExit,
} from "../supervisor-shadow.js";
import { snapshotProcessFdEntriesForThread } from "../ownership/fd-table.js";
import {
  restoreRegularFileOffsetsForStep,
  snapshotOfdSlotsForThread,
} from "../ownership/ofd-table.js";
import { reconcileKernelSharePipeControl } from "../kernel-share.js";
import { snapshotSocketStateForThread } from "../ownership/socket-control.js";
import { supervisorProcessOwnerMatches } from "../process-identity.js";
import {
  clearThreadRunningGeneration,
  getThreadGeneration,
  getThreadOwnerPid,
  getThreadReinitCount,
  getThreadOwnerAsyncStatus,
  getThreadRunningGeneration,
  getThreadWaitingInWorkerStatus,
  setThreadHostIdentity,
  setThreadInitialized,
  setThreadReinitCount,
  setThreadRunningGeneration,
} from "../thread-host-state.js";
import {
  getThreadActivityState,
  incrementBoundedActivityCounter,
  pushRecentStepActivity,
  resetThreadActivityState,
} from "../thread-activity-state.js";
import {
  takePendingGuestMemoryWritesForNextStep,
  takePendingKernelStateForNextStep,
  takePendingPipeSlotsForNextStep,
} from "../thread-step-payload.js";
import {
  appendSchedulerQueueDebugLog,
  formatSchedulerStepDebugSuffix,
  formatSchedulerStepFdSnapshot,
  hasBrokenSchedulerThreadState,
} from "../diagnostics/scheduler-queue.js";
import type {
  ThreadWorkerInbound,
  WorkerSchedulerQueueHelpers,
} from "./worker-scheduler-types.js";

const DEFAULT_NODE_SCHEDULER_STEP_BUDGET = 5_000_000;
const DEFAULT_BROWSER_SCHEDULER_STEP_BUDGET = 10_000;
const MIN_SCHEDULER_STEP_BUDGET = 1_000;
const MAX_SCHEDULER_STEP_BUDGET = 5_000_000;
const SCHEDULER_STEP_BUDGET = readSchedulerStepBudget();
let nextThreadWorkerGeneration = 1;

function readSchedulerStepBudget(): number {
  const hostConfigured = (globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
    TIDEMARK_SCHEDULER_STEP_BUDGET?: unknown;
  }).TIDEMARK_SCHEDULER_STEP_BUDGET;
  const envConfigured = (globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_SCHEDULER_STEP_BUDGET;
  const parsed = Number.parseInt(String(hostConfigured ?? envConfigured ?? ""), 10);
  if (Number.isFinite(parsed)) {
    return Math.min(MAX_SCHEDULER_STEP_BUDGET, Math.max(MIN_SCHEDULER_STEP_BUDGET, parsed));
  }
  const processLike = (globalThis as typeof globalThis & {
    process?: { versions?: { node?: string } };
  }).process;
  const isNodeRuntime = typeof processLike?.versions?.node === "string";
  const isBrowserRuntime = typeof window !== "undefined" && !isNodeRuntime;
  return isBrowserRuntime
    ? DEFAULT_BROWSER_SCHEDULER_STEP_BUDGET
    : DEFAULT_NODE_SCHEDULER_STEP_BUDGET;
}

interface WorkerSchedulerQueueDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void;
  clearHostBlockedRetryTimer(handle: ProcessHandle, worker: Worker): void;
  takeThreadWorkerBlockedResume(handle: ProcessHandle, worker: Worker): boolean;
  isSupervisorFutexWaiter(handle: ProcessHandle, worker: Worker): boolean;
  completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number, exitVmPtr?: number): Promise<void>;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleWorkerMessage(handle: ProcessHandle, worker: Worker, msg: ThreadWorkerOutbound): void;
}

function removeVforkReadyWorker(handle: ProcessHandle, worker: Worker): void {
  for (let index = hostExecutionState(handle).vforkReadyWorkers.length - 1; index >= 0; index -= 1) {
    if (hostExecutionState(handle).vforkReadyWorkers[index] === worker) {
      hostExecutionState(handle).vforkReadyWorkers.splice(index, 1);
    }
  }
}

function clearBlockedWorker(deps: WorkerSchedulerQueueDeps, handle: ProcessHandle, worker: Worker): void {
  const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
  if (!blocked) {
    return;
  }
  deps.clearHostBlockedRetryTimer(handle, worker);
  hostExecutionState(handle).hostBlockedWorkers.delete(worker);
}

export function createWorkerSchedulerQueueHelpers(
  deps: WorkerSchedulerQueueDeps,
): WorkerSchedulerQueueHelpers {
  const observedForkSharedSyncGates = new WeakMap<ProcessHandle, Promise<void>>();
  const supervisorResumeHostBoundary = createSupervisorResumeHostBoundary({
    threadWorkerStates: deps.threadWorkerStates,
    appendWorkerDebugLog: deps.appendWorkerDebugLog,
    clearBlockedWorker: (handle, worker) => clearBlockedWorker(deps, handle, worker),
    removeVforkReadyWorker,
  });

  function observeForkSharedSyncGate(handle: ProcessHandle, promise: Promise<void>): void {
    if (observedForkSharedSyncGates.get(handle) === promise) {
      return;
    }
    observedForkSharedSyncGates.set(handle, promise);
    const onSettled = (): void => {
      if (observedForkSharedSyncGates.get(handle) === promise) {
        observedForkSharedSyncGates.delete(handle);
      }
      clearForkSharedSyncIfCurrent(handle, promise);
      pumpProcess(handle);
    };
    promise.then(onSettled, onSettled);
  }

  function sanitizeRunningWorker(handle: ProcessHandle): Worker | null {
    const runningWorker = hostExecutionState(handle).runningWorker;
    if (runningWorker === null) {
      return null;
    }
    const thread = deps.threadWorkerStates.get(runningWorker);
    if (
      processHasWorker(handle, runningWorker)
      && thread
      && (getThreadRunningGeneration(thread) ?? getThreadGeneration(thread)) === getThreadGeneration(thread)
    ) {
      return runningWorker;
    }
    hostExecutionState(handle).runningWorker = null;
    if (thread) {
      clearThreadRunningGeneration(thread);
    }
    deps.appendWorkerDebugLog(handle, "running-worker-cleared:stale");
    return null;
  }

  function assertThreadOwnershipInvariant(
    handle: ProcessHandle,
    vmPtr: number,
    context: string,
  ): void {
    if (vmPtr === 0 || handle.pid <= 0) {
      return;
    }
    if (!supervisorProcessOwnerMatches(handle.kernel, vmPtr, handle.pid)) {
      const currentPid = handle.kernel.process_current_pid(vmPtr);
      throw new Error(
        `${context} worker/process ownership mismatch vm=0x${vmPtr.toString(16)}`
          + ` expected_pid=${handle.pid} actual_pid=${currentPid}`,
      );
    }
  }

  function detachTransferredWorker(handle: ProcessHandle, worker: Worker): void {
    const thread = deps.threadWorkerStates.get(worker);
    clearBlockedWorker(deps, handle, worker);
    removeVforkReadyWorker(handle, worker);
    if (hostExecutionState(handle).runningWorker === worker) {
      hostExecutionState(handle).runningWorker = null;
    }
    if (hostExecutionState(handle).vforkWorker === worker) {
      hostExecutionState(handle).vforkWorker = null;
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `attach-transfer-detach:${handle.pid}`
        + `:vm=0x${(thread?.vmPtr ?? 0).toString(16)}`
        + `:owner=${thread === undefined ? "none" : getThreadOwnerPid(thread)}`
        + `:workers=${processWorkerCount(handle)}`,
    );
    deleteProcessWorker(handle, worker);
  }

  function attachThreadWorker(handle: ProcessHandle, request: AttachThreadWorkerRequest): void {
    const { worker, vmPtr, forcePrepare, transferFrom, preserveMemoryBytes, preserveMemoryRanges } = request;
    const generation = nextThreadWorkerGeneration++;
    const existingThread = deps.threadWorkerStates.get(worker);
    const reinitWithinHandle = existingThread !== undefined
      && transferFrom === undefined
      && getThreadOwnerPid(existingThread) === handle.pid;
    if (existingThread) {
      if (reinitWithinHandle) {
        clearBlockedWorker(deps, handle, worker);
        removeVforkReadyWorker(handle, worker);
        if (hostExecutionState(handle).runningWorker === worker) {
          hostExecutionState(handle).runningWorker = null;
        }
        if (hostExecutionState(handle).vforkWorker === worker) {
          hostExecutionState(handle).vforkWorker = null;
        }
      } else {
        const sourceHandle = transferFrom;
        if (!sourceHandle) {
          throw new Error(
            `attachThreadWorker requires transferFrom for existing worker owner=${getThreadOwnerPid(existingThread)}`
              + ` target=${handle.pid} vm=0x${vmPtr.toString(16)}`,
          );
        }
        if (sourceHandle === handle) {
          throw new Error(
            `attachThreadWorker transferFrom must differ from target pid=${handle.pid}`
              + ` vm=0x${vmPtr.toString(16)}`,
          );
        }
        if (getThreadOwnerPid(existingThread) !== sourceHandle.pid) {
          throw new Error(
            `attachThreadWorker owner mismatch worker_owner=${getThreadOwnerPid(existingThread)}`
              + ` transfer_from=${sourceHandle.pid} target=${handle.pid}`,
          );
        }
        detachTransferredWorker(sourceHandle, worker);
      }
    } else if (transferFrom) {
      throw new Error(
        `attachThreadWorker received transferFrom for unattached worker`
          + ` source=${transferFrom.pid} target=${handle.pid} vm=0x${vmPtr.toString(16)}`,
      );
    }

    assertThreadOwnershipInvariant(handle, vmPtr, "attachThreadWorker");
    addProcessWorker(handle, worker);
    deps.appendWorkerDebugLog(
      handle,
      `${reinitWithinHandle ? "reinit" : "attach"}:0x${vmPtr.toString(16)}${formatSchedulerStepDebugSuffix(handle, vmPtr)}`,
    );
    const reinitCount = reinitWithinHandle ? getThreadReinitCount(existingThread) + 1 : 0;
    const thread: ThreadWorkerState = {
      vmPtr,
    };
    deps.threadWorkerStates.set(worker, thread);
    setThreadHostIdentity(thread, handle.pid, generation);
    setThreadReinitCount(thread, reinitCount);
    setThreadInitialized(thread, false);
    resetThreadActivityState(thread);
    setWorkerMessageHandler(worker, (event: MessageEvent<ThreadWorkerOutbound>) => {
      deps.handleWorkerMessage(handle, worker, event.data);
    });
    setWorkerErrorHandler(worker, (event) => {
      const detail = [
        event.message,
        event.filename ? `at ${event.filename}:${event.lineno}:${event.colno}` : null,
        event.error instanceof Error ? (event.error.stack ?? event.error.message) : null,
      ].filter((part): part is string => !!part).join(" ");
      deps.appendWorkerDebugLog(handle, `worker-error:0x${vmPtr.toString(16)}:${JSON.stringify(detail || "unknown worker error")}`);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `clone-child-worker-error:${handle.pid}:vm=0x${vmPtr.toString(16)}`
          + `:detail=${JSON.stringify(detail || "unknown worker error")}`,
      );
      deps.failProcess(handle, new Error(
        `thread worker error vm=0x${vmPtr.toString(16)} ${detail || "unknown worker error"}`,
      ));
      event.preventDefault?.();
    });
    setWorkerMessageErrorHandler(worker, (event) => {
      deps.appendWorkerDebugLog(handle, `worker-messageerror:0x${vmPtr.toString(16)}`);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `clone-child-messageerror:${handle.pid}:vm=0x${vmPtr.toString(16)}`,
      );
      deps.failProcess(handle, new Error(
        `thread worker messageerror vm=0x${vmPtr.toString(16)}`,
      ));
      event.preventDefault?.();
    });
    const runtimeEpoch = publishRuntimeBridgeEpoch(handle.runtimeBridge);
    const fdEntries = snapshotProcessFdEntriesForThread(handle, vmPtr);
    const initMessage: ThreadWorkerInbound = {
      type: "init",
      wasmModule: handle.wasmModule,
      memory: handle.memory,
      forcePrepare,
      vmPtr,
      guestBase: handle.guestBase,
      kernelStackSlot: kernelStackSlotForWorker(worker),
      generation,
      runtimeEpoch,
      preserveMemoryBytes,
      preserveMemoryRanges,
      guestWriteWatchRegions: guestWriteWatchRegions(handle),
      kernelState: handle.kernelState,
      sabPageCache: handle.sabPageCache,
      runtimeNotify: handle.runtimeBridge.notifySab,
      runtimeState: handle.runtimeBridge.stateSab,
    };
    const initSocketState = snapshotSocketStateForThread(handle);
    const initOfdSlots = snapshotOfdSlotsForThread(handle, fdEntries);
    if (initSocketState !== undefined) {
      initMessage.socketState = initSocketState;
    }
    if (fdEntries !== undefined) {
      initMessage.fdEntries = fdEntries;
    }
    if (initOfdSlots !== undefined) {
      initMessage.ofdSlots = initOfdSlots;
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `clone-child-init-post:${handle.pid}:vm=0x${vmPtr.toString(16)}`
        + `:generation=${generation}:force=${forcePrepare ? 1 : 0}`
        + `:workers=${processWorkerCount(handle)}:ready=${hostExecutionState(handle).vforkReadyWorkers.length}`,
    );
    worker.postMessage(initMessage);
  }

  function enqueueWorker(handle: ProcessHandle, worker: Worker): void {
    if (processExitInProgress(handle)) {
      return;
    }
    const thread = deps.threadWorkerStates.get(worker);
    if (
      hostExecutionState(handle).runningWorker === worker
      && processHasWorker(handle, worker)
      && thread !== undefined
    ) {
      return;
    }
    if (
      deps.isSupervisorFutexWaiter(handle, worker)
      || getThreadWaitingInWorkerStatus(thread) !== null
      || getThreadOwnerAsyncStatus(thread) !== null
    ) {
      const ownerAsyncStatus = getThreadOwnerAsyncStatus(thread);
      if (
        thread !== undefined
        && getThreadWaitingInWorkerStatus(thread) === null
        && ownerAsyncStatus !== null
      ) {
        appendSchedulerQueueDebugLog(handle, `runnable-defer-owner-async:0x${thread.vmPtr.toString(16)}:async=${ownerAsyncStatus}`);
      } else if (ownerAsyncStatus === null) {
        removeVforkReadyWorker(handle, worker);
      }
      pumpProcess(handle);
      return;
    }
    const vmPtr = thread?.vmPtr;
    if (vmPtr !== undefined) {
      const brokenThreadState = hasBrokenSchedulerThreadState(handle, vmPtr);
      appendSchedulerQueueDebugLog(handle, `runnable-published:0x${vmPtr.toString(16)}${formatSchedulerStepDebugSuffix(handle, vmPtr)}`);
      if (brokenThreadState) {
        deps.appendWorkerDebugLog(
          handle,
          `ready_break:0x${vmPtr.toString(16)}${formatSchedulerStepDebugSuffix(handle, vmPtr)}`,
        );
      }
    }
    if (
      hostExecutionState(handle).vforkSnapshot !== null
      && hostExecutionState(handle).vforkWorker === worker
      && !hostExecutionState(handle).vforkReadyWorkers.includes(worker)
    ) {
      hostExecutionState(handle).vforkReadyWorkers.push(worker);
    }
    pumpProcess(handle);
  }

  function completeFaultedProcessWithoutWorkers(handle: ProcessHandle): boolean {
    if (
      processWorkerCount(handle) !== 0
      || hostExecutionState(handle).vforkReadyWorkers.length !== 0
      || hostExecutionState(handle).runningWorker !== null
      || hostExecutionState(handle).hostBlockedWorkers.size !== 0
    ) {
      return false;
    }
    const decision = readSupervisorFaultExitDecision(handle, handle.vmPtr);
    if (
      decision === undefined
      || decision.kind !== supervisorFaultExitKindProcessExit(handle.kernel)
    ) {
      return false;
    }
    deps.appendWorkerDebugLog(
      handle,
      `complete-exit:no-workers-fault:0x${decision.exitVmPtr.toString(16)}`
        + `:seq=${decision.seq.toString()}`
        + `:threads=${decision.liveThreads}/${decision.remainingThreads}`,
    );
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `complete-exit-request:no-workers-fault:${handle.pid}:vm=0x${decision.exitVmPtr.toString(16)}`
        + `:exit=${decision.exitCode}:${decision.termSignal}`
        + `:seq=${decision.seq.toString()}`
        + `:threads=${decision.liveThreads}/${decision.remainingThreads}`,
    );
    void deps.completeProcessExit(
      handle,
      decision.exitCode,
      decision.termSignal,
      decision.exitVmPtr,
    ).catch((error) => {
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    });
    return true;
  }

  function pumpProcess(handle: ProcessHandle): void {
    if (processExitInProgress(handle)) {
      return;
    }
    const forkSharedSyncPromise = getProcessKernelShareForkSharedSync(handle);
    if (forkSharedSyncPromise !== null) {
      const staleGate = clearStaleForkSharedSyncGate(handle, forkSharedSyncPromise);
      if (staleGate !== null) {
        appendSchedulerQueueDebugLog(handle, `pump-cleared:${staleGate}`);
      } else {
        appendSchedulerQueueDebugLog(handle, `pump-held:fork-shared-sync:ready=${hostExecutionState(handle).vforkReadyWorkers.length}`);
        observeForkSharedSyncGate(handle, forkSharedSyncPromise);
        return;
      }
    }
    const runningWorker = sanitizeRunningWorker(handle);
    if (runningWorker !== null) {
      const thread = deps.threadWorkerStates.get(runningWorker);
      appendSchedulerQueueDebugLog(
        handle,
        `pump-held:running:ready=${hostExecutionState(handle).vforkReadyWorkers.length}`
          + (thread ? `:vm=0x${thread.vmPtr.toString(16)}:status=${handle.kernel.thread_status(thread.vmPtr)}` : ""),
      );
      return;
    }
    supervisorResumeHostBoundary.drainSupervisorResumeQueue(handle);
    if (completeFaultedProcessWithoutWorkers(handle)) {
      return;
    }
    let next: Worker | undefined;
    const supervisorSelected = supervisorResumeHostBoundary.selectSupervisorRunnableWorker(handle);
    if (supervisorSelected !== undefined) {
      next = supervisorSelected ?? undefined;
    } else {
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-resume:runnable:missing-export:ready=${hostExecutionState(handle).vforkReadyWorkers.length}`,
      );
      return;
    }
    if (!next) {
      return;
    }
    hostExecutionState(handle).runningWorker = next;
    const thread = deps.threadWorkerStates.get(next);
    const stepBudget = SCHEDULER_STEP_BUDGET;
    const vmPtr = thread?.vmPtr;
    if (thread && vmPtr !== undefined) {
      appendSchedulerQueueDebugLog(
        handle,
        `pump-run:ready=${hostExecutionState(handle).vforkReadyWorkers.length}:vm=0x${vmPtr.toString(16)}:status=${handle.kernel.thread_status(vmPtr)}`,
      );
      restoreRegularFileOffsetsForStep(handle);
      reconcileKernelSharePipeControl(handle);
      const activity = getThreadActivityState(thread);
      activity.stepCount += 1;
      setThreadRunningGeneration(thread, getThreadGeneration(thread));
      activity.totalStepBudget += stepBudget;
      activity.lastStepAtMs = Date.now();
      activity.lastStepProcessPtr = handle.kernel.thread_process_ptr(vmPtr);
      activity.lastStepPc = handle.kernel.thread_pc(vmPtr);
      activity.lastStepSp = handle.kernel.thread_sp(vmPtr);
      activity.lastStepS11 = handle.kernel.thread_s11?.(vmPtr) ?? 0n;
      incrementBoundedActivityCounter(activity.hotStepPcs, `0x${activity.lastStepPc.toString(16)}`);
      pushRecentStepActivity(activity, {
        pc: activity.lastStepPc,
        sp: activity.lastStepSp,
        s11: activity.lastStepS11,
        atMs: activity.lastStepAtMs,
      });
      const brokenThreadState = hasBrokenSchedulerThreadState(handle, vmPtr);
      appendSchedulerQueueDebugLog(handle, `step:0x${vmPtr.toString(16)}${formatSchedulerStepDebugSuffix(handle, vmPtr)}`);
      if (brokenThreadState) {
        deps.appendWorkerDebugLog(
          handle,
          `step_break:0x${vmPtr.toString(16)}${formatSchedulerStepDebugSuffix(handle, vmPtr)}`,
        );
      }
    }
    publishRuntimeBridgeEpoch(handle.runtimeBridge);
    const fdEntries = snapshotProcessFdEntriesForThread(handle, vmPtr ?? 0);
    const pendingKernelState = takePendingKernelStateForNextStep(thread);
    const stepMessage: ThreadWorkerInbound = {
      type: "step",
      budget: stepBudget,
      kernelState: pendingKernelState ?? handle.kernelState,
    };
    const stepSocketState = snapshotSocketStateForThread(handle);
    const stepOfdSlots = snapshotOfdSlotsForThread(handle, fdEntries);
    if (stepSocketState !== undefined) {
      stepMessage.socketState = stepSocketState;
    }
    if (fdEntries !== undefined) {
      stepMessage.fdEntries = fdEntries;
    }
    if (stepOfdSlots !== undefined) {
      stepMessage.ofdSlots = stepOfdSlots;
    }
    appendSchedulerQueueDebugLog(
      handle,
      `step-fds:0x${(vmPtr ?? 0).toString(16)}:${formatSchedulerStepFdSnapshot(handle, vmPtr, fdEntries, stepOfdSlots)}`,
    );
    const pendingPipeSlots = takePendingPipeSlotsForNextStep(thread);
    if (pendingPipeSlots.length > 0) {
      stepMessage.pipeSlots = pendingPipeSlots;
    }
    const pendingGuestMemoryWrites = takePendingGuestMemoryWritesForNextStep(thread);
    if (pendingGuestMemoryWrites.length > 0) {
      stepMessage.guestMemoryWrites = pendingGuestMemoryWrites;
    }
    if (thread && deps.takeThreadWorkerBlockedResume(handle, next)) {
      stepMessage.resumeBlockedSyscall = true;
    }
    next.postMessage(stepMessage);
  }

  return {
    attachThreadWorker,
    enqueueWorker,
    pumpProcess,
  };
}
