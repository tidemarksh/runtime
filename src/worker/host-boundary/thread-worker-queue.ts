import { hostExecutionState } from "../host-execution-state.js";
import {
  addProcessWorker,
  deleteProcessWorker,
  processHasWorker,
  processWorkerCount,
  processWorkerList,
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
import { kernelStackSlotForWorker, releaseWorker } from "../pool.js";
import {
  readSupervisorFaultExitDecision,
  supervisorFaultExitKindProcessExit,
} from "../supervisor-shadow.js";
import {
  describeSupervisorProcessFdTableIncoherence,
  snapshotAuthoritativeProcessFdEntriesForThread,
  snapshotProcessFdEntriesForThread,
  supervisorProcessFdTableIsCoherent,
} from "../ownership/fd-table.js";
import {
  restoreRegularFileOffsetsForStep,
  snapshotOfdSlotsForThread,
} from "../ownership/ofd-table.js";
import { reconcileKernelSharePipeControl } from "../kernel-share.js";
import { snapshotSocketStateForThread } from "../ownership/socket-control.js";
import { supervisorProcessOwnerMatches } from "../process-identity.js";
import {
  clearThreadOwnerAsyncStatus,
  clearThreadRunningGeneration,
  clearThreadWaitingInWorkerStatus,
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
import { clearSupervisorHostTimer } from "../supervisor-host-op.js";
import { isBlockedRpcPending } from "../host-blocked-state.js";
import {
  getThreadActivityState,
  incrementBoundedActivityCounter,
  pushRecentStepActivity,
  resetThreadActivityState,
  threadLatestStepAnswered,
} from "../thread-activity-state.js";
import {
  recordPostedFdEntriesForNextStatus,
  recordPostedOfdSlotsForNextStatus,
  takePendingGuestMemoryWritesForNextStep,
  takePendingExitedThreadsForNextStep,
  takePendingAuthoritativeFdEntriesForNextStep,
  takePendingKernelStateForNextStep,
  takePendingKernelMemoryWritesForNextStep,
  takePendingPipeSlotsForNextStep,
} from "../thread-step-payload.js";
import {
  appendSchedulerQueueDebugLog,
  formatSchedulerStepDebugSuffix,
  formatSchedulerStepFdSnapshot,
  hasBrokenSchedulerThreadState,
  schedulerQueueDebugLogEnabled,
} from "../diagnostics/scheduler-queue.js";
import type {
  ThreadWorkerInbound,
  ThreadWorkerProcessIdentity,
  WorkerSchedulerQueueHelpers,
} from "./worker-scheduler-types.js";
import { DEFAULT_NODE_EXECUTION_STEP_BUDGET } from "../../execution/budget.js";

const DEFAULT_NODE_SCHEDULER_STEP_BUDGET = DEFAULT_NODE_EXECUTION_STEP_BUDGET;
const DEFAULT_BROWSER_SCHEDULER_STEP_BUDGET = DEFAULT_NODE_EXECUTION_STEP_BUDGET;
const MIN_SCHEDULER_STEP_BUDGET = 1_000;
const MAX_SCHEDULER_STEP_BUDGET = 100_000_000;
const FD_TABLE_COHERENCE_RETRY_MS = 1;
const SCHEDULER_STEP_BUDGET = readSchedulerStepBudget();
const FS_LIFECYCLE_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_LIFECYCLE_DEBUG ?? "") === "1";
let nextThreadWorkerGeneration = 1;

export function snapshotThreadWorkerProcessIdentity(
  handle: ProcessHandle,
  vmPtr: number,
): ThreadWorkerProcessIdentity | undefined {
  if (vmPtr === 0) {
    return undefined;
  }
  const processPtr = handle.kernel.thread_process_ptr(vmPtr);
  const currentPid = handle.kernel.process_current_pid(vmPtr);
  const vforkSnapshot = hostExecutionState(handle).vforkSnapshot;
  const activeVforkChildPid = handle.kernel.thread_vfork_child_pid();
  const transientVforkChild = activeVforkChildPid > 0
    && currentPid === activeVforkChildPid
    && currentPid !== handle.pid;
  const canonicalVforkChild = vforkSnapshot?.childPid;
  const canonicalVforkChildMatches = canonicalVforkChild === null
    || canonicalVforkChild === undefined
    || canonicalVforkChild === activeVforkChildPid;
  const useVforkChildIdentity = transientVforkChild && canonicalVforkChildMatches;
  const pid = useVforkChildIdentity ? currentPid : handle.pid;
  const ppid = useVforkChildIdentity
    ? handle.kernel.process_child_ppid?.(pid)
    : (handle.parentPid ?? 0);
  const nextPid = handle.kernel.process_next_pid?.();
  if (
    !Number.isInteger(processPtr)
    || processPtr <= 0
    || !Number.isInteger(pid)
    || pid <= 0
    || ppid === undefined
    || !Number.isInteger(ppid)
    || ppid < 0
    || nextPid === undefined
    || !Number.isInteger(nextPid)
    || nextPid <= 0
  ) {
    return undefined;
  }
  return { processPtr, pid, ppid, nextPid };
}

function appendDetailedSchedulerQueueDebugLog(
  handle: ProcessHandle,
  formatEvent: () => string,
): void {
  if (!schedulerQueueDebugLogEnabled()) {
    return;
  }
  appendSchedulerQueueDebugLog(handle, formatEvent());
}

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
  takeThreadWorkerBlockedTimeoutExpired?(handle: ProcessHandle, worker: Worker): boolean;
  isSupervisorFutexWaiter(handle: ProcessHandle, worker: Worker): boolean;
  completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number, exitVmPtr?: number): Promise<void>;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleWorkerMessage(handle: ProcessHandle, worker: Worker, msg: ThreadWorkerOutbound): void;
  includeDetailedSupervisorDiagnostics?: boolean;
}

function appendSchedulerFsTiming(
  deps: WorkerSchedulerQueueDeps,
  handle: ProcessHandle,
  phase: string,
  detail = "",
): void {
  if (!FS_LIFECYCLE_DEBUG_ENABLED) {
    return;
  }
  const event = `fs-scheduler-${phase}:${handle.pid}:t=${Date.now()}${detail}`;
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
  console.error(`[tidemark-fs] ${event}`);
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
  clearSupervisorHostTimer(handle, blocked.vmPtr);
  deps.clearHostBlockedRetryTimer(handle, worker);
  hostExecutionState(handle).hostBlockedWorkers.delete(worker);
}

export function createWorkerSchedulerQueueHelpers(
  deps: WorkerSchedulerQueueDeps,
): WorkerSchedulerQueueHelpers {
  const observedForkSharedSyncGates = new WeakMap<ProcessHandle, Promise<void>>();
  const fdTableCoherenceRetryTimers = new WeakMap<ProcessHandle, ReturnType<typeof setTimeout>>();
  const supervisorResumeHostBoundary = createSupervisorResumeHostBoundary({
    threadWorkerStates: deps.threadWorkerStates,
    appendWorkerDebugLog: deps.appendWorkerDebugLog,
    clearBlockedWorker: (handle, worker) => clearBlockedWorker(deps, handle, worker),
    removeVforkReadyWorker,
    retryDeferredResumes: (handle) => pumpProcess(handle),
    includeDetailedDiagnostics: deps.includeDetailedSupervisorDiagnostics,
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
      if (threadLatestStepAnswered(thread)) {
        hostExecutionState(handle).runningWorker = null;
        clearThreadRunningGeneration(thread);
        deps.appendWorkerDebugLog(handle, "running-worker-cleared:answered");
        return null;
      }
      return runningWorker;
    }
    hostExecutionState(handle).runningWorker = null;
    if (thread) {
      clearThreadRunningGeneration(thread);
    }
    deps.appendWorkerDebugLog(handle, "running-worker-cleared:stale");
    return null;
  }

  function selectActiveVforkChildReadyWorker(
    handle: ProcessHandle,
  ): Worker | undefined {
    const state = hostExecutionState(handle);
    const snapshot = state.vforkSnapshot;
    const childPid = snapshot?.childPid ?? null;
    if (
      snapshot === null
      || childPid === null
      || childPid <= 0
      || state.vforkReadyWorkers.length !== 1
    ) {
      return undefined;
    }
    const worker = state.vforkReadyWorkers[0];
    const thread = deps.threadWorkerStates.get(worker);
    if (
      worker !== state.vforkWorker
      || thread === undefined
      || !processHasWorker(handle, worker)
      || getThreadWaitingInWorkerStatus(thread) !== null
      || getThreadOwnerAsyncStatus(thread) !== null
    ) {
      return undefined;
    }
    const status = handle.kernel.thread_status(thread.vmPtr);
    const currentPid = handle.kernel.process_current_pid(thread.vmPtr);
    const processPtr = handle.kernel.thread_process_ptr(thread.vmPtr);
    if (
      (
        status !== handle.kernel.abi_status_vfork_wait()
        && status !== handle.kernel.abi_status_ok()
      )
      || currentPid !== childPid
      || processPtr === 0
    ) {
      return undefined;
    }
    removeVforkReadyWorker(handle, worker);
    clearBlockedWorker(deps, handle, worker);
    clearThreadWaitingInWorkerStatus(thread);
    clearThreadRunningGeneration(thread);
    deps.appendWorkerDebugLog(
      handle,
      `supervisor-resume:runnable:vfork-child-ready:0x${thread.vmPtr.toString(16)}`
        + `:child=${childPid}:process=0x${processPtr.toString(16)}`
        + `:status=${status}:ready=1`,
    );
    return worker;
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
    const fdEntries = snapshotAuthoritativeProcessFdEntriesForThread(handle, vmPtr);
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
    const processIdentity = snapshotThreadWorkerProcessIdentity(handle, vmPtr);
    if (processIdentity !== undefined) {
      initMessage.processIdentity = processIdentity;
    }
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
    recordPostedFdEntriesForNextStatus(thread, fdEntries);
    recordPostedOfdSlotsForNextStatus(thread, initOfdSlots);
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
    if (FS_LIFECYCLE_DEBUG_ENABLED) {
      appendSchedulerFsTiming(
        deps,
        handle,
        "enqueue-entry",
        `:vm=0x${(thread?.vmPtr ?? 0).toString(16)}`
          + `:running=${hostExecutionState(handle).runningWorker === null ? 0 : 1}`
          + `:ownerAsync=${getThreadOwnerAsyncStatus(thread) ?? "null"}`
          + `:waiting=${getThreadWaitingInWorkerStatus(thread) ?? "null"}`,
      );
    }
    if (
      hostExecutionState(handle).runningWorker === worker
      && processHasWorker(handle, worker)
      && thread !== undefined
    ) {
      if (!threadLatestStepAnswered(thread)) {
        return;
      }
      hostExecutionState(handle).runningWorker = null;
      clearThreadRunningGeneration(thread);
      appendSchedulerQueueDebugLog(
        handle,
        `running-worker-cleared:enqueue-answered:0x${thread.vmPtr.toString(16)}`,
      );
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
        if (FS_LIFECYCLE_DEBUG_ENABLED) {
          appendSchedulerFsTiming(
            deps,
            handle,
            "enqueue-defer-owner-async",
            `:vm=0x${thread.vmPtr.toString(16)}:async=${ownerAsyncStatus}`,
          );
        }
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
      appendDetailedSchedulerQueueDebugLog(
        handle,
        () => `runnable-published:0x${vmPtr.toString(16)}${formatSchedulerStepDebugSuffix(handle, vmPtr)}`,
      );
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
    if (FS_LIFECYCLE_DEBUG_ENABLED) {
      appendSchedulerFsTiming(
        deps,
        handle,
        "enqueue-ready",
        `:vm=0x${(vmPtr ?? 0).toString(16)}:ready=${hostExecutionState(handle).vforkReadyWorkers.length}`,
      );
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

  function completeFaultedWaitingWorker(handle: ProcessHandle): boolean {
    const faultStatus = handle.kernel.abi_status_fault?.();
    if (faultStatus === undefined) {
      return false;
    }
    for (const worker of processWorkerList(handle)) {
      const thread = deps.threadWorkerStates.get(worker);
      const waitingStatus = getThreadWaitingInWorkerStatus(thread);
      if (thread === undefined || waitingStatus === null) {
        continue;
      }
      if (handle.kernel.thread_status(thread.vmPtr) !== faultStatus) {
        continue;
      }
      const decision = readSupervisorFaultExitDecision(handle, thread.vmPtr);
      if (decision === undefined) {
        deps.failProcess(
          handle,
          new Error(`supervisor fault exit decision required for vm=0x${thread.vmPtr.toString(16)}`),
        );
        return true;
      }

      clearBlockedWorker(deps, handle, worker);
      removeVforkReadyWorker(handle, worker);
      clearSupervisorHostTimer(handle, thread.vmPtr);
      clearThreadWaitingInWorkerStatus(thread);
      clearThreadOwnerAsyncStatus(thread);
      clearThreadRunningGeneration(thread);
      if (hostExecutionState(handle).runningWorker === worker) {
        hostExecutionState(handle).runningWorker = null;
      }
      if (hostExecutionState(handle).vforkWorker === worker) {
        hostExecutionState(handle).vforkWorker = null;
      }

      if (decision.kind === supervisorFaultExitKindProcessExit(handle.kernel)) {
        deps.appendWorkerDebugLog(
          handle,
          `complete-exit:waiting-fault:0x${decision.exitVmPtr.toString(16)}`
            + `:wait=${waitingStatus}:seq=${decision.seq.toString()}`
            + `:threads=${decision.liveThreads}/${decision.remainingThreads}`,
        );
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `complete-exit-request:waiting-fault:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:exitVm=0x${decision.exitVmPtr.toString(16)}`
            + `:exit=${decision.exitCode}:${decision.termSignal}`
            + `:seq=${decision.seq.toString()}`
            + `:wait=${waitingStatus}`
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

      deps.appendWorkerDebugLog(
        handle,
        `fault-wait-worker-delete:0x${thread.vmPtr.toString(16)}`
          + `:wait=${waitingStatus}:seq=${decision.seq.toString()}`
          + `:threads=${decision.liveThreads}/${decision.remainingThreads}`,
      );
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `fault-wait-worker-delete:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
          + `:wait=${waitingStatus}`
          + `:exit=${decision.exitCode}:${decision.termSignal}`
          + `:seq=${decision.seq.toString()}`
          + `:threads=${decision.liveThreads}/${decision.remainingThreads}`,
      );
      deleteProcessWorker(handle, worker);
      deps.threadWorkerStates.delete(worker);
      releaseWorker(worker);
      pumpProcess(handle);
      return true;
    }
    return false;
  }

  function selectBlockedResumeWorkerAfterSupervisorNone(handle: ProcessHandle): Worker | null {
    for (const worker of processWorkerList(handle)) {
      const thread = deps.threadWorkerStates.get(worker);
      if (thread === undefined) {
        continue;
      }
      if (!deps.takeThreadWorkerBlockedResume(handle, worker)) {
        continue;
      }
      if (deps.isSupervisorFutexWaiter(handle, worker)) {
        deps.appendWorkerDebugLog(
          handle,
          `supervisor-resume:runnable:discard-blocked-resume-futex-waiter:0x${thread.vmPtr.toString(16)}`,
        );
        continue;
      }
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-resume:runnable:blocked-resume:0x${thread.vmPtr.toString(16)}`
          + `:status=${handle.kernel.thread_status(thread.vmPtr)}`,
      );
      return worker;
    }
    return null;
  }

  function pumpProcess(handle: ProcessHandle): void {
    if (processExitInProgress(handle)) {
      return;
    }
    if (FS_LIFECYCLE_DEBUG_ENABLED) {
      appendSchedulerFsTiming(
        deps,
        handle,
        "pump-entry",
        `:running=${hostExecutionState(handle).runningWorker === null ? 0 : 1}`
          + `:ready=${hostExecutionState(handle).vforkReadyWorkers.length}`
          + `:blocked=${hostExecutionState(handle).hostBlockedWorkers.size}`,
      );
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
    const regularFileFsInFlight = hostExecutionState(handle).regularFileFsInFlight;
    if (regularFileFsInFlight !== null) {
      appendSchedulerQueueDebugLog(
        handle,
        `pump-held:regular-file-fs:vm=0x${regularFileFsInFlight.vmPtr.toString(16)}`
          + `:fd=${regularFileFsInFlight.fd}:ofd=${regularFileFsInFlight.ofdId}`,
      );
      if (FS_LIFECYCLE_DEBUG_ENABLED) {
        appendSchedulerFsTiming(
          deps,
          handle,
          "pump-held-regular-file-fs",
          `:vm=0x${regularFileFsInFlight.vmPtr.toString(16)}`
            + `:fd=${regularFileFsInFlight.fd}:ofd=${regularFileFsInFlight.ofdId}`,
        );
      }
      return;
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
    if (
      [...hostExecutionState(handle).hostBlockedWorkers.values()]
        .some((blocked) => isBlockedRpcPending(blocked))
    ) {
      appendSchedulerQueueDebugLog(handle, "pump-held:blocked-rpc-pending");
      return;
    }
    if (!supervisorProcessFdTableIsCoherent(handle, handle.vmPtr)) {
      appendSchedulerQueueDebugLog(
        handle,
        `pump-held:fd-table-incoherent:${describeSupervisorProcessFdTableIncoherence(handle, handle.vmPtr)}`,
      );
      if (!fdTableCoherenceRetryTimers.has(handle)) {
        const timer = setTimeout(() => {
          fdTableCoherenceRetryTimers.delete(handle);
          pumpProcess(handle);
        }, FD_TABLE_COHERENCE_RETRY_MS);
        fdTableCoherenceRetryTimers.set(handle, timer);
      }
      return;
    }
    const coherenceRetryTimer = fdTableCoherenceRetryTimers.get(handle);
    if (coherenceRetryTimer !== undefined) {
      clearTimeout(coherenceRetryTimer);
      fdTableCoherenceRetryTimers.delete(handle);
    }
    const supervisorResumeSelected = supervisorResumeHostBoundary.drainSupervisorResumeQueue(handle);
    if (completeFaultedWaitingWorker(handle)) {
      return;
    }
    if (completeFaultedProcessWithoutWorkers(handle)) {
      return;
    }
    let next: Worker | undefined;
    let selectedForBlockedResume = false;
    if (supervisorResumeSelected !== null) {
      next = supervisorResumeSelected.worker;
      selectedForBlockedResume = supervisorResumeSelected.resumeBlockedSyscall;
    } else {
      next = selectActiveVforkChildReadyWorker(handle);
      if (!next) {
        const supervisorSelected = supervisorResumeHostBoundary.selectSupervisorRunnableWorker(handle);
        if (supervisorSelected !== undefined) {
          if (supervisorSelected !== null) {
            next = supervisorSelected.worker;
            selectedForBlockedResume = supervisorSelected.resumeBlockedSyscall;
          } else {
            const blockedResumeWorker = selectBlockedResumeWorkerAfterSupervisorNone(handle);
            if (blockedResumeWorker !== null) {
              next = blockedResumeWorker;
              selectedForBlockedResume = true;
            }
          }
        } else {
          deps.appendWorkerDebugLog(
            handle,
            `supervisor-resume:runnable:missing-export:ready=${hostExecutionState(handle).vforkReadyWorkers.length}`,
          );
          return;
        }
      }
    }
    if (!next) {
      if (FS_LIFECYCLE_DEBUG_ENABLED) {
        appendSchedulerFsTiming(
          deps,
          handle,
          "pump-none",
          `:ready=${hostExecutionState(handle).vforkReadyWorkers.length}`,
        );
      }
      return;
    }
    hostExecutionState(handle).runningWorker = next;
    const thread = deps.threadWorkerStates.get(next);
    const stepBudget = SCHEDULER_STEP_BUDGET;
    const vmPtr = thread?.vmPtr;
    if (FS_LIFECYCLE_DEBUG_ENABLED) {
      appendSchedulerFsTiming(
        deps,
        handle,
        "pump-selected",
        `:vm=0x${(vmPtr ?? 0).toString(16)}:budget=${stepBudget}`,
      );
    }
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
      if (FS_LIFECYCLE_DEBUG_ENABLED) {
        appendSchedulerFsTiming(
          deps,
          handle,
          "step-activity",
          `:vm=0x${vmPtr.toString(16)}:step=${activity.stepCount}:pc=0x${activity.lastStepPc.toString(16)}`,
        );
      }
      incrementBoundedActivityCounter(activity.hotStepPcs, `0x${activity.lastStepPc.toString(16)}`);
      pushRecentStepActivity(activity, {
        pc: activity.lastStepPc,
        sp: activity.lastStepSp,
        s11: activity.lastStepS11,
        atMs: activity.lastStepAtMs,
      });
      const brokenThreadState = hasBrokenSchedulerThreadState(handle, vmPtr);
      appendDetailedSchedulerQueueDebugLog(
        handle,
        () => `step:0x${vmPtr.toString(16)}${formatSchedulerStepDebugSuffix(handle, vmPtr)}`,
      );
      if (brokenThreadState) {
        deps.appendWorkerDebugLog(
          handle,
          `step_break:0x${vmPtr.toString(16)}${formatSchedulerStepDebugSuffix(handle, vmPtr)}`,
        );
      }
    }
    publishRuntimeBridgeEpoch(handle.runtimeBridge);
    const fdEntries = takePendingAuthoritativeFdEntriesForNextStep(
      thread,
      snapshotProcessFdEntriesForThread(handle, vmPtr ?? 0),
    );
    const pendingKernelState = takePendingKernelStateForNextStep(thread);
    const stepSequence = thread === undefined
      ? 0
      : getThreadActivityState(thread).stepCount;
    const stepMessage: ThreadWorkerInbound = {
      type: "step",
      stepSequence,
      budget: stepBudget,
      kernelState: pendingKernelState ?? handle.kernelState,
    };
    const pendingKernelMemoryWrites = takePendingKernelMemoryWritesForNextStep(thread);
    if (pendingKernelMemoryWrites.length > 0) {
      stepMessage.kernelMemoryWrites = pendingKernelMemoryWrites;
    }
    const processIdentity = snapshotThreadWorkerProcessIdentity(handle, vmPtr ?? 0);
    if (processIdentity !== undefined) {
      stepMessage.processIdentity = processIdentity;
    }
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
    if (thread) {
      recordPostedFdEntriesForNextStatus(thread, fdEntries);
      recordPostedOfdSlotsForNextStatus(thread, stepOfdSlots);
    }
    appendDetailedSchedulerQueueDebugLog(
      handle,
      () => `step-fds:0x${(vmPtr ?? 0).toString(16)}:${formatSchedulerStepFdSnapshot(handle, vmPtr, fdEntries, stepOfdSlots)}`,
    );
    const pendingPipeSlots = takePendingPipeSlotsForNextStep(thread);
    if (pendingPipeSlots.length > 0) {
      stepMessage.pipeSlots = pendingPipeSlots;
    }
    const pendingGuestMemoryWrites = takePendingGuestMemoryWritesForNextStep(thread);
    if (pendingGuestMemoryWrites.length > 0) {
      stepMessage.guestMemoryWrites = pendingGuestMemoryWrites;
    }
    const pendingExitedThreads = takePendingExitedThreadsForNextStep(thread);
    if (pendingExitedThreads.length > 0) {
      stepMessage.exitedThreads = pendingExitedThreads;
    }
    if (thread) {
      const pendingBlockedResume = deps.takeThreadWorkerBlockedResume(handle, next);
      const blockedTimeoutExpired = deps.takeThreadWorkerBlockedTimeoutExpired?.(handle, next)
        === true;
      if (selectedForBlockedResume || pendingBlockedResume || blockedTimeoutExpired) {
        stepMessage.resumeBlockedSyscall = true;
        if (blockedTimeoutExpired) {
          stepMessage.resumeBlockedSyscallTimeoutExpired = true;
        }
      }
    }
    const postStartMs = FS_LIFECYCLE_DEBUG_ENABLED ? Date.now() : 0;
    if (FS_LIFECYCLE_DEBUG_ENABLED) {
      appendSchedulerFsTiming(
        deps,
        handle,
        "step-post-start",
        `:vm=0x${(vmPtr ?? 0).toString(16)}:fdEntries=${fdEntries?.length ?? 0}:ofdSlots=${stepOfdSlots?.length ?? 0}`,
      );
    }
    next.postMessage(stepMessage);
    if (FS_LIFECYCLE_DEBUG_ENABLED) {
      appendSchedulerFsTiming(
        deps,
        handle,
        "step-post-done",
        `:vm=0x${(vmPtr ?? 0).toString(16)}:dt=${Date.now() - postStartMs}`,
      );
    }
  }

  return {
    attachThreadWorker,
    enqueueWorker,
    pumpProcess,
  };
}
