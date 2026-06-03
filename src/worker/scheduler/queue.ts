import {
  setWorkerErrorHandler,
  setWorkerMessageErrorHandler,
  setWorkerMessageHandler,
} from "../../bridge/compat.js";
import { publishRuntimeBridgeEpoch } from "../../bridge/runtime-state.js";
import { createReadyWorkerFairness } from "./fairness.js";
import { createFutexReconciliation } from "./futex.js";
import type {
  AttachThreadWorkerRequest,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type { ThreadWorkerOutbound } from "../../messages.js";
import { kernelStackSlotForWorker } from "../pool.js";
import { snapshotProcessFdEntriesForThread } from "../ownership/fd-table.js";
import {
  restoreRegularFileOffsetsForStep,
  snapshotOfdSlotsForThread,
} from "../ownership/ofd-table.js";
import {
  composeHandlePipeUsed,
  preserveHostStdioPipeControlForStep,
} from "../ownership/pipe-control.js";
import { snapshotSocketStateForThread } from "../ownership/socket-control.js";
import type {
  ThreadWorkerInbound,
  WorkerSchedulerQueueHelpers,
} from "./types.js";

const DEFAULT_NODE_SCHEDULER_STEP_BUDGET = 5_000_000;
const DEFAULT_BROWSER_SCHEDULER_STEP_BUDGET = 10_000;
const MIN_SCHEDULER_STEP_BUDGET = 1_000;
const MAX_SCHEDULER_STEP_BUDGET = 5_000_000;
const SCHEDULER_STEP_BUDGET = readSchedulerStepBudget();
const SCHEDULER_QUEUE_DEBUG_LOG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_SCHEDULER_QUEUE_DEBUG_LOG ?? "") === "1";
const SCHEDULER_QUEUE_DEBUG_LOG_LIMIT = 256;
let nextThreadWorkerGeneration = 1;
const HOT_PC_COUNTER_LIMIT = 32;
const STALE_SLEEP_RUNNING_WORKER_REPAIR_MS = 2_000;

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

function formatChildExitRecords(
  handle: ProcessHandle,
): Array<{ childPid: number; waitStatus: number; parentPid: number }> {
  return [...handle.childExitSyncRecords].map(([childPid, waitStatus]) => ({
    childPid,
    waitStatus,
    parentPid: handle.pid,
  }));
}

interface WorkerSchedulerQueueDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleWorkerMessage(handle: ProcessHandle, worker: Worker, msg: ThreadWorkerOutbound): void;
}

function formatStepDebugSuffix(handle: ProcessHandle, vmPtr: number): string {
  const parts: string[] = [];
  const pid = handle.kernel.process_current_pid?.(vmPtr);
  if (pid !== undefined) {
    parts.push(`pid=${pid}`);
  }
  const tid = handle.kernel.thread_tid?.(vmPtr);
  if (tid !== undefined) {
    parts.push(`tid=${tid}`);
  }
  const pc = handle.kernel.thread_pc?.(vmPtr);
  if (pc !== undefined) {
    parts.push(`pc=0x${pc.toString(16)}`);
  }
  const ra = handle.kernel.thread_ra?.(vmPtr);
  if (ra !== undefined) {
    parts.push(`ra=0x${ra.toString(16)}`);
  }
  const sp = handle.kernel.thread_sp?.(vmPtr);
  if (sp !== undefined) {
    parts.push(`sp=0x${sp.toString(16)}`);
  }
  for (let index = 0; index <= 32; index += 1) {
    const stackWord = handle.kernel.thread_stack_word?.(vmPtr, index);
    if (stackWord !== undefined) {
      parts.push(`stk${index}=0x${stackWord.toString(16)}`);
    }
  }
  const a0 = handle.kernel.thread_a0?.(vmPtr);
  if (a0 !== undefined) {
    parts.push(`a0=${a0.toString()}`);
  }
  const a1 = handle.kernel.thread_a1?.(vmPtr);
  if (a1 !== undefined) {
    parts.push(`a1=${a1.toString()}`);
  }
  const a2 = handle.kernel.thread_reg?.(vmPtr, 12);
  if (a2 !== undefined) {
    parts.push(`a2=${a2.toString()}`);
  }
  const a3 = handle.kernel.thread_reg?.(vmPtr, 13);
  if (a3 !== undefined) {
    parts.push(`a3=${a3.toString()}`);
  }
  const gp = handle.kernel.thread_gp?.(vmPtr);
  if (gp !== undefined) {
    parts.push(`gp=0x${gp.toString(16)}`);
  }
  const tp = handle.kernel.thread_tp?.(vmPtr);
  if (tp !== undefined) {
    parts.push(`tp=0x${tp.toString(16)}`);
  }
  for (const [label, reg] of [["s0", 8], ["s1", 9], ["s2", 18], ["s3", 19], ["s4", 20], ["s5", 21], ["s6", 22]] as const) {
    const value = handle.kernel.thread_reg?.(vmPtr, reg);
    if (value !== undefined) {
      parts.push(`${label}=0x${value.toString(16)}`);
    }
  }
  const s11 = handle.kernel.thread_s11?.(vmPtr);
  if (s11 !== undefined) {
    parts.push(`s11=0x${s11.toString(16)}`);
  }
  return parts.length > 0 ? `:${parts.join(":")}` : "";
}

function hasBrokenThreadState(handle: ProcessHandle, vmPtr: number): boolean {
  const pc = handle.kernel.thread_pc?.(vmPtr) ?? 0n;
  const sp = handle.kernel.thread_sp?.(vmPtr) ?? 0n;
  const s11 = handle.kernel.thread_s11?.(vmPtr) ?? 0n;
  return pc < 0x1000n || sp === 0n || s11 === 0n;
}

function appendSchedulerQueueDebugLog(
  deps: WorkerSchedulerQueueDeps,
  handle: ProcessHandle,
  event: string,
): void {
  if (!SCHEDULER_QUEUE_DEBUG_LOG_ENABLED) {
    return;
  }
  const queueDebugCount = handle.workerDebugLog.reduce(
    (count, entry) => count + (
      entry.startsWith("pump-") || entry.startsWith("step:") || entry.startsWith("ready:")
        ? 1
        : 0
    ),
    0,
  );
  if (queueDebugCount >= SCHEDULER_QUEUE_DEBUG_LOG_LIMIT) {
    if (!handle.workerDebugLog.includes("pump-debug-limit")) {
      deps.appendWorkerDebugLog(handle, "pump-debug-limit");
    }
    return;
  }
  deps.appendWorkerDebugLog(handle, event);
}

function removeReadyWorker(handle: ProcessHandle, worker: Worker): void {
  for (let index = handle.readyWorkers.length - 1; index >= 0; index -= 1) {
    if (handle.readyWorkers[index] === worker) {
      handle.readyWorkers.splice(index, 1);
    }
  }
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

function statusBlocksInOwnerScheduler(handle: ProcessHandle, status: number): boolean {
  return status === handle.kernel.abi_status_epoll_wait()
    || status === handle.kernel.abi_status_fs()
    || status === handle.kernel.abi_status_sleep()
    || status === handle.kernel.abi_status_pipe_wait()
    || status === handle.kernel.abi_status_wait4()
    || status === handle.kernel.abi_status_flock_wait();
}

function incrementBoundedCounter(
  counters: Record<string, number>,
  key: string,
  maxEntries = HOT_PC_COUNTER_LIMIT,
): void {
  const existing = counters[key];
  if (existing !== undefined) {
    counters[key] = existing + 1;
    return;
  }
  const entries = Object.keys(counters);
  if (entries.length < maxEntries) {
    counters[key] = 1;
    return;
  }
  for (const entry of entries) {
    const next = (counters[entry] ?? 0) - 1;
    if (next <= 0) {
      delete counters[entry];
      continue;
    }
    counters[entry] = next;
  }
}

export function createWorkerSchedulerQueueHelpers(
  deps: WorkerSchedulerQueueDeps,
): WorkerSchedulerQueueHelpers {
  const observedForkSharedSyncGates = new WeakMap<ProcessHandle, Promise<void>>();
  const readyWorkerFairness = createReadyWorkerFairness(pumpProcess);
  const futexReconciliation = createFutexReconciliation({
    threadWorkerStates: deps.threadWorkerStates,
    appendWorkerDebugLog: deps.appendWorkerDebugLog,
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
      if (handle.kernelShare.forkSharedSyncPromise === promise) {
        handle.kernelShare.forkSharedSyncPromise = null;
      }
      if (handle.forkSharedSyncPromise === promise) {
        handle.forkSharedSyncPromise = null;
      }
      pumpProcess(handle);
    };
    promise.then(onSettled, onSettled);
  }

  function runningWorkerStepAppearsIdle(handle: ProcessHandle, thread: ThreadWorkerState): boolean {
    const activeStepVmPtr = handle.kernel.debug_thread_step_vm_ptr?.();
    if (activeStepVmPtr === undefined) {
      return false;
    }
    if (activeStepVmPtr === thread.vmPtr) {
      return false;
    }
    const lastStepAtMs = thread.lastStepAtMs;
    const lastStatusAtMs = thread.lastStatusAtMs;
    if (lastStepAtMs === null) {
      return false;
    }
    const hasUnansweredStep = thread.stepCount > thread.statusCount;
    if (lastStatusAtMs !== null && lastStepAtMs <= lastStatusAtMs && !hasUnansweredStep) {
      return false;
    }
    return Date.now() - lastStepAtMs >= STALE_SLEEP_RUNNING_WORKER_REPAIR_MS;
  }

  function sanitizeRunningWorker(handle: ProcessHandle): Worker | null {
    const runningWorker = handle.runningWorker;
    if (runningWorker === null) {
      return null;
    }
    const thread = deps.threadWorkerStates.get(runningWorker);
    if (
      handle.workers.has(runningWorker)
      && thread
      && (thread.runningGeneration ?? thread.generation) === thread.generation
    ) {
      const status = handle.kernel.thread_status(thread.vmPtr);
      if (status === handle.kernel.abi_status_futex_wait()) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
        thread.waitingInWorkerStatus = status;
        deps.appendWorkerDebugLog(handle, `running-worker-cleared:futex-wait:0x${thread.vmPtr.toString(16)}`);
        return null;
      }
      if (statusBlocksInOwnerScheduler(handle, status)) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
        deps.blockWorker(handle, runningWorker, status, thread.vmPtr);
        deps.appendWorkerDebugLog(handle, `running-worker-cleared:blocking-status-${status}:0x${thread.vmPtr.toString(16)}`);
        return null;
      }
      if (
        handle.readyWorkers.length > 0
        && status === handle.kernel.abi_status_ok()
        && runningWorkerStepAppearsIdle(handle, thread)
      ) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
        deps.appendWorkerDebugLog(
          handle,
          `running-worker-cleared:stale-ok:0x${thread.vmPtr.toString(16)}:last=${thread.lastStatus ?? "null"}`,
        );
        return null;
      }
      return runningWorker;
    }
    handle.runningWorker = null;
    if (thread) {
      thread.runningGeneration = null;
    }
    deps.appendWorkerDebugLog(handle, "running-worker-cleared:stale");
    return null;
  }

  function hasExclusiveVforkWorker(handle: ProcessHandle): boolean {
    const worker = handle.vforkWorker;
    if (handle.vforkSnapshot === null || worker === null) {
      return false;
    }
    if (sanitizeRunningWorker(handle) === worker) {
      return true;
    }
    if (handle.readyWorkers.includes(worker)) {
      return true;
    }
    if (handle.blockedWorkers.has(worker)) {
      return true;
    }
    const thread = deps.threadWorkerStates.get(worker);
    return thread?.waitingInWorkerStatus !== null;
  }

  function assertThreadOwnershipInvariant(
    handle: ProcessHandle,
    vmPtr: number,
    context: string,
  ): void {
    if (vmPtr === 0 || handle.pid <= 0) {
      return;
    }
    const currentPid = handle.kernel.process_current_pid(vmPtr);
    if (currentPid !== handle.pid) {
      throw new Error(
        `${context} worker/process ownership mismatch vm=0x${vmPtr.toString(16)}`
          + ` expected_pid=${handle.pid} actual_pid=${currentPid}`,
      );
    }
  }

  function detachTransferredWorker(handle: ProcessHandle, worker: Worker): void {
    clearBlockedWorker(handle, worker);
    removeReadyWorker(handle, worker);
    if (handle.runningWorker === worker) {
      handle.runningWorker = null;
    }
    if (handle.vforkWorker === worker) {
      handle.vforkWorker = null;
    }
    handle.workers.delete(worker);
  }

  function attachThreadWorker(handle: ProcessHandle, request: AttachThreadWorkerRequest): void {
    const { worker, vmPtr, forcePrepare, transferFrom, preserveMemoryBytes, preserveMemoryRanges } = request;
    const generation = nextThreadWorkerGeneration++;
    const existingThread = deps.threadWorkerStates.get(worker);
    const reinitWithinHandle = existingThread !== undefined
      && transferFrom === undefined
      && existingThread.ownerPid === handle.pid;
    if (existingThread) {
      if (reinitWithinHandle) {
        clearBlockedWorker(handle, worker);
        removeReadyWorker(handle, worker);
        if (handle.runningWorker === worker) {
          handle.runningWorker = null;
        }
        if (handle.vforkWorker === worker) {
          handle.vforkWorker = null;
        }
      } else {
        const sourceHandle = transferFrom;
        if (!sourceHandle) {
          throw new Error(
            `attachThreadWorker requires transferFrom for existing worker owner=${existingThread.ownerPid}`
              + ` target=${handle.pid} vm=0x${vmPtr.toString(16)}`,
          );
        }
        if (sourceHandle === handle) {
          throw new Error(
            `attachThreadWorker transferFrom must differ from target pid=${handle.pid}`
              + ` vm=0x${vmPtr.toString(16)}`,
          );
        }
        if (existingThread.ownerPid !== sourceHandle.pid) {
          throw new Error(
            `attachThreadWorker owner mismatch worker_owner=${existingThread.ownerPid}`
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
    handle.workers.add(worker);
    deps.appendWorkerDebugLog(
      handle,
      `${reinitWithinHandle ? "reinit" : "attach"}:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`,
    );
    deps.threadWorkerStates.set(worker, {
      vmPtr,
      ownerPid: handle.pid,
      generation,
      runningGeneration: null,
      reinitCount: reinitWithinHandle ? (existingThread?.reinitCount ?? 0) + 1 : 0,
      stepCount: 0,
      totalStepBudget: 0,
      totalStepWallMs: 0,
      maxStepWallMs: 0,
      lastStepDurationMs: null,
      statusCount: 0,
      statusCounts: {},
      lastStatus: null,
      lastStepAtMs: null,
      lastStatusAtMs: null,
      lastStepProcessPtr: null,
      lastStepPc: null,
      lastStepSp: null,
      lastStepS11: null,
      hotStepPcs: {},
      lastStatusPc: null,
      lastStatusSp: null,
      lastStatusS11: null,
      hotStatusPcs: {},
      initialized: false,
      waitingInWorkerStatus: null,
      waitingInWorkerReconcileDeferred: false,
      ownerAsyncStatus: null,
      resumeBlockedOnNextStep: false,
      pendingPipeSlotsForNextStep: [],
      pendingGuestMemoryWrites: [],
      resumeWorkersOnReady: [],
    });
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
      deps.failProcess(handle, new Error(
        `thread worker error vm=0x${vmPtr.toString(16)} ${detail || "unknown worker error"}`,
      ));
      event.preventDefault?.();
    });
    setWorkerMessageErrorHandler(worker, (event) => {
      deps.appendWorkerDebugLog(handle, `worker-messageerror:0x${vmPtr.toString(16)}`);
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
      guestWriteWatchRegions: handle.guestWriteWatchRegions,
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
    const initChildExitRecords = formatChildExitRecords(handle);
    if (initChildExitRecords.length > 0) {
      initMessage.childExitRecords = initChildExitRecords;
    }
    worker.postMessage(initMessage);
  }

  function enqueueWorker(handle: ProcessHandle, worker: Worker): void {
    if (handle.exitInProgress) {
      return;
    }
    const thread = deps.threadWorkerStates.get(worker);
    if (
      handle.runningWorker === worker
      && handle.workers.has(worker)
      && thread !== undefined
    ) {
      return;
    }
    if (
      (thread?.waitingInWorkerStatus ?? null) !== null
      || (thread?.ownerAsyncStatus ?? null) !== null
    ) {
      removeReadyWorker(handle, worker);
      pumpProcess(handle);
      return;
    }
    if (!handle.readyWorkers.includes(worker)) {
      const vmPtr = thread?.vmPtr;
      if (vmPtr !== undefined) {
        const brokenThreadState = hasBrokenThreadState(handle, vmPtr);
        appendSchedulerQueueDebugLog(deps, handle, `ready:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`);
        if (brokenThreadState) {
          deps.appendWorkerDebugLog(
            handle,
            `ready_break:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`,
          );
        }
      }
      handle.readyWorkers.push(worker);
      if (handle.runningWorker !== null) {
        readyWorkerFairness.wakeRunningWorkerForReadyWork(handle);
      }
      if (handle.vforkSnapshot !== null && handle.vforkWorker === worker) {
      }
    }
    pumpProcess(handle);
  }

  function recoverRunnableIdleWorkers(handle: ProcessHandle): void {
    const exclusiveWorker = handle.vforkSnapshot !== null ? handle.vforkWorker : null;
    for (const worker of handle.workers) {
      if (handle.runningWorker === worker || handle.readyWorkers.includes(worker) || handle.blockedWorkers.has(worker)) {
        continue;
      }
      if (exclusiveWorker !== null && worker !== exclusiveWorker) {
        continue;
      }
      const thread = deps.threadWorkerStates.get(worker);
      if (
        !thread?.initialized
        || thread.waitingInWorkerStatus !== null
        || thread.ownerAsyncStatus !== null
      ) {
        continue;
      }
      if (handle.kernel.thread_status(thread.vmPtr) !== handle.kernel.abi_status_ok()) {
        continue;
      }
      deps.appendWorkerDebugLog(handle, `ready_recover:0x${thread.vmPtr.toString(16)}${formatStepDebugSuffix(handle, thread.vmPtr)}`);
      handle.readyWorkers.push(worker);
    }
  }

  function pumpProcess(handle: ProcessHandle): void {
    if (handle.exitInProgress) {
      return;
    }
    const forkSharedSyncPromise = handle.kernelShare.forkSharedSyncPromise;
    if (forkSharedSyncPromise !== null) {
      appendSchedulerQueueDebugLog(deps, handle, `pump-held:fork-shared-sync:ready=${handle.readyWorkers.length}`);
      observeForkSharedSyncGate(handle, forkSharedSyncPromise);
      return;
    }
    const runningWorker = sanitizeRunningWorker(handle);
    if (runningWorker !== null) {
      const thread = deps.threadWorkerStates.get(runningWorker);
      if (handle.readyWorkers.length > 0) {
        readyWorkerFairness.scheduleRunningWorkerRepairRetry(handle);
      }
      appendSchedulerQueueDebugLog(
        deps,
        handle,
        `pump-held:running:ready=${handle.readyWorkers.length}`
          + (thread ? `:vm=0x${thread.vmPtr.toString(16)}:status=${handle.kernel.thread_status(thread.vmPtr)}` : ""),
      );
      return;
    }
    futexReconciliation.reconcileResolvedWorkerLocalWaits(handle);
    recoverRunnableIdleWorkers(handle);
    let next: Worker | undefined;
    if (hasExclusiveVforkWorker(handle)) {
      const index = handle.readyWorkers.indexOf(handle.vforkWorker!);
      if (index === -1) {
        return;
      }
      const candidate = handle.readyWorkers[index];
      const candidateThread = candidate ? deps.threadWorkerStates.get(candidate) : undefined;
      if (
        (candidateThread?.ownerAsyncStatus ?? null) !== null
        || futexReconciliation.hasWorkerLocalWait(candidateThread)
      ) {
        appendSchedulerQueueDebugLog(
          deps,
          handle,
          `pump-skip-candidate:vfork:ready=${handle.readyWorkers.length}`
            + (candidateThread ? `:vm=0x${candidateThread.vmPtr.toString(16)}`
              + `:wait=${candidateThread.waitingInWorkerStatus ?? "null"}`
              + `:async=${candidateThread.ownerAsyncStatus ?? "null"}` : ""),
        );
        handle.readyWorkers.splice(index, 1);
        return;
      }
      next = handle.readyWorkers.splice(index, 1)[0];
    } else {
      let index = handle.readyWorkers.findIndex((candidate) => {
        const candidateThread = deps.threadWorkerStates.get(candidate);
        return (candidateThread?.ownerAsyncStatus ?? null) === null
          && !futexReconciliation.hasWorkerLocalWait(candidateThread);
      });
      if (index > 0) {
        handle.readyWorkers.splice(0, index);
        index = 0;
      }
      if (index === -1) {
        appendSchedulerQueueDebugLog(deps, handle, `pump-no-runnable-ready:ready=${handle.readyWorkers.length}`);
        handle.readyWorkers.length = 0;
        return;
      }
      next = handle.readyWorkers.splice(index, 1)[0];
    }
    if (!next) {
      return;
    }
    handle.runningWorker = next;
    if (handle.readyWorkers.length > 0) {
      readyWorkerFairness.scheduleRunningWorkerRepairRetry(handle);
    }
    const vmPtr = deps.threadWorkerStates.get(next)?.vmPtr;
    const thread = deps.threadWorkerStates.get(next);
    if (thread && vmPtr !== undefined) {
      appendSchedulerQueueDebugLog(
        deps,
        handle,
        `pump-run:ready=${handle.readyWorkers.length}:vm=0x${vmPtr.toString(16)}:status=${handle.kernel.thread_status(vmPtr)}`,
      );
      restoreRegularFileOffsetsForStep(handle);
      preserveHostStdioPipeControlForStep(handle, vmPtr);
      thread.stepCount += 1;
      thread.runningGeneration = thread.generation;
      thread.totalStepBudget += SCHEDULER_STEP_BUDGET;
      thread.lastStepAtMs = Date.now();
      thread.lastStepProcessPtr = handle.kernel.thread_process_ptr(vmPtr);
      thread.lastStepPc = handle.kernel.thread_pc(vmPtr);
      thread.lastStepSp = handle.kernel.thread_sp(vmPtr);
      thread.lastStepS11 = handle.kernel.thread_s11?.(vmPtr) ?? 0n;
      incrementBoundedCounter(thread.hotStepPcs, `0x${thread.lastStepPc.toString(16)}`);
      const brokenThreadState = hasBrokenThreadState(handle, vmPtr);
      appendSchedulerQueueDebugLog(deps, handle, `step:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`);
      if (brokenThreadState) {
        deps.appendWorkerDebugLog(
          handle,
          `step_break:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`,
        );
      }
    }
    publishRuntimeBridgeEpoch(handle.runtimeBridge);
    const fdEntries = snapshotProcessFdEntriesForThread(handle, vmPtr ?? 0);
    const stepMessage: ThreadWorkerInbound = {
      type: "step",
      budget: SCHEDULER_STEP_BUDGET,
      kernelState: handle.kernelState,
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
    if (thread?.pendingPipeSlotsForNextStep?.length) {
      stepMessage.pipeSlots = thread.pendingPipeSlotsForNextStep.splice(0);
    }
    const stepChildExitRecords = formatChildExitRecords(handle);
    if (stepChildExitRecords.length > 0) {
      stepMessage.childExitRecords = stepChildExitRecords;
      stepMessage.kernelState = {
        ...stepMessage.kernelState,
        pipeUsed: composeHandlePipeUsed(handle, stepMessage.kernelState.pipeUsed),
      };
    }
    if (thread && (thread.pendingGuestMemoryWrites?.length ?? 0) > 0) {
      stepMessage.guestMemoryWrites = thread.pendingGuestMemoryWrites.splice(0);
    }
    if (thread?.resumeBlockedOnNextStep) {
      thread.resumeBlockedOnNextStep = false;
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
