import {
  setWorkerErrorHandler,
  setWorkerMessageErrorHandler,
  setWorkerMessageHandler,
} from "../../bridge/compat.js";
import { publishRuntimeBridgeEpoch } from "../../bridge/runtime-state.js";
import type {
  AttachThreadWorkerRequest,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type { ThreadWorkerOutbound } from "../../messages.js";
import { kernelStackSlotForWorker } from "../pool.js";
import type {
  ThreadWorkerInbound,
  WorkerSchedulerQueueHelpers,
} from "./types.js";

const SCHEDULER_STEP_BUDGET = typeof window === "undefined" ? 100_000 : 10_000;
const SCHEDULER_QUEUE_DEBUG_LOG_ENABLED = false;
let nextThreadWorkerGeneration = 1;
const HOT_PC_COUNTER_LIMIT = 32;

interface WorkerSchedulerQueueDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
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
  function sanitizeRunningWorker(handle: ProcessHandle): Worker | null {
    const runningWorker = handle.runningWorker;
    if (runningWorker === null) {
      return null;
    }
    if (handle.workers.has(runningWorker) && deps.threadWorkerStates.has(runningWorker)) {
      return runningWorker;
    }
    handle.runningWorker = null;
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
    const { worker, vmPtr, forcePrepare, transferFrom } = request;
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
      reinitCount: reinitWithinHandle ? (existingThread?.reinitCount ?? 0) + 1 : 0,
      stepCount: 0,
      totalStepBudget: 0,
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
    worker.postMessage({
      type: "init",
      wasmModule: handle.wasmModule,
      memory: handle.memory,
      forcePrepare,
      vmPtr,
      guestBase: handle.guestBase,
      kernelStackSlot: kernelStackSlotForWorker(worker),
      generation,
      runtimeEpoch,
      guestWriteWatchRegions: handle.guestWriteWatchRegions,
      kernelState: handle.kernelState,
      childExitRecords: [...handle.childExitSyncRecords].map(([childPid, waitStatus]) => ({
        childPid,
        waitStatus,
      })),
      sabPageCache: handle.sabPageCache,
      runtimeNotify: handle.runtimeBridge.notifySab,
      runtimeState: handle.runtimeBridge.stateSab,
    } satisfies ThreadWorkerInbound);
  }

  function enqueueWorker(handle: ProcessHandle, worker: Worker): void {
    if (handle.exitInProgress) {
      return;
    }
    if (!handle.readyWorkers.includes(worker)) {
      const vmPtr = deps.threadWorkerStates.get(worker)?.vmPtr;
      if (vmPtr !== undefined) {
        const brokenThreadState = hasBrokenThreadState(handle, vmPtr);
        if (SCHEDULER_QUEUE_DEBUG_LOG_ENABLED) {
          deps.appendWorkerDebugLog(handle, `ready:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`);
        }
        if (brokenThreadState) {
          deps.appendWorkerDebugLog(
            handle,
            `ready_break:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`,
          );
        }
      }
      handle.readyWorkers.push(worker);
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
      if (!thread?.initialized || thread.waitingInWorkerStatus !== null) {
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
    if (handle.kernelShare.forkSharedSyncPromise !== null) {
      return;
    }
    const runningWorker = sanitizeRunningWorker(handle);
    if (runningWorker !== null) {
      return;
    }
    recoverRunnableIdleWorkers(handle);
    let next: Worker | undefined;
    if (hasExclusiveVforkWorker(handle)) {
      const index = handle.readyWorkers.indexOf(handle.vforkWorker!);
      if (index === -1) {
        return;
      }
      next = handle.readyWorkers.splice(index, 1)[0];
    } else {
      next = handle.readyWorkers.shift();
    }
    if (!next) {
      return;
    }
    handle.runningWorker = next;
    const vmPtr = deps.threadWorkerStates.get(next)?.vmPtr;
    const thread = deps.threadWorkerStates.get(next);
    if (thread && vmPtr !== undefined) {
      thread.stepCount += 1;
      thread.totalStepBudget += SCHEDULER_STEP_BUDGET;
      thread.lastStepAtMs = Date.now();
      thread.lastStepProcessPtr = handle.kernel.thread_process_ptr(vmPtr);
      thread.lastStepPc = handle.kernel.thread_pc(vmPtr);
      thread.lastStepSp = handle.kernel.thread_sp(vmPtr);
      thread.lastStepS11 = handle.kernel.thread_s11?.(vmPtr) ?? 0n;
      incrementBoundedCounter(thread.hotStepPcs, `0x${thread.lastStepPc.toString(16)}`);
      const brokenThreadState = hasBrokenThreadState(handle, vmPtr);
      if (SCHEDULER_QUEUE_DEBUG_LOG_ENABLED) {
        deps.appendWorkerDebugLog(handle, `step:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`);
      }
      if (brokenThreadState) {
        deps.appendWorkerDebugLog(
          handle,
          `step_break:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`,
        );
      }
    }
    publishRuntimeBridgeEpoch(handle.runtimeBridge);
    next.postMessage({
      type: "step",
      budget: SCHEDULER_STEP_BUDGET,
      kernelState: handle.kernelState,
      childExitRecords: [...handle.childExitSyncRecords].map(([childPid, waitStatus]) => ({
        childPid,
        waitStatus,
      })),
    } satisfies ThreadWorkerInbound);
  }

  return {
    attachThreadWorker,
    enqueueWorker,
    pumpProcess,
  };
}
