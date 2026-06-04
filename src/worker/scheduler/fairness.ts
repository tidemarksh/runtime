import {
  wakeRuntimeBridge,
} from "../../bridge/runtime-state.js";
import type { ProcessHandle } from "../types.js";

const WORKER_DEBUG_LOG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_WORKER_DEBUG_LOG ?? "") === "1";
const STALE_SLEEP_RUNNING_WORKER_REPAIR_MS = 2_000;

export interface ReadyWorkerFairness {
  scheduleRunningWorkerRepairRetry(handle: ProcessHandle): void;
  scheduleRunningWorkerYieldRetry(handle: ProcessHandle): void;
  wakeRunningWorkerForReadyWork(handle: ProcessHandle): void;
}

export function appendWorkerDebugLog(handle: ProcessHandle, event: string): void {
  if (!WORKER_DEBUG_LOG_ENABLED) {
    return;
  }
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
}

export function createReadyWorkerFairness(pumpProcess: (handle: ProcessHandle) => void): ReadyWorkerFairness {
  const pendingRunningWorkerPumps = new WeakMap<ProcessHandle, Set<string>>();

  function scheduleRunningWorkerPump(handle: ProcessHandle, delayMs: number, wakeBeforePump = false): void {
    let pendingDelays = pendingRunningWorkerPumps.get(handle);
    if (!pendingDelays) {
      pendingDelays = new Set();
      pendingRunningWorkerPumps.set(handle, pendingDelays);
    }
    const key = `${delayMs}:${wakeBeforePump ? 1 : 0}`;
    if (pendingDelays.has(key)) {
      return;
    }
    pendingDelays.add(key);
    setTimeout(() => {
      const currentPendingDelays = pendingRunningWorkerPumps.get(handle);
      currentPendingDelays?.delete(key);
      if (currentPendingDelays?.size === 0) {
        pendingRunningWorkerPumps.delete(handle);
      }
      if (wakeBeforePump) {
        wakeRuntimeBridge(handle.runtimeBridge);
      }
      pumpProcess(handle);
    }, delayMs);
  }

  function wakeRunningWorkerForReadyWork(handle: ProcessHandle): void {
    wakeRuntimeBridge(handle.runtimeBridge);
    scheduleRunningWorkerPump(handle, 0);
    scheduleRunningWorkerPump(handle, STALE_SLEEP_RUNNING_WORKER_REPAIR_MS);
  }

  return {
    scheduleRunningWorkerRepairRetry: (handle) => {
      scheduleRunningWorkerPump(handle, STALE_SLEEP_RUNNING_WORKER_REPAIR_MS);
    },
    scheduleRunningWorkerYieldRetry: (handle) => {
      scheduleRunningWorkerPump(handle, STALE_SLEEP_RUNNING_WORKER_REPAIR_MS, true);
    },
    wakeRunningWorkerForReadyWork,
  };
}
