import type { ProcessHandle } from "./types.js";
import type { ThreadWorkerState } from "./types.js";

function hasRunnableWorker(
  handle: ProcessHandle,
  threadWorkerStates?: WeakMap<Worker, ThreadWorkerState>,
): boolean {
  if (!threadWorkerStates) {
    return true;
  }
  const workers = new Set(handle.workers);
  workers.add(handle.worker);
  if (handle.runningWorker !== null) {
    workers.add(handle.runningWorker);
  }
  for (const worker of handle.readyWorkers) {
    workers.add(worker);
  }
  for (const worker of handle.blockedWorkers.keys()) {
    workers.add(worker);
  }
  for (const worker of workers) {
    if (threadWorkerStates.has(worker)) {
      return true;
    }
  }
  return false;
}

export function hasActiveChildProcess(
  processHandles: ReadonlyMap<number, ProcessHandle>,
  parentPid: number,
  _threadWorkerStates?: WeakMap<Worker, ThreadWorkerState>,
): boolean {
  for (const candidate of processHandles.values()) {
    if (candidate.parentPid !== parentPid) {
      continue;
    }
    if (candidate.exitInProgress) {
      continue;
    }
    return true;
  }
  return false;
}

export function pumpActiveChildProcesses(
  processHandles: ReadonlyMap<number, ProcessHandle>,
  parentPid: number,
  pumpProcess: (handle: ProcessHandle) => void,
  kickBlockedWorkers?: (handle: ProcessHandle) => void,
  threadWorkerStates?: WeakMap<Worker, ThreadWorkerState>,
): boolean {
  let pumped = false;
  for (const candidate of processHandles.values()) {
    if (candidate.parentPid !== parentPid) {
      continue;
    }
    if (candidate.exitInProgress) {
      continue;
    }
    if (!hasRunnableWorker(candidate, threadWorkerStates)) {
      continue;
    }
    kickBlockedWorkers?.(candidate);
    pumpProcess(candidate);
    pumped = true;
  }
  return pumped;
}
