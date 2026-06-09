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
  let sawAttachedThread = false;
  for (const worker of workers) {
    const thread = threadWorkerStates.get(worker);
    if (!thread) {
      continue;
    }
    sawAttachedThread = true;
    if (handle.kernel.thread_process_ptr?.(thread.vmPtr) === 0) {
      continue;
    }
    return true;
  }
  if (!sawAttachedThread) {
    return true;
  }
  return false;
}

function childProcessCountsAsActive(
  handle: ProcessHandle,
  threadWorkerStates?: WeakMap<Worker, ThreadWorkerState>,
): boolean {
  if (handle.exitInProgress) {
    return handle.processMemfsDirty || handle.processMemfsPublishPending;
  }
  if (threadWorkerStates === undefined) {
    return true;
  }
  return hasRunnableWorker(handle, threadWorkerStates);
}

function childProcessCanBePumped(
  handle: ProcessHandle,
  threadWorkerStates?: WeakMap<Worker, ThreadWorkerState>,
): boolean {
  if (handle.exitInProgress) {
    return false;
  }
  if (threadWorkerStates === undefined) {
    return true;
  }
  return hasRunnableWorker(handle, threadWorkerStates);
}

export function hasActiveChildProcess(
  processHandles: ReadonlyMap<number, ProcessHandle>,
  parentPid: number,
  threadWorkerStates?: WeakMap<Worker, ThreadWorkerState>,
): boolean {
  for (const candidate of processHandles.values()) {
    if (candidate.parentPid !== parentPid) {
      continue;
    }
    if (childProcessCountsAsActive(candidate, threadWorkerStates)) {
      return true;
    }
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
    if (!childProcessCanBePumped(candidate, threadWorkerStates)) {
      continue;
    }
    kickBlockedWorkers?.(candidate);
    pumpProcess(candidate);
    pumped = true;
  }
  return pumped;
}
