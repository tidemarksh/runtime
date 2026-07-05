import type { ProcessHandle } from "./types.js";

const processWorkerSets = new WeakMap<ProcessHandle, Set<Worker>>();

type LegacyProcessWorkersSeed = {
  workers?: Set<Worker>;
};

export function processWorkers(handle: ProcessHandle): Set<Worker> {
  const workers = processWorkerSets.get(handle);
  if (workers !== undefined) {
    return workers;
  }
  const legacyWorkers = (handle as LegacyProcessWorkersSeed).workers;
  if (legacyWorkers !== undefined) {
    processWorkerSets.set(handle, legacyWorkers);
    delete (handle as LegacyProcessWorkersSeed).workers;
    return legacyWorkers;
  }
  const created = new Set<Worker>();
  processWorkerSets.set(handle, created);
  return created;
}

export function setProcessWorkers(handle: ProcessHandle, workers: Iterable<Worker>): void {
  delete (handle as LegacyProcessWorkersSeed).workers;
  processWorkerSets.set(handle, new Set(workers));
}

export function addProcessWorker(handle: ProcessHandle, worker: Worker): void {
  processWorkers(handle).add(worker);
}

export function deleteProcessWorker(handle: ProcessHandle, worker: Worker): boolean {
  return processWorkers(handle).delete(worker);
}

export function clearProcessWorkers(handle: ProcessHandle): void {
  processWorkers(handle).clear();
}

export function processHasWorker(handle: ProcessHandle, worker: Worker): boolean {
  return processWorkers(handle).has(worker);
}

export function processWorkerCount(handle: ProcessHandle): number {
  return processWorkers(handle).size;
}

export function processWorkerList(handle: ProcessHandle): Worker[] {
  return [...processWorkers(handle)];
}
