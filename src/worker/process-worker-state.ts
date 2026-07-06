import type { ProcessHandle } from "./types.js";

const processWorkerSets = new WeakMap<ProcessHandle, Set<Worker>>();
const PROCESS_WORKER_SET = Symbol.for("tidemark.worker.processWorkers");

type ProcessWorkerSetCarrier = ProcessHandle & {
  [PROCESS_WORKER_SET]?: Set<Worker>;
};

function storeProcessWorkers(handle: ProcessHandle, workers: Set<Worker>): Set<Worker> {
  processWorkerSets.set(handle, workers);
  Object.defineProperty(handle, PROCESS_WORKER_SET, {
    configurable: true,
    enumerable: false,
    value: workers,
    writable: true,
  });
  return workers;
}

export function processWorkers(handle: ProcessHandle): Set<Worker> {
  const workers = processWorkerSets.get(handle);
  if (workers !== undefined) {
    return workers;
  }
  const carrier = handle as ProcessWorkerSetCarrier;
  if (Object.prototype.hasOwnProperty.call(carrier, PROCESS_WORKER_SET)) {
    const sharedWorkers = carrier[PROCESS_WORKER_SET];
    if (sharedWorkers !== undefined) {
      processWorkerSets.set(handle, sharedWorkers);
      return sharedWorkers;
    }
  }
  const created = new Set<Worker>();
  return storeProcessWorkers(handle, created);
}

export function setProcessWorkers(handle: ProcessHandle, workers: Iterable<Worker>): void {
  storeProcessWorkers(handle, new Set(workers));
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
