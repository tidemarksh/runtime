interface WorkerPoolEntry {
  worker: Worker;
  lastUsed: number;
  busy: boolean;
}

const workerPool: WorkerPoolEntry[] = [];
const workerEntryMap = new WeakMap<Worker, WorkerPoolEntry>();
const MAX_WORKERS = 4;
const IDLE_TIMEOUT_MS = 60_000;
const EVICTION_INTERVAL_MS = 10_000;

let evictionTimer: ReturnType<typeof setInterval> | null = null;

function maybeUnrefWorker(worker: Worker): void {
  const nodeWorker = worker as Worker & { unref?: () => void };
  nodeWorker.unref?.();
}

export function ensureEvictionTimer(): void {
  if (evictionTimer !== null) {
    return;
  }
  evictionTimer = setInterval(evictIdleWorkers, EVICTION_INTERVAL_MS);
  if (typeof evictionTimer === "object" && "unref" in evictionTimer) {
    evictionTimer.unref();
  }
}

function evictIdleWorkers(): void {
  const now = Date.now();
  for (let i = workerPool.length - 1; i >= 0; i -= 1) {
    const entry = workerPool[i];
    if (entry.busy || now - entry.lastUsed <= IDLE_TIMEOUT_MS) {
      continue;
    }
    entry.worker.terminate();
    workerEntryMap.delete(entry.worker);
    workerPool.splice(i, 1);
  }
}

export function acquireWorker(): Worker {
  for (const entry of workerPool) {
    if (entry.busy) {
      continue;
    }
    entry.busy = true;
    entry.lastUsed = Date.now();
    return entry.worker;
  }

  const workerUrl = new URL("../thread-worker.mjs", import.meta.url);
  if (workerPool.length < MAX_WORKERS) {
    const worker = new Worker(workerUrl, { type: "module" });
    maybeUnrefWorker(worker);
    const entry: WorkerPoolEntry = {
      worker,
      lastUsed: Date.now(),
      busy: true,
    };
    workerPool.push(entry);
    workerEntryMap.set(worker, entry);
    return worker;
  }

  let lruIndex = -1;
  let lruTime = Number.POSITIVE_INFINITY;
  for (let i = 0; i < workerPool.length; i += 1) {
    const entry = workerPool[i];
    if (!entry.busy && entry.lastUsed < lruTime) {
      lruIndex = i;
      lruTime = entry.lastUsed;
    }
  }

  if (lruIndex === -1) {
    const worker = new Worker(workerUrl, { type: "module" });
    maybeUnrefWorker(worker);
    return worker;
  }

  const evicted = workerPool[lruIndex];
  evicted.worker.terminate();
  workerEntryMap.delete(evicted.worker);

  const worker = new Worker(workerUrl, { type: "module" });
  maybeUnrefWorker(worker);
  const entry: WorkerPoolEntry = {
    worker,
    lastUsed: Date.now(),
    busy: true,
  };
  workerPool[lruIndex] = entry;
  workerEntryMap.set(worker, entry);
  return worker;
}

export function releaseWorker(worker: Worker): void {
  const entry = workerEntryMap.get(worker);
  if (!entry) {
    worker.terminate();
    return;
  }
  entry.busy = false;
  entry.lastUsed = Date.now();
}

export function removeWorkerFromPool(worker: Worker): void {
  const entry = workerEntryMap.get(worker);
  if (!entry) {
    return;
  }
  const index = workerPool.indexOf(entry);
  if (index >= 0) {
    workerPool.splice(index, 1);
  }
  workerEntryMap.delete(worker);
}

export function destroyWorkerPool(): void {
  if (evictionTimer !== null) {
    clearInterval(evictionTimer);
    evictionTimer = null;
  }
  for (const entry of workerPool) {
    entry.worker.terminate();
    workerEntryMap.delete(entry.worker);
  }
  workerPool.length = 0;
}

export function maybeUnrefTimer(
  timer: ReturnType<typeof setTimeout> | ReturnType<typeof setInterval>,
): void {
  if (typeof timer === "object" && timer !== null && "unref" in timer) {
    timer.unref();
  }
}
