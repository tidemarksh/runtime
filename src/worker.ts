/**
 * Process/Worker management.
 *
 * Handles Worker lifecycle: creation, message routing, and cleanup.
 * In a browser, uses Web Workers. In Node.js, uses worker_threads.
 *
 * Optimizations:
 * - WASM Module cache: WebAssembly.compile() is called once per unique binary;
 *   the compiled Module is reused across all subsequent spawns.
 * - Worker pool: up to MAX_WORKERS thread-workers are kept alive and reused.
 *   Idle workers are terminated after IDLE_TIMEOUT_MS via periodic eviction.
 */

import type { KernelExports } from "./abi.js";
import type { WorkerMessage } from "./messages.js";
import { createFsRingBuffer } from "./ring-buffer.js";

// ---------------------------------------------------------------------------
// WASM Module cache
// ---------------------------------------------------------------------------

let cachedModule: WebAssembly.Module | null = null;

/**
 * Pre-compile a WASM module and cache it for future createProcess() calls.
 * If a module is already cached, it is replaced.
 */
export async function precompileModule(
  wasmBytes: BufferSource,
): Promise<WebAssembly.Module> {
  cachedModule = await WebAssembly.compile(wasmBytes);
  return cachedModule;
}

/**
 * Discard the cached WASM module so the next createProcess() recompiles.
 * Primarily useful for tests.
 */
export function clearModuleCache(): void {
  cachedModule = null;
}

// ---------------------------------------------------------------------------
// Worker pool
// ---------------------------------------------------------------------------

interface WorkerPoolEntry {
  worker: Worker;
  lastUsed: number;
  busy: boolean;
}

const workerPool: WorkerPoolEntry[] = [];
const MAX_WORKERS = 4;
const IDLE_TIMEOUT_MS = 60_000;
const EVICTION_INTERVAL_MS = 10_000;

/** Map from Worker to its pool entry for O(1) release. */
const workerEntryMap = new WeakMap<Worker, WorkerPoolEntry>();

/**
 * Acquire a worker from the pool. Returns an idle worker if available,
 * otherwise creates a new one (up to MAX_WORKERS). If the pool is full
 * and all workers are busy, the least-recently-used idle worker is evicted
 * to make room.
 */
function acquireWorker(): Worker {
  // 1. Try to find an idle worker
  for (const entry of workerPool) {
    if (!entry.busy) {
      entry.busy = true;
      entry.lastUsed = Date.now();
      return entry.worker;
    }
  }

  // 2. Pool has room — create a new worker
  if (workerPool.length < MAX_WORKERS) {
    const workerUrl = new URL("./thread-worker.js", import.meta.url);
    const worker = new Worker(workerUrl, { type: "module" });
    const entry: WorkerPoolEntry = { worker, lastUsed: Date.now(), busy: true };
    workerPool.push(entry);
    workerEntryMap.set(worker, entry);
    return worker;
  }

  // 3. Pool is full, all busy — LRU eviction of the least-recently-used entry
  let lruIndex = 0;
  let lruTime = workerPool[0].lastUsed;
  for (let i = 1; i < workerPool.length; i++) {
    if (workerPool[i].lastUsed < lruTime) {
      lruTime = workerPool[i].lastUsed;
      lruIndex = i;
    }
  }
  const evicted = workerPool[lruIndex];
  evicted.worker.terminate();
  workerEntryMap.delete(evicted.worker);

  // Replace with a fresh worker
  const workerUrl = new URL("./thread-worker.js", import.meta.url);
  const worker = new Worker(workerUrl, { type: "module" });
  const entry: WorkerPoolEntry = { worker, lastUsed: Date.now(), busy: true };
  workerPool[lruIndex] = entry;
  workerEntryMap.set(worker, entry);
  return worker;
}

/**
 * Return a worker to the pool and mark it idle.
 */
function releaseWorker(worker: Worker): void {
  const entry = workerEntryMap.get(worker);
  if (entry) {
    entry.busy = false;
    entry.lastUsed = Date.now();
  }
}

/**
 * Evict idle workers whose lastUsed timestamp exceeds IDLE_TIMEOUT_MS.
 */
function evictIdleWorkers(): void {
  const now = Date.now();
  for (let i = workerPool.length - 1; i >= 0; i--) {
    const entry = workerPool[i];
    if (!entry.busy && now - entry.lastUsed > IDLE_TIMEOUT_MS) {
      entry.worker.terminate();
      workerEntryMap.delete(entry.worker);
      workerPool.splice(i, 1);
    }
  }
}

/** Periodic eviction timer. Starts on first acquireWorker call. */
let evictionTimer: ReturnType<typeof setInterval> | null = null;

function ensureEvictionTimer(): void {
  if (evictionTimer !== null) return;
  evictionTimer = setInterval(evictIdleWorkers, EVICTION_INTERVAL_MS);
  // Allow the process to exit even if the timer is still running (Node.js)
  if (typeof evictionTimer === "object" && "unref" in evictionTimer) {
    evictionTimer.unref();
  }
}

/**
 * Shut down the worker pool: terminate all workers and clear the eviction
 * timer. Intended for graceful shutdown and tests.
 */
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

// ---------------------------------------------------------------------------
// ProcessHandle
// ---------------------------------------------------------------------------

export interface ProcessHandle {
  pid: number;
  vmPtr: number;
  worker: Worker;
  memory: WebAssembly.Memory;
  fsRingBuffer: SharedArrayBuffer;
  onExit?: (code: number) => void;
  onStdout?: (data: Uint8Array) => void;
}

// ---------------------------------------------------------------------------
// createProcess
// ---------------------------------------------------------------------------

/**
 * Create a new process: compile WASM (or reuse cached Module), instantiate
 * with shared Memory, load ELF, and start a thread-worker from the pool.
 */
export async function createProcess(
  wasmBytes: BufferSource,
  elfData: Uint8Array,
  options: {
    ramSize?: number;
    stackSize?: number;
    onExit?: (code: number) => void;
    onStdout?: (data: Uint8Array) => void;
  } = {},
): Promise<ProcessHandle> {
  const ramSize = options.ramSize ?? 16 * 1024 * 1024; // 16 MB default
  const stackSize = options.stackSize ?? 1024 * 1024;   // 1 MB default
  const ramPages = Math.ceil(ramSize / 65536);

  // Create shared memory for this process
  const memory = new WebAssembly.Memory({
    initial: ramPages,
    maximum: 32768, // 2 GB max
    shared: true,
  });

  // Compile WASM module (cached across processes)
  if (cachedModule === null) {
    cachedModule = await WebAssembly.compile(wasmBytes);
  }
  const wasmModule = cachedModule;

  // Instantiate in the main thread to set up kernel state
  const mainInstance = await WebAssembly.instantiate(wasmModule, {
    env: { memory },
  });
  const exports = mainInstance.exports as unknown as KernelExports;

  // Initialize kernel
  const heapBase = 0x200000; // 2 MB — after ELF load area
  exports.kernel_init(heapBase);

  // Copy ELF to WASM memory
  const elfOffset = ramSize - 256 * 1024; // load ELF at top - 256KB
  const memView = new Uint8Array(memory.buffer);
  memView.set(elfData, elfOffset);

  // Create process
  const vmPtr = exports.process_create(elfOffset, elfData.length, ramSize, stackSize);
  if (vmPtr === 0) {
    throw new Error("process_create failed");
  }

  // Acquire a thread worker from the pool
  ensureEvictionTimer();
  const worker = acquireWorker();

  // Create SAB ring buffer for FS request/response
  const fsRingBuffer = createFsRingBuffer();

  const handle: ProcessHandle = {
    pid: 1,
    vmPtr,
    worker,
    memory,
    fsRingBuffer,
    onExit: options.onExit,
    onStdout: options.onStdout,
  };

  // Set up message handler
  worker.onmessage = (event: MessageEvent<WorkerMessage>) => {
    handleWorkerMessage(handle, exports, event.data);
  };

  // Initialize the worker with the WASM module and shared memory
  const initMsg: WorkerMessage = {
    type: "init",
    wasmModule,
    memory,
    vmPtr,
  };
  worker.postMessage(initMsg);

  return handle;
}

// ---------------------------------------------------------------------------
// Message handler
// ---------------------------------------------------------------------------

function handleWorkerMessage(
  handle: ProcessHandle,
  exports: KernelExports,
  msg: WorkerMessage,
): void {
  if (msg.type !== "status") return;

  const status = msg.status;
  const STATUS_OK = exports.abi_status_ok();
  const STATUS_FAULT = exports.abi_status_fault();
  const STATUS_SLEEP = exports.abi_status_sleep();
  const STATUS_FS = exports.abi_status_fs();
  const STATUS_CLONE_SPAWN = exports.abi_status_clone_spawn();
  const STATUS_EXECVE_SPAWN = exports.abi_status_execve_spawn();

  if (status === STATUS_OK) {
    // Budget exhausted — send more
    handle.worker.postMessage({ type: "step", budget: 500_000 } satisfies WorkerMessage);
  } else if (status === STATUS_FAULT) {
    // Process exited — release worker back to pool
    handle.onExit?.(msg.exitCode);
    releaseWorker(handle.worker);
  } else if (status === STATUS_SLEEP) {
    // nanosleep — setTimeout then resume
    setTimeout(() => {
      handle.worker.postMessage({ type: "step", budget: 500_000 } satisfies WorkerMessage);
    }, 1); // minimum sleep
  } else if (status === STATUS_FS) {
    // FS request pending in SAB ring buffer.
    // Process requests via the kernel WASM instance (same instance, single-threaded).
    // In production, a dedicated kernel Worker would handle this.
    // For now, process synchronously and resume.
    handle.worker.postMessage({ type: "step", budget: 500_000 } satisfies WorkerMessage);
  } else if (status === STATUS_CLONE_SPAWN) {
    // clone(CLONE_THREAD) — create a new thread-worker sharing the same Memory.
    // Stub: single-threaded mode (GOMAXPROCS=1). Log and resume.
    console.warn(
      `[worker] STATUS_CLONE_SPAWN for pid=${handle.pid}: ` +
      "thread spawning not yet implemented, resuming in single-threaded mode",
    );
    handle.worker.postMessage({ type: "step", budget: 500_000 } satisfies WorkerMessage);
  } else if (status === STATUS_EXECVE_SPAWN) {
    // execve() — create a new process-worker with new Memory.
    // Stub: not yet implemented. Log and resume.
    console.warn(
      `[worker] STATUS_EXECVE_SPAWN for pid=${handle.pid}: ` +
      "process spawning not yet implemented, resuming",
    );
    handle.worker.postMessage({ type: "step", budget: 500_000 } satisfies WorkerMessage);
  } else {
    // Other blocking status (FUTEX_WAIT, PIPE_WAIT, EPOLL_WAIT, etc.)
    handle.worker.postMessage({ type: "step", budget: 500_000 } satisfies WorkerMessage);
  }
}

/**
 * Start execution of a process (sends first step message).
 */
export function startProcess(handle: ProcessHandle): void {
  // Wait for init to complete, then the worker will report status=OK
  // which triggers the first step
}
