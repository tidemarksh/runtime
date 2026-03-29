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

import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  readProcessSpawnHandoff,
  type KernelExports,
  type KernelRuntimeState,
} from "./abi.js";
import { RING_BUFFER_SIZE } from "./ring-buffer.js";
import type {
  ThreadWorkerInbound,
  ThreadWorkerOutbound,
} from "./messages.js";

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

  // 3. Pool is full — try to evict an IDLE worker (never evict busy workers).
  // §6.10: LRU eviction must not kill busy workers.
  let lruIndex = -1;
  let lruTime = Infinity;
  for (let i = 0; i < workerPool.length; i++) {
    if (!workerPool[i].busy && workerPool[i].lastUsed < lruTime) {
      lruTime = workerPool[i].lastUsed;
      lruIndex = i;
    }
  }

  if (lruIndex === -1) {
    // All workers are busy — create beyond pool limit (will not be pooled).
    const workerUrl = new URL("./thread-worker.js", import.meta.url);
    const worker = new Worker(workerUrl, { type: "module" });
    return worker;
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
 * Workers created beyond the pool limit (overflow) are terminated immediately.
 */
function releaseWorker(worker: Worker): void {
  const entry = workerEntryMap.get(worker);
  if (entry) {
    entry.busy = false;
    entry.lastUsed = Date.now();
  } else {
    // Overflow worker (not in pool) — terminate to prevent leak.
    worker.terminate();
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
  wasmModule: WebAssembly.Module;
  kernel: KernelExports;
  kernelState: KernelRuntimeState;
  fsRingBuffer: SharedArrayBuffer;
  /** §6.3: SAB page cache for direct reads (shared between all Workers). */
  sabPageCache: SharedArrayBuffer;
  workers: Set<Worker>;
  readyWorkers: Worker[];
  runningWorker: Worker | null;
  onExit?: (code: number) => void;
  onStdout?: (data: Uint8Array) => void;
}

interface ThreadWorkerState {
  vmPtr: number;
  initialized: boolean;
}

/** Global process registry for child exit notification. */
const processHandles = new Map<number, ProcessHandle>();

const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();

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
    argv?: string[];
    env?: string[];
    kernelState?: KernelRuntimeState;
    sabPageCache?: SharedArrayBuffer;
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

  if (options.kernelState) {
    // §6.1: Attach kernel state from kernel Worker instead of initializing locally.
    // This shares MemFS, process table, and signal state from the kernel Worker.
    const wasmHeapBase = (exports as any).__heap_base?.value ?? (ramSize - 4 * 1024 * 1024);
    exports.kernel_init(wasmHeapBase);
    attachKernelRuntimeState(exports, options.kernelState);
  } else {
    // Standalone mode (tests, no kernel Worker).
    const heapBase = ramSize - 4 * 1024 * 1024;
    exports.kernel_init(heapBase);
  }

  // Set wall clock base time
  const now = Date.now();
  exports.set_base_time(BigInt(Math.floor(now / 1000)), BigInt((now % 1000) * 1_000_000));

  // Copy ELF to WASM memory
  const elfOffset = ramSize - 256 * 1024; // load ELF at top - 256KB
  const memView = new Uint8Array(memory.buffer);
  memView.set(elfData, elfOffset);

  // Push argv and env to kernel before process_create
  const encoder = new TextEncoder();
  exports.process_clear_args();
  let strOffset = elfOffset - 4096;
  for (const arg of options.argv ?? []) {
    const bytes = encoder.encode(arg + "\0");
    memView.set(bytes, strOffset);
    exports.process_push_arg(strOffset);
    strOffset += bytes.length;
  }
  for (const e of options.env ?? []) {
    const bytes = encoder.encode(e + "\0");
    memView.set(bytes, strOffset);
    exports.process_push_env(strOffset);
    strOffset += bytes.length;
  }

  // Create process
  const vmPtr = exports.process_create(elfOffset, elfData.length, ramSize, stackSize);
  if (vmPtr === 0) {
    throw new Error("process_create failed");
  }

  // Acquire a thread worker from the pool
  ensureEvictionTimer();
  const worker = acquireWorker();

  const fsRingBuffer = new SharedArrayBuffer(RING_BUFFER_SIZE);
  const sabPageCache = options.sabPageCache ?? new SharedArrayBuffer(16 * 1024 * 1024);

  const handle: ProcessHandle = {
    pid: 1,
    vmPtr,
    worker,
    memory,
    wasmModule,
    kernel: exports,
    kernelState: readKernelRuntimeState(exports),
    fsRingBuffer,
    sabPageCache,
    workers: new Set(),
    readyWorkers: [],
    runningWorker: null,
    onExit: options.onExit,
    onStdout: options.onStdout,
  };

  attachThreadWorker(handle, worker, vmPtr);
  processHandles.set(handle.pid, handle);

  return handle;
}

// ---------------------------------------------------------------------------
// Message handler
// ---------------------------------------------------------------------------

function attachThreadWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
): void {
  handle.workers.add(worker);
  threadWorkerStates.set(worker, {
    vmPtr,
    initialized: false,
  });
  worker.onmessage = (event: MessageEvent<ThreadWorkerOutbound>) => {
    handleWorkerMessage(handle, worker, event.data);
  };

  const initMsg: ThreadWorkerInbound = {
    type: "init",
    wasmModule: handle.wasmModule,
    memory: handle.memory,
    vmPtr,
    kernelState: handle.kernelState,
    fsRingBuffer: handle.fsRingBuffer,
    sabPageCache: handle.sabPageCache,
  };
  worker.postMessage(initMsg);
}

function enqueueWorker(handle: ProcessHandle, worker: Worker): void {
  if (!handle.readyWorkers.includes(worker)) {
    handle.readyWorkers.push(worker);
  }
  pumpProcess(handle);
}

function pumpProcess(handle: ProcessHandle): void {
  if (handle.runningWorker !== null) return;
  const next = handle.readyWorkers.shift();
  if (!next) return;
  handle.runningWorker = next;
  next.postMessage({
    type: "step",
    budget: 500_000,
    kernelState: handle.kernelState,
  } satisfies ThreadWorkerInbound);
}

function handleWorkerMessage(
  handle: ProcessHandle,
  worker: Worker,
  msg: ThreadWorkerOutbound,
): void {
  if (msg.type !== "status") return;

  const thread = threadWorkerStates.get(worker);
  if (!thread) return;

  handle.kernelState = msg.kernelState;
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  if (handle.runningWorker === worker) {
    handle.runningWorker = null;
  }

  const status = msg.status;
  const STATUS_OK = handle.kernel.abi_status_ok();
  const STATUS_FAULT = handle.kernel.abi_status_fault();
  const STATUS_SLEEP = handle.kernel.abi_status_sleep();
  const STATUS_FS = handle.kernel.abi_status_fs();
  const STATUS_CLONE_SPAWN = handle.kernel.abi_status_clone_spawn();
  const STATUS_EXECVE_SPAWN = handle.kernel.abi_status_execve_spawn();

  if (status === STATUS_OK) {
    thread.initialized = true;
    enqueueWorker(handle, worker);
  } else if (status === STATUS_FAULT) {
    const remainingThreads = handle.kernel.thread_process_thread_count(thread.vmPtr);
    handle.workers.delete(worker);
    threadWorkerStates.delete(worker);
    releaseWorker(worker);
    if (remainingThreads === 0) {
      handle.onExit?.(msg.exitCode);
    } else {
      pumpProcess(handle);
    }
  } else if (status === STATUS_SLEEP) {
    // nanosleep — read actual duration from kernel, then setTimeout
    const sec = Number(handle.kernel.thread_sleep_seconds(thread.vmPtr));
    const nsec = Number(handle.kernel.thread_sleep_nanoseconds(thread.vmPtr));
    const ms = Math.max(1, sec * 1000 + Math.floor(nsec / 1_000_000));
    setTimeout(() => {
      worker.postMessage({ type: "wake-sleep" } satisfies ThreadWorkerInbound);
    }, ms);
  } else if (status === STATUS_FS) {
    handle.kernel.thread_process_fs(thread.vmPtr);
    handle.kernel.thread_resume_fs(thread.vmPtr);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    enqueueWorker(handle, worker);
  } else if (status === STATUS_CLONE_SPAWN) {
    // clone(CLONE_THREAD) — create a new thread-worker sharing the same Memory.
    const childVmPtr = handle.kernel.thread_spawn_from_clone(thread.vmPtr);
    if (childVmPtr === 0) {
      throw new Error(
        `[worker] STATUS_CLONE_SPAWN for pid=${handle.pid}: thread_spawn_from_clone failed`,
      );
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    const childWorker = acquireWorker();
    attachThreadWorker(handle, childWorker, childVmPtr);
    enqueueWorker(handle, worker);
  } else if (status === STATUS_EXECVE_SPAWN) {
    handleExecveSpawn(handle, worker, thread);
  } else if (status === handle.kernel.abi_status_vfork_wait()) {
    // §6.6: vfork parent suspended. Child continues in same thread-worker.
    // The kernel already set child's a0=0 and continues execution.
    // Mark this thread as vfork-active and keep stepping.
    enqueueWorker(handle, worker);
  } else if (status === handle.kernel.abi_status_wait4()) {
    handleWait4(handle, worker, thread);
  } else {
    // Other blocking status (FUTEX_WAIT, PIPE_WAIT, EPOLL_WAIT, etc.)
    // thread-worker handles these locally via Atomics.wait.
    // If we get here, it means the thread-worker posted a status
    // that it couldn't handle locally — just re-enqueue.
    enqueueWorker(handle, worker);
  }
}

/**
 * Start execution of a process (sends first step message).
 */
export function startProcess(handle: ProcessHandle): void {
  // Wait for init to complete, then the worker will report status=OK
  // which triggers the first step
}

// ---------------------------------------------------------------------------
// EXECVE_SPAWN handler (§6.4, §6.6)
// ---------------------------------------------------------------------------

function handleExecveSpawn(
  handle: ProcessHandle,
  worker: Worker,
  thread: ThreadWorkerState,
): void {
  const handoff = readProcessSpawnHandoff(handle.kernel, thread.vmPtr);
  const SPAWN_KIND_EXECVE = handle.kernel.abi_spawn_kind_execve();
  const SPAWN_KIND_FORK = handle.kernel.abi_spawn_kind_fork();

  if (handoff.kind === SPAWN_KIND_EXECVE) {
    // vfork+execve: child needs new Memory + new process.
    // §6.6: The vfork parent is already suspended (STATUS_VFORK_WAIT
    // was handled earlier by just continuing to step the child).
    // Now the child called execve — create the child process.
    handleVforkExecve(handle, worker, thread, handoff.childPid);
  } else if (handoff.kind === SPAWN_KIND_FORK) {
    // plain fork: copy Memory, create child process.
    handlePlainFork(handle, worker, thread, handoff.childPid);
  } else {
    // Unknown spawn kind — resume parent.
    handle.kernel.thread_clear_spawn_handoff(thread.vmPtr);
    enqueueWorker(handle, worker);
  }
}

/**
 * §6.6 vfork+execve: Create child process with new Memory.
 * The kernel has already prepared the spawn handoff with path/argv/envp
 * and the parent's ThreadState snapshot is saved in vfork state.
 */
async function handleVforkExecve(
  handle: ProcessHandle,
  parentWorker: Worker,
  parentThread: ThreadWorkerState,
  childPid: number,
): Promise<void> {
  // Read the execve path from guest memory to get the ELF
  const pathAddr = Number(handle.kernel.thread_spawn_path(parentThread.vmPtr));
  const mem = new Uint8Array(handle.memory.buffer);

  // Read null-terminated path from guest memory
  let pathEnd = pathAddr;
  while (mem[pathEnd] !== 0 && pathEnd < mem.length) pathEnd++;
  const pathBytes = mem.slice(pathAddr, pathEnd);

  // Read argv strings from guest memory
  const argvAddr = Number(handle.kernel.thread_spawn_argv(parentThread.vmPtr));
  const argv = readGuestStringArray(mem, argvAddr);

  // Read envp strings from guest memory
  const envpAddr = Number(handle.kernel.thread_spawn_envp(parentThread.vmPtr));
  const envp = readGuestStringArray(mem, envpAddr);

  // Resolve ELF path in kernel's MemFS
  // For now, read the ELF from the parent's guest memory MemFS
  // TODO: use kernel Worker's memfs_resolve_path once fully wired
  const elfIno = handle.kernel.memfs_resolve_path(pathAddr, pathBytes.length);
  let elfData: Uint8Array;
  if (elfIno !== 0) {
    const elfSize = Number(handle.kernel.memfs_file_size(elfIno));
    const scratchOffset = 0x100000; // 1MB scratch in kernel WASM memory
    const bytesRead = Number(handle.kernel.memfs_read_file_to(elfIno, 0n, scratchOffset, elfSize));
    elfData = new Uint8Array(
      (handle.kernel.memory as WebAssembly.Memory).buffer.slice(scratchOffset, scratchOffset + bytesRead),
    );
  } else {
    // ELF not found in MemFS — resume parent with error
    handle.kernel.thread_clear_spawn_handoff(parentThread.vmPtr);
    handle.kernel.thread_resume_vfork_parent(parentThread.vmPtr, -1);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    enqueueWorker(handle, parentWorker);
    return;
  }

  // Clear spawn handoff before creating child
  handle.kernel.thread_clear_spawn_handoff(parentThread.vmPtr);

  // Create child process with new Memory, sharing parent's SAB page cache
  const childRamSize = handle.memory.buffer.byteLength;
  const childHandle = await createProcess(
    new Uint8Array(0), // wasmBytes not needed — cached module reused
    elfData,
    {
      ramSize: childRamSize,
      argv,
      env: envp,
      sabPageCache: handle.sabPageCache,
      onExit: (code) => {
        // Child exit — notify parent's kernel for wait4
        processHandles.delete(childPid);
      },
    },
  );
  childHandle.pid = childPid;
  processHandles.set(childPid, childHandle);

  // Resume vfork parent with child PID
  handle.kernel.thread_resume_vfork_parent(parentThread.vmPtr, childPid);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  enqueueWorker(handle, parentWorker);
}

/**
 * Plain fork: copy entire guest Memory and create child process.
 */
async function handlePlainFork(
  handle: ProcessHandle,
  parentWorker: Worker,
  parentThread: ThreadWorkerState,
  childPid: number,
): Promise<void> {
  // Copy parent's guest memory
  const parentMem = new Uint8Array(handle.memory.buffer);
  const childRamSize = handle.memory.buffer.byteLength;
  const childRamPages = Math.ceil(childRamSize / 65536);

  const childMemory = new WebAssembly.Memory({
    initial: childRamPages,
    maximum: 32768,
    shared: true,
  });
  new Uint8Array(childMemory.buffer).set(parentMem);

  // Instantiate child kernel with copied memory
  const childInstance = await WebAssembly.instantiate(handle.wasmModule, {
    env: { memory: childMemory },
  });
  const childKernel = childInstance.exports as unknown as KernelExports;

  // Attach parent's kernel state to child
  attachKernelRuntimeState(childKernel, handle.kernelState);

  // Clear spawn handoff on parent
  handle.kernel.thread_clear_spawn_handoff(parentThread.vmPtr);

  // Set parent's return value to child PID
  // The kernel already set a0 = child_pid for the parent

  // Start child thread-worker
  ensureEvictionTimer();
  const childWorker = acquireWorker();
  const childFsRingBuffer = new SharedArrayBuffer(RING_BUFFER_SIZE);

  const childHandle: ProcessHandle = {
    pid: childPid,
    vmPtr: handle.vmPtr, // child starts with same vmPtr layout in its own memory
    worker: childWorker,
    memory: childMemory,
    wasmModule: handle.wasmModule,
    kernel: childKernel,
    kernelState: readKernelRuntimeState(childKernel),
    fsRingBuffer: childFsRingBuffer,
    sabPageCache: handle.sabPageCache,
    workers: new Set(),
    readyWorkers: [],
    runningWorker: null,
  };
  processHandles.set(childPid, childHandle);
  attachThreadWorker(childHandle, childWorker, childHandle.vmPtr);

  // Resume parent
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  enqueueWorker(handle, parentWorker);
}

// ---------------------------------------------------------------------------
// WAIT4 handler
// ---------------------------------------------------------------------------

function handleWait4(
  handle: ProcessHandle,
  worker: Worker,
  thread: ThreadWorkerState,
): void {
  // Try immediate resume — kernel checks if any child has exited
  handle.kernel.thread_resume_wait4(thread.vmPtr);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  const newStatus = handle.kernel.thread_status(thread.vmPtr);

  if (newStatus !== handle.kernel.abi_status_wait4()) {
    // Child was already exited — wait4 completed
    enqueueWorker(handle, worker);
  } else {
    // No child exited yet — poll periodically
    const pollWait4 = () => {
      handle.kernel.thread_resume_wait4(thread.vmPtr);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      const s = handle.kernel.thread_status(thread.vmPtr);
      if (s !== handle.kernel.abi_status_wait4()) {
        enqueueWorker(handle, worker);
      } else {
        setTimeout(pollWait4, 10);
      }
    };
    setTimeout(pollWait4, 10);
  }
}

// ---------------------------------------------------------------------------
// Guest memory helpers
// ---------------------------------------------------------------------------

/** Read a null-terminated u64 pointer array of C strings from guest memory. */
function readGuestStringArray(mem: Uint8Array, ptrArrayAddr: number): string[] {
  if (ptrArrayAddr === 0) return [];
  const result: string[] = [];
  const dv = new DataView(mem.buffer);
  let offset = ptrArrayAddr;
  for (;;) {
    const strAddr = Number(dv.getBigUint64(offset, true));
    if (strAddr === 0) break;
    let end = strAddr;
    while (mem[end] !== 0 && end < mem.length) end++;
    result.push(new TextDecoder().decode(mem.slice(strAddr, end)));
    offset += 8;
  }
  return result;
}
