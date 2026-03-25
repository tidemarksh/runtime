/**
 * Process/Worker management.
 *
 * Handles Worker lifecycle: creation, message routing, and cleanup.
 * In a browser, uses Web Workers. In Node.js, uses worker_threads.
 */

import type { KernelExports } from "./abi.js";
import type { WorkerMessage } from "./messages.js";

export interface ProcessHandle {
  pid: number;
  vmPtr: number;
  worker: Worker;
  memory: WebAssembly.Memory;
  onExit?: (code: number) => void;
  onStdout?: (data: Uint8Array) => void;
}

/**
 * Create a new process: compile WASM, instantiate with shared Memory,
 * load ELF, and start a thread-worker.
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

  // Compile WASM module (can be cached across processes)
  const wasmModule = await WebAssembly.compile(wasmBytes);

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

  // Create thread worker
  const workerUrl = new URL("./thread-worker.js", import.meta.url);
  const worker = new Worker(workerUrl, { type: "module" });

  const handle: ProcessHandle = {
    pid: 1,
    vmPtr,
    worker,
    memory,
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

  if (status === STATUS_OK) {
    // Budget exhausted — send more
    handle.worker.postMessage({ type: "step", budget: 500_000 } satisfies WorkerMessage);
  } else if (status === STATUS_FAULT) {
    // Process exited
    handle.onExit?.(msg.exitCode);
    handle.worker.terminate();
  } else if (status === STATUS_SLEEP) {
    // nanosleep — setTimeout then resume
    setTimeout(() => {
      handle.worker.postMessage({ type: "step", budget: 500_000 } satisfies WorkerMessage);
    }, 1); // minimum sleep
  } else {
    // Other blocking status — for now, just continue
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
