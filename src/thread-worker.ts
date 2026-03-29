/**
 * Thread worker: runs a single RISC-V thread.
 *
 * Instantiates the kernel WASM module with the shared Memory,
 * then runs the exec loop. Returns STATUS codes to the parent
 * via postMessage for blocking syscalls.
 */

import type { KernelExports, StatusCodes } from "./abi.js";
import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  readStatusCodes,
} from "./abi.js";
import type { WorkerMessage } from "./messages.js";

let exports: KernelExports;
let status: StatusCodes;
let vmPtr: number;

/**
 * Initialize the thread worker with a pre-compiled WASM module
 * and shared memory.
 */
async function init(
  wasmModule: WebAssembly.Module,
  memory: WebAssembly.Memory,
  threadVmPtr: number,
  kernelState: import("./abi.js").KernelRuntimeState,
): Promise<void> {
  const instance = await WebAssembly.instantiate(wasmModule, {
    env: { memory },
  });
  exports = instance.exports as unknown as KernelExports;
  attachKernelRuntimeState(exports, kernelState);
  status = readStatusCodes(exports);
  vmPtr = threadVmPtr;
}

/**
 * Run the exec loop for `budget` instructions.
 * Handles STATUS codes: OK continues, FAULT/SLEEP/etc. are reported
 * back to the parent thread.
 */
function step(budget: number): void {
  exports.thread_step(vmPtr, budget);
  const s = exports.thread_status(vmPtr);

  const msg: WorkerMessage = {
    type: "status",
    status: s,
    exitCode: exports.thread_exit_code(vmPtr) ?? 0,
    pc: exports.thread_pc(vmPtr),
    kernelState: readKernelRuntimeState(exports),
  };

  if (s === status.OK) {
    // Budget exhausted — request more from parent
    postMessage(msg);
  } else if (s === status.FAULT) {
    postMessage(msg);
  } else if (s === status.SLEEP) {
    // nanosleep: the sleep duration is in fault_addr/fault_pc
    // Parent will setTimeout and then send another step message
    postMessage(msg);
  } else if (s === status.FS) {
    // FS request pending in SAB ring buffer
    // Parent (or kernel worker) will process and notify
    postMessage(msg);
  } else {
    // Other blocking status (FUTEX_WAIT, PIPE_WAIT, EPOLL_WAIT, etc.)
    postMessage(msg);
  }
}

// Message handler
self.onmessage = async (event: MessageEvent<WorkerMessage>) => {
  const msg = event.data;
  switch (msg.type) {
    case "init":
      await init(msg.wasmModule, msg.memory, msg.vmPtr, msg.kernelState);
      postMessage({
        type: "status",
        status: 0,
        exitCode: 0,
        pc: 0n,
        kernelState: readKernelRuntimeState(exports),
      } satisfies WorkerMessage);
      break;
    case "step":
      attachKernelRuntimeState(exports, msg.kernelState);
      step(msg.budget);
      break;
  }
};
