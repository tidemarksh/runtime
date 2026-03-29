/**
 * Thread worker: runs a single RISC-V thread.
 *
 * Instantiates the kernel WASM module with the shared Memory,
 * then runs the exec loop. Returns STATUS codes to the parent
 * via postMessage for blocking syscalls.
 *
 * §6.4: Blocking statuses (FUTEX_WAIT, PIPE_WAIT, etc.) are handled
 * locally via Atomics.wait. Only lifecycle events (CLONE_SPAWN,
 * EXECVE_SPAWN) and SLEEP are posted to the parent.
 */

import type { KernelExports, KernelRuntimeState, StatusCodes } from "./abi.js";
import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  readStatusCodes,
} from "./abi.js";
import type { ThreadWorkerInbound, ThreadWorkerOutbound } from "./messages.js";

let exports: KernelExports;
let statusCodes: StatusCodes;
let vmPtr: number;
let fsRingBuffer: SharedArrayBuffer;

/**
 * Initialize the thread worker with a pre-compiled WASM module
 * and shared memory.
 */
async function init(
  wasmModule: WebAssembly.Module,
  memory: WebAssembly.Memory,
  threadVmPtr: number,
  kernelState: KernelRuntimeState,
  ringBuffer: SharedArrayBuffer,
): Promise<void> {
  const instance = await WebAssembly.instantiate(wasmModule, {
    env: { memory },
  });
  exports = instance.exports as unknown as KernelExports;
  attachKernelRuntimeState(exports, kernelState);
  statusCodes = readStatusCodes(exports);
  vmPtr = threadVmPtr;
  fsRingBuffer = ringBuffer;
}

/**
 * Post a status message to the parent (process owner).
 */
function postStatus(s: number): void {
  const msg: ThreadWorkerOutbound = {
    type: "status",
    status: s,
    exitCode: exports.thread_exit_code(vmPtr) ?? 0,
    pc: exports.thread_pc(vmPtr),
    vmPtr,
    kernelState: readKernelRuntimeState(exports),
  };
  postMessage(msg);
}

/**
 * Run the exec loop for `budget` instructions.
 *
 * §6.4: STATUS codes determine control flow:
 * - OK: budget exhausted, request more from parent
 * - FAULT: thread exited
 * - FUTEX_WAIT/PIPE_WAIT/EPOLL_WAIT: Atomics.wait locally
 * - FS: ring buffer submit → Atomics.wait → resume
 * - SLEEP/CLONE_SPAWN/EXECVE_SPAWN/VFORK_WAIT/WAIT4: post to parent
 */
function step(budget: number): void {
  exports.thread_step(vmPtr, budget);
  const s = exports.thread_status(vmPtr);

  if (s === statusCodes.OK) {
    // Budget exhausted — post to parent for cooperative scheduling
    postStatus(s);
  } else if (s === statusCodes.FAULT) {
    postStatus(s);
  } else if (s === statusCodes.FUTEX_WAIT) {
    handleFutexWait();
  } else if (s === statusCodes.FS) {
    // TODO: ring buffer submit → Atomics.wait → resume
    // For now, post to parent for synchronous processing
    // fsRingBuffer will be used here once kernel-worker.ts is ready
    void fsRingBuffer;
    postStatus(s);
  } else if (s === statusCodes.PIPE_WAIT) {
    // TODO: Atomics.wait on pipe SAB notify word
    postStatus(s);
  } else if (s === statusCodes.EPOLL_WAIT) {
    // TODO: Atomics.wait on epoll SAB wake counter
    postStatus(s);
  } else {
    // SLEEP, CLONE_SPAWN, EXECVE_SPAWN, VFORK_WAIT, WAIT4:
    // Require parent orchestration — post to parent
    postStatus(s);
  }
}

/**
 * §6.4: FUTEX_WAIT — Atomics.wait32 on guest Memory futex word.
 * Another thread's FUTEX_WAKE writes to the address and calls
 * memory.atomic.notify, which wakes this Atomics.wait.
 */
function handleFutexWait(): void {
  const addr = exports.thread_futex_wait_addr(vmPtr);
  const guestAddr = Number(addr);

  // Futex word is in shared guest Memory — Atomics.wait32 directly
  const i32 = new Int32Array(exports.memory.buffer);
  const wordIndex = guestAddr >>> 2;
  const currentValue = Atomics.load(i32, wordIndex);

  // Check if already woken (blocked_addr cleared by another thread's FUTEX_WAKE)
  const blockedAddr = exports.thread_record_blocked_addr(vmPtr);
  if (blockedAddr === 0) {
    // Already woken before we got here
    exports.thread_resume_futex_wait(vmPtr);
    step(500_000);
    return;
  }

  // Wait on the futex word — will be woken by another thread's
  // memory.atomic.notify on the same address
  const timeout = exports.thread_futex_wait_timeout(vmPtr);
  const timeoutMs = timeout !== 0n ? 5000 : undefined; // TODO: read actual timespec

  const result = Atomics.wait(i32, wordIndex, currentValue, timeoutMs);

  if (result === "timed-out") {
    exports.thread_resume_futex_wait_result(vmPtr, BigInt(-110)); // ETIMEDOUT
  } else {
    exports.thread_resume_futex_wait(vmPtr);
  }

  // Continue execution
  step(500_000);
}

// Message handler
self.onmessage = async (event: MessageEvent<ThreadWorkerInbound>) => {
  const msg = event.data;
  switch (msg.type) {
    case "init":
      await init(
        msg.wasmModule,
        msg.memory,
        msg.vmPtr,
        msg.kernelState,
        msg.fsRingBuffer,
      );
      postStatus(0); // STATUS_OK — ready
      break;
    case "step":
      attachKernelRuntimeState(exports, msg.kernelState);
      step(msg.budget);
      break;
    case "wake-sleep":
      // Woken from nanosleep by parent's setTimeout
      step(500_000);
      break;
  }
};
