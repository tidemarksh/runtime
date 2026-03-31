/**
 * Thread-worker: executes a single guest thread on a shared process memory.
 */

import type { KernelExports, KernelRuntimeState, StatusCodes } from "./abi.js";
import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  readStatusCodes,
} from "./abi.js";
import type { ThreadWorkerInbound, ThreadWorkerOutbound } from "./messages.js";
import {
  readPublishedKernelRuntimeState,
  type RuntimeStateBridge,
  waitForRuntimeBridgeWake,
} from "./runtime-state-bridge.js";
import { detectWasmFeatures, type WasmFeatureSupport } from "./wasm-features.js";
import { createKernelEnv } from "./wasm-env.js";
import { bindWorkerMessageHandler, workerPostMessage } from "./worker-scope.js";

let exports: KernelExports;
let statusCodes: StatusCodes;
let vmPtr = 0;
let guestBase = 0;
let wasmFeatures: WasmFeatureSupport;
let runtimeBridge: RuntimeStateBridge | null = null;
const THREAD_STEP_BUDGET = 20_000;

async function init(
  wasmModule: WebAssembly.Module,
  memory: WebAssembly.Memory,
  threadVmPtr: number,
  threadGuestBase: number,
  kernelState: KernelRuntimeState,
  _notifySab: SharedArrayBuffer,
  _stateSab: SharedArrayBuffer,
): Promise<void> {
  const instance = await WebAssembly.instantiate(wasmModule, {
    env: createKernelEnv(memory),
  });
  exports = instance.exports as unknown as KernelExports;
  attachKernelRuntimeState(exports, kernelState);
  statusCodes = readStatusCodes(exports);
  vmPtr = threadVmPtr;
  guestBase = threadGuestBase;
  wasmFeatures = detectWasmFeatures();
  runtimeBridge = {
    notifySab: _notifySab,
    stateSab: _stateSab,
  };
}

function reattachPublishedKernelRuntimeState(): void {
  if (runtimeBridge === null) {
    return;
  }
  attachKernelRuntimeState(exports, readPublishedKernelRuntimeState(runtimeBridge));
}

function postReady(): void {
  const msg: ThreadWorkerOutbound = {
    type: "ready",
    vmPtr,
    kernelState: readKernelRuntimeState(exports),
    wasmFeatures,
  };
  workerPostMessage(msg);
}

function collectDebugParts(): string[] {
  if (typeof exports === "undefined") {
    return [];
  }

  const debugParts: string[] = [];
  const syscallNr = exports.debug_syscall_trace_nr?.();
  if (syscallNr !== undefined && syscallNr !== 0n) {
    debugParts.push(`syscall=${syscallNr.toString()}`);
    debugParts.push(`status=${exports.debug_syscall_trace_status?.() ?? "n/a"}`);
    debugParts.push(`result=${exports.debug_syscall_trace_result?.().toString() ?? "n/a"}`);
  }
  const memoryNr = exports.debug_memory_trace_nr?.();
  if (memoryNr !== undefined && memoryNr !== 0n) {
    debugParts.push(`mem_nr=${memoryNr.toString()}`);
    debugParts.push(`mem_a0=${exports.debug_memory_trace_arg0?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_a1=${exports.debug_memory_trace_arg1?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_a2=${exports.debug_memory_trace_arg2?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_a3=${exports.debug_memory_trace_arg3?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_result=${exports.debug_memory_trace_result?.().toString() ?? "n/a"}`);
  }
  const execveStage = exports.debug_execve_stage?.();
  if (execveStage !== undefined && execveStage !== 0) {
    debugParts.push(`execve_stage=${execveStage}`);
    debugParts.push(`execve_v0=${exports.debug_execve_value0?.().toString() ?? "n/a"}`);
    debugParts.push(`execve_v1=${exports.debug_execve_value1?.().toString() ?? "n/a"}`);
  }

  return debugParts;
}

function postStatus(status: number, waitInWorker = false): void {
  const msg: ThreadWorkerOutbound = {
    type: "status",
    status,
    exitCode: exports.thread_exit_code(vmPtr),
    termSignal: exports.thread_term_signal(vmPtr),
    pc: exports.thread_pc(vmPtr),
    vmPtr,
    kernelState: readKernelRuntimeState(exports),
    debug: (() => {
      const parts = collectDebugParts();
      return parts.length === 0 ? undefined : parts.join(" ");
    })(),
  };
  if (waitInWorker) {
    msg.waitInWorker = true;
  }
  workerPostMessage(msg);
}

function formatWorkerError(error: unknown): string {
  const base = error instanceof Error ? (error.stack ?? error.message) : String(error);
  const debugParts = collectDebugParts();
  return debugParts.length === 0 ? base : `${base}\n${debugParts.join(" ")}`;
}

function postError(error: unknown): void {
  const msg: ThreadWorkerOutbound = {
    type: "error",
    vmPtr,
    message: formatWorkerError(error),
  };
  workerPostMessage(msg);
}

function step(budget: number): void {
  for (;;) {
    exports.thread_step(vmPtr, budget);
    const status = exports.thread_status(vmPtr);

    if (status === statusCodes.FUTEX_WAIT) {
      if (handleFutexWait(status)) {
        budget = THREAD_STEP_BUDGET;
        continue;
      }
      return;
    }

    if (isRuntimeBridgeBlockedStatus(status)) {
      handleRuntimeBridgeBlockedStatus(status);
      return;
    }

    postStatus(status);
    return;
  }
}

function isRuntimeBridgeBlockedStatus(status: number): boolean {
  // Owner-side blocked retry keeps process/thread liveness centralized. Resuming
  // pipe/socket/flock waits inside the thread-worker can target stale vm_ptrs
  // after execve discards sibling threads.
  return false;
}

function handleRuntimeBridgeBlockedStatus(status: number): void {
  if (runtimeBridge === null) {
    postStatus(status);
    return;
  }

  postStatus(status, true);

  const waitResult = status === statusCodes.EPOLL_WAIT
    ? waitForRuntimeBridgeWake(
        runtimeBridge,
        Math.max(1, Number(exports.thread_epoll_wait_timeout(vmPtr))),
      )
    : waitForRuntimeBridgeWake(runtimeBridge);

  reattachPublishedKernelRuntimeState();
  if (status === statusCodes.EPOLL_WAIT && waitResult === "timed-out") {
    exports.thread_resume_epoll_wait_timeout(vmPtr);
  } else {
    exports.thread_resume_blocked_syscall(vmPtr);
  }

  postStatus(exports.thread_status(vmPtr));
}

function handleFutexWait(status: number): boolean {
  const addr = guestBase + Number(exports.thread_futex_wait_addr(vmPtr));
  const i32 = new Int32Array(exports.memory.buffer);
  const wordIndex = addr >>> 2;
  const expected = Atomics.load(i32, wordIndex);

  if (exports.thread_record_blocked_addr(vmPtr) === 0) {
    exports.thread_resume_futex_wait(vmPtr);
    return true;
  }

  postStatus(status, true);

  const timeoutPtr = Number(exports.thread_futex_wait_timeout(vmPtr));
  const timeoutMs = readTimespecMillis(timeoutPtr);
  const result = timeoutMs === undefined
    ? Atomics.wait(i32, wordIndex, expected)
    : Atomics.wait(i32, wordIndex, expected, timeoutMs);

  reattachPublishedKernelRuntimeState();
  if (result === "timed-out") {
    exports.thread_resume_futex_wait_result(vmPtr, BigInt(-110));
  } else {
    exports.thread_resume_futex_wait(vmPtr);
  }

  postStatus(exports.thread_status(vmPtr));
  return false;
}

function readTimespecMillis(timespecPtr: number): number | undefined {
  if (timespecPtr === 0) {
    return undefined;
  }
  const view = new DataView(exports.memory.buffer);
  const absolute = guestBase + timespecPtr;
  const sec = Number(view.getBigUint64(absolute, true));
  const nsec = Number(view.getBigUint64(absolute + 8, true));
  return Math.max(0, sec * 1000 + Math.floor(nsec / 1_000_000));
}

await bindWorkerMessageHandler<ThreadWorkerInbound>(async (event) => {
  const msg = event.data;

  switch (msg.type) {
    case "init":
      try {
        await init(
          msg.wasmModule,
          msg.memory,
          msg.vmPtr,
          msg.guestBase,
          msg.kernelState,
          msg.runtimeNotify,
          msg.runtimeState,
        );
        postReady();
      } catch (error) {
        postError(error);
      }
      break;
    case "step":
      try {
        attachKernelRuntimeState(exports, msg.kernelState);
        step(msg.budget);
      } catch (error) {
        postError(error);
      }
      break;
  }
});
