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
} from "./bridge/runtime-state.js";
import { detectWasmFeatures, type WasmFeatureSupport } from "./wasm/features.js";
import { createKernelEnv } from "./wasm/env.js";
import { bindWorkerMessageHandler, workerPostMessage } from "./bridge/scope.js";
import {
  collectDebugParts,
  formatWorkerError,
} from "./thread-worker/debug.js";
import { stepThread } from "./thread-worker/exec.js";
import {
  loadSyscallNumbers,
  readCompletedSyncEffects,
  type SyscallNumbers,
} from "./thread-worker/sync-effects.js";

let exports: KernelExports;
let statusCodes: StatusCodes;
let vmPtr = 0;
let guestBase = 0;
let wasmFeatures: WasmFeatureSupport;
let runtimeBridge: RuntimeStateBridge | null = null;
const THREAD_STEP_BUDGET = 20_000;
const EINTR_ERRNO = 4;
const FUTEX_WAIT_SLICE_MS = 10;

let syscallNumbers: SyscallNumbers | null = null;

async function init(
  wasmModule: WebAssembly.Module,
  memory: WebAssembly.Memory,
  threadVmPtr: number,
  threadGuestBase: number,
  kernelState: KernelRuntimeState,
  sabPageCache: SharedArrayBuffer | null,
  _notifySab: SharedArrayBuffer,
  _stateSab: SharedArrayBuffer,
): Promise<void> {
  const instance = await WebAssembly.instantiate(wasmModule, {
    env: createKernelEnv(memory, { sabPageCache }),
  });
  exports = instance.exports as unknown as KernelExports;
  attachKernelRuntimeState(exports, kernelState);
  statusCodes = readStatusCodes(exports);
  vmPtr = threadVmPtr;
  guestBase = threadGuestBase;
  wasmFeatures = detectWasmFeatures();
  syscallNumbers = loadSyscallNumbers(exports);
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

function postStatus(status: number, waitInWorker = false): void {
  const includeDebug = status !== statusCodes.FS;
  const msg: ThreadWorkerOutbound = {
    type: "status",
    status,
    exitCode: exports.thread_exit_code(vmPtr),
    termSignal: exports.thread_term_signal(vmPtr),
    pc: exports.thread_pc(vmPtr),
    vmPtr,
    kernelState: readKernelRuntimeState(exports),
    syncEffects: readCompletedSyncEffects(exports, statusCodes, status, syscallNumbers),
    debug: includeDebug ? (() => {
      const parts = collectDebugParts(exports, vmPtr);
      return parts.length === 0 ? undefined : parts.join(" ");
    })() : undefined,
  };
  if (waitInWorker) {
    msg.waitInWorker = true;
  }
  workerPostMessage(msg);
}

function postError(error: unknown): void {
  const msg: ThreadWorkerOutbound = {
    type: "error",
    vmPtr,
    message: formatWorkerError(exports, vmPtr, error),
  };
  workerPostMessage(msg);
}

function step(budget: number): void {
  stepThread({
    exports,
    statusCodes,
    vmPtr,
    guestBase,
    threadStepBudget: THREAD_STEP_BUDGET,
    eintrErrno: EINTR_ERRNO,
    futexWaitSliceMs: FUTEX_WAIT_SLICE_MS,
    runtimeBridge,
    postStatus,
    reattachPublishedKernelRuntimeState,
  }, budget);
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
          msg.sabPageCache,
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
