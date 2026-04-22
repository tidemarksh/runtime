/**
 * Thread-worker: executes a single guest thread on a shared process memory.
 */

import type { KernelExports, KernelRuntimeState, StatusCodes } from "./abi.js";
import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  readStatusCodes,
} from "./abi.js";
import type { GuestWriteWatchRegion, ThreadWorkerInbound, ThreadWorkerOutbound } from "./messages.js";
import {
  readPublishedRuntimeBridgeEpoch,
  readPublishedKernelRuntimeState,
  type RuntimeStateBridge,
} from "./bridge/runtime-state.js";
import { detectWasmFeatures, type WasmFeatureSupport } from "./wasm/features.js";
import { createKernelEnv } from "./wasm/env.js";
import { configureKernelInstanceStack } from "./wasm/stack.js";
import { bindWorkerMessageHandler, workerPostMessage } from "./bridge/scope.js";
import {
  collectDebugParts,
  formatWorkerError,
} from "./thread-worker/debug.js";
import { stepThread } from "./thread-worker/exec.js";
import {
  shouldPrepareThreadWorker,
  type ThreadWorkerPrepareInputs,
} from "./thread-worker/session.js";
import {
  restoreThreadWorkerPrepareMemory,
  snapshotThreadWorkerPrepareMemory,
} from "./thread-worker/prepare-memory.js";
import {
  loadSyscallNumbers,
  readCompletedSyncEffects,
  type SyscallNumbers,
} from "./thread-worker/sync-effects.js";

let exports: KernelExports | undefined;
let statusCodes: StatusCodes;
let vmPtr = 0;
let guestBase = 0;
let generation = 0;
let wasmFeatures: WasmFeatureSupport;
let runtimeBridge: RuntimeStateBridge | null = null;
let preparedInputs: ThreadWorkerPrepareInputs | null = null;
const THREAD_STEP_BUDGET = typeof window === "undefined" ? 20_000 : 2_000;
const EINTR_ERRNO = 4;
const FUTEX_WAIT_SLICE_MS = 10;
let syscallNumbers: SyscallNumbers | null = null;

function toKernelU64(value: number): bigint {
  return BigInt(Math.trunc(value));
}

function requireExports(): KernelExports {
  if (!exports) {
    throw new Error("thread worker kernel exports are not initialized");
  }
  return exports;
}

function applyGuestWriteWatchRegions(regions?: GuestWriteWatchRegion[]): void {
  const kernelExports = requireExports();
  kernelExports.debug_clear_guest_write_watch_trace?.();
  if (!regions || regions.length === 0) {
    return;
  }
  for (const region of regions) {
    kernelExports.debug_configure_guest_write_watch?.(
      region.slot,
      toKernelU64(region.start),
      toKernelU64(region.end),
    );
  }
}

async function prepare(
  inputs: ThreadWorkerPrepareInputs,
  kernelState: KernelRuntimeState | undefined,
  preserveBytes: number,
  _notifySab: SharedArrayBuffer,
  _stateSab: SharedArrayBuffer,
  kernelStackSlot: number,
): Promise<void> {
  // Instantiation replays active data segments into imported memory. Preserve
  // only the static/kernel image below heapBase so live thread/process state
  // above heapBase is not rolled back during worker transfer.
  const preservedMemory = snapshotThreadWorkerPrepareMemory(
    inputs.memory,
    exports,
    kernelState,
    preserveBytes,
  );
  const instance = await WebAssembly.instantiate(inputs.wasmModule, {
    env: createKernelEnv(inputs.memory, { sabPageCache: inputs.sabPageCache }),
  });
  const nextExports = instance.exports as unknown as KernelExports;
  exports = nextExports;
  restoreThreadWorkerPrepareMemory(inputs.memory, preservedMemory);
  configureKernelInstanceStack(nextExports, kernelStackSlot);
  wasmFeatures = detectWasmFeatures();
  preparedInputs = inputs;
  runtimeBridge = {
    notifySab: _notifySab,
    stateSab: _stateSab,
  };
}

function attach(
  threadVmPtr: number,
  threadGuestBase: number,
  kernelState: KernelRuntimeState,
  nextGeneration: number,
): void {
  const kernelExports = requireExports();
  attachKernelRuntimeState(kernelExports, kernelState);
  statusCodes = readStatusCodes(kernelExports);
  vmPtr = threadVmPtr;
  guestBase = threadGuestBase;
  generation = nextGeneration;
  syscallNumbers = loadSyscallNumbers(kernelExports);
}

function postPrepared(): void {
  const msg: ThreadWorkerOutbound = {
    type: "prepared",
    wasmFeatures,
  };
  workerPostMessage(msg);
}

function reattachPublishedKernelRuntimeState(): void {
  if (runtimeBridge === null) {
    return;
  }
  attachKernelRuntimeState(requireExports(), readPublishedKernelRuntimeState(runtimeBridge));
}

function postReady(): void {
  const kernelExports = requireExports();
  const msg: ThreadWorkerOutbound = {
    type: "ready",
    generation,
    vmPtr,
    kernelState: readKernelRuntimeState(kernelExports),
    wasmFeatures,
    debug: (() => {
      const parts = collectDebugParts(kernelExports, vmPtr, guestBase);
      return parts.length === 0 ? undefined : parts.join(" ");
    })(),
  };
  workerPostMessage(msg);
}

function postStatus(status: number, waitInWorker = false): void {
  const kernelExports = requireExports();
  const includeDebug = status !== statusCodes.FS;
  const msg: ThreadWorkerOutbound = {
    type: "status",
    generation,
    status,
    exitCode: kernelExports.thread_exit_code(vmPtr),
    termSignal: kernelExports.thread_term_signal(vmPtr),
    pc: kernelExports.thread_pc(vmPtr),
    ra: kernelExports.thread_ra?.(vmPtr),
    sp: kernelExports.thread_sp?.(vmPtr),
    s11: kernelExports.thread_s11?.(vmPtr),
    vmPtr,
    kernelState: readKernelRuntimeState(kernelExports),
    syncEffects: readCompletedSyncEffects(kernelExports, statusCodes, status, syscallNumbers),
    debug: includeDebug ? (() => {
      const parts = collectDebugParts(kernelExports, vmPtr, guestBase);
      return parts.length === 0 ? undefined : parts.join(" ");
    })() : undefined,
  };
  if (waitInWorker) {
    msg.waitInWorker = true;
  }
  workerPostMessage(msg);
}

function postError(error: unknown): void {
  const base = error instanceof Error ? (error.stack ?? error.message) : String(error);
  const msg: ThreadWorkerOutbound = {
    type: "error",
    generation,
    vmPtr,
    message: exports
      ? formatWorkerError(exports, vmPtr, guestBase, error)
      : base,
  };
  workerPostMessage(msg);
}

function step(budget: number): void {
  const kernelExports = requireExports();
  stepThread({
    exports: kernelExports,
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
    case "prepare":
      try {
        await prepare(
          {
            wasmModule: msg.wasmModule,
            memory: msg.memory,
            sabPageCache: msg.sabPageCache,
          },
          undefined,
          msg.preserveBytes ?? msg.memory.buffer.byteLength,
          msg.runtimeNotify,
          msg.runtimeState,
          msg.kernelStackSlot,
        );
        postPrepared();
      } catch (error) {
        postError(error);
      }
      break;
    case "init":
      try {
        const nextInputs = {
          wasmModule: msg.wasmModule,
          memory: msg.memory,
          sabPageCache: msg.sabPageCache,
        };
        const nextBridge = {
          notifySab: msg.runtimeNotify,
          stateSab: msg.runtimeState,
        };
        const minimumEpoch = msg.runtimeEpoch ?? 0;
        const currentEpoch = readPublishedRuntimeBridgeEpoch(nextBridge);
        // The init payload already carries the canonical kernel state. If the
        // bridge epoch lags during worker transfer, spinning here can strand
        // the worker before it ever reattaches to the new owner.
        if (minimumEpoch > 0 && currentEpoch < minimumEpoch) {
        }
        const shouldPrepare = msg.forcePrepare || shouldPrepareThreadWorker(preparedInputs, nextInputs);
        if (shouldPrepare) {
          await prepare(
            nextInputs,
            msg.kernelState,
            msg.kernelState.heapBase,
            msg.runtimeNotify,
            msg.runtimeState,
            msg.kernelStackSlot,
          );
        } else {
          runtimeBridge = nextBridge;
        }
        attach(msg.vmPtr, msg.guestBase, msg.kernelState, msg.generation);
        applyGuestWriteWatchRegions(msg.guestWriteWatchRegions);
        postReady();
      } catch (error) {
        postError(error);
      }
      break;
    case "step":
      try {
        attachKernelRuntimeState(requireExports(), msg.kernelState);
        step(msg.budget);
      } catch (error) {
        postError(error);
      }
      break;
  }
});
