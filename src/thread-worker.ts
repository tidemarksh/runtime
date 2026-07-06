/**
 * Thread-worker: executes a single guest thread on a shared process memory.
 */

import type { KernelExports, KernelRuntimeState, StatusCodes } from "./abi.js";
import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  readStatusCodes,
} from "./abi.js";
import type {
  ThreadWorkerMemoryPreserveRange,
  ThreadWorkerInbound,
  ThreadWorkerOutbound,
} from "./messages.js";
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
import { resumeBlockedSyscallStep } from "./thread-worker/blocking.js";
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
  publishThreadWorkerStatusOutcome,
  type ThreadWorkerBoundaryOutcome,
} from "./thread-worker/outcomes.js";
import { postThreadWorkerStatus } from "./thread-worker/status.js";
import {
  applyThreadWorkerGuestMemoryWrites,
  applyThreadWorkerInboundState,
} from "./thread-worker/inbound-state.js";

let exports: KernelExports | undefined;
let statusCodes: StatusCodes;
let vmPtr = 0;
let guestBase = 0;
let generation = 0;
let wasmFeatures: WasmFeatureSupport;
let runtimeBridge: RuntimeStateBridge | null = null;
let preparedInputs: ThreadWorkerPrepareInputs | null = null;
const DEFAULT_NODE_THREAD_STEP_BUDGET = 5_000_000;
const DEFAULT_BROWSER_THREAD_STEP_BUDGET = 2_000;
const THREAD_STEP_BUDGET = readThreadStepBudget();
const EINTR_ERRNO = 4;
const FUTEX_WAIT_SLICE_MS = 10;
const PIPE_SLOT_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_PIPE_SLOT_DEBUG ?? "") === "1";
const PIPE_RESUME_DEBUG_ENABLED = PIPE_SLOT_DEBUG_ENABLED
  || ((globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_PIPE_RESUME_DEBUG ?? "") === "1";
let lastResumeBlockedDebug: string | undefined;
let lastFutexWaitDebug: string | undefined;

function acquireSharedMemoryForStep(buffer: ArrayBufferLike): void {
  if (runtimeBridge !== null) {
    readPublishedRuntimeBridgeEpoch(runtimeBridge);
  }
  if (
    typeof SharedArrayBuffer !== "undefined"
    && Object.prototype.toString.call(buffer) === "[object SharedArrayBuffer]"
  ) {
    Atomics.load(new Int32Array(buffer as SharedArrayBuffer), 0);
  }
}

function readThreadStepBudget(): number {
  const envBudget = (globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_THREAD_STEP_BUDGET;
  const parsed = envBudget === undefined ? NaN : Number.parseInt(envBudget, 10);
  if (Number.isFinite(parsed) && parsed > 0) {
    return parsed;
  }
  const processLike = (globalThis as typeof globalThis & {
    process?: { versions?: { node?: string } };
  }).process;
  const isNodeRuntime = typeof processLike?.versions?.node === "string";
  const isBrowserRuntime = typeof window !== "undefined" && !isNodeRuntime;
  return isBrowserRuntime
    ? DEFAULT_BROWSER_THREAD_STEP_BUDGET
    : DEFAULT_NODE_THREAD_STEP_BUDGET;
}

function requireExports(): KernelExports {
  if (!exports) {
    throw new Error("thread worker kernel exports are not initialized");
  }
  return exports;
}

async function prepare(
  inputs: ThreadWorkerPrepareInputs,
  kernelState: KernelRuntimeState | undefined,
  preserveBytes: number,
  _notifySab: SharedArrayBuffer,
  _stateSab: SharedArrayBuffer,
  kernelStackSlot: number,
  preserveMemoryRanges?: readonly ThreadWorkerMemoryPreserveRange[],
): Promise<void> {
  // Instantiation replays active data segments into imported memory. Preserve
  // the static prefix and explicit runtime overlays; mutable heap allocations
  // such as AddressSpaceState/VMA Vec contents must remain live process state.
  const hasExplicitPreserveRanges = (preserveMemoryRanges?.length ?? 0) > 0;
  const effectivePreserveBytes = hasExplicitPreserveRanges ? 0 : preserveBytes;
  const preservedMemory = snapshotThreadWorkerPrepareMemory(
    inputs.memory,
    exports,
    kernelState,
    effectivePreserveBytes,
    { preserveRanges: preserveMemoryRanges },
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

function postStatus(status: number, waitInWorker = false, completedSyncEffects?: number): void {
  postThreadWorkerStatus({
    kernelExports: requireExports(),
    statusCodes,
    vmPtr,
    guestBase,
    generation,
    runtimeBridge,
    lastResumeBlockedDebug,
    lastFutexWaitDebug,
    setLastResumeBlockedDebug(value) {
      lastResumeBlockedDebug = value;
    },
    setLastFutexWaitDebug(value) {
      lastFutexWaitDebug = value;
    },
  }, status, waitInWorker, completedSyncEffects);
}

function postStatusOutcome(outcome: ThreadWorkerBoundaryOutcome): void {
  publishThreadWorkerStatusOutcome(postStatus, outcome);
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
    postStatus: postStatusOutcome,
    reattachPublishedKernelRuntimeState,
    recordFutexWaitDebug(debug) {
      lastFutexWaitDebug = debug;
    },
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
          msg.preserveMemoryRanges,
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
            Math.max(msg.kernelState.heapBase, msg.preserveMemoryBytes ?? 0),
            msg.runtimeNotify,
            msg.runtimeState,
            msg.kernelStackSlot,
            msg.preserveMemoryRanges,
          );
        } else {
          runtimeBridge = nextBridge;
        }
        attach(msg.vmPtr, msg.guestBase, msg.kernelState, msg.generation);
        applyThreadWorkerInboundState({
          kernelExports: requireExports(),
          guestBase,
        }, {
          kernelState: msg.kernelState,
          targetVmPtr: msg.vmPtr,
          ofdSlots: msg.ofdSlots,
          fdEntries: msg.fdEntries,
          socketState: msg.socketState,
          pipeSlots: msg.pipeSlots,
          guestMemoryWrites: msg.guestMemoryWrites,
          guestWriteWatchRegions: msg.guestWriteWatchRegions,
        });
        postReady();
      } catch (error) {
        postError(error);
      }
      break;
    case "step":
      try {
        const kernelExports = requireExports();
        attachKernelRuntimeState(kernelExports, msg.kernelState);
        applyThreadWorkerInboundState({
          kernelExports,
          guestBase,
        }, {
          kernelState: msg.kernelState,
          targetVmPtr: vmPtr,
          ofdSlots: msg.ofdSlots,
          fdEntries: msg.fdEntries,
          socketState: msg.socketState,
          pipeSlots: msg.pipeSlots,
        });
        acquireSharedMemoryForStep(kernelExports.memory.buffer);
        const resumeOutcome = resumeBlockedSyscallStep({
          exports: kernelExports,
          vmPtr,
          guestBase,
          resumeBlockedSyscall: msg.resumeBlockedSyscall === true,
          pipeResumeDebugEnabled: PIPE_RESUME_DEBUG_ENABLED,
          applyGuestMemoryWrites() {
            applyThreadWorkerGuestMemoryWrites({ kernelExports, guestBase }, msg.guestMemoryWrites);
          },
          recordResumeBlockedDebug(debug) {
            lastResumeBlockedDebug = debug;
          },
        });
        if (resumeOutcome !== undefined) {
          postStatusOutcome(resumeOutcome);
          return;
        }
        applyThreadWorkerGuestMemoryWrites({ kernelExports, guestBase }, msg.guestMemoryWrites);
        step(msg.budget);
      } catch (error) {
        postError(error);
      }
      break;
  }
});
