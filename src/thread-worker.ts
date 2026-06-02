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
  loadSyscallNumbers,
  type SyscallNumbers,
} from "./thread-worker/sync-effects.js";
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
let releaseExecTraceInitialized = false;
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
let syscallNumbers: SyscallNumbers | null = null;
const DIAGNOSTIC_FD_ENTRY_BYTES = 8;
const DIAGNOSTIC_GUEST_WRITE_WATCH_SLOTS = 64;
const DIAGNOSTIC_FD_TABLE_WATCH_SLOT = 0;
const DIAGNOSTIC_EXEC_WATCH_FIRST_SLOT = 1;
const DIAGNOSTIC_DATA_WATCH_FIRST_SLOT = 33;
const DIAGNOSTIC_PROT_EXEC = 4;
const DIAGNOSTIC_PROT_WRITE = 2;
const PAGE_CACHE_IMPORT_EXEC_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_PAGE_CACHE_IMPORT_EXEC_DEBUG ?? "") === "1";

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

function configureDiagnosticGuestWriteWatch(
  kernelExports: KernelExports,
  threadVmPtr: number,
): void {
  if (!kernelExports.debug_configure_guest_write_watch || threadVmPtr === 0) {
    return;
  }
  const processPtr = kernelExports.thread_process_ptr?.(threadVmPtr) ?? 0;
  if (processPtr > 0) {
    const fdTableBytes = kernelExports.abi_max_fds() * DIAGNOSTIC_FD_ENTRY_BYTES;
    kernelExports.debug_configure_guest_write_watch(
      DIAGNOSTIC_FD_TABLE_WATCH_SLOT,
      BigInt(processPtr),
      BigInt(processPtr + fdTableBytes),
    );
  }
  let nextSlot = DIAGNOSTIC_EXEC_WATCH_FIRST_SLOT;
  const vmaLen = kernelExports.debug_memory_vma_len?.() ?? 0;
  for (
    let index = 0;
    index < vmaLen && nextSlot < DIAGNOSTIC_DATA_WATCH_FIRST_SLOT;
    index += 1
  ) {
    const prot = kernelExports.debug_memory_vma_prot?.(index) ?? 0;
    if ((prot & DIAGNOSTIC_PROT_EXEC) === 0) {
      continue;
    }
    const start = kernelExports.debug_memory_vma_start?.(index) ?? 0n;
    const end = kernelExports.debug_memory_vma_end?.(index) ?? 0n;
    if (BigInt(start) >= BigInt(end)) {
      continue;
    }
    kernelExports.debug_configure_guest_write_watch(nextSlot, start, end);
    nextSlot += 1;
  }
  for (; nextSlot < DIAGNOSTIC_DATA_WATCH_FIRST_SLOT; nextSlot += 1) {
    kernelExports.debug_configure_guest_write_watch(nextSlot, 0n, 0n);
  }
  nextSlot = DIAGNOSTIC_DATA_WATCH_FIRST_SLOT;
  for (
    let index = 0;
    index < vmaLen && nextSlot < DIAGNOSTIC_GUEST_WRITE_WATCH_SLOTS;
    index += 1
  ) {
    const prot = kernelExports.debug_memory_vma_prot?.(index) ?? 0;
    if ((prot & DIAGNOSTIC_PROT_WRITE) === 0 || (prot & DIAGNOSTIC_PROT_EXEC) !== 0) {
      continue;
    }
    const start = kernelExports.debug_memory_vma_start?.(index) ?? 0n;
    const end = kernelExports.debug_memory_vma_end?.(index) ?? 0n;
    if (BigInt(start) >= BigInt(end)) {
      continue;
    }
    kernelExports.debug_configure_guest_write_watch(nextSlot, start, end);
    nextSlot += 1;
  }
  for (; nextSlot < DIAGNOSTIC_GUEST_WRITE_WATCH_SLOTS; nextSlot += 1) {
    kernelExports.debug_configure_guest_write_watch(nextSlot, 0n, 0n);
  }
}

function enableDiagnosticReleaseExecTrace(kernelExports: KernelExports): void {
  if (!kernelExports.debug_set_release_exec_trace_enabled) {
    return;
  }
  if (!releaseExecTraceInitialized) {
    kernelExports.debug_clear_release_exec_trace?.();
    releaseExecTraceInitialized = true;
  }
  kernelExports.debug_set_release_exec_trace_enabled(1);
}

function readDiagnosticVmaPath(kernelExports: KernelExports, index: number): string {
  const len = kernelExports.debug_memory_vma_path_len?.(index) ?? 0;
  if (len <= 0) {
    return "";
  }
  const bytes: number[] = [];
  for (let offset = 0; offset < Math.min(len, 160); offset += 1) {
    const byte = kernelExports.debug_memory_vma_path_byte?.(index, offset) ?? 0;
    if (byte === 0) {
      break;
    }
    bytes.push(byte & 0xff);
  }
  return new TextDecoder().decode(new Uint8Array(bytes));
}

function sampleBytes(bytes: Uint8Array | undefined, offset: number, length: number): string {
  if (!bytes || offset >= bytes.length) {
    return "";
  }
  return [...bytes.subarray(offset, Math.min(bytes.length, offset + length))]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function recordPageCacheImportExecOverlap(event: {
  source: "page-cache-read" | "page-cache-stat";
  ino: number;
  offset: number;
  ptr: number;
  length: number;
  bytes?: Uint8Array;
}): void {
  if (!PAGE_CACHE_IMPORT_EXEC_DEBUG_ENABLED || !exports || guestBase <= 0 || event.length <= 0) {
    return;
  }
  const guestStart = event.ptr - guestBase;
  if (!Number.isFinite(guestStart) || guestStart < 0) {
    return;
  }
  const writeStart = BigInt(guestStart);
  const writeEnd = writeStart + BigInt(event.length);
  const vmaLen = exports.debug_memory_vma_len?.() ?? 0;
  for (let index = 0; index < vmaLen; index += 1) {
    const prot = exports.debug_memory_vma_prot?.(index) ?? 0;
    if ((prot & DIAGNOSTIC_PROT_EXEC) === 0) {
      continue;
    }
    const vmaStart = BigInt(exports.debug_memory_vma_start?.(index) ?? 0n);
    const vmaEnd = BigInt(exports.debug_memory_vma_end?.(index) ?? 0n);
    if (vmaStart >= vmaEnd || writeEnd <= vmaStart || writeStart >= vmaEnd) {
      continue;
    }
    const overlapStart = writeStart > vmaStart ? writeStart : vmaStart;
    const overlapEnd = writeEnd < vmaEnd ? writeEnd : vmaEnd;
    const sampleOffset = Number(overlapStart - writeStart);
    lastResumeBlockedDebug = [
      "page-cache-import-exec-overlap",
      `src=${event.source}`,
      `ino=${event.ino}`,
      `off=${event.offset}`,
      `ptr=0x${event.ptr.toString(16)}`,
      `guest=0x${guestStart.toString(16)}`,
      `len=${event.length}`,
      `overlap=0x${overlapStart.toString(16)}-0x${overlapEnd.toString(16)}`,
      `vma=0x${vmaStart.toString(16)}-0x${vmaEnd.toString(16)}`,
      `prot=${prot}`,
      `fd=${exports.debug_memory_vma_fd?.(index) ?? "n/a"}`,
      `path=${JSON.stringify(readDiagnosticVmaPath(exports, index))}`,
      `sample=${sampleBytes(event.bytes, sampleOffset, 16)}`,
    ].join(":");
    return;
  }
  const ownedLen = exports.debug_kernel_owned_guest_range_len?.() ?? 0;
  for (let index = 0; index < ownedLen; index += 1) {
    const ownedStart = BigInt(exports.debug_kernel_owned_guest_range_start?.(index) ?? 0n);
    const ownedEnd = BigInt(exports.debug_kernel_owned_guest_range_end?.(index) ?? 0n);
    if (ownedStart >= ownedEnd || writeEnd <= ownedStart || writeStart >= ownedEnd) {
      continue;
    }
    const overlapStart = writeStart > ownedStart ? writeStart : ownedStart;
    const overlapEnd = writeEnd < ownedEnd ? writeEnd : ownedEnd;
    const sampleOffset = Number(overlapStart - writeStart);
    lastResumeBlockedDebug = [
      "page-cache-import-kernel-owned-overlap",
      `src=${event.source}`,
      `ino=${event.ino}`,
      `off=${event.offset}`,
      `ptr=0x${event.ptr.toString(16)}`,
      `guest=0x${guestStart.toString(16)}`,
      `len=${event.length}`,
      `overlap=0x${overlapStart.toString(16)}-0x${overlapEnd.toString(16)}`,
      `owned=0x${ownedStart.toString(16)}-0x${ownedEnd.toString(16)}`,
      `sample=${sampleBytes(event.bytes, sampleOffset, 16)}`,
    ].join(":");
    return;
  }
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
  // Instantiation replays active data segments into imported memory. When this
  // worker already has exports, preserve precise runtime overlays. A fresh
  // worker cannot derive overlay sizes yet, so preserve the allocated kernel
  // heap prefix to keep inherited fd/OFD state intact across fork/exec attach.
  const hasExplicitPreserveRanges = (preserveMemoryRanges?.length ?? 0) > 0;
  const effectivePreserveBytes = !hasExplicitPreserveRanges && !exports && kernelState
    ? Math.max(preserveBytes, kernelState.heapPtr)
    : preserveBytes;
  const preservedMemory = snapshotThreadWorkerPrepareMemory(
    inputs.memory,
    exports,
    kernelState,
    effectivePreserveBytes,
    { preserveRanges: preserveMemoryRanges },
  );
  const instance = await WebAssembly.instantiate(inputs.wasmModule, {
    env: createKernelEnv(inputs.memory, {
      sabPageCache: inputs.sabPageCache,
      pageCacheMemoryWriteObserver: recordPageCacheImportExecOverlap,
    }),
  });
  const nextExports = instance.exports as unknown as KernelExports;
  exports = nextExports;
  restoreThreadWorkerPrepareMemory(inputs.memory, preservedMemory);
  configureKernelInstanceStack(nextExports, kernelStackSlot);
  wasmFeatures = detectWasmFeatures();
  preparedInputs = inputs;
  releaseExecTraceInitialized = false;
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

function postStatus(status: number, waitInWorker = false, completedSyncEffects?: number): void {
  postThreadWorkerStatus({
    kernelExports: requireExports(),
    statusCodes,
    vmPtr,
    guestBase,
    generation,
    runtimeBridge,
    syscallNumbers,
    lastResumeBlockedDebug,
    setLastResumeBlockedDebug(value) {
      lastResumeBlockedDebug = value;
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
    syscallNumbers,
    runtimeBridge,
    postStatus: postStatusOutcome,
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
          childExitRecords: msg.childExitRecords,
          pipeSlots: msg.pipeSlots,
          guestMemoryWrites: msg.guestMemoryWrites,
          guestWriteWatchRegions: msg.guestWriteWatchRegions,
        });
        enableDiagnosticReleaseExecTrace(requireExports());
        configureDiagnosticGuestWriteWatch(requireExports(), vmPtr);
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
          childExitRecords: msg.childExitRecords,
          pipeSlots: msg.pipeSlots,
        });
        enableDiagnosticReleaseExecTrace(kernelExports);
        configureDiagnosticGuestWriteWatch(kernelExports, vmPtr);
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
