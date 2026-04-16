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
  observeRuntimeBridgeEpoch,
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
  shouldPrepareThreadWorker,
  type ThreadWorkerPrepareInputs,
} from "./thread-worker/session.js";
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
let preparedInputs: ThreadWorkerPrepareInputs | null = null;
let preparedKernelHeapBase = 0;
const THREAD_STEP_BUDGET = typeof window === "undefined" ? 20_000 : 2_000;
const EINTR_ERRNO = 4;
const FUTEX_WAIT_SLICE_MS = 10;
const PREPARE_PRESERVE_PREFIX_FALLBACK_BYTES = 64 * 1024 * 1024;
const MEMORY_SNAPSHOT_CHUNK_BYTES = 4 * 1024 * 1024;

let syscallNumbers: SyscallNumbers | null = null;

function toKernelU64(value: number): bigint {
  return BigInt(Math.trunc(value));
}

function applyGuestWriteWatchRegions(regions?: GuestWriteWatchRegion[]): void {
  exports.debug_clear_guest_write_watch_trace?.();
  if (!regions || regions.length === 0) {
    return;
  }
  for (const region of regions) {
    exports.debug_configure_guest_write_watch?.(
      region.slot,
      toKernelU64(region.start),
      toKernelU64(region.end),
    );
  }
}

interface MemorySnapshotChunk {
  start: number;
  bytes: Uint8Array;
}

interface MemorySnapshotRange {
  chunks: MemorySnapshotChunk[];
}

function snapshotMemoryRange(
  memory: WebAssembly.Memory,
  start: number,
  length: number,
): MemorySnapshotRange | null {
  if (length <= 0 || start < 0 || start >= memory.buffer.byteLength) {
    return null;
  }
  const end = Math.min(memory.buffer.byteLength, start + length);
  if (end <= start) {
    return null;
  }
  const chunks: MemorySnapshotChunk[] = [];
  for (let offset = start; offset < end; offset += MEMORY_SNAPSHOT_CHUNK_BYTES) {
    const chunkEnd = Math.min(end, offset + MEMORY_SNAPSHOT_CHUNK_BYTES);
    chunks.push({
      start: offset,
      bytes: new Uint8Array(memory.buffer, offset, chunkEnd - offset).slice(),
    });
  }
  return { chunks };
}

function restoreMemoryRange(
  memory: WebAssembly.Memory,
  range: MemorySnapshotRange | null,
): void {
  if (!range || range.chunks.length === 0) {
    return;
  }
  const bytes = new Uint8Array(memory.buffer);
  for (const chunk of range.chunks) {
    if (chunk.bytes.byteLength === 0) {
      continue;
    }
    bytes.set(chunk.bytes, chunk.start);
  }
}

async function prepare(
  inputs: ThreadWorkerPrepareInputs,
  _notifySab: SharedArrayBuffer,
  _stateSab: SharedArrayBuffer,
  preserveGuestRange?: { guestBase: number; guestBytes: number },
  preserveKernelHeapRange?: { heapBase: number; heapBytes: number },
): Promise<void> {
  const preservePrefixBytes = Math.min(
    inputs.memory.buffer.byteLength,
    preparedKernelHeapBase > 0
      ? preparedKernelHeapBase
      : PREPARE_PRESERVE_PREFIX_FALLBACK_BYTES,
  );
  const preservedPrefix = snapshotMemoryRange(inputs.memory, 0, preservePrefixBytes);
  // A freshly attached plain-fork child reaches thread-worker init with a live
  // guest image already staged in shared memory. Browser imported-memory
  // instantiation replays module data segments over that guest window, so keep
  // the guest range intact across instantiate before the first step.
  const preservedGuestRange = preserveGuestRange
    ? snapshotMemoryRange(inputs.memory, preserveGuestRange.guestBase, preserveGuestRange.guestBytes)
    : null;
  const preservedKernelHeapRange = preserveKernelHeapRange
    ? snapshotMemoryRange(inputs.memory, preserveKernelHeapRange.heapBase, preserveKernelHeapRange.heapBytes)
    : null;
  const instance = await WebAssembly.instantiate(inputs.wasmModule, {
    env: createKernelEnv(inputs.memory, { sabPageCache: inputs.sabPageCache }),
  });
  exports = instance.exports as unknown as KernelExports;
  const nextKernelHeapBase = exports.kernel_heap_base();
  if (preservedPrefix && preservedPrefix.chunks.length > 0) {
    const restoreChunks: MemorySnapshotChunk[] = [];
    for (const chunk of preservedPrefix.chunks) {
      if (chunk.start >= nextKernelHeapBase) {
        break;
      }
      const restoreEnd = Math.min(chunk.start + chunk.bytes.byteLength, nextKernelHeapBase);
      if (restoreEnd <= chunk.start) {
        continue;
      }
      restoreChunks.push({
        start: chunk.start,
        bytes: chunk.bytes.subarray(0, restoreEnd - chunk.start),
      });
    }
    if (restoreChunks.length > 0) {
      restoreMemoryRange(inputs.memory, { chunks: restoreChunks });
    }
  }
  restoreMemoryRange(inputs.memory, preservedKernelHeapRange);
  restoreMemoryRange(inputs.memory, preservedGuestRange);
  preparedKernelHeapBase = nextKernelHeapBase;
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
): void {
  attachKernelRuntimeState(exports, kernelState);
  statusCodes = readStatusCodes(exports);
  vmPtr = threadVmPtr;
  guestBase = threadGuestBase;
  syscallNumbers = loadSyscallNumbers(exports);
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
    ra: exports.thread_ra?.(vmPtr),
    sp: exports.thread_sp?.(vmPtr),
    s11: exports.thread_s11?.(vmPtr),
    vmPtr,
    kernelState: readKernelRuntimeState(exports),
    syncEffects: readCompletedSyncEffects(exports, statusCodes, status, syscallNumbers),
    debug: includeDebug ? (() => {
      const parts = collectDebugParts(exports, vmPtr, guestBase);
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
    message: formatWorkerError(exports, vmPtr, guestBase, error),
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
    case "prepare":
      try {
        await prepare(
          {
            wasmModule: msg.wasmModule,
            memory: msg.memory,
            sabPageCache: msg.sabPageCache,
          },
          msg.runtimeNotify,
          msg.runtimeState,
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
        observeRuntimeBridgeEpoch(nextBridge, msg.runtimeEpoch ?? 0);
        if (shouldPrepareThreadWorker(preparedInputs, nextInputs)) {
          await prepare(
            nextInputs,
            msg.runtimeNotify,
            msg.runtimeState,
            msg.guestBytes !== undefined
              ? { guestBase: msg.guestBase, guestBytes: msg.guestBytes }
              : undefined,
            msg.preserveKernelHeapBase !== undefined && msg.preserveKernelHeapBytes !== undefined
              ? {
                  heapBase: msg.preserveKernelHeapBase,
                  heapBytes: msg.preserveKernelHeapBytes,
                }
              : undefined,
          );
        } else {
          runtimeBridge = nextBridge;
        }
        attach(msg.vmPtr, msg.guestBase, msg.kernelState);
        applyGuestWriteWatchRegions(msg.guestWriteWatchRegions);
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
