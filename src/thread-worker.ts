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
  GuestWriteWatchRegion,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerSocketStateSnapshot,
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
const DEFAULT_NODE_THREAD_STEP_BUDGET = 250_000;
const DEFAULT_BROWSER_THREAD_STEP_BUDGET = 2_000;
const THREAD_STEP_BUDGET = readThreadStepBudget();
const EINTR_ERRNO = 4;
const FUTEX_WAIT_SLICE_MS = 10;
let syscallNumbers: SyscallNumbers | null = null;

function readThreadStepBudget(): number {
  const envBudget = (globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_THREAD_STEP_BUDGET;
  const parsed = envBudget === undefined ? NaN : Number.parseInt(envBudget, 10);
  if (Number.isFinite(parsed) && parsed > 0) {
    return parsed;
  }
  return typeof window === "undefined"
    ? DEFAULT_NODE_THREAD_STEP_BUDGET
    : DEFAULT_BROWSER_THREAD_STEP_BUDGET;
}

function applyChildExitRecords(records?: Array<{ childPid: number; waitStatus: number }>): void {
  if (!records || records.length === 0) {
    return;
  }
  const kernelExports = requireExports();
  for (const record of records) {
    kernelExports.process_record_child_exit(record.childPid, record.waitStatus);
  }
}

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
  // Instantiation replays active data segments into imported memory. When this
  // worker already has exports, preserve precise runtime overlays. A fresh
  // worker cannot derive overlay sizes yet, so preserve the allocated kernel
  // heap prefix to keep inherited fd/OFD state intact across fork/exec attach.
  const effectivePreserveBytes = !exports && kernelState
    ? Math.max(preserveBytes, kernelState.heapPtr)
    : preserveBytes;
  const preservedMemory = snapshotThreadWorkerPrepareMemory(
    inputs.memory,
    exports,
    kernelState,
    effectivePreserveBytes,
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

function snapshotProcessFdEntries(
  kernelExports: KernelExports,
  processVmPtr: number,
): KernelWorkerFdEntrySnapshot[] {
  if (processVmPtr === 0) {
    return [];
  }
  const fdEntries: KernelWorkerFdEntrySnapshot[] = [];
  const maxFds = kernelExports.abi_max_fds();
  for (let fd = 0; fd < maxFds; fd += 1) {
    fdEntries.push({
      fd,
      ofdId: kernelExports.process_fd_ofd_id(processVmPtr, fd),
      fdFlags: kernelExports.process_fd_flags(processVmPtr, fd),
    });
  }
  return fdEntries;
}

function snapshotOfdSlotsForFdEntries(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
  processVmPtr: number,
  fdEntries: KernelWorkerFdEntrySnapshot[],
): KernelWorkerOfdSlotSnapshot[] {
  const maxFds = kernelExports.abi_max_fds();
  const tableBytes = kernelExports.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return [];
  }

  const ofdIds = new Set<number>();
  for (const entry of fdEntries) {
    if (entry.ofdId > 0 && entry.ofdId < maxFds) {
      ofdIds.add(entry.ofdId);
    }
  }
  const blockedOfdId = processVmPtr === 0
    ? 0
    : (kernelExports.thread_blocked_ofd_id?.(processVmPtr) ?? 0);
  if (blockedOfdId > 0 && blockedOfdId < maxFds) {
    ofdIds.add(blockedOfdId);
  }

  const memory = new Uint8Array(kernelExports.memory.buffer);
  const snapshots: KernelWorkerOfdSlotSnapshot[] = [];
  for (const ofdId of ofdIds) {
    const offset = kernelState.ofdTablePtr + ofdId * slotBytes;
    snapshots.push({
      ofdId,
      slot: memory.slice(offset, offset + slotBytes),
    });
  }
  return snapshots;
}

function snapshotSocketState(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
): KernelWorkerSocketStateSnapshot | undefined {
  if (kernelState.socketUsed === 0 && kernelState.netBufUsed === 0n) {
    return undefined;
  }
  const memory = new Uint8Array(kernelExports.memory.buffer);
  const socketTableBytes = kernelExports.kernel_socket_table_bytes();
  const netBufPoolBytes = kernelExports.kernel_net_buf_pool_bytes();
  return {
    socketTable: memory.slice(
      kernelState.socketTablePtr,
      kernelState.socketTablePtr + socketTableBytes,
    ),
    netBufPool: memory.slice(
      kernelState.netBufBase,
      kernelState.netBufBase + netBufPoolBytes,
    ),
    socketUsed: kernelState.socketUsed,
    netBufUsed: kernelState.netBufUsed,
  };
}

function postStatus(status: number, waitInWorker = false): void {
  const kernelExports = requireExports();
  const includeDebug = status !== statusCodes.FS;
  const syncEffects = readCompletedSyncEffects(kernelExports, statusCodes, status, syscallNumbers);
  const kernelState = readKernelRuntimeState(kernelExports);
  const fdEntries = snapshotProcessFdEntries(kernelExports, vmPtr);
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
    kernelState,
    syncEffects,
    fdEntries,
    ofdSlots: snapshotOfdSlotsForFdEntries(kernelExports, kernelState, vmPtr, fdEntries),
    socketState: snapshotSocketState(kernelExports, kernelState),
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
        applyChildExitRecords(msg.childExitRecords);
        applyGuestWriteWatchRegions(msg.guestWriteWatchRegions);
        postReady();
      } catch (error) {
        postError(error);
      }
      break;
    case "step":
      try {
        attachKernelRuntimeState(requireExports(), msg.kernelState);
        applyChildExitRecords(msg.childExitRecords);
        step(msg.budget);
      } catch (error) {
        postError(error);
      }
      break;
  }
});
