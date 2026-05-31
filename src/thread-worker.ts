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
  KernelWorkerPipeSlotSnapshot,
  KernelWorkerSocketStateSnapshot,
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
  createThreadWorkerStatusOutcome,
  publishThreadWorkerStatusOutcome,
  type ThreadWorkerBoundaryOutcome,
} from "./thread-worker/outcomes.js";
import {
  formatThreadWorkerGuestBufferPreview,
  postThreadWorkerStatus,
} from "./thread-worker/status.js";

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
let syscallNumbers: SyscallNumbers | null = null;

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

function applyChildExitRecords(
  records?: Array<{ childPid: number; waitStatus: number; parentPid?: number }>,
): void {
  if (!records || records.length === 0) {
    return;
  }
  const kernelExports = requireExports();
  for (const record of records) {
    if (
      record.parentPid !== undefined
      && record.parentPid > 0
      && kernelExports.process_record_child_exit_for_parent
    ) {
      kernelExports.process_record_child_exit_for_parent(
        record.parentPid,
        record.childPid,
        record.waitStatus,
      );
    } else {
      kernelExports.process_record_child_exit(record.childPid, record.waitStatus);
    }
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

function applyInboundPipeSlots(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
  snapshots?: KernelWorkerPipeSlotSnapshot[],
): void {
  if (!snapshots || snapshots.length === 0) {
    return;
  }
  const maxPipes = kernelExports.abi_max_pipes();
  const slotBytes = kernelExports.kernel_pipe_region_bytes() / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
  }
  const memory = new Uint8Array(kernelExports.memory.buffer);
  for (const snapshot of snapshots) {
    if (
      snapshot.pipeSlot <= 0
      || snapshot.pipeSlot >= maxPipes
      || snapshot.slot.length !== slotBytes
    ) {
      continue;
    }
    memory.set(snapshot.slot, kernelState.pipeBase + snapshot.pipeSlot * slotBytes);
  }
}

function applyGuestMemoryWrites(
  kernelExports: KernelExports,
  writes?: Array<{ guestPtr: number; bytes: Uint8Array }>,
): void {
  if (!writes || writes.length === 0) {
    return;
  }
  const memory = new Uint8Array(kernelExports.memory.buffer);
  for (const write of writes) {
    const start = guestBase + write.guestPtr;
    if (
      !Number.isFinite(start)
      || start < 0
      || start + write.bytes.length > memory.byteLength
    ) {
      continue;
    }
    memory.set(write.bytes, start);
  }
}

function applySocketState(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
  socketState?: KernelWorkerSocketStateSnapshot,
): void {
  if (!socketState) {
    return;
  }
  const memory = new Uint8Array(kernelExports.memory.buffer);
  const socketTableBytes = kernelExports.kernel_socket_table_bytes();
  const netBufPoolBytes = kernelExports.kernel_net_buf_pool_bytes();
  const maxBuffers = Math.max(1, Number(kernelExports.abi_max_sockets?.() ?? 32) * 2);
  const netBufSlotBytes = Math.floor(netBufPoolBytes / maxBuffers);
  if (
    socketState.socketTable.length !== socketTableBytes
    || (socketState.netBufPool !== undefined && socketState.netBufPool.length !== netBufPoolBytes)
  ) {
    return;
  }
  memory.set(socketState.socketTable, kernelState.socketTablePtr);
  if (socketState.netBufPool !== undefined) {
    memory.set(socketState.netBufPool, kernelState.netBufBase);
  } else if (netBufSlotBytes > 0) {
    for (const slot of socketState.netBufSlots ?? []) {
      if (slot.bufferIndex < 0 || slot.bufferIndex >= maxBuffers || slot.bytes.length !== netBufSlotBytes) {
        continue;
      }
      memory.set(slot.bytes, kernelState.netBufBase + slot.bufferIndex * netBufSlotBytes);
    }
  }
  attachKernelRuntimeState(kernelExports, {
    ...kernelState,
    socketUsed: kernelState.socketUsed | socketState.socketUsed,
    netBufUsed: kernelState.netBufUsed | socketState.netBufUsed,
  });
}

function applyOfdSlots(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
  ofdSlots?: KernelWorkerOfdSlotSnapshot[],
): void {
  if (!ofdSlots || ofdSlots.length === 0) {
    return;
  }
  const maxFds = kernelExports.abi_max_fds();
  const tableBytes = kernelExports.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
  }
  const memory = new Uint8Array(kernelExports.memory.buffer);
  for (const snapshot of ofdSlots) {
    if (
      snapshot.ofdId <= 0
      || snapshot.ofdId >= maxFds
      || snapshot.slot.length !== slotBytes
    ) {
      continue;
    }
    memory.set(snapshot.slot, kernelState.ofdTablePtr + snapshot.ofdId * slotBytes);
  }
}

function applyFdEntries(
  kernelExports: KernelExports,
  targetVmPtr: number,
  fdEntries?: KernelWorkerFdEntrySnapshot[],
): void {
  if (!fdEntries || fdEntries.length === 0 || targetVmPtr === 0) {
    return;
  }
  const maxFds = kernelExports.abi_max_fds();
  for (const entry of fdEntries) {
    if (entry.fd < 0 || entry.fd >= maxFds || entry.ofdId < 0 || entry.ofdId >= maxFds) {
      continue;
    }
    kernelExports.process_set_fd_entry(targetVmPtr, entry.fd, entry.ofdId, entry.fdFlags);
  }
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
        applyOfdSlots(requireExports(), msg.kernelState, msg.ofdSlots);
        applyFdEntries(requireExports(), msg.vmPtr, msg.fdEntries);
        applySocketState(requireExports(), msg.kernelState, msg.socketState);
        applyChildExitRecords(msg.childExitRecords);
        applyInboundPipeSlots(requireExports(), msg.kernelState, msg.pipeSlots);
        applyGuestMemoryWrites(requireExports(), msg.guestMemoryWrites);
        applyGuestWriteWatchRegions(msg.guestWriteWatchRegions);
        postReady();
      } catch (error) {
        postError(error);
      }
      break;
    case "step":
      try {
        attachKernelRuntimeState(requireExports(), msg.kernelState);
        applyOfdSlots(requireExports(), msg.kernelState, msg.ofdSlots);
        applyFdEntries(requireExports(), vmPtr, msg.fdEntries);
        applySocketState(requireExports(), msg.kernelState, msg.socketState);
        applyChildExitRecords(msg.childExitRecords);
        applyInboundPipeSlots(requireExports(), msg.kernelState, msg.pipeSlots);
        acquireSharedMemoryForStep(requireExports().memory.buffer);
        if (msg.resumeBlockedSyscall === true) {
          const kernelExports = requireExports();
          applyGuestMemoryWrites(kernelExports, msg.guestMemoryWrites);
          const resumeFd = kernelExports.thread_a0(vmPtr);
          const resumeBuf = kernelExports.thread_a1(vmPtr);
          const resumeCount = kernelExports.thread_reg?.(vmPtr, 12) ?? 0n;
          const blockedSyscallNr = kernelExports.thread_blocked_syscall_nr?.(vmPtr) ?? 0n;
          const blockedArg0 = kernelExports.thread_blocked_syscall_arg?.(vmPtr, 0) ?? 0n;
          const blockedArg1 = kernelExports.thread_blocked_syscall_arg?.(vmPtr, 1) ?? 0n;
          const blockedArg2 = kernelExports.thread_blocked_syscall_arg?.(vmPtr, 2) ?? 0n;
          const resumeResult = kernelExports.thread_resume_blocked_syscall(vmPtr);
          if (PIPE_RESUME_DEBUG_ENABLED) {
            lastResumeBlockedDebug = [
              "thread-resume-blocked",
              `fd=${resumeFd.toString()}`,
              `buf=0x${resumeBuf.toString(16)}`,
              `count=${resumeCount.toString()}`,
              `saved=${blockedSyscallNr.toString()}/${blockedArg0.toString()}/0x${blockedArg1.toString(16)}/${blockedArg2.toString()}`,
              `ret=${resumeResult.toString()}`,
              `status=${kernelExports.thread_status(vmPtr)}`,
              `data="${formatThreadWorkerGuestBufferPreview(
                kernelExports,
                guestBase,
                resumeBuf,
                resumeResult > 0n ? resumeResult : resumeCount,
              )}"`,
            ].join(":");
          }
          postStatusOutcome(createThreadWorkerStatusOutcome(kernelExports.thread_status(vmPtr)));
          return;
        }
        applyGuestMemoryWrites(requireExports(), msg.guestMemoryWrites);
        step(msg.budget);
      } catch (error) {
        postError(error);
      }
      break;
  }
});
