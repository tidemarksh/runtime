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
  GuestMemoryWriteSnapshot,
  GuestWriteWatchRegion,
  ForkStackSnapshot,
  KernelMemoryWriteSnapshot,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPipeSlotSnapshot,
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
  collectFdTraceDebug,
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
const DEFAULT_NODE_THREAD_STEP_BUDGET = 1_000_000;
const DEFAULT_BROWSER_THREAD_STEP_BUDGET = 2_000;
const THREAD_STEP_BUDGET = readThreadStepBudget();
const EINTR_ERRNO = 4;
const FUTEX_WAIT_SLICE_MS = 10;
const SYNC_EFFECT_PIPE_STATE = 1 << 0;
const SYNC_EFFECT_PROCESS_MEMFS = 1 << 7;
const FS_RING_BUFFER_BYTES = 32 + 64 * 64 + 64 * 16;
const FS_OP_WRITE = 2;
const PIPE_SLOT_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_PIPE_SLOT_DEBUG ?? "") === "1";
const PIPE_RESUME_DEBUG_ENABLED = PIPE_SLOT_DEBUG_ENABLED
  || ((globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_PIPE_RESUME_DEBUG ?? "") === "1";
let lastResumeBlockedDebug: string | undefined;
let syscallNumbers: SyscallNumbers | null = null;
const regularFileDataLayouts = new Map<number, { dataOffset: number; dataCap: number }>();

function acquirePipeSlotForSnapshot(buffer: ArrayBufferLike, slotOffset: number): void {
  if (buffer instanceof SharedArrayBuffer && slotOffset % 4 === 0) {
    const i32 = new Int32Array(buffer);
    Atomics.load(i32, slotOffset / 4);
    Atomics.load(i32, slotOffset / 4 + 1);
  }
}

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
  return typeof window === "undefined"
    ? DEFAULT_NODE_THREAD_STEP_BUDGET
    : DEFAULT_BROWSER_THREAD_STEP_BUDGET;
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

function formatGuestBufferPreview(
  kernelExports: KernelExports,
  ptr: bigint | number,
  length: bigint | number,
): string {
  const start = typeof ptr === "bigint" ? Number(ptr) : ptr;
  const requested = typeof length === "bigint" ? Number(length) : length;
  if (!Number.isFinite(start) || !Number.isFinite(requested) || start <= 0 || requested <= 0) {
    return "";
  }
  const absolute = guestBase + start;
  const previewLen = Math.min(requested, 48);
  const memory = new Uint8Array(kernelExports.memory.buffer);
  if (absolute < 0 || absolute + previewLen > memory.byteLength) {
    return "";
  }
  const preview: string[] = [];
  for (let offset = 0; offset < previewLen; offset += 1) {
    const value = memory[absolute + offset];
    preview.push(value >= 0x20 && value <= 0x7e ? String.fromCharCode(value) : ".");
  }
  return preview.join("");
}

function snapshotForkStack(kernelExports: KernelExports, status: number): ForkStackSnapshot | undefined {
  if (
    status !== statusCodes.EXECVE_SPAWN
    || kernelExports.thread_spawn_kind(vmPtr) !== kernelExports.abi_spawn_kind_fork()
  ) {
    return undefined;
  }
  const sp = Number(kernelExports.thread_sp?.(vmPtr) ?? 0n);
  const ramLimit = kernelExports.thread_guest_ram_limit(vmPtr);
  if (!Number.isFinite(sp) || sp <= 0 || sp >= ramLimit) {
    return undefined;
  }
  const startGuest = Math.max(0, Math.floor(sp / 65536) * 65536 - 1024 * 1024);
  const endGuest = ramLimit;
  const memory = new Uint8Array(kernelExports.memory.buffer);
  return {
    startGuest,
    bytes: memory.slice(guestBase + startGuest, guestBase + endGuest),
  };
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

function snapshotExecveProcessState(
  kernelExports: KernelExports,
  status: number,
): Uint8Array | undefined {
  if (status !== statusCodes.EXECVE_SPAWN) {
    return undefined;
  }
  const processPtr = kernelExports.thread_process_ptr(vmPtr);
  if (processPtr === 0) {
    return undefined;
  }
  const processStateBytes = kernelExports.process_state_bytes();
  const start = guestBase + processPtr;
  const end = start + processStateBytes;
  if (end > kernelExports.memory.buffer.byteLength) {
    return undefined;
  }
  return new Uint8Array(kernelExports.memory.buffer, start, processStateBytes).slice();
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

function snapshotCompletedPipeSlots(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
): KernelWorkerPipeSlotSnapshot[] {
  const currentSyscall = readOptionalTraceU64(kernelExports.thread_a7?.(vmPtr));
  const pipe2Syscall = syscallNumbers?.pipe2;
  const completedSyscallIsPipe2 = pipe2Syscall !== null
    && pipe2Syscall !== undefined
    && currentSyscall === BigInt(pipe2Syscall);
  const completedSlots = kernelExports.thread_take_completed_pipe_sync_slots?.(vmPtr) ?? 0;
  const completedMask = typeof completedSlots === "bigint" ? completedSlots : BigInt(completedSlots);
  const readSlots = kernelExports.thread_take_completed_pipe_sync_read_slots?.(vmPtr);
  const writeSlots = kernelExports.thread_take_completed_pipe_sync_write_slots?.(vmPtr);
  const freshSlots = kernelExports.thread_take_completed_pipe_fresh_slots?.(vmPtr);
  const readMask = readSlots === undefined
    ? undefined
    : typeof readSlots === "bigint" ? readSlots : BigInt(readSlots);
  const writeMask = writeSlots === undefined
    ? undefined
    : typeof writeSlots === "bigint" ? writeSlots : BigInt(writeSlots);
  const freshMask = freshSlots === undefined
    ? undefined
    : typeof freshSlots === "bigint" ? freshSlots : BigInt(freshSlots);
  const snapshotMask = completedMask | (freshMask ?? 0n);

  const memory = new Uint8Array(kernelExports.memory.buffer);
  const slotBytes = kernelExports.kernel_pipe_region_bytes() / kernelExports.abi_max_pipes();
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return [];
  }
  const snapshots: KernelWorkerPipeSlotSnapshot[] = [];
  for (let pipeSlot = 1; pipeSlot < Math.min(kernelExports.abi_max_pipes(), 64); pipeSlot += 1) {
    if ((snapshotMask & (1n << BigInt(pipeSlot))) === 0n) {
      continue;
    }
    const offset = kernelState.pipeBase + pipeSlot * slotBytes;
    const bit = 1n << BigInt(pipeSlot);
    const freshSlot = freshMask === undefined
      ? completedSyscallIsPipe2
      : (freshMask & bit) !== 0n;
    const mode = freshSlot
      ? 1 | 2
      : readMask === undefined || writeMask === undefined
      ? undefined
      : ((readMask & bit) !== 0n ? 1 : 0) | ((writeMask & bit) !== 0n ? 2 : 0);
    acquirePipeSlotForSnapshot(kernelExports.memory.buffer, offset);
    snapshots.push({
      pipeSlot,
      slot: memory.slice(offset, offset + slotBytes),
      mode,
      fresh: freshSlot,
    });
  }
  return snapshots;
}

function readOptionalTraceU64(value: bigint | number | undefined): bigint | null {
  if (value === undefined) {
    return null;
  }
  return typeof value === "bigint" ? value : BigInt(value);
}

function readOptionalTraceI64(value: bigint | number | undefined): bigint | null {
  if (value === undefined) {
    return null;
  }
  return BigInt.asIntN(64, typeof value === "bigint" ? value : BigInt(value));
}

function snapshotLatestFdTraceForStatus(
  kernelExports: KernelExports,
  vmPtr: number,
): {
  nr: bigint;
  arg0OfdId?: number;
  arg0OfdType?: number;
  arg1?: bigint;
  arg2OfdId?: number;
  arg2OfdType?: number;
  result?: bigint;
} | null {
  const len = kernelExports.debug_fd_syscall_trace_len?.() ?? 0;
  for (let index = len - 1; index >= 0; index -= 1) {
    const traceVmPtr = kernelExports.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
    if (traceVmPtr !== 0 && traceVmPtr !== vmPtr) {
      continue;
    }
    const nr = readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_nr?.(index));
    if (nr === null) {
      continue;
    }
    return {
      nr,
      arg0OfdId: kernelExports.debug_fd_syscall_trace_arg0_ofd_id?.(index),
      arg0OfdType: kernelExports.debug_fd_syscall_trace_arg0_ofd_type?.(index),
      arg1: readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_arg1?.(index)) ?? undefined,
      arg2OfdId: kernelExports.debug_fd_syscall_trace_arg2_ofd_id?.(index),
      arg2OfdType: kernelExports.debug_fd_syscall_trace_arg2_ofd_type?.(index),
      result: readOptionalTraceI64(kernelExports.debug_fd_syscall_trace_result?.(index)) ?? undefined,
    };
  }
  return null;
}

function snapshotPipeSlot(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
  pipeSlot: number,
  mode?: number,
): KernelWorkerPipeSlotSnapshot | null {
  const maxPipes = kernelExports.abi_max_pipes();
  const slotBytes = kernelExports.kernel_pipe_region_bytes() / maxPipes;
  if (
    pipeSlot <= 0
    || pipeSlot >= maxPipes
    || !Number.isInteger(slotBytes)
    || slotBytes <= 0
  ) {
    return null;
  }
  const memory = new Uint8Array(kernelExports.memory.buffer);
  const offset = kernelState.pipeBase + pipeSlot * slotBytes;
  acquirePipeSlotForSnapshot(kernelExports.memory.buffer, offset);
  return {
    pipeSlot,
    slot: memory.slice(offset, offset + slotBytes),
    mode,
  };
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

function snapshotCompletedPipeReadGuestMemory(
  kernelExports: KernelExports,
  status: number,
  syncEffects: number,
): GuestMemoryWriteSnapshot[] | undefined {
  if (
    status !== statusCodes.OK
    || (syncEffects & SYNC_EFFECT_PIPE_STATE) === 0
    || syscallNumbers?.read === null
    || syscallNumbers?.read === undefined
  ) {
    return undefined;
  }
  if (
    kernelExports.thread_completed_pipe_read_guest_ptr
    && kernelExports.thread_take_completed_pipe_read_len
  ) {
    const guestPtr = Number(readOptionalTraceU64(
      kernelExports.thread_completed_pipe_read_guest_ptr(vmPtr),
    ) ?? 0n);
    const length = kernelExports.thread_take_completed_pipe_read_len(vmPtr);
    const start = guestBase + guestPtr;
    const memory = new Uint8Array(kernelExports.memory.buffer);
    if (
      Number.isFinite(guestPtr)
      && Number.isFinite(length)
      && guestPtr > 0
      && length > 0
      && start >= 0
      && start + length <= memory.byteLength
    ) {
      return [{ guestPtr, bytes: memory.slice(start, start + length) }];
    }
  }
  const len = kernelExports.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return undefined;
  }
  const readSyscall = BigInt(syscallNumbers.read);
  for (let index = len - 1; index >= 0; index -= 1) {
    const traceVmPtr = kernelExports.debug_fd_syscall_trace_vm_ptr?.(index) ?? vmPtr;
    if (traceVmPtr !== vmPtr) {
      continue;
    }
    const traceNr = readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_nr?.(index));
    if (traceNr !== readSyscall) {
      return undefined;
    }
    if ((kernelExports.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0) !== kernelExports.abi_fd_type_pipe()) {
      return undefined;
    }
    const result = readOptionalTraceI64(kernelExports.debug_fd_syscall_trace_result?.(index));
    if (result === null || result <= 0n) {
      return undefined;
    }
    const guestPtr = Number(readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_arg1?.(index)) ?? 0n);
    const requested = Number(readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_arg2?.(index)) ?? 0n);
    const readLen = Number(result);
    const length = requested > 0 ? Math.min(readLen, requested) : readLen;
    const start = guestBase + guestPtr;
    const memory = new Uint8Array(kernelExports.memory.buffer);
    if (
      !Number.isFinite(guestPtr)
      || !Number.isFinite(length)
      || guestPtr <= 0
      || length <= 0
      || start < 0
      || start + length > memory.byteLength
    ) {
      return undefined;
    }
    return [{ guestPtr, bytes: memory.slice(start, start + length) }];
  }
  return undefined;
}

function snapshotAbsoluteMemory(
  kernelExports: KernelExports,
  ptr: number,
  length: number,
): KernelMemoryWriteSnapshot | undefined {
  const memory = new Uint8Array(kernelExports.memory.buffer);
  if (
    !Number.isFinite(ptr)
    || !Number.isFinite(length)
    || ptr <= 0
    || length <= 0
    || ptr + length > memory.byteLength
  ) {
    return undefined;
  }
  return { ptr, bytes: memory.slice(ptr, ptr + length) };
}

function snapshotGuestMemoryWrite(
  kernelExports: KernelExports,
  guestPtr: number,
  length: number,
): GuestMemoryWriteSnapshot | undefined {
  const memory = new Uint8Array(kernelExports.memory.buffer);
  const start = guestBase + guestPtr;
  if (
    !Number.isFinite(guestPtr)
    || !Number.isFinite(length)
    || guestPtr <= 0
    || length <= 0
    || start < 0
    || start + length > memory.byteLength
  ) {
    return undefined;
  }
  return { guestPtr, bytes: memory.slice(start, start + length) };
}

function snapshotGuestCString(
  kernelExports: KernelExports,
  guestPtr: number,
  maxLength = 4096,
): GuestMemoryWriteSnapshot | undefined {
  const memory = new Uint8Array(kernelExports.memory.buffer);
  const start = guestBase + guestPtr;
  if (!Number.isFinite(guestPtr) || guestPtr <= 0 || start < 0 || start >= memory.byteLength) {
    return undefined;
  }
  let length = 0;
  while (length < maxLength && start + length < memory.byteLength) {
    length += 1;
    if (memory[start + length - 1] === 0) {
      break;
    }
  }
  return snapshotGuestMemoryWrite(kernelExports, guestPtr, length);
}

function snapshotPendingFsRequestGuestMemory(kernelExports: KernelExports): GuestMemoryWriteSnapshot[] {
  if (kernelExports.thread_fs_ring_base === undefined) {
    return [];
  }
  const ringBase = kernelExports.thread_fs_ring_base(vmPtr);
  const memory = new Uint8Array(kernelExports.memory.buffer);
  if (ringBase <= 0 || ringBase + FS_RING_BUFFER_BYTES > memory.byteLength) {
    return [];
  }
  const view = new DataView(memory.buffer);
  const submitHead = view.getUint32(ringBase, true);
  const submitTail = view.getUint32(ringBase + 4, true);
  const snapshots: GuestMemoryWriteSnapshot[] = [];
  const count = Math.min(submitHead - submitTail, 64);
  for (let offset = 0; offset < count; offset += 1) {
    const pos = submitTail + offset;
    const entry = ringBase + 32 + (pos % 64) * 64;
    const opcode = view.getUint32(entry, true);
    const len = Number(view.getBigUint64(entry + 16, true));
    const bufPtr = Number(view.getBigUint64(entry + 24, true));
    const pathPtr = Number(view.getBigUint64(entry + 32, true));
    if (opcode === FS_OP_WRITE && len > 0 && bufPtr > 0) {
      const snapshot = snapshotGuestMemoryWrite(kernelExports, bufPtr, Math.min(len, 1 << 20));
      if (snapshot) {
        snapshots.push(snapshot);
      }
    }
    if (pathPtr > 0) {
      const snapshot = snapshotGuestCString(kernelExports, pathPtr);
      if (snapshot) {
        snapshots.push(snapshot);
      }
    }
  }
  return snapshots;
}

function snapshotFsRingMemory(
  kernelExports: KernelExports,
  status: number,
): KernelMemoryWriteSnapshot[] | undefined {
  if (status !== statusCodes.FS || kernelExports.thread_fs_ring_base === undefined) {
    return undefined;
  }
  const ringBase = kernelExports.thread_fs_ring_base(vmPtr);
  const snapshot = snapshotAbsoluteMemory(kernelExports, ringBase, FS_RING_BUFFER_BYTES);
  return snapshot ? [snapshot] : undefined;
}

function snapshotProcessMemfsMemory(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
  mode: "none" | "full" | "regular-write",
): KernelMemoryWriteSnapshot[] {
  if (mode === "none" || kernelState.memfsPtr <= 0) {
    return [];
  }
  const snapshots: KernelMemoryWriteSnapshot[] = [];
  const push = (ptr: number | undefined, length: number | undefined): void => {
    const snapshot = snapshotAbsoluteMemory(kernelExports, ptr ?? 0, length ?? 0);
    if (snapshot) {
      snapshots.push(snapshot);
    }
  };
  const referencedDataUsed = computeReferencedMemfsDataUsed(kernelExports, kernelState);
  const pushHeader = (): void => {
    const snapshot = snapshotAbsoluteMemory(kernelExports, kernelState.memfsPtr, kernelState.memfsHeaderBytes);
    if (!snapshot) {
      return;
    }
    if (snapshot.bytes.length >= 12) {
      new DataView(snapshot.bytes.buffer, snapshot.bytes.byteOffset, snapshot.bytes.byteLength)
        .setUint32(8, referencedDataUsed, true);
    }
    snapshots.push(snapshot);
  };
  if (mode === "regular-write") {
    pushHeader();
    const writeSnapshots = snapshotRegularFileWriteMemfsMemory(kernelExports);
    return writeSnapshots.length > 0
      ? [...snapshots, ...writeSnapshots]
      : snapshotProcessMemfsMemory(kernelExports, kernelState, "full");
  }
  pushHeader();
  push(kernelExports.kernel_memfs_inodes_ptr?.(), kernelState.memfsInodesBytes);
  push(kernelExports.kernel_memfs_first_child_ptr?.(), kernelState.memfsFirstChildBytes);
  push(kernelExports.kernel_memfs_dir_entries_ptr?.(), kernelState.memfsDirEntriesBytes);
  push(kernelExports.kernel_memfs_data_ptr?.(), referencedDataUsed);
  return snapshots;
}

function computeReferencedMemfsDataUsed(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
): number {
  const dataCap = kernelExports.kernel_memfs_data_bytes?.() ?? kernelState.memfsDataBytes;
  let dataUsed = kernelExports.memfs_data_used?.() ?? 0;
  const inodesPtr = kernelExports.kernel_memfs_inodes_ptr?.() ?? 0;
  const maxInodes = kernelState.memfsFirstChildBytes / 4;
  const inodeBytes = maxInodes > 0 ? kernelState.memfsInodesBytes / maxInodes : 0;
  const inodeCount = kernelExports.memfs_inode_count?.() ?? maxInodes;
  if (
    inodesPtr <= 0
    || maxInodes <= 0
    || inodeBytes < 32
    || inodeCount <= 0
  ) {
    return Math.min(Math.max(dataUsed, 0), dataCap);
  }

  const memory = kernelExports.memory.buffer;
  const inodeView = new DataView(memory);
  const inodeLimit = Math.min(inodeCount, maxInodes);
  for (let ino = 1; ino < inodeLimit; ino += 1) {
    const inodeBase = inodesPtr + ino * inodeBytes;
    if (inodeBase < 0 || inodeBase + inodeBytes > memory.byteLength) {
      continue;
    }
    const mode = inodeView.getUint32(inodeBase, true);
    if ((mode & 0o170000) !== 0o100000) {
      continue;
    }
    const size = Number(inodeView.getBigUint64(inodeBase + 16, true));
    const dataOffset = inodeView.getUint32(inodeBase + 24, true);
    const dataFileCap = inodeView.getUint32(inodeBase + 28, true);
    if (size <= 0 || dataFileCap === 0) {
      continue;
    }
    dataUsed = Math.max(dataUsed, dataOffset + Math.min(size, dataFileCap));
  }
  return Math.min(Math.max(dataUsed, 0), dataCap);
}

function latestCompletedRegularFileWriteTrace(
  kernelExports: KernelExports,
): { ofdId: number; ino: number; startOffset: number; endOffset: number; writeLen: number } | null {
  if (syscallNumbers === null) {
    return null;
  }
  const writeSyscalls = new Set(
    [syscallNumbers.write, syscallNumbers.pwrite64, syscallNumbers.writev]
      .filter((nr): nr is bigint => nr !== null),
  );
  if (writeSyscalls.size === 0) {
    return null;
  }
  const len = kernelExports.debug_fd_syscall_trace_len?.() ?? 0;
  for (let index = len - 1; index >= 0; index -= 1) {
    if ((kernelExports.debug_fd_syscall_trace_vm_ptr?.(index) ?? vmPtr) !== vmPtr) {
      continue;
    }
    const nr = readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_nr?.(index));
    if (nr === null || !writeSyscalls.has(nr)) {
      continue;
    }
    if ((kernelExports.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0) !== kernelExports.abi_fd_type_file()) {
      continue;
    }
    const result = readOptionalTraceI64(kernelExports.debug_fd_syscall_trace_result?.(index));
    if (result !== null && result > 0n) {
      const ofdId = kernelExports.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0;
      const ino = kernelExports.debug_fd_syscall_trace_arg0_ofd_ino?.(index)
        ?? kernelExports.kernel_ofd_ino?.(ofdId)
        ?? 0;
      const writeLen = Number(result);
      if (!Number.isFinite(writeLen) || ofdId <= 0 || ino <= 0) {
        return null;
      }
      const nr = readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_nr?.(index));
      const pwriteOffset = nr !== null && nr === syscallNumbers.pwrite64
        ? Number(readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_arg3?.(index)) ?? -1n)
        : -1;
      const endOffset = Number(kernelExports.debug_fd_syscall_trace_arg0_ofd_offset?.(index)
        ?? kernelExports.kernel_ofd_offset?.(ofdId)
        ?? 0n);
      const startOffset = pwriteOffset >= 0 ? pwriteOffset : endOffset - writeLen;
      if (
        !Number.isFinite(startOffset)
        || !Number.isFinite(endOffset)
        || startOffset < 0
      ) {
        return null;
      }
      return {
        ofdId,
        ino,
        startOffset,
        endOffset: pwriteOffset >= 0 ? startOffset + writeLen : endOffset,
        writeLen,
      };
    }
  }
  return null;
}

function snapshotRegularFileWriteMemfsMemory(
  kernelExports: KernelExports,
): KernelMemoryWriteSnapshot[] {
  const trace = latestCompletedRegularFileWriteTrace(kernelExports);
  if (!trace) {
    return [];
  }
  const ino = trace.ino;
  const inodesPtr = kernelExports.kernel_memfs_inodes_ptr?.() ?? 0;
  const dataPtr = kernelExports.kernel_memfs_data_ptr?.() ?? 0;
  const inodeBytes = 32;
  const inodePtr = inodesPtr + ino * inodeBytes;
  const memory = new Uint8Array(kernelExports.memory.buffer);
  if (
    ino <= 0
    || inodesPtr <= 0
    || dataPtr <= 0
    || inodePtr < 0
    || inodePtr + inodeBytes > memory.byteLength
    || !Number.isFinite(trace.startOffset)
    || !Number.isFinite(trace.endOffset)
    || trace.endOffset <= 0
  ) {
    return [];
  }
  const inodeView = new DataView(memory.buffer, inodePtr, inodeBytes);
  const fileSize = Number(inodeView.getBigUint64(16, true));
  const dataOffset = inodeView.getUint32(24, true);
  const dataCap = inodeView.getUint32(28, true);
  const previousLayout = regularFileDataLayouts.get(ino);
  const layoutChanged = previousLayout === undefined
    || previousLayout.dataOffset !== dataOffset
    || previousLayout.dataCap !== dataCap;
  regularFileDataLayouts.set(ino, { dataOffset, dataCap });

  const snapshotStart = layoutChanged ? 0 : trace.startOffset;
  const snapshotEnd = Math.min(
    fileSize,
    layoutChanged ? trace.endOffset : trace.startOffset + trace.writeLen,
  );
  const snapshotLen = Math.max(0, snapshotEnd - snapshotStart);
  const dataStart = dataPtr + dataOffset + snapshotStart;
  const snapshots: KernelMemoryWriteSnapshot[] = [];
  const inodeSnapshot = snapshotAbsoluteMemory(kernelExports, inodePtr, inodeBytes);
  if (inodeSnapshot) {
    snapshots.push(inodeSnapshot);
  }
  const dataSnapshot = snapshotAbsoluteMemory(kernelExports, dataStart, snapshotLen);
  if (dataSnapshot) {
    snapshots.push(dataSnapshot);
  }
  return snapshots;
}

function shouldSnapshotProcessMemfsMemory(
  status: number,
  syncEffects: number,
  kernelExports: KernelExports,
): "none" | "full" | "regular-write" {
  if (status === statusCodes.FAULT) {
    return "full";
  }
  if (status === statusCodes.EXECVE_SPAWN) {
    return "full";
  }
  if (latestCompletedRegularFileWriteTrace(kernelExports)) {
    return "regular-write";
  }
  return (syncEffects & SYNC_EFFECT_PROCESS_MEMFS) !== 0
    ? "full"
    : "none";
}

function formatOwnerDebug(kernelExports: KernelExports): string | undefined {
  const processPtr = kernelExports.thread_process_ptr?.(vmPtr) ?? 0;
  if (guestBase <= 0 || processPtr <= 0) {
    return undefined;
  }
  const fdTableBytes = kernelExports.abi_max_fds() * 8;
  const threadCountAddr = processPtr + fdTableBytes;
  const reservationGenAddr = threadCountAddr + 4;
  const reservationLockAddr = reservationGenAddr + 64 * 4;
  const pidAddr = reservationLockAddr + 64 * 4;
  const ppidAddr = pidAddr + 4;
  const absolutePidAddr = guestBase + pidAddr;
  const absolutePpidAddr = guestBase + ppidAddr;
  const view = new DataView(kernelExports.memory.buffer);
  if (absolutePidAddr < 0 || absolutePpidAddr + 4 > view.byteLength) {
    return undefined;
  }
  return [
    `process_ptr=0x${processPtr.toString(16)}`,
    `process_pid_field=${view.getInt32(absolutePidAddr, true)}`,
    `process_ppid_field=${view.getInt32(absolutePpidAddr, true)}`,
  ].join(" ");
}

function formatLatestPipeReadDebug(
  kernelExports: KernelExports,
  status: number,
): string | undefined {
  if (
    !PIPE_SLOT_DEBUG_ENABLED
    || status !== statusCodes.OK
    || syscallNumbers?.read === null
    || syscallNumbers?.read === undefined
  ) {
    return undefined;
  }
  const len = kernelExports.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return undefined;
  }
  const readSyscall = BigInt(syscallNumbers.read);
  for (let index = len - 1; index >= 0; index -= 1) {
    const traceVmPtr = kernelExports.debug_fd_syscall_trace_vm_ptr?.(index) ?? vmPtr;
    if (traceVmPtr !== vmPtr) {
      continue;
    }
    const traceNr = readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_nr?.(index));
    if (traceNr !== readSyscall) {
      return undefined;
    }
    if ((kernelExports.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0) !== kernelExports.abi_fd_type_pipe()) {
      return undefined;
    }
    const result = readOptionalTraceI64(kernelExports.debug_fd_syscall_trace_result?.(index));
    if (result === null || result <= 0n) {
      return undefined;
    }
    const fd = readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_arg0?.(index)) ?? 0n;
    const guestPtr = readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_arg1?.(index)) ?? 0n;
    const requested = readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_arg2?.(index)) ?? 0n;
    return [
      "pipe-read-complete",
      `fd=${fd.toString()}`,
      `buf=0x${guestPtr.toString(16)}`,
      `count=${requested.toString()}`,
      `ret=${result.toString()}`,
      `data="${formatGuestBufferPreview(kernelExports, guestPtr, result)}"`,
    ].join(":");
  }
  return undefined;
}

function snapshotPipe2FallbackPipeSlots(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
): KernelWorkerPipeSlotSnapshot[] {
  const pipe2Syscall = syscallNumbers?.pipe2;
  if (pipe2Syscall === null || pipe2Syscall === undefined) {
    return [];
  }
  const len = kernelExports.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0 || readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_nr?.(len - 1)) !== BigInt(pipe2Syscall)) {
    return [];
  }
  const snapshots = new Map<number, KernelWorkerPipeSlotSnapshot>();
  for (let index = len - 1; index >= 0; index -= 1) {
    const traceNr = readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_nr?.(index));
    if (traceNr !== BigInt(pipe2Syscall)) {
      continue;
    }
    for (const ofdId of [
      kernelExports.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0,
      kernelExports.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? 0,
    ]) {
      if (
        ofdId <= 0
        || ofdId >= kernelExports.abi_max_fds()
        || kernelExports.kernel_ofd_fd_type(ofdId) !== kernelExports.abi_fd_type_pipe()
      ) {
        continue;
      }
      const pipeSlot = kernelExports.kernel_ofd_ino(ofdId);
      if (snapshots.has(pipeSlot)) {
        continue;
      }
      const snapshot = snapshotPipeSlot(kernelExports, kernelState, pipeSlot, 1 | 2);
      if (snapshot) {
        snapshot.fresh = true;
        snapshots.set(pipeSlot, snapshot);
      }
    }
    break;
  }
  return [...snapshots.values()];
}

function snapshotNonEmptyProcessPipeSlots(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
  fdEntries: KernelWorkerFdEntrySnapshot[],
): KernelWorkerPipeSlotSnapshot[] {
  const pipeSlotModes = new Map<number, number>();
  for (const entry of fdEntries) {
    const ofdId = entry.ofdId;
    if (
      ofdId <= 0
      || ofdId >= kernelExports.abi_max_fds()
      || kernelExports.kernel_ofd_fd_type(ofdId) !== kernelExports.abi_fd_type_pipe()
    ) {
      continue;
    }
    const pipeSlot = kernelExports.kernel_ofd_ino(ofdId);
    if (pipeSlot <= 0 || pipeSlot >= kernelExports.abi_max_pipes()) {
      continue;
    }
    const mode = kernelExports.kernel_ofd_host_fd(ofdId) >= 0 ? 1 : 2;
    pipeSlotModes.set(pipeSlot, (pipeSlotModes.get(pipeSlot) ?? 0) | mode);
  }

  const maxPipes = kernelExports.abi_max_pipes();
  const slotBytes = kernelExports.kernel_pipe_region_bytes() / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return [];
  }
  const view = new DataView(kernelExports.memory.buffer);
  const snapshots: KernelWorkerPipeSlotSnapshot[] = [];
  for (const [pipeSlot, mode] of pipeSlotModes) {
    const offset = kernelState.pipeBase + pipeSlot * slotBytes;
    const head = view.getUint32(offset, true);
    const tail = view.getUint32(offset + 4, true);
    if (head === tail) {
      continue;
    }
    const snapshot = snapshotPipeSlot(kernelExports, kernelState, pipeSlot, mode);
    if (snapshot) {
      snapshots.push(snapshot);
    }
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
  if (
    socketState.socketTable.length !== socketTableBytes
    || socketState.netBufPool.length !== netBufPoolBytes
  ) {
    return;
  }
  memory.set(socketState.socketTable, kernelState.socketTablePtr);
  memory.set(socketState.netBufPool, kernelState.netBufBase);
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

function formatPipeSlotDebug(pipeSlots: KernelWorkerPipeSlotSnapshot[] | undefined): string | undefined {
  if (!PIPE_SLOT_DEBUG_ENABLED || pipeSlots === undefined) {
    return undefined;
  }
  if (pipeSlots.length === 0) {
    return "pipe_slots=[]";
  }
  const entries = pipeSlots.map((snapshot) => {
    const view = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
    const data = snapshot.slot.subarray(32, 56);
    const ascii = [...data]
      .map((byte) => byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : ".")
      .join("");
    return [
      `slot=${snapshot.pipeSlot}`,
      `mode=${snapshot.mode ?? -1}`,
      `head=${view.getUint32(0, true)}`,
      `tail=${view.getUint32(4, true)}`,
      `ro=${view.getInt32(8, true)}`,
      `wo=${view.getInt32(12, true)}`,
      `gen=${view.getUint32(20, true)}`,
      `fresh=${snapshot.fresh ? 1 : 0}`,
      `data=${JSON.stringify(ascii)}`,
    ].join("/");
  });
  return `pipe_slots=[${entries.join(",")}]`;
}

function postStatus(status: number, waitInWorker = false): void {
  const kernelExports = requireExports();
  const includeDebug = status !== statusCodes.FS;
  const syncEffects = readCompletedSyncEffects(kernelExports, statusCodes, status, syscallNumbers, vmPtr);
  const kernelState = readKernelRuntimeState(kernelExports);
  const fdEntries = snapshotProcessFdEntries(kernelExports, vmPtr);
  const shouldSnapshotPipeSlots = status === statusCodes.FAULT
    || (syncEffects & SYNC_EFFECT_PIPE_STATE) !== 0;
  const pipeSlots = shouldSnapshotPipeSlots
    ? (() => {
        const completed = snapshotCompletedPipeSlots(kernelExports, kernelState);
        if (completed.length > 0) {
          return completed;
        }
        const processSlots = [
          ...snapshotNonEmptyProcessPipeSlots(kernelExports, kernelState, fdEntries),
          ...snapshotPipe2FallbackPipeSlots(kernelExports, kernelState),
        ];
        return processSlots;
      })()
    : undefined;
  const guestMemoryWrites = [
    ...(snapshotCompletedPipeReadGuestMemory(kernelExports, status, syncEffects) ?? []),
    ...snapshotPendingFsRequestGuestMemory(kernelExports),
  ];
  const memfsSnapshotMode = shouldSnapshotProcessMemfsMemory(status, syncEffects, kernelExports);
  const kernelMemoryWrites = [
    ...(snapshotFsRingMemory(kernelExports, status) ?? []),
    ...snapshotProcessMemfsMemory(
      kernelExports,
      kernelState,
      memfsSnapshotMode,
    ),
  ];
  const ofdSlots = snapshotOfdSlotsForFdEntries(kernelExports, kernelState, vmPtr, fdEntries);
  const latestFdTrace = snapshotLatestFdTraceForStatus(kernelExports, vmPtr);
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
    syscallNr: kernelExports.thread_a7?.(vmPtr),
    vmPtr,
    kernelState,
    syncEffects,
    fdEntries,
    ofdSlots,
    processState: snapshotExecveProcessState(kernelExports, status),
    pipeSlots,
    socketState: snapshotSocketState(kernelExports, kernelState),
    guestMemoryWrites: guestMemoryWrites.length > 0 ? guestMemoryWrites : undefined,
    kernelMemoryWrites: kernelMemoryWrites.length > 0 ? kernelMemoryWrites : undefined,
    fdTraceDebug: collectFdTraceDebug(kernelExports, vmPtr, guestBase),
    fdTraceSyscallNr: latestFdTrace?.nr,
    fdTraceArg0OfdId: latestFdTrace?.arg0OfdId,
    fdTraceArg0OfdType: latestFdTrace?.arg0OfdType,
    fdTraceArg1: latestFdTrace?.arg1,
    fdTraceArg2OfdId: latestFdTrace?.arg2OfdId,
    fdTraceArg2OfdType: latestFdTrace?.arg2OfdType,
    fdTraceResult: latestFdTrace?.result,
    forkStackSnapshot: snapshotForkStack(kernelExports, status),
    debug: includeDebug ? (() => {
      const parts = collectDebugParts(kernelExports, vmPtr, guestBase);
      const pipeSlotDebug = formatPipeSlotDebug(pipeSlots);
      if (pipeSlotDebug) {
        parts.push(pipeSlotDebug);
      }
      if (lastResumeBlockedDebug) {
        parts.push(lastResumeBlockedDebug);
        lastResumeBlockedDebug = undefined;
      }
      const pipeReadDebug = formatLatestPipeReadDebug(kernelExports, status);
      if (pipeReadDebug) {
        parts.push(pipeReadDebug);
      }
      return parts.length === 0 ? undefined : parts.join(" ");
    })() : formatOwnerDebug(kernelExports),
  };
  if (waitInWorker) {
    msg.waitInWorker = true;
  }
  if (runtimeBridge !== null) {
    Atomics.add(new Int32Array(runtimeBridge.notifySab), 0, 1);
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
            Math.max(msg.kernelState.heapBase, msg.preserveMemoryBytes ?? 0),
            msg.runtimeNotify,
            msg.runtimeState,
            msg.kernelStackSlot,
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
              `data="${formatGuestBufferPreview(
                kernelExports,
                resumeBuf,
                resumeResult > 0n ? resumeResult : resumeCount,
              )}"`,
            ].join(":");
          }
          postStatus(kernelExports.thread_status(vmPtr));
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
