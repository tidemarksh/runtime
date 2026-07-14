import type { KernelExports, KernelRuntimeState, StatusCodes } from "../abi.js";
import {
  readKernelRuntimeState,
  supervisorOfdFdType,
  supervisorOfdHostFd,
  supervisorOfdIno,
  supervisorProcessFdFlagsForFd,
  supervisorProcessOfdForFd,
} from "../abi.js";
import { kernelNetBufferCount } from "../kernel-socket-layout.js";
import type {
  GuestMemoryWriteSnapshot,
  KernelMemoryWriteSnapshot,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPipeSlotSnapshot,
  KernelWorkerSocketStateSnapshot,
  ThreadWorkerOutbound,
} from "../messages.js";
import { publishRuntimeBridgeDebugStage, type RuntimeStateBridge } from "../bridge/runtime-state.js";
import { workerPostMessage } from "../bridge/scope.js";
import { ensureMemoryCapacity, kernelScratchBase } from "../worker/kernel-scratch.js";
import { collectDebugParts, collectFdTraceDebug } from "./debug.js";
import { consumeThreadWorkerInboundGuestWriteDebug } from "./inbound-state.js";
import { readCompletedSyncEffects } from "./sync-effects.js";
import {
  FS_OP_WRITE,
  FS_RING_REQUEST_BUF_PTR_OFFSET,
  FS_RING_REQUEST_LEN_OFFSET,
  FS_RING_REQUEST_OPCODE_OFFSET,
  FS_RING_REQUEST_PATH_PTR_OFFSET,
  FS_RING_SUBMIT_HEAD_OFFSET,
  FS_RING_SUBMIT_QUEUE_SIZE,
  FS_RING_SUBMIT_TAIL_OFFSET,
  FS_RING_TOTAL_BYTES,
  fsRingRequestEntryOffset,
} from "../layout/fs-ring.js";
import {
  OFD_OFFSET_OFFSET,
  OFD_TYPE_OFFSET,
} from "../layout/ofd.js";

const SYNC_EFFECT_PIPE_STATE = 1 << 0;
const SYNC_EFFECT_PROCESS_MEMFS = 1 << 7;
const SYS_READ = 63n;
const SYS_WRITE = 64n;
const SYS_READV = 65n;
const SYS_WRITEV = 66n;
const PIPE_SLOT_MODE_READ = 1;
const PIPE_SLOT_MODE_WRITE = 2;
const PIPE_SLOT_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_PIPE_SLOT_DEBUG ?? "") === "1";
const THREAD_WORKER_STAGE_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_THREAD_WORKER_STAGE_DEBUG ?? "") === "1";
const FD_CONTROL_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FD_CONTROL_DEBUG ?? "") === "1";
const THREAD_WORKER_STATUS_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_THREAD_WORKER_STATUS_DEBUG ?? "") === "1";
const SUPERVISOR_FD_CONTROL_SCRATCH_OFFSET = 0x31000;

let exports: KernelExports | undefined;
let statusCodes: StatusCodes;
let vmPtr = 0;
let guestBase = 0;
let generation = 0;
let runtimeBridge: RuntimeStateBridge | null = null;
let lastResumeBlockedDebug: string | undefined;
let lastFutexWaitDebug: string | undefined;
let lastSupervisorFdControlSeq = 0n;
interface LatestFdTraceSnapshot {
  nr: bigint;
  arg0OfdId?: number;
  arg0OfdType?: number;
  arg0OfdOffset?: bigint;
  arg1?: bigint;
  arg2OfdId?: number;
  arg2OfdType?: number;
  result?: bigint;
}

export interface ThreadWorkerStatusContext {
  kernelExports: KernelExports;
  statusCodes: StatusCodes;
  vmPtr: number;
  guestBase: number;
  generation: number;
  runtimeBridge: RuntimeStateBridge | null;
  lastResumeBlockedDebug: string | undefined;
  lastFutexWaitDebug: string | undefined;
  setLastResumeBlockedDebug(value: string | undefined): void;
  setLastFutexWaitDebug(value: string | undefined): void;
}

function requireExports(): KernelExports {
  if (!exports) {
    throw new Error("thread worker kernel exports are not initialized");
  }
  return exports;
}

function acquirePipeSlotForSnapshot(buffer: ArrayBufferLike, slotOffset: number): void {
  if (buffer instanceof SharedArrayBuffer && slotOffset % 4 === 0) {
    const i32 = new Int32Array(buffer);
    Atomics.load(i32, slotOffset / 4);
    Atomics.load(i32, slotOffset / 4 + 1);
  }
}

function formatGuestBufferPreviewAtBase(
  kernelExports: KernelExports,
  base: number,
  ptr: bigint | number,
  length: bigint | number,
): string {
  const start = typeof ptr === "bigint" ? Number(ptr) : ptr;
  const requested = typeof length === "bigint" ? Number(length) : length;
  if (!Number.isFinite(start) || !Number.isFinite(requested) || start <= 0 || requested <= 0) {
    return "";
  }
  const absolute = base + start;
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

export function formatThreadWorkerGuestBufferPreview(
  kernelExports: KernelExports,
  threadGuestBase: number,
  ptr: bigint | number,
  length: bigint | number,
): string {
  return formatGuestBufferPreviewAtBase(kernelExports, threadGuestBase, ptr, length);
}

function formatGuestBufferPreview(
  kernelExports: KernelExports,
  ptr: bigint | number,
  length: bigint | number,
): string {
  return formatGuestBufferPreviewAtBase(kernelExports, guestBase, ptr, length);
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
      ofdId: supervisorProcessOfdForFd(kernelExports, processVmPtr, fd),
      fdFlags: supervisorProcessFdFlagsForFd(kernelExports, processVmPtr, fd),
      generation: kernelExports.process_fd_generation?.(processVmPtr, fd),
    });
  }
  return fdEntries;
}

function formatFocusedFdEntries(fdEntries: readonly KernelWorkerFdEntrySnapshot[]): string {
  return [3, 4, 5].map((fd) => {
    const entry = fdEntries.find((candidate) => candidate.fd === fd);
    return entry === undefined
      ? `${fd}=missing`
      : `${fd}=${entry.ofdId}/fl${entry.fdFlags}/gen${entry.generation ?? "n/a"}`;
  }).join(",");
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
  const completedSlots = kernelExports.thread_take_completed_pipe_sync_slots?.(vmPtr) ?? 0;
  const completedMask = typeof completedSlots === "bigint" ? completedSlots : BigInt(completedSlots);
  const readSlots = kernelExports.thread_take_completed_pipe_sync_read_slots?.(vmPtr);
  const writeSlots = kernelExports.thread_take_completed_pipe_sync_write_slots?.(vmPtr);
  const controlSlots = kernelExports.thread_take_completed_pipe_sync_control_slots?.(vmPtr);
  const freshSlots = kernelExports.thread_take_completed_pipe_fresh_slots?.(vmPtr);
  const readMask = readSlots === undefined
    ? undefined
    : typeof readSlots === "bigint" ? readSlots : BigInt(readSlots);
  const writeMask = writeSlots === undefined
    ? undefined
    : typeof writeSlots === "bigint" ? writeSlots : BigInt(writeSlots);
  const controlMask = controlSlots === undefined
    ? undefined
    : typeof controlSlots === "bigint" ? controlSlots : BigInt(controlSlots);
  const freshMask = freshSlots === undefined
    ? undefined
    : typeof freshSlots === "bigint" ? freshSlots : BigInt(freshSlots);
  const snapshotMask = completedMask | (controlMask ?? 0n) | (freshMask ?? 0n);

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
      ? false
      : (freshMask & bit) !== 0n;
    const mode = freshSlot
      ? 1 | 2
      : readMask === undefined || writeMask === undefined
      ? undefined
      : ((readMask & bit) !== 0n ? 1 : 0)
        | ((writeMask & bit) !== 0n ? 2 : 0);
    acquirePipeSlotForSnapshot(kernelExports.memory.buffer, offset);
    snapshots.push({
      pipeSlot,
      slot: memory.slice(offset, offset + slotBytes),
      mode,
      fresh: freshSlot,
      controlChanged: controlMask !== undefined && (controlMask & bit) !== 0n,
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

function collectSupervisorFdControlDebug(kernelExports: KernelExports): string | undefined {
  if (!FD_CONTROL_DEBUG_ENABLED) {
    return undefined;
  }
  const count = kernelExports.supervisor_shadow_fd_control_count?.();
  if (count === undefined || BigInt(count) === 0n) {
    return undefined;
  }
  const readRecord = kernelExports.supervisor_fd_control_record;
  const recordBytes = kernelExports.supervisor_fd_control_record_bytes?.();
  if (!readRecord || recordBytes === undefined || recordBytes <= 0) {
    return undefined;
  }
  const scratch = kernelScratchBase(kernelExports) + SUPERVISOR_FD_CONTROL_SCRATCH_OFFSET;
  ensureMemoryCapacity(kernelExports.memory, scratch + recordBytes);
  const result = readRecord.call(kernelExports, scratch, recordBytes);
  if (result < 0 || result < recordBytes) {
    return `supervisor-fd-control:source=thread-status:error=read:${result}`;
  }
  const view = new DataView(kernelExports.memory.buffer, scratch, result);
  const expectedMagic = kernelExports.supervisor_fd_control_record_magic?.();
  const magic = view.getUint32(0, true);
  if (expectedMagic !== undefined && magic !== expectedMagic) {
    return `supervisor-fd-control:source=thread-status:error=magic:0x${magic.toString(16)}`;
  }
  const seq = view.getBigUint64(16, true);
  if (seq <= lastSupervisorFdControlSeq) {
    return undefined;
  }
  lastSupervisorFdControlSeq = seq;
  return [
    "supervisor-fd-control",
    "source=thread-status",
    `kind=${view.getUint32(12, true)}`,
    `seq=${seq.toString()}`,
    `vm=0x${view.getUint32(24, true).toString(16)}`,
    `process=0x${view.getUint32(28, true).toString(16)}`,
    `pid=${view.getUint32(32, true)}`,
    `fd=${view.getUint32(36, true)}`,
    `fd2=${view.getUint32(40, true)}`,
    `ofd=${view.getUint32(44, true)}`,
    `ofd2=${view.getUint32(48, true)}`,
    `type=${view.getUint32(52, true)}`,
    `slot=${view.getUint32(56, true)}`,
    `generation=${view.getUint32(60, true)}`,
    `result=${view.getBigInt64(64, true).toString()}`,
    `flags=0x${view.getBigUint64(72, true).toString(16)}`,
    `detail0=${view.getBigUint64(80, true).toString()}`,
    `detail1=${view.getBigUint64(88, true).toString()}`,
  ].join(":");
}

function snapshotLatestFdTraceForStatus(
  kernelExports: KernelExports,
  vmPtr: number,
): LatestFdTraceSnapshot | null {
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
      arg0OfdOffset: readOptionalTraceI64(kernelExports.debug_fd_syscall_trace_arg0_ofd_offset?.(index)) ?? undefined,
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

function pipeSlotHasBufferedBytes(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
  pipeSlot: number,
): boolean {
  const maxPipes = kernelExports.abi_max_pipes();
  const slotBytes = kernelExports.kernel_pipe_region_bytes() / maxPipes;
  if (
    pipeSlot <= 0
    || pipeSlot >= maxPipes
    || !Number.isInteger(slotBytes)
    || slotBytes <= 0
  ) {
    return false;
  }
  const offset = kernelState.pipeBase + pipeSlot * slotBytes;
  acquirePipeSlotForSnapshot(kernelExports.memory.buffer, offset);
  const view = new DataView(kernelExports.memory.buffer);
  return view.getUint32(offset, true) !== view.getUint32(offset + 4, true);
}

function fdTracePipeSlotMode(nr: bigint): number {
  if (nr === SYS_READ || nr === SYS_READV) {
    return PIPE_SLOT_MODE_READ;
  }
  if (nr === SYS_WRITE || nr === SYS_WRITEV) {
    return PIPE_SLOT_MODE_WRITE;
  }
  return 0;
}

function patchEventfdOfdSlotsFromTrace(
  kernelExports: KernelExports,
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[],
  latestFdTrace: LatestFdTraceSnapshot | null,
): void {
  const eventfdType = kernelExports.abi_fd_type_eventfd?.();
  if (
    eventfdType === undefined
    || latestFdTrace === null
    || (
      latestFdTrace.nr !== SYS_READ
      && latestFdTrace.nr !== SYS_WRITE
      && latestFdTrace.nr !== SYS_READV
      && latestFdTrace.nr !== SYS_WRITEV
    )
    || latestFdTrace.arg0OfdType !== eventfdType
    || latestFdTrace.arg0OfdId === undefined
    || latestFdTrace.arg0OfdOffset === undefined
    || latestFdTrace.result === undefined
    || latestFdTrace.result < 0n
  ) {
    return;
  }

  for (const snapshot of ofdSlots) {
    if (
      snapshot.ofdId !== latestFdTrace.arg0OfdId
      || snapshot.slot.byteLength < OFD_OFFSET_OFFSET + 8
    ) {
      continue;
    }
    const view = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
    if (view.getInt32(OFD_TYPE_OFFSET, true) !== eventfdType) {
      return;
    }
    view.setBigInt64(OFD_OFFSET_OFFSET, latestFdTrace.arg0OfdOffset, true);
    return;
  }
}

function snapshotTracedPipeSlots(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
  processVmPtr: number,
  existingSnapshots: readonly KernelWorkerPipeSlotSnapshot[],
): KernelWorkerPipeSlotSnapshot[] {
  const len = kernelExports.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0 || !kernelExports.debug_fd_syscall_trace_arg0_ofd_ino) {
    return [];
  }
  const existingSlots = new Set(existingSnapshots.map((snapshot) => snapshot.pipeSlot));
  const pipeSlotModes = new Map<number, number>();
  for (let index = len - 1; index >= 0; index -= 1) {
    const traceVmPtr = kernelExports.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
    if (traceVmPtr !== 0 && traceVmPtr !== processVmPtr) {
      continue;
    }
    const nr = readOptionalTraceU64(kernelExports.debug_fd_syscall_trace_nr?.(index));
    if (nr === null) {
      continue;
    }
    const mode = fdTracePipeSlotMode(nr);
    if (mode === 0) {
      continue;
    }
    if ((kernelExports.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0) !== kernelExports.abi_fd_type_pipe()) {
      continue;
    }
    const result = readOptionalTraceI64(kernelExports.debug_fd_syscall_trace_result?.(index));
    if (result === null || result <= 0n) {
      continue;
    }
    const pipeSlot = kernelExports.debug_fd_syscall_trace_arg0_ofd_ino(index);
    if (pipeSlot <= 0 || existingSlots.has(pipeSlot)) {
      continue;
    }
    pipeSlotModes.set(pipeSlot, (pipeSlotModes.get(pipeSlot) ?? 0) | mode);
  }
  const snapshots: KernelWorkerPipeSlotSnapshot[] = [];
  for (const [pipeSlot, mode] of pipeSlotModes) {
    if (!pipeSlotHasBufferedBytes(kernelExports, kernelState, pipeSlot)) {
      continue;
    }
    const snapshot = snapshotPipeSlot(kernelExports, kernelState, pipeSlot, mode);
    if (snapshot) {
      snapshots.push(snapshot);
    }
  }
  return snapshots;
}

function snapshotCompletedReadGuestMemory(
  kernelExports: KernelExports,
  status: number,
): GuestMemoryWriteSnapshot[] | undefined {
  if (status !== statusCodes.OK) {
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
  if (ringBase <= 0 || ringBase + FS_RING_TOTAL_BYTES > memory.byteLength) {
    return [];
  }
  const view = new DataView(memory.buffer);
  const submitHead = view.getUint32(ringBase + FS_RING_SUBMIT_HEAD_OFFSET, true);
  const submitTail = view.getUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, true);
  const snapshots: GuestMemoryWriteSnapshot[] = [];
  const count = Math.min(submitHead - submitTail, FS_RING_SUBMIT_QUEUE_SIZE);
  for (let offset = 0; offset < count; offset += 1) {
    const pos = submitTail + offset;
    const entry = fsRingRequestEntryOffset(ringBase, pos);
    const opcode = view.getUint32(entry + FS_RING_REQUEST_OPCODE_OFFSET, true);
    const len = Number(view.getBigUint64(entry + FS_RING_REQUEST_LEN_OFFSET, true));
    const bufPtr = Number(view.getBigUint64(entry + FS_RING_REQUEST_BUF_PTR_OFFSET, true));
    const pathPtr = Number(view.getBigUint64(entry + FS_RING_REQUEST_PATH_PTR_OFFSET, true));
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
  const snapshot = snapshotAbsoluteMemory(kernelExports, ringBase, FS_RING_TOTAL_BYTES);
  return snapshot ? [snapshot] : undefined;
}

function snapshotProcessMemfsMemory(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
  mode: "none" | "full",
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

function shouldSnapshotProcessMemfsMemory(
  status: number,
  syncEffects: number,
): "none" | "full" {
  if (status === statusCodes.FAULT) {
    return "full";
  }
  if (status === statusCodes.EXECVE_SPAWN) {
    return "full";
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
  return [
    `process_ptr=0x${processPtr.toString(16)}`,
    `process_pid_field=${kernelExports.process_current_pid(vmPtr)}`,
    `process_ppid_field=${kernelExports.process_current_ppid(vmPtr)}`,
  ].join(" ");
}

function formatLatestPipeReadDebug(
  kernelExports: KernelExports,
  status: number,
): string | undefined {
  if (
    !PIPE_SLOT_DEBUG_ENABLED
    || status !== statusCodes.OK
  ) {
    return undefined;
  }
  const readSyscallNr = kernelExports.abi_sys_read?.();
  if (readSyscallNr === undefined) {
    return undefined;
  }
  const len = kernelExports.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return undefined;
  }
  const readSyscall = typeof readSyscallNr === "bigint" ? readSyscallNr : BigInt(readSyscallNr);
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
      || supervisorOfdFdType(kernelExports, ofdId) !== kernelExports.abi_fd_type_pipe()
    ) {
      continue;
    }
    const pipeSlot = supervisorOfdIno(kernelExports, ofdId);
    if (pipeSlot <= 0 || pipeSlot >= kernelExports.abi_max_pipes()) {
      continue;
    }
    const mode = supervisorOfdHostFd(kernelExports, ofdId) >= 0 ? 1 : 2;
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
  const maxBuffers = kernelNetBufferCount(kernelExports);
  const netBufSlotBytes = Math.floor(netBufPoolBytes / maxBuffers);
  const netBufSlots: KernelWorkerSocketStateSnapshot["netBufSlots"] = [];
  if (netBufSlotBytes > 0) {
    for (let bufferIndex = 0; bufferIndex < maxBuffers; bufferIndex += 1) {
      if ((kernelState.netBufUsed & (1n << BigInt(bufferIndex))) === 0n) {
        continue;
      }
      const offset = kernelState.netBufBase + bufferIndex * netBufSlotBytes;
      netBufSlots.push({
        bufferIndex,
        bytes: memory.slice(offset, offset + netBufSlotBytes),
      });
    }
  }
  return {
    socketTable: memory.slice(
      kernelState.socketTablePtr,
      kernelState.socketTablePtr + socketTableBytes,
    ),
    netBufSlots,
    socketUsed: kernelState.socketUsed,
    netBufUsed: kernelState.netBufUsed,
  };
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
      `control=${snapshot.controlChanged ? 1 : 0}`,
      `data=${JSON.stringify(ascii)}`,
    ].join("/");
  });
  return `pipe_slots=[${entries.join(",")}]`;
}

function publishThreadWorkerStage(stage: number, value = 0): void {
  if (THREAD_WORKER_STAGE_DEBUG_ENABLED && runtimeBridge !== null) {
    publishRuntimeBridgeDebugStage(runtimeBridge, stage, value);
  }
}

function postStatus(status: number, waitInWorker = false, completedSyncEffects?: number): void {
  publishThreadWorkerStage(100, status);
  const kernelExports = requireExports();
  const blockedSyscallNr = kernelExports.thread_blocked_syscall_nr?.(vmPtr) ?? 0n;
  const blockedSyscallArgs = blockedSyscallNr === 0n
    ? undefined
    : Array.from(
        { length: 6 },
        (_, index) => kernelExports.thread_blocked_syscall_arg?.(vmPtr, index) ?? 0n,
      ) as [bigint, bigint, bigint, bigint, bigint, bigint];
  const syncEffects = completedSyncEffects
    ?? readCompletedSyncEffects(kernelExports, statusCodes, status, vmPtr);
  publishThreadWorkerStage(101, status);
  const kernelState = readKernelRuntimeState(kernelExports);
  publishThreadWorkerStage(102, status);
  const completedPipeSlots = snapshotCompletedPipeSlots(kernelExports, kernelState);
  const effectiveSyncEffects = completedPipeSlots.length > 0
    ? syncEffects | SYNC_EFFECT_PIPE_STATE
    : syncEffects;
  const shouldSnapshotOwnedState = status !== statusCodes.SLEEP
    || effectiveSyncEffects !== 0
    || FD_CONTROL_DEBUG_ENABLED
    || THREAD_WORKER_STATUS_DEBUG_ENABLED;
  const fdEntries = shouldSnapshotOwnedState
    ? snapshotProcessFdEntries(kernelExports, vmPtr)
    : undefined;
  if (FD_CONTROL_DEBUG_ENABLED) {
    console.error(
      "[tidemark-fd-snapshot] "
        + `source=thread-status:vm=0x${vmPtr.toString(16)}`
        + `:status=${status}:syscall=${kernelExports.thread_a7?.(vmPtr)?.toString() ?? "n/a"}`
        + `:${formatFocusedFdEntries(fdEntries ?? [])}`,
    );
  }
  publishThreadWorkerStage(103, status);
  const shouldSnapshotPipeSlots = status === statusCodes.FAULT
    || (effectiveSyncEffects & SYNC_EFFECT_PIPE_STATE) !== 0;
  const pipeSlots = shouldSnapshotPipeSlots
    ? (() => {
        const fdPipeSlots = completedPipeSlots.length > 0
          ? completedPipeSlots
          : snapshotNonEmptyProcessPipeSlots(kernelExports, kernelState, fdEntries ?? []);
        const tracedPipeSlots = snapshotTracedPipeSlots(
          kernelExports,
          kernelState,
          vmPtr,
          fdPipeSlots,
        );
        return tracedPipeSlots.length > 0
          ? [...fdPipeSlots, ...tracedPipeSlots]
          : fdPipeSlots;
      })()
    : undefined;
  publishThreadWorkerStage(104, status);
  const latestFdTrace = shouldSnapshotOwnedState
    ? snapshotLatestFdTraceForStatus(kernelExports, vmPtr)
    : null;
  const guestMemoryWrites = [
    ...(snapshotCompletedReadGuestMemory(kernelExports, status) ?? []),
    ...snapshotPendingFsRequestGuestMemory(kernelExports),
  ];
  publishThreadWorkerStage(105, status);
  const memfsSnapshotMode = shouldSnapshotProcessMemfsMemory(status, effectiveSyncEffects);
  const processPendingSnapshot = snapshotAbsoluteMemory(
    kernelExports,
    kernelState.processPendingPtr,
    kernelState.processPendingBytes ?? 0,
  );
  const kernelMemoryWrites = [
    ...(processPendingSnapshot ? [processPendingSnapshot] : []),
    ...(snapshotFsRingMemory(kernelExports, status) ?? []),
    ...snapshotProcessMemfsMemory(
      kernelExports,
      kernelState,
      memfsSnapshotMode,
    ),
  ];
  publishThreadWorkerStage(106, status);
  const ofdSlots = fdEntries === undefined
    ? undefined
    : snapshotOfdSlotsForFdEntries(kernelExports, kernelState, vmPtr, fdEntries);
  publishThreadWorkerStage(107, status);
  if (ofdSlots !== undefined) {
    patchEventfdOfdSlotsFromTrace(kernelExports, ofdSlots, latestFdTrace);
  }
  publishThreadWorkerStage(108, status);
  const socketState = shouldSnapshotOwnedState
    ? snapshotSocketState(kernelExports, kernelState)
    : undefined;
  publishThreadWorkerStage(109, status);
  const supervisorFdControlDebug = collectSupervisorFdControlDebug(kernelExports);
  if (supervisorFdControlDebug !== undefined) {
    console.error(`[tidemark-fd-control] ${supervisorFdControlDebug}`);
  }
  const fdTraceDebug = [
    collectFdTraceDebug(kernelExports, vmPtr, guestBase),
    supervisorFdControlDebug,
  ].filter((part): part is string => part !== undefined && part !== "").join(",");
  const includeDetailedDebug = status === statusCodes.FAULT
    || THREAD_WORKER_STATUS_DEBUG_ENABLED;
  const debug = status !== statusCodes.FS ? (() => {
    publishThreadWorkerStage(120, status);
    const parts = includeDetailedDebug
      ? collectDebugParts(kernelExports, vmPtr, guestBase)
      : [];
    const pipeSlotDebug = formatPipeSlotDebug(pipeSlots);
    if (pipeSlotDebug) {
      parts.push(pipeSlotDebug);
    }
    if (lastResumeBlockedDebug) {
      parts.push(lastResumeBlockedDebug);
      lastResumeBlockedDebug = undefined;
    }
    if (lastFutexWaitDebug) {
      parts.push(lastFutexWaitDebug);
      lastFutexWaitDebug = undefined;
    }
    const pipeReadDebug = formatLatestPipeReadDebug(kernelExports, status);
    if (pipeReadDebug) {
      parts.push(pipeReadDebug);
    }
    const inboundGuestWrites = consumeThreadWorkerInboundGuestWriteDebug();
    if (inboundGuestWrites.length > 0) {
      parts.push(`thread_inbound_guest_writes=[${inboundGuestWrites.join(",")}]`);
    }
    publishThreadWorkerStage(121, status);
    return parts.length === 0 ? undefined : parts.join(" ");
  })() : formatOwnerDebug(kernelExports);
  publishThreadWorkerStage(122, status);
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
    blockedFd: blockedSyscallNr === 0n ? undefined : kernelExports.thread_blocked_fd(vmPtr),
    blockedHasOfdPin: blockedSyscallNr === 0n
      ? undefined
      : kernelExports.thread_blocked_ofd_id(vmPtr) !== 0,
    blockedSyscallNr: blockedSyscallNr === 0n ? undefined : blockedSyscallNr,
    blockedSyscallArgs,
    futexWaitAddr: status === statusCodes.FUTEX_WAIT
      ? kernelExports.thread_futex_wait_addr(vmPtr)
      : undefined,
    futexBlockedAddr: status === statusCodes.FUTEX_WAIT
      ? kernelExports.thread_record_blocked_addr(vmPtr)
      : undefined,
    vmPtr,
    processPtr: kernelExports.thread_process_ptr(vmPtr),
    processPid: kernelExports.process_current_pid(vmPtr),
    kernelState,
    syncEffects: effectiveSyncEffects,
    fdEntries,
    ofdSlots,
    processState: snapshotExecveProcessState(kernelExports, status),
    pipeSlots,
    socketState,
    guestMemoryWrites: guestMemoryWrites.length > 0 ? guestMemoryWrites : undefined,
    kernelMemoryWrites: kernelMemoryWrites.length > 0 ? kernelMemoryWrites : undefined,
    fdTraceDebug: fdTraceDebug === "" ? undefined : fdTraceDebug,
    fdTraceSyscallNr: latestFdTrace?.nr,
    fdTraceArg0OfdId: latestFdTrace?.arg0OfdId,
    fdTraceArg0OfdType: latestFdTrace?.arg0OfdType,
    fdTraceArg0OfdOffset: latestFdTrace?.arg0OfdOffset,
    fdTraceArg1: latestFdTrace?.arg1,
    fdTraceArg2OfdId: latestFdTrace?.arg2OfdId,
    fdTraceArg2OfdType: latestFdTrace?.arg2OfdType,
    fdTraceResult: latestFdTrace?.result,
    debug,
  };
  if (waitInWorker) {
    msg.waitInWorker = true;
  }
  if (runtimeBridge !== null) {
    Atomics.add(new Int32Array(runtimeBridge.notifySab), 0, 1);
  }
  publishThreadWorkerStage(130, status);
  workerPostMessage(msg);
  publishThreadWorkerStage(131, status);
}


export function postThreadWorkerStatus(
  context: ThreadWorkerStatusContext,
  status: number,
  waitInWorker = false,
  completedSyncEffects?: number,
): void {
  exports = context.kernelExports;
  statusCodes = context.statusCodes;
  vmPtr = context.vmPtr;
  guestBase = context.guestBase;
  generation = context.generation;
  runtimeBridge = context.runtimeBridge;
  lastResumeBlockedDebug = context.lastResumeBlockedDebug;
  lastFutexWaitDebug = context.lastFutexWaitDebug;
  postStatus(status, waitInWorker, completedSyncEffects);
  context.setLastResumeBlockedDebug(lastResumeBlockedDebug);
  context.setLastFutexWaitDebug(lastFutexWaitDebug);
}
