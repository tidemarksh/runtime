import type { KernelExports, ProcessSpawnHandoff } from "../abi.js";
import { ensureMemoryCapacity, kernelScratchBase } from "./kernel-scratch.js";
import type { ProcessHandle } from "./types.js";

export const SUPERVISOR_SHADOW_THREAD_EVENT_MAGIC = 0x5645_5354;
export const SUPERVISOR_SHADOW_THREAD_EVENT_VERSION = 1;
export const SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES = 64;
const SUPERVISOR_FAULT_EXIT_DECISION_MAGIC = 0x5846_5354;
const SUPERVISOR_FAULT_EXIT_DECISION_BYTES = 88;
const SUPERVISOR_FAULT_EXIT_KIND_THREAD_EXIT = 1;
const SUPERVISOR_FAULT_EXIT_KIND_PROCESS_EXIT = 2;
const SUPERVISOR_FAULT_EXIT_SCRATCH_OFFSET = 0x2d000;
const SUPERVISOR_PROCESS_HOST_OP_COMPLETION_MAGIC = 0x4348_5354;
const SUPERVISOR_PROCESS_HOST_OP_COMPLETION_BYTES = 64;
const SUPERVISOR_PROCESS_HOST_OP_COMPLETION_KIND_SPAWN_PROCESS_WORKER = 1;
const SUPERVISOR_PROCESS_HOST_OP_COMPLETION_OUTCOME_SUCCESS = 1;
const SUPERVISOR_PROCESS_HOST_OP_COMPLETION_OUTCOME_FAILURE = 2;
const SUPERVISOR_PROCESS_HOST_OP_COMPLETION_SCRATCH_OFFSET = 0x2f000;
export const SUPERVISOR_FD_CONTROL_RECORD_MAGIC = 0x4446_5354;
export const SUPERVISOR_FD_CONTROL_RECORD_BYTES = 96;
export const SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED = 1;
export const SUPERVISOR_FD_CONTROL_KIND_FD_CLOSED = 2;
export const SUPERVISOR_FD_CONTROL_KIND_FD_DUPLICATED = 3;
export const SUPERVISOR_FD_CONTROL_KIND_FD_FLAGS_UPDATED = 4;
export const SUPERVISOR_FD_CONTROL_KIND_EPOLL_CREATED = 5;
export const SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_ADD = 6;
export const SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_DEL = 7;
export const SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_MOD = 8;
const SUPERVISOR_FD_CONTROL_SCRATCH_OFFSET = 0x31000;

export interface SupervisorShadowKernelExports extends KernelExports {
  supervisor_abi_version(): number;
  supervisor_submit_thread_event(eventPtr: number, eventLen: number): number;
}

export interface SupervisorShadowThreadEvent {
  sequence?: bigint | number;
  vmPtr: bigint | number;
  pid: number;
  tid: number;
  status: number;
  syncEffects?: number;
  blockedReason?: number;
  pc?: bigint | number;
  sp?: bigint | number;
}

export interface SupervisorFaultExitDecision {
  kind: number;
  seq: bigint;
  vmPtr: number;
  exitVmPtr: number;
  tid: number;
  processPtr: number;
  remainingThreads: number;
  liveThreads: number;
  exitCode: number;
  termSignal: number;
  status: number;
  faultSignal: number;
  faultKind: number;
  faultPc: bigint;
  faultAddr: bigint;
}

export interface SupervisorProcessHostOpCompletion {
  vmPtr: number;
  handoff: ProcessSpawnHandoff;
  success: boolean;
  detail?: number;
}

export interface SupervisorFdControlRecord {
  kind: number;
  seq: bigint;
  vmPtr: number;
  processPtr: number;
  pid: number;
  fd: number;
  fd2: number;
  ofdId: number;
  ofd2Id: number;
  fdType: number;
  resourceSlot: number;
  resourceGeneration: number;
  result: bigint;
  flags: bigint;
  detail0: bigint;
  detail1: bigint;
}

export function hasSupervisorShadowProtocol(
  kernelExports: KernelExports,
): kernelExports is SupervisorShadowKernelExports {
  return typeof kernelExports.supervisor_abi_version === "function"
    && typeof kernelExports.supervisor_submit_thread_event === "function";
}

export function encodeSupervisorShadowThreadEvent(
  view: DataView,
  offset: number,
  event: SupervisorShadowThreadEvent,
): void {
  if (offset < 0 || offset + SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES > view.byteLength) {
    throw new RangeError("supervisor shadow thread event record does not fit in the target view");
  }

  view.setUint32(offset, SUPERVISOR_SHADOW_THREAD_EVENT_MAGIC, true);
  view.setUint32(offset + 4, SUPERVISOR_SHADOW_THREAD_EVENT_VERSION, true);
  view.setUint32(offset + 8, SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES, true);
  view.setUint32(offset + 12, event.status, true);
  view.setBigUint64(offset + 16, BigInt(event.sequence ?? 0), true);
  view.setBigUint64(offset + 24, BigInt(event.vmPtr), true);
  view.setUint32(offset + 32, event.pid, true);
  view.setUint32(offset + 36, event.tid, true);
  view.setUint32(offset + 40, event.syncEffects ?? 0, true);
  view.setUint32(offset + 44, event.blockedReason ?? 0, true);
  view.setBigUint64(offset + 48, BigInt(event.pc ?? 0), true);
  view.setBigUint64(offset + 56, BigInt(event.sp ?? 0), true);
}

export function submitSupervisorShadowThreadEvent(
  kernelExports: KernelExports,
  recordPtr: number,
  event: SupervisorShadowThreadEvent,
): number | undefined {
  if (!hasSupervisorShadowProtocol(kernelExports)) {
    return undefined;
  }

  encodeSupervisorShadowThreadEvent(new DataView(kernelExports.memory.buffer), recordPtr, event);
  return kernelExports.supervisor_submit_thread_event(
    recordPtr,
    SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES,
  );
}

export function completeSupervisorProcessHostOp(
  handle: ProcessHandle,
  completion: SupervisorProcessHostOpCompletion,
): number | undefined {
  const completeHostOp = handle.kernel.supervisor_complete_host_op;
  if (completeHostOp === undefined || completion.handoff.supervisorHostOp !== true) {
    return undefined;
  }
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = kernelScratchBase(handle.kernel)
    + SUPERVISOR_PROCESS_HOST_OP_COMPLETION_SCRATCH_OFFSET;
  ensureMemoryCapacity(memory, scratch + SUPERVISOR_PROCESS_HOST_OP_COMPLETION_BYTES);
  const view = new DataView(memory.buffer, scratch, SUPERVISOR_PROCESS_HOST_OP_COMPLETION_BYTES);
  view.setUint32(0, SUPERVISOR_PROCESS_HOST_OP_COMPLETION_MAGIC, true);
  view.setUint32(4, handle.kernel.supervisor_abi_version?.() ?? 1, true);
  view.setUint32(8, SUPERVISOR_PROCESS_HOST_OP_COMPLETION_BYTES, true);
  view.setUint32(12, SUPERVISOR_PROCESS_HOST_OP_COMPLETION_KIND_SPAWN_PROCESS_WORKER, true);
  view.setBigUint64(16, 0n, true);
  view.setUint32(24, completion.vmPtr, true);
  view.setUint32(28, completion.handoff.kind, true);
  view.setInt32(32, completion.handoff.childPid, true);
  view.setUint32(
    36,
    completion.success
      ? SUPERVISOR_PROCESS_HOST_OP_COMPLETION_OUTCOME_SUCCESS
      : SUPERVISOR_PROCESS_HOST_OP_COMPLETION_OUTCOME_FAILURE,
    true,
  );
  view.setUint32(40, completion.detail ?? 0, true);
  view.setUint32(44, 0, true);
  view.setBigUint64(48, completion.handoff.path, true);
  view.setBigUint64(56, 0n, true);
  return completeHostOp(scratch, SUPERVISOR_PROCESS_HOST_OP_COMPLETION_BYTES);
}

function supervisorFaultExitKindThreadExit(kernelExports: KernelExports): number {
  return kernelExports.supervisor_fault_exit_kind_thread_exit?.()
    ?? SUPERVISOR_FAULT_EXIT_KIND_THREAD_EXIT;
}

export function supervisorFaultExitKindProcessExit(kernelExports: KernelExports): number {
  return kernelExports.supervisor_fault_exit_kind_process_exit?.()
    ?? SUPERVISOR_FAULT_EXIT_KIND_PROCESS_EXIT;
}

function decodeSupervisorFaultExitDecision(
  kernelExports: KernelExports,
  view: DataView,
  vmPtr: number,
): SupervisorFaultExitDecision {
  if (view.byteLength < SUPERVISOR_FAULT_EXIT_DECISION_BYTES) {
    throw new Error(`supervisor fault exit decision record too short: ${view.byteLength}`);
  }
  const magic = view.getUint32(0, true);
  const version = view.getUint32(4, true);
  const bytes = view.getUint32(8, true);
  const kind = view.getUint32(12, true);
  if (magic !== SUPERVISOR_FAULT_EXIT_DECISION_MAGIC) {
    throw new Error(`supervisor fault exit decision bad magic: 0x${magic.toString(16)}`);
  }
  const expectedVersion = kernelExports.supervisor_abi_version?.() ?? version;
  if (version !== expectedVersion) {
    throw new Error(`supervisor fault exit decision bad version: ${version}`);
  }
  if (bytes < SUPERVISOR_FAULT_EXIT_DECISION_BYTES || bytes > view.byteLength) {
    throw new Error(`supervisor fault exit decision bad size: ${bytes}`);
  }
  if (
    kind !== supervisorFaultExitKindThreadExit(kernelExports)
    && kind !== supervisorFaultExitKindProcessExit(kernelExports)
  ) {
    throw new Error(`supervisor fault exit decision bad kind: ${kind}`);
  }
  const decision: SupervisorFaultExitDecision = {
    kind,
    seq: view.getBigUint64(16, true),
    vmPtr: view.getUint32(24, true),
    exitVmPtr: view.getUint32(28, true),
    tid: view.getUint32(32, true),
    processPtr: view.getUint32(36, true),
    remainingThreads: view.getUint32(40, true),
    liveThreads: view.getUint32(44, true),
    exitCode: view.getInt32(48, true),
    termSignal: view.getInt32(52, true),
    status: view.getUint32(56, true),
    faultSignal: view.getUint32(60, true),
    faultKind: view.getUint32(64, true),
    faultPc: view.getBigUint64(72, true),
    faultAddr: view.getBigUint64(80, true),
  };
  if (decision.vmPtr !== vmPtr) {
    throw new Error(
      `supervisor fault exit decision vm mismatch: `
        + `0x${decision.vmPtr.toString(16)} != 0x${vmPtr.toString(16)}`,
    );
  }
  return decision;
}

export function readSupervisorFaultExitDecision(
  handle: ProcessHandle,
  vmPtr: number,
): SupervisorFaultExitDecision | undefined {
  const recordBytesExport = handle.kernel.supervisor_fault_exit_decision_bytes;
  const readDecision = handle.kernel.supervisor_fault_exit_decision;
  if (recordBytesExport === undefined || readDecision === undefined) {
    return undefined;
  }
  const recordBytes = recordBytesExport();
  if (recordBytes < SUPERVISOR_FAULT_EXIT_DECISION_BYTES) {
    throw new Error(`supervisor fault exit decision bytes too small: ${recordBytes}`);
  }
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = kernelScratchBase(handle.kernel) + SUPERVISOR_FAULT_EXIT_SCRATCH_OFFSET;
  ensureMemoryCapacity(memory, scratch + recordBytes);
  const result = readDecision(vmPtr, scratch, recordBytes);
  if (result < 0 || result > recordBytes) {
    throw new Error(`supervisor fault exit decision failed: ${result}`);
  }
  if (result === 0) {
    return undefined;
  }
  return decodeSupervisorFaultExitDecision(
    handle.kernel,
    new DataView(memory.buffer, scratch, result),
    vmPtr,
  );
}

function supervisorFdControlKindPipeCreated(kernelExports: KernelExports): number {
  return kernelExports.supervisor_fd_control_kind_pipe_created?.()
    ?? SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED;
}

function supervisorFdControlKindFdClosed(kernelExports: KernelExports): number {
  return kernelExports.supervisor_fd_control_kind_fd_closed?.()
    ?? SUPERVISOR_FD_CONTROL_KIND_FD_CLOSED;
}

function supervisorFdControlKindFdDuplicated(kernelExports: KernelExports): number {
  return kernelExports.supervisor_fd_control_kind_fd_duplicated?.()
    ?? SUPERVISOR_FD_CONTROL_KIND_FD_DUPLICATED;
}

function supervisorFdControlKindFdFlagsUpdated(kernelExports: KernelExports): number {
  return kernelExports.supervisor_fd_control_kind_fd_flags_updated?.()
    ?? SUPERVISOR_FD_CONTROL_KIND_FD_FLAGS_UPDATED;
}

function supervisorFdControlKindEpollCreated(kernelExports: KernelExports): number {
  return kernelExports.supervisor_fd_control_kind_epoll_created?.()
    ?? SUPERVISOR_FD_CONTROL_KIND_EPOLL_CREATED;
}

function supervisorFdControlKindEpollCtlAdd(kernelExports: KernelExports): number {
  return kernelExports.supervisor_fd_control_kind_epoll_ctl_add?.()
    ?? SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_ADD;
}

function supervisorFdControlKindEpollCtlDel(kernelExports: KernelExports): number {
  return kernelExports.supervisor_fd_control_kind_epoll_ctl_del?.()
    ?? SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_DEL;
}

function supervisorFdControlKindEpollCtlMod(kernelExports: KernelExports): number {
  return kernelExports.supervisor_fd_control_kind_epoll_ctl_mod?.()
    ?? SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_MOD;
}

export function decodeSupervisorFdControlRecord(
  kernelExports: KernelExports,
  view: DataView,
): SupervisorFdControlRecord {
  if (view.byteLength < SUPERVISOR_FD_CONTROL_RECORD_BYTES) {
    throw new Error(`supervisor fd control record too short: ${view.byteLength}`);
  }
  const magic = view.getUint32(0, true);
  const version = view.getUint32(4, true);
  const bytes = view.getUint32(8, true);
  const kind = view.getUint32(12, true);
  if (magic !== SUPERVISOR_FD_CONTROL_RECORD_MAGIC) {
    throw new Error(`supervisor fd control bad magic: 0x${magic.toString(16)}`);
  }
  const expectedVersion = kernelExports.supervisor_abi_version?.() ?? version;
  if (version !== expectedVersion) {
    throw new Error(`supervisor fd control bad version: ${version}`);
  }
  if (bytes < SUPERVISOR_FD_CONTROL_RECORD_BYTES || bytes > view.byteLength) {
    throw new Error(`supervisor fd control bad size: ${bytes}`);
  }
  if (
    kind !== supervisorFdControlKindPipeCreated(kernelExports)
    && kind !== supervisorFdControlKindFdClosed(kernelExports)
    && kind !== supervisorFdControlKindFdDuplicated(kernelExports)
    && kind !== supervisorFdControlKindFdFlagsUpdated(kernelExports)
    && kind !== supervisorFdControlKindEpollCreated(kernelExports)
    && kind !== supervisorFdControlKindEpollCtlAdd(kernelExports)
    && kind !== supervisorFdControlKindEpollCtlDel(kernelExports)
    && kind !== supervisorFdControlKindEpollCtlMod(kernelExports)
  ) {
    throw new Error(`supervisor fd control bad kind: ${kind}`);
  }
  return {
    kind,
    seq: view.getBigUint64(16, true),
    vmPtr: view.getUint32(24, true),
    processPtr: view.getUint32(28, true),
    pid: view.getUint32(32, true),
    fd: view.getUint32(36, true),
    fd2: view.getUint32(40, true),
    ofdId: view.getUint32(44, true),
    ofd2Id: view.getUint32(48, true),
    fdType: view.getUint32(52, true),
    resourceSlot: view.getUint32(56, true),
    resourceGeneration: view.getUint32(60, true),
    result: view.getBigInt64(64, true),
    flags: view.getBigUint64(72, true),
    detail0: view.getBigUint64(80, true),
    detail1: view.getBigUint64(88, true),
  };
}

export function readSupervisorFdControlRecord(
  handle: ProcessHandle,
): SupervisorFdControlRecord | undefined {
  const recordBytesExport = handle.kernel.supervisor_fd_control_record_bytes;
  const readRecord = handle.kernel.supervisor_fd_control_record;
  if (recordBytesExport === undefined || readRecord === undefined) {
    return undefined;
  }
  const recordBytes = recordBytesExport();
  if (recordBytes < SUPERVISOR_FD_CONTROL_RECORD_BYTES) {
    throw new Error(`supervisor fd control bytes too small: ${recordBytes}`);
  }
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = kernelScratchBase(handle.kernel) + SUPERVISOR_FD_CONTROL_SCRATCH_OFFSET;
  ensureMemoryCapacity(memory, scratch + recordBytes);
  const result = readRecord(scratch, recordBytes);
  if (result < 0 || result > recordBytes) {
    throw new Error(`supervisor fd control read failed: ${result}`);
  }
  if (result === 0) {
    return undefined;
  }
  return decodeSupervisorFdControlRecord(
    handle.kernel,
    new DataView(memory.buffer, scratch, result),
  );
}

export function formatSupervisorFdControlRecord(record: SupervisorFdControlRecord): string {
  return [
    "supervisor-fd-control",
    `kind=${record.kind}`,
    `seq=${record.seq.toString()}`,
    `vm=0x${record.vmPtr.toString(16)}`,
    `process=0x${record.processPtr.toString(16)}`,
    `pid=${record.pid}`,
    `fd=${record.fd}`,
    `fd2=${record.fd2}`,
    `ofd=${record.ofdId}`,
    `ofd2=${record.ofd2Id}`,
    `type=${record.fdType}`,
    `slot=${record.resourceSlot}`,
    `generation=${record.resourceGeneration}`,
    `result=${record.result.toString()}`,
    `flags=0x${record.flags.toString(16)}`,
    `detail0=${record.detail0.toString()}`,
    `detail1=${record.detail1.toString()}`,
  ].join(":");
}
