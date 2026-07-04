import type { KernelExports, ProcessSpawnHandoff } from "../abi.js";
import { ensureMemoryCapacity, kernelScratchBase } from "./kernel-scratch.js";
import type { ProcessHandle } from "./types.js";

const SUPERVISOR_SHADOW_THREAD_EVENT_SCRATCH_OFFSET = 0x2c000;
const SUPERVISOR_FAULT_EXIT_SCRATCH_OFFSET = 0x2d000;
const SUPERVISOR_PROCESS_HOST_OP_COMPLETION_SCRATCH_OFFSET = 0x2f000;
const SUPERVISOR_FD_CONTROL_SCRATCH_OFFSET = 0x31000;
const SUPERVISOR_FS_CONTROL_SCRATCH_OFFSET = 0x32000;
const SUPERVISOR_SIGNAL_CONTROL_SCRATCH_OFFSET = 0x33000;

const SUPERVISOR_THREAD_EVENT_DETAIL_END = 80;

export interface SupervisorShadowKernelExports extends KernelExports {
  supervisor_abi_version(): number;
  supervisor_submit_thread_event(eventPtr: number, eventLen: number): number;
}

export interface SupervisorShadowThreadEvent {
  eventKind?: number;
  sequence?: bigint | number;
  vmPtr: bigint | number;
  pid: number;
  tid: number;
  status: number;
  syncEffects?: number;
  blockedReason?: number;
  pc?: bigint | number;
  sp?: bigint | number;
  detail?: bigint | number;
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

export interface SupervisorFsControlRecord {
  kind: number;
  seq: bigint;
  vmPtr: number;
  processPtr: number;
  pid: number;
  fd: number;
  ino: number;
  opcode: number;
  fileObjectId: bigint;
  pageIndex: bigint;
  offset: bigint;
  length: bigint;
  result: bigint;
  flags: bigint;
  detail0: bigint;
  detail1: bigint;
}

export interface SupervisorSignalControlRecord {
  kind: number;
  seq: bigint;
  vmPtr: number;
  processPtr: number;
  pid: number;
  tid: number;
  sig: number;
  status: number;
  sigInfoCode: number;
  boundary: number;
  threadPending: bigint;
  processPending: bigint;
  recordPending: bigint;
  deliverable: bigint;
  mask: bigint;
  pc: bigint;
  detail0: bigint;
  detail1: bigint;
}

export function hasSupervisorShadowProtocol(
  kernelExports: KernelExports,
): kernelExports is SupervisorShadowKernelExports {
  return typeof kernelExports.supervisor_abi_version === "function"
    && typeof kernelExports.supervisor_submit_thread_event === "function";
}

function supervisorNumericExport(kernelExports: KernelExports, name: keyof KernelExports): number {
  const exportFn = kernelExports[name] as (() => bigint | number) | undefined;
  if (exportFn === undefined) {
    throw new Error(`kernel supervisor ABI missing ${String(name)}`);
  }
  const value = exportFn.call(kernelExports);
  return typeof value === "bigint" ? Number(value) : value;
}

function supervisorAbiVersion(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_abi_version");
}

function supervisorThreadEventRecordBytes(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_thread_event_record_bytes");
}

function supervisorThreadEventKindBlocked(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_thread_event_kind_blocked");
}

export function encodeSupervisorShadowThreadEvent(
  kernelExports: KernelExports,
  view: DataView,
  offset: number,
  event: SupervisorShadowThreadEvent,
): void {
  const recordBytes = supervisorThreadEventRecordBytes(kernelExports);
  if (recordBytes < SUPERVISOR_THREAD_EVENT_DETAIL_END) {
    throw new Error(`supervisor shadow thread event bytes too small: ${recordBytes}`);
  }
  if (offset < 0 || offset + recordBytes > view.byteLength) {
    throw new RangeError("supervisor shadow thread event record does not fit in the target view");
  }

  new Uint8Array(view.buffer, view.byteOffset + offset, recordBytes).fill(0);
  view.setUint32(offset, supervisorNumericExport(kernelExports, "supervisor_thread_event_record_magic"), true);
  view.setUint32(offset + 4, supervisorAbiVersion(kernelExports), true);
  view.setUint32(offset + 8, recordBytes, true);
  view.setUint32(offset + 12, event.eventKind ?? supervisorThreadEventKindBlocked(kernelExports), true);
  view.setBigUint64(offset + 16, BigInt(event.sequence ?? 0), true);
  view.setBigUint64(offset + 24, BigInt(event.vmPtr), true);
  view.setUint32(offset + 32, event.pid, true);
  view.setUint32(offset + 36, event.tid, true);
  view.setUint32(offset + 40, event.status, true);
  view.setUint32(offset + 44, event.syncEffects ?? 0, true);
  view.setUint32(offset + 48, event.blockedReason ?? 0, true);
  view.setUint32(offset + 52, 0, true);
  view.setBigUint64(offset + 56, BigInt(event.pc ?? 0), true);
  view.setBigUint64(offset + 64, BigInt(event.sp ?? 0), true);
  view.setBigUint64(offset + 72, BigInt(event.detail ?? 0), true);
}

export function submitSupervisorShadowThreadEvent(
  kernelExports: KernelExports,
  recordPtr: number,
  event: SupervisorShadowThreadEvent,
): number | undefined {
  if (!hasSupervisorShadowProtocol(kernelExports)) {
    return undefined;
  }

  const recordBytes = supervisorThreadEventRecordBytes(kernelExports);
  encodeSupervisorShadowThreadEvent(kernelExports, new DataView(kernelExports.memory.buffer), recordPtr, event);
  return kernelExports.supervisor_submit_thread_event(
    recordPtr,
    recordBytes,
  );
}

export function supervisorThreadEventKindForStatus(
  kernelExports: KernelExports,
  status: number,
): number {
  if (status === kernelExports.abi_status_fault()) {
    return supervisorNumericExport(kernelExports, "supervisor_thread_event_kind_faulted");
  }
  if (
    status === kernelExports.abi_status_ok()
    || status === kernelExports.abi_status_jit_ready()
  ) {
    return supervisorNumericExport(kernelExports, "supervisor_thread_event_kind_ready");
  }
  return supervisorThreadEventKindBlocked(kernelExports);
}

export function submitSupervisorShadowStatusEvent(
  handle: ProcessHandle,
  event: Omit<SupervisorShadowThreadEvent, "eventKind"> & { eventKind?: number },
): number | undefined {
  if (!hasSupervisorShadowProtocol(handle.kernel)) {
    return undefined;
  }
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = kernelScratchBase(handle.kernel) + SUPERVISOR_SHADOW_THREAD_EVENT_SCRATCH_OFFSET;
  ensureMemoryCapacity(memory, scratch + supervisorThreadEventRecordBytes(handle.kernel));
  return submitSupervisorShadowThreadEvent(handle.kernel, scratch, {
    ...event,
    eventKind: event.eventKind ?? supervisorThreadEventKindForStatus(handle.kernel, event.status),
  });
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
  const recordBytes = supervisorNumericExport(handle.kernel, "supervisor_host_completion_record_bytes");
  ensureMemoryCapacity(memory, scratch + recordBytes);
  const view = new DataView(memory.buffer, scratch, recordBytes);
  new Uint8Array(memory.buffer, scratch, recordBytes).fill(0);
  view.setUint32(0, supervisorNumericExport(handle.kernel, "supervisor_host_completion_record_magic"), true);
  view.setUint32(4, supervisorAbiVersion(handle.kernel), true);
  view.setUint32(8, recordBytes, true);
  view.setUint32(12, supervisorNumericExport(handle.kernel, "supervisor_process_host_op_kind_spawn_process_worker"), true);
  view.setBigUint64(16, completion.handoff.supervisorHostOpId ?? 0n, true);
  view.setUint32(24, completion.vmPtr, true);
  view.setUint32(28, completion.handoff.kind, true);
  view.setInt32(32, completion.handoff.childPid, true);
  view.setUint32(
    36,
    completion.success
      ? supervisorNumericExport(handle.kernel, "supervisor_host_completion_outcome_success")
      : supervisorNumericExport(handle.kernel, "supervisor_host_completion_outcome_failure"),
    true,
  );
  view.setUint32(40, completion.detail ?? 0, true);
  view.setUint32(44, 0, true);
  view.setBigUint64(48, completion.handoff.path, true);
  view.setBigUint64(56, 0n, true);
  return completeHostOp.call(handle.kernel, scratch, recordBytes);
}

function supervisorFaultExitKindThreadExit(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fault_exit_kind_thread_exit");
}

export function supervisorFaultExitKindProcessExit(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fault_exit_kind_process_exit");
}

function decodeSupervisorFaultExitDecision(
  kernelExports: KernelExports,
  view: DataView,
  vmPtr: number,
): SupervisorFaultExitDecision {
  const recordBytes = supervisorNumericExport(kernelExports, "supervisor_fault_exit_decision_bytes");
  if (view.byteLength < recordBytes) {
    throw new Error(`supervisor fault exit decision record too short: ${view.byteLength}`);
  }
  const magic = view.getUint32(0, true);
  const version = view.getUint32(4, true);
  const bytes = view.getUint32(8, true);
  const kind = view.getUint32(12, true);
  if (magic !== supervisorNumericExport(kernelExports, "supervisor_fault_exit_decision_magic")) {
    throw new Error(`supervisor fault exit decision bad magic: 0x${magic.toString(16)}`);
  }
  const expectedVersion = supervisorAbiVersion(kernelExports);
  if (version !== expectedVersion) {
    throw new Error(`supervisor fault exit decision bad version: ${version}`);
  }
  if (bytes < recordBytes || bytes > view.byteLength) {
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
  const readDecision = handle.kernel.supervisor_fault_exit_decision;
  if (readDecision === undefined) {
    return undefined;
  }
  const recordBytes = supervisorNumericExport(handle.kernel, "supervisor_fault_exit_decision_bytes");
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = kernelScratchBase(handle.kernel) + SUPERVISOR_FAULT_EXIT_SCRATCH_OFFSET;
  ensureMemoryCapacity(memory, scratch + recordBytes);
  const result = readDecision.call(handle.kernel, vmPtr, scratch, recordBytes);
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
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_pipe_created");
}

function supervisorFdControlKindFdClosed(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_fd_closed");
}

function supervisorFdControlKindFdDuplicated(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_fd_duplicated");
}

function supervisorFdControlKindFdFlagsUpdated(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_fd_flags_updated");
}

function supervisorFdControlKindEpollCreated(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_epoll_created");
}

function supervisorFdControlKindEpollCtlAdd(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_epoll_ctl_add");
}

function supervisorFdControlKindEpollCtlDel(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_epoll_ctl_del");
}

function supervisorFdControlKindEpollCtlMod(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_epoll_ctl_mod");
}

function supervisorFdControlKindSocketCreated(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_socket_created");
}

function supervisorFdControlKindSocketpairCreated(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_socketpair_created");
}

function supervisorFdControlKindSocketBound(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_socket_bound");
}

function supervisorFdControlKindSocketListening(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_socket_listening");
}

function supervisorFdControlKindSocketConnectStarted(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_socket_connect_started");
}

function supervisorFdControlKindSocketConnected(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_socket_connected");
}

function supervisorFdControlKindSocketConnectFailed(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_socket_connect_failed");
}

function supervisorFdControlKindSocketAccepted(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fd_control_kind_socket_accepted");
}

export function decodeSupervisorFdControlRecord(
  kernelExports: KernelExports,
  view: DataView,
): SupervisorFdControlRecord {
  const recordBytes = supervisorNumericExport(kernelExports, "supervisor_fd_control_record_bytes");
  if (view.byteLength < recordBytes) {
    throw new Error(`supervisor fd control record too short: ${view.byteLength}`);
  }
  const magic = view.getUint32(0, true);
  const version = view.getUint32(4, true);
  const bytes = view.getUint32(8, true);
  const kind = view.getUint32(12, true);
  if (magic !== supervisorNumericExport(kernelExports, "supervisor_fd_control_record_magic")) {
    throw new Error(`supervisor fd control bad magic: 0x${magic.toString(16)}`);
  }
  const expectedVersion = supervisorAbiVersion(kernelExports);
  if (version !== expectedVersion) {
    throw new Error(`supervisor fd control bad version: ${version}`);
  }
  if (bytes < recordBytes || bytes > view.byteLength) {
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
    && kind !== supervisorFdControlKindSocketCreated(kernelExports)
    && kind !== supervisorFdControlKindSocketpairCreated(kernelExports)
    && kind !== supervisorFdControlKindSocketBound(kernelExports)
    && kind !== supervisorFdControlKindSocketListening(kernelExports)
    && kind !== supervisorFdControlKindSocketConnectStarted(kernelExports)
    && kind !== supervisorFdControlKindSocketConnected(kernelExports)
    && kind !== supervisorFdControlKindSocketConnectFailed(kernelExports)
    && kind !== supervisorFdControlKindSocketAccepted(kernelExports)
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
  const readRecord = handle.kernel.supervisor_fd_control_record;
  if (readRecord === undefined) {
    return undefined;
  }
  const recordBytes = supervisorNumericExport(handle.kernel, "supervisor_fd_control_record_bytes");
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = kernelScratchBase(handle.kernel) + SUPERVISOR_FD_CONTROL_SCRATCH_OFFSET;
  ensureMemoryCapacity(memory, scratch + recordBytes);
  const result = readRecord.call(handle.kernel, scratch, recordBytes);
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

export function pollSupervisorFdControlDiagnostic(handle: ProcessHandle): string | undefined {
  const countExport = handle.kernel.supervisor_shadow_fd_control_count;
  if (countExport === undefined) {
    return undefined;
  }
  const count = BigInt(countExport.call(handle.kernel));
  if (count === 0n) {
    return undefined;
  }
  try {
    const record = readSupervisorFdControlRecord(handle);
    if (record === undefined) {
      return undefined;
    }
    const lastSeq = handle.lastSupervisorFdControlSeq ?? 0n;
    if (record.seq <= lastSeq) {
      return undefined;
    }
    handle.lastSupervisorFdControlSeq = record.seq;
    return formatSupervisorFdControlRecord(record);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return `supervisor-fd-control:error=${JSON.stringify(message)}`;
  }
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

function supervisorFsControlKindPageCacheMiss(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fs_control_kind_page_cache_miss");
}

function supervisorFsControlKindPageCacheFill(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fs_control_kind_page_cache_fill");
}

function supervisorFsControlKindDirtyPagePublished(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fs_control_kind_dirty_page_published");
}

function supervisorFsControlKindDirtyPageFlushed(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_fs_control_kind_dirty_page_flushed");
}

export function decodeSupervisorFsControlRecord(
  kernelExports: KernelExports,
  view: DataView,
): SupervisorFsControlRecord {
  const recordBytes = supervisorNumericExport(kernelExports, "supervisor_fs_control_record_bytes");
  if (view.byteLength < recordBytes) {
    throw new Error(`supervisor fs control record too short: ${view.byteLength}`);
  }
  const magic = view.getUint32(0, true);
  const version = view.getUint32(4, true);
  const bytes = view.getUint32(8, true);
  const kind = view.getUint32(12, true);
  if (magic !== supervisorNumericExport(kernelExports, "supervisor_fs_control_record_magic")) {
    throw new Error(`supervisor fs control bad magic: 0x${magic.toString(16)}`);
  }
  const expectedVersion = supervisorAbiVersion(kernelExports);
  if (version !== expectedVersion) {
    throw new Error(`supervisor fs control bad version: ${version}`);
  }
  if (bytes < recordBytes || bytes > view.byteLength) {
    throw new Error(`supervisor fs control bad size: ${bytes}`);
  }
  if (
    kind !== supervisorFsControlKindPageCacheMiss(kernelExports)
    && kind !== supervisorFsControlKindPageCacheFill(kernelExports)
    && kind !== supervisorFsControlKindDirtyPagePublished(kernelExports)
    && kind !== supervisorFsControlKindDirtyPageFlushed(kernelExports)
  ) {
    throw new Error(`supervisor fs control bad kind: ${kind}`);
  }
  return {
    kind,
    seq: view.getBigUint64(16, true),
    vmPtr: view.getUint32(24, true),
    processPtr: view.getUint32(28, true),
    pid: view.getUint32(32, true),
    fd: view.getUint32(36, true),
    ino: view.getUint32(40, true),
    opcode: view.getUint32(44, true),
    fileObjectId: view.getBigUint64(48, true),
    pageIndex: view.getBigUint64(56, true),
    offset: view.getBigUint64(64, true),
    length: view.getBigUint64(72, true),
    result: view.getBigInt64(80, true),
    flags: view.getBigUint64(88, true),
    detail0: view.getBigUint64(96, true),
    detail1: view.getBigUint64(104, true),
  };
}

export function readSupervisorFsControlRecord(
  handle: ProcessHandle,
): SupervisorFsControlRecord | undefined {
  const readRecord = handle.kernel.supervisor_fs_control_record;
  if (readRecord === undefined) {
    return undefined;
  }
  const recordBytes = supervisorNumericExport(handle.kernel, "supervisor_fs_control_record_bytes");
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = kernelScratchBase(handle.kernel) + SUPERVISOR_FS_CONTROL_SCRATCH_OFFSET;
  ensureMemoryCapacity(memory, scratch + recordBytes);
  const result = readRecord.call(handle.kernel, scratch, recordBytes);
  if (result < 0 || result > recordBytes) {
    throw new Error(`supervisor fs control read failed: ${result}`);
  }
  if (result === 0) {
    return undefined;
  }
  return decodeSupervisorFsControlRecord(
    handle.kernel,
    new DataView(memory.buffer, scratch, result),
  );
}

export function pollSupervisorFsControlDiagnostic(handle: ProcessHandle): string | undefined {
  const countExport = handle.kernel.supervisor_shadow_fs_control_count;
  if (countExport === undefined) {
    return undefined;
  }
  const count = BigInt(countExport.call(handle.kernel));
  if (count === 0n) {
    return undefined;
  }
  try {
    const record = readSupervisorFsControlRecord(handle);
    if (record === undefined) {
      return undefined;
    }
    const lastSeq = handle.lastSupervisorFsControlSeq ?? 0n;
    if (record.seq <= lastSeq) {
      return undefined;
    }
    handle.lastSupervisorFsControlSeq = record.seq;
    return formatSupervisorFsControlRecord(record);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return `supervisor-fs-control:error=${JSON.stringify(message)}`;
  }
}

export function formatSupervisorFsControlRecord(record: SupervisorFsControlRecord): string {
  return [
    "supervisor-fs-control",
    `kind=${record.kind}`,
    `seq=${record.seq.toString()}`,
    `vm=0x${record.vmPtr.toString(16)}`,
    `process=0x${record.processPtr.toString(16)}`,
    `pid=${record.pid}`,
    `fd=${record.fd}`,
    `ino=${record.ino}`,
    `opcode=${record.opcode}`,
    `fileObject=${record.fileObjectId.toString()}`,
    `page=${record.pageIndex.toString()}`,
    `offset=${record.offset.toString()}`,
    `length=${record.length.toString()}`,
    `result=${record.result.toString()}`,
    `flags=0x${record.flags.toString(16)}`,
    `detail0=${record.detail0.toString()}`,
    `detail1=${record.detail1.toString()}`,
  ].join(":");
}

function supervisorSignalControlKindBoundaryCheck(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_signal_control_kind_boundary_check");
}

function supervisorSignalControlKindSignalDelivered(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_signal_control_kind_signal_delivered");
}

function supervisorSignalControlKindBlockedWaitInterrupted(kernelExports: KernelExports): number {
  return supervisorNumericExport(kernelExports, "supervisor_signal_control_kind_blocked_wait_interrupted");
}

export function decodeSupervisorSignalControlRecord(
  kernelExports: KernelExports,
  view: DataView,
): SupervisorSignalControlRecord {
  const recordBytes = supervisorNumericExport(kernelExports, "supervisor_signal_control_record_bytes");
  if (view.byteLength < recordBytes) {
    throw new Error(`supervisor signal control record too short: ${view.byteLength}`);
  }
  const magic = view.getUint32(0, true);
  const version = view.getUint32(4, true);
  const bytes = view.getUint32(8, true);
  const kind = view.getUint32(12, true);
  if (magic !== supervisorNumericExport(kernelExports, "supervisor_signal_control_record_magic")) {
    throw new Error(`supervisor signal control bad magic: 0x${magic.toString(16)}`);
  }
  const expectedVersion = supervisorAbiVersion(kernelExports);
  if (version !== expectedVersion) {
    throw new Error(`supervisor signal control bad version: ${version}`);
  }
  if (bytes < recordBytes || bytes > view.byteLength) {
    throw new Error(`supervisor signal control bad size: ${bytes}`);
  }
  if (
    kind !== supervisorSignalControlKindBoundaryCheck(kernelExports)
    && kind !== supervisorSignalControlKindSignalDelivered(kernelExports)
    && kind !== supervisorSignalControlKindBlockedWaitInterrupted(kernelExports)
  ) {
    throw new Error(`supervisor signal control bad kind: ${kind}`);
  }
  return {
    kind,
    seq: view.getBigUint64(16, true),
    vmPtr: view.getUint32(24, true),
    processPtr: view.getUint32(28, true),
    pid: view.getUint32(32, true),
    tid: view.getUint32(36, true),
    sig: view.getUint32(40, true),
    status: view.getUint32(44, true),
    sigInfoCode: view.getInt32(48, true),
    boundary: view.getUint32(52, true),
    threadPending: view.getBigUint64(56, true),
    processPending: view.getBigUint64(64, true),
    recordPending: view.getBigUint64(72, true),
    deliverable: view.getBigUint64(80, true),
    mask: view.getBigUint64(88, true),
    pc: view.getBigUint64(96, true),
    detail0: view.getBigUint64(104, true),
    detail1: view.getBigUint64(112, true),
  };
}

export function readSupervisorSignalControlRecord(
  handle: ProcessHandle,
): SupervisorSignalControlRecord | undefined {
  const readRecord = handle.kernel.supervisor_signal_control_record;
  if (readRecord === undefined) {
    return undefined;
  }
  const recordBytes = supervisorNumericExport(handle.kernel, "supervisor_signal_control_record_bytes");
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = kernelScratchBase(handle.kernel) + SUPERVISOR_SIGNAL_CONTROL_SCRATCH_OFFSET;
  ensureMemoryCapacity(memory, scratch + recordBytes);
  const result = readRecord.call(handle.kernel, scratch, recordBytes);
  if (result < 0 || result > recordBytes) {
    throw new Error(`supervisor signal control read failed: ${result}`);
  }
  if (result === 0) {
    return undefined;
  }
  return decodeSupervisorSignalControlRecord(
    handle.kernel,
    new DataView(memory.buffer, scratch, result),
  );
}

export function pollSupervisorSignalControlDiagnostic(handle: ProcessHandle): string | undefined {
  const countExport = handle.kernel.supervisor_shadow_signal_control_count;
  if (countExport === undefined) {
    return undefined;
  }
  const count = BigInt(countExport.call(handle.kernel));
  if (count === 0n) {
    return undefined;
  }
  try {
    const record = readSupervisorSignalControlRecord(handle);
    if (record === undefined) {
      return undefined;
    }
    const lastSeq = handle.lastSupervisorSignalControlSeq ?? 0n;
    if (record.seq <= lastSeq) {
      return undefined;
    }
    handle.lastSupervisorSignalControlSeq = record.seq;
    return formatSupervisorSignalControlRecord(record);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return `supervisor-signal-control:error=${JSON.stringify(message)}`;
  }
}

export function formatSupervisorSignalControlRecord(
  record: SupervisorSignalControlRecord,
): string {
  return [
    "supervisor-signal-control",
    `kind=${record.kind}`,
    `seq=${record.seq.toString()}`,
    `vm=0x${record.vmPtr.toString(16)}`,
    `process=0x${record.processPtr.toString(16)}`,
    `pid=${record.pid}`,
    `tid=${record.tid}`,
    `sig=${record.sig}`,
    `status=${record.status}`,
    `code=${record.sigInfoCode}`,
    `boundary=${record.boundary}`,
    `threadPending=0x${record.threadPending.toString(16)}`,
    `processPending=0x${record.processPending.toString(16)}`,
    `recordPending=0x${record.recordPending.toString(16)}`,
    `deliverable=0x${record.deliverable.toString(16)}`,
    `mask=0x${record.mask.toString(16)}`,
    `pc=0x${record.pc.toString(16)}`,
    `detail0=${record.detail0.toString()}`,
    `detail1=${record.detail1.toString()}`,
  ].join(":");
}
