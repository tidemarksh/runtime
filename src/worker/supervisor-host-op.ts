import { ensureMemoryCapacity, kernelScratchBase } from "./kernel-scratch.js";
import type { WorkerMessageHandlerDeps } from "./message-handler/types.js";
import type { ProcessHandle } from "./types.js";

const SUPERVISOR_HOST_OP_RECORD_BYTES = 64;
const SUPERVISOR_HOST_OP_RECORD_MAGIC = 0x4f48_5354;
const SUPERVISOR_HOST_OP_KIND_SPAWN_THREAD_WORKER = 1;
const SUPERVISOR_HOST_OP_KIND_ARM_HOST_TIMER = 12;
const SUPERVISOR_HOST_OP_SCRATCH_OFFSET = 0x2c000;
const SUPERVISOR_HOST_COMPLETION_RECORD_BYTES = 64;
const SUPERVISOR_HOST_COMPLETION_RECORD_MAGIC = 0x4348_5354;
const SUPERVISOR_HOST_COMPLETION_OUTCOME_SUCCESS = 1;
const SUPERVISOR_HOST_COMPLETION_OUTCOME_FAILURE = 2;
const SUPERVISOR_HOST_COMPLETION_SCRATCH_OFFSET = 0x2d000;

export interface SupervisorHostOpRecord {
  kind: number;
  seq: bigint;
  parentVmPtr: number;
  childVmPtr: number;
  tid: number;
  processPtr: number;
  status: number;
  flags: bigint;
  childTidPtr: bigint;
}

export type SupervisorCloneSpawnHostOp = SupervisorHostOpRecord;

function supervisorHostOpScratchBase(handle: ProcessHandle): number {
  return kernelScratchBase(handle.kernel) + SUPERVISOR_HOST_OP_SCRATCH_OFFSET;
}

function supervisorHostOpKindSpawnThreadWorker(handle: ProcessHandle): number {
  return handle.kernel.supervisor_host_op_kind_spawn_thread_worker?.()
    ?? SUPERVISOR_HOST_OP_KIND_SPAWN_THREAD_WORKER;
}

function supervisorHostOpKindArmHostTimer(handle: ProcessHandle): number {
  return handle.kernel.supervisor_host_op_kind_arm_host_timer?.()
    ?? SUPERVISOR_HOST_OP_KIND_ARM_HOST_TIMER;
}

function decodeSupervisorHostOpRecord(
  handle: ProcessHandle,
  view: DataView,
  context: string,
): SupervisorHostOpRecord {
  if (view.byteLength < SUPERVISOR_HOST_OP_RECORD_BYTES) {
    throw new Error(`${context} record too short: ${view.byteLength}`);
  }
  const magic = view.getUint32(0, true);
  const version = view.getUint32(4, true);
  const bytes = view.getUint32(8, true);
  const kind = view.getUint32(12, true);
  if (magic !== SUPERVISOR_HOST_OP_RECORD_MAGIC) {
    throw new Error(`${context} bad magic: 0x${magic.toString(16)}`);
  }
  const expectedVersion = handle.kernel.supervisor_abi_version?.() ?? version;
  if (version !== expectedVersion) {
    throw new Error(`${context} bad version: ${version}`);
  }
  if (bytes < SUPERVISOR_HOST_OP_RECORD_BYTES || bytes > view.byteLength) {
    throw new Error(`${context} bad size: ${bytes}`);
  }
  const record: SupervisorCloneSpawnHostOp = {
    kind,
    seq: view.getBigUint64(16, true),
    parentVmPtr: view.getUint32(24, true),
    childVmPtr: view.getUint32(28, true),
    tid: view.getUint32(32, true),
    processPtr: view.getUint32(36, true),
    status: view.getUint32(40, true),
    flags: view.getBigUint64(48, true),
    childTidPtr: view.getBigUint64(56, true),
  };
  return record;
}

function decodeSupervisorCloneSpawnHostOp(
  handle: ProcessHandle,
  view: DataView,
  parentVmPtr: number,
): SupervisorCloneSpawnHostOp {
  const record = decodeSupervisorHostOpRecord(handle, view, "supervisor clone spawn HostOp");
  if (record.kind !== supervisorHostOpKindSpawnThreadWorker(handle)) {
    throw new Error(`supervisor clone spawn HostOp bad kind: ${record.kind}`);
  }
  if (record.parentVmPtr !== parentVmPtr) {
    throw new Error(
      `supervisor clone spawn HostOp parent mismatch: `
        + `0x${record.parentVmPtr.toString(16)} != 0x${parentVmPtr.toString(16)}`,
    );
  }
  if (record.childVmPtr === 0) {
    throw new Error(`supervisor clone spawn HostOp missing child for parent=0x${parentVmPtr.toString(16)}`);
  }
  if (record.status !== handle.kernel.abi_status_ok()) {
    throw new Error(`supervisor clone spawn HostOp child status not OK: ${record.status}`);
  }
  return record;
}

function decodeSupervisorArmHostTimerOp(
  handle: ProcessHandle,
  view: DataView,
  vmPtr: number,
): SupervisorHostOpRecord {
  const record = decodeSupervisorHostOpRecord(handle, view, "supervisor arm host timer HostOp");
  if (record.kind !== supervisorHostOpKindArmHostTimer(handle)) {
    throw new Error(`supervisor arm host timer HostOp bad kind: ${record.kind}`);
  }
  if (record.parentVmPtr !== vmPtr) {
    throw new Error(
      `supervisor arm host timer HostOp vm mismatch: `
        + `0x${record.parentVmPtr.toString(16)} != 0x${vmPtr.toString(16)}`,
    );
  }
  return record;
}

function pollSupervisorHostOpKind(
  handle: ProcessHandle,
  kind: number,
  recordBytes: number,
  scratch: number,
): number | undefined {
  const nextHostOpKind = handle.kernel.supervisor_next_host_op_kind;
  if (nextHostOpKind !== undefined) {
    return nextHostOpKind(kind, scratch, recordBytes);
  }
  return handle.kernel.supervisor_next_host_op?.(scratch, recordBytes);
}

export function drainSupervisorCloneSpawnHostOp(
  handle: ProcessHandle,
  parentVmPtr: number,
): SupervisorCloneSpawnHostOp | null | undefined {
  const recordBytesExport = handle.kernel.supervisor_host_op_record_bytes;
  if (
    recordBytesExport === undefined
    || (
      handle.kernel.supervisor_next_host_op_kind === undefined
      && handle.kernel.supervisor_next_host_op === undefined
    )
  ) {
    return undefined;
  }
  const recordBytes = recordBytesExport();
  if (recordBytes < SUPERVISOR_HOST_OP_RECORD_BYTES) {
    throw new Error(`supervisor clone spawn HostOp record bytes too small: ${recordBytes}`);
  }
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = supervisorHostOpScratchBase(handle);
  ensureMemoryCapacity(memory, scratch + recordBytes);
  const result = pollSupervisorHostOpKind(
    handle,
    supervisorHostOpKindSpawnThreadWorker(handle),
    recordBytes,
    scratch,
  );
  if (result === undefined) {
    return undefined;
  }
  if (result === 0) {
    return null;
  }
  if (result < 0 || result > recordBytes) {
    throw new Error(`supervisor clone spawn HostOp poll failed: ${result}`);
  }
  return decodeSupervisorCloneSpawnHostOp(
    handle,
    new DataView(memory.buffer, scratch, result),
    parentVmPtr,
  );
}

export function drainSupervisorArmHostTimerHostOp(
  handle: ProcessHandle,
  vmPtr: number,
): SupervisorHostOpRecord | null | undefined {
  const recordBytesExport = handle.kernel.supervisor_host_op_record_bytes;
  if (
    recordBytesExport === undefined
    || (
      handle.kernel.supervisor_next_host_op_kind === undefined
      && handle.kernel.supervisor_next_host_op === undefined
    )
  ) {
    return undefined;
  }
  const recordBytes = recordBytesExport();
  if (recordBytes < SUPERVISOR_HOST_OP_RECORD_BYTES) {
    throw new Error(`supervisor arm host timer HostOp record bytes too small: ${recordBytes}`);
  }
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = supervisorHostOpScratchBase(handle);
  ensureMemoryCapacity(memory, scratch + recordBytes);
  const result = pollSupervisorHostOpKind(
    handle,
    supervisorHostOpKindArmHostTimer(handle),
    recordBytes,
    scratch,
  );
  if (result === undefined) {
    return undefined;
  }
  if (result === 0) {
    return null;
  }
  if (result < 0 || result > recordBytes) {
    throw new Error(`supervisor arm host timer HostOp poll failed: ${result}`);
  }
  return decodeSupervisorArmHostTimerOp(
    handle,
    new DataView(memory.buffer, scratch, result),
    vmPtr,
  );
}

function supervisorHostCompletionOutcomeSuccess(handle: ProcessHandle): number {
  return handle.kernel.supervisor_host_completion_outcome_success?.()
    ?? SUPERVISOR_HOST_COMPLETION_OUTCOME_SUCCESS;
}

function supervisorHostCompletionOutcomeFailure(handle: ProcessHandle): number {
  return handle.kernel.supervisor_host_completion_outcome_failure?.()
    ?? SUPERVISOR_HOST_COMPLETION_OUTCOME_FAILURE;
}

function submitSupervisorHostOpCompletion(
  handle: ProcessHandle,
  record: SupervisorHostOpRecord,
  outcome: number,
  detail: number,
): number | undefined {
  const completeHostOp = handle.kernel.supervisor_complete_host_op;
  if (completeHostOp === undefined) {
    return undefined;
  }
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = kernelScratchBase(handle.kernel) + SUPERVISOR_HOST_COMPLETION_SCRATCH_OFFSET;
  ensureMemoryCapacity(memory, scratch + SUPERVISOR_HOST_COMPLETION_RECORD_BYTES);
  const view = new DataView(memory.buffer, scratch, SUPERVISOR_HOST_COMPLETION_RECORD_BYTES);
  view.setUint32(0, SUPERVISOR_HOST_COMPLETION_RECORD_MAGIC, true);
  view.setUint32(4, handle.kernel.supervisor_abi_version?.() ?? 1, true);
  view.setUint32(8, SUPERVISOR_HOST_COMPLETION_RECORD_BYTES, true);
  view.setUint32(12, record.kind, true);
  view.setBigUint64(16, record.seq, true);
  view.setUint32(24, record.parentVmPtr, true);
  view.setUint32(28, record.childVmPtr, true);
  view.setUint32(32, record.tid, true);
  view.setUint32(36, outcome, true);
  view.setUint32(40, detail, true);
  view.setUint32(44, 0, true);
  view.setBigUint64(48, record.flags, true);
  view.setBigUint64(56, record.childTidPtr, true);
  return completeHostOp(scratch, SUPERVISOR_HOST_COMPLETION_RECORD_BYTES);
}

export function armSupervisorHostTimer(
  handle: ProcessHandle,
  record: SupervisorHostOpRecord,
  deps: Pick<
    WorkerMessageHandlerDeps,
    "appendRuntimeLifecycleEvent" | "appendWorkerDebugLog" | "failProcess" | "pumpProcess"
  >,
): void {
  const delayMs = Number(
    record.flags > BigInt(0x7fff_ffff)
      ? 0x7fff_ffffn
      : record.flags,
  );
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `supervisor-host-timer-arm:${handle.pid}:vm=0x${record.parentVmPtr.toString(16)}`
      + `:seq=${record.seq.toString()}:delay=${delayMs}`,
  );
  globalThis.setTimeout(() => {
    try {
      const status = submitSupervisorHostOpCompletion(
        handle,
        record,
        supervisorHostCompletionOutcomeSuccess(handle),
        0,
      ) ?? 0;
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-host-timer-fired:vm=0x${record.parentVmPtr.toString(16)}`
          + `:seq=${record.seq.toString()}:status=${status}`,
      );
      deps.pumpProcess(handle);
    } catch (error) {
      submitSupervisorHostOpCompletion(
        handle,
        record,
        supervisorHostCompletionOutcomeFailure(handle),
        0,
      );
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    }
  }, delayMs);
}
