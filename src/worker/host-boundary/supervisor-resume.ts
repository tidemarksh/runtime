import { hostExecutionState } from "../host-execution-state.js";
import { processWorkers } from "../process-worker-state.js";
import { ensureMemoryCapacity, kernelScratchBase } from "../kernel-scratch.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import {
  clearThreadRunningGeneration,
  clearThreadWaitingInWorkerStatus,
  getThreadOwnerAsyncDetail,
  getThreadOwnerAsyncStatus,
  isThreadInitialized,
} from "../thread-host-state.js";
import { clearSupervisorHostTimer } from "../supervisor-host-op.js";
import { supervisorBlockedStatusIsHostWait } from "../../supervisor-blocked-resume.js";

const SUPERVISOR_RESUME_DRAIN_LIMIT = 64;
const SUPERVISOR_RESUME_SCRATCH_OFFSET = 0x28000;
const SUPERVISOR_THREAD_STATE_HEADER_BYTES = 32;
const SUPERVISOR_THREAD_STATE_RECORD_BYTES = 32;

interface SupervisorResumeRecord {
  kind: number;
  seq: bigint;
  vmPtr: number;
  tid: number;
  processPtr: number;
  status: number;
  detail0: bigint;
}

interface SupervisorThreadStateRecord {
  tid: number;
  state: number;
  vmPtr: number;
  status: number;
  blockedAddr: bigint;
  pendingSigset: bigint;
}

interface SelectedSupervisorResumeWorker {
  worker: Worker;
  resumeBlockedSyscall: boolean;
}

export interface SupervisorResumeHostBoundaryDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  clearBlockedWorker(handle: ProcessHandle, worker: Worker): void;
  removeVforkReadyWorker(handle: ProcessHandle, worker: Worker): void;
  ensureWorkerForVm(handle: ProcessHandle, vmPtr: number): void;
}

export interface SupervisorResumeHostBoundary {
  selectSupervisorRunnableWorker(handle: ProcessHandle): SupervisorResumeSelection | null | undefined;
  drainSupervisorResumeQueue(handle: ProcessHandle): SupervisorResumeSelection | null;
}

export interface SupervisorResumeSelection {
  worker: Worker;
  resumeBlockedSyscall: boolean;
}

function supervisorResumeRecordBytes(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_resume_record_bytes;
  if (exportFn === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_resume_record_bytes");
  }
  return exportFn.call(handle.kernel);
}

function supervisorResumeRecordMagic(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_resume_record_magic;
  if (exportFn === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_resume_record_magic");
  }
  return exportFn.call(handle.kernel);
}

function supervisorResumeKindFutexWake(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_resume_kind_futex_wake;
  if (exportFn === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_resume_kind_futex_wake");
  }
  return exportFn.call(handle.kernel);
}

function supervisorResumeKindRunnableThread(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_resume_kind_runnable_thread;
  if (exportFn === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_resume_kind_runnable_thread");
  }
  return exportFn.call(handle.kernel);
}

function notifySupervisorFutexWaiter(handle: ProcessHandle, record: SupervisorResumeRecord): void {
  const guestBase = handle.kernel.thread_guest_base?.(record.vmPtr) ?? 0;
  const guestAddress = Number(record.detail0);
  const absoluteAddress = guestBase + guestAddress;
  const i32 = new Int32Array((handle.kernel.memory ?? handle.memory).buffer);
  if (
    !Number.isSafeInteger(guestAddress)
    || !Number.isSafeInteger(absoluteAddress)
    || (absoluteAddress & (Int32Array.BYTES_PER_ELEMENT - 1)) !== 0
  ) {
    return;
  }
  const index = absoluteAddress / Int32Array.BYTES_PER_ELEMENT;
  if (index < 0 || index >= i32.length) {
    return;
  }
  Atomics.notify(i32, index, 1);
}

function completeSupervisorFutexWake(handle: ProcessHandle, record: SupervisorResumeRecord): void {
  const completionCode = handle.kernel.supervisor_futex_wait_completion_woken;
  const complete = handle.kernel.supervisor_complete_futex_wait;
  if (completionCode === undefined || complete === undefined) {
    throw new Error("kernel supervisor ABI missing futex wake completion exports");
  }
  const result = complete.call(handle.kernel, record.vmPtr, completionCode.call(handle.kernel));
  if (result < 0) {
    throw new Error(`kernel supervisor futex wake completion failed: ${result}`);
  }
}

function supervisorAbiVersion(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_abi_version;
  if (exportFn === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_abi_version");
  }
  return exportFn.call(handle.kernel);
}

function supervisorThreadStateSnapshotMagic(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_thread_state_snapshot_magic;
  if (exportFn === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_thread_state_snapshot_magic");
  }
  return exportFn.call(handle.kernel);
}

function supervisorResumeScratchBase(handle: ProcessHandle): number {
  const exportedScratch = handle.kernel.kernel_scratch_base?.();
  if (exportedScratch !== undefined) {
    return exportedScratch;
  }
  return kernelScratchBase(handle.kernel) + SUPERVISOR_RESUME_SCRATCH_OFFSET;
}

function decodeSupervisorResumeRecord(
  handle: ProcessHandle,
  view: DataView,
): SupervisorResumeRecord | null {
  if (
    view.byteLength < supervisorResumeRecordBytes(handle)
    || view.getUint32(0, true) !== supervisorResumeRecordMagic(handle)
    || view.getUint32(4, true) !== supervisorAbiVersion(handle)
    || view.getUint32(8, true) !== supervisorResumeRecordBytes(handle)
  ) {
    return null;
  }
  return {
    kind: view.getUint32(12, true),
    seq: view.getBigUint64(16, true),
    vmPtr: view.getUint32(24, true),
    tid: view.getUint32(28, true),
    processPtr: view.getUint32(32, true),
    status: view.getUint32(36, true),
    detail0: view.getBigUint64(40, true),
  };
}

function supervisorThreadStateSnapshotBytes(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_thread_state_snapshot_bytes;
  return exportFn === undefined ? 0 : exportFn.call(handle.kernel);
}

function decodeSupervisorThreadStateRecord(
  view: DataView,
  offset: number,
): SupervisorThreadStateRecord {
  return {
    tid: view.getUint32(offset, true),
    state: view.getUint32(offset + 4, true),
    vmPtr: view.getUint32(offset + 8, true),
    status: view.getUint32(offset + 12, true),
    blockedAddr: view.getBigUint64(offset + 16, true),
    pendingSigset: view.getBigUint64(offset + 24, true),
  };
}

function formatSupervisorThreadStateSnapshot(
  handle: ProcessHandle,
  vmPtr: number,
): string {
  const snapshotExport = handle.kernel.supervisor_thread_state_snapshot;
  const snapshotBytes = supervisorThreadStateSnapshotBytes(handle);
  if (snapshotExport === undefined || snapshotBytes === 0) {
    return "";
  }
  if (!Number.isFinite(snapshotBytes) || snapshotBytes < SUPERVISOR_THREAD_STATE_HEADER_BYTES) {
    return `:snapshot=invalid-bytes:${snapshotBytes}`;
  }
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = supervisorResumeScratchBase(handle);
  ensureMemoryCapacity(memory, scratch + snapshotBytes);
  try {
    const result = snapshotExport.call(handle.kernel, vmPtr, scratch, snapshotBytes);
    if (result <= 0 || result > snapshotBytes || result < SUPERVISOR_THREAD_STATE_HEADER_BYTES) {
      return `:snapshot=err${result}`;
    }
    const view = new DataView(memory.buffer, scratch, result);
    if (
      view.getUint32(0, true) !== supervisorThreadStateSnapshotMagic(handle)
      || view.getUint32(4, true) !== supervisorAbiVersion(handle)
    ) {
      return ":snapshot=decode-error";
    }
    const totalBytes = view.getUint32(8, true);
    const maxThreads = view.getUint32(12, true);
    const processPtr = view.getUint32(16, true);
    const ownerVmPtr = view.getUint32(20, true);
    const recordBytes = view.getUint32(24, true);
    if (
      totalBytes < SUPERVISOR_THREAD_STATE_HEADER_BYTES
      || totalBytes > result
      || recordBytes < SUPERVISOR_THREAD_STATE_RECORD_BYTES
    ) {
      return ":snapshot=decode-error";
    }
    const availableRecords = Math.min(
      maxThreads,
      Math.floor((totalBytes - SUPERVISOR_THREAD_STATE_HEADER_BYTES) / recordBytes),
    );
    const records: string[] = [];
    for (let slot = 0; slot < availableRecords; slot += 1) {
      const offset = SUPERVISOR_THREAD_STATE_HEADER_BYTES + slot * recordBytes;
      const record = decodeSupervisorThreadStateRecord(view, offset);
      if (
        record.tid === 0
        && record.state === 0
        && record.vmPtr === 0
        && record.status === 0
        && record.blockedAddr === 0n
        && record.pendingSigset === 0n
      ) {
        continue;
      }
      let encoded = `${record.tid}/${record.state}/0x${record.vmPtr.toString(16)}`
        + `/${record.status}/0x${record.blockedAddr.toString(16)}`;
      if (record.pendingSigset !== 0n) {
        encoded += `/sig=0x${record.pendingSigset.toString(16)}`;
      }
      records.push(encoded);
    }
    return `:snapshot=process=0x${processPtr.toString(16)}`
      + `:owner=0x${ownerVmPtr.toString(16)}`
      + `:threads=${records.join(",") || "empty"}`;
  } catch {
    return ":snapshot=err";
  }
}

export function supervisorThreadStateSnapshotStatus(
  handle: ProcessHandle,
  vmPtr: number,
): number | null {
  const snapshotExport = handle.kernel.supervisor_thread_state_snapshot;
  const snapshotBytes = supervisorThreadStateSnapshotBytes(handle);
  if (snapshotExport === undefined || snapshotBytes === 0) {
    return null;
  }
  if (!Number.isFinite(snapshotBytes) || snapshotBytes < SUPERVISOR_THREAD_STATE_HEADER_BYTES) {
    return null;
  }
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = supervisorResumeScratchBase(handle);
  ensureMemoryCapacity(memory, scratch + snapshotBytes);
  try {
    const result = snapshotExport.call(handle.kernel, vmPtr, scratch, snapshotBytes);
    if (result <= 0 || result > snapshotBytes || result < SUPERVISOR_THREAD_STATE_HEADER_BYTES) {
      return null;
    }
    const view = new DataView(memory.buffer, scratch, result);
    if (
      view.getUint32(0, true) !== supervisorThreadStateSnapshotMagic(handle)
      || view.getUint32(4, true) !== supervisorAbiVersion(handle)
    ) {
      return null;
    }
    const totalBytes = view.getUint32(8, true);
    const maxThreads = view.getUint32(12, true);
    const recordBytes = view.getUint32(24, true);
    if (
      totalBytes < SUPERVISOR_THREAD_STATE_HEADER_BYTES
      || totalBytes > result
      || recordBytes < SUPERVISOR_THREAD_STATE_RECORD_BYTES
    ) {
      return null;
    }
    const availableRecords = Math.min(
      maxThreads,
      Math.floor((totalBytes - SUPERVISOR_THREAD_STATE_HEADER_BYTES) / recordBytes),
    );
    for (let slot = 0; slot < availableRecords; slot += 1) {
      const offset = SUPERVISOR_THREAD_STATE_HEADER_BYTES + slot * recordBytes;
      const record = decodeSupervisorThreadStateRecord(view, offset);
      if (record.vmPtr === vmPtr) {
        return record.status;
      }
    }
  } catch {
    return null;
  }
  return null;
}

export function createSupervisorResumeHostBoundary(
  deps: SupervisorResumeHostBoundaryDeps,
): SupervisorResumeHostBoundary {
  const deferredResumeRecords = new WeakMap<ProcessHandle, SupervisorResumeRecord[]>();

  function findWorkerByVmPtr(handle: ProcessHandle, vmPtr: number): Worker | undefined {
    for (const worker of processWorkers(handle)) {
      const thread = deps.threadWorkerStates.get(worker);
      if (thread?.vmPtr === vmPtr) {
        return worker;
      }
    }
    return undefined;
  }

  function supervisorResumeKindLabel(handle: ProcessHandle, kind: number): string {
    if (kind === supervisorResumeKindFutexWake(handle)) {
      return "futex-wake";
    }
    if (kind === supervisorResumeKindRunnableThread(handle)) {
      return "runnable";
    }
    return `kind-${kind}`;
  }

  function ownerAsyncStatusForResumeRecord(
    handle: ProcessHandle,
    record: SupervisorResumeRecord,
  ): number | null {
    const worker = findWorkerByVmPtr(handle, record.vmPtr);
    const thread = worker === undefined ? undefined : deps.threadWorkerStates.get(worker);
    if (thread === undefined) {
      return null;
    }
    return getThreadOwnerAsyncStatus(thread);
  }

  function deferResumeRecord(
    handle: ProcessHandle,
    record: SupervisorResumeRecord,
    ownerAsyncStatus: number,
  ): void {
    const records = deferredResumeRecords.get(handle) ?? [];
    records.push(record);
    deferredResumeRecords.set(handle, records);
    const worker = findWorkerByVmPtr(handle, record.vmPtr);
    const thread = worker === undefined ? undefined : deps.threadWorkerStates.get(worker);
    const ownerAsync = getThreadOwnerAsyncDetail(thread);
    const ownerAsyncAgeMs = ownerAsync?.startedAtMs === null || ownerAsync?.startedAtMs === undefined
      ? "null"
      : Math.max(0, Date.now() - ownerAsync.startedAtMs);
    deps.appendWorkerDebugLog(
      handle,
      `supervisor-resume:${supervisorResumeKindLabel(handle, record.kind)}:defer-owner-async`
        + `:0x${record.vmPtr.toString(16)}:tid=${record.tid}:seq=${record.seq.toString()}`
        + `:status=${record.status}:owner=${ownerAsyncStatus}`
        + `:ownerDetail=${ownerAsync?.detail ?? "unknown"}:ownerAgeMs=${ownerAsyncAgeMs}`,
    );
  }

  function selectSupervisorResumeWorker(
    handle: ProcessHandle,
    record: SupervisorResumeRecord,
    source: string,
    options: { resumeBlockedSyscall?: boolean } = {},
  ): SelectedSupervisorResumeWorker | null {
    const selectedWorker = findWorkerByVmPtr(handle, record.vmPtr);
    const selectedThread = selectedWorker === undefined
      ? undefined
      : deps.threadWorkerStates.get(selectedWorker);
    if (selectedWorker === undefined || selectedThread === undefined) {
      deps.ensureWorkerForVm(handle, record.vmPtr);
      deps.appendWorkerDebugLog(
        handle,
        `${source}:missing-selected:0x${record.vmPtr.toString(16)}`
          + `:tid=${record.tid}:seq=${record.seq.toString()}`,
      );
      return null;
    }
    if (!isThreadInitialized(selectedThread)) {
      deps.appendWorkerDebugLog(
        handle,
        `${source}:await-reattach:0x${record.vmPtr.toString(16)}`
          + `:tid=${record.tid}:seq=${record.seq.toString()}`,
      );
      return null;
    }
    const blocked = hostExecutionState(handle).hostBlockedWorkers.get(selectedWorker);
    let resumeBlockedSyscall = options.resumeBlockedSyscall === true;
    if (blocked !== undefined && blocked.vmPtr === record.vmPtr) {
      const currentStatus = supervisorThreadStateSnapshotStatus(handle, record.vmPtr)
        ?? handle.kernel.thread_status(record.vmPtr);
      if (currentStatus === blocked.status) {
        if (
          record.kind === supervisorResumeKindRunnableThread(handle)
          && record.status === handle.kernel.abi_status_ok()
          && supervisorBlockedStatusIsHostWait(handle.kernel, blocked.status)
        ) {
          resumeBlockedSyscall = true;
        } else {
          deps.appendWorkerDebugLog(
            handle,
            `${source}:defer-blocked:0x${record.vmPtr.toString(16)}`
              + `:tid=${record.tid}:seq=${record.seq.toString()}`
              + `:status=${record.status}:current=${currentStatus}:blocked=${blocked.status}`
              + formatSupervisorThreadStateSnapshot(handle, record.vmPtr),
          );
          return null;
        }
      }
    }
    const ownerAsyncStatus = getThreadOwnerAsyncStatus(selectedThread);
    if (ownerAsyncStatus !== null) {
      const ownerAsync = getThreadOwnerAsyncDetail(selectedThread);
      const ownerAsyncAgeMs = ownerAsync?.startedAtMs === null || ownerAsync?.startedAtMs === undefined
        ? "null"
        : Math.max(0, Date.now() - ownerAsync.startedAtMs);
      deps.appendWorkerDebugLog(
        handle,
        `${source}:defer-owner-async:0x${record.vmPtr.toString(16)}`
          + `:tid=${record.tid}:seq=${record.seq.toString()}`
          + `:status=${record.status}:owner=${ownerAsyncStatus}`
          + `:ownerDetail=${ownerAsync?.detail ?? "unknown"}:ownerAgeMs=${ownerAsyncAgeMs}`,
      );
      return null;
    }
    deps.clearBlockedWorker(handle, selectedWorker);
    deps.removeVforkReadyWorker(handle, selectedWorker);
    clearSupervisorHostTimer(handle, record.vmPtr);
    clearThreadWaitingInWorkerStatus(selectedThread);
    clearThreadRunningGeneration(selectedThread);
    deps.appendWorkerDebugLog(
      handle,
      `${source}:selected:0x${record.vmPtr.toString(16)}`
        + `:tid=${record.tid}:seq=${record.seq.toString()}`
        + `:process=0x${record.processPtr.toString(16)}`
        + `:status=${record.status}:detail=0x${record.detail0.toString(16)}`
        + formatSupervisorThreadStateSnapshot(handle, record.vmPtr),
    );
    return { worker: selectedWorker, resumeBlockedSyscall };
  }

  function selectSupervisorProcessResumeWorker(handle: ProcessHandle): SelectedSupervisorResumeWorker | null | undefined {
    const processResume = handle.kernel.supervisor_process_resume;
    if (processResume === undefined) {
      return undefined;
    }
    const recordBytes = supervisorResumeRecordBytes(handle);
    const memory = handle.kernel.memory ?? handle.memory;
    const scratch = supervisorResumeScratchBase(handle);
    ensureMemoryCapacity(memory, scratch + recordBytes);
    const result = processResume.call(handle.kernel, handle.vmPtr, scratch, recordBytes);
    if (result === 0) {
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-resume:runnable:none:ready=${hostExecutionState(handle).vforkReadyWorkers.length}`
          + formatSupervisorThreadStateSnapshot(handle, handle.vmPtr),
      );
      return null;
    }
    if (result < 0 || result > recordBytes) {
      deps.appendWorkerDebugLog(handle, `supervisor-resume:runnable:poll-error:${result}`);
      return null;
    }
    const record = decodeSupervisorResumeRecord(handle, new DataView(memory.buffer, scratch, result));
    if (record === null) {
      deps.appendWorkerDebugLog(handle, `supervisor-resume:runnable:decode-error:${result}`);
      return null;
    }
    if (record.kind !== supervisorResumeKindRunnableThread(handle)) {
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-resume:runnable:unexpected-kind:${record.kind}`
          + `:vm=0x${record.vmPtr.toString(16)}:seq=${record.seq.toString()}`,
      );
      return null;
    }
    return selectSupervisorResumeWorker(handle, record, "supervisor-resume:runnable");
  }

  function selectSupervisorRunnableWorker(handle: ProcessHandle): SelectedSupervisorResumeWorker | null | undefined {
    return selectSupervisorProcessResumeWorker(handle);
  }

  function recordSupervisorResumeSelection(
    handle: ProcessHandle,
    worker: Worker,
    thread: ThreadWorkerState,
    record: SupervisorResumeRecord,
  ): void {
    const label = supervisorResumeKindLabel(handle, record.kind);
    if (record.kind === supervisorResumeKindFutexWake(handle)) {
      completeSupervisorFutexWake(handle, record);
      notifySupervisorFutexWaiter(handle, record);
    }
    clearThreadWaitingInWorkerStatus(thread);
    clearThreadRunningGeneration(thread);
    deps.appendWorkerDebugLog(
      handle,
      `supervisor-resume:${label}:observed:0x${thread.vmPtr.toString(16)}`
        + `:tid=${record.tid}:seq=${record.seq.toString()}`
        + `:process=0x${record.processPtr.toString(16)}`
        + `:detail=0x${record.detail0.toString(16)}`
        + `:status=${record.status}`
        + formatSupervisorThreadStateSnapshot(handle, record.vmPtr),
    );
  }

  function handleSupervisorResumeRecord(handle: ProcessHandle, record: SupervisorResumeRecord): SupervisorResumeSelection | null {
    if (record.kind === supervisorResumeKindRunnableThread(handle)) {
      const selection = selectSupervisorResumeWorker(handle, record, "supervisor-resume:runnable");
      return selection === null
        ? null
        : selection;
    }
    if (record.kind !== supervisorResumeKindFutexWake(handle)) {
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-resume:unknown-kind:${record.kind}:vm=0x${record.vmPtr.toString(16)}:seq=${record.seq.toString()}`,
      );
      return null;
    }
    const worker = findWorkerByVmPtr(handle, record.vmPtr);
    const thread = worker === undefined ? undefined : deps.threadWorkerStates.get(worker);
    if (worker === undefined || thread === undefined) {
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-resume:futex-wake:missing:0x${record.vmPtr.toString(16)}:seq=${record.seq.toString()}`,
      );
      return null;
    }
    recordSupervisorResumeSelection(handle, worker, thread, record);
    const selectedWorker = selectSupervisorResumeWorker(
      handle,
      record,
      "supervisor-resume:futex-wake",
      { resumeBlockedSyscall: true },
    );
    return selectedWorker === null
      ? null
      : selectedWorker;
  }

  function drainSupervisorResumeQueue(handle: ProcessHandle): SupervisorResumeSelection | null {
    const nextResume = handle.kernel.supervisor_next_resume;
    if (nextResume === undefined) {
      return null;
    }
    const recordBytes = supervisorResumeRecordBytes(handle);
    const memory = handle.kernel.memory ?? handle.memory;
    const scratch = supervisorResumeScratchBase(handle);
    ensureMemoryCapacity(memory, scratch + recordBytes);
    let selected: SupervisorResumeSelection | null = null;
    for (let drained = 0; drained < SUPERVISOR_RESUME_DRAIN_LIMIT; drained += 1) {
      const deferred = deferredResumeRecords.get(handle);
      if (deferred !== undefined && deferred.length > 0) {
        const record = deferred[0];
        const ownerAsyncStatus = ownerAsyncStatusForResumeRecord(handle, record);
        if (ownerAsyncStatus !== null) {
          break;
        }
        deferred.shift();
        if (deferred.length === 0) {
          deferredResumeRecords.delete(handle);
        }
        const recordSelection = handleSupervisorResumeRecord(handle, record);
        if (recordSelection !== null) {
          selected = recordSelection;
          break;
        }
        continue;
      }
      const result = nextResume.call(handle.kernel, scratch, recordBytes);
      if (result === 0) {
        break;
      }
      if (result < 0 || result > recordBytes) {
        deps.appendWorkerDebugLog(handle, `supervisor-resume:poll-error:${result}`);
        break;
      }
      const record = decodeSupervisorResumeRecord(handle, new DataView(memory.buffer, scratch, result));
      if (record === null) {
        deps.appendWorkerDebugLog(handle, `supervisor-resume:decode-error:${result}`);
        continue;
      }
      const ownerAsyncStatus = ownerAsyncStatusForResumeRecord(handle, record);
      if (ownerAsyncStatus !== null) {
        deferResumeRecord(handle, record, ownerAsyncStatus);
        break;
      }
      const recordSelection = handleSupervisorResumeRecord(handle, record);
      if (selected === null && recordSelection !== null) {
        selected = recordSelection;
        break;
      }
    }
    return selected;
  }

  return {
    selectSupervisorRunnableWorker,
    drainSupervisorResumeQueue,
  };
}
