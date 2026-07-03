import { ensureMemoryCapacity, kernelScratchBase } from "../kernel-scratch.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";

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

export interface SupervisorResumeHostBoundaryDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  clearBlockedWorker(handle: ProcessHandle, worker: Worker): void;
  removeVforkReadyWorker(handle: ProcessHandle, worker: Worker): void;
}

export interface SupervisorResumeHostBoundary {
  selectSupervisorRunnableWorker(handle: ProcessHandle): Worker | null | undefined;
  drainSupervisorResumeQueue(handle: ProcessHandle): void;
}

function supervisorResumeRecordBytes(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_resume_record_bytes;
  if (exportFn === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_resume_record_bytes");
  }
  return exportFn();
}

function supervisorResumeRecordMagic(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_resume_record_magic;
  if (exportFn === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_resume_record_magic");
  }
  return exportFn();
}

function supervisorResumeKindFutexWake(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_resume_kind_futex_wake;
  if (exportFn === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_resume_kind_futex_wake");
  }
  return exportFn();
}

function supervisorResumeKindRunnableThread(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_resume_kind_runnable_thread;
  if (exportFn === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_resume_kind_runnable_thread");
  }
  return exportFn();
}

function supervisorAbiVersion(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_abi_version;
  if (exportFn === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_abi_version");
  }
  return exportFn();
}

function supervisorThreadStateSnapshotMagic(handle: ProcessHandle): number {
  const exportFn = handle.kernel.supervisor_thread_state_snapshot_magic;
  if (exportFn === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_thread_state_snapshot_magic");
  }
  return exportFn();
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
  return handle.kernel.supervisor_thread_state_snapshot_bytes?.() ?? 0;
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
    const result = snapshotExport(vmPtr, scratch, snapshotBytes);
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

export function createSupervisorResumeHostBoundary(
  deps: SupervisorResumeHostBoundaryDeps,
): SupervisorResumeHostBoundary {
  function findWorkerByVmPtr(handle: ProcessHandle, vmPtr: number): Worker | undefined {
    for (const worker of handle.workers) {
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

  function selectSupervisorResumeWorker(
    handle: ProcessHandle,
    record: SupervisorResumeRecord,
    source: string,
  ): Worker | null {
    const selectedWorker = findWorkerByVmPtr(handle, record.vmPtr);
    const selectedThread = selectedWorker === undefined
      ? undefined
      : deps.threadWorkerStates.get(selectedWorker);
    if (selectedWorker === undefined || selectedThread === undefined) {
      deps.appendWorkerDebugLog(
        handle,
        `${source}:missing-selected:0x${record.vmPtr.toString(16)}`
          + `:tid=${record.tid}:seq=${record.seq.toString()}`,
      );
      return null;
    }
    deps.clearBlockedWorker(handle, selectedWorker);
    deps.removeVforkReadyWorker(handle, selectedWorker);
    selectedThread.waitingInWorkerStatus = null;
    selectedThread.runningGeneration = null;
    deps.appendWorkerDebugLog(
      handle,
      `${source}:selected:0x${record.vmPtr.toString(16)}`
        + `:tid=${record.tid}:seq=${record.seq.toString()}`
        + `:process=0x${record.processPtr.toString(16)}`
        + `:status=${record.status}:detail=0x${record.detail0.toString(16)}`
        + formatSupervisorThreadStateSnapshot(handle, record.vmPtr),
    );
    return selectedWorker;
  }

  function selectSupervisorProcessResumeWorker(handle: ProcessHandle): Worker | null | undefined {
    const processResume = handle.kernel.supervisor_process_resume;
    if (processResume === undefined) {
      return undefined;
    }
    const recordBytes = supervisorResumeRecordBytes(handle);
    const memory = handle.kernel.memory ?? handle.memory;
    const scratch = supervisorResumeScratchBase(handle);
    ensureMemoryCapacity(memory, scratch + recordBytes);
    const result = processResume(handle.vmPtr, scratch, recordBytes);
    if (result === 0) {
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-resume:runnable:none:ready=${handle.vforkReadyWorkers.length}`,
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

  function selectSupervisorRunnableWorker(handle: ProcessHandle): Worker | null | undefined {
    return selectSupervisorProcessResumeWorker(handle);
  }

  function recordSupervisorResumeSelection(
    handle: ProcessHandle,
    worker: Worker,
    thread: ThreadWorkerState,
    record: SupervisorResumeRecord,
  ): void {
    const label = supervisorResumeKindLabel(handle, record.kind);
    thread.waitingInWorkerStatus = null;
    thread.runningGeneration = null;
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

  function handleSupervisorResumeRecord(handle: ProcessHandle, record: SupervisorResumeRecord): void {
    if (record.kind === supervisorResumeKindRunnableThread(handle)) {
      selectSupervisorResumeWorker(handle, record, "supervisor-resume:runnable");
      return;
    }
    if (record.kind !== supervisorResumeKindFutexWake(handle)) {
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-resume:unknown-kind:${record.kind}:vm=0x${record.vmPtr.toString(16)}:seq=${record.seq.toString()}`,
      );
      return;
    }
    const worker = findWorkerByVmPtr(handle, record.vmPtr);
    const thread = worker === undefined ? undefined : deps.threadWorkerStates.get(worker);
    if (worker === undefined || thread === undefined) {
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-resume:futex-wake:missing:0x${record.vmPtr.toString(16)}:seq=${record.seq.toString()}`,
      );
      return;
    }
    recordSupervisorResumeSelection(handle, worker, thread, record);
  }

  function drainSupervisorResumeQueue(handle: ProcessHandle): void {
    const nextResume = handle.kernel.supervisor_next_resume;
    if (nextResume === undefined) {
      return;
    }
    const recordBytes = supervisorResumeRecordBytes(handle);
    const memory = handle.kernel.memory ?? handle.memory;
    const scratch = supervisorResumeScratchBase(handle);
    ensureMemoryCapacity(memory, scratch + recordBytes);
    for (let drained = 0; drained < SUPERVISOR_RESUME_DRAIN_LIMIT; drained += 1) {
      const result = nextResume(scratch, recordBytes);
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
      handleSupervisorResumeRecord(handle, record);
    }
  }

  return {
    selectSupervisorRunnableWorker,
    drainSupervisorResumeQueue,
  };
}
