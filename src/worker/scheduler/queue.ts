import {
  setWorkerErrorHandler,
  setWorkerMessageErrorHandler,
  setWorkerMessageHandler,
} from "../../bridge/compat.js";
import { publishRuntimeBridgeEpoch } from "../../bridge/runtime-state.js";
import { ensureMemoryCapacity, kernelScratchBase } from "../kernel-scratch.js";
import { createReadyWorkerFairness } from "./fairness.js";
import {
  threadHasCanonicalFutexBlock,
} from "./futex.js";
import { clearStaleForkSharedSyncGate } from "./fork-shared-gate.js";
import type {
  AttachThreadWorkerRequest,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  ThreadWorkerOutbound,
} from "../../messages.js";
import { kernelStackSlotForWorker } from "../pool.js";
import {
  readSupervisorFaultExitDecision,
  supervisorFaultExitKindProcessExit,
} from "../supervisor-shadow.js";
import { snapshotProcessFdEntriesForThread } from "../ownership/fd-table.js";
import {
  restoreRegularFileOffsetsForStep,
  snapshotOfdSlotsForThread,
} from "../ownership/ofd-table.js";
import {
  composeHandlePipeUsed,
  preserveHostStdioPipeControlForStep,
} from "../ownership/pipe-control.js";
import { snapshotSocketStateForThread } from "../ownership/socket-control.js";
import type {
  ThreadWorkerInbound,
  WorkerSchedulerQueueHelpers,
} from "./types.js";

const DEFAULT_NODE_SCHEDULER_STEP_BUDGET = 5_000_000;
const DEFAULT_BROWSER_SCHEDULER_STEP_BUDGET = 10_000;
const CONTENDED_SCHEDULER_STEP_BUDGET = 100_000;
const MIN_SCHEDULER_STEP_BUDGET = 1_000;
const MAX_SCHEDULER_STEP_BUDGET = 5_000_000;
const SCHEDULER_STEP_BUDGET = readSchedulerStepBudget();
const SCHEDULER_QUEUE_DEBUG_LOG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_SCHEDULER_QUEUE_DEBUG_LOG ?? "") === "1";
const SCHEDULER_QUEUE_DEBUG_LOG_LIMIT = 256;
const SCHEDULER_QUEUE_DEBUG_LOG_PREFIXES = [
  "pump-",
  "ready:",
  "runnable-",
  "step:",
] as const;
let nextThreadWorkerGeneration = 1;
const HOT_PC_COUNTER_LIMIT = 32;
const STALE_SLEEP_RUNNING_WORKER_REPAIR_MS = 2_000;
const SUPERVISOR_RESUME_RECORD_BYTES = 48;
const SUPERVISOR_RESUME_RECORD_MAGIC = 0x5255_5354;
const SUPERVISOR_RESUME_KIND_FUTEX_WAKE = 1;
const SUPERVISOR_RESUME_KIND_RUNNABLE_THREAD = 2;
const SUPERVISOR_RESUME_DRAIN_LIMIT = 64;
const SUPERVISOR_RESUME_SCRATCH_OFFSET = 0x28000;
const SUPERVISOR_THREAD_STATE_SNAPSHOT_MAGIC = 0x4854_5354;
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

function readSchedulerStepBudget(): number {
  const hostConfigured = (globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
    TIDEMARK_SCHEDULER_STEP_BUDGET?: unknown;
  }).TIDEMARK_SCHEDULER_STEP_BUDGET;
  const envConfigured = (globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_SCHEDULER_STEP_BUDGET;
  const parsed = Number.parseInt(String(hostConfigured ?? envConfigured ?? ""), 10);
  if (Number.isFinite(parsed)) {
    return Math.min(MAX_SCHEDULER_STEP_BUDGET, Math.max(MIN_SCHEDULER_STEP_BUDGET, parsed));
  }
  const processLike = (globalThis as typeof globalThis & {
    process?: { versions?: { node?: string } };
  }).process;
  const isNodeRuntime = typeof processLike?.versions?.node === "string";
  const isBrowserRuntime = typeof window !== "undefined" && !isNodeRuntime;
  return isBrowserRuntime
    ? DEFAULT_BROWSER_SCHEDULER_STEP_BUDGET
    : DEFAULT_NODE_SCHEDULER_STEP_BUDGET;
}

function formatChildExitRecords(
  handle: ProcessHandle,
): Array<{ childPid: number; waitStatus: number; parentPid: number }> {
  return [...handle.childExitSyncRecords].map(([childPid, waitStatus]) => ({
    childPid,
    waitStatus,
    parentPid: handle.pid,
  }));
}

interface WorkerSchedulerQueueDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void;
  completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number, exitVmPtr?: number): Promise<void>;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleWorkerMessage(handle: ProcessHandle, worker: Worker, msg: ThreadWorkerOutbound): void;
}

function formatStepDebugSuffix(handle: ProcessHandle, vmPtr: number): string {
  const parts: string[] = [];
  const pid = handle.kernel.process_current_pid?.(vmPtr);
  if (pid !== undefined) {
    parts.push(`pid=${pid}`);
  }
  const tid = handle.kernel.thread_tid?.(vmPtr);
  if (tid !== undefined) {
    parts.push(`tid=${tid}`);
  }
  const pc = handle.kernel.thread_pc?.(vmPtr);
  if (pc !== undefined) {
    parts.push(`pc=0x${pc.toString(16)}`);
  }
  const ra = handle.kernel.thread_ra?.(vmPtr);
  if (ra !== undefined) {
    parts.push(`ra=0x${ra.toString(16)}`);
  }
  const sp = handle.kernel.thread_sp?.(vmPtr);
  if (sp !== undefined) {
    parts.push(`sp=0x${sp.toString(16)}`);
    for (const byteOffset of [32, 24, 16, 8]) {
      const stackWord = handle.kernel.thread_stack_minus?.(vmPtr, byteOffset);
      if (stackWord !== undefined) {
        parts.push(`stkm${byteOffset}=0x${stackWord.toString(16)}`);
      }
    }
  }
  for (let index = 0; index <= 32; index += 1) {
    const stackWord = handle.kernel.thread_stack_word?.(vmPtr, index);
    if (stackWord !== undefined) {
      parts.push(`stk${index}=0x${stackWord.toString(16)}`);
    }
  }
  for (const byteOffset of [8, 40, 56, 88]) {
    const wordIndex = byteOffset / 8;
    const stackWord = handle.kernel.thread_stack_word?.(vmPtr, wordIndex);
    if (stackWord !== undefined) {
      parts.push(`slot${byteOffset}=0x${stackWord.toString(16)}`);
    }
  }
  const a0 = handle.kernel.thread_a0?.(vmPtr);
  if (a0 !== undefined) {
    parts.push(`a0=${a0.toString()}`);
  }
  const a1 = handle.kernel.thread_a1?.(vmPtr);
  if (a1 !== undefined) {
    parts.push(`a1=${a1.toString()}`);
  }
  const a2 = handle.kernel.thread_reg?.(vmPtr, 12);
  if (a2 !== undefined) {
    parts.push(`a2=${a2.toString()}`);
  }
  const a3 = handle.kernel.thread_reg?.(vmPtr, 13);
  if (a3 !== undefined) {
    parts.push(`a3=${a3.toString()}`);
  }
  const gp = handle.kernel.thread_gp?.(vmPtr);
  if (gp !== undefined) {
    parts.push(`gp=0x${gp.toString(16)}`);
  }
  const tp = handle.kernel.thread_tp?.(vmPtr);
  if (tp !== undefined) {
    parts.push(`tp=0x${tp.toString(16)}`);
  }
  for (const [label, reg] of [["s0", 8], ["s1", 9], ["s2", 18], ["s3", 19], ["s4", 20], ["s5", 21], ["s6", 22]] as const) {
    const value = handle.kernel.thread_reg?.(vmPtr, reg);
    if (value !== undefined) {
      parts.push(`${label}=0x${value.toString(16)}`);
    }
  }
  const s11 = handle.kernel.thread_s11?.(vmPtr);
  if (s11 !== undefined) {
    parts.push(`s11=0x${s11.toString(16)}`);
  }
  return parts.length > 0 ? `:${parts.join(":")}` : "";
}

function tryFormatOfdType(handle: ProcessHandle, ofdId: number): string {
  try {
    const fdType = handle.kernel.kernel_ofd_fd_type(ofdId);
    if (fdType === handle.kernel.abi_fd_type_file?.()) {
      return "file";
    }
    if (fdType === handle.kernel.abi_fd_type_pipe?.()) {
      return "pipe";
    }
    if (fdType === handle.kernel.abi_fd_type_epoll?.()) {
      return "epoll";
    }
    if (fdType === handle.kernel.abi_fd_type_eventfd?.()) {
      return "eventfd";
    }
    if (fdType === handle.kernel.abi_fd_type_socket?.()) {
      return "socket";
    }
    return String(fdType);
  } catch {
    return "err";
  }
}

function formatStepFdSnapshot(
  handle: ProcessHandle,
  vmPtr: number | undefined,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[] | undefined,
): string {
  if (vmPtr === undefined || vmPtr === 0 || fdEntries === undefined) {
    return "fds=none";
  }
  const focusFds = new Set([9, 10, 11, 12]);
  const selectedFdEntries = fdEntries.filter((entry) => entry.ofdId !== 0 || focusFds.has(entry.fd));
  const fdSummary = selectedFdEntries
    .slice(0, 24)
    .map((entry) => {
      if (entry.ofdId === 0) {
        return `${entry.fd}=0`;
      }
      let ino = "err";
      let refcount = "err";
      try {
        ino = String(handle.kernel.kernel_ofd_ino(entry.ofdId));
      } catch {
        // The diagnostic must survive older or partial harness exports.
      }
      try {
        refcount = String(handle.kernel.kernel_ofd_refcount(entry.ofdId));
      } catch {
        // The diagnostic must survive older or partial harness exports.
      }
      return `${entry.fd}=${entry.ofdId}/${tryFormatOfdType(handle, entry.ofdId)}/ino${ino}/ref${refcount}/fl${entry.fdFlags}`;
    });
  if (selectedFdEntries.length > 24) {
    fdSummary.push(`more=${selectedFdEntries.length - 24}`);
  }
  const ofdSlotIds = ofdSlots?.map((slot) => slot.ofdId).sort((left, right) => left - right) ?? [];
  return `fds=${fdSummary.join(",") || "empty"}:ofdSlots=${ofdSlotIds.join(",") || "none"}`;
}

function hasBrokenThreadState(handle: ProcessHandle, vmPtr: number): boolean {
  const pc = handle.kernel.thread_pc?.(vmPtr) ?? 0n;
  const sp = handle.kernel.thread_sp?.(vmPtr) ?? 0n;
  const s11 = handle.kernel.thread_s11?.(vmPtr) ?? 0n;
  return pc < 0x1000n || sp === 0n || s11 === 0n;
}

function appendSchedulerQueueDebugLog(
  deps: WorkerSchedulerQueueDeps,
  handle: ProcessHandle,
  event: string,
): void {
  if (!SCHEDULER_QUEUE_DEBUG_LOG_ENABLED) {
    return;
  }
  const queueDebugCount = handle.workerDebugLog.reduce(
    (count, entry) => count + (
      entry.startsWith("pump-") || entry.startsWith("step:") || entry.startsWith("ready:") || entry.startsWith("runnable-")
        ? 1
        : 0
    ),
    0,
  );
  if (queueDebugCount >= SCHEDULER_QUEUE_DEBUG_LOG_LIMIT) {
    const oldestQueueDebugIndex = handle.workerDebugLog.findIndex((entry) =>
      SCHEDULER_QUEUE_DEBUG_LOG_PREFIXES.some((prefix) => entry.startsWith(prefix)));
    if (oldestQueueDebugIndex !== -1) {
      handle.workerDebugLog.splice(oldestQueueDebugIndex, 1);
    } else {
      handle.workerDebugLog.shift();
    }
  }
  handle.workerDebugLog.push(event);
}

function removeReadyWorker(handle: ProcessHandle, worker: Worker): void {
  for (let index = handle.readyWorkers.length - 1; index >= 0; index -= 1) {
    if (handle.readyWorkers[index] === worker) {
      handle.readyWorkers.splice(index, 1);
    }
  }
}

function clearBlockedWorker(handle: ProcessHandle, worker: Worker): void {
  const blocked = handle.blockedWorkers.get(worker);
  if (!blocked) {
    return;
  }
  if (blocked.retryTimer !== null) {
    clearTimeout(blocked.retryTimer);
  }
  handle.blockedWorkers.delete(worker);
}

function statusBlocksInOwnerScheduler(handle: ProcessHandle, status: number): boolean {
  return status === handle.kernel.abi_status_epoll_wait()
    || status === handle.kernel.abi_status_fs()
    || status === handle.kernel.abi_status_pipe_wait()
    || status === handle.kernel.abi_status_wait4()
    || status === handle.kernel.abi_status_flock_wait();
}

function blockedRetryPressureStatus(handle: ProcessHandle, status: number): boolean {
  return status === handle.kernel.abi_status_epoll_wait()
    || status === handle.kernel.abi_status_fs()
    || status === handle.kernel.abi_status_pipe_wait()
    || status === handle.kernel.abi_status_wait4()
    || status === handle.kernel.abi_status_flock_wait()
    || status === handle.kernel.abi_status_connect()
    || status === handle.kernel.abi_status_socket_recv()
    || status === handle.kernel.abi_status_socket_send();
}

function readyWorkerCanUseFullBudgetUnderBlockedRetryPressure(
  handle: ProcessHandle,
  thread: ThreadWorkerState | undefined,
): boolean {
  return thread !== undefined
    && (
      thread.lastStatus === handle.kernel.abi_status_ok()
      || thread.lastStatus === handle.kernel.abi_status_clone_spawn()
    )
    && thread.waitingInWorkerStatus === null
    && thread.ownerAsyncStatus === null
    && thread.pendingGuestMemoryWrites.length === 0
    && !thread.resumeBlockedOnNextStep
    && thread.pendingPipeSlotsForNextStep.length === 0;
}

function schedulerStepBudgetForNextStep(
  handle: ProcessHandle,
  selectedThread: ThreadWorkerState | undefined,
): number {
  for (const blocked of handle.blockedWorkers.values()) {
    if (blockedRetryPressureStatus(handle, blocked.status)) {
      if (readyWorkerCanUseFullBudgetUnderBlockedRetryPressure(handle, selectedThread)) {
        return SCHEDULER_STEP_BUDGET;
      }
      return Math.min(SCHEDULER_STEP_BUDGET, CONTENDED_SCHEDULER_STEP_BUDGET);
    }
  }
  return SCHEDULER_STEP_BUDGET;
}

function incrementBoundedCounter(
  counters: Record<string, number>,
  key: string,
  maxEntries = HOT_PC_COUNTER_LIMIT,
): void {
  const existing = counters[key];
  if (existing !== undefined) {
    counters[key] = existing + 1;
    return;
  }
  const entries = Object.keys(counters);
  if (entries.length < maxEntries) {
    counters[key] = 1;
    return;
  }
  for (const entry of entries) {
    const next = (counters[entry] ?? 0) - 1;
    if (next <= 0) {
      delete counters[entry];
      continue;
    }
    counters[entry] = next;
  }
}

function supervisorResumeRecordBytes(handle: ProcessHandle): number {
  return handle.kernel.supervisor_resume_record_bytes?.() ?? SUPERVISOR_RESUME_RECORD_BYTES;
}

function supervisorResumeKindFutexWake(handle: ProcessHandle): number {
  return handle.kernel.supervisor_resume_kind_futex_wake?.() ?? SUPERVISOR_RESUME_KIND_FUTEX_WAKE;
}

function supervisorResumeKindRunnableThread(handle: ProcessHandle): number {
  return handle.kernel.supervisor_resume_kind_runnable_thread?.() ?? SUPERVISOR_RESUME_KIND_RUNNABLE_THREAD;
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
    view.byteLength < SUPERVISOR_RESUME_RECORD_BYTES
    || view.getUint32(0, true) !== SUPERVISOR_RESUME_RECORD_MAGIC
    || view.getUint32(4, true) !== 1
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
      view.getUint32(0, true) !== SUPERVISOR_THREAD_STATE_SNAPSHOT_MAGIC
      || view.getUint32(4, true) !== 1
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

export function createWorkerSchedulerQueueHelpers(
  deps: WorkerSchedulerQueueDeps,
): WorkerSchedulerQueueHelpers {
  const observedForkSharedSyncGates = new WeakMap<ProcessHandle, Promise<void>>();
  const pendingSupervisorFutexResumes = new WeakMap<ProcessHandle, Set<number>>();
  const readyWorkerFairness = createReadyWorkerFairness(pumpProcess);

  function hasWorkerLocalWait(thread: ThreadWorkerState | undefined): boolean {
    return (thread?.waitingInWorkerStatus ?? null) !== null;
  }

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
    clearBlockedWorker(handle, selectedWorker);
    removeReadyWorker(handle, selectedWorker);
    selectedThread.waitingInWorkerStatus = null;
    selectedThread.waitingInWorkerReconcileDeferred = false;
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
    if (recordBytes < SUPERVISOR_RESUME_RECORD_BYTES) {
      deps.appendWorkerDebugLog(handle, `supervisor-resume:runnable:invalid-record-bytes:${recordBytes}`);
      return null;
    }
    const memory = handle.kernel.memory ?? handle.memory;
    const scratch = supervisorResumeScratchBase(handle);
    ensureMemoryCapacity(memory, scratch + recordBytes);
    const result = processResume(handle.vmPtr, scratch, recordBytes);
    if (result === 0) {
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-resume:runnable:none:ready=${handle.readyWorkers.length}`,
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

  function markSupervisorFutexResumeSelected(handle: ProcessHandle, vmPtr: number): void {
    let selected = pendingSupervisorFutexResumes.get(handle);
    if (selected === undefined) {
      selected = new Set();
      pendingSupervisorFutexResumes.set(handle, selected);
    }
    selected.add(vmPtr);
  }

  function enqueueSupervisorSelectedWorker(
    handle: ProcessHandle,
    worker: Worker,
    thread: ThreadWorkerState,
    record: SupervisorResumeRecord,
  ): void {
    const label = supervisorResumeKindLabel(handle, record.kind);
    thread.waitingInWorkerStatus = null;
    thread.waitingInWorkerReconcileDeferred = false;
    thread.runningGeneration = null;
    if (
      handle.runningWorker !== worker
      && !handle.blockedWorkers.has(worker)
      && !handle.readyWorkers.includes(worker)
    ) {
      handle.readyWorkers.unshift(worker);
    }
    deps.appendWorkerDebugLog(
      handle,
      `supervisor-resume:${label}:ready:0x${thread.vmPtr.toString(16)}`
        + `:tid=${record.tid}:seq=${record.seq.toString()}`
        + `:process=0x${record.processPtr.toString(16)}`
        + `:detail=0x${record.detail0.toString(16)}`
        + `:status=${record.status}`
        + formatSupervisorThreadStateSnapshot(handle, record.vmPtr),
    );
  }

  function reconcileSupervisorSelectedFutexResumes(handle: ProcessHandle): void {
    const selected = pendingSupervisorFutexResumes.get(handle);
    if (selected === undefined || selected.size === 0) {
      return;
    }
    const futexWaitStatus = handle.kernel.abi_status_futex_wait();
    for (const vmPtr of [...selected]) {
      const worker = findWorkerByVmPtr(handle, vmPtr);
      const thread = worker === undefined ? undefined : deps.threadWorkerStates.get(worker);
      if (worker === undefined || thread === undefined) {
        selected.delete(vmPtr);
        deps.appendWorkerDebugLog(handle, `supervisor-resume:futex-wake:missing:0x${vmPtr.toString(16)}`);
        continue;
      }
      if (
        handle.kernel.thread_status(vmPtr) === futexWaitStatus
        || threadHasCanonicalFutexBlock(handle, vmPtr)
      ) {
        thread.waitingInWorkerStatus = futexWaitStatus;
        thread.waitingInWorkerReconcileDeferred = false;
        deps.appendWorkerDebugLog(
          handle,
          `supervisor-resume:futex-wake:parked:0x${vmPtr.toString(16)}`
            + formatSupervisorThreadStateSnapshot(handle, vmPtr),
        );
        continue;
      }
      selected.delete(vmPtr);
      enqueueSupervisorSelectedWorker(handle, worker, thread, {
        kind: supervisorResumeKindFutexWake(handle),
        seq: 0n,
        vmPtr,
        tid: handle.kernel.thread_tid?.(vmPtr) ?? 0,
        processPtr: handle.kernel.thread_process_ptr?.(vmPtr) ?? 0,
        status: handle.kernel.thread_status(vmPtr),
        detail0: 0n,
      });
    }
    if (selected.size === 0) {
      pendingSupervisorFutexResumes.delete(handle);
    }
  }

  function handleSupervisorResumeRecord(handle: ProcessHandle, record: SupervisorResumeRecord): void {
    if (record.kind === supervisorResumeKindRunnableThread(handle)) {
      const worker = selectSupervisorResumeWorker(handle, record, "supervisor-resume:runnable");
      if (worker === null || handle.runningWorker === worker || handle.readyWorkers.includes(worker)) {
        return;
      }
      handle.readyWorkers.unshift(worker);
      return;
    }
    if (record.kind !== supervisorResumeKindFutexWake(handle)) {
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-resume:unknown-kind:${record.kind}:vm=0x${record.vmPtr.toString(16)}:seq=${record.seq.toString()}`,
      );
      return;
    }
    markSupervisorFutexResumeSelected(handle, record.vmPtr);
    const worker = findWorkerByVmPtr(handle, record.vmPtr);
    const thread = worker === undefined ? undefined : deps.threadWorkerStates.get(worker);
    if (worker === undefined || thread === undefined) {
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-resume:futex-wake:missing:0x${record.vmPtr.toString(16)}:seq=${record.seq.toString()}`,
      );
      return;
    }
    if (
      handle.kernel.thread_status(record.vmPtr) === handle.kernel.abi_status_futex_wait()
      || threadHasCanonicalFutexBlock(handle, record.vmPtr)
    ) {
      thread.waitingInWorkerStatus = handle.kernel.abi_status_futex_wait();
      thread.waitingInWorkerReconcileDeferred = false;
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-resume:futex-wake:parked:0x${record.vmPtr.toString(16)}`
          + `:tid=${record.tid}:seq=${record.seq.toString()}:addr=0x${record.detail0.toString(16)}`
          + formatSupervisorThreadStateSnapshot(handle, record.vmPtr),
      );
      return;
    }
    const selected = pendingSupervisorFutexResumes.get(handle);
    selected?.delete(record.vmPtr);
    enqueueSupervisorSelectedWorker(handle, worker, thread, record);
    if (selected?.size === 0) {
      pendingSupervisorFutexResumes.delete(handle);
    }
  }

  function drainSupervisorResumeQueue(handle: ProcessHandle): void {
    const nextResume = handle.kernel.supervisor_next_resume;
    if (nextResume === undefined) {
      return;
    }
    const recordBytes = supervisorResumeRecordBytes(handle);
    if (recordBytes < SUPERVISOR_RESUME_RECORD_BYTES) {
      deps.appendWorkerDebugLog(handle, `supervisor-resume:invalid-record-bytes:${recordBytes}`);
      return;
    }
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
    reconcileSupervisorSelectedFutexResumes(handle);
  }

  function observeForkSharedSyncGate(handle: ProcessHandle, promise: Promise<void>): void {
    if (observedForkSharedSyncGates.get(handle) === promise) {
      return;
    }
    observedForkSharedSyncGates.set(handle, promise);
    const onSettled = (): void => {
      if (observedForkSharedSyncGates.get(handle) === promise) {
        observedForkSharedSyncGates.delete(handle);
      }
      if (handle.kernelShare.forkSharedSyncPromise === promise) {
        handle.kernelShare.forkSharedSyncPromise = null;
      }
      if (handle.forkSharedSyncPromise === promise) {
        handle.forkSharedSyncPromise = null;
      }
      pumpProcess(handle);
    };
    promise.then(onSettled, onSettled);
  }

  function runningWorkerStepAppearsIdle(handle: ProcessHandle, thread: ThreadWorkerState): boolean {
    const activeStepVmPtr = handle.kernel.debug_thread_step_vm_ptr?.();
    if (activeStepVmPtr === thread.vmPtr) {
      return false;
    }
    const lastStepAtMs = thread.lastStepAtMs;
    const lastStatusAtMs = thread.lastStatusAtMs;
    if (lastStepAtMs === null) {
      return false;
    }
    const hasUnansweredStep = thread.stepCount > thread.statusCount;
    if (hasUnansweredStep) {
      return false;
    }
    if (lastStatusAtMs !== null) {
      return Date.now() - Math.max(lastStepAtMs, lastStatusAtMs) >= STALE_SLEEP_RUNNING_WORKER_REPAIR_MS;
    }
    return Date.now() - lastStepAtMs >= STALE_SLEEP_RUNNING_WORKER_REPAIR_MS;
  }

  function clearStaleOwnerAsyncStatus(
    handle: ProcessHandle,
    thread: ThreadWorkerState,
  ): boolean {
    if ((thread.ownerAsyncStatus ?? null) === null) {
      return false;
    }
    if (!thread.initialized || thread.waitingInWorkerStatus !== null) {
      return false;
    }
    if (thread.stepCount > thread.statusCount || thread.lastStatusAtMs === null) {
      return false;
    }
    if (Date.now() - thread.lastStatusAtMs < STALE_SLEEP_RUNNING_WORKER_REPAIR_MS) {
      return false;
    }
    if (handle.kernel.thread_process_ptr(thread.vmPtr) === 0) {
      return false;
    }
    if (handle.kernel.thread_status(thread.vmPtr) !== handle.kernel.abi_status_ok()) {
      return false;
    }
    deps.appendWorkerDebugLog(
      handle,
      `owner-async-cleared:stale-ok:0x${thread.vmPtr.toString(16)}:async=${thread.ownerAsyncStatus}`,
    );
    thread.ownerAsyncStatus = null;
    return true;
  }

  function sanitizeRunningWorker(handle: ProcessHandle): Worker | null {
    const runningWorker = handle.runningWorker;
    if (runningWorker === null) {
      return null;
    }
    const thread = deps.threadWorkerStates.get(runningWorker);
    if (
      handle.workers.has(runningWorker)
      && thread
      && (thread.runningGeneration ?? thread.generation) === thread.generation
    ) {
      if (handle.kernel.thread_process_ptr(thread.vmPtr) === 0) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
        deps.appendWorkerDebugLog(handle, `running-worker-cleared:stale-process:0x${thread.vmPtr.toString(16)}`);
        return null;
      }
      const status = handle.kernel.thread_status(thread.vmPtr);
      if (
        status === handle.kernel.abi_status_futex_wait()
        || threadHasCanonicalFutexBlock(handle, thread.vmPtr)
      ) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
        thread.waitingInWorkerStatus = handle.kernel.abi_status_futex_wait();
        deps.appendWorkerDebugLog(handle, `running-worker-cleared:futex-wait:0x${thread.vmPtr.toString(16)}:status=${status}`);
        return null;
      }
      if (status === handle.kernel.abi_status_sleep()) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
        deps.appendWorkerDebugLog(handle, `running-worker-cleared:sleep-supervisor-owned:0x${thread.vmPtr.toString(16)}`);
        return null;
      }
      if (statusBlocksInOwnerScheduler(handle, status)) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
        deps.blockWorker(handle, runningWorker, status, thread.vmPtr);
        deps.appendWorkerDebugLog(handle, `running-worker-cleared:blocking-status-${status}:0x${thread.vmPtr.toString(16)}`);
        return null;
      }
      if (status === handle.kernel.abi_status_fault()) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
        deps.appendWorkerDebugLog(handle, `running-worker-cleared:status-${status}:0x${thread.vmPtr.toString(16)}`);
        return null;
      }
      if (
        handle.readyWorkers.length > 0
        && runningWorkerStepAppearsIdle(handle, thread)
      ) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
        deps.appendWorkerDebugLog(
          handle,
          `running-worker-cleared:stale-ok:0x${thread.vmPtr.toString(16)}:last=${thread.lastStatus ?? "null"}`,
        );
        return null;
      }
      if (
        handle.readyWorkers.length === 0
        && thread.stepCount <= thread.statusCount
        && runningWorkerStepAppearsIdle(handle, thread)
      ) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
        deps.appendWorkerDebugLog(
          handle,
          `running-worker-cleared:stale-ok-no-ready:0x${thread.vmPtr.toString(16)}:last=${thread.lastStatus ?? "null"}`,
        );
        return null;
      }
      if (
        handle.readyWorkers.length === 0
        && thread.stepCount > thread.statusCount
        && thread.lastStatusAtMs !== null
        && thread.lastStepAtMs !== null
        && thread.lastStepAtMs >= thread.lastStatusAtMs
        && thread.pendingGuestMemoryWrites.length === 0
        && runningWorkerStepAppearsIdle(handle, thread)
      ) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
        const unansweredSteps = thread.stepCount - thread.statusCount;
        deps.appendWorkerDebugLog(
          handle,
          `running-worker-cleared:${unansweredSteps > 1 ? "surplus-" : ""}stale-unanswered-no-ready:0x${thread.vmPtr.toString(16)}:last=${thread.lastStatus ?? "null"}:unanswered=${unansweredSteps}`,
        );
        return null;
      }
      return runningWorker;
    }
    handle.runningWorker = null;
    if (thread) {
      thread.runningGeneration = null;
    }
    deps.appendWorkerDebugLog(handle, "running-worker-cleared:stale");
    return null;
  }

  function hasExclusiveVforkWorker(handle: ProcessHandle): boolean {
    const worker = handle.vforkWorker;
    if (handle.vforkSnapshot === null || worker === null) {
      return false;
    }
    if (sanitizeRunningWorker(handle) === worker) {
      return true;
    }
    if (handle.readyWorkers.includes(worker)) {
      return true;
    }
    if (handle.blockedWorkers.has(worker)) {
      return true;
    }
    const thread = deps.threadWorkerStates.get(worker);
    return thread?.waitingInWorkerStatus !== null;
  }

  function parkReadyWorkerStillInCanonicalFutexWait(
    handle: ProcessHandle,
    thread: ThreadWorkerState | undefined,
  ): boolean {
    if (thread === undefined || thread.waitingInWorkerStatus !== null) {
      return false;
    }
    if (handle.kernel.thread_process_ptr(thread.vmPtr) === 0) {
      return false;
    }
    const futexWaitStatus = handle.kernel.abi_status_futex_wait();
    if (
      handle.kernel.thread_status(thread.vmPtr) !== futexWaitStatus
      && !threadHasCanonicalFutexBlock(handle, thread.vmPtr)
    ) {
      return false;
    }
    thread.waitingInWorkerStatus = futexWaitStatus;
    thread.waitingInWorkerReconcileDeferred = false;
    thread.runningGeneration = null;
    deps.appendWorkerDebugLog(
      handle,
      `ready-worker-parked:futex-wait:0x${thread.vmPtr.toString(16)}`,
    );
    return true;
  }

  function assertThreadOwnershipInvariant(
    handle: ProcessHandle,
    vmPtr: number,
    context: string,
  ): void {
    if (vmPtr === 0 || handle.pid <= 0) {
      return;
    }
    const currentPid = handle.kernel.process_current_pid(vmPtr);
    if (currentPid !== handle.pid) {
      throw new Error(
        `${context} worker/process ownership mismatch vm=0x${vmPtr.toString(16)}`
          + ` expected_pid=${handle.pid} actual_pid=${currentPid}`,
      );
    }
  }

  function detachTransferredWorker(handle: ProcessHandle, worker: Worker): void {
    const thread = deps.threadWorkerStates.get(worker);
    clearBlockedWorker(handle, worker);
    removeReadyWorker(handle, worker);
    if (handle.runningWorker === worker) {
      handle.runningWorker = null;
    }
    if (handle.vforkWorker === worker) {
      handle.vforkWorker = null;
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `attach-transfer-detach:${handle.pid}`
        + `:vm=0x${(thread?.vmPtr ?? 0).toString(16)}`
        + `:owner=${thread?.ownerPid ?? "none"}`
        + `:workers=${handle.workers.size}`,
    );
    handle.workers.delete(worker);
  }

  function attachThreadWorker(handle: ProcessHandle, request: AttachThreadWorkerRequest): void {
    const { worker, vmPtr, forcePrepare, transferFrom, preserveMemoryBytes, preserveMemoryRanges } = request;
    const generation = nextThreadWorkerGeneration++;
    const existingThread = deps.threadWorkerStates.get(worker);
    const reinitWithinHandle = existingThread !== undefined
      && transferFrom === undefined
      && existingThread.ownerPid === handle.pid;
    if (existingThread) {
      if (reinitWithinHandle) {
        clearBlockedWorker(handle, worker);
        removeReadyWorker(handle, worker);
        if (handle.runningWorker === worker) {
          handle.runningWorker = null;
        }
        if (handle.vforkWorker === worker) {
          handle.vforkWorker = null;
        }
      } else {
        const sourceHandle = transferFrom;
        if (!sourceHandle) {
          throw new Error(
            `attachThreadWorker requires transferFrom for existing worker owner=${existingThread.ownerPid}`
              + ` target=${handle.pid} vm=0x${vmPtr.toString(16)}`,
          );
        }
        if (sourceHandle === handle) {
          throw new Error(
            `attachThreadWorker transferFrom must differ from target pid=${handle.pid}`
              + ` vm=0x${vmPtr.toString(16)}`,
          );
        }
        if (existingThread.ownerPid !== sourceHandle.pid) {
          throw new Error(
            `attachThreadWorker owner mismatch worker_owner=${existingThread.ownerPid}`
              + ` transfer_from=${sourceHandle.pid} target=${handle.pid}`,
          );
        }
        detachTransferredWorker(sourceHandle, worker);
      }
    } else if (transferFrom) {
      throw new Error(
        `attachThreadWorker received transferFrom for unattached worker`
          + ` source=${transferFrom.pid} target=${handle.pid} vm=0x${vmPtr.toString(16)}`,
      );
    }

    assertThreadOwnershipInvariant(handle, vmPtr, "attachThreadWorker");
    handle.workers.add(worker);
    deps.appendWorkerDebugLog(
      handle,
      `${reinitWithinHandle ? "reinit" : "attach"}:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`,
    );
    deps.threadWorkerStates.set(worker, {
      vmPtr,
      ownerPid: handle.pid,
      generation,
      runningGeneration: null,
      reinitCount: reinitWithinHandle ? (existingThread?.reinitCount ?? 0) + 1 : 0,
      stepCount: 0,
      totalStepBudget: 0,
      totalStepWallMs: 0,
      maxStepWallMs: 0,
      lastStepDurationMs: null,
      statusCount: 0,
      statusCounts: {},
      lastStatus: null,
      recentStatuses: [],
      lastStepAtMs: null,
      lastStatusAtMs: null,
      lastStepProcessPtr: null,
      lastStepPc: null,
      lastStepSp: null,
      lastStepS11: null,
      hotStepPcs: {},
      recentSteps: [],
      lastStatusPc: null,
      lastStatusSp: null,
      lastStatusS11: null,
      lastStatusFutexWaitAddr: null,
      lastStatusFutexBlockedAddr: null,
      hotStatusPcs: {},
      initialized: false,
      waitingInWorkerStatus: null,
      waitingInWorkerReconcileDeferred: false,
      ownerAsyncStatus: null,
      resumeBlockedOnNextStep: false,
      pendingPipeSlotsForNextStep: [],
      pendingGuestMemoryWrites: [],
      resumeWorkersOnReady: [],
    });
    setWorkerMessageHandler(worker, (event: MessageEvent<ThreadWorkerOutbound>) => {
      deps.handleWorkerMessage(handle, worker, event.data);
    });
    setWorkerErrorHandler(worker, (event) => {
      const detail = [
        event.message,
        event.filename ? `at ${event.filename}:${event.lineno}:${event.colno}` : null,
        event.error instanceof Error ? (event.error.stack ?? event.error.message) : null,
      ].filter((part): part is string => !!part).join(" ");
      deps.appendWorkerDebugLog(handle, `worker-error:0x${vmPtr.toString(16)}:${JSON.stringify(detail || "unknown worker error")}`);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `clone-child-worker-error:${handle.pid}:vm=0x${vmPtr.toString(16)}`
          + `:detail=${JSON.stringify(detail || "unknown worker error")}`,
      );
      deps.failProcess(handle, new Error(
        `thread worker error vm=0x${vmPtr.toString(16)} ${detail || "unknown worker error"}`,
      ));
      event.preventDefault?.();
    });
    setWorkerMessageErrorHandler(worker, (event) => {
      deps.appendWorkerDebugLog(handle, `worker-messageerror:0x${vmPtr.toString(16)}`);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `clone-child-messageerror:${handle.pid}:vm=0x${vmPtr.toString(16)}`,
      );
      deps.failProcess(handle, new Error(
        `thread worker messageerror vm=0x${vmPtr.toString(16)}`,
      ));
      event.preventDefault?.();
    });
    const runtimeEpoch = publishRuntimeBridgeEpoch(handle.runtimeBridge);
    const fdEntries = snapshotProcessFdEntriesForThread(handle, vmPtr);
    const initMessage: ThreadWorkerInbound = {
      type: "init",
      wasmModule: handle.wasmModule,
      memory: handle.memory,
      forcePrepare,
      vmPtr,
      guestBase: handle.guestBase,
      kernelStackSlot: kernelStackSlotForWorker(worker),
      generation,
      runtimeEpoch,
      preserveMemoryBytes,
      preserveMemoryRanges,
      guestWriteWatchRegions: handle.guestWriteWatchRegions,
      kernelState: handle.kernelState,
      sabPageCache: handle.sabPageCache,
      runtimeNotify: handle.runtimeBridge.notifySab,
      runtimeState: handle.runtimeBridge.stateSab,
    };
    const initSocketState = snapshotSocketStateForThread(handle);
    const initOfdSlots = snapshotOfdSlotsForThread(handle, fdEntries);
    if (initSocketState !== undefined) {
      initMessage.socketState = initSocketState;
    }
    if (fdEntries !== undefined) {
      initMessage.fdEntries = fdEntries;
    }
    if (initOfdSlots !== undefined) {
      initMessage.ofdSlots = initOfdSlots;
    }
    const initChildExitRecords = formatChildExitRecords(handle);
    if (initChildExitRecords.length > 0) {
      initMessage.childExitRecords = initChildExitRecords;
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `clone-child-init-post:${handle.pid}:vm=0x${vmPtr.toString(16)}`
        + `:generation=${generation}:force=${forcePrepare ? 1 : 0}`
        + `:workers=${handle.workers.size}:ready=${handle.readyWorkers.length}`,
    );
    worker.postMessage(initMessage);
  }

  function enqueueWorker(handle: ProcessHandle, worker: Worker): void {
    if (handle.exitInProgress) {
      return;
    }
    const thread = deps.threadWorkerStates.get(worker);
    if (
      handle.runningWorker === worker
      && handle.workers.has(worker)
      && thread !== undefined
    ) {
      return;
    }
    if (
      (thread?.waitingInWorkerStatus ?? null) !== null
      || (thread?.ownerAsyncStatus ?? null) !== null
    ) {
      if (
        thread !== undefined
        && thread.waitingInWorkerStatus === null
        && thread.ownerAsyncStatus !== null
      ) {
        appendSchedulerQueueDebugLog(deps, handle, `runnable-defer-owner-async:0x${thread.vmPtr.toString(16)}:async=${thread.ownerAsyncStatus}`);
      } else if ((thread?.ownerAsyncStatus ?? null) === null) {
        removeReadyWorker(handle, worker);
      }
      pumpProcess(handle);
      return;
    }
    const vmPtr = thread?.vmPtr;
    if (vmPtr !== undefined) {
      const brokenThreadState = hasBrokenThreadState(handle, vmPtr);
      appendSchedulerQueueDebugLog(deps, handle, `runnable-published:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`);
      if (brokenThreadState) {
        deps.appendWorkerDebugLog(
          handle,
          `ready_break:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`,
        );
      }
    }
    if (
      handle.vforkSnapshot !== null
      && handle.vforkWorker === worker
      && !handle.readyWorkers.includes(worker)
    ) {
      handle.readyWorkers.push(worker);
    }
    if (handle.runningWorker !== null) {
      readyWorkerFairness.wakeRunningWorkerForReadyWork(handle);
    }
    pumpProcess(handle);
  }

  function recoverRunnableIdleWorkers(handle: ProcessHandle): void {
    const exclusiveWorker = handle.vforkSnapshot !== null ? handle.vforkWorker : null;
    for (const worker of handle.workers) {
      if (handle.runningWorker === worker || handle.readyWorkers.includes(worker) || handle.blockedWorkers.has(worker)) {
        continue;
      }
      if (exclusiveWorker !== null && worker !== exclusiveWorker) {
        continue;
      }
      const thread = deps.threadWorkerStates.get(worker);
      if (thread && clearStaleOwnerAsyncStatus(handle, thread)) {
        continue;
      }
      if (
        !thread?.initialized
        || thread.waitingInWorkerStatus !== null
        || thread.ownerAsyncStatus !== null
        || handle.kernel.thread_process_ptr(thread.vmPtr) === 0
      ) {
        continue;
      }
      const currentStatus = handle.kernel.thread_status(thread.vmPtr);
      if (statusBlocksInOwnerScheduler(handle, currentStatus)) {
        deps.blockWorker(handle, worker, currentStatus, thread.vmPtr);
        deps.appendWorkerDebugLog(
          handle,
          `idle-worker-blocked:status-${currentStatus}:0x${thread.vmPtr.toString(16)}`,
        );
        continue;
      }
      if (thread.stepCount > thread.statusCount) {
        continue;
      }
      if (currentStatus !== handle.kernel.abi_status_ok()) {
        continue;
      }
    }
  }

  function completeFaultedProcessWithoutWorkers(handle: ProcessHandle): boolean {
    if (
      handle.workers.size !== 0
      || handle.readyWorkers.length !== 0
      || handle.runningWorker !== null
      || handle.blockedWorkers.size !== 0
    ) {
      return false;
    }
    const decision = readSupervisorFaultExitDecision(handle, handle.vmPtr);
    if (
      decision === undefined
      || decision.kind !== supervisorFaultExitKindProcessExit(handle.kernel)
    ) {
      return false;
    }
    deps.appendWorkerDebugLog(
      handle,
      `complete-exit:no-workers-fault:0x${decision.exitVmPtr.toString(16)}`
        + `:seq=${decision.seq.toString()}`
        + `:threads=${decision.liveThreads}/${decision.remainingThreads}`,
    );
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `complete-exit-request:no-workers-fault:${handle.pid}:vm=0x${decision.exitVmPtr.toString(16)}`
        + `:exit=${decision.exitCode}:${decision.termSignal}`
        + `:seq=${decision.seq.toString()}`
        + `:threads=${decision.liveThreads}/${decision.remainingThreads}`,
    );
    void deps.completeProcessExit(
      handle,
      decision.exitCode,
      decision.termSignal,
      decision.exitVmPtr,
    ).catch((error) => {
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    });
    return true;
  }

  function pumpProcess(handle: ProcessHandle): void {
    if (handle.exitInProgress) {
      return;
    }
    const forkSharedSyncPromise = handle.kernelShare.forkSharedSyncPromise;
    if (forkSharedSyncPromise !== null) {
      const staleGate = clearStaleForkSharedSyncGate(handle, forkSharedSyncPromise);
      if (staleGate !== null) {
        appendSchedulerQueueDebugLog(deps, handle, `pump-cleared:${staleGate}`);
      } else {
        appendSchedulerQueueDebugLog(deps, handle, `pump-held:fork-shared-sync:ready=${handle.readyWorkers.length}`);
        observeForkSharedSyncGate(handle, forkSharedSyncPromise);
        return;
      }
    }
    const runningWorker = sanitizeRunningWorker(handle);
    if (runningWorker !== null) {
      const thread = deps.threadWorkerStates.get(runningWorker);
      if (handle.readyWorkers.length > 0) {
        readyWorkerFairness.scheduleRunningWorkerRepairRetry(handle);
      } else {
        readyWorkerFairness.scheduleRunningWorkerYieldRetry(handle);
      }
      appendSchedulerQueueDebugLog(
        deps,
        handle,
        `pump-held:running:ready=${handle.readyWorkers.length}`
          + (thread ? `:vm=0x${thread.vmPtr.toString(16)}:status=${handle.kernel.thread_status(thread.vmPtr)}` : ""),
      );
      return;
    }
    drainSupervisorResumeQueue(handle);
    recoverRunnableIdleWorkers(handle);
    if (completeFaultedProcessWithoutWorkers(handle)) {
      return;
    }
    let next: Worker | undefined;
    if (hasExclusiveVforkWorker(handle)) {
      const index = handle.readyWorkers.indexOf(handle.vforkWorker!);
      if (index === -1) {
        return;
      }
      const candidate = handle.readyWorkers[index];
      const candidateThread = candidate ? deps.threadWorkerStates.get(candidate) : undefined;
      if (
        (candidateThread?.ownerAsyncStatus ?? null) !== null
        || hasWorkerLocalWait(candidateThread)
        || parkReadyWorkerStillInCanonicalFutexWait(handle, candidateThread)
        || (candidateThread !== undefined && handle.kernel.thread_process_ptr(candidateThread.vmPtr) === 0)
      ) {
        appendSchedulerQueueDebugLog(
          deps,
          handle,
          `pump-skip-candidate:vfork:ready=${handle.readyWorkers.length}`
            + (candidateThread ? `:vm=0x${candidateThread.vmPtr.toString(16)}`
              + `:wait=${candidateThread.waitingInWorkerStatus ?? "null"}`
              + `:async=${candidateThread.ownerAsyncStatus ?? "null"}` : ""),
        );
        handle.readyWorkers.splice(index, 1);
        return;
      }
      next = handle.readyWorkers.splice(index, 1)[0];
    } else {
      const supervisorSelected = selectSupervisorRunnableWorker(handle);
      if (supervisorSelected !== undefined) {
        next = supervisorSelected ?? undefined;
      } else {
        deps.appendWorkerDebugLog(
          handle,
          `supervisor-resume:runnable:missing-export:ready=${handle.readyWorkers.length}`,
        );
        return;
      }
    }
    if (!next) {
      return;
    }
    handle.runningWorker = next;
    const thread = deps.threadWorkerStates.get(next);
    const stepBudget = schedulerStepBudgetForNextStep(handle, thread);
    if (handle.readyWorkers.length > 0) {
      readyWorkerFairness.scheduleRunningWorkerRepairRetry(handle);
    } else {
      readyWorkerFairness.scheduleRunningWorkerYieldRetry(handle);
    }
    const vmPtr = thread?.vmPtr;
    if (thread && vmPtr !== undefined) {
      appendSchedulerQueueDebugLog(
        deps,
        handle,
        `pump-run:ready=${handle.readyWorkers.length}:vm=0x${vmPtr.toString(16)}:status=${handle.kernel.thread_status(vmPtr)}`,
      );
      restoreRegularFileOffsetsForStep(handle);
      preserveHostStdioPipeControlForStep(handle, vmPtr);
      thread.stepCount += 1;
      thread.yieldedReady = false;
      thread.runningGeneration = thread.generation;
      thread.totalStepBudget += stepBudget;
      thread.lastStepAtMs = Date.now();
      thread.lastStepProcessPtr = handle.kernel.thread_process_ptr(vmPtr);
      thread.lastStepPc = handle.kernel.thread_pc(vmPtr);
      thread.lastStepSp = handle.kernel.thread_sp(vmPtr);
      thread.lastStepS11 = handle.kernel.thread_s11?.(vmPtr) ?? 0n;
      incrementBoundedCounter(thread.hotStepPcs, `0x${thread.lastStepPc.toString(16)}`);
      const recentSteps = thread.recentSteps ??= [];
      recentSteps.push({
        pc: thread.lastStepPc,
        sp: thread.lastStepSp,
        s11: thread.lastStepS11,
        atMs: thread.lastStepAtMs,
      });
      if (recentSteps.length > 16) {
        recentSteps.splice(0, recentSteps.length - 16);
      }
      const brokenThreadState = hasBrokenThreadState(handle, vmPtr);
      appendSchedulerQueueDebugLog(deps, handle, `step:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`);
      if (brokenThreadState) {
        deps.appendWorkerDebugLog(
          handle,
          `step_break:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`,
        );
      }
    }
    publishRuntimeBridgeEpoch(handle.runtimeBridge);
    const fdEntries = snapshotProcessFdEntriesForThread(handle, vmPtr ?? 0);
    const pendingKernelState = thread?.pendingKernelStateForNextStep;
    if (thread && pendingKernelState !== undefined) {
      thread.pendingKernelStateForNextStep = undefined;
    }
    const stepMessage: ThreadWorkerInbound = {
      type: "step",
      budget: stepBudget,
      kernelState: pendingKernelState ?? handle.kernelState,
    };
    const stepSocketState = snapshotSocketStateForThread(handle);
    const stepOfdSlots = snapshotOfdSlotsForThread(handle, fdEntries);
    if (stepSocketState !== undefined) {
      stepMessage.socketState = stepSocketState;
    }
    if (fdEntries !== undefined) {
      stepMessage.fdEntries = fdEntries;
    }
    if (stepOfdSlots !== undefined) {
      stepMessage.ofdSlots = stepOfdSlots;
    }
    appendSchedulerQueueDebugLog(
      deps,
      handle,
      `step-fds:0x${(vmPtr ?? 0).toString(16)}:${formatStepFdSnapshot(handle, vmPtr, fdEntries, stepOfdSlots)}`,
    );
    if (thread?.pendingPipeSlotsForNextStep?.length) {
      stepMessage.pipeSlots = thread.pendingPipeSlotsForNextStep.splice(0);
    }
    const stepChildExitRecords = formatChildExitRecords(handle);
    if (stepChildExitRecords.length > 0) {
      stepMessage.childExitRecords = stepChildExitRecords;
      stepMessage.kernelState = {
        ...stepMessage.kernelState,
        pipeUsed: composeHandlePipeUsed(handle, stepMessage.kernelState.pipeUsed),
      };
    }
    if (thread && (thread.pendingGuestMemoryWrites?.length ?? 0) > 0) {
      stepMessage.guestMemoryWrites = thread.pendingGuestMemoryWrites.splice(0);
    }
    if (thread?.resumeBlockedOnNextStep) {
      thread.resumeBlockedOnNextStep = false;
      stepMessage.resumeBlockedSyscall = true;
    }
    next.postMessage(stepMessage);
  }

  return {
    attachThreadWorker,
    enqueueWorker,
    pumpProcess,
  };
}
