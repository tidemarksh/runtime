import type {
  ProcessHandle,
  ThreadWorkerState,
  TransferDebugStats,
} from "./types.js";
import { readRuntimeBridgeDebugStage } from "../bridge/runtime-state.js";

const NET_BUFFER_HEADER_BYTES = 16;
const NET_BUFFER_HEAD_OFFSET = 0;
const NET_BUFFER_TAIL_OFFSET = 4;

export interface WorkerLogFocus {
  vmPtr: number;
  ownerState: string;
  waitingInWorkerStatus: number | null;
  threadState: string | null;
  lastCredibleState: string | null;
  lastStatus: string | null;
  lastStatusDebug: string | null;
  lastReady: string | null;
  lastReadyDebug: string | null;
  lastStep: string | null;
  lastWait20: string | null;
  lastResume20: string | null;
  lastWrite: string | null;
  lastSleep: string | null;
  lastYield: string | null;
  lastFaultDebug: string | null;
  execTrace: string | null;
  eventTail: string[];
  statusTail: string[];
}

export interface ThreadStepDebugSnapshot {
  vmPtr: number;
  tid: number | null;
  phase: number | null;
  status: number;
  budget: number;
  remaining: number | null;
  pc: string;
  ra: string;
  sp: string;
  s11: string;
}

export interface RuntimeProcessWorkerLogFocus {
  pid: number;
  cmd: string;
  workers: WorkerLogFocus[];
}

export interface WorkerExecutionStats {
  vmPtr: number;
  ownerState: string;
  stepCount: number;
  totalStepBudget: number;
  totalStepWallMs: number;
  maxStepWallMs: number;
  lastStepDurationMs: number | null;
  statusCount: number;
  statusCounts: Record<string, number>;
  statusCountsByName: Record<string, number>;
  lastStatus: number | null;
  lastStatusName: string | null;
  lastStepAtMs: number | null;
  lastStatusAtMs: number | null;
  lastStepPc: string | null;
  lastStepSp: string | null;
  lastStepS11: string | null;
  hotStepPcs: Array<{ pc: string; count: number }>;
  lastStatusPc: string | null;
  lastStatusSp: string | null;
  lastStatusS11: string | null;
  hotStatusPcs: Array<{ pc: string; count: number }>;
  waitingInWorkerStatus: number | null;
  threadStepEntry: ThreadStepDebugSnapshot | null;
  threadStepStatus: ThreadStepDebugSnapshot | null;
  threadStepPhase: ThreadStepDebugSnapshot | null;
}

export interface ProcessExecutionStats {
  pid: number;
  cmd: string;
  running: boolean;
  memoryBytes: number;
  ramSize: number;
  maxMemoryBytes: number;
  forkMaxMemoryBytes: number;
  guestBase: number;
  readyWorkers: number;
  blockedWorkers: number;
  pendingChildExitRecords: number;
  pendingChildSpawnRecords: number;
  activeChildren: number;
  runtimeBridgeDebugStage: { stage: number; value: number };
  workerMessageDebugStats: ProcessHandle["workerMessageDebugStats"];
  transferDebugStats: TransferDebugStats;
  memoryLogTail: string[];
  memoryVmas: Array<{
    start: string;
    end: string;
    prot: number;
    flags: number;
  }>;
  cpuDebugStats: CpuDebugStats | null;
  syscallDebugStats: SyscallDebugStats | null;
  workers: WorkerExecutionStats[];
}

export interface SyscallDebugStats {
  topSyscalls: Array<{ nr: number; count: string; nonOkStatusCount: string; errorCount: string }>;
}

export interface CpuDebugStats {
  blockHits: string;
  blockMisses: string;
  blockBuilds: string;
  blockBuildFailures: string;
  blockBuildOps: string;
  blockBuildBudget: string;
  blockBudgetSkips: string;
  blockCacheUnsupported: string;
  blockExecOps: string;
  blockExecUnsupported: string;
  blockExecOpSampleInterval: string;
  blockExecSampledBlocks: string;
  blockMissSampleInterval: string;
  blockMissSampled: string;
  blockMissSamples: Array<{ pc: string; evictedPc: string; cacheIndex: number; count: string }>;
  interpreterOps: string;
  blockBuildOpCounts: Array<{ opId: number; count: string }>;
  blockExecOpCounts: Array<{ opId: number; count: string }>;
  interpreterOpCounts: Array<{ opId: number; count: string }>;
  unsupportedOpCounts: Array<{ opId: number; count: string }>;
  memoryPermitChecks: string;
  memoryPermitCacheHits: string;
  memoryPermitCoverHits: string;
  memoryPermitDenied: string;
  memoryPermitByProt: Array<{
    prot: number;
    checks: string;
    cacheHits: string;
    coverHits: string;
    denied: string;
  }>;
}

const decoder = new TextDecoder();

function formatU64(value: bigint): string {
  return BigInt.asUintN(64, value).toString(16);
}

function formatU64Decimal(value: bigint | number | undefined): string {
  if (typeof value === "bigint") {
    return BigInt.asUintN(64, value).toString(10);
  }
  return String(value ?? 0);
}

function readWorkerOwnerState(
  handle: Pick<ProcessHandle, "runningWorker" | "readyWorkers" | "blockedWorkers">,
  worker: Worker,
  state: ThreadWorkerState,
): string {
  if (handle.runningWorker === worker) {
    return "running";
  }
  if (handle.readyWorkers.includes(worker)) {
    return "ready";
  }
  if (handle.blockedWorkers.has(worker)) {
    return "blocked";
  }
  if (state.waitingInWorkerStatus !== null) {
    return `wait:${state.waitingInWorkerStatus}`;
  }
  if (state.ownerAsyncStatus !== null) {
    return `async:${state.ownerAsyncStatus}`;
  }
  if (!state.initialized) {
    return "init";
  }
  return "idle";
}

function formatWorkerSchedulerDetails(handle: ProcessHandle, worker: Worker, state: ThreadWorkerState): string {
  const ownerState = readWorkerOwnerState(handle, worker, state);
  const threadStatus = handle.kernel.thread_status?.(state.vmPtr);
  const processPtr = handle.kernel.thread_process_ptr?.(state.vmPtr);
  const currentPid = handle.kernel.process_current_pid?.(state.vmPtr);
  const tid = handle.kernel.thread_tid?.(state.vmPtr);
  return [
    `0x${state.vmPtr.toString(16)}`,
    `owner=${ownerState}`,
    `ownerPid=${state.ownerPid}`,
    `gen=${state.generation}`,
    `runGen=${state.runningGeneration ?? "null"}`,
    `init=${state.initialized ? 1 : 0}`,
    `wait=${state.waitingInWorkerStatus ?? "null"}`,
    `async=${state.ownerAsyncStatus ?? "null"}`,
    `kernelStatus=${threadStatus ?? "n/a"}`,
    `currentPid=${currentPid ?? "n/a"}`,
    `tid=${tid ?? "n/a"}`,
    `process=0x${(processPtr ?? 0).toString(16)}`,
    `steps=${state.stepCount}`,
    `statuses=${state.statusCount}`,
    `last=${state.lastStatus ?? "null"}`,
    `lastPc=${state.lastStatusPc === null ? "null" : `0x${state.lastStatusPc.toString(16)}`}`,
    `lastStepPc=${state.lastStepPc === null ? "null" : `0x${state.lastStepPc.toString(16)}`}`,
    `pendingPipeSlots=${state.pendingPipeSlotsForNextStep.length}`,
    `pendingGuestWrites=${state.pendingGuestMemoryWrites.length}`,
    `resumeReady=${state.resumeWorkersOnReady.length}`,
  ].join(":");
}

function findLastWorkerLogEntry(
  workerDebugLog: readonly string[],
  prefix: string,
  vmPtr: number,
): string | null {
  const vmTag = `0x${vmPtr.toString(16)}`;
  for (let index = workerDebugLog.length - 1; index >= 0; index -= 1) {
    const entry = workerDebugLog[index];
    if (entry?.startsWith(`${prefix}:${vmTag}`)) {
      return entry;
    }
  }
  return null;
}

function collectWorkerEventTail(
  workerDebugLog: readonly string[],
  vmPtr: number,
): string[] {
  const vmTag = `0x${vmPtr.toString(16)}`;
  const prefixes = [
    "status",
    "ready",
    "step",
    "wait",
    "resume",
    "write",
    "fdtrace",
    "sleep",
    "yield",
    "fault_debug",
  ];
  const out: string[] = [];
  for (let index = workerDebugLog.length - 1; index >= 0; index -= 1) {
    const entry = workerDebugLog[index];
    if (!entry?.includes(vmTag)) {
      continue;
    }
    if (!prefixes.some((prefix) => entry.startsWith(`${prefix}:${vmTag}`))) {
      continue;
    }
    if (entry.startsWith(`write:${vmTag}`) && !entry.includes("write_data=")) {
      continue;
    }
    out.push(entry);
    if (out.length >= 12) {
      break;
    }
  }
  return out.reverse();
}

function collectWorkerStatusTail(
  workerDebugLog: readonly string[],
  vmPtr: number,
): string[] {
  const vmTag = `0x${vmPtr.toString(16)}`;
  const prefixes = [
    "status",
    "status_debug",
    "ready",
    "ready-debug",
    "step",
    "wait",
    "resume",
    "fdtrace",
    "fault_debug",
  ];
  const out: string[] = [];
  for (let index = workerDebugLog.length - 1; index >= 0; index -= 1) {
    const entry = workerDebugLog[index];
    if (!entry?.includes(vmTag)) {
      continue;
    }
    if (!prefixes.some((prefix) => entry.startsWith(`${prefix}:${vmTag}`))) {
      continue;
    }
    out.push(entry);
    if (out.length >= 20) {
      break;
    }
  }
  return out.reverse();
}

function readCpuDebugStats(handle: ProcessHandle): CpuDebugStats | null {
  if (!handle.kernel.debug_cpu_profile_block_hits) {
    return null;
  }
  const readCounts = (
    accessor: ((opId: number) => bigint | number) | undefined,
  ): Array<{ opId: number; count: string }> => {
    if (!accessor) {
      return [];
    }
    const counts: Array<{ opId: number; count: string }> = [];
    for (let opId = 0; opId < 32; opId += 1) {
      const count = formatU64Decimal(accessor(opId));
      if (count !== "0") {
        counts.push({ opId, count });
      }
    }
    return counts;
  };
  const readBlockMissSamples = (): Array<{
    pc: string;
    evictedPc: string;
    cacheIndex: number;
    count: string;
  }> => {
    const sampleCount = handle.kernel.debug_cpu_profile_block_miss_sample_count?.() ?? 0;
    const samples: Array<{ pc: string; evictedPc: string; cacheIndex: number; count: string }> = [];
    for (let index = 0; index < sampleCount; index += 1) {
      const count = formatU64Decimal(handle.kernel.debug_cpu_profile_block_miss_sample_count_at?.(index));
      if (count === "0") {
        continue;
      }
      samples.push({
        pc: `0x${formatU64(BigInt(handle.kernel.debug_cpu_profile_block_miss_sample_pc?.(index) ?? 0))}`,
        evictedPc: `0x${formatU64(BigInt(handle.kernel.debug_cpu_profile_block_miss_sample_evicted_pc?.(index) ?? 0))}`,
        cacheIndex: handle.kernel.debug_cpu_profile_block_miss_sample_cache_index?.(index) ?? 0,
        count,
      });
    }
    samples.sort((left, right) => Number(BigInt(right.count) - BigInt(left.count)));
    return samples.slice(0, 16);
  };
  const readMemoryPermitByProt = (): CpuDebugStats["memoryPermitByProt"] => {
    const rows: CpuDebugStats["memoryPermitByProt"] = [];
    for (let prot = 0; prot < 8; prot += 1) {
      const checks = formatU64Decimal(handle.kernel.debug_memory_permit_checks_by_prot?.(prot));
      if (checks === "0") {
        continue;
      }
      rows.push({
        prot,
        checks,
        cacheHits: formatU64Decimal(handle.kernel.debug_memory_permit_cache_hits_by_prot?.(prot)),
        coverHits: formatU64Decimal(handle.kernel.debug_memory_permit_cover_hits_by_prot?.(prot)),
        denied: formatU64Decimal(handle.kernel.debug_memory_permit_denied_by_prot?.(prot)),
      });
    }
    return rows;
  };
  return {
    blockHits: formatU64Decimal(handle.kernel.debug_cpu_profile_block_hits()),
    blockMisses: formatU64Decimal(handle.kernel.debug_cpu_profile_block_misses?.()),
    blockBuilds: formatU64Decimal(handle.kernel.debug_cpu_profile_block_builds?.()),
    blockBuildFailures: formatU64Decimal(handle.kernel.debug_cpu_profile_block_build_failures?.()),
    blockBuildOps: formatU64Decimal(handle.kernel.debug_cpu_profile_block_build_ops?.()),
    blockBuildBudget: formatU64Decimal(handle.kernel.debug_cpu_profile_block_build_budget?.()),
    blockBudgetSkips: formatU64Decimal(handle.kernel.debug_cpu_profile_block_budget_skips?.()),
    blockCacheUnsupported: formatU64Decimal(handle.kernel.debug_cpu_profile_block_cache_unsupported?.()),
    blockExecOps: formatU64Decimal(handle.kernel.debug_cpu_profile_block_exec_ops?.()),
    blockExecUnsupported: formatU64Decimal(handle.kernel.debug_cpu_profile_block_exec_unsupported?.()),
    blockExecOpSampleInterval: formatU64Decimal(handle.kernel.debug_cpu_profile_block_exec_op_sample_interval?.()),
    blockExecSampledBlocks: formatU64Decimal(handle.kernel.debug_cpu_profile_block_exec_sampled_blocks?.()),
    blockMissSampleInterval: formatU64Decimal(handle.kernel.debug_cpu_profile_block_miss_sample_interval?.()),
    blockMissSampled: formatU64Decimal(handle.kernel.debug_cpu_profile_block_miss_sampled?.()),
    blockMissSamples: readBlockMissSamples(),
    interpreterOps: formatU64Decimal(handle.kernel.debug_cpu_profile_interpreter_ops?.()),
    blockBuildOpCounts: readCounts(handle.kernel.debug_cpu_profile_block_build_op_count),
    blockExecOpCounts: readCounts(handle.kernel.debug_cpu_profile_block_exec_op_count),
    interpreterOpCounts: readCounts(handle.kernel.debug_cpu_profile_interpreter_op_count),
    unsupportedOpCounts: readCounts(handle.kernel.debug_cpu_profile_unsupported_op_count),
    memoryPermitChecks: formatU64Decimal(handle.kernel.debug_memory_permit_checks?.()),
    memoryPermitCacheHits: formatU64Decimal(handle.kernel.debug_memory_permit_cache_hits?.()),
    memoryPermitCoverHits: formatU64Decimal(handle.kernel.debug_memory_permit_cover_hits?.()),
    memoryPermitDenied: formatU64Decimal(handle.kernel.debug_memory_permit_denied?.()),
    memoryPermitByProt: readMemoryPermitByProt(),
  };
}

function readSyscallDebugStats(handle: ProcessHandle): SyscallDebugStats | null {
  const countAccessor = handle.kernel.debug_syscall_profile_count;
  if (!countAccessor) {
    return null;
  }
  const nonOkAccessor = handle.kernel.debug_syscall_profile_non_ok_status_count;
  const errorAccessor = handle.kernel.debug_syscall_profile_error_count;
  const topSyscalls: SyscallDebugStats["topSyscalls"] = [];
  for (let nr = 0; nr < 512; nr += 1) {
    const count = formatU64Decimal(countAccessor(nr));
    if (count === "0") {
      continue;
    }
    topSyscalls.push({
      nr,
      count,
      nonOkStatusCount: formatU64Decimal(nonOkAccessor?.(nr)),
      errorCount: formatU64Decimal(errorAccessor?.(nr)),
    });
  }
  topSyscalls.sort((left, right) => Number(BigInt(right.count) - BigInt(left.count)) || left.nr - right.nr);
  return { topSyscalls: topSyscalls.slice(0, 32) };
}

function hasThreadStepDebugSignal(snapshot: ThreadStepDebugSnapshot): boolean {
  return (snapshot.tid ?? 0) !== 0
    || (snapshot.phase ?? 0) !== 0
    || snapshot.status !== 0
    || snapshot.budget !== 0
    || (snapshot.remaining ?? 0) !== 0
    || snapshot.pc !== "0x0"
    || snapshot.ra !== "0x0"
    || snapshot.sp !== "0x0"
    || snapshot.s11 !== "0x0";
}

function readThreadStepEntryDebug(
  handle: Pick<ProcessHandle, "kernel">,
  vmPtr: number,
): ThreadStepDebugSnapshot | null {
  if (!handle.kernel.debug_thread_step_entry_for_vm_status) {
    return null;
  }
  const snapshot = {
    vmPtr,
    tid: handle.kernel.debug_thread_step_entry_for_vm_tid?.(vmPtr) ?? null,
    phase: null,
    status: handle.kernel.debug_thread_step_entry_for_vm_status?.(vmPtr) ?? 0,
    budget: handle.kernel.debug_thread_step_entry_for_vm_budget?.(vmPtr) ?? 0,
    remaining: null,
    pc: `0x${formatU64(handle.kernel.debug_thread_step_entry_for_vm_pc?.(vmPtr) ?? 0n)}`,
    ra: `0x${formatU64(handle.kernel.debug_thread_step_entry_for_vm_ra?.(vmPtr) ?? 0n)}`,
    sp: `0x${formatU64(handle.kernel.debug_thread_step_entry_for_vm_sp?.(vmPtr) ?? 0n)}`,
    s11: `0x${formatU64(handle.kernel.debug_thread_step_entry_for_vm_s11?.(vmPtr) ?? 0n)}`,
  } satisfies ThreadStepDebugSnapshot;
  return hasThreadStepDebugSignal(snapshot) ? snapshot : null;
}

function readThreadStepStatusDebug(
  handle: Pick<ProcessHandle, "kernel">,
  vmPtr: number,
): ThreadStepDebugSnapshot | null {
  if (!handle.kernel.debug_thread_step_for_vm_status) {
    return null;
  }
  const snapshot = {
    vmPtr,
    tid: handle.kernel.debug_thread_step_for_vm_tid?.(vmPtr) ?? null,
    phase: null,
    status: handle.kernel.debug_thread_step_for_vm_status?.(vmPtr) ?? 0,
    budget: handle.kernel.debug_thread_step_for_vm_budget?.(vmPtr) ?? 0,
    remaining: handle.kernel.debug_thread_step_for_vm_remaining?.(vmPtr) ?? null,
    pc: `0x${formatU64(handle.kernel.debug_thread_step_for_vm_pc?.(vmPtr) ?? 0n)}`,
    ra: `0x${formatU64(handle.kernel.debug_thread_step_for_vm_ra?.(vmPtr) ?? 0n)}`,
    sp: `0x${formatU64(handle.kernel.debug_thread_step_for_vm_sp?.(vmPtr) ?? 0n)}`,
    s11: `0x${formatU64(handle.kernel.debug_thread_step_for_vm_s11?.(vmPtr) ?? 0n)}`,
  } satisfies ThreadStepDebugSnapshot;
  return hasThreadStepDebugSignal(snapshot) ? snapshot : null;
}

function readThreadStepPhaseDebug(
  handle: Pick<ProcessHandle, "kernel">,
  vmPtr: number,
): ThreadStepDebugSnapshot | null {
  if (!handle.kernel.debug_thread_step_phase_for_vm) {
    return null;
  }
  const snapshot = {
    vmPtr,
    tid: null,
    phase: handle.kernel.debug_thread_step_phase_for_vm?.(vmPtr) ?? 0,
    status: handle.kernel.debug_thread_step_phase_for_vm_status?.(vmPtr) ?? 0,
    budget: handle.kernel.debug_thread_step_phase_for_vm_budget?.(vmPtr) ?? 0,
    remaining: handle.kernel.debug_thread_step_phase_for_vm_remaining?.(vmPtr) ?? null,
    pc: `0x${formatU64(handle.kernel.debug_thread_step_phase_for_vm_pc?.(vmPtr) ?? 0n)}`,
    ra: `0x${formatU64(handle.kernel.debug_thread_step_phase_for_vm_ra?.(vmPtr) ?? 0n)}`,
    sp: `0x${formatU64(handle.kernel.debug_thread_step_phase_for_vm_sp?.(vmPtr) ?? 0n)}`,
    s11: `0x${formatU64(handle.kernel.debug_thread_step_phase_for_vm_s11?.(vmPtr) ?? 0n)}`,
  } satisfies ThreadStepDebugSnapshot;
  return hasThreadStepDebugSignal(snapshot) ? snapshot : null;
}

function isSuspiciousLowStateWorkerLogEntry(entry: string | null): boolean {
  if (!entry) {
    return false;
  }
  return /(?:^|:)pc=0x[0-9a-f]{1,3}(?::|$)/i.test(entry)
    || /(?:^|:)sp=0x0(?::|$)/.test(entry)
    || /(?:^|:)s11=0x0(?::|$)/.test(entry)
    || /(?:^|[\s:])fault_pc=0x[0-9a-f]{1,3}(?::|[\s]|$)/i.test(entry);
}

function findLastCredibleWorkerState(
  workerDebugLog: readonly string[],
  vmPtr: number,
): string | null {
  const vmTag = `0x${vmPtr.toString(16)}`;
  for (let index = workerDebugLog.length - 1; index >= 0; index -= 1) {
    const entry = workerDebugLog[index];
    if (!entry?.includes(vmTag)) {
      continue;
    }
    if (!(entry.startsWith(`status:${vmTag}`) || entry.startsWith(`ready:${vmTag}`) || entry.startsWith(`step:${vmTag}`))) {
      continue;
    }
    if (!isSuspiciousLowStateWorkerLogEntry(entry)) {
      return entry;
    }
  }
  return null;
}

function collectLoggedWorkerVmPtrs(
  workerDebugLog: readonly string[],
): number[] {
  const vmPtrs = new Set<number>();
  for (const entry of workerDebugLog) {
    const match = entry.match(/^[a-z_-]+:0x([0-9a-f]+)/);
    if (!match) {
      continue;
    }
    const vmPtr = Number.parseInt(match[1] ?? "", 16);
    if (Number.isFinite(vmPtr) && vmPtr !== 0) {
      vmPtrs.add(vmPtr);
    }
  }
  return [...vmPtrs];
}

function formatWorkerExecTrace(
  handle: Pick<ProcessHandle, "kernel">,
  vmPtr: number,
): string | null {
  const len = handle.kernel.debug_exec_trace_len?.() ?? 0;
  if (len <= 0) {
    return null;
  }
  const targetEpoch = handle.kernel.debug_exec_trace_epoch_for_vm?.(vmPtr) ?? 0;
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const entryVmPtr = handle.kernel.debug_exec_trace_entry_vm_ptr?.(index) ?? 0;
    if (entryVmPtr !== vmPtr) {
      continue;
    }
    const entryEpoch = handle.kernel.debug_exec_trace_entry_epoch?.(index) ?? 0;
    if (targetEpoch !== 0 && entryEpoch !== targetEpoch) {
      continue;
    }
    const tid = handle.kernel.debug_exec_trace_entry_tid?.(index) ?? 0;
    const pc = handle.kernel.debug_exec_trace_entry_pc?.(index) ?? 0n;
    const raw = handle.kernel.debug_exec_trace_entry_raw?.(index) ?? 0;
    const step = handle.kernel.debug_exec_trace_entry_step?.(index) ?? 0;
    const sp = handle.kernel.debug_exec_trace_entry_sp?.(index) ?? 0n;
    const ra = handle.kernel.debug_exec_trace_entry_ra?.(index) ?? 0n;
    const a0 = handle.kernel.debug_exec_trace_entry_a0?.(index) ?? 0n;
    const s11 = handle.kernel.debug_exec_trace_entry_s11?.(index) ?? 0n;
    entries.push(
      `${index}:tid=${tid}:pc=0x${formatU64(pc)}:raw=0x${raw.toString(16)}:step=${step}:sp=0x${formatU64(sp)}:ra=0x${formatU64(ra)}:a0=0x${formatU64(a0)}:s11=0x${formatU64(s11)}`,
    );
  }
  if (entries.length === 0) {
    const faultLen = handle.kernel.debug_fault_exec_trace_len_for_vm?.(vmPtr) ?? 0;
    if (faultLen <= 0) {
      return null;
    }
    const faultEntries: string[] = [];
    for (let index = 0; index < faultLen; index += 1) {
      const pc = handle.kernel.debug_fault_exec_trace_entry_pc_for_vm?.(vmPtr, index) ?? 0n;
      const raw = handle.kernel.debug_fault_exec_trace_entry_raw_for_vm?.(vmPtr, index) ?? 0;
      const step = handle.kernel.debug_fault_exec_trace_entry_step_for_vm?.(vmPtr, index) ?? 0;
      const sp = handle.kernel.debug_fault_exec_trace_entry_sp_for_vm?.(vmPtr, index) ?? 0n;
      const ra = handle.kernel.debug_fault_exec_trace_entry_ra_for_vm?.(vmPtr, index) ?? 0n;
      const a0 = handle.kernel.debug_fault_exec_trace_entry_a0_for_vm?.(vmPtr, index) ?? 0n;
      const s11 = handle.kernel.debug_fault_exec_trace_entry_s11_for_vm?.(vmPtr, index) ?? 0n;
      faultEntries.push(
        `${index}:pc=0x${formatU64(pc)}:raw=0x${raw.toString(16)}:step=${step}:sp=0x${formatU64(sp)}:ra=0x${formatU64(ra)}:a0=0x${formatU64(a0)}:s11=0x${formatU64(s11)}`,
      );
    }
    return faultEntries.join(" ");
  }
  return entries.slice(-32).join(" ");
}

function formatWorkerThreadState(
  handle: Pick<ProcessHandle, "kernel">,
  vmPtr: number,
): string | null {
  const tid = handle.kernel.thread_tid?.(vmPtr);
  if (tid === undefined) {
    return null;
  }
  const pc = handle.kernel.thread_pc(vmPtr);
  const ra = handle.kernel.thread_ra?.(vmPtr) ?? 0n;
  const sp = handle.kernel.thread_sp(vmPtr);
  const a0 = handle.kernel.thread_a0(vmPtr);
  const a1 = handle.kernel.thread_a1(vmPtr);
  const a2 = handle.kernel.thread_reg?.(vmPtr, 12) ?? 0n;
  const a3 = handle.kernel.thread_reg?.(vmPtr, 13) ?? 0n;
  const gp = handle.kernel.thread_gp?.(vmPtr) ?? 0n;
  const tp = handle.kernel.thread_tp?.(vmPtr) ?? 0n;
  const s11 = handle.kernel.thread_s11?.(vmPtr) ?? 0n;
  const sigMask = handle.kernel.thread_sig_mask?.(vmPtr) ?? 0n;
  const sigPending = handle.kernel.thread_sig_pending(vmPtr);
  const sigCode = handle.kernel.thread_sig_info_code?.(vmPtr) ?? 0;
  return [
    `tid=${tid}`,
    `pc=0x${formatU64(pc)}`,
    `ra=0x${formatU64(ra)}`,
    `sp=0x${formatU64(sp)}`,
    `a0=${a0.toString()}`,
    `a1=${a1.toString()}`,
    `a2=0x${formatU64(a2)}`,
    `a3=0x${formatU64(a3)}`,
    `gp=0x${formatU64(gp)}`,
    `tp=0x${formatU64(tp)}`,
    `s11=0x${formatU64(s11)}`,
    `sig_mask=0x${formatU64(sigMask)}`,
    `sig_pending=0x${formatU64(sigPending)}`,
    `sig_code=${sigCode}`,
  ].join(":");
}

export function summarizeProcessWorkerLogFocus(
  handle: Pick<
    ProcessHandle,
    | "workers"
    | "runningWorker"
    | "readyWorkers"
    | "blockedWorkers"
    | "workerDebugLog"
    | "kernel"
  >,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): WorkerLogFocus[] {
  const attachedStates = [...handle.workers]
    .map((worker) => {
      const state = threadWorkerStates.get(worker);
      if (!state || state.vmPtr === 0) {
        return null;
      }
      return {
        vmPtr: state.vmPtr,
        ownerState: readWorkerOwnerState(handle, worker, state),
        waitingInWorkerStatus: state.waitingInWorkerStatus,
      };
    })
    .filter((entry): entry is {
      vmPtr: number;
      ownerState: string;
      waitingInWorkerStatus: number | null;
    } => entry !== null);
  const attachedVmPtrs = new Set(attachedStates.map((entry) => entry.vmPtr));
  const detachedStates = collectLoggedWorkerVmPtrs(handle.workerDebugLog)
    .filter((vmPtr) => !attachedVmPtrs.has(vmPtr))
    .map((vmPtr) => ({
      vmPtr,
      ownerState: "detached",
      waitingInWorkerStatus: null,
    }));

  return [...attachedStates, ...detachedStates]
    .map((state) => {
      const eventTail = collectWorkerEventTail(handle.workerDebugLog, state.vmPtr);
      const statusTail = collectWorkerStatusTail(handle.workerDebugLog, state.vmPtr);
      return {
        vmPtr: state.vmPtr,
        ownerState: state.ownerState,
        waitingInWorkerStatus: state.waitingInWorkerStatus,
        threadState: formatWorkerThreadState(handle, state.vmPtr),
        lastCredibleState: findLastCredibleWorkerState(handle.workerDebugLog, state.vmPtr),
        lastStatus: findLastWorkerLogEntry(handle.workerDebugLog, "status", state.vmPtr),
        lastStatusDebug: findLastWorkerLogEntry(handle.workerDebugLog, "status_debug", state.vmPtr),
        lastReady: findLastWorkerLogEntry(handle.workerDebugLog, "ready", state.vmPtr),
        lastReadyDebug: findLastWorkerLogEntry(handle.workerDebugLog, "ready-debug", state.vmPtr),
        lastStep: findLastWorkerLogEntry(handle.workerDebugLog, "step", state.vmPtr),
        lastWait20: findLastWorkerLogEntry(handle.workerDebugLog, "wait", state.vmPtr),
        lastResume20: findLastWorkerLogEntry(handle.workerDebugLog, "resume", state.vmPtr),
        lastWrite: findLastWorkerLogEntry(handle.workerDebugLog, "write", state.vmPtr),
        lastSleep: findLastWorkerLogEntry(handle.workerDebugLog, "sleep", state.vmPtr),
        lastYield: findLastWorkerLogEntry(handle.workerDebugLog, "yield", state.vmPtr),
        lastFaultDebug: findLastWorkerLogEntry(handle.workerDebugLog, "fault_debug", state.vmPtr),
        execTrace: formatWorkerExecTrace(handle, state.vmPtr),
        eventTail,
        statusTail,
      } satisfies WorkerLogFocus;
    })
    .filter((entry): entry is WorkerLogFocus => entry !== null)
    .sort((left, right) => left.vmPtr - right.vmPtr);
}

export function debugRuntimeWorkerLogFocus(
  runtimeId: number,
  processHandles: Map<number, ProcessHandle>,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): RuntimeProcessWorkerLogFocus[] {
  return [...processHandles.values()]
    .filter((handle) => handle.runtimeId === runtimeId)
    .sort((left, right) => left.pid - right.pid)
    .map((handle) => ({
      pid: handle.pid,
      cmd: handle.debugCommand,
      workers: summarizeProcessWorkerLogFocus(handle, threadWorkerStates),
    }));
}

function formatOptionalBigIntHex(value: bigint | null): string | null {
  return value === null ? null : `0x${formatU64(value)}`;
}

function readStatusNames(handle: Pick<ProcessHandle, "kernel">): Map<number, string> {
  return new Map([
    [handle.kernel.abi_status_ok(), "OK"],
    [handle.kernel.abi_status_fault(), "FAULT"],
    [handle.kernel.abi_status_fs(), "FS"],
    [handle.kernel.abi_status_futex_wait(), "FUTEX_WAIT"],
    [handle.kernel.abi_status_pipe_wait(), "PIPE_WAIT"],
    [handle.kernel.abi_status_epoll_wait(), "EPOLL_WAIT"],
    [handle.kernel.abi_status_vfork_wait(), "VFORK_WAIT"],
    [handle.kernel.abi_status_execve_spawn(), "EXECVE_SPAWN"],
    [handle.kernel.abi_status_clone_spawn(), "CLONE_SPAWN"],
    [handle.kernel.abi_status_sleep(), "SLEEP"],
    [handle.kernel.abi_status_jit_ready(), "JIT_READY"],
    [handle.kernel.abi_status_wait4(), "WAIT4"],
    [handle.kernel.abi_status_connect(), "CONNECT"],
    [handle.kernel.abi_status_socket_send(), "SOCKET_SEND"],
    [handle.kernel.abi_status_socket_recv(), "SOCKET_RECV"],
    [handle.kernel.abi_status_flock_wait(), "FLOCK_WAIT"],
  ]);
}

function formatStatusCountsByName(
  statusCounts: Record<string, number>,
  statusNames: Map<number, string>,
): Record<string, number> {
  const out: Record<string, number> = {};
  for (const [rawStatus, count] of Object.entries(statusCounts)) {
    const status = Number(rawStatus);
    out[statusNames.get(status) ?? rawStatus] = count;
  }
  return out;
}

function formatHotCounters(
  counters: Record<string, number>,
): Array<{ pc: string; count: number }> {
  return Object.entries(counters)
    .map(([pc, count]) => ({ pc, count }))
    .sort((left, right) => (
      right.count - left.count
      || left.pc.localeCompare(right.pc)
    ))
    .slice(0, 8);
}

function readMemoryVmas(
  handle: Pick<ProcessHandle, "kernel">,
): ProcessExecutionStats["memoryVmas"] {
  const len = handle.kernel.debug_memory_vma_len?.() ?? 0;
  const out: ProcessExecutionStats["memoryVmas"] = [];
  for (let index = 0; index < len; index += 1) {
    out.push({
      start: `0x${BigInt(handle.kernel.debug_memory_vma_start?.(index) ?? 0).toString(16)}`,
      end: `0x${BigInt(handle.kernel.debug_memory_vma_end?.(index) ?? 0).toString(16)}`,
      prot: handle.kernel.debug_memory_vma_prot?.(index) ?? 0,
      flags: handle.kernel.debug_memory_vma_flags?.(index) ?? 0,
    });
  }
  return out;
}

export function debugRuntimeExecutionStats(
  runtimeId: number,
  processHandles: Map<number, ProcessHandle>,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): ProcessExecutionStats[] {
  return [...processHandles.values()]
    .filter((handle) => handle.runtimeId === runtimeId)
    .sort((left, right) => left.pid - right.pid)
    .map((handle) => {
      const statusNames = readStatusNames(handle);
      return {
        pid: handle.pid,
        cmd: handle.debugCommand,
        running: handle.runningWorker !== null,
        memoryBytes: handle.memory.buffer.byteLength,
        ramSize: handle.ramSize,
        maxMemoryBytes: handle.maxMemoryPages * 65536,
        forkMaxMemoryBytes: handle.forkMaxMemoryPages * 65536,
        guestBase: handle.guestBase,
        readyWorkers: handle.readyWorkers.length,
        blockedWorkers: handle.blockedWorkers.size,
        pendingChildExitRecords: handle.pendingChildExitRecords,
        pendingChildSpawnRecords: handle.pendingChildSpawnRecords,
        activeChildren: [...processHandles.values()].filter((candidate) => candidate.parentPid === handle.pid).length,
        runtimeBridgeDebugStage: readRuntimeBridgeDebugStage(handle.runtimeBridge),
        workerMessageDebugStats: {
          ...handle.workerMessageDebugStats,
          lastEvents: handle.workerMessageDebugStats.lastEvents.slice(-16),
        },
        transferDebugStats: { ...handle.transferDebugStats },
        memoryLogTail: handle.workerDebugLog
          .filter((entry) =>
            entry.startsWith("memory-state:")
            || entry.startsWith("plain-fork-memory:")
          )
          .slice(-8),
        memoryVmas: readMemoryVmas(handle),
        cpuDebugStats: readCpuDebugStats(handle),
        syscallDebugStats: readSyscallDebugStats(handle),
        workers: [...handle.workers]
          .map((worker) => {
            const state = threadWorkerStates.get(worker);
            if (!state || state.vmPtr === 0) {
              return null;
            }
            return {
              vmPtr: state.vmPtr,
              ownerState: readWorkerOwnerState(handle, worker, state),
              stepCount: state.stepCount,
              totalStepBudget: state.totalStepBudget,
              totalStepWallMs: state.totalStepWallMs,
              maxStepWallMs: state.maxStepWallMs,
              lastStepDurationMs: state.lastStepDurationMs,
              statusCount: state.statusCount,
              statusCounts: { ...state.statusCounts },
              statusCountsByName: formatStatusCountsByName(state.statusCounts, statusNames),
              lastStatus: state.lastStatus,
              lastStatusName: state.lastStatus === null ? null : statusNames.get(state.lastStatus) ?? String(state.lastStatus),
              lastStepAtMs: state.lastStepAtMs,
              lastStatusAtMs: state.lastStatusAtMs,
              lastStepPc: formatOptionalBigIntHex(state.lastStepPc),
              lastStepSp: formatOptionalBigIntHex(state.lastStepSp),
              lastStepS11: formatOptionalBigIntHex(state.lastStepS11),
              hotStepPcs: formatHotCounters(state.hotStepPcs),
              lastStatusPc: formatOptionalBigIntHex(state.lastStatusPc),
              lastStatusSp: formatOptionalBigIntHex(state.lastStatusSp),
              lastStatusS11: formatOptionalBigIntHex(state.lastStatusS11),
              hotStatusPcs: formatHotCounters(state.hotStatusPcs),
              waitingInWorkerStatus: state.waitingInWorkerStatus,
              threadStepEntry: readThreadStepEntryDebug(handle, state.vmPtr),
              threadStepStatus: readThreadStepStatusDebug(handle, state.vmPtr),
              threadStepPhase: readThreadStepPhaseDebug(handle, state.vmPtr),
            } satisfies WorkerExecutionStats;
          })
          .filter((entry): entry is WorkerExecutionStats => entry !== null)
          .sort((left, right) => left.vmPtr - right.vmPtr),
      };
    });
}

function formatThreadDebug(handle: ProcessHandle, vmPtr: number): string {
  const tid = handle.kernel.thread_tid?.(vmPtr) ?? 0;
  const pc = handle.kernel.thread_pc?.(vmPtr) ?? 0n;
  const ra = handle.kernel.thread_ra?.(vmPtr) ?? 0n;
  const sp = handle.kernel.thread_sp?.(vmPtr) ?? 0n;
  const a0 = handle.kernel.thread_a0?.(vmPtr) ?? 0n;
  const a1 = handle.kernel.thread_a1?.(vmPtr) ?? 0n;
  const exitCode = handle.kernel.thread_exit_code?.(vmPtr) ?? 0;
  const termSignal = handle.kernel.thread_term_signal?.(vmPtr) ?? 0;
  const faultPc = handle.kernel.thread_fault_pc?.(vmPtr) ?? 0n;
  const faultAddr = handle.kernel.thread_fault_addr?.(vmPtr) ?? 0n;
  const sigPending = handle.kernel.thread_sig_pending?.(vmPtr) ?? 0n;
  const cloneFlags = handle.kernel.thread_clone_flags?.(vmPtr) ?? 0n;
  const cloneChildTidptr = handle.kernel.thread_clone_child_tidptr?.(vmPtr) ?? 0n;
  const parts = [
    `tid=${tid}`,
    `pc=0x${pc.toString(16)}`,
    `ra=0x${ra.toString(16)}`,
    `sp=0x${sp.toString(16)}`,
    `a0=${a0.toString()}`,
    `a1=${a1.toString()}`,
    `exit=${exitCode}`,
    `sig=${termSignal}`,
    `fault_pc=0x${faultPc.toString(16)}`,
    `fault_addr=0x${faultAddr.toString(16)}`,
    `sig_pending=0x${sigPending.toString(16)}`,
    `clone_flags=0x${cloneFlags.toString(16)}`,
    `clone_child_tidptr=0x${cloneChildTidptr.toString(16)}`,
  ];
  const syscallNr = handle.kernel.debug_syscall_trace_nr?.();
  if (syscallNr !== undefined && syscallNr !== 0n) {
    parts.push(`syscall=${syscallNr.toString()}`);
    parts.push(`status=${handle.kernel.debug_syscall_trace_status?.() ?? "n/a"}`);
    parts.push(`result=${handle.kernel.debug_syscall_trace_result?.().toString() ?? "n/a"}`);
  }
  const signalDeliverySig = handle.kernel.debug_signal_delivery_sig?.();
  if (signalDeliverySig !== undefined && signalDeliverySig !== 0) {
    parts.push(`sig_deliver=${signalDeliverySig}`);
    parts.push(`sig_pc=0x${(handle.kernel.debug_signal_delivery_pc?.() ?? 0n).toString(16)}`);
  }
  const spawnKind = handle.kernel.thread_spawn_kind?.(vmPtr) ?? 0;
  if (spawnKind !== 0) {
    const spawnPath = handle.kernel.thread_spawn_path?.(vmPtr) ?? 0n;
    const spawnArgv = handle.kernel.thread_spawn_argv?.(vmPtr) ?? 0n;
    const spawnEnvp = handle.kernel.thread_spawn_envp?.(vmPtr) ?? 0n;
    parts.push(`spawn_kind=${spawnKind}`);
    parts.push(`spawn_path=0x${spawnPath.toString(16)}:${JSON.stringify(readGuestCString(handle, spawnPath) ?? "")}`);
    parts.push(`spawn_argv=0x${spawnArgv.toString(16)}:${formatGuestStrvec(handle, spawnArgv, 4)}`);
    parts.push(`spawn_envp=0x${spawnEnvp.toString(16)}:${formatGuestStrvec(handle, spawnEnvp, 4)}`);
  }
  const execveStage = handle.kernel.debug_execve_stage?.() ?? 0;
  if (execveStage !== 0) {
    parts.push(`execve_stage=${execveStage}`);
    parts.push(`execve_v0=${handle.kernel.debug_execve_value0?.().toString() ?? "n/a"}`);
    parts.push(`execve_v1=${handle.kernel.debug_execve_value1?.().toString() ?? "n/a"}`);
  }
  return parts.join(" ");
}

function readGuestCString(handle: ProcessHandle, addr: bigint | number): string | null {
  const ptr = typeof addr === "bigint" ? Number(addr) : addr;
  if (!Number.isFinite(ptr) || ptr <= 0) {
    return null;
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  const out: number[] = [];
  for (let i = 0; i < 256; i += 1) {
    const value = bytes[handle.guestBase + ptr + i];
    if (value === undefined) {
      return null;
    }
    if (value === 0) {
      return decoder.decode(new Uint8Array(out));
    }
    out.push(value);
  }
  return null;
}

function readAbsoluteCString(handle: ProcessHandle, ptr: number): string | null {
  if (!Number.isFinite(ptr) || ptr <= 0 || ptr >= handle.memory.buffer.byteLength) {
    return null;
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  const out: number[] = [];
  for (let i = 0; i < 256; i += 1) {
    const value = bytes[ptr + i];
    if (value === undefined) {
      return null;
    }
    if (value === 0) {
      return decoder.decode(new Uint8Array(out));
    }
    out.push(value);
  }
  return null;
}

function readGuestU64(handle: ProcessHandle, addr: bigint | number): bigint | null {
  const ptr = typeof addr === "bigint" ? Number(addr) : addr;
  if (!Number.isFinite(ptr) || ptr < 0) {
    return null;
  }
  const absolute = handle.guestBase + ptr;
  if (absolute < 0 || absolute + 8 > handle.memory.buffer.byteLength) {
    return null;
  }
  return new DataView(handle.memory.buffer).getBigUint64(absolute, true);
}

function formatGuestStrvec(handle: ProcessHandle, vecAddr: bigint, maxEntries: number): string {
  if (vecAddr === 0n) {
    return "[]";
  }
  const entries: string[] = [];
  for (let index = 0; index < maxEntries; index += 1) {
    const ptr = readGuestU64(handle, vecAddr + BigInt(index * 8));
    if (ptr === null) {
      entries.push(`@${index}=unreadable`);
      break;
    }
    if (ptr === 0n) {
      break;
    }
    entries.push(`@${index}=0x${ptr.toString(16)}:${JSON.stringify(readGuestCString(handle, ptr) ?? "")}`);
  }
  return `[${entries.join(",")}]`;
}

function formatProcessSyscallTrace(handle: ProcessHandle): string {
  const len = handle.kernel.debug_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return "";
  }
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_syscall_trace_entry_vm_ptr?.(index) ?? 0;
    const nr = handle.kernel.debug_syscall_trace_entry_nr?.(index);
    const status = handle.kernel.debug_syscall_trace_entry_status?.(index) ?? 0;
    const result = handle.kernel.debug_syscall_trace_entry_result?.(index);
    entries.push(
      `${index}:vm=0x${traceVmPtr.toString(16)}:nr=${nr?.toString() ?? "n/a"}:status=${status}:result=${result?.toString() ?? "n/a"}`,
    );
  }
  return entries.join(" ");
}

function formatProcessRiscvSyscallTrace(handle: ProcessHandle): string {
  const len = handle.kernel.debug_riscv_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return "";
  }
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_riscv_syscall_trace_entry_vm_ptr?.(index) ?? 0;
    const nr = handle.kernel.debug_riscv_syscall_trace_entry_nr?.(index);
    const arg0 = handle.kernel.debug_riscv_syscall_trace_entry_arg0?.(index);
    const arg1 = handle.kernel.debug_riscv_syscall_trace_entry_arg1?.(index);
    const arg2 = handle.kernel.debug_riscv_syscall_trace_entry_arg2?.(index);
    const arg3 = handle.kernel.debug_riscv_syscall_trace_entry_arg3?.(index);
    const arg4 = handle.kernel.debug_riscv_syscall_trace_entry_arg4?.(index);
    const arg5 = handle.kernel.debug_riscv_syscall_trace_entry_arg5?.(index);
    const result = handle.kernel.debug_riscv_syscall_trace_entry_result?.(index);
    const status = handle.kernel.debug_riscv_syscall_trace_entry_status?.(index) ?? 0;
    entries.push(
      `${index}:vm=0x${traceVmPtr.toString(16)}:nr=${nr?.toString() ?? "n/a"}`
      + `:a0=${arg0?.toString() ?? "n/a"}:a1=${arg1?.toString() ?? "n/a"}`
      + `:a2=${arg2?.toString() ?? "n/a"}:a3=${arg3?.toString() ?? "n/a"}`
      + `:a4=${arg4?.toString() ?? "n/a"}:a5=${arg5?.toString() ?? "n/a"}`
      + `:status=${status}:result=${result?.toString() ?? "n/a"}`,
    );
  }
  return entries.join(" ");
}

function formatProcessFdTrace(handle: ProcessHandle): string {
  const len = handle.kernel.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return "";
  }
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
    const nr = handle.kernel.debug_fd_syscall_trace_nr?.(index);
    const arg0 = handle.kernel.debug_fd_syscall_trace_arg0?.(index);
    const arg0Ofd = handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0;
    const arg0Type = handle.kernel.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0;
    const arg0HostFd = handle.kernel.debug_fd_syscall_trace_arg0_ofd_host_fd?.(index);
    const arg0StatusFlags = handle.kernel.debug_fd_syscall_trace_arg0_ofd_status_flags?.(index);
    const arg0Ino = handle.kernel.debug_fd_syscall_trace_arg0_ofd_ino?.(index);
    const arg0InodeMode = handle.kernel.debug_fd_syscall_trace_arg0_inode_mode?.(index);
    const arg0PipeGeneration = handle.kernel.debug_fd_syscall_trace_arg0_pipe_generation?.(index);
    const arg1 = handle.kernel.debug_fd_syscall_trace_arg1?.(index);
    const arg2 = handle.kernel.debug_fd_syscall_trace_arg2?.(index);
    const arg2Ofd = handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? 0;
    const arg2Type = handle.kernel.debug_fd_syscall_trace_arg2_ofd_type?.(index) ?? 0;
    const arg3 = handle.kernel.debug_fd_syscall_trace_arg3?.(index);
    const result = handle.kernel.debug_fd_syscall_trace_result?.(index);
    const status = handle.kernel.debug_fd_syscall_trace_status?.(index) ?? 0;
    const pidSample0 = handle.kernel.debug_fd_syscall_trace_pid_sample0?.(index);
    const pidSample1 = handle.kernel.debug_fd_syscall_trace_pid_sample1?.(index);
    entries.push(
      `${index}:vm=0x${traceVmPtr.toString(16)}:nr=${nr?.toString() ?? "n/a"}`
      + `:a0=${arg0?.toString() ?? "n/a"}/ofd=${arg0Ofd}/type=${arg0Type}/host=${arg0HostFd ?? "n/a"}`
      + `/flags=${arg0StatusFlags ?? "n/a"}/ino=${arg0Ino ?? "n/a"}/mode=${arg0InodeMode ?? "n/a"}/gen=${arg0PipeGeneration ?? "n/a"}`
      + `:a1=${arg1?.toString() ?? "n/a"}`
      + `:a2=${arg2?.toString() ?? "n/a"}/ofd=${arg2Ofd}/type=${arg2Type}`
      + `:a3=${arg3?.toString() ?? "n/a"}`
      + `:status=${status}:result=${result?.toString() ?? "n/a"}`
      + `:pid=${pidSample0?.toString() ?? "n/a"}/${pidSample1?.toString() ?? "n/a"}`,
    );
  }
  return entries.join(" ");
}

function formatProcessPathTrace(handle: ProcessHandle): string {
  const len = handle.kernel.debug_path_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return "";
  }
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_path_syscall_trace_vm_ptr?.(index) ?? 0;
    const nr = handle.kernel.debug_path_syscall_trace_nr?.(index);
    const dirfd = handle.kernel.debug_path_syscall_trace_dirfd?.(index);
    const status = handle.kernel.debug_path_syscall_trace_status?.(index) ?? 0;
    const result = handle.kernel.debug_path_syscall_trace_result?.(index);
    const path = readAbsoluteCString(handle, handle.kernel.debug_path_syscall_trace_path_ptr?.(index) ?? 0);
    entries.push(
      `${index}:vm=0x${traceVmPtr.toString(16)}:nr=${nr?.toString() ?? "n/a"}:dirfd=${dirfd?.toString() ?? "n/a"}:status=${status}:result=${result?.toString() ?? "n/a"}:path=${JSON.stringify(path ?? "")}`,
    );
  }
  return entries.join(" ");
}

function formatExecveError(handle: ProcessHandle): string {
  const errno = handle.kernel.debug_execve_error_errno?.();
  const stage = handle.kernel.debug_execve_error_stage?.() ?? 0;
  const value0 = handle.kernel.debug_execve_error_value0?.();
  const value1 = handle.kernel.debug_execve_error_value1?.();
  if ((errno === undefined || Number(errno) === 0) && stage === 0) {
    return "none";
  }
  return [
    `errno=${errno?.toString() ?? "n/a"}`,
    `stage=${stage}`,
    `value0=${value0?.toString() ?? "n/a"}`,
    `value1=${value1?.toString() ?? "n/a"}`,
  ].join(":");
}

function formatSocketRecvBufferHeadTail(handle: ProcessHandle, socketIdx: number): string {
  if (socketIdx < 0) {
    return "-1/-1";
  }
  const recvBufIdx = handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1;
  if (recvBufIdx < 0) {
    return "-1/-1";
  }
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? handle.kernel.kernel_net_buf_pool_bytes() / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return "-1/-1";
  }
  const absolute = handle.kernelState.netBufBase + recvBufIdx * netBufferBytes;
  const view = new DataView(handle.memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
  return `${view.getUint32(NET_BUFFER_HEAD_OFFSET, true)}/${view.getUint32(NET_BUFFER_TAIL_OFFSET, true)}`;
}

export function debugRuntimeProcessHandles(
  runtimeId: number,
  processHandles: Map<number, ProcessHandle>,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
  runtimeLifecycleEvents: Map<number, string[]>,
  formatPipeCounterDebug: (handle: ProcessHandle, fd: number) => string,
): string {
  const lifecycle = runtimeLifecycleEvents.get(runtimeId);
  const lines = [...processHandles.values()]
    .filter((handle) => handle.runtimeId === runtimeId)
    .sort((left, right) => left.pid - right.pid)
    .map((handle) => {
      try {
        const processPtr = handle.kernel.thread_process_ptr(handle.vmPtr);
        const blocked = [...handle.blockedWorkers.values()]
          .map((state) => `${state.status}@0x${state.vmPtr.toString(16)}`)
          .join(",");
        const workerStates = [...handle.workers]
          .map((worker) => {
            const state = threadWorkerStates.get(worker);
            if (!state) {
              return null;
            }
            return `0x${state.vmPtr.toString(16)}:${readWorkerOwnerState(handle, worker, state)}`;
          })
          .filter(Boolean)
          .join(",");
        const workerDetails = [...handle.workers]
          .map((worker) => {
            const state = threadWorkerStates.get(worker);
            if (!state) {
              return null;
            }
            return formatWorkerSchedulerDetails(handle, worker, state);
          })
          .filter(Boolean)
          .join(" ");
        const workerFocusSummary = summarizeProcessWorkerLogFocus(handle, threadWorkerStates)
          .map((worker) => [
            `0x${worker.vmPtr.toString(16)}`,
            worker.ownerState,
            worker.lastCredibleState ? `credible=${JSON.stringify(worker.lastCredibleState)}` : null,
            worker.lastWait20 ? `wait=${JSON.stringify(worker.lastWait20)}` : null,
            worker.lastResume20 ? `resume=${JSON.stringify(worker.lastResume20)}` : null,
            worker.lastSleep ? `sleep=${JSON.stringify(worker.lastSleep)}` : null,
            worker.lastFaultDebug ? `fault=${JSON.stringify(worker.lastFaultDebug)}` : null,
          ].filter(Boolean).join(":"))
          .join(" ");
        const threadStates = Array.from({ length: handle.kernel.abi_max_threads() }, (_value, slot) => {
          const threadVmPtr = handle.kernel.process_thread_vm_ptr(handle.vmPtr, slot);
          if (threadVmPtr === 0) {
            return null;
          }
          return `${slot}:${handle.kernel.process_thread_tid(handle.vmPtr, slot)}/${handle.kernel.process_thread_state(handle.vmPtr, slot)}/0x${threadVmPtr.toString(16)}`;
        }).filter(Boolean).join(",");
        const fdSummary = Array.from({ length: handle.kernel.abi_max_fds() }, (_, fd) => {
          const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
          if (ofdId === 0) {
            return null;
          }
          return [
            `${fd}->${ofdId}`,
            `flags=${handle.kernel.process_fd_flags(handle.vmPtr, fd)}`,
            `type=${handle.kernel.kernel_ofd_fd_type(ofdId)}`,
            `ino=${handle.kernel.kernel_ofd_ino(ofdId)}`,
            `host=${handle.kernel.kernel_ofd_host_fd(ofdId)}`,
            `status=${handle.kernel.kernel_ofd_status_flags(ofdId)}`,
            `offset=${handle.kernel.kernel_ofd_offset(ofdId).toString()}`,
          ].join("/");
        }).filter(Boolean).join(" ");
        const networkLogSummary = [
          `inject=${handle.workerDebugLog.filter((entry) => entry.startsWith("net-inject:")).length}`,
          `drain_write=${handle.workerDebugLog.filter((entry) => entry.startsWith("net-drain:") && entry.endsWith(":write")).length}`,
          `drain_empty=${handle.workerDebugLog.filter((entry) => entry.startsWith("net-drain:") && entry.includes(":n=0:")).length}`,
          `close=${handle.workerDebugLog.filter((entry) => entry.startsWith("net-close:")).length}`,
          `samples=${handle.workerDebugLog
            .filter((entry) => (
              entry.startsWith("net-read-")
              || entry.startsWith("net-inbound:")
              || entry.startsWith("net-inject")
              || entry.startsWith("net-close:")
              || entry.startsWith("net-force-close:")
              || entry.startsWith("socket-state-")
              || entry.startsWith("socket-sync-")
              || entry.startsWith("socket-restore-")
            ))
            .slice(-80)
            .join("|")}`,
        ].join("/");
        const networkFdSummary = [...handle.networkConnections.keys()]
          .map((fd) => {
            const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(handle.vmPtr, fd) ?? -1;
            const peerIdx = socketIdx >= 0
              ? handle.kernel.thread_socket_peer_idx?.(socketIdx) ?? -1
              : -1;
            return [
              `fd=${fd}`,
              `sock=${socketIdx}`,
              `state=${socketIdx >= 0 ? handle.kernel.thread_socket_state?.(socketIdx) ?? -1 : -1}`,
              `buf=${socketIdx >= 0 ? handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1 : -1}`,
              `recv=${socketIdx >= 0 ? handle.kernel.thread_socket_recv_available?.(socketIdx) ?? -1 : -1}`,
              `ht=${formatSocketRecvBufferHeadTail(handle, socketIdx)}`,
              `read_count=${socketIdx >= 0 ? handle.kernel.thread_socket_read_count?.(socketIdx) ?? -1 : -1}`,
              `read_bytes=${socketIdx >= 0 ? handle.kernel.thread_socket_read_bytes?.(socketIdx)?.toString() ?? "-1" : "-1"}`,
              `read_last=${socketIdx >= 0 ? [
                handle.kernel.thread_socket_read_last_fd?.(socketIdx) ?? -1,
                handle.kernel.thread_socket_read_last_result?.(socketIdx)?.toString() ?? "-1",
                handle.kernel.thread_socket_read_last_before?.(socketIdx) ?? -1,
                handle.kernel.thread_socket_read_last_after?.(socketIdx) ?? -1,
              ].join("/") : "-1"}`,
              `peer=${peerIdx}`,
              `peer_buf=${peerIdx >= 0 ? handle.kernel.thread_socket_recv_buf_idx?.(peerIdx) ?? -1 : -1}`,
              `peer_recv=${peerIdx >= 0 ? handle.kernel.thread_socket_recv_available?.(peerIdx) ?? -1 : -1}`,
            ].join(":");
          })
          .join(" ");
        const socketFdSummary = Array.from({ length: handle.kernel.abi_max_fds() }, (_, fd) => {
          const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
          if (
            ofdId === 0
            || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_socket()
          ) {
            return null;
          }
          const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(handle.vmPtr, fd) ?? -1;
          const peerIdx = socketIdx >= 0
            ? handle.kernel.thread_socket_peer_idx?.(socketIdx) ?? -1
            : -1;
          return [
            `fd=${fd}`,
            `sock=${socketIdx}`,
            `state=${socketIdx >= 0 ? handle.kernel.thread_socket_state?.(socketIdx) ?? -1 : -1}`,
            `buf=${socketIdx >= 0 ? handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1 : -1}`,
            `recv=${socketIdx >= 0 ? handle.kernel.thread_socket_recv_available?.(socketIdx) ?? -1 : -1}`,
            `ht=${formatSocketRecvBufferHeadTail(handle, socketIdx)}`,
            `read_count=${socketIdx >= 0 ? handle.kernel.thread_socket_read_count?.(socketIdx) ?? -1 : -1}`,
            `read_bytes=${socketIdx >= 0 ? handle.kernel.thread_socket_read_bytes?.(socketIdx)?.toString() ?? "-1" : "-1"}`,
            `read_last=${socketIdx >= 0 ? [
              handle.kernel.thread_socket_read_last_fd?.(socketIdx) ?? -1,
              handle.kernel.thread_socket_read_last_result?.(socketIdx)?.toString() ?? "-1",
              handle.kernel.thread_socket_read_last_before?.(socketIdx) ?? -1,
              handle.kernel.thread_socket_read_last_after?.(socketIdx) ?? -1,
            ].join("/") : "-1"}`,
            `peer=${peerIdx}`,
            `peer_buf=${peerIdx >= 0 ? handle.kernel.thread_socket_recv_buf_idx?.(peerIdx) ?? -1 : -1}`,
            `peer_recv=${peerIdx >= 0 ? handle.kernel.thread_socket_recv_available?.(peerIdx) ?? -1 : -1}`,
          ].join(":");
        }).filter(Boolean).join(" ");

        return [
          `pid=${handle.pid}`,
          `cmd=${JSON.stringify(handle.debugCommand)}`,
          `ppid=${handle.parentPid ?? 0}`,
          `vm=0x${handle.vmPtr.toString(16)}`,
          `process=0x${processPtr.toString(16)}`,
          `workers=${handle.workers.size}`,
          `worker_states=[${workerStates}]`,
          `worker_details=[${workerDetails}]`,
          `worker_focus=[${workerFocusSummary}]`,
          `thread_states=[${threadStates}]`,
          `ready=${handle.readyWorkers.length}`,
          `running=${handle.runningWorker ? "1" : "0"}`,
          `blocked=[${blocked}]`,
          `pipe_sync=${handle.pipeSyncPromise === null ? 0 : 1}/${handle.pipeSyncQueued ? 1 : 0}`,
          `fork_sync=${handle.forkSharedSyncPromise === null ? 0 : 1}/${handle.kernelShare.forkSharedSyncPromise === null ? 0 : 1}`,
          `vfork=${handle.vforkSnapshot ? "1" : "0"}`,
          `stdio_pipes=${handle.stdinPipe}/${handle.stdoutPipe}/${handle.stderrPipe}`,
          `process_stdio=${handle.kernel.process_stdin_pipe(handle.vmPtr)}/${handle.kernel.process_stdout_pipe(handle.vmPtr)}/${handle.kernel.process_stderr_pipe(handle.vmPtr)}`,
          `stdio_pipe_counters=[${[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            .map((fd) => formatPipeCounterDebug(handle, fd))
            .join(" ")}]`,
          `shared_ofds=[${[...handle.kernelShare.sharedOfdIds].sort((left, right) => left - right).join(",")}]`,
          `fds=[${fdSummary}]`,
          `worker_net=[${networkLogSummary}]`,
          `network_fds=[${networkFdSummary}]`,
          `network_closed_fds=[${[...handle.networkClosedFds].sort((left, right) => left - right).join(",")}]`,
          `socket_fds=[${socketFdSummary}]`,
          `worker_log=[${handle.workerDebugLog.slice(-48).join(", ")}]`,
          `debug=${handle.lastDebugMessage ?? "n/a"}`,
          `execve_error=${formatExecveError(handle)}`,
          `syscalls=[${formatProcessSyscallTrace(handle)}]`,
          `riscv_syscalls=[${formatProcessRiscvSyscallTrace(handle)}]`,
          `fd_syscalls=[${formatProcessFdTrace(handle)}]`,
          `path_syscalls=[${formatProcessPathTrace(handle)}]`,
        ].join(" ");
      } catch (error) {
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        return [
          `pid=${handle.pid}`,
          `cmd=${JSON.stringify(handle.debugCommand)}`,
          `vm=0x${handle.vmPtr.toString(16)}`,
          `debug_error=${JSON.stringify(detail)}`,
          `execve_error=${formatExecveError(handle)}`,
          `syscalls=[${formatProcessSyscallTrace(handle)}]`,
          `riscv_syscalls=[${formatProcessRiscvSyscallTrace(handle)}]`,
          `path_syscalls=[${formatProcessPathTrace(handle)}]`,
        ].join(" ");
      }
    });
  if (lifecycle && lifecycle.length > 0) {
    const fsLifecycleTail = lifecycle
      .filter((event) => event.startsWith("fs-"))
      .slice(-120);
    if (fsLifecycleTail.length > 0) {
      lines.unshift(`fs_lifecycle_tail=[${fsLifecycleTail.join(", ")}]`);
    }
    const processLifecycleTail = lifecycle
      .filter((event) => (
        event.includes("fork")
        || event.includes("spawn")
        || event.includes("child")
        || event.includes("exit")
        || event.includes("wait4")
        || event.includes("reroute")
        || event.includes("ownership")
      ))
      .slice(-160);
    if (processLifecycleTail.length > 0) {
      lines.push(`process_lifecycle_tail=[${processLifecycleTail.join(", ")}]`);
    }
    lines.push(`lifecycle=[${lifecycle.join(", ")}]`);
  }
  return lines.join("\n");
}

export function appendRuntimeLifecycleEvent(
  runtimeId: number,
  event: string,
  runtimeLifecycleEvents: Map<number, string[]>,
): void {
  const events = runtimeLifecycleEvents.get(runtimeId) ?? [];
  events.push(event);
  if (events.length > 5000) {
    const important = events.filter((entry) => (
      entry.includes("fork")
      || entry.includes("spawn")
      || entry.includes("child")
      || entry.includes("exit")
      || entry.includes("wait4")
      || entry.includes("status-file")
      || entry.includes("reroute")
      || entry.includes("ownership")
    ));
    const tail = events.slice(-3000);
    events.splice(0, events.length, ...[...new Set([...important.slice(-2000), ...tail])]);
  }
  runtimeLifecycleEvents.set(runtimeId, events);
}

export function formatAllLiveThreadDebug(
  handle: ProcessHandle,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): string {
  const vmPtrs = new Set<number>();
  if (handle.vmPtr !== 0) {
    vmPtrs.add(handle.vmPtr);
  }
  vmPtrs.add(threadWorkerStates.get(handle.worker)?.vmPtr ?? 0);
  for (const worker of handle.workers) {
    vmPtrs.add(threadWorkerStates.get(worker)?.vmPtr ?? 0);
  }
  return [...vmPtrs]
    .filter((vmPtr) => vmPtr !== 0)
    .map((vmPtr) => {
      const tid = handle.kernel.thread_tid?.(vmPtr) ?? 0;
      return `${tid}:${JSON.stringify(formatThreadDebug(handle, vmPtr))}`;
    })
    .join(" ");
}

export function assertProcessStandardFdIntegrity(
  handle: ProcessHandle,
  context: string,
  debugSnapshot: string,
  formatPipeCounterDebug: (handle: ProcessHandle, fd: number) => string,
): void {
  const selfFdSummary = (): string => Array.from({ length: 16 }, (_value, fd) => {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId === 0) {
      return null;
    }
    return `${fd}->${ofdId}/${handle.kernel.process_fd_flags(handle.vmPtr, fd)}/${handle.kernel.kernel_ofd_fd_type(ofdId)}/${handle.kernel.kernel_ofd_ino(ofdId)}`;
  }).filter(Boolean).join(" ");
  if (handle.pid <= 0) {
    return;
  }
  for (const fd of [0, 1, 2]) {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId === 0) {
      continue;
    }
    const type = handle.kernel.kernel_ofd_fd_type(ofdId);
    if (type <= 0 || type > 64) {
      throw new Error(
        `${context}: pid=${handle.pid} fd=${fd} ofd=${ofdId} type=${type}`
        + ` self_fds=[${selfFdSummary()}]`
        + ` self_syscalls=[${formatProcessSyscallTrace(handle)}]`
        + ` self_riscv_syscalls=[${formatProcessRiscvSyscallTrace(handle)}]\n${debugSnapshot}`,
      );
    }
    if (type !== handle.kernel.abi_fd_type_pipe()) {
      continue;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    const base = handle.kernelState.pipeBase + pipeSlot * (32 + 65_536);
    const i32 = new Int32Array(handle.memory.buffer);
    const readOpen = Atomics.load(i32, (base + 8) >>> 2);
    const writeOpen = Atomics.load(i32, (base + 12) >>> 2);
    if (readOpen >= 0 && writeOpen >= 0) {
      continue;
    }
    throw new Error(
      `${context}: pid=${handle.pid} fd=${fd} ofd=${ofdId} pipe=${pipeSlot} ro=${readOpen} wo=${writeOpen}\n`
      + `${formatPipeCounterDebug(handle, fd)}\n`
      + debugSnapshot,
    );
  }
}
