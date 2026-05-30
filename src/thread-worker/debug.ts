import type { KernelExports } from "../abi.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const FD_TRACE_LOG_ENABLED = ((globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.TIDEMARK_WORKER_DEBUG_FD_TRACE ?? "") === "1";
const RESERVATION_DEBUG_ENABLED = ((globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.TIDEMARK_WORKER_DEBUG_RESERVATION ?? "") === "1";
const loggedFdTraceEntries = new Set<string>();
export const THREAD_WORKER_DEBUG_OBSERVATION_MODE = "read-only-observation" as const;

function copyBytes(view: Uint8Array): Uint8Array {
  const copy = new Uint8Array(view.byteLength);
  copy.set(view);
  return copy;
}

function formatU64(value: bigint): string {
  return BigInt.asUintN(64, value).toString(16);
}

function formatTraceArg(value: bigint): string {
  return `0x${formatU64(value)}`;
}

function readOptionalTraceU64(value: bigint | number | undefined): bigint {
  if (value === undefined) {
    return 0n;
  }
  return typeof value === "bigint" ? value : BigInt(value);
}

function readOptionalTraceI64(value: bigint | number | undefined): bigint {
  return BigInt.asIntN(64, readOptionalTraceU64(value));
}

function sampleToPid(sample: bigint | number | undefined): number {
  if (sample === undefined) {
    return 0;
  }
  return Number(BigInt.asUintN(32, readOptionalTraceU64(sample)));
}

function readGuestSlice(
  exports: KernelExports,
  guestBase: number,
  ptr: bigint,
  len: bigint,
  maxLen: number,
): Uint8Array | null {
  const start = Number(ptr);
  const requestedLen = Number(len);
  if (
    !Number.isFinite(start)
    || !Number.isFinite(requestedLen)
    || start <= 0
    || requestedLen <= 0
  ) {
    return null;
  }
  const clampedLen = Math.min(requestedLen, maxLen);
  const absolute = guestBase + start;
  if (absolute < 0 || absolute + clampedLen > exports.memory.buffer.byteLength) {
    return null;
  }
  return copyBytes(new Uint8Array(exports.memory.buffer, absolute, clampedLen));
}

function readGuestU64(
  exports: KernelExports,
  guestBase: number,
  ptr: bigint,
): bigint | null {
  const start = Number(ptr);
  const absolute = guestBase + start;
  if (
    !Number.isFinite(start)
    || start <= 0
    || absolute < 0
    || absolute + 8 > exports.memory.buffer.byteLength
  ) {
    return null;
  }
  return new DataView(exports.memory.buffer).getBigUint64(absolute, true);
}

function formatFdTraceWriteSample(
  exports: KernelExports,
  guestBase: number,
  nr: bigint,
  arg1: bigint,
  arg2: bigint,
): string {
  if (nr === 64n) {
    const bytes = readGuestSlice(exports, guestBase, arg1, arg2, 160);
    return bytes === null ? "" : `:text=${JSON.stringify(decoder.decode(bytes))}`;
  }
  if (nr !== 66n) {
    return "";
  }
  const iovBase = readGuestU64(exports, guestBase, arg1);
  const iovLen = readGuestU64(exports, guestBase, arg1 + 8n);
  if (iovBase === null || iovLen === null) {
    return "";
  }
  const bytes = readGuestSlice(exports, guestBase, iovBase, iovLen, 160);
  return bytes === null ? "" : `:iov0=${JSON.stringify(decoder.decode(bytes))}`;
}

function formatFdTraceReadSample(
  exports: KernelExports,
  guestBase: number,
  nr: bigint,
  arg1: bigint,
  result: bigint,
): string {
  if (nr !== 63n || result <= 0n) {
    return "";
  }
  const bytes = readGuestSlice(exports, guestBase, arg1, result, 160);
  return bytes === null ? "" : `:read=${JSON.stringify(decoder.decode(bytes))}`;
}

export function collectFdTraceDebug(
  exports: KernelExports,
  vmPtr: number,
  guestBase: number,
): string | undefined {
  if (!FD_TRACE_LOG_ENABLED) {
    return undefined;
  }
  const len = exports.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return undefined;
  }
  const pipeType = exports.abi_fd_type_pipe();
  const interestingSyscalls = new Set<bigint>([
    23n, // dup
    24n, // dup3
    25n, // fcntl
    45n, // truncate
    46n, // ftruncate
    56n, // openat
    57n, // close
    59n, // pipe2
    62n, // lseek
    63n, // read
    64n, // write
    65n, // readv
    66n, // writev
    67n, // pread64
    73n, // ppoll
    198n, // socket
    199n, // socketpair
    203n, // connect
    242n, // accept4
    436n, // close_range
  ]);
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = exports.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
    if (traceVmPtr !== 0 && traceVmPtr !== vmPtr) {
      continue;
    }
    const nr = readOptionalTraceU64(exports.debug_fd_syscall_trace_nr?.(index));
    if (!interestingSyscalls.has(nr)) {
      continue;
    }
    const arg0 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg0?.(index));
    const arg1 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg1?.(index));
    const arg2 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg2?.(index));
    const arg3 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg3?.(index));
    const arg0Ofd = exports.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0;
    const arg0Type = exports.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0;
    const arg0Offset = readOptionalTraceI64(exports.debug_fd_syscall_trace_arg0_ofd_offset?.(index));
    const arg0HostFd = exports.debug_fd_syscall_trace_arg0_ofd_host_fd?.(index);
    const arg0StatusFlags = exports.debug_fd_syscall_trace_arg0_ofd_status_flags?.(index);
    const arg0Ino = exports.debug_fd_syscall_trace_arg0_ofd_ino?.(index);
    const arg0InodeMode = exports.debug_fd_syscall_trace_arg0_inode_mode?.(index);
    const arg0PipeGeneration = exports.debug_fd_syscall_trace_arg0_pipe_generation?.(index);
    const arg2Ofd = exports.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? 0;
    const arg2Type = exports.debug_fd_syscall_trace_arg2_ofd_type?.(index) ?? 0;
    const result = readOptionalTraceI64(exports.debug_fd_syscall_trace_result?.(index));
    if (
      result >= 0n
      && arg0Type !== pipeType
      && arg2Type !== pipeType
      && arg0 > 20n
      && arg2 > 20n
    ) {
      continue;
    }
    const entry = [
      `${nr.toString()}`,
      `pid=${sampleToPid(exports.debug_fd_syscall_trace_pid_sample0?.(index))}`,
      `fd=${arg0.toString()}`,
      `ofd=${arg0Ofd}`,
      `ty=${arg0Type}`,
      `sampleOff=${arg0Offset.toString()}`,
      `sampleHost=${arg0HostFd ?? "n/a"}`,
      `sampleFlags=${arg0StatusFlags ?? "n/a"}`,
      `sampleIno=${arg0Ino ?? "n/a"}`,
      `sampleMode=${arg0InodeMode ?? "n/a"}`,
      `pipeGen=${arg0PipeGeneration ?? "n/a"}`,
      `off=${arg0Ofd > 0 && exports.kernel_ofd_offset ? exports.kernel_ofd_offset(arg0Ofd).toString() : "-1"}`,
      `a1=${arg1.toString()}`,
      `a2=${arg2.toString()}`,
      `a2ofd=${arg2Ofd}`,
      `a2ty=${arg2Type}`,
      `a3=${arg3.toString()}`,
      `ret=${result.toString()}`,
      `st=${exports.debug_fd_syscall_trace_status?.(index) ?? 0}`,
    ].join(":")
      + formatFdTraceWriteSample(exports, guestBase, nr, arg1, arg2)
      + formatFdTraceReadSample(exports, guestBase, nr, arg1, result);
    if (loggedFdTraceEntries.has(entry)) {
      continue;
    }
    loggedFdTraceEntries.add(entry);
    entries.push(entry);
  }
  if (loggedFdTraceEntries.size > 8192) {
    loggedFdTraceEntries.clear();
  }
  return entries.length === 0 ? undefined : entries.slice(-48).join(",");
}

function collectFdTraceSummaryForDebug(exports: KernelExports, vmPtr: number): string | null {
  const len = exports.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return null;
  }
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = exports.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
    if (traceVmPtr !== 0 && traceVmPtr !== vmPtr) {
      continue;
    }
    const nr = readOptionalTraceU64(exports.debug_fd_syscall_trace_nr?.(index));
    if (nr === 0n) {
      continue;
    }
    const result = readOptionalTraceI64(exports.debug_fd_syscall_trace_result?.(index));
    entries.push([
      `nr=${nr.toString()}`,
      `fd=${readOptionalTraceU64(exports.debug_fd_syscall_trace_arg0?.(index)).toString()}`,
      `ofd=${exports.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0}`,
      `ty=${exports.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0}`,
      `sampleOff=${readOptionalTraceI64(exports.debug_fd_syscall_trace_arg0_ofd_offset?.(index)).toString()}`,
      `sampleHost=${exports.debug_fd_syscall_trace_arg0_ofd_host_fd?.(index) ?? "n/a"}`,
      `pipeGen=${exports.debug_fd_syscall_trace_arg0_pipe_generation?.(index) ?? "n/a"}`,
      `a1=${readOptionalTraceU64(exports.debug_fd_syscall_trace_arg1?.(index)).toString()}`,
      `a2=${readOptionalTraceU64(exports.debug_fd_syscall_trace_arg2?.(index)).toString()}`,
      `a2ofd=${exports.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? 0}`,
      `a2ty=${exports.debug_fd_syscall_trace_arg2_ofd_type?.(index) ?? 0}`,
      `ret=${result.toString()}`,
      `st=${exports.debug_fd_syscall_trace_status?.(index) ?? 0}`,
      `pid=${sampleToPid(exports.debug_fd_syscall_trace_pid_sample0?.(index))}`,
    ].join(":"));
  }
  return entries.length === 0 ? null : entries.slice(-64).join(",");
}

function debugScratchBase(exports: KernelExports): number {
  return (exports.kernel_scratch_base?.() ?? (exports.kernel_heap_ptr() + 0x10000));
}

function readOptionalThreadU64(
  reader: ((vmPtr: number) => bigint) | undefined,
  vmPtr: number,
): bigint {
  return reader?.(vmPtr) ?? 0n;
}

function readOptionalThreadI32(
  reader: ((vmPtr: number) => number) | undefined,
  vmPtr: number,
): number {
  return reader?.(vmPtr) ?? 0;
}

interface ThreadStepTraceSnapshot {
  vmPtr: number;
  tid: number;
  status: number;
  budget: number;
  remaining: number;
  pc: bigint;
  ra: bigint;
  sp: bigint;
  s11: bigint;
}

interface ThreadStepEntryTraceSnapshot {
  vmPtr: number;
  tid: number;
  status: number;
  budget: number;
  pc: bigint;
  ra: bigint;
  sp: bigint;
  s11: bigint;
}

interface ThreadStepPhaseSnapshot {
  vmPtr: number;
  phase: number;
  status: number;
  budget: number;
  remaining: number;
  pc: bigint;
  ra: bigint;
  sp: bigint;
  s11: bigint;
}

function readThreadStepEntryTraceSnapshot(
  exports: KernelExports,
  vmPtr: number,
): ThreadStepEntryTraceSnapshot {
  return {
    vmPtr: 0,
    tid: exports.debug_thread_step_entry_for_vm_tid?.(vmPtr) ?? 0,
    status: exports.debug_thread_step_entry_for_vm_status?.(vmPtr) ?? 0,
    budget: exports.debug_thread_step_entry_for_vm_budget?.(vmPtr) ?? 0,
    pc: exports.debug_thread_step_entry_for_vm_pc?.(vmPtr) ?? 0n,
    ra: exports.debug_thread_step_entry_for_vm_ra?.(vmPtr) ?? 0n,
    sp: exports.debug_thread_step_entry_for_vm_sp?.(vmPtr) ?? 0n,
    s11: exports.debug_thread_step_entry_for_vm_s11?.(vmPtr) ?? 0n,
  } satisfies ThreadStepEntryTraceSnapshot;
}

function readThreadStepTraceSnapshot(exports: KernelExports, vmPtr: number): ThreadStepTraceSnapshot {
  const perVm = {
    vmPtr,
    tid: exports.debug_thread_step_for_vm_tid?.(vmPtr) ?? 0,
    status: exports.debug_thread_step_for_vm_status?.(vmPtr) ?? 0,
    budget: exports.debug_thread_step_for_vm_budget?.(vmPtr) ?? 0,
    remaining: exports.debug_thread_step_for_vm_remaining?.(vmPtr) ?? 0,
    pc: exports.debug_thread_step_for_vm_pc?.(vmPtr) ?? 0n,
    ra: exports.debug_thread_step_for_vm_ra?.(vmPtr) ?? 0n,
    sp: exports.debug_thread_step_for_vm_sp?.(vmPtr) ?? 0n,
    s11: exports.debug_thread_step_for_vm_s11?.(vmPtr) ?? 0n,
  } satisfies ThreadStepTraceSnapshot;
  const hasPerVmTrace = perVm.tid !== 0
    || perVm.status !== 0
    || perVm.budget !== 0
    || perVm.remaining !== 0
    || perVm.pc !== 0n
    || perVm.ra !== 0n
    || perVm.sp !== 0n
    || perVm.s11 !== 0n;
  if (hasPerVmTrace) {
    return perVm;
  }
  return {
    vmPtr: exports.debug_thread_step_vm_ptr?.() ?? 0,
    tid: exports.debug_thread_step_tid?.() ?? 0,
    status: exports.debug_thread_step_status?.() ?? 0,
    budget: exports.debug_thread_step_budget?.() ?? 0,
    remaining: exports.debug_thread_step_remaining?.() ?? 0,
    pc: exports.debug_thread_step_pc?.() ?? 0n,
    ra: exports.debug_thread_step_ra?.() ?? 0n,
    sp: exports.debug_thread_step_sp?.() ?? 0n,
    s11: exports.debug_thread_step_s11?.() ?? 0n,
  } satisfies ThreadStepTraceSnapshot;
}

function readThreadStepPhaseSnapshot(exports: KernelExports, vmPtr: number): ThreadStepPhaseSnapshot {
  const perVm = {
    vmPtr,
    phase: exports.debug_thread_step_phase_for_vm?.(vmPtr) ?? 0,
    status: exports.debug_thread_step_phase_for_vm_status?.(vmPtr) ?? 0,
    budget: exports.debug_thread_step_phase_for_vm_budget?.(vmPtr) ?? 0,
    remaining: exports.debug_thread_step_phase_for_vm_remaining?.(vmPtr) ?? 0,
    pc: exports.debug_thread_step_phase_for_vm_pc?.(vmPtr) ?? 0n,
    ra: exports.debug_thread_step_phase_for_vm_ra?.(vmPtr) ?? 0n,
    sp: exports.debug_thread_step_phase_for_vm_sp?.(vmPtr) ?? 0n,
    s11: exports.debug_thread_step_phase_for_vm_s11?.(vmPtr) ?? 0n,
  } satisfies ThreadStepPhaseSnapshot;
  const hasPerVmTrace = perVm.phase !== 0
    || perVm.status !== 0
    || perVm.budget !== 0
    || perVm.remaining !== 0
    || perVm.pc !== 0n
    || perVm.ra !== 0n
    || perVm.sp !== 0n
    || perVm.s11 !== 0n;
  if (hasPerVmTrace) {
    return perVm;
  }
  return {
    vmPtr: exports.debug_thread_step_phase_vm_ptr?.() ?? 0,
    phase: exports.debug_thread_step_phase?.() ?? 0,
    status: exports.debug_thread_step_phase_status?.() ?? 0,
    budget: exports.debug_thread_step_phase_budget?.() ?? 0,
    remaining: exports.debug_thread_step_phase_remaining?.() ?? 0,
    pc: exports.debug_thread_step_phase_pc?.() ?? 0n,
    ra: exports.debug_thread_step_phase_ra?.() ?? 0n,
    sp: exports.debug_thread_step_phase_sp?.() ?? 0n,
    s11: exports.debug_thread_step_phase_s11?.() ?? 0n,
  } satisfies ThreadStepPhaseSnapshot;
}

function formatThreadStepTraceEntries(exports: KernelExports, vmPtr: number): string | null {
  const len = exports.debug_thread_step_trace_len?.() ?? 0;
  if (len <= 0) {
    return null;
  }
  const start = Math.max(0, len - 8);
  const entries: string[] = [];
  for (let index = start; index < len; index += 1) {
    const entryVmPtr = exports.debug_thread_step_trace_entry_vm_ptr?.(index) ?? 0;
    const tid = exports.debug_thread_step_trace_entry_tid?.(index) ?? 0;
    const status = exports.debug_thread_step_trace_entry_status?.(index) ?? 0;
    const budget = exports.debug_thread_step_trace_entry_budget?.(index) ?? 0;
    const remaining = exports.debug_thread_step_trace_entry_remaining?.(index) ?? 0;
    const pc = exports.debug_thread_step_trace_entry_pc?.(index) ?? 0n;
    const ra = exports.debug_thread_step_trace_entry_ra?.(index) ?? 0n;
    const sp = exports.debug_thread_step_trace_entry_sp?.(index) ?? 0n;
    const s11 = exports.debug_thread_step_trace_entry_s11?.(index) ?? 0n;
    const matchFlag = entryVmPtr === vmPtr ? "*" : "";
    entries.push(
      `${index}${matchFlag}:vm=0x${entryVmPtr.toString(16)}:tid=${tid}:status=${status}:budget=${budget}:remaining=${remaining}:pc=0x${formatU64(pc)}:ra=0x${formatU64(ra)}:sp=0x${formatU64(sp)}:s11=0x${formatU64(s11)}`,
    );
  }
  return entries.length > 0 ? entries.join(" ") : null;
}

export function formatExecTraceEntries(exports: KernelExports, vmPtr: number): string | null {
  const len = exports.debug_exec_trace_len?.() ?? 0;
  const targetEpoch = exports.debug_exec_trace_epoch_for_vm?.(vmPtr) ?? 0;
  const epochEntries: string[] = [];
  const recentVmEntries: string[] = [];
  if (len > 0) {
    for (let index = 0; index < len; index += 1) {
      const entryVmPtr = exports.debug_exec_trace_entry_vm_ptr?.(index) ?? 0;
      if (entryVmPtr !== vmPtr) {
        continue;
      }
      const entryEpoch = exports.debug_exec_trace_entry_epoch?.(index) ?? 0;
      if (targetEpoch !== 0 && entryEpoch !== targetEpoch) {
        continue;
      }
      const tid = exports.debug_exec_trace_entry_tid?.(index) ?? 0;
      const pc = exports.debug_exec_trace_entry_pc?.(index) ?? 0n;
      const raw = exports.debug_exec_trace_entry_raw?.(index) ?? 0;
      const step = exports.debug_exec_trace_entry_step?.(index) ?? 0;
      const sp = exports.debug_exec_trace_entry_sp?.(index) ?? 0n;
      const ra = exports.debug_exec_trace_entry_ra?.(index) ?? 0n;
      const a0 = exports.debug_exec_trace_entry_a0?.(index) ?? 0n;
      const s11 = exports.debug_exec_trace_entry_s11?.(index) ?? 0n;
      const formatted =
        `${index}:tid=${tid}:pc=0x${formatU64(pc)}:raw=0x${raw.toString(16)}:step=${step}:sp=0x${formatU64(sp)}:ra=0x${formatU64(ra)}:a0=0x${formatU64(a0)}:s11=0x${formatU64(s11)}`;
      recentVmEntries.push(formatted);
      if (targetEpoch !== 0 && entryEpoch !== targetEpoch) {
        continue;
      }
      epochEntries.push(formatted);
    }
  }
  if (epochEntries.length === 0) {
    const threadStepLen = exports.debug_thread_step_exec_trace_len_for_vm?.(vmPtr) ?? 0;
    if (threadStepLen > 0) {
      const threadStepEntries: string[] = [];
      for (let index = 0; index < threadStepLen; index += 1) {
        const pc = exports.debug_thread_step_exec_trace_entry_pc_for_vm?.(vmPtr, index) ?? 0n;
        const raw = exports.debug_thread_step_exec_trace_entry_raw_for_vm?.(vmPtr, index) ?? 0;
        const step = exports.debug_thread_step_exec_trace_entry_step_for_vm?.(vmPtr, index) ?? 0;
        const sp = exports.debug_thread_step_exec_trace_entry_sp_for_vm?.(vmPtr, index) ?? 0n;
        const ra = exports.debug_thread_step_exec_trace_entry_ra_for_vm?.(vmPtr, index) ?? 0n;
        const a0 = exports.debug_thread_step_exec_trace_entry_a0_for_vm?.(vmPtr, index) ?? 0n;
        const s11 = exports.debug_thread_step_exec_trace_entry_s11_for_vm?.(vmPtr, index) ?? 0n;
        threadStepEntries.push(
          `${index}:pc=0x${formatU64(pc)}:raw=0x${raw.toString(16)}:step=${step}:sp=0x${formatU64(sp)}:ra=0x${formatU64(ra)}:a0=0x${formatU64(a0)}:s11=0x${formatU64(s11)}`,
        );
      }
      if (threadStepEntries.length > 0) {
        return threadStepEntries.join(" ");
      }
    }
    if (recentVmEntries.length > 0) {
      return recentVmEntries.slice(-256).join(" ");
    }
    const faultLen = exports.debug_fault_exec_trace_len_for_vm?.(vmPtr) ?? 0;
    if (faultLen <= 0) {
      return null;
    }
    const faultEntries: string[] = [];
    for (let index = 0; index < faultLen; index += 1) {
      const pc = exports.debug_fault_exec_trace_entry_pc_for_vm?.(vmPtr, index) ?? 0n;
      const raw = exports.debug_fault_exec_trace_entry_raw_for_vm?.(vmPtr, index) ?? 0;
      const step = exports.debug_fault_exec_trace_entry_step_for_vm?.(vmPtr, index) ?? 0;
      const sp = exports.debug_fault_exec_trace_entry_sp_for_vm?.(vmPtr, index) ?? 0n;
      const ra = exports.debug_fault_exec_trace_entry_ra_for_vm?.(vmPtr, index) ?? 0n;
      const a0 = exports.debug_fault_exec_trace_entry_a0_for_vm?.(vmPtr, index) ?? 0n;
      const s11 = exports.debug_fault_exec_trace_entry_s11_for_vm?.(vmPtr, index) ?? 0n;
      faultEntries.push(
        `${index}:pc=0x${formatU64(pc)}:raw=0x${raw.toString(16)}:step=${step}:sp=0x${formatU64(sp)}:ra=0x${formatU64(ra)}:a0=0x${formatU64(a0)}:s11=0x${formatU64(s11)}`,
      );
    }
    return faultEntries.join(" ");
  }
  return epochEntries.slice(-256).join(" ");
}

export function formatSyncFaultExecTraceEntries(exports: KernelExports, vmPtr: number): string | null {
  const len = exports.debug_sync_fault_exec_trace_len_for_vm?.(vmPtr) ?? 0;
  if (len <= 0) {
    return null;
  }
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const pc = exports.debug_sync_fault_exec_trace_entry_pc_for_vm?.(vmPtr, index) ?? 0n;
    const raw = exports.debug_sync_fault_exec_trace_entry_raw_for_vm?.(vmPtr, index) ?? 0;
    const step = exports.debug_sync_fault_exec_trace_entry_step_for_vm?.(vmPtr, index) ?? 0;
    const sp = exports.debug_sync_fault_exec_trace_entry_sp_for_vm?.(vmPtr, index) ?? 0n;
    const ra = exports.debug_sync_fault_exec_trace_entry_ra_for_vm?.(vmPtr, index) ?? 0n;
    const a0 = exports.debug_sync_fault_exec_trace_entry_a0_for_vm?.(vmPtr, index) ?? 0n;
    const s11 = exports.debug_sync_fault_exec_trace_entry_s11_for_vm?.(vmPtr, index) ?? 0n;
    entries.push(
      `${index}:pc=0x${formatU64(pc)}:raw=0x${raw.toString(16)}:step=${step}:sp=0x${formatU64(sp)}:ra=0x${formatU64(ra)}:a0=0x${formatU64(a0)}:s11=0x${formatU64(s11)}`,
    );
  }
  return entries.join(" ");
}

export function formatBreakExecTraceEntries(exports: KernelExports, vmPtr: number): string | null {
  const len = exports.debug_break_exec_trace_len_for_vm?.(vmPtr) ?? 0;
  if (len <= 0) {
    return null;
  }
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const pc = exports.debug_break_exec_trace_entry_pc_for_vm?.(vmPtr, index) ?? 0n;
    const raw = exports.debug_break_exec_trace_entry_raw_for_vm?.(vmPtr, index) ?? 0;
    const step = exports.debug_break_exec_trace_entry_step_for_vm?.(vmPtr, index) ?? 0;
    const sp = exports.debug_break_exec_trace_entry_sp_for_vm?.(vmPtr, index) ?? 0n;
    const ra = exports.debug_break_exec_trace_entry_ra_for_vm?.(vmPtr, index) ?? 0n;
    const a0 = exports.debug_break_exec_trace_entry_a0_for_vm?.(vmPtr, index) ?? 0n;
    const s11 = exports.debug_break_exec_trace_entry_s11_for_vm?.(vmPtr, index) ?? 0n;
    entries.push(
      `${index}:pc=0x${formatU64(pc)}:raw=0x${raw.toString(16)}:step=${step}:sp=0x${formatU64(sp)}:ra=0x${formatU64(ra)}:a0=0x${formatU64(a0)}:s11=0x${formatU64(s11)}`,
    );
  }
  return entries.join(" ");
}

interface FaultDispatchTraceSnapshot {
  phase: number;
  tid: number;
  pc: bigint;
  ra: bigint;
  sp: bigint;
  s11: bigint;
  faultAddr: bigint;
}

function readFaultDispatchTraceSnapshot(exports: KernelExports, vmPtr: number): FaultDispatchTraceSnapshot {
  const perVm = {
    phase: exports.debug_fault_dispatch_for_vm_phase?.(vmPtr) ?? 0,
    tid: exports.debug_fault_dispatch_for_vm_tid?.(vmPtr) ?? 0,
    pc: exports.debug_fault_dispatch_for_vm_pc?.(vmPtr) ?? 0n,
    ra: exports.debug_fault_dispatch_for_vm_ra?.(vmPtr) ?? 0n,
    sp: exports.debug_fault_dispatch_for_vm_sp?.(vmPtr) ?? 0n,
    s11: exports.debug_fault_dispatch_for_vm_s11?.(vmPtr) ?? 0n,
    faultAddr: exports.debug_fault_dispatch_for_vm_addr?.(vmPtr) ?? 0n,
  } satisfies FaultDispatchTraceSnapshot;
  if (perVm.phase !== 0 || perVm.tid !== 0 || perVm.pc !== 0n || perVm.ra !== 0n || perVm.sp !== 0n || perVm.s11 !== 0n || perVm.faultAddr !== 0n) {
    return perVm;
  }
  return {
    phase: 0,
    tid: 0,
    pc: 0n,
    ra: 0n,
    sp: 0n,
    s11: 0n,
    faultAddr: 0n,
  } satisfies FaultDispatchTraceSnapshot;
}

function readOptionalThreadReg(
  exports: KernelExports,
  vmPtr: number,
  regIndex: number,
): bigint {
  return exports.thread_reg?.(vmPtr, regIndex) ?? 0n;
}


function formatMemorySyscallTraceEntries(
  exports: KernelExports,
  vmPtr: number,
): string | null {
  const len = exports.debug_memory_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return null;
  }

  const processPtr = exports.thread_process_ptr?.(vmPtr) ?? 0;
  const processPid = exports.process_current_pid?.(vmPtr) ?? 0;
  const byProcessPtr: string[] = [];
  const byPid: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const entryProcessPtr = exports.debug_memory_syscall_trace_entry_process_ptr?.(index) ?? 0;
    const entryPid = exports.debug_memory_syscall_trace_entry_pid?.(index) ?? 0;
    const entry = [
      `idx=${index}`,
      `nr=${readOptionalTraceU64(exports.debug_memory_syscall_trace_entry_nr?.(index)).toString()}`,
      `a0=${readOptionalTraceU64(exports.debug_memory_syscall_trace_entry_arg0?.(index)).toString()}`,
      `a1=${readOptionalTraceU64(exports.debug_memory_syscall_trace_entry_arg1?.(index)).toString()}`,
      `a2=${readOptionalTraceU64(exports.debug_memory_syscall_trace_entry_arg2?.(index)).toString()}`,
      `a3=${readOptionalTraceU64(exports.debug_memory_syscall_trace_entry_arg3?.(index)).toString()}`,
      `a4=${readOptionalTraceU64(exports.debug_memory_syscall_trace_entry_arg4?.(index)).toString()}`,
      `a5=${readOptionalTraceU64(exports.debug_memory_syscall_trace_entry_arg5?.(index)).toString()}`,
      `ret=${BigInt.asIntN(64, readOptionalTraceU64(exports.debug_memory_syscall_trace_entry_result?.(index))).toString()}`,
      `pid=${entryPid}`,
      `process=0x${entryProcessPtr.toString(16)}`,
    ].join(":");

    if (processPtr !== 0 && entryProcessPtr === processPtr) {
      byProcessPtr.push(entry);
    }
    if (processPid > 0 && entryPid === processPid) {
      byPid.push(entry);
    }
  }

  const selected = byProcessPtr.length > 0 ? byProcessPtr : byPid;
  return selected.length > 0 ? selected.slice(-32).join(",") : null;
}

function readGuestU32(exports: KernelExports, guestBase: number, addr: number): number | null {
  const absolute = guestBase + addr;
  if (addr <= 0 || absolute + 4 > exports.memory.buffer.byteLength) {
    return null;
  }
  return new DataView(exports.memory.buffer).getUint32(absolute, true);
}

function readGuestI32(exports: KernelExports, guestBase: number, addr: number): number | null {
  const absolute = guestBase + addr;
  if (addr <= 0 || absolute + 4 > exports.memory.buffer.byteLength) {
    return null;
  }
  return new DataView(exports.memory.buffer).getInt32(absolute, true);
}

function summarizeReservationSlots(
  exports: KernelExports,
  guestBase: number,
  reservationGenAddr: number,
  reservationLockAddr: number,
): string {
  const oddGenerations: string[] = [];
  const lockedSlots: string[] = [];
  for (let slot = 0; slot < 64; slot += 1) {
    const generation = readGuestU32(exports, guestBase, reservationGenAddr + slot * 4);
    const lock = readGuestU32(exports, guestBase, reservationLockAddr + slot * 4);
    if (generation !== null && (generation & 1) !== 0) {
      oddGenerations.push(`${slot}:${generation}`);
    }
    if (lock !== null && lock !== 0) {
      lockedSlots.push(`${slot}:${lock}`);
    }
  }
  return `odd=${oddGenerations.length === 0 ? "-" : oddGenerations.join(",")}`
    + `:locked=${lockedSlots.length === 0 ? "-" : lockedSlots.join(",")}`;
}

export function clearSyscallTrace(exports: KernelExports): void {
  exports.debug_clear_syscall_trace?.();
  exports.debug_clear_signal_trace?.();
  exports.debug_clear_cpu_trace?.();
}

function readGuestInsn(exports: KernelExports, guestBase: number, pc: bigint): string | null {
  const addr = Number(pc);
  if (!Number.isFinite(addr) || addr < 0) {
    return null;
  }
  const absolute = guestBase + addr;
  if (absolute < 0 || absolute + 4 > exports.memory.buffer.byteLength) {
    return null;
  }
  const view = new DataView(exports.memory.buffer);
  return `0x${view.getUint32(absolute, true).toString(16)}`;
}

function readGuestU64ForDebug(
  exports: KernelExports,
  guestBase: number,
  addr: bigint,
): bigint | null {
  const guestAddr = Number(addr);
  if (!Number.isFinite(guestAddr) || guestAddr < 0 || guestAddr > Number.MAX_SAFE_INTEGER) {
    return null;
  }
  const absolute = guestBase + guestAddr;
  if (absolute < 0 || absolute + 8 > exports.memory.buffer.byteLength) {
    return null;
  }
  const view = new DataView(exports.memory.buffer);
  return view.getBigUint64(absolute, true);
}

function readGuestCStringForDebug(
  exports: KernelExports,
  guestBase: number,
  addr: bigint,
  limit = 160,
): string | null {
  const guestAddr = Number(addr);
  if (!Number.isFinite(guestAddr) || guestAddr <= 0 || guestAddr > Number.MAX_SAFE_INTEGER) {
    return null;
  }
  const absolute = guestBase + guestAddr;
  if (absolute < 0 || absolute >= exports.memory.buffer.byteLength) {
    return null;
  }
  const bytes = new Uint8Array(exports.memory.buffer);
  const out: number[] = [];
  for (let i = 0; i < limit; i += 1) {
    const index = absolute + i;
    if (index >= bytes.length) {
      return null;
    }
    const byte = bytes[index] ?? 0;
    if (byte === 0) {
      return decoder.decode(new Uint8Array(out));
    }
    out.push(byte);
  }
  return decoder.decode(new Uint8Array(out));
}

function formatExecveVecForDebug(
  exports: KernelExports,
  guestBase: number,
  vecAddr: bigint,
  maxEntries: number,
): string | null {
  if (vecAddr === 0n) {
    return "[]";
  }
  const entries: string[] = [];
  for (let index = 0; index < maxEntries; index += 1) {
    const ptr = readGuestU64ForDebug(exports, guestBase, vecAddr + BigInt(index * 8));
    if (ptr === null) {
      entries.push(`@${index}=unreadable`);
      break;
    }
    if (ptr === 0n) {
      break;
    }
    const value = readGuestCStringForDebug(exports, guestBase, ptr);
    entries.push(`@${index}=0x${formatU64(ptr)}:${JSON.stringify(value ?? "<unreadable>")}`);
  }
  return `[${entries.join(",")}]`;
}

function formatMemoryVma(exports: KernelExports, index: number): string {
  return `0x${exports.debug_memory_vma_start?.(index).toString(16) ?? "0"}-0x${exports.debug_memory_vma_end?.(index).toString(16) ?? "0"}`
    + `:${exports.debug_memory_vma_prot?.(index) ?? "n/a"}/${exports.debug_memory_vma_flags?.(index) ?? "n/a"}`;
}

function findContainingMemoryVma(
  exports: KernelExports,
  vmaLen: number,
  addr: bigint,
): string | null {
  if (addr === 0n) {
    return null;
  }
  for (let i = 0; i < vmaLen; i += 1) {
    const start = BigInt(exports.debug_memory_vma_start?.(i) ?? 0);
    const end = BigInt(exports.debug_memory_vma_end?.(i) ?? 0);
    if (start <= addr && addr < end) {
      return formatMemoryVma(exports, i);
    }
  }
  return null;
}

function findAdjacentMemoryVmas(
  exports: KernelExports,
  vmaLen: number,
  addr: bigint,
): { prev: string | null; next: string | null } {
  let prev: string | null = null;
  let next: string | null = null;
  if (addr === 0n) {
    return { prev, next };
  }
  for (let i = 0; i < vmaLen; i += 1) {
    const start = BigInt(exports.debug_memory_vma_start?.(i) ?? 0);
    const end = BigInt(exports.debug_memory_vma_end?.(i) ?? 0);
    if (end <= addr) {
      prev = formatMemoryVma(exports, i);
      continue;
    }
    if (addr < start) {
      next = formatMemoryVma(exports, i);
      break;
    }
  }
  return { prev, next };
}

function pushAddressVmaDebug(
  debugParts: string[],
  exports: KernelExports,
  vmaLen: number,
  label: string,
  addr: bigint,
) {
  if (addr === 0n) {
    return;
  }
  const currentVma = findContainingMemoryVma(exports, vmaLen, addr);
  debugParts.push(`${label}_vma=${currentVma ?? "none"}`);
  if (currentVma === null) {
    const adjacent = findAdjacentMemoryVmas(exports, vmaLen, addr);
    debugParts.push(`${label}_vma_prev=${adjacent.prev ?? "none"}`);
    debugParts.push(`${label}_vma_next=${adjacent.next ?? "none"}`);
  }
}

function pushRecoveredDebugPart(
  debugParts: string[],
  label: string,
  read: () => string | null,
) {
  try {
    const value = read();
    if (value !== null) {
      debugParts.push(`${label}=[${value}]`);
    }
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    debugParts.push(`${label}_recover_error=${JSON.stringify(detail)}`);
  }
}

function pushRecoveredTraceDebug(debugParts: string[], exports: KernelExports, vmPtr: number) {
  pushRecoveredDebugPart(
    debugParts,
    "thread_step_traces",
    () => formatThreadStepTraceEntries(exports, vmPtr),
  );
  pushRecoveredDebugPart(
    debugParts,
    "thread_step_exec_trace",
    () => formatExecTraceEntries(exports, vmPtr),
  );
  pushRecoveredDebugPart(
    debugParts,
    "sync_fault_exec_trace",
    () => formatSyncFaultExecTraceEntries(exports, vmPtr),
  );
  pushRecoveredDebugPart(
    debugParts,
    "break_exec_trace",
    () => formatBreakExecTraceEntries(exports, vmPtr),
  );
}

export function collectDebugParts(exports: KernelExports, vmPtr: number, guestBase: number): string[] {
  const debugParts: string[] = [];
  try {
  debugParts.push(`mem_bytes=${exports.memory.buffer.byteLength}`);
  debugParts.push(`guest_base=0x${(exports.thread_guest_base?.(vmPtr) ?? guestBase).toString(16)}`);
  debugParts.push(`guest_ram_limit=${(exports.thread_guest_ram_limit?.(vmPtr) ?? 0) >>> 0}`);
  const faultPc = readOptionalThreadU64(exports.thread_fault_pc, vmPtr);
  const currentPc = exports.thread_pc(vmPtr);
  debugParts.push(`process=0x${exports.thread_process_ptr(vmPtr).toString(16)}`);
  debugParts.push(`process_threads=${exports.thread_process_thread_count(vmPtr)}`);
  debugParts.push(`tid=${readOptionalThreadI32(exports.thread_tid, vmPtr)}`);
  debugParts.push(`gp=0x${formatU64(readOptionalThreadU64(exports.thread_gp, vmPtr))}`);
  debugParts.push(`tp=0x${formatU64(readOptionalThreadU64(exports.thread_tp, vmPtr))}`);
  debugParts.push(`s11=0x${formatU64(readOptionalThreadU64(exports.thread_s11, vmPtr))}`);
  debugParts.push(`tls=0x${formatU64(readOptionalThreadU64(exports.thread_tls_base, vmPtr))}`);
  debugParts.push(`sig_mask=0x${formatU64(readOptionalThreadU64(exports.thread_sig_mask, vmPtr))}`);
  debugParts.push(`sig_pending=0x${formatU64(exports.thread_sig_pending(vmPtr))}`);
  debugParts.push(`sig_code=${readOptionalThreadI32(exports.thread_sig_info_code, vmPtr)}`);
  debugParts.push(`syscall_a7=${formatU64(readOptionalThreadU64(exports.thread_a7, vmPtr))}`);
  debugParts.push(`syscall_a0=${formatU64(readOptionalThreadU64(exports.thread_a0, vmPtr))}`);
  debugParts.push(`poll_pipe=0x${(exports.thread_poll_wait_first_pipe_revents?.(vmPtr) ?? 0).toString(16)}`);
  debugParts.push(`blocked_fd=${exports.thread_blocked_fd?.(vmPtr) ?? 0}`);
  debugParts.push(`blocked_ofd_id=${exports.thread_blocked_ofd_id?.(vmPtr) ?? 0}`);
  debugParts.push(`epoll_timeout_ms=${exports.thread_epoll_wait_timeout?.(vmPtr)?.toString() ?? "n/a"}`);
  const blockedAddr = exports.thread_record_blocked_addr?.(vmPtr) ?? 0;
  if (blockedAddr !== 0) {
    debugParts.push(`blocked_addr=0x${blockedAddr.toString(16)}`);
  }
  const futexWaitAddr = exports.thread_futex_wait_addr?.(vmPtr) ?? 0n;
  if (futexWaitAddr !== 0n) {
    debugParts.push(`futex_wait_addr=0x${formatU64(futexWaitAddr)}`);
    debugParts.push(`futex_wait_expected=${exports.thread_futex_wait_expected?.(vmPtr) ?? 0}`);
    debugParts.push(`futex_wait_timeout=0x${formatU64(exports.thread_futex_wait_timeout?.(vmPtr) ?? 0n)}`);
  }
  debugParts.push(`fault_pc=0x${formatU64(faultPc)}`);
  const faultInsn = faultPc !== 0n ? readGuestInsn(exports, guestBase, faultPc) : null;
  if (faultInsn !== null) {
    debugParts.push(`fault_insn=${faultInsn}`);
  }
  debugParts.push(`fault_addr=0x${formatU64(readOptionalThreadU64(exports.thread_fault_addr, vmPtr))}`);
  debugParts.push(`pc=0x${currentPc.toString(16)}`);
  const currentInsn = readGuestInsn(exports, guestBase, currentPc);
  if (currentInsn !== null) {
    debugParts.push(`pc_insn=${currentInsn}`);
  }
  debugParts.push(`ra=0x${readOptionalThreadU64(exports.thread_ra, vmPtr).toString(16)}`);
  debugParts.push(`sp=0x${exports.thread_sp(vmPtr).toString(16)}`);
  const syscallNr = exports.debug_syscall_trace_nr?.();
  if (syscallNr !== undefined && syscallNr !== 0n) {
    debugParts.push(`syscall=${syscallNr.toString()}`);
    debugParts.push(`status=${exports.debug_syscall_trace_status?.() ?? "n/a"}`);
    debugParts.push(`result=${exports.debug_syscall_trace_result?.().toString() ?? "n/a"}`);
    const execveNr = exports.abi_sys_execve?.();
    if (execveNr !== undefined && BigInt(syscallNr) === BigInt(execveNr)) {
      const execveArgv = formatExecveVecForDebug(exports, guestBase, readOptionalThreadReg(exports, vmPtr, 11), 4);
      const execveEnvp = formatExecveVecForDebug(exports, guestBase, readOptionalThreadReg(exports, vmPtr, 12), 3);
      if (execveArgv !== null) {
        debugParts.push(`execve_argv=${execveArgv}`);
      }
      if (execveEnvp !== null) {
        debugParts.push(`execve_envp=${execveEnvp}`);
      }
    }
  }
  const syscallTraceLen = exports.debug_syscall_trace_len?.() ?? 0;
  if (syscallTraceLen > 0) {
    const traces: string[] = [];
    for (let index = 0; index < syscallTraceLen; index += 1) {
      const nr = readOptionalTraceU64(exports.debug_syscall_trace_entry_nr?.(index));
      if (nr === 0n) {
        continue;
      }
      const arg0 = readOptionalTraceU64(exports.debug_syscall_trace_entry_arg0?.(index));
      const arg1 = readOptionalTraceU64(exports.debug_syscall_trace_entry_arg1?.(index));
      const arg2 = readOptionalTraceU64(exports.debug_syscall_trace_entry_arg2?.(index));
      const arg3 = readOptionalTraceU64(exports.debug_syscall_trace_entry_arg3?.(index));
      const arg4 = readOptionalTraceU64(exports.debug_syscall_trace_entry_arg4?.(index));
      const arg5 = readOptionalTraceU64(exports.debug_syscall_trace_entry_arg5?.(index));
      const result = readOptionalTraceU64(exports.debug_syscall_trace_entry_result?.(index));
      const status = exports.debug_syscall_trace_entry_status?.(index) ?? 0;
      const traceVmPtr = exports.debug_syscall_trace_entry_vm_ptr?.(index) ?? 0;
      const pidSample0 = readOptionalTraceU64(exports.debug_syscall_trace_entry_pid_sample0?.(index));
      const pidSample1 = readOptionalTraceU64(exports.debug_syscall_trace_entry_pid_sample1?.(index));
      const pidSampleBytes = new Uint8Array(16);
      new DataView(pidSampleBytes.buffer).setBigUint64(0, pidSample0, true);
      new DataView(pidSampleBytes.buffer).setBigUint64(8, pidSample1, true);
      traces.push(
        `0x${traceVmPtr.toString(16)}:${nr.toString()}=${BigInt.asIntN(64, result).toString()}/${status}`
        + `(${formatTraceArg(arg0)},${formatTraceArg(arg1)},${formatTraceArg(arg2)},${formatTraceArg(arg3)},${formatTraceArg(arg4)},${formatTraceArg(arg5)})`
        + `:pid=${[...pidSampleBytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`,
      );
    }
    if (traces.length > 0) {
      debugParts.push(`syscalls=[${traces.join(",")}]`);
    }
  }
  const riscvSyscallTraceLen = exports.debug_riscv_syscall_trace_len?.() ?? 0;
  if (riscvSyscallTraceLen > 0) {
    const traces: string[] = [];
    for (let index = 0; index < riscvSyscallTraceLen; index += 1) {
      const nr = readOptionalTraceU64(exports.debug_riscv_syscall_trace_entry_nr?.(index));
      if (nr === 0n) {
        continue;
      }
      const arg0 = readOptionalTraceU64(exports.debug_riscv_syscall_trace_entry_arg0?.(index));
      const arg1 = readOptionalTraceU64(exports.debug_riscv_syscall_trace_entry_arg1?.(index));
      const arg2 = readOptionalTraceU64(exports.debug_riscv_syscall_trace_entry_arg2?.(index));
      const arg3 = readOptionalTraceU64(exports.debug_riscv_syscall_trace_entry_arg3?.(index));
      const arg4 = readOptionalTraceU64(exports.debug_riscv_syscall_trace_entry_arg4?.(index));
      const arg5 = readOptionalTraceU64(exports.debug_riscv_syscall_trace_entry_arg5?.(index));
      const result = readOptionalTraceU64(exports.debug_riscv_syscall_trace_entry_result?.(index));
      const status = exports.debug_riscv_syscall_trace_entry_status?.(index) ?? 0;
      const traceVmPtr = exports.debug_riscv_syscall_trace_entry_vm_ptr?.(index) ?? 0;
      traces.push(
        `0x${traceVmPtr.toString(16)}:${nr.toString()}=${BigInt.asIntN(64, result).toString()}/${status}`
        + `(${formatTraceArg(arg0)},${formatTraceArg(arg1)},${formatTraceArg(arg2)},${formatTraceArg(arg3)},${formatTraceArg(arg4)},${formatTraceArg(arg5)})`,
      );
    }
    if (traces.length > 0) {
      debugParts.push(`riscv_syscalls=[${traces.join(",")}]`);
    }
  }
  const memoryNr = exports.debug_memory_trace_nr?.();
  const memoryTraceLen = exports.debug_memory_trace_len?.() ?? 0;
  if (memoryTraceLen > 0) {
    const traces: string[] = [];
    for (let index = 0; index < memoryTraceLen; index += 1) {
      const nr = readOptionalTraceU64(exports.debug_memory_trace_entry_nr?.(index));
      if (nr === 0n) {
        continue;
      }
      const arg0 = readOptionalTraceU64(exports.debug_memory_trace_entry_arg0?.(index));
      const arg1 = readOptionalTraceU64(exports.debug_memory_trace_entry_arg1?.(index));
      const arg2 = readOptionalTraceU64(exports.debug_memory_trace_entry_arg2?.(index));
      const arg3 = readOptionalTraceU64(exports.debug_memory_trace_entry_arg3?.(index));
      const result = readOptionalTraceU64(exports.debug_memory_trace_entry_result?.(index));
      traces.push(
        `${nr.toString()}(${formatTraceArg(arg0)},${formatTraceArg(arg1)},${formatTraceArg(arg2)},${formatTraceArg(arg3)})=${BigInt.asIntN(64, result).toString()}`,
      );
    }
    if (traces.length > 0) {
      debugParts.push(`mem_syscalls=[${traces.join(",")}]`);
    }
  }
  if (memoryNr !== undefined && memoryNr !== 0n) {
    debugParts.push(`mem_nr=${memoryNr.toString()}`);
    debugParts.push(`mem_a0=${exports.debug_memory_trace_arg0?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_a1=${exports.debug_memory_trace_arg1?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_a2=${exports.debug_memory_trace_arg2?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_a3=${exports.debug_memory_trace_arg3?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_result=${exports.debug_memory_trace_result?.().toString() ?? "n/a"}`);
  }
  const memorySyscallTrace = formatMemorySyscallTraceEntries(exports, vmPtr);
  if (memorySyscallTrace !== null) {
    debugParts.push(`memory_syscalls=${memorySyscallTrace}`);
  }
  try {
    const fdSyscallTrace = collectFdTraceSummaryForDebug(exports, vmPtr);
    if (fdSyscallTrace !== null) {
      debugParts.push(`fd_syscalls=${fdSyscallTrace}`);
    }
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    debugParts.push(`debug_collect_error=${JSON.stringify(detail)}`);
  }
  const vmaLen = exports.debug_memory_vma_len?.() ?? 0;
  const memBrkBase = exports.debug_memory_brk_base?.();
  if (memBrkBase !== undefined) {
    debugParts.push(`mem_brk_base=0x${memBrkBase.toString(16)}`);
    debugParts.push(`mem_brk_current=0x${exports.debug_memory_brk_current?.().toString(16) ?? "n/a"}`);
    const currentVma = findContainingMemoryVma(exports, vmaLen, currentPc);
    if (currentPc !== 0n) {
      debugParts.push(`pc_vma=${currentVma ?? "none"}`);
      if (currentVma === null) {
        const adjacent = findAdjacentMemoryVmas(exports, vmaLen, currentPc);
        debugParts.push(`pc_vma_prev=${adjacent.prev ?? "none"}`);
        debugParts.push(`pc_vma_next=${adjacent.next ?? "none"}`);
      }
    }
    const faultVma = findContainingMemoryVma(exports, vmaLen, faultPc);
    if (faultPc !== 0n) {
      debugParts.push(`fault_vma=${faultVma ?? "none"}`);
      if (faultVma === null) {
        const adjacent = findAdjacentMemoryVmas(exports, vmaLen, faultPc);
        debugParts.push(`fault_vma_prev=${adjacent.prev ?? "none"}`);
        debugParts.push(`fault_vma_next=${adjacent.next ?? "none"}`);
      }
    }
  }
  const processPtr = exports.thread_process_ptr?.(vmPtr) ?? 0;
  if (guestBase > 0 && processPtr > 0) {
    const fdTableBytes = exports.abi_max_fds() * 8;
    const threadCountAddr = processPtr + fdTableBytes;
    const reservationGenAddr = threadCountAddr + 4;
    const reservationLockAddr = reservationGenAddr + 64 * 4;
    const pidAddr = reservationLockAddr + 64 * 4;
    const ppidAddr = pidAddr + 4;
    debugParts.push(`process_ptr=0x${processPtr.toString(16)}`);
    debugParts.push(`process_thread_count=${readGuestU32(exports, guestBase, threadCountAddr) ?? "n/a"}`);
    if (RESERVATION_DEBUG_ENABLED) {
      debugParts.push(`reservation=${summarizeReservationSlots(exports, guestBase, reservationGenAddr, reservationLockAddr)}`);
    }
    debugParts.push(`process_pid_field=${readGuestI32(exports, guestBase, pidAddr) ?? "n/a"}`);
    debugParts.push(`process_ppid_field=${readGuestI32(exports, guestBase, ppidAddr) ?? "n/a"}`);
  }
  const signalDeliverySig = exports.debug_signal_delivery_for_vm_sig?.(vmPtr)
    ?? exports.debug_signal_delivery_sig?.();
  if (signalDeliverySig !== undefined && signalDeliverySig !== 0) {
    debugParts.push(`sig_deliver=${signalDeliverySig}`);
    debugParts.push(`sig_frame=0x${formatU64(exports.debug_signal_delivery_for_vm_frame_base?.(vmPtr) ?? exports.debug_signal_delivery_frame_base?.() ?? 0n)}`);
    debugParts.push(`sig_pc=0x${formatU64(exports.debug_signal_delivery_for_vm_pc?.(vmPtr) ?? exports.debug_signal_delivery_pc?.() ?? 0n)}`);
    debugParts.push(`sig_ra=0x${formatU64(exports.debug_signal_delivery_for_vm_ra?.(vmPtr) ?? exports.debug_signal_delivery_ra?.() ?? 0n)}`);
    debugParts.push(`sig_sp=0x${formatU64(exports.debug_signal_delivery_for_vm_sp?.(vmPtr) ?? exports.debug_signal_delivery_sp?.() ?? 0n)}`);
    debugParts.push(`sig_gp=0x${formatU64(exports.debug_signal_delivery_for_vm_gp?.(vmPtr) ?? exports.debug_signal_delivery_gp?.() ?? 0n)}`);
    debugParts.push(`sig_tp=0x${formatU64(exports.debug_signal_delivery_for_vm_tp?.(vmPtr) ?? exports.debug_signal_delivery_tp?.() ?? 0n)}`);
    debugParts.push(`sig_s11=0x${formatU64(exports.debug_signal_delivery_for_vm_s11?.(vmPtr) ?? exports.debug_signal_delivery_s11?.() ?? 0n)}`);
  }
  const sigreturnPc = exports.debug_sigreturn_pc?.();
  if (sigreturnPc !== undefined && sigreturnPc !== 0n) {
    debugParts.push(`sigret_frame=0x${formatU64(exports.debug_sigreturn_frame_base?.() ?? 0n)}`);
    debugParts.push(`sigret_pc=0x${formatU64(sigreturnPc)}`);
    debugParts.push(`sigret_ra=0x${formatU64(exports.debug_sigreturn_ra?.() ?? 0n)}`);
    debugParts.push(`sigret_sp=0x${formatU64(exports.debug_sigreturn_sp?.() ?? 0n)}`);
    debugParts.push(`sigret_gp=0x${formatU64(exports.debug_sigreturn_gp?.() ?? 0n)}`);
    debugParts.push(`sigret_tp=0x${formatU64(exports.debug_sigreturn_tp?.() ?? 0n)}`);
    debugParts.push(`sigret_s11=0x${formatU64(exports.debug_sigreturn_s11?.() ?? 0n)}`);
  }
  const cloneSpawnTid = exports.debug_clone_spawn_tid?.();
  if (cloneSpawnTid !== undefined && cloneSpawnTid !== 0) {
    const cloneSp = exports.debug_clone_spawn_sp?.() ?? 0n;
    debugParts.push(`clone_tid=${cloneSpawnTid}`);
    debugParts.push(`clone_pc=0x${formatU64(exports.debug_clone_spawn_pc?.() ?? 0n)}`);
    debugParts.push(`clone_ra=0x${formatU64(exports.debug_clone_spawn_ra?.() ?? 0n)}`);
    debugParts.push(`clone_gp=0x${formatU64(exports.debug_clone_spawn_gp?.() ?? 0n)}`);
    debugParts.push(`clone_sp=0x${formatU64(cloneSp)}`);
    debugParts.push(`clone_s11=0x${formatU64(exports.debug_clone_spawn_s11?.() ?? 0n)}`);
    debugParts.push(`clone_stack_m32=0x${formatU64(exports.debug_clone_spawn_stack_m32?.() ?? 0n)}`);
    debugParts.push(`clone_stack_m24=0x${formatU64(exports.debug_clone_spawn_stack_m24?.() ?? 0n)}`);
    debugParts.push(`clone_stack_m16=0x${formatU64(exports.debug_clone_spawn_stack_m16?.() ?? 0n)}`);
    debugParts.push(`clone_stack_m8=0x${formatU64(exports.debug_clone_spawn_stack_m8?.() ?? 0n)}`);
    if (cloneSp >= 32n) {
      debugParts.push(`clone_live_m32=0x${formatU64(readGuestU64ForDebug(exports, guestBase, cloneSp - 32n) ?? 0n)}`);
      debugParts.push(`clone_live_m24=0x${formatU64(readGuestU64ForDebug(exports, guestBase, cloneSp - 24n) ?? 0n)}`);
      debugParts.push(`clone_live_m16=0x${formatU64(readGuestU64ForDebug(exports, guestBase, cloneSp - 16n) ?? 0n)}`);
      debugParts.push(`clone_live_m8=0x${formatU64(readGuestU64ForDebug(exports, guestBase, cloneSp - 8n) ?? 0n)}`);
    }
    const cloneJalrPc = exports.debug_clone_jalr_pc?.();
    if (cloneJalrPc !== undefined && cloneJalrPc !== 0n) {
      debugParts.push(`clone_jalr_pc=0x${formatU64(cloneJalrPc)}`);
      debugParts.push(`clone_jalr_rs1=${exports.debug_clone_jalr_rs1?.() ?? "n/a"}`);
      debugParts.push(`clone_jalr_rd=${exports.debug_clone_jalr_rd?.() ?? "n/a"}`);
      debugParts.push(`clone_jalr_rs1v=0x${formatU64(exports.debug_clone_jalr_rs1_value?.() ?? 0n)}`);
      debugParts.push(`clone_jalr_target=0x${formatU64(exports.debug_clone_jalr_target?.() ?? 0n)}`);
    }
    const cloneStartupLen = exports.debug_clone_startup_trace_len?.() ?? 0;
    if (cloneStartupLen > 0) {
      const cloneStartupEntries: string[] = [];
      for (let index = 0; index < cloneStartupLen; index += 1) {
        const entryVmPtr = exports.debug_clone_startup_trace_entry_vm_ptr?.(index) ?? 0;
        if (entryVmPtr !== vmPtr) {
          continue;
        }
        cloneStartupEntries.push(
          `${index}:tid=${exports.debug_clone_startup_trace_entry_tid?.(index) ?? 0}`
          + `:pc=0x${formatU64(exports.debug_clone_startup_trace_entry_pc?.(index) ?? 0n)}`
          + `:raw=0x${(exports.debug_clone_startup_trace_entry_raw?.(index) ?? 0).toString(16)}`
          + `:a0=0x${formatU64(exports.debug_clone_startup_trace_entry_a0?.(index) ?? 0n)}`
          + `:t1=0x${formatU64(exports.debug_clone_startup_trace_entry_t1?.(index) ?? 0n)}`
          + `:t2=0x${formatU64(exports.debug_clone_startup_trace_entry_t2?.(index) ?? 0n)}`
          + `:sp=0x${formatU64(exports.debug_clone_startup_trace_entry_sp?.(index) ?? 0n)}`
          + `:s11=0x${formatU64(exports.debug_clone_startup_trace_entry_s11?.(index) ?? 0n)}`,
        );
      }
      if (cloneStartupEntries.length > 0) {
        debugParts.push(`clone_startup_trace=[${cloneStartupEntries.join(" ")}]`);
      }
    }
  }
  const trampolineTid = exports.debug_signal_trampoline_hit_tid?.();
  if (trampolineTid !== undefined && trampolineTid !== 0) {
    debugParts.push(`sigtramp_tid=${trampolineTid}`);
    debugParts.push(`sigtramp_pc=0x${formatU64(exports.debug_signal_trampoline_hit_pc?.() ?? 0n)}`);
  }
  const sigreturnCount = exports.debug_sigreturn_count?.();
  if (sigreturnCount !== undefined && sigreturnCount !== 0) {
    debugParts.push(`sigreturn_count=${sigreturnCount}`);
  }
  const panicLine = exports.debug_panic_line?.() ?? 0;
  if (panicLine !== 0) {
    debugParts.push(`panic_line=${panicLine}`);
    debugParts.push(`panic_col=${exports.debug_panic_column?.() ?? 0}`);
    const panicFilePtr = exports.debug_panic_file_ptr?.() ?? 0;
    const panicFileLen = exports.debug_panic_file_len?.() ?? 0;
    if (panicFilePtr !== 0 && panicFileLen !== 0) {
      const panicFileBytes = copyBytes(new Uint8Array(exports.memory.buffer, panicFilePtr, panicFileLen));
      debugParts.push(`panic_file=${JSON.stringify(decoder.decode(panicFileBytes))}`);
    }
  }
  const pathTraceLen = exports.debug_path_syscall_trace_len?.() ?? 0;
  if (pathTraceLen > 0) {
    const traces: string[] = [];
    for (let index = 0; index < pathTraceLen; index += 1) {
      const nr = readOptionalTraceU64(exports.debug_path_syscall_trace_nr?.(index));
      if (nr === 0n) {
        continue;
      }
      const pathPtr = exports.debug_path_syscall_trace_path_ptr?.(index) ?? 0;
      const pathLen = exports.debug_path_syscall_trace_path_len?.(index) ?? 0;
      const path = pathPtr !== 0 && pathLen > 0
        ? decoder.decode(copyBytes(new Uint8Array(exports.memory.buffer, pathPtr, pathLen)))
        : "";
      const traceVmPtr = exports.debug_path_syscall_trace_vm_ptr?.(index) ?? 0;
      const dirfd = readOptionalTraceU64(exports.debug_path_syscall_trace_dirfd?.(index));
      const result = readOptionalTraceU64(exports.debug_path_syscall_trace_result?.(index));
      const status = exports.debug_path_syscall_trace_status?.(index) ?? 0;
      traces.push(
        `0x${traceVmPtr.toString(16)}:${nr.toString()}(${BigInt.asIntN(64, dirfd).toString()},${JSON.stringify(path)})=`
        + `${BigInt.asIntN(64, result).toString()}/${status}`,
      );
    }
    if (traces.length > 0) {
      debugParts.push(`path_syscalls=[${traces.join(",")}]`);
    }
  }
  const zeroRegPc = exports.debug_zero_reg_for_vm_pc?.(vmPtr) ?? exports.debug_zero_reg_pc?.();
  if (zeroRegPc !== undefined && zeroRegPc !== 0n) {
    debugParts.push(`zero_reg_tid=${exports.debug_zero_reg_for_vm_tid?.(vmPtr) ?? exports.debug_zero_reg_tid?.() ?? "n/a"}`);
    debugParts.push(`zero_reg_pc=0x${formatU64(zeroRegPc)}`);
    pushAddressVmaDebug(debugParts, exports, vmaLen, "zero_reg_pc", zeroRegPc);
    debugParts.push(`zero_reg_raw=0x${(exports.debug_zero_reg_for_vm_raw?.(vmPtr) ?? exports.debug_zero_reg_raw?.() ?? 0).toString(16)}`);
    const zeroRegInsn = readGuestInsn(exports, guestBase, zeroRegPc);
    if (zeroRegInsn !== null) {
      debugParts.push(`zero_reg_insn=${zeroRegInsn}`);
    }
    debugParts.push(`zero_reg=${exports.debug_zero_reg_for_vm_index?.(vmPtr) ?? exports.debug_zero_reg_index?.() ?? "n/a"}`);
    debugParts.push(`zero_reg_prev=0x${formatU64(exports.debug_zero_reg_for_vm_prev_value?.(vmPtr) ?? exports.debug_zero_reg_prev_value?.() ?? 0n)}`);
    debugParts.push(`zero_reg_sp=0x${formatU64(exports.debug_zero_reg_for_vm_sp?.(vmPtr) ?? exports.debug_zero_reg_sp?.() ?? 0n)}`);
    debugParts.push(`zero_reg_s11=0x${formatU64(exports.debug_zero_reg_for_vm_s11?.(vmPtr) ?? exports.debug_zero_reg_s11?.() ?? 0n)}`);
  }
  const s11WritePc = exports.debug_s11_write_for_vm_pc?.(vmPtr) ?? exports.debug_s11_write_pc?.();
  if (s11WritePc !== undefined && s11WritePc !== 0n) {
    debugParts.push(`s11_write_tid=${exports.debug_s11_write_for_vm_tid?.(vmPtr) ?? exports.debug_s11_write_tid?.() ?? "n/a"}`);
    debugParts.push(`s11_write_pc=0x${formatU64(s11WritePc)}`);
    pushAddressVmaDebug(debugParts, exports, vmaLen, "s11_write_pc", s11WritePc);
    debugParts.push(`s11_write_raw=0x${(exports.debug_s11_write_for_vm_raw?.(vmPtr) ?? exports.debug_s11_write_raw?.() ?? 0).toString(16)}`);
    const s11WriteInsn = readGuestInsn(exports, guestBase, s11WritePc);
    if (s11WriteInsn !== null) {
      debugParts.push(`s11_write_insn=${s11WriteInsn}`);
    }
    debugParts.push(`s11_write_src=${exports.debug_s11_write_for_vm_source?.(vmPtr) ?? exports.debug_s11_write_source?.() ?? "n/a"}`);
    debugParts.push(
      `s11_write_prev=0x${formatU64(exports.debug_s11_write_for_vm_prev_value?.(vmPtr) ?? exports.debug_s11_write_prev_value?.() ?? 0n)}`,
    );
    debugParts.push(`s11_write=0x${formatU64(exports.debug_s11_write_for_vm_value?.(vmPtr) ?? exports.debug_s11_write_value?.() ?? 0n)}`);
  }
  const spWritePc = exports.debug_sp_write_for_vm_pc?.(vmPtr) ?? exports.debug_sp_write_pc?.();
  if (spWritePc !== undefined && spWritePc !== 0n) {
    debugParts.push(`sp_write_tid=${exports.debug_sp_write_for_vm_tid?.(vmPtr) ?? exports.debug_sp_write_tid?.() ?? "n/a"}`);
    debugParts.push(`sp_write_pc=0x${formatU64(spWritePc)}`);
    pushAddressVmaDebug(debugParts, exports, vmaLen, "sp_write_pc", spWritePc);
    debugParts.push(`sp_write_raw=0x${(exports.debug_sp_write_for_vm_raw?.(vmPtr) ?? exports.debug_sp_write_raw?.() ?? 0).toString(16)}`);
    const spWriteInsn = readGuestInsn(exports, guestBase, spWritePc);
    if (spWriteInsn !== null) {
      debugParts.push(`sp_write_insn=${spWriteInsn}`);
    }
    debugParts.push(`sp_write_src=${exports.debug_sp_write_for_vm_source?.(vmPtr) ?? exports.debug_sp_write_source?.() ?? "n/a"}`);
    debugParts.push(`sp_write_prev=0x${formatU64(exports.debug_sp_write_for_vm_prev_value?.(vmPtr) ?? exports.debug_sp_write_prev_value?.() ?? 0n)}`);
    debugParts.push(`sp_write=0x${formatU64(exports.debug_sp_write_for_vm_value?.(vmPtr) ?? exports.debug_sp_write_value?.() ?? 0n)}`);
  }
  const threadStepTrace = readThreadStepTraceSnapshot(exports, vmPtr);
  const threadStepEntryTrace = readThreadStepEntryTraceSnapshot(exports, vmPtr);
  const threadStepPhaseTrace = readThreadStepPhaseSnapshot(exports, vmPtr);
  if (threadStepEntryTrace.tid !== 0 || threadStepEntryTrace.pc !== 0n || threadStepEntryTrace.sp !== 0n || threadStepEntryTrace.s11 !== 0n) {
    debugParts.push(`thread_step_entry_tid=${threadStepEntryTrace.tid}`);
    debugParts.push(`thread_step_entry_status=${threadStepEntryTrace.status}`);
    debugParts.push(`thread_step_entry_budget=${threadStepEntryTrace.budget}`);
    debugParts.push(`thread_step_entry_pc=0x${formatU64(threadStepEntryTrace.pc)}`);
    debugParts.push(`thread_step_entry_ra=0x${formatU64(threadStepEntryTrace.ra)}`);
    debugParts.push(`thread_step_entry_sp=0x${formatU64(threadStepEntryTrace.sp)}`);
    debugParts.push(`thread_step_entry_s11=0x${formatU64(threadStepEntryTrace.s11)}`);
  }
  debugParts.push(`thread_step_tid=${threadStepTrace.tid}`);
  debugParts.push(`thread_step_vm=0x${threadStepTrace.vmPtr.toString(16)}`);
  debugParts.push(`thread_step_status=${threadStepTrace.status}`);
  debugParts.push(`thread_step_budget=${threadStepTrace.budget}`);
  debugParts.push(`thread_step_remaining=${threadStepTrace.remaining}`);
  debugParts.push(`thread_step_pc=0x${formatU64(threadStepTrace.pc)}`);
  debugParts.push(`thread_step_ra=0x${formatU64(threadStepTrace.ra)}`);
  debugParts.push(`thread_step_sp=0x${formatU64(threadStepTrace.sp)}`);
  debugParts.push(`thread_step_s11=0x${formatU64(threadStepTrace.s11)}`);
  if (threadStepPhaseTrace.phase !== 0
    || threadStepPhaseTrace.status !== 0
    || threadStepPhaseTrace.budget !== 0
    || threadStepPhaseTrace.remaining !== 0
    || threadStepPhaseTrace.pc !== 0n
    || threadStepPhaseTrace.ra !== 0n
    || threadStepPhaseTrace.sp !== 0n
    || threadStepPhaseTrace.s11 !== 0n) {
    debugParts.push(`thread_step_phase=${threadStepPhaseTrace.phase}`);
    debugParts.push(`thread_step_phase_vm=0x${threadStepPhaseTrace.vmPtr.toString(16)}`);
    debugParts.push(`thread_step_phase_status=${threadStepPhaseTrace.status}`);
    debugParts.push(`thread_step_phase_budget=${threadStepPhaseTrace.budget}`);
    debugParts.push(`thread_step_phase_remaining=${threadStepPhaseTrace.remaining}`);
    debugParts.push(`thread_step_phase_pc=0x${formatU64(threadStepPhaseTrace.pc)}`);
    debugParts.push(`thread_step_phase_ra=0x${formatU64(threadStepPhaseTrace.ra)}`);
    debugParts.push(`thread_step_phase_sp=0x${formatU64(threadStepPhaseTrace.sp)}`);
    debugParts.push(`thread_step_phase_s11=0x${formatU64(threadStepPhaseTrace.s11)}`);
  }
  if (
    (threadStepTrace.tid === 0 && threadStepTrace.pc === 0n && threadStepTrace.ra === 0n && threadStepTrace.sp === 0n && threadStepTrace.s11 === 0n)
    || threadStepTrace.pc < 0x1000n
    || threadStepTrace.sp === 0n
    || threadStepTrace.s11 === 0n
  ) {
    const threadStepTraceEntries = formatThreadStepTraceEntries(exports, vmPtr);
    if (threadStepTraceEntries !== null) {
      debugParts.push(`thread_step_traces=[${threadStepTraceEntries}]`);
    }
  }
  if (threadStepTrace.pc === 0x4n || threadStepTrace.sp === 0n || threadStepTrace.s11 === 0n) {
    const threadStepExecTraceEntries = formatExecTraceEntries(exports, vmPtr);
    if (threadStepExecTraceEntries !== null) {
      debugParts.push(`thread_step_exec_trace=[${threadStepExecTraceEntries}]`);
    }
    const breakExecTraceEntries = formatBreakExecTraceEntries(exports, vmPtr);
    if (breakExecTraceEntries !== null) {
      debugParts.push(`break_exec_trace=[${breakExecTraceEntries}]`);
    }
  }
  const faultDispatchTrace = readFaultDispatchTraceSnapshot(exports, vmPtr);
  if (faultDispatchTrace.phase !== 0) {
    debugParts.push(`fault_dispatch_tid=${faultDispatchTrace.tid}`);
    debugParts.push(`fault_dispatch_phase=${faultDispatchTrace.phase}`);
    debugParts.push(`fault_dispatch_pc=0x${formatU64(faultDispatchTrace.pc)}`);
    debugParts.push(`fault_dispatch_ra=0x${formatU64(faultDispatchTrace.ra)}`);
    debugParts.push(`fault_dispatch_sp=0x${formatU64(faultDispatchTrace.sp)}`);
    debugParts.push(`fault_dispatch_s11=0x${formatU64(faultDispatchTrace.s11)}`);
    debugParts.push(`fault_dispatch_addr=0x${formatU64(faultDispatchTrace.faultAddr)}`);
    const syncFaultExecTraceEntries = formatSyncFaultExecTraceEntries(exports, vmPtr);
    if (syncFaultExecTraceEntries !== null) {
      debugParts.push(`sync_fault_exec_trace=[${syncFaultExecTraceEntries}]`);
    }
    const execTraceEntries = formatExecTraceEntries(exports, vmPtr);
    if (execTraceEntries !== null) {
      debugParts.push(`exec_trace=[${execTraceEntries}]`);
    }
  }
  const jalrPc = exports.debug_jalr_for_vm_pc?.(vmPtr) ?? exports.debug_jalr_pc?.();
  if (jalrPc !== undefined && jalrPc !== 0n) {
    const jalrRs1Value = exports.debug_jalr_for_vm_rs1_value?.(vmPtr) ?? exports.debug_jalr_rs1_value?.() ?? 0n;
    const jalrTarget = exports.debug_jalr_for_vm_target?.(vmPtr) ?? exports.debug_jalr_target?.() ?? 0n;
    debugParts.push(`jalr_pc=0x${formatU64(jalrPc)}`);
    debugParts.push(`jalr_rs1=${exports.debug_jalr_for_vm_rs1?.(vmPtr) ?? exports.debug_jalr_rs1?.() ?? "n/a"}`);
    debugParts.push(`jalr_rd=${exports.debug_jalr_for_vm_rd?.(vmPtr) ?? exports.debug_jalr_rd?.() ?? "n/a"}`);
    debugParts.push(`jalr_rs1v=0x${formatU64(jalrRs1Value)}`);
    debugParts.push(`jalr_target=0x${formatU64(jalrTarget)}`);
  }
  try {
    const pathBytes = encoder.encode("/proc/self/exe");
    const scratchBase = debugScratchBase(exports);
    const pathPtr = scratchBase;
    const bufPtr = scratchBase + 0x200;
    new Uint8Array(exports.memory.buffer, pathPtr, pathBytes.length).set(pathBytes);
    const ino = exports.memfs_resolve_path(pathPtr, pathBytes.length);
    if (ino === 0 || (exports.memfs_inode_mode(ino) & 0o170000) !== 0o120000) {
      debugParts.push("proc_self_exe=missing");
    } else {
      const len = Number(exports.memfs_read_link(ino, bufPtr, 1024));
      if (len <= 0) {
        debugParts.push("proc_self_exe=empty");
      } else {
        const target = copyBytes(new Uint8Array(exports.memory.buffer, bufPtr, len));
        debugParts.push(`proc_self_exe=${JSON.stringify(decoder.decode(target))}`);
      }
    }
  } catch {
    debugParts.push("proc_self_exe=unavailable");
  }
  } catch (error) {
    pushRecoveredTraceDebug(debugParts, exports, vmPtr);
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    debugParts.push(`debug_collect_error=${JSON.stringify(detail)}`);
  }

  return debugParts;
}

export function formatWorkerError(
  exports: KernelExports,
  vmPtr: number,
  guestBase: number,
  error: unknown,
): string {
  const base = error instanceof Error ? (error.stack ?? error.message) : String(error);
  const debugParts = collectDebugParts(exports, vmPtr, guestBase);
  return debugParts.length === 0 ? base : `${base}\n${debugParts.join(" ")}`;
}
