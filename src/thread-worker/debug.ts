import type { KernelExports } from "../abi.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function copyBytes(view: Uint8Array): Uint8Array {
  const copy = new Uint8Array(view.byteLength);
  copy.set(view);
  return copy;
}

function formatU64(value: bigint): string {
  return BigInt.asUintN(64, value).toString(16);
}

function readOptionalTraceU64(value: bigint | number | undefined): bigint {
  if (value === undefined) {
    return 0n;
  }
  return typeof value === "bigint" ? value : BigInt(value);
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

interface SyncSignalTraceSnapshot {
  tid: number;
  localPc: bigint;
  localRa: bigint;
  localSp: bigint;
  localS11: bigint;
  localA0: bigint;
  vmPc: bigint;
  vmRa: bigint;
  vmSp: bigint;
  vmS11: bigint;
  vmA0: bigint;
  faultAddr: bigint;
  sigPending: bigint;
  sigMask: bigint;
  sigInfoCode: number;
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

function readSyncSignalTraceSnapshot(exports: KernelExports, vmPtr: number): SyncSignalTraceSnapshot {
  const perVm = {
    tid: exports.debug_sync_signal_for_vm_tid?.(vmPtr) ?? 0,
    localPc: exports.debug_sync_signal_for_vm_local_pc?.(vmPtr) ?? 0n,
    localRa: exports.debug_sync_signal_for_vm_local_ra?.(vmPtr) ?? 0n,
    localSp: exports.debug_sync_signal_for_vm_local_sp?.(vmPtr) ?? 0n,
    localS11: exports.debug_sync_signal_for_vm_local_s11?.(vmPtr) ?? 0n,
    localA0: exports.debug_sync_signal_for_vm_local_a0?.(vmPtr) ?? 0n,
    vmPc: exports.debug_sync_signal_for_vm_vm_pc?.(vmPtr) ?? 0n,
    vmRa: exports.debug_sync_signal_for_vm_vm_ra?.(vmPtr) ?? 0n,
    vmSp: exports.debug_sync_signal_for_vm_vm_sp?.(vmPtr) ?? 0n,
    vmS11: exports.debug_sync_signal_for_vm_vm_s11?.(vmPtr) ?? 0n,
    vmA0: exports.debug_sync_signal_for_vm_vm_a0?.(vmPtr) ?? 0n,
    faultAddr: exports.debug_sync_signal_for_vm_addr?.(vmPtr) ?? 0n,
    sigPending: exports.debug_sync_signal_for_vm_sig_pending?.(vmPtr) ?? 0n,
    sigMask: exports.debug_sync_signal_for_vm_sig_mask?.(vmPtr) ?? 0n,
    sigInfoCode: exports.debug_sync_signal_for_vm_sig_info_code?.(vmPtr) ?? 0,
  } satisfies SyncSignalTraceSnapshot;
  if (
    perVm.tid !== 0
    || perVm.localPc !== 0n
    || perVm.localRa !== 0n
    || perVm.localSp !== 0n
    || perVm.localS11 !== 0n
    || perVm.localA0 !== 0n
    || perVm.vmPc !== 0n
    || perVm.vmRa !== 0n
    || perVm.vmSp !== 0n
    || perVm.vmS11 !== 0n
    || perVm.vmA0 !== 0n
    || perVm.faultAddr !== 0n
    || perVm.sigPending !== 0n
    || perVm.sigMask !== 0n
    || perVm.sigInfoCode !== 0
  ) {
    return perVm;
  }
  return {
    tid: 0,
    localPc: 0n,
    localRa: 0n,
    localSp: 0n,
    localS11: 0n,
    localA0: 0n,
    vmPc: 0n,
    vmRa: 0n,
    vmSp: 0n,
    vmS11: 0n,
    vmA0: 0n,
    faultAddr: 0n,
    sigPending: 0n,
    sigMask: 0n,
    sigInfoCode: 0,
  } satisfies SyncSignalTraceSnapshot;
}

function readOptionalThreadReg(
  exports: KernelExports,
  vmPtr: number,
  regIndex: number,
): bigint {
  return exports.thread_reg?.(vmPtr, regIndex) ?? 0n;
}

function readOptionalThreadStackMinus(
  exports: KernelExports,
  vmPtr: number,
  byteOffset: number,
): bigint {
  return exports.thread_stack_minus?.(vmPtr, byteOffset) ?? 0n;
}

function formatWriteTraceBytes(exports: KernelExports): string | null {
  const ptr = exports.debug_write_trace_ptr?.() ?? 0;
  const len = exports.debug_write_trace_len?.() ?? 0;
  if (ptr === 0 || len === 0) {
    return null;
  }
  const bytes = new Uint8Array(exports.memory.buffer, ptr, len);
  let out = "";
  for (const byte of bytes) {
    if (byte >= 0x20 && byte <= 0x7e) {
      out += String.fromCharCode(byte);
    } else if (byte === 0x0a) {
      out += "\\n";
    } else if (byte === 0x0d) {
      out += "\\r";
    } else if (byte === 0x09) {
      out += "\\t";
    } else {
      out += `\\x${byte.toString(16).padStart(2, "0")}`;
    }
  }
  return out;
}

function formatGuestWriteOverlapSample(sample: bigint, sampleLen: number): string {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, sample, true);
  return [...bytes.slice(0, sampleLen)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function readKernelU32(exports: KernelExports, addr: number): number | null {
  if (addr <= 0 || addr + 4 > exports.memory.buffer.byteLength) {
    return null;
  }
  return new DataView(exports.memory.buffer).getUint32(addr, true);
}

function readKernelU64(exports: KernelExports, addr: number): bigint | null {
  if (addr <= 0 || addr + 8 > exports.memory.buffer.byteLength) {
    return null;
  }
  return new DataView(exports.memory.buffer).getBigUint64(addr, true);
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

function formatTrackedEpollState(exports: KernelExports, vmPtr: number): string | null {
  const tablePtr = exports.kernel_epoll_table_ptr?.() ?? 0;
  const tableBytes = exports.kernel_epoll_table_bytes?.() ?? 0;
  const instanceBytes = exports.kernel_epoll_instance_bytes?.() ?? 0;
  if (tablePtr === 0 || tableBytes === 0 || instanceBytes < 4) {
    return null;
  }

  const table = new Uint32Array(exports.memory.buffer, tablePtr, tableBytes >>> 2);
  const view = new DataView(exports.memory.buffer);
  const entries: string[] = [];
  for (let fd = 0; fd < Math.min(exports.abi_max_fds(), 32); fd += 1) {
    const ofdId = exports.process_fd_ofd_id(vmPtr, fd);
    if (ofdId === 0 || exports.kernel_ofd_fd_type(ofdId) !== exports.abi_fd_type_epoll()) {
      continue;
    }
    const ino = exports.kernel_ofd_ino(ofdId);
    const ptr = ino >= 0 && ino < table.length ? table[ino] ?? 0 : 0;
    const count = ptr !== 0 ? view.getUint32(ptr + instanceBytes - 4, true) : 0;
    entries.push(`fd${fd}:${ofdId}/${ino}->0x${ptr.toString(16)}#${count}`);
  }

  return entries.length > 0 ? entries.join(",") : null;
}

function formatProcessThreads(exports: KernelExports, vmPtr: number): string | null {
  const threadCount = exports.thread_process_thread_count(vmPtr);
  if (threadCount <= 0) {
    return null;
  }
  const entries: string[] = [];
  const limit = Math.min(exports.abi_max_threads(), Math.max(threadCount, 8));
  for (let slot = 0; slot < limit; slot += 1) {
    const tid = exports.process_thread_tid(vmPtr, slot);
    const state = exports.process_thread_state(vmPtr, slot);
    const threadVmPtr = exports.process_thread_vm_ptr(vmPtr, slot);
    if (tid === 0 && state === 0 && threadVmPtr === 0) {
      continue;
    }
    entries.push(`${slot}:${tid}/${state}/0x${threadVmPtr.toString(16)}`);
  }
  return entries.length > 0 ? entries.join(",") : null;
}

function formatProcessFds(exports: KernelExports, vmPtr: number): string | null {
  const entries: string[] = [];
  const maxFds = Math.min(exports.abi_max_fds(), 32);
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = exports.process_fd_ofd_id(vmPtr, fd);
    const fdFlags = exports.process_fd_flags(vmPtr, fd);
    if (ofdId === 0 && fdFlags === 0) {
      continue;
    }
    const fdType = ofdId === 0 ? 0 : exports.kernel_ofd_fd_type(ofdId);
    const ino = ofdId === 0 ? 0 : exports.kernel_ofd_ino(ofdId);
    entries.push(`${fd}:${ofdId}/${fdType}/${ino}/${fdFlags}`);
  }
  return entries.length > 0 ? entries.join(",") : null;
}

export function clearSyscallTrace(exports: KernelExports): void {
  exports.debug_clear_syscall_trace?.();
  exports.debug_clear_signal_trace?.();
  exports.debug_clear_cpu_trace?.();
  exports.debug_clear_write_trace?.();
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

function formatGuestBytesForDebug(
  exports: KernelExports,
  guestBase: number,
  addr: bigint,
  length: number,
): string | null {
  const guestAddr = Number(addr);
  if (!Number.isFinite(guestAddr) || guestAddr < 0 || guestAddr > Number.MAX_SAFE_INTEGER) {
    return null;
  }
  const absolute = guestBase + guestAddr;
  if (absolute < 0 || absolute + length > exports.memory.buffer.byteLength) {
    return null;
  }
  return [...new Uint8Array(exports.memory.buffer, absolute, length)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
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
  debugParts.push(`sp_m32=0x${readOptionalThreadStackMinus(exports, vmPtr, 32).toString(16)}`);
  debugParts.push(`sp_m24=0x${readOptionalThreadStackMinus(exports, vmPtr, 24).toString(16)}`);
  debugParts.push(`sp_m16=0x${readOptionalThreadStackMinus(exports, vmPtr, 16).toString(16)}`);
  debugParts.push(`sp_m8=0x${readOptionalThreadStackMinus(exports, vmPtr, 8).toString(16)}`);
  debugParts.push(`stk0=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 0) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk1=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 1) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk4=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 4) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk8=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 8) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk10=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 10) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk14=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 14) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk18=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 18) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk19=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 19) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`a0=${exports.thread_a0(vmPtr).toString()}`);
  debugParts.push(`a1=${exports.thread_a1(vmPtr).toString()}`);
  debugParts.push(`a2=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 12))}`);
  debugParts.push(`a3=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 13))}`);
  debugParts.push(`a4=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 14))}`);
  debugParts.push(`a5=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 15))}`);
  debugParts.push(`a6=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 16))}`);
  debugParts.push(`a7=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 17))}`);
  debugParts.push(`t3=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 28))}`);
  debugParts.push(`t5=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 30))}`);
  const tpWriteTid = exports.debug_tp_write_tid?.() ?? 0;
  if (tpWriteTid !== 0) {
    debugParts.push(`tp_write_tid=${tpWriteTid}`);
  }
  const cloneFlags = exports.thread_clone_flags(vmPtr);
  if (cloneFlags !== 0n) {
    debugParts.push(`clone_flags=0x${formatU64(cloneFlags)}`);
    debugParts.push(`clone_child_stack=0x${formatU64(readOptionalThreadU64(exports.thread_clone_child_stack, vmPtr))}`);
    debugParts.push(`clone_tls=0x${formatU64(readOptionalThreadU64(exports.thread_clone_tls, vmPtr))}`);
    debugParts.push(`clone_child_tidptr=0x${formatU64(readOptionalThreadU64(exports.thread_clone_child_tidptr, vmPtr))}`);
  }
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
        + `:pid=${[...pidSampleBytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`,
      );
    }
    if (traces.length > 0) {
      debugParts.push(`syscalls=[${traces.join(",")}]`);
    }
  }
  const writeNr = exports.debug_write_trace_last_nr?.();
  if (writeNr !== undefined && writeNr !== 0n) {
    const writeFd = Number(exports.debug_write_trace_last_fd?.() ?? 0n);
    const writeOfdId = exports.debug_write_trace_last_ofd_id?.() ?? 0;
    const writeOfdType = exports.debug_write_trace_last_ofd_type?.() ?? "n/a";
    debugParts.push(`write_nr=${writeNr.toString()}`);
    debugParts.push(`write_fd=${writeFd.toString()}`);
    debugParts.push(`write_buf=0x${exports.debug_write_trace_last_buf_addr?.().toString(16) ?? "0"}`);
    debugParts.push(`write_count=${exports.debug_write_trace_last_count?.().toString() ?? "n/a"}`);
    debugParts.push(`write_result=${exports.debug_write_trace_last_result?.().toString() ?? "n/a"}`);
    debugParts.push(`write_ofd_id=${writeOfdId}`);
    debugParts.push(`write_ofd_type=${writeOfdType}`);
    if (writeOfdId > 0) {
      const writeOfdIno = exports.debug_write_trace_last_ofd_ino?.() ?? exports.kernel_ofd_ino(writeOfdId);
      debugParts.push(`write_ofd_ino=${writeOfdIno}`);
      debugParts.push(`write_ofd_host_fd=${exports.kernel_ofd_host_fd(writeOfdId)}`);
      debugParts.push(`write_ofd_flags=${exports.kernel_ofd_status_flags(writeOfdId)}`);
      if (writeOfdType === 6) {
        debugParts.push(`write_pipe_read_open=${exports.debug_write_trace_last_pipe_read_open?.() ?? "n/a"}`);
        debugParts.push(`write_pipe_write_open=${exports.debug_write_trace_last_pipe_write_open?.() ?? "n/a"}`);
      }
    }
    const writeBytes = formatWriteTraceBytes(exports);
    if (writeBytes !== null) {
      debugParts.push(`write_data=${JSON.stringify(writeBytes)}`);
    }
  }
  const lastPipeCloseSlot = exports.debug_last_pipe_close_slot?.() ?? 0;
  if (lastPipeCloseSlot !== 0) {
    debugParts.push(`last_pipe_close_slot=${lastPipeCloseSlot}`);
    debugParts.push(`last_pipe_close_end=${exports.debug_last_pipe_close_is_read_end?.() === 1 ? "read" : "write"}`);
    debugParts.push(`last_pipe_close_ofd=${exports.debug_last_pipe_close_ofd_id?.() ?? "n/a"}`);
    debugParts.push(`last_pipe_close_ro=${exports.debug_last_pipe_close_read_open?.() ?? "n/a"}`);
    debugParts.push(`last_pipe_close_wo=${exports.debug_last_pipe_close_write_open?.() ?? "n/a"}`);
  }
  const memoryNr = exports.debug_memory_trace_nr?.();
  if (memoryNr !== undefined && memoryNr !== 0n) {
    debugParts.push(`mem_nr=${memoryNr.toString()}`);
    debugParts.push(`mem_a0=${exports.debug_memory_trace_arg0?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_a1=${exports.debug_memory_trace_arg1?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_a2=${exports.debug_memory_trace_arg2?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_a3=${exports.debug_memory_trace_arg3?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_result=${exports.debug_memory_trace_result?.().toString() ?? "n/a"}`);
  }
  const memBrkBase = exports.debug_memory_brk_base?.();
  if (memBrkBase !== undefined) {
    debugParts.push(`heap_base=0x${exports.kernel_heap_base?.().toString(16) ?? "0"}`);
    debugParts.push(`heap_ptr=0x${exports.kernel_heap_ptr?.().toString(16) ?? "0"}`);
    debugParts.push(`mem_state_ptr=0x${exports.kernel_mem_state_ptr?.().toString(16) ?? "0"}`);
    debugParts.push(`mem_brk_base=0x${memBrkBase.toString(16)}`);
    debugParts.push(`mem_brk_current=0x${exports.debug_memory_brk_current?.().toString(16) ?? "n/a"}`);
    debugParts.push(`mem_mmap_next=0x${exports.debug_memory_mmap_next?.().toString(16) ?? "n/a"}`);
    const vmaLen = exports.debug_memory_vma_len?.() ?? 0;
    debugParts.push(`mem_vmas=${vmaLen}`);
    for (let i = 0; i < Math.min(vmaLen, 4); i += 1) {
      debugParts.push(`mem_vma${i}=${formatMemoryVma(exports, i)}`);
    }
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
  const guestMemAddr = exports.debug_guest_mem_last_addr?.();
  if (guestMemAddr !== undefined && guestMemAddr !== 0n) {
    debugParts.push(`guest_mem_addr=0x${formatU64(guestMemAddr)}`);
    debugParts.push(`guest_mem_len=${exports.debug_guest_mem_last_len?.() ?? "n/a"}`);
    debugParts.push(`guest_mem_kind=${exports.debug_guest_mem_last_kind?.() ?? "n/a"}`);
  }
  const processPtr = exports.thread_process_ptr?.(vmPtr) ?? 0;
  if (guestBase > 0 && processPtr > 0) {
    const fdTableBytes = exports.abi_max_fds() * 8;
    const threadCountAddr = processPtr + fdTableBytes;
    const reservationGenAddr = threadCountAddr + 4;
    const reservationLockAddr = reservationGenAddr + 64 * 4;
    const pidAddr = reservationLockAddr + 64 * 4;
    const ppidAddr = pidAddr + 4;
    const cwdAddr = ppidAddr + 4;
    const stdinPipeAddr = cwdAddr + 4;
    const stdoutPipeAddr = stdinPipeAddr + 4;
    const stderrPipeAddr = stdoutPipeAddr + 4;
    debugParts.push(`process_ptr=0x${processPtr.toString(16)}`);
    debugParts.push(`process_thread_count=${readGuestU32(exports, guestBase, threadCountAddr) ?? "n/a"}`);
    debugParts.push(`process_pid_field=${readGuestI32(exports, guestBase, pidAddr) ?? "n/a"}`);
    debugParts.push(`process_ppid_field=${readGuestI32(exports, guestBase, ppidAddr) ?? "n/a"}`);
    debugParts.push(`process_cwd_field=${readGuestU32(exports, guestBase, cwdAddr) ?? "n/a"}`);
    debugParts.push(`process_stdio_slots=${[
      readGuestU32(exports, guestBase, stdinPipeAddr) ?? "n/a",
      readGuestU32(exports, guestBase, stdoutPipeAddr) ?? "n/a",
      readGuestU32(exports, guestBase, stderrPipeAddr) ?? "n/a",
    ].join("/")}`);
  }
  const currentPid = exports.process_current_pid?.(vmPtr) ?? 0;
  const sigSlotsPtr = exports.kernel_sig_slots_ptr?.() ?? 0;
  if (currentPid > 0 && sigSlotsPtr > 0) {
    const slotAddr = sigSlotsPtr + (currentPid - 1) * 4;
    const slotPtr = readKernelU32(exports, slotAddr);
    debugParts.push(`sig_slots_ptr=0x${sigSlotsPtr.toString(16)}`);
    debugParts.push(`sig_slot_pid=${currentPid}`);
    debugParts.push(`sig_slot_addr=0x${slotAddr.toString(16)}`);
    debugParts.push(`sig_slot_ptr=0x${(slotPtr ?? 0).toString(16)}`);
  }
  const processPendingPtr = exports.kernel_process_pending_ptr?.() ?? 0;
  if (currentPid > 0 && processPendingPtr > 0) {
    const pendingAddr = processPendingPtr + (currentPid - 1) * 8;
    const pending = readKernelU64(exports, pendingAddr);
    debugParts.push(`process_pending_ptr=0x${processPendingPtr.toString(16)}`);
    debugParts.push(`process_pending_addr=0x${pendingAddr.toString(16)}`);
    debugParts.push(`process_pending=${pending !== null ? `0x${formatU64(pending)}` : "n/a"}`);
  }
  const guestWriteOverlapTraceLen = exports.debug_guest_write_overlap_trace_len?.() ?? 0;
  if (guestWriteOverlapTraceLen > 0) {
    const traces: string[] = [];
    for (let index = 0; index < guestWriteOverlapTraceLen; index += 1) {
      const slot = exports.debug_guest_write_overlap_trace_slot?.(index) ?? 0;
      const op = exports.debug_guest_write_overlap_trace_op?.(index) ?? 0;
      const addr = readOptionalTraceU64(exports.debug_guest_write_overlap_trace_addr?.(index));
      const len = exports.debug_guest_write_overlap_trace_len_bytes?.(index) ?? 0;
      const sampleLen = exports.debug_guest_write_overlap_trace_sample_len?.(index) ?? 0;
      const sample = readOptionalTraceU64(exports.debug_guest_write_overlap_trace_sample?.(index));
      const execveStage = exports.debug_guest_write_overlap_trace_execve_stage?.(index) ?? 0;
      traces.push(
        `${slot}:${op}@0x${formatU64(addr)}+${len}:s${execveStage}:` +
        `${sampleLen > 0 ? formatGuestWriteOverlapSample(sample, sampleLen) : "-"}`,
      );
    }
    if (traces.length > 0) {
      debugParts.push(`guest_write_overlaps=[${traces.join(",")}]`);
    }
  }
  const epollWaitReason = exports.debug_epoll_wait_reason?.() ?? 0;
  if (epollWaitReason !== 0) {
    debugParts.push(`epoll_wait_fd=${exports.debug_epoll_wait_epfd?.().toString() ?? "n/a"}`);
    debugParts.push(`epoll_wait_ofd=${exports.debug_epoll_wait_ofd_id?.() ?? "n/a"}`);
    debugParts.push(`epoll_wait_type=${exports.debug_epoll_wait_ofd_type?.() ?? "n/a"}`);
    debugParts.push(`epoll_wait_idx=${exports.debug_epoll_wait_idx?.() ?? "n/a"}`);
    debugParts.push(`epoll_wait_ptr=0x${(exports.debug_epoll_wait_ptr?.() ?? 0).toString(16)}`);
    debugParts.push(`epoll_wait_count=${exports.debug_epoll_wait_count?.() ?? "n/a"}`);
    debugParts.push(`epoll_wait_result=${exports.debug_epoll_wait_result?.().toString() ?? "n/a"}`);
    debugParts.push(`epoll_wait_reason=${epollWaitReason}`);
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
  const fdTraceLen = exports.debug_fd_syscall_trace_len?.() ?? 0;
  if (fdTraceLen > 0) {
    const traces: string[] = [];
    for (let index = 0; index < fdTraceLen; index += 1) {
      const nr = readOptionalTraceU64(exports.debug_fd_syscall_trace_nr?.(index));
      if (nr === 0n) {
        continue;
      }
      const arg0 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg0?.(index));
      const arg0OfdId = exports.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0;
      const arg0OfdType = exports.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0;
      const arg1 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg1?.(index));
      const arg2 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg2?.(index));
      const arg2OfdId = exports.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? 0;
      const arg2OfdType = exports.debug_fd_syscall_trace_arg2_ofd_type?.(index) ?? 0;
      const arg3 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg3?.(index));
      const result = readOptionalTraceU64(exports.debug_fd_syscall_trace_result?.(index));
      const status = exports.debug_fd_syscall_trace_status?.(index) ?? 0;
      const traceVmPtr = exports.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
      traces.push(
        `0x${traceVmPtr.toString(16)}:${nr.toString()}(${arg0.toString()}[${arg0OfdId}/${arg0OfdType}],${arg1.toString()},${arg2.toString()}[${arg2OfdId}/${arg2OfdType}],${arg3.toString()})=`
        + `${BigInt.asIntN(64, result).toString()}/${status}`,
      );
    }
    if (traces.length > 0) {
      debugParts.push(`fd_syscalls=[${traces.join(",")}]`);
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
  const threadCountTraceLen = exports.debug_thread_count_trace_len?.() ?? 0;
  if (threadCountTraceLen > 0) {
    const traces: string[] = [];
    for (let index = 0; index < threadCountTraceLen; index += 1) {
      const kind = exports.debug_thread_count_trace_kind?.(index) ?? 0;
      if (kind === 0) {
        continue;
      }
      const traceVmPtr = exports.debug_thread_count_trace_vm_ptr?.(index) ?? 0;
      const traceTid = exports.debug_thread_count_trace_tid?.(index) ?? 0;
      const traceProcessPtr = exports.debug_thread_count_trace_process_ptr?.(index) ?? 0;
      const before = exports.debug_thread_count_trace_before?.(index) ?? 0;
      const after = exports.debug_thread_count_trace_after?.(index) ?? 0;
      traces.push(
        `${kind}:${traceTid}@0x${traceVmPtr.toString(16)}/0x${traceProcessPtr.toString(16)}:${before}->${after}`,
      );
    }
    if (traces.length > 0) {
      debugParts.push(`thread_counts=[${traces.join(",")}]`);
    }
  }
  const trackedEpollState = formatTrackedEpollState(exports, vmPtr);
  if (trackedEpollState !== null) {
    debugParts.push(`epoll_state=[${trackedEpollState}]`);
  }
  const processThreads = formatProcessThreads(exports, vmPtr);
  if (processThreads !== null) {
    debugParts.push(`process_slots=[${processThreads}]`);
  }
  const processFds = formatProcessFds(exports, vmPtr);
  if (processFds !== null) {
    debugParts.push(`process_fds=[${processFds}]`);
  }
  const zeroRegPc = exports.debug_zero_reg_for_vm_pc?.(vmPtr) ?? exports.debug_zero_reg_pc?.();
  if (zeroRegPc !== undefined && zeroRegPc !== 0n) {
    debugParts.push(`zero_reg_tid=${exports.debug_zero_reg_for_vm_tid?.(vmPtr) ?? exports.debug_zero_reg_tid?.() ?? "n/a"}`);
    debugParts.push(`zero_reg_pc=0x${formatU64(zeroRegPc)}`);
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
  const tpWritePc = exports.debug_tp_write_pc?.();
  if (tpWritePc !== undefined && tpWritePc !== 0n) {
    debugParts.push(`tp_write_pc=0x${formatU64(tpWritePc)}`);
    debugParts.push(`tp_write_raw=0x${(exports.debug_tp_write_raw?.() ?? 0).toString(16)}`);
    const tpWriteInsn = readGuestInsn(exports, guestBase, tpWritePc);
    if (tpWriteInsn !== null) {
      debugParts.push(`tp_write_insn=${tpWriteInsn}`);
    }
    debugParts.push(`tp_write_src=${exports.debug_tp_write_source?.() ?? "n/a"}`);
    debugParts.push(`tp_write_prev=0x${formatU64(exports.debug_tp_write_prev_value?.() ?? 0n)}`);
    debugParts.push(`tp_write=0x${formatU64(exports.debug_tp_write_value?.() ?? 0n)}`);
    debugParts.push(`tp_write_rs1=${exports.debug_tp_write_rs1?.() ?? "n/a"}`);
    debugParts.push(`tp_write_rs1v=0x${formatU64(exports.debug_tp_write_rs1_value?.() ?? 0n)}`);
  }
  const spWritePc = exports.debug_sp_write_for_vm_pc?.(vmPtr) ?? exports.debug_sp_write_pc?.();
  if (spWritePc !== undefined && spWritePc !== 0n) {
    debugParts.push(`sp_write_tid=${exports.debug_sp_write_for_vm_tid?.(vmPtr) ?? exports.debug_sp_write_tid?.() ?? "n/a"}`);
    debugParts.push(`sp_write_pc=0x${formatU64(spWritePc)}`);
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
  if (threadStepTrace.tid === 0 && threadStepTrace.pc === 0n && threadStepTrace.ra === 0n && threadStepTrace.sp === 0n && threadStepTrace.s11 === 0n) {
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
  const syncSignalTrace = readSyncSignalTraceSnapshot(exports, vmPtr);
  if (syncSignalTrace.tid !== 0) {
    debugParts.push(`sync_signal_tid=${syncSignalTrace.tid}`);
    debugParts.push(`sync_signal_local_pc=0x${formatU64(syncSignalTrace.localPc)}`);
    debugParts.push(`sync_signal_local_ra=0x${formatU64(syncSignalTrace.localRa)}`);
    debugParts.push(`sync_signal_local_sp=0x${formatU64(syncSignalTrace.localSp)}`);
    debugParts.push(`sync_signal_local_s11=0x${formatU64(syncSignalTrace.localS11)}`);
    debugParts.push(`sync_signal_local_a0=0x${formatU64(syncSignalTrace.localA0)}`);
    debugParts.push(`sync_signal_vm_pc=0x${formatU64(syncSignalTrace.vmPc)}`);
    debugParts.push(`sync_signal_vm_ra=0x${formatU64(syncSignalTrace.vmRa)}`);
    debugParts.push(`sync_signal_vm_sp=0x${formatU64(syncSignalTrace.vmSp)}`);
    debugParts.push(`sync_signal_vm_s11=0x${formatU64(syncSignalTrace.vmS11)}`);
    debugParts.push(`sync_signal_vm_a0=0x${formatU64(syncSignalTrace.vmA0)}`);
    debugParts.push(`sync_signal_addr=0x${formatU64(syncSignalTrace.faultAddr)}`);
    debugParts.push(`sync_signal_sig_pending=0x${formatU64(syncSignalTrace.sigPending)}`);
    debugParts.push(`sync_signal_sig_mask=0x${formatU64(syncSignalTrace.sigMask)}`);
    debugParts.push(`sync_signal_sig_info_code=${syncSignalTrace.sigInfoCode}`);
  }
  const futexResumePhase = exports.debug_futex_resume_for_vm_phase?.(vmPtr) ?? 0;
  if (futexResumePhase !== 0) {
    debugParts.push(`futex_resume_tid=${exports.debug_futex_resume_for_vm_tid?.(vmPtr) ?? 0}`);
    debugParts.push(`futex_resume_phase=${futexResumePhase}`);
    debugParts.push(`futex_resume_status=${exports.debug_futex_resume_for_vm_status?.(vmPtr) ?? 0}`);
    debugParts.push(`futex_resume_pc=0x${formatU64(exports.debug_futex_resume_for_vm_pc?.(vmPtr) ?? 0n)}`);
    debugParts.push(`futex_resume_ra=0x${formatU64(exports.debug_futex_resume_for_vm_ra?.(vmPtr) ?? 0n)}`);
    debugParts.push(`futex_resume_sp=0x${formatU64(exports.debug_futex_resume_for_vm_sp?.(vmPtr) ?? 0n)}`);
    debugParts.push(`futex_resume_s11=0x${formatU64(exports.debug_futex_resume_for_vm_s11?.(vmPtr) ?? 0n)}`);
    debugParts.push(`futex_resume_a0=0x${formatU64(exports.debug_futex_resume_for_vm_a0?.(vmPtr) ?? 0n)}`);
    debugParts.push(`futex_resume_sig_pending=0x${formatU64(exports.debug_futex_resume_for_vm_sig_pending?.(vmPtr) ?? 0n)}`);
    debugParts.push(`futex_resume_sig_mask=0x${formatU64(exports.debug_futex_resume_for_vm_sig_mask?.(vmPtr) ?? 0n)}`);
  }
  const jalrPc = exports.debug_jalr_for_vm_pc?.(vmPtr) ?? exports.debug_jalr_pc?.();
  if (jalrPc !== undefined && jalrPc !== 0n) {
    debugParts.push(`jalr_pc=0x${formatU64(jalrPc)}`);
    debugParts.push(`jalr_rs1=${exports.debug_jalr_for_vm_rs1?.(vmPtr) ?? exports.debug_jalr_rs1?.() ?? "n/a"}`);
    debugParts.push(`jalr_rd=${exports.debug_jalr_for_vm_rd?.(vmPtr) ?? exports.debug_jalr_rd?.() ?? "n/a"}`);
    debugParts.push(`jalr_rs1v=0x${formatU64(exports.debug_jalr_for_vm_rs1_value?.(vmPtr) ?? exports.debug_jalr_rs1_value?.() ?? 0n)}`);
    debugParts.push(`jalr_target=0x${formatU64(exports.debug_jalr_for_vm_target?.(vmPtr) ?? exports.debug_jalr_target?.() ?? 0n)}`);
  }
  const execveStage = exports.debug_execve_stage?.();
  if (execveStage !== undefined && execveStage !== 0) {
    debugParts.push(`execve_stage=${execveStage}`);
    debugParts.push(`execve_v0=${exports.debug_execve_value0?.().toString() ?? "n/a"}`);
    debugParts.push(`execve_v1=${exports.debug_execve_value1?.().toString() ?? "n/a"}`);
    const execveV0Bytes = formatGuestBytesForDebug(
      exports,
      guestBase,
      readOptionalTraceU64(exports.debug_execve_value0?.()),
      32,
    );
    if (execveV0Bytes !== null) {
      debugParts.push(`execve_v0_bytes=${execveV0Bytes}`);
    }
  }
  const execveEcallValue0 = exports.debug_execve_ecall_value0?.();
  if (execveEcallValue0 !== undefined && execveEcallValue0 !== 0n) {
    const execveEcallValue1 = exports.debug_execve_ecall_value1?.() ?? 0n;
    debugParts.push(`execve_ecall_v0=${execveEcallValue0.toString()}`);
    debugParts.push(`execve_ecall_v1=${execveEcallValue1.toString()}`);
    const execveEcallBytes = formatGuestBytesForDebug(exports, guestBase, execveEcallValue0, 16);
    if (execveEcallBytes !== null) {
      debugParts.push(`execve_ecall_v0_bytes=${execveEcallBytes}`);
    }
  }
  const execvePreValue0 = exports.debug_execve_pre_value0?.();
  if (execvePreValue0 !== undefined && execvePreValue0 !== 0n) {
    const execvePreValue1 = exports.debug_execve_pre_value1?.() ?? 0n;
    debugParts.push(`execve_pre_pc=0x${formatU64(exports.debug_execve_pre_pc?.() ?? 0n)}`);
    debugParts.push(`execve_pre_raw=0x${(exports.debug_execve_pre_raw?.() ?? 0).toString(16)}`);
    debugParts.push(`execve_pre_v0=${execvePreValue0.toString()}`);
    debugParts.push(`execve_pre_v1=${execvePreValue1.toString()}`);
    const execvePreBytes = formatGuestBytesForDebug(exports, guestBase, execvePreValue0, 16);
    if (execvePreBytes !== null) {
      debugParts.push(`execve_pre_v0_bytes=${execvePreBytes}`);
    }
  }
  const execveApproachTraceLen = exports.debug_execve_approach_trace_len?.() ?? 0;
  if (execveApproachTraceLen > 0) {
    const traces: string[] = [];
    for (let index = 0; index < execveApproachTraceLen; index += 1) {
      const pc = exports.debug_execve_approach_trace_pc?.(index) ?? 0n;
      const raw = exports.debug_execve_approach_trace_raw?.(index) ?? 0;
      const pidSample0 = exports.debug_execve_approach_trace_pid_sample0?.(index) ?? 0n;
      const pidSample1 = exports.debug_execve_approach_trace_pid_sample1?.(index) ?? 0n;
      const pidSampleBytes = new Uint8Array(16);
      new DataView(pidSampleBytes.buffer).setBigUint64(0, pidSample0, true);
      new DataView(pidSampleBytes.buffer).setBigUint64(8, pidSample1, true);
      traces.push(
        `0x${formatU64(pc)}:0x${raw.toString(16)}:${[...pidSampleBytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`,
      );
    }
    debugParts.push(`execve_approach_trace=[${traces.join(",")}]`);
  }
  const execveDispatchTraceLen = exports.debug_execve_dispatch_trace_len?.() ?? 0;
  if (execveDispatchTraceLen > 0) {
    const traces: string[] = [];
    for (let index = 0; index < execveDispatchTraceLen; index += 1) {
      const stage = exports.debug_execve_dispatch_trace_stage?.(index) ?? 0;
      const pidSample0 = exports.debug_execve_dispatch_trace_pid_sample0?.(index) ?? 0n;
      const pidSample1 = exports.debug_execve_dispatch_trace_pid_sample1?.(index) ?? 0n;
      const pidSampleBytes = new Uint8Array(16);
      new DataView(pidSampleBytes.buffer).setBigUint64(0, pidSample0, true);
      new DataView(pidSampleBytes.buffer).setBigUint64(8, pidSample1, true);
      traces.push(
        `${stage}:${[...pidSampleBytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`,
      );
    }
    debugParts.push(`execve_dispatch_trace=[${traces.join(",")}]`);
  }
  const execveStageTraceLen = exports.debug_execve_stage_trace_len?.() ?? 0;
  if (execveStageTraceLen > 0) {
    const traces: string[] = [];
    for (let index = 0; index < execveStageTraceLen; index += 1) {
      const stage = exports.debug_execve_stage_trace_stage?.(index) ?? 0;
      const value0 = readOptionalTraceU64(exports.debug_execve_stage_trace_value0?.(index));
      const value1 = readOptionalTraceU64(exports.debug_execve_stage_trace_value1?.(index));
      const value0Bytes = formatGuestBytesForDebug(exports, guestBase, value0, 16);
      traces.push(
        `${stage}:0x${formatU64(value0)}/0x${formatU64(value1)}${value0Bytes !== null ? `:${value0Bytes}` : ""}`,
      );
    }
    if (traces.length > 0) {
      debugParts.push(`execve_trace=[${traces.join(",")}]`);
    }
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
