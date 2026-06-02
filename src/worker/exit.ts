import { settleInheritedChildOutput, settleVisibleProcessOutput } from "./stdio.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import type { ProcessHandle } from "./types.js";
import type { SyncCause } from "./contracts/sync.js";

interface WorkerExitDeps {
  processHandles: Map<number, ProcessHandle>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  captureRuntimeFailureSnapshot(handle: ProcessHandle, exitCode: number, termSignal: number): void;
  debugRuntimeProcessHandles(runtimeId: number): string;
  drainProcessOutput(handle: ProcessHandle): boolean;
  drainHiddenProcessOutput(handle: ProcessHandle): boolean;
  destroyProcessHandle(handle: ProcessHandle, signal: number, notifyExit: boolean): void;
  formatAllLiveThreadDebug(handle: ProcessHandle): string;
  kickBlockedWorkers(handle: ProcessHandle): void;
  publishRuntimeFsState?(runtimeId: number, entries: FsSnapshotEntry[]): number;
  recordChildExitViaKernelWorker(parent: ProcessHandle, childPid: number, waitStatus: number): Promise<void>;
  releaseSharedHostStdio(handle: ProcessHandle): Promise<void>;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  snapshotProcessFs?(handle: ProcessHandle): FsSnapshotEntry[];
  syncForkSharedStateViaKernelWorker?(
    handle: ProcessHandle,
    targets: ProcessHandle[],
      options: {
        cause: SyncCause;
        includeMemfs?: boolean;
        memfsOnly?: boolean;
        trackHeldOfds?: boolean;
        preserveSourceRefcounts?: boolean;
        preservePublishedRegularFileData?: boolean;
        sourceVmPtr?: number;
      },
  ): Promise<void>;
  unregisterProcessWithKernelWorker(handle: ProcessHandle): Promise<void>;
}

export function createWorkerExitHelpers(deps: WorkerExitDeps) {
  const unregisteredKernelWorkerHandles = new WeakSet<ProcessHandle>();
  const exitCompletionPromises = new WeakMap<ProcessHandle, Promise<void>>();
  const parentWaitStatusPrepublishedHandles = new WeakSet<ProcessHandle>();

  function readAbsoluteCString(handle: ProcessHandle, ptr: number): string {
    if (ptr <= 0 || ptr >= handle.memory.buffer.byteLength) {
      return "";
    }
    const bytes = new Uint8Array(handle.memory.buffer);
    const out: number[] = [];
    for (let index = ptr; index < bytes.length && out.length < 256; index += 1) {
      const value = bytes[index];
      if (value === 0) {
        break;
      }
      out.push(value);
    }
    return new TextDecoder().decode(new Uint8Array(out));
  }

  function formatExecveStageTrace(handle: ProcessHandle): string {
    const len = handle.kernel.debug_execve_stage_trace_len?.() ?? 0;
    if (len <= 0) {
      return "";
    }
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const stage = handle.kernel.debug_execve_stage_trace_stage?.(index) ?? 0;
      const value0 = handle.kernel.debug_execve_stage_trace_value0?.(index) ?? 0n;
      const value1 = handle.kernel.debug_execve_stage_trace_value1?.(index) ?? 0n;
      entries.push(`${stage}:${value0.toString(16)}:${value1.toString(16)}`);
    }
    return entries.join(",");
  }

  function formatRiscvSyscalls(handle: ProcessHandle): string {
    const len = handle.kernel.debug_riscv_syscall_trace_len?.() ?? 0;
    if (len <= 0) {
      return "";
    }
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const vmPtr = handle.kernel.debug_riscv_syscall_trace_entry_vm_ptr?.(index) ?? 0;
      if (vmPtr !== handle.vmPtr) {
        continue;
      }
      const nr = handle.kernel.debug_riscv_syscall_trace_entry_nr?.(index);
      const result = handle.kernel.debug_riscv_syscall_trace_entry_result?.(index);
      const status = handle.kernel.debug_riscv_syscall_trace_entry_status?.(index) ?? 0;
      const arg0 = handle.kernel.debug_riscv_syscall_trace_entry_arg0?.(index);
      const arg1 = handle.kernel.debug_riscv_syscall_trace_entry_arg1?.(index);
      entries.push(
        `${index}:${nr?.toString() ?? "n/a"}:${arg0?.toString() ?? "n/a"}:${arg1?.toString() ?? "n/a"}:${status}:${result?.toString() ?? "n/a"}`,
      );
    }
    return entries.slice(-16).join(",");
  }

  function formatPathSyscalls(handle: ProcessHandle): string {
    const len = handle.kernel.debug_path_syscall_trace_len?.() ?? 0;
    if (len <= 0) {
      return "";
    }
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const vmPtr = handle.kernel.debug_path_syscall_trace_vm_ptr?.(index) ?? 0;
      if (vmPtr !== handle.vmPtr) {
        continue;
      }
      const nr = handle.kernel.debug_path_syscall_trace_nr?.(index);
      if (Number(nr ?? -1n) !== 221) {
        continue;
      }
      const status = handle.kernel.debug_path_syscall_trace_status?.(index) ?? 0;
      const result = handle.kernel.debug_path_syscall_trace_result?.(index);
      const pathPtr = handle.kernel.debug_path_syscall_trace_path_ptr?.(index) ?? 0;
      entries.push(
        `${index}:${nr?.toString() ?? "n/a"}:${status}:${result?.toString() ?? "n/a"}:${JSON.stringify(readAbsoluteCString(handle, pathPtr))}`,
      );
    }
    return entries.slice(-64).join(",");
  }

  function formatAllPathSyscalls(handle: ProcessHandle): string {
    const len = handle.kernel.debug_path_syscall_trace_len?.() ?? 0;
    if (len <= 0) {
      return "";
    }
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const vmPtr = handle.kernel.debug_path_syscall_trace_vm_ptr?.(index) ?? 0;
      const nr = handle.kernel.debug_path_syscall_trace_nr?.(index);
      const status = handle.kernel.debug_path_syscall_trace_status?.(index) ?? 0;
      const result = handle.kernel.debug_path_syscall_trace_result?.(index);
      const pathPtr = handle.kernel.debug_path_syscall_trace_path_ptr?.(index) ?? 0;
      entries.push(
        `${index}:vm=0x${vmPtr.toString(16)}:${nr?.toString() ?? "n/a"}:${status}:${result?.toString() ?? "n/a"}:${JSON.stringify(readAbsoluteCString(handle, pathPtr))}`,
      );
    }
    return entries.slice(-96).join(",");
  }

  function formatAllRiscvSyscalls(handle: ProcessHandle): string {
    const len = handle.kernel.debug_riscv_syscall_trace_len?.() ?? 0;
    if (len <= 0) {
      return "";
    }
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const vmPtr = handle.kernel.debug_riscv_syscall_trace_entry_vm_ptr?.(index) ?? 0;
      const nr = handle.kernel.debug_riscv_syscall_trace_entry_nr?.(index);
      const result = handle.kernel.debug_riscv_syscall_trace_entry_result?.(index);
      const status = handle.kernel.debug_riscv_syscall_trace_entry_status?.(index) ?? 0;
      const arg0 = handle.kernel.debug_riscv_syscall_trace_entry_arg0?.(index);
      const arg1 = handle.kernel.debug_riscv_syscall_trace_entry_arg1?.(index);
      entries.push(
        `${index}:vm=0x${vmPtr.toString(16)}:${nr?.toString() ?? "n/a"}:${arg0?.toString() ?? "n/a"}:${arg1?.toString() ?? "n/a"}:${status}:${result?.toString() ?? "n/a"}`,
      );
    }
    return entries.slice(-48).join(",");
  }

  function formatTraceU64(value: bigint | number | undefined): string {
    return BigInt.asUintN(64, BigInt(value ?? 0)).toString(16);
  }

  function formatFrozenExecTrace(
    handle: ProcessHandle,
    len: () => number,
    entry: (index: number) => {
      pc?: bigint;
      raw?: number;
      step?: number;
      sp?: bigint;
      ra?: bigint;
      a0?: bigint;
      s11?: bigint;
    },
  ): string {
    const count = len();
    if (count <= 0) {
      return "";
    }
    const entries: string[] = [];
    for (let index = 0; index < count; index += 1) {
      const item = entry(index);
      entries.push(
        `${index}:pc=0x${formatTraceU64(item.pc)}`
        + `:raw=0x${(item.raw ?? 0).toString(16)}`
        + `:step=${item.step ?? 0}`
        + `:sp=0x${formatTraceU64(item.sp)}`
        + `:ra=0x${formatTraceU64(item.ra)}`
        + `:a0=0x${formatTraceU64(item.a0)}`
        + `:s11=0x${formatTraceU64(item.s11)}`,
      );
    }
    return entries.join(",");
  }

  function formatLiveExecTraceForVm(handle: ProcessHandle, vmPtr: number): string {
    const len = handle.kernel.debug_exec_trace_len?.() ?? 0;
    if (len <= 0) {
      return "";
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
      entries.push(
        `${index}:pc=0x${formatTraceU64(handle.kernel.debug_exec_trace_entry_pc?.(index))}`
        + `:raw=0x${(handle.kernel.debug_exec_trace_entry_raw?.(index) ?? 0).toString(16)}`
        + `:step=${handle.kernel.debug_exec_trace_entry_step?.(index) ?? 0}`
        + `:sp=0x${formatTraceU64(handle.kernel.debug_exec_trace_entry_sp?.(index))}`
        + `:ra=0x${formatTraceU64(handle.kernel.debug_exec_trace_entry_ra?.(index))}`
        + `:a0=0x${formatTraceU64(handle.kernel.debug_exec_trace_entry_a0?.(index))}`
        + `:s11=0x${formatTraceU64(handle.kernel.debug_exec_trace_entry_s11?.(index))}`,
      );
    }
    return entries.slice(-32).join(",");
  }

  function safeFrozenExecTrace(label: string, read: () => string): string {
    try {
      const trace = read();
      return trace.length > 0 ? `${label}=[${trace}]` : "";
    } catch (error) {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      return `${label}_error=${JSON.stringify(detail)}`;
    }
  }

  function formatFrozenExitTraces(handle: ProcessHandle, vmPtr = handle.vmPtr): string {
    return [
      safeFrozenExecTrace(
        "thread_step_exec_trace",
        () => formatFrozenExecTrace(
          handle,
          () => handle.kernel.debug_thread_step_exec_trace_len_for_vm?.(vmPtr) ?? 0,
          (index) => ({
            pc: handle.kernel.debug_thread_step_exec_trace_entry_pc_for_vm?.(vmPtr, index),
            raw: handle.kernel.debug_thread_step_exec_trace_entry_raw_for_vm?.(vmPtr, index),
            step: handle.kernel.debug_thread_step_exec_trace_entry_step_for_vm?.(vmPtr, index),
            sp: handle.kernel.debug_thread_step_exec_trace_entry_sp_for_vm?.(vmPtr, index),
            ra: handle.kernel.debug_thread_step_exec_trace_entry_ra_for_vm?.(vmPtr, index),
            a0: handle.kernel.debug_thread_step_exec_trace_entry_a0_for_vm?.(vmPtr, index),
            s11: handle.kernel.debug_thread_step_exec_trace_entry_s11_for_vm?.(vmPtr, index),
          }),
        ),
      ),
      safeFrozenExecTrace(
        "sync_fault_exec_trace",
        () => formatFrozenExecTrace(
          handle,
          () => handle.kernel.debug_sync_fault_exec_trace_len_for_vm?.(vmPtr) ?? 0,
          (index) => ({
            pc: handle.kernel.debug_sync_fault_exec_trace_entry_pc_for_vm?.(vmPtr, index),
            raw: handle.kernel.debug_sync_fault_exec_trace_entry_raw_for_vm?.(vmPtr, index),
            step: handle.kernel.debug_sync_fault_exec_trace_entry_step_for_vm?.(vmPtr, index),
            sp: handle.kernel.debug_sync_fault_exec_trace_entry_sp_for_vm?.(vmPtr, index),
            ra: handle.kernel.debug_sync_fault_exec_trace_entry_ra_for_vm?.(vmPtr, index),
            a0: handle.kernel.debug_sync_fault_exec_trace_entry_a0_for_vm?.(vmPtr, index),
            s11: handle.kernel.debug_sync_fault_exec_trace_entry_s11_for_vm?.(vmPtr, index),
          }),
        ),
      ),
      safeFrozenExecTrace(
        "break_exec_trace",
        () => formatFrozenExecTrace(
          handle,
          () => handle.kernel.debug_break_exec_trace_len_for_vm?.(vmPtr) ?? 0,
          (index) => ({
            pc: handle.kernel.debug_break_exec_trace_entry_pc_for_vm?.(vmPtr, index),
            raw: handle.kernel.debug_break_exec_trace_entry_raw_for_vm?.(vmPtr, index),
            step: handle.kernel.debug_break_exec_trace_entry_step_for_vm?.(vmPtr, index),
            sp: handle.kernel.debug_break_exec_trace_entry_sp_for_vm?.(vmPtr, index),
            ra: handle.kernel.debug_break_exec_trace_entry_ra_for_vm?.(vmPtr, index),
            a0: handle.kernel.debug_break_exec_trace_entry_a0_for_vm?.(vmPtr, index),
            s11: handle.kernel.debug_break_exec_trace_entry_s11_for_vm?.(vmPtr, index),
          }),
        ),
      ),
      safeFrozenExecTrace(
        "exec_trace",
        () => formatLiveExecTraceForVm(handle, vmPtr),
      ),
    ].filter((entry) => entry.length > 0).join(" ");
  }

  function formatFdSyscalls(
    handle: ProcessHandle,
    pidFilter: number | null = null,
    limit = 256,
    vmPtrFilter = handle.vmPtr,
  ): string {
    const len = handle.kernel.debug_fd_syscall_trace_len?.() ?? 0;
    if (len <= 0) {
      return "";
    }
    const sampleToPid = (sample: bigint | number | undefined): number => {
      if (sample === undefined) {
        return 0;
      }
      return Number(BigInt.asIntN(32, BigInt(sample) & 0xffffffffn));
    };
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const vmPtr = handle.kernel.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
      if (vmPtr !== vmPtrFilter) {
        continue;
      }
      const nr = Number(handle.kernel.debug_fd_syscall_trace_nr?.(index) ?? -1n);
      if (![24, 25, 56, 57, 59, 62, 63, 64, 66, 67, 68, 70, 198, 199, 203, 242, 436].includes(nr)) {
        continue;
      }
      const arg0OfdType = handle.kernel.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0;
      const arg2OfdType = handle.kernel.debug_fd_syscall_trace_arg2_ofd_type?.(index) ?? 0;
      const pid = sampleToPid(handle.kernel.debug_fd_syscall_trace_pid_sample0?.(index));
      if (pidFilter !== null && pid !== pidFilter) {
        continue;
      }
      entries.push([
        `${index}:${nr}`,
        `pid=${pid}`,
        `a0=${handle.kernel.debug_fd_syscall_trace_arg0?.(index)?.toString() ?? "n/a"}`,
        `a0ofd=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? "n/a"}`,
        `a0ty=${arg0OfdType}`,
        `a0flags=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_status_flags?.(index) ?? "n/a"}`,
        `a0ino=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_ino?.(index) ?? "n/a"}`,
        `a0mode=${handle.kernel.debug_fd_syscall_trace_arg0_inode_mode?.(index) ?? "n/a"}`,
        `a1=${handle.kernel.debug_fd_syscall_trace_arg1?.(index)?.toString() ?? "n/a"}`,
        `a2=${handle.kernel.debug_fd_syscall_trace_arg2?.(index)?.toString() ?? "n/a"}`,
        `a2ofd=${handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? "n/a"}`,
        `a2ty=${arg2OfdType}`,
        `ret=${handle.kernel.debug_fd_syscall_trace_result?.(index)?.toString() ?? "n/a"}`,
        `st=${handle.kernel.debug_fd_syscall_trace_status?.(index) ?? "n/a"}`,
      ].join(":"));
    }
    return entries.slice(-limit).join(",");
  }

  function formatChildExitSyncRecords(handle: ProcessHandle): string {
    return [...handle.childExitSyncRecords]
      .map(([childPid, waitStatus]) => `${childPid}/${waitStatus}`)
      .join(".") || "none";
  }

  function boundedChildDebugMessage(handle: ProcessHandle, limit = 8192): string {
    const message = handle.lastDebugMessage ?? "debug=n/a";
    return message.length <= limit ? message : `${message.slice(0, limit)}...<truncated:${message.length}>`;
  }

  function formatMemorySyscalls(handle: ProcessHandle): string {
    const len = handle.kernel.debug_memory_syscall_trace_len?.() ?? 0;
    if (len <= 0) {
      return "";
    }
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const pid = handle.kernel.debug_memory_syscall_trace_entry_pid?.(index) ?? 0;
      if (pid !== handle.pid) {
        continue;
      }
      entries.push([
        `${index}:${handle.kernel.debug_memory_syscall_trace_entry_nr?.(index)?.toString() ?? "n/a"}`,
        `a0=${handle.kernel.debug_memory_syscall_trace_entry_arg0?.(index)?.toString() ?? "n/a"}`,
        `a1=${handle.kernel.debug_memory_syscall_trace_entry_arg1?.(index)?.toString() ?? "n/a"}`,
        `a2=${handle.kernel.debug_memory_syscall_trace_entry_arg2?.(index)?.toString() ?? "n/a"}`,
        `a3=${handle.kernel.debug_memory_syscall_trace_entry_arg3?.(index)?.toString() ?? "n/a"}`,
        `a4=${handle.kernel.debug_memory_syscall_trace_entry_arg4?.(index)?.toString() ?? "n/a"}`,
        `a5=${handle.kernel.debug_memory_syscall_trace_entry_arg5?.(index)?.toString() ?? "n/a"}`,
        `ret=${handle.kernel.debug_memory_syscall_trace_entry_result?.(index)?.toString() ?? "n/a"}`,
      ].join(":"));
    }
    return entries.slice(-96).join(",");
  }

  function formatFaultState(handle: ProcessHandle, vmPtr = handle.vmPtr): string {
    return [
      `pc=0x${(handle.kernel.thread_pc?.(vmPtr) ?? 0n).toString(16)}`,
      `sp=0x${(handle.kernel.thread_sp?.(vmPtr) ?? 0n).toString(16)}`,
      `a0=${(handle.kernel.thread_a0?.(vmPtr) ?? 0n).toString()}`,
      `a1=${(handle.kernel.thread_a1?.(vmPtr) ?? 0n).toString()}`,
      `term_signal=${handle.kernel.thread_term_signal?.(vmPtr) ?? 0}`,
      `fault_pc=0x${(handle.kernel.thread_fault_pc?.(vmPtr) ?? 0n).toString(16)}`,
      `fault_addr=0x${(handle.kernel.thread_fault_addr?.(vmPtr) ?? 0n).toString(16)}`,
      `fault_kind=${handle.kernel.thread_fault_kind?.(vmPtr) ?? 0}`,
      `fault_signal=${handle.kernel.thread_fault_signal?.(vmPtr) ?? 0}`,
      `fault_code=${handle.kernel.thread_fault_code?.(vmPtr) ?? 0}`,
    ].join(":");
  }

  function formatAsciiLe(value: bigint): string {
    const chars: string[] = [];
    for (let index = 0; index < 8; index += 1) {
      const byte = Number((value >> BigInt(index * 8)) & 0xffn);
      chars.push(byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : ".");
    }
    return JSON.stringify(chars.join(""));
  }

  function classifyGuestAddress(
    value: bigint,
    guestAddressLimit: bigint,
    guestBackingBytes: bigint,
  ): string {
    if (value === 0n) {
      return "zero";
    }
    if (guestAddressLimit > 0n && value >= guestAddressLimit) {
      return "outside_guest_limit";
    }
    if (value >= guestBackingBytes) {
      return "outside_backing";
    }
    return "inside_backing";
  }

  function formatFaultBounds(handle: ProcessHandle, vmPtr = handle.vmPtr): string {
    const memoryBytes = BigInt(handle.memory.buffer.byteLength);
    const guestBase = BigInt(Math.max(0, handle.guestBase));
    const guestBackingBytes = memoryBytes > guestBase ? memoryBytes - guestBase : 0n;
    const guestAddressLimit = handle.kernel.thread_guest_address_limit?.(vmPtr) ?? 0n;
    const pc = handle.kernel.thread_pc?.(vmPtr) ?? 0n;
    const faultPc = handle.kernel.thread_fault_pc?.(vmPtr) ?? 0n;
    const faultAddr = handle.kernel.thread_fault_addr?.(vmPtr) ?? 0n;
    return [
      `mem_bytes=${memoryBytes.toString()}`,
      `guest_base=0x${guestBase.toString(16)}`,
      `guest_backing_bytes=${guestBackingBytes.toString()}`,
      `guest_address_limit=0x${guestAddressLimit.toString(16)}`,
      `pc=${classifyGuestAddress(pc, guestAddressLimit, guestBackingBytes)}`,
      `fault_pc=${classifyGuestAddress(faultPc, guestAddressLimit, guestBackingBytes)}`,
      `fault_addr=${classifyGuestAddress(faultAddr, guestAddressLimit, guestBackingBytes)}`,
      `pc_ascii_le=${formatAsciiLe(BigInt.asUintN(64, pc))}`,
      `fault_pc_ascii_le=${formatAsciiLe(BigInt.asUintN(64, faultPc))}`,
    ].join(":");
  }

  function formatDefaultSignalTrace(handle: ProcessHandle): string {
    const sig = handle.kernel.debug_default_signal_sig?.() ?? 0;
    if (sig === 0) {
      return "default_signal=[]";
    }
    return `default_signal=[${
      [
        `sig=${sig}`,
        `code=${handle.kernel.debug_default_signal_code?.() ?? 0}`,
        `vm=0x${(handle.kernel.debug_default_signal_vm_ptr?.() ?? 0).toString(16)}`,
        `pc=0x${(handle.kernel.debug_default_signal_pc?.() ?? 0n).toString(16)}`,
        `ra=0x${(handle.kernel.debug_default_signal_ra?.() ?? 0n).toString(16)}`,
        `sp=0x${(handle.kernel.debug_default_signal_sp?.() ?? 0n).toString(16)}`,
        `gp=0x${(handle.kernel.debug_default_signal_gp?.() ?? 0n).toString(16)}`,
        `tp=0x${(handle.kernel.debug_default_signal_tp?.() ?? 0n).toString(16)}`,
        `s11=0x${(handle.kernel.debug_default_signal_s11?.() ?? 0n).toString(16)}`,
        `fault_pc=0x${(handle.kernel.debug_default_signal_fault_pc?.() ?? 0n).toString(16)}`,
        `fault_addr=0x${(handle.kernel.debug_default_signal_fault_addr?.() ?? 0n).toString(16)}`,
      ].join(":")
    }]`;
  }

  function formatSignalState(handle: ProcessHandle, vmPtr = handle.vmPtr): string {
    const deliverySig = handle.kernel.debug_signal_delivery_for_vm_sig?.(vmPtr)
      ?? handle.kernel.debug_signal_delivery_sig?.()
      ?? 0;
    return `signal_state=[${
      [
        `mask=0x${(handle.kernel.thread_sig_mask?.(vmPtr) ?? 0n).toString(16)}`,
        `pending=0x${(handle.kernel.thread_sig_pending?.(vmPtr) ?? 0n).toString(16)}`,
        `info=${handle.kernel.thread_sig_info_code?.(vmPtr) ?? 0}`,
        `deliver_sig=${deliverySig}`,
        `deliver_frame=0x${(handle.kernel.debug_signal_delivery_for_vm_frame_base?.(vmPtr) ?? handle.kernel.debug_signal_delivery_frame_base?.() ?? 0n).toString(16)}`,
        `deliver_pc=0x${(handle.kernel.debug_signal_delivery_for_vm_pc?.(vmPtr) ?? handle.kernel.debug_signal_delivery_pc?.() ?? 0n).toString(16)}`,
        `deliver_ra=0x${(handle.kernel.debug_signal_delivery_for_vm_ra?.(vmPtr) ?? handle.kernel.debug_signal_delivery_ra?.() ?? 0n).toString(16)}`,
        `deliver_sp=0x${(handle.kernel.debug_signal_delivery_for_vm_sp?.(vmPtr) ?? handle.kernel.debug_signal_delivery_sp?.() ?? 0n).toString(16)}`,
      ].join(":")
    }]`;
  }

  function formatExitTrace(handle: ProcessHandle, vmPtr = handle.vmPtr): string {
    const execveStage = handle.kernel.debug_execve_stage?.() ?? 0;
    const execveValue0 = handle.kernel.debug_execve_value0?.() ?? 0n;
    const execveValue1 = handle.kernel.debug_execve_value1?.() ?? 0n;
    return [
      `vm=0x${vmPtr.toString(16)}`,
      `fault_state=[${formatFaultState(handle, vmPtr)}]`,
      `fault_bounds=[${formatFaultBounds(handle, vmPtr)}]`,
      formatDefaultSignalTrace(handle),
      formatSignalState(handle, vmPtr),
      formatFrozenExitTraces(handle, vmPtr),
      `execve_stage=${execveStage}`,
      `execve_v0=${execveValue0.toString()}`,
      `execve_v1=${execveValue1.toString()}`,
      `execve_stage_trace=[${formatExecveStageTrace(handle)}]`,
      `exit_fds=[${formatFdSummary(handle)}]`,
      `riscv_syscalls=[${formatRiscvSyscalls(handle)}]`,
      `all_riscv_syscalls=[${formatAllRiscvSyscalls(handle)}]`,
      `path_syscalls=[${formatPathSyscalls(handle)}]`,
      `all_path_syscalls=[${formatAllPathSyscalls(handle)}]`,
      `fd_syscalls=[${formatFdSyscalls(handle, null, 256, vmPtr)}]`,
      `memory_syscalls=[${formatMemorySyscalls(handle)}]`,
    ].join(" ");
  }

  function safeDebugRuntimeProcessHandles(runtimeId: number): string {
    try {
      return deps.debugRuntimeProcessHandles(runtimeId);
    } catch (error) {
      return `unavailable:${error instanceof Error ? error.message : String(error)}`;
    }
  }

  function safeFormatAllLiveThreadDebug(handle: ProcessHandle): string {
    try {
      return deps.formatAllLiveThreadDebug(handle);
    } catch (error) {
      return `unavailable:${error instanceof Error ? error.message : String(error)}`;
    }
  }

  function encodeWaitStatus(exitCode: number, termSignal: number): number {
    if (termSignal !== 0) {
      return termSignal & 0x7f;
    }
    return (exitCode & 0xff) << 8;
  }

  function invalidateBlockedParentRpcsForChildExit(parent: ProcessHandle, childPid: number): void {
    for (const blocked of parent.blockedWorkers.values()) {
      if (!blocked.rpcPending) {
        continue;
      }
      if (
        blocked.status !== parent.kernel.abi_status_wait4()
        && blocked.status !== parent.kernel.abi_status_pipe_wait()
      ) {
        continue;
      }
      blocked.rpcPending = false;
      blocked.rpcGeneration += 1;
      blocked.pipeSyncedBeforePassive = false;
      deps.appendRuntimeLifecycleEvent(
        parent.runtimeId,
        `exit-parent-clear-blocked-rpc:${parent.pid}:${childPid}:status=${blocked.status}`,
      );
    }
  }

  function prepublishParentWaitStatus(
    handle: ProcessHandle,
    exitCode: number,
    termSignal: number,
  ): boolean {
    if (handle.parentPid === null || handle.pid <= 0) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `exit-parent-early-prepublish-skip:${handle.pid}:parent=${handle.parentPid ?? 0}`,
      );
      return false;
    }
    const parent = deps.processHandles.get(handle.parentPid);
    if (!parent) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `exit-parent-early-prepublish-skip:${handle.pid}:parent=${handle.parentPid}`,
      );
      return false;
    }
    if (parentWaitStatusPrepublishedHandles.has(handle)) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `exit-parent-early-prepublish-existing:${parent.pid}:${handle.pid}:pending=${parent.pendingChildExitRecords}:records=${formatChildExitSyncRecords(parent)}`,
      );
      return true;
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `exit-parent-early-prepublish-start:${parent.pid}:${handle.pid}:pending=${parent.pendingChildExitRecords}:records=${formatChildExitSyncRecords(parent)}`,
    );
    if (!parent.childExitSyncRecords.has(handle.pid)) {
      parent.pendingChildExitRecords += 1;
      parent.childExitSyncRecords.set(handle.pid, encodeWaitStatus(exitCode, termSignal));
      if (parent.pendingChildExitRecords > 0) {
        parent.pendingChildExitRecords -= 1;
      }
    }
    parentWaitStatusPrepublishedHandles.add(handle);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `exit-parent-early-prepublish-done:${parent.pid}:${handle.pid}:pending=${parent.pendingChildExitRecords}:records=${formatChildExitSyncRecords(parent)}`,
    );
    invalidateBlockedParentRpcsForChildExit(parent, handle.pid);
    deps.kickBlockedWorkers(parent);
    return true;
  }

  function fdPipeSlot(handle: ProcessHandle, fd: number): number {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (
      ofdId === 0
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      return 0;
    }
    return handle.kernel.kernel_ofd_ino(ofdId);
  }

  function formatFdSummary(handle: ProcessHandle, limit = 16): string {
    const entries: string[] = [];
    for (let fd = 0; fd < limit; fd += 1) {
      const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
      if (ofdId === 0) {
        continue;
      }
      entries.push([
        `${fd}->${ofdId}`,
        `flags=${handle.kernel.process_fd_flags(handle.vmPtr, fd)}`,
        `type=${handle.kernel.kernel_ofd_fd_type(ofdId)}`,
        `host=${handle.kernel.kernel_ofd_host_fd(ofdId)}`,
        `ino=${handle.kernel.kernel_ofd_ino(ofdId)}`,
        `status=${handle.kernel.kernel_ofd_status_flags(ofdId)}`,
      ].join(":"));
    }
    return entries.join(" ");
  }

  function usesInheritedParentOutput(handle: ProcessHandle, parent: ProcessHandle): boolean {
    if (handle.stdioOwner !== parent) {
      return false;
    }
    const inheritedSlots = new Set(
      [
        parent.hostStdio.stdoutPipe,
        parent.hostStdio.stderrPipe,
      ].filter((slot) => slot > 0),
    );
    const fdSlots = [fdPipeSlot(handle, 1), fdPipeSlot(handle, 2)]
      .filter((slot) => slot > 0);
    if (fdSlots.length > 0) {
      return fdSlots.some((slot) => inheritedSlots.has(slot));
    }
    return parent.stdioOwner === null
      && [handle.hostStdio.stdoutPipe, handle.hostStdio.stderrPipe]
        .some((slot) => slot > 0 && inheritedSlots.has(slot));
  }

  function formatVisibleOutputDebug(handle: ProcessHandle): string {
    const maxPipes = handle.kernel.abi_max_pipes?.() ?? 0;
    const regionBytes = handle.kernel.kernel_pipe_region_bytes?.() ?? 0;
    const slotBytes = maxPipes > 0 ? regionBytes / maxPipes : 0;
    if (!Number.isInteger(slotBytes) || slotBytes <= 32) {
      return "pipe-layout=invalid";
    }
    const view = new DataView(handle.memory.buffer);
    const memory = new Uint8Array(handle.memory.buffer);
    const formatSlot = (label: string, pipeSlot: number): string => {
      if (pipeSlot <= 0 || pipeSlot >= maxPipes) {
        return `${label}=0`;
      }
      const offset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
      const head = view.getUint32(offset, true);
      const tail = view.getUint32(offset + 4, true);
      const readOpen = view.getInt32(offset + 8, true);
      const writeOpen = view.getInt32(offset + 12, true);
      const unread = tail >= head ? tail - head : (slotBytes - 32) - head + tail;
      const previewStart = offset + 32 + head;
      const previewLen = Math.max(0, Math.min(96, unread, offset + slotBytes - previewStart));
      const preview = [...memory.subarray(previewStart, previewStart + previewLen)]
        .map((byte) => {
          if (byte === 10) {
            return "\\n";
          }
          if (byte >= 32 && byte <= 126 && byte !== 34 && byte !== 92) {
            return String.fromCharCode(byte);
          }
          return ".";
        })
        .join("");
      return `${label}=slot${pipeSlot}/h${head}/t${tail}/ro${readOpen}/wo${writeOpen}/u${unread}/d=${JSON.stringify(preview)}`;
    };
    return [
      `on=${handle.onStdout ? 1 : 0}`,
      `backlog=${handle.outputBacklog.length}`,
      formatSlot("stdout", handle.hostStdio.stdoutPipe),
      formatSlot("stderr", handle.hostStdio.stderrPipe),
    ].join(":");
  }

  async function publishProcessFsStateOnExit(handle: ProcessHandle, targets: ProcessHandle[] = []): Promise<void> {
    if (!deps.snapshotProcessFs || !deps.publishRuntimeFsState) {
      return;
    }
    if (!handle.processMemfsDirty && (!handle.kernelWorkerRequest || handle.pid <= 0)) {
      return;
    }
    try {
      if (handle.kernelWorkerRequest && handle.pid > 0) {
        await deps.syncForkSharedStateViaKernelWorker?.(handle, targets, {
          cause: { kind: "syscall", nr: -1n, name: "process-exit-memfs-publication" },
          includeMemfs: true,
          memfsOnly: true,
          preservePublishedRegularFileData: false,
          trackHeldOfds: false,
          sourceVmPtr: handle.vmPtr,
        });
      }
      const entries = deps.snapshotProcessFs(handle);
      deps.publishRuntimeFsState(handle.runtimeId, entries);
      handle.processMemfsDirty = false;
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `exit-fs-publish:${handle.pid}:${entries.length}`);
    } catch (error) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `exit-fs-publish-error:${handle.pid}:${JSON.stringify(error instanceof Error ? error.message : String(error))}`,
      );
    }
  }

  async function recordExitOnParent(
    handle: ProcessHandle,
    exitCode: number,
    termSignal: number,
  ): Promise<number | null> {
    if (handle.parentPid === null || handle.pid <= 0) {
      return null;
    }

    const parent = deps.processHandles.get(handle.parentPid);
    if (!parent) {
      return null;
    }

    const hasInheritedParentOutput = usesInheritedParentOutput(handle, parent);
    if (!hasInheritedParentOutput && handle.kernelWorkerRequest && handle.pid > 0) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `parent-pipe-sync-before-child-exit-start:${parent.pid}:${handle.pid}`);
      await deps.requestPipeStateSync(parent, true);
      deps.drainProcessOutput(parent);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `parent-pipe-sync-before-child-exit-done:${parent.pid}:${handle.pid}`);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-pipe-sync-start:${handle.pid}`);
      try {
        await deps.requestPipeStateSync(handle, true);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-pipe-sync-done:${handle.pid}`);
      } catch (error) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `child-pipe-sync-error:${handle.pid}:${JSON.stringify(error instanceof Error ? error.message : String(error))}`,
        );
      }
    }
    if (handle.stdioOwner === null) {
      deps.drainProcessOutput(handle);
    } else if (!hasInheritedParentOutput) {
      deps.drainHiddenProcessOutput(handle);
    }
    if (hasInheritedParentOutput) {
      await settleInheritedChildOutput(handle, deps.requestPipeStateSync);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-parent-pipe-refresh:${parent.pid}:${handle.pid}`);
      deps.kickBlockedWorkers(parent);
    }
    await publishProcessFsStateOnExit(handle, [parent]);
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-unregister-before-parent-record:${handle.pid}`);
      await deps.unregisterProcessWithKernelWorker(handle);
      unregisteredKernelWorkerHandles.add(handle);
      handle.kernelWorkerRequest = undefined;
    }

    deps.appendWorkerDebugLog(
      parent,
      `child-exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
    );
    const childDebug = boundedChildDebugMessage(handle);
    deps.appendWorkerDebugLog(parent, `child-debug:${handle.pid}:${childDebug}`);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `child-debug:${handle.pid}:${childDebug.slice(0, 4096)}`,
    );
    parent.lastDebugMessage = [
      `child-exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
      `child-debug:${handle.pid}:${childDebug}`,
      parent.lastDebugMessage ?? "debug=n/a",
    ].join(" ");

    const waitStatus = encodeWaitStatus(exitCode, termSignal);
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `parent-record-start:${parent.pid}:${handle.pid}`);
    await deps.recordChildExitViaKernelWorker(parent, handle.pid, waitStatus);
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `parent-record-done:${parent.pid}:${handle.pid}`);
    if (!hasInheritedParentOutput) {
      await settleVisibleProcessOutput(parent, deps.requestPipeStateSync);
    }
    return waitStatus;
  }

  function visibleInheritedChildren(handle: ProcessHandle): ProcessHandle[] {
    return [...deps.processHandles.values()].filter((child) => (
      child !== handle
      && child.stdioOwner === handle
      && child.pid > 0
      && !!child.kernelWorkerRequest
    ));
  }

  function formatVisibleChildState(child: ProcessHandle): string {
    const parts = [`${child.pid}`];
    try {
      parts.push(`status=${child.kernel.thread_status(child.vmPtr)}`);
      parts.push(`a7=${child.kernel.thread_a7?.(child.vmPtr)?.toString() ?? "n/a"}`);
      parts.push(`a0=${child.kernel.thread_a0(child.vmPtr).toString()}`);
      parts.push(`blocked_fd=${child.kernel.thread_blocked_fd(child.vmPtr)}`);
      parts.push(`blocked_ofd=${child.kernel.thread_blocked_ofd_id(child.vmPtr)}`);
      parts.push(`poll_ready=${child.kernel.thread_poll_wait_ready_count?.(child.vmPtr) ?? -1}`);
      parts.push(`poll_pipe=0x${(child.kernel.thread_poll_wait_first_pipe_revents?.(child.vmPtr) ?? 0).toString(16)}`);
      parts.push(`pipe_buf=${child.kernel.thread_blocked_pipe_buffered_len?.(child.vmPtr) ?? -1}`);
      parts.push(`pipe_wo=${child.kernel.thread_blocked_pipe_write_open?.(child.vmPtr) ?? -1}`);
      const fdParts: string[] = [];
      const maxFds = child.kernel.abi_max_fds();
      for (let fd = 0; fd < Math.min(maxFds, 6); fd += 1) {
        const ofdId = child.kernel.process_fd_ofd_id(child.vmPtr, fd);
        if (ofdId === 0) {
          continue;
        }
        const type = ofdId > 0 && ofdId < maxFds ? child.kernel.kernel_ofd_fd_type(ofdId) : -1;
        const ino = ofdId > 0 && ofdId < maxFds ? child.kernel.kernel_ofd_ino(ofdId) : -1;
        const hostFd = ofdId > 0 && ofdId < maxFds ? child.kernel.kernel_ofd_host_fd(ofdId) : -1;
        fdParts.push(`${fd}->${ofdId}/${type}/${ino}/${hostFd}`);
      }
      parts.push(`fds=${fdParts.join("_") || "none"}`);
    } catch (error) {
      parts.push(`state_error=${JSON.stringify(error instanceof Error ? error.message : String(error))}`);
    }
    parts.push(`running=${child.runningWorker === null ? 0 : 1}`);
    parts.push(`ready=${child.readyWorkers.length}`);
    parts.push(`blocked=${child.blockedWorkers.size}`);
    parts.push(`exit=${child.exitInProgress ? 1 : 0}`);
    return parts.join("/");
  }

  async function settleVisibleInheritedChildrenOutput(handle: ProcessHandle): Promise<void> {
    if (handle.stdioOwner !== null) {
      return;
    }
    let pass = 0;
    for (;;) {
      const children = visibleInheritedChildren(handle);
      if (children.length === 0) {
        break;
      }
      let active = false;
      for (const child of children) {
        if (pass === 0) {
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `visible-inherited-child-output-start:${handle.pid}:${child.pid}`);
        }
        await settleInheritedChildOutput(child, deps.requestPipeStateSync);
        if (pass === 0) {
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `visible-inherited-child-output-done:${handle.pid}:${child.pid}`);
        }
        const exitCompletionPromise = exitCompletionPromises.get(child);
        if (exitCompletionPromise) {
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `visible-inherited-child-exit-wait-start:${handle.pid}:${child.pid}`);
          try {
            await exitCompletionPromise;
          } finally {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `visible-inherited-child-exit-wait-done:${handle.pid}:${child.pid}`);
          }
          active = true;
          continue;
        }
        active ||= deps.processHandles.get(child.pid) === child && !child.exitInProgress;
      }
      if (!active) {
        break;
      }
      if (pass % 100 === 0) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `visible-inherited-child-live-wait:${handle.pid}:${children.map(formatVisibleChildState).join(".")}`,
        );
      }
      pass += 1;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  async function completeProcessExit(
    handle: ProcessHandle,
    exitCode: number,
    termSignal: number,
    exitVmPtr = handle.vmPtr,
  ): Promise<void> {
    const existingCompletionPromise = exitCompletionPromises.get(handle);
    if (existingCompletionPromise) {
      return existingCompletionPromise;
    }
    const completionPromise = completeProcessExitImpl(handle, exitCode, termSignal, exitVmPtr)
      .finally(() => {
        exitCompletionPromises.delete(handle);
      });
    exitCompletionPromises.set(handle, completionPromise);
    return completionPromise;
  }

  async function completeProcessExitImpl(
    handle: ProcessHandle,
    exitCode: number,
    termSignal: number,
    exitVmPtr: number,
  ): Promise<void> {
    handle.lastDebugMessage = [
      `exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
      `exit_trace=${formatExitTrace(handle, exitVmPtr)}`,
      `live_processes=${safeDebugRuntimeProcessHandles(handle.runtimeId)}`,
      `cmd=${JSON.stringify(handle.debugCommand)}`,
      `worker_log=[${handle.workerDebugLog.join(", ")}]`,
      `thread_debugs=[${safeFormatAllLiveThreadDebug(handle)}]`,
      handle.lastDebugMessage ?? "debug=n/a",
    ].join(" ");
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
    );
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `exit-fd-debug:${handle.pid}:${formatFdSyscalls(handle).slice(-4096)}`,
    );
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `exit-fd-debug-self:${handle.pid}:${formatFdSyscalls(handle, handle.pid, 512).slice(-12000)}`,
    );
    if (exitCode !== 0 || termSignal !== 0) {
      deps.captureRuntimeFailureSnapshot(handle, exitCode, termSignal);
    }

    const parent = handle.parentPid !== null ? (deps.processHandles.get(handle.parentPid) ?? null) : null;
    const hasInheritedParentOutput = parent !== null && usesInheritedParentOutput(handle, parent);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `exit-output-branch:${handle.pid}:parent=${parent?.pid ?? 0}:owner=${handle.stdioOwner?.pid ?? 0}`,
    );
    let parentWaitStatusPublished = false;
    if (parent) {
      if (parentWaitStatusPrepublishedHandles.has(handle)) {
        parentWaitStatusPublished = true;
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `exit-parent-prepublish-already-published:${parent.pid}:${handle.pid}:pending=${parent.pendingChildExitRecords}:records=${formatChildExitSyncRecords(parent)}`,
        );
      } else if (handle.pid > 0 && parent.childExitSyncRecords.has(handle.pid)) {
        parentWaitStatusPrepublishedHandles.add(handle);
        parentWaitStatusPublished = true;
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `exit-parent-prepublish-existing:${parent.pid}:${handle.pid}:pending=${parent.pendingChildExitRecords}:records=${formatChildExitSyncRecords(parent)}`,
        );
      } else {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `exit-parent-prepublish-start:${parent.pid}:${handle.pid}:pending=${parent.pendingChildExitRecords}:records=${formatChildExitSyncRecords(parent)}`,
        );
        parent.pendingChildExitRecords += 1;
        if (handle.pid > 0) {
          parent.childExitSyncRecords.set(handle.pid, encodeWaitStatus(exitCode, termSignal));
          if (parent.pendingChildExitRecords > 0) {
            parent.pendingChildExitRecords -= 1;
          }
          parentWaitStatusPublished = true;
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `exit-parent-prepublish-done:${parent.pid}:${handle.pid}:pending=${parent.pendingChildExitRecords}:records=${formatChildExitSyncRecords(parent)}`,
          );
          invalidateBlockedParentRpcsForChildExit(parent, handle.pid);
          deps.kickBlockedWorkers(parent);
        }
      }
    } else {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `exit-parent-prepublish-skip:${handle.pid}:parent=${handle.parentPid ?? 0}`,
      );
    }
    let cleanupError: Error | null = null;
    let parentWaitStatus: number | null = null;

    try {
      parentWaitStatus = await recordExitOnParent(handle, exitCode, termSignal);
      if (parent && parentWaitStatus !== null && !parentWaitStatusPublished) {
        parent.childExitSyncRecords.set(handle.pid, parentWaitStatus);
        if (parent.pendingChildExitRecords > 0) {
          parent.pendingChildExitRecords -= 1;
        }
        parentWaitStatus = null;
        deps.kickBlockedWorkers(parent);
      } else if (parentWaitStatusPublished) {
        parentWaitStatus = null;
      }
      if (parent === null && handle.stdioOwner === null) {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `exit-visible-output-start:${handle.pid}:${formatVisibleOutputDebug(handle)}`);
        try {
          await handle.forkSharedSyncPromise;
        } catch {
          // The final visible-output drain below should still run so process
          // exit cannot hide bytes that were already written to stdout/stderr.
        }
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `exit-visible-output-after-fork-sync:${handle.pid}:${formatVisibleOutputDebug(handle)}`);
        await settleVisibleInheritedChildrenOutput(handle);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `exit-visible-output-after-inherited:${handle.pid}:${formatVisibleOutputDebug(handle)}`);
        if (handle.kernelWorkerRequest && handle.pid > 0) {
          await deps.requestPipeStateSync(handle, true);
        }
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `exit-visible-output-after-pipe-sync:${handle.pid}:${formatVisibleOutputDebug(handle)}`);
        const firstDrain = deps.drainProcessOutput(handle);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `exit-visible-output-after-first-drain:${handle.pid}:drained=${firstDrain ? 1 : 0}:${formatVisibleOutputDebug(handle)}`);
        const settled = await settleVisibleProcessOutput(handle, deps.requestPipeStateSync);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `exit-visible-output-after-settle:${handle.pid}:settled=${settled ? 1 : 0}:${formatVisibleOutputDebug(handle)}`);
        const finalDrain = deps.drainProcessOutput(handle);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `exit-visible-output-after-final-drain:${handle.pid}:drained=${finalDrain ? 1 : 0}:${formatVisibleOutputDebug(handle)}`);
      }
      await publishProcessFsStateOnExit(handle);
      await deps.releaseSharedHostStdio(handle);
    } catch (error) {
      cleanupError = error instanceof Error ? error : new Error(String(error));
      handle.lastDebugMessage = [
        `exit_cleanup_error=${cleanupError.message}`,
        handle.lastDebugMessage,
      ].join(" ");
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `exit-cleanup-error:${handle.pid}:${JSON.stringify(cleanupError.message)}`,
      );
    } finally {
      try {
        await handle.forkSharedSyncPromise;
      } catch (error) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `fork-shared-sync-error:${handle.pid}:${JSON.stringify(error instanceof Error ? error.message : String(error))}`,
        );
      }
      try {
        if (!unregisteredKernelWorkerHandles.has(handle)) {
          await deps.unregisterProcessWithKernelWorker(handle);
        }
      } catch (error) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `unregister-error:${handle.pid}:${JSON.stringify(error instanceof Error ? error.message : String(error))}`,
        );
      }
      deps.destroyProcessHandle(handle, termSignal || 15, false);
    }

    try {
      if (parent && !hasInheritedParentOutput) {
        await deps.requestPipeStateSync(parent, true);
        deps.drainProcessOutput(parent);
      }
    } finally {
      if (parent) {
        if (parentWaitStatus !== null) {
          parent.childExitSyncRecords.set(handle.pid, parentWaitStatus);
        }
        if (parent.pendingChildExitRecords > 0) {
          parent.pendingChildExitRecords -= 1;
        }
        deps.kickBlockedWorkers(parent);
      }
    }
    handle.onExit?.(exitCode);
  }

  function failProcess(handle: ProcessHandle, error: Error): void {
    handle.lastDebugMessage = [
      error.message,
      `exit_trace=${formatExitTrace(handle)}`,
      `live_processes=${safeDebugRuntimeProcessHandles(handle.runtimeId)}`,
      `cmd=${JSON.stringify(handle.debugCommand)}`,
      `worker_log=[${handle.workerDebugLog.join(", ")}]`,
      `thread_debugs=[${safeFormatAllLiveThreadDebug(handle)}]`,
      handle.lastDebugMessage ?? "debug=n/a",
    ].join(" ");
    deps.destroyProcessHandle(handle, 15, false);
    handle.onError?.(error);
  }

  return {
    completeProcessExit,
    failProcess,
    prepublishParentWaitStatus,
    recordExitOnParent,
  };
}
