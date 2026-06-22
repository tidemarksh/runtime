import { settleInheritedChildOutput, settleVisibleProcessOutput } from "./stdio.js";
import { ensureMemoryCapacityFor, scratchBaseFor } from "../kernel-worker/common.js";
import { readInodeRecord } from "../fs/sab-page-cache.js";
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
        sourceVmPtr?: number;
      },
  ): Promise<void>;
  unregisterProcessWithKernelWorker(handle: ProcessHandle): Promise<void>;
}

export function createWorkerExitHelpers(deps: WorkerExitDeps) {
  const unregisteredKernelWorkerHandles = new WeakSet<ProcessHandle>();
  const exitCompletionPromises = new WeakMap<ProcessHandle, Promise<void>>();
  const parentWaitStatusPrepublishedHandles = new WeakSet<ProcessHandle>();

  function debugGuestByteAddresses(): bigint[] {
    const raw = (globalThis as { process?: { env?: Record<string, string | undefined> } })
      .process?.env?.TIDEMARK_DEBUG_GUEST_BYTES ?? "";
    if (raw.trim() === "") {
      return [];
    }
    return raw
      .split(/[\s,]+/)
      .filter((part) => part.length > 0)
      .slice(0, 8)
      .flatMap((part) => {
        try {
          return [BigInt(part)];
        } catch {
          return [];
        }
      });
  }

  function debugMemfsFilePaths(): string[] {
    const raw = (globalThis as { process?: { env?: Record<string, string | undefined> } })
      .process?.env?.TIDEMARK_DEBUG_MEMFS_FILES ?? "";
    if (raw.trim() === "") {
      return [];
    }
    return raw
      .split(/[\n,]+/)
      .map((part) => part.trim())
      .filter((part) => part.length > 0)
      .slice(0, 4);
  }

  function debugExitVmasEnabled(): boolean {
    return ((globalThis as { process?: { env?: Record<string, string | undefined> } })
      .process?.env?.TIDEMARK_DEBUG_EXIT_VMAS ?? "") === "1";
  }

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
        "fault_exec_trace",
        () => formatFrozenExecTrace(
          handle,
          () => handle.kernel.debug_fault_exec_trace_len_for_vm?.(vmPtr) ?? 0,
          (index) => ({
            pc: handle.kernel.debug_fault_exec_trace_entry_pc_for_vm?.(vmPtr, index),
            raw: handle.kernel.debug_fault_exec_trace_entry_raw_for_vm?.(vmPtr, index),
            step: handle.kernel.debug_fault_exec_trace_entry_step_for_vm?.(vmPtr, index),
            sp: handle.kernel.debug_fault_exec_trace_entry_sp_for_vm?.(vmPtr, index),
            ra: handle.kernel.debug_fault_exec_trace_entry_ra_for_vm?.(vmPtr, index),
            a0: handle.kernel.debug_fault_exec_trace_entry_a0_for_vm?.(vmPtr, index),
            s11: handle.kernel.debug_fault_exec_trace_entry_s11_for_vm?.(vmPtr, index),
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
    vmPtrFilter: number | null = handle.vmPtr,
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
      if (vmPtrFilter !== null && vmPtr !== vmPtrFilter) {
        continue;
      }
      const nr = Number(handle.kernel.debug_fd_syscall_trace_nr?.(index) ?? -1n);
      if (![24, 25, 32, 56, 57, 59, 62, 63, 64, 65, 66, 67, 68, 69, 70, 79, 80, 198, 199, 203, 242, 291, 436].includes(nr)) {
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

  function formatFocusedFdSyscalls(handle: ProcessHandle, fds: readonly number[], limit = 160): string {
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
    const focused = new Set(fds);
    const alwaysIncludeSyscalls = new Set([24, 25, 32, 56, 57, 59, 62, 63, 64, 65, 66, 67, 68, 69, 79, 80, 291, 436]);
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const nr = Number(handle.kernel.debug_fd_syscall_trace_nr?.(index) ?? -1n);
      const arg0 = Number(handle.kernel.debug_fd_syscall_trace_arg0?.(index) ?? -1n);
      const arg2 = Number(handle.kernel.debug_fd_syscall_trace_arg2?.(index) ?? -1n);
      const result = handle.kernel.debug_fd_syscall_trace_result?.(index);
      const signedResult = result === undefined ? null : BigInt.asIntN(64, BigInt(result));
      if (
        !focused.has(arg0)
        && !focused.has(arg2)
        && signedResult !== -9n
        && !alwaysIncludeSyscalls.has(nr)
      ) {
        continue;
      }
      const vmPtr = handle.kernel.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
      entries.push([
        `${index}:vm=0x${vmPtr.toString(16)}:${nr}`,
        `pid=${sampleToPid(handle.kernel.debug_fd_syscall_trace_pid_sample0?.(index))}`,
        `a0=${arg0}`,
        `a0ofd=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? "n/a"}`,
        `a0ty=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? "n/a"}`,
        `a0flags=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_status_flags?.(index) ?? "n/a"}`,
        `a0ino=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_ino?.(index) ?? "n/a"}`,
        `a1=${handle.kernel.debug_fd_syscall_trace_arg1?.(index)?.toString() ?? "n/a"}`,
        `a2=${arg2}`,
        `a2ofd=${handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? "n/a"}`,
        `a2ty=${handle.kernel.debug_fd_syscall_trace_arg2_ofd_type?.(index) ?? "n/a"}`,
        `ret=${signedResult?.toString() ?? "n/a"}`,
        `st=${handle.kernel.debug_fd_syscall_trace_status?.(index) ?? "n/a"}`,
      ].join(":"));
    }
    return entries.slice(-limit).join(",");
  }

  function formatStrictFocusedFdSyscalls(handle: ProcessHandle, fds: readonly number[], limit = 512): string {
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
    const focused = new Set(fds);
    const closeRange = Number(handle.kernel.abi_sys_close_range?.() ?? 436);
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const nr = Number(handle.kernel.debug_fd_syscall_trace_nr?.(index) ?? -1n);
      const arg0 = Number(handle.kernel.debug_fd_syscall_trace_arg0?.(index) ?? -1n);
      const arg1 = Number(handle.kernel.debug_fd_syscall_trace_arg1?.(index) ?? -1n);
      const arg2 = Number(handle.kernel.debug_fd_syscall_trace_arg2?.(index) ?? -1n);
      const result = handle.kernel.debug_fd_syscall_trace_result?.(index);
      const signedResult = result === undefined ? null : BigInt.asIntN(64, BigInt(result));
      const focusCloseRange = nr === closeRange
        && fds.some((fd) => fd >= arg0 && fd <= arg1);
      if (!focused.has(arg0) && !focused.has(arg2) && signedResult !== -9n && !focusCloseRange) {
        continue;
      }
      const vmPtr = handle.kernel.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
      entries.push([
        `${index}:vm=0x${vmPtr.toString(16)}:${nr}`,
        `pid=${sampleToPid(handle.kernel.debug_fd_syscall_trace_pid_sample0?.(index))}`,
        `a0=${arg0}`,
        `a0ofd=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? "n/a"}`,
        `a0ty=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? "n/a"}`,
        `a0flags=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_status_flags?.(index) ?? "n/a"}`,
        `a0ino=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_ino?.(index) ?? "n/a"}`,
        `a1=${arg1}`,
        `a2=${arg2}`,
        `a2ofd=${handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? "n/a"}`,
        `a2ty=${handle.kernel.debug_fd_syscall_trace_arg2_ofd_type?.(index) ?? "n/a"}`,
        `ret=${signedResult?.toString() ?? "n/a"}`,
        `st=${handle.kernel.debug_fd_syscall_trace_status?.(index) ?? "n/a"}`,
      ].join(":"));
    }
    return entries.slice(-limit).join(",");
  }

  function collectFdFocusVmPtrs(handle: ProcessHandle, vmPtr: number): number[] {
    const vmPtrs = new Set<number>();
    const add = (candidate: number | undefined | null) => {
      if (candidate !== undefined && candidate !== null && candidate !== 0) {
        vmPtrs.add(candidate);
      }
    };
    add(handle.vmPtr);
    add(vmPtr);
    try {
      for (let slot = 0; slot < handle.kernel.abi_max_threads(); slot += 1) {
        add(handle.kernel.process_thread_vm_ptr?.(handle.vmPtr, slot));
      }
    } catch {
      // Best-effort diagnostics only.
    }
    for (const blocked of handle.blockedWorkers.values()) {
      add(blocked.vmPtr);
    }
    return [...vmPtrs].slice(0, 24);
  }

  function formatFdFocusState(handle: ProcessHandle, vmPtr: number, fds: readonly number[]): string {
    const maxFds = handle.kernel.abi_max_fds();
    const entries: string[] = [];
    for (const candidateVmPtr of collectFdFocusVmPtrs(handle, vmPtr)) {
      const fdParts: string[] = [];
      for (const fd of fds) {
        if (fd < 0 || fd >= maxFds) {
          continue;
        }
        try {
          const ofdId = handle.kernel.process_fd_ofd_id?.(candidateVmPtr, fd) ?? 0;
          const flags = handle.kernel.process_fd_flags?.(candidateVmPtr, fd) ?? 0;
          const generation = handle.kernel.process_fd_generation?.(candidateVmPtr, fd) ?? 0;
          if (ofdId <= 0 || ofdId >= maxFds) {
            fdParts.push(`${fd}->${ofdId}/fl=${flags}/gen=${generation}`);
            continue;
          }
          fdParts.push(
            `${fd}->${ofdId}`
            + `/fl=${flags}`
            + `/gen=${generation}`
            + `/ty=${handle.kernel.kernel_ofd_fd_type(ofdId)}`
            + `/ref=${handle.kernel.kernel_ofd_refcount(ofdId)}`
            + `/host=${handle.kernel.kernel_ofd_host_fd(ofdId)}`
            + `/ino=${handle.kernel.kernel_ofd_ino(ofdId)}`
            + `/st=${handle.kernel.kernel_ofd_status_flags(ofdId)}`,
          );
        } catch (error) {
          fdParts.push(`${fd}->error:${JSON.stringify(error instanceof Error ? error.message : String(error))}`);
        }
      }
      let processState: string;
      try {
        processState = [
          `pid=${handle.kernel.process_current_pid?.(candidateVmPtr) ?? 0}`,
          `proc=0x${(handle.kernel.thread_process_ptr?.(candidateVmPtr) ?? 0).toString(16)}`,
        ].join(":");
      } catch (error) {
        processState = `state_error=${JSON.stringify(error instanceof Error ? error.message : String(error))}`;
      }
      entries.push([
        `vm=0x${candidateVmPtr.toString(16)}`,
        processState,
        `fds=${fdParts.join("|")}`,
      ].join(":"));
    }
    return entries.join(",");
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

  const SYS_MUNMAP = 215n;
  const SYS_MREMAP = 216n;
  const SYS_MMAP = 222n;
  const SYS_MPROTECT = 226n;
  const SYS_MSYNC = 227n;
  const SYS_MADVISE = 233n;
  const SYS_RISCV_FLUSH_ICACHE = 259n;
  const PAGE_MASK = ~4095n;

  function pageAlignUp(value: bigint): bigint {
    return (value + 4095n) & PAGE_MASK;
  }

  function overlapsWatchedRegion(
    handle: ProcessHandle,
    start: bigint,
    end: bigint,
  ): boolean {
    if (end <= start) {
      return false;
    }
    for (const region of handle.guestWriteWatchRegions) {
      const regionStart = BigInt(region.start);
      const regionEnd = BigInt(region.end);
      if (start < regionEnd && end > regionStart) {
        return true;
      }
    }
    return false;
  }

  function watchedMemorySyscallRange(
    nr: bigint,
    arg0: bigint,
    arg1: bigint,
    arg2: bigint,
    arg3: bigint,
    arg4: bigint,
    result: bigint,
  ): Array<{ start: bigint; end: bigint; label: string }> {
    if (nr === SYS_MMAP) {
      const signedResult = BigInt.asIntN(64, result);
      if (signedResult < 0n) {
        return [];
      }
      return [{ start: signedResult, end: signedResult + pageAlignUp(arg1), label: "new" }];
    }
    if (nr === SYS_MUNMAP || nr === SYS_MPROTECT || nr === SYS_MSYNC || nr === SYS_MADVISE) {
      return [{ start: arg0, end: arg0 + pageAlignUp(arg1), label: "range" }];
    }
    if (nr === SYS_MREMAP) {
      const ranges = [{ start: arg0, end: arg0 + pageAlignUp(arg1), label: "old" }];
      const signedResult = BigInt.asIntN(64, result);
      if (signedResult >= 0n) {
        ranges.push({ start: signedResult, end: signedResult + pageAlignUp(arg2), label: "new" });
      }
      if ((arg3 & 2n) !== 0n) {
        ranges.push({ start: arg4, end: arg4 + pageAlignUp(arg2), label: "fixed" });
      }
      return ranges;
    }
    return [];
  }

  function formatWatchedMemorySyscalls(handle: ProcessHandle, vmPtr: number): string {
    if (handle.guestWriteWatchRegions.length === 0) {
      return "";
    }
    const len = handle.kernel.debug_memory_syscall_trace_len?.() ?? 0;
    if (len <= 0) {
      return "";
    }
    const processPtr = handle.kernel.thread_process_ptr?.(vmPtr) ?? 0;
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const entryProcessPtr = handle.kernel.debug_memory_syscall_trace_entry_process_ptr?.(index) ?? 0;
      if (processPtr !== 0 && entryProcessPtr !== processPtr) {
        continue;
      }
      const nr = handle.kernel.debug_memory_syscall_trace_entry_nr?.(index) ?? 0n;
      const arg0 = handle.kernel.debug_memory_syscall_trace_entry_arg0?.(index) ?? 0n;
      const arg1 = handle.kernel.debug_memory_syscall_trace_entry_arg1?.(index) ?? 0n;
      const arg2 = handle.kernel.debug_memory_syscall_trace_entry_arg2?.(index) ?? 0n;
      const arg3 = handle.kernel.debug_memory_syscall_trace_entry_arg3?.(index) ?? 0n;
      const arg4 = handle.kernel.debug_memory_syscall_trace_entry_arg4?.(index) ?? 0n;
      const arg5 = handle.kernel.debug_memory_syscall_trace_entry_arg5?.(index) ?? 0n;
      const result = handle.kernel.debug_memory_syscall_trace_entry_result?.(index) ?? 0n;
      const ranges = watchedMemorySyscallRange(nr, arg0, arg1, arg2, arg3, arg4, result)
        .filter((range) => overlapsWatchedRegion(handle, range.start, range.end));
      if (ranges.length === 0) {
        continue;
      }
      entries.push([
        `${index}:${nr.toString()}`,
        `pid=${handle.kernel.debug_memory_syscall_trace_entry_pid?.(index) ?? 0}`,
        `process=0x${entryProcessPtr.toString(16)}`,
        `a0=${arg0.toString()}`,
        `a1=${arg1.toString()}`,
        `a2=${arg2.toString()}`,
        `a3=${arg3.toString()}`,
        `a4=${arg4.toString()}`,
        `a5=${arg5.toString()}`,
        `ret=${result.toString()}`,
        `ranges=${ranges.map((range) => (
          `${range.label}:0x${range.start.toString(16)}..0x${range.end.toString(16)}`
        )).join("|")}`,
      ].join(":"));
    }
    return entries.join(",");
  }

  function watchedFlushIcacheRanges(
    start: bigint,
    endOrLength: bigint,
  ): Array<{ start: bigint; end: bigint; label: string }> {
    if (endOrLength < start) {
      return [{ start, end: start + endOrLength, label: "start+len" }];
    }
    return [{ start, end: endOrLength, label: "start..end" }];
  }

  function formatWatchedRiscvFlushIcache(handle: ProcessHandle): string {
    if (handle.guestWriteWatchRegions.length === 0) {
      return "";
    }
    const len = handle.kernel.debug_riscv_syscall_trace_len?.() ?? 0;
    if (len <= 0) {
      return "";
    }
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const nr = handle.kernel.debug_riscv_syscall_trace_entry_nr?.(index) ?? 0n;
      if (nr !== SYS_RISCV_FLUSH_ICACHE) {
        continue;
      }
      const arg0 = BigInt(handle.kernel.debug_riscv_syscall_trace_entry_arg0?.(index) ?? 0n);
      const arg1 = BigInt(handle.kernel.debug_riscv_syscall_trace_entry_arg1?.(index) ?? 0n);
      const arg2 = handle.kernel.debug_riscv_syscall_trace_entry_arg2?.(index) ?? 0n;
      const ranges = watchedFlushIcacheRanges(arg0, arg1)
        .filter((range) => overlapsWatchedRegion(handle, range.start, range.end));
      if (ranges.length === 0) {
        continue;
      }
      const vmPtr = handle.kernel.debug_riscv_syscall_trace_entry_vm_ptr?.(index) ?? 0;
      const result = handle.kernel.debug_riscv_syscall_trace_entry_result?.(index);
      const status = handle.kernel.debug_riscv_syscall_trace_entry_status?.(index) ?? 0;
      entries.push([
        `${index}:vm=0x${vmPtr.toString(16)}`,
        `a0=${arg0.toString()}`,
        `a1=${arg1.toString()}`,
        `a2=${arg2.toString()}`,
        `status=${status}`,
        `ret=${result?.toString() ?? "n/a"}`,
        `ranges=${ranges.map((range) => (
          `${range.label}:0x${range.start.toString(16)}..0x${range.end.toString(16)}`
        )).join("|")}`,
      ].join(":"));
    }
    return entries.join(",");
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

  function readGuestInsn(handle: ProcessHandle, pc: bigint): string | null {
    const addr = Number(pc);
    if (!Number.isFinite(addr) || addr < 0) {
      return null;
    }
    const absolute = Math.max(0, handle.guestBase) + addr;
    if (absolute < 0 || absolute + 4 > handle.memory.buffer.byteLength) {
      return null;
    }
    return `0x${new DataView(handle.memory.buffer).getUint32(absolute, true).toString(16)}`;
  }

  function formatFaultInstructions(handle: ProcessHandle, vmPtr = handle.vmPtr): string {
    const pc = handle.kernel.thread_pc?.(vmPtr) ?? 0n;
    const faultPc = handle.kernel.thread_fault_pc?.(vmPtr) ?? 0n;
    return [
      `pc_insn=${readGuestInsn(handle, pc) ?? "n/a"}`,
      `fault_insn=${faultPc === 0n ? "n/a" : (readGuestInsn(handle, faultPc) ?? "n/a")}`,
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

  interface FaultDebugVma {
    index: number;
    start: bigint;
    end: bigint;
    prot: number;
    flags: number;
    fd: number | null;
    offset: bigint;
  }

  function findVmaForAddress(handle: ProcessHandle, value: bigint): FaultDebugVma | null {
    if (value === 0n) {
      return null;
    }
    const len = handle.kernel.debug_memory_vma_len?.() ?? 0;
    for (let index = 0; index < len; index += 1) {
      const start = BigInt(handle.kernel.debug_memory_vma_start?.(index) ?? 0);
      const end = BigInt(handle.kernel.debug_memory_vma_end?.(index) ?? 0);
      if (value < start || value >= end) {
        continue;
      }
      const fd = handle.kernel.debug_memory_vma_fd?.(index);
      return {
        index,
        start,
        end,
        prot: handle.kernel.debug_memory_vma_prot?.(index) ?? 0,
        flags: handle.kernel.debug_memory_vma_flags?.(index) ?? 0,
        fd: typeof fd === "number" ? fd : null,
        offset: BigInt(handle.kernel.debug_memory_vma_offset?.(index) ?? 0),
      };
    }
    return null;
  }

  function formatVmaForAddress(handle: ProcessHandle, value: bigint): string {
    if (value === 0n) {
      return "zero";
    }
    const vma = findVmaForAddress(handle, value);
    if (vma !== null) {
      return [
        `${vma.index}`,
        `0x${vma.start.toString(16)}-0x${vma.end.toString(16)}`,
        `prot=${vma.prot}`,
        `flags=${vma.flags}`,
        `fd=${vma.fd ?? "n/a"}`,
        `off=${vma.offset.toString()}`,
      ].join("/");
    }
    return "none";
  }

  function readFaultDebugVmas(handle: ProcessHandle): FaultDebugVma[] {
    const len = handle.kernel.debug_memory_vma_len?.() ?? 0;
    const out: FaultDebugVma[] = [];
    for (let index = 0; index < len; index += 1) {
      const start = BigInt(handle.kernel.debug_memory_vma_start?.(index) ?? 0);
      const end = BigInt(handle.kernel.debug_memory_vma_end?.(index) ?? 0);
      const fd = handle.kernel.debug_memory_vma_fd?.(index);
      out.push({
        index,
        start,
        end,
        prot: handle.kernel.debug_memory_vma_prot?.(index) ?? 0,
        flags: handle.kernel.debug_memory_vma_flags?.(index) ?? 0,
        fd: typeof fd === "number" ? fd : null,
        offset: BigInt(handle.kernel.debug_memory_vma_offset?.(index) ?? 0),
      });
    }
    return out;
  }

  function formatExitMemoryVmas(handle: ProcessHandle): string | null {
    if (!debugExitVmasEnabled()) {
      return null;
    }
    const vmas = readFaultDebugVmas(handle);
    const entries = vmas.map((vma) => (
      `${vma.index}/0x${vma.start.toString(16)}-0x${vma.end.toString(16)}`
      + `/p=${vma.prot}/f=${vma.flags}/fd=${vma.fd ?? "n/a"}/off=${vma.offset.toString()}`
    ));
    const overlaps: string[] = [];
    for (let left = 0; left < vmas.length; left += 1) {
      for (let right = left + 1; right < vmas.length; right += 1) {
        const a = vmas[left];
        const b = vmas[right];
        if (a.start < b.end && b.start < a.end) {
          overlaps.push(`${a.index}-${b.index}`);
        }
      }
    }
    return `memory_vmas=[${entries.join(" ")}] memory_vma_overlaps=[${overlaps.join(" ")}]`;
  }

  function formatHexBytes(bytes: Uint8Array): string {
    return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  }

  function readGuestBytes(handle: ProcessHandle, address: bigint, len: number): string {
    if (address < 0n || address > BigInt(Number.MAX_SAFE_INTEGER)) {
      return "n/a";
    }
    const absolute = BigInt(handle.guestBase) + address;
    if (absolute < 0n || absolute > BigInt(Number.MAX_SAFE_INTEGER)) {
      return "n/a";
    }
    const start = Number(absolute);
    const end = start + len;
    if (start < 0 || end > handle.memory.buffer.byteLength) {
      return "n/a";
    }
    return formatHexBytes(new Uint8Array(handle.memory.buffer, start, len));
  }

  function guestAbsoluteOffset(handle: ProcessHandle, address: bigint, len: number): number | null {
    if (address < 0n || address > BigInt(Number.MAX_SAFE_INTEGER)) {
      return null;
    }
    const absolute = BigInt(handle.guestBase) + address;
    if (absolute < 0n || absolute > BigInt(Number.MAX_SAFE_INTEGER)) {
      return null;
    }
    const start = Number(absolute);
    if (start < 0 || start + len > handle.memory.buffer.byteLength) {
      return null;
    }
    return start;
  }

  function readGuestU32(handle: ProcessHandle, address: bigint): number | null {
    const offset = guestAbsoluteOffset(handle, address, 4);
    if (offset === null) {
      return null;
    }
    return new DataView(handle.memory.buffer).getUint32(offset, true);
  }

  function readGuestU64(handle: ProcessHandle, address: bigint): bigint | null {
    const offset = guestAbsoluteOffset(handle, address, 8);
    if (offset === null) {
      return null;
    }
    return new DataView(handle.memory.buffer).getBigUint64(offset, true);
  }

  function formatSignalFrameState(handle: ProcessHandle, frameBase: bigint): string {
    if (frameBase === 0n) {
      return "signal_frame=[]";
    }
    const siginfoBase = frameBase + 32n;
    const ucontextBase = siginfoBase + 128n;
    const regsBase = ucontextBase + 176n;
    const hex = (value: bigint | null): string => `0x${(value ?? 0n).toString(16)}`;
    return `signal_frame=[${
      [
        `home_signo=${readGuestU64(handle, frameBase + 8n)?.toString() ?? "n/a"}`,
        `home_info=${hex(readGuestU64(handle, frameBase + 16n))}`,
        `home_uc=${hex(readGuestU64(handle, frameBase + 24n))}`,
        `signo=${readGuestU32(handle, siginfoBase)?.toString() ?? "n/a"}`,
        `code=${readGuestU32(handle, siginfoBase + 8n)?.toString() ?? "n/a"}`,
        `addr=${hex(readGuestU64(handle, siginfoBase + 16n))}`,
        `uc_pc=${hex(readGuestU64(handle, regsBase))}`,
        `uc_ra=${hex(readGuestU64(handle, regsBase + 8n))}`,
        `uc_sp=${hex(readGuestU64(handle, regsBase + 16n))}`,
        `uc_a0=${hex(readGuestU64(handle, regsBase + 80n))}`,
      ].join(":")
    }]`;
  }

  function readVmaFileBytes(handle: ProcessHandle, fd: number, offset: bigint, len: number): string {
    if (fd < 0) {
      return "n/a";
    }
    const pageCache = handle.sabPageCache;
    if (pageCache) {
      const published = readInodeRecord(pageCache, fd);
      const offsetNumber = Number(offset);
      if (
        published
        && Number.isSafeInteger(offsetNumber)
        && offsetNumber >= 0
        && offsetNumber < published.size
      ) {
        return `page=external:size=${published.size}`;
      }
    }
    if (typeof handle.kernel.memfs_read_file_to !== "function") {
      return "n/a";
    }
    try {
      const bufPtr = scratchBaseFor(handle.kernel) + 0x24000;
      ensureMemoryCapacityFor(handle.memory, bufPtr + len);
      const read = Number(handle.kernel.memfs_read_file_to(fd, offset, bufPtr, len));
      if (read <= 0) {
        return `read=${read}`;
      }
      return formatHexBytes(new Uint8Array(handle.memory.buffer, bufPtr, Math.min(read, len)));
    } catch (error) {
      return `err=${error instanceof Error ? error.message : String(error)}`;
    }
  }

  function formatFaultByteSource(handle: ProcessHandle, label: string, value: bigint): string {
    if (value === 0n) {
      return `${label}=zero`;
    }
    const vma = findVmaForAddress(handle, value);
    if (vma === null) {
      return `${label}=none`;
    }
    const sourceOffset = vma.offset + (value - vma.start);
    return [
      `${label}=vma${vma.index}`,
      `fd=${vma.fd ?? "n/a"}`,
      `src=0x${sourceOffset.toString(16)}`,
      `guest=${readGuestBytes(handle, value, 8)}`,
      `file=${vma.fd === null ? "n/a" : readVmaFileBytes(handle, vma.fd, sourceOffset, 8)}`,
    ].join("/");
  }

  function formatDebugGuestByteSources(handle: ProcessHandle): string | null {
    const addresses = debugGuestByteAddresses();
    if (addresses.length === 0) {
      return null;
    }
    return `debug_guest_bytes=[${
      addresses.map((address, index) => (
        formatFaultByteSource(handle, `addr${index}:0x${address.toString(16)}`, address)
      )).join(",")
    }]`;
  }

  function formatDebugMemfsFiles(handle: ProcessHandle): string | null {
    const paths = debugMemfsFilePaths();
    if (paths.length === 0) {
      return null;
    }
    if (
      typeof handle.kernel.memfs_resolve_path !== "function"
      || typeof handle.kernel.memfs_file_size !== "function"
      || typeof handle.kernel.memfs_read_file_to !== "function"
    ) {
      return "debug_memfs_files=[unavailable]";
    }
    const encoder = new TextEncoder();
    const decoder = new TextDecoder();
    const entries: string[] = [];
    for (const path of paths) {
      try {
        const bytes = encoder.encode(path);
        const pathPtr = scratchBaseFor(handle.kernel) + 0x26000;
        ensureMemoryCapacityFor(handle.memory, pathPtr + bytes.length + 8192);
        new Uint8Array(handle.memory.buffer, pathPtr, bytes.length).set(bytes);
        const ino = handle.kernel.memfs_resolve_path(pathPtr, bytes.length);
        if (ino === 0) {
          entries.push(`${JSON.stringify(path)}:missing`);
          continue;
        }
        const size = handle.kernel.memfs_file_size(ino);
        const readLen = Math.min(Number(size), 4096);
        const outPtr = pathPtr + bytes.length + 16;
        const read = Number(handle.kernel.memfs_read_file_to(ino, 0n, outPtr, readLen));
        if (read <= 0) {
          entries.push(`${JSON.stringify(path)}:ino=${ino}:size=${size.toString()}:read=${read}`);
          continue;
        }
        const text = decoder.decode(new Uint8Array(handle.memory.buffer, outPtr, read));
        entries.push(
          `${JSON.stringify(path)}:ino=${ino}:size=${size.toString()}:head=${JSON.stringify(text)}`,
        );
      } catch (error) {
        entries.push(`${JSON.stringify(path)}:error=${JSON.stringify(error instanceof Error ? error.message : String(error))}`);
      }
    }
    return `debug_memfs_files=[${entries.join(",")}]`;
  }

  function formatFaultVmas(handle: ProcessHandle, vmPtr = handle.vmPtr): string {
    const pc = handle.kernel.thread_pc?.(vmPtr) ?? 0n;
    const faultPc = handle.kernel.thread_fault_pc?.(vmPtr) ?? 0n;
    const faultAddr = handle.kernel.thread_fault_addr?.(vmPtr) ?? 0n;
    return [
      `pc=${formatVmaForAddress(handle, pc)}`,
      `fault_pc=${formatVmaForAddress(handle, faultPc)}`,
      `fault_addr=${formatVmaForAddress(handle, faultAddr)}`,
    ].join(":");
  }

  function formatFaultByteSources(handle: ProcessHandle, vmPtr = handle.vmPtr): string {
    const pc = handle.kernel.thread_pc?.(vmPtr) ?? 0n;
    const faultPc = handle.kernel.thread_fault_pc?.(vmPtr) ?? 0n;
    const faultAddr = handle.kernel.thread_fault_addr?.(vmPtr) ?? 0n;
    return [
      formatFaultByteSource(handle, "pc", pc),
      formatFaultByteSource(handle, "fault_pc", faultPc),
      formatFaultByteSource(handle, "fault_addr", faultAddr),
    ].join(":");
  }

  function formatDefaultSignalTrace(handle: ProcessHandle): string {
    const sig = handle.kernel.debug_default_signal_sig?.() ?? 0;
    if (sig === 0) {
      return "default_signal=[]";
    }
    const pc = handle.kernel.debug_default_signal_pc?.() ?? 0n;
    const faultPc = handle.kernel.debug_default_signal_fault_pc?.() ?? 0n;
    const faultAddr = handle.kernel.debug_default_signal_fault_addr?.() ?? 0n;
    return `default_signal=[${
      [
        `sig=${sig}`,
        `code=${handle.kernel.debug_default_signal_code?.() ?? 0}`,
        `vm=0x${(handle.kernel.debug_default_signal_vm_ptr?.() ?? 0).toString(16)}`,
        `pc=0x${pc.toString(16)}`,
        `ra=0x${(handle.kernel.debug_default_signal_ra?.() ?? 0n).toString(16)}`,
        `sp=0x${(handle.kernel.debug_default_signal_sp?.() ?? 0n).toString(16)}`,
        `gp=0x${(handle.kernel.debug_default_signal_gp?.() ?? 0n).toString(16)}`,
        `tp=0x${(handle.kernel.debug_default_signal_tp?.() ?? 0n).toString(16)}`,
        `s11=0x${(handle.kernel.debug_default_signal_s11?.() ?? 0n).toString(16)}`,
        `fault_pc=0x${faultPc.toString(16)}`,
        `fault_addr=0x${faultAddr.toString(16)}`,
        `pc_vma=${formatVmaForAddress(handle, pc)}`,
        `fault_pc_vma=${formatVmaForAddress(handle, faultPc)}`,
        `fault_addr_vma=${formatVmaForAddress(handle, faultAddr)}`,
      ].join(":")
    }]`;
  }

  function formatSignalSendTrace(handle: ProcessHandle): string {
    const len = handle.kernel.debug_signal_send_trace_len?.() ?? 0;
    if (len <= 0) {
      return "signal_sends=[]";
    }
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const sig = handle.kernel.debug_signal_send_trace_entry_sig?.(index) ?? 0;
      if (sig === 0) {
        continue;
      }
      entries.push(
        `${index}:kind=${handle.kernel.debug_signal_send_trace_entry_kind?.(index) ?? 0}`
        + `:caller=0x${(handle.kernel.debug_signal_send_trace_entry_caller_vm_ptr?.(index) ?? 0).toString(16)}`
        + `:pid=${handle.kernel.debug_signal_send_trace_entry_pid?.(index) ?? 0}`
        + `:tid=${handle.kernel.debug_signal_send_trace_entry_tid?.(index) ?? 0}`
        + `:sig=${sig}`
        + `:result=${handle.kernel.debug_signal_send_trace_entry_result?.(index)?.toString() ?? "n/a"}`,
      );
    }
    return entries.length === 0 ? "signal_sends=[]" : `signal_sends=[${entries.join(",")}]`;
  }

  function formatSignalDeliveryTrace(handle: ProcessHandle): string {
    const len = handle.kernel.debug_signal_delivery_trace_len?.() ?? 0;
    if (len <= 0) {
      return "signal_deliveries=[]";
    }
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const sig = handle.kernel.debug_signal_delivery_trace_entry_sig?.(index) ?? 0;
      if (sig === 0) {
        continue;
      }
      entries.push(
        `${index}:vm=0x${(handle.kernel.debug_signal_delivery_trace_entry_vm_ptr?.(index) ?? 0).toString(16)}`
        + `:sig=${sig}`
        + `:code=${handle.kernel.debug_signal_delivery_trace_entry_code?.(index) ?? 0}`
        + `:frame=0x${formatTraceU64(handle.kernel.debug_signal_delivery_trace_entry_frame_base?.(index))}`
        + `:pc=0x${formatTraceU64(handle.kernel.debug_signal_delivery_trace_entry_pc?.(index))}`
        + `:ra=0x${formatTraceU64(handle.kernel.debug_signal_delivery_trace_entry_ra?.(index))}`
        + `:sp=0x${formatTraceU64(handle.kernel.debug_signal_delivery_trace_entry_sp?.(index))}`
        + `:s11=0x${formatTraceU64(handle.kernel.debug_signal_delivery_trace_entry_s11?.(index))}`
        + `:fault_pc=0x${formatTraceU64(handle.kernel.debug_signal_delivery_trace_entry_fault_pc?.(index))}`
        + `:fault_addr=0x${formatTraceU64(handle.kernel.debug_signal_delivery_trace_entry_fault_addr?.(index))}`,
      );
    }
    return entries.length === 0
      ? "signal_deliveries=[]"
      : `signal_deliveries=[${entries.join(",")}]`;
  }

  function formatSyncFaultTrace(handle: ProcessHandle): string {
    const vmPtr = handle.kernel.debug_sync_fault_vm_ptr?.() ?? 0;
    if (vmPtr === 0) {
      return "sync_fault=[]";
    }
    return `sync_fault=[${
      [
        `vm=0x${vmPtr.toString(16)}`,
        `tid=${handle.kernel.debug_sync_fault_tid?.() ?? 0}`,
        `pc=0x${(handle.kernel.debug_sync_fault_pc?.() ?? 0n).toString(16)}`,
        `ra=0x${(handle.kernel.debug_sync_fault_ra?.() ?? 0n).toString(16)}`,
        `sp=0x${(handle.kernel.debug_sync_fault_sp?.() ?? 0n).toString(16)}`,
        `s11=0x${(handle.kernel.debug_sync_fault_s11?.() ?? 0n).toString(16)}`,
        `a0=0x${(handle.kernel.debug_sync_fault_a0?.() ?? 0n).toString(16)}`,
        `a1=0x${(handle.kernel.debug_sync_fault_a1?.() ?? 0n).toString(16)}`,
        `a2=0x${(handle.kernel.debug_sync_fault_a2?.() ?? 0n).toString(16)}`,
        `a3=0x${(handle.kernel.debug_sync_fault_a3?.() ?? 0n).toString(16)}`,
        `a4=0x${(handle.kernel.debug_sync_fault_a4?.() ?? 0n).toString(16)}`,
        `a5=0x${(handle.kernel.debug_sync_fault_a5?.() ?? 0n).toString(16)}`,
        `a6=0x${(handle.kernel.debug_sync_fault_a6?.() ?? 0n).toString(16)}`,
        `a7=0x${(handle.kernel.debug_sync_fault_a7?.() ?? 0n).toString(16)}`,
        `s1=0x${(handle.kernel.debug_sync_fault_s1?.() ?? 0n).toString(16)}`,
        `s2=0x${(handle.kernel.debug_sync_fault_s2?.() ?? 0n).toString(16)}`,
        `s3=0x${(handle.kernel.debug_sync_fault_s3?.() ?? 0n).toString(16)}`,
        `s4=0x${(handle.kernel.debug_sync_fault_s4?.() ?? 0n).toString(16)}`,
        `s5=0x${(handle.kernel.debug_sync_fault_s5?.() ?? 0n).toString(16)}`,
        `s6=0x${(handle.kernel.debug_sync_fault_s6?.() ?? 0n).toString(16)}`,
        `s7=0x${(handle.kernel.debug_sync_fault_s7?.() ?? 0n).toString(16)}`,
        `addr=0x${(handle.kernel.debug_sync_fault_addr?.() ?? 0n).toString(16)}`,
        `code=${handle.kernel.debug_sync_fault_code?.() ?? 0}`,
      ].join(":")
    }]`;
  }

  function formatSigreturnTrace(handle: ProcessHandle): string {
    const frameBase = handle.kernel.debug_sigreturn_frame_base?.() ?? 0n;
    const count = handle.kernel.debug_sigreturn_count?.() ?? 0;
    const trampolineTid = handle.kernel.debug_signal_trampoline_hit_tid?.() ?? 0;
    const trampolinePc = handle.kernel.debug_signal_trampoline_hit_pc?.() ?? 0n;
    if (frameBase === 0n && count === 0 && trampolineTid === 0 && trampolinePc === 0n) {
      return "sigreturn=[]";
    }
    return `sigreturn=[${
      [
        `count=${count}`,
        `frame=0x${frameBase.toString(16)}`,
        `pc=0x${(handle.kernel.debug_sigreturn_pc?.() ?? 0n).toString(16)}`,
        `ra=0x${(handle.kernel.debug_sigreturn_ra?.() ?? 0n).toString(16)}`,
        `sp=0x${(handle.kernel.debug_sigreturn_sp?.() ?? 0n).toString(16)}`,
        `gp=0x${(handle.kernel.debug_sigreturn_gp?.() ?? 0n).toString(16)}`,
        `tp=0x${(handle.kernel.debug_sigreturn_tp?.() ?? 0n).toString(16)}`,
        `s11=0x${(handle.kernel.debug_sigreturn_s11?.() ?? 0n).toString(16)}`,
        `trampoline_tid=${trampolineTid}`,
        `trampoline_pc=0x${trampolinePc.toString(16)}`,
        formatSignalFrameState(handle, frameBase),
      ].join(":")
    }]`;
  }

  function formatIllegalInstructionTrace(handle: ProcessHandle): string {
    const pc = handle.kernel.debug_illegal_instruction_pc?.() ?? 0n;
    if (pc === 0n) {
      return "illegal_instruction=[]";
    }
    return `illegal_instruction=[${
      [
        `vm=0x${(handle.kernel.debug_illegal_instruction_vm_ptr?.() ?? 0).toString(16)}`,
        `pc=0x${pc.toString(16)}`,
        `raw=0x${(handle.kernel.debug_illegal_instruction_raw?.() ?? 0).toString(16)}`,
        `ra=0x${(handle.kernel.debug_illegal_instruction_ra?.() ?? 0n).toString(16)}`,
        `sp=0x${(handle.kernel.debug_illegal_instruction_sp?.() ?? 0n).toString(16)}`,
        `a0=0x${(handle.kernel.debug_illegal_instruction_a0?.() ?? 0n).toString(16)}`,
        `a4=0x${(handle.kernel.debug_illegal_instruction_a4?.() ?? 0n).toString(16)}`,
        `s11=0x${(handle.kernel.debug_illegal_instruction_s11?.() ?? 0n).toString(16)}`,
        `jalr_pc=0x${(handle.kernel.debug_illegal_instruction_jalr_pc?.() ?? 0n).toString(16)}`,
        `jalr_rs1=${handle.kernel.debug_illegal_instruction_jalr_rs1?.() ?? 0}`,
        `jalr_rd=${handle.kernel.debug_illegal_instruction_jalr_rd?.() ?? 0}`,
        `jalr_rs1v=0x${(handle.kernel.debug_illegal_instruction_jalr_rs1_value?.() ?? 0n).toString(16)}`,
        `jalr_target=0x${(handle.kernel.debug_illegal_instruction_jalr_target?.() ?? 0n).toString(16)}`,
        `sig_frame=0x${(handle.kernel.debug_illegal_instruction_signal_frame_base?.() ?? 0n).toString(16)}`,
        `sig_handler=0x${(handle.kernel.debug_illegal_instruction_signal_handler_pc?.() ?? 0n).toString(16)}`,
        `sig_code=${handle.kernel.debug_illegal_instruction_signal_code?.() ?? 0}`,
        `sig_addr=0x${(handle.kernel.debug_illegal_instruction_signal_addr?.() ?? 0n).toString(16)}`,
        `sig_uc_pc=0x${(handle.kernel.debug_illegal_instruction_signal_uc_pc?.() ?? 0n).toString(16)}`,
        `sig_uc_ra=0x${(handle.kernel.debug_illegal_instruction_signal_uc_ra?.() ?? 0n).toString(16)}`,
        `sig_uc_sp=0x${(handle.kernel.debug_illegal_instruction_signal_uc_sp?.() ?? 0n).toString(16)}`,
        `sig_uc_a0=0x${(handle.kernel.debug_illegal_instruction_signal_uc_a0?.() ?? 0n).toString(16)}`,
      ].join(":")
    }]`;
  }

  function formatIllegalInstructionVmTraces(handle: ProcessHandle, currentVmPtr: number): string {
    const vmPtr = handle.kernel.debug_illegal_instruction_vm_ptr?.() ?? 0;
    if (vmPtr === 0 || vmPtr === currentVmPtr) {
      return "illegal_vm_traces=[]";
    }
    const frameBase = handle.kernel.debug_signal_delivery_for_vm_frame_base?.(vmPtr)
      ?? handle.kernel.debug_signal_delivery_frame_base?.()
      ?? 0n;
    const signalState = formatSignalState(handle, vmPtr);
    const signalFrame = formatSignalFrameState(handle, frameBase);
    const traces = formatFrozenExitTraces(handle, vmPtr);
    return `illegal_vm_traces=[vm=0x${vmPtr.toString(16)} ${signalState} ${signalFrame}${traces.length > 0 ? ` ${traces}` : ""}]`;
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

  function formatGuestWriteOverlapTrace(handle: ProcessHandle): string {
    const len = handle.kernel.debug_guest_write_overlap_trace_len?.() ?? 0;
    if (len <= 0) {
      return "guest_write_overlaps=[]";
    }
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const addr = handle.kernel.debug_guest_write_overlap_trace_entry_addr?.(index) ?? 0n;
      const pc = handle.kernel.debug_guest_write_overlap_trace_entry_pc?.(index) ?? 0n;
      const sample = handle.kernel.debug_guest_write_overlap_trace_entry_sample?.(index) ?? 0n;
      entries.push(
        `${index}:slot=${handle.kernel.debug_guest_write_overlap_trace_entry_slot?.(index) ?? 0}`
        + `:op=${handle.kernel.debug_guest_write_overlap_trace_entry_op?.(index) ?? 0}`
        + `:tid=${handle.kernel.debug_guest_write_overlap_trace_entry_tid?.(index) ?? 0}`
        + `:pc=0x${BigInt(pc).toString(16)}`
        + `:addr=0x${addr.toString(16)}`
        + `:len=${handle.kernel.debug_guest_write_overlap_trace_entry_len?.(index) ?? 0}`
        + `:sample_len=${handle.kernel.debug_guest_write_overlap_trace_entry_sample_len?.(index) ?? 0}`
        + `:sample=0x${sample.toString(16)}`
        + `:stage=${handle.kernel.debug_guest_write_overlap_trace_entry_execve_stage?.(index) ?? 0}`,
      );
    }
    return `guest_write_overlaps=[${entries.join(",")}]`;
  }

  function formatGuestRangePermitDeniedTrace(handle: ProcessHandle): string {
    const len = handle.kernel.debug_guest_range_permit_denied_trace_len?.() ?? 0;
    if (len <= 0) {
      return "guest_range_permit_denied=[]";
    }
    const entries: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const pc = handle.kernel.debug_guest_range_permit_denied_trace_entry_pc?.(index) ?? 0n;
      const start = handle.kernel.debug_guest_range_permit_denied_trace_entry_start?.(index) ?? 0n;
      const accessLen = handle.kernel.debug_guest_range_permit_denied_trace_entry_len?.(index) ?? 0n;
      const end = handle.kernel.debug_guest_range_permit_denied_trace_entry_end?.(index) ?? 0n;
      const guestLimit = handle.kernel.debug_guest_range_permit_denied_trace_entry_guest_limit?.(index) ?? 0n;
      const coverStart = handle.kernel.debug_guest_range_permit_denied_trace_entry_cover_start?.(index) ?? 0n;
      const coverEnd = handle.kernel.debug_guest_range_permit_denied_trace_entry_cover_end?.(index) ?? 0n;
      const a0 = handle.kernel.debug_guest_range_permit_denied_trace_entry_a0?.(index) ?? 0n;
      const a1 = handle.kernel.debug_guest_range_permit_denied_trace_entry_a1?.(index) ?? 0n;
      const a2 = handle.kernel.debug_guest_range_permit_denied_trace_entry_a2?.(index) ?? 0n;
      const a3 = handle.kernel.debug_guest_range_permit_denied_trace_entry_a3?.(index) ?? 0n;
      const a4 = handle.kernel.debug_guest_range_permit_denied_trace_entry_a4?.(index) ?? 0n;
      const a5 = handle.kernel.debug_guest_range_permit_denied_trace_entry_a5?.(index) ?? 0n;
      const s1 = handle.kernel.debug_guest_range_permit_denied_trace_entry_s1?.(index) ?? 0n;
      const s2 = handle.kernel.debug_guest_range_permit_denied_trace_entry_s2?.(index) ?? 0n;
      const s3 = handle.kernel.debug_guest_range_permit_denied_trace_entry_s3?.(index) ?? 0n;
      const s11 = handle.kernel.debug_guest_range_permit_denied_trace_entry_s11?.(index) ?? 0n;
      entries.push(
        `${index}:vm=0x${(handle.kernel.debug_guest_range_permit_denied_trace_entry_vm_ptr?.(index) ?? 0).toString(16)}`
        + `:tid=${handle.kernel.debug_guest_range_permit_denied_trace_entry_tid?.(index) ?? 0}`
        + `:pc=0x${pc.toString(16)}`
        + `:regs=a0=0x${a0.toString(16)}/a1=0x${a1.toString(16)}/a2=0x${a2.toString(16)}`
        + `/a3=0x${a3.toString(16)}/a4=0x${a4.toString(16)}/a5=0x${a5.toString(16)}`
        + `/s1=0x${s1.toString(16)}/s2=0x${s2.toString(16)}/s3=0x${s3.toString(16)}`
        + `/s11=0x${s11.toString(16)}`
        + `:start=0x${start.toString(16)}`
        + `:len=${accessLen.toString()}`
        + `:end=0x${end.toString(16)}`
        + `:limit=0x${guestLimit.toString(16)}`
        + `:prot=${handle.kernel.debug_guest_range_permit_denied_trace_entry_prot?.(index) ?? 0}`
        + `:cover=0x${coverStart.toString(16)}-0x${coverEnd.toString(16)}`
        + `/prot=${handle.kernel.debug_guest_range_permit_denied_trace_entry_cover_prot?.(index) ?? 0}`
        + `:reason=${handle.kernel.debug_guest_range_permit_denied_trace_entry_reason?.(index) ?? 0}`,
      );
    }
    return `guest_range_permit_denied=[${entries.join(",")}]`;
  }

  function formatFaultDispatchTrace(handle: ProcessHandle, vmPtr = handle.vmPtr): string {
    const phase = handle.kernel.debug_fault_dispatch_for_vm_phase?.(vmPtr) ?? 0;
    if (phase === 0) {
      return "fault_dispatch=[]";
    }
    const pc = handle.kernel.debug_fault_dispatch_for_vm_pc?.(vmPtr) ?? 0n;
    const ra = handle.kernel.debug_fault_dispatch_for_vm_ra?.(vmPtr) ?? 0n;
    const sp = handle.kernel.debug_fault_dispatch_for_vm_sp?.(vmPtr) ?? 0n;
    const s11 = handle.kernel.debug_fault_dispatch_for_vm_s11?.(vmPtr) ?? 0n;
    const faultAddr = handle.kernel.debug_fault_dispatch_for_vm_addr?.(vmPtr) ?? 0n;
    return [
      "fault_dispatch=[",
      `tid=${handle.kernel.debug_fault_dispatch_for_vm_tid?.(vmPtr) ?? 0}`,
      `:phase=${phase}`,
      `:pc=0x${pc.toString(16)}`,
      `:ra=0x${ra.toString(16)}`,
      `:sp=0x${sp.toString(16)}`,
      `:s11=0x${s11.toString(16)}`,
      `:addr=0x${faultAddr.toString(16)}`,
      "]",
    ].join("");
  }

  function formatExitTrace(handle: ProcessHandle, vmPtr = handle.vmPtr): string {
    const execveStage = handle.kernel.debug_execve_stage?.() ?? 0;
    const execveValue0 = handle.kernel.debug_execve_value0?.() ?? 0n;
    const execveValue1 = handle.kernel.debug_execve_value1?.() ?? 0n;
    const debugGuestByteSources = formatDebugGuestByteSources(handle);
    const debugMemfsFiles = formatDebugMemfsFiles(handle);
    const exitMemoryVmas = formatExitMemoryVmas(handle);
    return [
      `vm=0x${vmPtr.toString(16)}`,
      `fault_state=[${formatFaultState(handle, vmPtr)}]`,
      `fault_insn=[${formatFaultInstructions(handle, vmPtr)}]`,
      `fault_bounds=[${formatFaultBounds(handle, vmPtr)}]`,
      `fault_vma=[${formatFaultVmas(handle, vmPtr)}]`,
      `fault_bytes=[${formatFaultByteSources(handle, vmPtr)}]`,
      ...(exitMemoryVmas === null ? [] : [exitMemoryVmas]),
      ...(debugGuestByteSources === null ? [] : [debugGuestByteSources]),
      ...(debugMemfsFiles === null ? [] : [debugMemfsFiles]),
      formatDefaultSignalTrace(handle),
      formatSyncFaultTrace(handle),
      formatSigreturnTrace(handle),
      formatIllegalInstructionTrace(handle),
      formatIllegalInstructionVmTraces(handle, vmPtr),
      formatSignalState(handle, vmPtr),
      formatSignalSendTrace(handle),
      formatSignalDeliveryTrace(handle),
      formatGuestWriteOverlapTrace(handle),
      formatGuestRangePermitDeniedTrace(handle),
      formatFaultDispatchTrace(handle, vmPtr),
      formatFrozenExitTraces(handle, vmPtr),
      `execve_stage=${execveStage}`,
      `execve_v0=${execveValue0.toString()}`,
      `execve_v1=${execveValue1.toString()}`,
      `execve_stage_trace=[${formatExecveStageTrace(handle)}]`,
      `exit_fds=[${formatFdSummary(handle)}]`,
      `riscv_syscalls=[${formatRiscvSyscalls(handle)}]`,
      `all_riscv_syscalls=[${formatAllRiscvSyscalls(handle)}]`,
      `watched_riscv_flush_icache=[${formatWatchedRiscvFlushIcache(handle)}]`,
      `path_syscalls=[${formatPathSyscalls(handle)}]`,
      `all_path_syscalls=[${formatAllPathSyscalls(handle)}]`,
      `fd_syscalls=[${formatFdSyscalls(handle, null, 256, vmPtr)}]`,
      `fd_syscalls_all_vm=[${formatFdSyscalls(handle, null, 512, null)}]`,
      `fd_focus_state=[${formatFdFocusState(handle, vmPtr, [10, 12])}]`,
      `fd_syscalls_focus_all=[${formatFocusedFdSyscalls(handle, [10, 12])}]`,
      `fd_syscalls_focus_strict=[${formatStrictFocusedFdSyscalls(handle, [10, 12])}]`,
      `memory_syscalls=[${formatMemorySyscalls(handle)}]`,
      `watched_memory_syscalls=[${formatWatchedMemorySyscalls(handle, vmPtr)}]`,
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

  function parentHasBlockedWait4(parent: ProcessHandle): boolean {
    for (const blocked of parent.blockedWorkers.values()) {
      if (blocked.status === parent.kernel.abi_status_wait4()) {
        return true;
      }
    }
    return false;
  }

  function parentHasBlockedPipeWait(parent: ProcessHandle): boolean {
    for (const blocked of parent.blockedWorkers.values()) {
      if (blocked.status === parent.kernel.abi_status_pipe_wait()) {
        return true;
      }
    }
    return false;
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
    if (handle.processMemfsDirty || handle.processMemfsPublishPending) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `exit-parent-early-prepublish-skip:${handle.pid}:parent=${parent.pid}:dirty-memfs=1:publish-pending=${handle.processMemfsPublishPending ? 1 : 0}`,
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

  function exitProcessThreadsAreGone(handle: ProcessHandle): boolean {
    try {
      const threadCount = handle.kernel.thread_process_thread_count?.(handle.vmPtr);
      return threadCount !== undefined && Number(threadCount) <= 0;
    } catch {
      return false;
    }
  }

  function exitProcessThreadCount(handle: ProcessHandle): number | null {
    try {
      const threadCount = handle.kernel.thread_process_thread_count?.(handle.vmPtr);
      if (threadCount === undefined) {
        return null;
      }
      const numeric = Number(threadCount);
      return Number.isFinite(numeric) ? numeric : null;
    } catch {
      return null;
    }
  }

  function exitProcessHasActiveOwnerWork(handle: ProcessHandle): boolean {
    return handle.runningWorker !== null
      || handle.readyWorkers.length > 0
      || handle.blockedWorkers.size > 0;
  }

  function exitProcessHasNoLiveThreadSlots(handle: ProcessHandle): boolean {
    if (exitProcessHasActiveOwnerWork(handle)) {
      return false;
    }
    const threadCount = exitProcessThreadCount(handle);
    if (threadCount === null || threadCount <= 0) {
      return false;
    }
    let maxThreads = 0;
    try {
      maxThreads = Number(handle.kernel.abi_max_threads?.() ?? 0);
    } catch {
      return false;
    }
    if (!Number.isFinite(maxThreads) || maxThreads <= 0) {
      return false;
    }
    for (let slot = 0; slot < maxThreads; slot += 1) {
      try {
        if ((handle.kernel.process_thread_vm_ptr?.(handle.vmPtr, slot) ?? 0) !== 0) {
          return false;
        }
      } catch {
        return false;
      }
    }
    return true;
  }

  function exitProcessThreadSlotsAreTerminal(handle: ProcessHandle): boolean {
    if (exitProcessHasActiveOwnerWork(handle)) {
      return false;
    }

    let faultStatus: number;
    try {
      faultStatus = handle.kernel.abi_status_fault?.() ?? -1;
    } catch {
      return false;
    }
    if (faultStatus < 0) {
      return false;
    }

    let anchorFaulted = false;
    try {
      anchorFaulted = handle.kernel.thread_status?.(handle.vmPtr) === faultStatus;
    } catch {
      return false;
    }
    if (!anchorFaulted) {
      return false;
    }

    let maxThreads = 0;
    try {
      maxThreads = Number(handle.kernel.abi_max_threads?.() ?? 0);
    } catch {
      return false;
    }
    if (!Number.isFinite(maxThreads) || maxThreads <= 0) {
      return false;
    }

    let sawThreadSlot = false;
    for (let slot = 0; slot < maxThreads; slot += 1) {
      let threadVmPtr = 0;
      try {
        threadVmPtr = handle.kernel.process_thread_vm_ptr?.(handle.vmPtr, slot) ?? 0;
      } catch {
        return false;
      }
      if (threadVmPtr === 0) {
        continue;
      }
      sawThreadSlot = true;
      try {
        if (handle.kernel.thread_status?.(threadVmPtr) !== faultStatus) {
          return false;
        }
      } catch {
        return false;
      }
    }

    return sawThreadSlot;
  }

  async function publishProcessFsStateOnExit(handle: ProcessHandle, targets: ProcessHandle[] = []): Promise<void> {
    if (!deps.snapshotProcessFs || !deps.publishRuntimeFsState) {
      return;
    }
    if (!handle.processMemfsDirty && !handle.processMemfsPublishPending) {
      return;
    }
    try {
      if (handle.kernelWorkerRequest && handle.pid > 0) {
        if (exitProcessThreadsAreGone(handle)) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `exit-fs-publish-local-no-threads:${handle.pid}`,
          );
        } else if (exitProcessHasNoLiveThreadSlots(handle)) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `exit-fs-publish-local-no-live-thread-slots:${handle.pid}`,
          );
        } else if (exitProcessThreadSlotsAreTerminal(handle)) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `exit-fs-publish-local-terminal-threads:${handle.pid}`,
          );
        } else {
          await deps.syncForkSharedStateViaKernelWorker?.(handle, targets, {
            cause: { kind: "syscall", nr: -1n, name: "process-exit-memfs-publication" },
            includeMemfs: true,
            memfsOnly: true,
            trackHeldOfds: false,
            sourceVmPtr: handle.vmPtr,
          });
        }
      }
      const entries = deps.snapshotProcessFs(handle);
      deps.publishRuntimeFsState(handle.runtimeId, entries);
      handle.processMemfsDirty = false;
      handle.processMemfsPublishPending = false;
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
      if (parentHasBlockedWait4(parent) || parentHasBlockedPipeWait(parent)) {
        prepublishParentWaitStatus(handle, exitCode, termSignal);
      }
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `parent-pipe-sync-before-child-exit-start:${parent.pid}:${handle.pid}`);
      await deps.requestPipeStateSync(parent, true);
      deps.drainProcessOutput(parent);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `parent-pipe-sync-before-child-exit-done:${parent.pid}:${handle.pid}`);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-pipe-sync-start:${handle.pid}`);
      const childPipeSyncPromise = deps.requestPipeStateSync(handle, true)
        .then(() => {
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-pipe-sync-done:${handle.pid}`);
        })
        .catch((error) => {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `child-pipe-sync-error:${handle.pid}:${JSON.stringify(error instanceof Error ? error.message : String(error))}`,
          );
        });
      try {
        await childPipeSyncPromise;
      } catch {
        // Errors are recorded by childPipeSyncPromise so exit teardown can continue.
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
    if (!parentWaitStatusPrepublishedHandles.has(handle) && parentHasBlockedWait4(parent)) {
      prepublishParentWaitStatus(handle, exitCode, termSignal);
    }
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `parent-record-start:${parent.pid}:${handle.pid}`);
    const recordPromise = deps.recordChildExitViaKernelWorker(parent, handle.pid, waitStatus);
    if (parentWaitStatusPrepublishedHandles.has(handle)) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `parent-record-deferred:${parent.pid}:${handle.pid}`);
      void recordPromise
        .then(async () => {
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `parent-record-done:${parent.pid}:${handle.pid}`);
          if (!hasInheritedParentOutput) {
            await settleVisibleProcessOutput(parent, deps.requestPipeStateSync);
          }
        })
        .catch((error) => {
          const failure = error instanceof Error ? error : new Error(String(error));
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `parent-record-error:${parent.pid}:${handle.pid}:${JSON.stringify(failure.message)}`,
          );
          failProcess(parent, failure);
        });
      return waitStatus;
    }
    await recordPromise;
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

  async function settleVisibleInheritedChildrenOutput(handle: ProcessHandle, maxPasses = 6): Promise<void> {
    if (handle.stdioOwner !== null) {
      return;
    }
    for (let pass = 0; pass < maxPasses; pass += 1) {
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
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    const remainingChildren = visibleInheritedChildren(handle);
    if (remainingChildren.length > 0) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `visible-inherited-child-live-give-up:${handle.pid}:${remainingChildren.map(formatVisibleChildState).join(".")}`,
      );
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
      } else if (handle.processMemfsDirty || handle.processMemfsPublishPending) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `exit-parent-prepublish-skip:${parent.pid}:${handle.pid}:dirty-memfs=1:publish-pending=${handle.processMemfsPublishPending ? 1 : 0}`,
        );
      }
    } else {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `exit-parent-prepublish-skip:${handle.pid}:parent=${handle.parentPid ?? 0}`,
      );
    }
    let cleanupError: Error | null = null;
    let parentWaitStatus: number | null = null;
    let exitNotified = false;
    const notifyExit = (): void => {
      if (exitNotified) {
        return;
      }
      exitNotified = true;
      const onExit = handle.onExit;
      handle.onExit = undefined;
      onExit?.(exitCode);
    };

    try {
      parentWaitStatus = await recordExitOnParent(handle, exitCode, termSignal);
      const parentWaitStatusPublishedAfterRecord = parentWaitStatusPublished
        || parentWaitStatusPrepublishedHandles.has(handle);
      if (parent && parentWaitStatus !== null && !parentWaitStatusPublishedAfterRecord) {
        parent.childExitSyncRecords.set(handle.pid, parentWaitStatus);
        if (parent.pendingChildExitRecords > 0) {
          parent.pendingChildExitRecords -= 1;
        }
        invalidateBlockedParentRpcsForChildExit(parent, handle.pid);
        parentWaitStatus = null;
        deps.kickBlockedWorkers(parent);
      } else if (parentWaitStatusPublishedAfterRecord) {
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
      notifyExit();
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
      notifyExit();
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
      notifyExit();
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
          invalidateBlockedParentRpcsForChildExit(parent, handle.pid);
        }
        if (parent.pendingChildExitRecords > 0) {
          parent.pendingChildExitRecords -= 1;
        }
        deps.kickBlockedWorkers(parent);
      }
    }
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
