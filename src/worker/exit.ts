import { settleInheritedChildOutput, settleVisibleProcessOutput } from "./stdio.js";
import type { ProcessHandle } from "./types.js";

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
  recordChildExitViaKernelWorker(parent: ProcessHandle, childPid: number, waitStatus: number): Promise<void>;
  releaseSharedHostStdio(handle: ProcessHandle): Promise<void>;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  unregisterProcessWithKernelWorker(handle: ProcessHandle): Promise<void>;
}

export function createWorkerExitHelpers(deps: WorkerExitDeps) {
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

  function formatFdSyscalls(handle: ProcessHandle): string {
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
      if (vmPtr !== handle.vmPtr) {
        continue;
      }
      const nr = Number(handle.kernel.debug_fd_syscall_trace_nr?.(index) ?? -1n);
      if (![24, 25, 56, 57, 59, 62, 63, 64, 66, 67, 436].includes(nr)) {
        continue;
      }
      const arg0OfdType = handle.kernel.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0;
      const arg2OfdType = handle.kernel.debug_fd_syscall_trace_arg2_ofd_type?.(index) ?? 0;
      const pid = sampleToPid(handle.kernel.debug_fd_syscall_trace_pid_sample0?.(index));
      entries.push([
        `${index}:${nr}`,
        `pid=${pid}`,
        `a0=${handle.kernel.debug_fd_syscall_trace_arg0?.(index)?.toString() ?? "n/a"}`,
        `a0ofd=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? "n/a"}`,
        `a0ty=${arg0OfdType}`,
        `a1=${handle.kernel.debug_fd_syscall_trace_arg1?.(index)?.toString() ?? "n/a"}`,
        `a2=${handle.kernel.debug_fd_syscall_trace_arg2?.(index)?.toString() ?? "n/a"}`,
        `a2ofd=${handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? "n/a"}`,
        `a2ty=${arg2OfdType}`,
        `ret=${handle.kernel.debug_fd_syscall_trace_result?.(index)?.toString() ?? "n/a"}`,
        `st=${handle.kernel.debug_fd_syscall_trace_status?.(index) ?? "n/a"}`,
      ].join(":"));
    }
    return entries.slice(-256).join(",");
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

  function formatExitTrace(handle: ProcessHandle): string {
    const execveStage = handle.kernel.debug_execve_stage?.() ?? 0;
    const execveValue0 = handle.kernel.debug_execve_value0?.() ?? 0n;
    const execveValue1 = handle.kernel.debug_execve_value1?.() ?? 0n;
    return [
      `vm=0x${handle.vmPtr.toString(16)}`,
      `execve_stage=${execveStage}`,
      `execve_v0=${execveValue0.toString()}`,
      `execve_v1=${execveValue1.toString()}`,
      `execve_stage_trace=[${formatExecveStageTrace(handle)}]`,
      `exit_fds=[${formatFdSummary(handle)}]`,
      `riscv_syscalls=[${formatRiscvSyscalls(handle)}]`,
      `path_syscalls=[${formatPathSyscalls(handle)}]`,
      `fd_syscalls=[${formatFdSyscalls(handle)}]`,
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
    return [fdPipeSlot(handle, 1), fdPipeSlot(handle, 2)]
      .some((slot) => inheritedSlots.has(slot));
  }

  async function recordExitOnParent(
    handle: ProcessHandle,
    exitCode: number,
    termSignal: number,
  ): Promise<void> {
    if (handle.parentPid === null || handle.pid <= 0) {
      return;
    }

    const parent = deps.processHandles.get(handle.parentPid);
    if (!parent) {
      return;
    }

    parent.pendingChildExitRecords += 1;
    try {
      const hasInheritedParentOutput = usesInheritedParentOutput(handle, parent);
      if (!hasInheritedParentOutput && handle.kernelWorkerRequest && handle.pid > 0) {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-pipe-sync-start:${handle.pid}`);
        await deps.requestPipeStateSync(handle, true);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-pipe-sync-done:${handle.pid}`);
      }
      if (handle.stdioOwner === null) {
        deps.drainProcessOutput(handle);
      } else if (!hasInheritedParentOutput) {
        deps.drainHiddenProcessOutput(handle);
      }

      deps.appendWorkerDebugLog(
        parent,
        `child-exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
      );
      deps.appendWorkerDebugLog(
        parent,
        `child-debug:${handle.pid}:${handle.lastDebugMessage ?? "debug=n/a"}`,
      );
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `child-debug:${handle.pid}:${(handle.lastDebugMessage ?? "debug=n/a").slice(0, 4096)}`,
      );
      parent.lastDebugMessage = [
        `child-exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
        `child-debug:${handle.pid}:${handle.lastDebugMessage ?? "debug=n/a"}`,
        parent.lastDebugMessage ?? "debug=n/a",
      ].join(" ");

      const waitStatus = encodeWaitStatus(exitCode, termSignal);
      parent.childExitSyncRecords.set(handle.pid, waitStatus);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `parent-record-start:${parent.pid}:${handle.pid}`);
      await deps.recordChildExitViaKernelWorker(parent, handle.pid, waitStatus);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `parent-record-done:${parent.pid}:${handle.pid}`);
      if (hasInheritedParentOutput) {
        void settleInheritedChildOutput(handle, deps.requestPipeStateSync)
          .then(() => {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-parent-pipe-refresh:${parent.pid}:${handle.pid}`);
            deps.kickBlockedWorkers(parent);
          })
          .catch(() => {});
      }
      if (!hasInheritedParentOutput) {
        await settleVisibleProcessOutput(parent, deps.requestPipeStateSync);
      }
    } finally {
      if (parent.pendingChildExitRecords > 0) {
        parent.pendingChildExitRecords -= 1;
      }
      deps.kickBlockedWorkers(parent);
    }
  }

  async function completeProcessExit(
    handle: ProcessHandle,
    exitCode: number,
    termSignal: number,
  ): Promise<void> {
    handle.lastDebugMessage = [
      `exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
      `exit_trace=${formatExitTrace(handle)}`,
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
    if (exitCode !== 0 || termSignal !== 0) {
      deps.captureRuntimeFailureSnapshot(handle, exitCode, termSignal);
    }

    const parent = handle.parentPid !== null ? (deps.processHandles.get(handle.parentPid) ?? null) : null;
    const hasInheritedParentOutput = parent !== null && usesInheritedParentOutput(handle, parent);
    let cleanupError: Error | null = null;

    try {
      await recordExitOnParent(handle, exitCode, termSignal);
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
        await deps.unregisterProcessWithKernelWorker(handle);
      } catch (error) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `unregister-error:${handle.pid}:${JSON.stringify(error instanceof Error ? error.message : String(error))}`,
        );
      }
      deps.destroyProcessHandle(handle, termSignal || 15, false);
    }

    if (parent && !hasInheritedParentOutput) {
      try {
        await deps.requestPipeStateSync(parent, true);
        deps.drainProcessOutput(parent);
      } finally {
        deps.kickBlockedWorkers(parent);
      }
    } else if (parent) {
      deps.kickBlockedWorkers(parent);
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
    recordExitOnParent,
  };
}
