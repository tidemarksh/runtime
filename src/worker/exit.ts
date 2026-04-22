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
      const status = handle.kernel.debug_path_syscall_trace_status?.(index) ?? 0;
      const result = handle.kernel.debug_path_syscall_trace_result?.(index);
      const pathPtr = handle.kernel.debug_path_syscall_trace_path_ptr?.(index) ?? 0;
      entries.push(
        `${index}:${nr?.toString() ?? "n/a"}:${status}:${result?.toString() ?? "n/a"}:${JSON.stringify(readAbsoluteCString(handle, pathPtr))}`,
      );
    }
    return entries.slice(-16).join(",");
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
      `riscv_syscalls=[${formatRiscvSyscalls(handle)}]`,
      `path_syscalls=[${formatPathSyscalls(handle)}]`,
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
      const usesInheritedParentOutput = handle.stdioOwner === parent;
      if (!usesInheritedParentOutput && handle.kernelWorkerRequest && handle.pid > 0) {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-pipe-sync-start:${handle.pid}`);
        await deps.requestPipeStateSync(handle, false);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-pipe-sync-done:${handle.pid}`);
      }
      if (handle.stdioOwner === null) {
        deps.drainProcessOutput(handle);
      } else if (!usesInheritedParentOutput) {
        deps.drainHiddenProcessOutput(handle);
      }
      if (usesInheritedParentOutput) {
        await settleInheritedChildOutput(handle, deps.requestPipeStateSync);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-parent-pipe-refresh:${parent.pid}:${handle.pid}`);
      }

      deps.appendWorkerDebugLog(
        parent,
        `child-exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
      );
      deps.appendWorkerDebugLog(
        parent,
        `child-debug:${handle.pid}:${handle.lastDebugMessage ?? "debug=n/a"}`,
      );
      parent.lastDebugMessage = [
        `child-exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
        `child-debug:${handle.pid}:${handle.lastDebugMessage ?? "debug=n/a"}`,
        parent.lastDebugMessage ?? "debug=n/a",
      ].join(" ");

      const waitStatus = encodeWaitStatus(exitCode, termSignal);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `parent-record-start:${parent.pid}:${handle.pid}`);
      await deps.recordChildExitViaKernelWorker(parent, handle.pid, waitStatus);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `parent-record-done:${parent.pid}:${handle.pid}`);
      if (!usesInheritedParentOutput) {
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

    const parent = handle.parentPid !== null ? deps.processHandles.get(handle.parentPid) : null;
    const usesInheritedParentOutput = parent !== null && handle.stdioOwner === parent;
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
      deps.destroyProcessHandle(handle, termSignal || 15, false);
    }

    if (parent && !usesInheritedParentOutput) {
      try {
        await deps.requestPipeStateSync(parent, false);
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
