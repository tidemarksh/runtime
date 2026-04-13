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
      if (handle.kernelWorkerRequest && handle.pid > 0) {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-pipe-sync-start:${handle.pid}`);
        await deps.requestPipeStateSync(handle, false);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `child-pipe-sync-done:${handle.pid}`);
      }
      if (handle.stdioOwner === null) {
        deps.drainProcessOutput(handle);
      } else {
        deps.drainHiddenProcessOutput(handle);
      }
      if (handle.stdioOwner === parent) {
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
      await settleVisibleProcessOutput(parent, deps.requestPipeStateSync);
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

    await recordExitOnParent(handle, exitCode, termSignal);
    await deps.releaseSharedHostStdio(handle);
    const parent = handle.parentPid !== null ? deps.processHandles.get(handle.parentPid) : null;
    deps.destroyProcessHandle(handle, termSignal || 15, false);
    if (parent) {
      await deps.requestPipeStateSync(parent, false);
      deps.drainProcessOutput(parent);
      deps.kickBlockedWorkers(parent);
    }
    handle.onExit?.(exitCode);
  }

  function failProcess(handle: ProcessHandle, error: Error): void {
    handle.lastDebugMessage = [
      error.message,
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
