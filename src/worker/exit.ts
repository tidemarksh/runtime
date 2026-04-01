import type { ProcessHandle } from "./types.js";

interface WorkerExitDeps {
  processHandles: Map<number, ProcessHandle>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  debugRuntimeProcessHandles(runtimeId: number): string;
  destroyProcessHandle(handle: ProcessHandle, signal: number, notifyExit: boolean): void;
  formatAllLiveThreadDebug(handle: ProcessHandle): string;
  recordChildExitViaKernelWorker(parent: ProcessHandle, childPid: number, waitStatus: number): Promise<void>;
}

export function createWorkerExitHelpers(deps: WorkerExitDeps) {
  function encodeWaitStatus(exitCode: number, termSignal: number): number {
    if (termSignal !== 0) {
      return (termSignal & 0x7f) | 0x80;
    }
    return (exitCode & 0xff) << 8;
  }

  function recordExitOnParent(handle: ProcessHandle, exitCode: number, termSignal: number): void {
    if (handle.parentPid === null || handle.pid <= 0) {
      return;
    }

    const parent = deps.processHandles.get(handle.parentPid);
    if (!parent) {
      return;
    }

    deps.appendWorkerDebugLog(
      parent,
      `child-exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
    );
    if (exitCode !== 0 || termSignal !== 0) {
      deps.appendWorkerDebugLog(
        parent,
        `child-debug:${handle.pid}:${handle.lastDebugMessage ?? "debug=n/a"}`,
      );
    }
    parent.lastDebugMessage = [
      `child-exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
      ...(exitCode !== 0 || termSignal !== 0
        ? [`child-debug:${handle.pid}:${handle.lastDebugMessage ?? "debug=n/a"}`]
        : []),
      parent.lastDebugMessage ?? "debug=n/a",
    ].join(" ");

    const waitStatus = encodeWaitStatus(exitCode, termSignal);
    void deps.recordChildExitViaKernelWorker(parent, handle.pid, waitStatus);
  }

  async function completeProcessExit(
    handle: ProcessHandle,
    exitCode: number,
    termSignal: number,
  ): Promise<void> {
    handle.lastDebugMessage = [
      `exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
      handle.lastDebugMessage ?? "debug=n/a",
    ].join(" ");
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
    );

    recordExitOnParent(handle, exitCode, termSignal);
    deps.destroyProcessHandle(handle, termSignal || 15, false);
    handle.onExit?.(exitCode);
  }

  function failProcess(handle: ProcessHandle, error: Error): void {
    handle.lastDebugMessage = [
      error.message,
      `live_processes=${deps.debugRuntimeProcessHandles(handle.runtimeId)}`,
      `cmd=${JSON.stringify(handle.debugCommand)}`,
      `worker_log=[${handle.workerDebugLog.join(", ")}]`,
      `thread_debugs=[${deps.formatAllLiveThreadDebug(handle)}]`,
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
