import { hostExecutionState } from "./host-execution-state.js";
import {
  processStderrPipe,
  processStdinPipe,
  processStdoutPipe,
} from "./process-stdio-state.js";
import { clearWorkerMessageHandler } from "../bridge/compat.js";
import { detachKernelShare } from "./kernel-share.js";
import { removeWorkerFromPool } from "./pool.js";
import { processOnExit } from "./process-callback-state.js";
import { processDebugCommand } from "./process-debug-state.js";
import {
  clearProcessWorkers,
  processWorkerList,
} from "./process-worker-state.js";
import { setProcessExitInProgress } from "./process-lifecycle-state.js";
import type { ProcessHandle, ThreadWorkerState } from "./types.js";

interface WorkerDestroyDeps {
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  runtimeFsDirtyHandles: Map<number, Set<ProcessHandle>>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  clearBlockedWorker(handle: ProcessHandle, worker: Worker): void;
  clearVforkSnapshot(handle: ProcessHandle): void;
  releaseSharedHostStdio(handle: ProcessHandle): Promise<void>;
  unregisterProcessWithKernelWorker(handle: ProcessHandle): Promise<void>;
}

export function createWorkerDestroyHelpers(deps: WorkerDestroyDeps) {
  function terminateTrackedWorker(worker: Worker): void {
    removeWorkerFromPool(worker);
    deps.threadWorkerStates.delete(worker);
    clearWorkerMessageHandler(worker);
    worker.terminate();
  }

  function destroyProcessHandle(handle: ProcessHandle, signal: number, notifyExit: boolean): void {
    setProcessExitInProgress(handle, true);
    for (const worker of [...hostExecutionState(handle).hostBlockedWorkers.keys()]) {
      deps.clearBlockedWorker(handle, worker);
    }
    hostExecutionState(handle).vforkReadyWorkers.length = 0;
    hostExecutionState(handle).runningWorker = null;
    deps.clearVforkSnapshot(handle);

    const workers = new Set(processWorkerList(handle));
    workers.add(handle.worker);
    for (const worker of workers) {
      terminateTrackedWorker(worker);
    }
    clearProcessWorkers(handle);

    const hadLocalHostStdio = processStdinPipe(handle) !== 0 || processStdoutPipe(handle) !== 0 || processStderrPipe(handle) !== 0;
    void (async () => {
      try {
        if (hadLocalHostStdio) {
          await deps.releaseSharedHostStdio(handle);
        }
      } finally {
        if (handle.kernelWorkerRequest && handle.pid > 0) {
          await deps.unregisterProcessWithKernelWorker(handle);
        }
      }
    })().catch((error) => {
      const message = error instanceof Error ? error.message : String(error);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `destroy-kernel-worker-cleanup-error:${handle.pid}:${JSON.stringify(message)}`);
    });
    detachKernelShare(handle);
    if (handle.pid > 0 && deps.processHandles.get(handle.pid) === handle) {
      deps.processHandles.delete(handle.pid);
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `destroy:${handle.pid}:${JSON.stringify(processDebugCommand(handle))}:${signal}:${notifyExit ? 1 : 0}`,
    );
    const dirtyHandles = deps.runtimeFsDirtyHandles.get(handle.runtimeId);
    dirtyHandles?.delete(handle);
    if (dirtyHandles && dirtyHandles.size === 0) {
      deps.runtimeFsDirtyHandles.delete(handle.runtimeId);
    }

    if (notifyExit) {
      processOnExit(handle)?.(128 + signal);
    }
  }

  function destroyRuntimeProcesses(runtimeId: number, signal = 15): void {
    for (const handle of [...deps.processHandles.values()]) {
      if (handle.runtimeId !== runtimeId) {
        continue;
      }
      destroyProcessHandle(handle, signal, true);
    }
  }

  function destroyProcessTree(rootPid: number, runtimeId: number, signal = 15): void {
    const pending = [rootPid];
    const seen = new Set<number>();

    while (pending.length > 0) {
      const pid = pending.pop()!;
      if (seen.has(pid)) {
        continue;
      }
      seen.add(pid);
      for (const handle of deps.processHandles.values()) {
        if (handle.runtimeId !== runtimeId) {
          continue;
        }
        if (handle.pid === pid) {
          destroyProcessHandle(handle, signal, true);
        } else if (handle.parentPid === pid) {
          pending.push(handle.pid);
        }
      }
    }
  }

  return {
    destroyProcessHandle,
    destroyProcessTree,
    destroyRuntimeProcesses,
  };
}
