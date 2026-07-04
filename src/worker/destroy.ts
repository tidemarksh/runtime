import { clearWorkerMessageHandler } from "../bridge/compat.js";
import { detachKernelShare } from "./kernel-share.js";
import { removeWorkerFromPool } from "./pool.js";
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
    handle.exitInProgress = true;
    for (const worker of [...handle.hostBlockedWorkers.keys()]) {
      deps.clearBlockedWorker(handle, worker);
    }
    handle.vforkReadyWorkers.length = 0;
    handle.runningWorker = null;
    deps.clearVforkSnapshot(handle);

    const workers = new Set(handle.workers);
    workers.add(handle.worker);
    for (const worker of workers) {
      terminateTrackedWorker(worker);
    }
    handle.workers.clear();

    const hadLocalHostStdio = handle.stdinPipe !== 0 || handle.stdoutPipe !== 0 || handle.stderrPipe !== 0;
    void (async () => {
      try {
        if (hadLocalHostStdio) {
          await deps.releaseSharedHostStdio(handle);
        }
      } finally {
        await deps.unregisterProcessWithKernelWorker(handle);
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
      `destroy:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${signal}:${notifyExit ? 1 : 0}`,
    );
    const dirtyHandles = deps.runtimeFsDirtyHandles.get(handle.runtimeId);
    dirtyHandles?.delete(handle);
    if (dirtyHandles && dirtyHandles.size === 0) {
      deps.runtimeFsDirtyHandles.delete(handle.runtimeId);
    }

    if (notifyExit) {
      handle.onExit?.(128 + signal);
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
