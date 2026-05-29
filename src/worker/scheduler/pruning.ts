import {
  collectLiveThreadVmPtrs,
} from "../live-threads.js";
import {
  releaseWorker,
} from "../pool.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";

export interface PruningSchedulerDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  clearBlockedWorker(handle: ProcessHandle, worker: Worker): void;
}

export interface PruningScheduler {
  selectProcessAnchorVmPtr(handle: ProcessHandle, excludedWorker?: Worker | null): number | null;
  pruneStaleProcessWorkers(handle: ProcessHandle, liveVmPtrs: Set<number>, currentWorker: Worker | null): void;
  pruneDetachedProcessWorkers(handle: ProcessHandle, anchorVmPtr: number, currentWorker: Worker | null): void;
}

export function createPruningScheduler(deps: PruningSchedulerDeps): PruningScheduler {
  function selectProcessAnchorVmPtr(
    handle: ProcessHandle,
    excludedWorker: Worker | null = null,
  ): number | null {
    const candidates: Worker[] = [];
    if (handle.runningWorker !== null) {
      candidates.push(handle.runningWorker);
    }
    candidates.push(...handle.readyWorkers, ...handle.workers);
    const seen = new Set<Worker>();
    for (const candidate of candidates) {
      if (candidate === excludedWorker || seen.has(candidate)) {
        continue;
      }
      seen.add(candidate);
      const vmPtr = deps.threadWorkerStates.get(candidate)?.vmPtr;
      if (vmPtr !== undefined && vmPtr !== 0) {
        return vmPtr;
      }
    }
    return null;
  }

  function removeReadyWorker(handle: ProcessHandle, worker: Worker): void {
    for (let index = handle.readyWorkers.length - 1; index >= 0; index -= 1) {
      if (handle.readyWorkers[index] === worker) {
        handle.readyWorkers.splice(index, 1);
      }
    }
  }

  function shouldPreserveTransientWorker(handle: ProcessHandle, vmPtr: number): boolean {
    const status = handle.kernel.thread_status(vmPtr);
    return status === handle.kernel.abi_status_vfork_wait()
      || status === handle.kernel.abi_status_execve_spawn()
      || status === handle.kernel.abi_status_clone_spawn();
  }

  function pruneStaleProcessWorkers(
    handle: ProcessHandle,
    liveVmPtrs: Set<number>,
    currentWorker: Worker | null,
  ): void {
    if (liveVmPtrs.size === 0) {
      return;
    }

    for (const worker of [...handle.workers]) {
      if (worker === currentWorker) {
        continue;
      }
      const thread = deps.threadWorkerStates.get(worker);
      if (!thread || liveVmPtrs.has(thread.vmPtr)) {
        continue;
      }
      if (shouldPreserveTransientWorker(handle, thread.vmPtr)) {
        deps.appendWorkerDebugLog(
          handle,
          `prune-skip-transient:0x${thread.vmPtr.toString(16)}:${handle.kernel.thread_status(thread.vmPtr)}`,
        );
        continue;
      }
      deps.clearBlockedWorker(handle, worker);
      removeReadyWorker(handle, worker);
      if (handle.runningWorker === worker) {
        handle.runningWorker = null;
      }
      if (handle.vforkWorker === worker) {
        handle.vforkWorker = null;
      }
      deps.appendWorkerDebugLog(handle, `prune:0x${thread.vmPtr.toString(16)}`);
      handle.workers.delete(worker);
      deps.threadWorkerStates.delete(worker);
      releaseWorker(worker);
    }
  }

  function pruneDetachedProcessWorkers(
    handle: ProcessHandle,
    anchorVmPtr: number,
    currentWorker: Worker | null,
  ): void {
    if (anchorVmPtr === 0 || handle.workers.size <= 1) {
      return;
    }
    if (handle.pendingChildSpawnRecords > 0) {
      deps.appendWorkerDebugLog(handle, `prune-skip-spawn:${handle.pendingChildSpawnRecords}`);
      return;
    }
    const liveVmPtrs = collectLiveThreadVmPtrs(handle.kernel, anchorVmPtr);
    pruneStaleProcessWorkers(handle, liveVmPtrs, currentWorker);
  }

  return {
    selectProcessAnchorVmPtr,
    pruneStaleProcessWorkers,
    pruneDetachedProcessWorkers,
  };
}
