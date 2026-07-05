import {
  collectLiveThreadVmPtrs,
} from "../live-threads.js";
import {
  releaseWorker,
} from "../pool.js";
import { pendingChildSpawnRecordCount } from "../pending-child-spawn-state.js";
import { supervisorProcessIdentityMatches } from "../process-identity.js";
import { supervisorProcessStatusIsLifecycleHandoff } from "../supervisor-status.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";

export interface PruningSchedulerDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
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
    candidates.push(...handle.vforkReadyWorkers, ...handle.workers);
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

  function removeVforkReadyWorker(handle: ProcessHandle, worker: Worker): void {
    for (let index = handle.vforkReadyWorkers.length - 1; index >= 0; index -= 1) {
      if (handle.vforkReadyWorkers[index] === worker) {
        handle.vforkReadyWorkers.splice(index, 1);
      }
    }
  }

  function shouldPreserveTransientWorker(handle: ProcessHandle, vmPtr: number): boolean {
    const status = handle.kernel.thread_status(vmPtr);
    return supervisorProcessStatusIsLifecycleHandoff(handle.kernel, status);
  }

  function shouldPreserveProcessOwnedWorker(handle: ProcessHandle, vmPtr: number): boolean {
    try {
      const processPtr = handle.kernel.thread_process_ptr(handle.vmPtr);
      if (processPtr === 0 || !supervisorProcessIdentityMatches(handle.kernel, vmPtr, handle.pid, processPtr)) {
        return false;
      }
      return handle.kernel.thread_status(vmPtr) !== handle.kernel.abi_status_fault();
    } catch {
      return false;
    }
  }

  function formatLiveVmPtrs(liveVmPtrs: Set<number>): string {
    return [...liveVmPtrs].map((vmPtr) => `0x${vmPtr.toString(16)}`).join(",");
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
      if (shouldPreserveProcessOwnedWorker(handle, thread.vmPtr)) {
        deps.appendWorkerDebugLog(
          handle,
          `prune-skip-process-owned:0x${thread.vmPtr.toString(16)}:${handle.kernel.thread_status(thread.vmPtr)}`,
        );
        continue;
      }
      deps.clearBlockedWorker(handle, worker);
      removeVforkReadyWorker(handle, worker);
      if (handle.runningWorker === worker) {
        handle.runningWorker = null;
      }
      if (handle.vforkWorker === worker) {
        handle.vforkWorker = null;
      }
      deps.appendWorkerDebugLog(handle, `prune:0x${thread.vmPtr.toString(16)}`);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `prune-delete:${handle.pid}:0x${thread.vmPtr.toString(16)}`
          + `:status=${handle.kernel.thread_status?.(thread.vmPtr) ?? "n/a"}`
          + `:live=[${formatLiveVmPtrs(liveVmPtrs)}]`,
      );
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
    const pendingChildSpawns = pendingChildSpawnRecordCount(handle);
    if (pendingChildSpawns > 0) {
      deps.appendWorkerDebugLog(handle, `prune-skip-spawn:${pendingChildSpawns}`);
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
