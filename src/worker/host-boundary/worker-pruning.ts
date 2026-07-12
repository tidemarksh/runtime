import { hostExecutionState } from "../host-execution-state.js";
import {
  collectLiveThreadVmPtrs,
} from "../live-threads.js";
import {
  releaseWorker,
} from "../pool.js";
import { pendingChildSpawnRecordCount } from "../pending-child-spawn-state.js";
import {
  deleteProcessWorker,
  processWorkerCount,
  processWorkerList,
} from "../process-worker-state.js";
import { supervisorProcessIdentityMatches } from "../process-identity.js";
import { supervisorProcessStatusIsLifecycleHandoff } from "../supervisor-status.js";
import { appendPendingExitedThreadForNextStep } from "../thread-step-payload.js";
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
    const runningWorker = hostExecutionState(handle).runningWorker;
    if (runningWorker !== null) {
      candidates.push(runningWorker);
    }
    candidates.push(...hostExecutionState(handle).vforkReadyWorkers, ...processWorkerList(handle));
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
    for (let index = hostExecutionState(handle).vforkReadyWorkers.length - 1; index >= 0; index -= 1) {
      if (hostExecutionState(handle).vforkReadyWorkers[index] === worker) {
        hostExecutionState(handle).vforkReadyWorkers.splice(index, 1);
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

    for (const worker of processWorkerList(handle)) {
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
        deps.clearBlockedWorker(handle, worker);
        deps.appendWorkerDebugLog(
          handle,
          `prune-skip-process-owned:0x${thread.vmPtr.toString(16)}:${handle.kernel.thread_status(thread.vmPtr)}`,
        );
        continue;
      }
      deps.clearBlockedWorker(handle, worker);
      const exitedTid = handle.kernel.thread_tid?.(thread.vmPtr) ?? 0;
      if (handle.kernel.thread_status(thread.vmPtr) === handle.kernel.abi_status_fault() && exitedTid > 0) {
        for (const remainingWorker of processWorkerList(handle)) {
          if (remainingWorker === worker) {
            continue;
          }
          const remainingThread = deps.threadWorkerStates.get(remainingWorker);
          if (remainingThread !== undefined) {
            appendPendingExitedThreadForNextStep(remainingThread, {
              vmPtr: thread.vmPtr,
              tid: exitedTid,
            });
          }
        }
      }
      removeVforkReadyWorker(handle, worker);
      if (hostExecutionState(handle).runningWorker === worker) {
        hostExecutionState(handle).runningWorker = null;
      }
      if (hostExecutionState(handle).vforkWorker === worker) {
        hostExecutionState(handle).vforkWorker = null;
      }
      deps.appendWorkerDebugLog(handle, `prune:0x${thread.vmPtr.toString(16)}`);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `prune-delete:${handle.pid}:0x${thread.vmPtr.toString(16)}`
          + `:status=${handle.kernel.thread_status?.(thread.vmPtr) ?? "n/a"}`
          + `:live=[${formatLiveVmPtrs(liveVmPtrs)}]`,
      );
      deleteProcessWorker(handle, worker);
      deps.threadWorkerStates.delete(worker);
      releaseWorker(worker);
    }
  }

  function pruneDetachedProcessWorkers(
    handle: ProcessHandle,
    anchorVmPtr: number,
    currentWorker: Worker | null,
  ): void {
    if (anchorVmPtr === 0 || processWorkerCount(handle) <= 1) {
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
