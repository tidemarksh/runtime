import {
  publishKernelRuntimeState,
} from "../../bridge/runtime-state.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";

export interface FutexSchedulerDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog?(handle: ProcessHandle, event: string): void;
  formatChildExitRecords(handle: ProcessHandle): string;
  childExitRecordsAlreadyApplied(handle: ProcessHandle, recordsKey: string): boolean;
  applyLocalChildExitRecords(handle: ProcessHandle): void;
}

export interface FutexScheduler {
  applyChildExitRecordsForWorkerLocalWaits(handle: ProcessHandle): void;
  reconcileResolvedWorkerLocalWaits(handle: ProcessHandle): Worker[];
  hasWorkerLocalWait(thread: ThreadWorkerState | undefined): boolean;
}

export interface FutexReconciliationDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendWorkerDebugLog?(handle: ProcessHandle, event: string): void;
}

export interface FutexReconciliation {
  reconcileResolvedWorkerLocalWaits(handle: ProcessHandle): Worker[];
  hasWorkerLocalWait(thread: ThreadWorkerState | undefined): boolean;
}

const BLOCKED_ADDR_NOT_FOUND = 0xffff_ffff_ffff_ffffn;

function threadRecordBlockedAddr(
  handle: ProcessHandle,
  vmPtr: number,
): bigint {
  return handle.kernel.thread_record_blocked_addr?.(vmPtr) ?? 0n;
}

function isKnownThreadRecordBlock(blockedAddr: bigint): boolean {
  return blockedAddr !== 0n && blockedAddr !== BLOCKED_ADDR_NOT_FOUND;
}

export function threadHasCanonicalFutexBlock(
  handle: ProcessHandle,
  vmPtr: number,
): boolean {
  const blockedAddr = threadRecordBlockedAddr(handle, vmPtr);
  if (!isKnownThreadRecordBlock(blockedAddr)) {
    return false;
  }
  if (handle.kernel.thread_status(vmPtr) === handle.kernel.abi_status_futex_wait()) {
    return true;
  }
  return (handle.kernel.thread_futex_wait_addr?.(vmPtr) ?? 0n) !== 0n;
}

export function clearStaleFutexRecordIfResolved(
  handle: ProcessHandle,
  vmPtr: number,
): boolean {
  const blockedAddr = threadRecordBlockedAddr(handle, vmPtr);
  if (!isKnownThreadRecordBlock(blockedAddr)) {
    return false;
  }
  if (handle.kernel.thread_status(vmPtr) !== handle.kernel.abi_status_ok()) {
    return false;
  }
  if ((handle.kernel.thread_futex_wait_addr?.(vmPtr) ?? 0n) !== 0n) {
    return false;
  }
  handle.kernel.thread_resume_futex_wait(vmPtr);
  return true;
}

export function createFutexReconciliation(deps: FutexReconciliationDeps): FutexReconciliation {
  function reconcileResolvedWorkerLocalWaits(handle: ProcessHandle): Worker[] {
    const reconciledWorkers: Worker[] = [];
    for (const worker of handle.workers) {
      if (handle.runningWorker === worker || handle.readyWorkers.includes(worker) || handle.blockedWorkers.has(worker)) {
        continue;
      }
      const thread = deps.threadWorkerStates.get(worker);
      const waitingStatus = thread?.waitingInWorkerStatus ?? null;
      if (
        thread === undefined
        || waitingStatus === null
        || thread.ownerAsyncStatus !== null
      ) {
        continue;
      }
      if (waitingStatus !== handle.kernel.abi_status_futex_wait()) {
        continue;
      }
      const currentStatus = handle.kernel.thread_status(thread.vmPtr);
      if (thread.waitingInWorkerReconcileDeferred) {
        thread.waitingInWorkerReconcileDeferred = false;
        if (currentStatus !== handle.kernel.abi_status_ok()) {
          continue;
        }
      }
      const staleRecordCleared = currentStatus === handle.kernel.abi_status_ok()
        && clearStaleFutexRecordIfResolved(handle, thread.vmPtr);
      if (
        currentStatus !== handle.kernel.abi_status_ok()
        || threadHasCanonicalFutexBlock(handle, thread.vmPtr)
      ) {
        continue;
      }
      thread.waitingInWorkerStatus = null;
      thread.waitingInWorkerReconcileDeferred = false;
      thread.runningGeneration = null;
      deps.appendWorkerDebugLog?.(
        handle,
        `wait-in-worker-reconciled:0x${thread.vmPtr.toString(16)}:${waitingStatus}->${currentStatus}`
          + `${staleRecordCleared ? ":stale-record-cleared" : ""}`,
      );
      reconciledWorkers.push(worker);
    }
    if (reconciledWorkers.length > 0) {
      handle.readyWorkers.unshift(...reconciledWorkers);
    }
    return reconciledWorkers;
  }

  function hasWorkerLocalWait(thread: ThreadWorkerState | undefined): boolean {
    return (thread?.waitingInWorkerStatus ?? null) !== null;
  }

  return {
    reconcileResolvedWorkerLocalWaits,
    hasWorkerLocalWait,
  };
}

export function createFutexScheduler(deps: FutexSchedulerDeps): FutexScheduler {
  const reconciliation = createFutexReconciliation(deps);

  function hasWorkerLocalFutexWait(handle: ProcessHandle): boolean {
    const futexWaitStatus = handle.kernel.abi_status_futex_wait();
    for (const worker of handle.workers) {
      const thread = deps.threadWorkerStates.get(worker);
      if (
        thread?.ownerPid === handle.pid
        && thread.waitingInWorkerStatus === futexWaitStatus
      ) {
        return true;
      }
    }
    return false;
  }

  function applyChildExitRecordsForWorkerLocalWaits(handle: ProcessHandle): void {
    if (handle.childExitSyncRecords.size === 0 || !hasWorkerLocalFutexWait(handle)) {
      return;
    }
    const recordsKey = deps.formatChildExitRecords(handle);
    if (deps.childExitRecordsAlreadyApplied(handle, recordsKey)) {
      return;
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `futex-wait-apply-child-records:${handle.pid}:${recordsKey}`,
    );
    deps.applyLocalChildExitRecords(handle);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
  }

  return {
    applyChildExitRecordsForWorkerLocalWaits,
    reconcileResolvedWorkerLocalWaits: reconciliation.reconcileResolvedWorkerLocalWaits,
    hasWorkerLocalWait: reconciliation.hasWorkerLocalWait,
  };
}
