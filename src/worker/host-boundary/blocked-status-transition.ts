import {
  supervisorBlockedPipeWaitHasReadyEvents,
  supervisorBlockedStatusTransitionAction,
} from "../../supervisor-blocked-resume.js";
import type { SyncCause } from "../contracts/sync.js";
import type { KernelRpcResumeOutcome } from "../kernel-rpc/outcomes.js";
import type {
  HostBlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import {
  beginBlockedRpc,
  finishBlockedRpc,
} from "../host-blocked-state.js";

interface BlockedStatusTransitionDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void;
  clearBlockedWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  notifyWorkerStatusProgress(handle: ProcessHandle): void;
  pumpProcess(handle: ProcessHandle): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  requestSocketStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  syncBlockedForkSharedStateViaKernelWorker(
    handle: ProcessHandle,
    sourceVmPtr: number | undefined,
    cause: SyncCause,
  ): Promise<void>;
}

function blockedRpcSyncCause(status: number): SyncCause {
  return { kind: "blocked-rpc", status };
}

function beginStatusTransitionRpc(blocked: HostBlockedWorkerState): number {
  return beginBlockedRpc(blocked);
}

function finishStatusTransitionRpc(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  rpcGeneration: number,
): HostBlockedWorkerState | null {
  const latestBlocked = handle.hostBlockedWorkers.get(worker);
  if (!latestBlocked || latestBlocked.vmPtr !== vmPtr) {
    return null;
  }
  finishBlockedRpc(latestBlocked, rpcGeneration);
  return latestBlocked;
}

function blockedOutcome(status: number): KernelRpcResumeOutcome {
  return { kind: "blocked", status };
}

export async function executeHostBlockedStatusTransition(
  deps: BlockedStatusTransitionDeps,
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  status: number,
): Promise<KernelRpcResumeOutcome> {
  const transitionAction = supervisorBlockedStatusTransitionAction(
    handle.kernel,
    vmPtr,
    status,
    {
      kernelWorkerAvailable: !!handle.kernelWorkerRequest && handle.pid > 0,
      networkConnectionsAvailable: handle.networkConnections.size > 0,
    },
  );

  if (transitionAction === "sync-pipe") {
    if (handle.runningWorker === worker) {
      handle.runningWorker = null;
    }
  }

  deps.blockWorker(handle, worker, status, vmPtr);

  if (transitionAction === "sync-pipe") {
    if (handle.runningWorker === worker) {
      handle.runningWorker = null;
    }
  }

  if (transitionAction === "block") {
    deps.pumpProcess(handle);
    return blockedOutcome(status);
  }

  const blocked = handle.hostBlockedWorkers.get(worker);
  const rpcGeneration = blocked ? beginStatusTransitionRpc(blocked) : -1;
  const finish = (): HostBlockedWorkerState | null =>
    finishStatusTransitionRpc(handle, worker, vmPtr, rpcGeneration);

  try {
    switch (transitionAction) {
      case "sync-socket":
        await deps.requestSocketStateSync(handle, true);
        finish();
        deps.notifyWorkerStatusProgress(handle);
        deps.pumpProcess(handle);
        return blockedOutcome(status);
      case "sync-pipe": {
        await deps.requestPipeStateSync(handle, true);
        const latestBlocked = finish();
        if (!latestBlocked) {
          return { kind: "stale" };
        }
        deps.notifyWorkerStatusProgress(handle);
        deps.pumpProcess(handle);
        return blockedOutcome(status);
      }
      case "sync-fork-shared":
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-start:${handle.pid}:status=${status}`);
        await deps.syncBlockedForkSharedStateViaKernelWorker(handle, vmPtr, blockedRpcSyncCause(status));
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-done:${handle.pid}:status=${status}`);
        finish();
        if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
          deps.clearBlockedWorker(handle, worker);
          return { kind: "process-gone" };
        }
        deps.notifyWorkerStatusProgress(handle);
        deps.pumpProcess(handle);
        return blockedOutcome(status);
      case "sync-pipe-and-fork-shared":
      case "sync-socket-and-fork-shared": {
        if (transitionAction === "sync-pipe-and-fork-shared") {
          await deps.requestPipeStateSync(handle, false);
        } else {
          await deps.requestSocketStateSync(handle, true);
        }
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-start:${handle.pid}:status=${status}`);
        await deps.syncBlockedForkSharedStateViaKernelWorker(handle, vmPtr, blockedRpcSyncCause(status));
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-done:${handle.pid}:status=${status}`);
        finish();
        if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
          deps.clearBlockedWorker(handle, worker);
          return { kind: "process-gone" };
        }
        if (
          transitionAction === "sync-pipe-and-fork-shared"
          && !supervisorBlockedPipeWaitHasReadyEvents(handle.kernel, vmPtr)
        ) {
          deps.pumpProcess(handle);
          return blockedOutcome(status);
        }
        deps.notifyWorkerStatusProgress(handle);
        deps.pumpProcess(handle);
        return blockedOutcome(status);
      }
      default: {
        const exhaustive: never = transitionAction;
        throw new Error(`unhandled supervisor blocked status transition action: ${exhaustive}`);
      }
    }
  } catch (error) {
    finish();
    throw error;
  }
}
