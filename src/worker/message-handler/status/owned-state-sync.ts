import { hostExecutionState } from "../../host-execution-state.js";
import { processHasKernelWorkerOwnership } from "../../kernel-worker-ownership.js";
import { collectHeldEpollOfdIds } from "../../ownership/ofd-table.js";
import {
  kernelShareOtherMembers,
  kernelShareMemberList,
  kernelShareSharedOfdIds,
  reconcileKernelSharePipeControl,
} from "../../kernel-share.js";
import { kernelWorkerOwnsEpollState } from "../../ownership/epoll-control.js";
import { shouldRequestForkSharedStateForEffect } from "../../ownership/ofd-peer.js";
import { supervisorVforkChildOwnerMismatch } from "../../process-identity.js";
import type { ProcessHandle, ThreadWorkerState } from "../../types.js";
import type { WorkerMessageHandlerDeps } from "../types.js";
import { createWorkerOwnershipGuard } from "../../ownership/process-state.js";
import { resolveWorkerMessageHandlerSyncEffects } from "../sync-effects.js";
import { requestOwnedForkSharedStateSync } from "../../host-boundary/fork-shared-state-sync.js";
import {
  isProcessMemfsDirty,
  setProcessMemfsDirty,
} from "../../process-memfs-state.js";
import {
  clearThreadOwnerAsyncStatus,
  setThreadOwnerAsyncStatus,
} from "../../thread-host-state.js";
import {
  supervisorOwnedStateSyncActions,
} from "../../../supervisor-blocked-resume.js";

interface OwnedStateSyncArgs {
  handle: ProcessHandle;
  worker: Worker;
  thread: ThreadWorkerState;
  status: number;
  completedSyncEffects: number;
  isRunningWorker: boolean;
  wroteToPipe: boolean;
  drainedPipe: boolean;
  continueAfterOwnedStateSync(): void;
}

export function createWorkerOwnedStateSyncHandler(deps: WorkerMessageHandlerDeps) {
  const syncEffects = resolveWorkerMessageHandlerSyncEffects(deps);
  return function maybeHandleOwnedStateSync({
    handle,
    worker,
    thread,
    status,
    completedSyncEffects,
    isRunningWorker,
    wroteToPipe,
    drainedPipe,
    continueAfterOwnedStateSync,
  }: OwnedStateSyncArgs): boolean {
    const workerStillOwnedByHandle = createWorkerOwnershipGuard(
      handle,
      worker,
      thread,
      deps.threadWorkerStates,
    );
    const workerOwnsAuthoritativeState = isRunningWorker || workerStillOwnedByHandle();
    const hasForkSharedEffect = (completedSyncEffects & syncEffects.syncEffectForkSharedOfdState) !== 0;
    const forkSharedHasTrackedState = kernelShareSharedOfdIds(handle).size > 0
      || kernelShareOtherMembers(handle).length > 0;
    const forkSharedEffectRequested = shouldRequestForkSharedStateForEffect(
      handle,
      thread.vmPtr,
      hasForkSharedEffect,
      forkSharedHasTrackedState,
    );
    const kernelWorkerAvailable = processHasKernelWorkerOwnership(handle);
    const kernelWorkerOwnsCompletedFs = kernelWorkerAvailable
      && status === handle.kernel.abi_status_fs?.();
    const epollStateKernelOwns = kernelWorkerOwnsEpollState(handle);
    const activeVforkChildOwnerMismatch = hostExecutionState(handle).vforkWorker === worker
      && supervisorVforkChildOwnerMismatch(handle.kernel, thread.vmPtr, handle.pid);
    const epollHeldOfdFactNeeded = isRunningWorker
      && epollStateKernelOwns
      && (completedSyncEffects & syncEffects.syncEffectEpollState) !== 0;
    const ownedStateSyncActions = supervisorOwnedStateSyncActions(
      handle.kernel,
      status,
      completedSyncEffects,
      {
        runningWorker: isRunningWorker,
        workerOwnsAuthoritativeState,
        kernelWorkerAvailable,
        epollStateKernelOwns,
        forkSharedEffectRequested,
        activeVforkChildOwnerMismatch,
        processMemfsDirty: isProcessMemfsDirty(handle),
        heldEpollOfd: epollHeldOfdFactNeeded && collectHeldEpollOfdIds(handle).length > 0,
        wroteOrDrainedPipe: wroteToPipe || drainedPipe,
      },
    );
    if (ownedStateSyncActions.markProcessMemfsDirty && !kernelWorkerOwnsCompletedFs) {
      setProcessMemfsDirty(handle, true);
    }
    const shouldSyncForkSharedState = ownedStateSyncActions.syncForkSharedState;
    const shouldSyncEpollDependentOfds = ownedStateSyncActions.syncEpollDependentOfds;
    const shouldSyncSocketState = ownedStateSyncActions.syncSocketState;
    const shouldWakeSocketWaiters = ownedStateSyncActions.wakeSocketWaiters;
    const shouldSyncProcessMemfs = ownedStateSyncActions.syncProcessMemfs
      && !kernelWorkerOwnsCompletedFs;
    const shouldSyncEpollState = ownedStateSyncActions.syncEpollState;
    if (
      !shouldSyncProcessMemfs
      && status === handle.kernel.abi_status_fs?.()
      && isProcessMemfsDirty(handle)
      && kernelShareOtherMembers(handle).length === 0
    ) {
      deps.markRuntimeFsDirty(handle);
      setProcessMemfsDirty(handle, false);
    }
    if (
      !shouldSyncForkSharedState
      && !shouldSyncEpollDependentOfds
      && !shouldSyncSocketState
      && !shouldSyncProcessMemfs
      && !shouldSyncEpollState
    ) {
      return false;
    }

    const syncForkSharedState = (): Promise<void> => shouldSyncForkSharedState
      ? requestOwnedForkSharedStateSync(handle, thread.vmPtr, {
          processHandles: deps.processHandles,
          appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
          syncForkSharedStateViaKernelWorker: deps.syncForkSharedStateViaKernelWorker,
        }).then(() => {
          reconcileKernelSharePipeControl(handle);
        })
      : Promise.resolve();
    const syncOfdState = (): Promise<void> => shouldSyncEpollDependentOfds
      ? deps.requestEpollDependentOfdStateSync(handle)
      : Promise.resolve();
    const syncSocketState = (): Promise<void> => shouldSyncSocketState
      ? deps.requestSocketStateSync(handle, shouldWakeSocketWaiters)
      : Promise.resolve();
    const syncProcessMemfs = (): Promise<void> => {
      if (!shouldSyncProcessMemfs) {
        return Promise.resolve();
      }
      const processMemfsTargets = kernelShareOtherMembers(handle);
      if (
        processMemfsTargets.length === 0
        && status === handle.kernel.abi_status_fs?.()
      ) {
        deps.markRuntimeFsDirty(handle);
        setProcessMemfsDirty(handle, false);
        return Promise.resolve();
      }
      return deps.syncForkSharedStateViaKernelWorker(
        handle,
        processMemfsTargets,
        {
          cause: { kind: "syscall", nr: -1n, name: "owned-state-process-memfs-sync" },
          includeMemfs: true,
          memfsOnly: true,
          trackHeldOfds: false,
          sourceVmPtr: thread.vmPtr,
        },
      ).then(() => {
        deps.markRuntimeFsDirty(handle);
        for (const member of processMemfsTargets) {
          deps.markRuntimeFsDirty(member);
        }
        setProcessMemfsDirty(handle, false);
      });
    };
    setThreadOwnerAsyncStatus(thread, status);
    void syncSocketState()
      .then(() => syncForkSharedState())
      .then(() => syncOfdState())
      .then(() => syncProcessMemfs())
      .then(() => (shouldSyncEpollState
        ? deps.requestEpollStateSync(handle, ownedStateSyncActions.retryEpollWaiters)
        : Promise.resolve()))
      .then(() => {
        clearThreadOwnerAsyncStatus(thread);
        if (!workerStillOwnedByHandle()) {
          return;
        }
        continueAfterOwnedStateSync();
        for (const member of kernelShareMemberList(handle)) {
          deps.pumpProcess(member);
        }
      })
      .catch((error) => {
        clearThreadOwnerAsyncStatus(thread);
        if (!workerStillOwnedByHandle()) {
          return;
        }
        const failure = error instanceof Error ? error : new Error(String(error));
        if (typeof deps.failProcess === "function") {
          deps.failProcess(handle, failure);
          return;
        }
        throw failure;
      });
    return true;
  };
}
