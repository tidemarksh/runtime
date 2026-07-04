import { collectHeldEpollOfdIds } from "../../ownership/ofd-table.js";
import {
  kernelShareOtherMembers,
  reconcileKernelSharePipeControl,
} from "../../kernel-share.js";
import { kernelWorkerOwnsEpollState } from "../../ownership/epoll-control.js";
import {
  sourceHoldsSharedRegularFileOfd,
  sourceSharesRegularFileOfdWithPeer,
} from "../../ownership/ofd-peer.js";
import { kernelWorkerOwnsSocketState } from "../../ownership/socket-control.js";
import { supervisorVforkChildOwnerMismatch } from "../../process-identity.js";
import type { ProcessHandle, ThreadWorkerState } from "../../types.js";
import type { WorkerMessageHandlerDeps } from "../types.js";
import { createWorkerOwnershipGuard } from "../../ownership/process-state.js";
import { resolveWorkerMessageHandlerSyncEffects } from "../sync-effects.js";
import { requestOwnedForkSharedStateSync } from "../../sync/fork-shared.js";

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
    const shouldWakeSocketWaiters = status === handle.kernel.abi_status_ok()
      && (completedSyncEffects & syncEffects.syncEffectSocketState) !== 0;
    const hasForkSharedEffect = (completedSyncEffects & syncEffects.syncEffectForkSharedOfdState) !== 0;
    const forkSharedSourceHoldsRegularFile = sourceHoldsSharedRegularFileOfd(handle, thread.vmPtr);
    const forkSharedRegularFileHasPeer = sourceSharesRegularFileOfdWithPeer(handle, thread.vmPtr);
    const forkSharedHasTrackedState = (handle.kernelShare.sharedOfdIds?.size ?? 0) > 0
      || kernelShareOtherMembers(handle).length > 0;
    const shouldRequestForkSharedForEffect = hasForkSharedEffect
      && forkSharedHasTrackedState
      && (
        !forkSharedSourceHoldsRegularFile
        || forkSharedRegularFileHasPeer
      );
    const hasProcessMemfsEffect = (completedSyncEffects & syncEffects.syncEffectProcessMemfs) !== 0;
    if (isRunningWorker && hasProcessMemfsEffect) {
      handle.processMemfsDirty = true;
    }
    const activeVforkChildOwnerMismatch = handle.vforkWorker === worker
      && supervisorVforkChildOwnerMismatch(handle.kernel, thread.vmPtr, handle.pid);
    const shouldSyncForkSharedState = workerOwnsAuthoritativeState
      && handle.kernelWorkerRequest !== null
      && handle.pid > 0
      && shouldRequestForkSharedForEffect
      && status === handle.kernel.abi_status_ok()
      && !activeVforkChildOwnerMismatch
      && !deps.statusDefersOwnedStateSync(handle, status);
    const shouldSyncEpollDependentOfds = isRunningWorker
      && kernelWorkerOwnsEpollState(handle)
      && (completedSyncEffects & syncEffects.syncEffectEpollDependentOfdState) !== 0
      && !activeVforkChildOwnerMismatch
      && !deps.statusDefersOwnedStateSync(handle, status);
    const shouldSyncSocketState = shouldSyncForkSharedState
      && isRunningWorker
      && kernelWorkerOwnsSocketState(handle)
      && (completedSyncEffects & syncEffects.syncEffectSocketState) !== 0
      && !activeVforkChildOwnerMismatch
      && !deps.statusDefersOwnedStateSync(handle, status);
    const shouldPublishDirtyMemfs = status !== handle.kernel.abi_status_ok()
      || (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0;
    const shouldSyncProcessMemfs = isRunningWorker
      && handle.kernelWorkerRequest !== null
      && handle.pid > 0
      && handle.processMemfsDirty
      && shouldPublishDirtyMemfs
      && !activeVforkChildOwnerMismatch
      && !deps.statusDefersOwnedStateSync(handle, status);
    const shouldSyncEpollState = isRunningWorker
      && kernelWorkerOwnsEpollState(handle)
      && (completedSyncEffects & syncEffects.syncEffectEpollState) !== 0
      && collectHeldEpollOfdIds(handle).length > 0
      && !activeVforkChildOwnerMismatch
      && !deps.statusDefersOwnedStateSync(handle, status);
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
    const syncProcessMemfs = (): Promise<void> => shouldSyncProcessMemfs
      ? deps.syncForkSharedStateViaKernelWorker(
          handle,
          kernelShareOtherMembers(handle),
          {
            cause: { kind: "syscall", nr: -1n, name: "owned-state-process-memfs-sync" },
            includeMemfs: true,
            memfsOnly: true,
            trackHeldOfds: false,
            sourceVmPtr: thread.vmPtr,
          },
        ).then(() => {
          deps.markRuntimeFsDirty(handle);
          for (const member of kernelShareOtherMembers(handle)) {
            deps.markRuntimeFsDirty(member);
          }
          handle.processMemfsDirty = false;
        })
      : Promise.resolve();
    thread.ownerAsyncStatus = status;
    void syncSocketState()
      .then(() => syncForkSharedState())
      .then(() => syncOfdState())
      .then(() => syncProcessMemfs())
      .then(() => (shouldSyncEpollState
        ? deps.requestEpollStateSync(handle, !(wroteToPipe || drainedPipe))
        : Promise.resolve()))
      .then(() => {
        thread.ownerAsyncStatus = null;
        if (!workerStillOwnedByHandle()) {
          return;
        }
        continueAfterOwnedStateSync();
        for (const member of handle.kernelShare.members) {
          deps.pumpProcess(member);
        }
      })
      .catch((error) => {
        thread.ownerAsyncStatus = null;
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
