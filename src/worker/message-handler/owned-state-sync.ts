import { collectHeldEpollOfdIds } from "../fork-shared.js";
import {
  kernelShareOtherMembers,
  kernelWorkerOwnsEpollState,
  kernelWorkerOwnsSocketState,
  reconcileKernelSharePipeControl,
} from "../kernel-share.js";
import type { ProcessHandle, ThreadWorkerState } from "../types.js";
import type { WorkerMessageHandlerDeps } from "./types.js";
import { createWorkerOwnershipGuard } from "./worker-ownership.js";

interface OwnedStateSyncArgs {
  handle: ProcessHandle;
  worker: Worker;
  thread: ThreadWorkerState;
  status: number;
  rawSyncEffects: number | undefined;
  completedSyncEffects: number;
  isRunningWorker: boolean;
  wroteToPipe: boolean;
  drainedPipe: boolean;
  continueAfterOwnedStateSync(): void;
}

export function createWorkerOwnedStateSyncHandler(deps: WorkerMessageHandlerDeps) {
  function requestForkSharedStateSync(
    handle: ProcessHandle,
    thread: ThreadWorkerState,
  ): Promise<void> {
    const startForkSharedStateSync = (): Promise<void> => {
      const promise = deps.syncForkSharedStateViaKernelWorker(
        handle,
        kernelShareOtherMembers(handle),
        {
          deltaOfdIds: true,
          includeMemfs: false,
          trackHeldOfds: true,
          sourceVmPtr: thread.vmPtr,
        },
      ).finally(() => {
        if (handle.forkSharedSyncPromise === promise) {
          handle.forkSharedSyncPromise = null;
        }
        if (handle.kernelShare.forkSharedSyncPromise === promise) {
          handle.kernelShare.forkSharedSyncPromise = null;
        }
      });
      handle.forkSharedSyncPromise = promise;
      handle.kernelShare.forkSharedSyncPromise = promise;
      return promise;
    };

    const activeSync = handle.kernelShare.forkSharedSyncPromise ?? handle.forkSharedSyncPromise;
    if (activeSync !== null) {
      const queuedSync = activeSync.then(() => startForkSharedStateSync());
      handle.forkSharedSyncPromise = queuedSync;
      handle.kernelShare.forkSharedSyncPromise = queuedSync;
      return queuedSync;
    }

    return startForkSharedStateSync();
  }

  return function maybeHandleOwnedStateSync({
    handle,
    worker,
    thread,
    status,
    rawSyncEffects,
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
    const shouldWakeSocketWaiters = status === handle.kernel.abi_status_ok()
      && (completedSyncEffects & deps.syncEffectSocketState) !== 0;
    const hasExplicitForkSharedEffect = rawSyncEffects !== undefined
      && (rawSyncEffects & deps.syncEffectForkSharedOfdState) !== 0;
    const shouldSyncForkSharedState = isRunningWorker
      && handle.kernelWorkerRequest !== null
      && handle.pid > 0
      && hasExplicitForkSharedEffect
      && handle.kernelShare.sharedOfdIds.size > 0
      && !deps.statusDefersOwnedStateSync(handle, status);
    const shouldSyncEpollDependentOfds = isRunningWorker
      && kernelWorkerOwnsEpollState(handle)
      && (completedSyncEffects & deps.syncEffectEpollDependentOfdState) !== 0
      && !deps.statusDefersOwnedStateSync(handle, status);
    const shouldSyncSocketState = isRunningWorker
      && kernelWorkerOwnsSocketState(handle)
      && (completedSyncEffects & deps.syncEffectSocketState) !== 0
      && !deps.statusDefersOwnedStateSync(handle, status);
    const shouldSyncEpollState = isRunningWorker
      && kernelWorkerOwnsEpollState(handle)
      && collectHeldEpollOfdIds(handle).length > 0
      && (completedSyncEffects & deps.syncEffectEpollState) !== 0
      && !deps.statusDefersOwnedStateSync(handle, status);
    if (!shouldSyncForkSharedState && !shouldSyncEpollDependentOfds && !shouldSyncSocketState && !shouldSyncEpollState) {
      return false;
    }

    const syncForkSharedState = shouldSyncForkSharedState
      ? requestForkSharedStateSync(handle, thread).then(() => {
          reconcileKernelSharePipeControl(handle);
        })
      : Promise.resolve();
    const syncOfdState = shouldSyncEpollDependentOfds
      ? deps.requestEpollDependentOfdStateSync(handle)
      : Promise.resolve();
    const syncSocketState = shouldSyncSocketState
      ? deps.requestSocketStateSync(handle, shouldWakeSocketWaiters)
      : Promise.resolve();
    void syncForkSharedState
      .then(() => syncOfdState)
      .then(() => syncSocketState)
      .then(() => (shouldSyncEpollState
        ? deps.requestEpollStateSync(handle, !(wroteToPipe || drainedPipe))
        : Promise.resolve()))
      .then(() => {
        if (!workerStillOwnedByHandle()) {
          return;
        }
        continueAfterOwnedStateSync();
        for (const member of handle.kernelShare.members) {
          deps.pumpProcess(member);
        }
      })
      .catch((error) => {
        if (!workerStillOwnedByHandle()) {
          return;
        }
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    return true;
  };
}
