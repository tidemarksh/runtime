import { collectHeldEpollOfdIds } from "../fork-shared.js";
import { kernelShareOtherMembers, kernelWorkerOwnsEpollState, kernelWorkerOwnsSocketState } from "../kernel-share.js";
import type { ProcessHandle } from "../types.js";
import type { WorkerMessageHandlerDeps } from "./types.js";

interface OwnedStateSyncArgs {
  handle: ProcessHandle;
  status: number;
  completedSyncEffects: number;
  isRunningWorker: boolean;
  wroteToPipe: boolean;
  drainedPipe: boolean;
  continueAfterOwnedStateSync(): void;
}

export function createWorkerOwnedStateSyncHandler(deps: WorkerMessageHandlerDeps) {
  return function maybeHandleOwnedStateSync({
    handle,
    status,
    completedSyncEffects,
    isRunningWorker,
    wroteToPipe,
    drainedPipe,
    continueAfterOwnedStateSync,
  }: OwnedStateSyncArgs): boolean {
    const shouldWakeSocketWaiters = status === handle.kernel.abi_status_ok()
      && (completedSyncEffects & deps.syncEffectSocketState) !== 0;
    const shouldSyncForkSharedState = isRunningWorker
      && handle.kernelWorkerRequest !== null
      && handle.pid > 0
      && handle.kernelShare.sharedOfdIds.size > 0
      && (completedSyncEffects & deps.syncEffectForkSharedOfdState) !== 0
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
      ? deps.syncForkSharedStateViaKernelWorker(
        handle,
        kernelShareOtherMembers(handle),
        { includeMemfs: false },
      )
      : Promise.resolve();
    const syncOfdState = shouldSyncEpollDependentOfds
      ? deps.requestEpollDependentOfdStateSync(handle)
      : Promise.resolve();
    const syncSocketState = shouldSyncSocketState
      ? deps.requestSocketStateSync(handle, shouldWakeSocketWaiters)
      : Promise.resolve();
    deps.pumpProcess(handle);
    void syncForkSharedState
      .then(() => syncOfdState)
      .then(() => syncSocketState)
      .then(() => (shouldSyncEpollState
        ? deps.requestEpollStateSync(handle, !(wroteToPipe || drainedPipe))
        : Promise.resolve()))
      .then(() => {
        continueAfterOwnedStateSync();
      })
      .catch((error) => {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    return true;
  };
}
