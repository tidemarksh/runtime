import { collectHeldEpollOfdIds } from "../../ownership/ofd-table.js";
import {
  describeForkSharedSyncPromise,
  registerForkSharedSyncPromise,
} from "../../fork-shared-sync-debug.js";
import {
  kernelShareOtherMembers,
  reconcileKernelSharePipeControl,
} from "../../kernel-share.js";
import { kernelWorkerOwnsEpollState } from "../../ownership/epoll-control.js";
import {
  sourceHoldsSharedRegularFileOfd,
  sourceSharesRegularFileOfdWithPeer,
  targetHasIncompatibleForkSharedOfd as ownershipTargetHasIncompatibleForkSharedOfd,
} from "../../ownership/ofd-peer.js";
import { kernelWorkerOwnsSocketState } from "../../ownership/socket-control.js";
import { supervisorVforkChildOwnerMismatch } from "../../process-identity.js";
import type { ProcessHandle, ThreadWorkerState } from "../../types.js";
import type { WorkerMessageHandlerDeps } from "../types.js";
import { createWorkerOwnershipGuard } from "../../ownership/process-state.js";
import { resolveWorkerMessageHandlerSyncEffects } from "../sync-effects.js";

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
  function requestForkSharedStateSync(
    handle: ProcessHandle,
    thread: ThreadWorkerState,
  ): Promise<void> {
    const otherMembers = kernelShareOtherMembers(handle);
    const activeSync = handle.kernelShare.forkSharedSyncPromise ?? handle.forkSharedSyncPromise;
    const syncTargetIsUsable = (member: ProcessHandle): boolean => {
      if (!Number.isFinite(member.pid) || member.pid <= 0) {
        return false;
      }
      const registered = deps.processHandles?.get(member.pid);
      if (
        member.runningWorker !== null
        || member.exitInProgress
        || registered?.exitInProgress
        || ownershipTargetHasIncompatibleForkSharedOfd(handle, member, handle.kernelShare.sharedOfdIds)
      ) {
        return false;
      }
      return registered === undefined || registered === member;
    };
    const syncTargetCanBecomeUsable = (member: ProcessHandle): boolean => {
      if (!Number.isFinite(member.pid) || member.pid <= 0) {
        return false;
      }
      const registered = deps.processHandles?.get(member.pid);
      if (
        member.exitInProgress
        || registered?.exitInProgress
        || ownershipTargetHasIncompatibleForkSharedOfd(handle, member, handle.kernelShare.sharedOfdIds)
      ) {
        return false;
      }
      return registered === undefined || registered === member;
    };
    const hasSyncTargetCandidate = otherMembers.some(syncTargetCanBecomeUsable);
    const startForkSharedStateSync = (targets = otherMembers.filter(syncTargetIsUsable)): Promise<void> => {
      if (targets.length === 0) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `fork-shared-sync-local:${handle.pid}:src=0x${thread.vmPtr.toString(16)}:targets=none`,
        );
        return deps.syncForkSharedStateViaKernelWorker(
          handle,
          targets,
          {
            cause: { kind: "syscall", nr: -1n, name: "owned-state-empty-fork-shared-sync" },
            deltaOfdIds: true,
            includeMemfs: false,
            trackHeldOfds: true,
            sourceVmPtr: thread.vmPtr,
          },
        );
      }
      const promise = deps.syncForkSharedStateViaKernelWorker(
        handle,
        targets,
        {
          cause: { kind: "syscall", nr: -1n, name: "owned-state-fork-shared-sync" },
          deltaOfdIds: true,
          includeMemfs: false,
          trackHeldOfds: true,
          sourceVmPtr: thread.vmPtr,
        },
      ).finally(() => {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `fork-shared-sync-settled:${describeForkSharedSyncPromise(promise)}`,
        );
        if (handle.forkSharedSyncPromise === promise) {
          handle.forkSharedSyncPromise = null;
        }
        if (handle.kernelShare.forkSharedSyncPromise === promise) {
          handle.kernelShare.forkSharedSyncPromise = null;
        }
      });
      const syncId = registerForkSharedSyncPromise(promise, {
        ownerPid: handle.pid,
        sourceVmPtr: thread.vmPtr,
        reason: "owned-ofd",
        targetPids: targets.map((target) => target.pid),
      });
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `fork-shared-sync-start:id=${syncId}:owner=${handle.pid}:src=0x${thread.vmPtr.toString(16)}`
          + `:targets=${targets.map((target) => target.pid).join(".") || "none"}`,
      );
      handle.forkSharedSyncPromise = promise;
      handle.kernelShare.forkSharedSyncPromise = promise;
      return promise;
    };

    if (activeSync !== null) {
      if (!hasSyncTargetCandidate) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `fork-shared-sync-local-queue:${describeForkSharedSyncPromise(activeSync)}:owner=${handle.pid}:src=0x${thread.vmPtr.toString(16)}`,
        );
        return activeSync.then(() => startForkSharedStateSync([]));
      }
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `fork-shared-sync-queue:${describeForkSharedSyncPromise(activeSync)}:owner=${handle.pid}:src=0x${thread.vmPtr.toString(16)}`,
      );
      const queuedSync = activeSync
        .then(() => startForkSharedStateSync())
        .finally(() => {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `fork-shared-sync-queue-settled:${describeForkSharedSyncPromise(queuedSync)}`,
          );
          if (handle.forkSharedSyncPromise === queuedSync) {
            handle.forkSharedSyncPromise = null;
          }
          if (handle.kernelShare.forkSharedSyncPromise === queuedSync) {
            handle.kernelShare.forkSharedSyncPromise = null;
          }
        });
      registerForkSharedSyncPromise(queuedSync, {
        ownerPid: handle.pid,
        sourceVmPtr: thread.vmPtr,
        reason: "queued-owned-ofd",
        targetPids: otherMembers.map((target) => target.pid),
      });
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
      ? requestForkSharedStateSync(handle, thread).then(() => {
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
