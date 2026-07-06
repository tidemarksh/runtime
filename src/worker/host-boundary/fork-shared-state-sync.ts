import { hostExecutionState } from "../host-execution-state.js";
import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  appendSocketSyncDebug,
  lastForkSharedActiveOfdIdsByHandle,
  formatSyncCause,
  formatOfdIdList,
  snapshotNetworkBridgeSocketState,
  restoreNetworkBridgeSocketState,
  snapshotRuntimeNetworkKernelState,
  restoreRuntimeNetworkKernelState
} from "./shared-state.js";
import {
  addKernelShareSharedOfdId,
  deleteKernelShareOfdSlotSnapshot,
  deleteKernelShareSharedOfdId,
  kernelShareHasSharedOfdId,
  kernelShareMemberList,
  kernelShareOtherMembers,
  kernelShareSharedOfdIdList,
  reconcileKernelSharePipeControl,
  restoreLocalOfdSlots,
} from "../kernel-share.js";
import { processExitInProgress } from "../process-lifecycle-state.js";
import { reapplyMemfsDeletedPaths } from "../memfs-tombstones.js";
import {
  mergeKernelShareOfdStateInto,
  pruneStaleKernelShareOfdIds,
  snapshotKernelShareOfdSlots,
  syncKernelShareOfdReservations,
} from "../fork-shared.js";
import { appendForkSharedMemfsDebug } from "./memfs-state-sync.js";
import { adoptKernelStateForHandle } from "../kernel-state.js";
import {
  activeKernelShareOfdIds,
  collectProcessHeldOfds,
  snapshotLocalOfdSlots,
  staleKernelShareOfdIds,
} from "../ownership/ofd-table.js";
import { supervisorOfdFdType } from "../ownership/ofd-metadata.js";
import {
  collectTracedForkSharedOfdIds,
  isRegularFileOfd,
  sourceForkSharedOfdPeerState,
  sourceOfdCanPublishForkSharedState,
  targetHasIncompatibleForkSharedOfd,
  targetHoldsSyncedRegularFileOfd,
  trackHeldForkSharedOfds,
} from "../ownership/ofd-peer.js";
import {
  ofdIdsFromFdEntries,
  supervisorProcessOfdForFd,
  snapshotProcessFdEntries,
} from "../ownership/fd-table.js";
import { supervisorProcessPipeSlotForFd } from "../ownership/pipe-slot.js";
import {
  restoreProcessStates,
  snapshotProcessStates,
} from "../ownership/process-state.js";
import {
  processHasKernelWorkerOwnership,
  requireKernelWorkerOwnership,
} from "../kernel-worker-ownership.js";
import {
  processHostStdio,
  processStdioOwner,
} from "../process-stdio-state.js";
import {
  composeHandlePipeUsed,
  expandPipePeerOfdIds,
  hasPipePeerInOtherKernelShareMember,
  isProcessHostStdioPipeOfd,
} from "../ownership/pipe-control.js";
import {
  collectRuntimeNetworkSocketOfdIds,
  hasRuntimeNetworkSocketState,
} from "../ownership/socket-control.js";
import {
  forkSharedActionAddsShared,
  forkSharedActionIncludes,
  forkSharedActionRehydrates,
  forkSharedDeltaSourceOfdAction,
  forkSharedPipePeerOfdAction,
  forkSharedRehydrateOfdAction,
  forkSharedSyncTargetAction,
  forkSharedSyncTargetActionReject,
  forkSharedSyncTargetActionUse,
  forkSharedSyncableOfdAction,
  forkSharedTrackedSourceOfdAction,
} from "../ownership/fork-shared-decision.js";
import { clearStaleForkSharedSyncGate } from "../fork-shared-sync-gate.js";
import {
  describeForkSharedSyncPromise,
  registerForkSharedSyncPromise,
} from "../fork-shared-sync-debug.js";
import {
  clearForkSharedSyncIfCurrent,
  getActiveForkSharedSync,
  setActiveForkSharedSync,
} from "../fork-shared-sync-state.js";
import { isBlockedRpcPending } from "../host-blocked-state.js";
import type {
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPidStateSnapshot,
  ProcessHandle,
  SyncCause
} from "./shared-state.js";

function hasPendingBlockedRpc(handle: ProcessHandle): boolean {
  return [...hostExecutionState(handle).hostBlockedWorkers.values()].some((blocked) => isBlockedRpcPending(blocked));
}

function forkSharedSyncTargetDecision(
  source: ProcessHandle,
  target: ProcessHandle,
  options: {
    processHandles?: ReadonlyMap<number, ProcessHandle>;
    ofdIds?: Iterable<number>;
    requireHostBlocked?: boolean;
    includeMemfs?: boolean;
    considerVforkReady?: boolean;
    considerRpcPending?: boolean;
    considerRuntimeNetworkState?: boolean;
  } = {},
): number {
  const targetHostState = hostExecutionState(target);
  const registered = options.processHandles?.get(target.pid);
  const invalidPid = !Number.isFinite(target.pid) || target.pid <= 0;
  const ofdIds = options.ofdIds === undefined ? [] : [...options.ofdIds];
  return forkSharedSyncTargetAction(source, {
    invalidPid,
    runningWorker: targetHostState.runningWorker !== null,
    vforkReadyWorker: options.considerVforkReady === true && targetHostState.vforkReadyWorkers.length > 0,
    hasHostBlockedWorker: targetHostState.hostBlockedWorkers.size > 0,
    rpcPending: options.considerRpcPending === true && hasPendingBlockedRpc(target),
    processExit: processExitInProgress(target),
    registeredProcessExit: registered !== undefined && processExitInProgress(registered),
    registeredMismatch: registered !== undefined && registered !== target,
    incompatibleOfd: ofdIds.length > 0 && targetHasIncompatibleForkSharedOfd(source, target, ofdIds),
    requireHostBlocked: options.requireHostBlocked === true,
    includeMemfs: options.includeMemfs === true,
    runtimeNetworkState: options.considerRuntimeNetworkState === true && hasRuntimeNetworkSocketState(target),
  });
}

function isForkSharedSyncTargetUsable(
  source: ProcessHandle,
  target: ProcessHandle,
  options: Parameters<typeof forkSharedSyncTargetDecision>[2] = {},
): boolean {
  return forkSharedSyncTargetDecision(source, target, options) === forkSharedSyncTargetActionUse(source);
}

function isForkSharedSyncTargetCandidate(
  source: ProcessHandle,
  target: ProcessHandle,
  options: Parameters<typeof forkSharedSyncTargetDecision>[2] = {},
): boolean {
  return forkSharedSyncTargetDecision(source, target, options) !== forkSharedSyncTargetActionReject(source);
}

function pruneUnpeeredRegularFileSharedOfds(handle: ProcessHandle): void {
  for (const ofdId of kernelShareSharedOfdIdList(handle)) {
    if (!isRegularFileOfd(handle, ofdId)) {
      continue;
    }
    if (sourceForkSharedOfdPeerState(handle, ofdId).compatiblePeer) {
      continue;
    }
    for (const member of kernelShareMemberList(handle)) {
      member.kernel.kernel_unreserve_ofd?.(ofdId);
    }
    deleteKernelShareSharedOfdId(handle, ofdId);
    deleteKernelShareOfdSlotSnapshot(handle, ofdId);
  }
}

function isInheritedOutputHostStdioPipeOfd(
  handle: ProcessHandle,
  sourceVmPtr: number,
  ofdId: number,
): boolean {
  if (sourceVmPtr === 0 || processStdioOwner(handle) === null) {
    return false;
  }
  const hostStdio = processHostStdio(handle);
  for (const [fd, hostPipeSlot] of [
    [1, hostStdio.stdoutPipe],
    [2, hostStdio.stderrPipe],
  ] as const) {
    if (
      hostPipeSlot > 0
      && supervisorProcessOfdForFd(handle, sourceVmPtr, fd) === ofdId
      && supervisorProcessPipeSlotForFd(handle, sourceVmPtr, fd) === hostPipeSlot
    ) {
      return true;
    }
  }
  return false;
}

interface OwnedForkSharedStateSyncDeps {
  processHandles?: ReadonlyMap<number, ProcessHandle>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  syncForkSharedStateViaKernelWorker?: typeof syncForkSharedStateViaKernelWorker;
}

export function requestOwnedForkSharedStateSync(
  handle: ProcessHandle,
  sourceVmPtr: number,
  deps: OwnedForkSharedStateSyncDeps,
): Promise<void> {
  const otherMembers = kernelShareOtherMembers(handle);
  const activeSync = getActiveForkSharedSync(handle);
  const syncForkSharedState = deps.syncForkSharedStateViaKernelWorker ?? syncForkSharedStateViaKernelWorker;
  const syncTargetIsUsable = (member: ProcessHandle): boolean => {
    return isForkSharedSyncTargetUsable(handle, member, {
      processHandles: deps.processHandles,
      ofdIds: kernelShareSharedOfdIdList(handle),
    });
  };
  const syncTargetCanBecomeUsable = (member: ProcessHandle): boolean => {
    return isForkSharedSyncTargetCandidate(handle, member, {
      processHandles: deps.processHandles,
      ofdIds: kernelShareSharedOfdIdList(handle),
    });
  };
  const hasSyncTargetCandidate = otherMembers.some(syncTargetCanBecomeUsable);
  const startForkSharedStateSync = (
    targets = otherMembers.filter(syncTargetIsUsable),
  ): Promise<void> => {
    if (targets.length === 0) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `fork-shared-sync-local:${handle.pid}:src=0x${sourceVmPtr.toString(16)}:targets=none`,
      );
      return syncForkSharedState(
        handle,
        targets,
        {
          cause: { kind: "syscall", nr: -1n, name: "owned-state-empty-fork-shared-sync" },
          deltaOfdIds: true,
          includeMemfs: false,
          trackHeldOfds: true,
          sourceVmPtr,
        },
      );
    }
    const promise = syncForkSharedState(
      handle,
      targets,
      {
        cause: { kind: "syscall", nr: -1n, name: "owned-state-fork-shared-sync" },
        deltaOfdIds: true,
        includeMemfs: false,
        trackHeldOfds: true,
        sourceVmPtr,
      },
    ).finally(() => {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `fork-shared-sync-settled:${describeForkSharedSyncPromise(promise)}`,
      );
      clearForkSharedSyncIfCurrent(handle, promise);
    });
    const syncId = registerForkSharedSyncPromise(promise, {
      ownerPid: handle.pid,
      sourceVmPtr,
      reason: "owned-ofd",
      targetPids: targets.map((target) => target.pid),
    });
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `fork-shared-sync-start:id=${syncId}:owner=${handle.pid}:src=0x${sourceVmPtr.toString(16)}`
        + `:targets=${targets.map((target) => target.pid).join(".") || "none"}`,
    );
    setActiveForkSharedSync(handle, promise);
    return promise;
  };

  if (activeSync !== null) {
    if (!hasSyncTargetCandidate) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `fork-shared-sync-local-queue:${describeForkSharedSyncPromise(activeSync)}:owner=${handle.pid}:src=0x${sourceVmPtr.toString(16)}`,
      );
      return activeSync.then(() => startForkSharedStateSync([]));
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `fork-shared-sync-queue:${describeForkSharedSyncPromise(activeSync)}:owner=${handle.pid}:src=0x${sourceVmPtr.toString(16)}`,
    );
    const queuedSync = activeSync
      .then(() => startForkSharedStateSync())
      .finally(() => {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `fork-shared-sync-queue-settled:${describeForkSharedSyncPromise(queuedSync)}`,
        );
        clearForkSharedSyncIfCurrent(handle, queuedSync);
      });
    registerForkSharedSyncPromise(queuedSync, {
      ownerPid: handle.pid,
      sourceVmPtr,
      reason: "queued-owned-ofd",
      targetPids: otherMembers.map((target) => target.pid),
    });
    setActiveForkSharedSync(handle, queuedSync);
    return queuedSync;
  }

  return startForkSharedStateSync();
}

export async function syncForkSharedStateViaKernelWorker(
  handle: ProcessHandle,
  targets: ProcessHandle[],
  options: {
    cause: SyncCause;
    includeMemfs?: boolean;
    trackHeldOfds?: boolean;
    deltaOfdIds?: boolean;
    memfsOnly?: boolean;
    preserveSourceRefcounts?: boolean;
    sourceVmPtr?: number;
    ofdIds?: number[];
  },
): Promise<void> {
  const kernelWorkerRequest = requireKernelWorkerOwnership(handle, "syncForkSharedStateViaKernelWorker");
  let syncTargets = targets.filter((target) => isForkSharedSyncTargetUsable(handle, target, {
    requireHostBlocked: true,
    includeMemfs: options.includeMemfs === true,
    considerVforkReady: true,
    considerRpcPending: true,
    considerRuntimeNetworkState: true,
  }));
  const memfsOnly = options.memfsOnly === true;

  if (!memfsOnly && options.trackHeldOfds === true) {
    trackHeldForkSharedOfds(handle, options.sourceVmPtr ?? handle.vmPtr);
    pruneUnpeeredRegularFileSharedOfds(handle);
  }
  const sourceVmPtr = options.sourceVmPtr ?? handle.vmPtr;
  const syncableForkSharedOfd = (ofdId: number): boolean => (
    !isProcessHostStdioPipeOfd(handle, ofdId)
    || isInheritedOutputHostStdioPipeOfd(handle, sourceVmPtr, ofdId)
  );
  const runtimeNetworkSocketOfdIds = collectRuntimeNetworkSocketOfdIds(kernelShareMemberList(handle));
  const sourceFdEntries = memfsOnly ? [] : snapshotProcessFdEntries(handle, sourceVmPtr);
  const sourceFdOfdIds = memfsOnly
    ? []
    : ofdIdsFromFdEntries(handle, sourceFdEntries).filter(syncableForkSharedOfd);
  const sourceHeldOfdIds = memfsOnly
    ? new Set<number>()
    : new Set([...collectProcessHeldOfds(handle)].filter(syncableForkSharedOfd));
  const sourcePublishedOfdIds = new Set<number>();
  const sourceConflictingOfdIds = new Set<number>();
  const allowedOfdIdSet = options.ofdIds === undefined ? null : new Set(options.ofdIds);
  if (!memfsOnly) {
    for (const ofdId of new Set([...sourceHeldOfdIds, ...sourceFdOfdIds])) {
      if (!kernelShareHasSharedOfdId(handle, ofdId)) {
        continue;
      }
      if (sourceOfdCanPublishForkSharedState(handle, ofdId)) {
        sourcePublishedOfdIds.add(ofdId);
      } else {
        sourceConflictingOfdIds.add(ofdId);
      }
    }
  }
  const canSyncForkSharedOfd = (ofdId: number): boolean => forkSharedActionIncludes(
    handle,
    forkSharedSyncableOfdAction(handle, {
      syncable: syncableForkSharedOfd(ofdId),
      sourceConflicting: sourceConflictingOfdIds.has(ofdId),
      allowed: allowedOfdIdSet === null || allowedOfdIdSet.has(ofdId),
      regularFile: isRegularFileOfd(handle, ofdId),
      sourcePublished: sourcePublishedOfdIds.has(ofdId),
      runtimeNetworkSocket: runtimeNetworkSocketOfdIds.has(ofdId),
    }),
  );
  const activeOfdIds = memfsOnly
    ? []
    : activeKernelShareOfdIds(handle).filter(canSyncForkSharedOfd);
  const preservedSourceFdOfdSlots = snapshotLocalOfdSlots(
    handle,
    sourceFdOfdIds.filter((ofdId) => !isRegularFileOfd(handle, ofdId)),
  );
  const sourceActiveOfdIds = activeOfdIds
    .filter((ofdId) => sourceHeldOfdIds.has(ofdId) || sourceFdOfdIds.includes(ofdId));
  const staleOfdIds = memfsOnly ? [] : staleKernelShareOfdIds(handle);
  if (!memfsOnly) {
    syncKernelShareOfdReservations(handle);
  }
  const preservedRuntimeNetworkKernelStates = kernelShareMemberList(handle).map((member) =>
    snapshotRuntimeNetworkKernelState(member, member === handle ? sourceVmPtr : member.vmPtr)
  );
  const omitRuntimeNetworkSocketOfds = (ofdIds: number[]): number[] => {
    if (runtimeNetworkSocketOfdIds.size === 0) {
      return ofdIds;
    }
    return ofdIds.filter((ofdId) => !runtimeNetworkSocketOfdIds.has(ofdId));
  };
  const publishableSourceHeldOfdIds = [...sourceHeldOfdIds].filter((ofdId) =>
    canSyncForkSharedOfd(ofdId)
  );
  const publishableSourceFdOfdIds = sourceFdOfdIds.filter((ofdId) =>
    canSyncForkSharedOfd(ofdId)
  );
  const sourceFdOfdIdSet = new Set(sourceFdOfdIds);
  const trackedSourceOfdIds = options.trackHeldOfds === true
    ? [...new Set([...sourceHeldOfdIds, ...sourceFdOfdIds])]
        .filter((ofdId) => {
          const sourceFdEntry = sourceFdOfdIdSet.has(ofdId);
          const action = forkSharedTrackedSourceOfdAction(handle, {
            kernelShared: kernelShareHasSharedOfdId(handle, ofdId),
            sourceFdEntry,
            syncable: canSyncForkSharedOfd(ofdId),
            typeNone: supervisorOfdFdType(handle, ofdId) === handle.kernel.abi_fd_type_none(),
            incompatiblePeer: sourceForkSharedOfdPeerState(handle, ofdId).incompatiblePeer,
          });
          return forkSharedActionIncludes(handle, action);
        })
    : [];
  let syncOfdIds = [...new Set([...publishableSourceHeldOfdIds, ...publishableSourceFdOfdIds, ...activeOfdIds])]
    .sort((left, right) => left - right);
  if (options.deltaOfdIds) {
    const sourceForkSharedFdOfdIds = sourceFdOfdIds
      .filter((ofdId) => {
        const action = forkSharedDeltaSourceOfdAction(handle, {
          allowed: allowedOfdIdSet === null || allowedOfdIdSet.has(ofdId),
          sourcePublished: sourcePublishedOfdIds.has(ofdId),
          sourceHeld: sourceHeldOfdIds.has(ofdId),
          pipePeer: hasPipePeerInOtherKernelShareMember(handle, ofdId),
        });
        if (forkSharedActionAddsShared(handle, action)) {
          addKernelShareSharedOfdId(handle, ofdId);
        }
        return forkSharedActionIncludes(handle, action);
      });
    const tracedOfdIds = collectTracedForkSharedOfdIds(handle, sourceVmPtr)
      .filter((ofdId) => {
        const action = forkSharedDeltaSourceOfdAction(handle, {
          allowed: allowedOfdIdSet === null || allowedOfdIdSet.has(ofdId),
          sourcePublished: sourcePublishedOfdIds.has(ofdId),
          sourceHeld: sourceHeldOfdIds.has(ofdId),
          pipePeer: hasPipePeerInOtherKernelShareMember(handle, ofdId),
        });
        if (forkSharedActionAddsShared(handle, action)) {
          addKernelShareSharedOfdId(handle, ofdId);
        }
        return forkSharedActionIncludes(handle, action);
      });
    syncKernelShareOfdReservations(handle);
    const deltaOfdIdSet = new Set([
      ...sourceActiveOfdIds,
      ...sourceForkSharedFdOfdIds,
      ...activeOfdIds,
      ...tracedOfdIds,
      ...trackedSourceOfdIds,
    ]);
    const staleOfdIdSet = new Set(staleOfdIds);
    for (const ofdId of expandPipePeerOfdIds(handle, deltaOfdIdSet)) {
      const action = forkSharedPipePeerOfdAction(handle, {
        stale: staleOfdIdSet.has(ofdId),
        syncable: syncableForkSharedOfd(ofdId),
      });
      if (forkSharedActionAddsShared(handle, action)) {
        addKernelShareSharedOfdId(handle, ofdId);
      }
      if (forkSharedActionIncludes(handle, action)) {
        deltaOfdIdSet.add(ofdId);
      }
    }
    const deltaOfdIds = omitRuntimeNetworkSocketOfds([...deltaOfdIdSet])
      .filter(canSyncForkSharedOfd)
      .sort((left, right) => left - right);
    deltaOfdIds.sort((left, right) => left - right);
    appendSocketSyncDebug(
      handle,
      [
        `fork-shared-delta:${handle.pid}`,
        formatSyncCause(options.cause),
        `src=0x${sourceVmPtr.toString(16)}`,
        `source_fd=${formatOfdIdList(sourceFdOfdIds)}`,
        `source_shared_fd=${formatOfdIdList(sourceForkSharedFdOfdIds)}`,
        `source_held=${formatOfdIdList([...sourceHeldOfdIds].sort((left, right) => left - right))}`,
        `source_active=${formatOfdIdList(sourceActiveOfdIds)}`,
        `tracked_source=${formatOfdIdList(trackedSourceOfdIds)}`,
        `active=${formatOfdIdList(activeOfdIds)}`,
        `stale=${formatOfdIdList(staleOfdIds)}`,
        `traced=${formatOfdIdList(tracedOfdIds)}`,
        `runtime_network_socket=${formatOfdIdList([...runtimeNetworkSocketOfdIds].sort((left, right) => left - right))}`,
        `sync=${formatOfdIdList(deltaOfdIds)}`,
      ].join(":"),
    );
    if (deltaOfdIds.length === 0 && !memfsOnly) {
      lastForkSharedActiveOfdIdsByHandle.set(handle, new Set(sourceActiveOfdIds));
      pruneStaleKernelShareOfdIds(handle, staleOfdIds);
      return;
    }
    syncOfdIds = deltaOfdIds;
  } else {
    syncOfdIds = omitRuntimeNetworkSocketOfds(syncOfdIds).filter(syncableForkSharedOfd);
  }
  if (!memfsOnly && syncOfdIds.length > 0) {
    const syncOfdIdSet = new Set(syncOfdIds);
    syncTargets = syncTargets.filter(
      (target) => !targetHoldsSyncedRegularFileOfd(target, syncOfdIdSet),
    );
  }
  if (!memfsOnly && syncOfdIds.length > 0) {
    syncTargets = syncTargets.filter(
      (target) => !targetHasIncompatibleForkSharedOfd(handle, target, syncOfdIds),
    );
  }
  const processStateSnapshots = snapshotProcessStates(syncTargets);
  const mergeSourceOfdIds = omitRuntimeNetworkSocketOfds(
    sourceActiveOfdIds.filter((ofdId) => !sourceFdOfdIds.includes(ofdId)),
  );
  mergeKernelShareOfdStateInto(handle, mergeSourceOfdIds);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  const snapshotOfdIds = syncOfdIds;
  const ofdSlots: KernelWorkerOfdSlotSnapshot[] = [...snapshotKernelShareOfdSlots(handle, snapshotOfdIds)]
    .map(([ofdId, slot]) => ({ ofdId, slot }));
  const targetPids = syncTargets.map((target) => target.pid).filter((pid) => pid > 0);
  const targetKernelStates: KernelWorkerPidStateSnapshot[] = syncTargets
    .filter((target) => target.pid > 0)
    .map((target) => ({
      pid: target.pid,
      vmPtr: target.vmPtr,
      kernelState: target.kernelState,
    }));
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState(kernelShareMemberList(handle));
  let response: Awaited<ReturnType<typeof kernelWorkerRequest>>;
  try {
    response = await kernelWorkerRequest({
      type: "sync-fork-shared-state",
      pid: handle.pid,
      vmPtr: options.sourceVmPtr ?? handle.vmPtr,
      targetPids,
      targetKernelStates,
      ofdIds: syncOfdIds,
      ofdSlots,
      fdEntries: sourceFdEntries,
      includeMemfs: options.includeMemfs ?? true,
      memfsOnly,
      preserveSourceRefcounts: options.preserveSourceRefcounts,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "fork-shared-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-fork-shared-state: ${response.type}`);
  }
  if (response.forkSharedDebug) {
    appendForkSharedMemfsDebug(handle, `fork-shared-memfs:${handle.pid}:${response.forkSharedDebug}`);
  }
  for (const ofdId of syncOfdIds) {
    addKernelShareSharedOfdId(handle, ofdId);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  handle.kernelState = {
    ...handle.kernelState,
    pipeUsed: composeHandlePipeUsed(handle, handle.kernelState.pipeUsed),
  };
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  reapplyMemfsDeletedPaths(handle);
  restoreLocalOfdSlots(handle, preservedSourceFdOfdSlots);
  for (const snapshot of preservedRuntimeNetworkKernelStates) {
    restoreRuntimeNetworkKernelState(snapshot);
  }
  reconcileKernelSharePipeControl(handle);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  for (const member of kernelShareOtherMembers(handle)) {
    const reapplied = reapplyMemfsDeletedPaths(member);
    if (reapplied.length > 0) {
      member.kernelState = readKernelRuntimeState(member.kernel);
      attachKernelRuntimeState(member.kernel, member.kernelState);
    }
  }
  lastForkSharedActiveOfdIdsByHandle.set(handle, new Set(sourceActiveOfdIds));
  pruneStaleKernelShareOfdIds(handle, staleOfdIds);
  if (!memfsOnly) {
    syncKernelShareOfdReservations(handle);
  }
  restoreProcessStates(processStateSnapshots);
  const heldAfterSync = collectProcessHeldOfds(handle);
  const unheldActiveOfdIds = activeKernelShareOfdIds(handle)
    .filter((ofdId) => forkSharedActionRehydrates(
      handle,
      forkSharedRehydrateOfdAction(handle, {
        active: true,
        sourceHeld: heldAfterSync.has(ofdId),
        allowed: allowedOfdIdSet === null || allowedOfdIdSet.has(ofdId),
      }),
    ));
  if (unheldActiveOfdIds.length > 0) {
    await rehydrateForkSharedStateViaKernelWorker([handle], unheldActiveOfdIds, {
      cause: options.cause,
      includeMemfs: false,
    });
  }
}

export async function rehydrateForkSharedStateViaKernelWorker(
  targets: ProcessHandle[],
  ofdIds: number[],
  options: { cause: SyncCause; procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean },
): Promise<void> {
  if (targets.length === 0) {
    return;
  }
  const owner = targets.find((target) => processHasKernelWorkerOwnership(target));
  if (!owner) {
    throw new Error("rehydrateForkSharedStateViaKernelWorker requires a kernel-worker-owned target");
  }
  const kernelWorkerRequest = requireKernelWorkerOwnership(owner, "rehydrateForkSharedStateViaKernelWorker");
  const targetPids = [...new Set(targets.map((target) => target.pid).filter((pid) => pid > 0))];
  const targetKernelStates: KernelWorkerPidStateSnapshot[] = targets
    .filter((target) => target.pid > 0)
    .map((target) => {
      target.kernelState = readKernelRuntimeState(target.kernel);
      attachKernelRuntimeState(target.kernel, target.kernelState);
      return {
        pid: target.pid,
        vmPtr: target.vmPtr,
        kernelState: target.kernelState,
      };
    });
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState(targets);
  appendSocketSyncDebug(
    owner,
    `fork-shared-rehydrate:${owner.pid}:${formatSyncCause(options.cause)}:targets=${targetPids.join(",") || "none"}:ofds=${ofdIds.join(",") || "none"}`,
  );
  let response: Awaited<ReturnType<typeof kernelWorkerRequest>>;
  try {
    response = await kernelWorkerRequest({
      type: "rehydrate-fork-shared-state",
      targetPids,
      targetKernelStates,
      ofdIds,
      procSelfExeTargets: options.procSelfExeTargets,
      includeMemfs: options.includeMemfs,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "fork-shared-state-rehydrated") {
    throw new Error(
      `unexpected kernel-worker response for rehydrate-fork-shared-state: ${response.type}`,
    );
  }
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
}

export async function syncBlockedForkSharedStateViaKernelWorker(
  handle: ProcessHandle,
  sourceVmPtr = handle.vmPtr,
  cause: SyncCause,
): Promise<void> {
  void requireKernelWorkerOwnership(handle, "syncBlockedForkSharedStateViaKernelWorker");
  const activeSync = getActiveForkSharedSync(handle);
  if (activeSync !== null) {
    const staleGate = clearStaleForkSharedSyncGate(handle, activeSync);
    if (staleGate === null) {
      await activeSync;
    } else {
      appendSocketSyncDebug(handle, `fork-shared-sync-clear-stale:${handle.pid}:${staleGate}`);
    }
  }
  const syncTargets = kernelShareOtherMembers(handle)
    .filter((member) => isForkSharedSyncTargetUsable(handle, member, {
      considerRpcPending: true,
    }));
  const syncPromise = syncForkSharedStateViaKernelWorker(
    handle,
    syncTargets,
    {
      cause,
      includeMemfs: false,
      deltaOfdIds: true,
      trackHeldOfds: true,
      sourceVmPtr,
    },
  ).finally(() => {
    clearForkSharedSyncIfCurrent(handle, syncPromise);
  });
  registerForkSharedSyncPromise(syncPromise, {
    ownerPid: handle.pid,
    sourceVmPtr,
    reason: "blocked-rpc",
    targetPids: syncTargets.map((target) => target.pid),
  });
  setActiveForkSharedSync(handle, syncPromise);
  await syncPromise;
}
