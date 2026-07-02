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
} from "./shared.js";
import {
  copyLegacySharedKernelObjectsLocal,
  kernelShareOtherMembers,
  restoreLocalOfdSlots,
} from "../kernel-share.js";
import { reapplyMemfsDeletedPaths } from "../memfs-tombstones.js";
import {
  mergeKernelShareOfdStateInto,
  pruneStaleKernelShareOfdIds,
  reconcileForkSharedOfdRefcounts,
  snapshotKernelShareOfdSlots,
  syncKernelShareOfdReservations,
} from "../fork-shared.js";
import { appendForkSharedMemfsDebug } from "./memfs.js";
import { adoptKernelStateForHandle } from "../kernel-state.js";
import {
  activeKernelShareOfdIds,
  collectProcessHeldOfds,
  snapshotLocalOfdSlots,
  staleKernelShareOfdIds,
} from "../ownership/ofd-table.js";
import {
  collectTrackableForkSharedOfds,
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
  snapshotProcessFdEntries,
} from "../ownership/fd-table.js";
import {
  restoreProcessStates,
  snapshotProcessStates,
} from "../ownership/process-state.js";
import {
  composeHandlePipeUsed,
  expandPipePeerOfdIds,
  hasPipePeerInOtherKernelShareMember,
  isProcessHostStdioPipeOfd,
  preserveHostStdioPipeControlForStep,
} from "../ownership/pipe-control.js";
import {
  collectRuntimeNetworkSocketOfdIds,
  hasRuntimeNetworkSocketState,
} from "../ownership/socket-control.js";
import { clearStaleForkSharedSyncGate } from "../fork-shared-sync-gate.js";
import { registerForkSharedSyncPromise } from "../fork-shared-sync-debug.js";
import type {
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPidStateSnapshot,
  ProcessHandle,
  SyncCause
} from "./shared.js";

export function blockedStatusUsesForkSharedStateSync(handle: ProcessHandle, status: number): boolean {
  return status === handle.kernel.abi_status_pipe_wait()
    || status === handle.kernel.abi_status_socket_recv()
    || status === handle.kernel.abi_status_socket_send()
    || status === handle.kernel.abi_status_flock_wait();
}

export function syncForkSharedStateLocally(
  handle: ProcessHandle,
  targets: ProcessHandle[],
  options: { cause: SyncCause; includeMemfs?: boolean; trackHeldOfds?: boolean; ofdIds?: number[] },
  collectTrackableOfds: (handle: ProcessHandle) => Set<number> = collectTrackableForkSharedOfds,
): void {
  if (options.trackHeldOfds === true) {
    for (const ofdId of collectTrackableOfds(handle)) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
    }
  }
  const activeOfdIds = activeKernelShareOfdIds(handle);
  const allowedOfdIdSet = options.ofdIds === undefined ? null : new Set(options.ofdIds);
  const staleOfdIds = staleKernelShareOfdIds(handle);
  syncKernelShareOfdReservations(handle);
  const syncOfdIds = [...new Set(
    allowedOfdIdSet === null
      ? activeOfdIds
      : activeOfdIds.filter((ofdId) => allowedOfdIdSet.has(ofdId)),
  )].sort((left, right) => left - right);
  mergeKernelShareOfdStateInto(handle, syncOfdIds);
  reconcileForkSharedOfdRefcounts(handle, syncOfdIds);
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState(handle.kernelShare.members);
  const processStateSnapshots = snapshotProcessStates(targets);
  try {
    for (const target of targets) {
      copyLegacySharedKernelObjectsLocal(handle, target, syncOfdIds, options);
      attachKernelRuntimeState(target.kernel, target.kernelState);
    }
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
    restoreProcessStates(processStateSnapshots);
  }
  pruneStaleKernelShareOfdIds(handle, staleOfdIds);
}

export function syncForkSharedStateLocallyForWorkerRuntime(
  handle: ProcessHandle,
  targets: ProcessHandle[],
  options: { cause?: SyncCause; includeMemfs?: boolean; trackHeldOfds?: boolean; ofdIds?: number[] } = {},
): void {
  syncForkSharedStateLocally(
    handle,
    targets,
    { ...options, cause: options.cause ?? { kind: "syscall", nr: -1n, name: "fork-shared-local-sync" } },
  );
}

function isQuiescentForkSharedSyncTarget(target: ProcessHandle): boolean {
  return target.runningWorker === null
    && target.vforkReadyWorkers.length === 0
    && target.hostBlockedWorkers.size > 0
    && !hasPendingBlockedRpc(target)
    && !target.exitInProgress;
}

function hasPendingBlockedRpc(handle: ProcessHandle): boolean {
  return [...handle.hostBlockedWorkers.values()].some((blocked) => blocked.rpcPending);
}

function pruneUnpeeredRegularFileSharedOfds(handle: ProcessHandle): void {
  for (const ofdId of [...handle.kernelShare.sharedOfdIds]) {
    if (!isRegularFileOfd(handle, ofdId)) {
      continue;
    }
    if (sourceForkSharedOfdPeerState(handle, ofdId).compatiblePeer) {
      continue;
    }
    for (const member of handle.kernelShare.members) {
      member.kernel.kernel_unreserve_ofd?.(ofdId);
    }
    handle.kernelShare.sharedOfdIds.delete(ofdId);
    handle.kernelShare.ofdSlotSnapshots.delete(ofdId);
  }
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
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    throw new Error("syncForkSharedStateViaKernelWorker requires kernel-worker ownership");
  }
  const quiescentTargets = targets.filter(isQuiescentForkSharedSyncTarget);
  let syncTargets = options.includeMemfs === true
    ? quiescentTargets.filter((target) => !hasRuntimeNetworkSocketState(target))
    : quiescentTargets;
  const memfsOnly = options.memfsOnly === true;

  if (!memfsOnly && options.trackHeldOfds === true) {
    trackHeldForkSharedOfds(handle, options.sourceVmPtr ?? handle.vmPtr);
    pruneUnpeeredRegularFileSharedOfds(handle);
  }
  const syncableForkSharedOfd = (ofdId: number): boolean => !isProcessHostStdioPipeOfd(handle, ofdId);
  const sourceVmPtr = options.sourceVmPtr ?? handle.vmPtr;
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
      if (!handle.kernelShare.sharedOfdIds.has(ofdId)) {
        continue;
      }
      if (sourceOfdCanPublishForkSharedState(handle, ofdId)) {
        sourcePublishedOfdIds.add(ofdId);
      } else {
        sourceConflictingOfdIds.add(ofdId);
      }
    }
  }
  const canSyncForkSharedOfd = (ofdId: number): boolean =>
    syncableForkSharedOfd(ofdId)
    && !sourceConflictingOfdIds.has(ofdId)
    && (allowedOfdIdSet === null || allowedOfdIdSet.has(ofdId))
    && (!isRegularFileOfd(handle, ofdId) || sourcePublishedOfdIds.has(ofdId));
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
  const runtimeNetworkSocketOfdIds = collectRuntimeNetworkSocketOfdIds(handle.kernelShare.members);
  const preservedRuntimeNetworkKernelStates = [...handle.kernelShare.members].map((member) =>
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
        .filter((ofdId) =>
          (handle.kernelShare.sharedOfdIds.has(ofdId) || sourceFdOfdIdSet.has(ofdId))
          && canSyncForkSharedOfd(ofdId)
          && handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_none()
          && (sourceFdOfdIdSet.has(ofdId) || !sourceForkSharedOfdPeerState(handle, ofdId).incompatiblePeer)
        )
    : [];
  let syncOfdIds = [...new Set([...publishableSourceHeldOfdIds, ...publishableSourceFdOfdIds, ...activeOfdIds])]
    .sort((left, right) => left - right);
  if (options.deltaOfdIds) {
    const sourceForkSharedFdOfdIds = sourceFdOfdIds
      .filter((ofdId) => {
        if (allowedOfdIdSet !== null && !allowedOfdIdSet.has(ofdId)) {
          return false;
        }
        if (sourcePublishedOfdIds.has(ofdId)) {
          return true;
        }
        if (sourceHeldOfdIds.has(ofdId) && hasPipePeerInOtherKernelShareMember(handle, ofdId)) {
          handle.kernelShare.sharedOfdIds.add(ofdId);
          return true;
        }
        return false;
      });
    const tracedOfdIds = collectTracedForkSharedOfdIds(handle, sourceVmPtr)
      .filter((ofdId) => {
        if (allowedOfdIdSet !== null && !allowedOfdIdSet.has(ofdId)) {
          return false;
        }
        if (sourcePublishedOfdIds.has(ofdId)) {
          return true;
        }
        if (sourceHeldOfdIds.has(ofdId) && hasPipePeerInOtherKernelShareMember(handle, ofdId)) {
          handle.kernelShare.sharedOfdIds.add(ofdId);
          return true;
        }
        return false;
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
      if (staleOfdIdSet.has(ofdId) || !syncableForkSharedOfd(ofdId)) {
        continue;
      }
      handle.kernelShare.sharedOfdIds.add(ofdId);
      deltaOfdIdSet.add(ofdId);
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
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState(handle.kernelShare.members);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
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
    handle.kernelShare.sharedOfdIds.add(ofdId);
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
  preserveHostStdioPipeControlForStep(handle, handle.vmPtr);
  for (const member of kernelShareOtherMembers(handle)) {
    if (member.runningWorker !== null || member.exitInProgress) {
      continue;
    }
    preserveHostStdioPipeControlForStep(member, member.vmPtr);
  }
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
    .filter((ofdId) =>
      !heldAfterSync.has(ofdId)
      && (allowedOfdIdSet === null || allowedOfdIdSet.has(ofdId))
    );
  if (unheldActiveOfdIds.length > 0) {
    await rehydrateForkSharedStateViaKernelWorker([handle], unheldActiveOfdIds, {
      cause: options.cause,
      includeMemfs: false,
    });
  }
}

export async function syncForkSharedStateForWorkerRuntime(
  handle: ProcessHandle,
  targets: ProcessHandle[],
  options: {
    cause?: SyncCause;
    includeMemfs?: boolean;
    trackHeldOfds?: boolean;
    deltaOfdIds?: boolean;
    memfsOnly?: boolean;
    preserveSourceRefcounts?: boolean;
    sourceVmPtr?: number;
    ofdIds?: number[];
  } = {},
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    syncForkSharedStateLocallyForWorkerRuntime(handle, targets, options);
    return;
  }

  if (options.trackHeldOfds === true && options.deltaOfdIds !== true) {
    for (const ofdId of collectTrackableForkSharedOfds(handle)) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
    }
  }
  await syncForkSharedStateViaKernelWorker(
    handle,
    targets,
    { ...options, cause: options.cause ?? { kind: "syscall", nr: -1n, name: "fork-shared-state-sync" } },
  );
}

export async function rehydrateForkSharedStateViaKernelWorker(
  targets: ProcessHandle[],
  ofdIds: number[],
  options: { cause: SyncCause; procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean },
): Promise<void> {
  if (targets.length === 0) {
    return;
  }
  const owner = targets.find((target) => target.kernelWorkerRequest && target.pid > 0);
  if (!owner?.kernelWorkerRequest) {
    return;
  }
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
  if (targetPids.length === 0) {
    return;
  }
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState(targets);
  appendSocketSyncDebug(
    owner,
    `fork-shared-rehydrate:${owner.pid}:${formatSyncCause(options.cause)}:targets=${targetPids.join(",") || "none"}:ofds=${ofdIds.join(",") || "none"}`,
  );
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await owner.kernelWorkerRequest({
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

export async function rehydrateForkSharedStateForWorkerRuntime(
  targets: ProcessHandle[],
  ofdIds: number[],
  options: { procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean } = {},
): Promise<void> {
  await rehydrateForkSharedStateViaKernelWorker(targets, ofdIds, {
    ...options,
    cause: { kind: "syscall", nr: -1n, name: "fork-shared-rehydrate" },
  });
}

export async function syncBlockedForkSharedStateViaKernelWorker(
  handle: ProcessHandle,
  sourceVmPtr = handle.vmPtr,
  cause: SyncCause,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }
  const activeSync = handle.kernelShare.forkSharedSyncPromise ?? handle.forkSharedSyncPromise;
  if (activeSync !== null) {
    const staleGate = clearStaleForkSharedSyncGate(handle, activeSync);
    if (staleGate === null) {
      await activeSync;
    } else {
      appendSocketSyncDebug(handle, `fork-shared-sync-clear-stale:${handle.pid}:${staleGate}`);
    }
  }
  const syncTargets = kernelShareOtherMembers(handle)
    .filter((member) => (
      member.runningWorker === null
      && !member.exitInProgress
      && !hasPendingBlockedRpc(member)
    ));
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
    if (handle.forkSharedSyncPromise === syncPromise) {
      handle.forkSharedSyncPromise = null;
    }
    if (handle.kernelShare.forkSharedSyncPromise === syncPromise) {
      handle.kernelShare.forkSharedSyncPromise = null;
    }
  });
  registerForkSharedSyncPromise(syncPromise, {
    ownerPid: handle.pid,
    sourceVmPtr,
    reason: "blocked-rpc",
    targetPids: syncTargets.map((target) => target.pid),
  });
  handle.forkSharedSyncPromise = syncPromise;
  handle.kernelShare.forkSharedSyncPromise = syncPromise;
  await syncPromise;
}

export async function syncBlockedForkSharedStateForWorkerRuntime(
  handle: ProcessHandle,
  sourceVmPtr?: number,
): Promise<void> {
  await syncBlockedForkSharedStateViaKernelWorker(
    handle,
    sourceVmPtr,
    { kind: "blocked-rpc", status: handle.kernel.abi_status_pipe_wait() },
  );
}
