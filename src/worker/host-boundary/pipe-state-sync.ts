import { hostExecutionState } from "../host-execution-state.js";
import {
  processStdioOwner,
} from "../process-stdio-state.js";
import { processWorkerCount } from "../process-worker-state.js";
import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  formatPipeSnapshotDebug,
  appendSocketSyncDebug,
  formatSyncCause,
  formatFdSnapshotDebug,
  formatOfdSnapshotDebug,
  formatOfdIdList,
  mergePipeSlotSnapshotsIntoHandle,
  snapshotFreshDuplexPipeSlots,
  snapshotNonEmptyPipeSlotsForOfds,
  snapshotNonEmptyHostOutputPipeSlots,
  snapshotNetworkBridgeSocketState,
  restoreNetworkBridgeSocketState,
  snapshotRuntimeNetworkFdEntries,
  snapshotRuntimeNetworkKernelState,
  restoreRuntimeNetworkKernelState
} from "./shared-state.js";
import {
  kernelShareMemberList,
  kernelShareOtherMembers,
  reconcileKernelSharePipeControl,
  restoreLocalOfdSlots,
} from "../kernel-share.js";
import { processExitInProgress } from "../process-lifecycle-state.js";
import { snapshotKernelShareOfdSlots } from "../fork-shared.js";
import { adoptKernelStateForHandle } from "../kernel-state.js";
import {
  activeKernelShareOfdIds,
  collectProcessHeldOfds,
  restoreIncompatibleLocalOfdSlots,
  restoreRuntimeNetworkOfdSlots,
  snapshotLocalOfdSlots,
  snapshotRuntimeNetworkOfdSlots,
} from "../ownership/ofd-table.js";
import {
  isPipeSyncOfdCandidate,
  isRegularFileOfd,
} from "../ownership/ofd-peer.js";
import {
  ofdIdsFromFdEntries,
  restoreProcessFdEntries,
  snapshotProcessFdEntries,
} from "../ownership/fd-table.js";
import {
  restoreProcessState,
  snapshotProcessState,
} from "../ownership/process-state.js";
import { collectRuntimeNetworkSocketOfdIds } from "../ownership/socket-control.js";
import { requireKernelWorkerOwnership } from "../kernel-worker-ownership.js";
import {
  dropPendingPipeSlotSnapshotPrefix,
  snapshotPendingPipeSlotSnapshots,
} from "../pending-pipe-slot-state.js";
import type {
  ProcessHandle,
  SyncCause
} from "./shared-state.js";

export {
  formatPipeSnapshotDebug,
  int32AtomicView,
  loadPipeI32,
  loadPipeU32,
} from "./shared-state.js";

export async function syncPipeStateViaKernelWorker(
  handle: ProcessHandle,
  cause: SyncCause,
): Promise<bigint> {
  const kernelWorkerRequest = requireKernelWorkerOwnership(handle, "syncPipeStateViaKernelWorker");
  // Process FD tables are authoritative for pipe endpoint ownership. Publish
  // their aggregate before taking snapshots so endpoint closure reaches the
  // canonical kernel and blocked workers in this sync, not the next one.
  reconcileKernelSharePipeControl(handle);
  const processStateSnapshot = snapshotProcessState(handle);
  const fdEntries = snapshotProcessFdEntries(handle, handle.vmPtr);
  const fdEntryOfdIds = ofdIdsFromFdEntries(handle, fdEntries);
  const localPipeOfdIds = [...new Set([
    ...collectProcessHeldOfds(handle),
    ...fdEntryOfdIds,
  ])];
  const preservedFdOfdSlots = snapshotLocalOfdSlots(
    handle,
    fdEntryOfdIds.filter((ofdId) => !isRegularFileOfd(handle, ofdId)),
  );
  const runtimeNetworkSocketOfdIds = collectRuntimeNetworkSocketOfdIds([handle]);
  const preservedRuntimeNetworkSocketOfdSlots = snapshotRuntimeNetworkOfdSlots(
    handle,
    runtimeNetworkSocketOfdIds,
  );
  const preservedRuntimeNetworkFdEntries = snapshotRuntimeNetworkFdEntries(
    handle,
    handle.vmPtr,
    runtimeNetworkSocketOfdIds,
  );
  const preservedRuntimeNetworkKernelStates = kernelShareMemberList(handle).map((member) =>
    snapshotRuntimeNetworkKernelState(member)
  );
  const pipeOfdIds = [...new Set([
    ...collectProcessHeldOfds(handle),
    ...fdEntryOfdIds,
    ...activeKernelShareOfdIds(handle),
  ])].filter((ofdId) => isPipeSyncOfdCandidate(handle, ofdId));
  const preservedLocalPipeOfdSlots = snapshotLocalOfdSlots(handle, pipeOfdIds.filter((ofdId) => (
    fdEntryOfdIds.includes(ofdId)
  )));
  const ofdSlots = [...snapshotKernelShareOfdSlots(handle, pipeOfdIds)]
    .map(([ofdId, slot]) => ({ ofdId, slot }));
  const pendingPipeSlots = snapshotPendingPipeSlotSnapshots(handle);
  const hostOutputPipeSlots = snapshotNonEmptyHostOutputPipeSlots(handle, pendingPipeSlots);
  const pendingAndHostPipeSlots = [...pendingPipeSlots, ...hostOutputPipeSlots];
  const pipeSlots = [
    ...pendingPipeSlots,
    ...hostOutputPipeSlots,
    ...snapshotNonEmptyPipeSlotsForOfds(handle, localPipeOfdIds, pendingAndHostPipeSlots),
    ...snapshotFreshDuplexPipeSlots(handle, fdEntries, pendingAndHostPipeSlots),
  ];
  appendSocketSyncDebug(
    handle,
    `pipe-sync-request-slots:${handle.pid}:${formatPipeSnapshotDebug("slots", pipeSlots)}`,
  );
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState(kernelShareMemberList(handle));
  let response: Awaited<ReturnType<typeof kernelWorkerRequest>>;
  const applyFdEntries = hostExecutionState(handle).runningWorker === null
    && hostExecutionState(handle).vforkReadyWorkers.length === 0;
  appendSocketSyncDebug(
    handle,
    [
      `pipe-sync-request:${handle.pid}`,
      formatSyncCause(cause),
      `vm=0x${handle.vmPtr.toString(16)}`,
      `applyFd=${applyFdEntries ? 1 : 0}`,
      `running=${hostExecutionState(handle).runningWorker === null ? 0 : 1}`,
      `ready=${hostExecutionState(handle).vforkReadyWorkers.length}`,
      `workers=${processWorkerCount(handle)}`,
      `blocked=${hostExecutionState(handle).hostBlockedWorkers.size}`,
      `stdioOwner=${processStdioOwner(handle)?.pid ?? 0}`,
      formatFdSnapshotDebug("fds", fdEntries),
      `pipeOfds=${formatOfdIdList(pipeOfdIds)}`,
      `localPipeOfds=${formatOfdIdList(localPipeOfdIds)}`,
      formatOfdSnapshotDebug("ofds", ofdSlots),
    ].join(":"),
  );
  try {
    response = await kernelWorkerRequest({
      type: "sync-pipe-state",
      pid: handle.pid,
      vmPtr: handle.vmPtr,
      fdEntries,
      applyFdEntries,
      ofdSlots,
      pipeSlots,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "pipe-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-pipe-state: ${response.type}`);
  }
  appendSocketSyncDebug(
    handle,
    `pipe-sync-response-slots:${handle.pid}:${formatPipeSnapshotDebug("slots", response.pipeSlots ?? [])}`,
  );
  dropPendingPipeSlotSnapshotPrefix(handle, pendingPipeSlots.length);
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  if (response.pipeSlots && response.pipeSlots.length > 0) {
    mergePipeSlotSnapshotsIntoHandle(handle, response.pipeSlots);
    for (const member of kernelShareOtherMembers(handle)) {
      if (hostExecutionState(member).runningWorker !== null || processExitInProgress(member)) {
        continue;
      }
      mergePipeSlotSnapshotsIntoHandle(member, response.pipeSlots);
    }
  }
  if (hostOutputPipeSlots.length > 0) {
    mergePipeSlotSnapshotsIntoHandle(handle, hostOutputPipeSlots);
  }
  if (pipeSlots.length > 0) {
    for (const member of kernelShareOtherMembers(handle)) {
      if (hostExecutionState(member).runningWorker !== null || processExitInProgress(member)) {
        continue;
      }
      mergePipeSlotSnapshotsIntoHandle(member, pipeSlots);
    }
  }
  restoreLocalOfdSlots(handle, preservedFdOfdSlots);
  restoreRuntimeNetworkOfdSlots(handle, preservedRuntimeNetworkSocketOfdSlots);
  restoreProcessFdEntries(handle, handle.vmPtr, preservedRuntimeNetworkFdEntries);
  for (const snapshot of preservedRuntimeNetworkKernelStates) {
    restoreRuntimeNetworkKernelState(snapshot);
  }
  restoreIncompatibleLocalOfdSlots(handle, preservedLocalPipeOfdSlots);
  reconcileKernelSharePipeControl(handle);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  restoreProcessState(handle, processStateSnapshot, { preserveCurrentFdTable: true });
  return handle.kernelState.pipeUsed;
}
