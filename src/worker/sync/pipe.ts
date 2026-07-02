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
} from "./shared.js";
import {
  kernelShareOtherMembers,
  reconcileKernelSharePipeControl,
  restoreLocalOfdSlots,
} from "../kernel-share.js";
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
import { isRegularFileOfd } from "../ownership/ofd-peer.js";
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
import type {
  ProcessHandle,
  SyncCause
} from "./shared.js";

export {
  formatPipeSnapshotDebug,
  int32AtomicView,
  loadPipeI32,
  loadPipeU32,
} from "./shared.js";

export async function syncPipeStateViaKernelWorker(
  handle: ProcessHandle,
  cause: SyncCause,
): Promise<bigint> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return handle.kernelState.pipeUsed;
  }
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
  const preservedRuntimeNetworkKernelStates = [...handle.kernelShare.members].map((member) =>
    snapshotRuntimeNetworkKernelState(member)
  );
  const pipeOfdIds = [...new Set([
    ...collectProcessHeldOfds(handle),
    ...fdEntryOfdIds,
    ...activeKernelShareOfdIds(handle),
  ])]
    .filter((ofdId) => {
      const fdType = handle.kernel.kernel_ofd_fd_type(ofdId);
      return fdType === handle.kernel.abi_fd_type_pipe()
        || fdType === handle.kernel.abi_fd_type_pty_master?.()
        || fdType === handle.kernel.abi_fd_type_pty_slave?.();
    });
  const preservedLocalPipeOfdSlots = snapshotLocalOfdSlots(handle, pipeOfdIds.filter((ofdId) => (
    fdEntryOfdIds.includes(ofdId)
  )));
  const ofdSlots = [...snapshotKernelShareOfdSlots(handle, pipeOfdIds)]
    .map(([ofdId, slot]) => ({ ofdId, slot }));
  const pendingPipeSlots = handle.pendingPipeSlotSnapshots.slice();
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
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState(handle.kernelShare.members);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  const applyFdEntries = handle.runningWorker === null
    && handle.vforkReadyWorkers.length === 0
    && handle.workers.size === 0
    && handle.hostBlockedWorkers.size === 0;
  appendSocketSyncDebug(
    handle,
    [
      `pipe-sync-request:${handle.pid}`,
      formatSyncCause(cause),
      `vm=0x${handle.vmPtr.toString(16)}`,
      `applyFd=${applyFdEntries ? 1 : 0}`,
      `running=${handle.runningWorker === null ? 0 : 1}`,
      `ready=${handle.vforkReadyWorkers.length}`,
      `workers=${handle.workers.size}`,
      `blocked=${handle.hostBlockedWorkers.size}`,
      `stdioOwner=${handle.stdioOwner?.pid ?? 0}`,
      formatFdSnapshotDebug("fds", fdEntries),
      `pipeOfds=${formatOfdIdList(pipeOfdIds)}`,
      `localPipeOfds=${formatOfdIdList(localPipeOfdIds)}`,
      formatOfdSnapshotDebug("ofds", ofdSlots),
    ].join(":"),
  );
  try {
    response = await handle.kernelWorkerRequest({
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
  if (pendingPipeSlots.length > 0) {
    handle.pendingPipeSlotSnapshots.splice(0, pendingPipeSlots.length);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  if (response.pipeSlots && response.pipeSlots.length > 0) {
    mergePipeSlotSnapshotsIntoHandle(handle, response.pipeSlots);
    for (const member of kernelShareOtherMembers(handle)) {
      if (member.runningWorker !== null || member.exitInProgress) {
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
      if (member.runningWorker !== null || member.exitInProgress) {
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

export function syncPipeStateForWorkerRuntime(handle: ProcessHandle): Promise<bigint> {
  return syncPipeStateViaKernelWorker(handle, { kind: "syscall", nr: -1n, name: "pipe-state-sync" });
}
