import {
  attachKernelRuntimeState,
  collectHeldEpollOfdIds,
  collectProcessHeldOfds,
  ofdIdsFromFdEntries,
  adoptKernelStateForHandle,
  appendSocketSyncDebug,
  formatSyncCause,
  snapshotKernelShareOfdSlots,
  snapshotProcessFdEntries,
  snapshotNetworkBridgeSocketState,
  restoreNetworkBridgeSocketState
} from "./shared.js";
import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  ProcessHandle,
  SyncCause
} from "./shared.js";

export function snapshotProcessFdOfdStateForKernelWorker(
  handle: ProcessHandle,
  vmPtr: number,
): {
  fdEntries: KernelWorkerFdEntrySnapshot[];
  ofdSlots: KernelWorkerOfdSlotSnapshot[];
} {
  const fdEntries = snapshotProcessFdEntries(handle, vmPtr);
  const ofdIds = [...new Set([
    ...ofdIdsFromFdEntries(handle, fdEntries),
    ...collectProcessHeldOfds(handle),
  ])];
  const ofdSlots = [...snapshotKernelShareOfdSlots(handle, ofdIds)]
    .map(([ofdId, slot]) => ({ ofdId, slot }));
  return { fdEntries, ofdSlots };
}

export async function syncEpollStateViaKernelWorker(
  handle: ProcessHandle,
  cause: SyncCause,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }
  if (collectHeldEpollOfdIds(handle).length === 0) {
    return;
  }

  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  appendSocketSyncDebug(handle, `epoll-sync-request:${handle.pid}:${formatSyncCause(cause)}`);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "sync-epoll-state",
      pid: handle.pid,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "epoll-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-epoll-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

export async function syncOfdStateViaKernelWorker(
  handle: ProcessHandle,
  ofdIds: number[],
  cause: SyncCause,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0 || ofdIds.length === 0) {
    return;
  }

  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  appendSocketSyncDebug(
    handle,
    `ofd-sync-request:${handle.pid}:${formatSyncCause(cause)}:ofds=${ofdIds.join(",") || "none"}`,
  );
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "sync-ofd-state",
      pid: handle.pid,
      ofdIds,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "ofd-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-ofd-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}
