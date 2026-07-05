import {
  attachKernelRuntimeState,
  appendSocketSyncDebug,
  formatSyncCause,
  snapshotNetworkBridgeSocketState,
  restoreNetworkBridgeSocketState
} from "../sync/shared.js";
import { adoptKernelStateForHandle } from "../kernel-state.js";
import {
  collectHeldEpollOfdIds,
  collectProcessHeldOfds,
} from "../ownership/ofd-table.js";
import {
  ofdIdsFromFdEntries,
  snapshotProcessFdEntries,
} from "../ownership/fd-table.js";
import { snapshotKernelShareOfdSlots } from "../fork-shared.js";
import { requireKernelWorkerOwnership } from "../kernel-worker-ownership.js";
import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  ProcessHandle,
  SyncCause
} from "../sync/shared.js";

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
  if (collectHeldEpollOfdIds(handle).length === 0) {
    return;
  }
  const kernelWorkerRequest = requireKernelWorkerOwnership(handle, "syncEpollStateViaKernelWorker");

  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  appendSocketSyncDebug(handle, `epoll-sync-request:${handle.pid}:${formatSyncCause(cause)}`);
  let response: Awaited<ReturnType<typeof kernelWorkerRequest>>;
  try {
    response = await kernelWorkerRequest({
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
  options: { mode?: "push" | "pull" } = {},
): Promise<void> {
  if (ofdIds.length === 0) {
    return;
  }
  const kernelWorkerRequest = requireKernelWorkerOwnership(handle, "syncOfdStateViaKernelWorker");

  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  appendSocketSyncDebug(
    handle,
    `ofd-sync-request:${handle.pid}:${formatSyncCause(cause)}:ofds=${ofdIds.join(",") || "none"}`,
  );
  let response: Awaited<ReturnType<typeof kernelWorkerRequest>>;
  try {
    response = await kernelWorkerRequest({
      type: "sync-ofd-state",
      pid: handle.pid,
      ofdIds,
      mode: options.mode,
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
