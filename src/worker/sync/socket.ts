import {
  attachKernelRuntimeState,
  appendSocketSyncDebug,
  formatSyncCause,
  formatNetworkSocketDebug,
  snapshotNetworkBridgeSocketState,
  snapshotSocketStateForKernelWorker,
  applySocketStateFromKernelWorker,
  restoreNetworkBridgeSocketState,
  networkBridgeSocketSnapshotProgressed
} from "./shared.js";
import {
  adoptKernelState,
  adoptKernelStateForHandle,
  repairHandleVmPidAfterKernelStateAttach,
} from "../kernel-state.js";
import { requireKernelWorkerOwnership } from "../kernel-worker-ownership.js";
import type {
  KernelWorkerSocketStateSnapshot,
  ProcessHandle,
  SyncCause
} from "./shared.js";

export {
  appendSocketSyncDebug,
  applySocketStateFromKernelWorker,
  socketSyncDebugLogEnabled,
} from "./shared.js";

export async function syncSocketStateViaKernelWorker(
  handle: ProcessHandle,
  cause: SyncCause,
): Promise<{
  socketUsed: number;
  netBufUsed: bigint;
  socketState?: KernelWorkerSocketStateSnapshot;
}> {
  const kernelWorkerRequest = requireKernelWorkerOwnership(handle, "syncSocketStateViaKernelWorker");

  const pidBeforeSync = handle.vmPtr === 0
    ? 0
    : handle.kernel.process_current_pid(handle.vmPtr);
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  appendSocketSyncDebug(
    handle,
    `socket-sync-start:${handle.pid}:${formatSyncCause(cause)}:${formatNetworkSocketDebug(handle)}`,
  );
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await kernelWorkerRequest({
      type: "sync-socket-state",
      pid: handle.pid,
      kernelState: handle.kernelState,
      socketState: snapshotSocketStateForKernelWorker(handle),
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
    appendSocketSyncDebug(handle, `socket-sync-finally-restore:${handle.pid}:${formatNetworkSocketDebug(handle)}`);
  }
  if (response.type !== "socket-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-socket-state: ${response.type}`);
  }
  const localNetworkProgress = networkSocketSnapshots.some(networkBridgeSocketSnapshotProgressed);
  handle.kernelState = response.socketState && !localNetworkProgress
    ? adoptKernelState(handle.kernelState, response.kernelState)
    : adoptKernelStateForHandle(handle, response.kernelState);
  if (response.socketState && !localNetworkProgress) {
    applySocketStateFromKernelWorker(handle, response.socketState);
    appendSocketSyncDebug(
      handle,
      `socket-sync-response-apply:${handle.pid}:${formatNetworkSocketDebug(handle)}`,
    );
  } else if (response.socketState) {
    appendSocketSyncDebug(handle, `socket-sync-response-skip-progress:${handle.pid}:${formatNetworkSocketDebug(handle)}`);
  }
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  appendSocketSyncDebug(handle, `socket-sync-restore:${handle.pid}:${formatNetworkSocketDebug(handle)}`);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  repairHandleVmPidAfterKernelStateAttach(handle, handle.vmPtr, pidBeforeSync);
  return {
    socketUsed: handle.kernelState.socketUsed,
    netBufUsed: handle.kernelState.netBufUsed,
    socketState: response.socketState,
  };
}
