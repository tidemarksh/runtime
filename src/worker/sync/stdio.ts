import {
  adoptKernelStateForHandle,
  appendSocketSyncDebug,
  attachKernelRuntimeState,
  formatSyncCause,
  snapshotNetworkBridgeSocketState,
  restoreNetworkBridgeSocketState,
} from "./shared.js";
import type {
  ProcessHandle,
  SyncCause,
} from "./shared.js";

export function hostStdioReleaseCause(): SyncCause {
  return { kind: "syscall", nr: -1n, name: "release-host-stdio" };
}

export async function releaseHostStdioViaKernelWorker(
  handle: ProcessHandle,
  cause: SyncCause = hostStdioReleaseCause(),
): Promise<bigint> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    handle.kernel.process_release_host_stdio(handle.vmPtr);
    return handle.kernelState.pipeUsed;
  }

  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "release-host-stdio",
      pid: handle.pid,
      vmPtr: handle.vmPtr,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "host-stdio-released") {
    throw new Error(`unexpected kernel-worker response for release-host-stdio: ${response.type}`);
  }
  appendSocketSyncDebug(handle, `host-stdio-release:${handle.pid}:${formatSyncCause(cause)}`);
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  return handle.kernelState.pipeUsed;
}
