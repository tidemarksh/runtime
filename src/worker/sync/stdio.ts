import {
  appendSocketSyncDebug,
  attachKernelRuntimeState,
  formatSyncCause,
  snapshotNetworkBridgeSocketState,
  restoreNetworkBridgeSocketState,
} from "./shared.js";
import { adoptKernelStateForHandle } from "../kernel-state.js";
import { requireKernelWorkerOwnership } from "../kernel-worker-ownership.js";
import { processExitInProgress } from "../process-lifecycle-state.js";
import type {
  ProcessHandle,
  SyncCause,
} from "./shared.js";

export interface SharedHostStdioReleasePorts {
  wakePipeAndEpollWaitersInKernelShare(handle: ProcessHandle, pipeUsed: bigint): void;
}

export function hostStdioReleaseCause(): SyncCause {
  return { kind: "syscall", nr: -1n, name: "release-host-stdio" };
}

export async function releaseHostStdioViaKernelWorker(
  handle: ProcessHandle,
  cause: SyncCause,
): Promise<bigint> {
  const kernelWorkerRequest = requireKernelWorkerOwnership(handle, "releaseHostStdioViaKernelWorker");

  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  let response: Awaited<ReturnType<typeof kernelWorkerRequest>>;
  try {
    response = await kernelWorkerRequest({
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

export async function releaseSharedHostStdioViaKernelWorker(
  handle: ProcessHandle,
  ports: SharedHostStdioReleasePorts,
): Promise<void> {
  const hadLocalHostStdio = handle.stdinPipe !== 0 || handle.stdoutPipe !== 0 || handle.stderrPipe !== 0;
  if (!hadLocalHostStdio) {
    return;
  }
  const shared = handle.hostStdio;
  if (shared.refCount > 0) {
    shared.refCount -= 1;
  }
  handle.stdinPipe = 0;
  handle.stdoutPipe = 0;
  handle.stderrPipe = 0;
  if (shared.refCount > 0) {
    return;
  }
  if (handle.stdioOwner !== null && !processExitInProgress(handle.stdioOwner)) {
    return;
  }
  const pipeUsed = await releaseHostStdioViaKernelWorker(handle, hostStdioReleaseCause());
  ports.wakePipeAndEpollWaitersInKernelShare(handle, pipeUsed);
}
