import type { SyncCause } from "./effects.js";
import type { KernelRuntimeState } from "../../abi.js";
import type { ProcessHandle } from "../types.js";

export function hostStdioReleaseCause(): SyncCause {
  return { kind: "syscall", nr: -1n, name: "release-host-stdio" };
}

interface HostStdioReleaseDeps<TSocketSnapshot> {
  snapshotNetworkBridgeSocketState(handles: ProcessHandle[]): TSocketSnapshot[];
  restoreNetworkBridgeSocketState(snapshots: TSocketSnapshot[]): void;
  adoptKernelStateForHandle(handle: ProcessHandle, incoming: KernelRuntimeState): KernelRuntimeState;
  attachKernelRuntimeState(kernel: ProcessHandle["kernel"], state: KernelRuntimeState): void;
}

export async function releaseHostStdioViaKernelWorkerForPlane<TSocketSnapshot>(
  handle: ProcessHandle,
  deps: HostStdioReleaseDeps<TSocketSnapshot>,
  cause: SyncCause = hostStdioReleaseCause(),
): Promise<bigint> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    handle.kernel.process_release_host_stdio(handle.vmPtr);
    return handle.kernelState.pipeUsed;
  }

  void cause;
  const networkSocketSnapshots = deps.snapshotNetworkBridgeSocketState([handle]);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "release-host-stdio",
      pid: handle.pid,
      vmPtr: handle.vmPtr,
      kernelState: handle.kernelState,
    });
  } finally {
    deps.restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "host-stdio-released") {
    throw new Error(`unexpected kernel-worker response for release-host-stdio: ${response.type}`);
  }
  handle.kernelState = deps.adoptKernelStateForHandle(handle, response.kernelState);
  deps.restoreNetworkBridgeSocketState(networkSocketSnapshots);
  deps.attachKernelRuntimeState(handle.kernel, handle.kernelState);
  return handle.kernelState.pipeUsed;
}
