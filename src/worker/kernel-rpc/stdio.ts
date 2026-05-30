import {
  attachKernelRuntimeState,
} from "../../abi.js";
import {
  adoptKernelStateForHandle,
} from "../kernel-state.js";
import { describeSyncCause, type SyncCause } from "../contracts/sync.js";
import type { ProcessHandle } from "../types.js";
import type { WorkerKernelRpcLifecycleDeps } from "./lifecycle/types.js";
import { expectKernelWorkerResponse } from "./transport.js";

export function createWorkerKernelRpcStdioHelpers(
  deps: Pick<WorkerKernelRpcLifecycleDeps, "appendWorkerDebugLog">,
) {
  async function releaseHostStdioViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<bigint> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return 0n;
    }
    deps.appendWorkerDebugLog(handle, `release-host-stdio:rpc-start:${describeSyncCause(cause)}`);
    const response = expectKernelWorkerResponse(await handle.kernelWorkerRequest({
      type: "release-host-stdio",
      pid: handle.pid,
      vmPtr: handle.vmPtr,
      kernelState: handle.kernelState,
    }), "host-stdio-released", "release-host-stdio");
    handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    deps.appendWorkerDebugLog(handle, `release-host-stdio:rpc-done:${describeSyncCause(cause)}`);
    return handle.kernelState.pipeUsed;
  }

  return {
    releaseHostStdioViaKernelWorker,
  };
}
