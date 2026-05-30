import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import {
  adoptKernelStateForHandle,
} from "../sync.js";
import { settleVisibleProcessOutput } from "../stdio.js";
import type { ProcessHandle } from "../types.js";
import type { WorkerKernelRpcLifecycleDeps } from "./lifecycle/types.js";
import { expectKernelWorkerResponse } from "./transport.js";

export function createWorkerKernelRpcWait4Helpers(
  deps: WorkerKernelRpcLifecycleDeps,
) {
  async function resumeWait4ViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
  ): Promise<void> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return;
    }

    try {
      const localStatus = handle.kernel.thread_status(vmPtr);
      if (localStatus !== handle.kernel.abi_status_wait4()) {
        const blocked = handle.blockedWorkers.get(worker);
        if (blocked && blocked.vmPtr === vmPtr) {
          blocked.rpcPending = false;
        }
        deps.appendWorkerDebugLog(
          handle,
          `wait4-rpc-skip:0x${vmPtr.toString(16)}:local=${localStatus}`,
        );
        deps.handleBlockedRetryResult(handle, worker, vmPtr, localStatus);
        return;
      }
      deps.appendWorkerDebugLog(
        handle,
        `wait4-rpc-start:0x${vmPtr.toString(16)}:local=${localStatus}`,
      );
      const response = expectKernelWorkerResponse(await handle.kernelWorkerRequest({
        type: "resume-wait4",
        pid: handle.pid,
        vmPtr,
        kernelState: handle.kernelState,
      }), "wait4-resumed", "resume-wait4");

      const blocked = handle.blockedWorkers.get(worker);
      if (!blocked || blocked.vmPtr !== vmPtr) {
        return;
      }
      blocked.rpcPending = false;

      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }

      handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      deps.appendWorkerDebugLog(
        handle,
        `wait4-rpc-adopted:0x${vmPtr.toString(16)}:local=${handle.kernel.thread_status(vmPtr)}`,
      );
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      await deps.requestPipeStateSync(handle, true);
      await settleVisibleProcessOutput(handle, deps.requestPipeStateSync);
      deps.appendWorkerDebugLog(
        handle,
        `wait4-rpc-post-sync:0x${vmPtr.toString(16)}:local=${handle.kernel.thread_status(vmPtr)}`,
      );

      deps.handleBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr));
    } catch (error) {
      const blocked = handle.blockedWorkers.get(worker);
      if (blocked && blocked.vmPtr === vmPtr) {
        blocked.rpcPending = false;
      }
      if (handle.workers.has(worker) && deps.threadWorkerStates.has(worker)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    }
  }

  return {
    resumeWait4ViaKernelWorker,
  };
}
