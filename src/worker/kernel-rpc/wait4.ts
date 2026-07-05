import { hostExecutionState } from "../host-execution-state.js";
import { processHasWorker } from "../process-worker-state.js";
import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import {
  adoptKernelStateForHandle,
} from "../kernel-state.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { supervisorWait4RpcLocalStatusAction } from "../../supervisor-blocked-resume.js";
import { requireKernelWorkerOwnership } from "../kernel-worker-ownership.js";
import type { ProcessHandle } from "../types.js";
import type { WorkerKernelRpcLifecycleDeps } from "./lifecycle/types.js";
import { completedKernelRpcOutcome, type KernelRpcResumeOutcome } from "./outcomes.js";
import { expectKernelWorkerResponse } from "./transport.js";
import { finishBlockedRpc } from "../host-blocked-state.js";

export function createWorkerKernelRpcWait4Helpers(
  deps: WorkerKernelRpcLifecycleDeps,
) {
  async function resumeWait4ViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
  ): Promise<KernelRpcResumeOutcome> {
    const kernelWorkerRequest = requireKernelWorkerOwnership(handle, "resumeWait4ViaKernelWorker");

    try {
      const localStatus = handle.kernel.thread_status(vmPtr);
      if (supervisorWait4RpcLocalStatusAction(handle.kernel, localStatus) !== "start") {
        const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
        if (blocked && blocked.vmPtr === vmPtr) {
          finishBlockedRpc(blocked, null);
        }
        deps.appendWorkerDebugLog(
          handle,
          `wait4-rpc-skip:0x${vmPtr.toString(16)}:local=${localStatus}`,
        );
        return completedKernelRpcOutcome(localStatus);
      }
      deps.appendWorkerDebugLog(
        handle,
        `wait4-rpc-start:0x${vmPtr.toString(16)}:local=${localStatus}`,
      );
      const response = expectKernelWorkerResponse(await kernelWorkerRequest({
        type: "resume-wait4",
        pid: handle.pid,
        vmPtr,
        kernelState: handle.kernelState,
      }), "wait4-resumed", "resume-wait4");

      const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
      if (!blocked || blocked.vmPtr !== vmPtr) {
        return { kind: "stale" };
      }
      finishBlockedRpc(blocked, null);

      if (!processHasWorker(handle, worker) || !deps.threadWorkerStates.has(worker)) {
        return { kind: "process-gone" };
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
      await deps.settleVisibleProcessOutput(handle, deps.requestPipeStateSync);
      deps.appendWorkerDebugLog(
        handle,
        `wait4-rpc-post-sync:0x${vmPtr.toString(16)}:local=${handle.kernel.thread_status(vmPtr)}`,
      );

      const status = handle.kernel.thread_status(vmPtr);
      return completedKernelRpcOutcome(status);
    } catch (error) {
      const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
      if (blocked && blocked.vmPtr === vmPtr) {
        finishBlockedRpc(blocked, null);
      }
      if (processHasWorker(handle, worker) && deps.threadWorkerStates.has(worker)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
      return { kind: "process-gone" };
    }
  }

  return {
    resumeWait4ViaKernelWorker,
  };
}
