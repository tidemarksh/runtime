import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import {
  adoptKernelStateForHandle,
} from "../kernel-state.js";
import type { ProcessHandle } from "../types.js";
import type { WorkerKernelRpcLifecycleDeps } from "./lifecycle/types.js";
import { completedKernelRpcOutcome, type KernelRpcResumeEffect, type KernelRpcResumeOutcome } from "./outcomes.js";
import { expectKernelWorkerResponse } from "./transport.js";

export function createWorkerKernelRpcWait4Helpers(
  deps: WorkerKernelRpcLifecycleDeps,
) {
  async function resumeWait4ViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
  ): Promise<KernelRpcResumeOutcome> {
    const effects: KernelRpcResumeEffect[] = [];
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr));
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
        return completedKernelRpcOutcome(localStatus, effects);
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
        return { kind: "stale", effects };
      }
      blocked.rpcPending = false;

      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return { kind: "process-gone", effects };
      }

      handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      deps.appendWorkerDebugLog(
        handle,
        `wait4-rpc-adopted:0x${vmPtr.toString(16)}:local=${handle.kernel.thread_status(vmPtr)}`,
      );
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      effects.push(
        { kind: "publish-runtime-kernel-state" },
        { kind: "request-pipe-state-sync", wakeWaiters: true },
        { kind: "settle-visible-process-output" },
      );
      deps.appendWorkerDebugLog(
        handle,
        `wait4-rpc-post-sync:0x${vmPtr.toString(16)}:local=${handle.kernel.thread_status(vmPtr)}`,
      );

      const status = handle.kernel.thread_status(vmPtr);
      return completedKernelRpcOutcome(status, effects);
    } catch (error) {
      const blocked = handle.blockedWorkers.get(worker);
      if (blocked && blocked.vmPtr === vmPtr) {
        blocked.rpcPending = false;
      }
      if (handle.workers.has(worker) && deps.threadWorkerStates.has(worker)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
      return { kind: "process-gone", effects };
    }
  }

  return {
    resumeWait4ViaKernelWorker,
  };
}
