import type { ProcessHandle } from "../types.js";
import {
  completedKernelRpcOutcome,
  type KernelRpcResumeOutcome,
} from "./outcomes.js";

export interface KernelRpcFsResumeHandlers {
  resumeFsViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    fsEffects: { memfsMutation: number; memfsFileData: number; sharedOfdState: number },
  ): Promise<void>;
}

export function createWorkerKernelRpcFsHelpers(
  handlers: KernelRpcFsResumeHandlers,
) {
  async function resumeFsViaKernelWorkerOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    fsEffects: { memfsMutation: number; memfsFileData: number; sharedOfdState: number },
  ): Promise<KernelRpcResumeOutcome> {
    await handlers.resumeFsViaKernelWorker(handle, worker, vmPtr, fsEffects);
    return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr));
  }

  async function resumeFsViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    fsEffects: { memfsMutation: number; memfsFileData: number; sharedOfdState: number },
  ): Promise<void> {
    await resumeFsViaKernelWorkerOutcome(handle, worker, vmPtr, fsEffects);
  }

  return {
    resumeFsViaKernelWorker,
    resumeFsViaKernelWorkerOutcome,
  };
}
