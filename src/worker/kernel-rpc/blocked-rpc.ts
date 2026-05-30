import type { ProcessHandle } from "../types.js";
import {
  completedKernelRpcOutcome,
  type KernelRpcResumeOutcome,
} from "./outcomes.js";

export interface KernelRpcBlockedResumeHandlers {
  resumeBlockedSyscallLocally(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    blockedStatus: number,
    timeoutExpired?: boolean,
  ): Promise<void>;
}

export function createWorkerKernelRpcBlockedRpcHelpers(
  handlers: KernelRpcBlockedResumeHandlers,
) {
  async function resumeBlockedSyscallOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    blockedStatus: number,
    timeoutExpired = false,
  ): Promise<KernelRpcResumeOutcome> {
    await handlers.resumeBlockedSyscallLocally(handle, worker, vmPtr, blockedStatus, timeoutExpired);
    return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr));
  }

  async function resumeBlockedSyscallLocally(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    blockedStatus: number,
    timeoutExpired = false,
  ): Promise<void> {
    await resumeBlockedSyscallOutcome(handle, worker, vmPtr, blockedStatus, timeoutExpired);
  }

  return {
    resumeBlockedSyscallOutcome,
    resumeBlockedSyscallLocally,
  };
}
