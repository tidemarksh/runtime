import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  type KernelExports,
} from "../../abi.js";
import {
  adoptKernelStateForHandle,
} from "../kernel-state.js";
import type {
  CanonicalizedChildLifecycle,
  ProcessHandle,
} from "../types.js";
import type { WorkerKernelRpcLifecycleDeps } from "./lifecycle/types.js";
import { completedKernelRpcOutcome, type KernelRpcResumeEffect, type KernelRpcResumeOutcome } from "./outcomes.js";
import { expectKernelWorkerResponse } from "./transport.js";

export interface WorkerKernelRpcVforkDeps extends WorkerKernelRpcLifecycleDeps {
  canonicalizeChildLifecycleViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
    provisionalChildPid: number,
  ): Promise<CanonicalizedChildLifecycle>;
}

function restoreVforkParentLocally(
  kernel: KernelExports,
  vmPtr: number,
  childPid: number,
): void {
  kernel.thread_clear_spawn_handoff(vmPtr);
  if (kernel.thread_restore_vfork_parent_without_cleanup) {
    kernel.thread_restore_vfork_parent_without_cleanup(vmPtr, childPid);
    kernel.thread_clear_vfork_runtime_state?.();
    return;
  }
  kernel.thread_resume_vfork_parent(vmPtr, childPid);
  kernel.thread_clear_vfork_runtime_state?.();
}

export function createWorkerKernelRpcVforkHelpers(
  deps: WorkerKernelRpcVforkDeps,
) {
  async function resumeVforkParentViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    waitStatus: number,
  ): Promise<KernelRpcResumeOutcome> {
    const effects: KernelRpcResumeEffect[] = [];
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr));
    }

    try {
      const response = expectKernelWorkerResponse(await handle.kernelWorkerRequest({
        type: "resume-vfork-parent",
        pid: handle.pid,
        vmPtr,
        waitStatus,
        kernelState: handle.kernelState,
      }), "vfork-parent-resumed", "resume-vfork-parent");
      if (response.childPid <= 0) {
        throw new Error(`kernel-worker did not report an active vfork child for pid=${handle.pid}`);
      }
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return { kind: "process-gone", effects };
      }

      handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      handle.kernel.thread_resume_vfork_parent(vmPtr, response.childPid);
      if (handle.kernel.process_record_child_exit_for_parent) {
        handle.kernel.process_record_child_exit_for_parent(handle.pid, response.childPid, waitStatus);
      } else {
        handle.kernel.process_record_child_exit(response.childPid, waitStatus);
      }
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      deps.clearVforkSnapshot(handle);
      effects.push(
        { kind: "publish-runtime-kernel-state" },
        { kind: "kick-blocked-workers" },
        { kind: "enqueue-worker" },
      );
      return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr), effects);
    } catch (error) {
      if (handle.workers.has(worker) && deps.threadWorkerStates.has(worker)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
      return { kind: "process-gone", effects };
    }
  }

  async function canonicalizeActiveVforkChildViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    provisionalChildPid: number,
  ): Promise<KernelRpcResumeOutcome> {
    const effects: KernelRpcResumeEffect[] = [];
    const snapshot = handle.vforkSnapshot;
    if (!snapshot || snapshot.vmPtr !== vmPtr) {
      return { kind: "stale", effects };
    }

    try {
      const lifecycle = await deps.canonicalizeChildLifecycleViaKernelWorker(
        handle,
        vmPtr,
        provisionalChildPid,
      );
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return { kind: "process-gone", effects };
      }

      snapshot.childPid = lifecycle.childPid;
      snapshot.nextPid = lifecycle.nextPid;
      snapshot.pgid = lifecycle.pgid;
      snapshot.sid = lifecycle.sid;
      effects.push(
        { kind: "publish-runtime-kernel-state" },
        { kind: "enqueue-worker" },
      );
      return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr), effects);
    } catch (error) {
      if (handle.workers.has(worker) && deps.threadWorkerStates.has(worker)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
      return { kind: "process-gone", effects };
    }
  }

  async function resumeVforkExecveParentViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
    childPid: number,
  ): Promise<KernelRpcResumeOutcome> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr));
    }

    deps.appendWorkerDebugLog(handle, `resume-vfork-execve-parent:rpc-start:0x${vmPtr.toString(16)}:${childPid}`);
    const response = expectKernelWorkerResponse(await handle.kernelWorkerRequest({
      type: "resume-vfork-execve-parent",
      pid: handle.pid,
      vmPtr,
      childPid,
      kernelState: handle.kernelState,
    }), "vfork-execve-parent-resumed", "resume-vfork-execve-parent");
    handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    deps.appendWorkerDebugLog(handle, `resume-vfork-execve-parent:local-start:0x${vmPtr.toString(16)}:${childPid}`);
    restoreVforkParentLocally(handle.kernel, vmPtr, childPid);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    deps.clearVforkSnapshot(handle);
    deps.appendWorkerDebugLog(handle, `resume-vfork-execve-parent:local-done:0x${vmPtr.toString(16)}:${childPid}`);
    return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr));
  }

  return {
    canonicalizeActiveVforkChildViaKernelWorker,
    resumeVforkExecveParentViaKernelWorker,
    resumeVforkParentViaKernelWorker,
  };
}
