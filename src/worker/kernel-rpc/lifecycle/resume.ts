import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../../abi.js";
import { publishKernelRuntimeState } from "../../../bridge/runtime-state.js";
import { adoptKernelState } from "../../sync.js";
import type {
  CanonicalizedChildLifecycle,
  ProcessHandle,
} from "../../types.js";
import type { WorkerKernelRpcLifecycleDeps } from "./types.js";

interface WorkerKernelRpcResumeLifecycleDeps extends WorkerKernelRpcLifecycleDeps {
  canonicalizeChildLifecycleViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
    provisionalChildPid: number,
  ): Promise<CanonicalizedChildLifecycle>;
}

export function createWorkerKernelRpcResumeLifecycleHelpers(
  deps: WorkerKernelRpcResumeLifecycleDeps,
) {
  async function resumeConnectViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    result: bigint,
  ): Promise<void> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      handle.kernel.thread_resume_connect_result(vmPtr, result);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      deps.kickBlockedWorkers(handle);
      deps.enqueueWorker(handle, worker);
      return;
    }

    try {
      const response = await handle.kernelWorkerRequest({
        type: "resume-connect",
        pid: handle.pid,
        vmPtr,
        result,
        kernelState: handle.kernelState,
      });
      if (response.type !== "connect-resumed") {
        throw new Error(`unexpected kernel-worker response for resume-connect: ${response.type}`);
      }
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }
      handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      deps.kickBlockedWorkers(handle);
      deps.enqueueWorker(handle, worker);
    } catch (error) {
      if (handle.workers.has(worker) && deps.threadWorkerStates.has(worker)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    }
  }

  async function resumeWait4ViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
  ): Promise<void> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return;
    }

    try {
      const response = await handle.kernelWorkerRequest({
        type: "resume-wait4",
        pid: handle.pid,
        vmPtr,
        kernelState: handle.kernelState,
      });
      if (response.type !== "wait4-resumed") {
        throw new Error(`unexpected kernel-worker response for resume-wait4: ${response.type}`);
      }

      const blocked = handle.blockedWorkers.get(worker);
      if (!blocked || blocked.vmPtr !== vmPtr) {
        return;
      }
      blocked.rpcPending = false;

      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }

      handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);

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

  async function resumeVforkParentViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    waitStatus: number,
  ): Promise<void> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return;
    }

    try {
      const response = await handle.kernelWorkerRequest({
        type: "resume-vfork-parent",
        pid: handle.pid,
        vmPtr,
        waitStatus,
        kernelState: handle.kernelState,
      });
      if (response.type !== "vfork-parent-resumed") {
        throw new Error(`unexpected kernel-worker response for resume-vfork-parent: ${response.type}`);
      }
      if (response.childPid <= 0) {
        throw new Error(`kernel-worker did not report an active vfork child for pid=${handle.pid}`);
      }
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }

      handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      handle.kernel.process_record_child_exit(response.childPid, waitStatus);
      handle.kernel.thread_resume_vfork_parent(vmPtr, response.childPid);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      deps.clearVforkSnapshot(handle);
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      deps.kickBlockedWorkers(handle);
      deps.enqueueWorker(handle, worker);
    } catch (error) {
      if (handle.workers.has(worker) && deps.threadWorkerStates.has(worker)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    }
  }

  async function canonicalizeActiveVforkChildViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    provisionalChildPid: number,
  ): Promise<void> {
    const snapshot = handle.vforkSnapshot;
    if (!snapshot || snapshot.vmPtr !== vmPtr) {
      return;
    }

    try {
      const lifecycle = await deps.canonicalizeChildLifecycleViaKernelWorker(
        handle,
        vmPtr,
        provisionalChildPid,
      );
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }

      snapshot.childPid = lifecycle.childPid;
      snapshot.nextPid = lifecycle.nextPid;
      snapshot.pgid = lifecycle.pgid;
      snapshot.sid = lifecycle.sid;
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      deps.enqueueWorker(handle, worker);
    } catch (error) {
      if (handle.workers.has(worker) && deps.threadWorkerStates.has(worker)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    }
  }

  async function resumeVforkExecveParentViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
    childPid: number,
  ): Promise<void> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return;
    }

    const response = await handle.kernelWorkerRequest({
      type: "resume-vfork-execve-parent",
      pid: handle.pid,
      vmPtr,
      childPid,
      kernelState: handle.kernelState,
    });
    if (response.type !== "vfork-execve-parent-resumed") {
      throw new Error(`unexpected kernel-worker response for resume-vfork-execve-parent: ${response.type}`);
    }
    handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    handle.kernel.thread_clear_spawn_handoff(vmPtr);
    if (handle.kernel.thread_restore_vfork_parent_without_cleanup) {
      handle.kernel.thread_restore_vfork_parent_without_cleanup(vmPtr, childPid);
    } else {
      handle.kernel.thread_resume_vfork_parent(vmPtr, childPid);
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
  }

  async function releaseHostStdioViaKernelWorker(handle: ProcessHandle): Promise<bigint> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return 0n;
    }
    const response = await handle.kernelWorkerRequest({
      type: "release-host-stdio",
      pid: handle.pid,
      vmPtr: handle.vmPtr,
      kernelState: handle.kernelState,
    });
    if (response.type !== "host-stdio-released") {
      throw new Error(`unexpected kernel-worker response for release-host-stdio: ${response.type}`);
    }
    handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    return handle.kernelState.pipeUsed;
  }

  return {
    canonicalizeActiveVforkChildViaKernelWorker,
    releaseHostStdioViaKernelWorker,
    resumeConnectViaKernelWorker,
    resumeVforkExecveParentViaKernelWorker,
    resumeVforkParentViaKernelWorker,
    resumeWait4ViaKernelWorker,
  };
}
