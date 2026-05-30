import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  type KernelExports,
} from "../../../abi.js";
import { publishKernelRuntimeState } from "../../../bridge/runtime-state.js";
import {
  adoptKernelStateForHandle,
  applySocketStateFromKernelWorker,
  snapshotProcessFdOfdStateForKernelWorker,
} from "../../sync.js";
import { describeSyncCause, type SyncCause } from "../../sync/effects.js";
import { settleVisibleProcessOutput } from "../../stdio.js";
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

export function createWorkerKernelRpcResumeLifecycleHelpers(
  deps: WorkerKernelRpcResumeLifecycleDeps,
) {
  async function resumeConnectViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    result: bigint,
  ): Promise<void> {
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      const blockedFd = handle.kernel.thread_blocked_fd(vmPtr);
      const pendingConnectFd = handle.networkPendingConnectFds.size === 1
        ? [...handle.networkPendingConnectFds][0]
        : 0;
      const connectFd = blockedFd !== 0 ? blockedFd : pendingConnectFd;
      const { fdEntries, ofdSlots } = snapshotProcessFdOfdStateForKernelWorker(handle, vmPtr);
      deps.appendWorkerDebugLog(handle, `connect-rpc-start:${handle.pid}:vm=0x${vmPtr.toString(16)}:result=${result.toString()}:fd=${connectFd}:blocked_fd=${blockedFd}:a0=${handle.kernel.thread_a0(vmPtr).toString()}`);
      const response = await handle.kernelWorkerRequest({
        type: "resume-connect",
        pid: handle.pid,
        vmPtr,
        result,
        connectFd: connectFd > 0 ? connectFd : undefined,
        fdEntries,
        ofdSlots,
        kernelState: handle.kernelState,
      });
      if (response.type !== "connect-resumed") {
        throw new Error(`unexpected kernel-worker response for resume-connect: ${response.type}`);
      }
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }
      handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      const fd = connectFd > 0 ? connectFd : Number(BigInt.asUintN(32, handle.kernel.thread_a0(vmPtr)));
      const restorePendingFd = handle.networkPendingConnectFds.has(fd);
      if (response.socketState) {
        handle.networkPendingConnectFds.delete(fd);
        try {
          applySocketStateFromKernelWorker(handle, response.socketState);
        } finally {
          if (restorePendingFd) {
            handle.networkPendingConnectFds.add(fd);
          }
        }
      }
      const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd);
      const peerIdx = socketIdx !== undefined && socketIdx >= 0
        ? handle.kernel.thread_socket_peer_idx?.(socketIdx)
        : undefined;
      deps.appendWorkerDebugLog(handle, `connect-rpc-adopted:${handle.pid}:vm=0x${vmPtr.toString(16)}:fd=${fd}:status=${handle.kernel.thread_status(vmPtr)}:a0=${handle.kernel.thread_a0(vmPtr).toString()}:sock=${socketIdx ?? "n/a"}:peer=${peerIdx ?? "n/a"}`);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      return;
    }

    const blockedFd = handle.kernel.thread_blocked_fd(vmPtr);
    const pendingConnectFd = handle.networkPendingConnectFds.size === 1
      ? [...handle.networkPendingConnectFds][0]
      : 0;
    const connectFd = blockedFd !== 0 ? blockedFd : pendingConnectFd;
    deps.appendWorkerDebugLog(handle, `connect-resume:${handle.pid}:vm=0x${vmPtr.toString(16)}:result=${result.toString()}:fd=${connectFd}:blocked_fd=${blockedFd}:a0=${handle.kernel.thread_a0(vmPtr).toString()}`);
    if (connectFd > 0 && handle.kernel.thread_resume_connect_result_for_fd) {
      handle.kernel.thread_resume_connect_result_for_fd(vmPtr, connectFd, result);
    } else {
      handle.kernel.thread_resume_connect_result(vmPtr, result);
    }
    const fd = connectFd > 0 ? connectFd : Number(BigInt.asUintN(32, handle.kernel.thread_a0(vmPtr)));
    const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd);
    const peerIdx = socketIdx !== undefined && socketIdx >= 0
      ? handle.kernel.thread_socket_peer_idx?.(socketIdx)
      : undefined;
    deps.appendWorkerDebugLog(handle, `connect-resumed:${handle.pid}:vm=0x${vmPtr.toString(16)}:fd=${fd}:status=${handle.kernel.thread_status(vmPtr)}:a0=${handle.kernel.thread_a0(vmPtr).toString()}:sock=${socketIdx ?? "n/a"}:peer=${peerIdx ?? "n/a"}`);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
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

    deps.appendWorkerDebugLog(handle, `resume-vfork-execve-parent:rpc-start:0x${vmPtr.toString(16)}:${childPid}`);
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
    handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    deps.appendWorkerDebugLog(handle, `resume-vfork-execve-parent:local-start:0x${vmPtr.toString(16)}:${childPid}`);
    restoreVforkParentLocally(handle.kernel, vmPtr, childPid);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    deps.clearVforkSnapshot(handle);
    deps.appendWorkerDebugLog(handle, `resume-vfork-execve-parent:local-done:0x${vmPtr.toString(16)}:${childPid}`);
  }

  async function releaseHostStdioViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<bigint> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return 0n;
    }
    deps.appendWorkerDebugLog(handle, `release-host-stdio:rpc-start:${describeSyncCause(cause)}`);
    const response = await handle.kernelWorkerRequest({
      type: "release-host-stdio",
      pid: handle.pid,
      vmPtr: handle.vmPtr,
      kernelState: handle.kernelState,
    });
    if (response.type !== "host-stdio-released") {
      throw new Error(`unexpected kernel-worker response for release-host-stdio: ${response.type}`);
    }
    handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    deps.appendWorkerDebugLog(handle, `release-host-stdio:rpc-done:${describeSyncCause(cause)}`);
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
