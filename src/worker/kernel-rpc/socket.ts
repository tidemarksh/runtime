import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import {
  adoptKernelStateForHandle,
  applySocketStateFromKernelWorker,
  snapshotProcessFdOfdStateForKernelWorker,
} from "../sync.js";
import type { ProcessHandle } from "../types.js";
import type { WorkerKernelRpcLifecycleDeps } from "./lifecycle/types.js";
import { completedKernelRpcOutcome, type KernelRpcResumeEffect, type KernelRpcResumeOutcome } from "./outcomes.js";
import { expectKernelWorkerResponse } from "./transport.js";

export function createWorkerKernelRpcSocketHelpers(
  deps: WorkerKernelRpcLifecycleDeps,
) {
  async function resumeConnectViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    result: bigint,
  ): Promise<KernelRpcResumeOutcome> {
    const effects: KernelRpcResumeEffect[] = [];
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      const blockedFd = handle.kernel.thread_blocked_fd(vmPtr);
      const pendingConnectFd = handle.networkPendingConnectFds.size === 1
        ? [...handle.networkPendingConnectFds][0]
        : 0;
      const connectFd = blockedFd !== 0 ? blockedFd : pendingConnectFd;
      const { fdEntries, ofdSlots } = snapshotProcessFdOfdStateForKernelWorker(handle, vmPtr);
      deps.appendWorkerDebugLog(handle, `connect-rpc-start:${handle.pid}:vm=0x${vmPtr.toString(16)}:result=${result.toString()}:fd=${connectFd}:blocked_fd=${blockedFd}:a0=${handle.kernel.thread_a0(vmPtr).toString()}`);
      const response = expectKernelWorkerResponse(await handle.kernelWorkerRequest({
        type: "resume-connect",
        pid: handle.pid,
        vmPtr,
        result,
        connectFd: connectFd > 0 ? connectFd : undefined,
        fdEntries,
        ofdSlots,
        kernelState: handle.kernelState,
      }), "connect-resumed", "resume-connect");
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return { kind: "process-gone", effects };
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
      effects.push({ kind: "publish-runtime-kernel-state" });
      return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr), effects);
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
    effects.push({ kind: "publish-runtime-kernel-state" });
    return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr), effects);
  }

  return {
    resumeConnectViaKernelWorker,
  };
}
