import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import {
  adoptKernelStateForHandle,
} from "../kernel-state.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { requireKernelWorkerOwnership } from "../kernel-worker-ownership.js";
import { networkPendingConnectFds } from "../network-socket-state.js";
import type { ProcessHandle } from "../types.js";
import type { WorkerKernelRpcLifecycleDeps } from "./lifecycle/types.js";
import { completedKernelRpcOutcome, type KernelRpcResumeOutcome } from "./outcomes.js";
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
    const kernelWorkerRequest = requireKernelWorkerOwnership(handle, "resumeConnectViaKernelWorker");
    const blockedFd = handle.kernel.thread_blocked_fd(vmPtr);
    const pendingConnectFds = networkPendingConnectFds(handle);
    const pendingConnectFd = pendingConnectFds.size === 1
      ? [...pendingConnectFds][0]
      : 0;
    const connectFd = blockedFd !== 0 ? blockedFd : pendingConnectFd;
    const { fdEntries, ofdSlots } = deps.snapshotProcessFdOfdStateForKernelWorker(handle, vmPtr);
    deps.appendWorkerDebugLog(handle, `connect-rpc-start:${handle.pid}:vm=0x${vmPtr.toString(16)}:result=${result.toString()}:fd=${connectFd}:blocked_fd=${blockedFd}:a0=${handle.kernel.thread_a0(vmPtr).toString()}`);
    const response = expectKernelWorkerResponse(await kernelWorkerRequest({
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
      return { kind: "process-gone" };
    }
    handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    const fd = connectFd > 0 ? connectFd : Number(BigInt.asUintN(32, handle.kernel.thread_a0(vmPtr)));
    const restorePendingFd = pendingConnectFds.has(fd);
    if (response.socketState) {
      pendingConnectFds.delete(fd);
      try {
        deps.applySocketStateFromKernelWorker(handle, response.socketState);
      } finally {
        if (restorePendingFd) {
          pendingConnectFds.add(fd);
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
    return completedKernelRpcOutcome(handle.kernel.thread_status?.(vmPtr));
  }

  return {
    resumeConnectViaKernelWorker,
  };
}
