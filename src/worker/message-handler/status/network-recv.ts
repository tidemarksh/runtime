import {
  closedSocketRecvBufferIndices,
  deriveNetBufUsedFromSocketSlots,
  netBufUsedMaskFromBufferIndices,
  preserveClosedNetworkRecvBuffers,
  preserveNetworkRecvBuffers as preserveNetworkRecvBuffersCommon,
  reconcileNetworkRecvBufferConsumers,
  preserveUsedNetBufferHeads,
  preservedSocketRecvBufferIndices,
  readNetworkRecvBufferDebug,
  restoreMergedNetBufferHeads,
  restoreNetworkRecvBufferProducers,
} from "../../network-recv-buffer.js";
import {
  kernelSocketIndexForFd,
  kernelSocketPeerIndex,
  kernelSocketRecvBufferIndex,
  pruneStaleNetworkConnectionsFromFdEntries,
  runtimeNetworkFds,
} from "../../ownership/socket-control.js";
import type { ProcessHandle } from "../../types.js";
import type { ThreadWorkerStatusMessage, WorkerMessageHandlerDeps } from "../types.js";
import { appendStatusSocketDebug } from "./debug.js";

const statusNetworkRecvDebug = {
  source: "status",
  append: appendStatusSocketDebug,
} as const;

export {
  closedSocketRecvBufferIndices,
  deriveNetBufUsedFromSocketSlots,
  netBufUsedMaskFromBufferIndices,
  preserveClosedNetworkRecvBuffers,
  preserveUsedNetBufferHeads,
  preservedSocketRecvBufferIndices,
  readNetworkRecvBufferDebug,
  restoreMergedNetBufferHeads,
  restoreNetworkRecvBufferProducers,
  reconcileNetworkRecvBufferConsumers,
};

export function pruneStatusNetworkConnectionsFromFdEntries(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  deps: Pick<WorkerMessageHandlerDeps, "appendWorkerDebugLog" | "failProcess">,
): void {
  pruneStaleNetworkConnectionsFromFdEntries(handle, msg.vmPtr, msg.fdEntries, {
    appendStatusSocketDebug,
    appendWorkerDebugLog: deps.appendWorkerDebugLog,
    failProcess: deps.failProcess,
  });
}

export function preserveNetworkRecvBuffers(
  handle: ProcessHandle,
  vmPtr: number,
  netBufPoolBytes: number,
): ReturnType<typeof preserveNetworkRecvBuffersCommon> {
  return preserveNetworkRecvBuffersCommon(handle, vmPtr, netBufPoolBytes, statusNetworkRecvDebug);
}

export function preserveNetworkRecvForKernelMemoryWrite(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  ptr: number,
  length: number,
): (() => void) | null {
  if (runtimeNetworkFds(handle).size === 0) {
    return null;
  }
  const netBufBase = handle.kernelState.netBufBase;
  const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes();
  const netBufEnd = netBufBase + netBufPoolBytes;
  if (netBufBase <= 0 || netBufPoolBytes <= 0 || ptr >= netBufEnd || ptr + length <= netBufBase) {
    return null;
  }
  const replacedFds = new Set<number>();
  for (const fd of runtimeNetworkFds(handle)) {
    const incoming = msg.fdEntries?.find((entry) => entry.fd === fd);
    const currentGeneration = handle.kernel.process_fd_generation?.(msg.vmPtr, fd);
    if (
      incoming?.generation !== undefined
      && currentGeneration !== undefined
      && incoming.generation > currentGeneration
    ) {
      replacedFds.add(fd);
    }
  }
  let currentNetBufUsed = handle.kernelState.netBufUsed
    | handle.kernel.kernel_net_buf_used()
    | deriveNetBufUsedFromSocketSlots(handle, handle.kernelState.socketUsed | handle.kernel.kernel_socket_used());
  for (const fd of replacedFds) {
    const socketIdx = kernelSocketIndexForFd(handle, msg.vmPtr, fd);
    for (const candidate of [socketIdx, socketIdx >= 0 ? kernelSocketPeerIndex(handle, socketIdx) : -1]) {
      if (candidate < 0) {
        continue;
      }
      const recvBufIdx = kernelSocketRecvBufferIndex(handle, candidate);
      if (recvBufIdx >= 0) {
        currentNetBufUsed &= ~(1n << BigInt(recvBufIdx));
      }
    }
  }
  const preservedRecvBuffers = preserveNetworkRecvBuffersCommon(
    handle,
    msg.vmPtr,
    netBufPoolBytes,
    statusNetworkRecvDebug,
    replacedFds,
  );
  const preservedNetBufferHeads = preserveUsedNetBufferHeads(
    handle,
    netBufPoolBytes,
    currentNetBufUsed,
  ).concat(preserveClosedNetworkRecvBuffers(handle, netBufPoolBytes, replacedFds));
  if (preservedRecvBuffers.length === 0 && preservedNetBufferHeads.length === 0) {
    return null;
  }
  return () => {
    const memory = new Uint8Array(handle.memory.buffer);
    restoreNetworkRecvBufferProducers(memory, handle.kernelState.netBufBase, preservedRecvBuffers);
    restoreMergedNetBufferHeads(memory, handle.kernelState.netBufBase, preservedNetBufferHeads);
  };
}
