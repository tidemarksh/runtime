import {
  closedSocketRecvBufferIndices,
  deriveNetBufUsedFromSocketSlots,
  netBufUsedMaskFromBufferIndices,
  preserveClosedNetworkRecvBuffers,
  preserveNetworkRecvBuffers as preserveNetworkRecvBuffersCommon,
  preserveUsedNetBufferHeads,
  preservedSocketRecvBufferIndices,
  readNetworkRecvBufferDebug,
  restoreMergedNetBufferHeads,
  restoreNetworkRecvBufferProducers,
} from "../../network-recv-buffer.js";
import {
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
  const currentNetBufUsed = handle.kernelState.netBufUsed
    | handle.kernel.kernel_net_buf_used()
    | deriveNetBufUsedFromSocketSlots(handle, handle.kernelState.socketUsed | handle.kernel.kernel_socket_used());
  const preservedRecvBuffers = preserveNetworkRecvBuffers(handle, msg.vmPtr, netBufPoolBytes);
  const preservedNetBufferHeads = preserveUsedNetBufferHeads(
    handle,
    netBufPoolBytes,
    currentNetBufUsed,
  ).concat(preserveClosedNetworkRecvBuffers(handle, netBufPoolBytes));
  if (preservedRecvBuffers.length === 0 && preservedNetBufferHeads.length === 0) {
    return null;
  }
  return () => {
    const memory = new Uint8Array(handle.memory.buffer);
    restoreNetworkRecvBufferProducers(memory, handle.kernelState.netBufBase, preservedRecvBuffers);
    restoreMergedNetBufferHeads(memory, handle.kernelState.netBufBase, preservedNetBufferHeads);
  };
}
