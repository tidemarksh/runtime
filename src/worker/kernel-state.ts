import { attachKernelRuntimeState, readKernelRuntimeState, type KernelRuntimeState } from "../abi.js";
import {
  networkClosedFds,
  networkClosedSocketSlots,
  networkConnections,
  networkPendingConnectFds,
} from "./network-socket-state.js";
import { liveProcessPipeUsedBits } from "./ownership/pipe-control.js";
import type { ProcessHandle } from "./types.js";

export function repairHandleVmPidAfterKernelStateAttach(
  handle: ProcessHandle,
  vmPtr: number,
  pidBeforeSync: number,
): void {
  if (vmPtr === 0 || pidBeforeSync !== handle.pid) {
    return;
  }
  const pidAfterSync = handle.kernel.process_current_pid(vmPtr);
  if (pidAfterSync <= 0 || pidAfterSync === handle.pid) {
    return;
  }
  handle.kernel.process_rewrite_child_pid(vmPtr, pidAfterSync, handle.pid, handle.pid + 1);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

export function adoptKernelState(
  current: KernelRuntimeState,
  incoming: KernelRuntimeState,
): KernelRuntimeState {
  return {
    heapBase: incoming.heapBase,
    heapPtr: Math.max(current.heapPtr, incoming.heapPtr),
    ofdTablePtr: current.ofdTablePtr,
    memfsPtr: current.memfsPtr,
    memfsHeaderBytes: current.memfsHeaderBytes > 0 ? current.memfsHeaderBytes : incoming.memfsHeaderBytes,
    memfsInodesBytes: current.memfsInodesBytes > 0 ? current.memfsInodesBytes : incoming.memfsInodesBytes,
    memfsFirstChildBytes: current.memfsFirstChildBytes > 0
      ? current.memfsFirstChildBytes
      : incoming.memfsFirstChildBytes,
    memfsDirEntriesBytes: current.memfsDirEntriesBytes > 0
      ? current.memfsDirEntriesBytes
      : incoming.memfsDirEntriesBytes,
    memfsDataBytes: current.memfsDataBytes > 0 ? current.memfsDataBytes : incoming.memfsDataBytes,
    memStatePtr: current.memStatePtr,
    epollTablePtr: current.epollTablePtr > 0 ? current.epollTablePtr : incoming.epollTablePtr,
    epollTableBytes: current.epollTableBytes ?? incoming.epollTableBytes,
    socketTablePtr: current.socketTablePtr,
    socketUsed: incoming.socketUsed,
    pipeBase: current.pipeBase,
    pipeRegionBytes: current.pipeRegionBytes ?? incoming.pipeRegionBytes,
    pipeUsed: incoming.pipeUsed,
    netBufBase: current.netBufBase,
    netBufPoolBytes: current.netBufPoolBytes ?? incoming.netBufPoolBytes,
    netBufUsed: incoming.netBufUsed,
    sigSlotsPtr: current.sigSlotsPtr,
    sigSlotsBytes: current.sigSlotsBytes ?? incoming.sigSlotsBytes,
    processPendingPtr: current.processPendingPtr,
    processPendingBytes: current.processPendingBytes ?? incoming.processPendingBytes,
    signalfdStatePtr: current.signalfdStatePtr,
    signalfdStateBytes: current.signalfdStateBytes ?? incoming.signalfdStateBytes,
    processRuntimeStatePtr: current.processRuntimeStatePtr,
    processRuntimeStateBytes: current.processRuntimeStateBytes ?? incoming.processRuntimeStateBytes,
    timeStatePtr: current.timeStatePtr,
    timeStateBytes: current.timeStateBytes ?? incoming.timeStateBytes,
  };
}

export function adoptKernelStateForHandle(
  handle: ProcessHandle,
  incoming: KernelRuntimeState,
): KernelRuntimeState {
  const current = handle.kernelState;
  const adopted = adoptKernelState(current, incoming);
  const pipeUsed = adopted.pipeUsed | liveProcessPipeUsedBits(handle);
  if (
    (networkConnections(handle)?.size ?? 0) === 0
    && networkPendingConnectFds(handle).size === 0
    && networkClosedFds(handle).size === 0
    && networkClosedSocketSlots(handle).size === 0
  ) {
    return {
      ...adopted,
      pipeUsed,
    };
  }
  return {
    ...adopted,
    pipeUsed,
    socketUsed: current.socketUsed | incoming.socketUsed,
    netBufUsed: current.netBufUsed | incoming.netBufUsed,
  };
}
