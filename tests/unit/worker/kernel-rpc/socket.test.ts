import { expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../../src/abi.js";
import {
  createRuntimeStateBridge,
  readPublishedKernelRuntimeState,
} from "../../../../src/bridge/runtime-state.js";
import { createWorkerKernelRpcSocketHelpers } from "../../../../src/worker/kernel-rpc/socket.js";
import { networkPendingConnectFds } from "../../../../src/worker/network-socket-state.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 32,
    memfsInodesBytes: 16_384 * 32,
    memfsFirstChildBytes: 16_384 * 4,
    memfsDirEntriesBytes: 16_384 * 264,
    memfsDataBytes: 256 * 1024,
    memStatePtr: 0x240000,
    epollTablePtr: 0x250000,
    socketTablePtr: 0x260000,
    socketUsed: 2,
    pipeBase: 0x270000,
    pipeUsed: 0n,
    netBufBase: 0x280000,
    netBufUsed: 0n,
    sigSlotsPtr: 0xc00000,
    processPendingPtr: 0xc10000,
    processRuntimeStatePtr: 0xc20000,
    timeStatePtr: 0xc30000,
  };
}

function makeKernel(state: KernelRuntimeState): KernelExports {
  let attachedState = state;
  return {
    ...makeSupervisorAbiStubs(),
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state(
      heapBase: number,
      heapPtr: number,
      ofdTablePtr: number,
      memfsPtr: number,
      memStatePtr: number,
      epollTablePtr: number,
      socketTablePtr: number,
      socketUsed: number,
      pipeBase: number,
      pipeUsed: bigint,
      netBufBase: number,
      netBufUsed: bigint,
      sigSlotsPtr: number,
      processPendingPtr: number,
      processRuntimeStatePtr: number,
      timeStatePtr: number,
    ) {
      attachedState = {
        ...attachedState,
        heapBase,
        heapPtr,
        ofdTablePtr,
        memfsPtr,
        memStatePtr,
        epollTablePtr,
        socketTablePtr,
        socketUsed,
        pipeBase,
        pipeUsed,
        netBufBase,
        netBufUsed,
        sigSlotsPtr,
        processPendingPtr,
        processRuntimeStatePtr,
        timeStatePtr,
      };
    },
    kernel_heap_base: () => attachedState.heapBase,
    kernel_heap_ptr: () => attachedState.heapPtr,
    kernel_ofd_table_ptr: () => attachedState.ofdTablePtr,
    kernel_memfs_ptr: () => attachedState.memfsPtr,
    kernel_memfs_header_bytes: () => attachedState.memfsHeaderBytes,
    kernel_memfs_inodes_bytes: () => attachedState.memfsInodesBytes,
    kernel_memfs_first_child_bytes: () => attachedState.memfsFirstChildBytes,
    kernel_memfs_dir_entries_bytes: () => attachedState.memfsDirEntriesBytes,
    kernel_memfs_data_bytes: () => attachedState.memfsDataBytes,
    kernel_mem_state_ptr: () => attachedState.memStatePtr,
    kernel_epoll_table_ptr: () => attachedState.epollTablePtr,
    kernel_socket_table_ptr: () => attachedState.socketTablePtr,
    kernel_socket_used: () => attachedState.socketUsed,
    kernel_pipe_base: () => attachedState.pipeBase,
    kernel_pipe_used: () => attachedState.pipeUsed,
    kernel_net_buf_base: () => attachedState.netBufBase,
    kernel_net_buf_used: () => attachedState.netBufUsed,
    kernel_sig_slots_ptr: () => attachedState.sigSlotsPtr,
    kernel_process_pending_ptr: () => attachedState.processPendingPtr,
    kernel_process_runtime_state_ptr: () => attachedState.processRuntimeStatePtr,
    kernel_time_state_ptr: () => attachedState.timeStatePtr,
    thread_blocked_fd: () => 5,
    thread_a0: () => 3n,
    thread_status: () => 0,
    thread_socket_idx_for_fd: (_vmPtr: number, fd: number) => fd + 10,
    thread_socket_peer_idx: (socketIdx: number) => socketIdx + 1,
  } as unknown as KernelExports;
}

test("resumeConnectViaKernelWorker adopts kernel-worker connect resume state", async () => {
  const worker = {} as Worker;
  const baseState = makeKernelState();
  const resumedState = {
    ...baseState,
    socketUsed: 7,
    netBufUsed: 11n,
    heapPtr: baseState.heapPtr + 0x80,
  };
  const kernel = makeKernel(baseState);
  const runtimeBridge = createRuntimeStateBridge();
  const fdEntries = [{ fd: 5, ofdId: 17, fdFlags: 0 }];
  const ofdSlots = [{ ofdId: 17, slot: new Uint8Array([1, 2, 3]) }];
  const socketState = {
    socketTable: new Uint8Array([4, 5, 6]),
    socketUsed: 7,
    netBufUsed: 11n,
  };
  const kernelWorkerRequest = vi.fn(async (message) => {
    expect(message).toMatchObject({
      type: "resume-connect",
      pid: 7,
      vmPtr: 0x1234,
      result: -111n,
      connectFd: 5,
      fdEntries,
      ofdSlots,
      kernelState: baseState,
    });
    return {
      type: "connect-resumed" as const,
      requestId: 0,
      kernelState: resumedState,
      socketState,
    };
  });
  const handle = {
    runtimeId: 1,
    pid: 7,
    vmPtr: 0x1234,
    kernel,
    kernelState: baseState,
    runtimeBridge,
    workers: new Set([worker]),
    workerDebugLog: [],
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  networkPendingConnectFds(handle).add(6);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const applySocketStateFromKernelWorker = vi.fn();
  const helpers = createWorkerKernelRpcSocketHelpers({
    appendWorkerDebugLog(handle, event) {
      handle.workerDebugLog.push(event);
    },
    processHandles: new Map([[7, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    drainProcessOutput: () => false,
    enqueueWorker: () => {},
    failProcess: () => {},
    notifyHostEffectProgress: () => {},
    requestPipeStateSync: async () => {},
    settleVisibleProcessOutput: async () => undefined,
    syncSocketStateViaKernelWorker: async () => ({ socketUsed: 0, netBufUsed: 0n }),
    syncProcessMemfsViaKernelWorker: async () => {},
    snapshotProcessFdOfdStateForKernelWorker: () => ({ fdEntries, ofdSlots }),
    applySocketStateFromKernelWorker,
    clearVforkSnapshot: () => {},
  });

  const outcome = await helpers.resumeConnectViaKernelWorker(handle, worker, 0x1234, -111n);

  expect(outcome).toEqual({ kind: "completed", status: 0 });
  expect(kernelWorkerRequest).toHaveBeenCalledTimes(1);
  expect(applySocketStateFromKernelWorker).toHaveBeenCalledWith(handle, socketState);
  expect(handle.kernelState).toMatchObject({
    socketUsed: resumedState.socketUsed,
    netBufUsed: resumedState.netBufUsed,
    heapPtr: resumedState.heapPtr,
  });
  expect(readPublishedKernelRuntimeState(runtimeBridge)).toMatchObject({
    socketUsed: resumedState.socketUsed,
    netBufUsed: resumedState.netBufUsed,
  });
  expect(handle.workerDebugLog).toEqual([
    expect.stringContaining("connect-rpc-start:7"),
    expect.stringContaining("connect-rpc-adopted:7"),
  ]);
});
