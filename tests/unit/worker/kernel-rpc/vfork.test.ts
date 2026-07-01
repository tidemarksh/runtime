import { expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../../src/abi.js";
import {
  createRuntimeStateBridge,
  readPublishedKernelRuntimeState,
} from "../../../../src/bridge/runtime-state.js";
import { createWorkerKernelRpcVforkHelpers } from "../../../../src/worker/kernel-rpc/vfork.js";
import type { ProcessHandle, ThreadWorkerState } from "../../../../src/worker/types.js";

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
    socketUsed: 0,
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

function makeKernel(state: KernelRuntimeState, order: string[]): KernelExports {
  let attachedState = state;
  return {
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state(
      heapBase: number,
      heapPtr: number,
      ofdStatePtr: number,
      memfsPtr: number,
      memStatePtr: number,
      epollTablePtr: number,
      socketStatePtr: number,
      socketUsed: number,
      pipeBase: number,
      pipeUsed: bigint,
      netBufBase: number,
      netBufUsed: bigint,
      sigSlotsPtr: number,
      processPendingPtr: number,
      processRuntimeStatePtr: number,
      timeStatePtr: number,
      signalfdStatePtr = 0,
    ) {
      attachedState = {
        ...attachedState,
        heapBase,
        heapPtr,
        ofdStatePtr,
        memfsPtr,
        memStatePtr,
        epollTablePtr,
        socketStatePtr,
        socketUsed,
        pipeBase,
        pipeUsed,
        netBufBase,
        netBufUsed,
        sigSlotsPtr,
        processPendingPtr,
        processRuntimeStatePtr,
        timeStatePtr,
        signalfdStatePtr,
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
    process_record_child_exit_for_parent(parentPid: number, childPid: number, waitStatus: number) {
      order.push(`record:${parentPid}:${childPid}:${waitStatus}`);
    },
    thread_resume_vfork_parent(vmPtr: number, childPid: number) {
      order.push(`resume:${vmPtr}:${childPid}`);
    },
    thread_status: () => 0,
  } as unknown as KernelExports;
}

test("resumeVforkParentViaKernelWorker publishes state before waking and enqueueing parent", async () => {
  const order: string[] = [];
  const baseState = makeKernelState();
  const resumedState = {
    ...baseState,
    heapPtr: baseState.heapPtr + 0x80,
  };
  const runtimeBridge = createRuntimeStateBridge();
  const worker = {} as Worker;
  const kernel = makeKernel(baseState, order);
  const handle = {
    pid: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: baseState,
    runtimeBridge,
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(async () => ({
      type: "vfork-parent-resumed",
      requestId: 0,
      childPid: 7,
      kernelState: resumedState,
    })),
  } as unknown as ProcessHandle;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, { vmPtr: 0x1234 } as ThreadWorkerState);
  const helpers = createWorkerKernelRpcVforkHelpers({
    appendWorkerDebugLog: () => {},
    canonicalizeChildLifecycleViaKernelWorker: async () => ({
      childPid: 7,
      nextPid: 8,
      pgid: 1,
      sid: 1,
    }),
    clearVforkSnapshot() {
      order.push("clear-vfork");
    },
    drainProcessOutput: () => false,
    enqueueWorker(parent, queuedWorker) {
      order.push(`enqueue:${readPublishedKernelRuntimeState(parent.runtimeBridge).heapPtr}`);
      expect(queuedWorker).toBe(worker);
    },
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    kickBlockedWorkers(parent) {
      order.push(`kick:${readPublishedKernelRuntimeState(parent.runtimeBridge).heapPtr}`);
    },
    processHandles: new Map([[1, handle]]),
    requestPipeStateSync: async () => {},
    syncSocketStateViaKernelWorker: async () => ({ socketUsed: 0, netBufUsed: 0n }),
    syncProcessMemfsViaKernelWorker: async () => {},
    snapshotProcessFdOfdStateForKernelWorker: () => ({ fdEntries: [], ofdSlots: [] }),
    applySocketStateFromKernelWorker: () => {},
    threadWorkerStates,
  });

  const outcome = await helpers.resumeVforkParentViaKernelWorker(handle, worker, 0x1234, 0x0300);

  expect(outcome).toEqual({ kind: "completed", status: 0, effects: undefined });
  expect(handle.kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "resume-vfork-parent",
    pid: 1,
    vmPtr: 0x1234,
    waitStatus: 0x0300,
  }));
  expect(order).toEqual([
    "resume:4660:7",
    "record:1:7:768",
    "clear-vfork",
    `kick:${resumedState.heapPtr}`,
    `enqueue:${resumedState.heapPtr}`,
  ]);
});
