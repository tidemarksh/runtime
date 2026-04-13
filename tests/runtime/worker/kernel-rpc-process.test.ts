import { expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { createRuntimeStateBridge } from "../../../src/bridge/runtime-state.js";
import { createWorkerKernelRpcProcessLifecycleHelpers } from "../../../src/worker/kernel-rpc/lifecycle/process.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
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

function makeHandle(
  overrides: Partial<ProcessHandle> = {},
): ProcessHandle & {
  recordedChildExits: Array<[number, number]>;
  publishedStates: KernelRuntimeState[];
} {
  const recordedChildExits: Array<[number, number]> = [];
  const baseState = makeKernelState();
  let attachedState = baseState;
  const handle = {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0x1234,
    ramSize: 0,
    guestBase: 0,
    worker: {} as Worker,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel: {
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
      process_record_child_exit(childPid: number, waitStatus: number) {
        recordedChildExits.push([childPid, waitStatus]);
      },
    } as unknown as KernelExports,
    kernelState: baseState,
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    stdioOwner: null,
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set<number>(),
    },
    stdinQueue: [],
    outputBacklog: [],
    debugCommand: "test",
    runtimeBridge: createRuntimeStateBridge(),
    sabPageCache: null,
    workers: new Set(),
    readyWorkers: [],
    runningWorker: null,
    blockedWorkers: new Map(),
    pipeSyncPromise: null,
    pipeSyncQueued: false,
    pipeWaiterWakeQueued: false,
    epollSyncPromise: null,
    epollSyncQueued: false,
    epollWaiterWakeQueued: false,
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    socketSyncPromise: null,
    socketSyncQueued: false,
    socketWaiterWakeQueued: false,
    vforkSnapshot: null,
    vforkWorker: null,
    workerDebugLog: [],
    recordedChildExits,
    publishedStates: [],
    ...overrides,
  } as unknown as ProcessHandle & {
    recordedChildExits: Array<[number, number]>;
    publishedStates: KernelRuntimeState[];
  };
  handle.kernelShare.members.add(handle);
  return handle;
}

test("recordChildExitViaKernelWorker syncs parent pipe state and drains output before parent wake", async () => {
  const order: string[] = [];
  const baseState = makeKernelState();
  const syncedState = {
    ...baseState,
    heapPtr: baseState.heapPtr + 0x80,
  };
  const publishedStates: KernelRuntimeState[] = [];
  const handle = makeHandle({
    kernelWorkerRequest: vi.fn(async () => ({
      type: "child-exit-recorded",
      requestId: 0,
      kernelState: syncedState,
    })),
  });

  const originalKernelState = handle.kernelState;
  const helpers = createWorkerKernelRpcProcessLifecycleHelpers({
    processHandles: new Map([[handle.pid, handle]]),
    threadWorkerStates: new WeakMap(),
    drainProcessOutput(parent) {
      order.push(`drain:${parent.pid}`);
      return true;
    },
    enqueueWorker() {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleBlockedRetryResult() {},
    kickBlockedWorkers(parent) {
      order.push(`kick:${parent.pid}`);
    },
    requestPipeStateSync: async (parent, wakeWaiters = false) => {
      order.push(`pipe-sync:${parent.pid}:${wakeWaiters ? "wake" : "sync"}`);
      parent.kernelState = syncedState;
    },
    clearVforkSnapshot() {},
  });

  await helpers.recordChildExitViaKernelWorker(handle, 7, 0x0300);

  expect(order).toEqual([
    "pipe-sync:1:sync",
    "drain:1",
    "pipe-sync:1:sync",
    "pipe-sync:1:sync",
    "drain:1",
  ]);
  expect(handle.recordedChildExits).toEqual([[7, 0x0300]]);
  expect(handle.kernelState.heapPtr).toBe(syncedState.heapPtr);
  expect(originalKernelState.heapPtr).toBe(baseState.heapPtr);
});
