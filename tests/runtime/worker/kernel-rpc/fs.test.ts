import { expect, test, vi } from "vite-plus/test";
import { createWorkerKernelRpcFsHelpers } from "../../../../src/worker/kernel-rpc/fs.js";
import type { KernelRuntimeState } from "../../../../src/abi.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0,
    memfsInodesBytes: 0,
    memfsFirstChildBytes: 0,
    memfsDirEntriesBytes: 0,
    memfsDataBytes: 0,
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

function makeKernel() {
  const state = makeKernelState();
  return {
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_heap_base: () => state.heapBase,
    kernel_heap_ptr: () => state.heapPtr,
    kernel_ofd_table_ptr: () => state.ofdTablePtr,
    kernel_memfs_ptr: () => state.memfsPtr,
    kernel_mem_state_ptr: () => state.memStatePtr,
    kernel_epoll_table_ptr: () => state.epollTablePtr,
    kernel_socket_table_ptr: () => state.socketTablePtr,
    kernel_socket_used: () => state.socketUsed,
    kernel_pipe_base: () => state.pipeBase,
    kernel_pipe_used: () => state.pipeUsed,
    kernel_net_buf_base: () => state.netBufBase,
    kernel_net_buf_used: () => state.netBufUsed,
    kernel_sig_slots_ptr: () => state.sigSlotsPtr,
    kernel_process_pending_ptr: () => state.processPendingPtr,
    kernel_process_runtime_state_ptr: () => state.processRuntimeStatePtr,
    kernel_time_state_ptr: () => state.timeStatePtr,
    thread_pending_fs_opcode: () => 0,
    thread_pending_fs_effects: () => 0,
    thread_pending_fs_target_ino: () => 1,
    thread_fs_ring_base: () => 0,
    thread_fs_ring_submit_head: () => 0,
    thread_fs_ring_submit_tail: () => 0,
    thread_process_fs: vi.fn(),
    thread_resume_fs: vi.fn(() => 0n),
    thread_status: () => 0,
    abi_eagain: () => 11,
    abi_status_fs: () => 31,
  };
}

test("kernel-rpc fs helper owns local resume and returns a typed outcome", async () => {
  const kernel = makeKernel();
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    processMemfsDirty: false,
    transferDebugStats: undefined,
    kernelShare: { members: new Set() },
    workers: new Set(),
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  handle.workers.add(worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const enqueueWorker = vi.fn();
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    blockWorker: vi.fn(),
    enqueueWorker,
    failProcess: vi.fn(),
    kickBlockedWorkers: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    syncForkSharedStateLocally: vi.fn(),
    syncForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncPipeStateViaKernelWorker: vi.fn(async () => 0n),
  });
  const effects = { memfsMutation: 1, memfsFileData: 2, sharedOfdState: 4 };

  await expect(helpers.resumeFsViaKernelWorker(handle, worker, 0x1234, effects)).resolves.toBeUndefined();
  await expect(helpers.resumeFsViaKernelWorkerOutcome(handle, worker, 0x1234, effects)).resolves.toEqual({
    kind: "completed",
    status: 0,
  });
  expect(kernel.thread_process_fs).toHaveBeenCalledTimes(2);
  expect(kernel.thread_resume_fs).toHaveBeenCalledTimes(2);
  expect(enqueueWorker).toHaveBeenCalledTimes(2);
});
