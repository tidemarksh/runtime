import { expect, test, vi } from "vite-plus/test";
import { createWorkerKernelRpcFsHelpers } from "../../../../src/worker/kernel-rpc/fs.js";
import type { KernelRuntimeState } from "../../../../src/abi.js";
import { createTransferDebugStats } from "../../../../src/worker/types.js";
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
    kernel_attach_runtime_state: vi.fn(),
    thread_pending_fs_opcode: () => 0,
    thread_pending_fs_effects: () => 0,
    thread_pending_fs_target_ino: () => 1,
    thread_fs_ring_base: () => 0,
    thread_fs_ring_submit_head: () => 0,
    thread_fs_ring_submit_tail: () => 0,
    thread_process_fs: vi.fn(),
    thread_resume_fs: vi.fn(() => 0n),
    thread_status: () => 0,
    abi_max_fds: () => 8,
    abi_max_threads: () => 4,
    abi_eagain: () => 11,
    abi_status_fs: () => 31,
    process_fd_ofd_id: () => 0,
    process_fd_flags: () => 0,
    process_thread_vm_ptr: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_table_bytes: () => 0,
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
    kernelShareOtherMembers: () => [],
    syncForkSharedStateLocallyAfterFs: vi.fn(),
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    publishMemfsAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });
  const effects = { memfsMutation: 1, memfsFileData: 2, sharedOfdState: 4 };

  await expect(helpers.resumeFsViaKernelWorker(handle, worker, 0x1234, effects)).resolves.toMatchObject({
    kind: "completed",
    status: 0,
  });
  await expect(helpers.resumeFsViaKernelWorkerOutcome(handle, worker, 0x1234, effects)).resolves.toMatchObject({
    kind: "completed",
    status: 0,
  });
  expect(kernel.thread_process_fs).toHaveBeenCalledTimes(2);
  expect(kernel.thread_resume_fs).toHaveBeenCalledTimes(2);
  expect(enqueueWorker).not.toHaveBeenCalled();
});

test("kernel-rpc fs outcome reports blocked fs status", async () => {
  const kernel = {
    ...makeKernel(),
    thread_resume_fs: vi.fn(() => 11n),
    thread_status: () => 31,
  };
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
  const blockWorker = vi.fn();
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    blockWorker,
    enqueueWorker: vi.fn(),
    failProcess: vi.fn(),
    kickBlockedWorkers: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [],
    syncForkSharedStateLocallyAfterFs: vi.fn(),
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    publishMemfsAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  await expect(helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234,
    { memfsMutation: 1, memfsFileData: 2, sharedOfdState: 4 },
  )).resolves.toMatchObject({
    kind: "blocked",
    status: 31,
  });
  expect(blockWorker).not.toHaveBeenCalled();
});

test("kernel-worker fs outcome skips memfs publication effect when no fork-share targets exist", async () => {
  const kernel = {
    ...makeKernel(),
    thread_pending_fs_opcode: () => 6,
    thread_pending_fs_effects: () => 1,
  };
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "fs-processed",
    requestId: 1,
    kernelState: makeKernelState(),
    result: 0n,
    memfsMutated: true,
    ofdStateChanged: false,
  }));
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    processMemfsDirty: false,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    workers: new Set(),
    blockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  handle.workers.add(worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    blockWorker: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    kickBlockedWorkers: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [],
    syncForkSharedStateLocallyAfterFs: vi.fn(),
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    publishMemfsAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234,
    { memfsMutation: 1, memfsFileData: 2, sharedOfdState: 4 },
  );

  expect(kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "process-fs",
    pid: 1,
    vmPtr: 0x1234,
  }));
  expect(outcome).toMatchObject({ kind: "completed", status: 0 });
  expect(outcome.effects).toEqual(expect.arrayContaining([
    { kind: "mark-runtime-fs-dirty", handle },
    { kind: "publish-runtime-kernel-state" },
    { kind: "kick-blocked-workers" },
    { kind: "enqueue-worker" },
  ]));
  expect(outcome.effects).not.toEqual(expect.arrayContaining([
    expect.objectContaining({ kind: "publish-memfs-after-fs" }),
  ]));
});

test("kernel-worker fs outcome applies mmap guest memory writes from fs response", async () => {
  const kernel = makeKernel();
  const mappedBytes = Uint8Array.from([0x7f, 0x45, 0x4c, 0x46]);
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "fs-processed",
    requestId: 1,
    kernelState: makeKernelState(),
    result: 0x200n,
    memfsMutated: false,
    ofdStateChanged: false,
    guestMemoryWrites: [{ guestPtr: 0x200, bytes: mappedBytes }],
  }));
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    guestBase: 0x1000,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    processMemfsDirty: false,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    workers: new Set(),
    blockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  handle.workers.add(worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    blockWorker: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    kickBlockedWorkers: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [],
    syncForkSharedStateLocallyAfterFs: vi.fn(),
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    publishMemfsAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234,
    { memfsMutation: 1, memfsFileData: 2, sharedOfdState: 4 },
  );

  expect(outcome).toMatchObject({ kind: "completed", status: 0 });
  expect(new Uint8Array(handle.memory.buffer, 0x1200, mappedBytes.length)).toEqual(mappedBytes);
});

test("kernel-worker fs outcome preserves published data when publishing file writes", async () => {
  const kernel = {
    ...makeKernel(),
    thread_pending_fs_opcode: () => 2,
    thread_pending_fs_effects: () => 2,
  };
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "fs-processed",
    requestId: 1,
    kernelState: makeKernelState(),
    result: 4096n,
    memfsMutated: true,
    ofdStateChanged: false,
  }));
  const targetKernel = makeKernel();
  const target = {
    pid: 2,
    vmPtr: 0x5678,
    kernel: targetKernel,
    memory: targetKernel.memory,
    blockedWorkers: new Map(),
    runningWorker: null,
  } as unknown as ProcessHandle;
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    processMemfsDirty: false,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set([target]), sharedOfdIds: new Set() },
    workers: new Set(),
    blockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  handle.workers.add(worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    blockWorker: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    kickBlockedWorkers: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [target],
    syncForkSharedStateLocallyAfterFs: vi.fn(),
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    publishMemfsAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234,
    { memfsMutation: 1, memfsFileData: 2, sharedOfdState: 4 },
  );

  expect(outcome.effects).toEqual(expect.arrayContaining([
    {
      kind: "publish-memfs-after-fs",
      targets: [target],
      preservePublishedRegularFileData: true,
      sourceVmPtr: 0x1234,
    },
  ]));
});
