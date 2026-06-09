import { expect, test, vi } from "vite-plus/test";
import { createWorkerKernelRpcBlockedRpcHelpers } from "../../../../src/worker/kernel-rpc/blocked-rpc.js";
import type { KernelRuntimeState } from "../../../../src/abi.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";
import { PIPE_SLOT_BYTES, PIPE_WRITE_OPEN_OFFSET } from "../../../../src/layout/pipe.js";

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
  let status = 0;
  return {
    memory: new WebAssembly.Memory({ initial: 2, maximum: 2, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
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
    abi_status_pipe_wait: () => 31,
    abi_status_ok: () => 0,
    abi_status_connect: () => 32,
    abi_status_socket_recv: () => 33,
    abi_status_socket_send: () => 34,
    abi_sys_read: () => 63n,
    abi_max_fds: () => 64,
    abi_max_pipes: () => 64,
    abi_fd_type_pipe: () => 6,
    thread_blocked_syscall_nr: () => 0n,
    thread_blocked_syscall_arg: () => 0n,
    thread_resume_blocked_syscall: vi.fn(() => {
      status = 0;
      return 0n;
    }),
    thread_status: () => status,
    process_current_pid: () => 1,
    thread_blocked_fd: () => 0,
    thread_a0: () => 0n,
    thread_a2: () => 0n,
    thread_a3: () => 0n,
    thread_a7: () => 0n,
    process_fd_ofd_id: () => 0,
    thread_a1: () => 0n,
    thread_pipe_read_eof_sync_pending: () => 0,
    thread_blocked_pipe_buffered_len: () => 0,
    thread_blocked_pipe_write_open: () => 0,
    thread_resume_pipe_read_open_empty: () => 0,
    thread_poll_wait_has_socket: () => 0,
    thread_blocked_pipe_read_ready: () => 0,
    thread_blocked_pipe_write_ready: () => 0,
    thread_poll_wait_ready_count: () => 0,
    thread_poll_hup_sync_pending: () => 0,
    thread_process_ptr: () => 0,
    kernel_ofd_table_ptr: () => state.ofdTablePtr,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_host_fd: () => 0,
    kernel_ofd_status_flags: () => 0,
    kernel_ofd_ino: () => 0,
    kernel_ofd_offset: () => 0n,
    thread_blocked_ofd_id: () => 0,
  };
}

test("blocked-rpc pipe EOF ignores stale active-child writer when canonical pipe has no writers", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const childWorker = {} as Worker;
  const vmPtr = 0x1234;
  const childVmPtr = 0x5678;
  const pipeSlot = 1;
  const readOfd = 26;
  const writeOfd = 27;
  let status = kernel.abi_status_pipe_wait();
  kernel.thread_status = () => status;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
    return 0n;
  });
  kernel.thread_blocked_syscall_nr = () => kernel.abi_sys_read();
  kernel.thread_blocked_syscall_arg = (_vmPtr: number, index: number) => index === 0 ? 12n : 0n;
  kernel.thread_blocked_fd = () => 12;
  kernel.thread_blocked_ofd_id = () => readOfd;
  kernel.thread_a0 = () => 12n;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 12 ? readOfd : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => (
    ofdId === readOfd || ofdId === writeOfd ? kernel.abi_fd_type_pipe() : 0
  );
  kernel.kernel_ofd_host_fd = (ofdId: number) => {
    if (ofdId === readOfd) {
      return 6;
    }
    if (ofdId === writeOfd) {
      return -7;
    }
    return 0;
  };
  kernel.kernel_ofd_ino = (ofdId: number) => (
    ofdId === readOfd || ofdId === writeOfd ? pipeSlot : 0
  );
  kernel.thread_pipe_read_eof_sync_pending = () => 1;

  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0,
  };
  new DataView(kernel.memory.buffer).setInt32(
    pipeSlot * PIPE_SLOT_BYTES + PIPE_WRITE_OPEN_OFFSET,
    0,
    true,
  );

  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr,
    kernel,
    kernelState,
    memory: kernel.memory,
    workers: new Set([worker]),
    blockedWorkers: new Map([[worker, {
      vmPtr,
      status: kernel.abi_status_pipe_wait(),
      retryTimer: null,
      timeoutExpired: false,
      timeoutDeadlineMs: null,
      sleepDeadlineTicks: null,
      rpcPending: false,
    }]]),
    networkClosedFds: new Set(),
    networkConnections: new Map(),
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set([readOfd, writeOfd]),
      forkSharedSyncPromise: null,
    },
    pendingChildExitRecords: 0,
  } as unknown as ProcessHandle;

  const childKernel = {
    ...kernel,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 13 ? writeOfd : 0,
  };
  const child = {
    pid: 2,
    parentPid: 1,
    vmPtr: childVmPtr,
    kernel: childKernel,
    kernelState,
    memory: kernel.memory,
    workers: new Set([childWorker]),
    exitInProgress: false,
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set([writeOfd]),
      forkSharedSyncPromise: null,
    },
  } as unknown as ProcessHandle;

  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const handleBlockedRetryResult = vi.fn();
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle], [2, child]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn(),
    handleBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(
    helpers.resumeBlockedSyscallLocally(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(vmPtr);
  expect(handleBlockedRetryResult).toHaveBeenCalledWith(handle, worker, vmPtr, kernel.abi_status_ok());
});

test("kernel-rpc blocked-rpc helper owns local resume and returns a typed outcome", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    workers: new Set([worker]),
    blockedWorkers: new Map([[worker, {
      vmPtr: 0x1234,
      status: 21,
      retryTimer: null,
      timeoutExpired: false,
      timeoutDeadlineMs: null,
      sleepDeadlineTicks: null,
      rpcPending: false,
    }]]),
    networkClosedFds: new Set(),
    networkConnections: new Map(),
    kernelShare: {
      members: new Set(),
      forkSharedSyncPromise: null,
    },
    pendingChildExitRecords: 0,
  } as unknown as ProcessHandle;
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const handleBlockedRetryResult = vi.fn(() => {
    handle.blockedWorkers.delete(worker);
  });
  const syncBlockedForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn(),
    handleBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker,
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(helpers.resumeBlockedSyscallLocally(handle, worker, 0x1234, 21)).resolves.toBeUndefined();
  handle.blockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 21,
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
  });
  await expect(helpers.resumeBlockedSyscallOutcome(handle, worker, 0x1234, 21)).resolves.toMatchObject({
    kind: "blocked",
    status: 0,
    effects: [{ kind: "sync-blocked-fork-shared-state", sourceVmPtr: 0x1234, status: 21 }],
  });
  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledTimes(2);
  expect(handleBlockedRetryResult).toHaveBeenCalledTimes(1);
  expect(syncBlockedForkSharedStateViaKernelWorker).toHaveBeenCalledTimes(1);
});

test("kernel-rpc blocked-rpc outcome reports blocked while the worker remains blocked", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    workers: new Set([worker]),
    blockedWorkers: new Map([[worker, {
      vmPtr: 0x1234,
      status: 21,
      retryTimer: null,
      timeoutExpired: false,
      timeoutDeadlineMs: null,
      sleepDeadlineTicks: null,
      rpcPending: false,
    }]]),
    networkClosedFds: new Set(),
    networkConnections: new Map(),
    kernelShare: {
      members: new Set(),
      forkSharedSyncPromise: null,
    },
    pendingChildExitRecords: 0,
  } as unknown as ProcessHandle;
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const syncBlockedForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn(),
    handleBlockedRetryResult: vi.fn(),
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker,
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(helpers.resumeBlockedSyscallOutcome(handle, worker, 0x1234, 21)).resolves.toMatchObject({
    kind: "blocked",
    status: 0,
    effects: [{ kind: "sync-blocked-fork-shared-state", sourceVmPtr: 0x1234, status: 21 }],
  });
  expect(syncBlockedForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
});

test("kernel-rpc blocked-rpc outcome reports retry effect without running scheduler retry", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    workers: new Set([worker]),
    blockedWorkers: new Map([[worker, {
      vmPtr: 0x1234,
      status: 21,
      retryTimer: null,
      timeoutExpired: false,
      timeoutDeadlineMs: null,
      sleepDeadlineTicks: null,
      rpcPending: false,
    }]]),
    networkClosedFds: new Set(),
    networkConnections: new Map(),
    kernelShare: {
      members: new Set(),
      forkSharedSyncPromise: null,
    },
    pendingChildExitRecords: 0,
  } as unknown as ProcessHandle;
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const syncBlockedForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn(),
    handleBlockedRetryResult: vi.fn(() => {
      handle.blockedWorkers.delete(worker);
    }),
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker,
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(helpers.resumeBlockedSyscallOutcome(handle, worker, 0x1234, 21)).resolves.toMatchObject({
    kind: "blocked",
    status: 0,
    effects: [{ kind: "sync-blocked-fork-shared-state", sourceVmPtr: 0x1234, status: 21 }],
  });
  expect(handle.blockedWorkers.has(worker)).toBe(true);
  expect(syncBlockedForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
});
