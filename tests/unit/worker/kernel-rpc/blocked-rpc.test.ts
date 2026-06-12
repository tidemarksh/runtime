import { expect, test, vi } from "vite-plus/test";
import { createWorkerKernelRpcBlockedRpcHelpers } from "../../../../src/worker/kernel-rpc/blocked-rpc.js";
import type { KernelRuntimeState } from "../../../../src/abi.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";
import { OFD_INO_OFFSET, OFD_OFFSET_OFFSET, OFD_TYPE_OFFSET } from "../../../../src/layout/ofd.js";
import { PIPE_GENERATION_OFFSET, PIPE_READ_OPEN_OFFSET, PIPE_SLOT_BYTES, PIPE_WRITE_OPEN_OFFSET } from "../../../../src/layout/pipe.js";

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x4000,
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
    memory: new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => state.heapBase,
    kernel_heap_ptr: () => state.heapPtr,
    kernel_ofd_table_ptr: () => state.ofdTablePtr,
    kernel_ofd_table_bytes: () => 64 * 32,
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
    kernel_pipe_region_bytes: () => PIPE_SLOT_BYTES * 64,
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
  kernel.kernel_pipe_base = () => 0;
  kernel.kernel_pipe_used = () => 1n << BigInt(pipeSlot);

  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0,
    pipeUsed: 1n << BigInt(pipeSlot),
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
  handle.kernelShare.members.add(handle);
  handle.kernelShare.members.add(child);
  child.kernelShare.members.add(handle);
  child.kernelShare.members.add(child);

  const threadState = { pendingPipeSlotsForNextStep: [], resumeBlockedOnNextStep: false };
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, threadState);
  const handleBlockedRetryResult = vi.fn();
  const enqueueWorker = vi.fn();
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle], [2, child]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    enqueueWorker,
    failProcess: vi.fn(),
    handleBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(
    helpers.resumeBlockedSyscallLocally(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handleBlockedRetryResult).not.toHaveBeenCalled();
  expect(threadState.resumeBlockedOnNextStep).toBe(true);
  expect(threadState.pendingPipeSlotsForNextStep).toHaveLength(1);
  expect(threadState.pendingPipeSlotsForNextStep[0]?.pipeSlot).toBe(pipeSlot);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("blocked-rpc pipe EOF uses current kernel pipe counters before stale shared writer records", async () => {
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
  kernel.thread_pipe_read_eof_sync_pending = () => 0;
  kernel.thread_blocked_pipe_read_ready = () => 0;
  kernel.thread_blocked_pipe_write_ready = () => 0;
  kernel.thread_blocked_pipe_buffered_len = () => 0;
  kernel.thread_blocked_pipe_write_open = () => 0;
  kernel.kernel_pipe_base = () => 0;
  kernel.kernel_pipe_used = () => 1n << BigInt(pipeSlot);

  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0,
    pipeUsed: 1n << BigInt(pipeSlot),
  };
  const staleKernelState = {
    ...kernelState,
    pipeBase: kernel.memory.buffer.byteLength + PIPE_SLOT_BYTES,
  };
  const view = new DataView(kernel.memory.buffer);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES, 0, true);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES + 4, 0, true);
  view.setInt32(pipeSlot * PIPE_SLOT_BYTES + PIPE_WRITE_OPEN_OFFSET, 0, true);

  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr,
    kernel,
    kernelState: staleKernelState,
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

  const threadState = { pendingPipeSlotsForNextStep: [], resumeBlockedOnNextStep: false };
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, threadState);
  const handleBlockedRetryResult = vi.fn();
  const enqueueWorker = vi.fn();
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle], [2, child]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    enqueueWorker,
    failProcess: vi.fn(),
    handleBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(
    helpers.resumeBlockedSyscallLocally(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handleBlockedRetryResult).not.toHaveBeenCalled();
  expect(threadState.resumeBlockedOnNextStep).toBe(true);
  expect(threadState.pendingPipeSlotsForNextStep).toHaveLength(1);
  expect(threadState.pendingPipeSlotsForNextStep[0]?.pipeSlot).toBe(pipeSlot);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("blocked-rpc resumes non-poll pipe read EOF from canonical closed pipe state", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
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
  kernel.thread_blocked_syscall_arg = (_vmPtr: number, index: number) => index === 0 ? 0n : 0n;
  kernel.thread_blocked_fd = () => 0;
  kernel.thread_blocked_ofd_id = () => readOfd;
  kernel.thread_a0 = () => 0n;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 0 ? readOfd : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === readOfd ? kernel.abi_fd_type_pipe() : 0;
  kernel.kernel_ofd_host_fd = (ofdId: number) => ofdId === readOfd ? 6 : 0;
  kernel.kernel_ofd_ino = (ofdId: number) => ofdId === readOfd ? pipeSlot : 0;
  kernel.thread_pipe_read_eof_sync_pending = () => 0;
  kernel.thread_blocked_pipe_read_ready = () => 0;
  kernel.thread_blocked_pipe_write_ready = () => 0;
  kernel.thread_blocked_pipe_buffered_len = () => 0;
  kernel.thread_blocked_pipe_write_open = () => 1;
  kernel.kernel_pipe_base = () => 0;
  kernel.kernel_pipe_used = () => 1n << BigInt(pipeSlot);

  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0,
    pipeUsed: 1n << BigInt(pipeSlot),
  };
  const view = new DataView(kernel.memory.buffer);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES, 0, true);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES + 4, 0, true);
  view.setInt32(pipeSlot * PIPE_SLOT_BYTES + PIPE_WRITE_OPEN_OFFSET, 1, true);

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
      sharedOfdIds: new Set([readOfd]),
      forkSharedSyncPromise: null,
    },
    pendingChildExitRecords: 0,
  } as unknown as ProcessHandle;

  const threadState = { pendingPipeSlotsForNextStep: [], resumeBlockedOnNextStep: false };
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, threadState);
  const handleBlockedRetryResult = vi.fn();
  const appendRuntimeLifecycleEvent = vi.fn();
  const appendWorkerDebugLog = vi.fn();
  const enqueueWorker = vi.fn();
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent,
    appendWorkerDebugLog,
    enqueueWorker,
    failProcess: vi.fn(),
    handleBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(
    helpers.resumeBlockedSyscallLocally(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handleBlockedRetryResult).not.toHaveBeenCalled();
  expect(threadState.resumeBlockedOnNextStep).toBe(true);
  expect(threadState.pendingPipeSlotsForNextStep).toHaveLength(1);
  expect(threadState.pendingPipeSlotsForNextStep[0]?.pipeSlot).toBe(pipeSlot);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("blocked-rpc resumes canonical pipe EOF when blocked syscall export returns numeric zero", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  const pipeSlot = 1;
  const readOfd = 26;
  let status = kernel.abi_status_pipe_wait();
  kernel.thread_status = () => status;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
    return 0n;
  });
  kernel.thread_blocked_syscall_nr = () => 0 as unknown as bigint;
  kernel.thread_a7 = () => kernel.abi_sys_read();
  kernel.thread_blocked_syscall_arg = (_vmPtr: number, index: number) => index === 0 ? 0n : 0n;
  kernel.thread_blocked_fd = () => 0;
  kernel.thread_blocked_ofd_id = () => readOfd;
  kernel.thread_a0 = () => 0n;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 0 ? readOfd : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === readOfd ? kernel.abi_fd_type_pipe() : 0;
  kernel.kernel_ofd_host_fd = (ofdId: number) => ofdId === readOfd ? 6 : 0;
  kernel.kernel_ofd_ino = (ofdId: number) => ofdId === readOfd ? pipeSlot : 0;
  kernel.thread_pipe_read_eof_sync_pending = () => 0;
  kernel.thread_blocked_pipe_read_ready = () => 0;
  kernel.thread_blocked_pipe_write_ready = () => 0;
  kernel.thread_blocked_pipe_buffered_len = () => 0;
  kernel.thread_blocked_pipe_write_open = () => 0;
  kernel.kernel_pipe_base = () => 0;
  kernel.kernel_pipe_used = () => 1n << BigInt(pipeSlot);

  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0,
    pipeUsed: 1n << BigInt(pipeSlot),
  };
  const view = new DataView(kernel.memory.buffer);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES, 0, true);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES + 4, 0, true);
  view.setInt32(pipeSlot * PIPE_SLOT_BYTES + PIPE_WRITE_OPEN_OFFSET, 0, true);

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
      sharedOfdIds: new Set([readOfd]),
      forkSharedSyncPromise: null,
    },
    pendingChildExitRecords: 0,
  } as unknown as ProcessHandle;

  const threadState = { pendingPipeSlotsForNextStep: [], resumeBlockedOnNextStep: false };
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, threadState);
  const handleBlockedRetryResult = vi.fn();
  const appendRuntimeLifecycleEvent = vi.fn();
  const appendWorkerDebugLog = vi.fn();
  const enqueueWorker = vi.fn();
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent,
    appendWorkerDebugLog,
    enqueueWorker,
    failProcess: vi.fn(),
    handleBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(
    helpers.resumeBlockedSyscallLocally(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handleBlockedRetryResult).not.toHaveBeenCalled();
  expect(threadState.resumeBlockedOnNextStep).toBe(true);
  expect(threadState.pendingPipeSlotsForNextStep).toHaveLength(1);
  expect(threadState.pendingPipeSlotsForNextStep[0]?.pipeSlot).toBe(pipeSlot);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("blocked-rpc resumes closed stdin EOF through fd-table fallback without pinned OFD", async () => {
  const kernel = makeKernel();
  kernel.memory = new WebAssembly.Memory({ initial: 5, maximum: 5, shared: true });
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  const pipeSlot = 3;
  const readOfd = 1;
  let status = kernel.abi_status_pipe_wait();
  kernel.thread_status = () => status;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
    return 0n;
  });
  kernel.thread_blocked_syscall_nr = () => kernel.abi_sys_read();
  kernel.thread_blocked_syscall_arg = (_vmPtr: number, index: number) => index === 0 ? 0n : 0n;
  kernel.thread_blocked_fd = () => 0;
  kernel.thread_blocked_ofd_id = () => 0;
  kernel.thread_a0 = () => 0n;
  kernel.thread_a7 = () => kernel.abi_sys_read();
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 0 ? readOfd : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === readOfd ? kernel.abi_fd_type_pipe() : 0;
  kernel.kernel_ofd_host_fd = (ofdId: number) => ofdId === readOfd ? 3 : 0;
  kernel.kernel_ofd_ino = (ofdId: number) => ofdId === readOfd ? pipeSlot : 0;
  kernel.thread_pipe_read_eof_sync_pending = () => 0;
  kernel.thread_blocked_pipe_read_ready = () => 0;
  kernel.thread_blocked_pipe_write_ready = () => 0;
  kernel.thread_blocked_pipe_buffered_len = () => 0;
  kernel.thread_blocked_pipe_write_open = () => 0;
  kernel.kernel_pipe_base = () => 0;
  kernel.kernel_pipe_used = () => 1n << BigInt(pipeSlot);

  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0,
    pipeUsed: 1n << BigInt(pipeSlot),
  };
  const view = new DataView(kernel.memory.buffer);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES, 0, true);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES + 4, 0, true);
  view.setInt32(pipeSlot * PIPE_SLOT_BYTES + PIPE_READ_OPEN_OFFSET, 1, true);
  view.setInt32(pipeSlot * PIPE_SLOT_BYTES + PIPE_WRITE_OPEN_OFFSET, 0, true);

  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr,
    kernel,
    kernelState,
    memory: kernel.memory,
    stdinPipe: 0,
    stdoutPipe: 4,
    stderrPipe: 5,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 4,
      stderrPipe: 5,
      refCount: 1,
    },
    stdinClosed: true,
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
      sharedOfdIds: new Set([readOfd]),
      forkSharedSyncPromise: null,
    },
    pendingChildExitRecords: 0,
  } as unknown as ProcessHandle;

  const threadState = { pendingPipeSlotsForNextStep: [], resumeBlockedOnNextStep: false };
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, threadState);
  const handleBlockedRetryResult = vi.fn();
  const enqueueWorker = vi.fn();
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    enqueueWorker,
    failProcess: vi.fn(),
    handleBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(
    helpers.resumeBlockedSyscallLocally(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handleBlockedRetryResult).not.toHaveBeenCalled();
  expect(threadState.resumeBlockedOnNextStep).toBe(true);
  expect(threadState.pendingPipeSlotsForNextStep).toHaveLength(1);
  expect(threadState.pendingPipeSlotsForNextStep[0]?.pipeSlot).toBe(pipeSlot);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("blocked-rpc clears stale pipe writer count before EOF-pending resume", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
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
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => {
    if (fd === 12) {
      return readOfd;
    }
    if (fd === 13) {
      return writeOfd;
    }
    return 0;
  };
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
  kernel.thread_blocked_pipe_read_ready = () => 0;
  kernel.thread_blocked_pipe_write_ready = () => 0;
  kernel.thread_blocked_pipe_buffered_len = () => 0;
  kernel.thread_blocked_pipe_write_open = () => 1;
  kernel.thread_resume_pipe_read_open_empty = () => 0;
  kernel.kernel_pipe_base = () => 0;
  kernel.kernel_pipe_used = () => 1n << BigInt(pipeSlot);

  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0,
    pipeUsed: 1n << BigInt(pipeSlot),
  };
  const view = new DataView(kernel.memory.buffer);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES, 0, true);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES + 4, 0, true);
  view.setInt32(pipeSlot * PIPE_SLOT_BYTES + PIPE_WRITE_OPEN_OFFSET, 1, true);

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
  handle.kernelShare.members.add(handle);

  const threadState = { pendingPipeSlotsForNextStep: [], resumeBlockedOnNextStep: false };
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, threadState);
  const handleBlockedRetryResult = vi.fn();
  const enqueueWorker = vi.fn();
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    enqueueWorker,
    failProcess: vi.fn(),
    handleBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(
    helpers.resumeBlockedSyscallLocally(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(view.getInt32(pipeSlot * PIPE_SLOT_BYTES + PIPE_WRITE_OPEN_OFFSET, true)).toBe(0);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handleBlockedRetryResult).not.toHaveBeenCalled();
  expect(threadState.resumeBlockedOnNextStep).toBe(true);
  expect(threadState.pendingPipeSlotsForNextStep).toHaveLength(1);
  expect(threadState.pendingPipeSlotsForNextStep[0]?.pipeSlot).toBe(pipeSlot);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("blocked-rpc does not resume pipe EOF while same process still has a write fd", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  const pipeSlot = 1;
  const readOfd = 13;
  const writeOfd = 14;
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
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => {
    if (fd === 12) {
      return readOfd;
    }
    if (fd === 13) {
      return writeOfd;
    }
    return 0;
  };
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
  kernel.thread_resume_pipe_read_open_empty = () => 1;
  kernel.thread_blocked_pipe_read_ready = () => 0;
  kernel.thread_blocked_pipe_write_ready = () => 0;
  kernel.thread_blocked_pipe_buffered_len = () => 0;
  kernel.thread_blocked_pipe_write_open = () => 1;
  kernel.kernel_pipe_base = () => 0;

  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0,
  };
  const view = new DataView(kernel.memory.buffer);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES, 0, true);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES + 4, 0, true);
  view.setInt32(pipeSlot * PIPE_SLOT_BYTES + PIPE_WRITE_OPEN_OFFSET, 1, true);

  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr,
    kernel,
    kernelState,
    memory: kernel.memory,
    kernelWorkerRequest: true,
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
  handle.kernelShare.members.add(handle);

  const threadState = { pendingPipeSlotsForNextStep: [], resumeBlockedOnNextStep: false };
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, threadState);
  const handleBlockedRetryResult = vi.fn();
  const enqueueWorker = vi.fn();
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    enqueueWorker,
    failProcess: vi.fn(),
    handleBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(
    helpers.resumeBlockedSyscallLocally(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(view.getInt32(pipeSlot * PIPE_SLOT_BYTES + PIPE_WRITE_OPEN_OFFSET, true)).toBe(1);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handleBlockedRetryResult).toHaveBeenCalledWith(handle, worker, vmPtr, kernel.abi_status_pipe_wait());
});

test("blocked-rpc restores blocked worker local pipe endpoints when canonical slot was already closed", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  const pipeSlot = 1;
  const readOfd = 13;
  const writeOfd = 14;
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
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => {
    if (fd === 12) {
      return readOfd;
    }
    if (fd === 13) {
      return writeOfd;
    }
    return 0;
  };
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
  kernel.thread_resume_pipe_read_open_empty = () => 0;
  kernel.thread_blocked_pipe_read_ready = () => 0;
  kernel.thread_blocked_pipe_write_ready = () => 0;
  kernel.thread_blocked_pipe_buffered_len = () => 0;
  kernel.thread_blocked_pipe_write_open = () => 1;
  kernel.kernel_pipe_base = () => 0;

  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0,
  };
  const view = new DataView(kernel.memory.buffer);
  const pipeOffset = pipeSlot * PIPE_SLOT_BYTES;
  view.setUint32(pipeOffset, 0, true);
  view.setUint32(pipeOffset + 4, 0, true);
  view.setInt32(pipeOffset + PIPE_READ_OPEN_OFFSET, 0, true);
  view.setInt32(pipeOffset + PIPE_WRITE_OPEN_OFFSET, 0, true);

  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr,
    kernel,
    kernelState,
    memory: kernel.memory,
    kernelWorkerRequest: true,
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
  handle.kernelShare.members.add(handle);

  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const handleBlockedRetryResult = vi.fn();
  const enqueueWorker = vi.fn();
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    enqueueWorker,
    failProcess: vi.fn(),
    handleBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(
    helpers.resumeBlockedSyscallLocally(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(view.getInt32(pipeOffset + PIPE_READ_OPEN_OFFSET, true)).toBe(1);
  expect(view.getInt32(pipeOffset + PIPE_WRITE_OPEN_OFFSET, true)).toBe(1);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handleBlockedRetryResult).toHaveBeenCalledWith(handle, worker, vmPtr, kernel.abi_status_pipe_wait());
  expect(enqueueWorker).not.toHaveBeenCalled();
});

test("blocked-rpc aligns pipe OFD generation before canonical EOF resume", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  const pipeSlot = 1;
  const readOfd = 26;
  const pipeGeneration = 9;
  let status = kernel.abi_status_pipe_wait();
  kernel.thread_status = () => status;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    const offset = new DataView(kernel.memory.buffer).getBigInt64(
      kernel.kernel_ofd_table_ptr() + readOfd * 32 + OFD_OFFSET_OFFSET,
      true,
    );
    if (offset === BigInt(pipeGeneration)) {
      status = kernel.abi_status_ok();
    }
    return 0n;
  });
  kernel.thread_blocked_syscall_nr = () => kernel.abi_sys_read();
  kernel.thread_blocked_syscall_arg = (_vmPtr: number, index: number) => index === 0 ? 12n : 0n;
  kernel.thread_blocked_fd = () => 12;
  kernel.thread_blocked_ofd_id = () => readOfd;
  kernel.thread_a0 = () => 12n;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 12 ? readOfd : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === readOfd ? kernel.abi_fd_type_pipe() : 0;
  kernel.kernel_ofd_host_fd = (ofdId: number) => ofdId === readOfd ? 6 : 0;
  kernel.kernel_ofd_ino = (ofdId: number) => ofdId === readOfd ? pipeSlot : 0;
  kernel.thread_pipe_read_eof_sync_pending = () => 0;
  kernel.thread_blocked_pipe_read_ready = () => 0;
  kernel.thread_blocked_pipe_write_ready = () => 0;
  kernel.thread_blocked_pipe_buffered_len = () => 0;
  kernel.thread_blocked_pipe_write_open = () => 0;
  kernel.kernel_pipe_base = () => 0;

  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0,
  };
  const view = new DataView(kernel.memory.buffer);
  const pipeOffset = pipeSlot * PIPE_SLOT_BYTES;
  view.setUint32(pipeOffset, 0, true);
  view.setUint32(pipeOffset + 4, 0, true);
  view.setInt32(pipeOffset + PIPE_WRITE_OPEN_OFFSET, 0, true);
  view.setUint32(pipeOffset + PIPE_GENERATION_OFFSET, pipeGeneration, true);
  const ofdOffset = kernelState.ofdTablePtr + readOfd * 32;
  view.setInt32(ofdOffset + OFD_TYPE_OFFSET, kernel.abi_fd_type_pipe(), true);
  view.setBigInt64(ofdOffset + OFD_OFFSET_OFFSET, 3n, true);
  view.setUint32(ofdOffset + OFD_INO_OFFSET, pipeSlot, true);

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
      sharedOfdIds: new Set([readOfd]),
      forkSharedSyncPromise: null,
    },
    pendingChildExitRecords: 0,
  } as unknown as ProcessHandle;

  const threadState = { pendingPipeSlotsForNextStep: [], resumeBlockedOnNextStep: false };
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, threadState);
  const handleBlockedRetryResult = vi.fn();
  const enqueueWorker = vi.fn();
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    enqueueWorker,
    failProcess: vi.fn(),
    handleBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(
    helpers.resumeBlockedSyscallLocally(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(view.getBigInt64(ofdOffset + OFD_OFFSET_OFFSET, true)).toBe(BigInt(pipeGeneration));
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handleBlockedRetryResult).not.toHaveBeenCalled();
  expect(threadState.resumeBlockedOnNextStep).toBe(true);
  expect(threadState.pendingPipeSlotsForNextStep).toHaveLength(1);
  expect(threadState.pendingPipeSlotsForNextStep[0]?.pipeSlot).toBe(pipeSlot);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("blocked-rpc does not preserve host stdin writer when stdin is explicitly closed", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  const pipeSlot = 1;
  const readOfd = 1;
  let status = kernel.abi_status_pipe_wait();
  kernel.thread_status = () => status;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
    return 0n;
  });
  kernel.thread_blocked_syscall_nr = () => kernel.abi_sys_read();
  kernel.thread_blocked_syscall_arg = (_vmPtr: number, index: number) => index === 0 ? 0n : 0n;
  kernel.thread_blocked_fd = () => 0;
  kernel.thread_blocked_ofd_id = () => readOfd;
  kernel.thread_a0 = () => 0n;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 0 ? readOfd : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === readOfd ? kernel.abi_fd_type_pipe() : 0;
  kernel.kernel_ofd_host_fd = (ofdId: number) => ofdId === readOfd ? 3 : 0;
  kernel.kernel_ofd_ino = (ofdId: number) => ofdId === readOfd ? pipeSlot : 0;
  kernel.thread_pipe_read_eof_sync_pending = () => 0;
  kernel.thread_blocked_pipe_read_ready = () => 0;
  kernel.thread_blocked_pipe_write_ready = () => 0;
  kernel.thread_blocked_pipe_buffered_len = () => 0;
  kernel.thread_blocked_pipe_write_open = () => 0;
  kernel.kernel_pipe_base = () => 0;

  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0,
  };
  const view = new DataView(kernel.memory.buffer);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES, 0, true);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES + 4, 0, true);
  view.setInt32(pipeSlot * PIPE_SLOT_BYTES + PIPE_WRITE_OPEN_OFFSET, 0, true);

  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr,
    kernel,
    kernelState,
    memory: kernel.memory,
    stdinPipe: pipeSlot,
    stdoutPipe: 4,
    stderrPipe: 5,
    hostStdio: {
      stdinPipe: pipeSlot,
      stdoutPipe: 4,
      stderrPipe: 5,
      refCount: 1,
    },
    stdioOwner: null,
    stdinClosed: true,
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
      sharedOfdIds: new Set([readOfd]),
      forkSharedSyncPromise: null,
    },
    pendingChildExitRecords: 0,
  } as unknown as ProcessHandle;

  const threadState = { pendingPipeSlotsForNextStep: [], resumeBlockedOnNextStep: false };
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, threadState);
  const handleBlockedRetryResult = vi.fn();
  const appendRuntimeLifecycleEvent = vi.fn();
  const appendWorkerDebugLog = vi.fn();
  const enqueueWorker = vi.fn();
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent,
    appendWorkerDebugLog,
    enqueueWorker,
    failProcess: vi.fn(),
    handleBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(
    helpers.resumeBlockedSyscallLocally(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(view.getInt32(pipeSlot * PIPE_SLOT_BYTES + PIPE_WRITE_OPEN_OFFSET, true)).toBe(0);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handleBlockedRetryResult).not.toHaveBeenCalled();
  expect(threadState.resumeBlockedOnNextStep).toBe(true);
  expect(threadState.pendingPipeSlotsForNextStep).toHaveLength(1);
  expect(threadState.pendingPipeSlotsForNextStep[0]?.pipeSlot).toBe(pipeSlot);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
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
  const threadState = { pendingGuestMemoryWrites: [] };
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, threadState);
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

test("kernel-rpc blocked-rpc outcome keeps ownership while continuation effects are pending", async () => {
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
      rpcGeneration: 4,
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
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn(),
    handleBlockedRetryResult: vi.fn(),
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(helpers.resumeBlockedSyscallOutcome(handle, worker, 0x1234, 21)).resolves.toMatchObject({
    kind: "blocked",
    status: 0,
    effects: [{ kind: "sync-blocked-fork-shared-state", sourceVmPtr: 0x1234, status: 21 }],
  });

  expect(handle.blockedWorkers.get(worker)?.rpcPending).toBe(true);
});

test("blocked-rpc does not resume poll hup when pselect watches no pipe slots", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  const pselect6 = 72n;
  const pipeSlot = 3;
  const readOfd = 1;
  kernel.abi_sys_pselect6 = () => pselect6;
  kernel.thread_status = () => kernel.abi_status_pipe_wait();
  kernel.thread_blocked_syscall_nr = () => pselect6;
  kernel.thread_blocked_syscall_arg = () => 0n;
  kernel.thread_a7 = () => pselect6;
  kernel.thread_blocked_fd = () => 0;
  kernel.thread_blocked_ofd_id = () => 0;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 0 ? readOfd : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === readOfd ? kernel.abi_fd_type_pipe() : 0;
  kernel.kernel_ofd_host_fd = (ofdId: number) => ofdId === readOfd ? 3 : 0;
  kernel.kernel_ofd_ino = (ofdId: number) => ofdId === readOfd ? pipeSlot : 0;
  kernel.thread_poll_wait_pipe_slots_mask = () => 0n;
  kernel.thread_poll_wait_first_pipe_revents = () => 0;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_blocked_pipe_buffered_len = () => 0;
  kernel.thread_blocked_pipe_write_open = () => 0;
  kernel.kernel_pipe_base = () => 0;

  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0,
    pipeUsed: 1n << BigInt(pipeSlot),
  };
  const view = new DataView(kernel.memory.buffer);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES, 0, true);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES + 4, 0, true);
  view.setInt32(pipeSlot * PIPE_SLOT_BYTES + PIPE_WRITE_OPEN_OFFSET, 0, true);

  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr,
    kernel,
    kernelState,
    memory: kernel.memory,
    guestBase: 0,
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
      forkSharedSyncPromise: null,
    },
    pendingChildExitRecords: 0,
  } as unknown as ProcessHandle;

  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const handleBlockedRetryResult = vi.fn();
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
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

  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handleBlockedRetryResult).toHaveBeenCalledWith(
    handle,
    worker,
    vmPtr,
    kernel.abi_status_pipe_wait(),
  );
});

test("blocked-rpc resumes pselect pipe HUP through blocked fd fallback", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  const pselect6 = 72n;
  const pipeSlot = 3;
  const readOfd = 1;
  const readfdsPtr = 0x20n;
  let status = kernel.abi_status_pipe_wait();
  kernel.abi_sys_pselect6 = () => pselect6;
  kernel.thread_status = () => status;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
    return 1n;
  });
  kernel.thread_blocked_syscall_nr = () => pselect6;
  kernel.thread_blocked_syscall_arg = (_vmPtr: number, index: number) => {
    switch (index) {
      case 0:
        return 1n;
      case 1:
        return readfdsPtr;
      default:
        return 0n;
    }
  };
  kernel.thread_a7 = () => pselect6;
  kernel.thread_blocked_fd = () => 0;
  kernel.thread_blocked_ofd_id = () => 0;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 0 ? readOfd : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === readOfd ? kernel.abi_fd_type_pipe() : 0;
  kernel.kernel_ofd_host_fd = (ofdId: number) => ofdId === readOfd ? 3 : 0;
  kernel.kernel_ofd_ino = (ofdId: number) => ofdId === readOfd ? pipeSlot : 0;
  kernel.thread_poll_wait_pipe_slots_mask = () => 0n;
  kernel.thread_poll_wait_first_pipe_revents = () => 0;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_blocked_pipe_buffered_len = () => 0;
  kernel.thread_blocked_pipe_write_open = () => 0;
  kernel.kernel_pipe_base = () => 0;

  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0,
    pipeUsed: 1n << BigInt(pipeSlot),
  };
  const view = new DataView(kernel.memory.buffer);
  view.setBigUint64(Number(readfdsPtr), 1n, true);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES, 0, true);
  view.setUint32(pipeSlot * PIPE_SLOT_BYTES + 4, 0, true);
  view.setInt32(pipeSlot * PIPE_SLOT_BYTES + PIPE_WRITE_OPEN_OFFSET, 0, true);

  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr,
    kernel,
    kernelState,
    memory: kernel.memory,
    guestBase: 0,
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
      forkSharedSyncPromise: null,
    },
    pendingChildExitRecords: 0,
  } as unknown as ProcessHandle;

  const threadState = { pendingGuestMemoryWrites: [] };
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, threadState);
  const handleBlockedRetryResult = vi.fn();
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    handleBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await expect(
    helpers.resumeBlockedSyscallLocally(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(vmPtr);
  expect(handleBlockedRetryResult).toHaveBeenCalledWith(
    handle,
    worker,
    vmPtr,
    kernel.abi_status_ok(),
  );
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
