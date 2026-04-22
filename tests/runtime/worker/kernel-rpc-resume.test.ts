import { expect, test, vi } from "vite-plus/test";
import { createRuntimeStateBridge } from "../../../src/bridge/runtime-state.js";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { createWorkerKernelRpcHelpers } from "../../../src/worker/kernel-rpc.js";
import { createWorkerKernelRpcResumeLifecycleHelpers } from "../../../src/worker/kernel-rpc/lifecycle/resume.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

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

function makeHandle(
  kernel: KernelExports,
  runtimeBridge: ProcessHandle["runtimeBridge"],
  worker: Worker,
  overrides: Partial<ProcessHandle> = {},
): ProcessHandle {
  return {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0x1234,
    ramSize: 0,
    maxMemoryPages: 0,
    forkMaxMemoryPages: 0,
    guestBase: 0,
    worker,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel,
    kernelState: makeKernelState(),
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    ttyPtySlot: 0,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    stdioOwner: null,
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
    stdinQueue: [],
    stdinFlushPromise: null,
    outputBacklog: [],
    debugCommand: "test",
    runtimeBridge,
    sabPageCache: null,
    workers: new Set([worker]),
    readyWorkers: [],
    runningWorker: null,
    blockedWorkers: new Map(),
    pendingChildExitRecords: 0,
    pendingChildSpawnRecords: 0,
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
    guestWriteWatchRegions: [],
    ...overrides,
  } as ProcessHandle;
}

test("resumeWait4ViaKernelWorker does not re-run local wait4 after kernel-worker resume", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const baseState = makeKernelState();
  const resumedState = {
    ...baseState,
    heapPtr: baseState.heapPtr + 0x80,
  };
  const threadResumeWait4 = vi.fn();
  let status = 31;
  const kernel = {
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => resumedState.heapBase,
    kernel_heap_ptr: () => resumedState.heapPtr,
    kernel_ofd_table_ptr: () => resumedState.ofdTablePtr,
    kernel_memfs_ptr: () => resumedState.memfsPtr,
    kernel_memfs_header_bytes: () => resumedState.memfsHeaderBytes,
    kernel_memfs_inodes_bytes: () => resumedState.memfsInodesBytes,
    kernel_memfs_first_child_bytes: () => resumedState.memfsFirstChildBytes,
    kernel_memfs_dir_entries_bytes: () => resumedState.memfsDirEntriesBytes,
    kernel_memfs_data_bytes: () => resumedState.memfsDataBytes,
    kernel_mem_state_ptr: () => resumedState.memStatePtr,
    kernel_epoll_table_ptr: () => resumedState.epollTablePtr,
    kernel_socket_table_ptr: () => resumedState.socketTablePtr,
    kernel_socket_used: () => resumedState.socketUsed,
    kernel_pipe_base: () => resumedState.pipeBase,
    kernel_pipe_used: () => resumedState.pipeUsed,
    kernel_net_buf_base: () => resumedState.netBufBase,
    kernel_net_buf_used: () => resumedState.netBufUsed,
    kernel_sig_slots_ptr: () => resumedState.sigSlotsPtr,
    kernel_process_pending_ptr: () => resumedState.processPendingPtr,
    kernel_process_runtime_state_ptr: () => resumedState.processRuntimeStatePtr,
    kernel_time_state_ptr: () => resumedState.timeStatePtr,
    abi_status_wait4: () => 31,
    thread_status: () => status,
    thread_resume_wait4: threadResumeWait4,
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    kernelState: baseState,
    kernelWorkerRequest: vi.fn(async () => {
      status = 0;
      return {
        type: "wait4-resumed",
        requestId: 0,
        kernelState: resumedState,
      } as const;
    }),
  });
  handle.blockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 31,
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: true,
  });

  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

  const handledStatuses: number[] = [];
  const pipeSyncWakeArgs: boolean[] = [];
  const helpers = createWorkerKernelRpcResumeLifecycleHelpers({
    appendWorkerDebugLog: () => {},
    canonicalizeChildLifecycleViaKernelWorker: async () => ({
      childPid: 2,
      nextPid: 3,
      pgid: 1,
      sid: 1,
    }),
    clearVforkSnapshot: () => {},
    drainProcessOutput: () => false,
    enqueueWorker: () => {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleBlockedRetryResult: (_handle, _worker, _vmPtr, nextStatus) => {
      handledStatuses.push(nextStatus);
    },
    kickBlockedWorkers: () => {},
    processHandles: new Map([[1, handle]]),
    requestPipeStateSync: async (_handle, wakeWaiters = false) => {
      pipeSyncWakeArgs.push(wakeWaiters);
    },
    threadWorkerStates: threadWorkerStates as never,
  });

  await helpers.resumeWait4ViaKernelWorker(handle, worker, 0x1234);

  expect(threadResumeWait4).not.toHaveBeenCalled();
  expect(handledStatuses).toEqual([0]);
  expect(pipeSyncWakeArgs[0]).toBe(true);
  expect(handle.kernelState.heapPtr).toBe(resumedState.heapPtr);
});

test("resumeWait4ViaKernelWorker skips RPC when local wait4 already resolved", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const kernel = {
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    abi_status_wait4: () => 31,
    thread_status: () => 0,
  } as unknown as KernelExports;

  const kernelWorkerRequest = vi.fn(async () => {
    throw new Error("kernel-worker resume should be skipped when local wait4 already resolved");
  });
  const handle = makeHandle(kernel, runtimeBridge, worker, {
    kernelWorkerRequest,
  });
  handle.blockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 31,
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: true,
  });

  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

  const handledStatuses: number[] = [];
  const helpers = createWorkerKernelRpcResumeLifecycleHelpers({
    appendWorkerDebugLog: () => {},
    canonicalizeChildLifecycleViaKernelWorker: async () => ({
      childPid: 2,
      nextPid: 3,
      pgid: 1,
      sid: 1,
    }),
    clearVforkSnapshot: () => {},
    drainProcessOutput: () => false,
    enqueueWorker: () => {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleBlockedRetryResult: (_handle, _worker, _vmPtr, nextStatus) => {
      handledStatuses.push(nextStatus);
    },
    kickBlockedWorkers: () => {},
    processHandles: new Map([[1, handle]]),
    requestPipeStateSync: async () => {},
    threadWorkerStates: threadWorkerStates as never,
  });

  await helpers.resumeWait4ViaKernelWorker(handle, worker, 0x1234);

  expect(kernelWorkerRequest).not.toHaveBeenCalled();
  expect(handledStatuses).toEqual([0]);
  expect(handle.blockedWorkers.get(worker)?.rpcPending).toBe(false);
});

test("resumeVforkExecveParentViaKernelWorker restores the parent before clearing vfork runtime state", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const baseState = makeKernelState();
  const resumedState = {
    ...baseState,
    heapPtr: baseState.heapPtr + 0x80,
  };
  const opLog: string[] = [];
  const kernel = {
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => resumedState.heapBase,
    kernel_heap_ptr: () => resumedState.heapPtr,
    kernel_ofd_table_ptr: () => resumedState.ofdTablePtr,
    kernel_memfs_ptr: () => resumedState.memfsPtr,
    kernel_memfs_header_bytes: () => resumedState.memfsHeaderBytes,
    kernel_memfs_inodes_bytes: () => resumedState.memfsInodesBytes,
    kernel_memfs_first_child_bytes: () => resumedState.memfsFirstChildBytes,
    kernel_memfs_dir_entries_bytes: () => resumedState.memfsDirEntriesBytes,
    kernel_memfs_data_bytes: () => resumedState.memfsDataBytes,
    kernel_mem_state_ptr: () => resumedState.memStatePtr,
    kernel_epoll_table_ptr: () => resumedState.epollTablePtr,
    kernel_socket_table_ptr: () => resumedState.socketTablePtr,
    kernel_socket_used: () => resumedState.socketUsed,
    kernel_pipe_base: () => resumedState.pipeBase,
    kernel_pipe_used: () => resumedState.pipeUsed,
    kernel_net_buf_base: () => resumedState.netBufBase,
    kernel_net_buf_used: () => resumedState.netBufUsed,
    kernel_sig_slots_ptr: () => resumedState.sigSlotsPtr,
    kernel_process_pending_ptr: () => resumedState.processPendingPtr,
    kernel_process_runtime_state_ptr: () => resumedState.processRuntimeStatePtr,
    kernel_time_state_ptr: () => resumedState.timeStatePtr,
    thread_clear_spawn_handoff: (vmPtr: number) => {
      opLog.push(`clear-spawn:${vmPtr}`);
    },
    thread_restore_vfork_parent_without_cleanup: (vmPtr: number, childPid: number) => {
      opLog.push(`restore-vfork:${vmPtr}:${childPid}`);
    },
    thread_resume_vfork_parent: (vmPtr: number, childPid: number) => {
      opLog.push(`resume-vfork:${vmPtr}:${childPid}`);
    },
    thread_clear_vfork_runtime_state: () => {
      opLog.push("clear-vfork");
    },
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    kernelState: baseState,
    kernelWorkerRequest: vi.fn(async () => ({
      type: "vfork-execve-parent-resumed",
      requestId: 0,
      kernelState: resumedState,
    })),
  });

  const helpers = createWorkerKernelRpcResumeLifecycleHelpers({
    appendWorkerDebugLog: () => {},
    canonicalizeChildLifecycleViaKernelWorker: async () => ({
      childPid: 2,
      nextPid: 3,
      pgid: 1,
      sid: 1,
    }),
    clearVforkSnapshot: () => {},
    drainProcessOutput: () => false,
    enqueueWorker: () => {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleBlockedRetryResult: () => {},
    kickBlockedWorkers: () => {},
    processHandles: new Map([[1, handle]]),
    requestPipeStateSync: async () => {},
    threadWorkerStates: new WeakMap() as never,
  });

  await helpers.resumeVforkExecveParentViaKernelWorker(handle, 0x1234, 7);

  expect(opLog).toEqual([
    "clear-spawn:4660",
    "restore-vfork:4660:7",
    "clear-vfork",
  ]);
  expect(handle.kernelState.heapPtr).toBe(resumedState.heapPtr);
});

test("resumeBlockedSyscallLocally skips kernel-worker shared-state syncs during active vfork child ownership mismatch", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const threadResumeBlockedSyscall = vi.fn();
  const syncBlockedForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const syncSocketStateViaKernelWorker = vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n }));
  const handleBlockedRetryResult = vi.fn();
  const kernel = {
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    abi_status_pipe_wait: () => 21,
    abi_status_connect: () => 28,
    abi_status_socket_recv: () => 29,
    abi_status_socket_send: () => 30,
    thread_resume_blocked_syscall: threadResumeBlockedSyscall,
    thread_status: () => 0,
    process_current_pid: () => 4,
    thread_a0: () => 0n,
    thread_a1: () => 0n,
    thread_process_ptr: () => 0x5000,
    process_fd_ofd_id: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_host_fd: () => 0,
    kernel_ofd_status_flags: () => 0,
    kernel_ofd_ino: () => 0,
    kernel_ofd_offset: () => 0n,
    thread_blocked_fd: () => 0,
    thread_blocked_ofd_id: () => 0,
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    pid: 1,
    kernelWorkerRequest: vi.fn(async () => ({ type: "unexpected" }) as never),
  });
  handle.blockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 29,
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: true,
  });

  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

  const helpers = createWorkerKernelRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendWorkerDebugLog: () => {},
    assertProcessStandardFdIntegrity: () => {},
    enqueueWorker: () => {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleBlockedRetryResult,
    kickBlockedWorkers: () => {},
    markRuntimeFsDirty: () => {},
    requestPipeStateSync: async () => {},
    syncBlockedForkSharedStateViaKernelWorker,
    syncForkSharedStateLocally: () => {},
    syncForkSharedStateViaKernelWorker: async () => {},
    rehydrateForkSharedStateViaKernelWorker: async () => {},
    syncPipeStateViaKernelWorker: async () => 0n,
    syncSocketStateViaKernelWorker,
    collectTrackableForkSharedOfds: () => new Set(),
  });

  await helpers.resumeBlockedSyscallLocally(handle, worker, 0x1234, 29);

  expect(threadResumeBlockedSyscall).toHaveBeenCalledWith(0x1234);
  expect(syncBlockedForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  expect(syncSocketStateViaKernelWorker).not.toHaveBeenCalled();
  expect(handleBlockedRetryResult).toHaveBeenCalledWith(handle, worker, 0x1234, 0);
  expect(handle.blockedWorkers.get(worker)?.rpcPending).toBe(false);
});

test("resumeBlockedSyscallLocally wakes pipe waiters through requestPipeStateSync", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const threadResumeBlockedSyscall = vi.fn();
  const syncBlockedForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const requestPipeStateSync = vi.fn(async () => {});
  const handleBlockedRetryResult = vi.fn();
  const kernel = {
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    abi_status_pipe_wait: () => 21,
    abi_status_connect: () => 28,
    abi_status_socket_recv: () => 29,
    abi_status_socket_send: () => 30,
    thread_resume_blocked_syscall: threadResumeBlockedSyscall,
    thread_status: () => 21,
    process_current_pid: () => 1,
    thread_a0: () => 0n,
    thread_a1: () => 0n,
    thread_process_ptr: () => 0x5000,
    process_fd_ofd_id: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_host_fd: () => 0,
    kernel_ofd_status_flags: () => 0,
    kernel_ofd_ino: () => 0,
    kernel_ofd_offset: () => 0n,
    thread_blocked_fd: () => 0,
    thread_blocked_ofd_id: () => 0,
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    pid: 1,
    kernelWorkerRequest: vi.fn(async () => ({ type: "unexpected" }) as never),
  });
  handle.blockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 21,
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: true,
  });

  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

  const helpers = createWorkerKernelRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendWorkerDebugLog: () => {},
    assertProcessStandardFdIntegrity: () => {},
    enqueueWorker: () => {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleBlockedRetryResult,
    kickBlockedWorkers: () => {},
    markRuntimeFsDirty: () => {},
    requestPipeStateSync,
    syncBlockedForkSharedStateViaKernelWorker,
    syncForkSharedStateLocally: () => {},
    syncForkSharedStateViaKernelWorker: async () => {},
    rehydrateForkSharedStateViaKernelWorker: async () => {},
    syncPipeStateViaKernelWorker: async () => 0n,
    syncSocketStateViaKernelWorker: async () => ({ socketUsed: 0, netBufUsed: 0n }),
    collectTrackableForkSharedOfds: () => new Set(),
  });

  await helpers.resumeBlockedSyscallLocally(handle, worker, 0x1234, 21);

  expect(threadResumeBlockedSyscall).toHaveBeenCalledWith(0x1234);
  expect(syncBlockedForkSharedStateViaKernelWorker).toHaveBeenCalledWith(handle, 0x1234);
  expect(requestPipeStateSync).toHaveBeenCalledWith(handle, true);
  expect(handleBlockedRetryResult).toHaveBeenCalledWith(handle, worker, 0x1234, 21);
  expect(handle.blockedWorkers.get(worker)?.rpcPending).toBe(false);
});

test("resumeFsViaKernelWorker skips memfs sync during active vfork child ownership mismatch after fs resume", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const threadProcessFs = vi.fn();
  const threadResumeFs = vi.fn();
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const markRuntimeFsDirty = vi.fn();
  const enqueueWorker = vi.fn();
  let currentPid = 1;

  const kernel = {
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    process_current_pid: () => currentPid,
    thread_pending_fs_opcode: () => 7,
    thread_pending_fs_effects: () => 1,
    thread_pending_fs_target_ino: () => 0,
    thread_process_fs: threadProcessFs,
    thread_resume_fs: () => {
      threadResumeFs();
      currentPid = 5;
      return 0n;
    },
    thread_a0: () => 0n,
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    pid: 1,
    kernelWorkerRequest: vi.fn(async () => ({ type: "unexpected" }) as never),
  });

  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

  const helpers = createWorkerKernelRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendWorkerDebugLog: () => {},
    assertProcessStandardFdIntegrity: () => {},
    enqueueWorker,
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleBlockedRetryResult: () => {},
    kickBlockedWorkers: () => {},
    markRuntimeFsDirty,
    requestPipeStateSync: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncForkSharedStateLocally: () => {},
    syncForkSharedStateViaKernelWorker,
    rehydrateForkSharedStateViaKernelWorker: async () => {},
    syncPipeStateViaKernelWorker: async () => 0n,
    syncSocketStateViaKernelWorker: async () => ({ socketUsed: 0, netBufUsed: 0n }),
    collectTrackableForkSharedOfds: () => new Set(),
  });

  await helpers.resumeFsViaKernelWorker(handle, worker, 0x1234, {
    memfsMutation: 1,
    memfsFileData: 4,
    sharedOfdState: 2,
  });

  expect(threadProcessFs).toHaveBeenCalledWith(0x1234);
  expect(threadResumeFs).toHaveBeenCalledTimes(1);
  expect(syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  expect(markRuntimeFsDirty).toHaveBeenCalledWith(handle);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
});
