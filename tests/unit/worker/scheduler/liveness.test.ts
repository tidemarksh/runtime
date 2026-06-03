import { afterEach, beforeEach, expect, test, vi } from "vite-plus/test";
import {
  createRuntimeStateBridge,
  readPublishedRuntimeBridgeEpoch,
} from "../../../../src/bridge/runtime-state.js";

vi.mock("../../../../src/worker/pool.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../../../src/worker/pool.js")>();
  return {
    ...actual,
    kernelStackSlotForWorker: () => 2,
  };
});

import { createWorkerScheduler } from "../../../../src/worker/scheduler.js";
import { createTransferDebugStats, createWorkerMessageDebugStats } from "../../../../src/worker/types.js";
import type { ProcessHandle, ThreadWorkerState } from "../../../../src/worker/types.js";

function makeKernel() {
  const threadResumeWait4 = vi.fn();
  let monotonicTicks = 1n;
  return {
    threadResumeWait4,
    exports: {
      abi_status_ok: () => 0,
      abi_status_futex_wait: () => 20,
      abi_status_sleep: () => 26,
      abi_status_wait4: () => 31,
      abi_status_epoll_wait: () => 7,
      abi_status_vfork_wait: () => 8,
      abi_status_execve_spawn: () => 9,
      abi_status_clone_spawn: () => 10,
      abi_status_fault: () => 3,
      abi_status_pipe_wait: () => 6,
      abi_status_connect: () => 28,
      abi_status_socket_recv: () => 30,
      abi_status_socket_send: () => 29,
      abi_status_flock_wait: () => 32,
      abi_status_fs: () => 33,
      abi_max_threads: () => 64,
      thread_resume_wait4: threadResumeWait4,
      thread_resume_blocked_syscall: vi.fn(),
      thread_resume_sleep: vi.fn(),
      thread_resume_epoll_wait_timeout: vi.fn(),
      thread_resume_poll_wait_timeout: vi.fn(),
      thread_resume_socket_wait_timeout: vi.fn(),
      thread_status: () => 0,
      thread_exit_code: () => 0,
      thread_term_signal: () => 0,
      monotonic_ticks_current: () => monotonicTicks,
      sync_monotonic_ticks_floor: vi.fn((ticks: bigint | number) => {
        const nextTicks = BigInt(ticks);
        if (nextTicks > monotonicTicks) {
          monotonicTicks = nextTicks;
        }
      }),
      thread_sleep_deadline_ticks: vi.fn(() => monotonicTicks),
      thread_sleep_seconds: () => 0n,
      thread_sleep_nanoseconds: () => 0n,
      thread_epoll_wait_timeout: () => 0n,
      thread_poll_wait_timeout: () => -1n,
      thread_poll_wait_ready_count: () => 0,
      thread_poll_wait_has_socket: () => 0,
      thread_poll_wait_first_socket_revents: () => 0,
      thread_socket_wait_timeout: () => -1n,
      thread_process_thread_count: () => 1,
      thread_process_ptr: () => 0x2000,
      thread_pc: () => 0x1000n,
      thread_sp: () => 0x8000n,
      thread_s11: () => 0x9000n,
      process_current_pid: () => 1,
      process_record_child_exit_for_parent: vi.fn(),
      abi_max_fds: () => 64,
      abi_max_pipes: () => 64,
      abi_fd_type_pipe: () => 2,
      kernel_heap_base: () => 0,
      kernel_heap_ptr: () => 0,
      kernel_ofd_table_ptr: () => 0,
      kernel_ofd_table_bytes: () => 0,
      kernel_ofd_fd_type: () => 0,
      kernel_ofd_ino: () => 0,
      kernel_memfs_ptr: () => 0,
      kernel_memfs_header_bytes: () => 0,
      kernel_memfs_inodes_bytes: () => 0,
      kernel_memfs_first_child_bytes: () => 0,
      kernel_memfs_dir_entries_bytes: () => 0,
      kernel_memfs_data_bytes: () => 0,
      kernel_mem_state_ptr: () => 0,
      kernel_epoll_table_ptr: () => 0,
      kernel_epoll_table_bytes: () => 0,
      kernel_epoll_instance_bytes: () => 0,
      kernel_socket_table_ptr: () => 0,
      kernel_socket_table_bytes: () => 0,
      kernel_socket_used: () => 0,
      kernel_pipe_base: () => 0,
      kernel_pipe_region_bytes: () => 0,
      kernel_pipe_used: () => 0n,
      kernel_net_buf_base: () => 0,
      kernel_net_buf_pool_bytes: () => 0,
      kernel_net_buf_used: () => 0n,
      kernel_sig_slots_ptr: () => 0,
      kernel_sig_slots_bytes: () => 0,
      kernel_signal_action_table_bytes: () => 0,
      kernel_process_pending_ptr: () => 0,
      kernel_process_pending_bytes: () => 0,
      kernel_process_runtime_state_ptr: () => 0,
      kernel_process_runtime_state_bytes: () => 0,
      kernel_time_state_ptr: () => 0,
      kernel_time_state_bytes: () => 0,
      kernel_attach_runtime_state: () => {},
      process_fd_ofd_id: () => 0,
    } as unknown as ProcessHandle["kernel"],
  };
}

function makeThreadState(vmPtr: number): ThreadWorkerState {
  return {
    vmPtr,
    ownerPid: 1,
    generation: 0,
    reinitCount: 0,
    stepCount: 0,
    totalStepBudget: 0,
    totalStepWallMs: 0,
    maxStepWallMs: 0,
    lastStepDurationMs: null,
    statusCount: 0,
    statusCounts: {},
    lastStatus: null,
    lastStepAtMs: null,
    lastStatusAtMs: null,
    lastStepProcessPtr: null,
    lastStepPc: null,
    lastStepSp: null,
    lastStepS11: null,
    hotStepPcs: {},
    lastStatusPc: null,
    lastStatusSp: null,
    lastStatusS11: null,
    hotStatusPcs: {},
    initialized: true,
    waitingInWorkerStatus: null,
    ownerAsyncStatus: null,
    resumeBlockedOnNextStep: false,
    pendingPipeSlotsForNextStep: [],
    pendingGuestMemoryWrites: [],
    resumeWorkersOnReady: [],
  };
}

function makeHandle(kernel: ProcessHandle["kernel"], worker: Worker): ProcessHandle {
  return {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0x1234,
    guestMemoryPolicyLimitBytes: 0,
    maxMemoryPages: 0,
    forkMaxMemoryPages: 0,
    guestBase: 0,
    worker,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel,
    kernelState: {
      heapBase: 0,
      heapPtr: 0,
      ofdTablePtr: 0,
      memfsPtr: 0,
      memfsHeaderBytes: 0,
      memfsInodesBytes: 0,
      memfsFirstChildBytes: 0,
      memfsDirEntriesBytes: 0,
      memfsDataBytes: 0,
      memStatePtr: 0,
      epollTablePtr: 0,
      socketTablePtr: 0,
      socketUsed: 0,
      pipeBase: 0,
      pipeUsed: 0n,
      netBufBase: 0,
      netBufUsed: 0n,
      sigSlotsPtr: 0,
      processPendingPtr: 0,
      processRuntimeStatePtr: 0,
      timeStatePtr: 0,
    },
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
      ofdSlotSnapshots: new Map(),
      forkSharedSyncPromise: null,
    },
    stdinQueue: [],
    stdinFlushPromise: null,
    outputBacklog: [],
    debugCommand: "/bin/sh",
    lastDebugMessage: undefined,
    runtimeBridge: createRuntimeStateBridge(),
    sabPageCache: null,
    kernelWorkerRequest: undefined,
    workers: new Set([worker]),
    readyWorkers: [],
    runningWorker: null,
    blockedWorkers: new Map(),
    pendingChildExitRecords: 0,
    pendingChildSpawnRecords: 1,
    childExitSyncRecords: new Map(),
    pipeSyncPromise: null,
    pipeSyncQueued: false,
    pipeWaiterWakeQueued: false,
    pendingPipeSlotSnapshots: [],
    epollSyncPromise: null,
    epollSyncQueued: false,
    epollWaiterWakeQueued: false,
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    socketSyncPromise: null,
    socketSyncQueued: false,
    socketWaiterWakeQueued: false,
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    networkRecvPayloadWatermarks: new Map(),
    transferDebugStats: createTransferDebugStats(),
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    vforkSnapshot: null,
    vforkWorker: null,
    exitInProgress: false,
    onExit: undefined,
    onError: undefined,
    onStdout: undefined,
    workerDebugLog: [],
    guestWriteWatchRegions: [],
  };
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

test("scheduler uses the Node step budget when Node process globals are present", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, worker);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.enqueueWorker(handle, worker);

  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 5_000_000,
  }));
});

test("blocked FS retry pumps after clearing owner async status", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_fs();
  kernel.thread_status = () => status;
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  const resumeFsViaKernelWorkerOutcome = vi.fn(async () => {
    expect(thread.ownerAsyncStatus).toBe(kernel.abi_status_fs());
    status = kernel.abi_status_ok();
    return { kind: "completed" as const };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeFsViaKernelWorkerOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, kernel.abi_status_fs(), 0x1234);
  await vi.advanceTimersByTimeAsync(1);
  await Promise.resolve();

  expect(resumeFsViaKernelWorkerOutcome).toHaveBeenCalledWith(handle, worker, 0x1234);
  expect(thread.ownerAsyncStatus).toBeNull();
  expect(handle.blockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("pumpProcess reconciles a running FS status through the owner scheduler", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234);
  thread.runningGeneration = thread.generation;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_fs();
  kernel.thread_status = () => status;
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.runningWorker = worker;
  const resumeFsViaKernelWorkerOutcome = vi.fn(async () => {
    expect(thread.ownerAsyncStatus).toBe(kernel.abi_status_fs());
    status = kernel.abi_status_ok();
    return { kind: "completed" as const };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeFsViaKernelWorkerOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);
  expect(handle.runningWorker).toBeNull();
  expect(thread.runningGeneration).toBeNull();
  expect(handle.blockedWorkers.has(worker)).toBe(true);

  await vi.advanceTimersByTimeAsync(1);
  await Promise.resolve();

  expect(resumeFsViaKernelWorkerOutcome).toHaveBeenCalledWith(handle, worker, 0x1234);
  expect(thread.ownerAsyncStatus).toBeNull();
  expect(handle.blockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("wait4 retry stays blocked while child spawn is still in flight", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  let status = kernel.abi_status_wait4();
  kernel.thread_status = () => status;
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, worker);

  const lifecycleEvents: string[] = [];
  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      lifecycleEvents.push(`fail:${error.message}`);
      throw error;
    },
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);
  expect(threadResumeWait4).not.toHaveBeenCalled();

  handle.pendingChildSpawnRecords = 0;
  const epochBeforeKick = readPublishedRuntimeBridgeEpoch(handle.runtimeBridge);
  scheduler.kickBlockedWorkers(handle);
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(readPublishedRuntimeBridgeEpoch(handle.runtimeBridge)).toBeGreaterThan(epochBeforeKick);
});

test("wait4 retry stays blocked while a child remains active even with kernel-worker support", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_wait4();
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => ({
    type: "wait4-resumed",
    requestId: 0,
    kernelState: handle.kernelState,
  }));

  const resumeWait4ViaKernelWorker = vi.fn(async () => {});
  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => true,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker,
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);

  expect(resumeWait4ViaKernelWorker).not.toHaveBeenCalled();
  expect(threadResumeWait4).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(worker)).toBe(true);
});

test("pipe retry pumps active child processes before remaining blocked", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_pipe_wait();
  kernel.thread_a7 = () => 63n;
  kernel.thread_a0 = () => 4n;
  const handle = makeHandle(kernel, worker);
  const pumpActiveChildProcesses = vi.fn(() => true);
  const resumeBlockedSyscallLocally = vi.fn(async () => {});

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => true,
    pumpActiveChildProcesses,
    resumeBlockedSyscallLocally,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  await vi.advanceTimersByTimeAsync(2);

  expect(pumpActiveChildProcesses).toHaveBeenCalledWith(1);
});

test("finite pipe poll with active children waits until timeout while child runs", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_pipe_wait();
  const resumePollTimeout = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  kernel.abi_sys_ppoll = () => 73;
  kernel.thread_a7 = () => 73n;
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => status;
  kernel.thread_poll_wait_timeout = () => 500n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_resume_poll_wait_timeout = resumePollTimeout;
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  const pumpActiveChildProcesses = vi.fn(() => true);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => true,
    pumpActiveChildProcesses,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  await vi.advanceTimersByTimeAsync(35);

  expect(pumpActiveChildProcesses).not.toHaveBeenCalled();
  expect(resumePollTimeout).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(500);

  expect(pumpActiveChildProcesses).toHaveBeenCalled();
  expect(resumePollTimeout).toHaveBeenCalledWith(0x1234);
});

test("pipe pselect retry applies child exit records before passive parking", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.abi_sys_pselect6 = () => 72;
  kernel.thread_a7 = () => 72n;
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => kernel.abi_status_pipe_wait();
  kernel.thread_poll_wait_timeout = () => -1n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_pipe_read_eof_sync_pending = () => 0;
  kernel.thread_poll_hup_sync_pending = () => 0;
  kernel.process_record_child_exit_for_parent = vi.fn();
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.childExitSyncRecords.set(7, 0);
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("scheduler retry should leave pipe wait resume to local helper");
  });
  const lifecycleEvents: string[] = [];
  const resumeBlockedSyscallLocally = vi.fn(async () => {});

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => true,
    resumeBlockedSyscallLocally,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  scheduler.kickBlockedWorkers(handle);
  await Promise.resolve();

  expect(lifecycleEvents).toContain("pipe-retry-apply-child-records:1:7/0");
  expect(lifecycleEvents).toContain("wait4-apply-records:1:7/0");
  expect(lifecycleEvents.some((event) => event.startsWith("pipe-retry-skip:1:active-child"))).toBe(false);
  expect(kernel.process_record_child_exit_for_parent).toHaveBeenCalledWith(1, 7, 0);
  expect(resumeBlockedSyscallLocally).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_pipe_wait(),
    false,
  );
  expect(handle.blockedWorkers.get(worker)?.pipeChildExitResumeKey).toContain("7/0:sys=72");
});

test("pipe pselect retry resumes after pipe sync when no writer remains", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.abi_sys_pselect6 = () => 72;
  kernel.abi_fd_type_pipe = () => 6;
  kernel.thread_a7 = () => 72n;
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => kernel.abi_status_pipe_wait();
  kernel.thread_poll_wait_timeout = () => -1n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_poll_wait_first_pipe_revents = () => 4 | (0x001 << 8);
  kernel.thread_pipe_read_eof_sync_pending = () => 0;
  kernel.thread_poll_hup_sync_pending = () => 0;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 4 ? 10 : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === 10 ? kernel.abi_fd_type_pipe() : 0;
  kernel.kernel_ofd_ino = (ofdId: number) => ofdId === 10 ? 2 : 0;
  const handle = makeHandle(kernel, worker);
  handle.memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("scheduler retry should leave pipe wait resume to local helper");
  });
  const resumeBlockedSyscallLocally = vi.fn(async () => {});
  const lifecycleEvents: string[] = [];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  const blocked = handle.blockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  blocked!.pipeSyncedBeforePassive = true;
  scheduler.kickBlockedWorkers(handle);
  await Promise.resolve();

  expect(lifecycleEvents.some((event) => event.startsWith("pipe-retry:1:a7=72"))).toBe(true);
  expect(resumeBlockedSyscallLocally).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_pipe_wait(),
    false,
  );
});

test("kernel-worker blocked syscall retry leaves rpc ownership to local resume helper", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_pipe_wait();
  kernel.thread_a7 = () => 999n;
  kernel.thread_a0 = () => 4n;
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("direct kernel-worker request should not be issued by scheduler retry");
  });
  const resumeBlockedSyscallLocally = vi.fn(async () => {});

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      lifecycleEvents.push(`fail:${error.message}`);
      throw error;
    },
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  const blocked = handle.blockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  if (blocked!.retryTimer !== null) {
    clearTimeout(blocked!.retryTimer);
    blocked!.retryTimer = null;
  }
  blocked!.pipeSyncedBeforePassive = true;
  scheduler.kickBlockedWorkers(handle);

  expect(resumeBlockedSyscallLocally).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_pipe_wait(),
    false,
  );
  expect(handle.blockedWorkers.get(worker)?.rpcPending).toBe(false);
  expect(handle.blockedWorkers.get(worker)?.rpcGeneration).toBe(0);
});

test("socket poll is resumed immediately after network pump changes state", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_pipe_wait();
  kernel.thread_status = () => status;
  kernel.thread_a7 = () => 999n;
  kernel.thread_a0 = () => 3n;
  kernel.thread_poll_wait_timeout = () => -1n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 1;
  kernel.thread_resume_blocked_syscall = vi.fn((vmPtr: number) => {
    expect(vmPtr).toBe(0x1234);
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.networkConnections.set(3, {} as never);
  const pumpNetworkConnections = vi.fn(async () => true);
  const failProcess = vi.fn();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess,
    hasActiveChildProcess: () => false,
    pumpNetworkConnections,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  await vi.advanceTimersByTimeAsync(2);

  expect(pumpNetworkConnections).toHaveBeenCalledWith(handle, 0x1234);
  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(0x1234);
  expect(handle.blockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({ type: "step" }));
  expect(failProcess).not.toHaveBeenCalled();
});

test("finite pipe poll timeout resumes locally before kernel-worker blocked syscall RPC", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_pipe_wait();
  const resumePollTimeout = vi.fn((vmPtr: number) => {
    expect(vmPtr).toBe(0x1234);
    status = kernel.abi_status_ok();
  });
  kernel.abi_sys_ppoll = () => 73;
  kernel.thread_a7 = () => 73n;
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => status;
  kernel.thread_poll_wait_timeout = () => 100n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_resume_poll_wait_timeout = resumePollTimeout;
  kernel.thread_resume_blocked_syscall = vi.fn();
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("kernel-worker RPC should not run for an expired finite pipe poll timeout");
  });
  const resumeBlockedSyscallLocally = vi.fn(async () => {});

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  const blocked = handle.blockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  blocked!.timeoutExpired = true;
  blocked!.pipeSyncedBeforePassive = true;
  scheduler.kickBlockedWorkers(handle);

  expect(resumePollTimeout).toHaveBeenCalledWith(0x1234);
  expect(kernel.sync_monotonic_ticks_floor).toHaveBeenCalled();
  expect(resumeBlockedSyscallLocally).not.toHaveBeenCalled();
  expect(handle.kernelWorkerRequest).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("finite pipe poll timeout uses saved blocked syscall when current a7 moved on", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_pipe_wait();
  const resumePollTimeout = vi.fn((vmPtr: number) => {
    expect(vmPtr).toBe(0x1234);
    const view = new DataView(handle.memory.buffer);
    view.setBigUint64(0x20, 0x08n, true);
    view.setBigUint64(0x28, 0x10n, true);
    view.setBigUint64(0x30, 0n, true);
    status = kernel.abi_status_ok();
  });
  kernel.abi_sys_pselect6 = () => 72;
  kernel.abi_max_fds = () => 64;
  kernel.thread_a7 = () => 48n;
  kernel.thread_blocked_syscall_nr = () => 72n;
  kernel.thread_blocked_syscall_arg = (_vmPtr: number, index: number) => {
    switch (index) {
      case 0:
        return 16n;
      case 1:
        return 0x20n;
      case 2:
        return 0x28n;
      case 3:
        return 0x30n;
      default:
        return 0n;
    }
  };
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => status;
  kernel.thread_poll_wait_timeout = () => 25n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_resume_poll_wait_timeout = resumePollTimeout;
  kernel.thread_resume_blocked_syscall = vi.fn();
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  await vi.advanceTimersByTimeAsync(1);
  expect(resumePollTimeout).not.toHaveBeenCalled();
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(25);

  expect(resumePollTimeout).toHaveBeenCalledWith(0x1234);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    guestMemoryWrites: [
      { guestPtr: 0x20, bytes: new Uint8Array([0x08, 0, 0, 0, 0, 0, 0, 0]) },
      { guestPtr: 0x28, bytes: new Uint8Array([0x10, 0, 0, 0, 0, 0, 0, 0]) },
      { guestPtr: 0x30, bytes: new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0]) },
    ],
  }));
});

test("expired finite pipe poll syncs pipe state before local timeout resume", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_pipe_wait();
  const order: string[] = [];
  const resumePollTimeout = vi.fn(() => {
    order.push("resume");
    status = kernel.abi_status_ok();
  });
  kernel.abi_sys_ppoll = () => 73;
  kernel.thread_a7 = () => 73n;
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => status;
  kernel.thread_poll_wait_timeout = () => 100n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_resume_poll_wait_timeout = resumePollTimeout;
  kernel.abi_max_fds = () => 64;
  kernel.abi_max_pipes = () => 64;
  kernel.abi_fd_type_pipe = () => 6;
  kernel.abi_fd_type_pty_master = () => 7;
  kernel.abi_fd_type_pty_slave = () => 8;
  kernel.process_fd_ofd_id = () => 0;
  kernel.process_fd_flags = () => 0;
  kernel.thread_process_thread_count = () => 1;
  kernel.process_thread_vm_ptr = (_processVmPtr, slot) => slot === 0 ? 0x1234 : 0;
  kernel.kernel_ofd_table_bytes = () => 64 * 32;
  kernel.kernel_ofd_fd_type = () => 0;
  kernel.kernel_ofd_ino = () => 0;
  kernel.kernel_ofd_host_fd = () => 0;
  kernel.kernel_pipe_region_bytes = () => 64 * (32 + 65_536);
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async (message) => {
    order.push("sync");
    expect(message.type).toBe("sync-pipe-state");
    return {
      type: "pipe-state-synced",
      kernelState: handle.kernelState,
      pipeSlots: [],
    };
  });

  const lifecycleEvents: string[] = [];
  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {
      throw new Error("kernel-worker blocked syscall RPC should not run for finite pipe poll timeout");
    },
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  const blocked = handle.blockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  if (blocked!.retryTimer !== null) {
    clearTimeout(blocked!.retryTimer);
    blocked!.retryTimer = null;
  }
  blocked!.timeoutExpired = true;
  scheduler.kickBlockedWorkers(handle);
  for (let i = 0; i < 10; i += 1) {
    await Promise.resolve();
  }
  await vi.runAllTimersAsync();

  expect(lifecycleEvents.some((event) => event.startsWith("pipe-sync-start:1:wake=0"))).toBe(true);
  expect(order).toEqual([]);
  expect(resumePollTimeout).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(worker)).toBe(true);
});

test("expired finite pipe poll with active child resumes locally without pipe sync", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_pipe_wait();
  const order: string[] = [];
  const resumePollTimeout = vi.fn(() => {
    order.push("resume");
    status = kernel.abi_status_ok();
  });
  kernel.abi_sys_ppoll = () => 73;
  kernel.thread_a7 = () => 73n;
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => status;
  kernel.thread_poll_wait_timeout = () => 100n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_resume_poll_wait_timeout = resumePollTimeout;
  kernel.abi_max_fds = () => 64;
  kernel.abi_max_pipes = () => 64;
  kernel.abi_fd_type_pipe = () => 6;
  kernel.abi_fd_type_pty_master = () => 7;
  kernel.abi_fd_type_pty_slave = () => 8;
  kernel.process_fd_ofd_id = () => 0;
  kernel.process_fd_flags = () => 0;
  kernel.process_thread_vm_ptr = () => 0;
  kernel.kernel_ofd_table_bytes = () => 64 * 32;
  kernel.kernel_ofd_fd_type = () => 0;
  kernel.kernel_ofd_ino = () => 0;
  kernel.kernel_ofd_host_fd = () => 0;
  kernel.kernel_pipe_region_bytes = () => 64 * (32 + 65_536);
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("active child finite pipe poll timeout should not sync pipe state");
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => true,
    resumeBlockedSyscallLocally: async () => {
      throw new Error("kernel-worker blocked syscall RPC should not run for finite pipe poll timeout");
    },
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  const blocked = handle.blockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  if (blocked!.retryTimer !== null) {
    clearTimeout(blocked!.retryTimer);
    blocked!.retryTimer = null;
  }
  blocked!.timeoutExpired = true;
  scheduler.kickBlockedWorkers(handle);
  await Promise.resolve();

  expect(order).toEqual(["resume"]);
  expect(resumePollTimeout).toHaveBeenCalledWith(0x1234);
  expect(handle.kernelWorkerRequest).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(worker)).toBe(false);
});

test("wait4 retry resumes after a fork-shared sync gate settles", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  let status = kernel.abi_status_wait4();
  kernel.thread_status = () => status;
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  let resolveSync!: () => void;
  const forkSharedSync = new Promise<void>((resolve) => {
    resolveSync = resolve;
  });
  handle.forkSharedSyncPromise = forkSharedSync;
  handle.kernelShare.forkSharedSyncPromise = forkSharedSync;
  const lifecycleEvents: string[] = [];
  let activeChild = true;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => activeChild,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);

  expect(lifecycleEvents.some((event) => event.startsWith("wait4-retry-skip:1:fork-shared-sync"))).toBe(true);
  expect(threadResumeWait4).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(worker)).toBe(true);

  activeChild = false;
  resolveSync();
  await Promise.resolve();
  await vi.advanceTimersByTimeAsync(0);

  expect(handle.forkSharedSyncPromise).toBeNull();
  expect(handle.kernelShare.forkSharedSyncPromise).toBeNull();
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
  expect(handle.blockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("idle ok worker resumes after a fork-shared sync gate settles", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  let resolveSync!: () => void;
  const forkSharedSync = new Promise<void>((resolve) => {
    resolveSync = resolve;
  });
  handle.forkSharedSyncPromise = forkSharedSync;
  handle.kernelShare.forkSharedSyncPromise = forkSharedSync;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);
  expect(worker.postMessage).not.toHaveBeenCalled();

  resolveSync();
  await Promise.resolve();
  await Promise.resolve();

  expect(handle.forkSharedSyncPromise).toBeNull();
  expect(handle.kernelShare.forkSharedSyncPromise).toBeNull();
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("wait4 retry consumes a local child exit record while fork-shared sync is pending", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  let status = kernel.abi_status_wait4();
  kernel.thread_status = () => status;
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
    status = kernel.abi_status_ok();
    return 7n;
  });
  kernel.process_record_child_exit_for_parent = vi.fn();
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.childExitSyncRecords.set(7, 0);
  const forkSharedSync = new Promise<void>(() => {});
  handle.forkSharedSyncPromise = forkSharedSync;
  handle.kernelShare.forkSharedSyncPromise = forkSharedSync;
  const lifecycleEvents: string[] = [];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => true,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);

  expect(lifecycleEvents).not.toContain("wait4-retry-skip:1:fork-shared-sync");
  expect(kernel.process_record_child_exit_for_parent).toHaveBeenCalledWith(1, 7, 0);
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
  expect(handle.childExitSyncRecords.has(7)).toBe(false);
  expect(handle.blockedWorkers.has(worker)).toBe(false);
  expect(handle.readyWorkers).toContain(worker);
});

test("wait4 retry reconciles stale blocked state before issuing a kernel-worker resume", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  let status = kernel.abi_status_ok();
  kernel.thread_status = () => status;
  const handle = makeHandle(kernel, worker);
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("kernel-worker resume should not run when local status is already OK");
  });

  const resumeWait4ViaKernelWorker = vi.fn(async () => {});
  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker,
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  scheduler.kickBlockedWorkers(handle);

  expect(resumeWait4ViaKernelWorker).not.toHaveBeenCalled();
  expect(threadResumeWait4).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("sleep-blocked worker advances guest time to the earliest deadline and resumes immediately", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_sleep();
  kernel.thread_status = () => status;
  kernel.thread_resume_sleep = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  kernel.thread_sleep_deadline_ticks = () => 1_000n;
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_sleep(), 0x1234);
  scheduler.kickBlockedWorkers(handle);

  expect(kernel.sync_monotonic_ticks_floor).toHaveBeenCalledWith(1_000n);
  expect(kernel.thread_resume_sleep).toHaveBeenCalledWith(0x1234);
  expect(handle.blockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("sleep-blocked worker retries at its deadline when other workers are runnable", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  threadWorkerStates.set(readyWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? kernel.abi_status_sleep()
    : kernel.abi_status_ok();
  kernel.thread_sleep_deadline_ticks = () => 1_000_001n;
  const handle = makeHandle(kernel, blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.workers.add(readyWorker);
  handle.readyWorkers.push(readyWorker);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_sleep(), 0x1234);
  scheduler.kickBlockedWorkers(handle);

  expect(kernel.sync_monotonic_ticks_floor).not.toHaveBeenCalled();
  expect(kernel.thread_resume_sleep).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(blockedWorker)).toBe(true);

  await vi.advanceTimersByTimeAsync(9);
  expect(kernel.thread_resume_sleep).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(1);

  expect(kernel.sync_monotonic_ticks_floor).toHaveBeenCalledWith(1_000_001n);
  expect(kernel.thread_resume_sleep).toHaveBeenCalledWith(0x1234);
  expect(handle.blockedWorkers.has(blockedWorker)).toBe(false);
  expect(handle.readyWorkers).toContain(blockedWorker);
});

test("blocked syscall retry is rescheduled after a running-worker skip", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  threadWorkerStates.set(runningWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_futex_wait();
  const resumeBlockedSyscall = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234 ? status : kernel.abi_status_ok();
  kernel.thread_resume_blocked_syscall = resumeBlockedSyscall;
  const handle = makeHandle(kernel, blockedWorker);
  handle.workers.add(runningWorker);
  handle.runningWorker = runningWorker;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_futex_wait(), 0x1234);
  await vi.advanceTimersByTimeAsync(1);
  expect(resumeBlockedSyscall).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(blockedWorker)).toBe(true);

  handle.runningWorker = null;
  await vi.advanceTimersByTimeAsync(1);

  expect(resumeBlockedSyscall).toHaveBeenCalledWith(0x1234);
  expect(handle.blockedWorkers.has(blockedWorker)).toBe(false);
  expect(blockedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("wait4 retry with a local child exit record wakes the running worker before rescheduling", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  threadWorkerStates.set(runningWorker, makeThreadState(0x5678));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? kernel.abi_status_wait4()
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.childExitSyncRecords.set(7, 0);
  handle.workers.add(runningWorker);
  handle.runningWorker = runningWorker;
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, 0);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);

  expect(threadResumeWait4).not.toHaveBeenCalled();
  expect(Atomics.load(notify, 0)).toBeGreaterThan(notifyBefore);
  expect(handle.blockedWorkers.has(blockedWorker)).toBe(true);
});

test("wait4 retry clears a stale running worker that already published a blocked status", () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  threadWorkerStates.set(runningWorker, makeThreadState(0x5678));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  kernel.thread_status = (vmPtr: number) => {
    if (vmPtr === 0x1234) {
      return kernel.abi_status_wait4();
    }
    if (vmPtr === 0x5678) {
      return kernel.abi_status_sleep();
    }
    return kernel.abi_status_ok();
  };
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
  });
  const handle = makeHandle(kernel, blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.workers.add(runningWorker);
  handle.runningWorker = runningWorker;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_wait4(), 0x1234);
  scheduler.kickBlockedWorkers(handle);

  expect(handle.runningWorker).toBeNull();
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
});

test("pumpProcess blocks a running sleeper before dispatching a ready sibling", () => {
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, makeThreadState(0x5678));
  threadWorkerStates.set(readyWorker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x5678
    ? kernel.abi_status_sleep()
    : kernel.abi_status_ok();
  kernel.thread_sleep_deadline_ticks = () => 10_000n;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.blockedWorkers.get(runningWorker)?.status).toBe(kernel.abi_status_sleep());
  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess clears a stale sleep-resumed running worker before dispatching a ready sibling", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x5678);
  const readyThread = makeThreadState(0x1234);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.lastStatusAtMs = 7_000;
  runningThread.lastStepAtMs = 7_500;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_sleep();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess clears a stale ok running worker after FS before dispatching a ready sibling", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x5678);
  const readyThread = makeThreadState(0x1234);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.lastStatusAtMs = 7_000;
  runningThread.lastStepAtMs = 7_500;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_fs();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess clears an answered stale ok running worker without active-step export", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x5678);
  const readyThread = makeThreadState(0x1234);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 161;
  runningThread.statusCount = 161;
  runningThread.lastStepAtMs = 7_000;
  runningThread.lastStatusAtMs = 7_100;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_ok();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess clears a stale ok running worker that has not returned status", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x66c42c8);
  const readyThread = makeThreadState(0x64a6e20);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 1;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = null;
  runningThread.lastStatus = null;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess retries stale running repair after an active step clears", async () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x66c42c8);
  const readyThread = makeThreadState(0x64a6e20);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 1;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = null;
  runningThread.lastStatus = null;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  let activeStepVmPtr = runningThread.vmPtr;
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => activeStepVmPtr;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);
  await vi.advanceTimersByTimeAsync(2_000);

  expect(handle.runningWorker).toBe(runningWorker);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();

  activeStepVmPtr = 0;
  await vi.advanceTimersByTimeAsync(2_000);

  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess schedules stale repair when dispatch leaves a ready sibling queued", async () => {
  vi.setSystemTime(new Date(10_000));
  const firstWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const secondWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const firstThread = makeThreadState(0x66c42c8);
  const secondThread = makeThreadState(0x64a6e20);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(firstWorker, firstThread);
  threadWorkerStates.set(secondWorker, secondThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, firstWorker);
  handle.workers.add(secondWorker);
  handle.readyWorkers = [firstWorker, secondWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(firstWorker);
  expect(firstThread.runningGeneration).toBe(firstThread.generation);
  expect(handle.readyWorkers).toEqual([secondWorker]);

  vi.setSystemTime(new Date(12_000));
  await vi.advanceTimersByTimeAsync(2_000);

  expect(firstThread.runningGeneration).toBeNull();
  expect(handle.runningWorker).toBe(secondWorker);
  expect(firstWorker.postMessage).toHaveBeenCalledTimes(1);
  expect(secondWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("pumpProcess clears a stale running worker when the unanswered step shares a status millisecond", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x66c42c8);
  const readyThread = makeThreadState(0x64a6e20);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 3;
  runningThread.statusCount = 2;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = 7_500;
  runningThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess clears a stale unanswered ok running worker without active-step export when status covers the step time", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x6648420);
  const readyThread = makeThreadState(0x9ba7260);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 162;
  runningThread.statusCount = 161;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = 7_500;
  runningThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess keeps an unanswered ok running worker without active-step export when the step is newer than status", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x6648420);
  const readyThread = makeThreadState(0x9ba7260);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 162;
  runningThread.statusCount = 161;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = 7_000;
  runningThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(handle.runningWorker).toBe(runningWorker);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess clears a stale answered ok running worker without active-step export when the step is newer than status", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x9ba7260);
  const readyThread = makeThreadState(0x9dc4708);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 9;
  runningThread.statusCount = 11;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = 7_000;
  runningThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess clears a running worker whose kernel process pointer disappeared", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x581f470);
  const readyThread = makeThreadState(0x6645d48);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 69;
  runningThread.statusCount = 68;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = 7_000;
  runningThread.lastStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.thread_process_ptr = (vmPtr: number) => vmPtr === runningThread.vmPtr ? 0 : 0x2000;
  delete kernel.debug_thread_step_vm_ptr;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess waits for socket status publication before dispatching a ready sibling", () => {
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, makeThreadState(0x5678));
  threadWorkerStates.set(readyWorker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x5678
    ? kernel.abi_status_socket_recv()
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(runningWorker);
  expect(handle.blockedWorkers.has(runningWorker)).toBe(false);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
});

test("pruning a stale running worker clears the running slot", () => {
  const staleWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(staleWorker, makeThreadState(0x1234));
  threadWorkerStates.set(anchorWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  const handle = makeHandle(kernel, staleWorker);
  handle.workers.add(anchorWorker);
  handle.runningWorker = staleWorker;
  handle.pendingChildSpawnRecords = 0;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pruneDetachedProcessWorkers(handle, 0x5678, null);

  expect(handle.runningWorker).toBeNull();
  expect(handle.workers.has(staleWorker)).toBe(false);
  expect(handle.workers.has(anchorWorker)).toBe(true);
});

test("pruning preserves a detached worker in vfork wait", () => {
  const vforkWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(vforkWorker, makeThreadState(0x1234));
  threadWorkerStates.set(anchorWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234 ? kernel.abi_status_vfork_wait() : kernel.abi_status_ok();
  const handle = makeHandle(kernel, vforkWorker);
  handle.workers.add(anchorWorker);
  handle.runningWorker = vforkWorker;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pruneDetachedProcessWorkers(handle, 0x5678, null);

  expect(handle.runningWorker).toBe(vforkWorker);
  expect(handle.workers.has(vforkWorker)).toBe(true);
  expect(handle.workers.has(anchorWorker)).toBe(true);
});

test("pruning preserves a detached worker during execve spawn handoff", () => {
  const execveWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(execveWorker, makeThreadState(0x1234));
  threadWorkerStates.set(anchorWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234 ? kernel.abi_status_execve_spawn() : kernel.abi_status_ok();
  const handle = makeHandle(kernel, execveWorker);
  handle.workers.add(anchorWorker);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pruneDetachedProcessWorkers(handle, 0x5678, null);

  expect(handle.workers.has(execveWorker)).toBe(true);
  expect(handle.workers.has(anchorWorker)).toBe(true);
});

test("pruning is deferred while child spawn records are still pending", () => {
  const staleWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(staleWorker, makeThreadState(0x1234));
  threadWorkerStates.set(anchorWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  const handle = makeHandle(kernel, staleWorker);
  handle.workers.add(anchorWorker);
  handle.pendingChildSpawnRecords = 1;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pruneDetachedProcessWorkers(handle, 0x5678, null);

  expect(handle.workers.has(staleWorker)).toBe(true);
  expect(handle.workers.has(anchorWorker)).toBe(true);
});

test("enqueue clears a dangling running worker before stepping the next ready worker", () => {
  const staleWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(readyWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, readyWorker);
  handle.workers.add(staleWorker);
  handle.runningWorker = staleWorker;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  handle.workers.delete(staleWorker);
  scheduler.enqueueWorker(handle, readyWorker);

  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("step publishes the full process fd table to the selected worker", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  kernel.abi_max_fds = () => 6;
  kernel.process_fd_ofd_id = (vmPtr: number, fd: number) => {
    if (vmPtr !== 0x5678) {
      return fd === 4 ? 99 : 0;
    }
    if (fd === 4) {
      return 7;
    }
    if (fd === 5) {
      return 8;
    }
    return 0;
  };
  kernel.process_fd_flags = (vmPtr: number, fd: number) => {
    if (vmPtr !== 0x5678) {
      return fd === 4 ? 2 : 0;
    }
    return fd === 4 ? 1 : 0;
  };
  const handle = makeHandle(kernel, worker);
  handle.readyWorkers = [worker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    fdEntries: [
      { fd: 0, ofdId: 0, fdFlags: 0 },
      { fd: 1, ofdId: 0, fdFlags: 0 },
      { fd: 2, ofdId: 0, fdFlags: 0 },
      { fd: 3, ofdId: 0, fdFlags: 0 },
      { fd: 4, ofdId: 7, fdFlags: 1 },
      { fd: 5, ofdId: 8, fdFlags: 0 },
    ],
  }));
});

test("pumpProcess prunes ready workers that are still waiting inside the worker", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runnableWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x1234);
  const runnableThread = makeThreadState(0x5678);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(runnableWorker, runnableThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? kernel.abi_status_futex_wait()
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, waitingWorker);
  handle.workers.add(runnableWorker);
  handle.readyWorkers = [waitingWorker, runnableWorker];
  waitingThread.waitingInWorkerStatus = kernel.abi_status_futex_wait();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(runnableWorker);
  expect(waitingWorker.postMessage).not.toHaveBeenCalled();
  expect(runnableWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(handle.readyWorkers).toEqual([]);
});

test("pumpProcess reconciles a worker-local futex wait that is already runnable in canonical state", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234);
  thread.waitingInWorkerStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, waitingWorker);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(thread.waitingInWorkerStatus).toBeNull();
  expect(handle.runningWorker).toBe(waitingWorker);
  expect(waitingWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("pumpProcess prioritizes reconciled worker-local futex waits over queued sleepers", () => {
  const sleeperWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const sleeperThread = makeThreadState(0x5678);
  const waitingThread = makeThreadState(0x1234);
  waitingThread.waitingInWorkerStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(sleeperWorker, sleeperThread);
  threadWorkerStates.set(waitingWorker, waitingThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, sleeperWorker);
  handle.workers.add(waitingWorker);
  handle.readyWorkers = [sleeperWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(waitingThread.waitingInWorkerStatus).toBeNull();
  expect(handle.runningWorker).toBe(waitingWorker);
  expect(waitingWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(sleeperWorker.postMessage).not.toHaveBeenCalled();
  expect(handle.readyWorkers).toEqual([sleeperWorker]);
});

test("kickBlockedWorkers applies child exit records for worker-local futex waits once", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x1234);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  const { exports: kernel } = makeKernel();
  const recordChildExit = vi.fn();
  kernel.process_record_child_exit_for_parent = recordChildExit;
  waitingThread.waitingInWorkerStatus = kernel.abi_status_futex_wait();
  const handle = makeHandle(kernel, waitingWorker);
  handle.childExitSyncRecords.set(4, 0);

  const lifecycleEvents: string[] = [];
  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.kickBlockedWorkers(handle);
  scheduler.kickBlockedWorkers(handle);

  expect(recordChildExit).toHaveBeenCalledTimes(1);
  expect(recordChildExit).toHaveBeenCalledWith(1, 4, 0);
  expect(lifecycleEvents).toContain("futex-wait-apply-child-records:1:4/0");
  expect(lifecycleEvents).toContain("wait4-apply-records:1:4/0");
});

test("pumpProcess clears a running worker that reached worker-local futex wait", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runnableWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x1234);
  const runnableThread = makeThreadState(0x5678);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(runnableWorker, runnableThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? kernel.abi_status_futex_wait()
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, waitingWorker);
  handle.workers.add(runnableWorker);
  handle.runningWorker = waitingWorker;
  waitingThread.runningGeneration = waitingThread.generation;
  handle.readyWorkers = [runnableWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(waitingThread.waitingInWorkerStatus).toBe(kernel.abi_status_futex_wait());
  expect(waitingThread.runningGeneration).toBeNull();
  expect(handle.runningWorker).toBe(runnableWorker);
  expect(waitingWorker.postMessage).not.toHaveBeenCalled();
  expect(runnableWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("enqueue does not place the current running worker back on the ready queue", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, worker);
  handle.runningWorker = worker;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.enqueueWorker(handle, worker);

  expect(handle.runningWorker).toBe(worker);
  expect(handle.readyWorkers).not.toContain(worker);
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("enqueue wakes the running worker when another worker becomes ready", () => {
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, makeThreadState(0x1234));
  threadWorkerStates.set(readyWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const before = Atomics.load(notify, 0);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.enqueueWorker(handle, readyWorker);

  expect(handle.runningWorker).toBe(runningWorker);
  expect(handle.readyWorkers).toContain(readyWorker);
  expect(Atomics.load(notify, 0)).toBe(before + 1);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
});

test("enqueue does not place a worker-local wait worker on the ready queue", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_futex_wait();
  const handle = makeHandle(kernel, worker);
  thread.waitingInWorkerStatus = kernel.abi_status_futex_wait();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.enqueueWorker(handle, worker);

  expect(handle.readyWorkers).not.toContain(worker);
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("attachThreadWorker fails the process when a browser worker emits an error before ready", () => {
  const worker = {
    postMessage: vi.fn(),
    onmessage: null,
    onerror: null,
    onmessageerror: null,
  } as unknown as Worker & {
    onmessage: ((event: MessageEvent<unknown>) => void) | null;
    onerror: ((event: ErrorEvent) => void) | null;
    onmessageerror: ((event: MessageEvent<unknown>) => void) | null;
  };
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, worker);
  handle.guestMemoryPolicyLimitBytes = 16n * 1024n * 1024n;
  handle.guestBase = 0x1000;
  const failProcess = vi.fn();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess,
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.attachThreadWorker(handle, { worker, vmPtr: 0x1234 });

  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "init",
    vmPtr: 0x1234,
  }));

  worker.onerror?.({
    message: "boot failed",
    filename: "thread-worker.mjs",
    lineno: 10,
    colno: 5,
    error: new Error("boot failed"),
    preventDefault: vi.fn(),
  } as unknown as ErrorEvent);

  expect(failProcess).toHaveBeenCalledTimes(1);
  expect(failProcess.mock.calls[0]?.[1]).toBeInstanceOf(Error);
  expect(String(failProcess.mock.calls[0]?.[1]?.message)).toContain("thread worker error vm=0x1234");
});

test("socket wait timeout resumes with timeout handler after deadline", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = 30;
  const resumeSocketTimeout = vi.fn(() => {
    status = 0;
  });
  kernel.thread_socket_wait_timeout = () => 25n;
  kernel.thread_resume_socket_wait_timeout = resumeSocketTimeout;
  kernel.thread_resume_blocked_syscall = vi.fn();
  kernel.thread_status = () => status;
  const handle = makeHandle(kernel, worker);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_socket_recv(), 0x1234);
  await vi.advanceTimersByTimeAsync(30);

  expect(resumeSocketTimeout).toHaveBeenCalledWith(0x1234);
});

test("connect timeout resumes with socket timeout handler after deadline", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = 28;
  const resumeSocketTimeout = vi.fn(() => {
    status = 0;
  });
  kernel.thread_socket_wait_timeout = () => 25n;
  kernel.thread_resume_socket_wait_timeout = resumeSocketTimeout;
  kernel.thread_resume_blocked_syscall = vi.fn();
  kernel.thread_status = () => status;
  const handle = makeHandle(kernel, worker);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_connect(), 0x1234);
  await vi.advanceTimersByTimeAsync(30);

  expect(resumeSocketTimeout).toHaveBeenCalledWith(0x1234);
});
