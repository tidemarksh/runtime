import { afterEach, beforeEach, expect, test, vi } from "vite-plus/test";
import {
  createRuntimeStateBridge,
  readPublishedRuntimeBridgeEpoch,
} from "../../../../src/bridge/runtime-state.js";
import { RUNTIME_BRIDGE_NOTIFY_INDEX } from "../../../../src/layout/runtime-state.js";

vi.mock("../../../../src/worker/pool.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../../../src/worker/pool.js")>();
  return {
    ...actual,
    kernelStackSlotForWorker: () => 2,
  };
});

import { createWorkerScheduler } from "../../../../src/worker/scheduler.js";
import { registerForkSharedSyncPromise } from "../../../../src/worker/fork-shared-sync-debug.js";
import { createWorkerStatusTransitionHandler } from "../../../../src/worker/message-handler/status/transitions.js";
import { createWorkerSchedulerQueueHelpers } from "../../../../src/worker/scheduler/queue.js";
import { createRunLoopScheduler } from "../../../../src/worker/scheduler/run-loop.js";
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
      thread_resume_futex_wait: vi.fn(),
      thread_resume_futex_wait_result: vi.fn(),
      supervisor_complete_futex_wait: vi.fn(() => 0),
      supervisor_futex_wait_completion_woken: () => 1,
      supervisor_futex_wait_completion_not_equal: () => 2,
      supervisor_futex_wait_completion_timed_out: () => 3,
      supervisor_futex_wait_completion_invalid_address: () => 4,
      supervisor_futex_wait_completion_signal_check: () => 5,
      thread_futex_wait_addr: () => 0n,
      thread_record_blocked_addr: () => 0n,
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
      thread_epoll_timerfd_deadline_ticks: vi.fn(() => 0n),
      thread_sleep_seconds: () => 0n,
      thread_sleep_nanoseconds: () => 0n,
      thread_epoll_wait_timeout: () => 0n,
      thread_poll_wait_timeout: () => -1n,
      thread_poll_wait_ready_count: () => 0,
      thread_poll_wait_has_socket: () => 0,
      thread_poll_wait_first_socket_revents: () => 0,
      thread_poll_wait_first_pipe_revents: () => 0,
      thread_a0: () => 0n,
      thread_a7: () => 0n,
      thread_blocked_fd: () => 0,
      thread_blocked_ofd_id: () => 0,
      thread_blocked_syscall_nr: () => 0n,
      thread_blocked_syscall_arg: () => 0n,
      thread_pipe_read_eof_sync_pending: () => 0,
      thread_poll_hup_sync_pending: () => 0,
      thread_socket_wait_timeout: () => -1n,
      thread_process_thread_count: () => 1,
      thread_process_ptr: () => 0x2000,
      thread_pc: () => 0x1000n,
      thread_sp: () => 0x8000n,
      thread_s11: () => 0x9000n,
      process_current_pid: () => 1,
      process_thread_tid: () => 0,
      process_thread_state: () => 0,
      process_thread_vm_ptr: (_processVmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0,
      process_record_child_exit_for_parent: vi.fn(),
      abi_max_fds: () => 64,
      abi_max_pipes: () => 64,
      abi_fd_type_pipe: () => 2,
      abi_fd_type_socket: () => 4,
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
      process_fd_flags: () => 0,
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
    lastStatusFutexWaitAddr: null,
    lastStatusFutexBlockedAddr: null,
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

function makeStatusTransitionDeps(
  handle: ProcessHandle,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
  overrides: Partial<Parameters<typeof createWorkerStatusTransitionHandler>[0]> = {},
): Parameters<typeof createWorkerStatusTransitionHandler>[0] {
  return {
    processHandles: new Map([[handle.pid, handle]]),
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    attachThreadWorker: vi.fn(),
    blockWorker: vi.fn(),
    canonicalizeActiveVforkChildViaKernelWorker: vi.fn(async () => ({ kind: "completed" })),
    captureVforkSnapshot: vi.fn(() => ({
      vmPtr: handle.vmPtr,
      childPid: null,
      kernelState: handle.kernelState,
      stackSnapshot: null,
    }) as NonNullable<ProcessHandle["vforkSnapshot"]>),
    clearVforkSnapshot: vi.fn(),
    clearBlockedWorker: vi.fn(),
    completeProcessExit: vi.fn(async () => {}),
    commitNetworkConnect: vi.fn(async () => {}),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    handleExecveSpawn: vi.fn(async () => {}),
    kickBlockedWorkers: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    pumpProcess: vi.fn(),
    pruneDetachedProcessWorkers: vi.fn(),
    pumpNetworkConnections: vi.fn(async () => false),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestPipeStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    resolveNetworkConnect: vi.fn(async () => 0n),
    resumeConnectViaKernelWorker: vi.fn(async () => ({ kind: "completed" })),
    resumeFsViaKernelWorker: vi.fn(async () => {}),
    resumeVforkParentAfterChildExit: vi.fn(() => false),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncProcessMemfsViaKernelWorker: vi.fn(async () => {}),
    syncProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    ...overrides,
  };
}

test("ok status defers re-enqueue while a blocked epoll retry rpc is pending", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x5860320);
  const blockedThread = makeThreadState(0x6286130);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, worker);
  handle.workers.add(blockedWorker);
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });
  const enqueueWorker = vi.fn();
  const kickBlockedWorkers = vi.fn(() => {
    const blocked = handle.blockedWorkers.get(blockedWorker);
    if (blocked) {
      blocked.rpcPending = true;
    }
  });
  const handleStatusTransition = createWorkerStatusTransitionHandler(makeStatusTransitionDeps(
    handle,
    threadWorkerStates,
    {
      enqueueWorker,
      kickBlockedWorkers,
    },
  ));

  handleStatusTransition({
    handle,
    worker,
    thread,
    msg: {
      type: "status",
      generation: thread.generation,
      status: kernel.abi_status_ok(),
      exitCode: 0,
      termSignal: 0,
      pc: 0x1000n,
      vmPtr: thread.vmPtr,
      kernelState: handle.kernelState,
    },
    status: kernel.abi_status_ok(),
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  expect(kickBlockedWorkers).toHaveBeenCalledWith(handle);
  expect(enqueueWorker).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(1);
  expect(enqueueWorker).not.toHaveBeenCalled();

  const blocked = handle.blockedWorkers.get(blockedWorker);
  expect(blocked).toBeDefined();
  blocked!.rpcPending = false;
  await vi.advanceTimersByTimeAsync(1);

  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("ok status re-enqueues immediately when no blocked retry rpc is pending", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x5860320);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, worker);
  const enqueueWorker = vi.fn();
  const kickBlockedWorkers = vi.fn();
  const handleStatusTransition = createWorkerStatusTransitionHandler(makeStatusTransitionDeps(
    handle,
    threadWorkerStates,
    {
      enqueueWorker,
      kickBlockedWorkers,
    },
  ));

  handleStatusTransition({
    handle,
    worker,
    thread,
    msg: {
      type: "status",
      generation: thread.generation,
      status: kernel.abi_status_ok(),
      exitCode: 0,
      termSignal: 0,
      pc: 0x1000n,
      vmPtr: thread.vmPtr,
      kernelState: handle.kernelState,
    },
    status: kernel.abi_status_ok(),
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  expect(kickBlockedWorkers).toHaveBeenCalledWith(handle);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
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
    failProcess: (_handle, error) => {
      throw error;
    },
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

test("scheduler caps step budget while owner retry waiters are blocked", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  threadWorkerStates.set(blockedWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, worker);
  handle.workers.add(blockedWorker);
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: 0x5678,
    status: kernel.abi_status_wait4(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    epollTimerfdDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: false,
    pipeChildExitResumeKey: null,
    pipeEofChildExitDeferrals: 0,
  });

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
    budget: 100_000,
  }));
});

test("scheduler uses pending worker kernel state for one active vfork child step", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234) as ThreadWorkerState & {
    pendingKernelStateForNextStep?: ProcessHandle["kernelState"];
  };
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, worker);
  const parentKernelState = {
    ...handle.kernelState,
    heapPtr: 0x111000,
    pipeUsed: 1n,
  };
  const childKernelState = {
    ...handle.kernelState,
    heapPtr: 0x222000,
    pipeUsed: 3n,
  };
  handle.kernelState = parentKernelState;
  handle.vforkWorker = worker;
  handle.vforkSnapshot = {
    vmPtr: 0x1234,
    parentPid: 1,
    parentProcessPtr: 0x2000,
    parentPc: 0x1000n,
    parentSp: 0x8000n,
    parentS11: 0x9000n,
    childPid: 3,
    nextPid: 4,
    pgid: 1,
    sid: 1,
    inheritedSharedOfdIds: [],
  };
  thread.pendingKernelStateForNextStep = childKernelState;

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
    kernelState: childKernelState,
  }));
  expect(thread.pendingKernelStateForNextStep).toBeUndefined();
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

test("blocked FS retry keeps owner-async enqueue ready until async clears", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234);
  thread.stepCount = 3;
  thread.statusCount = 2;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_fs();
  kernel.thread_status = () => status;
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  let scheduler: ReturnType<typeof createWorkerScheduler>;
  const resumeFsViaKernelWorkerOutcome = vi.fn(async () => {
    expect(thread.ownerAsyncStatus).toBe(kernel.abi_status_fs());
    status = kernel.abi_status_ok();
    scheduler.enqueueWorker(handle, worker);
    expect(handle.readyWorkers).toContain(worker);
    expect(worker.postMessage).not.toHaveBeenCalled();
    return { kind: "completed" as const };
  });

  scheduler = createWorkerScheduler({
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

test("pumpProcess recovers a stale OK owner-async worker", () => {
  vi.setSystemTime(new Date(10_000));
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234);
  thread.ownerAsyncStatus = 6;
  thread.stepCount = 8;
  thread.statusCount = 8;
  thread.lastStatusAtMs = 7_000;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(thread.ownerAsyncStatus).toBeNull();
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("pumpProcess keeps a fresh owner-async worker parked", () => {
  vi.setSystemTime(new Date(10_000));
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234);
  thread.ownerAsyncStatus = 6;
  thread.stepCount = 8;
  thread.statusCount = 8;
  thread.lastStatusAtMs = 9_000;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(thread.ownerAsyncStatus).toBe(6);
  expect(worker.postMessage).not.toHaveBeenCalled();
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

test("epoll retry wakes a running worker instead of resuming under it", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedVmPtr = 0x1234;
  const runningVmPtr = 0x5678;
  const blockedThread = makeThreadState(blockedVmPtr);
  const runningThread = makeThreadState(runningVmPtr);
  runningThread.runningGeneration = runningThread.generation;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, blockedThread);
  threadWorkerStates.set(runningWorker, runningThread);
  const { exports: kernel } = makeKernel();
  kernel.abi_max_threads = () => 4;
  kernel.abi_fd_type_epoll = () => 12;
  kernel.thread_status = (vmPtr: number) =>
    vmPtr === runningVmPtr ? kernel.abi_status_ok() : kernel.abi_status_epoll_wait();
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.thread_process_thread_count = () => 2;
  kernel.process_thread_vm_ptr = (_processVmPtr: number, slot: number) => {
    if (slot === 0) {
      return blockedVmPtr;
    }
    if (slot === 1) {
      return runningVmPtr;
    }
    return 0;
  };
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => (fd === 4 ? 10 : 0);
  kernel.kernel_ofd_fd_type = (ofdId: number) => (ofdId === 10 ? kernel.abi_fd_type_epoll() : 0);
  kernel.kernel_ofd_ino = (ofdId: number) => (ofdId === 10 ? 4 : 0);
  kernel.thread_blocked_ofd_id = () => 10;
  kernel.thread_resume_blocked_syscall = vi.fn();
  const handle = makeHandle(kernel, blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.workers = new Set([blockedWorker, runningWorker]);
  handle.runningWorker = runningWorker;
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("epoll retry must not sync or resume while a worker is running");
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
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), blockedVmPtr);
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, RUNTIME_BRIDGE_NOTIFY_INDEX);

  scheduler.kickBlockedWorkers(handle);
  await Promise.resolve();
  await Promise.resolve();

  expect(handle.kernelWorkerRequest).not.toHaveBeenCalled();
  expect(Atomics.load(notify, RUNTIME_BRIDGE_NOTIFY_INDEX)).toBeGreaterThan(notifyBefore);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(blockedWorker)).toBe(true);
  expect(lifecycleEvents).not.toContain("epoll-retry-result:1:vm=0x1234:status=22:a0=0");
});

test("epoll kernel-worker retry treats timeout zero as expired", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_epoll_wait();
  kernel.thread_status = () => status;
  kernel.thread_epoll_wait_timeout = () => 0n;
  kernel.abi_max_threads = () => 4;
  kernel.process_thread_vm_ptr = (_processVmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0;
  kernel.abi_fd_type_epoll = () => 7;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 4 ? 5 : 0;
  kernel.thread_blocked_ofd_id = () => 5;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === 5 ? 7 : 0;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    throw new Error("timeout-zero epoll wait must not re-dispatch the blocked syscall");
  });
  kernel.thread_resume_epoll_wait_timeout = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async (message) => {
    expect(message.type).toBe("sync-epoll-state");
    return {
      type: "epoll-state-synced",
      requestId: 0,
      kernelState: message.kernelState,
    };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);
  const blocked = handle.blockedWorkers.get(blockedWorker);
  expect(blocked).toBeDefined();
  if (blocked!.retryTimer !== null) {
    clearTimeout(blocked!.retryTimer);
    blocked!.retryTimer = null;
  }
  blocked!.timeoutExpired = false;

  scheduler.kickBlockedWorkers(handle);
  await vi.waitFor(() => {
    expect(kernel.thread_resume_epoll_wait_timeout).toHaveBeenCalledWith(0x1234);
  });

  expect(handle.kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "sync-epoll-state",
  }));
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(blockedWorker)).toBe(false);
  expect(blockedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
  }));
});

test("epoll kernel-worker retry delegates timeout completion to blocked syscall outcome when available", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_epoll_wait();
  kernel.thread_status = () => status;
  kernel.thread_epoll_wait_timeout = () => 0n;
  kernel.abi_max_threads = () => 4;
  kernel.process_thread_vm_ptr = (_processVmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0;
  kernel.abi_fd_type_epoll = () => 7;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 4 ? 5 : 0;
  kernel.thread_blocked_ofd_id = () => 5;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === 5 ? 7 : 0;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    throw new Error("epoll timeout completion must not re-dispatch the blocked syscall in the scheduler");
  });
  kernel.thread_resume_epoll_wait_timeout = vi.fn(() => {
    throw new Error("scheduler must delegate epoll timeout completion to the kernel-worker outcome");
  });
  const handle = makeHandle(kernel, blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async (message) => {
    expect(message.type).toBe("sync-epoll-state");
    return {
      type: "epoll-state-synced",
      requestId: 0,
      kernelState: message.kernelState,
    };
  });
  const lifecycleEvents: string[] = [];
  const resumeBlockedSyscallOutcome = vi.fn(async () => {
    status = kernel.abi_status_ok();
    return { kind: "completed" as const, status };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeBlockedSyscallOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);
  const blocked = handle.blockedWorkers.get(blockedWorker);
  expect(blocked).toBeDefined();
  if (blocked!.retryTimer !== null) {
    clearTimeout(blocked!.retryTimer);
    blocked!.retryTimer = null;
  }
  blocked!.timeoutExpired = false;

  scheduler.kickBlockedWorkers(handle);
  await vi.waitFor(() => {
    expect(resumeBlockedSyscallOutcome).toHaveBeenCalledWith(
      handle,
      blockedWorker,
      0x1234,
      handle.kernel.abi_status_epoll_wait(),
      true,
    );
  });

  expect(kernel.thread_resume_epoll_wait_timeout).not.toHaveBeenCalled();
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(lifecycleEvents).toContain("epoll-retry-resume-rpc:1:vm=0x1234:expired=1");
  expect(handle.blockedWorkers.has(blockedWorker)).toBe(false);
});

test("epoll sync timeout zero survives a running-worker retry skip", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedVmPtr = 0x1234;
  const runningVmPtr = 0x5678;
  const blockedThread = makeThreadState(blockedVmPtr);
  const runningThread = makeThreadState(runningVmPtr);
  runningThread.runningGeneration = runningThread.generation;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, blockedThread);
  threadWorkerStates.set(runningWorker, runningThread);
  const { exports: kernel } = makeKernel();
  let timeoutMs = -1n;
  kernel.abi_max_threads = () => 4;
  kernel.abi_fd_type_epoll = () => 7;
  kernel.thread_status = (vmPtr: number) =>
    vmPtr === runningVmPtr ? kernel.abi_status_ok() : kernel.abi_status_epoll_wait();
  kernel.thread_epoll_wait_timeout = () => timeoutMs;
  kernel.thread_process_thread_count = () => 2;
  kernel.process_thread_vm_ptr = (_processVmPtr: number, slot: number) => {
    if (slot === 0) {
      return blockedVmPtr;
    }
    if (slot === 1) {
      return runningVmPtr;
    }
    return 0;
  };
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 4 ? 5 : 0;
  kernel.thread_blocked_ofd_id = () => 5;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === 5 ? 7 : 0;
  kernel.thread_resume_blocked_syscall = vi.fn();
  kernel.thread_resume_epoll_wait_timeout = vi.fn();
  const handle = makeHandle(kernel, blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.workers = new Set([blockedWorker, runningWorker]);
  handle.kernelWorkerRequest = vi.fn(async (message) => {
    expect(message.type).toBe("sync-epoll-state");
    timeoutMs = 0n;
    handle.runningWorker = runningWorker;
    return {
      type: "epoll-state-synced",
      requestId: 0,
      kernelState: message.kernelState,
    };
  });
  const lifecycleEvents: string[] = [];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), blockedVmPtr);
  const blocked = handle.blockedWorkers.get(blockedWorker);
  expect(blocked).toBeDefined();
  if (blocked!.retryTimer !== null) {
    clearTimeout(blocked!.retryTimer);
    blocked!.retryTimer = null;
  }
  blocked!.timeoutExpired = false;

  scheduler.kickBlockedWorkers(handle);
  await vi.waitFor(() => {
    expect(lifecycleEvents.some((event) => event.startsWith("epoll-retry-skip:1:running=1:sched=1"))).toBe(true);
  });

  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(kernel.thread_resume_epoll_wait_timeout).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.get(blockedWorker)?.timeoutExpired).toBe(true);
  expect(handle.blockedWorkers.has(blockedWorker)).toBe(true);
});

test("epoll retry defers while clone child init is expected to release parent", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const parentWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const childWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedThread = makeThreadState(0x1234);
  const parentThread = makeThreadState(0x5678);
  const childThread = makeThreadState(0x9abc);
  parentThread.ownerAsyncStatus = 10;
  childThread.initialized = false;
  childThread.resumeWorkersOnReady = [parentWorker];
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, blockedThread);
  threadWorkerStates.set(parentWorker, parentThread);
  threadWorkerStates.set(childWorker, childThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? kernel.abi_status_epoll_wait()
    : kernel.abi_status_ok();
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.abi_fd_type_epoll = () => 7;
  const handle = makeHandle(kernel, blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.workers.add(parentWorker);
  handle.workers.add(childWorker);
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("local epoll ownership should not require a kernel-worker sync in this fixture");
  });
  const lifecycleEvents: string[] = [];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);
  const blocked = handle.blockedWorkers.get(blockedWorker);
  expect(blocked).toBeDefined();
  if (blocked!.retryTimer !== null) {
    clearTimeout(blocked!.retryTimer);
    blocked!.retryTimer = null;
  }

  scheduler.kickBlockedWorkers(handle);

  expect(lifecycleEvents).toContain("epoll-retry-skip:1:clone-child-init-pending");
  expect(handle.kernelWorkerRequest).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.get(blockedWorker)?.retryTimer).not.toBeNull();

  childThread.initialized = true;
  childThread.resumeWorkersOnReady = [];
  const retryBlocked = handle.blockedWorkers.get(blockedWorker);
  if (retryBlocked?.retryTimer !== null && retryBlocked?.retryTimer !== undefined) {
    clearTimeout(retryBlocked.retryTimer);
    retryBlocked.retryTimer = null;
  }
  scheduler.kickBlockedWorkers(handle);

  expect(lifecycleEvents.some((event) => event.startsWith("epoll-retry-rpc:1:vm=0x1234"))).toBe(true);
  expect(handle.kernelWorkerRequest).not.toHaveBeenCalled();
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

test("does not passively park synced pipe EOF", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.abi_sys_read = () => 63n;
  kernel.thread_status = () => kernel.abi_status_pipe_wait();
  kernel.thread_a7 = () => 63n;
  kernel.thread_blocked_syscall_nr = () => 63n;
  kernel.thread_blocked_pipe_buffered_len = () => 0;
  kernel.thread_blocked_pipe_write_open = () => 0;
  kernel.thread_blocked_pipe_read_ready = () => 0;
  kernel.thread_blocked_pipe_write_ready = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn() as never;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, kernel.abi_status_pipe_wait(), 0x1234);
  const blocked = handle.blockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  if (blocked!.retryTimer !== null) {
    clearTimeout(blocked!.retryTimer);
    blocked!.retryTimer = null;
  }
  blocked!.pipeSyncedBeforePassive = true;

  scheduler.blockWorker(handle, worker, kernel.abi_status_pipe_wait(), 0x1234);

  expect(handle.blockedWorkers.get(worker)?.retryTimer).not.toBeNull();
});

test("reblocking the same pipe wait preserves in-flight blocked rpc ownership", () => {
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
    throw new Error("same-status reblock must not start a duplicate retry");
  });
  const resumeBlockedSyscallLocally = vi.fn(async () => {});

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
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
  blocked!.rpcPending = true;
  blocked!.rpcGeneration = 7;
  blocked!.pipeSyncedBeforePassive = true;

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);

  const reblocked = handle.blockedWorkers.get(worker);
  expect(reblocked?.rpcPending).toBe(true);
  expect(reblocked?.rpcGeneration).toBe(7);
  expect(reblocked?.pipeSyncedBeforePassive).toBe(true);
  expect(resumeBlockedSyscallLocally).not.toHaveBeenCalled();
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

test("socket poll pump resumes through kernel-worker RPC when available", async () => {
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
  kernel.process_state_bytes = () => 0;
  kernel.kernel_ofd_table_bytes = () => 64 * 32;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.networkConnections.set(3, {} as never);
  handle.kernelWorkerRequest = vi.fn(async (msg) => {
    switch (msg.type) {
      case "sync-socket-state":
        return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
      case "sync-pipe-state":
        return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
      default:
        throw new Error(`unexpected kernel-worker request: ${msg.type}`);
    }
  }) as ProcessHandle["kernelWorkerRequest"];
  const pumpNetworkConnections = vi.fn(async () => true);
  const resumeBlockedSyscallOutcome = vi.fn(async () => {
    status = kernel.abi_status_ok();
    return { kind: "completed" as const, status };
  });
  const failProcess = vi.fn();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess,
    hasActiveChildProcess: () => false,
    pumpNetworkConnections,
    resumeBlockedSyscallLocally: async () => {},
    resumeBlockedSyscallOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  await vi.advanceTimersByTimeAsync(2);
  await Promise.resolve();
  await Promise.resolve();

  expect(pumpNetworkConnections).toHaveBeenCalledWith(handle, 0x1234);
  expect(failProcess).not.toHaveBeenCalled();
  expect(handle.kernelWorkerRequest).toHaveBeenCalled();
  expect(resumeBlockedSyscallOutcome).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_pipe_wait(),
    false,
  );
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({ type: "step" }));
  expect(failProcess).not.toHaveBeenCalled();
});

test("finite pipe poll timeout delegates to kernel-worker blocked syscall outcome when available", async () => {
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
  handle.kernelWorkerRequest = vi.fn(async () => ({ type: "unexpected" }) as never);
  const resumeBlockedSyscallLocally = vi.fn(async () => {});
  const resumeBlockedSyscallOutcome = vi.fn(async () => {
    status = kernel.abi_status_ok();
    return { kind: "completed" as const, status };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally,
    resumeBlockedSyscallOutcome,
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
  await Promise.resolve();
  await Promise.resolve();

  expect(resumeBlockedSyscallOutcome).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_pipe_wait(),
    true,
  );
  expect(resumePollTimeout).not.toHaveBeenCalled();
  expect(resumeBlockedSyscallLocally).not.toHaveBeenCalled();
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
      {
        guestPtr: 0x20,
        bytes: new Uint8Array([
          0x08, 0, 0, 0, 0, 0, 0, 0,
          0x10, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0, 0,
        ]),
      },
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

test("wait4 retry clears a stale fork-shared sync source before retrying", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  let status = kernel.abi_status_wait4();
  kernel.thread_status = () => status;
  kernel.thread_process_ptr = (vmPtr: number) => vmPtr === 0x9999 ? 0 : 0x2000;
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, worker);
  handle.kernelShare.members.add(handle);
  handle.pendingChildSpawnRecords = 0;
  const forkSharedSync = new Promise<void>(() => {});
  registerForkSharedSyncPromise(forkSharedSync, {
    ownerPid: handle.pid,
    sourceVmPtr: 0x9999,
    reason: "owned-ofd",
    targetPids: [5],
  });
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
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);

  expect(handle.forkSharedSyncPromise).toBeNull();
  expect(handle.kernelShare.forkSharedSyncPromise).toBeNull();
  expect(lifecycleEvents.some((event) =>
    event.startsWith("wait4-retry-cleared:1:fork-shared-sync-stale-source:")
    && event.includes(":owner=1:src=0x9999:reason=owned-ofd:targets=5"),
  )).toBe(true);
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
});

test("wait4 retry clears an empty-target fork-shared sync gate before retrying", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  let status = kernel.abi_status_wait4();
  kernel.thread_status = () => status;
  kernel.thread_process_ptr = () => 0x2000;
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, worker);
  handle.kernelShare.members.add(handle);
  handle.pendingChildSpawnRecords = 0;
  const forkSharedSync = new Promise<void>(() => {});
  registerForkSharedSyncPromise(forkSharedSync, {
    ownerPid: handle.pid,
    sourceVmPtr: 0x9999,
    reason: "blocked-rpc",
    targetPids: [],
  });
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
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);

  expect(handle.forkSharedSyncPromise).toBeNull();
  expect(handle.kernelShare.forkSharedSyncPromise).toBeNull();
  expect(lifecycleEvents.some((event) =>
    event.startsWith("wait4-retry-cleared:1:fork-shared-sync-empty-targets:")
    && event.includes(":owner=1:src=0x9999:reason=blocked-rpc:targets=none"),
  )).toBe(true);
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
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

test("wait4 retry reapplies a pending local child exit record until it is reaped", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_wait4();
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
    return 0n;
  });
  kernel.process_record_child_exit_for_parent = vi.fn();
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.childExitSyncRecords.set(7, 0);
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

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);
  await vi.advanceTimersByTimeAsync(11);

  expect(kernel.process_record_child_exit_for_parent.mock.calls.length).toBeGreaterThanOrEqual(2);
  expect(kernel.process_record_child_exit_for_parent).toHaveBeenNthCalledWith(1, 1, 7, 0);
  expect(kernel.process_record_child_exit_for_parent).toHaveBeenNthCalledWith(2, 1, 7, 0);
  expect(threadResumeWait4.mock.calls.length).toBeGreaterThanOrEqual(2);
  expect(handle.childExitSyncRecords.has(7)).toBe(true);
  expect(lifecycleEvents.filter((event) => event === "wait4-apply-records:1:7/0").length).toBeGreaterThanOrEqual(2);
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

test("epoll timerfd wait advances guest time to deadline before retrying epoll", async () => {
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
  let status = kernel.abi_status_epoll_wait();
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? status
    : kernel.abi_status_ok();
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.thread_epoll_timerfd_deadline_ticks = () => 1_000_001n;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
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

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);

  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  await vi.advanceTimersByTimeAsync(9);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(1);

  expect(kernel.sync_monotonic_ticks_floor).toHaveBeenCalledWith(1_000_001n);
  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(0x1234);
  expect(kernel.thread_resume_epoll_wait_timeout).not.toHaveBeenCalled();
});

test("epoll kernel-worker retry re-queries timerfd deadline after state sync", async () => {
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
  let status = kernel.abi_status_epoll_wait();
  let deadlineVisible = false;
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? status
    : kernel.abi_status_ok();
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.thread_process_thread_count = () => 2;
  kernel.process_thread_vm_ptr = (_vmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0;
  kernel.abi_fd_type_epoll = () => 7;
  kernel.abi_fd_type_timerfd = () => 10;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => {
    if (fd === 4) {
      return 5;
    }
    if (fd === 5) {
      return 6;
    }
    return 0;
  };
  kernel.thread_blocked_ofd_id = (vmPtr: number) => vmPtr === 0x1234 ? 5 : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => {
    if (ofdId === 5) {
      return 7;
    }
    if (ofdId === 6) {
      return 10;
    }
    return 0;
  };
  kernel.thread_epoll_timerfd_deadline_ticks = vi.fn(() => deadlineVisible ? 1_000_001n : 0n);
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.workers.add(readyWorker);
  handle.readyWorkers.push(readyWorker);
  handle.kernelWorkerRequest = vi.fn(async (message) => {
    if (message.type === "sync-ofd-state") {
      deadlineVisible = true;
      return {
        type: "ofd-state-synced",
        requestId: 0,
        kernelState: message.kernelState,
      };
    }
    if (message.type === "sync-epoll-state") {
      return {
        type: "epoll-state-synced",
        requestId: 0,
        kernelState: message.kernelState,
      };
    }
    throw new Error(`unexpected kernel worker request: ${message.type}`);
  });
  const lifecycleEvents: string[] = [];
  const failProcess = vi.fn();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess,
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);

  await vi.advanceTimersByTimeAsync(10);
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();

  expect(handle.kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "sync-ofd-state",
  }));
  expect(failProcess).not.toHaveBeenCalled();
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(lifecycleEvents).toContain("epoll-retry-deadline:1:vm=0x1234:ticks=1000001");

  await vi.advanceTimersByTimeAsync(9);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(1);

  expect(kernel.sync_monotonic_ticks_floor).toHaveBeenCalledWith(1_000_001n);
  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(0x1234);
  expect(kernel.thread_resume_epoll_wait_timeout).not.toHaveBeenCalled();
});

test("epoll kernel-worker retry syncs watched timerfd OFD after the timer fd is closed", async () => {
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
  let status = kernel.abi_status_epoll_wait();
  let deadlineVisible = false;
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? status
    : kernel.abi_status_ok();
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.thread_process_thread_count = () => 2;
  kernel.process_thread_vm_ptr = (_vmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0;
  kernel.abi_fd_type_epoll = () => 7;
  kernel.abi_fd_type_timerfd = () => 10;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 4 ? 5 : 0;
  kernel.thread_blocked_ofd_id = (vmPtr: number) => vmPtr === 0x1234 ? 5 : 0;
  kernel.thread_blocked_fd = (vmPtr: number) => vmPtr === 0x1234 ? 4 : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => {
    if (ofdId === 5) {
      return 7;
    }
    if (ofdId === 6) {
      return 10;
    }
    return 0;
  };
  kernel.kernel_ofd_ino = (ofdId: number) => {
    if (ofdId === 5) {
      return 0;
    }
    if (ofdId === 6) {
      return 1;
    }
    return 0;
  };
  kernel.kernel_ofd_offset = () => 0n;
  kernel.kernel_epoll_table_bytes = () => 64 * 8;
  kernel.kernel_epoll_instance_bytes = () => 64 * 32 + 4;
  kernel.thread_epoll_timerfd_deadline_ticks = vi.fn(() => deadlineVisible ? 1_000_001n : 0n);
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.workers.add(readyWorker);
  handle.readyWorkers.push(readyWorker);
  handle.kernelState.epollTablePtr = 0x1000;
  const epollInstancePtr = 0x2000;
  const epollView = new DataView(handle.memory.buffer);
  epollView.setUint32(handle.kernelState.epollTablePtr, epollInstancePtr, true);
  epollView.setUint32(epollInstancePtr, 5, true);
  epollView.setUint32(epollInstancePtr + 4, 6, true);
  epollView.setUint32(epollInstancePtr + 8, 0x40000001, true);
  epollView.setBigUint64(epollInstancePtr + 16, 5n, true);
  epollView.setUint32(epollInstancePtr + 24, 0, true);
  epollView.setUint32(epollInstancePtr + 64 * 32, 1, true);

  handle.kernelWorkerRequest = vi.fn(async (message) => {
    if (message.type === "sync-ofd-state") {
      expect(message.ofdIds).toEqual([6]);
      deadlineVisible = true;
      return {
        type: "ofd-state-synced",
        requestId: 0,
        kernelState: message.kernelState,
      };
    }
    if (message.type === "sync-epoll-state") {
      return {
        type: "epoll-state-synced",
        requestId: 0,
        kernelState: message.kernelState,
      };
    }
    throw new Error(`unexpected kernel worker request: ${message.type}`);
  });
  const lifecycleEvents: string[] = [];
  const failProcess = vi.fn();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess,
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);

  await vi.advanceTimersByTimeAsync(10);
  await Promise.resolve();
  await Promise.resolve();

  expect(handle.kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "sync-ofd-state",
    ofdIds: [6],
  }));
  expect(failProcess).not.toHaveBeenCalled();
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(lifecycleEvents).toContain("epoll-retry-deadline:1:vm=0x1234:ticks=1000001");

  await vi.advanceTimersByTimeAsync(10);

  expect(kernel.sync_monotonic_ticks_floor).toHaveBeenCalledWith(1_000_001n);
  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(0x1234);
  expect(kernel.thread_resume_epoll_wait_timeout).not.toHaveBeenCalled();
});

test("epoll kernel-worker retry syncs watched pipe, epoll table, then dependent eventfd", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_epoll_wait();
  kernel.thread_status = () => status;
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.thread_process_thread_count = () => 2;
  kernel.process_thread_vm_ptr = (_vmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0;
  kernel.abi_fd_type_epoll = () => 7;
  kernel.abi_fd_type_pipe = () => 6;
  kernel.abi_fd_type_file = () => 1;
  kernel.abi_fd_type_eventfd = () => 9;
  kernel.abi_fd_type_timerfd = () => 10;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 4 ? 5 : 0;
  kernel.process_fd_flags = () => 0;
  kernel.thread_blocked_ofd_id = (vmPtr: number) => vmPtr === 0x1234 ? 5 : 0;
  kernel.thread_blocked_fd = (vmPtr: number) => vmPtr === 0x1234 ? 4 : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => {
    if (ofdId === 5) {
      return 7;
    }
    if (ofdId === 6) {
      return 6;
    }
    if (ofdId === 7) {
      return 9;
    }
    return 0;
  };
  kernel.kernel_ofd_ino = (ofdId: number) => {
    if (ofdId === 5) {
      return 0;
    }
    if (ofdId === 6) {
      return 3;
    }
    return 0;
  };
  kernel.kernel_ofd_host_fd = (ofdId: number) => ofdId === 6 ? 3 : 0;
  kernel.kernel_ofd_status_flags = () => 0;
  kernel.kernel_ofd_offset = () => 0n;
  kernel.kernel_ofd_table_bytes = () => 64 * 32;
  kernel.kernel_pipe_region_bytes = () => 64 * (32 + 65_536);
  kernel.kernel_epoll_table_bytes = () => 64 * 8;
  kernel.kernel_epoll_instance_bytes = () => 64 * 32 + 4;
  kernel.process_state_bytes = () => 0;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
  });

  const handle = makeHandle(kernel, blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelState.epollTablePtr = 0x1000;
  handle.kernelState.ofdTablePtr = 0x3000;
  handle.kernelState.pipeBase = 0x4000;
  const epollTablePtr = handle.kernelState.epollTablePtr;
  const epollInstancePtr = 0x2000;
  const epollView = new DataView(handle.memory.buffer);
  epollView.setUint32(handle.kernelState.epollTablePtr, epollInstancePtr, true);
  epollView.setUint32(epollInstancePtr, 10, true);
  epollView.setUint32(epollInstancePtr + 4, 6, true);
  epollView.setUint32(epollInstancePtr + 8, 0x1, true);
  epollView.setBigUint64(epollInstancePtr + 16, 10n, true);
  epollView.setUint32(epollInstancePtr + 24, 0, true);
  epollView.setUint32(epollInstancePtr + 64 * 32, 1, true);

  const requests: string[] = [];
  handle.kernelWorkerRequest = vi.fn(async (message) => {
    requests.push(message.type);
    if (message.type === "sync-pipe-state") {
      return {
        type: "pipe-state-synced",
        requestId: 0,
        kernelState: { ...message.kernelState, epollTablePtr },
        pipeSlots: [],
      };
    }
    if (message.type === "sync-epoll-state") {
      epollView.setUint32(epollInstancePtr + 32, 12, true);
      epollView.setUint32(epollInstancePtr + 36, 7, true);
      epollView.setUint32(epollInstancePtr + 40, 0x1, true);
      epollView.setBigUint64(epollInstancePtr + 48, 12n, true);
      epollView.setUint32(epollInstancePtr + 56, 0, true);
      epollView.setUint32(epollInstancePtr + 64 * 32, 2, true);
      return {
        type: "epoll-state-synced",
        requestId: 0,
        kernelState: { ...message.kernelState, epollTablePtr },
      };
    }
    if (message.type === "sync-ofd-state") {
      expect(message.ofdIds).toEqual([7]);
      expect(message.mode).toBe("pull");
      return {
        type: "ofd-state-synced",
        requestId: 0,
        kernelState: message.kernelState,
      };
    }
    throw new Error(`unexpected kernel worker request: ${message.type}`);
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);

  await vi.advanceTimersByTimeAsync(10);
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();

  expect(requests).toEqual(["sync-pipe-state", "sync-epoll-state", "sync-ofd-state"]);
  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(0x1234);
});

test("epoll kernel-worker retry blocks a sibling sleeper published during state sync", async () => {
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
  let blockedStatus = kernel.abi_status_epoll_wait();
  let runningStatus = kernel.abi_status_ok();
  kernel.thread_status = (vmPtr: number) => {
    if (vmPtr === 0x1234) {
      return blockedStatus;
    }
    if (vmPtr === 0x5678) {
      return runningStatus;
    }
    return kernel.abi_status_ok();
  };
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.thread_process_thread_count = () => 2;
  kernel.process_thread_vm_ptr = (_vmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0;
  kernel.abi_fd_type_epoll = () => 7;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 4 ? 5 : 0;
  kernel.thread_blocked_ofd_id = (vmPtr: number) => vmPtr === 0x1234 ? 5 : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === 5 ? 7 : 0;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    blockedStatus = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.workers.add(runningWorker);
  handle.kernelWorkerRequest = vi.fn(async (message) => {
    if (message.type === "sync-ofd-state") {
      return {
        type: "ofd-state-synced",
        requestId: 0,
        kernelState: message.kernelState,
      };
    }
    if (message.type === "sync-epoll-state") {
      handle.runningWorker = runningWorker;
      runningStatus = kernel.abi_status_sleep();
      return {
        type: "epoll-state-synced",
        requestId: 0,
        kernelState: message.kernelState,
      };
    }
    throw new Error(`unexpected kernel worker request: ${message.type}`);
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
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);

  await vi.advanceTimersByTimeAsync(10);
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();

  expect(handle.runningWorker).toBe(blockedWorker);
  expect(handle.blockedWorkers.get(runningWorker)?.status).toBe(kernel.abi_status_sleep());
  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(0x1234);
  expect(lifecycleEvents).toContain("running-worker-blocked:1:status=26");
  expect(lifecycleEvents).not.toContain("epoll-retry-skip:1:running=1:sched=1");
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

test("wait4 retry blocks a stale running sleeper that already published a blocked status", () => {
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
  expect(handle.blockedWorkers.get(runningWorker)?.status).toBe(kernel.abi_status_sleep());
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

test("pumpProcess blocks an idle worker that reached canonical pipe wait", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_pipe_wait();
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

  scheduler.pumpProcess(handle);

  expect(handle.blockedWorkers.get(worker)?.status).toBe(kernel.abi_status_pipe_wait());
  expect(handle.runningWorker).toBeNull();
  expect(worker.postMessage).not.toHaveBeenCalled();
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

test("pumpProcess preserves an unanswered ok running worker that has not returned status", () => {
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

  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(handle.runningWorker).toBe(runningWorker);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess keeps an unanswered running worker after an active step clears", async () => {
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

  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(handle.runningWorker).toBe(runningWorker);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves a dispatched worker when a sibling remains queued", async () => {
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

  expect(firstThread.runningGeneration).toBe(firstThread.generation);
  expect(handle.runningWorker).toBe(firstWorker);
  expect(firstWorker.postMessage).toHaveBeenCalledTimes(1);
  expect(secondWorker.postMessage).not.toHaveBeenCalled();
  expect(handle.readyWorkers).toEqual([secondWorker]);
});

test("pumpProcess prioritizes blocked-RPC continuation ready workers", () => {
  const sleeperWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const continuationWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const sleeperThread = makeThreadState(0x70c8e98);
  const continuationThread = makeThreadState(0x7127a90);
  sleeperThread.lastStatus = 26;
  continuationThread.pendingPipeSlotsForNextStep.push({
    pipeSlot: 3,
    slot: new Uint8Array(16),
  } as never);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(sleeperWorker, sleeperThread);
  threadWorkerStates.set(continuationWorker, continuationThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, sleeperWorker);
  handle.workers.add(continuationWorker);
  handle.readyWorkers = [sleeperWorker, continuationWorker];

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

  expect(handle.runningWorker).toBe(continuationWorker);
  expect(continuationWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    pipeSlots: expect.any(Array),
  }));
  expect(sleeperWorker.postMessage).not.toHaveBeenCalled();
  expect(handle.readyWorkers).toEqual([sleeperWorker]);
  expect(continuationThread.pendingPipeSlotsForNextStep).toEqual([]);
});

test("pumpProcess defers ordinary sleep-yield ready workers under blocked I/O pressure", () => {
  const firstSleeper = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const secondSleeper = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const firstThread = makeThreadState(0x70f85b8);
  const secondThread = makeThreadState(0x711a608);
  const blockedThread = makeThreadState(0x7141c68);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(firstSleeper, firstThread);
  threadWorkerStates.set(secondSleeper, secondThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  firstThread.lastStatus = kernel.abi_status_sleep();
  secondThread.lastStatus = kernel.abi_status_sleep();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, firstSleeper);
  handle.workers.add(secondSleeper);
  handle.workers.add(blockedWorker);
  handle.readyWorkers = [firstSleeper, secondSleeper];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });

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

  expect(handle.runningWorker).toBeNull();
  expect(handle.readyWorkers).toEqual([firstSleeper, secondSleeper]);
  expect(firstSleeper.postMessage).not.toHaveBeenCalled();
  expect(secondSleeper.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess runs ordinary OK ready workers under blocked I/O pressure", () => {
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyThread = makeThreadState(0x7127a90);
  const blockedThread = makeThreadState(0x7141c68);
  readyThread.lastStatus = 0;
  readyThread.stepCount = 27;
  readyThread.statusCount = 27;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(readyWorker, readyThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, readyWorker);
  handle.workers.add(blockedWorker);
  handle.readyWorkers = [readyWorker];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });

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

  expect(handle.runningWorker).toBe(readyWorker);
  expect(handle.readyWorkers).toEqual([]);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 5_000_000,
  }));
});

test("pumpProcess runs clone-spawn ready workers under blocked I/O pressure", () => {
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyThread = makeThreadState(0x5860320);
  const blockedThread = makeThreadState(0x6286130);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(readyWorker, readyThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  readyThread.lastStatus = kernel.abi_status_clone_spawn();
  readyThread.stepCount = 59;
  readyThread.statusCount = 59;
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, readyWorker);
  handle.workers.add(blockedWorker);
  handle.readyWorkers = [readyWorker];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });

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

  expect(handle.runningWorker).toBe(readyWorker);
  expect(handle.readyWorkers).toEqual([]);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 5_000_000,
  }));
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess runs ordinary OK ready workers while blocked I/O retry timer waits", () => {
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyThread = makeThreadState(0x70bf0d8);
  const blockedThread = makeThreadState(0x7141c68);
  readyThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(readyWorker, readyThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, readyWorker);
  handle.workers.add(blockedWorker);
  handle.readyWorkers = [readyWorker];
  const retryTimer = setTimeout(() => {}, 10);
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 5_000_000,
  }));
});

test("pumpProcess prefers non-yield ready workers after sched_yield", () => {
  const yieldWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const okWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const yieldThread = makeThreadState(0x70c8e98);
  const okThread = makeThreadState(0x70bf0d8);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  yieldThread.lastStatus = 26;
  yieldThread.yieldedReady = true;
  okThread.lastStatus = 0;
  threadWorkerStates.set(yieldWorker, yieldThread);
  threadWorkerStates.set(okWorker, okThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, yieldWorker);
  handle.workers.add(okWorker);
  handle.readyWorkers = [yieldWorker, okWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(okWorker);
  expect(handle.readyWorkers).toEqual([yieldWorker]);
  expect(okThread.yieldedReady).toBe(false);
  expect(okWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 5_000_000,
  }));
  expect(yieldWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess runs yield-ready workers when no other ready worker exists", () => {
  const yieldWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const yieldThread = makeThreadState(0x70c8e98);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  yieldThread.lastStatus = 26;
  yieldThread.yieldedReady = true;
  threadWorkerStates.set(yieldWorker, yieldThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, yieldWorker);
  handle.readyWorkers = [yieldWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(yieldWorker);
  expect(yieldThread.yieldedReady).toBe(false);
  expect(yieldWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 5_000_000,
  }));
});

test("pumpProcess prefers ordinary OK ready workers over sleep-yield while blocked I/O retry timer waits", () => {
  const sleepWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const okWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const sleepThread = makeThreadState(0x70e4058);
  const okThread = makeThreadState(0x70c6578);
  const blockedThread = makeThreadState(0x7141c68);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(sleepWorker, sleepThread);
  threadWorkerStates.set(okWorker, okThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  sleepThread.lastStatus = kernel.abi_status_sleep();
  okThread.lastStatus = kernel.abi_status_ok();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, sleepWorker);
  handle.workers.add(okWorker);
  handle.workers.add(blockedWorker);
  handle.readyWorkers = [sleepWorker, okWorker];
  const retryTimer = setTimeout(() => {}, 10);
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(okWorker);
  expect(handle.readyWorkers).toEqual([sleepWorker]);
  expect(okWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 5_000_000,
  }));
  expect(sleepWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess wakes a no-ready running worker to request cooperative yield", async () => {
  vi.setSystemTime(new Date(10_000));
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x66c42c8);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
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

  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, RUNTIME_BRIDGE_NOTIFY_INDEX);
  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(worker);
  expect(worker.postMessage).toHaveBeenCalledTimes(1);
  expect(Atomics.load(notify, RUNTIME_BRIDGE_NOTIFY_INDEX)).toBe(notifyBefore);

  await vi.advanceTimersByTimeAsync(2_000);

  expect(handle.runningWorker).toBe(worker);
  expect(worker.postMessage).toHaveBeenCalledTimes(1);
  expect(Atomics.load(notify, RUNTIME_BRIDGE_NOTIFY_INDEX)).toBeGreaterThan(notifyBefore);
});

test("pumpProcess redispatches an answered stale ok running worker without ready siblings", async () => {
  vi.setSystemTime(new Date(10_000));
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x9ba7260);
  thread.runningGeneration = thread.generation;
  thread.stepCount = 10;
  thread.statusCount = 11;
  thread.lastStepAtMs = 7_500;
  thread.lastStatusAtMs = 7_000;
  thread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
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

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(worker);
  expect(thread.runningGeneration).toBe(thread.generation);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("pumpProcess preserves a stale unanswered ok running worker without active-step proof", async () => {
  vi.setSystemTime(new Date(10_000));
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x9ba7260);
  thread.runningGeneration = thread.generation;
  thread.stepCount = 12;
  thread.statusCount = 11;
  thread.lastStepAtMs = 7_500;
  thread.lastStatusAtMs = 7_000;
  thread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
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

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(worker);
  expect(thread.runningGeneration).toBe(thread.generation);
  expect(thread.stepCount).toBe(12);
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves a stale unanswered ok running worker when last trace points elsewhere", async () => {
  vi.setSystemTime(new Date(10_000));
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x57fe3d0);
  thread.runningGeneration = thread.generation;
  thread.stepCount = 101;
  thread.statusCount = 100;
  thread.lastStepAtMs = 7_500;
  thread.lastStatusAtMs = 7_500;
  thread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
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

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(worker);
  expect(thread.runningGeneration).toBe(thread.generation);
  expect(thread.stepCount).toBe(101);
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves a stale unanswered no-ready running worker with pending guest writes", async () => {
  vi.setSystemTime(new Date(10_000));
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x9ba7260);
  thread.runningGeneration = thread.generation;
  thread.stepCount = 12;
  thread.statusCount = 11;
  thread.lastStepAtMs = 7_500;
  thread.lastStatusAtMs = 7_000;
  thread.lastStatus = 0;
  thread.pendingGuestMemoryWrites.push({
    guestPtr: 0x4000,
    bytes: new Uint8Array([1, 2, 3]),
  });
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
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

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(worker);
  expect(thread.runningGeneration).toBe(thread.generation);
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves surplus unanswered running work before recovering idle work", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const idleWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x9ba7260);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 70;
  runningThread.statusCount = 11;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = 7_000;
  runningThread.lastStatus = 0;
  const idleThread = makeThreadState(0x6648420);
  idleThread.stepCount = 162;
  idleThread.statusCount = 161;
  idleThread.lastStepAtMs = 7_500;
  idleThread.lastStatusAtMs = 7_499;
  idleThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(idleWorker, idleThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(idleWorker);
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

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(runningThread.stepCount).toBe(70);
  expect(idleThread.runningGeneration).toBeUndefined();
  expect(idleThread.stepCount).toBe(162);
  expect(handle.runningWorker).toBe(runningWorker);
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
  expect(idleWorker.postMessage).not.toHaveBeenCalled();
});

test("blocked retry stale OK clear returns the running worker to ready ownership", () => {
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x5860320);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 42;
  runningThread.statusCount = 42;
  runningThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;

  const scheduler = createRunLoopScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    blockWorker: () => {},
    retryBlockedWorker: () => {},
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(true);
  expect(handle.runningWorker).toBeNull();
  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.readyWorkers).toEqual([runningWorker]);
});

test("pumpProcess runs an OK ready worker after an immediate epoll retry attempt", () => {
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyThread = makeThreadState(0x5860320);
  readyThread.stepCount = 42;
  readyThread.statusCount = 42;
  readyThread.lastStatus = 0;
  const blockedThread = makeThreadState(0x6286130);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(readyWorker, readyThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === blockedThread.vmPtr
    ? kernel.abi_status_epoll_wait()
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, readyWorker);
  handle.workers.add(blockedWorker);
  handle.readyWorkers = [readyWorker];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });

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

  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyThread.runningGeneration).toBe(readyThread.generation);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves an unanswered running worker when the step shares a status millisecond", () => {
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

  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(handle.runningWorker).toBe(runningWorker);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves an unanswered ok running worker when status covers the step time", () => {
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

  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(handle.runningWorker).toBe(runningWorker);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves an unanswered ok running worker when the step is stale", () => {
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

test("pumpProcess clears a stale fork-shared sync source before stale running cleanup", () => {
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
  const forkSharedSync = new Promise<void>(() => {});
  registerForkSharedSyncPromise(forkSharedSync, {
    ownerPid: handle.pid,
    sourceVmPtr: runningThread.vmPtr,
    reason: "owned-ofd",
    targetPids: [5],
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

  expect(handle.forkSharedSyncPromise).toBeNull();
  expect(handle.kernelShare.forkSharedSyncPromise).toBeNull();
  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess clears a faulted running worker before dispatching a ready sibling", () => {
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
  kernel.thread_status = (vmPtr: number) =>
    vmPtr === runningThread.vmPtr ? kernel.abi_status_fault() : kernel.abi_status_ok();
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

test("pumpProcess completes exit when all canonical threads fault after workers were released", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (_vmPtr: number, slot: number) => {
    if (slot === 0) {
      return 0x581f470;
    }
    if (slot === 1) {
      return 0x6645d48;
    }
    return 0;
  };
  kernel.thread_status = () => kernel.abi_status_fault();
  kernel.thread_exit_code = () => 1;
  kernel.thread_term_signal = () => 0;
  const handle = makeHandle(kernel, worker);
  handle.workers.clear();
  handle.readyWorkers = [];
  handle.runningWorker = null;
  handle.blockedWorkers.clear();
  const appendRuntimeLifecycleEvent = vi.fn();
  const completeProcessExit = vi.fn(async () => {});

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent,
    threadWorkerStates,
    completeProcessExit,
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(completeProcessExit).toHaveBeenCalledWith(handle, 1, 0, 0x581f470);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    1,
    "complete-exit-request:no-workers-fault:1:vm=0x581f470:exit=1:0:priority=0:threads=2",
  );
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("blocked retry cleanup clears a running worker whose kernel process pointer disappeared", () => {
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x581f470);
  const blockedThread = makeThreadState(0x6648420);
  runningThread.runningGeneration = runningThread.generation;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.thread_process_ptr = (vmPtr: number) => vmPtr === runningThread.vmPtr ? 0 : 0x2000;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_wait4(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });
  const appendRuntimeLifecycleEvent = vi.fn();
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent,
    blockWorker: vi.fn(),
    retryBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(true);

  expect(handle.runningWorker).toBeNull();
  expect(runningThread.runningGeneration).toBeNull();
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    1,
    "running-worker-cleared:1:stale-process:0x581f470",
  );
});

test("epoll retry clears inactive answered pipe running worker", () => {
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x70bf0d8);
  const blockedThread = makeThreadState(0x7141c68);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 2;
  runningThread.statusCount = 2;
  runningThread.lastStatus = 6;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_pipe_wait();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });
  const appendRuntimeLifecycleEvent = vi.fn();
  const blockWorker = vi.fn();
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent,
    blockWorker,
    retryBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(true);

  expect(handle.runningWorker).toBeNull();
  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.readyWorkers).not.toContain(runningWorker);
  expect(blockWorker).not.toHaveBeenCalled();
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    1,
    "running-worker-cleared:1:answered-blocking-status=6:0x70bf0d8",
  );
});

test("epoll retry clears answered pipe running worker without active-step export", () => {
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x7127a90);
  const blockedThread = makeThreadState(0x7141c68);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 5;
  runningThread.statusCount = 5;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_pipe_wait();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });
  const appendRuntimeLifecycleEvent = vi.fn();
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent,
    blockWorker: vi.fn(),
    retryBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(true);

  expect(handle.runningWorker).toBeNull();
  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.readyWorkers).not.toContain(runningWorker);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    1,
    "running-worker-cleared:1:answered-blocking-status=6:0x7127a90",
  );
});

test("epoll retry keeps unanswered futex running worker when last trace points elsewhere", () => {
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x7127a90);
  const blockedThread = makeThreadState(0x7141c68);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 2;
  runningThread.statusCount = 1;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_futex_wait();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });
  const appendRuntimeLifecycleEvent = vi.fn();
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent,
    blockWorker: vi.fn(),
    retryBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(false);

  expect(handle.runningWorker).toBe(runningWorker);
  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(handle.readyWorkers).not.toContain(runningWorker);
  expect(appendRuntimeLifecycleEvent).not.toHaveBeenCalled();
});

test("epoll retry clears stale unanswered ok running worker", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x7127a90);
  const blockedThread = makeThreadState(0x7141c68);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 688;
  runningThread.statusCount = 687;
  runningThread.lastStatus = 0;
  runningThread.lastStatusAtMs = 7_000;
  runningThread.lastStepAtMs = 7_500;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });
  const appendRuntimeLifecycleEvent = vi.fn();
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent,
    blockWorker: vi.fn(),
    retryBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(true);

  expect(handle.runningWorker).toBeNull();
  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.readyWorkers).toContain(runningWorker);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    1,
    "running-worker-cleared:1:inactive-unanswered-ok:0x7127a90:unanswered=1",
  );
});

test("epoll retry keeps unanswered futex running worker with a blocked record", () => {
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x7127a90);
  const blockedThread = makeThreadState(0x7141c68);
  const stackPointer = 0xa467710n;
  const stackLocalFutexAddr = 0xa467878n;
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 2;
  runningThread.statusCount = 1;
  runningThread.lastStatusFutexWaitAddr = stackLocalFutexAddr;
  runningThread.lastStatusFutexBlockedAddr = stackLocalFutexAddr;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_futex_wait();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  kernel.thread_sp = (vmPtr: number) => (
    vmPtr === runningThread.vmPtr ? stackPointer : 0x8000n
  );
  kernel.thread_futex_wait_addr = () => 0n;
  kernel.thread_record_blocked_addr = () => 0n;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });
  const appendRuntimeLifecycleEvent = vi.fn();
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent,
    blockWorker: vi.fn(),
    retryBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(false);

  expect(handle.runningWorker).toBe(runningWorker);
  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(runningThread.waitingInWorkerStatus).toBeNull();
  expect(handle.readyWorkers).not.toContain(runningWorker);
  expect(appendRuntimeLifecycleEvent).not.toHaveBeenCalled();
});

test("epoll retry clears answered ok running worker before skip", () => {
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x70c6578);
  const blockedThread = makeThreadState(0x7141c68);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 27;
  runningThread.statusCount = 27;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_ok();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });
  const appendRuntimeLifecycleEvent = vi.fn();
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent,
    blockWorker: vi.fn(),
    retryBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(true);

  expect(handle.runningWorker).toBeNull();
  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.readyWorkers).toContain(runningWorker);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    1,
    "running-worker-cleared:1:answered-ok:0x70c6578",
  );
});

test("epoll retry clears surplus stale unanswered ok running worker before skip", () => {
  vi.setSystemTime(10_000);
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x585f220);
  const blockedThread = makeThreadState(0x6285030);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 827;
  runningThread.statusCount = 825;
  runningThread.lastStatus = 0;
  runningThread.lastStepAtMs = 7_100;
  runningThread.lastStatusAtMs = 7_200;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_ok();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0xdeadbeef;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });
  const appendRuntimeLifecycleEvent = vi.fn();
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent,
    blockWorker: vi.fn(),
    retryBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(true);

  expect(handle.runningWorker).toBeNull();
  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.readyWorkers).toContain(runningWorker);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    1,
    "running-worker-cleared:1:surplus-stale-unanswered-ok:0x585f220:unanswered=2",
  );
});

test("epoll retry clears inactive surplus unanswered ok running worker before stale age", () => {
  vi.setSystemTime(10_000);
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x5860320);
  const blockedThread = makeThreadState(0x6285030);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 91;
  runningThread.statusCount = 89;
  runningThread.lastStatus = 0;
  runningThread.lastStatusAtMs = 9_800;
  runningThread.lastStepAtMs = 9_900;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_ok();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });
  const appendRuntimeLifecycleEvent = vi.fn();
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent,
    blockWorker: vi.fn(),
    retryBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(true);

  expect(handle.runningWorker).toBeNull();
  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.readyWorkers).toContain(runningWorker);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    1,
    "running-worker-cleared:1:inactive-surplus-unanswered-ok:0x5860320:unanswered=2",
  );
});

test("epoll retry clears inactive single unanswered ok running worker before stale age", () => {
  vi.setSystemTime(10_000);
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x5860320);
  const blockedThread = makeThreadState(0x6285030);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 83;
  runningThread.statusCount = 82;
  runningThread.lastStatus = 0;
  runningThread.lastStatusAtMs = 9_500;
  runningThread.lastStepAtMs = 9_600;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_ok();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.readyWorkers = [];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });
  const appendRuntimeLifecycleEvent = vi.fn();
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent,
    blockWorker: vi.fn(),
    retryBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(true);

  expect(handle.runningWorker).toBeNull();
  expect(runningThread.runningGeneration).toBeNull();
  expect(handle.readyWorkers).toContain(runningWorker);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    1,
    "running-worker-cleared:1:inactive-unanswered-ok:0x5860320:unanswered=1",
  );
});

test("epoll retry keeps active unanswered ok running worker", () => {
  vi.setSystemTime(10_000);
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x585f220);
  const blockedThread = makeThreadState(0x6285030);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 827;
  runningThread.statusCount = 825;
  runningThread.lastStatus = 0;
  runningThread.lastStatusAtMs = 7_000;
  runningThread.lastStepAtMs = 7_100;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_ok();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => runningThread.vmPtr;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });
  const appendRuntimeLifecycleEvent = vi.fn();
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    appendRuntimeLifecycleEvent,
    blockWorker: vi.fn(),
    retryBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(false);

  expect(handle.runningWorker).toBe(runningWorker);
  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(appendRuntimeLifecycleEvent).not.toHaveBeenCalled();
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
  kernel.thread_process_ptr = (vmPtr: number) => vmPtr === 0x5678 ? 0x2000 : 0;
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

test("pruning preserves a worker whose thread state still belongs to the process", () => {
  const detachedWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(detachedWorker, makeThreadState(0x1234));
  threadWorkerStates.set(anchorWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  kernel.thread_process_ptr = (vmPtr: number) => (
    vmPtr === 0x1234 || vmPtr === 0x5678 ? 0x2000 : 0
  );
  const handle = makeHandle(kernel, detachedWorker);
  handle.workers.add(anchorWorker);
  handle.runningWorker = detachedWorker;
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

  expect(handle.runningWorker).toBe(detachedWorker);
  expect(handle.workers.has(detachedWorker)).toBe(true);
  expect(handle.workers.has(anchorWorker)).toBe(true);
  expect(detachedWorker.terminate).not.toHaveBeenCalled();
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
      { fd: 4, ofdId: 99, fdFlags: 2 },
      { fd: 5, ofdId: 0, fdFlags: 0 },
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

test("pumpProcess skips a ready worker that is still in canonical futex wait", () => {
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
  kernel.thread_status = (vmPtr: number) => vmPtr === waitingThread.vmPtr
    ? kernel.abi_status_futex_wait()
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, waitingWorker);
  handle.workers.add(runnableWorker);
  handle.readyWorkers = [waitingWorker, runnableWorker];

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
});

test("pumpProcess skips a ready worker whose thread record is still futex-blocked", () => {
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
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.thread_record_blocked_addr = (vmPtr: number) => vmPtr === waitingThread.vmPtr
    ? 0xa46787cn
    : 0n;
  kernel.thread_futex_wait_addr = (vmPtr: number) => vmPtr === waitingThread.vmPtr
    ? 0xa46787cn
    : 0n;
  const handle = makeHandle(kernel, waitingWorker);
  handle.workers.add(runnableWorker);
  handle.readyWorkers = [waitingWorker, runnableWorker];

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
  expect(handle.runningWorker).toBe(runnableWorker);
  expect(waitingWorker.postMessage).not.toHaveBeenCalled();
  expect(runnableWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("pumpProcess clears stale futex record when worker-local wait already resolved", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const sleeperWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x1234);
  const sleeperThread = makeThreadState(0x5678);
  waitingThread.waitingInWorkerStatus = 20;
  waitingThread.lastStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(sleeperWorker, sleeperThread);
  const { exports: kernel } = makeKernel();
  let waitingBlockedRecordAddr = 0xa467830n;
  const completeSupervisorFutexWait = vi.fn((vmPtr: number, completion: number) => {
    expect(vmPtr).toBe(waitingThread.vmPtr);
    expect(completion).toBe(kernel.supervisor_futex_wait_completion_woken());
    waitingBlockedRecordAddr = 0n;
    return kernel.abi_status_ok();
  });
  kernel.supervisor_complete_futex_wait = completeSupervisorFutexWait;
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.thread_futex_wait_addr = () => 0n;
  kernel.thread_record_blocked_addr = (vmPtr: number) => vmPtr === waitingThread.vmPtr
    ? waitingBlockedRecordAddr
    : 0n;
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

  expect(completeSupervisorFutexWait).toHaveBeenCalledOnce();
  expect(waitingThread.waitingInWorkerStatus).toBeNull();
  expect(handle.runningWorker).toBe(waitingWorker);
  expect(waitingWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(sleeperWorker.postMessage).not.toHaveBeenCalled();
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

test("pumpProcess drains only supervisor-selected futex resumes when supervisor queue is available", () => {
  const selectedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const unselectedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const selectedThread = makeThreadState(0x1234);
  const unselectedThread = makeThreadState(0x5678);
  selectedThread.waitingInWorkerStatus = 20;
  unselectedThread.waitingInWorkerStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(selectedWorker, selectedThread);
  threadWorkerStates.set(unselectedWorker, unselectedThread);
  const { exports: kernel } = makeKernel();
  let resumePolled = false;
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.thread_record_blocked_addr = () => 0n;
  kernel.thread_futex_wait_addr = () => 0n;
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_futex_wake = () => 1;
  kernel.kernel_scratch_base = () => 0;
  kernel.supervisor_next_resume = vi.fn((outPtr: number, outLen: number) => {
    if (resumePolled) {
      return 0;
    }
    expect(outPtr).toBe(0);
    expect(outLen).toBe(48);
    const view = new DataView(kernel.memory.buffer);
    view.setUint32(0, 0x5255_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 48, true);
    view.setUint32(12, 1, true);
    view.setBigUint64(16, 1n, true);
    view.setUint32(24, selectedThread.vmPtr, true);
    view.setUint32(28, 2, true);
    view.setUint32(32, 0x2000, true);
    view.setUint32(36, kernel.abi_status_ok(), true);
    view.setBigUint64(40, 0xfeedn, true);
    resumePolled = true;
    return 48;
  });
  const handle = makeHandle(kernel, selectedWorker);
  kernel.memory = handle.memory;
  handle.workers.add(unselectedWorker);

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

  expect(kernel.supervisor_next_resume).toHaveBeenCalledTimes(2);
  expect(kernel.supervisor_complete_futex_wait).not.toHaveBeenCalled();
  expect(selectedThread.waitingInWorkerStatus).toBeNull();
  expect(unselectedThread.waitingInWorkerStatus).toBe(kernel.abi_status_futex_wait());
  expect(handle.runningWorker).toBe(selectedWorker);
  expect(selectedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(unselectedWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess records supervisor thread-state snapshot diagnostics for selected futex resumes", () => {
  const selectedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const unselectedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const selectedThread = makeThreadState(0x1234);
  const unselectedThread = makeThreadState(0x5678);
  selectedThread.waitingInWorkerStatus = 20;
  unselectedThread.waitingInWorkerStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(selectedWorker, selectedThread);
  threadWorkerStates.set(unselectedWorker, unselectedThread);
  const { exports: kernel } = makeKernel();
  let resumePolled = false;
  kernel.thread_status = (vmPtr: number) => vmPtr === selectedThread.vmPtr
    ? kernel.abi_status_ok()
    : kernel.abi_status_futex_wait();
  kernel.thread_record_blocked_addr = (vmPtr: number) => vmPtr === unselectedThread.vmPtr ? 0xbeefn : 0n;
  kernel.thread_futex_wait_addr = (vmPtr: number) => vmPtr === unselectedThread.vmPtr ? 0xbeefn : 0n;
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_futex_wake = () => 1;
  kernel.supervisor_thread_state_snapshot_bytes = () => 2080;
  kernel.kernel_scratch_base = () => 0;
  kernel.supervisor_next_resume = vi.fn((outPtr: number, outLen: number) => {
    if (resumePolled) {
      return 0;
    }
    expect(outPtr).toBe(0);
    expect(outLen).toBe(48);
    const view = new DataView(kernel.memory.buffer);
    view.setUint32(0, 0x5255_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 48, true);
    view.setUint32(12, 1, true);
    view.setBigUint64(16, 1n, true);
    view.setUint32(24, selectedThread.vmPtr, true);
    view.setUint32(28, 2, true);
    view.setUint32(32, 0x2000, true);
    view.setUint32(36, kernel.abi_status_ok(), true);
    view.setBigUint64(40, 0xfeedn, true);
    resumePolled = true;
    return 48;
  });
  kernel.supervisor_thread_state_snapshot = vi.fn((vmPtr: number, outPtr: number, outLen: number) => {
    expect(vmPtr).toBe(selectedThread.vmPtr);
    expect(outPtr).toBe(0);
    expect(outLen).toBe(2080);
    const view = new DataView(kernel.memory.buffer);
    view.setUint32(0, 0x4854_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 2080, true);
    view.setUint32(12, 64, true);
    view.setUint32(16, 0x2000, true);
    view.setUint32(20, selectedThread.vmPtr, true);
    view.setUint32(24, 32, true);
    view.setUint32(28, 0, true);
    view.setUint32(32, 2, true);
    view.setUint32(36, 2, true);
    view.setUint32(40, selectedThread.vmPtr, true);
    view.setUint32(44, kernel.abi_status_ok(), true);
    view.setBigUint64(48, 0n, true);
    view.setBigUint64(56, 0n, true);
    view.setUint32(64, 3, true);
    view.setUint32(68, 3, true);
    view.setUint32(72, unselectedThread.vmPtr, true);
    view.setUint32(76, kernel.abi_status_futex_wait(), true);
    view.setBigUint64(80, 0xbeefn, true);
    view.setBigUint64(88, 0x44n, true);
    return 2080;
  });
  const handle = makeHandle(kernel, selectedWorker);
  kernel.memory = handle.memory;
  handle.workers.add(unselectedWorker);
  const debugEvents: string[] = [];
  const queue = createWorkerSchedulerQueueHelpers({
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: (_handle, event) => {
      debugEvents.push(event);
    },
    blockWorker: () => {},
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    handleWorkerMessage: () => {},
    threadWorkerStates,
  });

  queue.pumpProcess(handle);

  expect(kernel.supervisor_thread_state_snapshot).toHaveBeenCalledTimes(1);
  expect(selectedThread.waitingInWorkerStatus).toBeNull();
  expect(unselectedThread.waitingInWorkerStatus).toBe(kernel.abi_status_futex_wait());
  expect(debugEvents).toContain(
    "supervisor-resume:futex-wake:ready:0x1234:tid=2:seq=1:addr=0xfeed:status=0"
      + ":snapshot=process=0x2000:owner=0x1234"
      + ":threads=2/2/0x1234/0/0x0,3/3/0x5678/20/0xbeef/sig=0x44",
  );
});

test("pumpProcess runs supervisor-selected runnable worker instead of TypeScript ready priority", () => {
  const selectedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const unselectedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const selectedThread = makeThreadState(0x1234);
  const unselectedThread = makeThreadState(0x5678);
  selectedThread.lastStatus = 26;
  selectedThread.yieldedReady = true;
  unselectedThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(selectedWorker, selectedThread);
  threadWorkerStates.set(unselectedWorker, unselectedThread);
  const { exports: kernel } = makeKernel();
  kernel.kernel_scratch_base = () => 0;
  kernel.supervisor_runnable_selection_bytes = () => 48;
  kernel.supervisor_select_runnable = vi.fn((
    vmPtr: number,
    candidatesPtr: number,
    candidateCount: number,
    outPtr: number,
    outLen: number,
  ) => {
    expect(vmPtr).toBe(0x1234);
    expect(candidatesPtr).toBe(48);
    expect(candidateCount).toBe(2);
    expect(outPtr).toBe(0);
    expect(outLen).toBe(48);
    const view = new DataView(kernel.memory.buffer);
    expect(view.getUint32(candidatesPtr, true)).toBe(selectedThread.vmPtr);
    expect(view.getUint32(candidatesPtr + 4, true)).toBe(unselectedThread.vmPtr);
    view.setUint32(outPtr, 0x4e52_5354, true);
    view.setUint32(outPtr + 4, 1, true);
    view.setUint32(outPtr + 8, 48, true);
    view.setUint32(outPtr + 12, 0, true);
    view.setBigUint64(outPtr + 16, 1n, true);
    view.setUint32(outPtr + 24, selectedThread.vmPtr, true);
    view.setUint32(outPtr + 28, 2, true);
    view.setUint32(outPtr + 32, 0x2000, true);
    view.setUint32(outPtr + 36, 2, true);
    view.setUint32(outPtr + 40, kernel.abi_status_ok(), true);
    view.setUint32(outPtr + 44, 1, true);
    return 48;
  });
  const handle = makeHandle(kernel, selectedWorker);
  kernel.memory = handle.memory;
  handle.workers.add(unselectedWorker);
  handle.readyWorkers = [selectedWorker, unselectedWorker];
  const debugEvents: string[] = [];
  const queue = createWorkerSchedulerQueueHelpers({
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: (_handle, event) => {
      debugEvents.push(event);
    },
    blockWorker: () => {},
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    handleWorkerMessage: () => {},
    threadWorkerStates,
  });

  queue.pumpProcess(handle);

  expect(kernel.supervisor_select_runnable).toHaveBeenCalledTimes(1);
  expect(handle.runningWorker).toBe(selectedWorker);
  expect(handle.readyWorkers).toEqual([unselectedWorker]);
  expect(selectedThread.yieldedReady).toBe(false);
  expect(selectedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(unselectedWorker.postMessage).not.toHaveBeenCalled();
  expect(debugEvents).toContain(
    "supervisor-runnable:selected:0x1234:tid=2:seq=1:slot=1:state=2:status=0",
  );
});

test("pumpProcess runs blocked-RPC continuation when supervisor selects no runnable worker", () => {
  const ordinaryWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const continuationWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const ordinaryThread = makeThreadState(0x1234);
  const continuationThread = makeThreadState(0x5678);
  continuationThread.pendingPipeSlotsForNextStep.push({
    pipeSlot: 1,
    slot: new Uint8Array(16),
  } as never);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(ordinaryWorker, ordinaryThread);
  threadWorkerStates.set(continuationWorker, continuationThread);
  const { exports: kernel } = makeKernel();
  ordinaryThread.lastStatus = kernel.abi_status_ok();
  continuationThread.lastStatus = kernel.abi_status_pipe_wait();
  kernel.kernel_scratch_base = () => 0;
  kernel.supervisor_runnable_selection_bytes = () => 48;
  kernel.supervisor_select_runnable = vi.fn((
    vmPtr: number,
    candidatesPtr: number,
    candidateCount: number,
    outPtr: number,
    outLen: number,
  ) => {
    expect(vmPtr).toBe(0x1234);
    expect(candidatesPtr).toBe(48);
    expect(candidateCount).toBe(2);
    expect(outPtr).toBe(0);
    expect(outLen).toBe(48);
    const view = new DataView(kernel.memory.buffer);
    expect(view.getUint32(candidatesPtr, true)).toBe(ordinaryThread.vmPtr);
    expect(view.getUint32(candidatesPtr + 4, true)).toBe(continuationThread.vmPtr);
    return 0;
  });
  const handle = makeHandle(kernel, ordinaryWorker);
  kernel.memory = handle.memory;
  handle.workers.add(continuationWorker);
  handle.readyWorkers = [ordinaryWorker, continuationWorker];
  const debugEvents: string[] = [];
  const queue = createWorkerSchedulerQueueHelpers({
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: (_handle, event) => {
      debugEvents.push(event);
    },
    blockWorker: () => {},
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    handleWorkerMessage: () => {},
    threadWorkerStates,
  });

  queue.pumpProcess(handle);

  expect(kernel.supervisor_select_runnable).toHaveBeenCalledTimes(1);
  expect(handle.runningWorker).toBe(continuationWorker);
  expect(handle.readyWorkers).toEqual([ordinaryWorker]);
  expect(ordinaryWorker.postMessage).not.toHaveBeenCalled();
  expect(continuationWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
    pipeSlots: expect.arrayContaining([expect.objectContaining({ pipeSlot: 1 })]),
  }));
  expect(debugEvents).toContain("supervisor-runnable:none:ready=2:candidates=2");
  expect(debugEvents).toContain("pump-run-blocked-rpc-continuation:vm=0x5678:ready=1");
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

test("pumpProcess recovers a quiescent worker-local futex waiter while a sibling is pipe-blocked", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x57fe3d0);
  const blockedThread = makeThreadState(0x7759fb0);
  waitingThread.waitingInWorkerStatus = 20;
  waitingThread.stepCount = 97;
  waitingThread.statusCount = 97;
  waitingThread.lastStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  let waitingStatus = kernel.abi_status_futex_wait();
  const completeSupervisorFutexWait = vi.fn((vmPtr: number, completion: number) => {
    expect(vmPtr).toBe(waitingThread.vmPtr);
    expect(completion).toBe(kernel.supervisor_futex_wait_completion_woken());
    waitingStatus = kernel.abi_status_ok();
    return waitingStatus;
  });
  kernel.supervisor_complete_futex_wait = completeSupervisorFutexWait;
  kernel.thread_status = (vmPtr: number) => {
    if (vmPtr === waitingThread.vmPtr) {
      return waitingStatus;
    }
    if (vmPtr === blockedThread.vmPtr) {
      return kernel.abi_status_pipe_wait();
    }
    return kernel.abi_status_ok();
  };
  const handle = makeHandle(kernel, waitingWorker);
  handle.workers.add(blockedWorker);
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_pipe_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });

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

  expect(completeSupervisorFutexWait).toHaveBeenCalledOnce();
  expect(waitingThread.waitingInWorkerStatus).toBeNull();
  expect(handle.runningWorker).toBe(waitingWorker);
  expect(waitingWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess recovers a quiescent worker-local futex waiter while a sibling is epoll-blocked", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x57fe3d0);
  const blockedThread = makeThreadState(0x7759fb0);
  waitingThread.waitingInWorkerStatus = 20;
  waitingThread.stepCount = 97;
  waitingThread.statusCount = 97;
  waitingThread.lastStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  let waitingStatus = kernel.abi_status_futex_wait();
  const completeSupervisorFutexWait = vi.fn((vmPtr: number, completion: number) => {
    expect(vmPtr).toBe(waitingThread.vmPtr);
    expect(completion).toBe(kernel.supervisor_futex_wait_completion_woken());
    waitingStatus = kernel.abi_status_ok();
    return waitingStatus;
  });
  kernel.supervisor_complete_futex_wait = completeSupervisorFutexWait;
  kernel.thread_status = (vmPtr: number) => {
    if (vmPtr === waitingThread.vmPtr) {
      return waitingStatus;
    }
    if (vmPtr === blockedThread.vmPtr) {
      return kernel.abi_status_epoll_wait();
    }
    return kernel.abi_status_ok();
  };
  const handle = makeHandle(kernel, waitingWorker);
  handle.workers.add(blockedWorker);
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });

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

  expect(completeSupervisorFutexWait).toHaveBeenCalledOnce();
  expect(waitingThread.waitingInWorkerStatus).toBeNull();
  expect(handle.runningWorker).toBe(waitingWorker);
  expect(waitingWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess recovers a quiescent worker-local futex waiter under fs rpc pressure without stale clear", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const cloneWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x57fe3d0);
  const cloneThread = makeThreadState(0x5860320);
  const blockedThread = makeThreadState(0x7759fb0);
  waitingThread.waitingInWorkerStatus = 20;
  waitingThread.stepCount = 97;
  waitingThread.statusCount = 97;
  waitingThread.lastStatus = 20;
  cloneThread.lastStatus = 10;
  cloneThread.stepCount = 59;
  cloneThread.statusCount = 59;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(cloneWorker, cloneThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  let waitingStatus = kernel.abi_status_futex_wait();
  const completeSupervisorFutexWait = vi.fn((vmPtr: number, completion: number) => {
    expect(vmPtr).toBe(waitingThread.vmPtr);
    expect(completion).toBe(kernel.supervisor_futex_wait_completion_woken());
    waitingStatus = kernel.abi_status_ok();
    return waitingStatus;
  });
  kernel.supervisor_complete_futex_wait = completeSupervisorFutexWait;
  kernel.thread_status = (vmPtr: number) => {
    if (vmPtr === waitingThread.vmPtr) {
      return waitingStatus;
    }
    if (vmPtr === blockedThread.vmPtr) {
      return kernel.abi_status_fs();
    }
    return kernel.abi_status_ok();
  };
  const handle = makeHandle(kernel, waitingWorker);
  handle.workers.add(cloneWorker);
  handle.workers.add(blockedWorker);
  handle.readyWorkers = [cloneWorker];
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_fs(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: true,
    rpcGeneration: 1,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
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
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(completeSupervisorFutexWait).not.toHaveBeenCalled();
  expect(waitingThread.waitingInWorkerStatus).toBe(kernel.abi_status_futex_wait());
  expect(handle.runningWorker).toBe(cloneWorker);
  expect(handle.readyWorkers).toEqual([]);
  expect(cloneWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 5_000_000,
  }));
  expect(waitingWorker.postMessage).not.toHaveBeenCalled();
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
  expect(lifecycleEvents.filter((event) => event.includes("running-worker-cleared"))).toEqual([]);

  handle.runningWorker = null;
  scheduler.pumpProcess(handle);

  expect(completeSupervisorFutexWait).toHaveBeenCalledOnce();
  expect(waitingThread.waitingInWorkerStatus).toBeNull();
  expect(handle.runningWorker).toBe(waitingWorker);
  expect(waitingWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
  expect(lifecycleEvents.filter((event) => event.includes("running-worker-cleared"))).toEqual([]);
});

test("pumpProcess leaves a quiescent stack-local futex waiter parked with a blocked record", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x57fe3d0);
  const blockedThread = makeThreadState(0x7759fb0);
  const stackPointer = 0xa4676f0n;
  const stackLocalFutexAddr = 0xa467858n;
  waitingThread.waitingInWorkerStatus = 20;
  waitingThread.stepCount = 97;
  waitingThread.statusCount = 97;
  waitingThread.lastStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  let waitingStatus = kernel.abi_status_futex_wait();
  const completeSupervisorFutexWait = vi.fn((vmPtr: number, completion: number) => {
    expect(vmPtr).toBe(waitingThread.vmPtr);
    expect(completion).toBe(kernel.supervisor_futex_wait_completion_woken());
    waitingStatus = kernel.abi_status_ok();
    return waitingStatus;
  });
  kernel.supervisor_complete_futex_wait = completeSupervisorFutexWait;
  kernel.thread_status = (vmPtr: number) => {
    if (vmPtr === waitingThread.vmPtr) {
      return waitingStatus;
    }
    if (vmPtr === blockedThread.vmPtr) {
      return kernel.abi_status_epoll_wait();
    }
    return kernel.abi_status_ok();
  };
  kernel.thread_sp = (vmPtr: number) => (
    vmPtr === waitingThread.vmPtr ? stackPointer : 0x8000n
  );
  kernel.thread_futex_wait_addr = (vmPtr: number) => (
    vmPtr === waitingThread.vmPtr ? stackLocalFutexAddr : 0n
  );
  kernel.thread_record_blocked_addr = (vmPtr: number) => (
    vmPtr === waitingThread.vmPtr ? stackLocalFutexAddr : 0n
  );
  const handle = makeHandle(kernel, waitingWorker);
  handle.workers.add(blockedWorker);
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });

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

  expect(completeSupervisorFutexWait).not.toHaveBeenCalled();
  expect(waitingThread.waitingInWorkerStatus).toBe(kernel.abi_status_futex_wait());
  expect(handle.runningWorker).toBeNull();
  expect(waitingWorker.postMessage).not.toHaveBeenCalled();
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess resumes a quiescent futex mismatch waiter with EAGAIN", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x57fe3d0);
  const blockedThread = makeThreadState(0x7759fb0);
  const futexWaitAddr = 0x100n;
  let blockedRecordAddr = 0x104n;
  let currentFutexWaitAddr = futexWaitAddr;
  waitingThread.waitingInWorkerStatus = 20;
  waitingThread.stepCount = 97;
  waitingThread.statusCount = 97;
  waitingThread.lastStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  let waitingStatus = kernel.abi_status_futex_wait();
  const completeSupervisorFutexWait = vi.fn((vmPtr: number, completion: number) => {
    expect(vmPtr).toBe(waitingThread.vmPtr);
    expect(completion).toBe(kernel.supervisor_futex_wait_completion_invalid_address());
    currentFutexWaitAddr = 0n;
    blockedRecordAddr = 0n;
    waitingStatus = kernel.abi_status_ok();
    return waitingStatus;
  });
  kernel.supervisor_complete_futex_wait = completeSupervisorFutexWait;
  kernel.thread_status = (vmPtr: number) => {
    if (vmPtr === waitingThread.vmPtr) {
      return waitingStatus;
    }
    if (vmPtr === blockedThread.vmPtr) {
      return kernel.abi_status_epoll_wait();
    }
    return kernel.abi_status_ok();
  };
  kernel.thread_futex_wait_addr = (vmPtr: number) => (
    vmPtr === waitingThread.vmPtr ? currentFutexWaitAddr : 0n
  );
  kernel.thread_futex_wait_expected = (vmPtr: number) => (
    vmPtr === waitingThread.vmPtr ? 0 : 0
  );
  kernel.thread_record_blocked_addr = (vmPtr: number) => (
    vmPtr === waitingThread.vmPtr ? blockedRecordAddr : 0n
  );
  const handle = makeHandle(kernel, waitingWorker);
  new Int32Array(handle.memory.buffer)[Number(futexWaitAddr >> 2n)] = 2;
  handle.workers.add(blockedWorker);
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });

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

  expect(completeSupervisorFutexWait).toHaveBeenCalledOnce();
  expect(waitingThread.waitingInWorkerStatus).toBeNull();
  expect(handle.runningWorker).toBe(waitingWorker);
  expect(waitingWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess leaves a quiescent worker-local futex waiter parked until its record is woken", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x57fe3d0);
  const blockedThread = makeThreadState(0x7759fb0);
  waitingThread.waitingInWorkerStatus = 20;
  waitingThread.stepCount = 97;
  waitingThread.statusCount = 97;
  waitingThread.lastStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => {
    if (vmPtr === waitingThread.vmPtr) {
      return kernel.abi_status_futex_wait();
    }
    if (vmPtr === blockedThread.vmPtr) {
      return kernel.abi_status_pipe_wait();
    }
    return kernel.abi_status_ok();
  };
  kernel.thread_record_blocked_addr = (vmPtr: number) => (
    vmPtr === waitingThread.vmPtr ? 0x162c1d0n : 0n
  );
  kernel.thread_futex_wait_addr = (vmPtr: number) => (
    vmPtr === waitingThread.vmPtr ? 0x162c1d0n : 0n
  );
  const handle = makeHandle(kernel, waitingWorker);
  handle.workers.add(blockedWorker);
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_pipe_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
  });

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

  expect(kernel.supervisor_complete_futex_wait).not.toHaveBeenCalled();
  expect(waitingThread.waitingInWorkerStatus).toBe(kernel.abi_status_futex_wait());
  expect(handle.runningWorker).toBeNull();
  expect(waitingWorker.postMessage).not.toHaveBeenCalled();
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
});

test("blockWorker schedules initial kernel-worker pipe retry before passive parking", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x70bf0d8);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.abi_sys_read = () => 63n;
  kernel.thread_blocked_syscall_nr = (vmPtr: number) => (
    vmPtr === thread.vmPtr ? 63n : 0n
  );
  kernel.thread_blocked_pipe_read_ready = () => 0;
  kernel.thread_blocked_pipe_write_ready = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn() as never;

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

  scheduler.blockWorker(handle, worker, kernel.abi_status_pipe_wait(), thread.vmPtr);

  const blocked = handle.blockedWorkers.get(worker);
  expect(blocked?.pipeSyncedBeforePassive).toBe(false);
  expect(blocked?.retryTimer).not.toBeNull();
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("kickBlockedWorkers clears answered futex-wait running sibling before pipe retry", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x57fe3d0);
  const blockedThread = makeThreadState(0x7759fb0);
  const now = Date.now();
  waitingThread.runningGeneration = waitingThread.generation;
  waitingThread.stepCount = 5278;
  waitingThread.statusCount = 10457;
  waitingThread.lastStepAtMs = now;
  waitingThread.lastStatusAtMs = now;
  waitingThread.lastStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => {
    if (vmPtr === blockedThread.vmPtr) {
      return kernel.abi_status_pipe_wait();
    }
    return kernel.abi_status_ok();
  };
  kernel.process_thread_vm_ptr = (_vmPtr: number, slot: number) => {
    if (slot === 0) {
      return waitingThread.vmPtr;
    }
    if (slot === 1) {
      return blockedThread.vmPtr;
    }
    return 0;
  };
  const handle = makeHandle(kernel, waitingWorker);
  handle.workers.add(blockedWorker);
  handle.runningWorker = waitingWorker;
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn() as never;
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_pipe_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
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

  scheduler.kickBlockedWorkers(handle);

  expect(handle.runningWorker).toBeNull();
  expect(waitingThread.runningGeneration).toBeNull();
  expect(lifecycleEvents.join("\n")).toContain("pipe-retry-rpc:1");
  expect(resumeBlockedSyscallLocally).toHaveBeenCalledWith(
    handle,
    blockedWorker,
    blockedThread.vmPtr,
    kernel.abi_status_pipe_wait(),
    false,
  );
});

test("pipe rpc blocked outcome pumps quiescent futex waiter", async () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x58216f0);
  const blockedThread = makeThreadState(0x70bf0c8);
  waitingThread.waitingInWorkerStatus = 20;
  waitingThread.waitingInWorkerReconcileDeferred = true;
  waitingThread.lastStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  let waitingStatus = kernel.abi_status_futex_wait();
  const completeSupervisorFutexWait = vi.fn((vmPtr: number, completion: number) => {
    expect(vmPtr).toBe(waitingThread.vmPtr);
    expect(completion).toBe(kernel.supervisor_futex_wait_completion_woken());
    waitingStatus = kernel.abi_status_ok();
    return waitingStatus;
  });
  kernel.supervisor_complete_futex_wait = completeSupervisorFutexWait;
  kernel.thread_status = (vmPtr: number) => {
    if (vmPtr === waitingThread.vmPtr) {
      return waitingStatus;
    }
    if (vmPtr === blockedThread.vmPtr) {
      return kernel.abi_status_pipe_wait();
    }
    return kernel.abi_status_ok();
  };
  kernel.process_thread_vm_ptr = (_vmPtr: number, slot: number) => {
    if (slot === 0) {
      return waitingThread.vmPtr;
    }
    if (slot === 1) {
      return blockedThread.vmPtr;
    }
    return 0;
  };
  const handle = makeHandle(kernel, waitingWorker);
  handle.workers.add(blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn() as never;
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_pipe_wait(),
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    rpcPending: false,
    rpcGeneration: 0,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: true,
    pipeEofChildExitDeferrals: 0,
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
    resumeBlockedSyscallLocally: async () => {},
    resumeBlockedSyscallOutcome: async () => ({
      kind: "blocked",
      status: kernel.abi_status_pipe_wait(),
    }),
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.kickBlockedWorkers(handle);
  await Promise.resolve();

  expect(lifecycleEvents.join("\n")).toContain("pipe-retry-rpc:1");
  expect(completeSupervisorFutexWait).toHaveBeenCalledOnce();
  expect(waitingThread.waitingInWorkerStatus).toBeNull();
  expect(handle.runningWorker).toBe(waitingWorker);
  expect(waitingWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
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

test("enqueue repair retry clears a stale-process running worker before stepping ready work", async () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x1234);
  const readyThread = makeThreadState(0x5678);
  runningThread.runningGeneration = runningThread.generation;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_process_ptr = (vmPtr: number) => vmPtr === runningThread.vmPtr ? 0 : 0x2000;
  const handle = makeHandle(kernel, runningWorker);
  handle.workers.add(readyWorker);
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

  scheduler.enqueueWorker(handle, readyWorker);

  expect(handle.runningWorker).toBe(readyWorker);
  expect(runningThread.runningGeneration).toBeNull();
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));

  await vi.advanceTimersByTimeAsync(2_000);

  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledTimes(1);
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

test("connect timeout delegates to kernel-worker blocked syscall outcome when available", async () => {
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
  handle.kernelWorkerRequest = vi.fn(async () => ({ type: "unexpected" }) as never);
  const runtimeEvents: string[] = [];
  const resumeBlockedSyscallOutcome = vi.fn(async () => {
    status = 0;
    return { kind: "completed" as const, status };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => runtimeEvents.push(event),
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeBlockedSyscallOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_connect(), 0x1234);
  await vi.advanceTimersByTimeAsync(30);
  await Promise.resolve();
  await Promise.resolve();

  expect(resumeBlockedSyscallOutcome).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_connect(),
    true,
  );
  expect(resumeSocketTimeout).not.toHaveBeenCalled();
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(runtimeEvents).toContain(`socket-retry-timeout-rpc:${handle.pid}:status=${handle.kernel.abi_status_connect()}`);
  expect(handle.blockedWorkers.has(worker)).toBe(false);
});
