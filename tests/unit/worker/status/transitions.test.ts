import { afterEach, expect, test, vi } from "vite-plus/test";
import { createWorkerStatusTransitionHandler } from "../../../../src/worker/message-handler/transitions.js";

function makeKernelState() {
  return {
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
  };
}

function makeHandle(worker: Worker) {
  return {
    pid: 0,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelState: makeKernelState(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    workerDebugLog: [],
    lastDebugMessage: undefined,
    pendingChildSpawnRecords: 0,
    pendingChildExitRecords: 0,
    vforkSnapshot: null,
    vforkWorker: null,
    runningWorker: null,
    outputBacklog: [],
    stdinQueue: [],
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    kernel: {
      abi_status_ok: () => 0,
      abi_status_fault: () => 3,
      abi_status_fs: () => 6,
      abi_status_futex_wait: () => 20,
      abi_status_pipe_wait: () => 21,
      abi_status_epoll_wait: () => 22,
      abi_status_vfork_wait: () => 23,
      abi_status_execve_spawn: () => 24,
      abi_status_clone_spawn: () => 25,
      abi_status_sleep: () => 26,
      abi_status_connect: () => 28,
      abi_status_socket_send: () => 29,
      abi_status_socket_recv: () => 30,
      abi_status_wait4: () => 31,
      abi_status_flock_wait: () => 32,
      abi_eagain: () => -11,
      abi_econnrefused: () => 111,
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
      thread_a0: () => 0n,
      thread_sleep_seconds: () => 0n,
      thread_sleep_nanoseconds: () => 0n,
      thread_resume_sleep: vi.fn(),
      abi_max_threads: () => 1,
      process_current_pid: () => 0,
      process_thread_vm_ptr: () => 0x1234,
      thread_process_ptr: () => 0,
      thread_process_thread_count: () => 1,
      process_fd_ofd_id: () => 0,
      kernel_ofd_fd_type: () => 0,
      abi_fd_type_socket: () => 8,
      abi_max_fds: () => 8,
      thread_status: () => 0,
      thread_spawn_from_clone: vi.fn(() => 0x2000),
    },
  };
}

function makeDeps(worker: Worker) {
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    ownerPid: number;
    ownerAsyncStatus: number | null;
    generation: number;
    waitingInWorkerStatus: number | null;
    resumeWorkersOnReady: Worker[];
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    ownerPid: 0,
    ownerAsyncStatus: null,
    generation: 1,
    waitingInWorkerStatus: null,
    resumeWorkersOnReady: [],
  });

  return {
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    attachThreadWorker: vi.fn(),
    blockWorker: vi.fn(),
    canonicalizeActiveVforkChildViaKernelWorker: vi.fn(async () => {}),
    captureVforkSnapshot: vi.fn(),
    clearVforkSnapshot: vi.fn(),
    clearBlockedWorker: vi.fn(),
    completeProcessExit: vi.fn(async () => {}),
    commitNetworkConnect: vi.fn(async () => {}),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn(),
    handleExecveSpawn: vi.fn(async () => {}),
    kickBlockedWorkers: vi.fn(),
    pumpProcess: vi.fn(),
    pumpNetworkConnections: vi.fn(async () => false),
    pruneDetachedProcessWorkers: vi.fn(),
    processHandles: new Map(),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestPipeStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    resolveNetworkConnect: vi.fn(async () => 0n),
    resumeConnectViaKernelWorker: vi.fn(async () => {}),
    resumeFsViaKernelWorker: vi.fn(async () => {}),
    resumeVforkParentAfterChildExit: vi.fn(() => false),
    statusDefersOwnedStateSync: vi.fn(() => false),
    statusOkCompletedSyncEffects: 0,
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncEffectEpollDependentOfdState: 0,
    syncEffectEpollState: 0,
    syncEffectForkSharedOfdState: 0,
    syncEffectProcessMemfs: 0,
    syncEffectPipeState: 0,
    syncEffectProcessLifecycle: 0,
    syncEffectSignalWaiters: 0,
    syncEffectSocketState: 0,
    syncForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    syncProcessMemfsViaKernelWorker: vi.fn(async () => {}),
  };
}

afterEach(() => {
  vi.useRealTimers();
});

test("STATUS_CONNECT restores the runtime network bridge from a related handle", async () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.pid = 4;
  handle.runtimeId = 99;
  handle.kernel.process_current_pid = () => 4;
  handle.networkConnections.set(3, {} as never);
  const bridge = {
    connect: vi.fn(),
  };
  const parent = {
    ...makeHandle({} as Worker),
    pid: 1,
    runtimeId: 99,
    networkBridge: bridge,
  };
  handle.kernelShare.members.add(parent as never);
  const deps = makeDeps(worker);
  deps.processHandles.set(1, parent as never);
  deps.processHandles.set(4, handle as never);
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: {
      type: "status",
      status: 28,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    },
    status: 28,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  await vi.waitFor(() => {
    expect(deps.commitNetworkConnect).toHaveBeenCalledWith(handle, 0x1234, 0n);
  });
  expect(deps.resolveNetworkConnect).toHaveBeenCalledWith(handle, 0x1234);
  expect(handle.networkBridge).toBe(bridge);
  expect(deps.resumeConnectViaKernelWorker).toHaveBeenCalledWith(handle, worker, 0x1234, 0n);
  expect(deps.pumpNetworkConnections).not.toHaveBeenCalled();
  expect(deps.blockWorker).not.toHaveBeenCalled();
});

test("STATUS_SOCKET_RECV restores a related runtime network connection before blocking", async () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.pid = 4;
  handle.runtimeId = 99;
  handle.kernel.process_current_pid = () => 4;
  handle.kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => (fd === 3 ? 12 : 0);
  handle.kernel.kernel_ofd_fd_type = (ofdId: number) => (ofdId === 12 ? 8 : 0);
  const connection = {} as never;
  const bridge = {
    connect: vi.fn(),
  };
  const parent = {
    ...makeHandle({} as Worker),
    pid: 1,
    runtimeId: 99,
    networkBridge: bridge,
  };
  parent.networkConnections.set(3, connection);
  parent.kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => (fd === 3 ? 12 : 0);
  parent.kernel.kernel_ofd_fd_type = (ofdId: number) => (ofdId === 12 ? 8 : 0);
  handle.kernelShare.members.add(parent as never);
  const deps = makeDeps(worker);
  deps.processHandles.set(1, parent as never);
  deps.processHandles.set(4, handle as never);
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: {
      type: "status",
      status: 30,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    },
    status: 30,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  await vi.waitFor(() => {
    expect(deps.pumpNetworkConnections).toHaveBeenCalledWith(handle, 0x1234);
  });
  expect(handle.networkBridge).toBe(bridge);
  expect(handle.networkConnections.get(3)).toBe(connection);
  expect(handle.networkConnectionOfds?.get(3)).toBe(12);
  expect(deps.blockWorker).toHaveBeenCalledWith(handle, worker, 30, 0x1234);
});

test("STATUS_SLEEP zero-duration yield kicks blocked workers before requeueing", () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  const deps = makeDeps(worker);
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: {
      type: "status",
      status: 26,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    },
    status: 26,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  expect(deps.kickBlockedWorkers).toHaveBeenCalledWith(handle);
  expect(deps.enqueueWorker).toHaveBeenCalledWith(handle, worker);
  expect(handle.kernel.thread_resume_sleep).toHaveBeenCalledWith(0x1234);
  expect(
    handle.kernel.thread_resume_sleep.mock.invocationCallOrder[0],
  ).toBeLessThan(deps.enqueueWorker.mock.invocationCallOrder[0]);
  expect(
    deps.kickBlockedWorkers.mock.invocationCallOrder[0],
  ).toBeLessThan(deps.enqueueWorker.mock.invocationCallOrder[0]);
});

test("STATUS_SLEEP zero-duration yield lets existing ready work run before requeueing itself", async () => {
  vi.useFakeTimers();
  const worker = {} as Worker;
  const readyWorker = {} as Worker;
  const handle = makeHandle(worker);
  handle.readyWorkers.push(readyWorker);
  const deps = makeDeps(worker);
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: {
      type: "status",
      status: 26,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    },
    status: 26,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  expect(deps.pumpProcess).toHaveBeenCalledWith(handle);
  expect(deps.enqueueWorker).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(0);

  expect(deps.enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("STATUS_SLEEP blocks in the owner scheduler instead of arming a host timer", () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.kernel.thread_sleep_nanoseconds = () => 1_000_000n;
  const deps = makeDeps(worker);
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: {
      type: "status",
      status: 26,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    },
    status: 26,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  expect(deps.blockWorker).toHaveBeenCalledWith(handle, worker, 26, 0x1234);
  expect(deps.kickBlockedWorkers).toHaveBeenCalledWith(handle);
  expect(deps.enqueueWorker).not.toHaveBeenCalled();
  expect(deps.pumpProcess).toHaveBeenCalledWith(handle);
  expect(
    deps.blockWorker.mock.invocationCallOrder[0],
  ).toBeLessThan(deps.kickBlockedWorkers.mock.invocationCallOrder[0]);
  expect(
    deps.kickBlockedWorkers.mock.invocationCallOrder[0],
  ).toBeLessThan(deps.pumpProcess.mock.invocationCallOrder[0]);
});

test("STATUS_CLONE_SPAWN ignores stale duplicate after kernel status moved on", () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.pid = 1;
  handle.runtimeId = 77;
  handle.kernel.thread_status = () => handle.kernel.abi_status_ok();
  handle.kernel.thread_spawn_from_clone = vi.fn(() => 0);
  const deps = makeDeps(worker);
  const transition = createWorkerStatusTransitionHandler(deps as never);

  expect(() => transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: {
      type: "status",
      status: 25,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    },
    status: 25,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  })).not.toThrow();

  expect(handle.kernel.thread_spawn_from_clone).toHaveBeenCalledWith(0x1234);
  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    77,
    "clone-spawn-stale-current:1:0x1234:25->0",
  );
  expect(deps.appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "clone_spawn_stale:0x1234:25->0",
  );
  expect(deps.attachThreadWorker).not.toHaveBeenCalled();
  expect(deps.kickBlockedWorkers).toHaveBeenCalledWith(handle);
  expect(deps.pumpProcess).toHaveBeenCalledWith(handle);
});

test("STATUS_CLONE_SPAWN still fails when spawn handoff is missing at current clone status", () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.pid = 1;
  handle.runtimeId = 77;
  handle.kernel.thread_status = () => handle.kernel.abi_status_clone_spawn();
  handle.kernel.thread_spawn_from_clone = vi.fn(() => 0);
  const deps = makeDeps(worker);
  const transition = createWorkerStatusTransitionHandler(deps as never);

  expect(() => transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: {
      type: "status",
      status: 25,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    },
    status: 25,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  })).toThrow("thread_spawn_from_clone failed for pid=1");

  expect(handle.kernel.thread_spawn_from_clone).toHaveBeenCalledWith(0x1234);
  expect(deps.appendRuntimeLifecycleEvent).not.toHaveBeenCalled();
  expect(deps.attachThreadWorker).not.toHaveBeenCalled();
});

test("STATUS_FS does not sync pipe state before filesystem resume completes", () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  const deps = makeDeps(worker);
  deps.syncEffectPipeState = 1;
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: {
      type: "status",
      status: 6,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    },
    status: 6,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 1,
  });

  expect(deps.requestPipeStateSync).not.toHaveBeenCalled();
  expect(deps.resumeFsViaKernelWorker).toHaveBeenCalledWith(handle, worker, 0x1234);
  expect(deps.kickBlockedWorkers).not.toHaveBeenCalled();
  expect(deps.pumpProcess).not.toHaveBeenCalled();
});

test("STATUS_FS with shared pipe fds resumes filesystem after pipe refresh", async () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.pid = 3;
  handle.kernelWorkerRequest = vi.fn(async () => ({ type: "unused" }));
  handle.kernelShare.members.add(handle as never);
  handle.kernelShare.members.add({ pid: 1 } as never);
  handle.kernel.abi_fd_type_pipe = () => 6;
  handle.kernel.process_fd_ofd_id = (_vmPtr, fd) => fd === 0 ? 1 : 0;
  handle.kernel.kernel_ofd_fd_type = (ofdId) => ofdId === 1 ? 6 : 0;
  const deps = makeDeps(worker);
  let resolvePipeSync: (() => void) | undefined;
  deps.requestPipeStateSync = vi.fn(() => new Promise<void>((resolve) => {
    resolvePipeSync = resolve;
  }));
  const resumeFsViaKernelWorkerOutcome = vi.fn(async () => ({ kind: "completed", status: 0 }));
  (deps as { resumeFsViaKernelWorkerOutcome?: typeof resumeFsViaKernelWorkerOutcome })
    .resumeFsViaKernelWorkerOutcome = resumeFsViaKernelWorkerOutcome;
  const thread = deps.threadWorkerStates.get(worker)!;
  thread.ownerPid = handle.pid;
  thread.ownerAsyncStatus = null;
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: {
      type: "status",
      status: 6,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    },
    status: 6,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  expect(thread.ownerAsyncStatus).toBe(6);
  expect(deps.requestPipeStateSync).toHaveBeenCalledWith(handle, false);
  expect(resumeFsViaKernelWorkerOutcome).not.toHaveBeenCalled();

  resolvePipeSync?.();
  await vi.waitFor(() => {
    expect(resumeFsViaKernelWorkerOutcome).toHaveBeenCalledWith(handle, worker, 0x1234);
  });

  expect(thread.ownerAsyncStatus).toBeNull();
  expect(deps.pumpProcess).toHaveBeenCalledWith(handle);
});

test("STATUS_OK network pump syncs socket state before requeueing", async () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.networkConnections.set(7, {} as never);
  const deps = makeDeps(worker);
  deps.pumpNetworkConnections = vi.fn(async () => true);
  let resolveSocketSync: (() => void) | undefined;
  deps.requestSocketStateSync = vi.fn(() => new Promise<void>((resolve) => {
    resolveSocketSync = resolve;
  }));
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: {
      type: "status",
      status: 0,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    },
    status: 0,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  await Promise.resolve();

  expect(deps.pumpNetworkConnections).toHaveBeenCalledWith(handle, 0x1234);
  expect(deps.requestSocketStateSync).toHaveBeenCalledWith(handle, true);
  expect(deps.enqueueWorker).not.toHaveBeenCalled();

  resolveSocketSync?.();
  for (let i = 0; i < 5; i += 1) {
    await Promise.resolve();
  }

  expect(deps.kickBlockedWorkers).toHaveBeenCalledWith(handle);
  expect(deps.enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("STATUS_OK pipe sync clears owner async state before requeueing", async () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.pid = 7;
  handle.kernelWorkerRequest = vi.fn(async () => ({ type: "pipe-state-synced" }));
  const deps = makeDeps(worker);
  deps.syncEffectPipeState = 1;
  let resolvePipeSync: (() => void) | undefined;
  deps.requestPipeStateSync = vi.fn(() => new Promise<void>((resolve) => {
    resolvePipeSync = resolve;
  }));
  const thread = deps.threadWorkerStates.get(worker)!;
  thread.ownerPid = handle.pid;
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: {
      type: "status",
      status: 0,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    },
    status: 0,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 1,
  });

  await Promise.resolve();

  expect(thread.ownerAsyncStatus).toBe(0);
  expect(deps.requestPipeStateSync).toHaveBeenCalledWith(handle, true);
  expect(deps.enqueueWorker).not.toHaveBeenCalled();

  resolvePipeSync?.();
  await vi.waitFor(() => {
    expect(thread.ownerAsyncStatus).toBeNull();
  });

  expect(deps.kickBlockedWorkers).toHaveBeenCalledWith(handle);
  expect(deps.enqueueWorker).toHaveBeenCalledWith(handle, worker);
});
