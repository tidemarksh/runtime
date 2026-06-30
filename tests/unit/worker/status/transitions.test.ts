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

function writeU32(view: DataView, offset: number, value: number): void {
  view.setUint32(offset, value, true);
}

function writeU64(view: DataView, offset: number, value: bigint): void {
  view.setBigUint64(offset, value, true);
}

function writeCloneSpawnHostOpRecord(
  memory: WebAssembly.Memory,
  ptr: number,
  parentVmPtr: number,
  childVmPtr: number,
): void {
  const view = new DataView(memory.buffer, ptr, 64);
  writeU32(view, 0, 0x4f48_5354);
  writeU32(view, 4, 1);
  writeU32(view, 8, 64);
  writeU32(view, 12, 1);
  writeU64(view, 16, 1n);
  writeU32(view, 24, parentVmPtr);
  writeU32(view, 28, childVmPtr);
  writeU32(view, 32, 2);
  writeU32(view, 36, 0x5000);
  writeU32(view, 40, 0);
  writeU32(view, 44, 0);
  writeU64(view, 48, 0x50f00n);
  writeU64(view, 56, 0x8800n);
}

function writeFaultExitDecisionRecord(
  memory: WebAssembly.Memory,
  ptr: number,
  kind: number,
  vmPtr: number,
  exitVmPtr: number,
  remainingThreads: number,
  liveThreads: number,
): void {
  const view = new DataView(memory.buffer, ptr, 88);
  writeU32(view, 0, 0x5846_5354);
  writeU32(view, 4, 1);
  writeU32(view, 8, 88);
  writeU32(view, 12, kind);
  writeU64(view, 16, 1n);
  writeU32(view, 24, vmPtr);
  writeU32(view, 28, exitVmPtr);
  writeU32(view, 32, 1);
  writeU32(view, 36, 0x5000);
  writeU32(view, 40, remainingThreads);
  writeU32(view, 44, liveThreads);
  view.setInt32(48, 42, true);
  view.setInt32(52, 0, true);
  writeU32(view, 56, 3);
  writeU32(view, 60, 0);
  writeU32(view, 64, 0);
  writeU64(view, 72, 0n);
  writeU64(view, 80, 0n);
}

function writeFdControlRecord(memory: WebAssembly.Memory, ptr: number, seq: bigint): void {
  const view = new DataView(memory.buffer, ptr, 96);
  writeU32(view, 0, 0x4446_5354);
  writeU32(view, 4, 1);
  writeU32(view, 8, 96);
  writeU32(view, 12, 1);
  writeU64(view, 16, seq);
  writeU32(view, 24, 0x1234);
  writeU32(view, 28, 0x2000);
  writeU32(view, 32, 9);
  writeU32(view, 36, 3);
  writeU32(view, 40, 0);
  writeU32(view, 44, 11);
  writeU32(view, 48, 0);
  writeU32(view, 52, 2);
  writeU32(view, 56, 5);
  writeU32(view, 60, 0);
  writeU64(view, 64, 0n);
  writeU64(view, 72, 0x800n);
  writeU64(view, 80, 1n);
  writeU64(view, 88, 0n);
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
  vi.unstubAllGlobals();
});

test("status transitions append supervisor fd-control diagnostics once per sequence", () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.pid = 1;
  const memory = new WebAssembly.Memory({ initial: 8, maximum: 8, shared: true });
  handle.memory = memory;
  let count = 1n;
  let seq = 7n;
  const kernel = handle.kernel as typeof handle.kernel & {
    memory: WebAssembly.Memory;
    supervisor_abi_version: () => number;
    supervisor_fd_control_record_bytes: () => number;
    supervisor_fd_control_kind_pipe_created: () => number;
    supervisor_shadow_fd_control_count: () => bigint;
    supervisor_fd_control_record: (outPtr: number, outLen: number) => number;
  };
  kernel.memory = memory;
  kernel.supervisor_abi_version = () => 1;
  kernel.supervisor_fd_control_record_bytes = () => 96;
  kernel.supervisor_fd_control_kind_pipe_created = () => 1;
  kernel.supervisor_shadow_fd_control_count = () => count;
  kernel.supervisor_fd_control_record = vi.fn((outPtr: number, outLen: number) => {
    expect(outLen).toBe(96);
    writeFdControlRecord(memory, outPtr, seq);
    return 96;
  });
  const deps = makeDeps(worker);
  const transition = createWorkerStatusTransitionHandler(deps as never);
  const statusMessage = {
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  };

  transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: statusMessage,
    status: 0,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });
  transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: statusMessage,
    status: 0,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });
  count = 2n;
  seq = 8n;
  transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: statusMessage,
    status: 0,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  const fdControlLogs = deps.appendWorkerDebugLog.mock.calls
    .map(([, event]) => event)
    .filter((event) => String(event).startsWith("supervisor-fd-control:"));
  expect(fdControlLogs).toEqual([
    expect.stringContaining("supervisor-fd-control:kind=1:seq=7"),
    expect.stringContaining("supervisor-fd-control:kind=1:seq=8"),
  ]);
});

test("STATUS_FAULT selects a worker VM with concrete fault evidence when the process table is stale", () => {
  const worker = {} as Worker;
  const faultWorker = {} as Worker;
  const handle = makeHandle(worker);
  handle.pid = 1;
  handle.runtimeId = 77;
  handle.workers.add(faultWorker);
  handle.kernel.process_current_pid = () => 1;
  handle.kernel.process_thread_vm_ptr = () => 0;
  handle.kernel.thread_process_thread_count = () => 0;
  handle.kernel.thread_status = () => 3;
  handle.kernel.thread_exit_code = () => 139;
  handle.kernel.thread_term_signal = () => 11;
  handle.kernel.thread_fault_pc = (vmPtr: number) => vmPtr === 0x5678 ? 0x72755a8n : 0n;
  handle.kernel.thread_fault_addr = (vmPtr: number) => vmPtr === 0x5678 ? 0x9n : 0n;
  const deps = makeDeps(worker);
  deps.threadWorkerStates.set(faultWorker, {
    vmPtr: 0x5678,
    ownerPid: 1,
    ownerAsyncStatus: null,
    generation: 1,
    waitingInWorkerStatus: null,
    resumeWorkersOnReady: [],
  });
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: {
      type: "status",
      status: 3,
      exitCode: 139,
      termSignal: 11,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    },
    status: 3,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  expect(deps.completeProcessExit).toHaveBeenCalledWith(handle, 139, 11, 0x5678);
  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    77,
    expect.stringContaining("complete-exit-request:transitions-fault:1:vm=0x1234:exitVm=0x5678:priority=2"),
  );
});

test("STATUS_FAULT uses supervisor thread-exit decision instead of runtime remaining-thread heuristic", () => {
  const worker = { terminate: vi.fn() } as unknown as Worker;
  const handle = makeHandle(worker);
  handle.pid = 1;
  handle.runtimeId = 77;
  handle.kernel.process_current_pid = () => 1;
  handle.kernel.thread_process_thread_count = () => 1;
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  handle.memory = memory;
  const kernel = handle.kernel as typeof handle.kernel & {
    memory: WebAssembly.Memory;
    supervisor_abi_version: () => number;
    supervisor_fault_exit_decision_bytes: () => number;
    supervisor_fault_exit_kind_thread_exit: () => number;
    supervisor_fault_exit_kind_process_exit: () => number;
    supervisor_fault_exit_decision: (vmPtr: number, outPtr: number, outLen: number) => number;
  };
  kernel.memory = memory;
  kernel.supervisor_abi_version = () => 1;
  kernel.supervisor_fault_exit_decision_bytes = () => 88;
  kernel.supervisor_fault_exit_kind_thread_exit = () => 1;
  kernel.supervisor_fault_exit_kind_process_exit = () => 2;
  kernel.supervisor_fault_exit_decision = vi.fn((vmPtr: number, outPtr: number, outLen: number) => {
    expect(vmPtr).toBe(0x1234);
    expect(outLen).toBe(88);
    writeFaultExitDecisionRecord(memory, outPtr, 1, 0x1234, 0x1234, 1, 1);
    return 88;
  });
  const deps = makeDeps(worker);
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: deps.threadWorkerStates.get(worker)! as never,
    msg: {
      type: "status",
      status: 3,
      exitCode: 42,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    },
    status: 3,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  expect(kernel.supervisor_fault_exit_decision).toHaveBeenCalledWith(0x1234, expect.any(Number), 88);
  expect(deps.completeProcessExit).not.toHaveBeenCalled();
  expect(handle.workers.has(worker)).toBe(false);
  expect(deps.kickBlockedWorkers).toHaveBeenCalledWith(handle);
  expect(deps.pumpProcess).toHaveBeenCalledWith(handle);
  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    77,
    expect.stringContaining("fault-worker-delete:1:vm=0x1234"),
  );
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

test("STATUS_SOCKET_RECV syncs socket state before fork-shared blocked sync", async () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.pid = 5;
  handle.kernelWorkerRequest = vi.fn(async () => ({ type: "unused" }));
  const deps = makeDeps(worker);
  const order: string[] = [];
  deps.requestSocketStateSync = vi.fn(async () => {
    order.push("socket");
  });
  deps.syncBlockedForkSharedStateViaKernelWorker = vi.fn(async () => {
    order.push("fork-shared");
  });
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
    expect(deps.syncBlockedForkSharedStateViaKernelWorker).toHaveBeenCalled();
  });
  expect(deps.blockWorker).toHaveBeenCalledWith(handle, worker, 30, 0x1234);
  expect(deps.requestSocketStateSync).toHaveBeenCalledWith(handle, true);
  expect(order).toEqual(["socket", "fork-shared"]);
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

  await vi.runOnlyPendingTimersAsync();

  expect(deps.enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("STATUS_SLEEP zero-duration yield waits for pending blocked RPC before ready churn", async () => {
  vi.useFakeTimers();
  const worker = {} as Worker;
  const readyWorker = {} as Worker;
  const blockedWorker = {} as Worker;
  const handle = makeHandle(worker);
  handle.readyWorkers.push(readyWorker);
  const blocked = {
    vmPtr: 0x5678,
    status: 22,
    retryTimer: null,
    timeoutExpired: false,
    timeoutDeadlineMs: null,
    sleepDeadlineTicks: null,
    epollTimerfdDeadlineTicks: null,
    rpcPending: true,
    rpcGeneration: 1,
    skipNextNetworkPump: false,
    pipeSyncedBeforePassive: false,
    pipeChildExitResumeKey: null,
    pipeEofChildExitDeferrals: 0,
  };
  handle.blockedWorkers.set(blockedWorker, blocked);
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
  expect(deps.pumpProcess).not.toHaveBeenCalled();
  expect(deps.enqueueWorker).not.toHaveBeenCalled();

  blocked.rpcPending = false;
  await vi.advanceTimersByTimeAsync(1);

  expect(deps.pumpProcess).not.toHaveBeenCalled();
  expect(deps.enqueueWorker).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(1);

  expect(deps.enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("STATUS_SLEEP zero-duration yield gives blocked I/O retry a real timer turn", async () => {
  vi.useFakeTimers();
  const worker = {} as Worker;
  const readyWorker = {} as Worker;
  const blockedWorker = {} as Worker;
  const handle = makeHandle(worker);
  handle.readyWorkers.push(readyWorker);
  handle.blockedWorkers.set(blockedWorker, {
    vmPtr: 0x5678,
    status: 22,
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

  expect(deps.pumpProcess).not.toHaveBeenCalled();
  await vi.advanceTimersByTimeAsync(0);

  expect(deps.enqueueWorker).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(1);

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

test("STATUS_CLONE_SPAWN attaches child from supervisor HostOp record", () => {
  const workerConstructor = vi.fn(function WorkerMock(this: Worker) {
    Object.assign(this, {
      postMessage: vi.fn(),
      terminate: vi.fn(),
      unref: vi.fn(),
    });
  });
  vi.stubGlobal("Worker", workerConstructor);

  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.pid = 1;
  handle.runtimeId = 77;
  handle.kernel.thread_status = () => handle.kernel.abi_status_clone_spawn();
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  handle.memory = memory;
  const kernel = handle.kernel as typeof handle.kernel & {
    memory: WebAssembly.Memory;
    supervisor_abi_version: () => number;
    supervisor_host_op_record_bytes: () => number;
    supervisor_host_op_kind_spawn_thread_worker: () => number;
    supervisor_next_host_op: (outPtr: number, outLen: number) => number;
  };
  kernel.memory = memory;
  kernel.supervisor_abi_version = () => 1;
  kernel.supervisor_host_op_record_bytes = () => 64;
  kernel.supervisor_host_op_kind_spawn_thread_worker = () => 1;
  kernel.thread_spawn_from_clone = vi.fn(() => {
    throw new Error("legacy clone spawn should not be called");
  });
  kernel.supervisor_next_host_op = vi.fn((outPtr: number, outLen: number) => {
    expect(outLen).toBe(64);
    writeCloneSpawnHostOpRecord(memory, outPtr, 0x1234, 0x2000);
    return 64;
  });
  const deps = makeDeps(worker);
  const thread = deps.threadWorkerStates.get(worker)!;
  thread.ownerPid = handle.pid;
  let childThread:
    | {
      vmPtr: number;
      ownerPid: number;
      ownerAsyncStatus: number | null;
      generation: number;
      waitingInWorkerStatus: number | null;
      resumeWorkersOnReady: Worker[];
    }
    | undefined;
  deps.attachThreadWorker = vi.fn((_handle, request: { worker: Worker; vmPtr: number }) => {
    childThread = {
      vmPtr: request.vmPtr,
      ownerPid: handle.pid,
      ownerAsyncStatus: null,
      generation: 1,
      waitingInWorkerStatus: null,
      resumeWorkersOnReady: [],
    };
    deps.threadWorkerStates.set(request.worker, childThread);
    handle.workers.add(request.worker);
  });
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: thread as never,
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
  });

  expect(kernel.supervisor_next_host_op).toHaveBeenCalledWith(expect.any(Number), 64);
  expect(kernel.thread_spawn_from_clone).not.toHaveBeenCalled();
  expect(deps.attachThreadWorker).toHaveBeenCalledWith(handle, {
    worker: expect.any(Object),
    vmPtr: 0x2000,
  });
  expect(thread.ownerAsyncStatus).toBe(25);
  expect(childThread?.resumeWorkersOnReady).toEqual([worker]);
  expect(deps.appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "clone_spawn_hostop:parent=0x1234:child=0x2000:tid=2:seq=1",
  );
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

test("STATUS_CLONE_SPAWN holds parent until attached child reports ready", () => {
  const workerConstructor = vi.fn(function WorkerMock(this: Worker) {
    Object.assign(this, {
      postMessage: vi.fn(),
      terminate: vi.fn(),
      unref: vi.fn(),
    });
  });
  vi.stubGlobal("Worker", workerConstructor);
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.pid = 1;
  handle.runtimeId = 77;
  handle.kernel.thread_status = () => handle.kernel.abi_status_clone_spawn();
  const deps = makeDeps(worker);
  const thread = deps.threadWorkerStates.get(worker)!;
  thread.ownerPid = handle.pid;
  let childThread:
    | {
      vmPtr: number;
      ownerPid: number;
      ownerAsyncStatus: number | null;
      generation: number;
      waitingInWorkerStatus: number | null;
      resumeWorkersOnReady: Worker[];
    }
    | undefined;
  deps.attachThreadWorker = vi.fn((_handle, request: { worker: Worker; vmPtr: number }) => {
    childThread = {
      vmPtr: request.vmPtr,
      ownerPid: handle.pid,
      ownerAsyncStatus: null,
      generation: 1,
      waitingInWorkerStatus: null,
      resumeWorkersOnReady: [],
    };
    deps.threadWorkerStates.set(request.worker, childThread);
    handle.workers.add(request.worker);
  });
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: thread as never,
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
  });

  expect(thread.ownerAsyncStatus).toBe(25);
  expect(childThread?.resumeWorkersOnReady).toEqual([worker]);
  expect(deps.enqueueWorker).not.toHaveBeenCalled();
  expect(deps.kickBlockedWorkers).toHaveBeenCalledWith(handle);
});

test("STATUS_CLONE_SPAWN defers child spawn until kernel-worker lifecycle sync completes", () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.pid = 1;
  handle.kernelWorkerRequest = vi.fn(async () => ({ type: "unused" }));
  handle.kernel.thread_status = () => handle.kernel.abi_status_clone_spawn();
  const deps = makeDeps(worker);
  deps.syncProcessLifecycleViaKernelWorker = vi.fn(() => new Promise<void>(() => {}));
  const thread = deps.threadWorkerStates.get(worker)!;
  thread.ownerPid = handle.pid;
  const transition = createWorkerStatusTransitionHandler(deps as never);

  transition({
    handle: handle as never,
    worker,
    thread: thread as never,
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
  });

  expect(deps.syncProcessLifecycleViaKernelWorker).toHaveBeenCalledWith(handle, 0x1234);
  expect(thread.ownerAsyncStatus).toBe(25);
  expect(handle.kernel.thread_spawn_from_clone).not.toHaveBeenCalled();
  expect(deps.attachThreadWorker).not.toHaveBeenCalled();
});

test("STATUS_CLONE_SPAWN ignores stale lifecycle sync completions", async () => {
  const worker = {} as Worker;
  const handle = makeHandle(worker);
  handle.pid = 1;
  handle.kernelWorkerRequest = vi.fn(async () => ({ type: "unused" }));
  handle.kernel.thread_status = () => handle.kernel.abi_status_clone_spawn();
  const deps = makeDeps(worker);
  const syncResolutions: Array<() => void> = [];
  deps.syncProcessLifecycleViaKernelWorker = vi.fn(() => new Promise<void>((resolve) => {
    syncResolutions.push(resolve);
  }));
  const thread = deps.threadWorkerStates.get(worker)!;
  thread.ownerPid = handle.pid;
  const transition = createWorkerStatusTransitionHandler(deps as never);
  const statusMsg = {
    type: "status" as const,
    status: 25,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  };

  transition({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: statusMsg,
    status: 25,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });
  transition({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: statusMsg,
    status: 25,
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  syncResolutions[0]?.();
  await Promise.resolve();

  expect(deps.syncProcessLifecycleViaKernelWorker).toHaveBeenCalledTimes(2);
  expect(thread.ownerAsyncStatus).toBe(25);
  expect(handle.kernel.thread_spawn_from_clone).not.toHaveBeenCalled();
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
