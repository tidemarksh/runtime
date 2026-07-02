import { expect, test, vi } from "vite-plus/test";
import { createWorkerMessageHandler } from "../../../src/worker/message-handler.js";
import type { ThreadWorkerOutbound } from "../../../src/messages.js";
import type { WorkerMessageHandlerDeps } from "../../../src/worker/message-handler/types.js";
import { createWorkerMessageDebugStats } from "../../../src/worker/types.js";
import type { ProcessHandle, ThreadWorkerState } from "../../../src/worker/types.js";
import { writeSupervisorFaultExitDecisionRecord } from "./support/supervisor-abi.js";

function makeThreadState(overrides: Partial<ThreadWorkerState> = {}): ThreadWorkerState {
  return {
    vmPtr: 0x1234,
    ownerPid: 3,
    generation: 2,
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
    pendingPipeSlotsForNextStep: [],
    pendingGuestMemoryWrites: [],
    resumeWorkersOnReady: [],
    ...overrides,
  };
}

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

function makeDeps(
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
  appendWorkerDebugLog: ReturnType<typeof vi.fn>,
  failProcess: ReturnType<typeof vi.fn>,
): WorkerMessageHandlerDeps {
  const processHandles = new Map<number, ProcessHandle>();
  return {
    processHandles,
    threadWorkerStates,
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 0,
    syncEffectSocketState: 0,
    syncEffectEpollState: 0,
    syncEffectEpollDependentOfdState: 0,
    syncEffectProcessLifecycle: 0,
    syncEffectSignalWaiters: 0,
    syncEffectForkSharedOfdState: 0,
    syncEffectProcessMemfs: 0,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog,
    assertProcessStandardFdIntegrity: () => {},
    attachThreadWorker: () => {},
    blockWorker: () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    captureVforkSnapshot: () => ({
      vmPtr: 0,
      childPid: null,
      nextPid: null,
      pgid: null,
      sid: null,
      inheritedSharedOfdIds: [],
    }),
    clearVforkSnapshot: () => {},
    clearBlockedWorker: () => {},
    completeProcessExit: async () => {},
    commitNetworkConnect: async () => {},
    enqueueWorker: () => {},
    failProcess,
    handleExecveSpawn: async () => {},
    notifyWorkerStatusProgress: () => {},
    pumpProcess: () => {},
    pruneDetachedProcessWorkers: () => {},
    pumpNetworkConnections: async () => false,
    requestEpollDependentOfdStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestPipeStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resolveNetworkConnect: async () => 0n,
    resumeConnectViaKernelWorker: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    statusDefersOwnedStateSync: () => false,
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker: async () => {},
    syncProcessMemfsViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
  };
}

test("stale generated messages are dropped before transferred worker ownership is checked", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState());
  const appendWorkerDebugLog = vi.fn();
  const failProcess = vi.fn();
  const deps = makeDeps(threadWorkerStates, appendWorkerDebugLog, failProcess);
  const handle = {
    pid: 1,
    exitInProgress: false,
    workerDebugLog: [],
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    workers: new Set<Worker>(),
  } as unknown as ProcessHandle;
  deps.processHandles.set(1, handle);
  const handleWorkerMessage = createWorkerMessageHandler(deps);

  handleWorkerMessage(handle, worker, {
    type: "status",
    generation: 1,
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0x1000n,
    vmPtr: 0x1234,
    kernelState: {} as ThreadWorkerOutbound extends { kernelState: infer State } ? State : never,
  } as ThreadWorkerOutbound);

  expect(failProcess).not.toHaveBeenCalled();
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "stale-message:0x1234:status:1->2",
  );
});

test("status from a removed process state is dropped as stale", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState({ ownerPid: 4 }));
  const appendWorkerDebugLog = vi.fn();
  const failProcess = vi.fn();
  const deps = makeDeps(threadWorkerStates, appendWorkerDebugLog, failProcess);
  const handle = {
    pid: 4,
    exitInProgress: false,
    workerDebugLog: [],
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    workers: new Set<Worker>(),
    vforkSnapshot: null,
    kernel: {
      abi_status_fault: () => 31,
      abi_status_vfork_wait: () => 11,
      abi_status_execve_spawn: () => 13,
      process_current_pid: () => 0,
      thread_process_thread_count: () => 0,
      thread_process_ptr: () => 0x48000,
      process_current_ppid: () => 0,
    },
  } as unknown as ProcessHandle;
  deps.processHandles.set(4, handle);
  const handleWorkerMessage = createWorkerMessageHandler(deps);

  handleWorkerMessage(handle, worker, {
    type: "status",
    generation: 2,
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0x1000n,
    vmPtr: 0x1234,
    kernelState: {} as ThreadWorkerOutbound extends { kernelState: infer State } ? State : never,
  } as ThreadWorkerOutbound);

  expect(failProcess).not.toHaveBeenCalled();
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "stale-process-status:0x1234:0:pid=4",
  );
});

test("ready from a rehydrating process state is accepted when worker debug still identifies the owner", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState({ ownerPid: 4 }));
  const appendWorkerDebugLog = vi.fn();
  const failProcess = vi.fn();
  const deps = makeDeps(threadWorkerStates, appendWorkerDebugLog, failProcess);
  const enqueueWorker = vi.fn();
  deps.enqueueWorker = enqueueWorker;
  const handle = {
    pid: 4,
    parentPid: 1,
    vmPtr: 0x1234,
    exitInProgress: false,
    workerDebugLog: [],
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    workers: new Set<Worker>([worker]),
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Set<Worker>(),
    vforkSnapshot: null,
    stdinPipe: 0,
    ttyPtySlot: 0,
    stdinQueue: [],
    outputBacklog: [],
    stdioOwner: null,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
    },
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Set(),
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(16),
      stateSab: new SharedArrayBuffer(512),
    },
    memory: new WebAssembly.Memory({ initial: 1 }),
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
    kernel: {
      abi_status_fault: () => 31,
      abi_status_vfork_wait: () => 11,
      abi_status_execve_spawn: () => 13,
      abi_status_clone_spawn: () => 25,
      kernel_attach_runtime_state: () => {},
      process_current_pid: () => 0,
      thread_process_thread_count: () => 0,
      thread_process_ptr: () => 0,
      process_current_ppid: () => 0,
      process_fd_ofd_id: () => 0,
    },
  } as unknown as ProcessHandle;
  deps.processHandles.set(4, handle);
  const handleWorkerMessage = createWorkerMessageHandler(deps);

  handleWorkerMessage(handle, worker, {
    type: "ready",
    generation: 2,
    vmPtr: 0x1234,
    kernelState: handle.kernelState,
    wasmFeatures: { bulkMemory: true },
    debug: "process_ptr=0x48000 process_pid_field=4 process_ppid_field=1",
  } as ThreadWorkerOutbound);

  expect(failProcess).not.toHaveBeenCalled();
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "ready-rehydrate:0x1234:pid=4",
  );
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("ready adopts transferred worker ownership when kernel process identity matches the target handle", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  const thread = makeThreadState({ ownerPid: 1 });
  threadWorkerStates.set(worker, thread);
  const appendWorkerDebugLog = vi.fn();
  const failProcess = vi.fn();
  const deps = makeDeps(threadWorkerStates, appendWorkerDebugLog, failProcess);
  const enqueueWorker = vi.fn();
  deps.enqueueWorker = enqueueWorker;
  const handle = {
    pid: 3,
    parentPid: 1,
    vmPtr: 0x1234,
    exitInProgress: false,
    workerDebugLog: [],
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    workers: new Set<Worker>([worker]),
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Set<Worker>(),
    vforkSnapshot: null,
    stdinPipe: 0,
    ttyPtySlot: 0,
    stdinQueue: [],
    outputBacklog: [],
    stdioOwner: null,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
    },
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Set(),
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(16),
      stateSab: new SharedArrayBuffer(512),
    },
    memory: new WebAssembly.Memory({ initial: 1 }),
    kernelState: makeKernelState(),
    kernel: {
      abi_status_fault: () => 31,
      abi_status_vfork_wait: () => 11,
      abi_status_execve_spawn: () => 13,
      abi_status_clone_spawn: () => 25,
      kernel_attach_runtime_state: () => {},
      process_current_pid: () => 3,
      thread_process_thread_count: () => 1,
      thread_process_ptr: () => 0x48000,
      process_current_ppid: () => 1,
      process_fd_ofd_id: () => 0,
    },
  } as unknown as ProcessHandle;
  deps.processHandles.set(3, handle);
  const handleWorkerMessage = createWorkerMessageHandler(deps);

  handleWorkerMessage(handle, worker, {
    type: "ready",
    generation: 2,
    vmPtr: 0x1234,
    kernelState: handle.kernelState,
    wasmFeatures: { bulkMemory: true },
  } as ThreadWorkerOutbound);

  expect(thread.ownerPid).toBe(3);
  expect(failProcess).not.toHaveBeenCalled();
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "ready-owner-adopt:0x1234:pid=3:previous=1",
  );
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("messages delivered through a stale handle are rerouted to the transferred worker owner", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  const thread = makeThreadState({ ownerPid: 3 });
  threadWorkerStates.set(worker, thread);
  const appendWorkerDebugLog = vi.fn();
  const failProcess = vi.fn();
  const deps = makeDeps(threadWorkerStates, appendWorkerDebugLog, failProcess);
  const enqueueWorker = vi.fn();
  deps.enqueueWorker = enqueueWorker;
  const parentHandle = {
    pid: 1,
    exitInProgress: false,
    workerDebugLog: [],
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    workers: new Set<Worker>(),
    vforkSnapshot: null,
  } as unknown as ProcessHandle;
  const childHandle = {
    pid: 3,
    parentPid: 1,
    vmPtr: 0x1234,
    exitInProgress: false,
    workerDebugLog: [],
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    workers: new Set<Worker>([worker]),
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Set<Worker>(),
    vforkSnapshot: null,
    stdinPipe: 0,
    ttyPtySlot: 0,
    stdinQueue: [],
    outputBacklog: [],
    stdioOwner: null,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
    },
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Set(),
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(16),
      stateSab: new SharedArrayBuffer(512),
    },
    memory: new WebAssembly.Memory({ initial: 1 }),
    kernelState: makeKernelState(),
    kernel: {
      abi_status_fault: () => 31,
      abi_status_vfork_wait: () => 11,
      abi_status_execve_spawn: () => 13,
      abi_status_clone_spawn: () => 25,
      kernel_attach_runtime_state: () => {},
      process_current_pid: () => 3,
      thread_process_thread_count: () => 1,
      thread_process_ptr: () => 0x48000,
      process_current_ppid: () => 1,
      process_fd_ofd_id: () => 0,
    },
  } as unknown as ProcessHandle;
  deps.processHandles.set(1, parentHandle);
  deps.processHandles.set(3, childHandle);
  const handleWorkerMessage = createWorkerMessageHandler(deps);

  handleWorkerMessage(parentHandle, worker, {
    type: "ready",
    generation: 2,
    vmPtr: 0x1234,
    kernelState: childHandle.kernelState,
    wasmFeatures: { bulkMemory: true },
    debug: "process_ptr=0x48000 process_pid_field=1 process_ppid_field=0",
  } as ThreadWorkerOutbound);

  expect(failProcess).not.toHaveBeenCalled();
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    parentHandle,
    "reroute-owner-message:pid=1->3:type=ready",
  );
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    childHandle,
    "ready-pid-mismatch-stale-debug:0x1234:expected_pid=3:actual_pid=1:threads=1",
  );
  expect(enqueueWorker).toHaveBeenCalledWith(childHandle, worker);
});

test("clone child ready clears parent clone-spawn hold before resuming parent", () => {
  const parentWorker = {} as Worker;
  const childWorker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  const parentThread = makeThreadState({
    ownerPid: 4,
    ownerAsyncStatus: 25,
  });
  const childThread = makeThreadState({
    vmPtr: 0x2000,
    ownerPid: 4,
    initialized: false,
    resumeWorkersOnReady: [parentWorker],
  });
  threadWorkerStates.set(parentWorker, parentThread);
  threadWorkerStates.set(childWorker, childThread);
  const appendWorkerDebugLog = vi.fn();
  const failProcess = vi.fn();
  const deps = makeDeps(threadWorkerStates, appendWorkerDebugLog, failProcess);
  const enqueueWorker = vi.fn();
  deps.enqueueWorker = enqueueWorker;
  const handle = {
    pid: 4,
    parentPid: 1,
    exitInProgress: false,
    workerDebugLog: [],
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    workers: new Set<Worker>([parentWorker, childWorker]),
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Map(),
    vforkSnapshot: null,
    vforkWorker: null,
    stdinPipe: 0,
    ttyPtySlot: 0,
    stdinQueue: [],
    outputBacklog: [],
    stdioOwner: null,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
    },
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Set(),
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(16),
      stateSab: new SharedArrayBuffer(512),
    },
    memory: new WebAssembly.Memory({ initial: 1 }),
    kernelState: makeKernelState(),
    kernel: {
      abi_status_clone_spawn: () => 25,
      kernel_attach_runtime_state: () => {},
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
      process_current_pid: () => 4,
      thread_process_thread_count: () => 2,
      thread_process_ptr: () => 0x48000,
      process_current_ppid: () => 1,
      process_fd_ofd_id: () => 0,
    },
  } as unknown as ProcessHandle;
  deps.processHandles.set(4, handle);
  const handleWorkerMessage = createWorkerMessageHandler(deps);

  handleWorkerMessage(handle, childWorker, {
    type: "ready",
    generation: 2,
    vmPtr: 0x2000,
    kernelState: makeKernelState(),
    wasmFeatures: { bulkMemory: true },
  } as ThreadWorkerOutbound);

  expect(parentThread.ownerAsyncStatus).toBeNull();
  expect(childThread.resumeWorkersOnReady).toEqual([]);
  expect(enqueueWorker).toHaveBeenNthCalledWith(1, handle, childWorker);
  expect(enqueueWorker).toHaveBeenNthCalledWith(2, handle, parentWorker);
});

test("active vfork child fault remains routed through the parent handle", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState({ ownerPid: 1 }));
  const appendWorkerDebugLog = vi.fn();
  const failProcess = vi.fn();
  const deps = makeDeps(threadWorkerStates, appendWorkerDebugLog, failProcess);
  const resumeVforkParentAfterChildExit = vi.fn(() => true);
  deps.resumeVforkParentAfterChildExit = resumeVforkParentAfterChildExit;
  const kernelState = makeKernelState();
  const memory = new WebAssembly.Memory({ initial: 4 });
  const kernel = {
    memory,
    abi_status_ok: () => 0,
    abi_status_fault: () => 31,
    abi_status_fs: () => 6,
    abi_status_sleep: () => 8,
    abi_status_futex_wait: () => 12,
    abi_status_vfork_wait: () => 23,
    abi_status_execve_spawn: () => 24,
    abi_status_clone_spawn: () => 25,
    abi_max_threads: () => 1,
    abi_max_fds: () => 0,
    kernel_attach_runtime_state: () => {},
    kernel_heap_ptr: () => 0,
    process_current_pid: () => 3,
    process_current_ppid: () => 1,
    process_thread_vm_ptr: () => 0x1234,
    thread_process_ptr: () => 0x48000,
    thread_process_thread_count: () => 1,
    thread_pc: () => 0x1000n,
    thread_sp: () => 0x2000n,
    thread_s11: () => 0x3000n,
    thread_status: () => 31,
    supervisor_abi_version: () => 1,
    supervisor_fault_exit_decision_bytes: () => 88,
    supervisor_fault_exit_kind_thread_exit: () => 1,
    supervisor_fault_exit_kind_process_exit: () => 2,
    supervisor_fault_exit_decision: vi.fn((vmPtr: number, outPtr: number, outLen: number) => {
      expect(vmPtr).toBe(0x1234);
      expect(outLen).toBe(88);
      writeSupervisorFaultExitDecisionRecord(memory, outPtr, {
        vmPtr,
        processPtr: 0x48000,
        exitCode: 12,
        termSignal: 0,
        status: 31,
      });
      return 88;
    }),
  };
  const parentHandle = {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0,
    exitInProgress: false,
    workerDebugLog: [],
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    workers: new Set<Worker>([worker]),
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Map<Worker, unknown>(),
    pendingChildSpawnRecords: 0,
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinQueue: [],
    outputBacklog: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(16),
      stateSab: new SharedArrayBuffer(512),
    },
    kernel,
    kernelState,
    runningWorker: worker,
    memory,
    vforkSnapshot: {
      vmPtr: 0x1234,
      childPid: 3,
      nextPid: 4,
      pgid: 1,
      sid: 1,
      parentPid: 1,
      parentProcessPtr: 0x44000,
      parentPc: 0x1000n,
      parentSp: 0x2000n,
      parentS11: 0x3000n,
      inheritedSharedOfdIds: [],
    },
    vforkWorker: worker,
  } as unknown as ProcessHandle;
  const childHandle = {
    pid: 3,
    exitInProgress: false,
    workerDebugLog: [],
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    workers: new Set<Worker>(),
    kernel,
  } as unknown as ProcessHandle;
  deps.processHandles.set(1, parentHandle);
  deps.processHandles.set(3, childHandle);
  const handleWorkerMessage = createWorkerMessageHandler(deps);

  handleWorkerMessage(parentHandle, worker, {
    type: "status",
    generation: 2,
    status: 31,
    exitCode: 12,
    termSignal: 0,
    pc: 0x1000n,
    sp: 0x2000n,
    s11: 0x3000n,
    vmPtr: 0x1234,
    kernelState,
    debug: "pc=0x1000 sp=0x2000 s11=0x3000 process_pid_field=3 process_ppid_field=1",
  } as ThreadWorkerOutbound);

  expect(resumeVforkParentAfterChildExit).toHaveBeenCalledWith(
    parentHandle,
    worker,
    0x1234,
    12,
    0,
  );
  expect(parentHandle.workerMessageDebugStats.rerouted).toBe(0);
  expect(failProcess).not.toHaveBeenCalled();
});
