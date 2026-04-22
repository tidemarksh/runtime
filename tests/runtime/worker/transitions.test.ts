import { afterEach, expect, test, vi } from "vite-plus/test";
import { createWorkerStatusTransitionHandler } from "../../../src/worker/message-handler/transitions.js";

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
      abi_econnrefused: () => 111,
      thread_sleep_seconds: () => 0n,
      thread_sleep_nanoseconds: () => 0n,
      process_current_pid: () => 0,
      thread_process_ptr: () => 0,
      thread_process_thread_count: () => 1,
    },
  };
}

function makeDeps(worker: Worker) {
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    ownerPid: number;
    generation: number;
    waitingInWorkerStatus: number | null;
    resumeWorkersOnReady: Worker[];
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    ownerPid: 0,
    generation: 1,
    waitingInWorkerStatus: null,
    resumeWorkersOnReady: [],
  });

  return {
    threadWorkerStates,
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    attachThreadWorker: vi.fn(),
    blockWorker: vi.fn(),
    canonicalizeActiveVforkChildViaKernelWorker: vi.fn(async () => {}),
    captureVforkSnapshot: vi.fn(),
    clearVforkSnapshot: vi.fn(),
    clearBlockedWorker: vi.fn(),
    completeProcessExit: vi.fn(async () => {}),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn(),
    handleExecveSpawn: vi.fn(async () => {}),
    kickBlockedWorkers: vi.fn(),
    pumpProcess: vi.fn(),
    pruneDetachedProcessWorkers: vi.fn(),
    processHandles: new Map(),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestPipeStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    resumeConnectViaKernelWorker: vi.fn(async () => {}),
    resumeFsViaKernelWorker: vi.fn(async () => {}),
    resumeVforkParentAfterChildExit: vi.fn(() => false),
    statusDefersOwnedStateSync: vi.fn(() => false),
    statusOkCompletedSyncEffects: 0,
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncEffectEpollDependentOfdState: 0,
    syncEffectEpollState: 0,
    syncEffectForkSharedOfdState: 0,
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
  expect(
    deps.kickBlockedWorkers.mock.invocationCallOrder[0],
  ).toBeLessThan(deps.enqueueWorker.mock.invocationCallOrder[0]);
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
