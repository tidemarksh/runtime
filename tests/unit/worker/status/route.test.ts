import { expect, test, vi } from "vite-plus/test";
import { writePipe } from "../../../../src/bridge/pipe.js";
import { createWorkerStatusMessageHandler as createRawWorkerStatusMessageHandler } from "../../../../src/worker/message-handler/status/route.js";
import { createWorkerMessageDebugStats } from "../../../../src/worker/types.js";

const PIPE_BUF_SIZE = 65_536;
const PIPE_SLOT_SIZE = 32 + PIPE_BUF_SIZE;

function withHandleDefaults<T extends Record<string, unknown>>(handle: T): T {
  handle.networkConnections ??= new Map();
  handle.networkConnectionOfds ??= new Map();
  handle.networkPendingConnectFds ??= new Set();
  handle.networkClosedFds ??= new Set();
  handle.networkRecvHeadWatermarks ??= new Map();
  handle.networkRecvTailWatermarks ??= new Map();
  handle.networkRecvPayloadWatermarks ??= new Map();
  handle.workerDebugLog ??= [];
  handle.workerMessageDebugStats ??= createWorkerMessageDebugStats();
  handle.vforkSnapshot ??= null;
  handle.processMemfsDirty ??= false;
  handle.pendingPipeSlotSnapshots ??= [];
  return handle;
}

function createWorkerStatusMessageHandler(deps: Parameters<typeof createRawWorkerStatusMessageHandler>[0]) {
  const handler = createRawWorkerStatusMessageHandler({
    processHandles: new Map(),
    appendRuntimeLifecycleEvent: () => {},
    pruneDetachedProcessWorkers: () => {},
    pumpNetworkConnections: async () => false,
    requestSocketStateSync: async () => {},
    ...deps,
  } as Parameters<typeof createRawWorkerStatusMessageHandler>[0]);
  return (handle: Record<string, unknown>, worker: Worker, msg: Parameters<ReturnType<typeof createRawWorkerStatusMessageHandler>>[2]) =>
    handler(withHandleDefaults(handle), worker, msg);
}

function makeKernelState() {
  return {
    heapBase: 0,
    heapPtr: 0,
    ofdTablePtr: 0,
    memfsPtr: 0,
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

function makeThreadState(overrides: Record<string, unknown> = {}) {
  return {
    vmPtr: 0x1234,
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
    ownerPid: 7,
    generation: 1,
    resumeWorkersOnReady: [],
    ...overrides,
  };
}

function makeKernel(pid: number, overrides: Record<string, unknown> = {}) {
  return {
    abi_max_fds: () => 64,
    abi_max_pipes: () => 64,
    abi_max_threads: () => 4,
    abi_fd_type_epoll: () => 5,
    abi_fd_type_pipe: () => 3,
    abi_status_ok: () => 0,
    abi_status_fault: () => 3,
    abi_status_fs: () => 4,
    abi_status_vfork_wait: () => 8,
    abi_status_execve_spawn: () => 9,
    abi_status_clone_spawn: () => 10,
    abi_status_sleep: () => 11,
    abi_status_futex_wait: () => 20,
    abi_status_pipe_wait: () => 21,
    abi_status_epoll_wait: () => 22,
    abi_status_socket_recv: () => 23,
    abi_status_socket_send: () => 24,
    abi_status_wait4: () => 25,
    abi_status_flock_wait: () => 26,
    abi_status_connect: () => 27,
    thread_pc: () => 0x2000n,
    thread_sp: () => 0x3000n,
    thread_s11: () => 0x4000n,
    thread_epoll_wait_timeout: () => 0n,
    thread_process_thread_count: () => 1,
    process_current_pid: () => pid,
    process_thread_vm_ptr: (vmPtr: number, slot: number) => (slot === 0 ? vmPtr : 0),
    thread_process_ptr: () => 0x5000,
    process_fd_ofd_id: () => 0,
    thread_blocked_ofd_id: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_ino: () => 0,
    kernel_ofd_table_bytes: () => 64,
    kernel_ofd_refcount: () => 0,
    kernel_retain_ofd: () => {},
    kernel_attach_runtime_state: () => {},
    kernel_net_buf_used: () => 0n,
    kernel_socket_used: () => 0,
    ...overrides,
  };
}

test("status handler logs futex resume with compact debug fields", () => {
  const appendWorkerDebugLog = vi.fn();
  const enqueueWorker = vi.fn();
  const clearBlockedWorker = vi.fn();
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  const thread = makeThreadState({
    waitingInWorkerStatus: 20,
    runningGeneration: undefined,
    lastStepAtMs: 100,
  });
  threadWorkerStates.set(worker, thread);
  const dateNow = vi.spyOn(Date, "now").mockReturnValue(125);

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog,
    clearBlockedWorker,
    enqueueWorker,
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    {
      pid: 7,
      stdoutPipe: 0,
      stderrPipe: 0,
      stdinPipe: 0,
      stdinQueue: [],
      hostStdio: {
        stdinPipe: 0,
        stdoutPipe: 0,
        stderrPipe: 0,
        refCount: 1,
      },
      memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
      outputBacklog: [],
      workers: new Set([worker]),
      readyWorkers: [],
      blockedWorkers: new Map(),
      kernelWorkerRequest: undefined,
      kernelShare: { members: new Set(), sharedOfdIds: new Set() },
      runtimeBridge: {
        notifySab: new SharedArrayBuffer(12),
        stateSab: new SharedArrayBuffer(176),
      },
      kernel: makeKernel(7, {
        abi_status_clone_spawn: () => 9,
      }),
      kernelState: makeKernelState(),
      onStdout: null,
      runningWorker: worker,
    } as never,
    worker,
    {
      type: "status",
      status: 0,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      kernelState: makeKernelState(),
      debug: [
        "pc=0x55f20",
        "ra=0x4d654",
        "sp=0x37d08dd0",
        "gp=0x11dab8",
        "tp=0x11eff0",
        "s11=0x37d08e00",
        "syscall=98",
        "process_thread_count=3",
        "blocked_addr=0x37d61c40",
        "futex_wait_addr=0x37d61c40",
        "futex_wait_expected=0",
        "futex_wait_timeout=0x0",
        "sig_deliver=11",
        "sig_pc=0x0",
        "sig_sp=0x0",
        "sig_s11=0x0",
        "zero_reg=2",
        "zero_reg_tid=3",
        "zero_reg_pc=0x55f20",
        "zero_reg_sp=0x0",
      ].join(" "),
    },
  );
  dateNow.mockRestore();

  expect(clearBlockedWorker).toHaveBeenCalledWith(expect.anything(), worker);
  expect(thread.lastStepDurationMs).toBe(25);
  expect(thread.totalStepWallMs).toBe(25);
  expect(thread.maxStepWallMs).toBe(25);
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    expect.anything(),
    "resume:0x1234@20->0:"
      + "pc=0x55f20:"
      + "ra=0x4d654:"
      + "sp=0x37d08dd0:"
      + "gp=0x11dab8:"
      + "tp=0x11eff0:"
      + "s11=0x37d08e00:"
      + "syscall=98:"
      + "process_thread_count=3:"
      + "blocked_addr=0x37d61c40:"
      + "futex_wait_addr=0x37d61c40:"
      + "futex_wait_expected=0:"
      + "futex_wait_timeout=0x0:"
      + "sig_deliver=11:"
      + "sig_pc=0x0:"
      + "sig_sp=0x0:"
      + "sig_s11=0x0:"
      + "zero_reg=2:"
      + "zero_reg_tid=3:"
      + "zero_reg_pc=0x55f20:"
      + "zero_reg_sp=0x0",
  );
  expect(enqueueWorker).toHaveBeenCalledWith(expect.anything(), worker);
});

test("status handler preserves FS ring snapshots during transient process owner mismatch", () => {
  const worker = {} as Worker;
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const bytes = new Uint8Array(memory.buffer);
  bytes.set([9, 9, 9], 0x100);
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeThreadState>>();
  threadWorkerStates.set(worker, makeThreadState());
  const resumeFsViaKernelWorker = vi.fn(async () => {});

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog: () => {},
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker,
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  const handle = {
    pid: 7,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    guestBase: 0,
    memory,
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(7, {
      thread_fs_ring_base: () => 0x100,
    }),
    kernelState: makeKernelState(),
    onStdout: null,
    runningWorker: worker,
  } as never;

  handler(handle, worker, {
    type: "status",
    status: 4,
    vmPtr: 0x1234,
    exitCode: 0,
    termSignal: 0,
    pc: 0x2000n,
    kernelState: { ...makeKernelState(), heapPtr: 0xdead },
    debug: "process_pid_field=99 pc=0x2000 sp=0x3000 s11=0x4000",
    kernelMemoryWrites: [
      { ptr: 0x100, bytes: new Uint8Array([1, 2, 3]) },
      { ptr: 0x2000, bytes: new Uint8Array([7, 8, 9]) },
    ],
    guestMemoryWrites: [{ guestPtr: 0x200, bytes: new Uint8Array([4, 5, 6]) }],
  });

  expect(Array.from(bytes.slice(0x100, 0x103))).toEqual([9, 9, 9]);
  expect(Array.from(bytes.slice(0x200, 0x203))).toEqual([4, 5, 6]);
  expect(Array.from(bytes.slice(0x2000, 0x2003))).toEqual([7, 8, 9]);
  expect((handle as { kernelState: ReturnType<typeof makeKernelState> }).kernelState.heapPtr).toBe(0);
  expect(resumeFsViaKernelWorker).toHaveBeenCalledWith(handle, worker, 0x1234);
});

test("status handler preserves FS ring snapshots while owner async status is pending", () => {
  const worker = {} as Worker;
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const bytes = new Uint8Array(memory.buffer);
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeThreadState>>();
  threadWorkerStates.set(worker, makeThreadState({ ownerAsyncStatus: 4 }));
  const resumeFsViaKernelWorker = vi.fn(async () => {});
  const appendWorkerDebugLog = vi.fn();

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog,
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker,
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  const handle = {
    pid: 7,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    guestBase: 0,
    memory,
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(7),
    kernelState: makeKernelState(),
    onStdout: null,
    runningWorker: worker,
  } as never;

  handler(handle, worker, {
    type: "status",
    status: 4,
    vmPtr: 0x1234,
    exitCode: 0,
    termSignal: 0,
    pc: 0x2000n,
    kernelState: { ...makeKernelState(), heapPtr: 0xdead },
    debug: "pc=0x2000 sp=0x3000 s11=0x4000",
    kernelMemoryWrites: [{ ptr: 0x100, bytes: new Uint8Array([7, 8, 9]) }],
    guestMemoryWrites: [{ guestPtr: 0x200, bytes: new Uint8Array([10, 11, 12]) }],
  });

  expect(Array.from(bytes.slice(0x100, 0x103))).toEqual([7, 8, 9]);
  expect(Array.from(bytes.slice(0x200, 0x203))).toEqual([10, 11, 12]);
  expect((handle as { kernelState: ReturnType<typeof makeKernelState> }).kernelState.heapPtr).toBe(0);
  expect(resumeFsViaKernelWorker).not.toHaveBeenCalled();
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "status-owner-async:0x1234:owner=4:status=4:ownerMatch=1",
  );
});

test("status handler does not roll back dirty memfs from fault snapshots", () => {
  const worker = {} as Worker;
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const bytes = new Uint8Array(memory.buffer);
  bytes.set([9, 9, 9, 9], 0x100);
  bytes.set([8, 8, 8, 8], 0x200);
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeThreadState>>();
  threadWorkerStates.set(worker, makeThreadState());

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog: () => {},
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  const handle = {
    pid: 7,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    guestBase: 0,
    memory,
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(7),
    kernelState: {
      ...makeKernelState(),
      memfsPtr: 0x100,
      memfsHeaderBytes: 0x20,
      memfsInodesBytes: 0,
      memfsFirstChildBytes: 0,
      memfsDirEntriesBytes: 0,
      memfsDataBytes: 0,
    },
    onStdout: null,
    runningWorker: worker,
    processMemfsDirty: true,
  } as never;

  handler(handle, worker, {
    type: "status",
    status: 3,
    vmPtr: 0x1234,
    exitCode: 1,
    termSignal: 0,
    pc: 0x2000n,
    kernelState: makeKernelState(),
    debug: "pc=0x2000 sp=0x3000 s11=0x4000",
    kernelMemoryWrites: [
      { ptr: 0x100, bytes: new Uint8Array([1, 2, 3, 4]) },
      { ptr: 0x200, bytes: new Uint8Array([5, 6, 7, 8]) },
    ],
  });

  expect(Array.from(bytes.slice(0x100, 0x104))).toEqual([9, 9, 9, 9]);
  expect(Array.from(bytes.slice(0x200, 0x204))).toEqual([5, 6, 7, 8]);
});

test("status handler preserves network recv buffers while owner async status is pending", () => {
  const appendWorkerDebugLog = vi.fn();
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null; ownerAsyncStatus?: number }>();
  threadWorkerStates.set(worker, makeThreadState({ ownerAsyncStatus: 4 }));
  const netBufferBytes = 256;
  const netBufPoolBytes = netBufferBytes * 2;
  const netBufBase = 0x1000;
  const recvBufIdx = 1;
  const recvBufBase = netBufBase + recvBufIdx * netBufferBytes;
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const bytes = new Uint8Array(memory.buffer);
  const view = new DataView(memory.buffer);
  view.setUint32(recvBufBase, 0, true);
  view.setUint32(recvBufBase + 4, 140, true);
  bytes[recvBufBase + 16] = 0xaa;
  bytes[recvBufBase + 17] = 0xbb;

  const incomingNetBufPool = new Uint8Array(netBufPoolBytes);
  const incomingView = new DataView(incomingNetBufPool.buffer);
  incomingView.setUint32(recvBufIdx * netBufferBytes, 8, true);
  incomingView.setUint32(recvBufIdx * netBufferBytes + 4, 8, true);
  incomingNetBufPool[recvBufIdx * netBufferBytes + 16] = 0x99;

  const handle = {
    pid: 7,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory,
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(7, {
      abi_max_sockets: () => 1,
      kernel_net_buf_pool_bytes: () => netBufPoolBytes,
      thread_socket_idx_for_fd: () => 0,
      thread_socket_recv_buf_idx: () => recvBufIdx,
      thread_socket_read_bytes: () => 8n,
    }),
    kernelState: {
      ...makeKernelState(),
      netBufBase,
      netBufUsed: 0b10n,
    },
    onStdout: null,
    runningWorker: worker,
    networkConnections: new Map([[7, {
      read: () => undefined,
      write: () => {},
    }]]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
  } as never;

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog,
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(handle, worker, {
    type: "status",
    status: 4,
    vmPtr: 0x1234,
    exitCode: 0,
    termSignal: 0,
    pc: 0x2000n,
    kernelState: makeKernelState(),
    debug: "pc=0x2000 sp=0x3000 s11=0x4000",
    kernelMemoryWrites: [{ ptr: netBufBase, bytes: incomingNetBufPool }],
  });

  expect(view.getUint32(recvBufBase, true)).toBe(8);
  expect(view.getUint32(recvBufBase + 4, true)).toBe(140);
  expect(bytes[recvBufBase + 16]).toBe(0xaa);
  expect(bytes[recvBufBase + 17]).toBe(0xbb);
});

test("status handler keeps faulting worker tracked until completeProcessExit runs for last-thread fault", () => {
  const appendWorkerDebugLog = vi.fn();
  const completeProcessExit = vi.fn(async () => {});
  const worker = {} as Worker;
  const sibling = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, makeThreadState());

  const handle = {
    pid: 7,
    lastDebugMessage: "pc=0x67c5be sp=0x197d120 s11=0x1 process_thread_count=2",
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    outputBacklog: [],
    workers: new Set([worker, sibling]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(7, {
      abi_status_clone_spawn: () => 9,
      thread_process_thread_count: () => 0,
      process_thread_vm_ptr: () => 0,
    }),
    kernelState: makeKernelState(),
    onStdout: null,
    runningWorker: worker,
  } as never;

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog,
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit,
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    handle,
    worker,
    {
      type: "status",
      status: 3,
      vmPtr: 0x1234,
      exitCode: 139,
      termSignal: 11,
      pc: 0n,
      kernelState: makeKernelState(),
      debug: "pc=0x4 process_thread_count=0",
    },
  );

  expect(completeProcessExit).toHaveBeenCalledWith(handle, 139, 11, 0x1234);
  expect(handle.workers.has(worker)).toBe(true);
  expect(threadWorkerStates.get(worker)?.vmPtr).toBe(0x1234);
  expect(handle.lastDebugMessage).toBe("pc=0x67c5be sp=0x197d120 s11=0x1 process_thread_count=2");
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(handle, "status_debug:0x1234:pc=0x4 process_thread_count=0");
});

test("status handler leaves blocked fork-shared sync to blocked-status handling", async () => {
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const syncBlockedForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const blockWorker = vi.fn();
  const pumpProcess = vi.fn();
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
    ownerPid: number;
    generation: number;
  }>();
  threadWorkerStates.set(worker, makeThreadState());

  const handle = {
    pid: 7,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: vi.fn(async () => ({ type: "noop" })),
    kernelShare: { members: new Set(), sharedOfdIds: new Set([3]) },
    forkSharedSyncPromise: null,
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(7, {
      abi_status_clone_spawn: () => 9,
      abi_status_wait4: () => 23,
      abi_status_socket_recv: () => 24,
      abi_status_socket_send: () => 25,
      abi_status_connect: () => 27,
    }),
    kernelState: makeKernelState(),
    onStdout: null,
    runningWorker: worker,
  } as never;

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog: () => {},
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess,
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker,
    syncBlockedForkSharedStateViaKernelWorker,
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker,
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    handle,
    worker,
    {
      type: "status",
      status: 24,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0x5000n,
      sp: 0x6000n,
      s11: 0x7000n,
      kernelState: makeKernelState(),
      syncEffects: 64,
      debug: "pc=0x5000 sp=0x6000 s11=0x7000 process_thread_count=1",
    },
  );

  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  expect(syncBlockedForkSharedStateViaKernelWorker).toHaveBeenCalledWith(
    handle,
    0x1234,
    { kind: "blocked-rpc", status: 24 },
  );
  expect(blockWorker).toHaveBeenCalledWith(handle, worker, 24, 0x1234);
  expect(pumpProcess).toHaveBeenCalledWith(handle);
});

test("status handler pumps ready siblings after pipe wait sync parks blocked worker", async () => {
  const syncBlockedForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const requestPipeStateSync = vi.fn(async () => {});
  const blockWorker = vi.fn();
  const pumpProcess = vi.fn();
  const worker = {} as Worker;
  const readyWorker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
    ownerPid: number;
    generation: number;
  }>();
  threadWorkerStates.set(worker, makeThreadState());
  threadWorkerStates.set(readyWorker, makeThreadState({ vmPtr: 0x5678 }));

  const handle = {
    pid: 7,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    outputBacklog: [],
    workers: new Set([worker, readyWorker]),
    readyWorkers: [readyWorker],
    blockedWorkers: new Map(),
    kernelWorkerRequest: vi.fn(async () => ({ type: "noop" })),
    kernelShare: { members: new Set(), sharedOfdIds: new Set([3]) },
    forkSharedSyncPromise: null,
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(7, {
      abi_status_clone_spawn: () => 9,
      thread_poll_wait_ready_count: () => 0,
      thread_poll_wait_timeout: () => -1,
    }),
    kernelState: makeKernelState(),
    onStdout: null,
    runningWorker: worker,
  } as never;

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog: () => {},
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess,
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync,
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker,
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker,
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    handle,
    worker,
    {
      type: "status",
      status: 21,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0x5000n,
      sp: 0x6000n,
      s11: 0x7000n,
      kernelState: makeKernelState(),
      debug: "pc=0x5000 sp=0x6000 s11=0x7000 process_thread_count=2",
    },
  );

  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requestPipeStateSync).toHaveBeenCalledWith(handle, false);
  expect(syncBlockedForkSharedStateViaKernelWorker).toHaveBeenCalledWith(
    handle,
    0x1234,
    { kind: "blocked-rpc", status: 21 },
  );
  expect(blockWorker).toHaveBeenCalledWith(handle, worker, 21, 0x1234);
  expect(pumpProcess).toHaveBeenCalledWith(handle);
});

test("status handler skips explicit fork-shared effects when no peer exists", async () => {
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
    ownerPid: number;
    generation: number;
  }>();
  threadWorkerStates.set(worker, makeThreadState());

  const handle = {
    pid: 7,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: vi.fn(async () => ({ type: "noop" })),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    forkSharedSyncPromise: null,
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(7, {
      abi_status_clone_spawn: () => 9,
    }),
    kernelState: makeKernelState(),
    onStdout: null,
    runningWorker: worker,
  } as never;

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog: () => {},
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker,
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    handle,
    worker,
    {
      type: "status",
      status: 0,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0x5000n,
      sp: 0x6000n,
      s11: 0x7000n,
      kernelState: makeKernelState(),
      syncEffects: 64,
      debug: "pc=0x5000 sp=0x6000 s11=0x7000 process_thread_count=1",
    },
  );

  await Promise.resolve();
  await Promise.resolve();

  expect(syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
});

test("status handler logs quoted write debug fields before wait handling", () => {
  const appendWorkerDebugLog = vi.fn();
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, makeThreadState());

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog,
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    {
      pid: 7,
      stdoutPipe: 0,
      stderrPipe: 0,
      stdinPipe: 0,
      stdinQueue: [],
      hostStdio: {
        stdinPipe: 0,
        stdoutPipe: 0,
        stderrPipe: 0,
        refCount: 1,
      },
      memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
      outputBacklog: [],
      workers: new Set([worker]),
      readyWorkers: [],
      blockedWorkers: new Map(),
      kernelWorkerRequest: undefined,
      kernelShare: { members: new Set(), sharedOfdIds: new Set() },
      runtimeBridge: {
        notifySab: new SharedArrayBuffer(12),
        stateSab: new SharedArrayBuffer(176),
      },
      kernel: makeKernel(7, {
        abi_status_clone_spawn: () => 9,
      }),
      kernelState: makeKernelState(),
      onStdout: null,
      runningWorker: worker,
    } as never,
    worker,
    {
      type: "status",
      status: 20,
      generation: 1,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      kernelState: makeKernelState(),
      waitInWorker: true,
      debug: [
        "pc=0x4d650",
        "gp=0x11dab8",
        "tp=0x11eff0",
        "write_fd=2",
        "write_count=31",
        "write_result=31",
        "write_data=\"fatal: bad g in signal handler\\\\n\"",
      ].join(" "),
    },
  );

  expect(appendWorkerDebugLog).toHaveBeenNthCalledWith(
    1,
    expect.anything(),
    "status-route:0x1234:owner_pid=7:handle_pid=7:generation=1:status=20",
  );
  expect(appendWorkerDebugLog).toHaveBeenNthCalledWith(
    2,
    expect.anything(),
    "status:0x1234@20:pc=0x4d650:gp=0x11dab8:tp=0x11eff0",
  );
  expect(appendWorkerDebugLog).toHaveBeenNthCalledWith(
    3,
    expect.anything(),
    "write:0x1234:pc=0x4d650:gp=0x11dab8:tp=0x11eff0:write_fd=2:write_count=31:write_result=31:write_data=\"fatal: bad g in signal handler\\\\n\"",
  );
});

test("status handler dispatches execve_spawn into the ownership handoff path", () => {
  const appendWorkerDebugLog = vi.fn();
  const handleExecveSpawn = vi.fn(async () => {});
  const clearBlockedWorker = vi.fn();
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, makeThreadState());

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog,
    clearBlockedWorker,
    enqueueWorker: vi.fn(),
    pumpProcess: vi.fn(),
    kickBlockedWorkers: vi.fn(),
    failProcess: vi.fn(),
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn,
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  const execveProcessState = Uint8Array.from([0xde, 0xad, 0xbe, 0xef]);
  const handle = {
    pid: 7,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(7, {
      abi_status_connect: () => 28,
      abi_status_socket_send: () => 29,
      abi_status_socket_recv: () => 30,
      abi_status_flock_wait: () => 32,
      abi_status_execve_spawn: () => 9,
      abi_status_clone_spawn: () => 10,
    }),
    kernelState: makeKernelState(),
    onStdout: null,
    runningWorker: worker,
  } as never;

  handler(
    handle,
    worker,
    {
      type: "status",
      status: 9,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      kernelState: makeKernelState(),
      processState: execveProcessState,
      debug: "spawn_kind=execve",
    },
  );

  expect(clearBlockedWorker).toHaveBeenCalledWith(handle, worker);
  expect(handleExecveSpawn).toHaveBeenCalledTimes(1);
  expect(handleExecveSpawn).toHaveBeenCalledWith(handle, worker, expect.objectContaining({ vmPtr: 0x1234 }));
  expect((threadWorkerStates.get(worker) as { execveProcessState?: Uint8Array }).execveProcessState).toBe(execveProcessState);
});

test("status handler keeps full debug for zeroed OK state", () => {
  const appendWorkerDebugLog = vi.fn();
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, makeThreadState());

  const debug = [
    "pc=0x0",
    "ra=0x0",
    "sp=0x0",
    "gp=0x0",
    "tp=0x0",
    "s11=0x0",
    "process_thread_count=3",
    "exec_trace=[0x83308:0x12345678]",
  ].join(" ");

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog,
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    {
      pid: 7,
      stdoutPipe: 0,
      stderrPipe: 0,
      stdinPipe: 0,
      stdinQueue: [],
      hostStdio: {
        stdinPipe: 0,
        stdoutPipe: 0,
        stderrPipe: 0,
        refCount: 1,
      },
      memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
      outputBacklog: [],
      workers: new Set([worker]),
      readyWorkers: [],
      blockedWorkers: new Map(),
      kernelWorkerRequest: undefined,
      kernelShare: { members: new Set(), sharedOfdIds: new Set() },
      runtimeBridge: {
        notifySab: new SharedArrayBuffer(12),
        stateSab: new SharedArrayBuffer(176),
      },
      kernel: makeKernel(7, {
        abi_status_clone_spawn: () => 9,
      }),
      kernelState: makeKernelState(),
      onStdout: null,
      runningWorker: null,
    } as never,
    worker,
    {
      type: "status",
      status: 0,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      kernelState: makeKernelState(),
      debug,
    },
  );

  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    expect.anything(),
    `status_debug:0x1234:${debug}`,
  );
});

test("status handler preserves the browser go ownership failure signature", () => {
  const appendWorkerDebugLog = vi.fn();
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, makeThreadState({ vmPtr: 0x3333, waitingInWorkerStatus: 20 }));

  const debug = [
    "pc=0x2",
    "ra=0x0",
    "sp=0x0",
    "gp=0x11dab8",
    "tp=0x11eff0",
    "s11=0x0",
    "status_origin=thread_step",
    "status_pre_pc=0xe",
    "status_pre_ra=0x0",
    "status_pre_sp=0x0",
    "status_pre_s11=0x0",
    "thread_step_pc=0x2",
    "thread_step_ra=0x0",
    "thread_step_sp=0x0",
    "thread_step_s11=0x0",
    "fault_dispatch_phase=1",
    "fault_dispatch_pc=0x2",
    "fault_dispatch_ra=0x0",
    "fault_dispatch_sp=0x0",
    "fault_dispatch_s11=0x0",
    "write_fd=2",
    "write_count=31",
    "write_result=31",
    "write_data=\"fatal: bad g in signal handler\\\\n\"",
  ].join(" ");

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog,
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    {
      pid: 7,
      stdoutPipe: 0,
      stderrPipe: 0,
      stdinPipe: 0,
      stdinQueue: [],
      hostStdio: {
        stdinPipe: 0,
        stdoutPipe: 0,
        stderrPipe: 0,
        refCount: 1,
      },
      memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
      outputBacklog: [],
      workers: new Set([worker]),
      readyWorkers: [],
      blockedWorkers: new Map(),
      kernelWorkerRequest: undefined,
      kernelShare: { members: new Set(), sharedOfdIds: new Set() },
      runtimeBridge: {
        notifySab: new SharedArrayBuffer(12),
        stateSab: new SharedArrayBuffer(176),
      },
      kernel: makeKernel(7, {
        abi_status_clone_spawn: () => 9,
      }),
      kernelState: makeKernelState(),
      onStdout: null,
      runningWorker: null,
    } as never,
    worker,
    {
      type: "status",
      status: 0,
      vmPtr: 0x3333,
      exitCode: 0,
      termSignal: 0,
      pc: 0x2n,
      ra: 0n,
      sp: 0n,
      s11: 0n,
      kernelState: makeKernelState(),
      debug,
    },
  );

  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    expect.anything(),
    "status_message_break:0x3333:pc=0x2:ra=0x0:sp=0x0:s11=0x0",
  );
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    expect.anything(),
    `status_debug:0x3333:${debug}`,
  );
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    expect.anything(),
    "write:0x3333:pc=0x2:ra=0x0:sp=0x0:gp=0x11dab8:tp=0x11eff0:s11=0x0:write_fd=2:write_count=31:write_result=31:write_data=\"fatal: bad g in signal handler\\\\n\"",
  );
});

test("status handler defers local pipe drain when kernel-worker owns pipe state", async () => {
  const requestPipeStateSync = vi.fn(async () => {});
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, makeThreadState());

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog: () => {},
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync,
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0x1000,
  };
  const view = new DataView(memory.buffer);
  const pipeBase = kernelState.pipeBase + 2 * PIPE_SLOT_SIZE;
  view.setInt32(pipeBase + 8, 1, true);
  view.setInt32(pipeBase + 12, 0, true);
  const seeded = writePipe(memory, kernelState.pipeBase, 2, new TextEncoder().encode("$ "));
  if (seeded.broken || seeded.written !== 2) {
    throw new Error("failed to seed stdout pipe");
  }
  const delivered: string[] = [];

  handler(
    {
      pid: 7,
      stdoutPipe: 2,
      stderrPipe: 3,
      stdinPipe: 1,
      stdinQueue: [],
      hostStdio: {
        stdinPipe: 1,
        stdoutPipe: 2,
        stderrPipe: 3,
        refCount: 1,
      },
      memory,
      outputBacklog: [],
      workers: new Set([worker]),
      readyWorkers: [],
      blockedWorkers: new Map(),
      kernelWorkerRequest: async () => ({ type: "unexpected" }) as never,
      kernelShare: { members: new Set(), sharedOfdIds: new Set() },
      runtimeBridge: {
        notifySab: new SharedArrayBuffer(12),
        stateSab: new SharedArrayBuffer(176),
      },
      kernel: makeKernel(7, {
        abi_max_fds: () => 0,
        abi_max_threads: () => 0,
        abi_status_clone_spawn: () => 9,
        process_thread_vm_ptr: () => 0,
        thread_process_thread_count: () => 0,
      }),
      kernelState,
      onStdout: (data: Uint8Array) => {
        delivered.push(new TextDecoder().decode(data));
      },
      runningWorker: worker,
    } as never,
    worker,
    {
      type: "status",
      status: 0,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      kernelState: makeKernelState(),
      syncEffects: 1,
      debug: "",
    },
  );

  await Promise.resolve();
  expect(delivered).toEqual([]);
  expect(requestPipeStateSync).toHaveBeenCalledWith(expect.anything(), true);
});

test("status handler treats zero OK sync effects as explicit no-op", async () => {
  const requestPipeStateSync = vi.fn(async () => {});
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, makeThreadState());

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog: () => {},
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync,
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 1,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    {
      pid: 7,
      stdoutPipe: 2,
      stderrPipe: 3,
      stdinPipe: 1,
      stdinQueue: [],
      hostStdio: {
        stdinPipe: 1,
        stdoutPipe: 2,
        stderrPipe: 3,
        refCount: 1,
      },
      memory: new WebAssembly.Memory({ initial: 2, maximum: 2, shared: true }),
      outputBacklog: [],
      workers: new Set([worker]),
      readyWorkers: [],
      blockedWorkers: new Map(),
      kernelWorkerRequest: async () => ({ type: "unexpected" }) as never,
      kernelShare: { members: new Set(), sharedOfdIds: new Set() },
      runtimeBridge: {
        notifySab: new SharedArrayBuffer(12),
        stateSab: new SharedArrayBuffer(176),
      },
      kernel: makeKernel(7, {
        abi_max_fds: () => 0,
        abi_max_threads: () => 0,
        abi_status_clone_spawn: () => 9,
        process_thread_vm_ptr: () => 0,
        thread_process_thread_count: () => 0,
      }),
      kernelState: makeKernelState(),
      onStdout: null,
      runningWorker: worker,
    } as never,
    worker,
    {
      type: "status",
      status: 0,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      kernelState: makeKernelState(),
      syncEffects: 0,
      debug: "",
    },
  );

  await Promise.resolve();
  expect(requestPipeStateSync).not.toHaveBeenCalled();
});

test("status handler applies thread pipe slot snapshots before pipe sync", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  const thread = makeThreadState();
  threadWorkerStates.set(worker, thread);
  const enqueueWorker = vi.fn();
  const clearBlockedWorker = vi.fn();
  const kickBlockedWorkers = vi.fn();
  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    clearBlockedWorker,
    enqueueWorker,
    kickBlockedWorkers,
    blockWorker: () => {},
    appendWorkerDebugLog: () => {},
    failProcess: (handle, error) => { throw error; },
    pumpProcess: () => {},
    releaseSharedHostStdio: () => {},
    recordChildExitViaKernelWorker: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    commitNetworkConnect: async () => {},
    resumeVforkParentViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  const memory = new WebAssembly.Memory({ initial: 80, maximum: 80, shared: true });
  const kernelState = { ...makeKernelState(), pipeBase: 0x1000 };
  const pipeSlot = 5;
  const slot = new Uint8Array(PIPE_SLOT_SIZE);
  const slotView = new DataView(slot.buffer);
  slotView.setInt32(8, 1, true);
  slotView.setInt32(12, 1, true);
  slot[32] = 0x7a;
  const handle = {
    pid: 7,
    memory,
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    stdinQueue: [],
    outputBacklog: [],
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: { notifySab: new SharedArrayBuffer(12), stateSab: new SharedArrayBuffer(176) },
    kernel: makeKernel(7, {
      abi_max_pipes: () => 64,
      kernel_pipe_region_bytes: () => 64 * PIPE_SLOT_SIZE,
      kernel_attach_runtime_state: vi.fn(),
    }),
    kernelState,
    runningWorker: worker,
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
  };

  thread.generation = 1;
  handler(handle, worker, {
    type: "status",
    status: 0,
    generation: 1,
    vmPtr: 0x1234,
    exitCode: 0,
    termSignal: 0,
    pc: 0x2000n,
    sp: 0x3000n,
    s11: 0x4000n,
    kernelState,
    pipeSlots: [{ pipeSlot, slot }],
    syncEffects: 0,
  });

  const applied = new Uint8Array(memory.buffer, kernelState.pipeBase + pipeSlot * PIPE_SLOT_SIZE, PIPE_SLOT_SIZE);
  const appliedView = new DataView(memory.buffer, kernelState.pipeBase + pipeSlot * PIPE_SLOT_SIZE, PIPE_SLOT_SIZE);
  expect(applied[32]).toBe(0x7a);
  expect(withHandleDefaults(handle).pendingPipeSlotSnapshots).toHaveLength(1);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);

  appliedView.setUint32(0, 9, true);
  appliedView.setUint32(4, 9, true);
  appliedView.setInt32(8, 1, true);
  appliedView.setInt32(12, 1, true);

  const staleReaderSlot = new Uint8Array(PIPE_SLOT_SIZE);
  const staleReaderView = new DataView(staleReaderSlot.buffer);
  staleReaderView.setUint32(0, 0, true);
  staleReaderView.setUint32(4, 9, true);
  staleReaderView.setInt32(8, 1, true);
  staleReaderView.setInt32(12, 1, true);
  staleReaderSlot[32] = 0x99;

  thread.generation = 2;
  handler(handle, worker, {
    type: "status",
    status: 0,
    generation: 2,
    vmPtr: 0x1234,
    exitCode: 0,
    termSignal: 0,
    pc: 0x2000n,
    sp: 0x3000n,
    s11: 0x4000n,
    kernelState,
    pipeSlots: [{ pipeSlot, slot: staleReaderSlot }],
    syncEffects: 0,
  });

  expect(appliedView.getUint32(0, true)).toBe(9);
  expect(appliedView.getUint32(4, true)).toBe(9);
  expect(applied[32]).toBe(0x7a);
  {
    const pending = withHandleDefaults(handle).pendingPipeSlotSnapshots;
    const pendingView = new DataView(pending[1].slot.buffer, pending[1].slot.byteOffset, pending[1].slot.byteLength);
    expect(pendingView.getUint32(0, true)).toBe(9);
    expect(pendingView.getUint32(4, true)).toBe(9);
    expect(pending[1].slot[32]).toBe(0x7a);
  }

  const writerSlot = new Uint8Array(PIPE_SLOT_SIZE);
  const writerView = new DataView(writerSlot.buffer);
  writerView.setUint32(0, 0, true);
  writerView.setUint32(4, 12, true);
  writerView.setInt32(8, 1, true);
  writerView.setInt32(12, 1, true);
  writerSlot[32 + 1] = 0xde;
  writerSlot[32 + 9] = 0xab;
  applied[32 + 1] = 0x7b;

  thread.generation = 3;
  handler(handle, worker, {
    type: "status",
    status: 0,
    generation: 3,
    vmPtr: 0x1234,
    exitCode: 0,
    termSignal: 0,
    pc: 0x2000n,
    sp: 0x3000n,
    s11: 0x4000n,
    kernelState,
    pipeSlots: [{ pipeSlot, slot: writerSlot }],
    syncEffects: 0,
  });

  expect(appliedView.getUint32(0, true)).toBe(9);
  expect(appliedView.getUint32(4, true)).toBe(12);
  expect(applied[32 + 1]).toBe(0x7b);
  expect(applied[32 + 9]).toBe(0xab);
  {
    const pending = withHandleDefaults(handle).pendingPipeSlotSnapshots;
    const pendingView = new DataView(pending[2].slot.buffer, pending[2].slot.byteOffset, pending[2].slot.byteLength);
    expect(pendingView.getUint32(0, true)).toBe(9);
    expect(pendingView.getUint32(4, true)).toBe(12);
    expect(pending[2].slot[32 + 1]).toBe(0x7b);
    expect(pending[2].slot[32 + 9]).toBe(0xab);
  }

  const staleDrainedReaderSlot = new Uint8Array(PIPE_SLOT_SIZE);
  const staleDrainedReaderView = new DataView(staleDrainedReaderSlot.buffer);
  staleDrainedReaderView.setUint32(0, 9, true);
  staleDrainedReaderView.setUint32(4, 9, true);
  staleDrainedReaderView.setInt32(8, 1, true);
  staleDrainedReaderView.setInt32(12, 1, true);

  thread.generation = 4;
  handler(handle, worker, {
    type: "status",
    status: 0,
    generation: 4,
    vmPtr: 0x1234,
    exitCode: 0,
    termSignal: 0,
    pc: 0x2000n,
    sp: 0x3000n,
    s11: 0x4000n,
    kernelState,
    pipeSlots: [{ pipeSlot, slot: staleDrainedReaderSlot, mode: 1 }],
    syncEffects: 0,
  });

  expect(appliedView.getUint32(0, true)).toBe(9);
  expect(appliedView.getUint32(4, true)).toBe(12);
  expect(applied[32 + 9]).toBe(0xab);
  {
    const pending = withHandleDefaults(handle).pendingPipeSlotSnapshots;
    const pendingView = new DataView(pending[3].slot.buffer, pending[3].slot.byteOffset, pending[3].slot.byteLength);
    expect(pendingView.getUint32(0, true)).toBe(9);
    expect(pendingView.getUint32(4, true)).toBe(12);
    expect(pending[3].slot[32 + 9]).toBe(0xab);
  }

  const drainedReaderSlot = new Uint8Array(PIPE_SLOT_SIZE);
  const drainedReaderView = new DataView(drainedReaderSlot.buffer);
  drainedReaderView.setUint32(0, 20, true);
  drainedReaderView.setUint32(4, 20, true);
  drainedReaderView.setInt32(8, 1, true);
  drainedReaderView.setInt32(12, 1, true);

  appliedView.setUint32(0, 0, true);
  appliedView.setUint32(4, 0, true);

  thread.generation = 5;
  handler(handle, worker, {
    type: "status",
    status: 0,
    generation: 5,
    vmPtr: 0x1234,
    exitCode: 0,
    termSignal: 0,
    pc: 0x2000n,
    sp: 0x3000n,
    s11: 0x4000n,
    kernelState,
    pipeSlots: [{ pipeSlot, slot: drainedReaderSlot, mode: 1 }],
    syncEffects: 0,
  });

  expect(appliedView.getUint32(0, true)).toBe(20);
  expect(appliedView.getUint32(4, true)).toBe(20);
  {
    const pending = withHandleDefaults(handle).pendingPipeSlotSnapshots;
    const pendingView = new DataView(pending[4].slot.buffer, pending[4].slot.byteOffset, pending[4].slot.byteLength);
    expect(pendingView.getUint32(0, true)).toBe(20);
    expect(pendingView.getUint32(4, true)).toBe(20);
  }
});

test("status handler adopts kernel state for transient pid mismatch while the active vfork worker is running", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
  }>();
  threadWorkerStates.set(worker, makeThreadState({ ownerPid: 1 }));

  const appendWorkerDebugLog = vi.fn();
  const enqueueWorker = vi.fn();
  const kickBlockedWorkers = vi.fn();
  const initialKernelState = makeKernelState();
  const incomingKernelState = {
    ...makeKernelState(),
    heapBase: 0x1000,
    heapPtr: 0x2000,
    socketUsed: 7,
    pipeUsed: 11n,
    netBufUsed: 13n,
  };
  const attachRuntimeState = vi.fn();

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog,
    clearBlockedWorker: () => {},
    enqueueWorker,
    pumpProcess: () => {},
    kickBlockedWorkers,
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  const handle = {
    pid: 1,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(1, {
      abi_status_clone_spawn: () => 9,
      abi_status_socket_recv: () => 23,
      abi_status_socket_send: () => 24,
      abi_status_connect: () => 28,
      abi_status_flock_wait: () => 32,
      abi_status_execve_spawn: () => 10,
      abi_status_vfork_wait: () => 8,
      kernel_attach_runtime_state: attachRuntimeState,
    }),
    kernelState: initialKernelState,
    onStdout: null,
    runningWorker: worker,
    vforkSnapshot: {
      vmPtr: 0x1234,
      parentPid: 1,
      parentProcessPtr: 0x5000,
      childPid: 4,
      nextPid: 5,
      pgid: 1,
      sid: 1,
      inheritedSharedOfdIds: [],
    },
    vforkWorker: worker,
  } as never;

  handler(
    handle,
    worker,
    {
      type: "status",
      status: 0,
      generation: 1,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0x2000n,
      sp: 0x3000n,
      s11: 0x4000n,
      kernelState: incomingKernelState,
      debug: "pc=0x2000 sp=0x3000 s11=0x4000 process_pid_field=4",
    },
  );

  expect(handle.kernelState).toEqual({
    ...initialKernelState,
    heapBase: 0x1000,
    heapPtr: 0x2000,
    socketUsed: 7,
    pipeUsed: 11n,
    netBufUsed: 13n,
  });
  expect(attachRuntimeState).toHaveBeenCalledWith(
    handle.kernelState.heapBase,
    handle.kernelState.heapPtr,
    handle.kernelState.ofdTablePtr,
    handle.kernelState.memfsPtr,
    handle.kernelState.memStatePtr,
    handle.kernelState.epollTablePtr,
    handle.kernelState.socketTablePtr,
    handle.kernelState.socketUsed,
    handle.kernelState.pipeBase,
    handle.kernelState.pipeUsed,
    handle.kernelState.netBufBase,
    handle.kernelState.netBufUsed,
    handle.kernelState.sigSlotsPtr,
    handle.kernelState.processPendingPtr,
    handle.kernelState.processRuntimeStatePtr,
    handle.kernelState.timeStatePtr,
    handle.kernelState.signalfdStatePtr ?? 0,
  );
  expect(kickBlockedWorkers).toHaveBeenCalledWith(handle);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "status_debug:0x1234:handle_pid=1:process_pid_field=4",
  );
});

test("status handler does not adopt child fault state while resuming an active vfork parent", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
  }>();
  threadWorkerStates.set(worker, makeThreadState({ ownerPid: 1 }));

  const appendWorkerDebugLog = vi.fn();
  const clearBlockedWorker = vi.fn();
  const resumeVforkParentAfterChildExit = vi.fn(() => true);
  const initialKernelState = makeKernelState();
  const incomingKernelState = {
    ...makeKernelState(),
    heapBase: 0x1000,
    heapPtr: 0x2000,
    socketUsed: 7,
    pipeUsed: 11n,
    netBufUsed: 13n,
  };
  const attachRuntimeState = vi.fn();

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog,
    clearBlockedWorker,
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  const handle = {
    pid: 1,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(1, {
      abi_status_clone_spawn: () => 9,
      abi_status_socket_recv: () => 23,
      abi_status_socket_send: () => 24,
      abi_status_connect: () => 28,
      abi_status_flock_wait: () => 32,
      abi_status_execve_spawn: () => 10,
      abi_status_vfork_wait: () => 8,
      kernel_attach_runtime_state: attachRuntimeState,
    }),
    kernelState: initialKernelState,
    onStdout: null,
    runningWorker: worker,
    vforkSnapshot: {
      vmPtr: 0x1234,
      parentPid: 1,
      parentProcessPtr: 0x5000,
      childPid: 4,
      nextPid: 5,
      pgid: 1,
      sid: 1,
      inheritedSharedOfdIds: [],
    },
    vforkWorker: worker,
  } as never;

  handler(
    handle,
    worker,
    {
      type: "status",
      status: 3,
      generation: 1,
      vmPtr: 0x1234,
      exitCode: 12,
      termSignal: 0,
      pc: 0x2000n,
      sp: 0x3000n,
      s11: 0x4000n,
      kernelState: incomingKernelState,
      debug: "pc=0x2000 sp=0x3000 s11=0x4000 process_pid_field=4",
    },
  );

  expect(resumeVforkParentAfterChildExit).toHaveBeenCalledWith(handle, worker, 0x1234, 12, 0);
  expect(handle.kernelState).toBe(initialKernelState);
  expect(attachRuntimeState).not.toHaveBeenCalled();
  expect(clearBlockedWorker).toHaveBeenCalledWith(handle, worker);
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "status_debug:0x1234:handle_pid=1:process_pid_field=4",
  );
});

test("status handler preserves host-injected network recv buffer when applying thread socket snapshots", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, makeThreadState());
  const netBufferBytes = 64;
  const netBufPoolBytes = netBufferBytes * 2;
  const netBufBase = 0x1000;
  const recvBufIdx = 1;
  const recvBufBase = netBufBase + recvBufIdx * netBufferBytes;
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const bytes = new Uint8Array(memory.buffer);
  const view = new DataView(memory.buffer);
  bytes[0x800 + 3] = 0x77;
  view.setUint32(recvBufBase, 8, true);
  view.setUint32(recvBufBase + 4, 17, true);
  bytes[recvBufBase + 16] = 0xaa;
  bytes[recvBufBase + 17] = 0xbb;

  const handle = {
    pid: 7,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory,
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(7, {
      abi_max_sockets: () => 1,
      abi_status_clone_spawn: () => 9,
      kernel_socket_table_bytes: () => 32,
      kernel_net_buf_pool_bytes: () => netBufPoolBytes,
      thread_socket_idx_for_fd: () => 0,
      thread_socket_recv_buf_idx: () => recvBufIdx,
      thread_socket_read_bytes: () => 17n,
    }),
    kernelState: {
      ...makeKernelState(),
      socketTablePtr: 0x800,
      netBufBase,
      netBufUsed: 0b10n,
    },
    onStdout: null,
    runningWorker: worker,
    networkConnections: new Map([[7, {
      read: () => undefined,
      write: () => {},
    }]]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
  } as never;

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog: () => {},
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    handle,
    worker,
    {
      type: "status",
      status: 0,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0x2000n,
      sp: 0x3000n,
      s11: 0x4000n,
      kernelState: makeKernelState(),
      socketState: {
        socketTable: new Uint8Array(32),
        netBufPool: new Uint8Array(netBufPoolBytes),
        socketUsed: 1,
        netBufUsed: 0n,
      },
      debug: "pc=0x2000 sp=0x3000 s11=0x4000 process_thread_count=1",
    },
  );

  expect(view.getUint32(recvBufBase, true)).toBe(17);
  expect(view.getUint32(recvBufBase + 4, true)).toBe(17);
  expect(bytes[recvBufBase + 16]).toBe(0xaa);
  expect(bytes[recvBufBase + 17]).toBe(0xbb);
  expect(bytes[0x800 + 3]).toBe(0x77);
  expect(handle.kernelState.netBufUsed).toBe(0b10n);
});

test("status handler keeps newer thread recv payload when producer tail is not ahead", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, makeThreadState());
  const netBufferBytes = 64;
  const netBufPoolBytes = netBufferBytes * 2;
  const netBufBase = 0x1000;
  const recvBufIdx = 1;
  const recvBufBase = netBufBase + recvBufIdx * netBufferBytes;
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const bytes = new Uint8Array(memory.buffer);
  const view = new DataView(memory.buffer);
  view.setUint32(recvBufBase, 0, true);
  view.setUint32(recvBufBase + 4, 17, true);
  bytes[recvBufBase + 16] = 0x00;
  bytes[recvBufBase + 17] = 0x00;

  const incomingNetBufPool = new Uint8Array(netBufPoolBytes);
  const incomingView = new DataView(incomingNetBufPool.buffer);
  incomingView.setUint32(recvBufIdx * netBufferBytes, 0, true);
  incomingView.setUint32(recvBufIdx * netBufferBytes + 4, 17, true);
  incomingNetBufPool[recvBufIdx * netBufferBytes + 16] = 0x11;
  incomingNetBufPool[recvBufIdx * netBufferBytes + 17] = 0x22;

  const handle = {
    pid: 7,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory,
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(7, {
      abi_max_sockets: () => 1,
      abi_status_clone_spawn: () => 9,
      kernel_socket_table_bytes: () => 32,
      kernel_net_buf_pool_bytes: () => netBufPoolBytes,
      thread_socket_idx_for_fd: () => 0,
      thread_socket_recv_buf_idx: () => recvBufIdx,
      thread_socket_read_bytes: () => 0n,
    }),
    kernelState: {
      ...makeKernelState(),
      socketTablePtr: 0x800,
      netBufBase,
      netBufUsed: 0b10n,
    },
    onStdout: null,
    runningWorker: worker,
    networkConnections: new Map([[7, {
      read: () => undefined,
      write: () => {},
    }]]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
  } as never;

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog: () => {},
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    handle,
    worker,
    {
      type: "status",
      status: 0,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0x2000n,
      sp: 0x3000n,
      s11: 0x4000n,
      kernelState: makeKernelState(),
      socketState: {
        socketTable: new Uint8Array(32),
        netBufPool: incomingNetBufPool,
        socketUsed: 1,
        netBufUsed: 0b10n,
      },
      debug: "pc=0x2000 sp=0x3000 s11=0x4000 process_thread_count=1",
    },
  );

  expect(view.getUint32(recvBufBase, true)).toBe(0);
  expect(view.getUint32(recvBufBase + 4, true)).toBe(17);
  expect(bytes[recvBufBase + 16]).toBe(0x11);
  expect(bytes[recvBufBase + 17]).toBe(0x22);
  expect(handle.kernelState.netBufUsed).toBe(0b10n);
});

test("status handler preserves host recv payload when producer tail wraps", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, makeThreadState());
  const netBufferBytes = 64;
  const netBufPoolBytes = netBufferBytes * 2;
  const capacity = netBufferBytes - 16;
  const netBufBase = 0x1000;
  const recvBufIdx = 1;
  const recvBufBase = netBufBase + recvBufIdx * netBufferBytes;
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const bytes = new Uint8Array(memory.buffer);
  const view = new DataView(memory.buffer);
  const staleTail = capacity - 1;
  const wrappedTail = Math.floor(capacity * 0.65);
  const availableBytes = (wrappedTail - staleTail + capacity) % capacity;
  view.setUint32(recvBufBase, staleTail, true);
  view.setUint32(recvBufBase + 4, wrappedTail, true);
  bytes[recvBufBase + 16] = 0xaa;
  bytes[recvBufBase + 17] = 0xbb;

  const incomingNetBufPool = new Uint8Array(netBufPoolBytes);
  const incomingView = new DataView(incomingNetBufPool.buffer);
  incomingView.setUint32(recvBufIdx * netBufferBytes, staleTail, true);
  incomingView.setUint32(recvBufIdx * netBufferBytes + 4, staleTail, true);
  incomingNetBufPool[recvBufIdx * netBufferBytes + 16] = 0x11;
  incomingNetBufPool[recvBufIdx * netBufferBytes + 17] = 0x22;

  const handle = {
    pid: 7,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory,
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(7, {
      abi_max_sockets: () => 1,
      abi_status_clone_spawn: () => 9,
      kernel_socket_table_bytes: () => 32,
      kernel_net_buf_pool_bytes: () => netBufPoolBytes,
      thread_socket_idx_for_fd: () => 0,
      thread_socket_recv_buf_idx: () => recvBufIdx,
      thread_socket_recv_available: () => availableBytes,
      thread_socket_read_bytes: () => BigInt(staleTail),
    }),
    kernelState: {
      ...makeKernelState(),
      socketTablePtr: 0x800,
      netBufBase,
      netBufUsed: 0b10n,
    },
    onStdout: null,
    runningWorker: worker,
    networkConnections: new Map([[7, {
      read: () => undefined,
      write: () => {},
    }]]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    networkRecvPayloadWatermarks: new Map([[7, {
      recvBufIdx,
      head: staleTail,
      tail: wrappedTail,
      capacity,
      readBytes: staleTail,
      availableBytes,
      deliveredBytes: staleTail + availableBytes,
      data: bytes.slice(recvBufBase + 16, recvBufBase + netBufferBytes),
    }]]),
  } as never;

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog: () => {},
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    handle,
    worker,
    {
      type: "status",
      status: 0,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0x2000n,
      sp: 0x3000n,
      s11: 0x4000n,
      kernelState: makeKernelState(),
      socketState: {
        socketTable: new Uint8Array(32),
        netBufPool: incomingNetBufPool,
        socketUsed: 1,
        netBufUsed: 0b10n,
      },
      debug: "pc=0x2000 sp=0x3000 s11=0x4000 process_thread_count=1",
    },
  );

  expect(view.getUint32(recvBufBase, true)).toBe(staleTail);
  expect(view.getUint32(recvBufBase + 4, true)).toBe(wrappedTail);
  expect(bytes[recvBufBase + 16]).toBe(0xaa);
  expect(bytes[recvBufBase + 17]).toBe(0xbb);
  expect(handle.kernelState.netBufUsed).toBe(0b10n);
});

test("status handler preserves closed network socket recv head when fd no longer resolves", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, makeThreadState());
  const netBufferBytes = 64;
  const netBufPoolBytes = netBufferBytes * 2;
  const netBufBase = 0x1000;
  const socketTablePtr = 0x800;
  const socketSlotBytes = 512;
  const recvBufIdx = 1;
  const recvBufBase = netBufBase + recvBufIdx * netBufferBytes;
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const bytes = new Uint8Array(memory.buffer);
  const view = new DataView(memory.buffer);
  const closedSlot = new Uint8Array(socketSlotBytes);
  new DataView(closedSlot.buffer).setInt32(312, recvBufIdx, true);
  view.setUint32(recvBufBase, 8192, true);
  view.setUint32(recvBufBase + 4, 8192, true);
  bytes[recvBufBase + 16] = 0x42;
  const incomingNetBufPool = new Uint8Array(netBufPoolBytes);
  const incomingView = new DataView(incomingNetBufPool.buffer);
  incomingView.setUint32(recvBufIdx * netBufferBytes, 4096, true);
  incomingView.setUint32(recvBufIdx * netBufferBytes + 4, 4096, true);
  incomingNetBufPool[recvBufIdx * netBufferBytes + 16] = 0x99;

  const handle = {
    pid: 7,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory,
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(7, {
      abi_max_sockets: () => 1,
      abi_status_clone_spawn: () => 9,
      kernel_socket_table_bytes: () => socketSlotBytes,
      kernel_net_buf_pool_bytes: () => netBufPoolBytes,
      thread_socket_idx_for_fd: () => -88,
      thread_socket_recv_buf_idx: () => -88,
    }),
    kernelState: {
      ...makeKernelState(),
      socketTablePtr,
      netBufBase,
      socketUsed: 0,
      netBufUsed: 0n,
    },
    onStdout: null,
    runningWorker: worker,
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map([[7, { offset: 0, data: closedSlot }]]),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
  } as never;

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog: () => {},
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    handle,
    worker,
    {
      type: "status",
      status: 0,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0x2000n,
      sp: 0x3000n,
      s11: 0x4000n,
      kernelState: makeKernelState(),
      socketState: {
        socketTable: new Uint8Array(socketSlotBytes),
        netBufPool: incomingNetBufPool,
        socketUsed: 0,
        netBufUsed: 0n,
      },
      debug: "pc=0x2000 sp=0x3000 s11=0x4000 process_thread_count=1",
    },
  );

  expect(view.getUint32(recvBufBase, true)).toBe(8192);
  expect(view.getUint32(recvBufBase + 4, true)).toBe(8192);
  expect(bytes[recvBufBase + 16]).toBe(0x42);
  expect(handle.kernelState.netBufUsed).toBe(0b10n);
});

test("status handler closes stale network bridge when fd snapshot drops the socket fd", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, makeThreadState());
  const fdMap = new Map<number, number>([
    [0, 7],
    [1, 6],
    [2, 3],
    [3, 11],
  ]);
  const close = vi.fn(async () => {});
  const appendWorkerDebugLog = vi.fn();
  const handle = {
    pid: 4,
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    stdinQueue: [],
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    outputBacklog: [],
    workers: new Set([worker]),
    readyWorkers: [],
    blockedWorkers: new Map(),
    kernelWorkerRequest: undefined,
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    kernel: makeKernel(4, {
      abi_fd_type_socket: () => 8,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
        if (ofdId > 0) {
          fdMap.set(fd, ofdId);
        } else {
          fdMap.delete(fd);
        }
      },
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 11 ? 8 : 0,
      kernel_ofd_ino: () => 0,
      kernel_ofd_host_fd: () => -1,
    }),
    kernelState: makeKernelState(),
    onStdout: null,
    runningWorker: worker,
    networkConnections: new Map([[3, { close, read: () => undefined, write: () => {} }]]),
    networkConnectionOfds: new Map([[3, 11]]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map([[3, 4096]]),
    networkRecvTailWatermarks: new Map([[3, 8192]]),
  } as never;

  const handler = createWorkerStatusMessageHandler({
    threadWorkerStates,
    appendWorkerDebugLog,
    clearBlockedWorker: () => {},
    enqueueWorker: () => {},
    pumpProcess: () => {},
    kickBlockedWorkers: () => {},
    failProcess: () => {},
    requestPipeStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    captureVforkSnapshot: () => {
      throw new Error("unexpected");
    },
    syncForkSharedStateViaKernelWorker: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    attachThreadWorker: () => {},
    completeProcessExit: async () => {},
    handleExecveSpawn: async () => {},
    blockWorker: () => {},
    assertProcessStandardFdIntegrity: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncEffectEpollState: 4,
    syncEffectEpollDependentOfdState: 8,
    syncEffectProcessLifecycle: 16,
    syncEffectSignalWaiters: 32,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    statusDefersOwnedStateSync: () => false,
  } as never);

  handler(
    handle,
    worker,
    {
      type: "status",
      status: 21,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0x2000n,
      sp: 0x3000n,
      s11: 0x4000n,
      kernelState: makeKernelState(),
      fdEntries: [
        { fd: 0, ofdId: 7, fdFlags: 0 },
        { fd: 1, ofdId: 6, fdFlags: 0 },
        { fd: 2, ofdId: 3, fdFlags: 0 },
        { fd: 3, ofdId: 0, fdFlags: 0 },
      ],
      debug: "pc=0x2000 sp=0x3000 s11=0x4000 process_thread_count=1",
    },
  );

  expect(handle.networkConnections.has(3)).toBe(false);
  expect(handle.networkConnectionOfds.has(3)).toBe(false);
  expect(handle.networkRecvHeadWatermarks.has(3)).toBe(false);
  expect(handle.networkRecvTailWatermarks.has(3)).toBe(false);
  expect(close).toHaveBeenCalledTimes(1);
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "net-status-close:4:fd=3:ofd=11->0",
  );
});
