import { expect, test, vi } from "vite-plus/test";
import { writePipe } from "../../../src/bridge/pipe.js";
import { createWorkerStatusMessageHandler } from "../../../src/worker/message-handler/status.js";

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
    initialized: true,
    waitingInWorkerStatus: null,
    ownerPid: 7,
    generation: 1,
    ...overrides,
  };
}

function makeKernel(pid: number, overrides: Record<string, unknown> = {}) {
  return {
    abi_max_fds: () => 64,
    abi_max_threads: () => 4,
    abi_fd_type_epoll: () => 5,
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
    kernel_ofd_table_bytes: () => 64,
    kernel_ofd_refcount: () => 0,
    kernel_retain_ofd: () => {},
    kernel_attach_runtime_state: () => {},
    ...overrides,
  };
}

test("status handler logs futex resume with compact debug fields", () => {
  const appendWorkerDebugLog = vi.fn();
  const enqueueWorker = vi.fn();
  const clearBlockedWorker = vi.fn();
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, makeThreadState({ waitingInWorkerStatus: 20 }));

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

  expect(clearBlockedWorker).toHaveBeenCalledWith(expect.anything(), worker);
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

  expect(completeProcessExit).toHaveBeenCalledWith(handle, 139, 11);
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

  expect(syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  expect(syncBlockedForkSharedStateViaKernelWorker).toHaveBeenCalledWith(handle, 0x1234);
  expect(blockWorker).toHaveBeenCalledWith(handle, worker, 24, 0x1234);
  expect(pumpProcess).toHaveBeenCalledWith(handle);
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
      debug: "spawn_kind=execve",
    },
  );

  expect(clearBlockedWorker).toHaveBeenCalledWith(handle, worker);
  expect(handleExecveSpawn).toHaveBeenCalledTimes(1);
  expect(handleExecveSpawn).toHaveBeenCalledWith(handle, worker, expect.objectContaining({ vmPtr: 0x1234 }));
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
    statusDefersOwnedStateSync: () => false,
  } as never);

  const memory = new WebAssembly.Memory({ initial: 2, maximum: 2, shared: true });
  const kernelState = {
    ...makeKernelState(),
    pipeBase: 0x1000,
  };
  const view = new DataView(memory.buffer);
  const pipeBase = kernelState.pipeBase + 2 * (32 + 4096);
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
  );
  expect(kickBlockedWorkers).toHaveBeenCalledWith(handle);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "status_debug:0x1234:handle_pid=1:process_pid_field=4",
  );
});
