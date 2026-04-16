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

test("status handler logs futex resume with compact debug fields", () => {
  const appendWorkerDebugLog = vi.fn();
  const enqueueWorker = vi.fn();
  const clearBlockedWorker = vi.fn();
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: 20,
  });

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
      kernel: {
        abi_status_ok: () => 0,
        abi_status_clone_spawn: () => 9,
        abi_status_futex_wait: () => 20,
        abi_status_epoll_wait: () => 22,
        thread_pc: () => 0x2000n,
        thread_sp: () => 0x3000n,
        thread_s11: () => 0x4000n,
        thread_epoll_wait_timeout: () => 0n,
        kernel_attach_runtime_state: () => {},
      },
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
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

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
    kernel: {
      abi_status_ok: () => 0,
      abi_status_fault: () => 3,
      abi_status_clone_spawn: () => 9,
      abi_status_futex_wait: () => 20,
      abi_status_epoll_wait: () => 22,
      thread_pc: () => 0x2000n,
      thread_sp: () => 0x3000n,
      thread_s11: () => 0x4000n,
      thread_process_thread_count: () => 0,
      abi_max_threads: () => 4,
      process_thread_vm_ptr: () => 0,
      kernel_attach_runtime_state: () => {},
    },
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

test("status handler logs quoted write debug fields before wait handling", () => {
  const appendWorkerDebugLog = vi.fn();
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

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
      kernel: {
        abi_status_ok: () => 0,
        abi_status_clone_spawn: () => 9,
        abi_status_futex_wait: () => 20,
        abi_status_epoll_wait: () => 22,
        abi_status_socket_recv: () => 23,
        abi_status_socket_send: () => 24,
        thread_pc: () => 0x2000n,
        thread_sp: () => 0x3000n,
        thread_s11: () => 0x4000n,
        thread_epoll_wait_timeout: () => 0n,
        kernel_attach_runtime_state: () => {},
      },
      kernelState: makeKernelState(),
      onStdout: null,
      runningWorker: worker,
    } as never,
    worker,
    {
      type: "status",
      status: 20,
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
    "status:0x1234@20:pc=0x4d650:gp=0x11dab8:tp=0x11eff0",
  );
  expect(appendWorkerDebugLog).toHaveBeenNthCalledWith(
    2,
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
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

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
    kernel: {
      abi_status_ok: () => 0,
      abi_status_fault: () => 3,
      abi_status_fs: () => 4,
      abi_status_futex_wait: () => 20,
      abi_status_pipe_wait: () => 21,
      abi_status_epoll_wait: () => 22,
      abi_status_connect: () => 28,
      abi_status_socket_send: () => 29,
      abi_status_socket_recv: () => 30,
      abi_status_flock_wait: () => 32,
      abi_status_sleep: () => 11,
      abi_status_execve_spawn: () => 9,
      abi_status_clone_spawn: () => 10,
      thread_pc: () => 0x2000n,
      thread_sp: () => 0x3000n,
      thread_s11: () => 0x4000n,
      thread_epoll_wait_timeout: () => 0n,
      kernel_attach_runtime_state: () => {},
    },
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
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

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
      kernel: {
        abi_status_ok: () => 0,
        abi_status_clone_spawn: () => 9,
        abi_status_futex_wait: () => 20,
        abi_status_epoll_wait: () => 22,
        thread_pc: () => 0x2000n,
        thread_sp: () => 0x3000n,
        thread_s11: () => 0x4000n,
        thread_epoll_wait_timeout: () => 0n,
        kernel_attach_runtime_state: () => {},
      },
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
  threadWorkerStates.set(worker, {
    vmPtr: 0x3333,
    initialized: true,
    waitingInWorkerStatus: 20,
  });

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
      kernel: {
        abi_status_ok: () => 0,
        abi_status_clone_spawn: () => 9,
        abi_status_futex_wait: () => 20,
        abi_status_epoll_wait: () => 22,
        thread_pc: () => 0x2000n,
        thread_sp: () => 0x3000n,
        thread_s11: () => 0x4000n,
        thread_epoll_wait_timeout: () => 0n,
        kernel_attach_runtime_state: () => {},
      },
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
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

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
      kernel: {
        abi_max_fds: () => 0,
        abi_max_threads: () => 0,
        abi_status_ok: () => 0,
        abi_status_clone_spawn: () => 9,
        abi_status_futex_wait: () => 20,
        abi_status_epoll_wait: () => 22,
        abi_status_socket_recv: () => 23,
        abi_status_socket_send: () => 24,
        thread_pc: () => 0x2000n,
        thread_sp: () => 0x3000n,
        thread_s11: () => 0x4000n,
        kernel_attach_runtime_state: () => {},
        process_fd_ofd_id: () => 0,
        process_thread_vm_ptr: () => 0,
        thread_process_thread_count: () => 0,
      },
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

test("status handler falls back to default OK sync effects when worker reports zero", async () => {
  const requestPipeStateSync = vi.fn(async () => {});
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

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
      kernel: {
        abi_max_fds: () => 0,
        abi_max_threads: () => 0,
        abi_status_ok: () => 0,
        abi_status_clone_spawn: () => 9,
        abi_status_futex_wait: () => 20,
        abi_status_epoll_wait: () => 22,
        abi_status_socket_recv: () => 23,
        abi_status_socket_send: () => 24,
        thread_pc: () => 0x2000n,
        thread_sp: () => 0x3000n,
        thread_s11: () => 0x4000n,
        kernel_attach_runtime_state: () => {},
        process_fd_ofd_id: () => 0,
        process_thread_vm_ptr: () => 0,
        thread_process_thread_count: () => 0,
      },
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
  expect(requestPipeStateSync).toHaveBeenCalledWith(expect.anything(), true);
});
