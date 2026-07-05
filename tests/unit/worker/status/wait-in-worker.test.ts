import { expect, test, vi } from "vite-plus/test";
import { writePipe } from "../../../../src/bridge/pipe.js";
import { createWorkerWaitInWorkerStatusHandler } from "../../../../src/worker/message-handler/status/wait-in-worker.js";
import {
  getThreadRunningGeneration,
  getThreadWaitingInWorkerStatus,
  setThreadRunningGeneration,
} from "../../../../src/worker/thread-host-state.js";
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";

test("waitInWorker debug log keeps compact futex fields", () => {
  const appendWorkerDebugLog = vi.fn();
  const notifyWorkerStatusProgress = vi.fn();
  const parkSupervisorFutexWait = vi.fn();
  const pumpProcess = vi.fn();
  const memory = new WebAssembly.Memory({ initial: 6, maximum: 6, shared: true });
  const thread = { vmPtr: 0x1234, initialized: true, waitingInWorkerStatus: null } as {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
  };
  const handler = createWorkerWaitInWorkerStatusHandler({
    appendWorkerDebugLog,
    clearBlockedWorker: () => {},
    notifyWorkerStatusProgress,
    parkSupervisorFutexWait,
    pumpProcess,
    blockWorker: () => {},
    failProcess: () => {},
    requestEpollStateSync: async () => {},
    requestPipeStateSync: async () => {},
    requestSocketStateSync: async () => {},
  } as never);

  handler(
    {
      stdoutPipe: 0,
      stderrPipe: 0,
      hostStdio: {
        stdinPipe: 0,
        stdoutPipe: 0,
        stderrPipe: 0,
        refCount: 1,
      },
      memory,
      outputBacklog: [],
      kernel: {
        ...makeSupervisorAbiStubs(),
        abi_status_futex_wait: () => 20,
        abi_status_epoll_wait: () => 22,
        abi_status_pipe_wait: () => 21,
        abi_status_socket_recv: () => 23,
        abi_status_socket_send: () => 24,
        supervisor_blocked_status_is_epoll_wait: (status: number) => status === 22 ? 1 : 0,
        supervisor_blocked_status_is_pipe_wait: (status: number) => status === 21 ? 1 : 0,
        supervisor_blocked_status_is_wait4: (status: number) => status === 31 ? 1 : 0,
        supervisor_blocked_status_is_socket_wait: (status: number) =>
          status === 23 || status === 24 ? 1 : 0,
        kernel_heap_ptr: () => 0,
        supervisor_host_op_record_bytes: () => 64,
        supervisor_next_host_op: () => 0,
      },
      kernelState: {
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
      },
      runtimeBridge: {
        notifySab: new SharedArrayBuffer(12),
        stateSab: new SharedArrayBuffer(176),
      },
      onStdout: null,
    } as never,
    {} as Worker,
    thread as never,
    {
      type: "status",
      status: 20,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      kernelState: {
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
      },
      waitInWorker: true,
      debug: [
        "pc=0x4d650",
        "ra=0x529a2",
        "sp=0x0",
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
        "sig_pc=0x80f70",
        "sig_sp=0x0",
        "sig_s11=0x0",
        "zero_reg=2",
        "zero_reg_tid=2",
        "zero_reg_pc=0x5cd10",
        "zero_reg_sp=0x0",
        "s11_write_tid=3",
        "s11_write_pc=0x810f4",
        "thread-futex-wait:stage=entry:vm=0x1234:addr=0x37d61c40:index=987:expected=0:word=0:result=n/a:blocked=0x37d61c40",
        "ignored_field=1",
      ].join(" "),
    },
    true,
  );

  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    expect.anything(),
    "wait:0x1234@20:"
      + "pc=0x4d650:"
      + "ra=0x529a2:"
      + "sp=0x0:"
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
      + "sig_pc=0x80f70:"
      + "sig_sp=0x0:"
      + "sig_s11=0x0:"
      + "zero_reg=2:"
      + "zero_reg_tid=2:"
      + "zero_reg_pc=0x5cd10:"
      + "zero_reg_sp=0x0:"
      + "s11_write_tid=3:"
      + "s11_write_pc=0x810f4",
  );
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    expect.anything(),
    expect.stringContaining(
      "futex_wait_debug:0x1234:",
    ),
  );
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    expect.anything(),
    expect.stringContaining(
      "thread-futex-wait:stage=entry:vm=0x1234:addr=0x37d61c40",
    ),
  );
  expect(getThreadWaitingInWorkerStatus(thread as never)).toBeNull();
  expect(parkSupervisorFutexWait).toHaveBeenCalledWith(
    expect.anything(),
    expect.anything(),
    0x1234,
    "wait-in-worker-futex-supervisor-owned",
  );
  expect(notifyWorkerStatusProgress).toHaveBeenCalledWith(expect.anything());
  expect(pumpProcess).toHaveBeenCalledWith(expect.anything());
});

test("waitInWorker defers local pipe drain when kernel-worker owns pipe state", async () => {
  const requestPipeStateSync = vi.fn(async () => {});
  const delivered: string[] = [];
  const memory = new WebAssembly.Memory({ initial: 8, maximum: 8, shared: true });
  const kernelState = {
    heapBase: 0,
    heapPtr: 0,
    ofdTablePtr: 0,
    memfsPtr: 0,
    memfsHeaderBytes: 32,
    memfsInodesBytes: 0,
    memfsFirstChildBytes: 0,
    memfsDirEntriesBytes: 0,
    memfsDataBytes: 0,
    memStatePtr: 0,
    epollTablePtr: 0,
    socketTablePtr: 0,
    socketUsed: 0,
    pipeBase: 0x1000,
    pipeUsed: 0n,
    netBufBase: 0,
    netBufUsed: 0n,
    sigSlotsPtr: 0,
    processPendingPtr: 0,
    processRuntimeStatePtr: 0,
    timeStatePtr: 0,
  };
  const pipeBase = kernelState.pipeBase + 2 * (32 + 65_536);
  const view = new DataView(memory.buffer);
  view.setInt32(pipeBase + 8, 1, true);
  view.setInt32(pipeBase + 12, 0, true);
  const seeded = writePipe(memory, kernelState.pipeBase, 2, new TextEncoder().encode("$ "));
  if (seeded.broken || seeded.written !== 2) {
    throw new Error("failed to seed stdout pipe");
  }

  const handler = createWorkerWaitInWorkerStatusHandler({
    appendWorkerDebugLog: () => {},
    clearBlockedWorker: () => {},
    pumpProcess: () => {},
    blockWorker: () => {},
    failProcess: () => {},
    requestEpollStateSync: async () => {},
    requestPipeStateSync,
    requestSocketStateSync: async () => {},
  } as never);

  handler(
    {
      pid: 7,
      stdoutPipe: 2,
      stderrPipe: 3,
      hostStdio: {
        stdinPipe: 0,
        stdoutPipe: 2,
        stderrPipe: 3,
        refCount: 1,
      },
      memory,
      outputBacklog: [],
      kernelWorkerRequest: async () => ({ type: "unexpected" }) as never,
      kernel: {
        abi_status_futex_wait: () => 25,
        abi_status_epoll_wait: () => 22,
        abi_status_pipe_wait: () => 21,
        abi_status_socket_recv: () => 23,
        abi_status_socket_send: () => 24,
        supervisor_blocked_status_is_epoll_wait: (status: number) => status === 22 ? 1 : 0,
        supervisor_blocked_status_is_pipe_wait: (status: number) => status === 21 ? 1 : 0,
        supervisor_blocked_status_is_wait4: (status: number) => status === 31 ? 1 : 0,
        supervisor_blocked_status_is_socket_wait: (status: number) =>
          status === 23 || status === 24 ? 1 : 0,
      },
      kernelState,
      runtimeBridge: {
        notifySab: new SharedArrayBuffer(12),
        stateSab: new SharedArrayBuffer(176),
      },
      onStdout: (data: Uint8Array) => {
        delivered.push(new TextDecoder().decode(data));
      },
    } as never,
    {} as Worker,
    { vmPtr: 0x1234, initialized: true, waitingInWorkerStatus: null } as never,
    {
      type: "status",
      status: 20,
      vmPtr: 0x1234,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      kernelState,
      waitInWorker: true,
      debug: "",
    },
    true,
  );

  await Promise.resolve();
  expect(delivered).toEqual([]);
  expect(requestPipeStateSync).not.toHaveBeenCalled();
});

test("waitInWorker WAIT4 routes to owner-side blocked scheduling with kernel-worker support", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x4321,
    generation: 9,
    ownerPid: 17,
    initialized: true,
    waitingInWorkerStatus: null as number | null,
  };
  setThreadRunningGeneration(thread as never, 12);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  const blockWorker = vi.fn();
  const requestPipeStateSync = vi.fn(async () => {});
  const notifyWorkerStatusProgress = vi.fn();
  const pumpProcess = vi.fn();
  const handler = createWorkerWaitInWorkerStatusHandler({
    appendWorkerDebugLog: () => {},
    clearBlockedWorker: () => {},
    pumpProcess,
    notifyWorkerStatusProgress,
    blockWorker,
    failProcess: () => {},
    requestEpollStateSync: async () => {},
    requestPipeStateSync,
    requestSocketStateSync: async () => {},
    threadWorkerStates,
  } as never);
  const kernelState = {
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
  const handle = {
    pid: 17,
    stdoutPipe: 0,
    stderrPipe: 0,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    outputBacklog: [],
    workers: new Set([worker]),
    kernelWorkerRequest: async () => ({ type: "unexpected" }) as never,
    kernel: {
      abi_status_futex_wait: () => 25,
      abi_status_epoll_wait: () => 22,
      abi_status_pipe_wait: () => 21,
      abi_status_socket_recv: () => 23,
      abi_status_socket_send: () => 24,
      abi_status_wait4: () => 20,
      supervisor_blocked_status_is_epoll_wait: (status: number) => status === 22 ? 1 : 0,
      supervisor_blocked_status_is_pipe_wait: (status: number) => status === 21 ? 1 : 0,
      supervisor_blocked_status_is_wait4: (status: number) => status === 20 ? 1 : 0,
      supervisor_blocked_status_is_socket_wait: (status: number) =>
        status === 23 || status === 24 ? 1 : 0,
    },
    kernelState,
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    onStdout: null,
    runningWorker: worker,
    vforkSnapshot: null,
    vforkWorker: null,
  } as never;

  handler(
    handle,
    worker,
    thread as never,
    {
      type: "status",
      status: 20,
      vmPtr: 0x4321,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      kernelState,
      waitInWorker: true,
      debug: "",
    },
    true,
  );

  expect(handle.runningWorker).toBeNull();
  expect(getThreadRunningGeneration(thread)).toBeNull();
  expect(getThreadWaitingInWorkerStatus(thread as never)).toBeNull();
  expect(blockWorker).toHaveBeenCalledWith(handle, worker, 20, 0x4321);
  expect(requestPipeStateSync).toHaveBeenCalledWith(handle, true);

  await Promise.resolve();
  await Promise.resolve();
  expect(notifyWorkerStatusProgress).toHaveBeenCalledWith(handle);
  expect(pumpProcess).toHaveBeenCalledWith(handle);
});
