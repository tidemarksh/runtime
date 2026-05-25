import { expect, test, vi } from "vite-plus/test";
import { writePipe } from "../../../src/bridge/pipe.js";
import { createWorkerWaitInWorkerStatusHandler } from "../../../src/worker/message-handler/wait-in-worker.js";

test("waitInWorker debug log keeps compact futex fields", () => {
  const appendWorkerDebugLog = vi.fn();
  const handler = createWorkerWaitInWorkerStatusHandler({
    appendWorkerDebugLog,
    clearBlockedWorker: () => {},
    pumpProcess: () => {},
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
      memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
      outputBacklog: [],
      kernel: {
        abi_status_epoll_wait: () => 22,
        abi_status_socket_recv: () => 23,
        abi_status_socket_send: () => 24,
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
    { vmPtr: 0x1234, initialized: true, waitingInWorkerStatus: null } as never,
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
        abi_status_epoll_wait: () => 22,
        abi_status_socket_recv: () => 23,
        abi_status_socket_send: () => 24,
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

test("waitInWorker clears a running worker that is already parked in worker-local wait", () => {
  const runningWorker = {} as Worker;
  const waitingWorker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
    runningGeneration?: number | null;
  }>();
  const runningThread = {
    vmPtr: 0x1111,
    initialized: true,
    waitingInWorkerStatus: 20,
    runningGeneration: 7,
  };
  const waitingThread = {
    vmPtr: 0x2222,
    initialized: true,
    waitingInWorkerStatus: null,
    runningGeneration: null,
  };
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(waitingWorker, waitingThread);
  const appendWorkerDebugLog = vi.fn();
  const pumpProcess = vi.fn();
  const handler = createWorkerWaitInWorkerStatusHandler({
    appendWorkerDebugLog,
    clearBlockedWorker: () => {},
    pumpProcess,
    kickBlockedWorkers: () => {},
    blockWorker: () => {},
    failProcess: () => {},
    requestEpollStateSync: async () => {},
    requestPipeStateSync: async () => {},
    requestSocketStateSync: async () => {},
    threadWorkerStates,
  } as never);

  const handle = {
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
    kernel: {
      abi_status_epoll_wait: () => 22,
      abi_status_socket_recv: () => 23,
      abi_status_socket_send: () => 24,
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
    runningWorker,
    vforkSnapshot: null,
    vforkWorker: null,
  } as never;

  handler(
    handle,
    waitingWorker,
    waitingThread as never,
    {
      type: "status",
      status: 20,
      vmPtr: 0x2222,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      kernelState: handle.kernelState,
      waitInWorker: true,
      debug: "",
    },
    false,
  );

  expect(handle.runningWorker).toBeNull();
  expect(runningThread.runningGeneration).toBeNull();
  expect(waitingThread.waitingInWorkerStatus).toBe(20);
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "running-worker-cleared:wait-in-worker:0x1111",
  );
  expect(pumpProcess).toHaveBeenCalledWith(handle);
});
