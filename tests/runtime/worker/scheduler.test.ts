import { afterEach, beforeEach, expect, test, vi } from "vite-plus/test";
import {
  createRuntimeStateBridge,
  readPublishedRuntimeBridgeEpoch,
} from "../../../src/bridge/runtime-state.js";
import { createWorkerScheduler } from "../../../src/worker/scheduler.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

function makeKernel() {
  const threadResumeWait4 = vi.fn();
  return {
    threadResumeWait4,
    exports: {
      abi_status_ok: () => 0,
      abi_status_wait4: () => 31,
      abi_status_epoll_wait: () => 7,
      abi_status_fault: () => 3,
      abi_status_pipe_wait: () => 6,
      abi_status_connect: () => 28,
      abi_status_socket_recv: () => 30,
      abi_status_socket_send: () => 29,
      abi_status_flock_wait: () => 32,
      thread_resume_wait4: threadResumeWait4,
      thread_resume_blocked_syscall: vi.fn(),
      thread_resume_epoll_wait_timeout: vi.fn(),
      thread_resume_socket_wait_timeout: vi.fn(),
      thread_status: () => 0,
      thread_exit_code: () => 0,
      thread_term_signal: () => 0,
      thread_epoll_wait_timeout: () => 0n,
      thread_socket_wait_timeout: () => -1n,
      thread_process_thread_count: () => 1,
      kernel_heap_base: () => 0,
      kernel_heap_ptr: () => 0,
      kernel_ofd_table_ptr: () => 0,
      kernel_memfs_ptr: () => 0,
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
      kernel_attach_runtime_state: () => {},
    } as unknown as ProcessHandle["kernel"],
  };
}

function makeHandle(kernel: ProcessHandle["kernel"], worker: Worker): ProcessHandle {
  return {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0x1234,
    ramSize: 0,
    maxMemoryPages: 0,
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
    },
    stdinQueue: [],
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
    pipeSyncPromise: null,
    pipeSyncQueued: false,
    pipeWaiterWakeQueued: false,
    epollSyncPromise: null,
    epollSyncQueued: false,
    epollWaiterWakeQueued: false,
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    socketSyncPromise: null,
    socketSyncQueued: false,
    socketWaiterWakeQueued: false,
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

test("wait4 retry stays blocked while child spawn is still in flight", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  const { exports: kernel, threadResumeWait4 } = makeKernel();
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

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);
  expect(threadResumeWait4).not.toHaveBeenCalled();

  handle.pendingChildSpawnRecords = 0;
  const epochBeforeKick = readPublishedRuntimeBridgeEpoch(handle.runtimeBridge);
  scheduler.kickBlockedWorkers(handle);
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 20_000,
  }));
  expect(readPublishedRuntimeBridgeEpoch(handle.runtimeBridge)).toBeGreaterThan(epochBeforeKick);
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
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, worker);
  handle.ramSize = 16 * 1024 * 1024;
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

  scheduler.attachThreadWorker(handle, worker, 0x1234);

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
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });
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

test("connect timeout resumes with socket timeout handler after deadline", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });
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

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_connect(), 0x1234);
  await vi.advanceTimersByTimeAsync(30);

  expect(resumeSocketTimeout).toHaveBeenCalledWith(0x1234);
});
