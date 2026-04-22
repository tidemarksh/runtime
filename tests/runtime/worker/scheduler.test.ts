import { afterEach, beforeEach, expect, test, vi } from "vite-plus/test";
import {
  createRuntimeStateBridge,
  readPublishedRuntimeBridgeEpoch,
} from "../../../src/bridge/runtime-state.js";

vi.mock("../../../src/worker/pool.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../../src/worker/pool.js")>();
  return {
    ...actual,
    kernelStackSlotForWorker: () => 2,
  };
});

import { createWorkerScheduler } from "../../../src/worker/scheduler.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

function makeKernel() {
  const threadResumeWait4 = vi.fn();
  let monotonicTicks = 1n;
  return {
    threadResumeWait4,
    exports: {
      abi_status_ok: () => 0,
      abi_status_futex_wait: () => 20,
      abi_status_sleep: () => 26,
      abi_status_wait4: () => 31,
      abi_status_epoll_wait: () => 7,
      abi_status_vfork_wait: () => 8,
      abi_status_execve_spawn: () => 9,
      abi_status_clone_spawn: () => 10,
      abi_status_fault: () => 3,
      abi_status_pipe_wait: () => 6,
      abi_status_connect: () => 28,
      abi_status_socket_recv: () => 30,
      abi_status_socket_send: () => 29,
      abi_status_flock_wait: () => 32,
      abi_max_threads: () => 64,
      thread_resume_wait4: threadResumeWait4,
      thread_resume_blocked_syscall: vi.fn(),
      thread_resume_sleep: vi.fn(),
      thread_resume_epoll_wait_timeout: vi.fn(),
      thread_resume_socket_wait_timeout: vi.fn(),
      thread_status: () => 0,
      thread_exit_code: () => 0,
      thread_term_signal: () => 0,
      monotonic_ticks_current: () => monotonicTicks,
      sync_monotonic_ticks_floor: vi.fn((ticks: bigint | number) => {
        const nextTicks = BigInt(ticks);
        if (nextTicks > monotonicTicks) {
          monotonicTicks = nextTicks;
        }
      }),
      thread_sleep_deadline_ticks: vi.fn(() => monotonicTicks),
      thread_sleep_seconds: () => 0n,
      thread_sleep_nanoseconds: () => 0n,
      thread_epoll_wait_timeout: () => 0n,
      thread_socket_wait_timeout: () => -1n,
      thread_process_thread_count: () => 1,
      thread_process_ptr: () => 0x2000,
      thread_pc: () => 0x1000n,
      thread_sp: () => 0x8000n,
      thread_s11: () => 0x9000n,
      process_current_pid: () => 1,
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
    forkMaxMemoryPages: 0,
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
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    ttyPtySlot: 0,
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
    stdinFlushPromise: null,
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
  let status = kernel.abi_status_wait4();
  kernel.thread_status = () => status;
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
    status = kernel.abi_status_ok();
  });
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

test("wait4 retry stays blocked while a child remains active even with kernel-worker support", async () => {
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
  kernel.thread_status = () => kernel.abi_status_wait4();
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => ({
    type: "wait4-resumed",
    requestId: 0,
    kernelState: handle.kernelState,
  }));

  const resumeWait4ViaKernelWorker = vi.fn(async () => {});
  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => true,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker,
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);

  expect(resumeWait4ViaKernelWorker).not.toHaveBeenCalled();
  expect(threadResumeWait4).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(worker)).toBe(true);
});

test("wait4 retry reconciles stale blocked state before issuing a kernel-worker resume", () => {
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
  let status = kernel.abi_status_ok();
  kernel.thread_status = () => status;
  const handle = makeHandle(kernel, worker);
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("kernel-worker resume should not run when local status is already OK");
  });

  const resumeWait4ViaKernelWorker = vi.fn(async () => {});
  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    hasActiveChildProcess: () => false,
    resumeBlockedSyscallLocally: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker,
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  scheduler.kickBlockedWorkers(handle);

  expect(resumeWait4ViaKernelWorker).not.toHaveBeenCalled();
  expect(threadResumeWait4).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 20_000,
  }));
});

test("sleep-blocked worker advances guest time to the earliest deadline and resumes immediately", () => {
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
  kernel.thread_status = () => kernel.abi_status_sleep();
  kernel.thread_sleep_deadline_ticks = () => 1_000n;
  const handle = makeHandle(kernel, worker);
  handle.pendingChildSpawnRecords = 0;

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

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_sleep(), 0x1234);
  scheduler.kickBlockedWorkers(handle);

  expect(kernel.sync_monotonic_ticks_floor).toHaveBeenCalledWith(1_000n);
  expect(kernel.thread_resume_sleep).toHaveBeenCalledWith(0x1234);
  expect(handle.blockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 20_000,
  }));
});

test("blocked syscall retry is rescheduled after a running-worker skip", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(blockedWorker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  threadWorkerStates.set(runningWorker, {
    vmPtr: 0x5678,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_futex_wait();
  const resumeBlockedSyscall = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234 ? status : kernel.abi_status_ok();
  kernel.thread_resume_blocked_syscall = resumeBlockedSyscall;
  const handle = makeHandle(kernel, blockedWorker);
  handle.workers.add(runningWorker);
  handle.runningWorker = runningWorker;

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

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_futex_wait(), 0x1234);
  await vi.advanceTimersByTimeAsync(1);
  expect(resumeBlockedSyscall).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.has(blockedWorker)).toBe(true);

  handle.runningWorker = null;
  await vi.advanceTimersByTimeAsync(1);

  expect(resumeBlockedSyscall).toHaveBeenCalledWith(0x1234);
  expect(handle.blockedWorkers.has(blockedWorker)).toBe(false);
  expect(blockedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 20_000,
  }));
});

test("wait4 retry clears a stale running worker that already published a blocked status", () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(blockedWorker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  threadWorkerStates.set(runningWorker, {
    vmPtr: 0x5678,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  kernel.thread_status = (vmPtr: number) => {
    if (vmPtr === 0x1234) {
      return kernel.abi_status_wait4();
    }
    if (vmPtr === 0x5678) {
      return kernel.abi_status_sleep();
    }
    return kernel.abi_status_ok();
  };
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
  });
  const handle = makeHandle(kernel, blockedWorker);
  handle.pendingChildSpawnRecords = 0;
  handle.workers.add(runningWorker);
  handle.runningWorker = runningWorker;

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

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_wait4(), 0x1234);
  scheduler.kickBlockedWorkers(handle);

  expect(handle.runningWorker).toBeNull();
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
});

test("pruning a stale running worker clears the running slot", () => {
  const staleWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(staleWorker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  threadWorkerStates.set(anchorWorker, {
    vmPtr: 0x5678,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  const handle = makeHandle(kernel, staleWorker);
  handle.workers.add(anchorWorker);
  handle.runningWorker = staleWorker;
  handle.pendingChildSpawnRecords = 0;

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

  scheduler.pruneDetachedProcessWorkers(handle, 0x5678, null);

  expect(handle.runningWorker).toBeNull();
  expect(handle.workers.has(staleWorker)).toBe(false);
  expect(handle.workers.has(anchorWorker)).toBe(true);
});

test("pruning preserves a detached worker in vfork wait", () => {
  const vforkWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(vforkWorker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  threadWorkerStates.set(anchorWorker, {
    vmPtr: 0x5678,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234 ? kernel.abi_status_vfork_wait() : kernel.abi_status_ok();
  const handle = makeHandle(kernel, vforkWorker);
  handle.workers.add(anchorWorker);
  handle.runningWorker = vforkWorker;

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

  scheduler.pruneDetachedProcessWorkers(handle, 0x5678, null);

  expect(handle.runningWorker).toBe(vforkWorker);
  expect(handle.workers.has(vforkWorker)).toBe(true);
  expect(handle.workers.has(anchorWorker)).toBe(true);
});

test("pruning preserves a detached worker during execve spawn handoff", () => {
  const execveWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(execveWorker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  threadWorkerStates.set(anchorWorker, {
    vmPtr: 0x5678,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234 ? kernel.abi_status_execve_spawn() : kernel.abi_status_ok();
  const handle = makeHandle(kernel, execveWorker);
  handle.workers.add(anchorWorker);

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

  scheduler.pruneDetachedProcessWorkers(handle, 0x5678, null);

  expect(handle.workers.has(execveWorker)).toBe(true);
  expect(handle.workers.has(anchorWorker)).toBe(true);
});

test("pruning is deferred while child spawn records are still pending", () => {
  const staleWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(staleWorker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  threadWorkerStates.set(anchorWorker, {
    vmPtr: 0x5678,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  const handle = makeHandle(kernel, staleWorker);
  handle.workers.add(anchorWorker);
  handle.pendingChildSpawnRecords = 1;

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

  scheduler.pruneDetachedProcessWorkers(handle, 0x5678, null);

  expect(handle.workers.has(staleWorker)).toBe(true);
  expect(handle.workers.has(anchorWorker)).toBe(true);
});

test("enqueue clears a dangling running worker before stepping the next ready worker", () => {
  const staleWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(readyWorker, {
    vmPtr: 0x5678,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, readyWorker);
  handle.workers.add(staleWorker);
  handle.runningWorker = staleWorker;

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

  handle.workers.delete(staleWorker);
  scheduler.enqueueWorker(handle, readyWorker);

  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 20_000,
  }));
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

  scheduler.attachThreadWorker(handle, { worker, vmPtr: 0x1234 });

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
