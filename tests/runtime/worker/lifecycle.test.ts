import { expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { createWorkerLifecycleHelpers } from "../../../src/worker/lifecycle.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memStatePtr: 0x240000,
    epollTablePtr: 0x250000,
    socketTablePtr: 0x260000,
    socketUsed: 0,
    pipeBase: 0x270000,
    pipeUsed: 0n,
    netBufBase: 0x280000,
    netBufUsed: 0n,
    sigSlotsPtr: 0xc00000,
    processPendingPtr: 0xc10000,
    processRuntimeStatePtr: 0xc20000,
    timeStatePtr: 0xc30000,
  };
}

function makeKernel(releaseHostStdio: () => void): KernelExports {
  return {
    abi_max_fds: () => 16,
    abi_max_threads: () => 4,
    abi_status_vfork_wait: () => 8,
    abi_status_execve_spawn: () => 9,
    abi_status_clone_spawn: () => 10,
    process_fd_ofd_id: () => 0,
    process_thread_vm_ptr: (_vmPtr, slot) => (slot === 0 ? 0x1234 : 0),
    thread_blocked_ofd_id: () => 0,
    process_release_host_stdio: releaseHostStdio,
  } as unknown as KernelExports;
}

function makeHandle(
  overrides: Partial<ProcessHandle> = {},
): { handle: ProcessHandle; releaseHostStdio: ReturnType<typeof vi.fn> } {
  const releaseHostStdio = vi.fn();
  const handle = {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0x1234,
    ramSize: 0,
    guestBase: 0,
    worker: {} as Worker,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel: makeKernel(releaseHostStdio),
    kernelState: makeKernelState(),
    stdinPipe: 0,
    stdoutPipe: 4,
    stderrPipe: 5,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 4,
      stderrPipe: 5,
      refCount: 1,
    },
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set<number>([7, 8]),
    },
    stdinQueue: [],
    outputBacklog: [],
    debugCommand: "test",
    runtimeBridge: {} as ProcessHandle["runtimeBridge"],
    sabPageCache: null,
    workers: new Set(),
    readyWorkers: [],
    runningWorker: null,
    blockedWorkers: new Map(),
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
    workerDebugLog: [],
    ...overrides,
  } as unknown as ProcessHandle;
  handle.kernelShare.members.add(handle);
  return { handle, releaseHostStdio };
}

test("completeProcessExit syncs pipe state before draining output when kernel-worker owns pipe state", async () => {
  const { handle } = makeHandle({
    kernelWorkerRequest: vi.fn(async () => ({ type: "pipe-state-synced" } as never)),
  });
  const order: string[] = [];
  const lifecycle = createWorkerLifecycleHelpers({
    appendRuntimeLifecycleEvent: () => {
      order.push("append");
    },
    drainProcessOutput: () => {
      order.push("drain");
      return true;
    },
    enqueueWorker: () => {},
    kickBlockedWorkers: () => {},
    recordChildExitViaKernelWorker: async () => {},
    releaseSharedHostStdio: async () => {
      order.push("release-stdio");
    },
    requestEpollStateSync: async () => {
      order.push("sync-epoll");
    },
    requestPipeStateSync: async () => {
      order.push("sync-pipe");
    },
    requestSocketStateSync: async () => {
      order.push("sync-socket");
    },
    resumeVforkParentViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker: async () => {
      order.push("sync-fork");
    },
    workerExitCompleteProcessExit: async () => {
      order.push("complete");
    },
    workerExitFailProcess: () => {},
  });

  await lifecycle.completeProcessExit(handle, 0, 0);

  expect(order).toEqual([
    "sync-pipe",
    "drain",
    "append",
    "sync-fork",
    "sync-socket",
    "sync-epoll",
    "release-stdio",
    "complete",
  ]);
});

test("completeProcessExit releases host stdio locally when no kernel-worker owns the pipe state", async () => {
  const { handle, releaseHostStdio } = makeHandle({
    pid: 0,
    kernelWorkerRequest: undefined,
  });
  const order: string[] = [];
  const lifecycle = createWorkerLifecycleHelpers({
    appendRuntimeLifecycleEvent: () => {
      order.push("append");
    },
    drainProcessOutput: () => {
      order.push("drain");
      return true;
    },
    enqueueWorker: () => {},
    kickBlockedWorkers: () => {},
    recordChildExitViaKernelWorker: async () => {},
    releaseSharedHostStdio: async () => {
      order.push("remote-release");
    },
    requestEpollStateSync: async () => {
      order.push("sync-epoll");
    },
    requestPipeStateSync: async () => {
      order.push("sync-pipe");
    },
    requestSocketStateSync: async () => {
      order.push("sync-socket");
    },
    resumeVforkParentViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker: async () => {
      order.push("sync-fork");
    },
    workerExitCompleteProcessExit: async () => {
      order.push("complete");
    },
    workerExitFailProcess: () => {},
  });

  await lifecycle.completeProcessExit(handle, 0, 0);

  expect(order).toEqual([
    "drain",
    "append",
    "complete",
  ]);
  expect(handle.stdoutPipe).toBe(0);
  expect(handle.stderrPipe).toBe(0);
  expect(releaseHostStdio).toHaveBeenCalledTimes(1);
});

test("completeProcessExit releases host stdio locally exactly once for signaled exits", async () => {
  const { handle, releaseHostStdio } = makeHandle({
    pid: 0,
    kernelWorkerRequest: undefined,
  });
  const lifecycle = createWorkerLifecycleHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: () => false,
    enqueueWorker: () => {},
    kickBlockedWorkers: () => {},
    recordChildExitViaKernelWorker: async () => {},
    releaseSharedHostStdio: async () => {
      throw new Error("unexpected remote stdio release");
    },
    requestEpollStateSync: async () => {},
    requestPipeStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeVforkParentViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker: async () => {},
    workerExitCompleteProcessExit: async () => {},
    workerExitFailProcess: () => {},
  });

  await lifecycle.completeProcessExit(handle, 0, 9);
  await lifecycle.completeProcessExit(handle, 0, 9);

  expect(handle.stdoutPipe).toBe(0);
  expect(handle.stderrPipe).toBe(0);
  expect(handle.hostStdio.refCount).toBe(0);
  expect(releaseHostStdio).toHaveBeenCalledTimes(1);
});
