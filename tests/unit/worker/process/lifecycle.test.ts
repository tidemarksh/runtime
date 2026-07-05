import { expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../../src/abi.js";
import { createWorkerLifecycleHelpers } from "../../../../src/worker/lifecycle.js";
import { processExitInProgress } from "../../../../src/worker/process-lifecycle-state.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";

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
    ...makeSupervisorAbiStubs(),
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
    guestMemoryPolicyLimitBytes: 0,
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
    stdioOwner: null,
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
    vforkReadyWorkers: [],
    runningWorker: null,
    hostBlockedWorkers: new Map(),
    vforkSnapshot: null,
    vforkWorker: null,
    workerDebugLog: [],
    ...overrides,
  } as unknown as ProcessHandle;
  handle.kernelShare.members.add(handle);
  return { handle, releaseHostStdio };
}

function nonLifecycleEvents(order: string[]): string[] {
  return order.filter((event) => event !== "append");
}

test("completeProcessExit syncs pipe state before draining output when kernel-worker owns pipe state", async () => {
  const { handle } = makeHandle({
    kernelWorkerRequest: vi.fn(async () => ({ type: "pipe-state-synced" } as never)),
  });
  const order: string[] = [];
  const requestPipeStateSync = vi.fn(async (_handle: ProcessHandle, _wakeWaiters?: boolean) => {
    order.push("sync-pipe");
  });
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {
    order.push("sync-fork");
  });
  const lifecycle = createWorkerLifecycleHelpers({
    appendRuntimeLifecycleEvent: () => {
      order.push("append");
    },
    drainProcessOutput: () => {
      order.push("drain");
      return true;
    },
    enqueueWorker: () => {},
    getProcessByPid: () => undefined,
    notifyLifecycleProgress: () => {},
    recordChildExitViaKernelWorker: async () => {},
    releaseSharedHostStdio: async () => {
      order.push("release-stdio");
    },
    requestEpollStateSync: async () => {
      order.push("sync-epoll");
    },
    requestPipeStateSync,
    requestSocketStateSync: async () => {
      order.push("sync-socket");
    },
    resumeVforkParentViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker,
    workerExitCompleteProcessExit: async () => {
      order.push("complete");
    },
    workerExitFailProcess: () => {},
  });

  await lifecycle.completeProcessExit(handle, 0, 0);

  expect(order[0]).toBe("append");
  expect(nonLifecycleEvents(order)).toEqual([
    "sync-fork",
    "sync-pipe",
    "sync-socket",
    "sync-epoll",
    "complete",
  ]);
  expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalledWith(
    handle,
    [],
    {
      cause: { kind: "syscall", nr: -1n, name: "process-exit-owned-state" },
      includeMemfs: false,
      deltaOfdIds: true,
      trackHeldOfds: true,
      sourceVmPtr: handle.vmPtr,
    },
  );
  expect(requestPipeStateSync).toHaveBeenCalledWith(handle, true);
});

test("completeProcessExit skips child-side visible drain when stdio is owned by the parent", async () => {
  const parent = makeHandle({
    pid: 7,
    parentPid: null,
  }).handle;
  const { handle } = makeHandle({
    parentPid: 7,
    stdioOwner: parent,
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
    getProcessByPid: (pid) => (pid === 7 ? parent : undefined),
    notifyLifecycleProgress: () => {},
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

  expect(order[0]).toBe("append");
  expect(nonLifecycleEvents(order)).toEqual([
    "sync-fork",
    "sync-pipe",
    "sync-socket",
    "sync-epoll",
    "complete",
  ]);
});

test("completeProcessExit delegates parent child-exit publication once", async () => {
  const parent = makeHandle({
    pid: 7,
    parentPid: null,
  }).handle;
  const { handle } = makeHandle({
    pid: 3,
    parentPid: 7,
  });
  const completedParents: number[] = [];
  const lifecycle = createWorkerLifecycleHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: () => false,
    enqueueWorker: () => {},
    getProcessByPid: (pid) => (pid === 7 ? parent : undefined),
    notifyLifecycleProgress: () => {},
    recordChildExitViaKernelWorker: async () => {},
    releaseSharedHostStdio: async () => {},
    requestEpollStateSync: async () => {},
    requestPipeStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeVforkParentViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker: async () => {},
    workerExitCompleteProcessExit: async () => {
      completedParents.push(parent.pid);
    },
    workerExitFailProcess: () => {},
  });

  await lifecycle.completeProcessExit(handle, 0, 0);

  expect(completedParents).toEqual([7]);
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
    getProcessByPid: () => undefined,
    notifyLifecycleProgress: () => {},
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

  expect(order).toContain("append");
  expect(nonLifecycleEvents(order)).toEqual([
    "drain",
    "complete",
  ]);
  expect(handle.stdoutPipe).toBe(4);
  expect(handle.stderrPipe).toBe(5);
  expect(handle.hostStdio.refCount).toBe(1);
  expect(releaseHostStdio).toHaveBeenCalledTimes(0);
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
    getProcessByPid: () => undefined,
    notifyLifecycleProgress: () => {},
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

  expect(handle.stdoutPipe).toBe(4);
  expect(handle.stderrPipe).toBe(5);
  expect(handle.hostStdio.refCount).toBe(1);
  expect(releaseHostStdio).toHaveBeenCalledTimes(0);
});

test("completeProcessExit runs only once while a kernel-worker-owned exit is already in progress", async () => {
  const { handle } = makeHandle({
    kernelWorkerRequest: vi.fn(async () => ({ type: "pipe-state-synced" } as never)),
  });
  const appendRuntimeLifecycleEvent = vi.fn();
  const drainProcessOutput = vi.fn(() => false);
  const requestPipeStateSync = vi.fn(async () => {});
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const requestSocketStateSync = vi.fn(async () => {});
  const requestEpollStateSync = vi.fn(async () => {});
  const workerExitCompleteProcessExit = vi.fn(async () => {});
  let releaseResolved = false;
  let resolveRelease: (() => void) | null = null;
  const releasePromise = new Promise<void>((resolve) => {
    resolveRelease = () => {
      releaseResolved = true;
      resolve();
    };
  });
  const lifecycle = createWorkerLifecycleHelpers({
    appendRuntimeLifecycleEvent,
    drainProcessOutput,
    enqueueWorker: () => {},
    getProcessByPid: () => undefined,
    notifyLifecycleProgress: () => {},
    recordChildExitViaKernelWorker: async () => {},
    releaseSharedHostStdio: vi.fn(async () => {
      await releasePromise;
    }),
    requestEpollStateSync,
    requestPipeStateSync,
    requestSocketStateSync,
    resumeVforkParentViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker,
    workerExitCompleteProcessExit,
    workerExitFailProcess: () => {},
  });

  const firstExit = lifecycle.completeProcessExit(handle, 0, 6);
  await Promise.resolve();
  await Promise.resolve();
  await lifecycle.completeProcessExit(handle, 0, 6);
  expect(processExitInProgress(handle)).toBe(true);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalled();
  expect(drainProcessOutput).toHaveBeenCalledTimes(0);
  expect(requestPipeStateSync).toHaveBeenCalledTimes(1);
  expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalledTimes(1);
  expect(requestSocketStateSync).toHaveBeenCalledTimes(1);
  expect(requestEpollStateSync).toHaveBeenCalledTimes(1);
  expect(workerExitCompleteProcessExit).toHaveBeenCalledTimes(0);

  resolveRelease?.();
  await firstExit;

  expect(releaseResolved).toBe(true);
  expect(workerExitCompleteProcessExit).toHaveBeenCalledTimes(1);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalled();
});
