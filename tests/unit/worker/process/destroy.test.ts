import { hostExecutionState } from "../../../../src/worker/host-execution-state.js";
import { expect, test, vi } from "vite-plus/test";
import { createWorkerDestroyHelpers } from "../../../../src/worker/destroy.js";
import { setProcessDebugCommand } from "../../../../src/worker/process-debug-state.js";
import type { ProcessHandle, ThreadWorkerState } from "../../../../src/worker/types.js";
import { FakeWorker } from "../../../support/fake-worker.js";

function makeHandle(overrides: Partial<ProcessHandle> = {}): ProcessHandle {
  const primaryWorker = new FakeWorker() as unknown as Worker;
  const extraWorker = new FakeWorker() as unknown as Worker;
  const handle = {
    runtimeId: 1,
    pid: 11,
    parentPid: null,
    vmPtr: 0x1234,
    guestMemoryPolicyLimitBytes: 0,
    guestBase: 0,
    worker: primaryWorker,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel: {
      process_release_host_stdio: vi.fn(),
    } as unknown as ProcessHandle["kernel"],
    kernelWorkerRequest: vi.fn(async () => ({ type: "process-unregistered", requestId: 0 } as never)),
    kernelState: {
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
    },
    stdinPipe: 1,
    stdoutPipe: 2,
    stderrPipe: 3,
    hostStdio: {
      stdinPipe: 1,
      stdoutPipe: 2,
      stderrPipe: 3,
      refCount: 1,
    },
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set([7]),
    },
    stdinQueue: [],
    outputBacklog: [],
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(8),
      stateSab: new SharedArrayBuffer(128),
    },
    sabPageCache: null,
    workers: new Set([extraWorker]),
    vforkReadyWorkers: [extraWorker],
    runningWorker: extraWorker,
    hostBlockedWorkers: new Map([[extraWorker, {
      vmPtr: 0x2222,
      status: 6,
    }]]),
    vforkSnapshot: {
      vmPtr: 0x1234,
      childPid: 77,
      nextPid: 88,
      pgid: 99,
      sid: 111,
      inheritedSharedOfdIds: [7],
    },
    vforkWorker: primaryWorker,
    onExit: vi.fn(),
    workerDebugLog: [],
    ...overrides,
  } as unknown as ProcessHandle;
  setProcessDebugCommand(handle, "destroy-test");
  return handle;
}

test("destroyProcessHandle tears down workers, clears process maps, and releases host stdio once", async () => {
  const handle = makeHandle();
  const primaryWorker = handle.worker as unknown as FakeWorker;
  const extraWorkers = [...handle.workers] as Array<Worker & FakeWorker>;
  handle.kernelShare.members.add(handle);
  const processHandles = new Map<number, ProcessHandle>([[handle.pid, handle]]);
  const runtimeFsDirtyHandles = new Map<number, Set<ProcessHandle>>([[handle.runtimeId, new Set([handle])]]);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(handle.worker, { vmPtr: handle.vmPtr, initialized: true, waitingInWorkerStatus: null });
  for (const worker of handle.workers) {
    threadWorkerStates.set(worker, { vmPtr: 0x2222, initialized: true, waitingInWorkerStatus: null });
  }
  const appendRuntimeLifecycleEvent = vi.fn();
  const clearBlockedWorker = vi.fn((targetHandle: ProcessHandle, worker: Worker) => {
    hostExecutionState(targetHandle).hostBlockedWorkers.delete(worker);
  });
  const clearVforkSnapshot = vi.fn((targetHandle: ProcessHandle) => {
    hostExecutionState(targetHandle).vforkSnapshot = null;
    hostExecutionState(targetHandle).vforkWorker = null;
  });
  const unregisterProcessWithKernelWorker = vi.fn(async () => {});
  const releaseSharedHostStdio = vi.fn(async (targetHandle: ProcessHandle) => {
    if (targetHandle.hostStdio.refCount > 0) {
      targetHandle.hostStdio.refCount -= 1;
    }
    targetHandle.stdinPipe = 0;
    targetHandle.stdoutPipe = 0;
    targetHandle.stderrPipe = 0;
  });

  const destroyHelpers = createWorkerDestroyHelpers({
    processHandles,
    threadWorkerStates,
    runtimeFsDirtyHandles,
    appendRuntimeLifecycleEvent,
    clearBlockedWorker,
    clearVforkSnapshot,
    releaseSharedHostStdio,
    unregisterProcessWithKernelWorker,
  });

  destroyHelpers.destroyProcessHandle(handle, 15, false);
  destroyHelpers.destroyProcessHandle(handle, 15, false);
  await Promise.resolve();
  await Promise.resolve();

  expect(clearBlockedWorker).toHaveBeenCalledTimes(1);
  expect(clearVforkSnapshot).toHaveBeenCalledWith(handle);
  expect(unregisterProcessWithKernelWorker).toHaveBeenCalledTimes(2);
  expect(primaryWorker.terminate).toHaveBeenCalledTimes(2);
  for (const worker of extraWorkers) {
    expect(worker.terminate).toHaveBeenCalledTimes(1);
  }
  expect(processHandles.has(handle.pid)).toBe(false);
  expect(runtimeFsDirtyHandles.has(handle.runtimeId)).toBe(false);
  expect(handle.stdinPipe).toBe(0);
  expect(handle.stdoutPipe).toBe(0);
  expect(handle.stderrPipe).toBe(0);
  expect(handle.hostStdio.refCount).toBe(0);
  expect(releaseSharedHostStdio).toHaveBeenCalledTimes(1);
  expect(handle.kernelShare.members.has(handle)).toBe(false);
});

test("destroyRuntimeProcesses only notifies exits for handles in the selected runtime", () => {
  const runtime1Root = makeHandle({
    runtimeId: 1,
    pid: 1,
    onExit: vi.fn(),
  });
  const runtime1Child = makeHandle({
    runtimeId: 1,
    pid: 2,
    parentPid: 1,
    onExit: vi.fn(),
  });
  const runtime2Handle = makeHandle({
    runtimeId: 2,
    pid: 3,
    onExit: vi.fn(),
  });
  const processHandles = new Map<number, ProcessHandle>([
    [1, runtime1Root],
    [2, runtime1Child],
    [3, runtime2Handle],
  ]);
  const releaseSharedHostStdio = vi.fn(async (targetHandle: ProcessHandle) => {
    if (targetHandle.hostStdio.refCount > 0) {
      targetHandle.hostStdio.refCount -= 1;
    }
    targetHandle.stdinPipe = 0;
    targetHandle.stdoutPipe = 0;
    targetHandle.stderrPipe = 0;
  });

  const destroyHelpers = createWorkerDestroyHelpers({
    processHandles,
    threadWorkerStates: new WeakMap(),
    runtimeFsDirtyHandles: new Map(),
    appendRuntimeLifecycleEvent: vi.fn(),
    clearBlockedWorker: vi.fn(),
    clearVforkSnapshot: vi.fn(),
    releaseSharedHostStdio,
    unregisterProcessWithKernelWorker: vi.fn(async () => {}),
  });

  destroyHelpers.destroyRuntimeProcesses(1, 9);

  expect(runtime1Root.onExit).toHaveBeenCalledWith(137);
  expect(runtime1Child.onExit).toHaveBeenCalledWith(137);
  expect(runtime2Handle.onExit).not.toHaveBeenCalled();
  expect(processHandles.has(3)).toBe(true);
});
