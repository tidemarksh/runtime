import { expect, test, vi } from "vite-plus/test";
import { createWorkerExitHelpers } from "../../../src/worker/exit.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

function makeHandle(
  overrides: Partial<ProcessHandle> = {},
): ProcessHandle {
  return {
    runtimeId: 1,
    pid: 3,
    parentPid: 1,
    vmPtr: 0x1234,
    ramSize: 0,
    maxMemoryPages: 0,
    guestBase: 0,
    worker: {} as Worker,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel: {} as ProcessHandle["kernel"],
    kernelState: {} as ProcessHandle["kernelState"],
    stdinPipe: 0,
    stdoutPipe: 2,
    stderrPipe: 2,
    hostStdio: {
      stdinPipe: 1,
      stdoutPipe: 2,
      stderrPipe: 2,
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
    runtimeBridge: {} as ProcessHandle["runtimeBridge"],
    sabPageCache: null,
    kernelWorkerRequest: vi.fn(async () => ({ type: "pipe-state-synced" } as never)),
    workers: new Set(),
    readyWorkers: [],
    runningWorker: null,
    blockedWorkers: new Map(),
    pendingChildExitRecords: 0,
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
    guestWriteWatchRegions: [],
    ...overrides,
  } as unknown as ProcessHandle;
}

test("recordExitOnParent syncs and drains child output before waking the parent", async () => {
  const order: string[] = [];
  const parent = makeHandle({
    pid: 1,
    parentPid: null,
    kernelWorkerRequest: vi.fn(async () => ({ type: "pipe-state-synced" } as never)),
  });
  const child = makeHandle();
  const processHandles = new Map<number, ProcessHandle>([
    [1, parent],
    [3, child],
  ]);

  const helpers = createWorkerExitHelpers({
    processHandles,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog(handle) {
      order.push(`debug:${handle.pid}`);
    },
    captureRuntimeFailureSnapshot: () => {},
    debugRuntimeProcessHandles: () => "pid=1,pid=3",
    drainHiddenProcessOutput(handle) {
      order.push(`hidden-drain:${handle.pid}`);
      return true;
    },
    drainProcessOutput(handle) {
      order.push(`drain:${handle.pid}`);
      return true;
    },
    destroyProcessHandle(handle) {
      order.push(`destroy:${handle.pid}`);
    },
    formatAllLiveThreadDebug: () => "threads",
    kickBlockedWorkers(handle) {
      order.push(`kick:${handle.pid}`);
    },
    publishRuntimeFsState: () => 1,
    recordChildExitViaKernelWorker: async (parentHandle) => {
      order.push(`record:${parentHandle.pid}`);
    },
    replaceRuntimeFsSnapshot: () => {},
    releaseSharedHostStdio: async (handle) => {
      order.push(`release:${handle.pid}`);
    },
    requestPipeStateSync: async (handle, wakeWaiters = false) => {
      order.push(`pipe-sync:${handle.pid}:${wakeWaiters ? "wake" : "sync"}`);
    },
    snapshotProcessFs: () => [],
  });

  await helpers.completeProcessExit(child, 0, 0);

  expect(order).toEqual([
    "pipe-sync:3:sync",
    "drain:3",
    "debug:1",
    "debug:1",
    "record:1",
    "pipe-sync:1:sync",
    "pipe-sync:1:sync",
    "kick:1",
    "release:3",
    "destroy:3",
    "pipe-sync:1:sync",
    "drain:1",
    "kick:1",
  ]);
});

test("recordExitOnParent skips child-side drain when stdio is owned by the parent", async () => {
  const order: string[] = [];
  const parent = makeHandle({
    pid: 1,
    parentPid: null,
    kernelWorkerRequest: vi.fn(async () => ({ type: "pipe-state-synced" } as never)),
  });
  const child = makeHandle({
    stdioOwner: parent,
  });
  const processHandles = new Map<number, ProcessHandle>([
    [1, parent],
    [3, child],
  ]);

  const helpers = createWorkerExitHelpers({
    processHandles,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog(handle) {
      order.push(`debug:${handle.pid}`);
    },
    captureRuntimeFailureSnapshot: () => {},
    debugRuntimeProcessHandles: () => "pid=1,pid=3",
    drainHiddenProcessOutput(handle) {
      order.push(`hidden-drain:${handle.pid}`);
      return true;
    },
    drainProcessOutput(handle) {
      order.push(`drain:${handle.pid}`);
      return true;
    },
    destroyProcessHandle(handle) {
      order.push(`destroy:${handle.pid}`);
    },
    formatAllLiveThreadDebug: () => "threads",
    kickBlockedWorkers(handle) {
      order.push(`kick:${handle.pid}`);
    },
    publishRuntimeFsState: () => 1,
    recordChildExitViaKernelWorker: async (parentHandle) => {
      order.push(`record:${parentHandle.pid}`);
    },
    replaceRuntimeFsSnapshot: () => {},
    releaseSharedHostStdio: async (handle) => {
      order.push(`release:${handle.pid}`);
    },
    requestPipeStateSync: async (handle, wakeWaiters = false) => {
      order.push(`pipe-sync:${handle.pid}:${wakeWaiters ? "wake" : "sync"}`);
    },
    snapshotProcessFs: () => [],
  });

  await helpers.completeProcessExit(child, 0, 0);

  expect(order).not.toContain("hidden-drain:3");
  expect(order.filter((entry) => entry === "pipe-sync:3:sync")).toHaveLength(2);
  expect(order.filter((entry) => entry === "pipe-sync:1:sync")).toHaveLength(0);
  expect(order).toContain("record:1");
  expect(order).toContain("release:3");
  expect(order).toContain("destroy:3");
  expect(order).not.toContain("drain:1");
  expect(order.at(-1)).toBe("kick:1");
});

test("recordExitOnParent re-kicks blocked parent after pending child exit bookkeeping clears", async () => {
  const parent = makeHandle({
    pid: 1,
    parentPid: null,
    pendingChildExitRecords: 0,
  });
  const child = makeHandle();
  const processHandles = new Map<number, ProcessHandle>([
    [1, parent],
    [3, child],
  ]);
  const kicked: number[] = [];
  const observedPendingCounts: number[] = [];

  const helpers = createWorkerExitHelpers({
    processHandles,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    captureRuntimeFailureSnapshot: () => {},
    debugRuntimeProcessHandles: () => "pid=1,pid=3",
    drainHiddenProcessOutput: () => false,
    drainProcessOutput: () => false,
    destroyProcessHandle: () => {},
    formatAllLiveThreadDebug: () => "threads",
    kickBlockedWorkers(handle) {
      kicked.push(handle.pendingChildExitRecords);
    },
    publishRuntimeFsState: () => 1,
    recordChildExitViaKernelWorker: async (parentHandle) => {
      observedPendingCounts.push(parentHandle.pendingChildExitRecords);
    },
    replaceRuntimeFsSnapshot: () => {},
    releaseSharedHostStdio: async () => {},
    requestPipeStateSync: async () => {},
    snapshotProcessFs: () => [],
  });

  await helpers.completeProcessExit(child, 0, 0);

  expect(observedPendingCounts).toEqual([1]);
  expect(kicked).toEqual([0, 0]);
  expect(parent.pendingChildExitRecords).toBe(0);
});

test("completeProcessExit still destroys the handle when cleanup fails", async () => {
  const order: string[] = [];
  const child = makeHandle({
    parentPid: null,
  });
  const processHandles = new Map<number, ProcessHandle>([
    [3, child],
  ]);

  const helpers = createWorkerExitHelpers({
    processHandles,
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      order.push(event);
    },
    appendWorkerDebugLog: () => {},
    captureRuntimeFailureSnapshot: () => {},
    debugRuntimeProcessHandles: () => "pid=3",
    drainHiddenProcessOutput: () => false,
    drainProcessOutput: () => false,
    destroyProcessHandle: (handle) => {
      order.push(`destroy:${handle.pid}`);
    },
    formatAllLiveThreadDebug: () => "threads",
    kickBlockedWorkers: () => {
      order.push("kick");
    },
    publishRuntimeFsState: () => 1,
    recordChildExitViaKernelWorker: async () => {},
    replaceRuntimeFsSnapshot: () => {},
    releaseSharedHostStdio: async () => {
      throw new Error("release failed");
    },
    requestPipeStateSync: async () => {},
    snapshotProcessFs: () => [],
  });

  await helpers.completeProcessExit(child, 0, 0);

  expect(order).toContain("destroy:3");
  expect(order).toContain('exit-cleanup-error:3:"release failed"');
});

test("completeProcessExit keeps execve stage summary without dispatch trace noise", async () => {
  const child = makeHandle({
    parentPid: null,
    kernel: {
      debug_execve_stage: () => 9,
      debug_execve_value0: () => 0x55n,
      debug_execve_value1: () => 0x66n,
      debug_execve_stage_trace_len: () => 2,
      debug_execve_stage_trace_stage: (index: number) => [10, 11][index] ?? 0,
      debug_execve_stage_trace_value0: (index: number) => [0x100n, 0x300n][index] ?? 0n,
      debug_execve_stage_trace_value1: (index: number) => [0x200n, 0x400n][index] ?? 0n,
      debug_riscv_syscall_trace_len: () => 0,
      debug_path_syscall_trace_len: () => 0,
    } as never,
  });
  const processHandles = new Map<number, ProcessHandle>([[3, child]]);

  const helpers = createWorkerExitHelpers({
    processHandles,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    captureRuntimeFailureSnapshot: () => {},
    debugRuntimeProcessHandles: () => "pid=3",
    drainHiddenProcessOutput: () => false,
    drainProcessOutput: () => false,
    destroyProcessHandle: () => {},
    formatAllLiveThreadDebug: () => "threads",
    kickBlockedWorkers: () => {},
    recordChildExitViaKernelWorker: async () => {},
    releaseSharedHostStdio: async () => {},
    requestPipeStateSync: async () => {},
  });

  await helpers.completeProcessExit(child, 1, 0);

  expect(child.lastDebugMessage).toContain("execve_stage=9");
  expect(child.lastDebugMessage).toContain("execve_stage_trace=[10:100:200,11:300:400]");
  expect(child.lastDebugMessage).not.toContain("execve_dispatch=[");
});
