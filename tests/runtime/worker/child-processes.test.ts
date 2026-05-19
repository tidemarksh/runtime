import { expect, test } from "vite-plus/test";
import {
  hasActiveChildProcess,
  pumpActiveChildProcesses,
} from "../../../src/worker/child-processes.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

function makeHandle(overrides: Partial<ProcessHandle> = {}): ProcessHandle {
  return {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0x1000,
    ramSize: 0,
    maxMemoryPages: 0,
    forkMaxMemoryPages: 0,
    guestBase: 0,
    worker: {} as Worker,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel: {} as ProcessHandle["kernel"],
    kernelState: {} as ProcessHandle["kernelState"],
    stdinPipe: 0,
    stdoutPipe: 1,
    stderrPipe: 2,
    ttyPtySlot: -1,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 1,
      stderrPipe: 2,
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
    runtimeBridge: {} as ProcessHandle["runtimeBridge"],
    sabPageCache: null,
    workers: new Set(),
    readyWorkers: [],
    runningWorker: null,
    blockedWorkers: new Map(),
    pendingChildExitRecords: 0,
    pendingChildSpawnRecords: 0,
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
    workerDebugLog: [],
    guestWriteWatchRegions: [],
    ...overrides,
  } as ProcessHandle;
}

test("hasActiveChildProcess ignores children already in exit cleanup", () => {
  const processHandles = new Map<number, ProcessHandle>([
    [1, makeHandle({ pid: 1 })],
    [2, makeHandle({ pid: 2, parentPid: 1, exitInProgress: true })],
  ]);

  expect(hasActiveChildProcess(processHandles, 1)).toBe(false);
});

test("hasActiveChildProcess still reports live children", () => {
  const processHandles = new Map<number, ProcessHandle>([
    [1, makeHandle({ pid: 1 })],
    [2, makeHandle({ pid: 2, parentPid: 1, exitInProgress: true })],
    [3, makeHandle({ pid: 3, parentPid: 1, exitInProgress: false })],
  ]);

  expect(hasActiveChildProcess(processHandles, 1)).toBe(true);
});

test("hasActiveChildProcess reports child handles whose workers are temporarily detached", () => {
  const childWorker = {} as Worker;
  const liveWorker = {} as Worker;
  const detachedChild = makeHandle({
    pid: 2,
    parentPid: 1,
    worker: childWorker,
    workers: new Set([childWorker]),
    exitInProgress: false,
  });
  const liveChild = makeHandle({
    pid: 3,
    parentPid: 1,
    worker: liveWorker,
    workers: new Set([liveWorker]),
    exitInProgress: false,
  });
  const processHandles = new Map<number, ProcessHandle>([
    [1, makeHandle({ pid: 1 })],
    [2, detachedChild],
    [3, liveChild],
  ]);
  const threadWorkerStates = new WeakMap<Worker, never>([
    [liveWorker, {} as never],
  ]);

  expect(hasActiveChildProcess(processHandles, 1, threadWorkerStates)).toBe(true);
  processHandles.delete(3);
  expect(hasActiveChildProcess(processHandles, 1, threadWorkerStates)).toBe(true);
});

test("pumpActiveChildProcesses pumps live children and skips exit cleanup", () => {
  const liveChild = makeHandle({ pid: 3, parentPid: 1, exitInProgress: false });
  const exitingChild = makeHandle({ pid: 2, parentPid: 1, exitInProgress: true });
  const unrelated = makeHandle({ pid: 4, parentPid: 9, exitInProgress: false });
  const processHandles = new Map<number, ProcessHandle>([
    [1, makeHandle({ pid: 1 })],
    [2, exitingChild],
    [3, liveChild],
    [4, unrelated],
  ]);
  const events: string[] = [];

  const result = pumpActiveChildProcesses(
    processHandles,
    1,
    (handle) => {
      events.push(`pump:${handle.pid}`);
    },
    (handle) => {
      events.push(`kick:${handle.pid}`);
    },
  );

  expect(result).toBe(true);
  expect(events).toEqual(["kick:3", "pump:3"]);
});
