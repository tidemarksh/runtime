import { afterEach, expect, test, vi } from "vite-plus/test";
import { createWorkerExitHelpers } from "../../../../src/worker/exit.js";
import { drainProcessOutput } from "../../../../src/worker/stdio.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

function makeHandle(
  overrides: Partial<ProcessHandle> = {},
): ProcessHandle {
  const kernel = {
    abi_max_fds: () => 16,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fd >= 0 && fd <= 2 ? fd + 1 : 0,
    process_fd_flags: () => 0,
    kernel_ofd_fd_type: (ofdId: number) => ofdId >= 1 && ofdId <= 3 ? 6 : 0,
    kernel_ofd_host_fd: () => -1,
    kernel_ofd_refcount: () => 1,
    kernel_ofd_status_flags: () => 0,
    abi_fd_type_pipe: () => 6,
    kernel_ofd_ino: (ofdId: number) => ofdId === 1 ? 1 : 2,
    ...(overrides.kernel as Partial<ProcessHandle["kernel"]> | undefined),
  } as ProcessHandle["kernel"];
  return {
    runtimeId: 1,
    pid: 3,
    parentPid: 1,
    vmPtr: 0x1234,
    guestMemoryPolicyLimitBytes: 0,
    maxMemoryPages: 0,
    guestBase: 0,
    worker: {} as Worker,
    memory: new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel,
    kernelState: { pipeBase: 0 } as ProcessHandle["kernelState"],
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
    childExitSyncRecords: new Map(),
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
    kernel,
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
    unregisterProcessWithKernelWorker: async (handle) => {
      order.push(`unregister:${handle.pid}`);
    },
    snapshotProcessFs: () => [],
  });

  await helpers.completeProcessExit(child, 0, 0);

  expect(order).toEqual([
    "kick:1",
    "pipe-sync:1:wake",
    "drain:1",
    "pipe-sync:3:wake",
    "drain:3",
    "unregister:3",
    "debug:1",
    "debug:1",
    "record:1",
    "pipe-sync:1:sync",
    "pipe-sync:1:sync",
    "release:3",
    "destroy:3",
    "pipe-sync:1:wake",
    "drain:1",
    "kick:1",
  ]);
});

test("completeProcessExit preserves published file data during exit MemFS publication", async () => {
  const handle = makeHandle({
    parentPid: null,
    processMemfsDirty: true,
  });
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const publishRuntimeFsState = vi.fn(() => 1);

  const helpers = createWorkerExitHelpers({
    processHandles: new Map([[3, handle]]),
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    captureRuntimeFailureSnapshot: () => {},
    debugRuntimeProcessHandles: () => "pid=3",
    drainHiddenProcessOutput: () => false,
    drainProcessOutput: () => false,
    destroyProcessHandle: () => {},
    formatAllLiveThreadDebug: () => "threads",
    kickBlockedWorkers: () => {},
    publishRuntimeFsState,
    recordChildExitViaKernelWorker: async () => {},
    releaseSharedHostStdio: async () => {},
    replaceRuntimeFsSnapshot: () => {},
    requestPipeStateSync: async () => {},
    snapshotProcessFs: () => [],
    syncForkSharedStateViaKernelWorker,
    unregisterProcessWithKernelWorker: async () => {},
  });

  await helpers.completeProcessExit(handle, 0, 0);

  expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalledWith(
    handle,
    [],
    expect.objectContaining({
      cause: { kind: "syscall", nr: -1n, name: "process-exit-memfs-publication" },
      includeMemfs: true,
      memfsOnly: true,
      trackHeldOfds: false,
      sourceVmPtr: handle.vmPtr,
    }),
  );
  expect(publishRuntimeFsState).toHaveBeenCalledWith(1, []);
});

test("completeProcessExit skips clean kernel-worker MemFS publication", async () => {
  const handle = makeHandle({
    parentPid: null,
    processMemfsDirty: false,
    processMemfsPublishPending: false,
  });
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const publishRuntimeFsState = vi.fn(() => 1);
  const snapshotProcessFs = vi.fn(() => []);

  const helpers = createWorkerExitHelpers({
    processHandles: new Map([[3, handle]]),
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    captureRuntimeFailureSnapshot: () => {},
    debugRuntimeProcessHandles: () => "pid=3",
    drainHiddenProcessOutput: () => false,
    drainProcessOutput: () => false,
    destroyProcessHandle: () => {},
    formatAllLiveThreadDebug: () => "threads",
    kickBlockedWorkers: () => {},
    publishRuntimeFsState,
    recordChildExitViaKernelWorker: async () => {},
    releaseSharedHostStdio: async () => {},
    replaceRuntimeFsSnapshot: () => {},
    requestPipeStateSync: async () => {},
    snapshotProcessFs,
    syncForkSharedStateViaKernelWorker,
    unregisterProcessWithKernelWorker: async () => {},
  });

  await helpers.completeProcessExit(handle, 0, 0);

  expect(syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  expect(snapshotProcessFs).not.toHaveBeenCalled();
  expect(publishRuntimeFsState).not.toHaveBeenCalled();
});

test("completeProcessExit drains top-level visible output before releasing stdio", async () => {
  const order: string[] = [];
  const stdoutChunks: string[] = [];
  const encoder = new TextEncoder();
  const handle = makeHandle({
    parentPid: null,
    stderrPipe: 3,
    hostStdio: {
      stdinPipe: 1,
      stdoutPipe: 2,
      stderrPipe: 3,
      refCount: 1,
    },
    onStdout: (data) => {
      stdoutChunks.push(new TextDecoder().decode(data));
    },
    kernel: {
      kernel_ofd_ino: (ofdId: number) => ofdId,
    } as never,
  });
  const payload = encoder.encode("final stdout\n");
  const slotBase = handle.kernelState.pipeBase + handle.hostStdio.stdoutPipe * (32 + 65_536);
  const view = new DataView(handle.memory.buffer);
  view.setUint32(slotBase, 0, true);
  view.setUint32(slotBase + 4, payload.length, true);
  view.setInt32(slotBase + 8, 1, true);
  view.setInt32(slotBase + 12, 1, true);
  new Uint8Array(handle.memory.buffer).set(payload, slotBase + 32);
  const processHandles = new Map<number, ProcessHandle>([
    [3, handle],
  ]);

  const helpers = createWorkerExitHelpers({
    processHandles,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    captureRuntimeFailureSnapshot: () => {},
    debugRuntimeProcessHandles: () => "pid=3",
    drainHiddenProcessOutput: () => false,
    drainProcessOutput(drainHandle) {
      order.push(`drain:${drainHandle.pid}`);
      return drainProcessOutput(drainHandle);
    },
    destroyProcessHandle(destroyHandle) {
      order.push(`destroy:${destroyHandle.pid}`);
    },
    formatAllLiveThreadDebug: () => "threads",
    kickBlockedWorkers: () => {},
    recordChildExitViaKernelWorker: async () => {},
    releaseSharedHostStdio: async (releaseHandle) => {
      order.push(`release:${releaseHandle.pid}`);
    },
    requestPipeStateSync: async (syncHandle) => {
      order.push(`pipe-sync:${syncHandle.pid}`);
    },
    unregisterProcessWithKernelWorker: async (unregisterHandle) => {
      order.push(`unregister:${unregisterHandle.pid}`);
    },
  });

  await helpers.completeProcessExit(handle, 0, 0);

  expect(order).toEqual([
    "pipe-sync:3",
    "drain:3",
    "pipe-sync:3",
    "pipe-sync:3",
    "drain:3",
    "release:3",
    "unregister:3",
    "destroy:3",
  ]);
  expect(stdoutChunks.join("")).toBe("final stdout\n");
});

test("completeProcessExit waits for stdio-owned child exit before releasing visible owner stdio", async () => {
  const order: string[] = [];
  let markChildRecordStarted!: () => void;
  let finishChildRecord!: () => void;
  const childRecordStarted = new Promise<void>((resolve) => {
    markChildRecordStarted = resolve;
  });
  const childRecordFinished = new Promise<void>((resolve) => {
    finishChildRecord = resolve;
  });
  const parent = makeHandle({
    pid: 1,
    parentPid: null,
    debugCommand: "/bin/parent",
  });
  const child = makeHandle({
    pid: 3,
    parentPid: 1,
    stdioOwner: parent,
    debugCommand: "/bin/child",
  });
  const processHandles = new Map<number, ProcessHandle>([
    [1, parent],
    [3, child],
  ]);

  const helpers = createWorkerExitHelpers({
    processHandles,
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      order.push(event);
    },
    appendWorkerDebugLog: () => {},
    captureRuntimeFailureSnapshot: () => {},
    debugRuntimeProcessHandles: () => "pid=1,pid=3",
    drainHiddenProcessOutput: () => false,
    drainProcessOutput: (handle) => {
      order.push(`drain:${handle.pid}`);
      return false;
    },
    destroyProcessHandle(handle) {
      order.push(`destroy:${handle.pid}`);
      processHandles.delete(handle.pid);
    },
    formatAllLiveThreadDebug: () => "threads",
    kickBlockedWorkers: () => {},
    recordChildExitViaKernelWorker: async () => {
      order.push("record-child-start");
      markChildRecordStarted();
      await childRecordFinished;
      order.push("record-child-done");
    },
    releaseSharedHostStdio: async (handle) => {
      order.push(`release:${handle.pid}`);
    },
    requestPipeStateSync: async () => {},
    unregisterProcessWithKernelWorker: async () => {},
  });

  const childExit = helpers.completeProcessExit(child, 0, 0);

  await childRecordStarted;
  const ownerExit = helpers.completeProcessExit(parent, 0, 0);
  expect(order).toContain("record-child-start");
  expect(order).not.toContain("release:1");

  finishChildRecord();
  await childExit;
  await ownerExit;
  expect(order.indexOf("destroy:3")).toBeLessThan(order.indexOf("release:1"));
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
    unregisterProcessWithKernelWorker: async (handle) => {
      order.push(`unregister:${handle.pid}`);
    },
    snapshotProcessFs: () => [],
  });

  await helpers.completeProcessExit(child, 0, 0);

  expect(order).not.toContain("hidden-drain:3");
  expect(order.filter((entry) => entry === "pipe-sync:3:sync")).toHaveLength(2);
  expect(order.filter((entry) => entry === "pipe-sync:1:sync")).toHaveLength(0);
  expect(order).toContain("record:1");
  expect(order).toContain("release:3");
  expect(order).toContain("unregister:3");
  expect(order).toContain("destroy:3");
  expect(order).not.toContain("drain:1");
  expect(order.at(-1)).toBe("kick:1");
});

test("completeProcessExit treats duped child stdio pipes as internal protocol pipes", async () => {
  const order: string[] = [];
  const parent = makeHandle({
    pid: 1,
    parentPid: null,
    kernelWorkerRequest: vi.fn(async () => ({ type: "pipe-state-synced" } as never)),
  });
  const child = makeHandle({
    stdioOwner: parent,
    kernel: {
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 1 ? 4 : fd === 2 ? 5 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 || ofdId === 5 ? 6 : 0,
      abi_fd_type_pipe: () => 6,
      kernel_ofd_ino: (ofdId: number) => ofdId === 4 ? 9 : ofdId === 5 ? 10 : 0,
    } as never,
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
    recordChildExitViaKernelWorker: async (parentHandle) => {
      order.push(`record:${parentHandle.pid}`);
    },
    releaseSharedHostStdio: async (handle) => {
      order.push(`release:${handle.pid}`);
    },
    requestPipeStateSync: async (handle, wakeWaiters = false) => {
      order.push(`pipe-sync:${handle.pid}:${wakeWaiters ? "wake" : "sync"}`);
    },
    unregisterProcessWithKernelWorker: async (handle) => {
      order.push(`unregister:${handle.pid}`);
    },
  });

  await helpers.completeProcessExit(child, 0, 0);

  expect(order).toContain("pipe-sync:3:wake");
  expect(order).toContain("hidden-drain:3");
  expect(order).toContain("record:1");
  expect(order).toContain("unregister:3");
  expect(order).toContain("pipe-sync:1:wake");
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
    unregisterProcessWithKernelWorker: async () => {},
    snapshotProcessFs: () => [],
  });

  await helpers.completeProcessExit(child, 0, 0);

  expect(observedPendingCounts).toEqual([0]);
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
    unregisterProcessWithKernelWorker: async () => {},
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
    unregisterProcessWithKernelWorker: async () => {},
  });

  await helpers.completeProcessExit(child, 1, 0);

  expect(child.lastDebugMessage).toContain("execve_stage=9");
  expect(child.lastDebugMessage).toContain("execve_stage_trace=[10:100:200,11:300:400]");
  expect(child.lastDebugMessage).not.toContain("execve_dispatch=[");
});

test("completeProcessExit includes frozen per-vm exec trace in exit trace", async () => {
  const child = makeHandle({
    parentPid: null,
    kernel: {
      debug_thread_step_exec_trace_len_for_vm: (vmPtr: number) => vmPtr === 0x1234 ? 2 : 0,
      debug_thread_step_exec_trace_entry_pc_for_vm: (_vmPtr: number, index: number) => [0x83336n, 0x83340n][index] ?? 0n,
      debug_thread_step_exec_trace_entry_raw_for_vm: (_vmPtr: number, index: number) => [0x00a00563, 0xfe013283][index] ?? 0,
      debug_thread_step_exec_trace_entry_step_for_vm: (_vmPtr: number, index: number) => [4, 4][index] ?? 0,
      debug_thread_step_exec_trace_entry_sp_for_vm: (_vmPtr: number, index: number) => [0x37d6c000n, 0x37d6c000n][index] ?? 0n,
      debug_thread_step_exec_trace_entry_ra_for_vm: (_vmPtr: number, index: number) => [0x4d9b4n, 0x4d9b4n][index] ?? 0n,
      debug_thread_step_exec_trace_entry_a0_for_vm: (_vmPtr: number, index: number) => [0n, 0n][index] ?? 0n,
      debug_thread_step_exec_trace_entry_s11_for_vm: (_vmPtr: number, index: number) => [0xbfdc80n, 0xbfdc80n][index] ?? 0n,
    } as never,
  });
  const helpers = createWorkerExitHelpers({
    processHandles: new Map<number, ProcessHandle>([[3, child]]),
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
    unregisterProcessWithKernelWorker: async () => {},
  });

  await helpers.completeProcessExit(child, 1, 0);

  expect(child.lastDebugMessage).toContain("thread_step_exec_trace=[0:pc=0x83336:raw=0xa00563");
  expect(child.lastDebugMessage).toContain("1:pc=0x83340:raw=0xfe013283");
});

test("completeProcessExit classifies high out-of-range fault pc and includes live exec trace", async () => {
  const faultVmPtr = 0x5a47fe0;
  const highPc = 0x4d5241355f534e68n;
  const child = makeHandle({
    pid: 5,
    parentPid: null,
    vmPtr: faultVmPtr,
    guestBase: 0x1000,
    debugCommand: "/usr/bin/riscv64-linux-gnu-gcc-16",
    kernel: {
      thread_pc: (vmPtr: number) => vmPtr === faultVmPtr ? highPc : 0n,
      thread_sp: (vmPtr: number) => vmPtr === faultVmPtr ? 0x4b4ff90n : 0n,
      thread_a0: (vmPtr: number) => vmPtr === faultVmPtr ? 6n : 0n,
      thread_a1: (vmPtr: number) => vmPtr === faultVmPtr ? 3855792n : 0n,
      thread_term_signal: (vmPtr: number) => vmPtr === faultVmPtr ? 11 : 0,
      thread_fault_pc: (vmPtr: number) => vmPtr === faultVmPtr ? highPc : 0n,
      thread_fault_addr: (vmPtr: number) => vmPtr === faultVmPtr ? highPc : 0n,
      thread_fault_kind: (vmPtr: number) => vmPtr === faultVmPtr ? 1 : 0,
      thread_fault_signal: (vmPtr: number) => vmPtr === faultVmPtr ? 11 : 0,
      thread_fault_code: (vmPtr: number) => vmPtr === faultVmPtr ? 1 : 0,
      thread_guest_address_limit: (vmPtr: number) => vmPtr === faultVmPtr ? 0x1_0000_0000n : 0n,
      debug_memory_vma_len: () => 1,
      debug_memory_vma_start: () => 0x1020_0000n,
      debug_memory_vma_end: () => 0x1021_0000n,
      debug_memory_vma_prot: () => 5,
      debug_memory_vma_flags: () => 2,
      debug_exec_trace_len: () => 2,
      debug_exec_trace_epoch_for_vm: (vmPtr: number) => vmPtr === faultVmPtr ? 7 : 0,
      debug_exec_trace_entry_vm_ptr: () => faultVmPtr,
      debug_exec_trace_entry_epoch: () => 7,
      debug_exec_trace_entry_pc: (_index: number) => _index === 0 ? 0x1020b1ccn : highPc,
      debug_exec_trace_entry_raw: (_index: number) => _index === 0 ? 0x00000013 : 0x00008067,
      debug_exec_trace_entry_step: () => 4,
      debug_exec_trace_entry_sp: () => 0x4b4ff90n,
      debug_exec_trace_entry_ra: () => 0xa6d0e6n,
      debug_exec_trace_entry_a0: () => 6n,
      debug_exec_trace_entry_s11: () => 0xbfdc80n,
    } as never,
  });
  const helpers = createWorkerExitHelpers({
    processHandles: new Map<number, ProcessHandle>([[5, child]]),
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    captureRuntimeFailureSnapshot: () => {},
    debugRuntimeProcessHandles: () => "pid=5",
    drainHiddenProcessOutput: () => false,
    drainProcessOutput: () => false,
    destroyProcessHandle: () => {},
    formatAllLiveThreadDebug: () => "threads",
    kickBlockedWorkers: () => {},
    recordChildExitViaKernelWorker: async () => {},
    releaseSharedHostStdio: async () => {},
    requestPipeStateSync: async () => {},
    unregisterProcessWithKernelWorker: async () => {},
  });

  await helpers.completeProcessExit(child, 139, 11, faultVmPtr);

  expect(child.lastDebugMessage).toContain("fault_bounds=[");
  expect(child.lastDebugMessage).toContain("fault_vma=[pc=none:fault_pc=none:fault_addr=none]");
  expect(child.lastDebugMessage).toContain("pc=outside_guest_limit");
  expect(child.lastDebugMessage).toContain("fault_pc=outside_guest_limit");
  expect(child.lastDebugMessage).toContain("fault_addr=outside_guest_limit");
  expect(child.lastDebugMessage).toContain("pc_ascii_le=\"hNS_5ARM\"");
  expect(child.lastDebugMessage).toContain("exec_trace=[0:pc=0x1020b1cc:raw=0x13");
  expect(child.lastDebugMessage).toContain("1:pc=0x4d5241355f534e68:raw=0x8067");
});

test("recordExitOnParent preserves child fault state in parent debug", async () => {
  vi.stubEnv("TIDEMARK_DEBUG_GUEST_BYTES", "0x7788");
  const handleVmPtr = 0x581aa20;
  const faultVmPtr = 0x5ce3040;
  const parent = makeHandle({
    pid: 1,
    parentPid: null,
    debugCommand: "/bin/parent",
  });
  const child = makeHandle({
    pid: 4,
    parentPid: 1,
    vmPtr: handleVmPtr,
    debugCommand: "rustc",
    kernel: {
      thread_pc: (vmPtr: number) => vmPtr === faultVmPtr ? 0x7788n : 0n,
      thread_sp: (vmPtr: number) => vmPtr === faultVmPtr ? 0x12340000n : 0n,
      thread_a0: (vmPtr: number) => vmPtr === faultVmPtr ? 139n : 0n,
      thread_a1: (vmPtr: number) => vmPtr === faultVmPtr ? 11n : 0n,
      thread_term_signal: (vmPtr: number) => vmPtr === faultVmPtr ? 11 : 0,
      thread_fault_pc: (vmPtr: number) => vmPtr === faultVmPtr ? 0x5566n : 0n,
      thread_fault_addr: (vmPtr: number) => vmPtr === faultVmPtr ? 0xdeadbeefn : 0n,
      thread_fault_kind: (vmPtr: number) => vmPtr === faultVmPtr ? 2 : 0,
      thread_fault_signal: (vmPtr: number) => vmPtr === faultVmPtr ? 11 : 0,
      thread_fault_code: (vmPtr: number) => vmPtr === faultVmPtr ? 1 : 0,
      thread_sig_mask: (vmPtr: number) => vmPtr === faultVmPtr ? 0x20n : 0n,
      thread_sig_pending: (vmPtr: number) => vmPtr === faultVmPtr ? 0x400n : 0n,
      thread_sig_info_code: (vmPtr: number) => vmPtr === faultVmPtr ? 1 : 0,
      debug_signal_delivery_for_vm_sig: (vmPtr: number) => vmPtr === faultVmPtr ? 11 : 0,
      debug_signal_delivery_for_vm_frame_base: (vmPtr: number) => vmPtr === faultVmPtr ? 0x12000n : 0n,
      debug_signal_delivery_for_vm_pc: (vmPtr: number) => vmPtr === faultVmPtr ? 0x7788n : 0n,
      debug_signal_delivery_for_vm_ra: (vmPtr: number) => vmPtr === faultVmPtr ? 0x123n : 0n,
      debug_signal_delivery_for_vm_sp: (vmPtr: number) => vmPtr === faultVmPtr ? 0x12340000n : 0n,
      debug_fd_syscall_trace_len: () => 320,
      debug_fd_syscall_trace_vm_ptr: () => faultVmPtr,
      debug_fd_syscall_trace_nr: () => 63n,
      debug_fd_syscall_trace_arg0_ofd_type: () => 6,
      debug_fd_syscall_trace_arg2_ofd_type: () => 6,
      debug_fd_syscall_trace_pid_sample0: () => 4n,
      debug_fd_syscall_trace_arg0: () => 2n,
      debug_fd_syscall_trace_arg0_ofd_id: () => 12,
      debug_fd_syscall_trace_arg0_ofd_status_flags: () => 2048,
      debug_fd_syscall_trace_arg0_ofd_ino: () => 99,
      debug_fd_syscall_trace_arg0_inode_mode: () => 0o100644,
      debug_fd_syscall_trace_arg1: (index: number) => BigInt(0x100000 + index * 4096),
      debug_fd_syscall_trace_arg2: () => 4096n,
      debug_fd_syscall_trace_arg2_ofd_id: () => 13,
      debug_fd_syscall_trace_result: () => 4096n,
      debug_fd_syscall_trace_status: () => 0,
      debug_default_signal_sig: () => 11,
      debug_default_signal_code: () => 1,
      debug_default_signal_vm_ptr: () => 0xabc,
      debug_default_signal_pc: () => 0x7788n,
      debug_default_signal_ra: () => 0x123n,
      debug_default_signal_sp: () => 0x12340000n,
      debug_default_signal_gp: () => 0x456n,
      debug_default_signal_tp: () => 0x789n,
      debug_default_signal_s11: () => 0xdefn,
      debug_default_signal_fault_pc: () => 0x5566n,
      debug_default_signal_fault_addr: () => 0xdeadbeefn,
      debug_illegal_instruction_vm_ptr: () => faultVmPtr,
      debug_illegal_instruction_pc: () => 0x7d00578cn,
      debug_illegal_instruction_raw: () => 0x0000007f,
      debug_memory_vma_len: () => 2,
      debug_memory_vma_start: (index: number) => index === 0 ? 0x5000n : 0x7000n,
      debug_memory_vma_end: (index: number) => index === 0 ? 0x6000n : 0x8000n,
      debug_memory_vma_prot: (index: number) => index === 0 ? 1 : 5,
      debug_memory_vma_flags: (index: number) => index === 0 ? 2 : 18,
      debug_memory_vma_fd: (index: number) => index === 0 ? 25 : 26,
      debug_memory_vma_offset: (index: number) => index === 0 ? 0x1000n : 0x2000n,
      kernel_heap_ptr: () => 0x9000,
      kernel_memfs_data_ptr: () => 0,
      kernel_memfs_data_bytes: () => 0,
    } as never,
  });
  const processHandles = new Map<number, ProcessHandle>([
    [1, parent],
    [4, child],
  ]);
  child.kernel.memfs_read_file_to = ((ino: number, offset: bigint, ptr: number, len: number) => {
    const bytes = new Uint8Array(child.memory.buffer, ptr, len);
    bytes.set(ino === 25
      ? Uint8Array.from([0xaa, 0xbb, Number(offset & 0xffn), 0xdd, 0xee, 0xff, 0x11, 0x22])
      : Uint8Array.from([0xcc, 0xdd, Number(offset & 0xffn), 0xee, 0xff, 0x11, 0x22, 0x33]));
    return BigInt(len);
  }) as never;
  new DataView(child.memory.buffer).setUint32(0x5566, 0x00008067, true);
  new DataView(child.memory.buffer).setUint32(0x7788, 0x00000013, true);

  const helpers = createWorkerExitHelpers({
    processHandles,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    captureRuntimeFailureSnapshot: () => {},
    debugRuntimeProcessHandles: () => "pid=1,pid=4",
    drainHiddenProcessOutput: () => false,
    drainProcessOutput: () => false,
    destroyProcessHandle: () => {},
    formatAllLiveThreadDebug: () => "threads",
    kickBlockedWorkers: () => {},
    recordChildExitViaKernelWorker: async () => {},
    releaseSharedHostStdio: async () => {},
    requestPipeStateSync: async () => {},
    unregisterProcessWithKernelWorker: async () => {},
  });

  await helpers.completeProcessExit(child, 0, 11, faultVmPtr);

  expect(parent.lastDebugMessage).toContain("child-debug:4:");
  expect(parent.lastDebugMessage).toContain(`child-debug:4:exit:4:"rustc":0:11 exit_trace=vm=0x${faultVmPtr.toString(16)}`);
  expect(parent.lastDebugMessage!.indexOf("fault_state=[")).toBeLessThan(parent.lastDebugMessage!.indexOf("fd_syscalls=["));
  expect(parent.lastDebugMessage).toContain("fault_state=[");
  expect(parent.lastDebugMessage).toContain("illegal_instruction=[vm=0x5ce3040:pc=0x7d00578c:raw=0x7f");
  expect(parent.lastDebugMessage).toContain("term_signal=11");
  expect(parent.lastDebugMessage).toContain("fault_pc=0x5566");
  expect(parent.lastDebugMessage).toContain("fault_addr=0xdeadbeef");
  expect(parent.lastDebugMessage).toContain("fault_insn=[pc_insn=0x13:fault_insn=0x8067]");
  expect(parent.lastDebugMessage).toContain("fault_vma=[pc=1/0x7000-0x8000/prot=5/flags=18/fd=26/off=8192:fault_pc=0/0x5000-0x6000/prot=1/flags=2/fd=25/off=4096:fault_addr=none]");
  expect(parent.lastDebugMessage).toContain("fault_bytes=[pc=vma1/fd=26/src=0x2788/guest=1300000000000000/file=ccdd88eeff112233:fault_pc=vma0/fd=25/src=0x1566/guest=6780000000000000/file=aabb66ddeeff1122:fault_addr=none]");
  expect(parent.lastDebugMessage).toContain("debug_guest_bytes=[addr0:0x7788=vma1/fd=26/src=0x2788/guest=1300000000000000/file=ccdd88eeff112233]");
  expect(parent.lastDebugMessage).toContain("fault_kind=2");
  expect(parent.lastDebugMessage).toContain("fault_signal=11");
  expect(parent.lastDebugMessage).toContain("fault_code=1");
  expect(parent.lastDebugMessage).toContain("default_signal=[");
  expect(parent.lastDebugMessage).toContain("sig=11");
  expect(parent.lastDebugMessage).toContain("vm=0xabc");
  expect(parent.lastDebugMessage).toContain("fault_addr=0xdeadbeef");
  expect(parent.lastDebugMessage).toContain("pc_vma=1/0x7000-0x8000/prot=5/flags=18/fd=26/off=8192");
  expect(parent.lastDebugMessage).toContain("fault_pc_vma=0/0x5000-0x6000/prot=1/flags=2/fd=25/off=4096");
  expect(parent.lastDebugMessage).toContain("fault_addr_vma=none");
  expect(parent.lastDebugMessage).toContain("signal_state=[");
  expect(parent.lastDebugMessage).toContain("mask=0x20");
  expect(parent.lastDebugMessage).toContain("pending=0x400");
  expect(parent.lastDebugMessage).toContain("info=1");
  expect(parent.lastDebugMessage).toContain("deliver_sig=11");
  expect(parent.lastDebugMessage).toContain("deliver_frame=0x12000");
  expect(parent.lastDebugMessage).toContain("deliver_pc=0x7788");
});
