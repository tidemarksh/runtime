import { addKernelShareMember, addKernelShareSharedOfdId, kernelShareHasSharedOfdId, kernelShareMembers, kernelShareSharedOfdIdList, processKernelShareGroup, setProcessKernelShareGroup } from "../../../src/worker/kernel-share.js";
import { expect, test } from "vite-plus/test";
import { createWorkerRuntimeContext } from "../../../src/worker/runtime-context.js";
import {
  setProcessDebugCommand,
  setProcessLastDebugMessage,
} from "../../../src/worker/process-debug-state.js";
import { setProcessMemoryOwnership } from "../../../src/worker/memory-ownership-state.js";
import { setProcessMemfsDirty } from "../../../src/worker/process-memfs-state.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

function makeHandle(runtimeId: number, pid: number): ProcessHandle {
  const memoryOwnership = {
    guestAddressProfile: 2,
    guestAddressLimitBytes: "9223372036489871359",
    guestMemoryPolicyLimitBytes: 64n * 1024n * 1024n,
    processMaxMemoryBytes: 2048 * 65536,
    forkProcessMaxMemoryBytes: 4096 * 65536,
    pageCacheDataCapacityBytes: 8 * 1024 * 1024,
    publishedMetadataCapacityBytes: 1024,
    fsRpcRingCapacityBytes: 4096,
    kernelWorkerPrivateMemoryBytes: 512 * 1024 * 1024,
    artifactMaterializationBudgetBytes: 16 * 1024 * 1024,
    artifactMaterializedBytes: 4 * 1024 * 1024,
  };
  const handle = {
    runtimeId,
    pid,
    parentPid: null,
    vmPtr: 0x1000 + pid,
    guestMemoryPolicyLimitBytes: 64n * 1024n * 1024n,
    guestBase: 0x100000,
    maxMemoryPages: 2048,
    forkMaxMemoryPages: 4096,
    worker: {} as Worker,
    memory: new WebAssembly.Memory({ initial: 32, maximum: 64, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel: {} as ProcessHandle["kernel"],
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
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    hostStdio: { stdinPipe: 0, stdoutPipe: 0, stderrPipe: 0, refCount: 1 },
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    stdinQueue: [],
    outputBacklog: [],
    runtimeBridge: {} as ProcessHandle["runtimeBridge"],
    sabPageCache: null,
    workers: new Set(),
    vforkReadyWorkers: [],
    runningWorker: null,
    hostBlockedWorkers: new Map(),
    vforkSnapshot: null,
    vforkWorker: null,
    workerDebugLog: [],
  } as unknown as ProcessHandle;
  setProcessMemoryOwnership(handle, memoryOwnership);
  setProcessDebugCommand(handle, "runtime-context");
  addKernelShareMember(handle);
  return handle;
}

function makeRootMemfsKernel() {
  const memory = new WebAssembly.Memory({ initial: 8, maximum: 8, shared: true });
  return {
    memory,
    kernel_heap_ptr: () => 0x10000,
    memfs_resolve_path: (ptr: number, len: number) => {
      const path = new TextDecoder().decode(new Uint8Array(memory.buffer, ptr, len));
      return path === "/" ? 1 : 0;
    },
    memfs_inode_mode: (ino: number) => (ino === 1 ? 0o040755 : 0),
    memfs_readdir_names: () => 0n,
    memfs_file_size: () => 0n,
    memfs_read_file_to: () => 0n,
  } as unknown as ProcessHandle["kernel"];
}

test("clearRuntimeFsState removes only the selected runtime snapshot and dirty handles", () => {
  const context = createWorkerRuntimeContext({
    formatPipeCounterDebug: () => "fd-debug",
  });
  context.primeRuntimeFsState(1, [{ kind: "dir", path: "/", mode: 0o755 }]);
  context.primeRuntimeFsState(2, [{ kind: "dir", path: "/", mode: 0o700 }]);
  const runtime1Handle = makeHandle(1, 11);
  const runtime2Handle = makeHandle(2, 22);
  context.markRuntimeFsDirty(runtime1Handle);
  context.markRuntimeFsDirty(runtime2Handle);

  context.clearRuntimeFsState(1);

  expect(context.getRuntimeFsState(1)).toBeNull();
  expect(context.getRuntimeFsState(2)).toEqual({
    version: 1,
    entries: [{ kind: "dir", path: "/", mode: 0o700 }],
  });
  expect(context.runtimeFsDirtyHandles.has(1)).toBe(false);
  expect(context.runtimeFsDirtyHandles.has(2)).toBe(true);
});

test("flushRuntimeFsStateFromDirtyHandle drops stale dirty handles instead of leaking them across tests", () => {
  const context = createWorkerRuntimeContext({
    formatPipeCounterDebug: () => "fd-debug",
  });
  const staleHandle = makeHandle(5, 55);
  context.markRuntimeFsDirty(staleHandle);

  const flushed = context.flushRuntimeFsStateFromDirtyHandle(5);

  expect(flushed).toBeNull();
  expect(context.runtimeFsDirtyHandles.has(5)).toBe(false);
});

test("flushRuntimeFsStateFromDirtyHandle publishes one snapshot for identical live handles and clears dirty state", () => {
  const context = createWorkerRuntimeContext({
    formatPipeCounterDebug: () => "fd-debug",
  });
  const handleA = makeHandle(7, 71);
  const handleB = makeHandle(7, 72);
  const kernelA = makeRootMemfsKernel();
  const kernelB = makeRootMemfsKernel();
  handleA.kernel = kernelA;
  handleA.memory = kernelA.memory;
  handleB.kernel = kernelB;
  handleB.memory = kernelB.memory;
  setProcessMemfsDirty(handleA, true);
  setProcessMemfsDirty(handleB, true);
  context.processHandles.set(handleA.pid, handleA);
  context.processHandles.set(handleB.pid, handleB);
  context.markRuntimeFsDirty(handleA);
  context.markRuntimeFsDirty(handleB);

  const flushed = context.flushRuntimeFsStateFromDirtyHandle(7);

  expect(flushed).toEqual({
    version: 1,
    entries: [{ kind: "dir", path: "/", ino: 1, mode: 0o755 }],
  });
  expect(context.runtimeFsDirtyHandles.has(7)).toBe(false);
  expect(context.getRuntimeFsState(7)).toEqual(flushed);

  const secondFlush = context.flushRuntimeFsStateFromDirtyHandle(7);
  expect(secondFlush).toBeNull();
});

test("captureRuntimeFailureSnapshot stores and clears the last runtime failure", () => {
  const context = createWorkerRuntimeContext({
    formatPipeCounterDebug: () => "fd-debug",
  });
  const handle = makeHandle(9, 91);
  setProcessDebugCommand(handle, "go version");
  setProcessLastDebugMessage(handle, "child-debug");
  context.processHandles.set(handle.pid, handle);

  context.captureRuntimeFailureSnapshot(handle, 2, 11);

  expect(context.debugRuntimeLastFailureSnapshot(9)).toEqual({
    pid: 91,
    cmd: "go version",
    exitCode: 2,
    termSignal: 11,
    debug: expect.any(String),
    memory: {
      memoryBytes: 32 * 65536,
      guestBase: 0x100000,
      processCommittedBytes: 0x100000,
      guestMemoryPolicyLimitBytes: String(64n * 1024n * 1024n),
      guestAddressProfile: 2,
      guestAddressLimitBytes: "9223372036489871359",
      processMaxMemoryBytes: 2048 * 65536,
      forkProcessMaxMemoryBytes: 4096 * 65536,
      pageCacheDataCapacityBytes: 8 * 1024 * 1024,
      publishedMetadataCapacityBytes: 1024,
      fsRpcRingCapacityBytes: 4096,
      kernelWorkerPrivateMemoryBytes: 512 * 1024 * 1024,
      artifactMaterializationBudgetBytes: 16 * 1024 * 1024,
      artifactMaterializedBytes: 4 * 1024 * 1024,
    },
    memorySyscalls: [],
    workerFocus: [{
      pid: 91,
      cmd: "go version",
      workers: [],
    }],
  });
  expect(() => JSON.stringify(context.debugRuntimeLastFailureSnapshot(9))).not.toThrow();

  context.clearRuntimeFailureSnapshot(9);
  expect(context.debugRuntimeLastFailureSnapshot(9)).toBeNull();
});
