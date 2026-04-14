import { expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { createKernelWorkerProcessControl } from "../../../src/kernel-worker/process-control.js";

type FakeKernelExports = KernelExports & {
  opLog: string[];
};

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

function makeKernelExports(options: {
  opcode?: number;
  effects?: number;
  resumeResult?: number;
  targetIno?: number;
  path?: string;
  absolutePath?: string;
  secondaryAbsolutePath?: string;
} = {}): FakeKernelExports {
  let runtimeState = makeKernelState();
  const opLog: string[] = [];
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const guestBase = 0x1000;
  const pathPtr = 0x2000;
  const pathBytes = options.path ? new TextEncoder().encode(options.path) : null;
  const absolutePathBytes = options.absolutePath ? new TextEncoder().encode(options.absolutePath) : null;
  const secondaryAbsolutePathBytes = options.secondaryAbsolutePath
    ? new TextEncoder().encode(options.secondaryAbsolutePath)
    : null;
  if (pathBytes) {
    new Uint8Array(memory.buffer).set(pathBytes, guestBase + pathPtr);
  }
  return {
    memory,
    kernel_heap_base: () => runtimeState.heapBase,
    kernel_heap_ptr: () => runtimeState.heapPtr,
    kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
    kernel_memfs_ptr: () => runtimeState.memfsPtr,
    kernel_mem_state_ptr: () => runtimeState.memStatePtr,
    kernel_epoll_table_ptr: () => runtimeState.epollTablePtr,
    kernel_socket_table_ptr: () => runtimeState.socketTablePtr,
    kernel_socket_used: () => runtimeState.socketUsed,
    kernel_pipe_base: () => runtimeState.pipeBase,
    kernel_pipe_used: () => runtimeState.pipeUsed,
    kernel_net_buf_base: () => runtimeState.netBufBase,
    kernel_net_buf_used: () => runtimeState.netBufUsed,
    kernel_sig_slots_ptr: () => runtimeState.sigSlotsPtr,
    kernel_process_pending_ptr: () => runtimeState.processPendingPtr,
    kernel_process_runtime_state_ptr: () => runtimeState.processRuntimeStatePtr,
    kernel_time_state_ptr: () => runtimeState.timeStatePtr,
    kernel_attach_runtime_state: (
      heapBase: number,
      heapPtr: number,
      ofdTablePtr: number,
      memfsPtr: number,
      memStatePtr: number,
      epollTablePtr: number,
      socketTablePtr: number,
      socketUsed: number,
      pipeBase: number,
      pipeUsed: bigint,
      netBufBase: number,
      netBufUsed: bigint,
      sigSlotsPtr: number,
      processPendingPtr: number,
      processRuntimeStatePtr: number,
      timeStatePtr: number,
    ) => {
      runtimeState = {
        heapBase,
        heapPtr,
        ofdTablePtr,
        memfsPtr,
        memStatePtr,
        epollTablePtr,
        socketTablePtr,
        socketUsed,
        pipeBase,
        pipeUsed,
        netBufBase,
        netBufUsed,
        sigSlotsPtr,
        processPendingPtr,
        processRuntimeStatePtr,
        timeStatePtr,
      };
    },
    thread_pending_fs_opcode: () => options.opcode ?? 0,
    thread_pending_fs_effects: () => options.effects ?? 0,
    thread_pending_fs_target_ino: () => options.targetIno ?? 0,
    thread_pending_fs_path_ptr: () => (pathBytes ? pathPtr : 0),
    thread_pending_fs_path_len: () => pathBytes?.length ?? 0,
    thread_pending_fs_absolute_path: (_vmPtr: number, bufPtr: number, bufLen: number) => {
      if (!absolutePathBytes || absolutePathBytes.length > bufLen) {
        return 0n;
      }
      new Uint8Array(memory.buffer).set(absolutePathBytes, bufPtr);
      return BigInt(absolutePathBytes.length);
    },
    thread_pending_fs_secondary_absolute_path: (_vmPtr: number, bufPtr: number, bufLen: number) => {
      if (!secondaryAbsolutePathBytes || secondaryAbsolutePathBytes.length > bufLen) {
        return 0n;
      }
      new Uint8Array(memory.buffer).set(secondaryAbsolutePathBytes, bufPtr);
      return BigInt(secondaryAbsolutePathBytes.length);
    },
    thread_guest_base: () => guestBase,
    thread_process_fs: () => {
      opLog.push("process-fs");
    },
    thread_resume_fs: () => {
      opLog.push("resume-fs");
      return BigInt(options.resumeResult ?? 0);
    },
    opLog,
  } as unknown as FakeKernelExports;
}

test("registerProcess rehydrates first process memfs metadata-only after canonical bootstrap", async () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports();
  const processKernels = new Map<number, KernelExports>();
  const copyCanonicalModes: boolean[] = [];
  const copyMemfsModes: boolean[] = [];
  const rebuiltFrom: KernelExports[] = [];
  const instantiateSpy = vi.spyOn(WebAssembly, "instantiate")
    .mockResolvedValue({ exports: processExports } as unknown as WebAssembly.WebAssemblyInstantiatedSource);

  try {
    const processControl = createKernelWorkerProcessControl({
      getCanonicalExports: () => canonicalExports,
      setCanonicalExports: () => {},
      getWasmModule: () => ({} as WebAssembly.Module),
      setWasmModule: () => {},
      getSabPageCache: () => new SharedArrayBuffer(65536),
      setSabPageCache: () => {},
      processKernels,
      lastProcessPipeSlotModes: new Map(),
      canonicalProcesses: new Map(),
      seedEntries: new Map([["/", { kind: "dir", path: "/", mode: 0o755 }]]),
      getCanonicalNextPid: () => 3,
      setCanonicalNextPid: () => {},
      publishRootInode: () => {},
      copyOfdState: () => {},
      copyMemfsState: (_sourceExports, _sourceState, _targetExports, _targetState, options) => {
        copyMemfsModes.push(Boolean(options?.includeData));
      },
      rebuildPageCacheFromExports: (sourceExports) => {
        rebuiltFrom.push(sourceExports);
      },
      rebuildPageCacheFromCanonical: () => {},
      syncPipeState: (_pid, kernelState) => kernelState,
      syncEpollState: (_pid, kernelState) => kernelState,
      syncSocketState: (_pid, kernelState) => kernelState,
      copyCanonicalMemfsToProcess: (_canonicalState, _targetExports, _targetState, options) => {
        copyCanonicalModes.push(Boolean(options?.includeData));
      },
      composeProcessPipeUsed: () => 0n,
      collectProcessHeldEpollOfdIds: () => [],
      collectProcessHeldPipeSlots: () => new Set(),
      mergePipeSlotsFromCanonical: () => {},
      recomputeCanonicalPipeControlState: () => ({
        kernelState: makeKernelState(),
        readOpen: new Int32Array(0),
        writeOpen: new Int32Array(0),
      }),
      applyPipeControlState: () => {},
      copyEpollState: () => {},
      copySelectedEpollState: () => {},
      copySocketState: () => {},
      syncFileDataMutation: () => false,
      syncFileDataMutationByPath: () => false,
      syncMetadataMutation: () => false,
      fanoutCanonicalMemfs: () => {},
      fsEffectMemfsMutation: 1 << 0,
      fsEffectSharedOfdState: 1 << 1,
      fsEffectMemfsFileData: 1 << 2,
      fsOpOpenAt: 3,
      fsOpMkdir: 6,
      fsOpUnlink: 7,
    });

    await processControl.registerProcess(7, 0x1234, processExports.memory, makeKernelState(), "full");

    expect(copyMemfsModes).toEqual([false]);
    expect(rebuiltFrom).toEqual([processExports]);
    expect(copyCanonicalModes).toEqual([false]);
  } finally {
    instantiateSpy.mockRestore();
  }
});

test("registerProcess rehydrates canonical snapshot metadata-only for the first process", async () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports();
  const processKernels = new Map<number, KernelExports>();
  const copyCanonicalModes: boolean[] = [];
  const instantiateSpy = vi.spyOn(WebAssembly, "instantiate")
    .mockResolvedValue({ exports: processExports } as unknown as WebAssembly.WebAssemblyInstantiatedSource);

  try {
    const processControl = createKernelWorkerProcessControl({
      getCanonicalExports: () => canonicalExports,
      setCanonicalExports: () => {},
      getWasmModule: () => ({} as WebAssembly.Module),
      setWasmModule: () => {},
      getSabPageCache: () => new SharedArrayBuffer(65536),
      setSabPageCache: () => {},
      processKernels,
      lastProcessPipeSlotModes: new Map(),
      canonicalProcesses: new Map(),
      seedEntries: new Map([
        ["/", { kind: "dir", path: "/", mode: 0o755 }],
        ["/work", { kind: "dir", path: "/work", mode: 0o755 }],
      ]),
      getCanonicalNextPid: () => 3,
      setCanonicalNextPid: () => {},
      publishRootInode: () => {},
      copyOfdState: () => {},
      copyMemfsState: () => {},
      rebuildPageCacheFromExports: () => {},
      rebuildPageCacheFromCanonical: () => {},
      syncPipeState: (_pid, kernelState) => kernelState,
      syncEpollState: (_pid, kernelState) => kernelState,
      syncSocketState: (_pid, kernelState) => kernelState,
      copyCanonicalMemfsToProcess: (_canonicalState, _targetExports, _targetState, options) => {
        copyCanonicalModes.push(Boolean(options?.includeData));
      },
      composeProcessPipeUsed: () => 0n,
      collectProcessHeldEpollOfdIds: () => [],
      collectProcessHeldPipeSlots: () => new Set(),
      mergePipeSlotsFromCanonical: () => {},
      recomputeCanonicalPipeControlState: () => ({
        kernelState: makeKernelState(),
        readOpen: new Int32Array(0),
        writeOpen: new Int32Array(0),
      }),
      applyPipeControlState: () => {},
      copyEpollState: () => {},
      copySelectedEpollState: () => {},
      copySocketState: () => {},
      syncFileDataMutation: () => false,
      syncFileDataMutationByPath: () => false,
      syncMetadataMutation: () => false,
      fanoutCanonicalMemfs: () => {},
      fsEffectMemfsMutation: 1 << 0,
      fsEffectSharedOfdState: 1 << 1,
      fsEffectMemfsFileData: 1 << 2,
      fsOpOpenAt: 3,
      fsOpMkdir: 6,
      fsOpUnlink: 7,
    });

    await processControl.registerProcess(7, 0x1234, processExports.memory, makeKernelState(), "full");

    expect(copyCanonicalModes).toEqual([false]);
  } finally {
    instantiateSpy.mockRestore();
  }
});

test("registerProcess rehydrates later process memfs metadata-only", async () => {
  const canonicalExports = makeKernelExports();
  const existingProcessExports = makeKernelExports();
  const processExports = makeKernelExports();
  const processKernels = new Map<number, KernelExports>([[5, existingProcessExports]]);
  const copyCanonicalModes: boolean[] = [];
  const instantiateSpy = vi.spyOn(WebAssembly, "instantiate")
    .mockResolvedValue({ exports: processExports } as unknown as WebAssembly.WebAssemblyInstantiatedSource);

  try {
    const processControl = createKernelWorkerProcessControl({
      getCanonicalExports: () => canonicalExports,
      setCanonicalExports: () => {},
      getWasmModule: () => ({} as WebAssembly.Module),
      setWasmModule: () => {},
      getSabPageCache: () => new SharedArrayBuffer(65536),
      setSabPageCache: () => {},
      processKernels,
      lastProcessPipeSlotModes: new Map(),
      canonicalProcesses: new Map(),
      seedEntries: new Map([
        ["/", { kind: "dir", path: "/", mode: 0o755 }],
        ["/work", { kind: "dir", path: "/work", mode: 0o755 }],
      ]),
      getCanonicalNextPid: () => 6,
      setCanonicalNextPid: () => {},
      publishRootInode: () => {},
      copyOfdState: () => {},
      copyMemfsState: () => {},
      rebuildPageCacheFromExports: () => {},
      rebuildPageCacheFromCanonical: () => {},
      syncPipeState: (_pid, kernelState) => kernelState,
      syncEpollState: (_pid, kernelState) => kernelState,
      syncSocketState: (_pid, kernelState) => kernelState,
      copyCanonicalMemfsToProcess: (_canonicalState, _targetExports, _targetState, options) => {
        copyCanonicalModes.push(Boolean(options?.includeData));
      },
      composeProcessPipeUsed: () => 0n,
      collectProcessHeldEpollOfdIds: () => [],
      collectProcessHeldPipeSlots: () => new Set(),
      mergePipeSlotsFromCanonical: () => {},
      recomputeCanonicalPipeControlState: () => ({
        kernelState: makeKernelState(),
        readOpen: new Int32Array(0),
        writeOpen: new Int32Array(0),
      }),
      applyPipeControlState: () => {},
      copyEpollState: () => {},
      copySelectedEpollState: () => {},
      copySocketState: () => {},
      syncFileDataMutation: () => false,
      syncFileDataMutationByPath: () => false,
      syncMetadataMutation: () => false,
      fanoutCanonicalMemfs: () => {},
      fsEffectMemfsMutation: 1 << 0,
      fsEffectSharedOfdState: 1 << 1,
      fsEffectMemfsFileData: 1 << 2,
      fsOpOpenAt: 3,
      fsOpMkdir: 6,
      fsOpUnlink: 7,
    });

    await processControl.registerProcess(7, 0x1234, processExports.memory, makeKernelState(), "full");

    expect(copyCanonicalModes).toEqual([false]);
  } finally {
    instantiateSpy.mockRestore();
  }
});

test("processFs treats unlink as metadata-only canonical sync", () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    opcode: 7,
    effects: 1 << 0,
    resumeResult: 0,
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const copyMemfsCalls: boolean[] = [];
  const fanoutCalls: boolean[] = [];

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => new SharedArrayBuffer(65536),
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    canonicalProcesses: new Map(),
    seedEntries: new Map(),
    getCanonicalNextPid: () => 3,
    setCanonicalNextPid: () => {},
    publishRootInode: () => {},
    copyOfdState: () => {},
    copyMemfsState: (_sourceExports, _sourceState, _targetExports, _targetState, options) => {
      copyMemfsCalls.push(Boolean(options?.includeData));
    },
    rebuildPageCacheFromExports: () => {},
    rebuildPageCacheFromCanonical: () => {},
    syncPipeState: (_pid, kernelState) => kernelState,
    syncEpollState: (_pid, kernelState) => kernelState,
    syncSocketState: (_pid, kernelState) => kernelState,
    copyCanonicalMemfsToProcess: () => {},
    composeProcessPipeUsed: () => 0n,
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    mergePipeSlotsFromCanonical: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
    }),
    applyPipeControlState: () => {},
    copyEpollState: () => {},
    copySelectedEpollState: () => {},
    copySocketState: () => {},
    syncFileDataMutation: () => false,
    syncFileDataMutationByPath: () => false,
    syncMetadataMutation: () => false,
    fanoutCanonicalMemfs: () => {
      fanoutCalls.push(true);
    },
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(copyMemfsCalls).toEqual([false]);
  expect(fanoutCalls).toEqual([true]);
});

test("processFs uses targeted metadata sync for absolute mkdir paths", () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    opcode: 6,
    effects: 1 << 0,
    resumeResult: 0,
    path: "/work/cache",
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const copyMemfsCalls: boolean[] = [];
  const syncedPaths: string[] = [];

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => new SharedArrayBuffer(65536),
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    canonicalProcesses: new Map(),
    seedEntries: new Map(),
    getCanonicalNextPid: () => 3,
    setCanonicalNextPid: () => {},
    publishRootInode: () => {},
    copyOfdState: () => {},
    copyMemfsState: (_sourceExports, _sourceState, _targetExports, _targetState, options) => {
      copyMemfsCalls.push(Boolean(options?.includeData));
    },
    rebuildPageCacheFromExports: () => {},
    rebuildPageCacheFromCanonical: () => {},
    syncPipeState: (_pid, kernelState) => kernelState,
    syncEpollState: (_pid, kernelState) => kernelState,
    syncSocketState: (_pid, kernelState) => kernelState,
    copyCanonicalMemfsToProcess: () => {},
    composeProcessPipeUsed: () => 0n,
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    mergePipeSlotsFromCanonical: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
    }),
    applyPipeControlState: () => {},
    copyEpollState: () => {},
    copySelectedEpollState: () => {},
    copySocketState: () => {},
    syncFileDataMutation: () => false,
    syncFileDataMutationByPath: () => false,
    syncMetadataMutation: (_sourceExports, path) => {
      syncedPaths.push(path);
      return true;
    },
    fanoutCanonicalMemfs: () => {
      throw new Error("full fanout should be skipped");
    },
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(syncedPaths).toEqual(["/work/cache"]);
  expect(copyMemfsCalls).toEqual([]);
});

test("processFs uses targeted metadata sync for absolute tree mutations without an opcode allowlist", () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    opcode: 99,
    effects: 1 << 0,
    resumeResult: 0,
    absolutePath: "/work/cache",
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const copyMemfsCalls: boolean[] = [];
  const syncedPaths: string[] = [];

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => new SharedArrayBuffer(65536),
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    canonicalProcesses: new Map(),
    seedEntries: new Map(),
    getCanonicalNextPid: () => 3,
    setCanonicalNextPid: () => {},
    publishRootInode: () => {},
    copyOfdState: () => {},
    copyMemfsState: (_sourceExports, _sourceState, _targetExports, _targetState, options) => {
      copyMemfsCalls.push(Boolean(options?.includeData));
    },
    rebuildPageCacheFromExports: () => {},
    rebuildPageCacheFromCanonical: () => {},
    syncPipeState: (_pid, kernelState) => kernelState,
    syncEpollState: (_pid, kernelState) => kernelState,
    syncSocketState: (_pid, kernelState) => kernelState,
    copyCanonicalMemfsToProcess: () => {},
    composeProcessPipeUsed: () => 0n,
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    mergePipeSlotsFromCanonical: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
    }),
    applyPipeControlState: () => {},
    copyEpollState: () => {},
    copySelectedEpollState: () => {},
    copySocketState: () => {},
    syncFileDataMutation: () => false,
    syncFileDataMutationByPath: () => false,
    syncMetadataMutation: (_sourceExports, path) => {
      syncedPaths.push(path);
      return true;
    },
    fanoutCanonicalMemfs: () => {
      throw new Error("full fanout should be skipped");
    },
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(syncedPaths).toEqual(["/work/cache"]);
  expect(copyMemfsCalls).toEqual([]);
});

test("processFs uses targeted metadata sync for absolute unlink paths", () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    opcode: 7,
    effects: 1 << 0,
    resumeResult: 0,
    path: "/work/remove.txt",
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const copyMemfsCalls: boolean[] = [];
  const syncedPaths: string[] = [];

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => new SharedArrayBuffer(65536),
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    canonicalProcesses: new Map(),
    seedEntries: new Map(),
    getCanonicalNextPid: () => 3,
    setCanonicalNextPid: () => {},
    publishRootInode: () => {},
    copyOfdState: () => {},
    copyMemfsState: (_sourceExports, _sourceState, _targetExports, _targetState, options) => {
      copyMemfsCalls.push(Boolean(options?.includeData));
    },
    rebuildPageCacheFromExports: () => {},
    rebuildPageCacheFromCanonical: () => {},
    syncPipeState: (_pid, kernelState) => kernelState,
    syncEpollState: (_pid, kernelState) => kernelState,
    syncSocketState: (_pid, kernelState) => kernelState,
    copyCanonicalMemfsToProcess: () => {},
    composeProcessPipeUsed: () => 0n,
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    mergePipeSlotsFromCanonical: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
    }),
    applyPipeControlState: () => {},
    copyEpollState: () => {},
    copySelectedEpollState: () => {},
    copySocketState: () => {},
    syncFileDataMutation: () => false,
    syncFileDataMutationByPath: () => false,
    syncMetadataMutation: (_sourceExports, path) => {
      syncedPaths.push(path);
      return true;
    },
    fanoutCanonicalMemfs: () => {
      throw new Error("full fanout should be skipped");
    },
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(syncedPaths).toEqual(["/work/remove.txt"]);
  expect(copyMemfsCalls).toEqual([]);
});

test("processFs uses kernel-provided absolute path for relative mkdir requests", () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    opcode: 6,
    effects: 1 << 0,
    resumeResult: 0,
    path: "cache",
    absolutePath: "/work/cache",
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const copyMemfsCalls: boolean[] = [];
  const syncedPaths: string[] = [];

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => new SharedArrayBuffer(65536),
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    canonicalProcesses: new Map(),
    seedEntries: new Map(),
    getCanonicalNextPid: () => 3,
    setCanonicalNextPid: () => {},
    publishRootInode: () => {},
    copyOfdState: () => {},
    copyMemfsState: (_sourceExports, _sourceState, _targetExports, _targetState, options) => {
      copyMemfsCalls.push(Boolean(options?.includeData));
    },
    rebuildPageCacheFromExports: () => {},
    rebuildPageCacheFromCanonical: () => {},
    syncPipeState: (_pid, kernelState) => kernelState,
    syncEpollState: (_pid, kernelState) => kernelState,
    syncSocketState: (_pid, kernelState) => kernelState,
    copyCanonicalMemfsToProcess: () => {},
    composeProcessPipeUsed: () => 0n,
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    mergePipeSlotsFromCanonical: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
    }),
    applyPipeControlState: () => {},
    copyEpollState: () => {},
    copySelectedEpollState: () => {},
    copySocketState: () => {},
    syncFileDataMutation: () => false,
    syncFileDataMutationByPath: () => false,
    syncMetadataMutation: (_sourceExports, path) => {
      syncedPaths.push(path);
      return true;
    },
    fanoutCanonicalMemfs: () => {
      throw new Error("full fanout should be skipped");
    },
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(syncedPaths).toEqual(["/work/cache"]);
  expect(copyMemfsCalls).toEqual([]);
});

test("processFs uses targeted file-data sync when target ino is unavailable but absolute path is known", () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    opcode: 3,
    effects: 1 << 2,
    resumeResult: 0,
    path: "app.bin",
    absolutePath: "/work/app.bin",
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const copyMemfsCalls: boolean[] = [];
  const syncedPaths: string[] = [];

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => new SharedArrayBuffer(65536),
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    canonicalProcesses: new Map(),
    seedEntries: new Map(),
    getCanonicalNextPid: () => 3,
    setCanonicalNextPid: () => {},
    publishRootInode: () => {},
    copyOfdState: () => {},
    copyMemfsState: (_sourceExports, _sourceState, _targetExports, _targetState, options) => {
      copyMemfsCalls.push(Boolean(options?.includeData));
    },
    rebuildPageCacheFromExports: () => {},
    rebuildPageCacheFromCanonical: () => {},
    syncPipeState: (_pid, kernelState) => kernelState,
    syncEpollState: (_pid, kernelState) => kernelState,
    syncSocketState: (_pid, kernelState) => kernelState,
    copyCanonicalMemfsToProcess: () => {},
    composeProcessPipeUsed: () => 0n,
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    mergePipeSlotsFromCanonical: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
    }),
    applyPipeControlState: () => {},
    copyEpollState: () => {},
    copySelectedEpollState: () => {},
    copySocketState: () => {},
    syncFileDataMutation: () => false,
    syncFileDataMutationByPath: (_sourceExports, path) => {
      syncedPaths.push(path);
      return true;
    },
    syncMetadataMutation: () => false,
    fanoutCanonicalMemfs: () => {
      throw new Error("full fanout should be skipped");
    },
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(syncedPaths).toEqual(["/work/app.bin"]);
  expect(copyMemfsCalls).toEqual([]);
});

test("processFs uses targeted metadata and file-data sync for openat(O_CREAT|O_TRUNC)", () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    opcode: 3,
    effects: (1 << 0) | (1 << 2),
    resumeResult: 0,
    path: "app.bin",
    absolutePath: "/work/app.bin",
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const copyMemfsCalls: boolean[] = [];
  const metadataPaths: string[] = [];
  const filePaths: string[] = [];

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => new SharedArrayBuffer(65536),
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    canonicalProcesses: new Map(),
    seedEntries: new Map(),
    getCanonicalNextPid: () => 3,
    setCanonicalNextPid: () => {},
    publishRootInode: () => {},
    copyOfdState: () => {},
    copyMemfsState: (_sourceExports, _sourceState, _targetExports, _targetState, options) => {
      copyMemfsCalls.push(Boolean(options?.includeData));
    },
    rebuildPageCacheFromExports: () => {},
    rebuildPageCacheFromCanonical: () => {},
    syncPipeState: (_pid, kernelState) => kernelState,
    syncEpollState: (_pid, kernelState) => kernelState,
    syncSocketState: (_pid, kernelState) => kernelState,
    copyCanonicalMemfsToProcess: () => {},
    composeProcessPipeUsed: () => 0n,
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    mergePipeSlotsFromCanonical: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
    }),
    applyPipeControlState: () => {},
    copyEpollState: () => {},
    copySelectedEpollState: () => {},
    copySocketState: () => {},
    syncFileDataMutation: () => false,
    syncFileDataMutationByPath: (_sourceExports, path) => {
      filePaths.push(path);
      return true;
    },
    syncMetadataMutation: (_sourceExports, path) => {
      metadataPaths.push(path);
      return true;
    },
    fanoutCanonicalMemfs: () => {
      throw new Error("full fanout should be skipped");
    },
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(metadataPaths).toEqual(["/work/app.bin"]);
  expect(filePaths).toEqual(["/work/app.bin"]);
  expect(copyMemfsCalls).toEqual([]);
});

test("processFs uses targeted metadata sync for both rename paths", () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    opcode: 8,
    effects: 1 << 0,
    resumeResult: 0,
    absolutePath: "/work/old.txt",
    secondaryAbsolutePath: "/work/new.txt",
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const copyMemfsCalls: boolean[] = [];
  const syncedPaths: string[] = [];

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => new SharedArrayBuffer(65536),
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    canonicalProcesses: new Map(),
    seedEntries: new Map(),
    getCanonicalNextPid: () => 3,
    setCanonicalNextPid: () => {},
    publishRootInode: () => {},
    copyOfdState: () => {},
    copyMemfsState: (_sourceExports, _sourceState, _targetExports, _targetState, options) => {
      copyMemfsCalls.push(Boolean(options?.includeData));
    },
    rebuildPageCacheFromExports: () => {},
    rebuildPageCacheFromCanonical: () => {},
    syncPipeState: (_pid, kernelState) => kernelState,
    syncEpollState: (_pid, kernelState) => kernelState,
    syncSocketState: (_pid, kernelState) => kernelState,
    copyCanonicalMemfsToProcess: () => {},
    composeProcessPipeUsed: () => 0n,
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    mergePipeSlotsFromCanonical: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
    }),
    applyPipeControlState: () => {},
    copyEpollState: () => {},
    copySelectedEpollState: () => {},
    copySocketState: () => {},
    syncFileDataMutation: () => false,
    syncFileDataMutationByPath: () => false,
    syncMetadataMutation: (_sourceExports, path) => {
      syncedPaths.push(path);
      return true;
    },
    fanoutCanonicalMemfs: () => {
      throw new Error("full fanout should be skipped");
    },
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(syncedPaths).toEqual(["/work/old.txt", "/work/new.txt"]);
  expect(copyMemfsCalls).toEqual([]);
});
