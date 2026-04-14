import { expect, test } from "vite-plus/test";
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
  path?: string;
} = {}): FakeKernelExports {
  let runtimeState = makeKernelState();
  const opLog: string[] = [];
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const guestBase = 0x1000;
  const pathPtr = 0x2000;
  const pathBytes = options.path ? new TextEncoder().encode(options.path) : null;
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
    thread_pending_fs_target_ino: () => 0,
    thread_pending_fs_path_ptr: () => (pathBytes ? pathPtr : 0),
    thread_pending_fs_path_len: () => pathBytes?.length ?? 0,
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
