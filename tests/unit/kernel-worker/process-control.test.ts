// Kernel-worker tests in this file validate transitional runtime-owned bridge behavior.
import { expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { createKernelWorkerProcessControl } from "../../../src/kernel-worker/process-control.js";
import {
  DATA_REGION_OFFSET,
  publishInode,
  readResidentFileData,
} from "../../../src/fs/sab-page-cache.js";
import {
  FS_OP_READ,
  FS_OP_WRITE,
  FS_OP_MMAP,
  FS_OP_TRUNCATE,
  FS_REQ_FLAG_PWRITE,
  FS_RING_REQUEST_BUF_PTR_OFFSET,
  FS_RING_REQUEST_FD_OFFSET,
  FS_RING_REQUEST_FLAGS_OFFSET,
  FS_RING_REQUEST_LEN_OFFSET,
  FS_RING_REQUEST_OFFSET_OFFSET,
  FS_RING_REQUEST_OPCODE_OFFSET,
  FS_RING_REQUEST_VM_PTR_OFFSET,
  FS_RING_SUBMIT_HEAD_OFFSET,
  fsRingRequestEntryOffset,
} from "../../../src/layout/fs-ring.js";

type FakeKernelExports = KernelExports & {
  opLog: string[];
};

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0x1000,
    memfsInodesBytes: 0x2000,
    memfsFirstChildBytes: 0x1000,
    memfsDirEntriesBytes: 0x2000,
    memfsDataBytes: 0x4000,
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
  pathPtr?: bigint | number;
  absolutePath?: string;
  secondaryAbsolutePath?: string;
  fdEntries?: Array<{ fd: number; ofdId: number; fdFlags?: number }>;
  ofdSlots?: Array<{ ofdId: number; bytes: Uint8Array }>;
  mmapRequest?: {
    guestPtrHint: number;
    length: number;
    resultGuestPtr: number;
    bytes: Uint8Array;
    fd?: number;
    offset?: number;
    ino?: number;
    size?: number;
  };
  readRequest?: { fd: number; offset: number; length: number; ino: number; size: number; pwrite?: boolean };
  writeRequest?: { fd: number; offset: number; length: number; ino: number; size: number; pwrite?: boolean };
  truncateRequest?: { fd: number; size: number; ino: number; currentSize: number };
} = {}): FakeKernelExports {
  let runtimeState = makeKernelState();
  const opLog: string[] = [];
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const stackPointer = new WebAssembly.Global({ value: "i32", mutable: true }, 0);
  const maxFds = 32;
  const ofdSlotBytes = 32;
  const fdOfdIds = new Map((options.fdEntries ?? []).map((entry) => [entry.fd, entry.ofdId]));
  const fdFlags = new Map((options.fdEntries ?? []).map((entry) => [entry.fd, entry.fdFlags ?? 0]));
  const guestBase = 0x1000;
  const fsRingBase = 0x3000;
  const pathPtr = options.pathPtr ?? 0x2000;
  const pathBytes = options.path ? new TextEncoder().encode(options.path) : null;
  const absolutePathBytes = options.absolutePath ? new TextEncoder().encode(options.absolutePath) : null;
  const secondaryAbsolutePathBytes = options.secondaryAbsolutePath
    ? new TextEncoder().encode(options.secondaryAbsolutePath)
    : null;
  if (
    pathBytes
    && typeof pathPtr === "number"
    && guestBase + pathPtr + pathBytes.length <= memory.buffer.byteLength
  ) {
    new Uint8Array(memory.buffer).set(pathBytes, guestBase + pathPtr);
  }
  for (const ofdSlot of options.ofdSlots ?? []) {
    new Uint8Array(memory.buffer).set(
      ofdSlot.bytes,
      runtimeState.ofdTablePtr + ofdSlot.ofdId * ofdSlotBytes,
    );
  }
  if (options.mmapRequest) {
    const view = new DataView(memory.buffer);
    view.setUint32(fsRingBase + FS_RING_SUBMIT_HEAD_OFFSET, 1, true);
    const entry = fsRingRequestEntryOffset(fsRingBase, 0);
    view.setUint32(entry + FS_RING_REQUEST_OPCODE_OFFSET, FS_OP_MMAP, true);
    view.setInt32(entry + FS_RING_REQUEST_FD_OFFSET, options.mmapRequest.fd ?? 5, true);
    view.setBigUint64(entry + FS_RING_REQUEST_OFFSET_OFFSET, BigInt(options.mmapRequest.offset ?? 0), true);
    view.setBigUint64(entry + FS_RING_REQUEST_LEN_OFFSET, BigInt(options.mmapRequest.length), true);
    view.setBigUint64(entry + FS_RING_REQUEST_BUF_PTR_OFFSET, BigInt(options.mmapRequest.guestPtrHint), true);
    view.setUint32(entry + FS_RING_REQUEST_VM_PTR_OFFSET, 0x1234, true);
  }
  if (options.readRequest) {
    const view = new DataView(memory.buffer);
    view.setUint32(fsRingBase + FS_RING_SUBMIT_HEAD_OFFSET, 1, true);
    const entry = fsRingRequestEntryOffset(fsRingBase, 0);
    view.setUint32(entry + FS_RING_REQUEST_OPCODE_OFFSET, FS_OP_READ, true);
    view.setInt32(entry + FS_RING_REQUEST_FD_OFFSET, options.readRequest.fd, true);
    view.setBigUint64(entry + FS_RING_REQUEST_OFFSET_OFFSET, BigInt(options.readRequest.offset), true);
    view.setBigUint64(entry + FS_RING_REQUEST_LEN_OFFSET, BigInt(options.readRequest.length), true);
    view.setUint32(
      entry + FS_RING_REQUEST_FLAGS_OFFSET,
      options.readRequest.pwrite ? FS_REQ_FLAG_PWRITE : 0,
      true,
    );
    view.setUint32(entry + FS_RING_REQUEST_VM_PTR_OFFSET, 0x1234, true);
  }
  if (options.writeRequest) {
    const view = new DataView(memory.buffer);
    view.setUint32(fsRingBase + FS_RING_SUBMIT_HEAD_OFFSET, 1, true);
    const entry = fsRingRequestEntryOffset(fsRingBase, 0);
    view.setUint32(entry + FS_RING_REQUEST_OPCODE_OFFSET, FS_OP_WRITE, true);
    view.setInt32(entry + FS_RING_REQUEST_FD_OFFSET, options.writeRequest.fd, true);
    view.setBigUint64(entry + FS_RING_REQUEST_OFFSET_OFFSET, BigInt(options.writeRequest.offset), true);
    view.setBigUint64(entry + FS_RING_REQUEST_LEN_OFFSET, BigInt(options.writeRequest.length), true);
    view.setUint32(
      entry + FS_RING_REQUEST_FLAGS_OFFSET,
      options.writeRequest.pwrite ? FS_REQ_FLAG_PWRITE : 0,
      true,
    );
    view.setUint32(entry + FS_RING_REQUEST_VM_PTR_OFFSET, 0x1234, true);
  }
  if (options.truncateRequest) {
    const view = new DataView(memory.buffer);
    view.setUint32(fsRingBase + FS_RING_SUBMIT_HEAD_OFFSET, 1, true);
    const entry = fsRingRequestEntryOffset(fsRingBase, 0);
    view.setUint32(entry + FS_RING_REQUEST_OPCODE_OFFSET, FS_OP_TRUNCATE, true);
    view.setInt32(entry + FS_RING_REQUEST_FD_OFFSET, options.truncateRequest.fd, true);
    view.setBigUint64(entry + FS_RING_REQUEST_OFFSET_OFFSET, BigInt(options.truncateRequest.size), true);
    view.setUint32(entry + FS_RING_REQUEST_VM_PTR_OFFSET, 0x1234, true);
  }
  return {
    memory,
    __stack_pointer: stackPointer,
    abi_max_fds: () => maxFds,
    kernel_heap_base: () => runtimeState.heapBase,
    kernel_heap_ptr: () => runtimeState.heapPtr,
    kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
    kernel_ofd_table_bytes: () => maxFds * ofdSlotBytes,
    kernel_memfs_ptr: () => runtimeState.memfsPtr,
    kernel_memfs_header_bytes: () => runtimeState.memfsHeaderBytes,
    kernel_memfs_inodes_bytes: () => runtimeState.memfsInodesBytes,
    kernel_memfs_first_child_bytes: () => runtimeState.memfsFirstChildBytes,
    kernel_memfs_dir_entries_bytes: () => runtimeState.memfsDirEntriesBytes,
    kernel_memfs_data_bytes: () => runtimeState.memfsDataBytes,
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
    thread_fs_ring_base: () => (
      options.mmapRequest || options.readRequest || options.writeRequest || options.truncateRequest ? fsRingBase : 0
    ),
    thread_guest_base: () => guestBase,
    thread_process_fs: () => {
      opLog.push("process-fs");
      if (options.mmapRequest) {
        new Uint8Array(memory.buffer).set(
          options.mmapRequest.bytes,
          guestBase + options.mmapRequest.resultGuestPtr,
        );
      }
    },
    thread_resume_fs: () => {
      opLog.push("resume-fs");
      if (options.mmapRequest) {
        return BigInt(options.mmapRequest.resultGuestPtr);
      }
      return BigInt(options.resumeResult ?? 0);
    },
    process_remove_process_entry: (pid: number) => {
      opLog.push(`remove-process:${pid}`);
    },
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdOfdIds.get(fd) ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdFlags.get(fd) ?? 0,
    kernel_ofd_ino: () => options.readRequest?.ino
      ?? options.writeRequest?.ino
      ?? options.mmapRequest?.ino
      ?? options.truncateRequest?.ino
      ?? 0,
    kernel_ofd_offset: () => BigInt(options.readRequest?.offset ?? options.writeRequest?.offset ?? 0),
    memfs_file_size: () => options.readRequest?.size
      ?? options.writeRequest?.size
      ?? options.mmapRequest?.size
      ?? options.truncateRequest?.currentSize
      ?? 0,
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
    expect(copyCanonicalModes).toEqual([true]);
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

    expect(copyCanonicalModes).toEqual([true]);
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

    expect(copyCanonicalModes).toEqual([true]);
  } finally {
    instantiateSpy.mockRestore();
  }
});

test("processFs treats unlink as metadata-only canonical sync", async () => {
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
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
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

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(copyMemfsCalls).toEqual([false]);
  expect(fanoutCalls).toEqual([true]);
});

test("processFs demand-fills page-aligned source-backed read ranges before processing fs", async () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    resumeResult: 16,
    fdEntries: [{ fd: 5, ofdId: 7 }],
    readRequest: { fd: 5, offset: 32, length: 16, ino: 11, size: 128, pwrite: true },
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 8192);
  const backing = Uint8Array.from({ length: 128 }, (_value, index) => 0x40 + index);
  let backingReady = false;
  const ensureBackingSourceRangeForInode = vi.fn(async () => true);
  publishInode(sab, 11, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("source"),
    nextSibling: 0,
    fileObjectId: 11,
    generation: 1,
  });

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => sab,
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    canonicalProcesses: new Map(),
    seedEntries: new Map(),
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
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
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
    readBackingFileRangeByInode: (_ino, offset, length) => {
      if (!backingReady) {
        return null;
      }
      return backing.subarray(offset, offset + length);
    },
    ensureBackingSourceRangeForInode: async (...args) => {
      const result = await ensureBackingSourceRangeForInode(...args);
      backingReady = true;
      return result;
    },
    fanoutCanonicalMemfs: () => {},
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.result).toBe(16n);
  expect(ensureBackingSourceRangeForInode).toHaveBeenCalledWith(11, 0, 128, 128);
  expect(readResidentFileData(sab, 11, 32, 16)).toEqual(backing.subarray(32, 48));
  expect(processExports.opLog).toEqual(["process-fs", "resume-fs"]);
});

test("processFs demand-fills source-backed mmap ranges before processing fs", async () => {
  const canonicalExports = makeKernelExports();
  const mappedBytes = Uint8Array.from([0xaa, 0xbb, 0xcc, 0xdd]);
  const processExports = makeKernelExports({
    fdEntries: [{ fd: 5, ofdId: 7 }],
    mmapRequest: {
      fd: 5,
      offset: 4096,
      length: 2048,
      ino: 14,
      size: 8192,
      guestPtrHint: 0,
      resultGuestPtr: 0x8000,
      bytes: mappedBytes,
    },
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 16384);
  const backing = Uint8Array.from({ length: 8192 }, (_value, index) => index & 0xff);
  let backingReady = false;
  const ensureBackingSourceRangeForInode = vi.fn(async () => true);
  publishInode(sab, 14, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("mapped"),
    nextSibling: 0,
    fileObjectId: 14,
    generation: 1,
  });

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => sab,
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    canonicalProcesses: new Map(),
    seedEntries: new Map(),
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
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
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
    readBackingFileRangeByInode: (_ino, offset, length) => {
      if (!backingReady) {
        return null;
      }
      return backing.subarray(offset, offset + length);
    },
    ensureBackingSourceRangeForInode: async (...args) => {
      const result = await ensureBackingSourceRangeForInode(...args);
      backingReady = true;
      return result;
    },
    restorePublishedRegularFileDataFromPageCache: () => {
      processExports.opLog.push("restore-published-data");
    },
    fanoutCanonicalMemfs: () => {},
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.result).toBe(0x8000n);
  expect(ensureBackingSourceRangeForInode).toHaveBeenCalledWith(14, 4096, 4096, 8192);
  expect(readResidentFileData(sab, 14, 4096, 2048)).toEqual(backing.subarray(4096, 6144));
  expect(processExports.opLog).toEqual(["restore-published-data", "process-fs", "resume-fs"]);
});

test("processFs demand-fills source-backed append writes before processing fs", async () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    resumeResult: 4,
    fdEntries: [{ fd: 5, ofdId: 7 }],
    writeRequest: { fd: 5, offset: 8192, length: 4, ino: 15, size: 8192, pwrite: true },
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 16384);
  const backing = Uint8Array.from({ length: 8192 }, (_value, index) => index & 0xff);
  let backingReady = false;
  const ensureBackingSourceRangeForInode = vi.fn(async () => true);
  publishInode(sab, 15, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("append"),
    nextSibling: 0,
    fileObjectId: 15,
    generation: 1,
  });

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => sab,
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    canonicalProcesses: new Map(),
    seedEntries: new Map(),
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
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
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
    readBackingFileRangeByInode: (_ino, offset, length) => {
      if (!backingReady) {
        return null;
      }
      return backing.subarray(offset, offset + length);
    },
    ensureBackingSourceRangeForInode: async (...args) => {
      const result = await ensureBackingSourceRangeForInode(...args);
      backingReady = true;
      return result;
    },
    fanoutCanonicalMemfs: () => {},
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.result).toBe(4n);
  expect(ensureBackingSourceRangeForInode).toHaveBeenCalledWith(15, 4096, 4096, 8192);
  expect(readResidentFileData(sab, 15, 4096, 4096)).toEqual(backing.subarray(4096, 8192));
});

test("processFs demand-fills source-backed truncate boundary before processing fs", async () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    resumeResult: 0,
    fdEntries: [{ fd: 5, ofdId: 7 }],
    truncateRequest: { fd: 5, size: 5000, ino: 16, currentSize: 8192 },
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 16384);
  const backing = Uint8Array.from({ length: 8192 }, (_value, index) => index & 0xff);
  let backingReady = false;
  const ensureBackingSourceRangeForInode = vi.fn(async () => true);
  publishInode(sab, 16, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("truncate"),
    nextSibling: 0,
    fileObjectId: 16,
    generation: 1,
  });

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => sab,
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    canonicalProcesses: new Map(),
    seedEntries: new Map(),
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
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
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
    readBackingFileRangeByInode: (_ino, offset, length) => {
      if (!backingReady) {
        return null;
      }
      return backing.subarray(offset, offset + length);
    },
    ensureBackingSourceRangeForInode: async (...args) => {
      const result = await ensureBackingSourceRangeForInode(...args);
      backingReady = true;
      return result;
    },
    fanoutCanonicalMemfs: () => {},
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.result).toBe(0n);
  expect(ensureBackingSourceRangeForInode).toHaveBeenCalledWith(16, 4096, 4096, 8192);
  expect(readResidentFileData(sab, 16, 4096, 904)).toEqual(backing.subarray(4096, 5000));
});

test("processFs retries execve after publishing kernel-reported backing miss range", async () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({ resumeResult: 0 });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 8192);
  const backing = Uint8Array.from({ length: 8192 }, (_value, index) => 0x20 + (index & 0x3f));
  const clearMiss = vi.fn();
  let miss: { ino: number; offset: number; length: number; expectedSize: number } | null = {
    ino: 13,
    offset: 128,
    length: 16,
    expectedSize: backing.length,
  };

  publishInode(sab, 13, {
    mode: 0o100755,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("exec"),
    nextSibling: 0,
    fileObjectId: 13,
    generation: 1,
  });

  processExports.thread_process_fs = () => {
    processExports.opLog.push("process-fs");
  };
  processExports.thread_execve_backing_miss_ino = () => miss?.ino ?? 0;
  processExports.thread_execve_backing_miss_offset = () => BigInt(miss?.offset ?? 0);
  processExports.thread_execve_backing_miss_len = () => miss?.length ?? 0;
  processExports.thread_execve_backing_miss_expected_size = () => miss?.expectedSize ?? 0;
  processExports.thread_clear_execve_backing_miss = () => {
    clearMiss();
    miss = null;
  };

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => sab,
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    canonicalProcesses: new Map(),
    seedEntries: new Map(),
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
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
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
    readBackingFileRangeByInode: (_ino, offset, length) => backing.subarray(offset, offset + length),
    ensureBackingSourceRangeForInode: async () => true,
    fanoutCanonicalMemfs: () => {},
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.result).toBe(0n);
  expect(clearMiss).toHaveBeenCalledTimes(1);
  expect(readResidentFileData(sab, 13, 128, 16)).toEqual(backing.subarray(128, 144));
  expect(processExports.opLog).toEqual(["process-fs", "process-fs", "resume-fs"]);
});

test("processFs materializes source-backed files before processing writes", async () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    resumeResult: 7,
    fdEntries: [{ fd: 5, ofdId: 7 }],
    writeRequest: { fd: 5, offset: 32, length: 7, ino: 11, size: 128, pwrite: true },
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const ensureBackingSourceRangeForInode = vi.fn(async () => true);

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
    copyMemfsState: () => {},
    rebuildPageCacheFromExports: () => {},
    rebuildPageCacheFromCanonical: () => {},
    syncPipeState: (_pid, kernelState) => kernelState,
    syncEpollState: (_pid, kernelState) => kernelState,
    syncSocketState: (_pid, kernelState) => kernelState,
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
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
    syncFileDataMutation: () => true,
    syncFileDataMutationByPath: () => false,
    syncMetadataMutation: () => false,
    readBackingFileRangeByInode: () => null,
    ensureBackingSourceRangeForInode,
    fanoutCanonicalMemfs: () => {},
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.result).toBe(7n);
  expect(ensureBackingSourceRangeForInode).toHaveBeenCalledWith(11, 0, 128, 128);
  expect(processExports.opLog).toEqual(["process-fs", "resume-fs"]);
});

test("processFs returns fd and OFD snapshots after successful fs processing", async () => {
  const canonicalExports = makeKernelExports();
  const ofdBytes = Uint8Array.from({ length: 32 }, (_value, index) => index + 1);
  const processExports = makeKernelExports({
    resumeResult: 5,
    fdEntries: [{ fd: 5, ofdId: 7, fdFlags: 1 }],
    ofdSlots: [{ ofdId: 7, bytes: ofdBytes }],
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);

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
    copyMemfsState: () => {},
    rebuildPageCacheFromExports: () => {},
    rebuildPageCacheFromCanonical: () => {},
    syncPipeState: (_pid, kernelState) => kernelState,
    syncEpollState: (_pid, kernelState) => kernelState,
    syncSocketState: (_pid, kernelState) => kernelState,
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
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

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.result).toBe(5n);
  expect(result.fdEntries?.find((entry) => entry.fd === 5)).toEqual({
    fd: 5,
    ofdId: 7,
    fdFlags: 1,
  });
  expect(Array.from(result.ofdSlots?.[0]?.slot ?? [])).toEqual(Array.from(ofdBytes));
});

test("processFs returns mmap guest memory populated by kernel-worker fs processing", async () => {
  const canonicalExports = makeKernelExports();
  const mappedBytes = Uint8Array.from([0x7f, 0x45, 0x4c, 0x46, 0xaa, 0xbb, 0xcc, 0xdd]);
  const processExports = makeKernelExports({
    mmapRequest: {
      guestPtrHint: 0,
      resultGuestPtr: 0x8000,
      length: mappedBytes.length,
      bytes: mappedBytes,
    },
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const restorePublishedRegularFileDataFromPageCache = vi.fn((targetExports?: KernelExports) => {
    expect(targetExports).toBe(processExports);
    processExports.opLog.push("restore-published-data");
  });

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
    copyMemfsState: () => {},
    rebuildPageCacheFromExports: () => {},
    rebuildPageCacheFromCanonical: () => {},
    restorePublishedRegularFileDataFromPageCache,
    syncPipeState: (_pid, kernelState) => kernelState,
    syncEpollState: (_pid, kernelState) => kernelState,
    syncSocketState: (_pid, kernelState) => kernelState,
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
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

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(restorePublishedRegularFileDataFromPageCache).toHaveBeenCalledTimes(1);
  expect(processExports.opLog.indexOf("restore-published-data")).toBeLessThan(
    processExports.opLog.indexOf("process-fs"),
  );
  expect(result.result).toBe(0x8000n);
  expect(result.guestMemoryWrites).toHaveLength(1);
  expect(result.guestMemoryWrites?.[0]?.guestPtr).toBe(0x8000);
  expect(result.guestMemoryWrites?.[0]?.bytes.length).toBe(4096);
  expect(result.guestMemoryWrites?.[0]?.bytes.subarray(0, mappedBytes.length)).toEqual(mappedBytes);
});

test("processFs snapshots fd entries before metadata sync side effects", async () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    opcode: 3,
    effects: (1 << 0) | (1 << 1),
    resumeResult: 5,
    absolutePath: "/work/created.txt",
    fdEntries: [{ fd: 5, ofdId: 7, fdFlags: 1 }],
  });
  let fdTableVisible = true;
  const processFdOfdId = processExports.process_fd_ofd_id.bind(processExports);
  const processFdFlags = processExports.process_fd_flags.bind(processExports);
  processExports.process_fd_ofd_id = (vmPtr, fd) => (fdTableVisible ? processFdOfdId(vmPtr, fd) : 0);
  processExports.process_fd_flags = (vmPtr, fd) => (fdTableVisible ? processFdFlags(vmPtr, fd) : 0);
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);

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
    copyMemfsState: () => {},
    rebuildPageCacheFromExports: () => {},
    rebuildPageCacheFromCanonical: () => {},
    syncPipeState: (_pid, kernelState) => kernelState,
    syncEpollState: (_pid, kernelState) => kernelState,
    syncSocketState: (_pid, kernelState) => kernelState,
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
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
    syncMetadataMutation: () => {
      fdTableVisible = false;
      return true;
    },
    fanoutCanonicalMemfs: () => {},
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.result).toBe(5n);
  expect(result.fdEntries?.find((entry) => entry.fd === 5)).toEqual({
    fd: 5,
    ofdId: 7,
    fdFlags: 1,
  });
});

test("processFs uses targeted metadata sync for absolute mkdir paths", async () => {
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

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(syncedPaths).toEqual(["/work/cache"]);
  expect(copyMemfsCalls).toEqual([]);
});

test("processFs falls back to metadata-only sync for Memory64 path pointers outside JS safe integer range", async () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    opcode: 6,
    effects: 1 << 0,
    resumeResult: 0,
    path: "/work/cache",
    pathPtr: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const copyMemfsCalls: boolean[] = [];
  const syncedPaths: string[] = [];
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
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
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
      fanoutCalls.push(true);
    },
    fsEffectMemfsMutation: 1 << 0,
    fsEffectSharedOfdState: 1 << 1,
    fsEffectMemfsFileData: 1 << 2,
    fsOpOpenAt: 3,
    fsOpMkdir: 6,
    fsOpUnlink: 7,
  });

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(syncedPaths).toEqual([]);
  expect(copyMemfsCalls).toEqual([false]);
  expect(fanoutCalls).toEqual([true]);
});

test("processFs does not full-sync file data when dirty resident sync fails", async () => {
  const canonicalExports = makeKernelExports();
  const processExports = makeKernelExports({
    opcode: 2,
    effects: 1 << 2,
    resumeResult: 4096,
    targetIno: 9,
  });
  const processKernels = new Map<number, KernelExports>([[7, processExports]]);
  const copyMemfsCalls: boolean[] = [];
  const fanoutCalls: boolean[] = [];
  const restorePublishedRegularFileDataFromPageCache = vi.fn();

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
    restorePublishedRegularFileDataFromPageCache,
    syncPipeState: (_pid, kernelState) => kernelState,
    syncEpollState: (_pid, kernelState) => kernelState,
    syncSocketState: (_pid, kernelState) => kernelState,
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
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

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(copyMemfsCalls).toEqual([]);
  expect(restorePublishedRegularFileDataFromPageCache).not.toHaveBeenCalled();
  expect(fanoutCalls).toEqual([]);
});

test("processFs uses targeted metadata sync for absolute tree mutations without an opcode allowlist", async () => {
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

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(syncedPaths).toEqual(["/work/cache"]);
  expect(copyMemfsCalls).toEqual([]);
});

test("processFs uses targeted metadata sync for absolute unlink paths", async () => {
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

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(syncedPaths).toEqual(["/work/remove.txt"]);
  expect(copyMemfsCalls).toEqual([]);
});

test("processFs uses kernel-provided absolute path for relative mkdir requests", async () => {
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

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(syncedPaths).toEqual(["/work/cache"]);
  expect(copyMemfsCalls).toEqual([]);
});

test("processFs uses targeted file-data sync when target ino is unavailable but absolute path is known", async () => {
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

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(syncedPaths).toEqual(["/work/app.bin"]);
  expect(copyMemfsCalls).toEqual([]);
});

test("processFs uses targeted metadata and file-data sync for openat(O_CREAT|O_TRUNC)", async () => {
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

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(metadataPaths).toEqual(["/work/app.bin"]);
  expect(filePaths).toEqual(["/work/app.bin"]);
  expect(copyMemfsCalls).toEqual([]);
});

test("processFs uses targeted metadata sync for both rename paths", async () => {
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

  const result = await processControl.processFs(7, 0x1234, makeKernelState());

  expect(result.memfsMutated).toBe(true);
  expect(syncedPaths).toEqual(["/work/old.txt", "/work/new.txt"]);
  expect(copyMemfsCalls).toEqual([]);
});

test("unregisterProcess refreshes remaining pipe control state", () => {
  const canonicalExports = makeKernelExports();
  const parentExports = makeKernelExports();
  const childExports = makeKernelExports();
  const processKernels = new Map<number, KernelExports>([
    [1, parentExports],
    [4, childExports],
  ]);
  const lastProcessPipeSlotModes = new Map<number, Map<number, number>>([[4, new Map([[2, 1]])]]);
  const canonicalProcesses = new Map([
    [1, { pid: 1, ppid: 0, pgid: 1, sid: 1, didExec: true, vmPtr: 0x1111 }],
    [4, { pid: 4, ppid: 1, pgid: 1, sid: 1, didExec: true, vmPtr: 0x4444 }],
  ]);
  const readOpen = new Int32Array(4);
  const writeOpen = new Int32Array(4);
  readOpen[2] = 1;
  const canonicalState = {
    ...makeKernelState(),
    pipeUsed: 1n << 2n,
  };
  const applyCalls: Array<{ target: KernelExports; pipeUsed: bigint; managedPipeSlots: number[] }> = [];
  const composedCalls: Array<{ vmPtr: number; sharedPipeUsed: bigint }> = [];

  const processControl = createKernelWorkerProcessControl({
    getCanonicalExports: () => canonicalExports,
    setCanonicalExports: () => {},
    getWasmModule: () => null,
    setWasmModule: () => {},
    getSabPageCache: () => new SharedArrayBuffer(65536),
    setSabPageCache: () => {},
    processKernels,
    lastProcessPipeSlotModes,
    canonicalProcesses,
    seedEntries: new Map(),
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
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
    composeProcessPipeUsed: (_processExports, vmPtr, sharedPipeUsed) => {
      composedCalls.push({ vmPtr, sharedPipeUsed });
      return sharedPipeUsed | (1n << 9n);
    },
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    mergePipeSlotsFromCanonical: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: canonicalState,
      readOpen,
      writeOpen,
      managedPipeSlots: [2],
    }),
    applyPipeControlState: (targetExports, _targetState, _readOpen, _writeOpen, pipeUsed, managedPipeSlots) => {
      applyCalls.push({ target: targetExports, pipeUsed, managedPipeSlots: [...managedPipeSlots] });
    },
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

  processControl.unregisterProcess(4);

  expect(processKernels.has(4)).toBe(false);
  expect(lastProcessPipeSlotModes.has(4)).toBe(false);
  expect(canonicalProcesses.has(4)).toBe(false);
  expect(parentExports.opLog).toEqual(["remove-process:4"]);
  expect(composedCalls).toEqual([{ vmPtr: 0x1111, sharedPipeUsed: 1n << 2n }]);
  expect(applyCalls).toEqual([
    {
      target: parentExports,
      pipeUsed: (1n << 2n) | (1n << 9n),
      managedPipeSlots: [2],
    },
  ]);
});
