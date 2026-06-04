// Kernel-worker tests in this file validate transitional runtime-owned bridge behavior.
import { expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import type { KernelWorkerOfdSlotSnapshot } from "../../../src/messages.js";
import { createKernelWorkerForkSharedStateSync } from "../../../src/kernel-worker/shared-state-sync/fork-shared.js";

type FakeKernelExports = KernelExports & {
  opLog: string[];
};

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0x100,
    memfsInodesBytes: 0x200,
    memfsFirstChildBytes: 0x300,
    memfsDirEntriesBytes: 0x400,
    memfsDataBytes: 0x500,
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

function makeKernelExports(label = "kernel", regularFiles: ReadonlyMap<number, Uint8Array> = new Map()): FakeKernelExports {
  let runtimeState = makeKernelState();
  const fdOfdIds = new Map<number, number>();
  const fdFlags = new Map<number, number>();
  const memfsFiles = new Map<number, Uint8Array>(
    [...regularFiles].map(([ino, data]) => [ino, data.slice()]),
  );
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const opLog: string[] = [];
  return {
    memory,
    abi_max_fds: () => 32,
    abi_fd_type_file: () => 1,
    kernel_heap_base: () => runtimeState.heapBase,
    kernel_heap_ptr: () => runtimeState.heapPtr,
    kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
    kernel_ofd_table_bytes: () => 32 * 32,
    kernel_ofd_fd_type: () => 0,
    kernel_memfs_ptr: () => runtimeState.memfsPtr,
    kernel_memfs_header_bytes: () => runtimeState.memfsHeaderBytes,
    kernel_memfs_inodes_bytes: () => runtimeState.memfsInodesBytes,
    kernel_memfs_first_child_bytes: () => runtimeState.memfsFirstChildBytes,
    kernel_memfs_dir_entries_bytes: () => runtimeState.memfsDirEntriesBytes,
    kernel_memfs_data_ptr: () => 0,
    kernel_memfs_data_bytes: () => runtimeState.memfsDataBytes,
    kernel_mem_state_ptr: () => runtimeState.memStatePtr,
    kernel_epoll_table_ptr: () => runtimeState.epollTablePtr,
    kernel_socket_table_bytes: () => 256,
    kernel_socket_table_ptr: () => runtimeState.socketTablePtr,
    kernel_socket_used: () => runtimeState.socketUsed,
    kernel_pipe_base: () => runtimeState.pipeBase,
    kernel_pipe_used: () => runtimeState.pipeUsed,
    kernel_net_buf_base: () => runtimeState.netBufBase,
    kernel_net_buf_pool_bytes: () => 256,
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
        memfsHeaderBytes: runtimeState.memfsHeaderBytes,
        memfsInodesBytes: runtimeState.memfsInodesBytes,
        memfsFirstChildBytes: runtimeState.memfsFirstChildBytes,
        memfsDirEntriesBytes: runtimeState.memfsDirEntriesBytes,
        memfsDataBytes: runtimeState.memfsDataBytes,
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
      opLog.push("attach");
    },
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdOfdIds.get(fd) ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdFlags.get(fd) ?? 0,
    process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number, flags: number) => {
      if (ofdId === 0) {
        fdOfdIds.delete(fd);
        fdFlags.delete(fd);
        return;
      }
      fdOfdIds.set(fd, ofdId);
      fdFlags.set(fd, flags);
    },
    memfs_inode_mode: (ino: number) => memfsFiles.has(ino) ? 0o100000 | 0o755 : 0,
    memfs_file_size: (ino: number) => BigInt(memfsFiles.get(ino)?.byteLength ?? 0),
    memfs_read_file_to: (ino: number, offset: bigint, bufPtr: number, bufLen: number) => {
      const data = memfsFiles.get(ino);
      if (!data) {
        return -1n;
      }
      const start = Number(offset);
      const chunk = data.subarray(start, Math.min(start + bufLen, data.byteLength));
      new Uint8Array(memory.buffer).set(chunk, bufPtr);
      return BigInt(chunk.byteLength);
    },
    memfs_truncate_file: (ino: number, size: bigint) => {
      const data = memfsFiles.get(ino);
      if (!data) {
        return -1n;
      }
      memfsFiles.set(ino, data.slice(0, Number(size)));
      opLog.push(`${label}:truncate:${ino}:${size.toString()}`);
      return size;
    },
    memfs_write_file_at: (ino: number, offset: bigint, dataPtr: number, dataLen: number) => {
      const data = memfsFiles.get(ino);
      if (!data) {
        return -1n;
      }
      const next = data.slice();
      next.set(new Uint8Array(memory.buffer, dataPtr, dataLen), Number(offset));
      memfsFiles.set(ino, next);
      opLog.push(`${label}:write:${ino}:${dataLen}`);
      return BigInt(dataLen);
    },
    opLog,
  } as unknown as FakeKernelExports;
}

test("syncForkSharedState copies memfs data when includeMemfs is enabled", () => {
  const canonicalExports = makeKernelExports();
  const sourceExports = makeKernelExports();
  const targetExports = makeKernelExports();
  const processKernels = new Map<number, KernelExports>([
    [1, sourceExports],
    [2, targetExports],
  ]);
  const memfsCopyModes: Array<boolean | undefined> = [];
  const canonicalFanoutModes: Array<boolean | undefined> = [];
  const restoreProcSelfExe = vi.fn();

  const sync = createKernelWorkerForkSharedStateSync({
    getCanonicalExports: () => canonicalExports,
    getCanonicalProcessVmPtr: () => 0x1234,
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    requireProcessKernel: (pid) => processKernels.get(pid)!,
    applySelectedOfdSnapshots: () => {},
    collectProcessHeldOfdRefcounts: () => new Int32Array(32),
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    collectProcessPrivatePipeSourceModes: () => new Map(),
    collectProcessPipeSlotModes: () => new Map(),
    collectProcessPrivatePipeSlots: () => new Set(),
    collectCanonicalManagedPipeSlots: () => [],
    copySelectedOfdState: () => {},
    copySelectedEpollState: () => {},
    clearSelectedOfdState: () => {},
    copyMemfsState: (_source, _sourceState, _target, _targetState, options) => {
      memfsCopyModes.push(options?.includeData);
    },
    clearMemfsState: () => {},
    reconcileCanonicalOfdRefcounts: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
      managedPipeSlots: [],
    }),
    collectPipeSlotsForOfdIds: () => [],
    copySelectedPipeSlots: () => {},
    clearSelectedPipeSlots: () => {},
    mergePipeSlotsFromProcess: () => {},
    mergePipeSlotsFromCanonical: () => {},
    applyPipeControlState: () => {},
    composeProcessPipeUsed: () => 0n,
    copyCanonicalMemfsToProcess: (_canonicalState, _target, _targetState, options) => {
      canonicalFanoutModes.push(options?.includeData);
    },
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe,
    copyEpollState: () => {},
    copySocketState: () => {},
  });

  sync.syncForkSharedState(
    1,
    0x1234,
    [2],
    [{ pid: 2, vmPtr: 0x5678, kernelState: makeKernelState() }],
    [7],
    [] as KernelWorkerOfdSlotSnapshot[],
    [],
    true,
    false,
    false,
    makeKernelState(),
  );

  expect(memfsCopyModes).toEqual([true]);
  expect(canonicalFanoutModes).toEqual([true]);
  expect(restoreProcSelfExe).toHaveBeenCalledTimes(1);
  expect(restoreProcSelfExe).toHaveBeenCalledWith(canonicalExports, null);
});

test("syncForkSharedState preserves published file data through page-cache restore", () => {
  const publishedData = new TextEncoder().encode("node");
  const canonicalExports = makeKernelExports("canonical", new Map([[3, publishedData]]));
  const sourceExports = makeKernelExports("source", new Map([[3, new Uint8Array(publishedData.byteLength)]]));
  const processKernels = new Map<number, KernelExports>([[1, sourceExports]]);
  const restorePublishedRegularFileDataFromPageCache = vi.fn(() => {
    canonicalExports.opLog.push("page-cache-restore");
  });

  const sync = createKernelWorkerForkSharedStateSync({
    getCanonicalExports: () => canonicalExports,
    getCanonicalProcessVmPtr: () => 0x1234,
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    requireProcessKernel: (pid) => processKernels.get(pid)!,
    applySelectedOfdSnapshots: () => {},
    collectProcessHeldOfdRefcounts: () => new Int32Array(32),
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    collectProcessPrivatePipeSourceModes: () => new Map(),
    collectProcessPipeSlotModes: () => new Map(),
    collectProcessPrivatePipeSlots: () => new Set(),
    collectCanonicalManagedPipeSlots: () => [],
    copySelectedOfdState: () => {},
    copySelectedEpollState: () => {},
    clearSelectedOfdState: () => {},
    copyMemfsState: () => {},
    clearMemfsState: () => {},
    restorePublishedRegularFileDataFromPageCache,
    reconcileCanonicalOfdRefcounts: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
      managedPipeSlots: [],
    }),
    collectPipeSlotsForOfdIds: () => [],
    copySelectedPipeSlots: () => {},
    clearSelectedPipeSlots: () => {},
    mergePipeSlotsFromProcess: () => {},
    mergePipeSlotsFromCanonical: () => {},
    applyPipeControlState: () => {},
    composeProcessPipeUsed: () => 0n,
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
    copyEpollState: () => {},
    copySocketState: () => {},
  });

  sync.syncForkSharedState(
    1,
    0x1234,
    [],
    [],
    [],
    [] as KernelWorkerOfdSlotSnapshot[],
    [],
    true,
    false,
    true,
    makeKernelState(),
  );

  expect(restorePublishedRegularFileDataFromPageCache).toHaveBeenCalledTimes(1);
  expect(canonicalExports.opLog).toContain("page-cache-restore");
  expect(canonicalExports.opLog).not.toContain("canonical:truncate:3:4");
  expect(canonicalExports.opLog).not.toContain("canonical:write:3:4");
  expect(sourceExports.opLog).not.toContain("source:truncate:3:4");
  expect(sourceExports.opLog).not.toContain("source:write:3:4");
});

test("syncForkSharedState restores preserved published file data to target process fanout", () => {
  const publishedData = new TextEncoder().encode("node");
  const canonicalExports = makeKernelExports("canonical", new Map([[3, publishedData]]));
  const sourceExports = makeKernelExports("source", new Map([[3, new Uint8Array(publishedData.byteLength)]]));
  const targetExports = makeKernelExports("target", new Map([[3, new Uint8Array(publishedData.byteLength)]]));
  const processKernels = new Map<number, KernelExports>([
    [1, sourceExports],
    [2, targetExports],
  ]);
  const restorePublishedRegularFileDataFromPageCache = vi.fn();

  const sync = createKernelWorkerForkSharedStateSync({
    getCanonicalExports: () => canonicalExports,
    getCanonicalProcessVmPtr: () => 0x1234,
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    requireProcessKernel: (pid) => processKernels.get(pid)!,
    applySelectedOfdSnapshots: () => {},
    collectProcessHeldOfdRefcounts: () => new Int32Array(32),
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    collectProcessPrivatePipeSourceModes: () => new Map(),
    collectProcessPipeSlotModes: () => new Map(),
    collectProcessPrivatePipeSlots: () => new Set(),
    collectCanonicalManagedPipeSlots: () => [],
    copySelectedOfdState: () => {},
    copySelectedEpollState: () => {},
    clearSelectedOfdState: () => {},
    copyMemfsState: () => {},
    clearMemfsState: () => {},
    restorePublishedRegularFileDataFromPageCache,
    reconcileCanonicalOfdRefcounts: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
      managedPipeSlots: [],
    }),
    collectPipeSlotsForOfdIds: () => [],
    copySelectedPipeSlots: () => {},
    clearSelectedPipeSlots: () => {},
    mergePipeSlotsFromProcess: () => {},
    mergePipeSlotsFromCanonical: () => {},
    applyPipeControlState: () => {},
    composeProcessPipeUsed: () => 0n,
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
    copyEpollState: () => {},
    copySocketState: () => {},
  });

  sync.syncForkSharedState(
    1,
    0x1234,
    [2],
    [{ pid: 2, vmPtr: 0x5678, kernelState: makeKernelState() }],
    [],
    [] as KernelWorkerOfdSlotSnapshot[],
    [],
    true,
    false,
    true,
    makeKernelState(),
  );

  expect(restorePublishedRegularFileDataFromPageCache).toHaveBeenCalledWith();
  expect(restorePublishedRegularFileDataFromPageCache).toHaveBeenCalledWith(targetExports);
});

test("syncForkSharedState filters private fd entries outside tracked OFDs", () => {
  const canonicalExports = makeKernelExports();
  const sourceExports = makeKernelExports();
  const processKernels = new Map<number, KernelExports>([[1, sourceExports]]);

  sourceExports.process_set_fd_entry(0x1234, 5, 7, 0);
  sourceExports.process_set_fd_entry(0x1234, 6, 8, 1);

  const sync = createKernelWorkerForkSharedStateSync({
    getCanonicalExports: () => canonicalExports,
    getCanonicalProcessVmPtr: () => 0x1234,
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    requireProcessKernel: (pid) => processKernels.get(pid)!,
    applySelectedOfdSnapshots: () => {},
    collectProcessHeldOfdRefcounts: () => {
      const refcounts = new Int32Array(32);
      refcounts[8] = 1;
      return refcounts;
    },
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    collectProcessPrivatePipeSourceModes: () => new Map(),
    collectProcessPipeSlotModes: () => new Map(),
    collectProcessPrivatePipeSlots: () => new Set(),
    collectCanonicalManagedPipeSlots: () => [],
    copySelectedOfdState: () => {},
    copySelectedEpollState: () => {},
    clearSelectedOfdState: () => {},
    copyMemfsState: () => {},
    clearMemfsState: () => {},
    reconcileCanonicalOfdRefcounts: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
      managedPipeSlots: [],
    }),
    collectPipeSlotsForOfdIds: () => [],
    copySelectedPipeSlots: () => {},
    clearSelectedPipeSlots: () => {},
    mergePipeSlotsFromProcess: () => {},
    mergePipeSlotsFromCanonical: () => {},
    applyPipeControlState: () => {},
    composeProcessPipeUsed: () => 0n,
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
    copyEpollState: () => {},
    copySocketState: () => {},
  });

  sync.syncForkSharedState(
    1,
    0x1234,
    [],
    [],
    [7, 8],
    [],
    [
      { fd: 5, ofdId: 10, fdFlags: 0 },
      { fd: 6, ofdId: 8, fdFlags: 1 },
      { fd: 7, ofdId: 11, fdFlags: 0 },
    ],
    false,
    false,
    false,
    makeKernelState(),
  );

  expect(sourceExports.process_fd_ofd_id(0x1234, 5)).toBe(0);
  expect(sourceExports.process_fd_ofd_id(0x1234, 6)).toBe(8);
  expect(sourceExports.process_fd_flags(0x1234, 6)).toBe(1);
  expect(sourceExports.process_fd_ofd_id(0x1234, 7)).toBe(0);
});

test("rehydrateForkSharedState copies memfs data when includeMemfs is enabled", () => {
  const canonicalExports = makeKernelExports();
  const targetExports = makeKernelExports();
  const processKernels = new Map<number, KernelExports>([[2, targetExports]]);
  const memfsCopyModes: Array<boolean | undefined> = [];

  const sync = createKernelWorkerForkSharedStateSync({
    getCanonicalExports: () => canonicalExports,
    getCanonicalProcessVmPtr: () => 0x1234,
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    requireProcessKernel: (pid) => processKernels.get(pid)!,
    applySelectedOfdSnapshots: () => {},
    collectProcessHeldOfdRefcounts: () => new Int32Array(32),
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    collectProcessPrivatePipeSourceModes: () => new Map(),
    collectProcessPipeSlotModes: () => new Map(),
    collectProcessPrivatePipeSlots: () => new Set(),
    collectCanonicalManagedPipeSlots: () => [],
    copySelectedOfdState: () => {},
    copySelectedEpollState: () => {},
    clearSelectedOfdState: () => {},
    copyMemfsState: (_source, _sourceState, _target, _targetState, options) => {
      memfsCopyModes.push(options?.includeData);
    },
    clearMemfsState: () => {},
    reconcileCanonicalOfdRefcounts: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
      managedPipeSlots: [],
    }),
    collectPipeSlotsForOfdIds: () => [],
    copySelectedPipeSlots: () => {},
    clearSelectedPipeSlots: () => {},
    mergePipeSlotsFromProcess: () => {},
    mergePipeSlotsFromCanonical: () => {},
    applyPipeControlState: () => {},
    composeProcessPipeUsed: () => 0n,
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
    copyEpollState: () => {},
    copySocketState: () => {},
  });

  sync.rehydrateForkSharedState(
    [2],
    [{ pid: 2, vmPtr: 0x5678, kernelState: makeKernelState() }],
    [7],
    [],
    true,
  );

  expect(memfsCopyModes).toEqual([true]);
});

test("rehydrateForkSharedState restores proc_self_exe from explicit targets even when memfs rehydrate is disabled", () => {
  const canonicalExports = makeKernelExports();
  const targetExports = makeKernelExports();
  const processKernels = new Map<number, KernelExports>([[2, targetExports]]);
  const restoreProcSelfExe = vi.fn();
  const copyMemfsState = vi.fn();
  const clearMemfsState = vi.fn();
  const procSelfExeTarget = new TextEncoder().encode("/usr/local/go/bin/go");

  const sync = createKernelWorkerForkSharedStateSync({
    getCanonicalExports: () => canonicalExports,
    getCanonicalProcessVmPtr: () => 0x1234,
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    requireProcessKernel: (pid) => processKernels.get(pid)!,
    applySelectedOfdSnapshots: () => {},
    collectProcessHeldOfdRefcounts: () => new Int32Array(32),
    collectProcessHeldEpollOfdIds: () => [],
    collectProcessHeldPipeSlots: () => new Set(),
    collectProcessPrivatePipeSourceModes: () => new Map(),
    collectProcessPipeSlotModes: () => new Map(),
    collectProcessPrivatePipeSlots: () => new Set(),
    collectCanonicalManagedPipeSlots: () => [],
    copySelectedOfdState: () => {},
    copySelectedEpollState: () => {},
    clearSelectedOfdState: () => {},
    copyMemfsState,
    clearMemfsState,
    reconcileCanonicalOfdRefcounts: () => {},
    recomputeCanonicalPipeControlState: () => ({
      kernelState: makeKernelState(),
      readOpen: new Int32Array(0),
      writeOpen: new Int32Array(0),
      managedPipeSlots: [],
    }),
    collectPipeSlotsForOfdIds: () => [],
    copySelectedPipeSlots: () => {},
    clearSelectedPipeSlots: () => {},
    mergePipeSlotsFromProcess: () => {},
    mergePipeSlotsFromCanonical: () => {},
    applyPipeControlState: () => {},
    composeProcessPipeUsed: () => 0n,
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe,
    copyEpollState: () => {},
    copySocketState: () => {},
  });

  sync.rehydrateForkSharedState(
    [2],
    [{ pid: 2, vmPtr: 0x5678, kernelState: makeKernelState() }],
    [],
    [{ pid: 2, bytes: procSelfExeTarget }],
    false,
  );

  expect(copyMemfsState).not.toHaveBeenCalled();
  expect(clearMemfsState).not.toHaveBeenCalled();
  expect(restoreProcSelfExe).toHaveBeenCalledWith(targetExports, procSelfExeTarget);
});
