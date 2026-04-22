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

function makeKernelExports(): FakeKernelExports {
  let runtimeState = makeKernelState();
  const opLog: string[] = [];
  return {
    memory: new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true }),
    abi_max_fds: () => 32,
    kernel_heap_base: () => runtimeState.heapBase,
    kernel_heap_ptr: () => runtimeState.heapPtr,
    kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
    kernel_memfs_ptr: () => runtimeState.memfsPtr,
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
    opLog,
  } as unknown as FakeKernelExports;
}

test("syncForkSharedState copies memfs metadata-only when includeMemfs is enabled", () => {
  const canonicalExports = makeKernelExports();
  const sourceExports = makeKernelExports();
  const targetExports = makeKernelExports();
  const processKernels = new Map<number, KernelExports>([
    [1, sourceExports],
    [2, targetExports],
  ]);
  const memfsCopyModes: Array<boolean | undefined> = [];
  const canonicalFanoutModes: Array<boolean | undefined> = [];

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
    restoreProcSelfExe: () => {},
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
    true,
    false,
    makeKernelState(),
  );

  expect(memfsCopyModes).toEqual([false]);
  expect(canonicalFanoutModes).toEqual([false]);
});

test("rehydrateForkSharedState copies memfs metadata-only when includeMemfs is enabled", () => {
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

  expect(memfsCopyModes).toEqual([false]);
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
