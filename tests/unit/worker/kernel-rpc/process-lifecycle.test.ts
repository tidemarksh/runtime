import { addKernelShareMember, addKernelShareSharedOfdId, kernelShareHasSharedOfdId, kernelShareMembers, kernelShareSharedOfdIdList, processKernelShareGroup, setProcessKernelShareGroup } from "../../../../src/worker/kernel-share.js";
import { expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../../src/abi.js";
import { createRuntimeStateBridge } from "../../../../src/bridge/runtime-state.js";
import { createWorkerKernelRpcProcessLifecycleHelpers } from "../../../../src/worker/kernel-rpc/process-lifecycle.js";
import { setProcessDebugCommand } from "../../../../src/worker/process-debug-state.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";
import { seedProcessKernelWorkerRequestFixture } from "../support/kernel-worker-request-fixture.js";
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 32,
    memfsInodesBytes: 16_384 * 32,
    memfsFirstChildBytes: 16_384 * 4,
    memfsDirEntriesBytes: 16_384 * 264,
    memfsDataBytes: 256 * 1024,
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

function makeHandle(
  overrides: Partial<ProcessHandle> = {},
): ProcessHandle & {
  recordedChildExits: Array<[number, number]>;
  publishedStates: KernelRuntimeState[];
} {
  const recordedChildExits: Array<[number, number]> = [];
  const baseState = makeKernelState();
  let attachedState = baseState;
  const handle = {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0x1234,
    guestMemoryPolicyLimitBytes: 0,
    guestBase: 0,
    worker: {} as Worker,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel: {
      ...makeSupervisorAbiStubs(),
      memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
      kernel_attach_runtime_state(
        heapBase: number,
        heapPtr: number,
        ofdTablePtr: number,
        memfsPtr: number,
        memfsHeaderBytes: number,
        memfsInodesBytes: number,
        memfsFirstChildBytes: number,
        memfsDirEntriesBytes: number,
        memfsDataBytes: number,
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
      ) {
        attachedState = {
          heapBase,
          heapPtr,
          ofdTablePtr,
          memfsPtr,
          memfsHeaderBytes,
          memfsInodesBytes,
          memfsFirstChildBytes,
          memfsDirEntriesBytes,
          memfsDataBytes,
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
      kernel_heap_base: () => attachedState.heapBase,
      kernel_heap_ptr: () => attachedState.heapPtr,
      kernel_ofd_table_ptr: () => attachedState.ofdTablePtr,
      kernel_memfs_ptr: () => attachedState.memfsPtr,
      kernel_memfs_header_bytes: () => attachedState.memfsHeaderBytes,
      kernel_memfs_inodes_bytes: () => attachedState.memfsInodesBytes,
      kernel_memfs_first_child_bytes: () => attachedState.memfsFirstChildBytes,
      kernel_memfs_dir_entries_bytes: () => attachedState.memfsDirEntriesBytes,
      kernel_memfs_data_bytes: () => attachedState.memfsDataBytes,
      kernel_mem_state_ptr: () => attachedState.memStatePtr,
      kernel_epoll_table_ptr: () => attachedState.epollTablePtr,
      kernel_socket_table_ptr: () => attachedState.socketTablePtr,
      kernel_socket_used: () => attachedState.socketUsed,
      kernel_pipe_base: () => attachedState.pipeBase,
      kernel_pipe_used: () => attachedState.pipeUsed,
      kernel_net_buf_base: () => attachedState.netBufBase,
      kernel_net_buf_used: () => attachedState.netBufUsed,
      kernel_sig_slots_ptr: () => attachedState.sigSlotsPtr,
      kernel_process_pending_ptr: () => attachedState.processPendingPtr,
      kernel_process_runtime_state_ptr: () => attachedState.processRuntimeStatePtr,
      kernel_time_state_ptr: () => attachedState.timeStatePtr,
      abi_max_fds: () => 16,
      abi_max_pipes: () => 16,
      abi_fd_type_pipe: () => 6,
      process_fd_ofd_id: () => 0,
      kernel_ofd_fd_type: () => 0,
      kernel_ofd_ino: () => 0,
      kernel_ofd_host_fd: () => 0,
      process_record_child_exit(childPid: number, waitStatus: number) {
        recordedChildExits.push([childPid, waitStatus]);
      },
      process_child_ppid: () => 1,
      process_child_pgid: () => 1,
      process_child_sid: () => 1,
      process_next_pid: () => 8,
    } as unknown as KernelExports,
    kernelState: baseState,
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    stdioOwner: null,
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set<number>(),
    },
    stdinQueue: [],
    outputBacklog: [],
    runtimeBridge: createRuntimeStateBridge(),
    sabPageCache: null,
    workers: new Set(),
    vforkReadyWorkers: [],
    runningWorker: null,
    hostBlockedWorkers: new Map(),
    vforkSnapshot: null,
    vforkWorker: null,
    workerDebugLog: [],
    recordedChildExits,
    publishedStates: [],
    ...overrides,
  } as unknown as ProcessHandle & {
    recordedChildExits: Array<[number, number]>;
    publishedStates: KernelRuntimeState[];
  };
  seedProcessKernelWorkerRequestFixture(handle);
  setProcessDebugCommand(handle, "test");
  addKernelShareMember(handle);
  return handle;
}

test("recordChildExitViaKernelWorker returns parent wake effects after draining output", async () => {
  const order: string[] = [];
  const baseState = makeKernelState();
  const syncedState = {
    ...baseState,
    heapPtr: baseState.heapPtr + 0x80,
  };
  const publishedStates: KernelRuntimeState[] = [];
  const handle = makeHandle({
    kernelWorkerRequest: vi.fn(async () => ({
      type: "child-exit-recorded",
      requestId: 0,
      kernelState: syncedState,
    })),
  });

  const originalKernelState = handle.kernelState;
  const helpers = createWorkerKernelRpcProcessLifecycleHelpers({
    processHandles: new Map([[handle.pid, handle]]),
    threadWorkerStates: new WeakMap(),
    drainProcessOutput(parent) {
      order.push(`drain:${parent.pid}`);
      return true;
    },
    enqueueWorker() {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    notifyHostEffectProgress(parent) {
      order.push(`progress:${parent.pid}`);
    },
    requestPipeStateSync: async (parent, wakeWaiters = false) => {
      order.push(`pipe-sync:${parent.pid}:${wakeWaiters ? "wake" : "sync"}`);
      parent.kernelState = syncedState;
    },
    clearVforkSnapshot() {},
  });

  const outcome = await helpers.recordChildExitViaKernelWorker(handle, 7, 0x0300);

  expect(order).toEqual([
    "drain:1",
    "drain:1",
    "pipe-sync:1:wake",
    "progress:1",
  ]);
  expect(outcome.effects).toBeUndefined();
  expect(handle.recordedChildExits).toEqual([[7, 0x0300]]);
  expect(handle.kernelState.heapPtr).toBe(syncedState.heapPtr);
  expect(originalKernelState.heapPtr).toBe(baseState.heapPtr);
});

test("syncProcessLifecycleViaKernelWorker rewrites provisional child pid after state attach", async () => {
  const baseState = makeKernelState();
  const syncedState = {
    ...baseState,
    heapPtr: baseState.heapPtr + 0x80,
  };
  let currentPid = 3;
  const rewriteChildPid = vi.fn((
    _vmPtr: number,
    oldPid: number,
    newPid: number,
    _nextPid: number,
  ) => {
    if (currentPid === oldPid) {
      currentPid = newPid;
    }
  });
  const handle = makeHandle({
    pid: 3,
    kernelWorkerRequest: vi.fn(async () => ({
      type: "process-lifecycle-synced",
      requestId: 0,
      kernelState: syncedState,
    })),
  });
  handle.kernel.process_current_pid = vi.fn(() => currentPid);
  handle.kernel.process_rewrite_child_pid = rewriteChildPid;
  const originalAttach = handle.kernel.kernel_attach_runtime_state.bind(handle.kernel);
  let clobberPidOnSyncedAttach = true;
  handle.kernel.kernel_attach_runtime_state = vi.fn((...args: Parameters<typeof handle.kernel.kernel_attach_runtime_state>) => {
    originalAttach(...args);
    if (clobberPidOnSyncedAttach && args[1] === syncedState.heapPtr) {
      clobberPidOnSyncedAttach = false;
      currentPid = 87703;
    }
  });

  const helpers = createWorkerKernelRpcProcessLifecycleHelpers({
    processHandles: new Map([[handle.pid, handle]]),
    threadWorkerStates: new WeakMap(),
    drainProcessOutput() {
      return false;
    },
    enqueueWorker() {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    notifyHostEffectProgress() {},
    requestPipeStateSync: async () => {},
    clearVforkSnapshot() {},
  });

  await helpers.syncProcessLifecycleViaKernelWorker(handle, 0x1234);

  expect(rewriteChildPid).toHaveBeenCalledWith(0x1234, 87703, 3, 4);
  expect(currentPid).toBe(3);
  expect(handle.kernelState.heapPtr).toBe(syncedState.heapPtr);
});

test("syncProcessLifecycleViaKernelWorker preserves an active vfork child pid after state attach", async () => {
  const baseState = makeKernelState();
  const syncedState = {
    ...baseState,
    heapPtr: baseState.heapPtr + 0x80,
  };
  let currentPid = 1;
  const rewriteChildPid = vi.fn();
  const handle = makeHandle({
    pid: 1,
    kernelWorkerRequest: vi.fn(async () => ({
      type: "process-lifecycle-synced",
      requestId: 0,
      kernelState: syncedState,
    })),
  });
  handle.kernel.process_current_pid = vi.fn(() => currentPid);
  handle.kernel.process_rewrite_child_pid = rewriteChildPid;
  handle.kernel.thread_vfork_child_pid = vi.fn(() => 4);
  const originalAttach = handle.kernel.kernel_attach_runtime_state.bind(handle.kernel);
  let exposeVforkChildOnSyncedAttach = true;
  handle.kernel.kernel_attach_runtime_state = vi.fn((...args: Parameters<typeof handle.kernel.kernel_attach_runtime_state>) => {
    originalAttach(...args);
    if (exposeVforkChildOnSyncedAttach && args[1] === syncedState.heapPtr) {
      exposeVforkChildOnSyncedAttach = false;
      currentPid = 4;
    }
  });

  const helpers = createWorkerKernelRpcProcessLifecycleHelpers({
    processHandles: new Map([[handle.pid, handle]]),
    threadWorkerStates: new WeakMap(),
    drainProcessOutput() {
      return false;
    },
    enqueueWorker() {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    notifyHostEffectProgress() {},
    requestPipeStateSync: async () => {},
    clearVforkSnapshot() {},
  });

  await helpers.syncProcessLifecycleViaKernelWorker(handle, 0x1234);

  expect(rewriteChildPid).not.toHaveBeenCalled();
  expect(currentPid).toBe(4);
  expect(handle.kernelState.heapPtr).toBe(syncedState.heapPtr);
});
