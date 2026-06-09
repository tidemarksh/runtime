import { expect, test, vi } from "vite-plus/test";
import { registerForkSharedSyncPromise } from "../../../../src/worker/fork-shared-sync-debug.js";
import {
  adoptKernelState,
  syncBlockedForkSharedStateViaKernelWorker,
  syncForkSharedStateViaKernelWorker,
} from "../../../../src/worker/sync.js";
import type { KernelRuntimeState } from "../../../../src/abi.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

const FD_TYPE_NONE = 0;
const FD_TYPE_FILE = 1;
const FD_TYPE_PIPE = 6;
const FD_TYPE_SOCKET = 8;
const PIPE_SLOT_BYTES = 32 + 65_536;
const OFD_SLOT_BYTES = 32;
const PROCESS_FD_ENTRY_BYTES = 8;

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0x20,
    memfsInodesBytes: 0x4000,
    memfsFirstChildBytes: 0x800,
    memfsDirEntriesBytes: 0x2000,
    memfsDataBytes: 0x400000,
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

function seedOfd(
  memory: WebAssembly.Memory,
  state: KernelRuntimeState,
  ofdId: number,
  type: number,
  ino: number,
  hostFd: number,
): void {
  const view = new DataView(memory.buffer);
  const base = state.ofdTablePtr + ofdId * OFD_SLOT_BYTES;
  view.setInt32(base, type, true);
  view.setInt32(base + 4, hostFd, true);
  view.setUint32(base + 8, hostFd >= 0 ? 0 : 1, true);
  view.setUint32(base + 12, 1, true);
  view.setBigInt64(base + 16, 0n, true);
  view.setUint32(base + 24, ino, true);
}

function seedPipeSlot(
  memory: WebAssembly.Memory,
  state: KernelRuntimeState,
  pipeSlot: number,
  readOpen: number,
  writeOpen: number,
): void {
  const view = new DataView(memory.buffer);
  const base = state.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
  view.setInt32(base + 8, readOpen, true);
  view.setInt32(base + 12, writeOpen, true);
  state.pipeUsed |= 1n << BigInt(pipeSlot);
}

function makeKernel(
  memory: WebAssembly.Memory,
  state: KernelRuntimeState,
  fdMap: Map<number, number>,
) {
  const fdEntryBase = state.memStatePtr;
  const readOfdInt32 = (ofdId: number, offset: number): number =>
    new DataView(memory.buffer).getInt32(state.ofdTablePtr + ofdId * OFD_SLOT_BYTES + offset, true);
  const readOfdUint32 = (ofdId: number, offset: number): number =>
    new DataView(memory.buffer).getUint32(state.ofdTablePtr + ofdId * OFD_SLOT_BYTES + offset, true);
  const readOfdBigInt64 = (ofdId: number, offset: number): bigint =>
    new DataView(memory.buffer).getBigInt64(state.ofdTablePtr + ofdId * OFD_SLOT_BYTES + offset, true);

  return {
    memory,
    abi_max_fds: () => 32,
    abi_max_threads: () => 4,
    abi_max_pipes: () => 32,
    abi_fd_type_none: () => FD_TYPE_NONE,
    abi_fd_type_file: () => FD_TYPE_FILE,
    abi_fd_type_pipe: () => FD_TYPE_PIPE,
    abi_fd_type_socket: () => FD_TYPE_SOCKET,
    kernel_heap_base: () => state.heapBase,
    kernel_heap_ptr: () => state.heapPtr,
    kernel_ofd_table_ptr: () => state.ofdTablePtr,
    kernel_ofd_table_bytes: () => 32 * OFD_SLOT_BYTES,
    kernel_memfs_ptr: () => state.memfsPtr,
    kernel_memfs_header_bytes: () => state.memfsHeaderBytes,
    kernel_memfs_inodes_bytes: () => state.memfsInodesBytes,
    kernel_memfs_first_child_bytes: () => state.memfsFirstChildBytes,
    kernel_memfs_dir_entries_bytes: () => state.memfsDirEntriesBytes,
    kernel_memfs_data_bytes: () => state.memfsDataBytes,
    kernel_mem_state_ptr: () => state.memStatePtr,
    kernel_epoll_table_ptr: () => state.epollTablePtr,
    kernel_socket_table_ptr: () => state.socketTablePtr,
    kernel_socket_table_bytes: () => 32 * 128,
    kernel_socket_used: () => state.socketUsed,
    kernel_pipe_base: () => state.pipeBase,
    kernel_pipe_used: () => state.pipeUsed,
    kernel_pipe_region_bytes: () => 32 * PIPE_SLOT_BYTES,
    kernel_net_buf_base: () => state.netBufBase,
    kernel_net_buf_pool_bytes: () => 32 * (16 + 8192),
    kernel_net_buf_used: () => state.netBufUsed,
    kernel_sig_slots_ptr: () => state.sigSlotsPtr,
    kernel_process_pending_ptr: () => state.processPendingPtr,
    kernel_process_runtime_state_ptr: () => state.processRuntimeStatePtr,
    kernel_time_state_ptr: () => state.timeStatePtr,
    kernel_attach_runtime_state: (
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
      pipeUsedLow: number,
      pipeUsedHigh: number,
      netBufBase: number,
      netBufUsedLow: number,
      netBufUsedHigh: number,
      sigSlotsPtr: number,
      processPendingPtr: number,
      processRuntimeStatePtr: number,
      timeStatePtr: number,
    ) => {
      Object.assign(state, {
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
        pipeUsed: BigInt(pipeUsedLow >>> 0) | (BigInt(pipeUsedHigh >>> 0) << 32n),
        netBufBase,
        netBufUsed: BigInt(netBufUsedLow >>> 0) | (BigInt(netBufUsedHigh >>> 0) << 32n),
        sigSlotsPtr,
        processPendingPtr,
        processRuntimeStatePtr,
        timeStatePtr,
      });
    },
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
    process_fd_flags: () => 0,
    process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
      if (ofdId > 0) {
        fdMap.set(fd, ofdId);
      } else {
        fdMap.delete(fd);
      }
    },
    process_fd_table_ptr: () => fdEntryBase,
    process_fd_entry_bytes: () => PROCESS_FD_ENTRY_BYTES,
    process_thread_vm_ptr: (_vmPtr: number, slot: number) => (slot === 0 ? 0x1234 : 0),
    thread_process_ptr: () => 0x1000,
    process_state_bytes: () => 256,
    process_stdin_pipe: () => 0,
    process_stdout_pipe: () => 0,
    process_stderr_pipe: () => 0,
    thread_blocked_ofd_id: () => 0,
    kernel_ofd_fd_type: (ofdId: number) => readOfdInt32(ofdId, 0),
    kernel_ofd_host_fd: (ofdId: number) => readOfdInt32(ofdId, 4),
    kernel_ofd_status_flags: (ofdId: number) => readOfdUint32(ofdId, 8),
    kernel_ofd_refcount: (ofdId: number) => readOfdUint32(ofdId, 12),
    kernel_ofd_offset: (ofdId: number) => readOfdBigInt64(ofdId, 16),
    kernel_ofd_ino: (ofdId: number) => readOfdUint32(ofdId, 24),
  };
}

function makeHandle(pid: number, fdMap: Map<number, number>): ProcessHandle {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernelState = makeKernelState();
  const handle = {
    runtimeId: 1,
    pid,
    parentPid: null,
    vmPtr: 0x1234,
    guestMemoryPolicyLimitBytes: 0,
    maxMemoryPages: 64,
    forkMaxMemoryPages: 64,
    guestBase: 0,
    worker: {} as Worker,
    memory,
    wasmModule: {} as WebAssembly.Module,
    kernel: makeKernel(memory, kernelState, fdMap),
    kernelState,
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    ttyPtySlot: 0,
    hostStdio: { stdinPipe: 0, stdoutPipe: 0, stderrPipe: 0, refCount: 1 },
    stdioOwner: null,
    kernelShare: {
      members: new Set<ProcessHandle>(),
      sharedOfdIds: new Set<number>(),
      ofdSlotSnapshots: new Map<number, Uint8Array>(),
      forkSharedSyncPromise: null,
    },
    stdinQueue: [],
    stdinFlushPromise: null,
    outputBacklog: [],
    debugCommand: "",
    runtimeBridge: {} as never,
    sabPageCache: null,
    kernelWorkerRequest: vi.fn(async (msg) => {
      if (msg.type === "rehydrate-fork-shared-state") {
        return {
          type: "fork-shared-state-rehydrated",
          requestId: msg.requestId,
        };
      }
      return {
        type: "fork-shared-state-synced",
        requestId: msg.requestId,
        kernelState: msg.kernelState,
      };
    }),
    networkConnections: new Map(),
    networkConnectionOfds: new Map(),
    networkOfdSlotWatermarks: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkSocketSlotWatermarks: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    networkRecvPayloadWatermarks: new Map(),
    workers: new Set(),
    readyWorkers: [],
    runningWorker: null,
    blockedWorkers: new Map([[{} as Worker, {
      vmPtr: 0x1234,
      status: 0,
      retryTimer: null,
      timeoutExpired: false,
      timeoutDeadlineMs: null,
      sleepDeadlineTicks: null,
      rpcPending: false,
      rpcGeneration: 0,
      skipNextNetworkPump: false,
      pipeSyncedBeforePassive: false,
      pipeChildExitResumeKey: null,
      pipeEofChildExitDeferrals: 0,
    }]]),
    pendingChildExitRecords: 0,
    pendingChildSpawnRecords: 0,
    childExitSyncRecords: new Map(),
    pipeSyncPromise: null,
    pipeSyncQueued: false,
    pipeWaiterWakeQueued: false,
    pendingPipeSlotSnapshots: [],
    epollSyncPromise: null,
    epollSyncQueued: false,
    epollWaiterWakeQueued: false,
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    regularFileOfdOffsets: new Map(),
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
  } as ProcessHandle;
  handle.kernelShare.members.add(handle);
  return handle;
}

test("adoptKernelState preserves canonical pointer fields while keeping heapPtr monotonic", () => {
  const current = {
    heapBase: 0x200000,
    heapPtr: 0x210080,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0x20,
    memfsInodesBytes: 0x4000,
    memfsFirstChildBytes: 0x800,
    memfsDirEntriesBytes: 0x2000,
    memfsDataBytes: 0x400000,
    memStatePtr: 0x240000,
    epollTablePtr: 0x250000,
    socketTablePtr: 0x260000,
    socketUsed: 1,
    pipeBase: 0x270000,
    pipeUsed: 0n,
    netBufBase: 0x280000,
    netBufUsed: 0n,
    sigSlotsPtr: 0xc00000,
    processPendingPtr: 0xc10000,
    processRuntimeStatePtr: 0xc20000,
    timeStatePtr: 0xc30000,
  };
  const incoming = {
    ...current,
    heapBase: 0x300000,
    heapPtr: 0x210040,
    ofdTablePtr: 0x320000,
    memfsPtr: 0x330000,
    memfsHeaderBytes: 0x40,
    memfsInodesBytes: 0x8000,
    memfsFirstChildBytes: 0x1000,
    memfsDirEntriesBytes: 0x4000,
    memfsDataBytes: 0x800000,
    memStatePtr: 0x340000,
    epollTablePtr: 0x350000,
    socketTablePtr: 0x360000,
    socketUsed: 3,
    pipeBase: 0x370000,
    pipeUsed: 5n,
    netBufBase: 0x380000,
    netBufUsed: 7n,
    sigSlotsPtr: 0xd00000,
    processPendingPtr: 0xd10000,
    processRuntimeStatePtr: 0xd20000,
    timeStatePtr: 0xd30000,
  };

  expect(adoptKernelState(current, incoming)).toEqual({
    ...current,
    heapBase: incoming.heapBase,
    heapPtr: current.heapPtr,
    socketUsed: incoming.socketUsed,
    pipeUsed: incoming.pipeUsed,
    netBufUsed: incoming.netBufUsed,
  });
});

test("fork-shared sync keeps a pipe reader target that also owns runtime network state", async () => {
  const child = makeHandle(7, new Map([[5, 8]]));
  const parent = makeHandle(3, new Map([[4, 7], [9, 19]]));
  child.kernelShare.members.add(parent);
  parent.kernelShare = child.kernelShare;
  child.kernelShare.sharedOfdIds.add(8);

  seedOfd(child.memory, child.kernelState, 8, FD_TYPE_PIPE, 6, -11);
  seedOfd(parent.memory, parent.kernelState, 7, FD_TYPE_PIPE, 6, 10);
  seedOfd(parent.memory, parent.kernelState, 19, FD_TYPE_SOCKET, 19, 99);
  seedPipeSlot(child.memory, child.kernelState, 6, 1, 1);
  seedPipeSlot(parent.memory, parent.kernelState, 6, 1, 1);
  parent.networkConnections.set(9, {
    close: vi.fn(async () => {}),
    read: vi.fn(),
    write: vi.fn(),
  } as never);

  await syncForkSharedStateViaKernelWorker(child, [parent], {
    cause: { kind: "syscall", nr: -1n, name: "test-fork-shared-sync" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: true,
    sourceVmPtr: child.vmPtr,
  });

  expect(child.kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "sync-fork-shared-state",
    targetPids: [3],
  }));
});

test("fork-shared delta skips regular file OFDs held only by a peer", async () => {
  const child = makeHandle(7, new Map());
  const parent = makeHandle(3, new Map([[4, 12]]));
  child.kernelShare.members.add(parent);
  parent.kernelShare = child.kernelShare;
  child.kernelShare.sharedOfdIds.add(12);

  seedOfd(child.memory, child.kernelState, 12, FD_TYPE_FILE, 44, 21);
  seedOfd(parent.memory, parent.kernelState, 12, FD_TYPE_FILE, 44, 21);

  await syncForkSharedStateViaKernelWorker(child, [parent], {
    cause: { kind: "syscall", nr: -1n, name: "test-fork-shared-regular-file-delta" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: false,
    sourceVmPtr: child.vmPtr,
  });

  expect(child.kernelWorkerRequest).not.toHaveBeenCalled();
});

test("fork-shared delta prunes stale shared OFDs when no sync is needed", async () => {
  const child = makeHandle(7, new Map());
  const parent = makeHandle(3, new Map());
  child.kernelShare.members.add(parent);
  parent.kernelShare = child.kernelShare;
  child.kernelShare.sharedOfdIds.add(12);

  seedOfd(child.memory, child.kernelState, 12, FD_TYPE_PIPE, 4, -1);
  seedOfd(parent.memory, parent.kernelState, 12, FD_TYPE_PIPE, 4, -1);

  await syncForkSharedStateViaKernelWorker(child, [parent], {
    cause: { kind: "syscall", nr: -1n, name: "test-fork-shared-stale-prune" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: true,
    sourceVmPtr: child.vmPtr,
  });

  expect(child.kernelShare.sharedOfdIds.has(12)).toBe(false);
  expect(child.kernelWorkerRequest).not.toHaveBeenCalled();
});

test("fork-shared tracking prunes regular file OFDs without a peer", async () => {
  const child = makeHandle(7, new Map([[3, 12]]));
  const parent = makeHandle(3, new Map());
  child.kernelShare.members.add(parent);
  parent.kernelShare = child.kernelShare;
  child.kernelShare.sharedOfdIds.add(12);

  seedOfd(child.memory, child.kernelState, 12, FD_TYPE_FILE, 44, 21);

  await syncForkSharedStateViaKernelWorker(child, [parent], {
    cause: { kind: "syscall", nr: -1n, name: "test-fork-shared-unpeered-regular-file-prune" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: true,
    sourceVmPtr: child.vmPtr,
  });

  expect(child.kernelShare.sharedOfdIds.has(12)).toBe(false);
  expect(child.kernelWorkerRequest).not.toHaveBeenCalled();
});

test("blocked fork-shared sync clears a stale active sync before waiting", async () => {
  const handle = makeHandle(7, new Map());
  const staleSync = new Promise<void>(() => {});
  registerForkSharedSyncPromise(staleSync, {
    ownerPid: handle.pid,
    sourceVmPtr: 0xdead,
    reason: "unit-stale-sync",
    targetPids: [],
  });
  handle.kernel.thread_process_ptr = vi.fn((vmPtr: number) => (vmPtr === 0xdead ? 0 : 0x1000));
  handle.forkSharedSyncPromise = staleSync;
  handle.kernelShare.forkSharedSyncPromise = staleSync;

  const result = await Promise.race([
    syncBlockedForkSharedStateViaKernelWorker(
      handle,
      handle.vmPtr,
      { kind: "blocked-rpc", status: 21 },
    ).then(() => "done"),
    new Promise<"timed-out">((resolve) => setTimeout(() => resolve("timed-out"), 25)),
  ]);

  expect(result).toBe("done");
  expect(handle.forkSharedSyncPromise).toBeNull();
  expect(handle.kernelShare.forkSharedSyncPromise).toBeNull();
});
