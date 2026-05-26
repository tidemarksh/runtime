import { expect, test, vi } from "vite-plus/test";
import { writePipe } from "../../../src/bridge/pipe.js";
import { createWorkerSchedulerStateSyncHelpers } from "../../../src/worker/scheduler/state-sync.js";
import { createTransferDebugStats } from "../../../src/worker/types.js";

function makeHandle(overrides: Record<string, unknown> = {}) {
  let runtimeState = {
    heapBase: 0,
    heapPtr: 0,
    ofdTablePtr: 0,
    memfsPtr: 0,
    memfsHeaderBytes: 0,
    memfsInodesBytes: 0,
    memfsFirstChildBytes: 0,
    memfsDirEntriesBytes: 0,
    memfsDataBytes: 0,
    memStatePtr: 0,
    epollTablePtr: 0,
    socketTablePtr: 0,
    socketUsed: 0,
    pipeBase: 0,
    pipeUsed: 7n,
    netBufBase: 0,
    netBufUsed: 0n,
    sigSlotsPtr: 0,
    processPendingPtr: 0,
    processRuntimeStatePtr: 0,
    timeStatePtr: 0,
  };
  return {
    pid: 1,
    vmPtr: 0,
    pendingChildSpawnRecords: 0,
    kernelWorkerRequest: vi.fn(async () => ({
      type: "pipe-state-synced",
      kernelState: runtimeState,
    })),
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    memory: new WebAssembly.Memory({ initial: 8, maximum: 8, shared: true }),
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 2,
      stderrPipe: 3,
      refCount: 1,
    },
    stdinPipe: 0,
    stdoutPipe: 2,
    stderrPipe: 3,
    outputBacklog: [],
    onStdout: null,
    kernel: {
      abi_max_fds: () => 16,
      abi_max_threads: () => 0,
      abi_max_pipes: () => 16,
      abi_max_sockets: () => 16,
      kernel_ofd_table_bytes: () => 512,
      kernel_pipe_region_bytes: () => 16 * (32 + 65_536),
      kernel_socket_table_bytes: () => 16 * 384,
      kernel_net_buf_pool_bytes: () => 16 * 65_536,
      kernel_attach_runtime_state(
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
      ) {
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
      kernel_heap_base: () => runtimeState.heapBase,
      kernel_heap_ptr: () => runtimeState.heapPtr,
      kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
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
      abi_status_pipe_wait: () => 19,
      abi_status_epoll_wait: () => 22,
      abi_status_socket_recv: () => 30,
      abi_status_socket_send: () => 29,
      abi_fd_type_file: () => 1,
      abi_fd_type_pipe: () => 6,
      process_current_pid: () => (overrides.pid as number | undefined) ?? 1,
      process_fd_ofd_id: () => 0,
      process_fd_flags: () => 0,
      kernel_ofd_fd_type: () => 0,
      kernel_ofd_ino: () => 0,
      kernel_ofd_host_fd: () => -1,
    },
    kernelState: runtimeState,
    runningWorker: null,
    readyWorkers: [],
    stdioOwner: null,
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
    blockedWorkers: new Map(),
    pendingPipeSlotSnapshots: [],
    kernelShare: { members: new Set(), sharedOfdIds: new Set(), ofdSlotSnapshots: new Map() },
    workers: new Set(),
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    networkRecvPayloadWatermarks: new Map(),
    transferDebugStats: createTransferDebugStats(),
    vforkSnapshot: null,
    vforkWorker: null,
    ...overrides,
  } as never;
}

test("requestPipeStateSync drains the visible stdio owner for hidden-child internal sync", async () => {
  const drainProcessOutput = vi.fn(() => true);
  const drainHiddenProcessOutput = vi.fn(() => true);
  const retryBlockedWorker = vi.fn();
  const parent = makeHandle();
  const child = makeHandle({ pid: 2, stdioOwner: parent });

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput,
    drainHiddenProcessOutput,
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestPipeStateSync(child, false);

  expect(drainProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainProcessOutput).toHaveBeenCalledWith(parent);
  expect(drainHiddenProcessOutput).not.toHaveBeenCalled();
  expect(child.kernelWorkerRequest).toHaveBeenCalledTimes(1);
  expect(parent.kernelWorkerRequest).toHaveBeenCalledTimes(1);
  expect(retryBlockedWorker).not.toHaveBeenCalled();
});

test("requestSocketStateSync wakes socket-backed poll and epoll waiters", async () => {
  const retryBlockedWorker = vi.fn();
  const handle = makeHandle({
    pid: 1,
    kernelWorkerRequest: vi.fn(async (msg) => {
      expect(msg.type).toBe("sync-socket-state");
      return {
        type: "socket-state-synced",
        requestId: 0,
        kernelState: {
          ...msg.kernelState,
          socketUsed: 3,
          netBufUsed: 5n,
        },
      };
    }),
  });
  handle.kernelShare.members.add(handle);
  const pipeWorker = {} as Worker;
  const epollWorker = {} as Worker;
  const recvWorker = {} as Worker;
  handle.blockedWorkers.set(pipeWorker, {
    status: handle.kernel.abi_status_pipe_wait(),
    pipeSyncedBeforePassive: true,
  } as never);
  handle.blockedWorkers.set(epollWorker, {
    status: handle.kernel.abi_status_epoll_wait(),
  } as never);
  handle.blockedWorkers.set(recvWorker, {
    status: handle.kernel.abi_status_socket_recv(),
  } as never);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestSocketStateSync(handle, true);

  expect(handle.kernelState.socketUsed).toBe(3);
  expect(handle.kernelState.netBufUsed).toBe(5n);
  expect(retryBlockedWorker).toHaveBeenCalledWith(handle, pipeWorker);
  expect(retryBlockedWorker).toHaveBeenCalledWith(handle, epollWorker);
  expect(retryBlockedWorker).toHaveBeenCalledWith(handle, recvWorker);
  expect(handle.blockedWorkers.get(pipeWorker)?.pipeSyncedBeforePassive).toBe(false);
});

test("requestSocketStateSync rewrites provisional pid restored by kernel-worker state", async () => {
  const retryBlockedWorker = vi.fn();
  let currentPid = 3;
  const handle = makeHandle({
    pid: 3,
    vmPtr: 0x1234,
    kernelWorkerRequest: vi.fn(async (msg) => {
      expect(msg.type).toBe("sync-socket-state");
      currentPid = 87703;
      return {
        type: "socket-state-synced",
        requestId: 0,
        kernelState: {
          ...msg.kernelState,
          socketUsed: 3,
          netBufUsed: 5n,
        },
      };
    }),
  });
  handle.kernel.process_current_pid = () => currentPid;
  handle.kernel.process_rewrite_child_pid = vi.fn((
    _vmPtr: number,
    oldPid: number,
    newPid: number,
  ) => {
    if (currentPid === oldPid) {
      currentPid = newPid;
    }
  });
  handle.kernelShare.members.add(handle);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestSocketStateSync(handle, true);

  expect(handle.kernel.process_rewrite_child_pid).toHaveBeenCalledWith(0x1234, 87703, 3, 4);
  expect(currentPid).toBe(3);
  expect(retryBlockedWorker).not.toHaveBeenCalled();
});

test("requestSocketStateSync preserves network recv payload when restoring a stale snapshot with the same tail", async () => {
  const retryBlockedWorker = vi.fn();
  const handle = makeHandle();
  handle.memory = new WebAssembly.Memory({ initial: 32, maximum: 32, shared: true });
  const socketTablePtr = 0x2000;
  const netBufBase = 0x4000;
  const maxSockets = handle.kernel.abi_max_sockets();
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = handle.kernel.kernel_net_buf_pool_bytes() / netBufferCount;
  const memory = new Uint8Array(handle.memory.buffer);
  const view = new DataView(handle.memory.buffer);
  handle.kernel.kernel_attach_runtime_state(
    0,
    0,
    0x1000,
    0,
    0,
    0,
    socketTablePtr,
    1,
    0,
    0n,
    netBufBase,
    1n,
    0,
    0,
    0,
    0,
  );
  handle.kernelState = {
    ...handle.kernelState,
    ofdTablePtr: 0x1000,
    socketTablePtr,
    socketUsed: 1,
    netBufBase,
    netBufUsed: 1n,
  };
  view.setInt32(socketTablePtr + 312, 0, true);
  view.setUint32(netBufBase, 0, true);
  view.setUint32(netBufBase + 4, 64, true);
  memory[netBufBase + 16] = 0xab;
  memory[netBufBase + 17] = 0xcd;
  handle.networkConnections.set(3, { read: () => undefined, write: () => {} } as never);
  handle.kernel.abi_fd_type_socket = () => 8;
  handle.kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => (fd === 3 ? 4 : 0);
  handle.kernel.kernel_ofd_fd_type = (ofdId: number) => (ofdId === 4 ? 8 : 0);
  handle.kernel.thread_socket_idx_for_fd = (_vmPtr: number, fd: number) => (fd === 3 ? 0 : -1);
  handle.kernel.thread_socket_recv_buf_idx = (socketIdx: number) => (socketIdx === 0 ? 0 : -1);
  handle.kernel.thread_socket_recv_available = () => 64;
  handle.kernel.thread_socket_read_bytes = () => 0n;
  handle.kernel.thread_socket_read_count = () => 0;
  handle.kernel.thread_socket_state = () => 3;
  handle.kernel.thread_socket_peer_idx = () => -1;
  handle.kernel.thread_socket_outbound_available = () => 0;
  handle.kernelWorkerRequest = vi.fn(async (msg) => {
    expect(msg.type).toBe("sync-socket-state");
    const staleNetBufPool = new Uint8Array(handle.kernel.kernel_net_buf_pool_bytes());
    new DataView(staleNetBufPool.buffer).setUint32(4, 64, true);
    return {
      type: "socket-state-synced",
      requestId: 0,
      kernelState: {
        ...msg.kernelState,
        socketUsed: 1,
        netBufUsed: 1n,
      },
      socketState: {
        socketTable: new Uint8Array(handle.kernel.kernel_socket_table_bytes()),
        netBufPool: staleNetBufPool,
        socketUsed: 1,
        netBufUsed: 1n,
      },
    };
  });
  handle.kernelShare.members.add(handle);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestSocketStateSync(handle, false);

  expect(view.getUint32(netBufBase, true)).toBe(0);
  expect(view.getUint32(netBufBase + 4, true)).toBe(64);
  expect(memory[netBufBase + 16]).toBe(0xab);
  expect(memory[netBufBase + 17]).toBe(0xcd);
  expect(netBufferBytes).toBeGreaterThan(64);
});

test("requestSocketStateSync keeps newer incoming network recv payload when tail advances", async () => {
  const retryBlockedWorker = vi.fn();
  const handle = makeHandle();
  handle.memory = new WebAssembly.Memory({ initial: 32, maximum: 32, shared: true });
  const socketTablePtr = 0x2000;
  const netBufBase = 0x4000;
  const memory = new Uint8Array(handle.memory.buffer);
  const view = new DataView(handle.memory.buffer);
  handle.kernel.kernel_attach_runtime_state(
    0,
    0,
    0x1000,
    0,
    0,
    0,
    socketTablePtr,
    1,
    0,
    0n,
    netBufBase,
    1n,
    0,
    0,
    0,
    0,
  );
  handle.kernelState = {
    ...handle.kernelState,
    ofdTablePtr: 0x1000,
    socketTablePtr,
    socketUsed: 1,
    netBufBase,
    netBufUsed: 1n,
  };
  view.setInt32(socketTablePtr + 312, 0, true);
  view.setUint32(netBufBase, 0, true);
  view.setUint32(netBufBase + 4, 64, true);
  memory[netBufBase + 16] = 0xab;
  memory[netBufBase + 17] = 0xcd;
  handle.networkConnections.set(3, { read: () => undefined, write: () => {} } as never);
  handle.kernel.abi_fd_type_socket = () => 8;
  handle.kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => (fd === 3 ? 4 : 0);
  handle.kernel.kernel_ofd_fd_type = (ofdId: number) => (ofdId === 4 ? 8 : 0);
  handle.kernel.thread_socket_idx_for_fd = (_vmPtr: number, fd: number) => (fd === 3 ? 0 : -1);
  handle.kernel.thread_socket_recv_buf_idx = (socketIdx: number) => (socketIdx === 0 ? 0 : -1);
  handle.kernel.thread_socket_recv_available = () => 64;
  handle.kernel.thread_socket_read_bytes = () => 0n;
  handle.kernel.thread_socket_read_count = () => 0;
  handle.kernel.thread_socket_state = () => 3;
  handle.kernel.thread_socket_peer_idx = () => -1;
  handle.kernel.thread_socket_outbound_available = () => 0;
  handle.kernelWorkerRequest = vi.fn(async (msg) => {
    expect(msg.type).toBe("sync-socket-state");
    const newerNetBufPool = new Uint8Array(handle.kernel.kernel_net_buf_pool_bytes());
    const newerView = new DataView(newerNetBufPool.buffer);
    newerView.setUint32(4, 128, true);
    newerNetBufPool[16] = 0x11;
    newerNetBufPool[17] = 0x22;
    return {
      type: "socket-state-synced",
      requestId: 0,
      kernelState: {
        ...msg.kernelState,
        socketUsed: 1,
        netBufUsed: 1n,
      },
      socketState: {
        socketTable: new Uint8Array(handle.kernel.kernel_socket_table_bytes()),
        netBufPool: newerNetBufPool,
        socketUsed: 1,
        netBufUsed: 1n,
      },
    };
  });
  handle.kernelShare.members.add(handle);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestSocketStateSync(handle, false);

  expect(view.getUint32(netBufBase, true)).toBe(0);
  expect(view.getUint32(netBufBase + 4, true)).toBe(128);
  expect(memory[netBufBase + 16]).toBe(0x11);
  expect(memory[netBufBase + 17]).toBe(0x22);
});

test("requestSocketStateSync preserves host recv payload when producer tail wraps", async () => {
  const retryBlockedWorker = vi.fn();
  const handle = makeHandle();
  handle.memory = new WebAssembly.Memory({ initial: 32, maximum: 32, shared: true });
  const socketTablePtr = 0x2000;
  const netBufBase = 0x4000;
  const maxSockets = handle.kernel.abi_max_sockets();
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = handle.kernel.kernel_net_buf_pool_bytes() / netBufferCount;
  const capacity = netBufferBytes - 16;
  const memory = new Uint8Array(handle.memory.buffer);
  const view = new DataView(handle.memory.buffer);
  handle.kernel.kernel_attach_runtime_state(
    0,
    0,
    0x1000,
    0,
    0,
    0,
    socketTablePtr,
    1,
    0,
    0n,
    netBufBase,
    1n,
    0,
    0,
    0,
    0,
  );
  handle.kernelState = {
    ...handle.kernelState,
    ofdTablePtr: 0x1000,
    socketTablePtr,
    socketUsed: 1,
    netBufBase,
    netBufUsed: 1n,
  };
  const wrappedTail = Math.floor(capacity * 0.65);
  const staleTail = capacity - 1;
  const availableBytes = (wrappedTail - staleTail + capacity) % capacity;
  view.setInt32(socketTablePtr + 312, 0, true);
  view.setUint32(netBufBase, staleTail, true);
  view.setUint32(netBufBase + 4, wrappedTail, true);
  memory[netBufBase + 16] = 0xab;
  memory[netBufBase + 17] = 0xcd;
  handle.networkConnections.set(3, { read: () => undefined, write: () => {} } as never);
  handle.networkRecvPayloadWatermarks.set(3, {
    recvBufIdx: 0,
    head: staleTail,
    tail: wrappedTail,
    capacity,
    readBytes: staleTail,
    availableBytes,
    deliveredBytes: staleTail + availableBytes,
    data: memory.slice(netBufBase + 16, netBufBase + netBufferBytes),
  });
  handle.kernel.abi_fd_type_socket = () => 8;
  handle.kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => (fd === 3 ? 4 : 0);
  handle.kernel.kernel_ofd_fd_type = (ofdId: number) => (ofdId === 4 ? 8 : 0);
  handle.kernel.thread_socket_idx_for_fd = (_vmPtr: number, fd: number) => (fd === 3 ? 0 : -1);
  handle.kernel.thread_socket_recv_buf_idx = (socketIdx: number) => (socketIdx === 0 ? 0 : -1);
  handle.kernel.thread_socket_recv_available = () => availableBytes;
  handle.kernel.thread_socket_read_bytes = () => BigInt(staleTail);
  handle.kernel.thread_socket_read_count = () => 64;
  handle.kernel.thread_socket_state = () => 3;
  handle.kernel.thread_socket_peer_idx = () => -1;
  handle.kernel.thread_socket_outbound_available = () => 0;
  handle.kernelWorkerRequest = vi.fn(async (msg) => {
    expect(msg.type).toBe("sync-socket-state");
    const staleNetBufPool = new Uint8Array(handle.kernel.kernel_net_buf_pool_bytes());
    const staleView = new DataView(staleNetBufPool.buffer);
    staleView.setUint32(0, staleTail, true);
    staleView.setUint32(4, staleTail, true);
    staleNetBufPool[16] = 0x11;
    staleNetBufPool[17] = 0x22;
    return {
      type: "socket-state-synced",
      requestId: 0,
      kernelState: {
        ...msg.kernelState,
        socketUsed: 1,
        netBufUsed: 1n,
      },
      socketState: {
        socketTable: new Uint8Array(handle.kernel.kernel_socket_table_bytes()),
        netBufPool: staleNetBufPool,
        socketUsed: 1,
        netBufUsed: 1n,
      },
    };
  });
  handle.kernelShare.members.add(handle);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestSocketStateSync(handle, false);

  expect(view.getUint32(netBufBase, true)).toBe(staleTail);
  expect(view.getUint32(netBufBase + 4, true)).toBe(wrappedTail);
  expect(memory[netBufBase + 16]).toBe(0xab);
  expect(memory[netBufBase + 17]).toBe(0xcd);
});

test("requestSocketStateSync does not restore stale recv bytes after local socket drain", async () => {
  const retryBlockedWorker = vi.fn();
  const handle = makeHandle();
  handle.memory = new WebAssembly.Memory({ initial: 32, maximum: 32, shared: true });
  const socketTablePtr = 0x2000;
  const netBufBase = 0x4000;
  const memory = new Uint8Array(handle.memory.buffer);
  const view = new DataView(handle.memory.buffer);
  handle.kernel.kernel_attach_runtime_state(
    0,
    0,
    0x1000,
    0,
    0,
    0,
    socketTablePtr,
    1,
    0,
    0n,
    netBufBase,
    1n,
    0,
    0,
    0,
    0,
  );
  handle.kernelState = {
    ...handle.kernelState,
    ofdTablePtr: 0x1000,
    socketTablePtr,
    socketUsed: 1,
    netBufBase,
    netBufUsed: 1n,
  };
  view.setInt32(socketTablePtr + 312, 0, true);
  view.setUint32(netBufBase, 65_536, true);
  view.setUint32(netBufBase + 4, 141_117, true);
  memory[netBufBase + 16] = 0xab;
  handle.networkConnections.set(3, { read: () => undefined, write: () => {} } as never);
  handle.kernel.abi_fd_type_socket = () => 8;
  handle.kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => (fd === 3 ? 4 : 0);
  handle.kernel.kernel_ofd_fd_type = (ofdId: number) => (ofdId === 4 ? 8 : 0);
  handle.kernel.thread_socket_idx_for_fd = (_vmPtr: number, fd: number) => (fd === 3 ? 0 : -1);
  handle.kernel.thread_socket_recv_buf_idx = (socketIdx: number) => (socketIdx === 0 ? 0 : -1);
  let recvAvailable = 75_581;
  let socketState = 3;
  let readCount = 16;
  handle.kernel.thread_socket_recv_available = () => recvAvailable;
  handle.kernel.thread_socket_read_bytes = () => 65_536n;
  handle.kernel.thread_socket_read_count = () => readCount;
  handle.kernel.thread_socket_state = () => socketState;
  handle.kernel.thread_socket_peer_idx = () => -1;
  handle.kernel.thread_socket_outbound_available = () => 0;
  handle.kernelWorkerRequest = vi.fn(async (msg) => {
    expect(msg.type).toBe("sync-socket-state");
    recvAvailable = 0;
    socketState = 5;
    readCount = 16;
    view.setUint32(netBufBase, 20_089, true);
    view.setUint32(netBufBase + 4, 20_089, true);
    memory[netBufBase + 16] = 0x44;

    const staleNetBufPool = new Uint8Array(handle.kernel.kernel_net_buf_pool_bytes());
    const staleView = new DataView(staleNetBufPool.buffer);
    staleView.setUint32(0, 65_536, true);
    staleView.setUint32(4, 141_117, true);
    staleNetBufPool[16] = 0xaa;
    return {
      type: "socket-state-synced",
      requestId: 0,
      kernelState: {
        ...msg.kernelState,
        socketUsed: 1,
        netBufUsed: 1n,
      },
      socketState: {
        socketTable: new Uint8Array(handle.kernel.kernel_socket_table_bytes()),
        netBufPool: staleNetBufPool,
        socketUsed: 1,
        netBufUsed: 1n,
      },
    };
  });
  handle.kernelShare.members.add(handle);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestSocketStateSync(handle, false);

  expect(view.getUint32(netBufBase, true)).toBe(20_089);
  expect(view.getUint32(netBufBase + 4, true)).toBe(20_089);
  expect(memory[netBufBase + 16]).toBe(0x44);
});

test("requestPipeStateSync drains visible stdio owner during kernel-owned waking sync", async () => {
  const drainProcessOutput = vi.fn(() => true);
  const drainHiddenProcessOutput = vi.fn(() => true);
  const retryBlockedWorker = vi.fn();
  const parent = makeHandle({ pid: 1 });
  const child = makeHandle({ pid: 2, stdioOwner: parent });

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput,
    drainHiddenProcessOutput,
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestPipeStateSync(child, true);

  expect(child.kernelWorkerRequest).toHaveBeenCalledTimes(1);
  expect(parent.kernelWorkerRequest).toHaveBeenCalledTimes(1);
  expect(drainProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainProcessOutput).toHaveBeenCalledWith(parent);
  expect(drainHiddenProcessOutput).not.toHaveBeenCalled();
});

test("requestPipeStateSync re-syncs the visible owner after self-drain advances the read head", async () => {
  const drainProcessOutput = vi.fn(() => true);
  const drainHiddenProcessOutput = vi.fn(() => true);
  const retryBlockedWorker = vi.fn();
  const handle = makeHandle({ pid: 1, stdioOwner: null });

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput,
    drainHiddenProcessOutput,
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestPipeStateSync(handle, false);

  expect(handle.kernelWorkerRequest).toHaveBeenCalledTimes(2);
  expect(drainProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainProcessOutput).toHaveBeenCalledWith(handle);
  expect(drainHiddenProcessOutput).not.toHaveBeenCalled();
});

test("requestPipeStateSync still drains hidden child output when stdout was remapped away from the visible host pipe", async () => {
  const drainProcessOutput = vi.fn(() => true);
  const drainHiddenProcessOutput = vi.fn(() => true);
  const retryBlockedWorker = vi.fn();
  const parent = makeHandle();
  const child = makeHandle({
    pid: 2,
    stdioOwner: parent,
    stdoutPipe: 9,
    stderrPipe: parent.stderrPipe,
    kernel: {
      ...makeHandle({ pid: 2 }).kernel,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 2 ? 6 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 6 ? 6 : 0),
      kernel_ofd_ino: (ofdId: number) => (ofdId === 6 ? parent.hostStdio.stderrPipe : 0),
    },
  });

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput,
    drainHiddenProcessOutput,
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestPipeStateSync(child, false);

  expect(drainHiddenProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainHiddenProcessOutput).toHaveBeenCalledWith(child);
});

test("requestPipeStateSync defers while active vfork child owns the worker pid", async () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    ownerPid: number;
    generation: number;
    waitingInWorkerStatus: number | null;
    resumeWorkersOnReady: Worker[];
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    ownerPid: 1,
    generation: 1,
    waitingInWorkerStatus: null,
    resumeWorkersOnReady: [],
  });
  const handle = makeHandle({
    pid: 1,
    vmPtr: 0x1234,
    workers: new Set([worker]),
    vforkWorker: worker,
    vforkSnapshot: {
      vmPtr: 0x1234,
      childPid: 3,
      nextPid: null,
      pgid: null,
      sid: null,
      parentPid: 1,
      parentProcessPtr: 0x2000,
      parentPc: 0n,
      parentSp: 0n,
      parentS11: 0n,
      inheritedSharedOfdIds: [],
    },
    kernel: {
      ...makeHandle().kernel,
      process_current_pid: () => 3,
    },
  });
  const drainProcessOutput = vi.fn(() => true);
  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: vi.fn(),
    drainProcessOutput,
    drainHiddenProcessOutput: vi.fn(() => true),
    threadWorkerStates,
    retryBlockedWorker: vi.fn(),
  });

  await helpers.requestPipeStateSync(handle, true);

  expect(handle.kernelWorkerRequest).not.toHaveBeenCalled();
  expect(drainProcessOutput).not.toHaveBeenCalled();
});
