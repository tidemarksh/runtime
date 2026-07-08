import { processKernelWorkerRequest, setProcessKernelWorkerRequest } from "../../../../src/worker/process-kernel-worker-request-state.js";
import { addProcessWorker } from "../../../../src/worker/process-worker-state.js";
import { addKernelShareMember, addKernelShareSharedOfdId, kernelShareHasSharedOfdId, kernelShareMembers, kernelShareSharedOfdIdList, processKernelShareGroup, setProcessKernelShareGroup } from "../../../../src/worker/kernel-share.js";
import { hostExecutionState } from "../../../../src/worker/host-execution-state.js";
import { setProcessExitInProgress } from "../../../../src/worker/process-lifecycle-state.js";
import {
  processHostStdio,
  processStderrPipe,
} from "../../../../src/worker/process-stdio-state.js";
import { expect, test, vi } from "vite-plus/test";
import { writePipe } from "../../../../src/bridge/pipe.js";
import { createWorkerSchedulerStateSyncHelpers } from "../../../../src/worker/host-boundary/state-sync.js";
import {
  setBlockedRpcGeneration,
  setBlockedRpcPending,
} from "../../../../src/worker/host-blocked-state.js";
import {
  networkConnections,
  networkRecvPayloadWatermarks,
} from "../../../../src/worker/network-socket-state.js";
import { createTransferDebugStats } from "../../../../src/worker/types.js";
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";
import { seedProcessHandleStateFixture } from "../support/process-handle-state-fixture.js";

function makeHandle(overrides: Record<string, unknown> = {}) {
  const hasKernelWorkerRequestOverride = Object.prototype.hasOwnProperty.call(
    overrides,
    "kernelWorkerRequest",
  );
  const {
    kernelWorkerRequest: overrideKernelWorkerRequest,
    ...handleOverrides
  } = overrides;
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
  let kernel: Record<string, unknown>;
  kernel = {
    ...makeSupervisorAbiStubs({ currentExports: () => kernel as never }),
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
    abi_fd_type_epoll: () => 7,
    process_current_pid: () => (overrides.pid as number | undefined) ?? 1,
    thread_process_ptr: () => 0,
    process_state_bytes: () => 0,
    process_fd_ofd_id: () => 0,
    process_fd_flags: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_ino: () => 0,
    kernel_ofd_host_fd: () => -1,
    thread_socket_idx_for_fd: () => -1,
    thread_socket_peer_idx: () => -1,
    thread_socket_state: () => 3,
    thread_socket_external_peer: () => 1,
    thread_socket_recv_buf_idx: () => -1,
    thread_socket_recv_available: () => 0,
    thread_socket_read_bytes: () => 0n,
  };
  const kernelWorkerRequest = hasKernelWorkerRequestOverride
    ? overrideKernelWorkerRequest
    : vi.fn(async () => ({
      type: "pipe-state-synced",
      kernelState: runtimeState,
    }));
  const handle = {
    pid: 1,
    vmPtr: 0,
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
    kernel,
    kernelState: runtimeState,
    runningWorker: null,
    vforkReadyWorkers: [],
    stdioOwner: null,
    hostBlockedWorkers: new Map(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set(), ofdSlotSnapshots: new Map() },
    workers: new Set(),
    transferDebugStats: createTransferDebugStats(),
    vforkSnapshot: null,
    vforkWorker: null,
    ...handleOverrides,
  } as never;
  setProcessKernelWorkerRequest(handle, kernelWorkerRequest as never);
  seedProcessHandleStateFixture(handle);
  return handle;
}

test("requestPipeStateSync falls back to visible stdio owner when hidden-child internal sync has no output", async () => {
  const drainProcessOutput = vi.fn(() => true);
  const drainHiddenProcessOutput = vi.fn(() => false);
  const retryHostBlockedWorker = vi.fn();
  const parent = makeHandle();
  const child = makeHandle({ pid: 2, stdioOwner: parent });

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput,
    drainHiddenProcessOutput,
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestPipeStateSync(child, false);

  expect(drainProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainProcessOutput).toHaveBeenCalledWith(parent);
  expect(drainHiddenProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainHiddenProcessOutput).toHaveBeenCalledWith(child);
  expect(processKernelWorkerRequest(child)).toHaveBeenCalledTimes(1);
  expect(processKernelWorkerRequest(parent)).toHaveBeenCalledTimes(1);
  expect(retryHostBlockedWorker).not.toHaveBeenCalled();
});

test("requestSocketStateSync wakes socket-backed poll and epoll waiters", async () => {
  const retryHostBlockedWorker = vi.fn();
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
  addKernelShareMember(handle);
  const clearPipeSyncBeforeRetry = vi.fn(() => 0);
  handle.kernel.supervisor_blocked_pipe_sync_before_retry_clear = clearPipeSyncBeforeRetry;
  const pipeWorker = {} as Worker;
  const epollWorker = {} as Worker;
  const recvWorker = {} as Worker;
  hostExecutionState(handle).hostBlockedWorkers.set(pipeWorker, {
    vmPtr: 0x1234,
    status: handle.kernel.abi_status_pipe_wait(),
  } as never);
  hostExecutionState(handle).hostBlockedWorkers.set(epollWorker, {
    status: handle.kernel.abi_status_epoll_wait(),
  } as never);
  hostExecutionState(handle).hostBlockedWorkers.set(recvWorker, {
    status: handle.kernel.abi_status_socket_recv(),
  } as never);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestSocketStateSync(handle, true);

  expect(handle.kernelState.socketUsed).toBe(3);
  expect(handle.kernelState.netBufUsed).toBe(5n);
  expect(retryHostBlockedWorker).toHaveBeenCalledWith(handle, pipeWorker);
  expect(retryHostBlockedWorker).toHaveBeenCalledWith(handle, epollWorker);
  expect(retryHostBlockedWorker).toHaveBeenCalledWith(handle, recvWorker);
  expect(clearPipeSyncBeforeRetry).toHaveBeenCalledWith(0x1234);
});

test("waking sync drains supervisor resume before runtime host retry", async () => {
  const retryHostBlockedWorker = vi.fn();
  const handle = makeHandle();
  addKernelShareMember(handle);
  const pipeWorker = {} as Worker;
  const pumpProcess = vi.fn((member) => {
    hostExecutionState(member).hostBlockedWorkers.delete(pipeWorker);
    hostExecutionState(member).runningWorker = pipeWorker;
  });
  hostExecutionState(handle).hostBlockedWorkers.set(pipeWorker, {
    status: handle.kernel.abi_status_pipe_wait(),
  } as never);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    pumpProcess,
    retryHostBlockedWorker,
  });

  await helpers.requestPipeStateSync(handle, true);

  expect(pumpProcess).toHaveBeenCalledWith(handle);
  expect(hostExecutionState(handle).runningWorker).toBe(pipeWorker);
  expect(retryHostBlockedWorker).not.toHaveBeenCalledWith(handle, pipeWorker);
});

test("requestSocketStateSync skips rpc-pending epoll waiters during waking sync", async () => {
  const retryHostBlockedWorker = vi.fn();
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
  addKernelShareMember(handle);
  const epollWorker = {} as Worker;
  const blockedEpoll = {
    status: handle.kernel.abi_status_epoll_wait(),
  };
  setBlockedRpcPending(blockedEpoll as never, true);
  setBlockedRpcGeneration(blockedEpoll as never, 7);
  hostExecutionState(handle).hostBlockedWorkers.set(epollWorker, blockedEpoll as never);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestSocketStateSync(handle, true);

  expect(handle.kernelState.socketUsed).toBe(3);
  expect(handle.kernelState.netBufUsed).toBe(5n);
  expect(retryHostBlockedWorker).not.toHaveBeenCalledWith(handle, epollWorker);
});

test("requestPipeStateSync skips rpc-pending pipe waiters during waking sync", async () => {
  const retryHostBlockedWorker = vi.fn();
  const handle = makeHandle();
  addKernelShareMember(handle);
  const clearPipeSyncBeforeRetry = vi.fn(() => 0);
  handle.kernel.supervisor_blocked_pipe_sync_before_retry_clear = clearPipeSyncBeforeRetry;
  const pipeWorker = {} as Worker;
  const blockedPipe = {
    vmPtr: 0x1234,
    status: handle.kernel.abi_status_pipe_wait(),
  };
  setBlockedRpcPending(blockedPipe as never, true);
  setBlockedRpcGeneration(blockedPipe as never, 7);
  hostExecutionState(handle).hostBlockedWorkers.set(pipeWorker, blockedPipe as never);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestPipeStateSync(handle, true);

  expect(retryHostBlockedWorker).not.toHaveBeenCalledWith(handle, pipeWorker);
  expect(clearPipeSyncBeforeRetry).not.toHaveBeenCalled();
});

test("requestEpollStateSync delegates no-ofd retry action to supervisor", async () => {
  const retryHostBlockedWorker = vi.fn();
  const appendRuntimeLifecycleEvent = vi.fn();
  const handle = makeHandle({ runtimeId: 1 });
  addKernelShareMember(handle);
  const epollWorker = {} as Worker;
  hostExecutionState(handle).hostBlockedWorkers.set(epollWorker, {
    status: handle.kernel.abi_status_epoll_wait(),
  } as never);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent,
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestEpollStateSync(handle, true);

  expect(processKernelWorkerRequest(handle)).not.toHaveBeenCalled();
  expect(retryHostBlockedWorker).toHaveBeenCalledWith(handle, epollWorker);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(1, "epoll-sync-skip-no-ofd:1:retry=1");
});

test("requestSocketStateSync applies socket payload state to fork-share members before wake", async () => {
  const retryHostBlockedWorker = vi.fn();
  const socketTable = new Uint8Array(16 * 384);
  socketTable[4] = 77;
  const parent = makeHandle({
    pid: 1,
    kernelWorkerRequest: vi.fn(async (msg) => {
      expect(msg.type).toBe("sync-socket-state");
      return {
        type: "socket-state-synced",
        requestId: 0,
        kernelState: {
          ...msg.kernelState,
          socketUsed: 3,
          netBufUsed: 0n,
        },
        socketState: {
          socketTable,
          netBufSlots: [],
          socketUsed: 3,
          netBufUsed: 0n,
        },
      };
    }),
  });
  const child = makeHandle({ pid: 2 });
  const share = { members: new Set([parent, child]), sharedOfdIds: new Set(), ofdSlotSnapshots: new Map() };
  setProcessKernelShareGroup(parent, share);
  setProcessKernelShareGroup(child, share);
  const recvWorker = {} as Worker;
  hostExecutionState(child).hostBlockedWorkers.set(recvWorker, {
    status: child.kernel.abi_status_socket_recv(),
  } as never);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestSocketStateSync(parent, true);

  expect(new Uint8Array(child.memory.buffer)[4]).toBe(77);
  expect(child.kernelState.socketUsed).toBe(3);
  expect(retryHostBlockedWorker).toHaveBeenCalledWith(child, recvWorker);
});

test("requestSocketStateSync rewrites provisional pid restored by kernel-worker state", async () => {
  const retryHostBlockedWorker = vi.fn();
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
  addKernelShareMember(handle);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestSocketStateSync(handle, true);

  expect(handle.kernel.process_rewrite_child_pid).toHaveBeenCalledWith(0x1234, 87703, 3, 4);
  expect(currentPid).toBe(3);
  expect(retryHostBlockedWorker).not.toHaveBeenCalled();
});

test("requestEpollDependentOfdStateSync uses ABI fd types for eventfd and timerfd", async () => {
  const retryHostBlockedWorker = vi.fn();
  const eventfdType = 41;
  const timerfdType = 42;
  const syncedOfdIds: number[][] = [];
  const handle = makeHandle({
    pid: 1,
    vmPtr: 0x1234,
    workers: new Set(),
    kernelWorkerRequest: vi.fn(async (msg) => {
      expect(msg.type).toBe("sync-ofd-state");
      syncedOfdIds.push([...msg.ofdIds]);
      return {
        type: "ofd-state-synced",
        requestId: 0,
        kernelState: msg.kernelState,
      };
    }),
  });
  handle.kernel.abi_max_fds = () => 16;
  handle.kernel.abi_max_threads = () => 0;
  handle.kernel.abi_fd_type_eventfd = () => eventfdType;
  handle.kernel.abi_fd_type_timerfd = () => timerfdType;
  handle.kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => {
    if (fd === 7) {
      return 11;
    }
    if (fd === 8) {
      return 12;
    }
    if (fd === 9) {
      return 13;
    }
    return 0;
  };
  handle.kernel.process_thread_vm_ptr = (_vmPtr: number, slot: number) => (
    slot === 0 ? 0x1234 : 0
  );
  handle.kernel.thread_blocked_ofd_id = (vmPtr: number) => (
    vmPtr === 0x1234 ? 12 : 0
  );
  handle.kernel.kernel_ofd_fd_type = (ofdId: number) => {
    if (ofdId === 11) {
      return eventfdType;
    }
    if (ofdId === 12) {
      return timerfdType;
    }
    if (ofdId === 13) {
      return 9;
    }
    return 0;
  };

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestEpollDependentOfdStateSync(handle);

  expect(syncedOfdIds).toEqual([[11, 12]]);
  expect(processKernelWorkerRequest(handle)).toHaveBeenCalledTimes(1);
  expect(retryHostBlockedWorker).not.toHaveBeenCalled();
});

test("requestSocketStateSync preserves network recv payload when restoring a stale snapshot with the same tail", async () => {
  const retryHostBlockedWorker = vi.fn();
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
  networkConnections(handle).set(3, { read: () => undefined, write: () => {} } as never);
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
  setProcessKernelWorkerRequest(handle, vi.fn(async (msg) => {
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
  }));
  addKernelShareMember(handle);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestSocketStateSync(handle, false);

  expect(view.getUint32(netBufBase, true)).toBe(0);
  expect(view.getUint32(netBufBase + 4, true)).toBe(64);
  expect(memory[netBufBase + 16]).toBe(0xab);
  expect(memory[netBufBase + 17]).toBe(0xcd);
  expect(netBufferBytes).toBeGreaterThan(64);
});

test("requestSocketStateSync keeps newer incoming network recv payload when tail advances", async () => {
  const retryHostBlockedWorker = vi.fn();
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
  networkConnections(handle).set(3, { read: () => undefined, write: () => {} } as never);
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
  setProcessKernelWorkerRequest(handle, vi.fn(async (msg) => {
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
  }));
  addKernelShareMember(handle);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestSocketStateSync(handle, false);

  expect(view.getUint32(netBufBase, true)).toBe(0);
  expect(view.getUint32(netBufBase + 4, true)).toBe(128);
  expect(memory[netBufBase + 16]).toBe(0x11);
  expect(memory[netBufBase + 17]).toBe(0x22);
});

test("requestSocketStateSync preserves host recv payload when producer tail wraps", async () => {
  const retryHostBlockedWorker = vi.fn();
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
  networkConnections(handle).set(3, { read: () => undefined, write: () => {} } as never);
  networkRecvPayloadWatermarks(handle as never).set(3, {
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
  setProcessKernelWorkerRequest(handle, vi.fn(async (msg) => {
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
  }));
  addKernelShareMember(handle);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestSocketStateSync(handle, false);

  expect(view.getUint32(netBufBase, true)).toBe(staleTail);
  expect(view.getUint32(netBufBase + 4, true)).toBe(wrappedTail);
  expect(memory[netBufBase + 16]).toBe(0xab);
  expect(memory[netBufBase + 17]).toBe(0xcd);
});

test("requestSocketStateSync does not restore stale recv bytes after local socket drain", async () => {
  const retryHostBlockedWorker = vi.fn();
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
  networkConnections(handle).set(3, { read: () => undefined, write: () => {} } as never);
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
  setProcessKernelWorkerRequest(handle, vi.fn(async (msg) => {
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
  }));
  addKernelShareMember(handle);

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestSocketStateSync(handle, false);

  expect(view.getUint32(netBufBase, true)).toBe(20_089);
  expect(view.getUint32(netBufBase + 4, true)).toBe(20_089);
  expect(memory[netBufBase + 16]).toBe(0x44);
});

test("requestPipeStateSync falls back to visible stdio owner during kernel-owned waking sync", async () => {
  const drainProcessOutput = vi.fn(() => true);
  const drainHiddenProcessOutput = vi.fn(() => false);
  const retryHostBlockedWorker = vi.fn();
  const parent = makeHandle({ pid: 1 });
  const child = makeHandle({ pid: 2, stdioOwner: parent });

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput,
    drainHiddenProcessOutput,
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestPipeStateSync(child, true);

  expect(processKernelWorkerRequest(child)).toHaveBeenCalledTimes(1);
  expect(processKernelWorkerRequest(parent)).toHaveBeenCalledTimes(1);
  expect(drainProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainProcessOutput).toHaveBeenCalledWith(parent);
  expect(drainHiddenProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainHiddenProcessOutput).toHaveBeenCalledWith(child);
});

test("requestPipeStateSync skips empty direct exit pipe sync", async () => {
  const appendRuntimeLifecycleEvent = vi.fn();
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "pipe-state-synced",
    kernelState: {
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
      pipeUsed: 1n,
      netBufBase: 0,
      netBufUsed: 0n,
      sigSlotsPtr: 0,
      processPendingPtr: 0,
      processRuntimeStatePtr: 0,
      timeStatePtr: 0,
    },
  }));
  const handle = makeHandle({
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernelWorkerRequest,
  });
  addKernelShareMember(handle);
  setProcessExitInProgress(handle, true);
  const drainProcessOutput = vi.fn(() => false);
  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent,
    drainProcessOutput,
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker: vi.fn(),
  });

  try {
    await helpers.requestPipeStateSync(handle, true);
  } finally {
    setProcessExitInProgress(handle, false);
  }

  expect(kernelWorkerRequest).not.toHaveBeenCalled();
  expect(drainProcessOutput).not.toHaveBeenCalled();
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    1,
    "pipe-sync-skip-empty-exit:1:wake=1",
  );
});

test("requestPipeStateSync keeps held pipe fd exit sync on kernel worker path", async () => {
  const kernelWorkerRequest = vi.fn(async (msg) => {
    expect(msg.type).toBe("sync-pipe-state");
    return {
      type: "pipe-state-synced",
      requestId: 0,
      kernelState: {
        ...msg.kernelState,
        pipeUsed: 5n,
      },
    };
  });
  const handle = makeHandle({
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernelWorkerRequest,
  });
  handle.kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 1 ? 4 : 0;
  handle.kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === 4 ? handle.kernel.abi_fd_type_pipe() : 0;
  handle.kernel.kernel_ofd_ino = (ofdId: number) => ofdId === 4 ? processHostStdio(handle).stdoutPipe : 0;
  handle.kernel.kernel_ofd_host_fd = (ofdId: number) => ofdId === 4 ? -1 : -1;
  addKernelShareMember(handle);
  setProcessExitInProgress(handle, true);
  const appendRuntimeLifecycleEvent = vi.fn();
  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent,
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker: vi.fn(),
  });

  try {
    await helpers.requestPipeStateSync(handle, true);
  } finally {
    setProcessExitInProgress(handle, false);
  }

  expect(kernelWorkerRequest).toHaveBeenCalledTimes(1);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(1, "pipe-sync-start:1:wake=1");
  expect(appendRuntimeLifecycleEvent).not.toHaveBeenCalledWith(
    1,
    "pipe-sync-skip-empty-exit:1:wake=1",
  );
});

test("requestSocketStateSync skips empty direct exit socket sync", async () => {
  const appendRuntimeLifecycleEvent = vi.fn();
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "socket-state-synced",
    kernelState: {
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
      pipeUsed: 1n,
      netBufBase: 0,
      netBufUsed: 0n,
      sigSlotsPtr: 0,
      processPendingPtr: 0,
      processRuntimeStatePtr: 0,
      timeStatePtr: 0,
    },
  }));
  const handle = makeHandle({
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernelWorkerRequest,
  });
  addKernelShareMember(handle);
  setProcessExitInProgress(handle, true);
  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent,
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker: vi.fn(),
  });

  try {
    await helpers.requestSocketStateSync(handle, false);
  } finally {
    setProcessExitInProgress(handle, false);
  }

  expect(kernelWorkerRequest).not.toHaveBeenCalled();
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    1,
    "socket-sync-skip-empty-exit:1:wake=0",
  );
});

test("requestSocketStateSync keeps existing socket state exit sync on kernel worker path", async () => {
  const kernelWorkerRequest = vi.fn(async (msg) => {
    expect(msg.type).toBe("sync-socket-state");
    return {
      type: "socket-state-synced",
      requestId: 0,
      kernelState: {
        ...msg.kernelState,
        socketUsed: 1,
      },
    };
  });
  const handle = makeHandle({
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernelWorkerRequest,
  });
  handle.kernelState = {
    ...handle.kernelState,
    socketUsed: 1,
  };
  addKernelShareMember(handle);
  setProcessExitInProgress(handle, true);
  const appendRuntimeLifecycleEvent = vi.fn();
  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent,
    drainProcessOutput: vi.fn(() => false),
    drainHiddenProcessOutput: vi.fn(() => false),
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker: vi.fn(),
  });

  try {
    await helpers.requestSocketStateSync(handle, false);
  } finally {
    setProcessExitInProgress(handle, false);
  }

  expect(kernelWorkerRequest).toHaveBeenCalledTimes(1);
  expect(appendRuntimeLifecycleEvent).not.toHaveBeenCalledWith(
    1,
    "socket-sync-skip-empty-exit:1:wake=0",
  );
});

test("requestPipeStateSync drains hidden inherited host stdio after child fds close", async () => {
  const drainProcessOutput = vi.fn(() => true);
  const drainHiddenProcessOutput = vi.fn(() => true);
  const retryHostBlockedWorker = vi.fn();
  const parent = makeHandle({ pid: 1 });
  const child = makeHandle({
    pid: 2,
    vmPtr: 0x1234,
    stdioOwner: parent,
    hostStdio: processHostStdio(parent),
  });

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput,
    drainHiddenProcessOutput,
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestPipeStateSync(child, true);

  expect(drainHiddenProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainHiddenProcessOutput).toHaveBeenCalledWith(child);
  expect(drainProcessOutput).not.toHaveBeenCalled();
});

test("requestPipeStateSync re-syncs the visible owner after self-drain advances the read head", async () => {
  const drainProcessOutput = vi.fn(() => true);
  const drainHiddenProcessOutput = vi.fn(() => true);
  const retryHostBlockedWorker = vi.fn();
  const handle = makeHandle({ pid: 1, stdioOwner: null });

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput,
    drainHiddenProcessOutput,
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
  });

  await helpers.requestPipeStateSync(handle, false);

  expect(processKernelWorkerRequest(handle)).toHaveBeenCalledTimes(2);
  expect(drainProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainProcessOutput).toHaveBeenCalledWith(handle);
  expect(drainHiddenProcessOutput).not.toHaveBeenCalled();
});

test("requestPipeStateSync still drains hidden child output when stdout was remapped away from the visible host pipe", async () => {
  const drainProcessOutput = vi.fn(() => true);
  const drainHiddenProcessOutput = vi.fn(() => true);
  const retryHostBlockedWorker = vi.fn();
  const parent = makeHandle();
  const child = makeHandle({
    pid: 2,
    vmPtr: 0x1234,
    stdioOwner: parent,
    stdoutPipe: 9,
    stderrPipe: processStderrPipe(parent),
    kernel: {
      ...makeHandle({ pid: 2 }).kernel,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 2 ? 6 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 6 ? 6 : 0),
      kernel_ofd_ino: (ofdId: number) => (ofdId === 6 ? processHostStdio(parent).stderrPipe : 0),
    },
  });

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput,
    drainHiddenProcessOutput,
    threadWorkerStates: new WeakMap(),
    retryHostBlockedWorker,
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
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    ownerPid: 1,
    generation: 1,
    waitingInWorkerStatus: null,
  });
  const supervisorVforkChildOwnerMismatch = vi.fn(() => 1);
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
      supervisor_vfork_child_owner_mismatch: supervisorVforkChildOwnerMismatch,
    },
  });
  const drainProcessOutput = vi.fn(() => true);
  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: vi.fn(),
    drainProcessOutput,
    drainHiddenProcessOutput: vi.fn(() => true),
    threadWorkerStates,
    retryHostBlockedWorker: vi.fn(),
  });

  await helpers.requestPipeStateSync(handle, true);

  expect(supervisorVforkChildOwnerMismatch).toHaveBeenCalledWith(0x1234, 1);
  expect(processKernelWorkerRequest(handle)).not.toHaveBeenCalled();
  expect(drainProcessOutput).not.toHaveBeenCalled();
});

test("requestPipeStateSync defers before request when an attached worker belongs to another pid", async () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    ownerPid: number;
    generation: number;
    waitingInWorkerStatus: number | null;
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    ownerPid: 1,
    generation: 1,
    waitingInWorkerStatus: null,
  });
  const supervisorProcessOwnerMismatch = vi.fn(() => 1);
  const drainProcessOutput = vi.fn(() => true);
  const handle = makeHandle({
    pid: 1,
    vmPtr: 0x1234,
  });
  handle.kernel.supervisor_process_owner_mismatch = supervisorProcessOwnerMismatch;
  addProcessWorker(handle, worker);
  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: vi.fn(),
    drainProcessOutput,
    drainHiddenProcessOutput: vi.fn(() => true),
    threadWorkerStates,
    retryHostBlockedWorker: vi.fn(),
  });

  await helpers.requestPipeStateSync(handle, true);

  expect(supervisorProcessOwnerMismatch).toHaveBeenCalledWith(0x1234, 1);
  expect(processKernelWorkerRequest(handle)).not.toHaveBeenCalled();
  expect(drainProcessOutput).not.toHaveBeenCalled();
});

test("requestPipeStateSync clears an in-flight request when worker owner changes during sync", async () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    ownerPid: number;
    generation: number;
    waitingInWorkerStatus: number | null;
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    ownerPid: 1,
    generation: 1,
    waitingInWorkerStatus: null,
  });
  let currentPid = 1;
  let handle: ReturnType<typeof makeHandle>;
  const kernelWorkerRequest = vi.fn(async () => {
    currentPid = 4;
    return {
      type: "pipe-state-synced",
      kernelState: handle.kernelState,
    };
  });
  const appendRuntimeLifecycleEvent = vi.fn();
  const drainProcessOutput = vi.fn(() => true);
  handle = makeHandle({
    pid: 1,
    vmPtr: 0x1234,
    kernelWorkerRequest,
  });
  handle.kernel.supervisor_process_owner_mismatch = vi.fn((_vmPtr: number, runtimePid: number) =>
    currentPid === runtimePid ? 0 : 1
  );
  const clearStateSync = vi.fn(() => 0);
  handle.kernel.supervisor_state_sync_clear = clearStateSync;
  addProcessWorker(handle, worker);
  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent,
    drainProcessOutput,
    drainHiddenProcessOutput: vi.fn(() => true),
    threadWorkerStates,
    retryHostBlockedWorker: vi.fn(),
  });

  await helpers.requestPipeStateSync(handle, true);

  expect(kernelWorkerRequest).toHaveBeenCalledTimes(1);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    handle.runtimeId,
    "pipe-sync-owner-mismatch-defer:1:after-run",
  );
  expect(clearStateSync).toHaveBeenCalledTimes(1);
  expect(drainProcessOutput).not.toHaveBeenCalled();
});
