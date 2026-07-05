import { addKernelShareMember, addKernelShareSharedOfdId, kernelShareHasSharedOfdId, kernelShareMembers, kernelShareSharedOfdIdList, processKernelShareGroup, setProcessKernelShareGroup } from "../../../../src/worker/kernel-share.js";
import { expect, test, vi } from "vite-plus/test";
import { createWorkerOwnedStateSyncHandler as createRawWorkerOwnedStateSyncHandler } from "../../../../src/worker/message-handler/status/owned-state-sync.js";
import { setProcessKernelWorkerRequest } from "../../../../src/worker/process-kernel-worker-request-state.js";
import {
  getHandleForkSharedSync,
  getProcessKernelShareForkSharedSync,
  setProcessKernelShareForkSharedSync,
} from "../../../../src/worker/fork-shared-sync-state.js";
import {
  getThreadOwnerAsyncStatus,
  setThreadHostIdentity,
} from "../../../../src/worker/thread-host-state.js";
import {
  isProcessMemfsDirty,
  setProcessMemfsDirty,
} from "../../../../src/worker/process-memfs-state.js";
import { setProcessExitInProgress } from "../../../../src/worker/process-lifecycle-state.js";

function seedKernelWorkerRequestFixture(handle: unknown): void {
  const fixture = handle as { kernelWorkerRequest?: unknown };
  if (!Object.prototype.hasOwnProperty.call(fixture, "kernelWorkerRequest")) {
    return;
  }
  const kernelWorkerRequest = fixture.kernelWorkerRequest;
  delete fixture.kernelWorkerRequest;
  setProcessKernelWorkerRequest(handle as never, kernelWorkerRequest as never);
}

function createWorkerOwnedStateSyncHandler(
  deps: Parameters<typeof createRawWorkerOwnedStateSyncHandler>[0],
): ReturnType<typeof createRawWorkerOwnedStateSyncHandler> {
  const handler = createRawWorkerOwnedStateSyncHandler(deps);
  return ((request: Parameters<ReturnType<typeof createRawWorkerOwnedStateSyncHandler>>[0]) => {
    seedKernelWorkerRequestFixture(request.handle);
    return handler(request);
  }) as ReturnType<typeof createRawWorkerOwnedStateSyncHandler>;
}

function makeKernelState() {
  return {
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
    pipeUsed: 0n,
    netBufBase: 0,
    netBufUsed: 0n,
    sigSlotsPtr: 0,
    processPendingPtr: 0,
    processRuntimeStatePtr: 0,
    timeStatePtr: 0,
  };
}

const supervisorPipeControlKernelStubs = {
  supervisor_fork_shared_peer_flag_compatible: () => 1,
  supervisor_fork_shared_peer_flag_incompatible: () => 1 << 1,
  supervisor_fork_shared_peer_flag_pipe_peer: () => 1 << 2,
  supervisor_fork_shared_held_ofd_track_flag_shared: () => 1,
  supervisor_fork_shared_held_ofd_track_flag_host_stdio_pipe: () => 1 << 1,
  supervisor_fork_shared_held_ofd_track_flag_stdio_fd: () => 1 << 2,
  supervisor_fork_shared_held_ofd_track_flag_fd_entry_source: () => 1 << 3,
  supervisor_fork_shared_sync_target_action_reject: () => 0,
  supervisor_fork_shared_sync_target_action_defer: () => 1,
  supervisor_fork_shared_sync_target_action_use: () => 2,
  supervisor_fork_shared_ofd_action_include: () => 1,
  supervisor_fork_shared_ofd_action_add_shared: () => 1 << 1,
  supervisor_fork_shared_ofd_action_rehydrate: () => 1 << 2,
  supervisor_fork_shared_held_ofd_track_action(this: {
    abi_max_fds?: () => number;
    abi_fd_type_file?: () => number;
    kernel_ofd_fd_type?: (ofdId: number) => number;
  }, ofdId: number, flags: number) {
    if (ofdId <= 0 || ofdId >= (this.abi_max_fds?.() ?? 0)) {
      return 0;
    }
    if ((flags & (1 << 1)) !== 0) {
      return 0;
    }
    if ((flags & (1 | (1 << 2) | (1 << 3))) !== 0) {
      return 1;
    }
    return this.kernel_ofd_fd_type?.(ofdId) === this.abi_fd_type_file?.() ? 0 : 1;
  },
  supervisor_fork_shared_ofd_identity_key: (ofdId: number) => (ofdId > 0 ? BigInt(ofdId) : 0n),
  supervisor_fork_shared_ofd_publish_action: (ofdId: number, peerFlags: number) => (
    ofdId > 0 && (peerFlags & (1 << 1)) === 0 ? 1 : 0
  ),
  supervisor_fork_shared_effect_sync_action: (
    hasEffect: number,
    hasTrackedState: number,
    sourceHoldsRegularFile: number,
    regularFileHasPeer: number,
  ) => (
    hasEffect !== 0
      && hasTrackedState !== 0
      && (sourceHoldsRegularFile === 0 || regularFileHasPeer !== 0)
      ? 1
      : 0
  ),
  supervisor_owned_state_sync_action_none: () => 0,
  supervisor_owned_state_sync_action_mark_process_memfs_dirty: () => 1,
  supervisor_owned_state_sync_action_sync_fork_shared: () => 2,
  supervisor_owned_state_sync_action_sync_epoll_dependent_ofds: () => 4,
  supervisor_owned_state_sync_action_sync_socket: () => 8,
  supervisor_owned_state_sync_action_wake_socket_waiters: () => 16,
  supervisor_owned_state_sync_action_sync_process_memfs: () => 32,
  supervisor_owned_state_sync_action_sync_epoll: () => 64,
  supervisor_owned_state_sync_action_retry_epoll_waiters: () => 128,
  supervisor_owned_state_sync_flag_running_worker: () => 1,
  supervisor_owned_state_sync_flag_worker_owns_authoritative_state: () => 2,
  supervisor_owned_state_sync_flag_kernel_worker_available: () => 4,
  supervisor_owned_state_sync_flag_epoll_state_kernel_owns: () => 8,
  supervisor_owned_state_sync_flag_fork_shared_effect_requested: () => 16,
  supervisor_owned_state_sync_flag_active_vfork_child_owner_mismatch: () => 32,
  supervisor_owned_state_sync_flag_process_memfs_dirty: () => 64,
  supervisor_owned_state_sync_flag_held_epoll_ofd: () => 128,
  supervisor_owned_state_sync_flag_wrote_or_drained_pipe: () => 256,
  supervisor_owned_state_sync_action(this: {
    abi_status_ok?: () => number;
    abi_status_vfork_wait?: () => number;
    abi_status_execve_spawn?: () => number;
    abi_status_clone_spawn?: () => number;
  }, status: number, completedSyncEffects: number, flags: number) {
    const syncEffectPipeState = 1 << 0;
    const syncEffectSocketState = 1 << 1;
    const syncEffectEpollState = 1 << 2;
    const syncEffectEpollDependentOfdState = 1 << 3;
    const syncEffectProcessMemfs = 1 << 7;
    const runningWorker = (flags & 1) !== 0;
    const workerOwnsAuthoritativeState = (flags & 2) !== 0;
    const kernelWorkerAvailable = (flags & 4) !== 0;
    const epollStateKernelOwns = (flags & 8) !== 0;
    const forkSharedEffectRequested = (flags & 16) !== 0;
    const activeVforkChildOwnerMismatch = (flags & 32) !== 0;
    const processMemfsDirty = (flags & 64) !== 0;
    const heldEpollOfd = (flags & 128) !== 0;
    const wroteOrDrainedPipe = (flags & 256) !== 0;
    const hasPipeEffect = (completedSyncEffects & syncEffectPipeState) !== 0;
    const hasSocketEffect = (completedSyncEffects & syncEffectSocketState) !== 0;
    const hasEpollEffect = (completedSyncEffects & syncEffectEpollState) !== 0;
    const hasEpollDependentOfdEffect = (completedSyncEffects & syncEffectEpollDependentOfdState) !== 0;
    const hasProcessMemfsEffect = (completedSyncEffects & syncEffectProcessMemfs) !== 0;
    const okStatus = this.abi_status_ok?.() ?? 0;
    let action = 0;
    const markProcessMemfsDirty = runningWorker && hasProcessMemfsEffect;
    if (markProcessMemfsDirty) {
      action |= 1;
    }
    if (status === okStatus && hasSocketEffect) {
      action |= 16;
    }
    if (
      activeVforkChildOwnerMismatch
      || status === this.abi_status_vfork_wait?.()
      || status === this.abi_status_execve_spawn?.()
      || status === this.abi_status_clone_spawn?.()
    ) {
      return action;
    }
    const shouldSyncForkShared = workerOwnsAuthoritativeState
      && kernelWorkerAvailable
      && forkSharedEffectRequested
      && status === okStatus;
    if (shouldSyncForkShared) {
      action |= 2;
    }
    if (runningWorker && epollStateKernelOwns && hasEpollDependentOfdEffect) {
      action |= 4;
    }
    if (shouldSyncForkShared && runningWorker && kernelWorkerAvailable && hasSocketEffect) {
      action |= 8;
    }
    if (
      runningWorker
      && kernelWorkerAvailable
      && (processMemfsDirty || markProcessMemfsDirty)
      && (status !== okStatus || hasPipeEffect)
    ) {
      action |= 32;
    }
    if (runningWorker && epollStateKernelOwns && hasEpollEffect && heldEpollOfd) {
      action |= 64;
      if (!wroteOrDrainedPipe) {
        action |= 128;
      }
    }
    return action;
  },
  supervisor_fork_shared_sync_target_action: (
    invalidPid: number,
    runningWorker: number,
    vforkReadyWorker: number,
    hasHostBlockedWorker: number,
    rpcPending: number,
    processExit: number,
    registeredProcessExit: number,
    registeredMismatch: number,
    incompatibleOfd: number,
    requireHostBlocked: number,
    includeMemfs: number,
    runtimeNetworkState: number,
  ) => {
    if (
      invalidPid !== 0
      || processExit !== 0
      || registeredProcessExit !== 0
      || registeredMismatch !== 0
      || incompatibleOfd !== 0
      || (includeMemfs !== 0 && runtimeNetworkState !== 0)
    ) {
      return 0;
    }
    if (
      runningWorker !== 0
      || vforkReadyWorker !== 0
      || rpcPending !== 0
      || (requireHostBlocked !== 0 && hasHostBlockedWorker === 0)
    ) {
      return 1;
    }
    return 2;
  },
  supervisor_fork_shared_syncable_ofd_action: (
    syncable: number,
    sourceConflicting: number,
    allowed: number,
    regularFile: number,
    sourcePublished: number,
    runtimeNetworkSocket: number,
  ) => (
    syncable !== 0
      && sourceConflicting === 0
      && allowed !== 0
      && runtimeNetworkSocket === 0
      && (regularFile === 0 || sourcePublished !== 0)
      ? 1
      : 0
  ),
  supervisor_fork_shared_delta_source_ofd_action: (
    allowed: number,
    sourcePublished: number,
    sourceHeld: number,
    pipePeer: number,
  ) => (
    allowed === 0
      ? 0
      : sourcePublished !== 0
        ? 1
        : sourceHeld !== 0 && pipePeer !== 0
          ? 1 | (1 << 1)
          : 0
  ),
  supervisor_fork_shared_tracked_source_ofd_action: (
    kernelShared: number,
    sourceFdEntry: number,
    syncable: number,
    typeNone: number,
    incompatiblePeer: number,
  ) => (
    syncable !== 0
      && typeNone === 0
      && (kernelShared !== 0 || sourceFdEntry !== 0)
      && (sourceFdEntry !== 0 || incompatiblePeer === 0)
      ? 1
      : 0
  ),
  supervisor_fork_shared_pipe_peer_ofd_action: (stale: number, syncable: number) => (
    stale === 0 && syncable !== 0 ? 1 | (1 << 1) : 0
  ),
  supervisor_fork_shared_rehydrate_ofd_action: (
    active: number,
    sourceHeld: number,
    allowed: number,
  ) => (
    active !== 0 && sourceHeld === 0 && allowed !== 0 ? 1 << 2 : 0
  ),
  supervisor_process_holds_regular_file_ofd(this: {
    abi_max_fds?: () => number;
    abi_fd_type_file?: () => number;
    process_fd_ofd_id?: (vmPtr: number, fd: number) => number;
    kernel_ofd_fd_type?: (ofdId: number) => number;
  }, vmPtr: number, ofdId: number) {
    const fileType = this.abi_fd_type_file?.();
    if (vmPtr === 0 || ofdId === 0 || fileType === undefined) {
      return 0;
    }
    const maxFds = this.abi_max_fds?.() ?? 0;
    for (let fd = 0; fd < maxFds; fd += 1) {
      if (this.process_fd_ofd_id?.(vmPtr, fd) === ofdId) {
        return this.kernel_ofd_fd_type?.(ofdId) === fileType ? 1 : 0;
      }
    }
    return 0;
  },
  supervisor_process_ofd_for_fd(this: {
    abi_max_fds?: () => number;
    process_fd_ofd_id?: (vmPtr: number, fd: number) => number;
  }, vmPtr: number, fd: number) {
    const maxFds = this.abi_max_fds?.() ?? 0;
    if (vmPtr === 0 || fd < 0 || fd >= maxFds) {
      return 0;
    }
    const ofdId = this.process_fd_ofd_id?.(vmPtr, fd) ?? 0;
    return ofdId > 0 && ofdId < maxFds ? ofdId : 0;
  },
  supervisor_process_fd_flags_for_fd(this: {
    abi_max_fds?: () => number;
    process_fd_flags?: (vmPtr: number, fd: number) => number;
  }, vmPtr: number, fd: number) {
    const maxFds = this.abi_max_fds?.() ?? 0;
    return vmPtr !== 0 && fd >= 0 && fd < maxFds
      ? this.process_fd_flags?.(vmPtr, fd) ?? 0
      : 0;
  },
  supervisor_ofd_is_regular_file(this: {
    abi_max_fds?: () => number;
    abi_fd_type_file?: () => number;
    kernel_ofd_fd_type?: (ofdId: number) => number;
  }, ofdId: number) {
    const fileType = this.abi_fd_type_file?.();
    return ofdId > 0
      && ofdId < (this.abi_max_fds?.() ?? 0)
      && fileType !== undefined
      && this.kernel_ofd_fd_type?.(ofdId) === fileType
      ? 1
      : 0;
  },
  supervisor_pipe_sync_ofd_candidate(this: {
    abi_max_fds?: () => number;
    abi_fd_type_pipe?: () => number;
    abi_fd_type_pty_master?: () => number;
    abi_fd_type_pty_slave?: () => number;
    kernel_ofd_fd_type?: (ofdId: number) => number;
  }, ofdId: number) {
    if (ofdId <= 0 || ofdId >= (this.abi_max_fds?.() ?? 0)) {
      return 0;
    }
    const fdType = this.kernel_ofd_fd_type?.(ofdId);
    return fdType !== undefined
      && (
        fdType === this.abi_fd_type_pipe?.()
        || fdType === this.abi_fd_type_pty_master?.()
        || fdType === this.abi_fd_type_pty_slave?.()
      )
      ? 1
      : 0;
  },
  supervisor_process_pipe_slot_for_fd(this: {
    abi_max_fds?: () => number;
    abi_max_pipes?: () => number;
    abi_fd_type_pipe?: () => number;
    process_fd_ofd_id?: (vmPtr: number, fd: number) => number;
    kernel_ofd_fd_type?: (ofdId: number) => number;
    kernel_ofd_ino?: (ofdId: number) => number;
  }, vmPtr: number, fd: number) {
    const maxFds = this.abi_max_fds?.() ?? 0;
    const ofdId = this.process_fd_ofd_id?.(vmPtr, fd) ?? 0;
    if (ofdId <= 0 || ofdId >= maxFds) {
      return 0;
    }
    if (this.kernel_ofd_fd_type?.(ofdId) !== this.abi_fd_type_pipe?.()) {
      return 0;
    }
    const pipeSlot = this.kernel_ofd_ino?.(ofdId) ?? 0;
    return pipeSlot > 0 && pipeSlot < (this.abi_max_pipes?.() ?? 0) ? pipeSlot : 0;
  },
  supervisor_process_pipe_ofd_for_fd(this: {
    process_fd_ofd_id?: (vmPtr: number, fd: number) => number;
    supervisor_process_pipe_slot_for_fd?: (vmPtr: number, fd: number) => number;
  }, vmPtr: number, fd: number) {
    const ofdId = this.process_fd_ofd_id?.(vmPtr, fd) ?? 0;
    return ofdId > 0 && this.supervisor_process_pipe_slot_for_fd?.(vmPtr, fd) !== 0 ? ofdId : 0;
  },
  supervisor_process_pipe_host_fd_for_fd(this: {
    process_fd_ofd_id?: (vmPtr: number, fd: number) => number;
    supervisor_process_pipe_slot_for_fd?: (vmPtr: number, fd: number) => number;
    kernel_ofd_host_fd?: (ofdId: number) => number;
  }, vmPtr: number, fd: number) {
    const ofdId = this.process_fd_ofd_id?.(vmPtr, fd) ?? 0;
    return ofdId > 0 && this.supervisor_process_pipe_slot_for_fd?.(vmPtr, fd) !== 0
      ? this.kernel_ofd_host_fd?.(ofdId) ?? -1
      : -1;
  },
  supervisor_process_pipe_used_mask: () => 1n,
  supervisor_process_pipe_read_open_count: () => 0,
  supervisor_process_pipe_write_open_count: () => 0,
};

function seedThreadIdentity(thread: { ownerPid: number; generation: number }): void {
  setThreadHostIdentity(thread as never, thread.ownerPid, thread.generation);
}

test("socket state sync runs before fork-shared OFD sync when both effects are present", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
    ownerAsyncStatus: null,
  };
  seedThreadIdentity(thread);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  const order: string[] = [];
  const handle = {
    pid: 7,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
    kernelState: makeKernelState(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      abi_status_ok: () => 0,
      abi_max_fds: () => 1,
      abi_max_pipes: () => 1,
      abi_max_threads: () => 1,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_pipe: () => 6,
      abi_sys_socket: () => 198n,
      thread_a7: () => 198n,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: () => 0,
      kernel_ofd_fd_type: () => 0,
      kernel_ofd_ino: () => 0,
      kernel_ofd_host_fd: () => 0,
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  const peer = {
    ...handle,
    vmPtr: 0x5678,
    kernel: {
      ...supervisorPipeControlKernelStubs,
      ...handle.kernel,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 0 ? 2 : 0),
    },
  };
  addKernelShareMember(handle);
  addKernelShareMember(handle, peer);
  const continueAfterOwnedStateSync = vi.fn(() => {
    order.push("continue");
  });
  const deps = {
    processHandles: new Map([[handle.pid, handle]]),
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    pumpProcess: vi.fn(),
    requestEpollDependentOfdStateSync: vi.fn(async () => {
      order.push("epoll-ofd");
    }),
    requestEpollStateSync: vi.fn(async () => {
      order.push("epoll");
    }),
    requestSocketStateSync: vi.fn(async () => {
      order.push("socket");
    }),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncEffectEpollDependentOfdState: 8,
    syncEffectEpollState: 4,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    syncEffectSocketState: 2,
    syncForkSharedStateViaKernelWorker: vi.fn(async () => {
      order.push("fork-shared");
    }),
  };
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    completedSyncEffects: 66,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(true);
  await vi.waitFor(() => {
    expect(continueAfterOwnedStateSync).toHaveBeenCalled();
  });
  expect(order).toEqual(["socket", "fork-shared", "continue"]);
});

test("fork-shared OFD sync accepts an owned worker status after running slot is cleared", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
  };
  seedThreadIdentity(thread);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  const handle = {
    runtimeId: 7,
    pid: 7,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
    kernelState: makeKernelState(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      abi_status_ok: () => 0,
      abi_max_fds: () => 4,
      abi_max_pipes: () => 1,
      abi_max_threads: () => 1,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_pipe: () => 6,
      abi_sys_dup3: () => 24n,
      thread_a7: () => 24n,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 1 ? 2 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 2 ? 6 : 0),
      kernel_ofd_ino: () => 1,
      kernel_ofd_host_fd: () => -2,
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  const peer = {
    ...handle,
    vmPtr: 0x5678,
    kernel: {
      ...supervisorPipeControlKernelStubs,
      ...handle.kernel,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 0 ? 2 : 0),
    },
  };
  addKernelShareMember(handle);
  addKernelShareMember(handle, peer);
  const continueAfterOwnedStateSync = vi.fn();
  let resolveSync!: () => void;
  const syncForkSharedStateViaKernelWorker = vi.fn(() => new Promise<void>((resolve) => {
    resolveSync = resolve;
  }));
  const deps = {
    processHandles: new Map([[handle.pid, handle]]),
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    pumpProcess: vi.fn(),
    requestPipeStateSync: vi.fn(async () => {}),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncEffectEpollDependentOfdState: 8,
    syncEffectEpollState: 4,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    syncEffectSocketState: 2,
    syncForkSharedStateViaKernelWorker,
  };
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    completedSyncEffects: 64,
    isRunningWorker: false,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(true);
  await vi.waitFor(() => {
    expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalled();
  });
  expect(getThreadOwnerAsyncStatus(thread as never)).toBe(0);
  expect(getHandleForkSharedSync(handle as never)).toBeNull();
  expect(getProcessKernelShareForkSharedSync(handle as never)).toBeNull();
  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    7,
    "fork-shared-sync-local:7:src=0x1234:targets=none",
  );
  expect(continueAfterOwnedStateSync).not.toHaveBeenCalled();
  resolveSync();
  await vi.waitFor(() => {
    expect(continueAfterOwnedStateSync).toHaveBeenCalled();
  });
  expect(getThreadOwnerAsyncStatus(thread as never)).toBeNull();
  expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalledWith(handle, [], {
    cause: { kind: "syscall", nr: -1n, name: "owned-state-empty-fork-shared-sync" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: true,
    sourceVmPtr: 0x1234,
  });
});

test("kernel completed fork-shared effect syncs available parent targets", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
  };
  seedThreadIdentity(thread);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  const handle = {
    pid: 7,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    runningWorker: worker,
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set([3]),
    },
    hostStdio: { refCount: 0, stdinPipe: 0, stdoutPipe: 0, stderrPipe: 0 },
    kernelState: makeKernelState(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      abi_status_ok: () => 0,
      abi_max_fds: () => 16,
      abi_max_pipes: () => 16,
      abi_max_threads: () => 1,
      abi_fd_type_file: () => 1,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_pipe: () => 6,
      abi_sys_dup3: () => 24n,
      thread_a7: () => 24n,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 2 ? 10 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 10 ? 1 : ofdId === 3 ? 6 : 0),
      kernel_ofd_ino: (ofdId: number) => (ofdId === 10 ? 22 : ofdId === 3 ? 5 : 0),
      kernel_ofd_host_fd: (ofdId: number) => (ofdId === 3 ? -2 : 0),
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  const parent = {
    pid: 3,
    vmPtr: 0x5678,
    runningWorker: null,
    exitInProgress: false,
    workers: new Set(),
    hostStdio: { refCount: 0, stdinPipe: 0, stdoutPipe: 0, stderrPipe: 0 },
    kernel: {
      ...supervisorPipeControlKernelStubs,
      ...handle.kernel,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 4 ? 7 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 7 ? 6 : 0),
      kernel_ofd_ino: (ofdId: number) => (ofdId === 7 ? 5 : 0),
      kernel_ofd_host_fd: (ofdId: number) => (ofdId === 7 ? 2 : 0),
    },
  };
  addKernelShareMember(handle);
  addKernelShareMember(handle, parent);
  const continueAfterOwnedStateSync = vi.fn();
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const deps = {
    processHandles: new Map([
      [7, handle],
      [3, parent],
    ]),
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    failProcess: vi.fn(),
    pumpProcess: vi.fn(),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncEffectEpollDependentOfdState: 8,
    syncEffectEpollState: 4,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    syncEffectSocketState: 2,
    syncForkSharedStateViaKernelWorker,
  };
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    completedSyncEffects: 64,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(true);
  await vi.waitFor(() => {
    expect(continueAfterOwnedStateSync).toHaveBeenCalled();
  });
  expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalledWith(handle, [parent], {
    cause: { kind: "syscall", nr: -1n, name: "owned-state-fork-shared-sync" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: true,
    sourceVmPtr: 0x1234,
  });
});

test("fcntl F_SETFL on an unshared OFD does not request fork-shared sync", () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
  };
  seedThreadIdentity(thread);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  const handle = {
    pid: 7,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    runningWorker: worker,
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set([8]),
    },
    hostStdio: { refCount: 0, stdinPipe: 0, stdoutPipe: 0, stderrPipe: 0 },
    kernelState: makeKernelState(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      abi_status_ok: () => 0,
      abi_max_fds: () => 16,
      abi_max_pipes: () => 16,
      abi_max_threads: () => 1,
      abi_fd_type_file: () => 1,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_pipe: () => 6,
      abi_sys_fcntl: () => 25n,
      thread_a7: () => 25n,
      thread_a1: () => 4n,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 3 ? 8 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 8 ? 1 : 0),
      kernel_ofd_ino: (ofdId: number) => (ofdId === 8 ? 21 : 0),
      kernel_ofd_host_fd: () => -1,
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  const parent = {
    pid: 3,
    vmPtr: 0x5678,
    runningWorker: null,
    exitInProgress: false,
    workers: new Set(),
    hostStdio: { refCount: 0, stdinPipe: 0, stdoutPipe: 0, stderrPipe: 0 },
    kernel: {
      ...supervisorPipeControlKernelStubs,
      ...handle.kernel,
      process_fd_ofd_id: () => 0,
    },
  };
  addKernelShareMember(handle);
  addKernelShareMember(handle, parent);
  const continueAfterOwnedStateSync = vi.fn();
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const deps = {
    processHandles: new Map([
      [7, handle],
      [3, parent],
    ]),
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    failProcess: vi.fn(),
    pumpProcess: vi.fn(),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncEffectEpollDependentOfdState: 8,
    syncEffectEpollState: 4,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    syncEffectSocketState: 2,
    syncForkSharedStateViaKernelWorker,
  };
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    completedSyncEffects: 64,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(false);
  expect(syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
});

test("kernel pipe data-plane effect does not request fork-shared sync", () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
  };
  seedThreadIdentity(thread);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  const handle = {
    pid: 7,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    runningWorker: worker,
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set([3]),
    },
    hostStdio: { refCount: 0, stdinPipe: 0, stdoutPipe: 5, stderrPipe: 0 },
    kernelState: makeKernelState(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      abi_status_ok: () => 0,
      abi_max_fds: () => 16,
      abi_max_pipes: () => 16,
      abi_max_threads: () => 1,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_file: () => 1,
      abi_fd_type_pipe: () => 6,
      abi_fd_type_socket: () => 8,
      abi_sys_write: () => 64n,
      thread_a7: () => 64n,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 1 ? 3 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 3 ? 6 : 0),
      kernel_ofd_ino: (ofdId: number) => (ofdId === 3 ? 5 : 0),
      kernel_ofd_host_fd: (ofdId: number) => (ofdId === 3 ? -2 : 0),
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  addKernelShareMember(handle);
  const continueAfterOwnedStateSync = vi.fn();
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const deps = {
    processHandles: new Map([[7, handle]]),
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    failProcess: vi.fn(),
    pumpProcess: vi.fn(),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncEffectEpollDependentOfdState: 8,
    syncEffectEpollState: 4,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    syncEffectSocketState: 2,
    syncForkSharedStateViaKernelWorker,
  };
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    completedSyncEffects: 1,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(false);
  expect(syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
});

test("fork-shared OFD sync skips exiting peer targets", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
  };
  seedThreadIdentity(thread);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  const handle = {
    pid: 7,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    runningWorker: null,
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
    kernelState: makeKernelState(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      abi_status_ok: () => 0,
      abi_max_fds: () => 4,
      abi_max_pipes: () => 1,
      abi_max_threads: () => 1,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_pipe: () => 6,
      abi_sys_dup3: () => 24n,
      thread_a7: () => 24n,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 1 ? 2 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 2 ? 6 : 0),
      kernel_ofd_ino: () => 1,
      kernel_ofd_host_fd: () => -2,
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  const exitingPeer = {
    pid: 8,
    runningWorker: null,
    workers: new Set(),
  };
  setProcessExitInProgress(exitingPeer as never, true);
  addKernelShareMember(handle);
  addKernelShareMember(handle, exitingPeer);
  const continueAfterOwnedStateSync = vi.fn();
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const deps = {
    processHandles: new Map([
      [7, handle],
      [8, exitingPeer],
    ]),
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    failProcess: vi.fn(),
    pumpProcess: vi.fn(),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncEffectEpollDependentOfdState: 8,
    syncEffectEpollState: 4,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    syncEffectSocketState: 2,
    syncForkSharedStateViaKernelWorker,
  };
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    completedSyncEffects: 64,
    isRunningWorker: false,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(true);
  await vi.waitFor(() => {
    expect(continueAfterOwnedStateSync).toHaveBeenCalled();
  });
  expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalledWith(handle, [], {
    cause: { kind: "syscall", nr: -1n, name: "owned-state-empty-fork-shared-sync" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: true,
    sourceVmPtr: 0x1234,
  });
});

test("fork-shared OFD sync keeps a quiescent parent reader target for a method-child writer", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
  };
  seedThreadIdentity(thread);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  const handle = {
    pid: 7,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    runningWorker: worker,
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set([8]),
    },
    kernelState: makeKernelState(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      abi_status_ok: () => 0,
      abi_max_fds: () => 16,
      abi_max_pipes: () => 16,
      abi_max_threads: () => 1,
      abi_fd_type_file: () => 1,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_pipe: () => 6,
      abi_sys_write: () => 64n,
      thread_a7: () => 64n,
      thread_a0: () => 5n,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 5 ? 8 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 8 ? 6 : 0),
      kernel_ofd_ino: (ofdId: number) => (ofdId === 8 ? 6 : 0),
      kernel_ofd_host_fd: (ofdId: number) => (ofdId === 8 ? -11 : 0),
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  const parent = {
    pid: 3,
    vmPtr: 0x5678,
    runningWorker: null,
    exitInProgress: false,
    workers: new Set(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      ...handle.kernel,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 4 ? 7 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 7 ? 6 : 0),
      kernel_ofd_ino: (ofdId: number) => (ofdId === 7 ? 6 : 0),
      kernel_ofd_host_fd: (ofdId: number) => (ofdId === 7 ? 10 : 0),
    },
  };
  addKernelShareMember(handle);
  addKernelShareMember(handle, parent);
  const continueAfterOwnedStateSync = vi.fn();
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const deps = {
    processHandles: new Map([
      [7, handle],
      [3, parent],
    ]),
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    failProcess: vi.fn(),
    pumpProcess: vi.fn(),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncEffectEpollDependentOfdState: 8,
    syncEffectEpollState: 4,
    syncEffectForkSharedOfdState: 64,
    syncEffectPipeState: 1,
    syncEffectProcessMemfs: 128,
    syncEffectSocketState: 2,
    syncForkSharedStateViaKernelWorker,
  };
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    completedSyncEffects: 64,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(true);
  await vi.waitFor(() => {
    expect(continueAfterOwnedStateSync).toHaveBeenCalled();
  });
  expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalledWith(handle, [parent], {
    cause: { kind: "syscall", nr: -1n, name: "owned-state-fork-shared-sync" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: true,
    sourceVmPtr: 0x1234,
  });
});

test("queued fork-shared OFD sync clears both handle and share promises", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
  };
  seedThreadIdentity(thread);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  let resolveActiveSync!: () => void;
  const activeSync = new Promise<void>((resolve) => {
    resolveActiveSync = resolve;
  });
  const handle = {
    pid: 7,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set([2]),
    },
    kernelState: makeKernelState(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      abi_status_ok: () => 0,
      abi_max_fds: () => 4,
      abi_max_pipes: () => 1,
      abi_max_threads: () => 1,
      abi_fd_type_file: () => 3,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_pipe: () => 6,
      abi_sys_dup: () => 32n,
      thread_a7: () => 32n,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 0 ? 2 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 2 ? 3 : 0),
      kernel_ofd_ino: () => 0,
      kernel_ofd_host_fd: () => 0,
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  setProcessKernelShareForkSharedSync(handle as never, activeSync);
  addKernelShareMember(handle);
  const peer = {
    pid: 3,
    vmPtr: 0x5678,
    runningWorker: null,
    exitInProgress: false,
    kernel: handle.kernel,
  } as never;
  setProcessKernelShareGroup(peer, processKernelShareGroup(handle));
  addKernelShareMember(handle, peer);
  const continueAfterOwnedStateSync = vi.fn();
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const deps = {
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    pumpProcess: vi.fn(),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncEffectEpollDependentOfdState: 8,
    syncEffectEpollState: 4,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    syncEffectSocketState: 2,
    syncForkSharedStateViaKernelWorker,
  };
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    completedSyncEffects: 64,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(true);
  await vi.waitFor(() => {
    expect(getHandleForkSharedSync(handle as never)).not.toBeNull();
  });
  expect(getHandleForkSharedSync(handle as never)).toBe(getProcessKernelShareForkSharedSync(handle as never));
  expect(getHandleForkSharedSync(handle as never)).not.toBe(activeSync);

  resolveActiveSync();
  await vi.waitFor(() => {
    expect(continueAfterOwnedStateSync).toHaveBeenCalled();
  });

  expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalledTimes(1);
  expect(getHandleForkSharedSync(handle as never)).toBeNull();
  expect(getProcessKernelShareForkSharedSync(handle as never)).toBeNull();
});

test("queued no-target fork-shared OFD sync waits without replacing the active share promise", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
  };
  seedThreadIdentity(thread);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  let resolveActiveSync!: () => void;
  const activeSync = new Promise<void>((resolve) => {
    resolveActiveSync = resolve;
  });
  const handle = {
    runtimeId: 7,
    pid: 7,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
    kernelState: makeKernelState(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      abi_status_ok: () => 0,
      abi_max_fds: () => 4,
      abi_max_pipes: () => 1,
      abi_max_threads: () => 1,
      abi_fd_type_file: () => 3,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_pipe: () => 6,
      abi_sys_dup: () => 32n,
      thread_a7: () => 32n,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: () => 0,
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 2 ? 3 : 0),
      kernel_ofd_ino: () => 0,
      kernel_ofd_host_fd: () => 0,
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  setProcessKernelShareForkSharedSync(handle as never, activeSync);
  addKernelShareMember(handle);
  const peer = {
    pid: 0,
    vmPtr: 0,
    runningWorker: null,
    exitInProgress: false,
    kernel: handle.kernel,
  } as never;
  setProcessKernelShareGroup(peer, processKernelShareGroup(handle));
  addKernelShareMember(handle, peer);
  const continueAfterOwnedStateSync = vi.fn();
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const deps = {
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    pumpProcess: vi.fn(),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncEffectEpollDependentOfdState: 8,
    syncEffectEpollState: 4,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    syncEffectSocketState: 2,
    syncForkSharedStateViaKernelWorker,
  };
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    completedSyncEffects: 64,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(true);
  expect(getHandleForkSharedSync(handle as never)).toBeNull();
  expect(getProcessKernelShareForkSharedSync(handle as never)).toBe(activeSync);
  expect(syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  await vi.waitFor(() => {
    expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
      7,
      expect.stringContaining("fork-shared-sync-local-queue:"),
    );
  });

  resolveActiveSync();
  await vi.waitFor(() => {
    expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalled();
  });
  expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalledWith(handle, [], {
    cause: { kind: "syscall", nr: -1n, name: "owned-state-empty-fork-shared-sync" },
    deltaOfdIds: true,
    includeMemfs: false,
    sourceVmPtr: thread.vmPtr,
    trackHeldOfds: true,
  });
  await vi.waitFor(() => {
    expect(continueAfterOwnedStateSync).toHaveBeenCalled();
  });
  expect(getHandleForkSharedSync(handle as never)).toBeNull();
  expect(getProcessKernelShareForkSharedSync(handle as never)).toBe(activeSync);
});

test("fork-shared-only pipe effects sync when the source holds tracked shared state", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
  };
  seedThreadIdentity(thread);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  const handle = {
    pid: 7,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set([1]),
    },
    kernelState: makeKernelState(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      abi_status_ok: () => 0,
      abi_max_fds: () => 4,
      abi_max_pipes: () => 1,
      abi_max_threads: () => 1,
      abi_fd_type_file: () => 3,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_pipe: () => 6,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 0 ? 1 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 1 ? 6 : 0),
      kernel_ofd_ino: () => 1,
      kernel_ofd_host_fd: () => 0,
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  addKernelShareMember(handle);
  const continueAfterOwnedStateSync = vi.fn();
  const deps = {
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    pumpProcess: vi.fn(),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncEffectEpollDependentOfdState: 8,
    syncEffectEpollState: 4,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncForkSharedStateViaKernelWorker: vi.fn(async () => {}),
  };
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    completedSyncEffects: 64,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(true);
  await vi.waitFor(() => {
    expect(continueAfterOwnedStateSync).toHaveBeenCalled();
  });
  expect(deps.syncForkSharedStateViaKernelWorker).toHaveBeenCalledTimes(1);
});

test("dirty process memfs syncs before publishing a pipe-status boundary", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
  };
  seedThreadIdentity(thread);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  const order: string[] = [];
  const handle = {
    pid: 7,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
    kernelState: makeKernelState(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      abi_status_ok: () => 0,
      abi_max_fds: () => 1,
      abi_max_pipes: () => 1,
      abi_max_threads: () => 1,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_pipe: () => 6,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: () => 0,
      kernel_ofd_fd_type: () => 0,
      kernel_ofd_ino: () => 0,
      kernel_ofd_host_fd: () => 0,
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  setProcessMemfsDirty(handle as never, true);
  addKernelShareMember(handle);
  const continueAfterOwnedStateSync = vi.fn(() => {
    order.push("continue");
  });
  const syncForkSharedStateViaKernelWorker = vi.fn(async (_handle, _targets, options) => {
    order.push(options?.includeMemfs ? "memfs" : "fork-shared");
  });
  const requestPipeStateSync = vi.fn(async () => {
    order.push("pipe");
  });
  const markRuntimeFsDirty = vi.fn();
  const deps = {
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    pumpProcess: vi.fn(),
    requestPipeStateSync,
    requestEpollDependentOfdStateSync: vi.fn(async () => {
      order.push("epoll-ofd");
    }),
    requestEpollStateSync: vi.fn(async () => {
      order.push("epoll");
    }),
    requestSocketStateSync: vi.fn(async () => {
      order.push("socket");
    }),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncEffectEpollDependentOfdState: 8,
    syncEffectEpollState: 4,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncForkSharedStateViaKernelWorker,
    markRuntimeFsDirty,
  };
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    completedSyncEffects: 1,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(true);
  await vi.waitFor(() => {
    expect(continueAfterOwnedStateSync).toHaveBeenCalled();
  });
  expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalledWith(handle, [], {
    cause: { kind: "syscall", nr: -1n, name: "owned-state-process-memfs-sync" },
    includeMemfs: true,
    memfsOnly: true,
    sourceVmPtr: thread.vmPtr,
    trackHeldOfds: false,
  });
  expect(requestPipeStateSync).not.toHaveBeenCalled();
  expect(markRuntimeFsDirty).toHaveBeenCalledWith(handle);
  expect(isProcessMemfsDirty(handle as never)).toBe(false);
  expect(order).toEqual(["memfs", "continue"]);
});

test("dirty process memfs is deferred on a successful process-memfs-only effect", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
  };
  seedThreadIdentity(thread);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  const handle = {
    pid: 7,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
    kernelState: makeKernelState(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      abi_status_ok: () => 0,
      abi_max_fds: () => 1,
      abi_max_pipes: () => 1,
      abi_max_threads: () => 1,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_pipe: () => 6,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: () => 0,
      kernel_ofd_fd_type: () => 0,
      kernel_ofd_ino: () => 0,
      kernel_ofd_host_fd: () => 0,
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  addKernelShareMember(handle);
  const continueAfterOwnedStateSync = vi.fn();
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const deps = {
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    pumpProcess: vi.fn(),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncEffectEpollDependentOfdState: 8,
    syncEffectEpollState: 4,
    syncEffectForkSharedOfdState: 64,
    syncEffectProcessMemfs: 128,
    syncEffectPipeState: 1,
    syncEffectSocketState: 2,
    syncForkSharedStateViaKernelWorker,
  };
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    completedSyncEffects: 128,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(false);
  expect(continueAfterOwnedStateSync).not.toHaveBeenCalled();
  expect(syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  expect(isProcessMemfsDirty(handle as never)).toBe(true);
});

test("active vfork child pid mismatch defers parent-owned state sync", () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 1,
    generation: 1,
    waitingInWorkerStatus: null,
  };
  seedThreadIdentity(thread);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  const supervisorVforkChildOwnerMismatch = vi.fn(() => 1);
  const handle = {
    pid: 1,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
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
    vforkWorker: worker,
    kernelState: makeKernelState(),
    kernel: {
      ...supervisorPipeControlKernelStubs,
      abi_status_ok: () => 0,
      abi_max_fds: () => 1,
      abi_max_pipes: () => 1,
      abi_max_threads: () => 1,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_pipe: () => 6,
      supervisor_vfork_child_owner_mismatch: supervisorVforkChildOwnerMismatch,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: () => 0,
      kernel_ofd_fd_type: () => 0,
      kernel_ofd_ino: () => 0,
      kernel_ofd_host_fd: () => 0,
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  addKernelShareMember(handle);
  const continueAfterOwnedStateSync = vi.fn();
  const deps = {
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    pumpProcess: vi.fn(),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncEffectEpollDependentOfdState: 8,
    syncEffectEpollState: 4,
    syncEffectForkSharedOfdState: 64,
    syncEffectPipeState: 1,
    syncEffectProcessMemfs: 128,
    syncEffectSocketState: 2,
    syncForkSharedStateViaKernelWorker: vi.fn(async () => {}),
  };
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    completedSyncEffects: 66,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(false);
  expect(supervisorVforkChildOwnerMismatch).toHaveBeenCalledWith(0x1234, 1);
  expect(deps.requestSocketStateSync).not.toHaveBeenCalled();
  expect(deps.syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  expect(continueAfterOwnedStateSync).not.toHaveBeenCalled();
});
