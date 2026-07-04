import { beforeEach, expect, test, vi } from "vite-plus/test";
import {
  createRuntimeStateBridge,
  readPublishedKernelRuntimeState,
} from "../../../../src/bridge/runtime-state.js";
import type { KernelExports, KernelRuntimeState } from "../../../../src/abi.js";
import { createWorkerKernelRpcHelpers as createWorkerKernelRpcHelpersBase } from "../../../../src/worker/kernel-rpc.js";
import { createWorkerKernelRpcBlockedRpcTestHelpers as createWorkerKernelRpcBlockedRpcHelpersBase } from "../../../../src/worker/host-boundary/blocked-rpc-resume.js";
import { createWorkerKernelRpcResumeLifecycleHelpers as createWorkerKernelRpcResumeLifecycleHelpersBase } from "../../../../src/worker/kernel-rpc/lifecycle/resume.js";
import type { ProcessHandle, ThreadWorkerState } from "../../../../src/worker/types.js";
import { installSupervisorBlockedResumeStub } from "../support/supervisor-blocked-resume.js";
import {
  FS_OP_CLOSE,
  FS_OP_EXECVE,
  FS_OP_MKDIR,
  FS_OP_MMAP,
  FS_OP_OPENAT,
  FS_OP_RENAME,
  FS_OP_UNLINK,
  FS_OP_WRITE,
} from "../../../../src/layout/fs-ring.js";

const requestThreadWorkerBlockedResume = vi.fn();

beforeEach(() => {
  requestThreadWorkerBlockedResume.mockClear();
});

function createWorkerKernelRpcHelpers(
  deps: Parameters<typeof createWorkerKernelRpcHelpersBase>[0],
) {
  return createWorkerKernelRpcHelpersBase({
    clearHostBlockedRetryTimer: () => {},
    ...deps,
    requestThreadWorkerBlockedResume,
  });
}

function createWorkerKernelRpcBlockedRpcHelpers(
  deps: Parameters<typeof createWorkerKernelRpcBlockedRpcHelpersBase>[0],
) {
  return createWorkerKernelRpcBlockedRpcHelpersBase({
    ...deps,
    requestThreadWorkerBlockedResume,
  });
}

type ResumeLifecycleDeps = Parameters<typeof createWorkerKernelRpcResumeLifecycleHelpersBase>[0];

function createWorkerKernelRpcResumeLifecycleHelpers(
  deps: Omit<ResumeLifecycleDeps, "settleVisibleProcessOutput">
    & Partial<Pick<ResumeLifecycleDeps, "settleVisibleProcessOutput">>,
) {
  return createWorkerKernelRpcResumeLifecycleHelpersBase({
    settleVisibleProcessOutput: async () => undefined,
    ...deps,
  });
}

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0,
    memfsInodesBytes: 0,
    memfsFirstChildBytes: 0,
    memfsDirEntriesBytes: 0,
    memfsDataBytes: 0,
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

function supervisorFsResponseDispositionTestStubs(
  currentPid: () => number,
  threadStatus: () => number = () => 0,
  execveSpawnStatus: () => number = () => 24,
): Partial<KernelExports> {
  return {
    abi_status_ok: () => 0,
    supervisor_thread_event_kind_backing_store_ready: () => 8,
    supervisor_process_status_is_execve_spawn: (status: number) =>
      status === execveSpawnStatus() ? 1 : 0,
    supervisor_process_status_defers_kernel_state_adoption: () => 0,
    supervisor_fs_completion_flag_memfs_dirty: () => 1,
    supervisor_fs_completion_flag_memfs_publish_ready: () => 2,
    supervisor_fs_completion_flag_ofd_state_changed: () => 4,
    supervisor_fs_completion_flag_regular_file_request: () => 8,
    supervisor_fs_completion_decision: (
      _vmPtr,
      opcode,
      effects,
      result,
      hostMemfsMutated,
      hostOfdStateChanged,
      pendingRingRequest,
      regularFileRequest,
    ) => {
      const signedResult = Number(BigInt.asIntN(64, typeof result === "bigint" ? result : BigInt(result)));
      const fsSucceeded = signedResult >= 0;
      const memfsStateMayChange = (effects & (1 | 2)) !== 0;
      const memfsPublishMayBeReady = (effects & 1) !== 0 || ((effects & 2) !== 0 && opcode === FS_OP_WRITE);
      const ofdStateMayChange = (effects & 4) !== 0;
      let flags = 0;
      if (hostMemfsMutated !== 0 || (fsSucceeded && (memfsStateMayChange || pendingRingRequest !== 0))) {
        flags |= 1;
      }
      if (fsSucceeded && memfsPublishMayBeReady) {
        flags |= 2;
      }
      if (hostOfdStateChanged !== 0 || (fsSucceeded && (ofdStateMayChange || pendingRingRequest !== 0))) {
        flags |= 4;
      }
      if (regularFileRequest !== 0) {
        flags |= 8;
      }
      return flags;
    },
    supervisor_fs_response_flag_adopt_kernel_state: () => 1,
    supervisor_fs_response_flag_apply_guest_memory_writes: () => 2,
    supervisor_fs_response_flag_queue_thread_worker_writes: () => 4,
    supervisor_fs_response_flag_apply_local_metadata: () => 8,
    supervisor_fs_response_flag_clear_tombstones: () => 16,
    supervisor_fs_response_flag_fd_response_may_clear_live_fds: () => 32,
    supervisor_fs_response_flag_store_pending_kernel_state: () => 64,
    supervisor_fs_response_flag_resolved_to_execve_spawn: () => 128,
    supervisor_fs_response_flag_process_owner_matches: () => 256,
    supervisor_fs_local_metadata_action_none: () => 0,
    supervisor_fs_local_metadata_action_tombstone_primary: () => 1,
    supervisor_fs_local_metadata_action_forget_primary: () => 2,
    supervisor_fs_local_metadata_action_forget_secondary: () => 4,
    supervisor_fs_pending_ring_request: () => 0,
    supervisor_fs_pending_regular_file_fd: () => -1,
    supervisor_fs_pending_regular_file_ofd: () => 0,
    supervisor_fs_backing_fill_span_decision: () => 0,
    supervisor_fs_backing_fill_span_offset: () => 0,
    supervisor_fs_backing_fill_span_end: () => 4096,
    supervisor_fs_backing_fill_span_length: () => 4096,
    supervisor_fs_pending_mmap_guest_write_snapshot_len: () => 0,
    supervisor_fs_resident_page_evict_not_found: () => 0,
    supervisor_fs_resident_page_evict_dirty_pinned: () => 1,
    supervisor_fs_resident_page_evict_evicted: () => 2,
    supervisor_fs_resident_page_slot_decision: (existingSlot: number, emptySlot: number, cleanEvictSlot: number) => {
      if (existingSlot >= 0) {
        return existingSlot;
      }
      if (emptySlot >= 0) {
        return emptySlot;
      }
      return cleanEvictSlot >= 0 ? cleanEvictSlot : -1;
    },
    supervisor_fs_resident_page_evict_status: (recordFound: number, dirty: number) => (
      recordFound === 0 ? 0 : dirty !== 0 ? 1 : 2
    ),
    supervisor_fs_response_disposition: (
      _vmPtr,
      runtimePid,
      opcode,
      result,
      ownerAsyncStatus,
      lastStatus,
    ) => {
      const processOwnerMatches = currentPid() <= 0 || runtimePid <= 0 || currentPid() === runtimePid;
      const signedResult = Number(BigInt.asIntN(64, typeof result === "bigint" ? result : BigInt(result)));
      const failedExecveChildOwner = opcode === FS_OP_EXECVE && signedResult < 0 && !processOwnerMatches;
      const spawnStatus = execveSpawnStatus();
      const resolvedToExecveSpawn = opcode === FS_OP_EXECVE
        && spawnStatus !== 0
        && (threadStatus() === spawnStatus || ownerAsyncStatus === spawnStatus || lastStatus === spawnStatus);
      let flags = 0;
      if (processOwnerMatches) {
        flags |= 256;
      }
      if (failedExecveChildOwner) {
        flags |= 64;
      } else {
        flags |= 1 | 2 | 16;
      }
      if (!failedExecveChildOwner && processOwnerMatches) {
        flags |= 8;
      }
      if (!failedExecveChildOwner && opcode !== FS_OP_MMAP) {
        flags |= 4;
      }
      if (opcode === FS_OP_CLOSE || opcode === FS_OP_EXECVE) {
        flags |= 32;
      }
      if (resolvedToExecveSpawn) {
        flags |= 128;
      }
      return flags;
    },
    supervisor_fs_local_metadata_action: (
      opcode,
      effects,
      result,
      hasPrimaryPath,
      hasSecondaryPath,
    ) => {
      const signedResult = Number(BigInt.asIntN(64, typeof result === "bigint" ? result : BigInt(result)));
      if (signedResult < 0 || hasPrimaryPath === 0 || (effects & 1) === 0) {
        return 0;
      }
      if (opcode === FS_OP_UNLINK) {
        return 1;
      }
      if (opcode === FS_OP_OPENAT || opcode === FS_OP_MKDIR) {
        return 2;
      }
      if (opcode === FS_OP_RENAME && hasSecondaryPath !== 0) {
        return 4;
      }
      return 0;
    },
  };
}

function makeHandle(
  kernel: KernelExports,
  runtimeBridge: ProcessHandle["runtimeBridge"],
  worker: Worker,
  overrides: Partial<ProcessHandle> = {},
): ProcessHandle {
  installSupervisorBlockedResumeStub(kernel);
  return {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0x1234,
    guestMemoryPolicyLimitBytes: 0,
    maxMemoryPages: 0,
    forkMaxMemoryPages: 0,
    guestBase: 0,
    worker,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel,
    kernelState: makeKernelState(),
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    ttyPtySlot: 0,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    stdioOwner: null,
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
    stdinQueue: [],
    stdinFlushPromise: null,
    outputBacklog: [],
    debugCommand: "test",
    runtimeBridge,
    sabPageCache: null,
    workers: new Set([worker]),
    vforkReadyWorkers: [],
    runningWorker: null,
    hostBlockedWorkers: new Map(),
    pendingChildSpawnRecords: 0,
    pipeSyncPromise: null,
    epollSyncPromise: null,
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    socketSyncPromise: null,
    networkConnections: new Map(),
    networkClosedFds: new Set(),
    vforkSnapshot: null,
    vforkWorker: null,
    workerDebugLog: [],
    guestWriteWatchRegions: [],
    ...overrides,
  } as ProcessHandle;
}

function createWorkerKernelRpcLocalBlockedRpcHelpers(
  deps: Parameters<typeof createWorkerKernelRpcHelpers>[0],
) {
  return createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: deps.processHandles,
    threadWorkerStates: deps.threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    appendWorkerDebugLog: deps.appendWorkerDebugLog,
    failProcess: deps.failProcess,
    clearHostBlockedRetryTimer: deps.clearHostBlockedRetryTimer,
    handleHostBlockedRetryResult: deps.handleHostBlockedRetryResult,
    requestPipeStateSync: deps.requestPipeStateSync,
    syncBlockedForkSharedStateViaKernelWorker: deps.syncBlockedForkSharedStateViaKernelWorker,
    syncSocketStateViaKernelWorker: deps.syncSocketStateViaKernelWorker,
  });
}

test("resumeWait4ViaKernelWorker does not re-run local wait4 after kernel-worker resume", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const baseState = makeKernelState();
  const resumedState = {
    ...baseState,
    heapPtr: baseState.heapPtr + 0x80,
  };
  const threadResumeWait4 = vi.fn();
  let status = 31;
  const kernel = {
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => resumedState.heapBase,
    kernel_heap_ptr: () => resumedState.heapPtr,
    kernel_ofd_table_ptr: () => resumedState.ofdTablePtr,
    kernel_memfs_ptr: () => resumedState.memfsPtr,
    kernel_memfs_header_bytes: () => resumedState.memfsHeaderBytes,
    kernel_memfs_inodes_bytes: () => resumedState.memfsInodesBytes,
    kernel_memfs_first_child_bytes: () => resumedState.memfsFirstChildBytes,
    kernel_memfs_dir_entries_bytes: () => resumedState.memfsDirEntriesBytes,
    kernel_memfs_data_bytes: () => resumedState.memfsDataBytes,
    kernel_mem_state_ptr: () => resumedState.memStatePtr,
    kernel_epoll_table_ptr: () => resumedState.epollTablePtr,
    kernel_socket_table_ptr: () => resumedState.socketTablePtr,
    kernel_socket_used: () => resumedState.socketUsed,
    kernel_pipe_base: () => resumedState.pipeBase,
    kernel_pipe_used: () => resumedState.pipeUsed,
    kernel_net_buf_base: () => resumedState.netBufBase,
    kernel_net_buf_used: () => resumedState.netBufUsed,
    kernel_sig_slots_ptr: () => resumedState.sigSlotsPtr,
    kernel_process_pending_ptr: () => resumedState.processPendingPtr,
    kernel_process_runtime_state_ptr: () => resumedState.processRuntimeStatePtr,
    kernel_time_state_ptr: () => resumedState.timeStatePtr,
    abi_status_wait4: () => 31,
    thread_status: () => status,
    thread_resume_wait4: threadResumeWait4,
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    kernelState: baseState,
    kernelWorkerRequest: vi.fn(async () => {
      status = 0;
      return {
        type: "wait4-resumed",
        requestId: 0,
        kernelState: resumedState,
      } as const;
    }),
  });
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 31,
    rpcPending: true,
  });

  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

  const handledStatuses: number[] = [];
  const pipeSyncWakeArgs: boolean[] = [];
  const settledHandles: ProcessHandle[] = [];
  const helpers = createWorkerKernelRpcResumeLifecycleHelpers({
    appendWorkerDebugLog: () => {},
    canonicalizeChildLifecycleViaKernelWorker: async () => ({
      childPid: 2,
      ppid: 1,
      nextPid: 3,
      pgid: 1,
      sid: 1,
    }),
    clearVforkSnapshot: () => {},
    drainProcessOutput: () => false,
    enqueueWorker: () => {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleHostBlockedRetryResult: (_handle, _worker, _vmPtr, nextStatus) => {
      handledStatuses.push(nextStatus);
    },
    notifyHostEffectProgress: () => {},
    processHandles: new Map([[1, handle]]),
    requestPipeStateSync: async (_handle, wakeWaiters = false) => {
      pipeSyncWakeArgs.push(wakeWaiters);
    },
    settleVisibleProcessOutput: async (settleHandle) => {
      settledHandles.push(settleHandle);
    },
    threadWorkerStates: threadWorkerStates as never,
  });

  const outcome = await helpers.resumeWait4ViaKernelWorker(handle, worker, 0x1234);

  expect(threadResumeWait4).not.toHaveBeenCalled();
  expect(handledStatuses).toEqual([]);
  expect(pipeSyncWakeArgs).toEqual([true]);
  expect(settledHandles).toEqual([handle]);
  expect(outcome.effects).toBeUndefined();
  expect(outcome.status).toBe(0);
  expect(handle.kernelState.heapPtr).toBe(resumedState.heapPtr);
  expect(readPublishedKernelRuntimeState(runtimeBridge).heapPtr).toBe(resumedState.heapPtr);
});

test("resumeWait4ViaKernelWorker skips RPC when local wait4 already resolved", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const kernel = {
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    abi_status_wait4: () => 31,
    thread_status: () => 0,
  } as unknown as KernelExports;

  const kernelWorkerRequest = vi.fn(async () => {
    throw new Error("kernel-worker resume should be skipped when local wait4 already resolved");
  });
  const handle = makeHandle(kernel, runtimeBridge, worker, {
    kernelWorkerRequest,
  });
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 31,
    rpcPending: true,
  });

  const threadWorkerStates = new WeakMap<Worker, { vmPtr: number; initialized: boolean; waitingInWorkerStatus: number | null }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

  const handledStatuses: number[] = [];
  const helpers = createWorkerKernelRpcResumeLifecycleHelpers({
    appendWorkerDebugLog: () => {},
    canonicalizeChildLifecycleViaKernelWorker: async () => ({
      childPid: 2,
      ppid: 1,
      nextPid: 3,
      pgid: 1,
      sid: 1,
    }),
    clearVforkSnapshot: () => {},
    drainProcessOutput: () => false,
    enqueueWorker: () => {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleHostBlockedRetryResult: (_handle, _worker, _vmPtr, nextStatus) => {
      handledStatuses.push(nextStatus);
    },
    notifyHostEffectProgress: () => {},
    processHandles: new Map([[1, handle]]),
    requestPipeStateSync: async () => {},
    threadWorkerStates: threadWorkerStates as never,
  });

  const outcome = await helpers.resumeWait4ViaKernelWorker(handle, worker, 0x1234);

  expect(kernelWorkerRequest).not.toHaveBeenCalled();
  expect(handledStatuses).toEqual([]);
  expect(outcome.effects).toBeUndefined();
  expect(outcome.status).toBe(0);
  expect(handle.hostBlockedWorkers.get(worker)?.rpcPending).toBe(false);
});

test("resumeVforkExecveParentViaKernelWorker restores the parent before clearing vfork runtime state", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const baseState = makeKernelState();
  const resumedState = {
    ...baseState,
    heapPtr: baseState.heapPtr + 0x80,
  };
  const opLog: string[] = [];
  const kernel = {
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn((
      _heapBase: number,
      heapPtr: number,
    ) => {
      opLog.push(`attach:${heapPtr}`);
    }),
    kernel_heap_base: () => resumedState.heapBase,
    kernel_heap_ptr: () => resumedState.heapPtr,
    kernel_ofd_table_ptr: () => resumedState.ofdTablePtr,
    kernel_memfs_ptr: () => resumedState.memfsPtr,
    kernel_memfs_header_bytes: () => resumedState.memfsHeaderBytes,
    kernel_memfs_inodes_bytes: () => resumedState.memfsInodesBytes,
    kernel_memfs_first_child_bytes: () => resumedState.memfsFirstChildBytes,
    kernel_memfs_dir_entries_bytes: () => resumedState.memfsDirEntriesBytes,
    kernel_memfs_data_bytes: () => resumedState.memfsDataBytes,
    kernel_mem_state_ptr: () => resumedState.memStatePtr,
    kernel_epoll_table_ptr: () => resumedState.epollTablePtr,
    kernel_socket_table_ptr: () => resumedState.socketTablePtr,
    kernel_socket_used: () => resumedState.socketUsed,
    kernel_pipe_base: () => resumedState.pipeBase,
    kernel_pipe_used: () => resumedState.pipeUsed,
    kernel_net_buf_base: () => resumedState.netBufBase,
    kernel_net_buf_used: () => resumedState.netBufUsed,
    kernel_sig_slots_ptr: () => resumedState.sigSlotsPtr,
    kernel_process_pending_ptr: () => resumedState.processPendingPtr,
    kernel_process_runtime_state_ptr: () => resumedState.processRuntimeStatePtr,
    kernel_time_state_ptr: () => resumedState.timeStatePtr,
    abi_max_fds: () => 16,
    abi_max_pipes: () => 16,
    abi_fd_type_pipe: () => 6,
    process_fd_ofd_id: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_ino: () => 0,
    thread_clear_spawn_handoff: (vmPtr: number) => {
      opLog.push(`clear-spawn:${vmPtr}`);
    },
    thread_restore_vfork_parent_without_cleanup: (vmPtr: number, childPid: number) => {
      opLog.push(`restore-vfork:${vmPtr}:${childPid}`);
    },
    thread_resume_vfork_parent: (vmPtr: number, childPid: number) => {
      opLog.push(`resume-vfork:${vmPtr}:${childPid}`);
    },
    thread_clear_vfork_runtime_state: () => {
      opLog.push("clear-vfork");
    },
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    kernelState: baseState,
    kernelWorkerRequest: vi.fn(async () => ({
      type: "vfork-execve-parent-resumed",
      requestId: 0,
      kernelState: resumedState,
    })),
  });

  const helpers = createWorkerKernelRpcResumeLifecycleHelpers({
    appendWorkerDebugLog: () => {},
    canonicalizeChildLifecycleViaKernelWorker: async () => ({
      childPid: 2,
      ppid: 1,
      nextPid: 3,
      pgid: 1,
      sid: 1,
    }),
    clearVforkSnapshot: () => {},
    drainProcessOutput: () => false,
    enqueueWorker: () => {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleHostBlockedRetryResult: () => {},
    notifyHostEffectProgress: () => {},
    processHandles: new Map([[1, handle]]),
    requestPipeStateSync: async () => {},
    threadWorkerStates: new WeakMap() as never,
  });

  await helpers.resumeVforkExecveParentViaKernelWorker(handle, 0x1234, 7);

  expect(opLog).toEqual([
    `attach:${resumedState.heapPtr}`,
    "clear-spawn:4660",
    "restore-vfork:4660:7",
    "clear-vfork",
    `attach:${resumedState.heapPtr}`,
  ]);
  expect(handle.kernelState.heapPtr).toBe(resumedState.heapPtr);
});

test("executeHostBlockedSyscallResume skips kernel-worker shared-state syncs during active vfork child ownership mismatch", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const threadResumeBlockedSyscall = vi.fn();
  const syncBlockedForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const syncSocketStateViaKernelWorker = vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n }));
  const handleHostBlockedRetryResult = vi.fn();
  const supervisorProcessOwnerMismatch = vi.fn(() => 1);
  const kernel = {
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    abi_status_pipe_wait: () => 21,
    abi_status_connect: () => 28,
    abi_status_socket_recv: () => 29,
    abi_status_socket_send: () => 30,
    thread_resume_blocked_syscall: threadResumeBlockedSyscall,
    thread_status: () => 0,
    process_current_pid: () => 4,
    supervisor_process_owner_mismatch: supervisorProcessOwnerMismatch,
    thread_a0: () => 0n,
    thread_a1: () => 0n,
    thread_process_ptr: () => 0x5000,
    process_fd_ofd_id: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_host_fd: () => 0,
    kernel_ofd_status_flags: () => 0,
    kernel_ofd_ino: () => 0,
    kernel_ofd_offset: () => 0n,
    thread_blocked_fd: () => 0,
    thread_blocked_ofd_id: () => 0,
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    pid: 1,
    kernelWorkerRequest: vi.fn(async () => ({ type: "unexpected" }) as never),
  });
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 29,
    rpcPending: true,
  });

  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

  const helpers = createWorkerKernelRpcLocalBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    assertProcessStandardFdIntegrity: () => {},
    enqueueWorker: () => {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    clearHostBlockedRetryTimer: () => {},
    handleHostBlockedRetryResult,
    notifyHostEffectProgress: () => {},
    markRuntimeFsDirty: () => {},
    requestPipeStateSync: async () => {},
    syncBlockedForkSharedStateViaKernelWorker,
    syncForkSharedStateViaKernelWorker: async () => {},
    rehydrateForkSharedStateViaKernelWorker: async () => {},
    syncSocketStateViaKernelWorker,
    collectTrackableForkSharedOfds: () => new Set(),
  });

  await helpers.executeHostBlockedSyscallResume(handle, worker, 0x1234, 29);

  expect(supervisorProcessOwnerMismatch).toHaveBeenCalledWith(0x1234, 1);
  expect(threadResumeBlockedSyscall).toHaveBeenCalledWith(0x1234);
  expect(syncBlockedForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  expect(syncSocketStateViaKernelWorker).not.toHaveBeenCalled();
  expect(handleHostBlockedRetryResult).toHaveBeenCalledWith(handle, worker, 0x1234, 0);
  expect(handle.hostBlockedWorkers.get(worker)?.rpcPending).toBe(false);
});

test("executeHostBlockedSyscallResume queues pselect fd_set writes after owner-side resume", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const bytes = new Uint8Array(memory.buffer);
  const threadResumeBlockedSyscall = vi.fn(() => {
    bytes.set([0x08, 0, 0, 0, 0, 0, 0, 0], 0x200);
    bytes.set([0x10, 0, 0, 0, 0, 0, 0, 0], 0x240);
    bytes.set([0, 0, 0, 0, 0, 0, 0, 0], 0x280);
    return 2n;
  });
  const handleHostBlockedRetryResult = vi.fn();
  const kernel = {
    memory,
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    abi_status_pipe_wait: () => 21,
    abi_status_connect: () => 28,
    abi_status_socket_recv: () => 29,
    abi_status_socket_send: () => 30,
    abi_sys_pselect6: () => 72n,
    abi_max_fds: () => 1024,
    thread_resume_blocked_syscall: threadResumeBlockedSyscall,
    thread_status: () => 0,
    process_current_pid: () => 1,
    thread_a0: () => 0n,
    thread_a1: () => 0x200n,
    thread_reg: () => 8n,
    thread_process_ptr: () => 0x5000,
    thread_poll_wait_has_socket: () => 1,
    thread_blocked_syscall_nr: () => 72n,
    thread_blocked_syscall_arg: (_vmPtr: number, index: number) => {
      if (index === 0) return 64n;
      if (index === 1) return 0x200n;
      if (index === 2) return 0x240n;
      if (index === 3) return 0x280n;
      return 0n;
    },
    process_fd_ofd_id: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_host_fd: () => 0,
    kernel_ofd_status_flags: () => 0,
    kernel_ofd_ino: () => 0,
    kernel_ofd_offset: () => 0n,
    thread_blocked_fd: () => 0,
    thread_blocked_ofd_id: () => 0,
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    memory,
    kernelWorkerRequest: vi.fn(async () => ({ type: "unexpected" }) as never),
  });
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 21,
    rpcPending: true,
    rpcGeneration: 0,
  });

  const threadState = {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
    pendingGuestMemoryWrites: [],
  };
  const threadWorkerStates = new WeakMap<Worker, typeof threadState>();
  threadWorkerStates.set(worker, threadState);

  const helpers = createWorkerKernelRpcLocalBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    assertProcessStandardFdIntegrity: () => {},
    enqueueWorker: () => {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleHostBlockedRetryResult,
    notifyHostEffectProgress: () => {},
    markRuntimeFsDirty: () => {},
    requestPipeStateSync: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker: async () => {},
    rehydrateForkSharedStateViaKernelWorker: async () => {},
    syncSocketStateViaKernelWorker: async () => ({ socketUsed: 0, netBufUsed: 0n }),
    collectTrackableForkSharedOfds: () => new Set(),
  });

  await helpers.executeHostBlockedSyscallResume(handle, worker, 0x1234, 21);

  expect(threadResumeBlockedSyscall).toHaveBeenCalledWith(0x1234);
  expect(threadState.pendingGuestMemoryWrites.map((write) => write.guestPtr)).toEqual([
    0x200,
    0x240,
    0x280,
  ]);
  expect([...threadState.pendingGuestMemoryWrites[0].bytes]).toEqual([0x08, 0, 0, 0, 0, 0, 0, 0]);
  expect([...threadState.pendingGuestMemoryWrites[1].bytes]).toEqual([0x10, 0, 0, 0, 0, 0, 0, 0]);
  expect([...threadState.pendingGuestMemoryWrites[2].bytes]).toEqual([0, 0, 0, 0, 0, 0, 0, 0]);
  expect(handleHostBlockedRetryResult).toHaveBeenCalledWith(handle, worker, 0x1234, 0);
});

test("executeHostBlockedSyscallResume follows supervisor pipe defer action", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const handleHostBlockedRetryResult = vi.fn();
  const kernel = {
    memory,
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    abi_status_ok: () => 0,
    abi_status_pipe_wait: () => 21,
    abi_status_connect: () => 28,
    abi_status_socket_recv: () => 29,
    abi_status_socket_send: () => 30,
    abi_sys_read: () => 63n,
    abi_max_fds: () => 1024,
    thread_resume_blocked_syscall: vi.fn(() => 0n),
    thread_status: () => 21,
    process_current_pid: () => 1,
    thread_poll_wait_has_socket: () => 0,
    thread_blocked_syscall_nr: () => 63n,
    thread_blocked_syscall_arg: () => 0n,
    process_fd_ofd_id: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_host_fd: () => 0,
    kernel_ofd_status_flags: () => 0,
    kernel_ofd_ino: () => 0,
    kernel_ofd_offset: () => 0n,
    thread_blocked_fd: () => 0,
    thread_blocked_ofd_id: () => 0,
    thread_a0: () => 0n,
    thread_a1: () => 0n,
    thread_process_ptr: () => 0,
    supervisor_blocked_pipe_resume_action_defer: () => 0,
    supervisor_blocked_pipe_resume_action_direct: () => 1,
    supervisor_blocked_pipe_resume_action: vi.fn(() => 0),
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, { memory });
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 21,
    rpcPending: true,
    rpcGeneration: 0,
    pipeCanonicalEofReady: true,
  });

  const threadState = {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
    pendingGuestMemoryWrites: [],
  };
  const threadWorkerStates = new WeakMap<Worker, typeof threadState>();
  threadWorkerStates.set(worker, threadState);
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleHostBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await helpers.executeHostBlockedSyscallResume(handle, worker, 0x1234, 21);

  expect(kernel.supervisor_blocked_pipe_resume_action).toHaveBeenCalledWith(0x1234);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handleHostBlockedRetryResult).toHaveBeenCalledWith(handle, worker, 0x1234, 21);
  expect(handle.hostBlockedWorkers.get(worker)?.pipeCanonicalEofReady).toBe(true);
});

test("executeHostBlockedSyscallResume follows supervisor pipe direct action", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const bytes = new Uint8Array(memory.buffer);
  const readBufferPtr = 0x200n;
  let status = 21;
  const threadResumeBlockedSyscall = vi.fn(() => {
    bytes.set([0x61, 0x62, 0x63], Number(readBufferPtr));
    status = 0;
    return 3n;
  });
  const handleHostBlockedRetryResult = vi.fn();
  const kernel = {
    memory,
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    abi_status_ok: () => 0,
    abi_status_pipe_wait: () => 21,
    abi_status_connect: () => 28,
    abi_status_socket_recv: () => 29,
    abi_status_socket_send: () => 30,
    abi_sys_read: () => 63n,
    abi_max_fds: () => 1024,
    thread_resume_blocked_syscall: threadResumeBlockedSyscall,
    thread_status: () => status,
    process_current_pid: () => 1,
    thread_poll_wait_has_socket: () => 0,
    thread_blocked_syscall_nr: () => 63n,
    thread_blocked_syscall_arg: (_vmPtr: number, index: number) => {
      if (index === 0) return 3n;
      if (index === 1) return readBufferPtr;
      if (index === 2) return 8n;
      return 0n;
    },
    process_fd_ofd_id: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_host_fd: () => 0,
    kernel_ofd_status_flags: () => 0,
    kernel_ofd_ino: () => 0,
    kernel_ofd_offset: () => 0n,
    thread_blocked_fd: () => 0,
    thread_blocked_ofd_id: () => 0,
    thread_a0: () => 0n,
    thread_a1: () => readBufferPtr,
    thread_process_ptr: () => 0,
    supervisor_blocked_pipe_resume_action_defer: () => 0,
    supervisor_blocked_pipe_resume_action_direct: () => 1,
    supervisor_blocked_pipe_resume_action: vi.fn(() => 1),
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, { memory });
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 21,
    rpcPending: true,
    rpcGeneration: 0,
  });

  const threadState = {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
    pendingGuestMemoryWrites: [],
  };
  const threadWorkerStates = new WeakMap<Worker, typeof threadState>();
  threadWorkerStates.set(worker, threadState);
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleHostBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
  });

  await helpers.executeHostBlockedSyscallResume(handle, worker, 0x1234, 21);

  expect(kernel.supervisor_blocked_pipe_resume_action).toHaveBeenCalledWith(0x1234);
  expect(threadResumeBlockedSyscall).toHaveBeenCalledWith(0x1234);
  expect(threadState.pendingGuestMemoryWrites.map((write) => write.guestPtr)).toEqual([Number(readBufferPtr)]);
  expect([...threadState.pendingGuestMemoryWrites[0].bytes]).toEqual([0x61, 0x62, 0x63]);
  expect(handleHostBlockedRetryResult).toHaveBeenCalledWith(handle, worker, 0x1234, 0);
});

test("executeHostBlockedSyscallResume syncs socket before mixed pipe supervisor action", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const order: string[] = [];
  const handleHostBlockedRetryResult = vi.fn();
  const kernel = {
    memory,
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    abi_status_ok: () => 0,
    abi_status_pipe_wait: () => 21,
    abi_status_connect: () => 28,
    abi_status_socket_recv: () => 29,
    abi_status_socket_send: () => 30,
    abi_sys_read: () => 63n,
    abi_max_fds: () => 1024,
    thread_resume_blocked_syscall: vi.fn(() => 0n),
    thread_status: () => 21,
    process_current_pid: () => 1,
    thread_poll_wait_has_socket: () => 1,
    thread_blocked_syscall_nr: () => 63n,
    thread_blocked_syscall_arg: () => 0n,
    process_fd_ofd_id: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_host_fd: () => 0,
    kernel_ofd_status_flags: () => 0,
    kernel_ofd_ino: () => 0,
    kernel_ofd_offset: () => 0n,
    thread_blocked_fd: () => 0,
    thread_blocked_ofd_id: () => 0,
    thread_a0: () => 0n,
    thread_a1: () => 0n,
    thread_process_ptr: () => 0,
    supervisor_blocked_pipe_resume_action_defer: () => 0,
    supervisor_blocked_pipe_resume_action_direct: () => 1,
    supervisor_blocked_pipe_resume_action: vi.fn(() => {
      order.push("action");
      return 0;
    }),
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, { memory });
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 21,
    rpcPending: true,
    rpcGeneration: 0,
  });

  const threadState = {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
    pendingGuestMemoryWrites: [],
  };
  const threadWorkerStates = new WeakMap<Worker, typeof threadState>();
  threadWorkerStates.set(worker, threadState);
  const syncSocketStateViaKernelWorker = vi.fn(async () => {
    order.push("socket-sync");
    return { socketUsed: 0, netBufUsed: 0n };
  });
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleHostBlockedRetryResult,
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker,
  });

  await helpers.executeHostBlockedSyscallResume(handle, worker, 0x1234, 21);

  expect(order).toEqual(["socket-sync", "action"]);
  expect(syncSocketStateViaKernelWorker).toHaveBeenCalledWith(handle, { kind: "blocked-rpc", status: 21 });
  expect(handleHostBlockedRetryResult).toHaveBeenCalledWith(handle, worker, 0x1234, 21);
});

test("executeHostBlockedSyscallResume queues the full socket read buffer after owner resume", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const bytes = new Uint8Array(memory.buffer);
  const readPtr = 0x200;
  const readLen = 70_000;
  const threadResumeBlockedSyscall = vi.fn(() => {
    for (let offset = 0; offset < readLen; offset += 1) {
      bytes[readPtr + offset] = offset & 0xff;
    }
    return BigInt(readLen);
  });
  const handleHostBlockedRetryResult = vi.fn();
  const kernel = {
    memory,
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    abi_status_pipe_wait: () => 21,
    abi_status_connect: () => 28,
    abi_status_socket_recv: () => 29,
    abi_status_socket_send: () => 30,
    abi_sys_read: () => 63n,
    abi_sys_recvfrom: () => 207n,
    thread_resume_blocked_syscall: threadResumeBlockedSyscall,
    thread_status: () => 0,
    process_current_pid: () => 1,
    thread_a0: () => 0n,
    thread_a1: () => BigInt(readPtr),
    thread_process_ptr: () => 0x5000,
    thread_blocked_syscall_nr: () => 63n,
    thread_blocked_syscall_arg: (_vmPtr: number, index: number) => {
      if (index === 0) return 4n;
      if (index === 1) return BigInt(readPtr);
      if (index === 2) return BigInt(readLen);
      return 0n;
    },
    process_fd_ofd_id: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_host_fd: () => 0,
    kernel_ofd_status_flags: () => 0,
    kernel_ofd_ino: () => 0,
    kernel_ofd_offset: () => 0n,
    thread_blocked_fd: () => 4,
    thread_blocked_ofd_id: () => 0,
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    memory,
    kernelWorkerRequest: vi.fn(async () => ({ type: "unexpected" }) as never),
  });
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 29,
    rpcPending: true,
  });

  const threadState = {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
    pendingGuestMemoryWrites: [],
  };
  const threadWorkerStates = new WeakMap<Worker, typeof threadState>();
  threadWorkerStates.set(worker, threadState);

  const helpers = createWorkerKernelRpcLocalBlockedRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    assertProcessStandardFdIntegrity: () => {},
    enqueueWorker: () => {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    handleHostBlockedRetryResult,
    notifyHostEffectProgress: () => {},
    markRuntimeFsDirty: () => {},
    requestPipeStateSync: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker: async () => {},
    rehydrateForkSharedStateViaKernelWorker: async () => {},
    syncSocketStateViaKernelWorker: async () => ({ socketUsed: 0, netBufUsed: 0n }),
    collectTrackableForkSharedOfds: () => new Set(),
  });

  await helpers.executeHostBlockedSyscallResume(handle, worker, 0x1234, 29);

  expect(threadResumeBlockedSyscall).toHaveBeenCalledWith(0x1234);
  expect(threadState.pendingGuestMemoryWrites).toHaveLength(1);
  expect(threadState.pendingGuestMemoryWrites[0].guestPtr).toBe(readPtr);
  expect(threadState.pendingGuestMemoryWrites[0].bytes.length).toBe(readLen);
  expect(threadState.pendingGuestMemoryWrites[0].bytes[65_535]).toBe(0xff);
  expect(threadState.pendingGuestMemoryWrites[0].bytes[65_536]).toBe(0x00);
  expect(threadState.pendingGuestMemoryWrites[0].bytes[readLen - 1]).toBe((readLen - 1) & 0xff);
  expect(handleHostBlockedRetryResult).toHaveBeenCalledWith(handle, worker, 0x1234, 0);
});

test("resumeFsViaKernelWorker skips memfs sync during active vfork child ownership mismatch after fs resume", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const processFsRequests: unknown[] = [];
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const markRuntimeFsDirty = vi.fn();
  const enqueueWorker = vi.fn();
  const assertProcessStandardFdIntegrity = vi.fn((_handle: ProcessHandle, context: string) => {
    if (context === "resume-fs") {
      throw new Error("parent fd integrity should not inspect a transient child-owned process");
    }
  });
  let currentPid = 1;

  const kernel = {
    ...supervisorFsResponseDispositionTestStubs(() => currentPid),
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    abi_max_fds: () => 16,
    abi_max_threads: () => 1,
    process_current_pid: () => currentPid,
    process_fd_ofd_id: () => 0,
    process_thread_vm_ptr: () => 0,
    thread_pending_fs_opcode: () => 7,
    thread_pending_fs_effects: () => 1,
    thread_pending_fs_target_ino: () => 0,
    thread_process_fs: vi.fn(),
    thread_resume_fs: vi.fn(),
    thread_a0: () => 0n,
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    pid: 1,
    kernelWorkerRequest: vi.fn(async (message) => {
      processFsRequests.push(message);
      if (message.type !== "process-fs") {
        return { type: "unexpected" } as never;
      }
      currentPid = 5;
      return {
        type: "fs-processed",
        requestId: 0,
        kernelState: message.kernelState,
        result: 0n,
        memfsMutated: true,
        ofdStateChanged: false,
      };
    }),
  });

  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

  const helpers = createWorkerKernelRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    assertProcessStandardFdIntegrity,
    enqueueWorker,
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    clearHostBlockedRetryTimer: () => {},
    handleHostBlockedRetryResult: () => {},
    notifyHostEffectProgress: () => {},
    markRuntimeFsDirty,
    requestPipeStateSync: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker,
    rehydrateForkSharedStateViaKernelWorker: async () => {},
    syncSocketStateViaKernelWorker: async () => ({ socketUsed: 0, netBufUsed: 0n }),
    collectTrackableForkSharedOfds: () => new Set(),
  });

  const outcome = await helpers.resumeFsViaKernelWorker(handle, worker, 0x1234, {
    memfsMutation: 1,
    memfsFileData: 4,
    sharedOfdState: 2,
  });

  expect(processFsRequests).toHaveLength(1);
  expect(assertProcessStandardFdIntegrity).toHaveBeenCalledTimes(1);
  expect(assertProcessStandardFdIntegrity).toHaveBeenCalledWith(handle, "resume-fs-before");
  expect(syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  expect(markRuntimeFsDirty).toHaveBeenCalledWith(handle);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
  expect(outcome.effects).toBeUndefined();
});

test("resumeFsViaKernelWorker does not adopt failed execve child state into the vfork parent", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const processFsRequests: unknown[] = [];
  const initialKernelState = makeKernelState();
  const childKernelState = {
    ...initialKernelState,
    heapPtr: initialKernelState.heapPtr + 0x4000,
    socketUsed: 7,
    pipeUsed: 11n,
    netBufUsed: 13n,
  };
  const localChildKernelState = {
    ...initialKernelState,
    heapPtr: initialKernelState.heapPtr + 0x8000,
    socketUsed: 17,
    pipeUsed: 19n,
    netBufUsed: 23n,
  };
  let attachedKernelState = initialKernelState;
  let currentPid = 1;
  let status = 3;
  const threadResumeFs = vi.fn(() => {
    status = 0;
    return -12n;
  });
  const fdEntries = new Map<number, { ofdId: number; fdFlags: number }>([
    [0, { ofdId: 1, fdFlags: 0 }],
  ]);

  const kernel = {
    ...supervisorFsResponseDispositionTestStubs(() => currentPid, () => status),
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn((
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
      attachedKernelState = {
        ...attachedKernelState,
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
    }),
    kernel_heap_base: () => attachedKernelState.heapBase,
    kernel_heap_ptr: () => attachedKernelState.heapPtr,
    kernel_ofd_table_ptr: () => attachedKernelState.ofdTablePtr,
    kernel_memfs_ptr: () => attachedKernelState.memfsPtr,
    kernel_memfs_header_bytes: () => attachedKernelState.memfsHeaderBytes,
    kernel_memfs_inodes_bytes: () => attachedKernelState.memfsInodesBytes,
    kernel_memfs_first_child_bytes: () => attachedKernelState.memfsFirstChildBytes,
    kernel_memfs_dir_entries_bytes: () => attachedKernelState.memfsDirEntriesBytes,
    kernel_memfs_data_bytes: () => attachedKernelState.memfsDataBytes,
    kernel_mem_state_ptr: () => attachedKernelState.memStatePtr,
    kernel_epoll_table_ptr: () => attachedKernelState.epollTablePtr,
    kernel_socket_table_ptr: () => attachedKernelState.socketTablePtr,
    kernel_socket_used: () => attachedKernelState.socketUsed,
    kernel_pipe_base: () => attachedKernelState.pipeBase,
    kernel_pipe_used: () => attachedKernelState.pipeUsed,
    kernel_net_buf_base: () => attachedKernelState.netBufBase,
    kernel_net_buf_used: () => attachedKernelState.netBufUsed,
    kernel_sig_slots_ptr: () => attachedKernelState.sigSlotsPtr,
    kernel_process_pending_ptr: () => attachedKernelState.processPendingPtr,
    kernel_process_runtime_state_ptr: () => attachedKernelState.processRuntimeStatePtr,
    kernel_time_state_ptr: () => attachedKernelState.timeStatePtr,
    abi_max_fds: () => 16,
    abi_max_threads: () => 1,
    process_current_pid: () => currentPid,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd)?.ofdId ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdEntries.get(fd)?.fdFlags ?? 0,
    process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
      fdEntries.set(fd, { ofdId, fdFlags });
    },
    abi_status_fs: () => 3,
    process_thread_vm_ptr: () => 0,
    thread_status: () => status,
    thread_pending_fs_opcode: () => 13,
    thread_pending_fs_effects: () => 0,
    thread_pending_fs_target_ino: () => 0,
    thread_a0: () => -12n,
    thread_resume_fs: threadResumeFs,
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    pid: 1,
    kernelState: initialKernelState,
    kernelWorkerRequest: vi.fn(async (message) => {
      processFsRequests.push(message);
      if (message.type !== "process-fs") {
        return { type: "unexpected" } as never;
      }
      currentPid = 3;
      attachedKernelState = localChildKernelState;
      return {
        type: "fs-processed",
        requestId: 0,
        kernelState: childKernelState,
        result: -12n,
        memfsMutated: false,
        ofdStateChanged: true,
        fdEntries: [{ fd: 0, ofdId: 1463582720, fdFlags: 1463582211 }],
      };
    }),
  });

  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });
  const enqueueWorker = vi.fn();

  const helpers = createWorkerKernelRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    assertProcessStandardFdIntegrity: () => {},
    enqueueWorker,
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    clearHostBlockedRetryTimer: () => {},
    handleHostBlockedRetryResult: () => {},
    notifyHostEffectProgress: () => {},
    markRuntimeFsDirty: () => {},
    requestPipeStateSync: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker: async () => {},
    rehydrateForkSharedStateViaKernelWorker: async () => {},
    syncSocketStateViaKernelWorker: async () => ({ socketUsed: 0, netBufUsed: 0n }),
    collectTrackableForkSharedOfds: () => new Set(),
  });

  const outcome = await helpers.resumeFsViaKernelWorker(handle, worker, 0x1234, {
    memfsMutation: 1,
    memfsFileData: 4,
    sharedOfdState: 2,
  });

  expect(processFsRequests).toHaveLength(1);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
  expect(outcome.effects).toBeUndefined();
  expect(handle.kernelState).toEqual(initialKernelState);
  expect(handle.kernelState).not.toEqual(localChildKernelState);
  expect(threadResumeFs).not.toHaveBeenCalled();
  expect((threadWorkerStates.get(worker) as ThreadWorkerState & {
    pendingKernelStateForNextStep?: KernelRuntimeState;
  }).pendingKernelStateForNextStep).toEqual(childKernelState);
  expect(kernel.kernel_attach_runtime_state).not.toHaveBeenCalledWith(
    childKernelState.heapBase,
    childKernelState.heapPtr,
    childKernelState.ofdTablePtr,
    childKernelState.memfsPtr,
    childKernelState.memStatePtr,
    childKernelState.epollTablePtr,
    childKernelState.socketTablePtr,
    childKernelState.socketUsed,
    childKernelState.pipeBase,
    childKernelState.pipeUsed,
    childKernelState.netBufBase,
    childKernelState.netBufUsed,
    childKernelState.sigSlotsPtr,
    childKernelState.processPendingPtr,
    childKernelState.processRuntimeStatePtr,
    childKernelState.timeStatePtr,
  );
  expect(fdEntries.get(0)).toEqual({ ofdId: 1, fdFlags: 0 });
});

test("resumeFsViaKernelWorker routes execve spawn before parent fd integrity checks", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const processFsRequests: unknown[] = [];
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const assertProcessStandardFdIntegrity = vi.fn((_handle: ProcessHandle, context: string) => {
    if (context === "resume-fs") {
      throw new Error("parent fd integrity should not inspect child-owned execve state");
    }
  });
  const enqueueWorker = vi.fn();
  const handleExecveSpawn = vi.fn(async () => {});
  const notifyHostEffectProgress = vi.fn();
  const execveSpawnStatus = 9;
  let status = 0;
  let currentPid = 1;

  const kernel = {
    ...supervisorFsResponseDispositionTestStubs(() => currentPid, () => status, () => execveSpawnStatus),
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    abi_max_fds: () => 16,
    abi_max_threads: () => 1,
    abi_status_execve_spawn: () => execveSpawnStatus,
    process_current_pid: () => currentPid,
    process_fd_ofd_id: () => 0,
    process_fd_flags: () => 0,
    process_thread_vm_ptr: () => 0,
    thread_pending_fs_opcode: () => 13,
    thread_pending_fs_effects: () => 1,
    thread_pending_fs_target_ino: () => 0,
    thread_status: () => status,
    thread_a0: () => 0n,
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    pid: 1,
    kernelWorkerRequest: vi.fn(async (message) => {
      processFsRequests.push(message);
      if (message.type !== "process-fs") {
        return { type: "unexpected" } as never;
      }
      currentPid = 3;
      return {
        type: "fs-processed",
        requestId: 0,
        kernelState: message.kernelState,
        result: 0n,
        memfsMutated: true,
        ofdStateChanged: true,
      };
    }),
  });

  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
    ownerAsyncStatus: number | null;
    lastStatus: number | null;
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
    ownerAsyncStatus: execveSpawnStatus,
    lastStatus: null,
  });

  const helpers = createWorkerKernelRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    assertProcessStandardFdIntegrity,
    enqueueWorker,
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    clearHostBlockedRetryTimer: () => {},
    handleHostBlockedRetryResult: () => {},
    handleExecveSpawn,
    notifyHostEffectProgress,
    markRuntimeFsDirty: () => {},
    requestPipeStateSync: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker,
    rehydrateForkSharedStateViaKernelWorker: async () => {},
    syncSocketStateViaKernelWorker: async () => ({ socketUsed: 0, netBufUsed: 0n }),
    collectTrackableForkSharedOfds: () => new Set(),
  });

  const outcome = await helpers.resumeFsViaKernelWorker(handle, worker, 0x1234, {
    memfsMutation: 1,
    memfsFileData: 4,
    sharedOfdState: 2,
  });

  expect(processFsRequests).toHaveLength(1);
  expect(assertProcessStandardFdIntegrity).toHaveBeenCalledTimes(1);
  expect(assertProcessStandardFdIntegrity).toHaveBeenCalledWith(handle, "resume-fs-before");
  expect(syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  expect(enqueueWorker).not.toHaveBeenCalled();
  expect(outcome.status).toBeUndefined();
  expect(outcome.effects).toBeUndefined();
  expect(notifyHostEffectProgress).toHaveBeenCalledWith(handle);
  expect(handleExecveSpawn).toHaveBeenCalledWith(
    handle,
    worker,
    threadWorkerStates.get(worker),
  );
  expect(threadWorkerStates.get(worker)?.ownerAsyncStatus).toBeNull();
});

test("resumeFsViaKernelWorker applies fd entries returned by kernel-worker fs processing", async () => {
  const worker = {} as Worker;
  const runtimeBridge = createRuntimeStateBridge();
  const processFsRequests: unknown[] = [];
  const fdEntries = new Map<number, { ofdId: number; fdFlags: number }>();

  const kernel = {
    ...supervisorFsResponseDispositionTestStubs(() => 1),
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    abi_max_fds: () => 16,
    abi_max_threads: () => 1,
    process_current_pid: () => 1,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd)?.ofdId ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdEntries.get(fd)?.fdFlags ?? 0,
    process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
      fdEntries.set(fd, { ofdId, fdFlags });
    },
    supervisor_fd_entry_restore_action_apply: () => 1,
    supervisor_fd_entry_restore_action_skip: () => 2,
    supervisor_fd_entry_restore_flag_preserve_live_regular_file: () => 1,
    supervisor_fd_entry_restore_flag_preserve_live_epoll_dependent: () => 2,
    supervisor_fd_entry_restore_action: () => 1,
    process_thread_vm_ptr: () => 0,
    thread_status: () => 0,
    thread_pending_fs_opcode: () => 3,
    thread_pending_fs_effects: () => 0,
    thread_pending_fs_target_ino: () => 0,
    thread_process_fs: vi.fn(),
    thread_resume_fs: vi.fn(),
    thread_a0: () => 0n,
  } as unknown as KernelExports;

  const handle = makeHandle(kernel, runtimeBridge, worker, {
    pid: 1,
    kernelWorkerRequest: vi.fn(async (message) => {
      processFsRequests.push(message);
      if (message.type !== "process-fs") {
        return { type: "unexpected" } as never;
      }
      return {
        type: "fs-processed",
        requestId: 0,
        kernelState: message.kernelState,
        result: 5n,
        memfsMutated: false,
        ofdStateChanged: false,
        fdEntries: [{ fd: 5, ofdId: 7, fdFlags: 0 }],
      };
    }),
  });

  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

  const helpers = createWorkerKernelRpcHelpers({
    processHandles: new Map([[1, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: () => {},
    assertProcessStandardFdIntegrity: () => {},
    enqueueWorker: () => {},
    failProcess(errorHandle, error) {
      throw new Error(`unexpected fail for pid=${errorHandle.pid}: ${error.message}`);
    },
    clearHostBlockedRetryTimer: () => {},
    handleHostBlockedRetryResult: () => {},
    notifyHostEffectProgress: () => {},
    markRuntimeFsDirty: () => {},
    requestPipeStateSync: async () => {},
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker: async () => {},
    rehydrateForkSharedStateViaKernelWorker: async () => {},
    syncSocketStateViaKernelWorker: async () => ({ socketUsed: 0, netBufUsed: 0n }),
    collectTrackableForkSharedOfds: () => new Set(),
  });

  await helpers.resumeFsViaKernelWorker(handle, worker, 0x1234, {
    memfsMutation: 1,
    memfsFileData: 4,
    sharedOfdState: 2,
  });

  expect(processFsRequests).toHaveLength(1);
  expect(fdEntries.get(5)).toEqual({ ofdId: 7, fdFlags: 0 });
});
