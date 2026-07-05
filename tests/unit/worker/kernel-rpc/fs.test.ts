import { expect, test, vi } from "vite-plus/test";
import {
  addProcessWorker,
} from "../../../../src/worker/process-worker-state.js";
import { createWorkerKernelRpcFsHelpers } from "../../../../src/worker/kernel-rpc/fs.js";
import {
  FS_OP_FSTAT,
  FS_OP_CLOSE,
  FS_OP_EXECVE,
  FS_OP_GETDENTS,
  FS_OP_MKDIR,
  FS_OP_MMAP,
  FS_OP_OPENAT,
  FS_OP_RENAME,
  FS_OP_UNLINK,
  FS_OP_WRITE,
  FS_RING_REQUEST_FD_OFFSET,
  FS_RING_REQUEST_OPCODE_OFFSET,
  FS_RING_REQUEST_VM_PTR_OFFSET,
  FS_RING_SUBMIT_HEAD_OFFSET,
  FS_RING_SUBMIT_TAIL_OFFSET,
  fsRingRequestEntryOffset,
} from "../../../../src/layout/fs-ring.js";
import type { KernelExports, KernelRuntimeState } from "../../../../src/abi.js";
import { snapshotPendingGuestMemoryWritesForNextStep } from "../../../../src/worker/thread-step-payload.js";
import {
  setProcessMemfsDirty,
  setProcessMemfsPublishPending,
} from "../../../../src/worker/process-memfs-state.js";
import { createTransferDebugStats } from "../../../../src/worker/types.js";
import type { ProcessHandle, ThreadWorkerState } from "../../../../src/worker/types.js";
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";

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

function makeKernel() {
  const state = makeKernelState();
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  let kernel: Partial<KernelExports>;
  kernel = {
    ...makeSupervisorAbiStubs({
      memory,
      currentExports: () => kernel,
    }),
    memory,
    kernel_heap_base: () => state.heapBase,
    kernel_heap_ptr: () => state.heapPtr,
    kernel_ofd_table_ptr: () => state.ofdTablePtr,
    kernel_memfs_ptr: () => state.memfsPtr,
    kernel_mem_state_ptr: () => state.memStatePtr,
    kernel_epoll_table_ptr: () => state.epollTablePtr,
    kernel_socket_table_ptr: () => state.socketTablePtr,
    kernel_socket_used: () => state.socketUsed,
    kernel_pipe_base: () => state.pipeBase,
    kernel_pipe_used: () => state.pipeUsed,
    kernel_net_buf_base: () => state.netBufBase,
    kernel_net_buf_used: () => state.netBufUsed,
    kernel_sig_slots_ptr: () => state.sigSlotsPtr,
    kernel_process_pending_ptr: () => state.processPendingPtr,
    kernel_process_runtime_state_ptr: () => state.processRuntimeStatePtr,
    kernel_time_state_ptr: () => state.timeStatePtr,
    kernel_attach_runtime_state: vi.fn(),
    thread_pending_fs_opcode: () => 0,
    thread_pending_fs_effects: () => 0,
    thread_pending_fs_target_ino: () => 1,
    thread_fs_ring_base: () => 0,
    thread_fs_ring_submit_head: () => 0,
    thread_fs_ring_submit_tail: () => 0,
    thread_process_fs: vi.fn(),
    thread_resume_fs: vi.fn(() => 0n),
    thread_status: () => 0,
    abi_max_fds: () => 8,
    abi_max_threads: () => 4,
    abi_eagain: () => 11,
    abi_fd_type_file: () => 3,
    abi_fd_type_pipe: () => 6,
    abi_max_pipes: () => 64,
    abi_status_fs: () => 31,
    abi_status_execve_spawn: () => 24,
    supervisor_process_status_is_execve_spawn: (status: number) => status === 24 ? 1 : 0,
    supervisor_process_status_defers_kernel_state_adoption: () => 0,
    supervisor_fs_completion_flag_memfs_dirty: () => 1,
    supervisor_fs_completion_flag_memfs_publish_ready: () => 2,
    supervisor_fs_completion_flag_ofd_state_changed: () => 4,
    supervisor_fs_completion_flag_regular_file_request: () => 8,
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
    supervisor_process_held_ofd_refcount: () => 0,
    supervisor_process_private_pipe_slot_mask: () => 0n,
    supervisor_process_pipe_used_mask: () => 1n,
    supervisor_process_pipe_read_open_count: () => 0,
    supervisor_process_pipe_write_open_count: () => 0,
    supervisor_fs_completion_decision: (
      _vmPtr: number,
      opcode: number,
      effects: number,
      result: bigint | number,
      hostMemfsMutated: number,
      hostOfdStateChanged: number,
      pendingRingRequest: number,
      regularFileRequest: number,
    ) => {
      const signedResult = Number(
        BigInt.asIntN(64, typeof result === "bigint" ? result : BigInt(result)),
      );
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
    supervisor_fs_response_disposition: (
      _vmPtr: number,
      _runtimePid: number,
      opcode: number,
      result: bigint | number,
      ownerAsyncStatus: number,
      lastStatus: number,
    ) => {
      const signedResult = Number(
        BigInt.asIntN(64, typeof result === "bigint" ? result : BigInt(result)),
      );
      const execveSpawnStatus = 24;
      let flags = 1 | 2 | 16 | 256;
      if (opcode !== FS_OP_MMAP) {
        flags |= 4;
      }
      if (opcode === FS_OP_CLOSE || opcode === FS_OP_EXECVE) {
        flags |= 32;
      }
      if (
        signedResult >= 0
        && opcode === FS_OP_EXECVE
        && (ownerAsyncStatus === execveSpawnStatus || lastStatus === execveSpawnStatus)
      ) {
        flags |= 128;
      }
      flags |= 8;
      return flags;
    },
    supervisor_fs_local_metadata_action: (
      opcode: number,
      effects: number,
      result: bigint | number,
      hasPrimaryPath: number,
      hasSecondaryPath: number,
    ) => {
      const signedResult = Number(
        BigInt.asIntN(64, typeof result === "bigint" ? result : BigInt(result)),
      );
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
    process_fd_ofd_id: () => 0,
    process_fd_flags: () => 0,
    process_thread_vm_ptr: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_table_bytes: () => 0,
  };
  return kernel;
}

function seedFsRingRequest(
  memory: WebAssembly.Memory,
  ringBase: number,
  vmPtr: number,
  opcode: number,
  fd: number,
): void {
  const view = new DataView(memory.buffer);
  view.setUint32(ringBase + FS_RING_SUBMIT_HEAD_OFFSET, 1, true);
  view.setUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, 0, true);
  const entry = fsRingRequestEntryOffset(ringBase, 0);
  view.setUint32(entry + FS_RING_REQUEST_OPCODE_OFFSET, opcode, true);
  view.setInt32(entry + FS_RING_REQUEST_FD_OFFSET, fd, true);
  view.setUint32(entry + FS_RING_REQUEST_VM_PTR_OFFSET, vmPtr, true);
}

test("kernel-rpc fs helper uses kernel-worker resume and returns a typed outcome", async () => {
  const kernel = makeKernel();
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "fs-processed",
    requestId: 1,
    kernelState: makeKernelState(),
    result: 0n,
    memfsMutated: false,
    ofdStateChanged: false,
  }));
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const enqueueWorker = vi.fn();
  const notifyHostEffectProgress = vi.fn();
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker,
    failProcess: vi.fn(),
    notifyHostEffectProgress,
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  await expect(helpers.resumeFsViaKernelWorker(handle, worker, 0x1234)).resolves.toMatchObject({
    kind: "completed",
    status: 0,
  });
  await expect(helpers.resumeFsViaKernelWorkerOutcome(handle, worker, 0x1234)).resolves.toMatchObject({
    kind: "completed",
    status: 0,
  });
  expect(kernelWorkerRequest).toHaveBeenCalledTimes(2);
  expect(kernel.thread_process_fs).not.toHaveBeenCalled();
  expect(kernel.thread_resume_fs).not.toHaveBeenCalled();
  expect(notifyHostEffectProgress).toHaveBeenCalledTimes(2);
  expect(enqueueWorker).toHaveBeenCalledTimes(2);
});

test("kernel-rpc fs outcome reports blocked fs status", async () => {
  const kernel = {
    ...makeKernel(),
    thread_status: () => 31,
  };
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "fs-processed",
    requestId: 1,
    kernelState: makeKernelState(),
    result: 11n,
    memfsMutated: false,
    ofdStateChanged: false,
  }));
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn(),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  await expect(helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  )).resolves.toMatchObject({
    kind: "blocked",
    status: 31,
  });
});

test("kernel-worker fs outcome returns wake effect after local publication", async () => {
  const kernel = {
    ...makeKernel(),
    thread_pending_fs_opcode: () => 6,
    thread_pending_fs_effects: () => 1,
  };
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "fs-processed",
    requestId: 1,
    kernelState: makeKernelState(),
    result: 0n,
    memfsMutated: true,
    ofdStateChanged: false,
  }));
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadState = {} as ThreadWorkerState;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, threadState);
  const markRuntimeFsDirty = vi.fn();
  const publishRuntimeKernelState = vi.fn();
  const enqueueWorker = vi.fn();
  const notifyHostEffectProgress = vi.fn();
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker,
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress,
    markRuntimeFsDirty,
    kernelShareOtherMembers: () => [],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState,
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  );

  expect(kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "process-fs",
    pid: 1,
    vmPtr: 0x1234,
  }));
  expect(outcome).toMatchObject({ kind: "completed", status: 0 });
  expect(outcome.effects).toBeUndefined();
  expect(markRuntimeFsDirty).toHaveBeenCalledWith(handle);
  expect(publishRuntimeKernelState).toHaveBeenCalledWith(handle);
  expect(notifyHostEffectProgress).toHaveBeenCalledWith(handle);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
  expect(markRuntimeFsDirty.mock.invocationCallOrder[0]).toBeLessThan(
    publishRuntimeKernelState.mock.invocationCallOrder[0],
  );
  expect(publishRuntimeKernelState.mock.invocationCallOrder[0]).toBeLessThan(
    notifyHostEffectProgress.mock.invocationCallOrder[0],
  );
  expect(notifyHostEffectProgress.mock.invocationCallOrder[0]).toBeLessThan(
    enqueueWorker.mock.invocationCallOrder[0],
  );
});

test("kernel-worker fs outcome follows supervisor publication decision", async () => {
  const supervisorFsCompletionDecision = vi.fn(() => 0);
  const kernel = {
    ...makeKernel(),
    thread_pending_fs_opcode: () => FS_OP_WRITE,
    thread_pending_fs_effects: () => 4,
    thread_pending_fs_target_ino: () => 42,
    supervisor_fs_pending_ring_request: () => 0,
    supervisor_fs_completion_decision: supervisorFsCompletionDecision,
  };
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "fs-processed",
    requestId: 1,
    kernelState: makeKernelState(),
    result: 4096n,
    memfsMutated: true,
    ofdStateChanged: true,
  }));
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const markRuntimeFsDirty = vi.fn();
  const syncSharedOfdStateAfterFs = vi.fn(async () => {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty,
    kernelShareOtherMembers: () => [],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs,
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  );

  expect(outcome).toMatchObject({ kind: "completed", status: 0 });
  expect(supervisorFsCompletionDecision).toHaveBeenCalledWith(
    0x1234,
    FS_OP_WRITE,
    4,
    4096n,
    1,
    1,
    0,
    0,
  );
  expect(markRuntimeFsDirty).not.toHaveBeenCalled();
  expect(syncSharedOfdStateAfterFs).not.toHaveBeenCalled();
});

test("kernel-worker fs outcome applies mmap guest memory writes from fs response", async () => {
  const kernel = makeKernel();
  const mappedBytes = Uint8Array.from([0x7f, 0x45, 0x4c, 0x46]);
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "fs-processed",
    requestId: 1,
    kernelState: makeKernelState(),
    result: 0x200n,
    memfsMutated: false,
    ofdStateChanged: false,
    guestMemoryWrites: [{ guestPtr: 0x200, bytes: mappedBytes }],
  }));
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    guestBase: 0x1000,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  const siblingWorker = {} as Worker;
  addProcessWorker(handle, worker);
  addProcessWorker(handle, siblingWorker);
  const threadState = {} as ThreadWorkerState;
  const siblingThreadState = {} as ThreadWorkerState;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, threadState);
  threadWorkerStates.set(siblingWorker, siblingThreadState);
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  );

  expect(outcome).toMatchObject({ kind: "completed", status: 0 });
  expect(new Uint8Array(handle.memory.buffer, 0x1200, mappedBytes.length)).toEqual(mappedBytes);
  const pendingWrites = snapshotPendingGuestMemoryWritesForNextStep(threadState);
  const siblingPendingWrites = snapshotPendingGuestMemoryWritesForNextStep(siblingThreadState);
  expect(pendingWrites).toHaveLength(1);
  expect(pendingWrites[0].guestPtr).toBe(0x200);
  expect(pendingWrites[0].bytes).toEqual(mappedBytes);
  expect(siblingPendingWrites).toHaveLength(1);
  expect(siblingPendingWrites[0].guestPtr).toBe(0x200);
  expect(siblingPendingWrites[0].bytes).toEqual(mappedBytes);
});

test("kernel-worker fs outcome defers file write publication to a later sync point", async () => {
  const kernel = {
    ...makeKernel(),
    thread_pending_fs_opcode: () => 2,
    thread_pending_fs_effects: () => 2,
  };
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "fs-processed",
    requestId: 1,
    kernelState: makeKernelState(),
    result: 4096n,
    memfsMutated: true,
    ofdStateChanged: false,
  }));
  const targetKernel = makeKernel();
  const target = {
    pid: 2,
    vmPtr: 0x5678,
    kernel: targetKernel,
    memory: targetKernel.memory,
    hostBlockedWorkers: new Map(),
    runningWorker: null,
  } as unknown as ProcessHandle;
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set([target]), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const markRuntimeFsDirty = vi.fn();
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty,
    kernelShareOtherMembers: () => [target],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  );

  expect(markRuntimeFsDirty).toHaveBeenCalledWith(handle);
  expect(markRuntimeFsDirty).toHaveBeenCalledWith(target);
});

test("kernel-worker fs outcome applies only request fd after regular file write", async () => {
  const ringBase = 0x3000;
  const supervisorFsPendingRegularFileFd = vi.fn(() => 3);
  const supervisorFsPendingRegularFileOfd = vi.fn(() => 5);
  const kernel = {
    ...makeKernel(),
    thread_pending_fs_opcode: () => FS_OP_WRITE,
    thread_pending_fs_effects: () => 4,
    thread_pending_fs_target_ino: () => 42,
    thread_fs_ring_base: () => ringBase,
    supervisor_fs_pending_regular_file_fd: supervisorFsPendingRegularFileFd,
    supervisor_fs_pending_regular_file_ofd: supervisorFsPendingRegularFileOfd,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 3 ? 5 : 0,
    process_set_fd_entry: vi.fn(),
    kernel_ofd_fd_type: (ofdId: number) => ofdId === 5 ? 3 : 0,
  };
  seedFsRingRequest(kernel.memory, ringBase, 0x1234, FS_OP_WRITE, 3);
  const kernelWorkerRequest = vi.fn(async () => {
    new DataView(kernel.memory.buffer).setUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, 1, true);
    return {
      type: "fs-processed",
      requestId: 1,
      kernelState: makeKernelState(),
      result: 8n,
      memfsMutated: false,
      ofdStateChanged: true,
      fdEntries: [
        { fd: 3, ofdId: 5, fdFlags: 0 },
        { fd: 4, ofdId: 7, fdFlags: 0 },
      ],
    };
  });
  const targetKernel = makeKernel();
  const target = {
    pid: 2,
    vmPtr: 0x5678,
    kernel: targetKernel,
    kernelState: makeKernelState(),
    memory: targetKernel.memory,
    hostBlockedWorkers: new Map(),
    runningWorker: null,
  } as unknown as ProcessHandle;
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set([target]), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const syncSharedOfdStateAfterFs = vi.fn(async () => {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [target],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs,
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  );

  expect(syncSharedOfdStateAfterFs).not.toHaveBeenCalled();
  expect(supervisorFsPendingRegularFileFd).toHaveBeenCalledWith(0x1234);
  expect(supervisorFsPendingRegularFileOfd).toHaveBeenCalledWith(0x1234);
  expect(kernel.process_set_fd_entry).toHaveBeenCalledTimes(1);
  expect(kernel.process_set_fd_entry).toHaveBeenCalledWith(0x1234, 3, 5, 0);
  expect(kernel.process_set_fd_entry).not.toHaveBeenCalledWith(0x1234, 4, 7, 0);
});

test("kernel-worker fs outcome applies only request fd after regular file fstat", async () => {
  const ringBase = 0x3000;
  const kernel = {
    ...makeKernel(),
    thread_pending_fs_opcode: () => FS_OP_FSTAT,
    thread_pending_fs_effects: () => 4,
    thread_pending_fs_target_ino: () => 42,
    thread_fs_ring_base: () => ringBase,
    supervisor_fs_pending_regular_file_fd: () => 3,
    supervisor_fs_pending_regular_file_ofd: () => 5,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => {
      if (fd === 3) {
        return 5;
      }
      if (fd === 17) {
        return 9;
      }
      return 0;
    },
    process_set_fd_entry: vi.fn(),
    kernel_ofd_fd_type: (ofdId: number) => (ofdId === 5 || ofdId === 9 ? 3 : 0),
  };
  seedFsRingRequest(kernel.memory, ringBase, 0x1234, FS_OP_FSTAT, 3);
  const kernelWorkerRequest = vi.fn(async () => {
    new DataView(kernel.memory.buffer).setUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, 1, true);
    return {
      type: "fs-processed",
      requestId: 1,
      kernelState: makeKernelState(),
      result: 0n,
      memfsMutated: false,
      ofdStateChanged: true,
      fdEntries: [
        { fd: 3, ofdId: 5, fdFlags: 0 },
        { fd: 17, ofdId: 0, fdFlags: 0 },
      ],
    };
  });
  const targetKernel = makeKernel();
  const target = {
    pid: 2,
    vmPtr: 0x5678,
    kernel: targetKernel,
    kernelState: makeKernelState(),
    memory: targetKernel.memory,
    hostBlockedWorkers: new Map(),
    runningWorker: null,
  } as unknown as ProcessHandle;
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set([target]), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const syncSharedOfdStateAfterFs = vi.fn(async () => {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [target],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs,
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  );

  expect(syncSharedOfdStateAfterFs).not.toHaveBeenCalled();
  expect(kernel.process_set_fd_entry).toHaveBeenCalledTimes(1);
  expect(kernel.process_set_fd_entry).toHaveBeenCalledWith(0x1234, 3, 5, 0);
  expect(kernel.process_set_fd_entry).not.toHaveBeenCalledWith(0x1234, 17, 0, 0);
});

test("kernel-worker fs outcome preserves unrelated live fd across openat snapshot", async () => {
  const ringBase = 0x3000;
  const kernel = {
    ...makeKernel(),
    abi_max_fds: () => 32,
    thread_pending_fs_opcode: () => FS_OP_OPENAT,
    thread_pending_fs_effects: () => 4,
    thread_pending_fs_target_ino: () => 42,
    thread_fs_ring_base: () => ringBase,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => {
      if (fd === 17) {
        return 9;
      }
      return 0;
    },
    process_set_fd_entry: vi.fn(),
    kernel_ofd_fd_type: (ofdId: number) => (ofdId === 6 || ofdId === 9 ? 3 : 0),
    kernel_ofd_refcount: (ofdId: number) => (ofdId === 9 ? 1 : 0),
  };
  seedFsRingRequest(kernel.memory, ringBase, 0x1234, FS_OP_OPENAT, -100);
  const kernelWorkerRequest = vi.fn(async () => {
    new DataView(kernel.memory.buffer).setUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, 1, true);
    return {
      type: "fs-processed",
      requestId: 1,
      kernelState: makeKernelState(),
      result: 5n,
      memfsMutated: false,
      ofdStateChanged: true,
      fdEntries: [
        { fd: 5, ofdId: 6, fdFlags: 0 },
        { fd: 17, ofdId: 0, fdFlags: 0 },
      ],
    };
  });
  const targetKernel = makeKernel();
  const target = {
    pid: 2,
    vmPtr: 0x5678,
    kernel: targetKernel,
    kernelState: makeKernelState(),
    memory: targetKernel.memory,
    hostBlockedWorkers: new Map(),
    runningWorker: null,
  } as unknown as ProcessHandle;
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set([target]), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const syncSharedOfdStateAfterFs = vi.fn(async () => {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [target],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs,
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  );

  expect(outcome).toMatchObject({ kind: "completed", status: 0 });
  expect(kernel.process_set_fd_entry).toHaveBeenCalledWith(0x1234, 5, 6, 0);
  expect(kernel.process_set_fd_entry).not.toHaveBeenCalledWith(0x1234, 17, 0, 0);
});

test("kernel-worker fs outcome preserves live fd from stale nonzero openat snapshot", async () => {
  const ringBase = 0x3000;
  const kernel = {
    ...makeKernel(),
    abi_max_fds: () => 32,
    thread_pending_fs_opcode: () => FS_OP_OPENAT,
    thread_pending_fs_effects: () => 4,
    thread_pending_fs_target_ino: () => 42,
    thread_fs_ring_base: () => ringBase,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 17 ? 9 : 0),
    process_set_fd_entry: vi.fn(),
    kernel_ofd_fd_type: (ofdId: number) => (ofdId === 6 || ofdId === 8 || ofdId === 9 ? 3 : 0),
    kernel_ofd_refcount: (ofdId: number) => (ofdId === 9 ? 1 : 0),
  };
  seedFsRingRequest(kernel.memory, ringBase, 0x1234, FS_OP_OPENAT, -100);
  const kernelWorkerRequest = vi.fn(async () => {
    new DataView(kernel.memory.buffer).setUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, 1, true);
    return {
      type: "fs-processed",
      requestId: 1,
      kernelState: makeKernelState(),
      result: 5n,
      memfsMutated: false,
      ofdStateChanged: true,
      fdEntries: [
        { fd: 5, ofdId: 6, fdFlags: 0 },
        { fd: 17, ofdId: 8, fdFlags: 0 },
      ],
    };
  });
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  );

  expect(outcome).toMatchObject({ kind: "completed", status: 0 });
  expect(kernel.process_set_fd_entry).toHaveBeenCalledWith(0x1234, 5, 6, 0);
  expect(kernel.process_set_fd_entry).not.toHaveBeenCalledWith(0x1234, 17, 8, 0);
});

test("kernel-worker fs outcome permits close snapshot to clear live fd", async () => {
  const ringBase = 0x3000;
  const kernel = {
    ...makeKernel(),
    abi_max_fds: () => 32,
    thread_pending_fs_opcode: () => FS_OP_CLOSE,
    thread_pending_fs_effects: () => 4,
    thread_pending_fs_target_ino: () => 42,
    thread_fs_ring_base: () => ringBase,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 17 ? 9 : 0),
    process_set_fd_entry: vi.fn(),
    kernel_ofd_fd_type: (ofdId: number) => (ofdId === 9 ? 3 : 0),
    kernel_ofd_refcount: (ofdId: number) => (ofdId === 9 ? 1 : 0),
  };
  seedFsRingRequest(kernel.memory, ringBase, 0x1234, FS_OP_CLOSE, 17);
  const kernelWorkerRequest = vi.fn(async () => {
    new DataView(kernel.memory.buffer).setUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, 1, true);
    return {
      type: "fs-processed",
      requestId: 1,
      kernelState: makeKernelState(),
      result: 0n,
      memfsMutated: false,
      ofdStateChanged: true,
      fdEntries: [{ fd: 17, ofdId: 0, fdFlags: 0 }],
    };
  });
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  );

  expect(outcome).toMatchObject({ kind: "completed", status: 0 });
  expect(kernel.process_set_fd_entry).toHaveBeenCalledWith(0x1234, 17, 0, 0);
});

test("kernel-worker fs outcome keeps generated file fd closeable after unrelated snapshots", async () => {
  const ringBase = 0x3000;
  let pendingOpcode = FS_OP_OPENAT;
  const fdTable = new Map<number, { ofdId: number; fdFlags: number }>([
    [17, { ofdId: 9, fdFlags: 0 }],
  ]);
  const kernel = {
    ...makeKernel(),
    abi_max_fds: () => 32,
    thread_pending_fs_opcode: () => pendingOpcode,
    thread_pending_fs_effects: () => 4,
    thread_pending_fs_target_ino: () => 42,
    thread_fs_ring_base: () => ringBase,
    supervisor_fs_pending_regular_file_fd: () => pendingOpcode === FS_OP_FSTAT ? 5 : -1,
    supervisor_fs_pending_regular_file_ofd: () => pendingOpcode === FS_OP_FSTAT ? 6 : 0,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdTable.get(fd)?.ofdId ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdTable.get(fd)?.fdFlags ?? 0,
    process_set_fd_entry: vi.fn((_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
      fdTable.set(fd, { ofdId, fdFlags });
    }),
    kernel_ofd_fd_type: (ofdId: number) => (ofdId === 6 || ofdId === 9 ? 3 : 0),
    kernel_ofd_refcount: (ofdId: number) => (ofdId === 9 ? 1 : 0),
  };
  const kernelWorkerRequest = vi.fn(async () => {
    new DataView(kernel.memory.buffer).setUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, 1, true);
    if (pendingOpcode === FS_OP_CLOSE) {
      return {
        type: "fs-processed",
        requestId: 2,
        kernelState: makeKernelState(),
        result: 0n,
        memfsMutated: false,
        ofdStateChanged: true,
        fdEntries: [{ fd: 17, ofdId: 0, fdFlags: 0 }],
      };
    }
    return {
      type: "fs-processed",
      requestId: 1,
      kernelState: makeKernelState(),
      result: pendingOpcode === FS_OP_OPENAT ? 5n : 0n,
      memfsMutated: false,
      ofdStateChanged: true,
      fdEntries: [
        { fd: 5, ofdId: 6, fdFlags: 0 },
        { fd: 17, ofdId: 0, fdFlags: 0 },
      ],
    };
  });
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const unrelatedSnapshots = [
    { opcode: FS_OP_OPENAT, fd: -100 },
    { opcode: FS_OP_FSTAT, fd: 5 },
    { opcode: FS_OP_GETDENTS, fd: 5 },
  ];
  for (const snapshot of unrelatedSnapshots) {
    pendingOpcode = snapshot.opcode;
    seedFsRingRequest(kernel.memory, ringBase, 0x1234, snapshot.opcode, snapshot.fd);
    await expect(helpers.resumeFsViaKernelWorkerOutcome(
      handle,
      worker,
      0x1234
    )).resolves.toMatchObject({ kind: "completed", status: 0 });

    expect(fdTable.get(5)).toEqual({ ofdId: 6, fdFlags: 0 });
    expect(fdTable.get(17)).toEqual({ ofdId: 9, fdFlags: 0 });
  }

  pendingOpcode = FS_OP_CLOSE;
  seedFsRingRequest(kernel.memory, ringBase, 0x1234, FS_OP_CLOSE, 17);
  await expect(helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  )).resolves.toMatchObject({ kind: "completed", status: 0 });

  expect(fdTable.get(17)).toEqual({ ofdId: 0, fdFlags: 0 });
});

test("kernel-worker fs outcome rejects fd snapshot entries stale against request baseline", async () => {
  const ringBase = 0x3000;
  const fdTable = new Map<number, { ofdId: number; fdFlags: number }>();
  const kernel = {
    ...makeKernel(),
    abi_max_fds: () => 32,
    thread_pending_fs_opcode: () => FS_OP_OPENAT,
    thread_pending_fs_effects: () => 4,
    thread_pending_fs_target_ino: () => 42,
    thread_fs_ring_base: () => ringBase,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdTable.get(fd)?.ofdId ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdTable.get(fd)?.fdFlags ?? 0,
    process_set_fd_entry: vi.fn((_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
      fdTable.set(fd, { ofdId, fdFlags });
    }),
    kernel_ofd_fd_type: (ofdId: number) => (ofdId === 6 ? 3 : ofdId === 12 ? 9 : 0),
    kernel_ofd_refcount: () => 0,
  };
  seedFsRingRequest(kernel.memory, ringBase, 0x1234, FS_OP_OPENAT, -100);
  const kernelWorkerRequest = vi.fn(async () => {
    fdTable.set(18, { ofdId: 12, fdFlags: 0 });
    new DataView(kernel.memory.buffer).setUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, 1, true);
    return {
      type: "fs-processed",
      requestId: 1,
      kernelState: makeKernelState(),
      result: 5n,
      memfsMutated: false,
      ofdStateChanged: true,
      fdEntries: [
        { fd: 5, ofdId: 6, fdFlags: 0 },
        { fd: 18, ofdId: 0, fdFlags: 0 },
      ],
    };
  });
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  await expect(helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  )).resolves.toMatchObject({ kind: "completed", status: 0 });

  expect(fdTable.get(5)).toEqual({ ofdId: 6, fdFlags: 0 });
  expect(fdTable.get(18)).toEqual({ ofdId: 12, fdFlags: 0 });
  expect(kernel.process_set_fd_entry).not.toHaveBeenCalledWith(0x1234, 18, 0, 0);
});

test("kernel-worker fs outcome rejects fd snapshot ABA using generation", async () => {
  const ringBase = 0x3000;
  const fdTable = new Map<number, { ofdId: number; fdFlags: number }>([
    [18, { ofdId: 0, fdFlags: 0 }],
  ]);
  const fdGeneration = new Map<number, number>([[18, 1]]);
  const kernel = {
    ...makeKernel(),
    abi_max_fds: () => 32,
    thread_pending_fs_opcode: () => FS_OP_OPENAT,
    thread_pending_fs_effects: () => 4,
    thread_pending_fs_target_ino: () => 42,
    thread_fs_ring_base: () => ringBase,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdTable.get(fd)?.ofdId ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdTable.get(fd)?.fdFlags ?? 0,
    process_fd_generation: (_vmPtr: number, fd: number) => fdGeneration.get(fd) ?? 0,
    process_set_fd_entry: vi.fn((_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
      fdTable.set(fd, { ofdId, fdFlags });
      fdGeneration.set(fd, (fdGeneration.get(fd) ?? 0) + 1);
    }),
    kernel_ofd_fd_type: (ofdId: number) => (ofdId === 6 ? 3 : 0),
    kernel_ofd_refcount: () => 0,
  };
  seedFsRingRequest(kernel.memory, ringBase, 0x1234, FS_OP_OPENAT, -100);
  const kernelWorkerRequest = vi.fn(async () => {
    fdTable.set(18, { ofdId: 12, fdFlags: 0 });
    fdGeneration.set(18, 2);
    fdTable.set(18, { ofdId: 0, fdFlags: 0 });
    fdGeneration.set(18, 3);
    new DataView(kernel.memory.buffer).setUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, 1, true);
    return {
      type: "fs-processed",
      requestId: 1,
      kernelState: makeKernelState(),
      result: 5n,
      memfsMutated: false,
      ofdStateChanged: true,
      fdEntries: [
        { fd: 5, ofdId: 6, fdFlags: 0, generation: 1 },
        { fd: 18, ofdId: 0, fdFlags: 0, generation: 1 },
      ],
    };
  });
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  await expect(helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  )).resolves.toMatchObject({ kind: "completed", status: 0 });

  expect(fdTable.get(5)).toEqual({ ofdId: 6, fdFlags: 0 });
  expect(fdTable.get(18)).toEqual({ ofdId: 0, fdFlags: 0 });
  expect(fdGeneration.get(18)).toBe(3);
  expect(kernel.process_set_fd_entry).not.toHaveBeenCalledWith(0x1234, 18, 0, 0);
});

test("kernel-worker fs outcome rejects ofd slot snapshots stale against request baseline", async () => {
  const ringBase = 0x3000;
  const ofdTablePtr = 0x4000;
  const maxFds = 32;
  const slotBytes = 16;
  const ofdId = 6;
  const baseKernel = makeKernel();
  const memory = new Uint8Array(baseKernel.memory.buffer);
  const slotOffset = ofdTablePtr + ofdId * slotBytes;
  const slotView = new DataView(baseKernel.memory.buffer, slotOffset, slotBytes);
  slotView.setInt32(0, 9, true);
  slotView.setInt32(4, 1, true);
  const fdTable = new Map<number, { ofdId: number; fdFlags: number }>([
    [5, { ofdId, fdFlags: 0 }],
  ]);
  const kernel = {
    ...baseKernel,
    abi_max_fds: () => maxFds,
    kernel_ofd_table_ptr: () => ofdTablePtr,
    kernel_ofd_table_bytes: () => maxFds * slotBytes,
    thread_pending_fs_opcode: () => FS_OP_OPENAT,
    thread_pending_fs_effects: () => 4,
    thread_pending_fs_target_ino: () => 42,
    thread_fs_ring_base: () => ringBase,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdTable.get(fd)?.ofdId ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdTable.get(fd)?.fdFlags ?? 0,
    process_set_fd_entry: vi.fn((_vmPtr: number, fd: number, nextOfdId: number, fdFlags: number) => {
      fdTable.set(fd, { ofdId: nextOfdId, fdFlags });
    }),
    kernel_ofd_fd_type: (nextOfdId: number) => {
      if (nextOfdId !== ofdId) {
        return 0;
      }
      return new DataView(baseKernel.memory.buffer, ofdTablePtr + nextOfdId * slotBytes, slotBytes)
        .getInt32(0, true);
    },
    kernel_ofd_refcount: () => 0,
  };
  seedFsRingRequest(kernel.memory, ringBase, 0x1234, FS_OP_OPENAT, -100);
  const responseSlot = new Uint8Array(slotBytes);
  const responseSlotView = new DataView(responseSlot.buffer);
  responseSlotView.setInt32(0, 9, true);
  responseSlotView.setInt32(4, 3, true);
  const kernelWorkerRequest = vi.fn(async () => {
    slotView.setInt32(4, 2, true);
    new DataView(kernel.memory.buffer).setUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, 1, true);
    return {
      type: "fs-processed",
      requestId: 1,
      kernelState: { ...makeKernelState(), ofdTablePtr },
      result: 5n,
      memfsMutated: false,
      ofdStateChanged: true,
      fdEntries: [{ fd: 5, ofdId, fdFlags: 0 }],
      ofdSlots: [{ ofdId, slot: responseSlot }],
    };
  });
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: { ...makeKernelState(), ofdTablePtr },
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  await expect(helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  )).resolves.toMatchObject({ kind: "completed", status: 0 });

  expect(memory[slotOffset]).toBe(9);
  expect(slotView.getInt32(4, true)).toBe(2);
});

test("kernel-worker fs outcome skips shared OFD sync after regular file mmap", async () => {
  const ringBase = 0x3000;
  const kernel = {
    ...makeKernel(),
    thread_pending_fs_opcode: () => FS_OP_MMAP,
    thread_pending_fs_effects: () => 0,
    thread_pending_fs_target_ino: () => 42,
    thread_fs_ring_base: () => ringBase,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 3 ? 5 : 0,
    kernel_ofd_fd_type: (ofdId: number) => ofdId === 5 ? 3 : 0,
  };
  seedFsRingRequest(kernel.memory, ringBase, 0x1234, FS_OP_MMAP, 3);
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "fs-processed",
    requestId: 1,
    kernelState: makeKernelState(),
    result: 0x2000n,
    memfsMutated: false,
    ofdStateChanged: false,
  }));
  const targetKernel = makeKernel();
  const target = {
    pid: 2,
    vmPtr: 0x5678,
    kernel: targetKernel,
    kernelState: makeKernelState(),
    memory: targetKernel.memory,
    hostBlockedWorkers: new Map(),
    runningWorker: null,
  } as unknown as ProcessHandle;
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set([target]), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const syncSharedOfdStateAfterFs = vi.fn(async () => {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [target],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs,
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  );

  expect(syncSharedOfdStateAfterFs).not.toHaveBeenCalled();
});

test("kernel-worker fs outcome keeps shared OFD sync after pipe write", async () => {
  const ringBase = 0x3000;
  const kernel = {
    ...makeKernel(),
    thread_pending_fs_opcode: () => FS_OP_WRITE,
    thread_pending_fs_effects: () => 4,
    thread_pending_fs_target_ino: () => 42,
    thread_fs_ring_base: () => ringBase,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 3 ? 5 : 0,
    process_set_fd_entry: vi.fn(),
    kernel_ofd_fd_type: (ofdId: number) => ofdId === 5 ? 6 : 0,
  };
  seedFsRingRequest(kernel.memory, ringBase, 0x1234, FS_OP_WRITE, 3);
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "fs-processed",
    requestId: 1,
    kernelState: makeKernelState(),
    result: 8n,
    memfsMutated: false,
    ofdStateChanged: true,
    fdEntries: [{ fd: 3, ofdId: 6, fdFlags: 0 }],
  }));
  const targetKernel = makeKernel();
  const target = {
    pid: 2,
    vmPtr: 0x5678,
    kernel: targetKernel,
    kernelState: makeKernelState(),
    memory: targetKernel.memory,
    hostBlockedWorkers: new Map(),
    runningWorker: null,
  } as unknown as ProcessHandle;
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set([target]), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const syncSharedOfdStateAfterFs = vi.fn(async () => {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [target],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs,
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  );

  expect(syncSharedOfdStateAfterFs).toHaveBeenCalledWith(handle, [target], { sourceVmPtr: 0x1234 });
  expect(kernel.process_set_fd_entry).toHaveBeenCalledWith(0x1234, 3, 6, 0);
});

test("kernel-worker fs outcome does not rehydrate unrelated peer shared OFD before path unlink", async () => {
  const ringBase = 0x3000;
  const kernel = {
    ...makeKernel(),
    thread_pending_fs_opcode: () => FS_OP_UNLINK,
    thread_pending_fs_effects: () => 1,
    thread_fs_ring_base: () => ringBase,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 3 ? 7 : 0,
    kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 3 : 0,
  };
  seedFsRingRequest(kernel.memory, ringBase, 0x1234, FS_OP_UNLINK, -100);
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "fs-processed",
    requestId: 1,
    kernelState: makeKernelState(),
    result: 0n,
    memfsMutated: true,
    ofdStateChanged: false,
  }));
  const peerKernel = {
    ...makeKernel(),
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 3 ? 4 : 0,
    kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? 6 : 0,
  };
  const peer = {
    pid: 3,
    vmPtr: 0x5678,
    kernel: peerKernel,
    kernelState: makeKernelState(),
    memory: peerKernel.memory,
    hostBlockedWorkers: new Map(),
    runningWorker: null,
  } as unknown as ProcessHandle;
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set([peer]), sharedOfdIds: new Set([4]) },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const rehydrateForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [peer],
    syncDirtyMemfsPeerViaKernelWorker: vi.fn(async () => {}),
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker,
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  );

  expect(rehydrateForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  expect(kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "process-fs",
    pid: 1,
    vmPtr: 0x1234,
  }));
  expect(outcome).toMatchObject({ kind: "completed", status: 0 });
});

test("kernel-worker fs outcome skips stale dirty MemFS peers before dispatch", async () => {
  const ringBase = 0x3000;
  const kernel = {
    ...makeKernel(),
    thread_pending_fs_opcode: () => FS_OP_UNLINK,
    thread_pending_fs_effects: () => 1,
    thread_fs_ring_base: () => ringBase,
  };
  seedFsRingRequest(kernel.memory, ringBase, 0x1234, FS_OP_UNLINK, -100);
  const kernelWorkerRequest = vi.fn(async () => ({
    type: "fs-processed",
    requestId: 1,
    kernelState: makeKernelState(),
    result: 0n,
    memfsMutated: true,
    ofdStateChanged: false,
  }));
  const stalePeerKernel = makeKernel();
  const stalePeerWorker = {} as Worker;
  const stalePeer = {
    pid: 3,
    vmPtr: 0x5678,
    kernel: stalePeerKernel,
    kernelState: makeKernelState(),
    memory: stalePeerKernel.memory,
    runningWorker: null,
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Map(),
    workers: new Set([stalePeerWorker]),
    exitInProgress: false,
    kernelWorkerRequest: vi.fn(async () => new Promise(() => {})),
  } as unknown as ProcessHandle;
  setProcessMemfsDirty(stalePeer, true);
  setProcessMemfsPublishPending(stalePeer, true);
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    transferDebugStats: createTransferDebugStats(),
    kernelShare: { members: new Set([stalePeer]), sharedOfdIds: new Set() },
    workers: new Set(),
    hostBlockedWorkers: new Map(),
    kernelWorkerRequest,
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  addProcessWorker(handle, worker);
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, {});
  const syncDirtyMemfsPeerViaKernelWorker = vi.fn(async () => {
    throw new Error("stale dirty peer should not be synced before fs dispatch");
  });
  const helpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    notifyHostEffectProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    kernelShareOtherMembers: () => [stalePeer],
    syncDirtyMemfsPeerViaKernelWorker,
    syncSharedOfdStateAfterFs: vi.fn(async () => {}),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    publishRuntimeKernelState: vi.fn(),
  });

  const outcome = await helpers.resumeFsViaKernelWorkerOutcome(
    handle,
    worker,
    0x1234
  );

  expect(syncDirtyMemfsPeerViaKernelWorker).not.toHaveBeenCalled();
  expect(kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "process-fs",
    pid: 1,
    vmPtr: 0x1234,
  }));
  expect(outcome).toMatchObject({ kind: "completed", status: 0 });
});
