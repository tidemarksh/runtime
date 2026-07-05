import { expect, test, vi } from "vite-plus/test";
import { writePipe } from "../../../src/bridge/pipe.js";
import { createRuntimeStateBridge } from "../../../src/bridge/runtime-state.js";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { createWorkerLifecycleHelpers } from "../../../src/worker/lifecycle.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

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

function makeKernel(options: {
  runtimeState?: KernelRuntimeState;
  vforkChildPid?: number;
  resumeParent?: ReturnType<typeof vi.fn>;
} = {}): KernelExports {
  const runtimeState = options.runtimeState ?? makeKernelState();
  const resumeParent = options.resumeParent ?? vi.fn();
  const vforkChildPid = options.vforkChildPid ?? 77;
  return {
    abi_max_fds: () => 16,
    abi_max_threads: () => 4,
    abi_status_vfork_wait: () => 8,
    abi_status_execve_spawn: () => 9,
    abi_status_clone_spawn: () => 10,
    process_fd_ofd_id: () => 0,
    process_thread_vm_ptr: (_vmPtr, slot) => (slot === 0 ? 0x1234 : 0),
    thread_blocked_ofd_id: () => 0,
    thread_vfork_child_pid: () => vforkChildPid,
    thread_resume_vfork_parent: resumeParent,
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
  } as unknown as KernelExports;
}

function makeHandle(
  overrides: Partial<ProcessHandle> = {},
): {
  handle: ProcessHandle;
  enqueueWorker: ReturnType<typeof vi.fn>;
  notifyLifecycleProgress: ReturnType<typeof vi.fn>;
  recordChildExitViaKernelWorker: ReturnType<typeof vi.fn>;
  resumeVforkParentViaKernelWorker: ReturnType<typeof vi.fn>;
  resumeParent: ReturnType<typeof vi.fn>;
} {
  const enqueueWorker = vi.fn();
  const notifyLifecycleProgress = vi.fn();
  const recordChildExitViaKernelWorker = vi.fn(async () => {});
  const resumeVforkParentViaKernelWorker = vi.fn(async () => {});
  const resumeParent = vi.fn();
  const worker = {} as Worker;
  const handle = {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0x1234,
    guestMemoryPolicyLimitBytes: 0,
    guestBase: 0,
    worker,
    memory: new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel: makeKernel({ resumeParent }),
    kernelState: makeKernelState(),
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 0,
    },
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
    stdinQueue: [],
    outputBacklog: [],
    debugCommand: "test",
    runtimeBridge: createRuntimeStateBridge(),
    sabPageCache: null,
    workers: new Set([worker]),
    vforkReadyWorkers: [],
    runningWorker: null,
    hostBlockedWorkers: new Map(),
    vforkSnapshot: {
      vmPtr: 0x1234,
      childPid: null,
      nextPid: null,
      pgid: null,
      sid: null,
      inheritedSharedOfdIds: [],
    },
    vforkWorker: worker,
    workerDebugLog: [],
    ...overrides,
  } as unknown as ProcessHandle;
  handle.kernelShare.members.add(handle);

  return {
    handle,
    enqueueWorker,
    notifyLifecycleProgress,
    recordChildExitViaKernelWorker,
    resumeVforkParentViaKernelWorker,
    resumeParent,
  };
}

function makeLifecycleDeps(spies: ReturnType<typeof makeHandle>) {
  return createWorkerLifecycleHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput: () => false,
    enqueueWorker: spies.enqueueWorker,
    getProcessByPid: () => undefined,
    notifyLifecycleProgress: spies.notifyLifecycleProgress,
    recordChildExitViaKernelWorker: spies.recordChildExitViaKernelWorker,
    releaseSharedHostStdio: async () => {},
    requestEpollStateSync: async () => {},
    requestPipeStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resumeVforkParentViaKernelWorker: spies.resumeVforkParentViaKernelWorker,
    syncForkSharedStateViaKernelWorker: async () => {},
    workerExitCompleteProcessExit: async () => {},
    workerExitFailProcess: () => {},
  });
}

function setPipeWritable(
  memory: WebAssembly.Memory,
  kernelState: KernelRuntimeState,
  pipeSlot: number,
): void {
  const view = new DataView(memory.buffer);
  const base = kernelState.pipeBase + pipeSlot * (32 + 65_536);
  view.setInt32(base + 8, 1, true);
  view.setInt32(base + 12, 1, true);
}

test("resumeVforkParentAfterChildExit delegates a normal exit wait status to kernel-worker", () => {
  const spies = makeHandle({
    kernelWorkerRequest: vi.fn(async () => ({ type: "noop" } as never)),
    stdinPipe: 1,
    stdoutPipe: 2,
    stderrPipe: 3,
    hostStdio: {
      stdinPipe: 1,
      stdoutPipe: 2,
      stderrPipe: 3,
      refCount: 1,
    },
  });
  const lifecycle = makeLifecycleDeps(spies);

  const resumed = lifecycle.resumeVforkParentAfterChildExit(
    spies.handle,
    spies.handle.worker,
    0x1234,
    3,
    0,
  );

  expect(resumed).toBe(true);
  expect(spies.resumeVforkParentViaKernelWorker).toHaveBeenCalledWith(
    spies.handle,
    spies.handle.worker,
    0x1234,
    3 << 8,
  );
  expect(spies.recordChildExitViaKernelWorker).not.toHaveBeenCalled();
  expect(spies.resumeParent).not.toHaveBeenCalled();
  expect(spies.enqueueWorker).not.toHaveBeenCalled();
  expect(spies.notifyLifecycleProgress).not.toHaveBeenCalled();
  expect(spies.handle.stdinPipe).toBe(1);
  expect(spies.handle.stdoutPipe).toBe(2);
  expect(spies.handle.stderrPipe).toBe(3);
  expect(spies.handle.hostStdio).toMatchObject({
    stdinPipe: 1,
    stdoutPipe: 2,
    stderrPipe: 3,
    refCount: 1,
  });
});

test("resumeVforkParentAfterChildExit delegates a signaled wait status to kernel-worker", () => {
  const spies = makeHandle({
    kernelWorkerRequest: vi.fn(async () => ({ type: "noop" } as never)),
  });
  const lifecycle = makeLifecycleDeps(spies);

  const resumed = lifecycle.resumeVforkParentAfterChildExit(
    spies.handle,
    spies.handle.worker,
    0x1234,
    0,
    15,
  );

  expect(resumed).toBe(true);
  expect(spies.resumeVforkParentViaKernelWorker).toHaveBeenCalledWith(
    spies.handle,
    spies.handle.worker,
    0x1234,
    15,
  );
  expect(spies.recordChildExitViaKernelWorker).not.toHaveBeenCalled();
  expect(spies.resumeParent).not.toHaveBeenCalled();
});

test("resumeVforkParentAfterChildExit delegates to kernel-worker when runtime ownership is externalized", () => {
  const spies = makeHandle({
    kernelWorkerRequest: vi.fn(async () => ({ type: "noop" } as never)),
  });
  const lifecycle = makeLifecycleDeps(spies);

  const resumed = lifecycle.resumeVforkParentAfterChildExit(
    spies.handle,
    spies.handle.worker,
    0x1234,
    0,
    9,
  );

  expect(resumed).toBe(true);
  expect(spies.resumeVforkParentViaKernelWorker).toHaveBeenCalledWith(
    spies.handle,
    spies.handle.worker,
    0x1234,
    9,
  );
  expect(spies.recordChildExitViaKernelWorker).not.toHaveBeenCalled();
  expect(spies.resumeParent).not.toHaveBeenCalled();
  expect(spies.enqueueWorker).not.toHaveBeenCalled();
});

test("resumeVforkParentAfterChildExit ignores stale worker completions after the parent already resumed", () => {
  const spies = makeHandle();
  const lifecycle = makeLifecycleDeps(spies);

  const resumed = lifecycle.resumeVforkParentAfterChildExit(
    spies.handle,
    spies.handle.worker,
    0x9999,
    0,
    0,
  );

  expect(resumed).toBe(false);
  expect(spies.recordChildExitViaKernelWorker).not.toHaveBeenCalled();
  expect(spies.resumeParent).not.toHaveBeenCalled();
  expect(spies.handle.vforkSnapshot).not.toBeNull();
});

test("resumeVforkParentAfterChildExit delegates fresh wait status for sequential child exits", () => {
  const spies = makeHandle({
    kernelWorkerRequest: vi.fn(async () => ({ type: "noop" } as never)),
  });
  const lifecycle = makeLifecycleDeps(spies);

  let resumed = lifecycle.resumeVforkParentAfterChildExit(
    spies.handle,
    spies.handle.worker,
    0x1234,
    7,
    0,
  );

  expect(resumed).toBe(true);
  expect(spies.resumeVforkParentViaKernelWorker).toHaveBeenNthCalledWith(
    1,
    spies.handle,
    spies.handle.worker,
    0x1234,
    7 << 8,
  );

  spies.handle.vforkSnapshot = {
    vmPtr: 0x1234,
    childPid: null,
    nextPid: null,
    pgid: null,
    sid: null,
    inheritedSharedOfdIds: [],
  };
  spies.handle.vforkWorker = spies.handle.worker;
  spies.recordChildExitViaKernelWorker.mockClear();
  spies.resumeVforkParentViaKernelWorker.mockClear();
  spies.resumeParent.mockClear();

  resumed = lifecycle.resumeVforkParentAfterChildExit(
    spies.handle,
    spies.handle.worker,
    0x1234,
    0,
    14,
  );

  expect(resumed).toBe(true);
  expect(spies.resumeVforkParentViaKernelWorker).toHaveBeenCalledWith(
    spies.handle,
    spies.handle.worker,
    0x1234,
    14,
  );
  expect(spies.recordChildExitViaKernelWorker).not.toHaveBeenCalled();
  expect(spies.resumeParent).not.toHaveBeenCalled();
});

test("resumeVforkParentAfterChildExit preserves parent stdio and cwd across sequential child exits", () => {
  const spies = makeHandle({
    memory: new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true }),
    stdinPipe: 1,
    stdoutPipe: 2,
    stderrPipe: 3,
    hostStdio: {
      stdinPipe: 1,
      stdoutPipe: 2,
      stderrPipe: 3,
      refCount: 1,
    },
  });
  const lifecycle = makeLifecycleDeps(spies);
  const processSetCwdIno = vi.fn();
  spies.handle.kernel.process_cwd_ino = () => 41;
  spies.handle.kernel.process_set_cwd_ino = processSetCwdIno;
  setPipeWritable(spies.handle.memory, spies.handle.kernelState, spies.handle.stderrPipe);

  expect(writePipe(
    spies.handle.memory,
    spies.handle.kernelState.pipeBase,
    spies.handle.stderrPipe,
    new TextEncoder().encode("parent-before\n"),
  )).toMatchObject({ written: "parent-before\n".length, broken: false });

  let resumed = lifecycle.resumeVforkParentAfterChildExit(
    spies.handle,
    spies.handle.worker,
    0x1234,
    5,
    0,
  );

  expect(resumed).toBe(true);
  expect(spies.handle.stdinPipe).toBe(1);
  expect(spies.handle.stdoutPipe).toBe(2);
  expect(spies.handle.stderrPipe).toBe(3);
  expect(spies.handle.kernel.process_cwd_ino(spies.handle.vmPtr)).toBe(41);
  expect(processSetCwdIno).not.toHaveBeenCalled();
  expect(writePipe(
    spies.handle.memory,
    spies.handle.kernelState.pipeBase,
    spies.handle.stderrPipe,
    new TextEncoder().encode("parent-after-1\n"),
  )).toMatchObject({ written: "parent-after-1\n".length, broken: false });

  spies.handle.vforkSnapshot = {
    vmPtr: 0x1234,
    childPid: null,
    nextPid: null,
    pgid: null,
    sid: null,
    inheritedSharedOfdIds: [],
  };
  spies.handle.vforkWorker = spies.handle.worker;

  resumed = lifecycle.resumeVforkParentAfterChildExit(
    spies.handle,
    spies.handle.worker,
    0x1234,
    0,
    12,
  );

  expect(resumed).toBe(true);
  expect(spies.handle.stdinPipe).toBe(1);
  expect(spies.handle.stdoutPipe).toBe(2);
  expect(spies.handle.stderrPipe).toBe(3);
  expect(spies.handle.kernel.process_cwd_ino(spies.handle.vmPtr)).toBe(41);
  expect(processSetCwdIno).not.toHaveBeenCalled();
  expect(writePipe(
    spies.handle.memory,
    spies.handle.kernelState.pipeBase,
    spies.handle.stderrPipe,
    new TextEncoder().encode("parent-after-2\n"),
  )).toMatchObject({ written: "parent-after-2\n".length, broken: false });
});
