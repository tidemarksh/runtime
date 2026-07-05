import { addKernelShareMember, addKernelShareSharedOfdId, kernelShareHasSharedOfdId, kernelShareMembers, kernelShareSharedOfdIdList, processKernelShareGroup, setProcessKernelShareGroup } from "../../../../src/worker/kernel-share.js";
import { hostExecutionState } from "../../../../src/worker/host-execution-state.js";
import { expect, test, vi } from "vite-plus/test";
import { createWorkerKernelRpcBlockedRpcTestHelpers as createWorkerKernelRpcBlockedRpcHelpersBase } from "../../../../src/worker/host-boundary/blocked-rpc-resume.js";
import type { KernelRuntimeState } from "../../../../src/abi.js";
import { POLLFD_BYTES } from "../../../../src/layout/poll.js";
import {
  isPipeCanonicalEofReady,
  setPipeCanonicalEofReady,
} from "../../../../src/worker/host-blocked-state.js";
import { snapshotPendingGuestMemoryWritesForNextStep } from "../../../../src/worker/thread-step-payload.js";
import type { ProcessHandle, ThreadWorkerState } from "../../../../src/worker/types.js";
import { seedProcessKernelWorkerRequestFixture } from "../support/kernel-worker-request-fixture.js";
import { seedProcessHandleStateFixture } from "../support/process-handle-state-fixture.js";
import { installSupervisorBlockedResumeStub } from "../support/supervisor-blocked-resume.js";

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x4000,
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
  let status = 0;
  const kernel = {
    memory: new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true }),
    kernel_attach_runtime_state: vi.fn(),
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
    abi_status_ok: () => 0,
    abi_status_pipe_wait: () => 31,
    abi_status_epoll_wait: () => 35,
    abi_status_connect: () => 32,
    abi_status_socket_recv: () => 33,
    abi_status_socket_send: () => 34,
    abi_status_wait4: () => 30,
    abi_status_flock_wait: () => 36,
    abi_sys_read: () => 63n,
    abi_sys_recvfrom: () => 207n,
    abi_sys_ppoll: () => 73n,
    abi_sys_pselect6: () => 72n,
    abi_max_fds: () => 64,
    abi_max_pipes: () => 64,
    thread_status: () => status,
    setStatus(nextStatus: number): void {
      status = nextStatus;
    },
    process_current_pid: () => 1,
    thread_blocked_syscall_nr: () => 0n,
    thread_blocked_syscall_arg: () => 0n,
    thread_blocked_fd: () => 0,
    thread_poll_wait_has_socket: () => 0,
    thread_resume_blocked_syscall: vi.fn(() => {
      status = 0;
      return 0n;
    }),
    thread_a0: () => 0n,
    thread_a1: () => 0n,
    thread_process_ptr: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_host_fd: () => 0,
    kernel_ofd_status_flags: () => 0,
    kernel_ofd_ino: () => 0,
    kernel_ofd_offset: () => 0n,
    process_fd_ofd_id: () => 0,
    thread_blocked_ofd_id: () => 0,
  };
  installSupervisorBlockedResumeStub(kernel);
  return kernel;
}

function makeHandle(
  kernel: ReturnType<typeof makeKernel>,
  worker: Worker,
  overrides: Partial<ProcessHandle> = {},
): ProcessHandle {
  const handle = {
    pid: 1,
    runtimeId: 1,
    vmPtr: 0x1234,
    kernel,
    kernelState: makeKernelState(),
    memory: kernel.memory,
    guestBase: 0,
    workers: new Set([worker]),
    hostBlockedWorkers: new Map([[worker, {
      vmPtr: 0x1234,
      status: kernel.thread_status(0x1234),
    }]]),
    workerDebugLog: [],
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
    ...overrides,
  } as unknown as ProcessHandle;
  seedProcessKernelWorkerRequestFixture(handle);
  seedProcessHandleStateFixture(handle);
  addKernelShareMember(handle);
  return handle;
}

function makeHelpers(
  handle: ProcessHandle,
  worker: Worker,
  overrides: Partial<Parameters<typeof createWorkerKernelRpcBlockedRpcHelpersBase>[0]> = {},
) {
  const threadState = {} as ThreadWorkerState;
  const threadWorkerStates = new WeakMap<Worker, object>();
  threadWorkerStates.set(worker, threadState);
  const deps = {
    processHandles: new Map([[handle.pid, handle]]),
    threadWorkerStates: threadWorkerStates as never,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    failProcess: vi.fn((_handle: ProcessHandle, error: Error) => {
      throw error;
    }),
    handleHostBlockedRetryResult: vi.fn(),
    requestPipeStateSync: vi.fn(async () => {}),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
    ...overrides,
  };
  return {
    deps,
    helpers: createWorkerKernelRpcBlockedRpcHelpersBase(deps),
    threadState,
  };
}

test("blocked-rpc follows supervisor pipe defer action", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  kernel.setStatus(kernel.abi_status_pipe_wait());
  kernel.supervisor_blocked_pipe_resume_action = vi.fn(() =>
    kernel.supervisor_blocked_pipe_resume_action_defer()
  );
  const handle = makeHandle(kernel, worker);
  const blocked = {
    vmPtr,
    status: kernel.abi_status_pipe_wait(),
  };
  setPipeCanonicalEofReady(blocked, true);
  hostExecutionState(handle).hostBlockedWorkers.set(worker, blocked);
  const { deps, helpers } = makeHelpers(handle, worker);

  await expect(
    helpers.executeHostBlockedSyscallResume(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(kernel.supervisor_blocked_pipe_resume_action).toHaveBeenCalledWith(vmPtr);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(deps.handleHostBlockedRetryResult).toHaveBeenCalledWith(
    handle,
    worker,
    vmPtr,
    kernel.abi_status_pipe_wait(),
  );
  expect(isPipeCanonicalEofReady(hostExecutionState(handle).hostBlockedWorkers.get(worker))).toBe(true);
  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    1,
    "blocked-rpc-defer-pipe-supervisor:1",
  );
});

test("blocked-rpc follows supervisor pipe direct action and captures read writeback", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  const readBufferPtr = 0x80n;
  kernel.setStatus(kernel.abi_status_pipe_wait());
  kernel.supervisor_blocked_pipe_resume_action = vi.fn(() =>
    kernel.supervisor_blocked_pipe_resume_action_direct()
  );
  kernel.thread_blocked_syscall_nr = () => kernel.abi_sys_read();
  kernel.thread_blocked_syscall_arg = (_vmPtr: number, index: number) => {
    switch (index) {
      case 0:
        return 3n;
      case 1:
        return readBufferPtr;
      case 2:
        return 8n;
      default:
        return 0n;
    }
  };
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    new Uint8Array(kernel.memory.buffer).set([0x61, 0x62, 0x63], Number(readBufferPtr));
    kernel.setStatus(kernel.abi_status_ok());
    return 3n;
  });
  const handle = makeHandle(kernel, worker);
  const { deps, helpers, threadState } = makeHelpers(handle, worker);

  await expect(
    helpers.executeHostBlockedSyscallResume(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(kernel.supervisor_blocked_pipe_resume_action).toHaveBeenCalledWith(vmPtr);
  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(vmPtr);
  expect(snapshotPendingGuestMemoryWritesForNextStep(threadState)).toEqual([{
    guestPtr: Number(readBufferPtr),
    bytes: new Uint8Array([0x61, 0x62, 0x63]),
  }]);
  expect(deps.handleHostBlockedRetryResult).toHaveBeenCalledWith(
    handle,
    worker,
    vmPtr,
    kernel.abi_status_ok(),
  );
});

test("blocked-rpc captures pselect writeback after supervisor pipe direct action", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  const readfdsPtr = 0x100n;
  kernel.setStatus(kernel.abi_status_pipe_wait());
  kernel.supervisor_blocked_pipe_resume_action = vi.fn(() =>
    kernel.supervisor_blocked_pipe_resume_action_direct()
  );
  kernel.thread_blocked_syscall_nr = () => kernel.abi_sys_pselect6();
  kernel.thread_blocked_syscall_arg = (_vmPtr: number, index: number) => {
    switch (index) {
      case 0:
        return 6n;
      case 1:
        return readfdsPtr;
      default:
        return 0n;
    }
  };
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    new Uint8Array(kernel.memory.buffer).set([0x10, 0, 0, 0, 0, 0, 0, 0], Number(readfdsPtr));
    kernel.setStatus(kernel.abi_status_ok());
    return 1n;
  });
  const handle = makeHandle(kernel, worker);
  const { helpers, threadState } = makeHelpers(handle, worker);

  await expect(
    helpers.executeHostBlockedSyscallResume(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  const pendingWrites = snapshotPendingGuestMemoryWritesForNextStep(threadState);
  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(vmPtr);
  expect(pendingWrites).toHaveLength(1);
  expect(pendingWrites[0]).toMatchObject({
    guestPtr: Number(readfdsPtr),
  });
  expect([...pendingWrites[0].bytes]).toEqual([0x10, 0, 0, 0, 0, 0, 0, 0]);
});

test("blocked-rpc syncs socket state before mixed pipe supervisor action", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  const order: string[] = [];
  kernel.setStatus(kernel.abi_status_pipe_wait());
  kernel.thread_poll_wait_has_socket = () => 1;
  kernel.supervisor_blocked_pipe_resume_action = vi.fn(() => {
    order.push("action");
    return kernel.supervisor_blocked_pipe_resume_action_defer();
  });
  const handle = makeHandle(kernel, worker);
  const { deps, helpers } = makeHelpers(handle, worker, {
    syncSocketStateViaKernelWorker: vi.fn(async () => {
      order.push("socket-sync");
      return { socketUsed: 0, netBufUsed: 0n };
    }),
  });

  await expect(
    helpers.executeHostBlockedSyscallResume(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  expect(order).toEqual(["socket-sync", "action"]);
  expect(deps.syncSocketStateViaKernelWorker).toHaveBeenCalledWith(handle, {
    kind: "blocked-rpc",
    status: kernel.abi_status_pipe_wait(),
  });
  expect(deps.handleHostBlockedRetryResult).toHaveBeenCalledWith(
    handle,
    worker,
    vmPtr,
    kernel.abi_status_pipe_wait(),
  );
});

test("blocked-rpc generic resume returns a typed completed outcome", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  kernel.setStatus(21);
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    kernel.setStatus(kernel.abi_status_ok());
    return 0n;
  });
  const handle = makeHandle(kernel, worker, {
    kernelWorkerRequest: vi.fn(async () => ({ type: "noop" } as never)),
    hostBlockedWorkers: new Map([[worker, {
      vmPtr,
      status: 21,
    }]]),
  });
  const { deps, helpers } = makeHelpers(handle, worker);

  await expect(
    helpers.executeHostBlockedSyscallResumeOutcome(handle, worker, vmPtr, 21),
  ).resolves.toMatchObject({
    kind: "completed",
    status: kernel.abi_status_ok(),
  });
  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(vmPtr);
  expect(deps.syncBlockedForkSharedStateViaKernelWorker).toHaveBeenCalledWith(handle, vmPtr, {
    kind: "blocked-rpc",
    status: 21,
  });
});

test("blocked-rpc socket recv keeps host read writeback and socket sync", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  const recvBufferPtr = 0x180n;
  kernel.setStatus(kernel.abi_status_socket_recv());
  kernel.thread_blocked_syscall_nr = () => kernel.abi_sys_recvfrom();
  kernel.thread_blocked_syscall_arg = (_vmPtr: number, index: number) => {
    switch (index) {
      case 1:
        return recvBufferPtr;
      case 2:
        return 16n;
      default:
        return 0n;
    }
  };
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    new Uint8Array(kernel.memory.buffer).set([0xde, 0xad, 0xbe, 0xef], Number(recvBufferPtr));
    kernel.setStatus(kernel.abi_status_ok());
    return 4n;
  });
  const handle = makeHandle(kernel, worker);
  const { deps, helpers, threadState } = makeHelpers(handle, worker);

  await expect(
    helpers.executeHostBlockedSyscallResume(handle, worker, vmPtr, kernel.abi_status_socket_recv()),
  ).resolves.toBeUndefined();

  expect(snapshotPendingGuestMemoryWritesForNextStep(threadState)).toEqual([{
    guestPtr: Number(recvBufferPtr),
    bytes: new Uint8Array([0xde, 0xad, 0xbe, 0xef]),
  }]);
  expect(deps.syncSocketStateViaKernelWorker).toHaveBeenCalledWith(handle, {
    kind: "blocked-rpc",
    status: kernel.abi_status_socket_recv(),
  });
  expect(deps.handleHostBlockedRetryResult).toHaveBeenCalledWith(
    handle,
    worker,
    vmPtr,
    kernel.abi_status_ok(),
  );
});

test("blocked-rpc captures ppoll writeback after supervisor pipe direct action", async () => {
  const kernel = makeKernel();
  const worker = {} as Worker;
  const vmPtr = 0x1234;
  const pollfdsPtr = 0x200n;
  kernel.setStatus(kernel.abi_status_pipe_wait());
  kernel.supervisor_blocked_pipe_resume_action = vi.fn(() =>
    kernel.supervisor_blocked_pipe_resume_action_direct()
  );
  kernel.thread_blocked_syscall_nr = () => kernel.abi_sys_ppoll();
  kernel.thread_blocked_syscall_arg = (_vmPtr: number, index: number) => {
    switch (index) {
      case 0:
        return pollfdsPtr;
      case 1:
        return 2n;
      default:
        return 0n;
    }
  };
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    new Uint8Array(kernel.memory.buffer).set(
      [3, 0, 0, 0, 1, 0, 1, 0, 4, 0, 0, 0, 4, 0, 4, 0],
      Number(pollfdsPtr),
    );
    kernel.setStatus(kernel.abi_status_ok());
    return 1n;
  });
  const handle = makeHandle(kernel, worker);
  const { helpers, threadState } = makeHelpers(handle, worker);

  await expect(
    helpers.executeHostBlockedSyscallResume(handle, worker, vmPtr, kernel.abi_status_pipe_wait()),
  ).resolves.toBeUndefined();

  const pendingWrites = snapshotPendingGuestMemoryWritesForNextStep(threadState);
  expect(pendingWrites).toHaveLength(1);
  expect(pendingWrites[0]).toMatchObject({
    guestPtr: Number(pollfdsPtr),
  });
  expect(pendingWrites[0].bytes.byteLength).toBe(2 * POLLFD_BYTES);
});
