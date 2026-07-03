import { expect, test, vi } from "vite-plus/test";
import { createWorkerOwnedStateSyncHandler } from "../../../../src/worker/message-handler/owned-state-sync.js";

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

function readOptionalU64(value: bigint | number | undefined): bigint | null {
  if (value === undefined) {
    return null;
  }
  return typeof value === "bigint" ? value : BigInt(value);
}

function syscallMatches(kernel: Record<string, unknown>, nr: bigint | null, exportName: string): boolean {
  const readSyscall = kernel[exportName];
  if (nr === null || typeof readSyscall !== "function") {
    return false;
  }
  return nr === BigInt((readSyscall as () => bigint | number)());
}

function fcntlCommandMayChangeTopology(command: bigint | null): boolean {
  return command === null || command === 4n;
}

function ioctlRequestMayChangeTopology(request: bigint | null): boolean {
  return request === null || request === 0x5421n;
}

function installSupervisorFdPolicy(kernel: Record<string, unknown>): void {
  const currentSyscall = -1n;
  const traceArgUnknown = -1n;
  const topologyUnresolved = 0;
  const topologyRulesOut = 1;
  const topologyNeedsPeerCheck = 2;
  const syscallNr = (vmPtr: number, syscall: bigint | number): bigint | null => {
    const nr = BigInt(syscall);
    if (nr !== currentSyscall) {
      return nr;
    }
    return readOptionalU64((kernel.thread_a7 as ((vmPtr: number) => bigint | number) | undefined)?.(vmPtr));
  };
  const syscallResultIsError = (vmPtr: number): boolean => {
    const result = readOptionalU64((kernel.thread_a0 as ((vmPtr: number) => bigint | number) | undefined)?.(vmPtr));
    return result !== null && BigInt.asIntN(64, result) < 0n;
  };
  const syscallArg1 = (vmPtr: number): bigint | null =>
    readOptionalU64((kernel.thread_a1 as ((vmPtr: number) => bigint | number) | undefined)?.(vmPtr));
  const processOfdTypeForFd = (vmPtr: number, fd: number): number => {
    const maxFds = (kernel.abi_max_fds as (() => number) | undefined)?.() ?? 0;
    if (!Number.isInteger(fd) || fd < 0 || fd >= maxFds) {
      return 0;
    }
    const ofdId = (kernel.process_fd_ofd_id as ((vmPtr: number, fd: number) => number) | undefined)?.(vmPtr, fd) ?? 0;
    if (ofdId <= 0 || ofdId >= maxFds) {
      return 0;
    }
    return (kernel.kernel_ofd_fd_type as ((ofdId: number) => number) | undefined)?.(ofdId) ?? 0;
  };

  Object.assign(kernel, {
    supervisor_fd_syscall_nr_current: () => currentSyscall,
    supervisor_fd_trace_arg_unknown: () => traceArgUnknown,
    supervisor_fd_trace_topology_action_unresolved: () => topologyUnresolved,
    supervisor_fd_trace_topology_action_rules_out: () => topologyRulesOut,
    supervisor_fd_trace_topology_action_needs_peer_check: () => topologyNeedsPeerCheck,
    supervisor_fd_syscall_may_change_fork_shared_ofd_topology: (vmPtr: number, syscall: bigint | number) => {
      if (syscallResultIsError(vmPtr)) {
        return 0;
      }
      const nr = syscallNr(vmPtr, syscall);
      if (syscallMatches(kernel, nr, "abi_sys_fcntl")) {
        return fcntlCommandMayChangeTopology(syscallArg1(vmPtr)) ? 1 : 0;
      }
      if (syscallMatches(kernel, nr, "abi_sys_ioctl")) {
        return ioctlRequestMayChangeTopology(syscallArg1(vmPtr)) ? 1 : 0;
      }
      return [
        "abi_sys_openat",
        "abi_sys_close",
        "abi_sys_close_range",
        "abi_sys_pipe2",
        "abi_sys_socket",
        "abi_sys_socketpair",
        "abi_sys_accept4",
        "abi_sys_eventfd2",
        "abi_sys_epoll_create1",
        "abi_sys_dup",
        "abi_sys_dup3",
        "abi_sys_execve",
      ].some((name) => syscallMatches(kernel, nr, name)) ? 1 : 0;
    },
    supervisor_fd_explicit_fork_shared_effect_may_require_sync: (vmPtr: number, syscall: bigint | number) => {
      if (syscallResultIsError(vmPtr)) {
        return 0;
      }
      const nr = syscallNr(vmPtr, syscall);
      if (syscallMatches(kernel, nr, "abi_sys_fcntl")) {
        return fcntlCommandMayChangeTopology(syscallArg1(vmPtr)) ? 1 : 0;
      }
      if (syscallMatches(kernel, nr, "abi_sys_ioctl")) {
        return ioctlRequestMayChangeTopology(syscallArg1(vmPtr)) ? 1 : 0;
      }
      return 1;
    },
    supervisor_fd_syscall_only_regular_file_offset_sync_when_peer_shared: (vmPtr: number, syscall: bigint | number) => {
      const nr = syscallNr(vmPtr, syscall);
      if (![
        "abi_sys_read",
        "abi_sys_readv",
        "abi_sys_write",
        "abi_sys_writev",
        "abi_sys_pwrite64",
        "abi_sys_lseek",
      ].some((name) => syscallMatches(kernel, nr, name))) {
        return 0;
      }
      const fd = Number(readOptionalU64((kernel.thread_a0 as ((vmPtr: number) => bigint | number) | undefined)?.(vmPtr)) ?? -1n);
      return processOfdTypeForFd(vmPtr, fd) === (kernel.abi_fd_type_file as (() => number) | undefined)?.() ? 1 : 0;
    },
    supervisor_fd_trace_fork_shared_topology_action: (
      vmPtr: number,
      nrValue: bigint | number,
      arg1Value: bigint | number,
      resultValue: bigint | number,
      resultKnown: number,
    ) => {
      const nr = BigInt(nrValue);
      const arg1 = BigInt(arg1Value) === traceArgUnknown ? null : BigInt(arg1Value);
      if (resultKnown !== 0 && BigInt.asIntN(64, BigInt(resultValue)) < 0n) {
        return topologyRulesOut;
      }
      if (syscallMatches(kernel, nr, "abi_sys_dup3")) {
        const newFd = arg1 === null ? -1 : Number(arg1);
        if (processOfdTypeForFd(vmPtr, newFd) === (kernel.abi_fd_type_file as (() => number) | undefined)?.()) {
          return topologyUnresolved;
        }
        return topologyUnresolved;
      }
      if ([
        "abi_sys_close",
        "abi_sys_dup",
        "abi_sys_pipe2",
        "abi_sys_socket",
        "abi_sys_socketpair",
        "abi_sys_accept4",
        "abi_sys_eventfd2",
        "abi_sys_epoll_create1",
      ].some((name) => syscallMatches(kernel, nr, name))) {
        return topologyNeedsPeerCheck;
      }
      if (syscallMatches(kernel, nr, "abi_sys_fcntl")) {
        return fcntlCommandMayChangeTopology(arg1) ? topologyNeedsPeerCheck : topologyRulesOut;
      }
      if (syscallMatches(kernel, nr, "abi_sys_ioctl")) {
        return ioctlRequestMayChangeTopology(arg1) ? topologyNeedsPeerCheck : topologyRulesOut;
      }
      return topologyUnresolved;
    },
    supervisor_fd_trace_data_plane_io_uses_socket_or_pipe: (
      nrValue: bigint | number,
      arg0OfdId: number,
      arg0OfdType: number,
      resultValue: bigint | number,
      resultKnown: number,
    ) => {
      const nr = BigInt(nrValue);
      if (resultKnown === 0 || BigInt.asIntN(64, BigInt(resultValue)) <= 0n) {
        return 0;
      }
      if (![
        "abi_sys_read",
        "abi_sys_readv",
        "abi_sys_write",
        "abi_sys_writev",
        "abi_sys_recvfrom",
        "abi_sys_recvmsg",
        "abi_sys_sendto",
        "abi_sys_sendmsg",
      ].some((name) => syscallMatches(kernel, nr, name))) {
        return 0;
      }
      const maxFds = (kernel.abi_max_fds as (() => number) | undefined)?.() ?? 0;
      const fdType = arg0OfdType > 0 || arg0OfdId <= 0 || arg0OfdId >= maxFds
        ? arg0OfdType
        : (kernel.kernel_ofd_fd_type as ((ofdId: number) => number) | undefined)?.(arg0OfdId) ?? 0;
      return fdType === (kernel.abi_fd_type_socket as (() => number) | undefined)?.()
        || fdType === (kernel.abi_fd_type_pipe as (() => number) | undefined)?.() ? 1 : 0;
    },
    supervisor_fd_trace_regular_file_write_mutated_memfs: (
      nrValue: bigint | number,
      arg0OfdType: number,
      resultValue: bigint | number,
      resultKnown: number,
    ) => {
      const nr = BigInt(nrValue);
      return resultKnown !== 0
        && BigInt.asIntN(64, BigInt(resultValue)) > 0n
        && syscallMatches(kernel, nr, "abi_sys_write")
        && arg0OfdType === (kernel.abi_fd_type_file as (() => number) | undefined)?.()
        ? 1
        : 0;
    },
  });
}

test("socket state sync runs before fork-shared OFD sync when both effects are present", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
    ownerAsyncStatus: null,
    resumeWorkersOnReady: [],
  };
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
      forkSharedSyncPromise: null,
    },
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    kernelState: makeKernelState(),
    kernel: {
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
      ...handle.kernel,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 0 ? 2 : 0),
    },
  };
  handle.kernelShare.members.add(handle);
  handle.kernelShare.members.add(peer);
  const continueAfterOwnedStateSync = vi.fn(() => {
    order.push("continue");
  });
  const deps = {
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
  installSupervisorFdPolicy(handle.kernel);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    rawSyncEffects: 66,
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
    resumeWorkersOnReady: [],
  };
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
      forkSharedSyncPromise: null,
    },
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    kernelState: makeKernelState(),
    kernel: {
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
      ...handle.kernel,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 0 ? 2 : 0),
    },
  };
  handle.kernelShare.members.add(handle);
  handle.kernelShare.members.add(peer);
  const continueAfterOwnedStateSync = vi.fn();
  let resolveSync!: () => void;
  const syncForkSharedStateViaKernelWorker = vi.fn(() => new Promise<void>((resolve) => {
    resolveSync = resolve;
  }));
  const deps = {
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
  installSupervisorFdPolicy(handle.kernel);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    rawSyncEffects: 64,
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
  expect(thread.ownerAsyncStatus).toBe(0);
  expect(handle.forkSharedSyncPromise).toBeNull();
  expect(handle.kernelShare.forkSharedSyncPromise).toBeNull();
  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    7,
    "fork-shared-sync-local:7:src=0x1234:targets=none",
  );
  expect(continueAfterOwnedStateSync).not.toHaveBeenCalled();
  resolveSync();
  await vi.waitFor(() => {
    expect(continueAfterOwnedStateSync).toHaveBeenCalled();
  });
  expect(thread.ownerAsyncStatus).toBeNull();
  expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalledWith(handle, [], {
    cause: { kind: "syscall", nr: -1n, name: "owned-state-empty-fork-shared-sync" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: true,
    sourceVmPtr: 0x1234,
  });
});

test("dup3 replacement cannot be ruled out by oldfd-only fd trace", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
    resumeWorkersOnReady: [],
  };
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
      forkSharedSyncPromise: null,
    },
    hostStdio: { refCount: 0, stdinPipe: 0, stdoutPipe: 0, stderrPipe: 0 },
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    kernelState: makeKernelState(),
    kernel: {
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
      ...handle.kernel,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 4 ? 7 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 7 ? 6 : 0),
      kernel_ofd_ino: (ofdId: number) => (ofdId === 7 ? 5 : 0),
      kernel_ofd_host_fd: (ofdId: number) => (ofdId === 7 ? 2 : 0),
    },
  };
  handle.kernelShare.members.add(handle);
  handle.kernelShare.members.add(parent);
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
  installSupervisorFdPolicy(handle.kernel);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    rawSyncEffects: 64,
    completedSyncEffects: 64,
    latestFdTrace: {
      nr: 24n,
      arg0OfdId: 10,
      arg0OfdType: 1,
      arg1: 2n,
      arg2OfdId: 0,
      arg2OfdType: 0,
      result: 2n,
    },
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
    resumeWorkersOnReady: [],
  };
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
      forkSharedSyncPromise: null,
    },
    hostStdio: { refCount: 0, stdinPipe: 0, stdoutPipe: 0, stderrPipe: 0 },
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    kernelState: makeKernelState(),
    kernel: {
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
      ...handle.kernel,
      process_fd_ofd_id: () => 0,
    },
  };
  handle.kernelShare.members.add(handle);
  handle.kernelShare.members.add(parent);
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
  installSupervisorFdPolicy(handle.kernel);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    rawSyncEffects: 64,
    completedSyncEffects: 64,
    latestFdTrace: {
      nr: 25n,
      arg0OfdId: 8,
      arg0OfdType: 1,
      arg1: 4n,
      arg2OfdId: 0,
      arg2OfdType: 0,
      result: 0n,
    },
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(false);
  expect(syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
});

test("kernel fd trace pipe write skips fork-shared sync when caller omits latest trace", () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
    resumeWorkersOnReady: [],
  };
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
      forkSharedSyncPromise: null,
    },
    hostStdio: { refCount: 0, stdinPipe: 0, stdoutPipe: 5, stderrPipe: 0 },
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    kernelState: makeKernelState(),
    kernel: {
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
      debug_fd_syscall_trace_len: () => 1,
      debug_fd_syscall_trace_vm_ptr: () => 0x1234,
      debug_fd_syscall_trace_nr: () => 64n,
      debug_fd_syscall_trace_arg0_ofd_id: () => 3,
      debug_fd_syscall_trace_arg0_ofd_type: () => 6,
      debug_fd_syscall_trace_arg1: () => 0x2000n,
      debug_fd_syscall_trace_arg2_ofd_id: () => 0,
      debug_fd_syscall_trace_arg2_ofd_type: () => 0,
      debug_fd_syscall_trace_result: () => 11n,
    },
  };
  handle.kernelShare.members.add(handle);
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
  installSupervisorFdPolicy(handle.kernel);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    rawSyncEffects: 64,
    completedSyncEffects: 64,
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
    resumeWorkersOnReady: [],
  };
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
      forkSharedSyncPromise: null,
    },
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    kernelState: makeKernelState(),
    kernel: {
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
    exitInProgress: true,
    workers: new Set(),
  };
  handle.kernelShare.members.add(handle);
  handle.kernelShare.members.add(exitingPeer);
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
  installSupervisorFdPolicy(handle.kernel);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    rawSyncEffects: 64,
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
    resumeWorkersOnReady: [],
  };
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
      forkSharedSyncPromise: null,
    },
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    kernelState: makeKernelState(),
    kernel: {
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
      ...handle.kernel,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => (fd === 4 ? 7 : 0),
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 7 ? 6 : 0),
      kernel_ofd_ino: (ofdId: number) => (ofdId === 7 ? 6 : 0),
      kernel_ofd_host_fd: (ofdId: number) => (ofdId === 7 ? 10 : 0),
    },
  };
  handle.kernelShare.members.add(handle);
  handle.kernelShare.members.add(parent);
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
  installSupervisorFdPolicy(handle.kernel);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    rawSyncEffects: 64,
    completedSyncEffects: 64,
    latestFdTrace: null,
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
    resumeWorkersOnReady: [],
  };
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
      forkSharedSyncPromise: activeSync,
    },
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    kernelState: makeKernelState(),
    kernel: {
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
  handle.kernelShare.members.add(handle);
  handle.kernelShare.members.add({
    pid: 3,
    vmPtr: 0x5678,
    runningWorker: null,
    exitInProgress: false,
    kernel: handle.kernel,
    kernelShare: handle.kernelShare,
  } as never);
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
  installSupervisorFdPolicy(handle.kernel);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    rawSyncEffects: 64,
    completedSyncEffects: 64,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(true);
  await vi.waitFor(() => {
    expect(handle.forkSharedSyncPromise).not.toBeNull();
  });
  expect(handle.forkSharedSyncPromise).toBe(handle.kernelShare.forkSharedSyncPromise);
  expect(handle.forkSharedSyncPromise).not.toBe(activeSync);

  resolveActiveSync();
  await vi.waitFor(() => {
    expect(continueAfterOwnedStateSync).toHaveBeenCalled();
  });

  expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalledTimes(1);
  expect(handle.forkSharedSyncPromise).toBeNull();
  expect(handle.kernelShare.forkSharedSyncPromise).toBeNull();
});

test("queued no-target fork-shared OFD sync waits without replacing the active share promise", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
    resumeWorkersOnReady: [],
  };
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
      forkSharedSyncPromise: activeSync,
    },
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    kernelState: makeKernelState(),
    kernel: {
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
  handle.kernelShare.members.add(handle);
  handle.kernelShare.members.add({
    pid: 0,
    vmPtr: 0,
    runningWorker: null,
    exitInProgress: false,
    kernel: handle.kernel,
    kernelShare: handle.kernelShare,
  } as never);
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
  installSupervisorFdPolicy(handle.kernel);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    syscallNr: 32n,
    rawSyncEffects: 64,
    completedSyncEffects: 64,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(true);
  expect(handle.forkSharedSyncPromise).toBeNull();
  expect(handle.kernelShare.forkSharedSyncPromise).toBe(activeSync);
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
  expect(handle.forkSharedSyncPromise).toBeNull();
  expect(handle.kernelShare.forkSharedSyncPromise).toBe(activeSync);
});

test("fork-shared-only pipe effects sync when the source holds tracked shared state", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
    resumeWorkersOnReady: [],
  };
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
      forkSharedSyncPromise: null,
    },
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    kernelState: makeKernelState(),
    kernel: {
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
  handle.kernelShare.members.add(handle);
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
  installSupervisorFdPolicy(handle.kernel);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    rawSyncEffects: 64,
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
    resumeWorkersOnReady: [],
  };
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
      forkSharedSyncPromise: null,
    },
    forkSharedSyncPromise: null,
    processMemfsDirty: true,
    kernelState: makeKernelState(),
    kernel: {
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
  handle.kernelShare.members.add(handle);
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
  installSupervisorFdPolicy(handle.kernel);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    rawSyncEffects: 1,
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
  expect(handle.processMemfsDirty).toBe(false);
  expect(order).toEqual(["memfs", "continue"]);
});

test("dirty process memfs is deferred on a successful process-memfs-only effect", async () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    waitingInWorkerStatus: null,
    resumeWorkersOnReady: [],
  };
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
      forkSharedSyncPromise: null,
    },
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    kernelState: makeKernelState(),
    kernel: {
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
  handle.kernelShare.members.add(handle);
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
  installSupervisorFdPolicy(handle.kernel);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    rawSyncEffects: 128,
    completedSyncEffects: 128,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(false);
  expect(continueAfterOwnedStateSync).not.toHaveBeenCalled();
  expect(syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  expect(handle.processMemfsDirty).toBe(true);
});

test("active vfork child pid mismatch defers parent-owned state sync", () => {
  const worker = {} as Worker;
  const thread = {
    vmPtr: 0x1234,
    ownerPid: 1,
    generation: 1,
    waitingInWorkerStatus: null,
    resumeWorkersOnReady: [],
  };
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  threadWorkerStates.set(worker, thread);
  const handle = {
    pid: 1,
    vmPtr: 0x1234,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    workers: new Set([worker]),
    kernelWorkerRequest: vi.fn(),
    kernelShare: {
      members: new Set(),
      forkSharedSyncPromise: null,
    },
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
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
      abi_status_ok: () => 0,
      abi_max_fds: () => 1,
      abi_max_pipes: () => 1,
      abi_max_threads: () => 1,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_pipe: () => 6,
      process_current_pid: () => 3,
      process_thread_vm_ptr: () => 0,
      process_fd_ofd_id: () => 0,
      kernel_ofd_fd_type: () => 0,
      kernel_ofd_ino: () => 0,
      kernel_ofd_host_fd: () => 0,
      kernel_attach_runtime_state: vi.fn(),
    },
  };
  handle.kernelShare.members.add(handle);
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
  installSupervisorFdPolicy(handle.kernel);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps as never);

  const handled = maybeHandleOwnedStateSync({
    handle: handle as never,
    worker,
    thread: thread as never,
    status: 0,
    rawSyncEffects: 66,
    completedSyncEffects: 66,
    isRunningWorker: true,
    wroteToPipe: false,
    drainedPipe: false,
    continueAfterOwnedStateSync,
  });

  expect(handled).toBe(false);
  expect(deps.requestSocketStateSync).not.toHaveBeenCalled();
  expect(deps.syncForkSharedStateViaKernelWorker).not.toHaveBeenCalled();
  expect(continueAfterOwnedStateSync).not.toHaveBeenCalled();
});
