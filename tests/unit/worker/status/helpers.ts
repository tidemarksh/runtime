import type { KernelExports } from "../../../../src/abi.js";
import {
  clearThreadRunningGeneration,
  setThreadHostIdentity,
  setThreadInitialized,
  setThreadOwnerAsyncStatus,
  setThreadReinitCount,
  setThreadRunningGeneration,
  setThreadWaitingInWorkerStatus,
} from "../../../../src/worker/thread-host-state.js";
import { getThreadActivityState } from "../../../../src/worker/thread-activity-state.js";
import { createWorkerMessageDebugStats } from "../../../../src/worker/types.js";
import { setProcessKernelWorkerRequest } from "../../../../src/worker/process-kernel-worker-request-state.js";
import {
  networkClosedFds,
  networkClosedSocketSlots,
  networkConnectionOfds,
  networkConnections,
  networkPendingConnectFds,
  networkRecvHeadWatermarks,
  networkRecvPayloadWatermarks,
  networkRecvTailWatermarks,
  networkSocketSlotWatermarks,
  type NetworkRecvPayloadWatermark,
  type NetworkSocketSlotSnapshot,
} from "../../../../src/worker/network-socket-state.js";
import type { KernelWorkerRequestFn, ProcessHandle } from "../../../../src/worker/types.js";
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";

type NetworkPayloadSeed = Omit<NetworkRecvPayloadWatermark, "recvBufIdx"> & {
  recvBufIdx?: number;
};

export function seedNetworkSocketState(handle: unknown, seed: {
  connections?: Iterable<readonly [number, unknown]>;
  connectionOfds?: Iterable<readonly [number, number]>;
  pendingConnectFds?: Iterable<number>;
  closedFds?: Iterable<number>;
  closedSocketSlots?: Iterable<readonly [number, NetworkSocketSlotSnapshot]>;
  socketSlotWatermarks?: Iterable<readonly [number, NetworkSocketSlotSnapshot]>;
  recvHeadWatermarks?: Iterable<readonly [number, number]>;
  recvTailWatermarks?: Iterable<readonly [number, number]>;
  recvPayloadWatermarks?: Iterable<readonly [number, NetworkPayloadSeed]>;
}): void {
  const processHandle = handle as ProcessHandle;
  for (const [fd, connection] of seed.connections ?? []) {
    networkConnections(processHandle).set(fd, connection as never);
  }
  for (const [fd, ofdId] of seed.connectionOfds ?? []) {
    networkConnectionOfds(processHandle).set(fd, ofdId);
  }
  for (const fd of seed.pendingConnectFds ?? []) {
    networkPendingConnectFds(processHandle).add(fd);
  }
  for (const fd of seed.closedFds ?? []) {
    networkClosedFds(processHandle).add(fd);
  }
  for (const [fd, slot] of seed.closedSocketSlots ?? []) {
    networkClosedSocketSlots(processHandle).set(fd, slot);
  }
  for (const [fd, slot] of seed.socketSlotWatermarks ?? []) {
    networkSocketSlotWatermarks(processHandle).set(fd, slot);
  }
  for (const [fd, head] of seed.recvHeadWatermarks ?? []) {
    networkRecvHeadWatermarks(processHandle).set(fd, head);
  }
  for (const [fd, tail] of seed.recvTailWatermarks ?? []) {
    networkRecvTailWatermarks(processHandle).set(fd, tail);
  }
  for (const [fd, snapshot] of seed.recvPayloadWatermarks ?? []) {
    networkRecvPayloadWatermarks(processHandle).set(fd, {
      recvBufIdx: snapshot.recvBufIdx ?? 0,
      head: snapshot.head,
      tail: snapshot.tail,
      capacity: snapshot.capacity,
      readBytes: snapshot.readBytes,
      availableBytes: snapshot.availableBytes,
      deliveredBytes: snapshot.deliveredBytes,
      data: snapshot.data,
    });
  }
}

export function makeKernelState(overrides: Record<string, unknown> = {}) {
  return {
    heapBase: 0,
    heapPtr: 0,
    ofdTablePtr: 0x1000,
    memfsPtr: 0,
    memStatePtr: 0,
    epollTablePtr: 0,
    socketTablePtr: 0x2000,
    socketUsed: 0,
    pipeBase: 0x3000,
    pipeUsed: 0n,
    netBufBase: 0x4000,
    netBufUsed: 0n,
    sigSlotsPtr: 0,
    processPendingPtr: 0,
    processRuntimeStatePtr: 0,
    timeStatePtr: 0,
    ...overrides,
  };
}

export function makeKernel(overrides: Record<string, unknown> = {}) {
  let kernel: Partial<KernelExports>;
  kernel = {
    ...makeSupervisorAbiStubs({
      currentExports: () => kernel,
    }),
    abi_max_fds: () => 8,
    abi_max_pipes: () => 4,
    abi_max_sockets: () => 1,
    abi_fd_type_file: () => 1,
    abi_fd_type_pipe: () => 3,
    abi_fd_type_socket: () => 8,
    abi_status_fs: () => 4,
    abi_status_execve_spawn: () => 9,
    abi_status_clone_spawn: () => 10,
    supervisor_process_status_is_execve_spawn: (status: number) => status === 9 ? 1 : 0,
    supervisor_process_status_defers_kernel_state_adoption: (status: number) => status === 10 ? 1 : 0,
    kernel_attach_runtime_state: () => {},
    kernel_ofd_table_bytes: () => 256,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_host_fd: () => -1,
    kernel_ofd_ino: () => 0,
    kernel_ofd_refcount: () => 0,
    kernel_ofd_offset: () => 0n,
    kernel_pipe_base: () => makeKernelState().pipeBase,
    kernel_pipe_region_bytes: () => 256,
    kernel_socket_table_bytes: () => 32,
    kernel_socket_table_ptr: () => undefined,
    kernel_socket_used: () => 0,
    kernel_net_buf_pool_bytes: () => 64,
    kernel_net_buf_used: () => 0n,
    process_fd_ofd_id: () => 0,
    process_fd_flags: () => 0,
    process_set_fd_entry: () => {},
    thread_process_ptr: () => 0,
    thread_socket_idx_for_fd: () => -1,
    thread_socket_recv_buf_idx: () => -1,
    thread_socket_read_bytes: () => 0n,
    thread_socket_outbound_available: () => 0,
    ...overrides,
  };
  return kernel;
}

export function makeHandle(overrides: Record<string, unknown> = {}) {
  const {
    kernel: overrideKernel,
    memory: overrideMemory,
    networkConnections: initialNetworkConnections,
    kernelWorkerRequest: initialKernelWorkerRequest,
    ...rest
  } = overrides;
  const memory = (overrideMemory as WebAssembly.Memory | undefined)
    ?? new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const kernel = (overrideKernel as Partial<KernelExports> | undefined) ?? makeKernel();
  kernel.memory ??= memory;
  const handle = {
    pid: 7,
    runtimeId: 1,
    vmPtr: 0x1234,
    guestBase: 0,
    memory,
    kernel,
    kernelState: makeKernelState(),
    stdoutPipe: 0,
    stderrPipe: 0,
    stdinPipe: 0,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    stdinQueue: [],
    outputBacklog: [],
    workers: new Set<Worker>(),
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Map(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    workerDebugLog: [],
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    runningWorker: null,
    onStdout: null,
    ...rest,
  };
  for (const [fd, connection] of initialNetworkConnections instanceof Map ? initialNetworkConnections : []) {
    networkConnections(handle as ProcessHandle).set(fd, connection as never);
  }
  setProcessKernelWorkerRequest(
    handle as ProcessHandle,
    initialKernelWorkerRequest as KernelWorkerRequestFn | undefined,
  );
  return handle;
}

export function makeStatusMessage(overrides: Record<string, unknown> = {}) {
  return {
    type: "status",
    status: 0,
    vmPtr: 0x1234,
    exitCode: 0,
    termSignal: 0,
    pc: 0x2000n,
    sp: 0x3000n,
    s11: 0x4000n,
    kernelState: makeKernelState(),
    debug: "",
    ...overrides,
  };
}

export function makeThreadState(overrides: Record<string, unknown> = {}) {
  const {
    generation = 1,
    initialized = true,
    ownerAsyncStatus,
    ownerPid = 7,
    reinitCount = 0,
    runningGeneration = 1,
    waitingInWorkerStatus,
    ...rest
  } = overrides;
  const thread = {
    vmPtr: 0x1234,
    statusCount: 0,
    statusCounts: {},
    ...rest,
  };
  Object.assign(getThreadActivityState(thread as never), rest);
  if (typeof ownerPid === "number" && typeof generation === "number") {
    setThreadHostIdentity(thread as never, ownerPid, generation);
  }
  setThreadInitialized(thread as never, initialized === true);
  if (typeof reinitCount === "number") {
    setThreadReinitCount(thread as never, reinitCount);
  }
  if (typeof runningGeneration === "number") {
    setThreadRunningGeneration(thread as never, runningGeneration);
  } else {
    clearThreadRunningGeneration(thread as never);
  }
  if (typeof ownerAsyncStatus === "number") {
    setThreadOwnerAsyncStatus(thread as never, ownerAsyncStatus);
  }
  if (typeof waitingInWorkerStatus === "number") {
    setThreadWaitingInWorkerStatus(thread as never, waitingInWorkerStatus);
  }
  return thread;
}
