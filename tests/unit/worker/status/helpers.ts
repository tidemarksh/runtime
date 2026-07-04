import type { KernelExports } from "../../../../src/abi.js";
import { createWorkerMessageDebugStats } from "../../../../src/worker/types.js";
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";

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
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  return {
    pid: 7,
    runtimeId: 1,
    vmPtr: 0x1234,
    guestBase: 0,
    memory,
    kernel: makeKernel(),
    kernelState: makeKernelState(),
    kernelWorkerRequest: undefined,
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
    networkConnections: new Map(),
    networkConnectionOfds: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    networkRecvPayloadWatermarks: new Map(),
    networkSocketSlotWatermarks: new Map(),
    networkClosedSocketSlots: new Map(),
    pendingPipeSlotSnapshots: [],
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    workerDebugLog: [],
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    runningWorker: null,
    onStdout: null,
    ...overrides,
  };
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
  return {
    vmPtr: 0x1234,
    ownerPid: 7,
    generation: 1,
    runningGeneration: 1,
    initialized: true,
    statusCount: 0,
    statusCounts: {},
    waitingInWorkerStatus: null,
    resumeWorkersOnReady: [],
    ...overrides,
  };
}
