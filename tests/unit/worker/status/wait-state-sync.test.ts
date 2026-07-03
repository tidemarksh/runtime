import { afterEach, beforeEach, expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../../src/abi.js";
import { writePipe } from "../../../../src/bridge/pipe.js";
import { readPublishedKernelRuntimeState } from "../../../../src/bridge/runtime-state.js";
import type {
  KernelWorkerOutbound,
  KernelWorkerRequest,
  ThreadWorkerInbound,
  ThreadWorkerOutbound,
} from "../../../../src/messages.js";
import {
  makeSupervisorAbiStubs,
  writeSupervisorArmHostTimerHostOpRecord,
  writeSupervisorRunnableResumeRecord,
} from "../support/supervisor-abi.js";

type WorkerModule = typeof import("../../../../src/worker.js");

let workerModule: WorkerModule | null = null;

function clearRuntimeFsState(runtimeId: number): void {
  workerModule?.clearRuntimeFsState(runtimeId);
}

function clearModuleCache(): void {
  workerModule?.clearModuleCache();
}

function createProcess(...args: Parameters<WorkerModule["createProcess"]>) {
  pendingCreatePid = args[2]?.pid ?? null;
  return workerModule!.createProcess(...args);
}

function destroyProcessTree(...args: Parameters<WorkerModule["destroyProcessTree"]>): void {
  workerModule?.destroyProcessTree(...args);
}

function injectHttpRequest(...args: Parameters<WorkerModule["injectHttpRequest"]>) {
  return workerModule!.injectHttpRequest(...args);
}

function mirrorRuntimeFsEntry(...args: Parameters<WorkerModule["mirrorRuntimeFsEntry"]>): void {
  workerModule?.mirrorRuntimeFsEntry(...args);
}

function primeRuntimeFsState(...args: Parameters<WorkerModule["primeRuntimeFsState"]>) {
  return workerModule!.primeRuntimeFsState(...args);
}

function replaceRuntimeFsSnapshot(...args: Parameters<WorkerModule["replaceRuntimeFsSnapshot"]>): void {
  workerModule?.replaceRuntimeFsSnapshot(...args);
}

function writeProcessStdin(...args: Parameters<WorkerModule["writeProcessStdin"]>): void {
  workerModule?.writeProcessStdin(...args);
}

class FakeWorker {
  onmessage:
    | ((event: MessageEvent<ThreadWorkerOutbound>) => void)
    | null = null;
  readonly postedMessages: ThreadWorkerInbound[] = [];
  terminated = false;

  constructor(
    readonly _url: URL,
    readonly _options: WorkerOptions,
  ) {}

  postMessage(message: ThreadWorkerInbound): void {
    if (message.type === "prepare") {
      queueMicrotask(() => {
        this.onmessage?.({ data: { type: "prepared", wasmFeatures: {} } as ThreadWorkerOutbound } as MessageEvent<ThreadWorkerOutbound>);
      });
      return;
    }
    this.postedMessages.push(stripWorkerStateSnapshots(message));
  }

  terminate(): void {
    this.terminated = true;
  }

  emit(message: ThreadWorkerOutbound): void {
    this.onmessage?.({ data: message } as MessageEvent<ThreadWorkerOutbound>);
  }

  clearPostedMessages(): void {
    this.postedMessages.length = 0;
  }
}

function stripWorkerStateSnapshots(message: ThreadWorkerInbound): ThreadWorkerInbound {
  if (message.type !== "init" && message.type !== "step") {
    return message;
  }
  const stripped = { ...message } as ThreadWorkerInbound & {
    fdEntries?: unknown;
    ofdSlots?: unknown;
  };
  delete stripped.fdEntries;
  delete stripped.ofdSlots;
  return stripped;
}

const createdWorkers: FakeWorker[] = [];
const createdKernelExports: Array<
  KernelExports & {
    recordedChildExits: Array<[number, number]>;
    retainedOfds: number[];
    liveThreadVmPtrs: number[];
    fsProcessCalls: number;
    memfsCreateCalls: Array<[number, number]>;
    memfsTruncateCalls: Array<[number, number]>;
    memfsSetFileSizeShallowCalls: Array<[number, number]>;
    memfsWriteCalls: Array<[number, number]>;
    memfsCreateWithDataCalls: Array<[number, number]>;
  }
> = [];
const threadCountQueue: number[] = [];
const injectedHttpRequests: Array<{ port: number; request: string }> = [];
const httpResponseChunks: Uint8Array[] = [];
const closedHttpConnections: number[] = [];
const originalWorker = globalThis.Worker;
const originalSchedulerStepBudget = process.env.TIDEMARK_SCHEDULER_STEP_BUDGET;
let pendingCreatePid: number | null = null;
let pendingExecveChildPid: number | null = null;

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 32,
    memfsInodesBytes: 16384 * 32,
    memfsFirstChildBytes: 16384 * 4,
    memfsDirEntriesBytes: 16384 * 264,
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

function makePostedKernelStateExpectation(
  overrides: Partial<KernelRuntimeState> = {},
): Partial<KernelRuntimeState> {
  const state = makeKernelState();
  return {
    heapBase: state.heapBase,
    heapPtr: state.heapPtr,
    ofdTablePtr: state.ofdTablePtr,
    memfsPtr: state.memfsPtr,
    memfsHeaderBytes: state.memfsHeaderBytes,
    memfsInodesBytes: state.memfsInodesBytes,
    memfsFirstChildBytes: state.memfsFirstChildBytes,
    memfsDirEntriesBytes: state.memfsDirEntriesBytes,
    memfsDataBytes: state.memfsDataBytes,
    memStatePtr: state.memStatePtr,
    epollTablePtr: state.epollTablePtr,
    socketTablePtr: state.socketTablePtr,
    socketUsed: state.socketUsed,
    pipeBase: state.pipeBase,
    netBufBase: state.netBufBase,
    netBufUsed: state.netBufUsed,
    sigSlotsPtr: state.sigSlotsPtr,
    processPendingPtr: state.processPendingPtr,
    processRuntimeStatePtr: state.processRuntimeStatePtr,
    timeStatePtr: state.timeStatePtr,
    ...overrides,
  };
}

function makeStepMessageExpectation(
  kernelState: Partial<KernelRuntimeState> = makePostedKernelStateExpectation(),
): ThreadWorkerInbound {
  return {
    type: "step",
    budget: 20_000,
    kernelState: expect.objectContaining(kernelState),
  } as ThreadWorkerInbound;
}

function attachKernelStateForTest(
  handle: { kernel: KernelExports; kernelState: KernelRuntimeState },
  state: KernelRuntimeState,
): void {
  handle.kernelState = state;
  handle.kernel.kernel_attach_runtime_state(
    state.heapBase,
    state.heapPtr,
    state.ofdTablePtr,
    state.memfsPtr,
    state.memStatePtr,
    state.epollTablePtr,
    state.socketTablePtr,
    state.socketUsed,
    state.pipeBase,
    state.pipeUsed,
    state.netBufBase,
    state.netBufUsed,
    state.sigSlotsPtr,
    state.processPendingPtr,
    state.processRuntimeStatePtr,
    state.timeStatePtr,
  );
}

const SYNC_EFFECT_PIPE_STATE = 1 << 0;
const SYNC_EFFECT_SOCKET_STATE = 1 << 1;
const SYNC_EFFECT_EPOLL_STATE = 1 << 2;
const SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE = 1 << 3;
const SYNC_EFFECT_PROCESS_LIFECYCLE = 1 << 4;
const SYNC_EFFECT_FORK_SHARED_OFD_STATE = 1 << 6;
const MOCK_PIPE_SLOT_BYTES = 32 + 65_536;

interface TestSupervisorTimerHostOp {
  vmPtr: number;
  status: number;
  delayMs: bigint;
  deadlineTicks: bigint;
  seq: bigint;
}

interface WaitStateSyncKernelExports extends KernelExports {
  recordedChildExits: Array<[number, number]>;
  fsProcessCalls: number;
  memfsCreateCalls: Array<[number, number]>;
  memfsTruncateCalls: Array<[number, number]>;
  memfsSetFileSizeShallowCalls: Array<[number, number]>;
  memfsWriteCalls: Array<[number, number]>;
  memfsCreateWithDataCalls: Array<[number, number]>;
  supervisorRunnableVmPtrs: number[];
}

function emitReady(
  worker: FakeWorker,
  vmPtr = 0x1234,
  kernelState = makeKernelState(),
): void {
  worker.emit({
    type: "ready",
    vmPtr,
    kernelState,
    wasmFeatures: {
      memory64: true,
      tailCalls: true,
      exceptionHandling: true,
    },
  });
}

async function flushMicrotasks(turns = 20): Promise<void> {
  for (let i = 0; i < turns; i++) {
    await Promise.resolve();
  }
}

function makeKernelExports(memory: WebAssembly.Memory): KernelExports {
  let nextVmPtr = 0x2000;
  let runtimeState = makeKernelState();
  const threadCount = threadCountQueue.shift() ?? 1;
  const recordedChildExits: Array<[number, number]> = [];
  const retainedOfds: number[] = [];
  const liveThreadVmPtrs = Array.from({ length: threadCount }, (_, index) => 0x1234 + index * 0x10);
  let fsProcessCalls = 0;
  const memfsCreateCalls: Array<[number, number]> = [];
  const memfsTruncateCalls: Array<[number, number]> = [];
  const memfsSetFileSizeShallowCalls: Array<[number, number]> = [];
  const memfsWriteCalls: Array<[number, number]> = [];
  const memfsCreateWithDataCalls: Array<[number, number]> = [];
  const pendingSupervisorTimers: TestSupervisorTimerHostOp[] = [];
  const supervisorRunnableVmPtrs: number[] = [];
  let nextSupervisorTimerSeq = 1n;
  let monotonicTicks = 0n;
  let nextIno = 10;
  interface MockProcessIdentity {
    pid: number;
    ppid: number;
    pgid: number;
    sid: number;
    didExec: number;
  }
  const processIdentities = new Map<number, MockProcessIdentity>();
  let currentPid = pendingExecveChildPid ?? pendingCreatePid ?? 1;
  let currentPpid = 1;
  let currentPgid = 1;
  let currentSid = 1;
  let currentDidExec = 0;
  pendingCreatePid = null;
  pendingExecveChildPid = null;
  const identityForVm = (vmPtr: number): MockProcessIdentity => processIdentities.get(vmPtr) ?? {
    pid: currentPid,
    ppid: currentPpid,
    pgid: currentPgid,
    sid: currentSid,
    didExec: currentDidExec,
  };
  const setIdentityForVm = (
    vmPtr: number,
    pid: number,
    ppid = currentPpid,
    pgid = currentPgid,
    sid = currentSid,
    didExec = currentDidExec,
  ): void => {
    processIdentities.set(vmPtr, { pid, ppid, pgid, sid, didExec });
    currentPid = pid;
    currentPpid = ppid;
    currentPgid = pgid;
    currentSid = sid;
    currentDidExec = didExec;
  };
  function initPipeSlot(slot: number): void {
    const base = runtimeState.pipeBase + slot * MOCK_PIPE_SLOT_BYTES;
    const view = new DataView(memory.buffer);
    view.setInt32(base + 8, 1, true);
    view.setInt32(base + 12, 1, true);
  }
  let exports: WaitStateSyncKernelExports;
  exports = {
    ...makeSupervisorAbiStubs({ currentExports: () => exports }),
    memory,
    __stack_pointer: new WebAssembly.Global({ value: "i32", mutable: true }, 0),
    abi_status_ok: () => 0,
    abi_status_fault: () => 3,
    abi_status_fs: () => 4,
    abi_status_futex_wait: () => 5,
    abi_status_pipe_wait: () => 6,
    abi_status_epoll_wait: () => 7,
    abi_status_vfork_wait: () => 8,
    abi_status_execve_spawn: () => 9,
    abi_status_clone_spawn: () => 10,
    abi_status_sleep: () => 11,
    abi_status_jit_ready: () => 12,
    abi_status_wait4: () => 31,
    abi_status_connect: () => 28,
    abi_status_socket_send: () => 29,
    abi_status_socket_recv: () => 30,
    abi_status_flock_wait: () => 32,
    abi_spawn_kind_none: () => 0,
    abi_spawn_kind_execve: () => 1,
    abi_spawn_kind_fork: () => 2,
    abi_page_size: () => 4096,
    abi_guest_address_profile_target: () => 2,
    abi_max_fds: () => 256,
    abi_max_threads: () => 64,
    abi_max_pipes: () => 64,
    abi_fd_type_none: () => 0,
    abi_fd_type_file: () => 1,
    abi_fd_type_pipe: () => 2,
    abi_fd_type_epoll: () => 3,
    abi_fd_type_socket: () => 4,
    abi_sigkill: () => 9,
    abi_sigterm: () => 15,
    abi_sigchld: () => 17,
    abi_sigpipe: () => 13,
    abi_clone_parent: () => 0x00008000,
    abi_o_rdonly: () => 0,
    abi_o_wronly: () => 1,
    abi_o_rdwr: () => 2,
    abi_o_creat: () => 0x40,
    abi_o_trunc: () => 0x200,
    abi_o_append: () => 0x400,
    abi_o_cloexec: () => 0x80000,
    abi_enoent: () => 2,
    abi_ebadf: () => 9,
    abi_einval: () => 22,
    abi_enomem: () => 12,
    abi_enosys: () => 38,
    abi_eintr: () => 4,
    abi_eagain: () => 11,
    abi_epipe: () => 32,
    abi_econnrefused: () => 111,
    set_base_time: () => {},
    sync_monotonic_ticks_floor: (ticks) => {
      const nextTicks = BigInt(ticks);
      if (nextTicks > monotonicTicks) {
        monotonicTicks = nextTicks;
      }
    },
    monotonic_ticks_current: () => monotonicTicks,
    kernel_init: () => {},
    kernel_heap_base: () => runtimeState.heapBase,
    kernel_heap_ptr: () => runtimeState.heapPtr,
    kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
    kernel_ofd_table_bytes: () => 256 * 32,
    kernel_memfs_ptr: () => runtimeState.memfsPtr,
    kernel_memfs_header_bytes: () => 32,
    kernel_memfs_inodes_ptr: () => runtimeState.memfsPtr + 0x100,
    kernel_memfs_inodes_bytes: () => 16384 * 32,
    kernel_memfs_first_child_ptr: () => runtimeState.memfsPtr + 0x200000,
    kernel_memfs_first_child_bytes: () => 16384 * 4,
    kernel_memfs_dir_entries_ptr: () => runtimeState.memfsPtr + 0x220000,
    kernel_memfs_dir_entries_bytes: () => 16384 * 264,
    kernel_memfs_data_ptr: () => runtimeState.memfsPtr + 0x900000,
    kernel_memfs_data_bytes: () => 256 * 1024,
    kernel_mem_state_ptr: () => runtimeState.memStatePtr,
    kernel_epoll_table_ptr: () => runtimeState.epollTablePtr,
    kernel_epoll_table_bytes: () => 64 * 4,
    kernel_epoll_instance_bytes: () => 64 * 16 + 4,
    kernel_socket_table_ptr: () => runtimeState.socketTablePtr,
    kernel_socket_table_bytes: () => 64 * 128,
    kernel_socket_used: () => runtimeState.socketUsed,
    kernel_pipe_base: () => runtimeState.pipeBase,
    kernel_pipe_region_bytes: () => 64 * MOCK_PIPE_SLOT_BYTES,
    kernel_pipe_used: () => runtimeState.pipeUsed,
    kernel_net_buf_base: () => runtimeState.netBufBase,
    kernel_net_buf_pool_bytes: () => 128 * (16 + 8192),
    kernel_net_buf_used: () => runtimeState.netBufUsed,
    kernel_sig_slots_ptr: () => runtimeState.sigSlotsPtr,
    kernel_sig_slots_bytes: () => 64 * 4,
    kernel_signal_action_table_bytes: () => 64 * 24,
    kernel_process_pending_ptr: () => runtimeState.processPendingPtr,
    kernel_process_pending_bytes: () => 64 * 8,
    kernel_process_runtime_state_ptr: () => runtimeState.processRuntimeStatePtr,
    kernel_process_runtime_state_bytes: () => 4096,
    kernel_time_state_ptr: () => runtimeState.timeStatePtr,
    kernel_time_state_bytes: () => 4096,
    kernel_reset_memfs: () => {},
    kernel_attach_runtime_state: (
      heapBase, heapPtr, ofdTablePtr, memfsPtr, memStatePtr,
      epollTablePtr, socketTablePtr, socketUsed, pipeBase, pipeUsed,
      netBufBase, netBufUsed, sigSlotsPtr, processPendingPtr,
      processRuntimeStatePtr, timeStatePtr,
    ) => {
      runtimeState = {
        heapBase, heapPtr, ofdTablePtr, memfsPtr, memStatePtr,
        epollTablePtr, socketTablePtr, socketUsed, pipeBase, pipeUsed,
        netBufBase, netBufUsed, sigSlotsPtr, processPendingPtr,
        processRuntimeStatePtr, timeStatePtr,
      };
    },
    process_push_arg: () => {},
    process_push_env: () => {},
    process_clear_args: () => {},
    process_guest_base: () => 0x1000000,
    process_guest_base_for_address_profile: () => 0x1000000n,
    process_inspect_elf_brk: () => 0x40_0000n,
    process_inspect_elf_brk_with_memory_policy: () => 0x40_0000n,
    process_create: () => {
      initPipeSlot(1);
      initPipeSlot(2);
      initPipeSlot(3);
      setIdentityForVm(0x1234, currentPid, currentPpid, currentPgid, currentSid, currentDidExec);
      return 0x1234;
    },
    process_create_with_memory_policy: () => {
      initPipeSlot(1);
      initPipeSlot(2);
      initPipeSlot(3);
      setIdentityForVm(0x1234, currentPid, currentPpid, currentPgid, currentSid, currentDidExec);
      return 0x1234;
    },
    thread_step: () => 0,
    thread_status: () => 0,
    thread_tid: (vmPtr) => {
      const slot = liveThreadVmPtrs.indexOf(vmPtr);
      return slot < 0 ? 0 : slot + 1;
    },
    thread_exit_code: () => 0,
    thread_term_signal: () => 0,
    thread_pc: () => 0n,
    thread_ra: () => 0n,
    thread_sp: () => 0n,
    thread_s11: () => 0n,
    thread_tls_base: () => 0n,
    thread_a0: () => 0n,
    thread_a1: () => 0n,
    thread_reg: () => 0n,
    thread_gp: () => 0n,
    thread_tp: () => 0n,
    thread_sig_mask: () => 0n,
    thread_sig_pending: () => 0n,
    thread_sig_info_code: () => 0,
    thread_pending_fs_effects: () => 0,
    thread_pending_fs_target_ino: () => 0,
    thread_pending_fs_opcode: () => 0,
    thread_process_fs: () => {
      fsProcessCalls += 1;
      runtimeState = { ...runtimeState, heapPtr: runtimeState.heapPtr + 0x10 };
    },
    thread_resume_fs: () => 0,
    thread_futex_wait_addr: () => 0n,
    thread_futex_wait_timeout: () => 0n,
    thread_futex_wait_expected: () => 0,
    thread_sleep_seconds: () => 0n,
    thread_sleep_nanoseconds: () => 0n,
    thread_record_blocked_addr: () => 0,
    thread_resume_wait4: () => 0n,
    thread_epoll_wait_timeout: () => -1n,
    thread_epoll_timerfd_deadline_ticks: () => 0n,
    thread_poll_wait_timeout: () => -1n,
    thread_poll_wait_ready_count: () => 0,
    thread_poll_wait_has_socket: () => 0,
    thread_resume_epoll_wait_timeout: () => {},
    thread_resume_blocked_syscall: () => 0n,
    thread_resume_connect_result: () => {},
    thread_vfork_child_pid: () => 0,
    thread_socket_wait_timeout: () => -1n,
    thread_resume_socket_wait_timeout: () => {},
    thread_blocked_fd: () => 0,
    thread_blocked_ofd_id: () => 0,
    thread_clone_flags: () => 0n,
    thread_spawn_kind: () => 0,
    thread_spawn_child_pid: () => 0,
    thread_spawn_path: () => 0n,
    thread_spawn_argv: () => 0n,
    thread_spawn_envp: () => 0n,
    thread_spawn_guest_memory_policy_limit: () => 0,
    thread_set_execve_memory_handoff: (vmPtr) => {
      setIdentityForVm(vmPtr, currentPid, currentPpid, currentPgid, currentSid, 1);
    },
    thread_set_guest_memory_policy_limit: () => {},
    thread_clear_spawn_handoff: () => {},
    thread_resume_vfork_parent: (vmPtr, childPid) => {
      const identity = identityForVm(vmPtr);
      if (identity.pid === childPid) {
        setIdentityForVm(vmPtr, identity.ppid || 1, 0, identity.pgid, identity.sid, 0);
      }
    },
    thread_restore_vfork_parent_without_cleanup: (vmPtr, childPid) => {
      const identity = identityForVm(vmPtr);
      if (identity.pid === childPid) {
        setIdentityForVm(vmPtr, identity.ppid || 1, 0, identity.pgid, identity.sid, 0);
      }
    },
    thread_complete_execve_handoff: () => 0n,
    thread_spawn_from_clone: (parentVmPtr) => {
      const childVmPtr = nextVmPtr++;
      const parentIdentity = identityForVm(parentVmPtr);
      processIdentities.set(childVmPtr, { ...parentIdentity });
      return childVmPtr;
    },
    thread_process_thread_count: () => threadCount,
    thread_process_ptr: () => 0x5000,
    process_state_bytes: () => 0x1000,
    process_thread_tid: (_vmPtr, slot) => (liveThreadVmPtrs[slot] ?? 0) === 0 ? 0 : slot + 1,
    process_thread_state: (_vmPtr, slot) => liveThreadVmPtrs[slot] ? 2 : 0,
    process_thread_vm_ptr: (_vmPtr, slot) => liveThreadVmPtrs[slot] ?? 0,
    process_record_child_exit: (pid, waitStatus) => {
      recordedChildExits.push([pid, waitStatus]);
    },
    process_current_pid: (vmPtr = 0x1234) => identityForVm(vmPtr).pid,
    process_current_ppid: (vmPtr = 0x1234) => identityForVm(vmPtr).ppid,
    process_current_pgid: (vmPtr = 0x1234) => identityForVm(vmPtr).pgid,
    process_current_sid: (vmPtr = 0x1234) => identityForVm(vmPtr).sid,
    process_current_did_exec: (vmPtr = 0x1234) => identityForVm(vmPtr).didExec,
    process_rewrite_child_pid: (vmPtr, oldPid, newPid, _nextPid) => {
      const identity = identityForVm(vmPtr);
      pendingExecveChildPid = newPid;
      if (identity.pid === oldPid) {
        setIdentityForVm(vmPtr, newPid, identity.ppid, identity.pgid, identity.sid, identity.didExec);
      }
    },
    process_reset_lifecycle: (vmPtr, pid, ppid, _nextPid, pgid, sid, didExec) => {
      setIdentityForVm(vmPtr, pid, ppid, pgid, sid, didExec);
    },
    process_upsert_process_entry: (_pid, _ppid, _pgid, _sid, _didExec) => {},
    process_remove_process_entry: () => {},
    process_stdin_pipe: () => 1,
    process_stdout_pipe: () => 2,
    process_stderr_pipe: () => 3,
    process_set_host_stdio: () => {},
    process_cwd_ino: () => 1,
    process_set_cwd_ino: () => {},
    process_fd_ofd_id: () => 0,
    process_fd_flags: () => 0,
    process_set_fd_entry: () => {},
    kernel_retain_ofd: (ofdId) => {
      retainedOfds.push(ofdId);
    },
    kernel_release_ofd: () => {},
    kernel_ofd_fd_type: (ofdId) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32, true),
    kernel_ofd_host_fd: (ofdId) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32 + 4, true),
    kernel_ofd_status_flags: (ofdId) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32 + 8, true),
    kernel_ofd_ino: (ofdId) => new DataView(memory.buffer)
      .getUint32(runtimeState.ofdTablePtr + ofdId * 32 + 24, true),
    kernel_ofd_refcount: () => 0,
    kernel_ofd_offset: () => 0n,
    process_release_host_stdio: () => {},
    process_inject_http: (_vmPtr, port, reqPtr, reqLen) => {
      const req = new TextDecoder().decode(new Uint8Array(memory.buffer, reqPtr, reqLen));
      injectedHttpRequests.push({ port, request: req });
      return 17;
    },
    process_read_http_response: (_vmPtr, _connId, dstPtr, dstLen) => {
      const chunk = httpResponseChunks.shift();
      if (!chunk) {
        return -1;
      }
      const copied = chunk.subarray(0, dstLen);
      new Uint8Array(memory.buffer).set(copied, dstPtr);
      return copied.length;
    },
    process_close_http_connection: (_vmPtr, connId) => {
      closedHttpConnections.push(connId);
    },
    memfs_resolve_path: () => 0,
    memfs_create_file: (parentIno, _namePtr, nameLen) => {
      memfsCreateCalls.push([parentIno, nameLen]);
      return nextIno++;
    },
    memfs_reserve_inode_until: (targetIno) => {
      nextIno = Math.max(nextIno, targetIno);
      return 0n;
    },
    memfs_write_file_at: (ino, offset, _dataPtr, dataLen) => {
      memfsWriteCalls.push([ino, dataLen]);
      return offset + BigInt(dataLen);
    },
    memfs_read_file_to: () => 0n,
    memfs_file_size: () => 0n,
    memfs_truncate_file: (ino, size) => {
      memfsTruncateCalls.push([ino, Number(size)]);
      return 0n;
    },
    memfs_set_file_size_shallow: (ino, size) => {
      memfsSetFileSizeShallowCalls.push([ino, Number(size)]);
      return 0n;
    },
    memfs_inode_mode: () => 0,
    memfs_inode_nlink: () => 0,
    memfs_inode_uid: () => 0,
    memfs_inode_gid: () => 0,
    memfs_ensure_dir: () => nextIno++,
    memfs_set_mode: () => 0n,
    memfs_create_file_with_data: (parentIno, _namePtr, nameLen, _dataPtr, dataLen) => {
      memfsCreateWithDataCalls.push([parentIno, nameLen + dataLen]);
      return nextIno++;
    },
    memfs_readdir_names: () => 0n,
    supervisor_process_resume: (processVmPtr, outPtr, outLen) => {
      if (outLen < 48 || processVmPtr !== 0x1234) {
        return 0;
      }
      const selectedIndex = supervisorRunnableVmPtrs.findIndex((vmPtr) =>
        vmPtr !== 0
        && liveThreadVmPtrs.includes(vmPtr)
        && exports.thread_status(vmPtr) === exports.abi_status_ok()
        && exports.thread_process_ptr(vmPtr) !== 0);
      if (selectedIndex < 0) {
        return 0;
      }
      const [selectedVmPtr] = supervisorRunnableVmPtrs.splice(selectedIndex, 1);
      return writeSupervisorRunnableResumeRecord(memory, outPtr, {
        vmPtr: selectedVmPtr,
        tid: exports.thread_tid?.(selectedVmPtr) ?? 0,
        processPtr: exports.thread_process_ptr(selectedVmPtr),
        status: exports.thread_status(selectedVmPtr),
      });
    },
    supervisor_enqueue_blocked_wait_timer: (vmPtr) => {
      const timer = selectTestSupervisorBlockedWaitTimer(exports, vmPtr, nextSupervisorTimerSeq);
      if (timer === null) {
        return 0;
      }
      nextSupervisorTimerSeq += 1n;
      pendingSupervisorTimers.push(timer);
      return 1;
    },
    supervisor_blocked_epoll_wait_timeout_ms: (vmPtr) => {
      if (vmPtr === 0) {
        return -14;
      }
      return exports.thread_status(vmPtr) === exports.abi_status_epoll_wait()
        ? Number(exports.thread_epoll_wait_timeout(vmPtr))
        : -1;
    },
    supervisor_blocked_status_is_host_wait: (status) =>
      [
        exports.abi_status_wait4(),
        exports.abi_status_pipe_wait(),
        exports.abi_status_epoll_wait(),
        exports.abi_status_socket_recv(),
        exports.abi_status_socket_send(),
        exports.abi_status_flock_wait(),
        exports.abi_status_connect(),
      ].includes(status) ? 1 : 0,
    supervisor_blocked_status_is_connect_wait: (status) =>
      status === exports.abi_status_connect() ? 1 : 0,
    supervisor_blocked_status_is_epoll_wait: (status) =>
      status === exports.abi_status_epoll_wait() ? 1 : 0,
    supervisor_blocked_status_needs_network_sidecar_restore: (status) =>
      [
        exports.abi_status_connect(),
        exports.abi_status_socket_recv(),
        exports.abi_status_socket_send(),
        exports.abi_status_pipe_wait(),
        exports.abi_status_epoll_wait(),
      ].includes(status) ? 1 : 0,
    supervisor_blocked_status_syncs_socket_after_resume: (status) =>
      [
        exports.abi_status_connect(),
        exports.abi_status_socket_recv(),
        exports.abi_status_socket_send(),
      ].includes(status) ? 1 : 0,
    supervisor_blocked_status_uses_fork_shared_state_sync: (status) =>
      [
        exports.abi_status_pipe_wait(),
        exports.abi_status_socket_recv(),
        exports.abi_status_socket_send(),
        exports.abi_status_flock_wait(),
      ].includes(status) ? 1 : 0,
    supervisor_next_host_op: (outPtr, outLen) => {
      if (outLen < 64) {
        return -1;
      }
      const timer = pendingSupervisorTimers.shift();
      if (timer === undefined) {
        return 0;
      }
      return writeSupervisorArmHostTimerHostOpRecord(memory, outPtr, {
        vmPtr: timer.vmPtr,
        delayMs: timer.delayMs,
        deadlineTicks: timer.deadlineTicks,
        seq: timer.seq,
        tid: exports.thread_tid?.(timer.vmPtr) ?? 0,
        processPtr: exports.thread_process_ptr(timer.vmPtr),
        status: timer.status,
      });
    },
    supervisor_next_host_op_kind: (kind, outPtr, outLen) => {
      if (kind !== exports.supervisor_host_op_kind_arm_host_timer()) {
        return 0;
      }
      return exports.supervisor_next_host_op(outPtr, outLen);
    },
    supervisor_complete_host_op: (completionPtr, completionLen) =>
      completeTestSupervisorBlockedWaitTimer(exports, memory, completionPtr, completionLen),
    recordedChildExits,
    retainedOfds,
    liveThreadVmPtrs,
    get fsProcessCalls() {
      return fsProcessCalls;
    },
    memfsCreateCalls,
    memfsTruncateCalls,
    memfsSetFileSizeShallowCalls,
    memfsWriteCalls,
    memfsCreateWithDataCalls,
    supervisorRunnableVmPtrs,
  };
  createdKernelExports.push(exports);
  return exports;
}

function enableSupervisorRunnableResume(kernel: KernelExports, vmPtr = 0x1234): void {
  (kernel as WaitStateSyncKernelExports).supervisorRunnableVmPtrs.push(vmPtr);
}

function testTimerfdDelayMs(kernel: KernelExports, deadlineTicks: bigint): bigint {
  const currentTicks = BigInt(kernel.monotonic_ticks_current?.() ?? 0n);
  if (deadlineTicks <= currentTicks) {
    return 0n;
  }
  const ticksPerSec = 100_000_000n;
  return ((deadlineTicks - currentTicks) * 1000n + (ticksPerSec - 1n)) / ticksPerSec;
}

function selectTestSupervisorBlockedWaitTimer(
  kernel: KernelExports,
  vmPtr: number,
  seq: bigint,
): TestSupervisorTimerHostOp | null {
  const status = kernel.thread_status(vmPtr);
  if (status === kernel.abi_status_epoll_wait()) {
    const timeoutMs = BigInt(kernel.thread_epoll_wait_timeout(vmPtr));
    if (timeoutMs >= 0n) {
      return { vmPtr, status, delayMs: timeoutMs, deadlineTicks: 0n, seq };
    }
    const deadlineTicks = BigInt(kernel.thread_epoll_timerfd_deadline_ticks?.(vmPtr) ?? 0n);
    return deadlineTicks === 0n
      ? null
      : { vmPtr, status, delayMs: testTimerfdDelayMs(kernel, deadlineTicks), deadlineTicks, seq };
  }
  if (status === kernel.abi_status_pipe_wait()) {
    const timeoutMs = BigInt(kernel.thread_poll_wait_timeout?.(vmPtr) ?? -1n);
    if (timeoutMs < 0n || (kernel.thread_poll_wait_ready_count?.(vmPtr) ?? 0) > 0) {
      return null;
    }
    return { vmPtr, status, delayMs: timeoutMs, deadlineTicks: 0n, seq };
  }
  if (
    status === kernel.abi_status_connect()
    || status === kernel.abi_status_socket_recv()
    || status === kernel.abi_status_socket_send()
  ) {
    const timeoutMs = BigInt(kernel.thread_socket_wait_timeout?.(vmPtr) ?? -1n);
    return timeoutMs < 0n ? null : { vmPtr, status, delayMs: timeoutMs, deadlineTicks: 0n, seq };
  }
  return null;
}

function completeTestSupervisorBlockedWaitTimer(
  kernel: KernelExports,
  memory: WebAssembly.Memory,
  completionPtr: number,
  completionLen: number,
): number {
  if (completionLen < 64) {
    return -1;
  }
  const view = new DataView(memory.buffer, completionPtr, completionLen);
  const vmPtr = view.getUint32(24, true);
  const deadlineTicks = view.getBigUint64(56, true);
  if (deadlineTicks !== 0n) {
    kernel.sync_monotonic_ticks_floor?.(deadlineTicks);
  }
  const status = kernel.thread_status(vmPtr);
  if (status === kernel.abi_status_epoll_wait()) {
    if (Number(kernel.thread_epoll_wait_timeout(vmPtr)) >= 0) {
      kernel.thread_resume_epoll_wait_timeout?.(vmPtr);
    } else {
      kernel.thread_resume_blocked_syscall?.(vmPtr);
    }
  } else if (status === kernel.abi_status_pipe_wait()) {
    kernel.thread_resume_poll_wait_timeout?.(vmPtr);
  } else if (
    status === kernel.abi_status_connect()
    || status === kernel.abi_status_socket_recv()
    || status === kernel.abi_status_socket_send()
  ) {
    kernel.thread_resume_socket_wait_timeout?.(vmPtr);
  }
  return kernel.thread_status(vmPtr);
}

beforeEach(async () => {
  vi.resetModules();
  process.env.TIDEMARK_SCHEDULER_STEP_BUDGET = "20000";
  workerModule = await import("../../../../src/worker.js");
  createdWorkers.length = 0;
  createdKernelExports.length = 0;
  threadCountQueue.length = 0;
  injectedHttpRequests.length = 0;
  httpResponseChunks.length = 0;
  closedHttpConnections.length = 0;
  pendingExecveChildPid = null;
  globalThis.Worker = class extends FakeWorker {
    constructor(url: URL, options: WorkerOptions) {
      super(url, options);
      createdWorkers.push(this);
    }
  } as unknown as typeof Worker;

  const fakeModule = {} as WebAssembly.Module;
  vi.spyOn(WebAssembly, "compile").mockResolvedValue(fakeModule);
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    return {
      exports: makeKernelExports(memory),
    } as WebAssembly.WebAssemblyInstantiatedSource;
  });
});

afterEach(() => {
  workerModule?.destroyWorkerPool();
  clearModuleCache();
  workerModule = null;
  vi.restoreAllMocks();
  globalThis.Worker = originalWorker;
  if (originalSchedulerStepBudget === undefined) {
    delete process.env.TIDEMARK_SCHEDULER_STEP_BUDGET;
  } else {
    process.env.TIDEMARK_SCHEDULER_STEP_BUDGET = originalSchedulerStepBudget;
  }
  vi.resetModules();
});


test("waitInWorker STATUS_EPOLL_WAIT skips epoll sync when the process holds no epoll OFDs", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return {
            type: "epoll-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x40,
            },
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  emitReady(worker);
  worker.clearPostedMessages();
  enableSupervisorRunnableResume(kernel);
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, 0);
  worker.emit({
    type: "status",
    status: 7,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();

  expect(requests.some((msg) => msg.type === "sync-epoll-state")).toBe(false);
  expect(requests.some((msg) => msg.type === "resume-epoll-wait")).toBe(false);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr);
  expect(Atomics.load(notify, 0)).toBe(notifyBefore);
  expect(worker.postedMessages.every((message) => message.type === "step")).toBe(true);
  expect(worker.postedMessages).toEqual(expect.arrayContaining([
    makeStepMessageExpectation(),
  ]));
});

test("waitInWorker STATUS_EPOLL_WAIT republishes synced kernel state and wakes bridge waiters", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return {
            type: "epoll-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x40,
            },
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const notify = new Int32Array(handle.runtimeBridge.notifySab);

  kernel.thread_epoll_wait_timeout = () => -1n;
  enableSupervisorRunnableResume(kernel);
  emitReady(worker);
  worker.clearPostedMessages();
  kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 4 ? 7 : 0);
  kernel.kernel_ofd_fd_type = (ofdId) => (ofdId === 7 ? kernel.abi_fd_type_epoll() : 0);
  const notifyBefore = Atomics.load(notify, 0);
  worker.emit({
    type: "status",
    status: 7,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks(100);

  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x40);
  expect(readPublishedKernelRuntimeState(handle.runtimeBridge).heapPtr).toBe(makeKernelState().heapPtr + 0x40);
  expect(Atomics.load(notify, 0)).toBeGreaterThan(notifyBefore);
  expect(worker.postedMessages).toEqual([]);
});

test("STATUS_PIPE_WAIT syncs fork-shared state to kernel-worker even without peer members", async () => {
  vi.useFakeTimers();

  const requests: KernelWorkerRequest[] = [];
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  kernel.thread_resume_blocked_syscall = () => 0n;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 6,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();

  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(true);
  expect(requests.some((msg) => msg.type === "sync-fork-shared-state")).toBe(false);

  vi.useRealTimers();
});

test("STATUS_PIPE_WAIT with kernel-worker-owned pipe state parks after pipe sync", async () => {
  const requests: KernelWorkerRequest[] = [];
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  let currentStatus = kernel.abi_status_pipe_wait();
  const resumeBlocked = vi.fn(() => 0n);
  kernel.thread_status = () => currentStatus;
  kernel.thread_resume_blocked_syscall = vi.fn((vmPtr) => {
    resumeBlocked(vmPtr);
    currentStatus = kernel.abi_status_ok();
    return 0n;
  });

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 6,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 20));
  await flushMicrotasks(100);

  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(worker.postedMessages).toEqual([]);
  expect(requests.filter((msg) => msg.type === "sync-fork-shared-state")).toHaveLength(0);
  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(true);

});

test("STATUS_PIPE_WAIT parked on kernel-worker-owned pipe state preserves thread register context", async () => {
  const requests: KernelWorkerRequest[] = [];
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  let currentStatus = 6;
  let currentPc = 0x83336n;
  let currentSp = 0x37d6c000n;
  let currentS11 = 0xbfdc80n;
  kernel.thread_status = () => currentStatus;
  kernel.thread_pc = () => currentPc;
  kernel.thread_sp = () => currentSp;
  kernel.thread_s11 = () => currentS11;
  kernel.thread_ra = () => 0x4d9b4n;

  const resumeBlocked = vi.fn(() => {
    currentStatus = 0;
    return 0n;
  });
  kernel.thread_resume_blocked_syscall = resumeBlocked;

  enableSupervisorRunnableResume(kernel);
  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 6,
    exitCode: 0,
    termSignal: 0,
    pc: currentPc,
    ra: 0x4d9b4n,
    sp: currentSp,
    s11: currentS11,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 20));
  await flushMicrotasks(100);

  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(kernel.thread_pc(0x1234)).toBe(0x83336n);
  expect(kernel.thread_sp(0x1234)).toBe(0x37d6c000n);
  expect(kernel.thread_s11(0x1234)).toBe(0xbfdc80n);
  expect(worker.postedMessages).toEqual([]);
  expect(requests.filter((msg) => msg.type === "sync-fork-shared-state")).toHaveLength(0);
  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(true);

});

test("waitInWorker STATUS_PIPE_WAIT syncs pipe state through kernel-worker and wakes bridge waiters", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return {
            type: "pipe-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x34,
            },
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const notify = new Int32Array(handle.runtimeBridge.notifySab);

  enableSupervisorRunnableResume(kernel);
  emitReady(worker);
  worker.clearPostedMessages();
  const notifyBefore = Atomics.load(notify, 0);
  worker.emit({
    type: "status",
    status: kernel.abi_status_pipe_wait(),
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks(100);

  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(true);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x34);
  expect(readPublishedKernelRuntimeState(handle.runtimeBridge).heapPtr).toBe(makeKernelState().heapPtr + 0x34);
  expect(Atomics.load(notify, 0)).toBeGreaterThan(notifyBefore);
  expect(worker.postedMessages).toEqual([]);
});

test("pipe sync wakes waitInWorker epoll waits through runtime bridge", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_epoll_wait_timeout = () => -1n;

  emitReady(worker);
  worker.clearPostedMessages();
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, 0);
  worker.emit({
    type: "status",
    status: 7,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  writeProcessStdin(handle, new TextEncoder().encode("x"));
  await flushMicrotasks();

  expect(requests.some((msg) => msg.type === "sync-epoll-state")).toBe(false);
  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(true);
  expect(requests.some((msg) => msg.type === "resume-epoll-wait")).toBe(false);
  expect(Atomics.load(notify, 0)).toBeGreaterThan(notifyBefore);
  expect(worker.postedMessages).toEqual([]);
});

test("pipe sync with held epoll OFDs refreshes epoll state and wakes waitInWorker bridge waiters", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return {
            type: "epoll-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x24,
            },
          };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 4 ? 7 : 0);
  kernel.kernel_ofd_fd_type = (ofdId) => (ofdId === 7 ? kernel.abi_fd_type_epoll() : 0);

  emitReady(worker);
  worker.clearPostedMessages();
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, 0);
  worker.emit({
    type: "status",
    status: 7,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  writeProcessStdin(handle, new TextEncoder().encode("x"));
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks();

  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(true);
  expect(requests.filter((msg) => msg.type === "sync-epoll-state")).toHaveLength(2);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x48);
  expect(readPublishedKernelRuntimeState(handle.runtimeBridge).heapPtr).toBe(makeKernelState().heapPtr + 0x48);
  expect(Atomics.load(notify, 0)).toBeGreaterThan(notifyBefore);
});

test("pipe sync refreshes epoll-dependent OFD state before retrying waitInWorker epoll waits", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-ofd-state":
          return {
            type: "ofd-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x10,
            },
          };
        case "sync-epoll-state":
          return {
            type: "epoll-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x20,
            },
          };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.process_fd_ofd_id = (_vmPtr, fd) => {
    if (fd === 3) {
      return 9;
    }
    if (fd === 4) {
      return 7;
    }
    return 0;
  };
  kernel.kernel_ofd_fd_type = (ofdId) => {
    if (ofdId === 7) {
      return kernel.abi_fd_type_epoll();
    }
    if (ofdId === 9) {
      return 9;
    }
    return 0;
  };

  emitReady(worker);
  worker.clearPostedMessages();
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, 0);
  worker.emit({
    type: "status",
    status: 7,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  writeProcessStdin(handle, new TextEncoder().encode("x"));
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks();

  const pipeSyncIndex = requests.findIndex((msg) => msg.type === "sync-pipe-state");
  const ofdSyncIndex = requests.findIndex((msg) => msg.type === "sync-ofd-state");
  const epollSyncIndex = requests.findLastIndex((msg) => msg.type === "sync-epoll-state");
  expect(pipeSyncIndex).toBeGreaterThanOrEqual(0);
  expect(ofdSyncIndex).toBeGreaterThan(pipeSyncIndex);
  expect(ofdSyncIndex).toBeGreaterThanOrEqual(0);
  expect(epollSyncIndex).toBeGreaterThan(ofdSyncIndex);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x50);
  expect(Atomics.load(notify, 0)).toBeGreaterThan(notifyBefore);
});

test("signal syscalls wake waitInWorker threads through the runtime bridge", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];

  emitReady(worker);
  worker.clearPostedMessages();
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, 0);

  worker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
    syncEffects: 1 << 5,
  });

  await flushMicrotasks();

  expect(Atomics.load(notify, 0)).toBeGreaterThan(notifyBefore);
});

test("STATUS_OK syncs epoll state through kernel-worker when available", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const kernel = handle.kernel;
  const ofdId = 7;
  new DataView(kernel.memory.buffer).setInt32(
    handle.kernelState.ofdTablePtr + ofdId * 32,
    kernel.abi_fd_type_epoll(),
    true,
  );
  new DataView(kernel.memory.buffer).setUint32(
    handle.kernelState.ofdTablePtr + ofdId * 32 + 24,
    1,
    true,
  );
  kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 4 ? ofdId : 0);
  const worker = createdWorkers[0];

  enableSupervisorRunnableResume(kernel);
  emitReady(worker);
  worker.clearPostedMessages();
  enableSupervisorRunnableResume(kernel);
  worker.emit({
    type: "status",
    status: 0,
    syncEffects: SYNC_EFFECT_EPOLL_STATE,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-epoll-state")).toBe(true);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr);
  expect(worker.postedMessages).toEqual([
    makeStepMessageExpectation(),
  ]);
});

test("STATUS_OK syncs eventfd-backed OFD state before epoll retries", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-ofd-state":
          return {
            type: "ofd-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x10,
            },
          };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const kernel = handle.kernel;
  const ofdId = 7;
  new DataView(kernel.memory.buffer).setInt32(handle.kernelState.ofdTablePtr + ofdId * 32, 9, true);
  kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 3 ? ofdId : 0);

  const worker = createdWorkers[0];
  enableSupervisorRunnableResume(kernel);
  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 0,
    syncEffects: SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE | SYNC_EFFECT_EPOLL_STATE,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-ofd-state" && msg.ofdIds.includes(ofdId))).toBe(true);
  expect(requests.some((msg) => msg.type === "sync-epoll-state")).toBe(false);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x10);
});

test("STATUS_OK syncs fork-shared OFD state through kernel-worker", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return {
            type: "fork-shared-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x20,
            },
          };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const kernel = handle.kernel;
  const ofdId = 5;
  new DataView(kernel.memory.buffer).setInt32(handle.kernelState.ofdTablePtr + ofdId * 32, 6, true);
  kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 3 ? ofdId : 0);
  handle.kernelShare.sharedOfdIds.add(ofdId);

  const worker = createdWorkers[0];
  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 0,
    syncEffects: SYNC_EFFECT_FORK_SHARED_OFD_STATE,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-fork-shared-state" && msg.ofdIds.includes(ofdId))).toBe(true);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x20);
});

test("STATUS_OK syncs socket state through kernel-worker when available", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return {
            type: "socket-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x18,
            },
          };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = handle.kernel;

  enableSupervisorRunnableResume(kernel);
  emitReady(worker);
  worker.clearPostedMessages();
  enableSupervisorRunnableResume(kernel);
  worker.emit({
    type: "status",
    status: 0,
    syncEffects: SYNC_EFFECT_SOCKET_STATE | SYNC_EFFECT_EPOLL_STATE,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-socket-state")).toBe(true);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x18);
  expect(worker.postedMessages).toEqual([
    makeStepMessageExpectation(makePostedKernelStateExpectation({
      heapPtr: makeKernelState().heapPtr + 0x18,
    })),
  ]);
});

test("STATUS_OK skips owner-side sync for thread-local returns", async () => {
  const requests: KernelWorkerRequest[] = [];
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  enableSupervisorRunnableResume(kernel);
  emitReady(worker);
  worker.clearPostedMessages();
  enableSupervisorRunnableResume(kernel);
  worker.emit({
    type: "status",
    status: 0,
    syncEffects: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(false);
  expect(requests.some((msg) => msg.type === "sync-process-lifecycle")).toBe(false);
  expect(requests.some((msg) => msg.type === "sync-socket-state")).toBe(false);
  expect(requests.some((msg) => msg.type === "sync-epoll-state")).toBe(false);
  expect(requests.some((msg) => msg.type === "sync-ofd-state")).toBe(false);
  expect(worker.postedMessages).toEqual([
    makeStepMessageExpectation(),
  ]);
});

test("waitInWorker status skips owner-side blocked retry scheduling", async () => {
  vi.useFakeTimers();

  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeBlocked = vi.fn(() => 0n);

  kernel.thread_resume_blocked_syscall = resumeBlocked;

  enableSupervisorRunnableResume(kernel);
  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 6,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  vi.advanceTimersByTime(20);

  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(worker.postedMessages).toEqual([]);

  vi.useRealTimers();
});

test("clone-spawn fixture does not fabricate a sibling worker for EPOLL_WAIT retry", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-ofd-state":
          return {
            type: "ofd-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x10,
            },
          };
        case "sync-epoll-state":
          return {
            type: "epoll-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x20,
            },
          };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const parentWorker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  let parentStatus = kernel.abi_status_epoll_wait();
  const resumeBlocked = vi.fn((vmPtr: number) => {
    if (vmPtr === 0x1234) {
      parentStatus = kernel.abi_status_ok();
    }
    return 0n;
  });

  kernel.thread_process_thread_count = () => 2;
  kernel.thread_status = (vmPtr) => vmPtr === 0x1234 ? parentStatus : kernel.abi_status_ok();
  kernel.thread_resume_blocked_syscall = resumeBlocked;
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.liveThreadVmPtrs.splice(0, kernel.liveThreadVmPtrs.length, 0x1234, 0x2000);
  kernel.process_fd_ofd_id = (_vmPtr, fd) => {
    if (fd === 3) {
      return 9;
    }
    if (fd === 4) {
      return 7;
    }
    return 0;
  };
  kernel.kernel_ofd_fd_type = (ofdId) => {
    if (ofdId === 7) {
      return kernel.abi_fd_type_epoll();
    }
    if (ofdId === 9) {
      return 9;
    }
    return 0;
  };

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: kernel.abi_status_clone_spawn(),
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks(100);

  expect(createdWorkers).toHaveLength(1);
  expect(requests.map((msg) => msg.type)).toEqual([
    "register-process",
    "initialize-process-lifecycle",
    "sync-epoll-state",
    "sync-process-lifecycle",
  ]);
  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(handle.workers.has(parentWorker as unknown as Worker)).toBe(true);
});

test("waitInWorker EPOLL_WAIT timeout=0 completes through supervisor timer HostOp", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  let currentStatus = kernel.abi_status_epoll_wait();
  const resumeTimeout = vi.fn(() => {
    currentStatus = kernel.abi_status_ok();
  });

  kernel.thread_epoll_wait_timeout = () => 0n;
  kernel.thread_resume_epoll_wait_timeout = resumeTimeout;
  kernel.thread_status = () => currentStatus;

  emitReady(worker);
  worker.clearPostedMessages();
  enableSupervisorRunnableResume(kernel);
  worker.emit({
    type: "status",
    status: kernel.abi_status_epoll_wait(),
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks(100);

  expect(resumeTimeout).toHaveBeenCalledWith(0x1234);
  expect(worker.postedMessages).toEqual([
    makeStepMessageExpectation(),
  ]);
});

test("waitInWorker STATUS_SOCKET_RECV syncs socket state through kernel-worker when available", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return {
            type: "socket-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x11,
            },
          };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeBlocked = vi.fn(() => 0n);

  kernel.thread_resume_blocked_syscall = resumeBlocked;

  enableSupervisorRunnableResume(kernel);
  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 30,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-socket-state")).toBe(true);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x11);
  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(worker.postedMessages).toEqual([]);
});

test("waitInWorker STATUS_SOCKET_SEND syncs socket state through kernel-worker when available", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return {
            type: "socket-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x12,
            },
          };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeBlocked = vi.fn(() => 0n);

  kernel.thread_resume_blocked_syscall = resumeBlocked;

  enableSupervisorRunnableResume(kernel);
  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 29,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-socket-state")).toBe(true);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x12);
  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(worker.postedMessages).toEqual([]);
});

test("stale clone-spawn status does not regress kernel runtime state", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const parentWorker = createdWorkers[0];

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  const advancedHeapPtr = makeKernelState().heapPtr + 0x10;
  attachKernelStateForTest(handle, {
    ...makeKernelState(),
    heapPtr: advancedHeapPtr,
  });
  expect(handle.kernelState.heapPtr).toBe(advancedHeapPtr);

  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(handle.kernelState.heapPtr).toBe(advancedHeapPtr);
  expect(createdWorkers).toHaveLength(1);
  expect(handle.kernelState.heapPtr).toBe(advancedHeapPtr);
});

test("stale running-worker kernel state does not move heapPtr backwards", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const worker = createdWorkers[0];

  emitReady(worker);
  worker.clearPostedMessages();
  const advancedHeapPtr = makeKernelState().heapPtr + 0x10;
  attachKernelStateForTest(handle, {
    ...makeKernelState(),
    heapPtr: advancedHeapPtr,
  });
  expect(handle.kernelState.heapPtr).toBe(advancedHeapPtr);

  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(handle.kernelState.heapPtr).toBe(advancedHeapPtr);
});
