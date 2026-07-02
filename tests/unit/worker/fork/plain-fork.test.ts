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
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";

type WorkerModule = typeof import("../../../../src/worker.js");

let workerModule: WorkerModule | null = null;

interface MockThreadStatus {
  status: number;
  exitCode: number;
  termSignal: number;
}

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
    if (message.type === "init") {
      const kernelIndex = createdKernelExports.findIndex((kernel) => kernel.memory === message.memory);
      if (kernelIndex >= 0) {
        workerKernelStatusIndexes.set(this, kernelIndex);
      }
    }
    this.postedMessages.push(stripWorkerStateSnapshots(message));
  }

  terminate(): void {
    this.terminated = true;
  }

  emit(message: ThreadWorkerOutbound): void {
    if (message.type === "ready" && nextReadyKernelStatusIndex < createdKernelStatuses.length) {
      workerKernelStatusIndexes.set(this, nextReadyKernelStatusIndex);
      nextReadyKernelStatusIndex += 1;
    }
    if (message.type === "status") {
      const kernelStatus = createdKernelStatuses[
        workerKernelStatusIndexes.get(this) ?? createdWorkers.indexOf(this)
      ];
      if (kernelStatus) {
        kernelStatus.status = message.status;
        kernelStatus.exitCode = message.exitCode;
        kernelStatus.termSignal = message.termSignal;
      }
    }
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
const createdKernelStatuses: MockThreadStatus[] = [];
const workerKernelStatusIndexes = new WeakMap<FakeWorker, number>();
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
    guestMemoryPolicyLimitCalls: Array<[number, bigint]>;
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
let nextReadyKernelStatusIndex = 0;

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
    epollTableBytes: 64 * 4,
    socketTablePtr: 0x260000,
    socketUsed: 0,
    pipeBase: 0x270000,
    pipeRegionBytes: 64 * MOCK_PIPE_SLOT_BYTES,
    pipeUsed: 0n,
    netBufBase: 0x280000,
    netBufPoolBytes: 128 * (16 + 8192),
    netBufUsed: 0n,
    sigSlotsPtr: 0xc00000,
    sigSlotsBytes: 64 * 4,
    processPendingPtr: 0xc10000,
    processPendingBytes: 64 * 8,
    processRuntimeStatePtr: 0xc20000,
    processRuntimeStateBytes: 4096,
    timeStatePtr: 0xc30000,
    timeStateBytes: 4096,
  };
}

const SYNC_EFFECT_PIPE_STATE = 1 << 0;
const SYNC_EFFECT_SOCKET_STATE = 1 << 1;
const SYNC_EFFECT_EPOLL_STATE = 1 << 2;
const SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE = 1 << 3;
const SYNC_EFFECT_PROCESS_LIFECYCLE = 1 << 4;
const SYNC_EFFECT_FORK_SHARED_OFD_STATE = 1 << 6;
const MOCK_PIPE_SLOT_BYTES = 32 + 65_536;

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
  const threadStatus: MockThreadStatus = { status: 0, exitCode: 0, termSignal: 0 };
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
  const guestMemoryPolicyLimitCalls: Array<[number, bigint]> = [];
  let nextIno = 10;
  interface MockProcessIdentity {
    pid: number;
    ppid: number;
    pgid: number;
    sid: number;
    didExec: number;
  }
  const processIdentities = new Map<number, MockProcessIdentity>();
  let currentPid = pendingExecveChildPid ?? pendingCreatePid ?? 2;
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
  let exportsRef: KernelExports | null = null;
  const exports: KernelExports & {
    recordedChildExits: Array<[number, number]>;
    fsProcessCalls: number;
    memfsCreateCalls: Array<[number, number]>;
    memfsTruncateCalls: Array<[number, number]>;
    memfsSetFileSizeShallowCalls: Array<[number, number]>;
    memfsWriteCalls: Array<[number, number]>;
    memfsCreateWithDataCalls: Array<[number, number]>;
    guestMemoryPolicyLimitCalls: Array<[number, bigint]>;
  } = {
    ...makeSupervisorAbiStubs({
      memory,
      abi_status_connect: () => 28,
      abi_status_clone_spawn: () => 10,
      abi_status_epoll_wait: () => 7,
      abi_status_pipe_wait: () => 6,
      abi_status_socket_recv: () => 30,
      abi_status_socket_send: () => 29,
      abi_status_wait4: () => 31,
      cloneSpawnParentVmPtr: () => 0x1234,
      currentExports: () => exportsRef ?? undefined,
      thread_exit_code: () => threadStatus.exitCode,
      thread_process_ptr: () => 0x5000,
      thread_status: () => threadStatus.status,
      thread_term_signal: () => threadStatus.termSignal,
    }),
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
    abi_guest_address_profile_target: () => 2,
    process_guest_base_for_address_profile: () => 0x1000000n,
    process_guest_address_limit_for_profile: () => 0xffff_ffffn,
    process_guest_base: () => 0x1000000,
    process_inspect_elf_brk_with_memory_policy: () => 0x40_0000n,
    process_inspect_elf_brk: () => 0x40_0000n,
    process_create_with_memory_policy: () => {
      initPipeSlot(1);
      initPipeSlot(2);
      initPipeSlot(3);
      setIdentityForVm(0x1234, currentPid, currentPpid, currentPgid, currentSid, currentDidExec);
      return 0x1234;
    },
    process_create: () => {
      initPipeSlot(1);
      initPipeSlot(2);
      initPipeSlot(3);
      setIdentityForVm(0x1234, currentPid, currentPpid, currentPgid, currentSid, currentDidExec);
      return 0x1234;
    },
    thread_step: () => 0,
    thread_status: () => threadStatus.status,
    thread_exit_code: () => threadStatus.exitCode,
    thread_term_signal: () => threadStatus.termSignal,
    thread_pc: () => 0n,
    thread_ra: () => 0n,
    thread_sp: () => 0n,
    thread_s11: () => 0n,
    thread_tls_base: () => 0n,
    thread_a0: () => 0n,
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
    thread_epoll_wait_timeout: () => 0n,
    thread_resume_epoll_wait_timeout: () => {},
    thread_resume_blocked_syscall: () => 0n,
    thread_resume_connect_result: () => {},
    thread_vfork_child_pid: () => 0,
    thread_socket_wait_timeout: () => -1n,
    thread_blocked_fd: () => 0,
    thread_blocked_ofd_id: () => 0,
    thread_clone_flags: () => 0n,
    thread_spawn_kind: () => 0,
    thread_spawn_child_pid: () => 0,
    thread_spawn_path: () => 0n,
    thread_spawn_argv: () => 0n,
    thread_spawn_envp: () => 0n,
    thread_spawn_process_max_memory_pages: () => 0,
    thread_spawn_guest_memory_policy_limit: () => 0,
    thread_set_execve_memory_handoff: (vmPtr) => {
      setIdentityForVm(vmPtr, currentPid, currentPpid, currentPgid, currentSid, 1);
    },
    thread_guest_address_limit: () => 0xffff_ffffn,
    thread_guest_memory_policy_limit: () => 0n,
    thread_set_guest_memory_policy_limit: (vmPtr, limit) => {
      guestMemoryPolicyLimitCalls.push([vmPtr, limit]);
    },
    thread_process_max_memory_pages: () => 0,
    thread_clear_spawn_handoff: () => {},
    thread_resume_vfork_parent: (vmPtr, childPid) => {
      threadStatus.status = 0;
      const identity = identityForVm(vmPtr);
      if (identity.pid === childPid) {
        setIdentityForVm(vmPtr, identity.ppid || 1, 0, identity.pgid, identity.sid, 0);
      }
    },
    thread_restore_vfork_parent_without_cleanup: (vmPtr, childPid) => {
      threadStatus.status = 0;
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
    process_child_ppid: () => currentPpid,
    process_child_pgid: () => currentPgid,
    process_child_sid: () => currentSid,
    process_next_pid: () => Math.max(3, currentPid + 1),
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
    guestMemoryPolicyLimitCalls,
  };
  exportsRef = exports;
  createdKernelStatuses.push(threadStatus);
  createdKernelExports.push(exports);
  return exports;
}

beforeEach(async () => {
  vi.resetModules();
  process.env.TIDEMARK_SCHEDULER_STEP_BUDGET = "20000";
  workerModule = await import("../../../../src/worker.js");
  createdWorkers.length = 0;
  createdKernelStatuses.length = 0;
  createdKernelExports.length = 0;
  nextReadyKernelStatusIndex = 0;
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


test("plain fork retains shared OFDs before spawning the child worker", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_spawn_kind = () => kernel.abi_spawn_kind_fork();
  kernel.thread_spawn_child_pid = () => 77;
  kernel.debug_memory_brk_current = () => 0x4000n;
  kernel.debug_memory_mmap_next = () => 0x1f0000n;
  kernel.debug_memory_vma_len = () => 2;
  kernel.debug_memory_vma_start = (index: number) => (index === 0 ? 0x5000n : 0x7000n);
  kernel.debug_memory_vma_end = (index: number) => (index === 0 ? 0x6000n : 0x8000n);
  kernel.process_fd_ofd_id = (_vmPtr, fd) => {
    switch (fd) {
      case 0:
        return 11;
      case 1:
        return 12;
      case 2:
        return 13;
      case 3:
        return 12;
      default:
        return 0;
    }
  };

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect([...new Set(kernel.retainedOfds)].sort((a, b) => a - b)).toEqual([11, 12, 13]);
});

test("plain fork preserves optional zero guest memory policy from child kernel state", async () => {
  const parent = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    guestMemoryPolicyLimitBytes: 64 * 1024 * 1024,
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_spawn_kind = () => kernel.abi_spawn_kind_fork();
  kernel.thread_spawn_child_pid = () => 77;
  kernel.debug_memory_brk_current = () => 0x4000n;
  kernel.debug_memory_mmap_next = () => 0x1f0000n;
  kernel.debug_memory_vma_len = () => 0;
  kernel.thread_guest_memory_policy_limit = () => 0n;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  const childKernel = createdKernelExports[1];
  expect(parent.guestMemoryPolicyLimitBytes).toBeGreaterThan(0n);
  expect(childKernel?.guestMemoryPolicyLimitCalls.at(-1)).toEqual([0x1234, 0n]);
});

test("plain fork canonicalizes child pid and reinitializes child lifecycle through kernel-worker", async () => {
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
        case "sync-process-memfs":
          return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "canonicalize-child-pid":
          return {
            type: "child-pid-canonicalized",
            requestId: 0,
            childPid: 177,
            ppid: 1,
            nextPid: 178,
            pgid: 1,
            sid: 1,
            kernelState: msg.kernelState,
          };
        case "sync-fork-shared-state":
          return {
            type: "fork-shared-state-synced",
            requestId: 0,
            kernelState: msg.kernelState,
          };
        case "rehydrate-fork-shared-state":
          return {
            type: "fork-shared-state-rehydrated",
            requestId: 0,
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

  kernel.thread_spawn_kind = () => kernel.abi_spawn_kind_fork();
  kernel.thread_spawn_child_pid = () => 77;

  emitReady(worker);
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  for (let i = 0; i < 10; i++) {
    await Promise.resolve();
  }
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(
    requests.some((msg) => msg.type === "canonicalize-child-pid" && msg.provisionalChildPid === 77),
  ).toBe(true);
  expect(
    requests.some((msg) => msg.type === "register-process" && msg.pid === 177),
  ).toBe(true);
  expect(
    requests.some((msg) => msg.type === "sync-fork-shared-state" && msg.pid === 1),
  ).toBe(true);
  expect(
    requests.some((msg) => msg.type === "initialize-process-lifecycle" && msg.pid === 177),
  ).toBe(true);
});

test("plain fork spawn path records child exit and resumes parent replacement worker", async () => {
  const requests: KernelWorkerRequest[] = [];
  threadCountQueue.push(1, 0);
  const parent = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-memfs":
          return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "canonicalize-child-pid":
          return {
            type: "child-pid-canonicalized",
            requestId: 0,
            childPid: 177,
            ppid: 1,
            nextPid: 178,
            pgid: 1,
            sid: 1,
            kernelState: msg.kernelState,
          };
        case "record-child-exit":
          return {
            type: "child-exit-recorded",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x80,
            },
          };
        case "resume-wait4":
          return {
            type: "wait4-resumed",
            requestId: 0,
            kernelState: msg.kernelState,
          };
        case "sync-fork-shared-state":
          return {
            type: "fork-shared-state-synced",
            requestId: 0,
            kernelState: msg.kernelState,
          };
        case "rehydrate-fork-shared-state":
          return {
            type: "fork-shared-state-rehydrated",
            requestId: 0,
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  let parentStatus = parentKernel.abi_status_wait4();
  const resumeWait4 = vi.fn(() => 0n);

  parentKernel.thread_resume_wait4 = resumeWait4;
  parentKernel.thread_status = () => parentStatus;
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
  parentKernel.thread_spawn_child_pid = () => 77;
  parentKernel.debug_memory_brk_current = () => 0x4000n;
  parentKernel.debug_memory_mmap_next = () => 0x1f0000n;
  parentKernel.debug_memory_vma_len = () => 2;
  parentKernel.debug_memory_vma_start = (index: number) => (index === 0 ? 0x5000n : 0x7000n);
  parentKernel.debug_memory_vma_end = (index: number) => (index === 0 ? 0x6000n : 0x8000n);

  emitReady(parentWorker);
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks(100);

  const childWorker = parentWorker;
  parentStatus = parentKernel.abi_status_fault();
  emitReady(childWorker);
  childWorker.emit({
    type: "status",
    status: 3,
    exitCode: 7,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 20));
  await flushMicrotasks(100);

  expect(createdWorkers).toHaveLength(2);
  const parentReplacementWorker = createdWorkers[1];
  const parentReplacementInit = parentReplacementWorker.postedMessages.find((message) => message.type === "init");
  expect(parentReplacementInit?.type).toBe("init");
  expect(
    parentReplacementInit?.type === "init"
      ? parentReplacementInit.preserveMemoryRanges
        ?.filter((range) => range.start >= 0x1000000)
        .reduce((max, range) => Math.max(max, range.end), 0)
      : 0,
  ).toBeLessThanOrEqual(0x1008000);
  expect(
    requests.some((msg) => msg.type === "canonicalize-child-pid" && msg.provisionalChildPid === 77),
  ).toBe(true);
  expect(
    requests.some((msg) => msg.type === "record-child-exit" && msg.pid === 1 && msg.childPid === 177),
  ).toBe(true);
  expect(parentKernel.recordedChildExits).toEqual([[177, 7 << 8]]);
  expect(parentKernel.kernel_heap_ptr()).toBe(makeKernelState().heapPtr + 0x80);

  parentReplacementWorker.clearPostedMessages();
  parentStatus = parentKernel.abi_status_wait4();
  parentReplacementWorker.emit({
    type: "status",
    status: 31,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: parent.kernelState,
  });
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 20));
  await flushMicrotasks();

  expect(resumeWait4).not.toHaveBeenCalled();
  expect(parentKernel.kernel_heap_ptr()).toBe(makeKernelState().heapPtr + 0x80);
  expect(requests.some((msg) => msg.type === "resume-wait4" && msg.pid === 1)).toBe(true);
  expect(parentReplacementWorker.postedMessages).toEqual([]);
});

test("plain fork rehydrates shared kernel objects after child memory scrub", async () => {
  const restoreDefaultInstantiate = (): void => {
    vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
      const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
      return {
        exports: makeKernelExports(memory),
      } as WebAssembly.WebAssemblyInstantiatedSource;
    });
  };

  let instantiateCount = 0;
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 2) {
      const state = makeKernelState();
      const epollTableBytes = Number(exports.kernel_epoll_table_bytes());
      const epollInstanceBytes = Number(exports.kernel_epoll_instance_bytes());
      const epollTable = new Uint32Array(memory.buffer, state.epollTablePtr, epollTableBytes >>> 2);
      const epollInstancePtrs = [...epollTable].filter((ptr) => ptr !== 0);
      const bytes = new Uint8Array(memory.buffer);
      bytes.fill(0, state.ofdTablePtr, state.ofdTablePtr + Number(exports.kernel_ofd_table_bytes()));
      bytes.fill(0, state.memfsPtr, state.memfsPtr + Number(exports.kernel_memfs_header_bytes()));
      bytes.fill(0, exports.kernel_memfs_inodes_ptr(), exports.kernel_memfs_inodes_ptr() + Number(exports.kernel_memfs_inodes_bytes()));
      bytes.fill(
        0,
        exports.kernel_memfs_first_child_ptr(),
        exports.kernel_memfs_first_child_ptr() + Number(exports.kernel_memfs_first_child_bytes()),
      );
      bytes.fill(
        0,
        exports.kernel_memfs_dir_entries_ptr(),
        exports.kernel_memfs_dir_entries_ptr() + Number(exports.kernel_memfs_dir_entries_bytes()),
      );
      bytes.fill(
        0,
        exports.kernel_memfs_data_ptr(),
        exports.kernel_memfs_data_ptr() + Number(exports.kernel_memfs_data_bytes()),
      );
      bytes.fill(0, state.pipeBase, state.pipeBase + Number(exports.kernel_pipe_region_bytes()));
      bytes.fill(0, state.socketTablePtr, state.socketTablePtr + Number(exports.kernel_socket_table_bytes()));
      bytes.fill(0, state.netBufBase, state.netBufBase + Number(exports.kernel_net_buf_pool_bytes()));
      bytes.fill(0, state.epollTablePtr, state.epollTablePtr + epollTableBytes);
      for (const ptr of epollInstancePtrs) {
        bytes.fill(0, ptr, ptr + epollInstanceBytes);
      }
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    const state = makeKernelState();
    const ofdId = 1;
    const ofdOffset = state.ofdTablePtr + ofdId * 32;
    const parentBytes = new Uint8Array(parentKernel.memory.buffer);

    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => fd === 3 ? ofdId : 0;
    parentBytes[ofdOffset] = 0x01;
    parentBytes[ofdOffset + 12] = 0x21;
    parentBytes[state.memfsPtr + 4] = 0x31;
    parentBytes[parentKernel.kernel_memfs_inodes_ptr() + 8] = 0x32;
    parentBytes[parentKernel.kernel_memfs_data_ptr() + 16] = 0x33;
    parentBytes[state.pipeBase + 64] = 0x41;
    parentBytes[state.socketTablePtr + 16] = 0x51;
    parentBytes[state.netBufBase + 32] = 0x61;
    new DataView(parentKernel.memory.buffer).setUint32(state.epollTablePtr, 0x250100, true);
    parentBytes[0x250100] = 0x71;

    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
    parentKernel.thread_spawn_child_pid = () => 88;

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 9,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    const childKernel = createdKernelExports[1];
    const childBytes = new Uint8Array(childKernel.memory.buffer);

    expect(childBytes[ofdOffset + 12]).toBe(0x21);
    expect(childBytes[state.memfsPtr + 4]).toBe(0x31);
    expect(childBytes[childKernel.kernel_memfs_inodes_ptr() + 8]).toBe(0x32);
    expect(childBytes[childKernel.kernel_memfs_data_ptr() + 16]).toBe(0x33);
    expect(childBytes[state.pipeBase + 64]).toBe(0x41);
    expect(new DataView(childKernel.memory.buffer).getUint32(state.epollTablePtr, true)).toBe(0x250100);
    expect(childBytes[0x250100]).toBe(0x71);
    expect(childBytes[state.socketTablePtr + 16]).toBe(0x51);
    expect(childBytes[state.netBufBase + 32]).toBe(0x61);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("plain fork does not keep signal disposition state synchronized after the initial copy", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  const signalSlotsPtr = makeKernelState().sigSlotsPtr;
  const processPendingPtr = makeKernelState().processPendingPtr;
  const actionTablePtr = 0xc40000;
  const initialAction = new Uint8Array(24).fill(0x11);
  const initialPending = new Uint8Array(8).fill(0x33);
  const emptyAction = new Uint8Array(initialAction.length);

  new Uint32Array(parentKernel.memory.buffer, signalSlotsPtr, 1)[0] = actionTablePtr;
  new Uint8Array(parentKernel.memory.buffer).set(initialAction, actionTablePtr);
  new Uint8Array(parentKernel.memory.buffer).set(initialPending, processPendingPtr);

  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
  parentKernel.thread_spawn_child_pid = () => 2;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await Promise.resolve();

  const childWorker = parentWorker;
  const childKernel = createdKernelExports[1];
  emitReady(childWorker);

  expect(new Uint32Array(childKernel.memory.buffer, signalSlotsPtr, 1)[0]).toBe(actionTablePtr);
  expect(
    [...new Uint8Array(childKernel.memory.buffer, actionTablePtr, initialAction.length)],
  ).toEqual([...emptyAction]);
  expect(
    [...new Uint8Array(childKernel.memory.buffer, processPendingPtr, initialPending.length)],
  ).toEqual([...initialPending]);

  const updatedAction = new Uint8Array(24).fill(0x44);
  const updatedPending = new Uint8Array(8).fill(0x55);
  new Uint8Array(parentKernel.memory.buffer).set(updatedAction, actionTablePtr);
  new Uint8Array(parentKernel.memory.buffer).set(updatedPending, processPendingPtr);

  childWorker.clearPostedMessages();
  childWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(
    [...new Uint8Array(childKernel.memory.buffer, actionTablePtr, initialAction.length)],
  ).toEqual([...emptyAction]);
  expect(
    [...new Uint8Array(childKernel.memory.buffer, processPendingPtr, initialPending.length)],
  ).toEqual([...initialPending]);
});

test("plain fork does not keep time state synchronized after the initial copy", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  const timeStatePtr = makeKernelState().timeStatePtr;
  const initialTimeState = new Uint8Array(32).fill(0x21);

  new Uint8Array(parentKernel.memory.buffer).set(initialTimeState, timeStatePtr);
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
  parentKernel.thread_spawn_child_pid = () => 2;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await Promise.resolve();

  const childWorker = parentWorker;
  const childKernel = createdKernelExports[1];
  emitReady(childWorker);

  expect(
    [...new Uint8Array(childKernel.memory.buffer, timeStatePtr, initialTimeState.length)],
  ).toEqual([...initialTimeState]);

  const updatedTimeState = new Uint8Array(32).fill(0x66);
  new Uint8Array(parentKernel.memory.buffer).set(updatedTimeState, timeStatePtr);

  childWorker.clearPostedMessages();
  childWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(
    [...new Uint8Array(childKernel.memory.buffer, timeStatePtr, initialTimeState.length)],
  ).toEqual([...initialTimeState]);
});

test("plain fork does not keep process lifecycle state synchronized after the initial copy", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  const processRuntimeStatePtr = makeKernelState().processRuntimeStatePtr;
  const initialLifecycleState = new Uint8Array(32).fill(0x72);

  new Uint8Array(parentKernel.memory.buffer).set(initialLifecycleState, processRuntimeStatePtr);
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
  parentKernel.thread_spawn_child_pid = () => 2;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await Promise.resolve();

  const childWorker = parentWorker;
  const childKernel = createdKernelExports[1];
  emitReady(childWorker);

  expect(
    [...new Uint8Array(childKernel.memory.buffer, processRuntimeStatePtr, initialLifecycleState.length)],
  ).toEqual([...initialLifecycleState]);

  const updatedLifecycleState = new Uint8Array(32).fill(0x93);
  new Uint8Array(parentKernel.memory.buffer).set(updatedLifecycleState, processRuntimeStatePtr);

  childWorker.clearPostedMessages();
  childWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(
    [...new Uint8Array(childKernel.memory.buffer, processRuntimeStatePtr, initialLifecycleState.length)],
  ).toEqual([...initialLifecycleState]);
});

test("plain fork child teardown opens the parent wait4 retry gate", async () => {
  threadCountQueue.push(1, 0);
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  let parentStatus = parentKernel.abi_status_wait4();
  const resumeWait4 = vi.fn(() => {
    parentStatus = parentKernel.abi_status_ok();
    return 0n;
  });

  parentKernel.thread_resume_wait4 = resumeWait4;
  parentKernel.thread_status = () => parentStatus;
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
  parentKernel.thread_spawn_child_pid = () => 2;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(40);
  await new Promise((resolve) => setTimeout(resolve, 0));
  const childWorker = parentWorker;
  emitReady(childWorker);
  childWorker.emit({
    type: "status",
    status: 3,
    exitCode: 7,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 20));
  await flushMicrotasks(100);

  expect(parentKernel.recordedChildExits).toEqual([[2, 7 << 8]]);
  expect(createdWorkers).toHaveLength(2);
  const parentReplacementWorker = createdWorkers[1];
  parentStatus = parentKernel.abi_status_wait4();

  parentReplacementWorker.clearPostedMessages();
  parentReplacementWorker.emit({
    type: "status",
    status: 31,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 20));
  await flushMicrotasks(100);

  expect(resumeWait4).toHaveBeenCalledWith(0x1234);
});

test("plain fork child fault records child exit on the parent state", async () => {
  threadCountQueue.push(1, 0);
  const onError = vi.fn();
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    onError,
  });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
  parentKernel.thread_spawn_child_pid = () => 2;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(400);
  await new Promise((resolve) => setTimeout(resolve, 20));
  await flushMicrotasks(400);

  const childWorker = parentWorker;
  emitReady(childWorker);
  childWorker.emit({
    type: "status",
    status: 3,
    exitCode: 7,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks(100);

  expect(onError).not.toHaveBeenCalled();
  expect(parentKernel.recordedChildExits).toEqual([[2, 7 << 8]]);
  expect(handle.lastDebugMessage).toContain("child-exit:2");
});
