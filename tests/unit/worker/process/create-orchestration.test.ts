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
import { FS_RING_TOTAL_BYTES } from "../../../../src/layout/fs-ring.js";

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
    memfsSetFileExternalCalls: Array<[number, number]>;
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
  const threadCount = threadCountQueue.shift() ?? 1;
  const recordedChildExits: Array<[number, number]> = [];
  const retainedOfds: number[] = [];
  const liveThreadVmPtrs = Array.from({ length: threadCount }, (_, index) => 0x1234 + index * 0x10);
  let fsProcessCalls = 0;
  const memfsCreateCalls: Array<[number, number]> = [];
  const memfsTruncateCalls: Array<[number, number]> = [];
  const memfsSetFileExternalCalls: Array<[number, number]> = [];
  const memfsSetFileSizeShallowCalls: Array<[number, number]> = [];
  const memfsWriteCalls: Array<[number, number]> = [];
  const memfsCreateWithDataCalls: Array<[number, number]> = [];
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
  const exports: KernelExports & {
    recordedChildExits: Array<[number, number]>;
    fsProcessCalls: number;
    memfsCreateCalls: Array<[number, number]>;
    memfsTruncateCalls: Array<[number, number]>;
    memfsSetFileExternalCalls: Array<[number, number]>;
    memfsSetFileSizeShallowCalls: Array<[number, number]>;
    memfsWriteCalls: Array<[number, number]>;
    memfsCreateWithDataCalls: Array<[number, number]>;
  } = {
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
    thread_status: () => 0,
    thread_exit_code: () => 0,
    thread_term_signal: () => 0,
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
    thread_resume_futex_wait: () => {},
    thread_resume_futex_wait_result: () => {},
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
    thread_spawn_guest_memory_policy_limit: () => 0,
    thread_spawn_address_profile: () => 1,
    thread_spawn_guest_memory_policy_limit: () => 0n,
    thread_spawn_process_max_memory_pages: () => 0,
    thread_guest_address_profile: () => 1,
    thread_guest_address_limit: () => 0xffff_ffffn,
    thread_guest_memory_policy_limit: () => 0n,
    thread_set_execve_memory_handoff: (vmPtr) => {
      setIdentityForVm(vmPtr, currentPid, currentPpid, currentPgid, currentSid, 1);
    },
    thread_set_execve_memory_handoff: (vmPtr) => {
      setIdentityForVm(vmPtr, currentPid, currentPpid, currentPgid, currentSid, 1);
    },
    thread_set_guest_memory_policy_limit: () => {},
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
    memfs_set_file_external: (ino, size) => {
      memfsSetFileExternalCalls.push([ino, Number(size)]);
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
    memfsSetFileExternalCalls,
    memfsSetFileSizeShallowCalls,
    memfsWriteCalls,
    memfsCreateWithDataCalls,
  };
  createdKernelExports.push(exports);
  return exports;
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


test("createProcess spawns a child worker on STATUS_CLONE_SPAWN", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];

  expect(handle.vmPtr).toBe(0x1234);
  expect(worker).toBeDefined();

  const refreshedState = makeKernelState();
  emitReady(worker, 0x1234, refreshedState);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(createdWorkers).toHaveLength(2);
  expect(createdWorkers[1].postedMessages).toEqual([
    expect.objectContaining({
      type: "init",
      wasmModule: expect.any(Object),
      memory: handle.memory,
      vmPtr: 0x2000,
      guestBase: 0x1000000,
      forcePrepare: undefined,
      preserveMemoryBytes: undefined,
      kernelStackSlot: expect.any(Number),
      generation: expect.any(Number),
      runtimeEpoch: expect.any(Number),
      guestWriteWatchRegions: [],
      kernelState: makeKernelState(),
      sabPageCache: null,
      runtimeNotify: expect.any(SharedArrayBuffer),
      runtimeState: expect.any(SharedArrayBuffer),
    }),
  ]);
  expect(worker.postedMessages).toEqual([]);
});

test("ready adopts worker kernel state before first step", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];
  const readyState = {
    ...makeKernelState(),
    heapPtr: makeKernelState().heapPtr + 0x88,
    pipeUsed: 0x40n,
    processRuntimeStatePtr: makeKernelState().processRuntimeStatePtr + 0x20,
  };

  worker.clearPostedMessages();
  emitReady(worker, 0x1234, readyState);

  expect(handle.kernelState).toEqual({
    ...readyState,
    processRuntimeStatePtr: makeKernelState().processRuntimeStatePtr,
  });
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...readyState,
        processRuntimeStatePtr: makeKernelState().processRuntimeStatePtr,
      },
    },
  ]);
});

test("clone spawn refreshes kernel state before child init and parent resume", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const refreshedState = {
    ...makeKernelState(),
    heapPtr: makeKernelState().heapPtr + 0x80,
    processRuntimeStatePtr: makeKernelState().processRuntimeStatePtr + 0x40,
  };

  kernel.kernel_attach_runtime_state(
    refreshedState.heapBase,
    refreshedState.heapPtr,
    refreshedState.ofdTablePtr,
    refreshedState.memfsPtr,
    refreshedState.memStatePtr,
    refreshedState.epollTablePtr,
    refreshedState.socketTablePtr,
    refreshedState.socketUsed,
    refreshedState.pipeBase,
    refreshedState.pipeUsed,
    refreshedState.netBufBase,
    refreshedState.netBufUsed,
    refreshedState.sigSlotsPtr,
    refreshedState.processPendingPtr,
    refreshedState.processRuntimeStatePtr,
    refreshedState.timeStatePtr,
  );

  emitReady(worker, 0x1234, refreshedState);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(handle.kernelState).toEqual({
    ...refreshedState,
    processRuntimeStatePtr: makeKernelState().processRuntimeStatePtr,
  });
  expect(createdWorkers).toHaveLength(2);
  expect(createdWorkers[1].postedMessages).toEqual([
    expect.objectContaining({
      type: "init",
      wasmModule: expect.any(Object),
      memory: handle.memory,
      vmPtr: 0x2000,
      guestBase: 0x1000000,
      forcePrepare: undefined,
      preserveMemoryBytes: undefined,
      kernelStackSlot: expect.any(Number),
      generation: expect.any(Number),
      runtimeEpoch: expect.any(Number),
      guestWriteWatchRegions: [],
      kernelState: {
        ...refreshedState,
        processRuntimeStatePtr: makeKernelState().processRuntimeStatePtr,
      },
      sabPageCache: null,
      runtimeNotify: expect.any(SharedArrayBuffer),
      runtimeState: expect.any(SharedArrayBuffer),
    }),
  ]);
  expect(worker.postedMessages).toEqual([]);
});


test("clone child init caps preserved guest bytes to guest policy limit", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];
  handle.guestMemoryPolicyLimitBytes = 0x2000n;

  emitReady(worker);
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(createdWorkers).toHaveLength(2);
  expect(createdWorkers[1].postedMessages[0]).toMatchObject({
    type: "init",
    guestBase: 0x1000000,
    preserveMemoryBytes: undefined,
  });
  expect(createdWorkers[1].postedMessages[0]).not.toHaveProperty("guestBytes");
});

test("clone child init preserves only live guest bytes when kernel debug memory state is narrower", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];
  handle.guestMemoryPolicyLimitBytes = 0x200000n;
  handle.kernel.debug_memory_brk_current = () => 0x4000n;
  handle.kernel.debug_memory_mmap_next = () => 0x1f0000n;
  handle.kernel.debug_memory_vma_len = () => 2;
  handle.kernel.debug_memory_vma_end = (index: number) => (index === 0 ? 0x6000n : 0x8000n);

  emitReady(worker);
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(createdWorkers).toHaveLength(2);
  expect(createdWorkers[1].postedMessages[0]).toMatchObject({
    type: "init",
    guestBase: 0x1000000,
    preserveMemoryBytes: undefined,
  });
  expect(createdWorkers[1].postedMessages[0]).not.toHaveProperty("guestBytes");
});

test("clone child first ready->step uses refreshed canonical kernel state", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const parentWorker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const staleState = makeKernelState();
  const refreshedState = {
    ...staleState,
    heapPtr: staleState.heapPtr + 0x80,
    processRuntimeStatePtr: staleState.processRuntimeStatePtr + 0x40,
  };

  kernel.kernel_attach_runtime_state(
    refreshedState.heapBase,
    refreshedState.heapPtr,
    refreshedState.ofdTablePtr,
    refreshedState.memfsPtr,
    refreshedState.memStatePtr,
    refreshedState.epollTablePtr,
    refreshedState.socketTablePtr,
    refreshedState.socketUsed,
    refreshedState.pipeBase,
    refreshedState.pipeUsed,
    refreshedState.netBufBase,
    refreshedState.netBufUsed,
    refreshedState.sigSlotsPtr,
    refreshedState.processPendingPtr,
    refreshedState.processRuntimeStatePtr,
    refreshedState.timeStatePtr,
  );

  emitReady(parentWorker, 0x1234, refreshedState);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: staleState,
  });

  const adoptedRefreshedState = {
    ...refreshedState,
    processRuntimeStatePtr: makeKernelState().processRuntimeStatePtr,
  };
  expect(readPublishedKernelRuntimeState(handle.runtimeBridge)).toEqual(adoptedRefreshedState);

  const childWorker = createdWorkers[1];
  childWorker.clearPostedMessages();
  emitReady(childWorker, 0x2000);

  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: refreshedState,
  });

  expect(childWorker.postedMessages).toEqual([
    { type: "step", budget: 20_000, kernelState: adoptedRefreshedState },
  ]);
});

test("clone child ready syncs owned epoll state before scheduling", async () => {
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
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-memfs":
          return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
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
  const parentWorker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const epollOfdId = 7;

  new DataView(kernel.memory.buffer).setInt32(
    handle.kernelState.ofdTablePtr + epollOfdId * 32,
    kernel.abi_fd_type_epoll(),
    true,
  );
  new DataView(kernel.memory.buffer).setUint32(
    handle.kernelState.ofdTablePtr + epollOfdId * 32 + 24,
    1,
    true,
  );
  kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 4 ? epollOfdId : 0);

  const refreshedState = makeKernelState();
  emitReady(parentWorker, 0x1234, refreshedState);
  parentWorker.clearPostedMessages();
  requests.length = 0;
  parentWorker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(createdWorkers).toHaveLength(2);
  const childWorker = createdWorkers[1];
  requests.length = 0;
  childWorker.clearPostedMessages();
  emitReady(childWorker, 0x2000);
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-epoll-state")).toBe(true);
  expect(childWorker.postedMessages).toEqual([]);
});

test("clone child ready ignores stale local pipe writes under kernel-worker-owned pipe state", async () => {
  const requests: KernelWorkerRequest[] = [];
  const stdoutChunks: Uint8Array[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    onStdout: (data) => {
      stdoutChunks.push(new Uint8Array(data));
    },
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
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
  const parentWorker = createdWorkers[0];

  emitReady(parentWorker);
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
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(createdWorkers).toHaveLength(2);
  const childWorker = createdWorkers[1];
  childWorker.clearPostedMessages();
  requests.length = 0;

  const warningBytes = new TextEncoder().encode("[java] cloned child warning\n");
  expect(writePipe(handle.memory, handle.kernelState.pipeBase, handle.stdoutPipe, warningBytes).written).toBe(
    warningBytes.length,
  );

  emitReady(childWorker, 0x2000);
  await flushMicrotasks();

  expect(stdoutChunks).toEqual([]);
  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(false);
  expect(childWorker.postedMessages).toEqual([
    { type: "step", budget: 20_000, kernelState: makeKernelState() },
  ]);
});

test("STATUS_OK with kernel-worker-owned pipe state syncs without local stdout drain", async () => {
  const requests: KernelWorkerRequest[] = [];
  const stdoutChunks: Uint8Array[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    onStdout: (data) => {
      stdoutChunks.push(new Uint8Array(data));
    },
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
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];

  emitReady(worker);
  worker.clearPostedMessages();
  requests.length = 0;

  const bannerBytes = new TextEncoder().encode("openjdk banner\n");
  expect(writePipe(handle.memory, handle.kernelState.pipeBase, handle.stdoutPipe, bannerBytes).written).toBe(
    bannerBytes.length,
  );

  worker.emit({
    type: "status",
    status: 0,
    syncEffects: SYNC_EFFECT_PIPE_STATE,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(stdoutChunks).toEqual([bannerBytes]);
  expect(requests.filter((msg) => msg.type === "sync-pipe-state")).toHaveLength(2);
});

test("running worker prunes stale blocked workers after the live thread table shrinks", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const parentWorker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  emitReady(parentWorker);
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

  const childWorker = createdWorkers[1];
  kernel.liveThreadVmPtrs.splice(0, kernel.liveThreadVmPtrs.length, 0x1234, 0x2000);
  emitReady(childWorker, 0x2000);
  childWorker.clearPostedMessages();
  childWorker.emit({
    type: "status",
    status: 31,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x2000,
    kernelState: makeKernelState(),
  });

  kernel.liveThreadVmPtrs.splice(0, kernel.liveThreadVmPtrs.length, 0x1234);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect((handle as { workers: Set<Worker> }).workers.has(childWorker as unknown as Worker)).toBe(false);
});

test("fault exits the process when the live thread table is empty despite a stale positive thread count", async () => {
  const onExit = vi.fn();
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { onExit });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_process_thread_count = () => 1;
  kernel.liveThreadVmPtrs.splice(0, kernel.liveThreadVmPtrs.length);

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 3,
    exitCode: 139,
    termSignal: 11,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

  expect(onExit).toHaveBeenCalledWith(139);
  expect((handle as { workers: Set<Worker> }).workers.size).toBe(0);
  expect(worker.terminated).toBe(true);
});

test("createProcess processes STATUS_FS and resumes worker with refreshed kernel state", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 4,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x10,
      },
    },
  ]);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x10);
});

test("createProcess handles STATUS_FS through kernel-worker before publishing refreshed kernel state", async () => {
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
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-memfs":
          return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
        case "process-fs":
          return {
            type: "fs-processed",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x10,
            },
            result: 0n,
            memfsMutated: false,
            ofdStateChanged: false,
          };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  expect(requests[0]).toMatchObject({
    type: "register-process",
    pid: 1,
    memory: handle.memory,
    kernelState: makeKernelState(),
  });

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 4,
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

  expect(requests.some((msg) => msg.type === "process-fs")).toBe(true);
  expect(requests.some((msg) => msg.type === "sync-fork-shared-state")).toBe(false);
  expect(kernel.fsProcessCalls).toBe(0);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x10,
      },
    },
  ]);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x10);
});

test("STATUS_FS does not sync fork-shared OFD state when no fork-share peers exist", async () => {
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
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];

  handle.kernel.thread_pending_fs_effects = () => 1 << 1;
  handle.kernelShare.sharedOfdIds.add(7);
  handle.kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 3 ? 7 : 0);
  new DataView(handle.memory.buffer).setInt32(handle.kernelState.ofdTablePtr + 7 * 32, 1, true);

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 4,
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

  expect(requests.some((msg) => msg.type === "sync-fork-shared-state")).toBe(false);
});

test("createProcess resumes STATUS_CONNECT from the process owner", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeConnect = vi.fn();
  kernel.thread_resume_connect_result = resumeConnect;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 28,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(resumeConnect).toHaveBeenCalledWith(0x1234, -111n);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: makeKernelState(),
    },
  ]);
});

test("createProcess resumes STATUS_CONNECT through kernel-worker when it owns process state", async () => {
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
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-memfs":
          return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "resume-connect":
          return {
            type: "connect-resumed",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x20,
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
  const resumeConnect = vi.fn();
  kernel.thread_resume_connect_result = resumeConnect;

  expect(requests[0]).toMatchObject({
    type: "register-process",
    pid: 1,
    memory: handle.memory,
    kernelState: makeKernelState(),
  });

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 28,
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

  expect(requests.some((msg) => msg.type === "resume-connect")).toBe(true);
  expect(resumeConnect).not.toHaveBeenCalled();
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x20);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: makeKernelState(),
    },
  ]);
});

test("createProcess blocks STATUS_CONNECT until socket timeout expires", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  let status = 28;
  const resumeConnect = vi.fn();
  const resumeSocketTimeout = vi.fn(() => {
    status = 0;
  });
  kernel.thread_resume_connect_result = resumeConnect;
  kernel.thread_resume_socket_wait_timeout = resumeSocketTimeout;
  kernel.thread_socket_wait_timeout = () => 0n;
  kernel.thread_status = () => status;

  emitReady(worker);
  await flushMicrotasks();
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 28,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(resumeConnect).not.toHaveBeenCalled();
  expect(handle.blockedWorkers.size).toBe(1);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks();
  expect(resumeSocketTimeout).toHaveBeenCalledWith(0x1234);
});

test("kernel-worker child exit bookkeeping tears down the child and resumes parent wait4", async () => {
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
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
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
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x40,
            },
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected parent kernel-worker request: ${msg.type}`);
      }
    },
  });
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 2,
    parentPid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected child kernel-worker request: ${msg.type}`);
      }
    },
  });

  const parentWorker = createdWorkers[0];
  const childWorker = createdWorkers[1];
  const parentKernel = createdKernelExports[0];
  let parentStatus = parentKernel.abi_status_wait4();
  const resumeWait4 = vi.fn(() => {
    parentStatus = parentKernel.abi_status_ok();
    return 0n;
  });
  parentKernel.thread_resume_wait4 = resumeWait4;
  parentKernel.thread_status = () => parentStatus;

  emitReady(parentWorker);
  emitReady(childWorker);
  childWorker.clearPostedMessages();
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
  await new Promise((resolve) => setTimeout(resolve, 20));
  await flushMicrotasks(100);

  expect(
    requests.some((msg) => msg.type === "record-child-exit" && msg.pid === 1 && msg.childPid === 2),
  ).toBe(true);
  expect(parentKernel.recordedChildExits).toEqual([[2, 7 << 8]]);
  expect(parentKernel.kernel_heap_ptr()).toBe(makeKernelState().heapPtr + 0x80);

  vi.useFakeTimers();
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 31,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: parent.kernelState,
  });

  await flushMicrotasks();
  vi.advanceTimersByTime(15);
  await flushMicrotasks();

  expect(requests.some((msg) => msg.type === "resume-wait4" && msg.pid === 1)).toBe(false);
  expect(resumeWait4).toHaveBeenCalledWith(0x1234);
  expect(parentKernel.kernel_heap_ptr()).toBe(makeKernelState().heapPtr + 0x80);
  expect(parentWorker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x80,
        pipeUsed: 15n,
      },
      childExitRecords: [{ childPid: 2, waitStatus: 7 << 8, parentPid: 1 }],
    },
  ]);
  vi.useRealTimers();
});

test("createProcess does not grow linear memory to requested guest policy during spawn staging", async () => {
  const requestedGuestPolicyLimitBytes = 512 * 1024 * 1024;
  const handle = await createProcess(
    new Uint8Array([0]),
    new Uint8Array(512 * 1024),
    { guestMemoryPolicyLimitBytes: requestedGuestPolicyLimitBytes },
  );

  expect(handle.guestMemoryPolicyLimitBytes).toBe(BigInt(requestedGuestPolicyLimitBytes));
  expect(handle.memory.buffer.byteLength).toBeLessThan(128 * 1024 * 1024);
});

test("createProcess routes explicit preload file payloads through kernel-worker backing metadata", async () => {
  const preloadBytes = new Uint8Array(8 * 1024 * 1024);
  const kernelWorkerRequest = vi.fn(async (msg: KernelWorkerRequest): Promise<KernelWorkerOutbound> => {
    switch (msg.type) {
      case "apply-file-layer":
        expect(msg.entries).toHaveLength(1);
        expect(msg.entries[0]).toMatchObject({
          kind: "file",
          path: "/workspace/bin/tool",
          mode: 0o755,
        });
        expect(msg.entries[0]?.kind === "file" ? msg.entries[0].data : undefined).toBe(preloadBytes);
        return { type: "file-layer-applied", requestId: 0 };
      case "snapshot-fs":
        expect(msg.includeData).toBe(false);
        return {
          type: "snapshot-fs-result",
          requestId: 0,
          entries: [
            { kind: "dir", path: "/", mode: 0o755 },
            { kind: "dir", path: "/workspace", mode: 0o755 },
            { kind: "dir", path: "/workspace/bin", mode: 0o755 },
            {
              kind: "file",
              path: "/workspace/bin/tool",
              size: preloadBytes.length,
              mode: 0o755,
            },
          ],
        };
      case "register-process":
        return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
      default:
        throw new Error(`unexpected kernel worker request: ${msg.type}`);
    }
  });

  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    guestMemoryPolicyLimitBytes: 64n * 1024n * 1024n,
    maxMemoryPages: 2048,
    forkMaxMemoryPages: 3072,
    sabPageCache: new SharedArrayBuffer(4 * 1024 * 1024),
    fsRpcRingCapacityBytes: FS_RING_TOTAL_BYTES,
    kernelWorkerPrivateMemoryBytes: 128 * 1024 * 1024,
    artifactMaterializationBudgetBytes: 9 * 1024 * 1024,
    preloadEntries: [
      {
        kind: "file",
        path: "/workspace/bin/tool",
        data: preloadBytes,
        mode: 0o755,
      },
    ],
    kernelWorkerRequest,
  });

  expect(createdKernelExports[0].memfsCreateCalls).toHaveLength(1);
  expect(createdKernelExports[0].memfsSetFileExternalCalls).toEqual([[12, preloadBytes.length]]);
  expect(createdKernelExports[0].memfsTruncateCalls).toEqual([]);
  expect(createdKernelExports[0].memfsWriteCalls).toEqual([]);
  expect(createdKernelExports[0].memfsCreateWithDataCalls).toEqual([]);
  expect(kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "apply-file-layer",
  }));
  expect(kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "snapshot-fs",
    includeData: false,
  }));
  expect(handle.memoryOwnership).toEqual({
    guestAddressProfile: 1,
    guestAddressLimitBytes: "4294967295",
    guestMemoryPolicyLimitBytes: 64n * 1024n * 1024n,
    processMaxMemoryBytes: 2048 * 65536,
    forkProcessMaxMemoryBytes: 3072 * 65536,
    pageCacheDataCapacityBytes: 4 * 1024 * 1024,
    publishedMetadataCapacityBytes: 32 + 16384 * 32 + 16384 * 4 + 16384 * 264,
    fsRpcRingCapacityBytes: FS_RING_TOTAL_BYTES,
    kernelWorkerPrivateMemoryBytes: 128 * 1024 * 1024,
    artifactMaterializationBudgetBytes: 9 * 1024 * 1024,
    artifactMaterializedBytes: preloadBytes.length,
  });
});

test("createProcess reports published metadata capacity independently from page-cache data capacity", async () => {
  const smallPageCache = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    sabPageCache: new SharedArrayBuffer(1024 * 1024),
  });
  const largePageCache = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    sabPageCache: new SharedArrayBuffer(8 * 1024 * 1024),
  });

  expect(smallPageCache.memoryOwnership?.pageCacheDataCapacityBytes).toBe(1024 * 1024);
  expect(largePageCache.memoryOwnership?.pageCacheDataCapacityBytes).toBe(8 * 1024 * 1024);
  expect(smallPageCache.memoryOwnership?.publishedMetadataCapacityBytes).toBeGreaterThan(0);
  expect(smallPageCache.memoryOwnership?.publishedMetadataCapacityBytes)
    .toBe(largePageCache.memoryOwnership?.publishedMetadataCapacityBytes);
});

test("createProcess reports FS RPC ring capacity independently from page-cache data and metadata capacity", async () => {
  const smallPageCache = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    sabPageCache: new SharedArrayBuffer(1024 * 1024),
    fsRpcRingCapacityBytes: FS_RING_TOTAL_BYTES,
  });
  const largePageCache = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    sabPageCache: new SharedArrayBuffer(8 * 1024 * 1024),
    fsRpcRingCapacityBytes: FS_RING_TOTAL_BYTES,
  });

  expect(smallPageCache.memoryOwnership?.pageCacheDataCapacityBytes).not
    .toBe(largePageCache.memoryOwnership?.pageCacheDataCapacityBytes);
  expect(smallPageCache.memoryOwnership?.publishedMetadataCapacityBytes)
    .toBe(largePageCache.memoryOwnership?.publishedMetadataCapacityBytes);
  expect(smallPageCache.memoryOwnership?.fsRpcRingCapacityBytes).toBe(FS_RING_TOTAL_BYTES);
  expect(largePageCache.memoryOwnership?.fsRpcRingCapacityBytes).toBe(FS_RING_TOTAL_BYTES);
});

test("createProcess rejects artifact materialization budget separately from guest policy", async () => {
  await expect(createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    guestMemoryPolicyLimitBytes: 64 * 1024 * 1024,
    artifactMaterializationBudgetBytes: 4095,
    preloadEntries: [
      {
        kind: "file",
        path: "/workspace/bin/tool",
        data: new Uint8Array(4096),
        mode: 0o755,
      },
    ],
  })).rejects.toThrow("artifact materialization budget exceeded: stagedBytes=4096 budgetBytes=4095");
  expect(createdKernelExports[0].memfsWriteCalls).toEqual([]);
});

test("createProcess rejects non-ABI FS RPC ring capacity", async () => {
  await expect(createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    fsRpcRingCapacityBytes: FS_RING_TOTAL_BYTES + 1,
  })).rejects.toThrow(`fsRpcRingCapacityBytes is fixed by kernel ABI at ${FS_RING_TOTAL_BYTES} bytes`);
  expect(createdKernelExports).toEqual([]);
});

test("createProcess seeds local memfs from runtime FS state when preloadEntries are omitted", async () => {
  const runtimeId = 77;
  primeRuntimeFsState(runtimeId, [
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/workspace", mode: 0o755 },
    {
      kind: "file",
      path: "/workspace/app.bin",
      data: new Uint8Array(4096),
      mode: 0o644,
    },
  ]);

  try {
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      runtimeId,
    });
  } finally {
    clearRuntimeFsState(runtimeId);
  }

  expect(createdKernelExports[0].memfsCreateCalls).toHaveLength(1);
  expect(createdKernelExports[0].memfsSetFileExternalCalls).toEqual([[11, 4096]]);
  expect(createdKernelExports[0].memfsTruncateCalls).toEqual([]);
  expect(createdKernelExports[0].memfsWriteCalls).toEqual([]);
});

test("createProcess seeds local memfs before requesting canonical metadata hydration", async () => {
  const runtimeId = 78;
  primeRuntimeFsState(runtimeId, [
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/workspace", mode: 0o755 },
    {
      kind: "file",
      path: "/workspace/app.bin",
      data: new Uint8Array(4096),
      mode: 0o644,
    },
  ]);

  const kernelWorkerRequest = vi.fn(async (msg: KernelWorkerRequest): Promise<KernelWorkerOutbound> => {
    switch (msg.type) {
      case "register-process":
        return {
          type: "process-registered",
          requestId: 0,
          kernelState: msg.kernelState,
        };
      case "initialize-process-lifecycle":
        return {
          type: "process-lifecycle-initialized",
          requestId: 0,
          kernelState: msg.kernelState,
        };
      case "snapshot-fs":
        return {
          type: "snapshot-fs-result",
          requestId: 0,
          entries: [
            { kind: "dir", path: "/", mode: 0o755 },
            { kind: "dir", path: "/workspace", mode: 0o755 },
            { kind: "file", path: "/workspace/app.bin", size: 4096, mode: 0o644 },
          ],
        };
      default:
        throw new Error(`unexpected kernel worker request: ${msg.type}`);
    }
  });

  try {
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      runtimeId,
      pid: 1,
      hydrateFsFromKernelWorker: true,
      kernelWorkerRequest,
    });
  } finally {
    clearRuntimeFsState(runtimeId);
  }

  expect(createdKernelExports[0].memfsCreateCalls).toHaveLength(1);
  expect(createdKernelExports[0].memfsSetFileExternalCalls).toEqual([[11, 4096]]);
  expect(createdKernelExports[0].memfsTruncateCalls).toEqual([]);
  expect(createdKernelExports[0].memfsWriteCalls).toEqual([]);
  expect(
    kernelWorkerRequest.mock.calls.some(([msg]) => msg.type === "snapshot-fs"),
  ).toBe(true);
  expect(kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "register-process",
    seedMemfsMode: "metadata",
  }));
});

test("createProcess preloads runtime fs entries as external metadata before process_create under kernel-worker ownership", async () => {
  const runtimeId = 79;
  primeRuntimeFsState(runtimeId, [
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/lib", mode: 0o755 },
    {
      kind: "file",
      path: "/lib/ld-linux-riscv64-lp64d.so.1",
      data: new Uint8Array(4096),
      mode: 0o755,
    },
  ]);

  vi.spyOn(WebAssembly, "instantiate").mockImplementationOnce(async (_module, imports) => {
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    const processCreate = vi.fn(() => {
      expect(exports.memfsCreateCalls).toHaveLength(1);
      expect(exports.memfsSetFileExternalCalls).toEqual([[11, 4096]]);
      expect(exports.memfsTruncateCalls).toEqual([]);
      expect(exports.memfsWriteCalls).toEqual([]);
      return 0x1234;
    });
    exports.process_create = processCreate;
    createdKernelExports.push(exports);
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  const kernelWorkerRequest = vi.fn(async (msg: KernelWorkerRequest): Promise<KernelWorkerOutbound> => {
    switch (msg.type) {
      case "register-process":
        return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
      case "initialize-process-lifecycle":
        return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
      case "snapshot-fs":
        return {
          type: "snapshot-fs-result",
          requestId: 0,
          entries: [
            { kind: "dir", path: "/", mode: 0o755 },
            { kind: "dir", path: "/lib", mode: 0o755 },
            { kind: "file", path: "/lib/ld-linux-riscv64-lp64d.so.1", size: 4096, mode: 0o755 },
          ],
        };
      default:
        throw new Error(`unexpected kernel worker request: ${msg.type}`);
    }
  });

  try {
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      runtimeId,
      pid: 1,
      hydrateFsFromKernelWorker: true,
      kernelWorkerRequest,
    });
  } finally {
    clearRuntimeFsState(runtimeId);
  }

  expect(
    kernelWorkerRequest.mock.calls.some(([msg]) => msg.type === "snapshot-fs"),
  ).toBe(true);
});

test("STATUS_EXECVE_SPAWN with unknown spawn kind does not immediately resume", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];

  expect(worker).toBeDefined();

  emitReady(worker);
  worker.clearPostedMessages();
  // Mock returns spawn_kind=0 (NONE) by default, so handleExecveSpawn
  // hits the unknown-kind path: clear handoff + enqueue parent
  worker.emit({
    type: "status",
    status: 9, // EXECVE_SPAWN
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(worker.postedMessages).toEqual([]);
});
