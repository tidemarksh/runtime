import { afterEach, beforeEach, expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../../src/abi.js";
import { writePipe } from "../../../../src/bridge/pipe.js";
import { readPublishedKernelRuntimeState } from "../../../../src/bridge/runtime-state.js";
import { DATA_REGION_OFFSET, publishInode } from "../../../../src/fs/sab-page-cache.js";
import { FS_OP_EXECVE } from "../../../../src/layout/fs-ring.js";
import type {
  KernelWorkerOutbound,
  KernelWorkerRequest,
  ThreadWorkerInbound,
  ThreadWorkerOutbound,
} from "../../../../src/messages.js";
import {
  makeSupervisorAbiStubs,
  writeSupervisorRunnableResumeRecord,
} from "../support/supervisor-abi.js";

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
    execveMemoryHandoffCalls: Array<{
      vmPtr: number;
      processPtr: number;
      pathAddr: bigint;
      argvAddr: bigint;
      envpAddr: bigint;
      addressProfile: number;
      guestMemoryPolicyLimit: bigint;
      processMaxMemoryPages: number;
    }>;
    guestMemoryPolicyLimitCalls: Array<[number, bigint]>;
  }
> = [];
const threadCountQueue: number[] = [];
const guestAddressProfileQueue: number[] = [];
const guestMemoryPolicyLimitQueue: bigint[] = [];
const processMaxMemoryPagesQueue: number[] = [];
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
  const execveMemoryHandoffCalls: Array<{
    vmPtr: number;
    processPtr: number;
    pathAddr: bigint;
    argvAddr: bigint;
    envpAddr: bigint;
    addressProfile: number;
    guestMemoryPolicyLimit: bigint;
    processMaxMemoryPages: number;
  }> = [];
  const guestMemoryPolicyLimitCalls: Array<[number, bigint]> = [];
  let guestAddressProfile = guestAddressProfileQueue.shift() ?? 1;
  let guestMemoryPolicyLimit = guestMemoryPolicyLimitQueue.shift() ?? 0n;
  let processMaxMemoryPages = processMaxMemoryPagesQueue.shift() ?? 0;
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
    execveMemoryHandoffCalls: Array<{
      vmPtr: number;
      processPtr: number;
      pathAddr: bigint;
      argvAddr: bigint;
      envpAddr: bigint;
      addressProfile: number;
      guestMemoryPolicyLimit: bigint;
      processMaxMemoryPages: number;
    }>;
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
    abi_max_sockets: () => 64,
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
    thread_spawn_address_profile: () => guestAddressProfile,
    thread_spawn_guest_memory_policy_limit: () => guestMemoryPolicyLimit,
    thread_spawn_process_max_memory_pages: () => processMaxMemoryPages,
    thread_guest_address_profile: () => guestAddressProfile,
    thread_guest_memory_policy_limit: () => guestMemoryPolicyLimit,
    thread_process_max_memory_pages: () => processMaxMemoryPages,
    thread_set_execve_memory_handoff: (
      vmPtr,
      processPtr,
      pathAddr,
      argvAddr,
      envpAddr,
      addressProfile,
      handoffGuestMemoryPolicyLimit,
      handoffProcessMaxMemoryPages,
    ) => {
      execveMemoryHandoffCalls.push({
        vmPtr,
        processPtr,
        pathAddr,
        argvAddr,
        envpAddr,
        addressProfile,
        guestMemoryPolicyLimit: handoffGuestMemoryPolicyLimit,
        processMaxMemoryPages: handoffProcessMaxMemoryPages,
      });
      guestAddressProfile = addressProfile;
      guestMemoryPolicyLimit = handoffGuestMemoryPolicyLimit;
      processMaxMemoryPages = handoffProcessMaxMemoryPages;
      setIdentityForVm(vmPtr, currentPid, currentPpid, currentPgid, currentSid, 1);
    },
    thread_set_guest_memory_policy_limit: (vmPtr, limit) => {
      guestMemoryPolicyLimitCalls.push([vmPtr, limit]);
      guestMemoryPolicyLimit = limit;
    },
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
    supervisor_vfork_execve_rollback: (vmPtr, childPid) => {
      const waitStatus = 127 << 8;
      recordedChildExits.push([childPid, waitStatus]);
      threadStatus.status = 0;
      const identity = identityForVm(vmPtr);
      if (identity.pid === childPid) {
        setIdentityForVm(vmPtr, identity.ppid || 1, 0, identity.pgid, identity.sid, 0);
      }
      return waitStatus;
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
    execveMemoryHandoffCalls,
    guestMemoryPolicyLimitCalls,
  };
  exportsRef = exports;
  createdKernelStatuses.push(threadStatus);
  createdKernelExports.push(exports);
  return exports;
}

function installVforkExecveSpawnChildPid(
  kernel: KernelExports,
  childPid = 156,
): void {
  let handoffPublished = false;
  kernel.thread_spawn_child_pid = () => {
    pendingExecveChildPid = childPid;
    if (!handoffPublished) {
      kernel.process_reset_lifecycle(0x1234, childPid, 1, 201, 33, 44, 0);
      handoffPublished = true;
    }
    return childPid;
  };
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
  guestAddressProfileQueue.length = 0;
  guestMemoryPolicyLimitQueue.length = 0;
  processMaxMemoryPagesQueue.length = 0;
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


test("vfork child fault records wait status and resumes parent on the same worker", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeVforkParent = vi.fn(kernel.thread_resume_vfork_parent.bind(kernel));

  kernel.thread_vfork_child_pid = () => 55;
  kernel.thread_resume_vfork_parent = resumeVforkParent;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

  expect(handle.vforkSnapshot?.vmPtr).toBe(0x1234);

  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 3,
    exitCode: 253,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(kernel.recordedChildExits).toEqual([[55, 253 << 8]]);
  expect(resumeVforkParent).toHaveBeenCalledWith(0x1234, 55);
  expect(handle.vforkSnapshot).toBeNull();
  expect(worker.terminated).toBe(false);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        pipeUsed: 1n,
      },
    },
  ]);
});

test("vfork child fault delegates wait status bookkeeping and parent resume to kernel-worker when available", async () => {
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
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "canonicalize-child-pid":
          return {
            type: "child-pid-canonicalized",
            requestId: 0,
            childPid: 56,
            ppid: 1,
            nextPid: 57,
            pgid: 1,
            sid: 1,
            kernelState: msg.kernelState,
          };
        case "resume-vfork-parent":
          return {
            type: "vfork-parent-resumed",
            requestId: 0,
            childPid: 56,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x24,
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
  const readVforkChildPid = vi.fn(() => 56);
  const resumeVforkParent = vi.fn(kernel.thread_resume_vfork_parent.bind(kernel));

  kernel.thread_vfork_child_pid = readVforkChildPid;
  kernel.thread_resume_vfork_parent = resumeVforkParent;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(handle.vforkSnapshot?.vmPtr).toBe(0x1234);

  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 3,
    exitCode: 252,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

  expect(
    requests.some((msg) => msg.type === "resume-vfork-parent" && msg.waitStatus === (252 << 8)),
  ).toBe(true);
  expect(kernel.recordedChildExits).toEqual([[56, 252 << 8]]);
  expect(readVforkChildPid).toHaveBeenCalled();
  expect(resumeVforkParent).toHaveBeenCalledWith(0x1234, 56);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x24);
  expect(handle.vforkSnapshot).toBeNull();
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x24,
        pipeUsed: 1n,
      },
    },
  ]);
});

test("STATUS_VFORK_WAIT syncs pre-vfork fork-shared OFD state before child canonicalization", async () => {
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
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "canonicalize-child-pid":
          return {
            type: "child-pid-canonicalized",
            requestId: 0,
            childPid: 56,
            ppid: 1,
            nextPid: 57,
            pgid: 1,
            sid: 1,
            kernelState: msg.kernelState,
          };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_vfork_child_pid = () => 55;

  emitReady(worker);
  requests.length = 0;
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
    syncEffects: SYNC_EFFECT_FORK_SHARED_OFD_STATE,
  });
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  const syncIndex = requests.findIndex((msg) => msg.type === "sync-fork-shared-state");
  const canonicalizeIndex = requests.findIndex((msg) => msg.type === "canonicalize-child-pid");
  expect(syncIndex).toBeGreaterThanOrEqual(0);
  expect(canonicalizeIndex).toBeGreaterThan(syncIndex);
  const syncRequest = requests.find((msg) => msg.type === "sync-fork-shared-state");
  expect(syncRequest?.type).toBe("sync-fork-shared-state");
  if (syncRequest?.type !== "sync-fork-shared-state") {
    throw new Error("expected sync-fork-shared-state request");
  }
  expect(syncRequest.includeMemfs).toBe(false);
  expect(syncRequest.preserveSourceRefcounts).toBe(true);
});

test("vfork execve resumes the parent and initializes the child lifecycle through kernel-worker", async () => {
  const requests: KernelWorkerRequest[] = [];
  const childRegisteredState = {
    ...makeKernelState(),
    heapPtr: makeKernelState().heapPtr + 0x40,
  };
  const childLifecycleState = {
    ...childRegisteredState,
    processRuntimeStatePtr: childRegisteredState.processRuntimeStatePtr + 0x20,
  };
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return {
            type: "process-registered",
            requestId: 0,
            kernelState: msg.pid === 156 ? childRegisteredState : msg.kernelState,
          };
        case "initialize-process-lifecycle":
          return {
            type: "process-lifecycle-initialized",
            requestId: 0,
            kernelState: msg.pid === 156 ? childLifecycleState : msg.kernelState,
          };
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
        case "canonicalize-child-pid":
          return {
            type: "child-pid-canonicalized",
            requestId: 0,
            childPid: 156,
            ppid: 1,
            nextPid: 201,
            pgid: 33,
            sid: 44,
            kernelState: msg.kernelState,
          };
        case "resume-vfork-execve-parent":
          return {
            type: "vfork-execve-parent-resumed",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x18,
            },
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
  const resumeVforkParent = vi.fn(parentKernel.thread_resume_vfork_parent.bind(parentKernel));
  const restoreVforkParent = parentKernel.thread_restore_vfork_parent_without_cleanup.bind(parentKernel);
  const restoreVforkParentWithoutCleanup = vi.fn((vmPtr: number, childPid: number) => {
    restoreVforkParent(vmPtr, childPid);
    parentKernel.process_reset_lifecycle(vmPtr, 1, 0, 201, 33, 44, 0);
  });
  const clearSpawnHandoff = vi.fn();

  parentKernel.thread_vfork_child_pid = () => 55;
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
  installVforkExecveSpawnChildPid(parentKernel);
  parentKernel.thread_spawn_address_profile = () => 2;
  parentKernel.thread_spawn_guest_memory_policy_limit = () => 0n;
  parentKernel.thread_spawn_process_max_memory_pages = () => 3333;
  parentKernel.thread_resume_vfork_parent = resumeVforkParent;
  parentKernel.thread_restore_vfork_parent_without_cleanup = restoreVforkParentWithoutCleanup;
  parentKernel.thread_clear_spawn_handoff = clearSpawnHandoff;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  parentWorker.clearPostedMessages();
  parentKernel.process_reset_lifecycle(0x1234, 156, 1, 201, 33, 44, 0);
  guestMemoryPolicyLimitQueue.push(0n);
  processMaxMemoryPagesQueue.push(3333);
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
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks(20);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks(20);

  expect(
    requests.some((msg) => msg.type === "resume-vfork-execve-parent" && msg.childPid === 156),
  ).toBe(true);
  expect(
    requests.some(
      (msg) => msg.type === "rehydrate-fork-shared-state"
        && msg.targetPids.length === 1
        && msg.targetPids[0] === 1
        && msg.includeMemfs === false,
    ),
  ).toBe(false);
  expect(
    requests.some((msg) => msg.type === "register-process" && msg.pid === 156),
  ).toBe(true);
  expect(
    requests.some((msg) => msg.type === "sync-epoll-state"),
  ).toBe(false);
  expect(
    requests.some(
      (msg) => msg.type === "initialize-process-lifecycle"
        && msg.pid === 156
        && msg.ppid === 1
        && msg.nextPid === 157
        && msg.pgid === 33
        && msg.sid === 44
        && msg.didExec,
    ),
  ).toBe(true);
  expect(restoreVforkParentWithoutCleanup).toHaveBeenCalledWith(0x1234, 156);
  expect(resumeVforkParent).not.toHaveBeenCalled();
  expect(clearSpawnHandoff).toHaveBeenCalledWith(0x1234);
  expect(handle.guestMemoryPolicyLimitBytes).toBeGreaterThan(0n);
  expect(createdKernelExports[1]?.execveMemoryHandoffCalls).toEqual([
    expect.objectContaining({
      vmPtr: 0x1234,
      addressProfile: 2,
      guestMemoryPolicyLimit: 0n,
      processMaxMemoryPages: 3333,
    }),
  ]);
  expect(createdKernelExports[1]?.guestMemoryPolicyLimitCalls.at(-1)).toEqual([0x1234, 0n]);
  expect(createdWorkers).toHaveLength(2);
  expect(parentWorker.postedMessages).toEqual([
    expect.objectContaining({
      type: "init",
      wasmModule: expect.any(Object),
      memory: expect.any(WebAssembly.Memory),
      vmPtr: 0x1234,
      guestBase: 0x1000000,
      forcePrepare: undefined,
      preserveMemoryBytes: undefined,
      kernelStackSlot: expect.any(Number),
      generation: expect.any(Number),
      runtimeEpoch: expect.any(Number),
      guestWriteWatchRegions: [],
      kernelState: childRegisteredState,
      sabPageCache: null,
      runtimeNotify: expect.any(SharedArrayBuffer),
      runtimeState: expect.any(SharedArrayBuffer),
    }),
  ]);
  expect(createdWorkers[1].postedMessages).toEqual([
    expect.objectContaining({
      type: "init",
      wasmModule: expect.any(Object),
      memory: expect.any(WebAssembly.Memory),
      vmPtr: 0x1234,
      guestBase: 0x1000000,
      forcePrepare: undefined,
      preserveMemoryBytes: undefined,
      kernelStackSlot: expect.any(Number),
      generation: expect.any(Number),
      runtimeEpoch: expect.any(Number),
      guestWriteWatchRegions: [],
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x18,
        pipeUsed: 1n,
      },
      sabPageCache: null,
      runtimeNotify: expect.any(SharedArrayBuffer),
      runtimeState: expect.any(SharedArrayBuffer),
    }),
  ]);
});

test("vfork execve follows supervisor backing retry limits", async () => {
  const chunkSize = 4096;
  const chunkCount = 260;
  const expectedSize = chunkSize * chunkCount;
  const requests: KernelWorkerRequest[] = [];
  let instantiateCount = 0;
  let publishedChunks = 0;
  let currentMiss: { ino: number; offset: number; length: number; expectedSize: number } | null = null;
  let completeCalls = 0;
  let clearMissCalls = 0;
  const sabPageCache = new SharedArrayBuffer(DATA_REGION_OFFSET + expectedSize + chunkCount * 128);
  publishInode(sabPageCache, 32, {
    mode: 0o100755,
    size: expectedSize,
    parentIno: 1,
    name: new TextEncoder().encode("large-exec"),
    nextSibling: 0,
    fileObjectId: 32,
    generation: 1,
  });

  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 2) {
      exports.thread_complete_execve_handoff = () => {
        completeCalls += 1;
        if (publishedChunks >= chunkCount) {
          currentMiss = null;
          return 0n;
        }
        currentMiss = {
          ino: 32,
          offset: publishedChunks * chunkSize,
          length: chunkSize,
          expectedSize,
        };
        return -11n;
      };
      exports.supervisor_fs_pending_backing_fill_ino = () => currentMiss?.ino ?? 0;
      exports.supervisor_fs_pending_backing_fill_offset = () => BigInt(currentMiss?.offset ?? 0);
      exports.supervisor_fs_pending_backing_fill_length = () => BigInt(currentMiss?.length ?? 0);
      exports.supervisor_fs_pending_backing_fill_expected_size = () => BigInt(currentMiss?.expectedSize ?? 0);
      exports.supervisor_fs_pending_backing_fill_opcode = () => currentMiss ? FS_OP_EXECVE : 0;
      exports.supervisor_fs_backing_retry_limit = () => chunkCount + 2;
      exports.supervisor_fs_clear_pending_backing_fill = () => {
        clearMissCalls += 1;
        publishedChunks += 1;
        currentMiss = null;
        return 1;
      };
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    sabPageCache,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "canonicalize-child-pid":
          return {
            type: "child-pid-canonicalized",
            requestId: 0,
            childPid: 156,
            ppid: 1,
            nextPid: 201,
            pgid: 33,
            sid: 44,
            kernelState: msg.kernelState,
          };
        case "resume-vfork-execve-parent":
          return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-memfs":
          return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "rehydrate-fork-shared-state":
          return { type: "fork-shared-state-rehydrated", requestId: 0 };
        case "read-file-range-by-inode":
          return {
            type: "file-range-read",
            requestId: 0,
            data: Uint8Array.from({ length: msg.length }, (_value, index) => (msg.offset + index) & 0xff),
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
  parentKernel.thread_vfork_child_pid = () => 55;
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
  installVforkExecveSpawnChildPid(parentKernel);

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

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
  for (let turn = 0; turn < 20 && clearMissCalls < chunkCount; turn += 1) {
    await flushMicrotasks(50);
    await new Promise((resolve) => setTimeout(resolve, 0));
  }

  const backingReads = requests.filter((msg) => msg.type === "read-file-range-by-inode");
  expect(backingReads).toHaveLength(chunkCount);
  expect(backingReads[0]).toMatchObject({ ino: 32, offset: 0, length: chunkSize, expectedSize });
  expect(backingReads.at(-1)).toMatchObject({
    ino: 32,
    offset: (chunkCount - 1) * chunkSize,
    length: chunkSize,
    expectedSize,
  });
  expect(completeCalls).toBe(chunkCount + 1);
  expect(clearMissCalls).toBe(chunkCount);
  expect(
    requests.some((msg) => msg.type === "initialize-process-lifecycle" && msg.pid === 156),
  ).toBe(true);
});

test("vfork execve detached child memory does not inherit the parent linear memory size", async () => {
  const instantiatedPages: number[] = [];
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    instantiatedPages.push(memory.buffer.byteLength / 65536);
    return {
      exports: makeKernelExports(memory),
    } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];

  const liveStackPtr = 0x2400000;
  (parentKernel.memory.grow as unknown as (delta: bigint) => number)(512n);
  const parentPages = parentKernel.memory.buffer.byteLength / 65536;
  parentKernel.thread_sp = () => BigInt(liveStackPtr);
  parentKernel.thread_vfork_child_pid = () => 55;
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
  installVforkExecveSpawnChildPid(parentKernel);

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    sp: BigInt(liveStackPtr),
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    sp: BigInt(liveStackPtr),
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(40);
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(instantiatedPages[1]).toBeLessThan(parentPages);
  expect(instantiatedPages[1]).toBeGreaterThanOrEqual((0x1000000 + liveStackPtr + 8 * 1024 * 1024) / 65536);
});

test("vfork execve preserves child-private OFDs across kernel-worker scrub", async () => {
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
      const childPrivateOfds = [7, 8, 9];
      exports.process_fd_ofd_id = (_vmPtr, fd) => {
        if (fd >= 0 && fd <= 2) {
          return childPrivateOfds[fd] ?? 0;
        }
        return fd === 3 ? 1 : 0;
      };
      exports.thread_complete_execve_handoff = () => {
        const bytes = new Uint8Array(memory.buffer);
        childPrivateOfds.forEach((ofdId, index) => {
          const slotOffset = state.ofdTablePtr + ofdId * 32;
          bytes[slotOffset] = 0x01;
          bytes[slotOffset + 12] = 0x20 + index;
          bytes[slotOffset + 13] = 0x40 + index;
        });
        return 0n;
      };
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "sync-process-lifecycle":
            return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              ppid: 1,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "rollback-vfork-execve-handoff":
            return {
              type: "vfork-execve-handoff-rolled-back",
              requestId: 0,
              waitStatus: 127 << 8,
              kernelState: msg.kernelState,
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
          case "sync-process-memfs":
            return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => fd === 3 ? 1 : 0;
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    installVforkExecveSpawnChildPid(parentKernel);

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

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
    for (const [index, ofdId] of [7, 8, 9].entries()) {
      const slotOffset = makeKernelState().ofdTablePtr + ofdId * 32;
      expect(childBytes[slotOffset]).toBe(0x01);
      expect(childBytes[slotOffset + 12]).toBe(0x20 + index);
      expect(childBytes[slotOffset + 13]).toBe(0x40 + index);
    }
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve keeps CLOEXEC-style parent-only OFDs out of the child without mutating the parent", async () => {
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
      exports.process_fd_ofd_id = (_vmPtr, fd) => {
        switch (fd) {
          case 0:
            return 1;
          case 1:
            return 2;
          case 2:
            return 3;
          case 3:
            return 4;
          case 4:
            return 0;
          default:
            return 0;
        }
      };
      exports.thread_complete_execve_handoff = () => 0n;
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
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
          case "sync-process-lifecycle":
            return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              ppid: 1,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "rollback-vfork-execve-handoff":
            return {
              type: "vfork-execve-handoff-rolled-back",
              requestId: 0,
              waitStatus: 127 << 8,
              kernelState: msg.kernelState,
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
          case "sync-process-memfs":
            return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const state = makeKernelState();
    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    const parentBytes = new Uint8Array(parentKernel.memory.buffer);
    const cloexecOfdId = 12;
    const cloexecSlotOffset = state.ofdTablePtr + cloexecOfdId * 32;
    parentBytes[cloexecSlotOffset] = 0x01;
    parentBytes[cloexecSlotOffset + 12] = 0xc1;
    parentBytes[cloexecSlotOffset + 13] = 0xd1;
    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => {
      switch (fd) {
        case 0:
          return 1;
        case 1:
          return 2;
        case 2:
          return 3;
        case 3:
          return 4;
        case 4:
          return cloexecOfdId;
        default:
          return 0;
      }
    };
    parentKernel.process_fd_flags = (_vmPtr, fd) => fd === 4 ? parentKernel.abi_o_cloexec() : 0;
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    installVforkExecveSpawnChildPid(parentKernel);

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

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
    await flushMicrotasks(20);
    await new Promise((resolve) => setTimeout(resolve, 0));
    await flushMicrotasks(20);

    const childKernel = createdKernelExports[1];
    expect(parentKernel.process_fd_ofd_id(0x1234, 4)).toBe(cloexecOfdId);
    expect(childKernel.process_fd_ofd_id(0x1234, 4)).toBe(0);
    expect(parentBytes[cloexecSlotOffset + 12]).toBe(0xc1);
    expect(parentBytes[cloexecSlotOffset + 13]).toBe(0xd1);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve preserves dup-style child aliases across fork-shared scrub", async () => {
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
      exports.process_fd_ofd_id = (_vmPtr, fd) => {
        switch (fd) {
          case 0:
            return 1;
          case 1:
            return 2;
          case 2:
            return 3;
          case 4:
          case 5:
            return 11;
          default:
            return 0;
        }
      };
      exports.thread_complete_execve_handoff = () => {
        const view = new DataView(memory.buffer);
        const seedPipeOfd = (ofdId: number, pipeSlot: number, hostFd: number, marker: number) => {
          const slotOffset = state.ofdTablePtr + ofdId * 32;
          view.setInt32(slotOffset, 2, true);
          view.setInt32(slotOffset + 4, hostFd, true);
          view.setUint32(slotOffset + 24, pipeSlot, true);
          view.setUint8(slotOffset + 12, marker);
          view.setUint8(slotOffset + 13, marker + 1);
          const pipeBase = state.pipeBase + pipeSlot * MOCK_PIPE_SLOT_BYTES;
          view.setInt32(pipeBase + 8, 1, true);
          view.setInt32(pipeBase + 12, 1, true);
        };
        seedPipeOfd(1, 1, 0, 0x11);
        seedPipeOfd(2, 2, -1, 0x22);
        seedPipeOfd(3, 3, -1, 0x33);
        const aliasedSlotOffset = state.ofdTablePtr + 11 * 32;
        view.setInt32(aliasedSlotOffset, 1, true);
        view.setInt32(aliasedSlotOffset + 4, 17, true);
        view.setUint32(aliasedSlotOffset + 24, 99, true);
        view.setUint8(aliasedSlotOffset + 12, 0xa1);
        view.setUint8(aliasedSlotOffset + 13, 0xb1);
        return 0n;
      };
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
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
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              ppid: 1,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "rollback-vfork-execve-handoff":
            return {
              type: "vfork-execve-handoff-rolled-back",
              requestId: 0,
              waitStatus: 127 << 8,
              kernelState: msg.kernelState,
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
          case "sync-process-memfs":
            return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => {
      switch (fd) {
        case 0:
          return 1;
        case 1:
          return 2;
        case 2:
          return 3;
        case 3:
          return 4;
        default:
          return 0;
      }
    };
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    installVforkExecveSpawnChildPid(parentKernel);

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

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
    await flushMicrotasks(20);
    await new Promise((resolve) => setTimeout(resolve, 0));

    const childKernel = createdKernelExports[1];
    expect(childKernel.process_fd_ofd_id(0x1234, 4)).toBe(11);
    expect(childKernel.process_fd_ofd_id(0x1234, 5)).toBe(11);
    const childBytes = new Uint8Array(childKernel.memory.buffer);
    const aliasedSlotOffset = makeKernelState().ofdTablePtr + 11 * 32;
    expect(childBytes[aliasedSlotOffset]).toBe(0x01);
    expect(childBytes[aliasedSlotOffset + 12]).toBe(0xa1);
    expect(childBytes[aliasedSlotOffset + 13]).toBe(0xb1);

    const rehydrate = [...requests].reverse().find((msg) => msg.type === "rehydrate-fork-shared-state");
    expect(rehydrate).toBeUndefined();
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve handoff failure does not publish a partially spawned child", async () => {
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
      exports.thread_complete_execve_handoff = () => -BigInt(exports.abi_enoent());
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const requests: KernelWorkerRequest[] = [];
    const onError = vi.fn();
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      onError,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        requests.push(msg);
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              ppid: 1,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "rollback-vfork-execve-handoff":
            return {
              type: "vfork-execve-handoff-rolled-back",
              requestId: 0,
              waitStatus: 127 << 8,
              kernelState: msg.kernelState,
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
          case "sync-process-memfs":
            return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    installVforkExecveSpawnChildPid(parentKernel);

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

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
    await flushMicrotasks();

    expect(onError).not.toHaveBeenCalled();
    expect(createdWorkers).toHaveLength(1);
    expect(
      requests.some((msg) => msg.type === "rollback-vfork-execve-handoff" && msg.childPid === 156),
    ).toBe(true);
    expect(
      requests.some((msg) => msg.type === "register-process" && msg.pid === 156),
    ).toBe(false);
    expect(
      requests.some((msg) => msg.type === "initialize-process-lifecycle" && msg.pid === 156),
    ).toBe(false);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve handoff failure restores the parent without surfacing a fatal runtime error", async () => {
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
      exports.thread_complete_execve_handoff = () => -BigInt(exports.abi_enoent());
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const requests: KernelWorkerRequest[] = [];
    const onError = vi.fn();
    const resumedState = {
      ...makeKernelState(),
      heapPtr: makeKernelState().heapPtr + 0x60,
      processPendingPtr: makeKernelState().processPendingPtr + 0x20,
    };
    const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      onError,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        requests.push(msg);
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              ppid: 1,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "rollback-vfork-execve-handoff":
            return {
              type: "vfork-execve-handoff-rolled-back",
              requestId: 0,
              waitStatus: 127 << 8,
              kernelState: {
                ...resumedState,
                processPendingPtr: makeKernelState().processPendingPtr,
              },
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: resumedState };
          case "sync-process-memfs":
            return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    const rollbackResumeEvents: string[] = [];
    const postParentWorkerMessage = parentWorker.postMessage.bind(parentWorker);
    parentWorker.postMessage = (message): void => {
      if (message.type === "init") {
        rollbackResumeEvents.push("parent-reattach");
      }
      postParentWorkerMessage(message);
    };
    const processCurrentPid = parentKernel.process_current_pid.bind(parentKernel);
    parentKernel.process_current_pid = (vmPtr): number => {
      rollbackResumeEvents.push("parent-resume-invariant");
      return processCurrentPid(vmPtr);
    };
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    installVforkExecveSpawnChildPid(parentKernel);

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    parentWorker.clearPostedMessages();
    rollbackResumeEvents.length = 0;
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

    expect(createdWorkers[0]?.terminated).toBe(false);
    expect(createdWorkers).toHaveLength(1);
    expect(onError).not.toHaveBeenCalled();
    expect(
      requests.some((msg) => msg.type === "rollback-vfork-execve-handoff" && msg.childPid === 156),
    ).toBe(true);
    expect(handle.kernelState.heapPtr).toBe(resumedState.heapPtr);
    expect(handle.kernelState.processPendingPtr).toBe(makeKernelState().processPendingPtr);
    expect(readPublishedKernelRuntimeState(handle.runtimeBridge)).toMatchObject({
      heapPtr: handle.kernelState.heapPtr,
      processPendingPtr: handle.kernelState.processPendingPtr,
    });
    expect(rollbackResumeEvents).toContain("parent-resume-invariant");
    expect(rollbackResumeEvents.indexOf("parent-resume-invariant")).toBeLessThan(
      rollbackResumeEvents.indexOf("parent-reattach"),
    );
    expect(parentWorker.postedMessages).toEqual([
      expect.objectContaining({
        type: "init",
        wasmModule: expect.any(Object),
        memory: expect.any(WebAssembly.Memory),
        vmPtr: 0x1234,
        guestBase: 0x1000000,
        forcePrepare: undefined,
        preserveMemoryBytes: undefined,
        kernelStackSlot: expect.any(Number),
        generation: expect.any(Number),
        runtimeEpoch: expect.any(Number),
        guestWriteWatchRegions: [],
        kernelState: handle.kernelState,
        sabPageCache: null,
        runtimeNotify: expect.any(SharedArrayBuffer),
        runtimeState: expect.any(SharedArrayBuffer),
      }),
    ]);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve status-style handoff failure restores the parent and surfaces the handoff marker", async () => {
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
      exports.thread_complete_execve_handoff = () => -BigInt(exports.abi_status_vfork_wait());
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const requests: KernelWorkerRequest[] = [];
    const onError = vi.fn();
    const resumedState = {
      ...makeKernelState(),
      heapPtr: makeKernelState().heapPtr + 0x20,
      processPendingPtr: makeKernelState().processPendingPtr + 0x10,
    };
    const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      onError,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        requests.push(msg);
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              ppid: 1,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "rollback-vfork-execve-handoff":
            return {
              type: "vfork-execve-handoff-rolled-back",
              requestId: 0,
              waitStatus: 127 << 8,
              kernelState: {
                ...resumedState,
                processPendingPtr: makeKernelState().processPendingPtr,
              },
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: resumedState };
          case "sync-process-memfs":
            return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    installVforkExecveSpawnChildPid(parentKernel);

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

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

    expect(createdWorkers[0]?.terminated).toBe(false);
    expect(createdWorkers).toHaveLength(1);
    expect(onError).not.toHaveBeenCalled();
    expect(
      requests.some((msg) => msg.type === "rollback-vfork-execve-handoff" && msg.childPid === 156),
    ).toBe(true);
    expect(handle.kernelState.heapPtr).toBe(resumedState.heapPtr);
    expect(handle.kernelState.processPendingPtr).toBe(makeKernelState().processPendingPtr);
    expect(handle.lastDebugMessage).toContain("vfork-execve:handoff-failed child=156 ret=-8");
    expect(parentWorker.postedMessages).toEqual([
      expect.objectContaining({
        type: "init",
        wasmModule: expect.any(Object),
        memory: expect.any(WebAssembly.Memory),
        vmPtr: 0x1234,
        guestBase: 0x1000000,
        forcePrepare: undefined,
        preserveMemoryBytes: undefined,
        kernelStackSlot: expect.any(Number),
        generation: expect.any(Number),
        runtimeEpoch: expect.any(Number),
        guestWriteWatchRegions: [],
        kernelState: handle.kernelState,
        sabPageCache: null,
        runtimeNotify: expect.any(SharedArrayBuffer),
        runtimeState: expect.any(SharedArrayBuffer),
      }),
    ]);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve handoff failure makes synthetic child exit visible to a later STATUS_WAIT4 retry", async () => {
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
      exports.thread_complete_execve_handoff = () => -BigInt(exports.abi_status_vfork_wait());
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const requests: KernelWorkerRequest[] = [];
    const resumedState = {
      ...makeKernelState(),
      heapPtr: makeKernelState().heapPtr + 0x20,
      processPendingPtr: makeKernelState().processPendingPtr + 0x10,
    };
    const wait4ResumedState = {
      ...resumedState,
      heapPtr: resumedState.heapPtr + 0x40,
    };
    const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        requests.push(msg);
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              ppid: 1,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "rollback-vfork-execve-handoff":
            return {
              type: "vfork-execve-handoff-rolled-back",
              requestId: 0,
              waitStatus: 127 << 8,
              kernelState: {
                ...resumedState,
                processPendingPtr: makeKernelState().processPendingPtr,
              },
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: resumedState };
          case "resume-wait4":
            return { type: "wait4-resumed", requestId: 0, kernelState: wait4ResumedState };
          case "sync-process-memfs":
            return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    let currentStatus = parentKernel.abi_status_ok();
    const resumeWait4 = vi.fn(() => {
      currentStatus = parentKernel.abi_status_ok();
      return 0n;
    });
    parentKernel.thread_resume_wait4 = resumeWait4;
    parentKernel.thread_status = () => currentStatus;
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    installVforkExecveSpawnChildPid(parentKernel);

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    parentWorker.clearPostedMessages();
    currentStatus = parentKernel.abi_status_wait4();
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

    expect(
      requests.some((msg) => msg.type === "rollback-vfork-execve-handoff" && msg.childPid === 156),
    ).toBe(true);
    expect(parentWorker.postedMessages).toEqual([
      expect.objectContaining({
        type: "init",
        wasmModule: expect.any(Object),
        memory: expect.any(WebAssembly.Memory),
        vmPtr: 0x1234,
        guestBase: 0x1000000,
        forcePrepare: undefined,
        preserveMemoryBytes: undefined,
        kernelStackSlot: expect.any(Number),
        generation: expect.any(Number),
        runtimeEpoch: expect.any(Number),
        guestWriteWatchRegions: [],
        kernelState: {
          ...resumedState,
          processPendingPtr: makeKernelState().processPendingPtr,
        },
        sabPageCache: null,
        runtimeNotify: expect.any(SharedArrayBuffer),
        runtimeState: expect.any(SharedArrayBuffer),
      }),
    ]);

    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 31,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: resumedState,
    });

    await flushMicrotasks();
    await new Promise((resolve) => setTimeout(resolve, 20));
    await flushMicrotasks();

    expect(requests.some((msg) => msg.type === "resume-wait4")).toBe(true);
    expect(resumeWait4).not.toHaveBeenCalled();
    expect(handle.kernelState.heapPtr).toBe(wait4ResumedState.heapPtr);
    expect(parentWorker.postedMessages).toEqual([]);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve handoff failure reinitializes the restored parent while a sibling is running", async () => {
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
    if (instantiateCount === 3) {
      exports.thread_complete_execve_handoff = () => -BigInt(exports.abi_status_vfork_wait());
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const resumedState = {
      ...makeKernelState(),
      heapPtr: makeKernelState().heapPtr + 0x20,
      processPendingPtr: makeKernelState().processPendingPtr + 0x10,
    };
    const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "sync-process-lifecycle":
            return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              ppid: 1,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "rollback-vfork-execve-handoff":
            return {
              type: "vfork-execve-handoff-rolled-back",
              requestId: 0,
              waitStatus: 127 << 8,
              kernelState: msg.kernelState,
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: resumedState };
          case "sync-process-memfs":
            return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
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

    const siblingWorker = createdWorkers[1];
    emitReady(siblingWorker, 0x2000);
    siblingWorker.clearPostedMessages();
    parentWorker.clearPostedMessages();

    handle.runningWorker = siblingWorker as unknown as Worker;
    handle.vforkReadyWorkers.length = 0;
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    installVforkExecveSpawnChildPid(parentKernel);

    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

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

    expect(handle.vforkReadyWorkers).toEqual([]);
    expect(handle.runningWorker).toBe(siblingWorker as unknown as Worker);
    expect(parentWorker.postedMessages).toEqual([
      expect.objectContaining({
        type: "init",
        wasmModule: expect.any(Object),
        memory: expect.any(WebAssembly.Memory),
        vmPtr: 0x1234,
        guestBase: 0x1000000,
        forcePrepare: undefined,
        preserveMemoryBytes: undefined,
        kernelStackSlot: expect.any(Number),
        generation: expect.any(Number),
        runtimeEpoch: expect.any(Number),
        guestWriteWatchRegions: [],
        kernelState: {
          ...makeKernelState(),
          pipeUsed: 1n,
        },
        sabPageCache: null,
        runtimeNotify: expect.any(SharedArrayBuffer),
        runtimeState: expect.any(SharedArrayBuffer),
      }),
    ]);

    siblingWorker.emit({
      type: "status",
      status: parentKernel.abi_status_futex_wait(),
      waitInWorker: true,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x2000,
      kernelState: resumedState,
    });
    await flushMicrotasks();

    expect(handle.runningWorker).toBe(null);
    expect(handle.vforkReadyWorkers).toEqual([]);
    expect(parentWorker.postedMessages).toHaveLength(1);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve keeps post-vfork stdio rewires child-private across kernel-worker scrub", async () => {
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
      const childPrivateStdioOfds = [7, 8, 9];
      exports.process_fd_ofd_id = (_vmPtr, fd) => {
        if (fd >= 0 && fd <= 2) {
          return childPrivateStdioOfds[fd] ?? 0;
        }
        return fd === 3 ? 4 : 0;
      };
      exports.thread_complete_execve_handoff = () => {
        const view = new DataView(memory.buffer);
        childPrivateStdioOfds.forEach((ofdId, index) => {
          const slotOffset = state.ofdTablePtr + ofdId * 32;
          view.setInt32(slotOffset, 2, true);
          view.setInt32(slotOffset + 4, index === 0 ? 1 : -1, true);
          view.setInt32(slotOffset + 8, 0, true);
          view.setUint32(slotOffset + 24, 4 + index, true);
          view.setUint8(slotOffset + 12, 0x70 + index);
          view.setUint8(slotOffset + 13, 0x80 + index);
        });
        const stdoutPipeBase = state.pipeBase + 4 * MOCK_PIPE_SLOT_BYTES;
        view.setInt32(stdoutPipeBase + 8, 5, true);
        view.setInt32(stdoutPipeBase + 12, 6, true);
        view.setUint8(stdoutPipeBase + 32, 0xc7);
        view.setUint8(stdoutPipeBase + 33, 0xc8);
        return 0n;
      };
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
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
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              ppid: 1,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
          case "sync-process-memfs":
            return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const state = makeKernelState();
    const writeOfdType = (bytes: Uint8Array, ofdId: number, marker: number): void => {
      const slotOffset = state.ofdTablePtr + ofdId * 32;
      bytes[slotOffset] = 0x01;
      bytes[slotOffset + 12] = marker;
      bytes[slotOffset + 13] = marker + 0x10;
    };

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    const parentBytes = new Uint8Array(parentKernel.memory.buffer);

    [1, 2, 3, 4].forEach((ofdId, index) => writeOfdType(parentBytes, ofdId, 0x20 + index));
    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => {
      if (fd >= 0 && fd <= 2) {
        return fd + 1;
      }
      return fd === 3 ? 4 : 0;
    };
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    installVforkExecveSpawnChildPid(parentKernel);

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    [7, 8, 9].forEach((ofdId, index) => writeOfdType(parentBytes, ofdId, 0x70 + index));
    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => {
      if (fd >= 0 && fd <= 2) {
        return 7 + fd;
      }
      return fd === 3 ? 4 : 0;
    };

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

    const rehydrate = [...requests].reverse().find((msg) => msg.type === "rehydrate-fork-shared-state");
    expect(rehydrate?.type).toBe("rehydrate-fork-shared-state");
    if (rehydrate?.type !== "rehydrate-fork-shared-state") {
      throw new Error("expected rehydrate-fork-shared-state request");
    }
    expect(rehydrate.ofdIds).toEqual([]);
    expect(rehydrate.targetPids).toEqual([156]);
    const childSync = [...requests].reverse().find((msg) => msg.type === "sync-fork-shared-state");
    expect(childSync?.type).toBe("sync-fork-shared-state");
    if (childSync?.type !== "sync-fork-shared-state") {
      throw new Error("expected sync-fork-shared-state request");
    }
    expect(childSync.ofdIds).toEqual([4]);

    const childKernel = createdKernelExports[1];
    const childBytes = new Uint8Array(childKernel.memory.buffer);
    for (const [index, ofdId] of [7, 8, 9].entries()) {
      const slotOffset = state.ofdTablePtr + ofdId * 32;
      expect(childBytes[slotOffset]).toBe(0x02);
      expect(childBytes[slotOffset + 12]).toBe(0x70 + index);
      expect(childBytes[slotOffset + 13]).toBe(0x80 + index);
    }
    const childView = new DataView(childKernel.memory.buffer);
    const stdoutPipeBase = state.pipeBase + 4 * MOCK_PIPE_SLOT_BYTES;
    expect(childView.getInt32(stdoutPipeBase + 8, true)).toBe(0);
    expect(childView.getInt32(stdoutPipeBase + 12, true)).toBe(0);
    expect(childView.getUint8(stdoutPipeBase + 32)).toBe(0xc7);
    expect(childView.getUint8(stdoutPipeBase + 33)).toBe(0xc8);

    requests.length = 0;
    const childWorker = parentWorker;
    emitReady(childWorker);
    childWorker.clearPostedMessages();
    childWorker.emit({
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

    const postReadySync = requests.find((msg) => msg.type === "sync-fork-shared-state" && msg.pid === 156);
    expect(postReadySync?.type).toBe("sync-fork-shared-state");
    if (postReadySync?.type !== "sync-fork-shared-state") {
      throw new Error("expected post-ready sync-fork-shared-state request");
    }
    expect(postReadySync.ofdIds).toEqual([4]);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve keeps distinct shared stdout and stderr pipe slots across kernel-worker scrub", async () => {
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
      exports.process_fd_ofd_id = (_vmPtr, fd) => {
        switch (fd) {
          case 0:
            return 1;
          case 1:
            return 8;
          case 2:
            return 10;
          case 3:
            return 4;
          default:
            return 0;
        }
      };
      exports.thread_complete_execve_handoff = () => {
        const view = new DataView(memory.buffer);
        const writePipeOfdIds = [
          [8, 4, 0x88],
          [10, 5, 0xaa],
        ] as const;
        for (const [ofdId, pipeSlot, marker] of writePipeOfdIds) {
          const slotOffset = state.ofdTablePtr + ofdId * 32;
          view.setInt32(slotOffset, 2, true);
          view.setInt32(slotOffset + 4, -1, true);
          view.setInt32(slotOffset + 8, 0, true);
          view.setUint32(slotOffset + 24, pipeSlot, true);
          view.setUint8(slotOffset + 12, marker);
          view.setUint8(slotOffset + 13, marker + 1);
        }
        const stdoutPipeBase = state.pipeBase + 4 * MOCK_PIPE_SLOT_BYTES;
        view.setInt32(stdoutPipeBase + 8, 1, true);
        view.setInt32(stdoutPipeBase + 12, 1, true);
        const stderrPipeBase = state.pipeBase + 5 * MOCK_PIPE_SLOT_BYTES;
        view.setInt32(stderrPipeBase + 8, 1, true);
        view.setInt32(stderrPipeBase + 12, 1, true);
        return 0n;
      };
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
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
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              ppid: 1,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
          case "sync-process-memfs":
            return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => {
      switch (fd) {
        case 0:
          return 1;
        case 1:
          return 2;
        case 2:
          return 3;
        case 3:
          return 4;
        default:
          return 0;
      }
    };
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    installVforkExecveSpawnChildPid(parentKernel);

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

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
    expect(childKernel.process_fd_ofd_id(0x1234, 1)).toBe(8);
    expect(childKernel.process_fd_ofd_id(0x1234, 2)).toBe(10);
    expect(childKernel.kernel_ofd_ino(8)).toBe(4);
    expect(childKernel.kernel_ofd_ino(10)).toBe(5);

    const childView = new DataView(childKernel.memory.buffer);
    const state = makeKernelState();
    const stdoutPipeBase = state.pipeBase + 4 * MOCK_PIPE_SLOT_BYTES;
    const stderrPipeBase = state.pipeBase + 5 * MOCK_PIPE_SLOT_BYTES;
    expect(childView.getInt32(stdoutPipeBase + 8, true)).toBe(1);
    expect(childView.getInt32(stdoutPipeBase + 12, true)).toBe(1);
    expect(childView.getInt32(stderrPipeBase + 8, true)).toBe(1);
    expect(childView.getInt32(stderrPipeBase + 12, true)).toBe(1);

  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve rehydrates child shared kernel objects without mutating parent memfs bytes", async () => {
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
      const ofdId = 1;
      const ofdOffset = state.ofdTablePtr + ofdId * 32;
      exports.process_fd_ofd_id = (_vmPtr, fd) => fd === 3 ? ofdId : 0;
      exports.thread_complete_execve_handoff = () => {
        const bytes = new Uint8Array(memory.buffer);
        bytes[ofdOffset] = 0x01;
        bytes[ofdOffset + 20] = 0x81;
        bytes[state.memfsPtr + 12] = 0x82;
        bytes[exports.kernel_memfs_inodes_ptr() + 6] = 0x83;
        bytes[exports.kernel_memfs_data_ptr() + 10] = 0x84;
        return 0n;
      };
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

    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => fd === 3 ? ofdId : 0;
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    installVforkExecveSpawnChildPid(parentKernel);

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

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

    const parentBytes = new Uint8Array(parentKernel.memory.buffer);
    const childKernel = createdKernelExports[1];
    const childBytes = new Uint8Array(childKernel.memory.buffer);

    expect(parentBytes[ofdOffset + 20]).toBe(0x81);
    expect(parentBytes[state.memfsPtr + 12]).toBe(0);
    expect(parentBytes[parentKernel.kernel_memfs_inodes_ptr() + 6]).toBe(0);
    expect(parentBytes[parentKernel.kernel_memfs_data_ptr() + 10]).toBe(0);
    expect(childBytes[ofdOffset + 20]).toBe(0x81);
    expect(childBytes[state.memfsPtr + 12]).toBe(0x82);
    expect(childBytes[childKernel.kernel_memfs_inodes_ptr() + 6]).toBe(0x83);
    expect(childBytes[childKernel.kernel_memfs_data_ptr() + 10]).toBe(0x84);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("child runtime errors propagate to the parent process handle", async () => {
  const parentError = vi.fn();
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    onError: parentError,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "canonicalize-child-pid":
          return {
            type: "child-pid-canonicalized",
            requestId: 0,
            childPid: 156,
            ppid: 1,
            nextPid: 201,
            pgid: 33,
            sid: 44,
            kernelState: msg.kernelState,
          };
        case "resume-vfork-execve-parent":
          return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-memfs":
          return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "rehydrate-fork-shared-state":
          return { type: "fork-shared-state-rehydrated", requestId: 0 };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });

  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  parentKernel.thread_vfork_child_pid = () => 55;
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
  installVforkExecveSpawnChildPid(parentKernel);

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

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
  childWorker.emit({
    type: "error",
    vmPtr: 0x1234,
    message: "boom",
  });
  await flushMicrotasks(20);

  expect(parentError).toHaveBeenCalledTimes(1);
  expect(parentError.mock.calls[0][0]).toBeInstanceOf(Error);
  expect((parentError.mock.calls[0][0] as Error).message).toContain("child-debug:156:");
  expect((parentError.mock.calls[0][0] as Error).message).toContain("boom");
});

test("vfork child runs exclusively until the parent thread is resumed", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];

  parentKernel.supervisor_process_resume = (_processVmPtr, outPtr, outLen) => {
    if (outLen < parentKernel.supervisor_resume_record_bytes()) {
      return 0;
    }
    return writeSupervisorRunnableResumeRecord(parentKernel.memory, outPtr, {
      vmPtr: 0x1234,
      tid: 1,
      processPtr: parentKernel.thread_process_ptr(0x1234),
      status: parentKernel.abi_status_ok(),
    });
  };

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

  const siblingWorker = createdWorkers[1];
  emitReady(siblingWorker, 0x2000);
  parentWorker.clearPostedMessages();
  siblingWorker.clearPostedMessages();

  parentWorker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

  expect(handle.vforkWorker).toBe(parentWorker);
  expect(parentWorker.postedMessages).toEqual([
    expect.objectContaining({ type: "step", budget: 20_000 }),
  ]);

  siblingWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x2000,
    kernelState: makeKernelState(),
  });

  expect(siblingWorker.postedMessages).toEqual([]);
});
