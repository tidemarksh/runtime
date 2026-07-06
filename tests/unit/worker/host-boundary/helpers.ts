import {
  createRuntimeStateBridge,
} from "../../../../src/bridge/runtime-state.js";
import {
  createTransferDebugStats,
  createWorkerMessageDebugStats,
  type ProcessHandle,
  type ThreadWorkerState,
} from "../../../../src/worker/types.js";
import {
  setProcessDebugCommand,
  setProcessLastDebugMessage,
} from "../../../../src/worker/process-debug-state.js";
import {
  clearThreadOwnerAsyncStatus,
  clearThreadWaitingInWorkerStatus,
  setThreadHostIdentity,
  setThreadInitialized,
  setThreadReinitCount,
} from "../../../../src/worker/thread-host-state.js";
import { installSupervisorBlockedResumeStub } from "../support/supervisor-blocked-resume.js";
import { seedProcessHandleStateFixture } from "../support/process-handle-state-fixture.js";
import { vi } from "vite-plus/test";

export function makeHostBoundaryKernel() {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  return installSupervisorBlockedResumeStub({
    abi_status_ok: () => 0,
    abi_status_futex_wait: () => 20,
    abi_status_sleep: () => 26,
    abi_status_wait4: () => 31,
    abi_status_epoll_wait: () => 7,
    abi_status_vfork_wait: () => 8,
    abi_status_execve_spawn: () => 9,
    abi_status_clone_spawn: () => 10,
    abi_status_fault: () => 3,
    abi_status_pipe_wait: () => 6,
    abi_status_connect: () => 28,
    abi_status_socket_recv: () => 30,
    abi_status_socket_send: () => 29,
    abi_status_flock_wait: () => 32,
    abi_status_fs: () => 33,
    abi_max_threads: () => 64,
    abi_max_fds: () => 64,
    abi_max_pipes: () => 64,
    abi_fd_type_file: () => 1,
    abi_fd_type_pipe: () => 2,
    memory,
    thread_status: () => 0,
    thread_process_ptr: () => 0x2000,
    thread_pc: () => 0x1000n,
    thread_sp: () => 0x8000n,
    thread_s11: () => 0x9000n,
    thread_tid: () => 2,
    thread_record_blocked_addr: () => 0n,
    thread_futex_wait_addr: () => 0n,
    thread_blocked_ofd_id: () => 0,
    thread_process_thread_count: () => 1,
    thread_exit_code: () => 0,
    thread_term_signal: () => 0,
    process_current_pid: () => 1,
    process_thread_vm_ptr: (_processVmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0,
    process_fd_ofd_id: () => 0,
    process_fd_flags: () => 0,
    kernel_scratch_base: () => 0,
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0,
    kernel_ofd_table_bytes: () => 0,
    kernel_ofd_fd_type: () => 0,
    kernel_ofd_refcount: () => 0,
    kernel_ofd_ino: () => 0,
    kernel_ofd_host_fd: () => 0,
    kernel_ofd_offset: () => 0n,
    kernel_pipe_region_bytes: () => 0,
    kernel_memfs_ptr: () => 0,
    kernel_memfs_header_bytes: () => 0,
    kernel_memfs_inodes_bytes: () => 0,
    kernel_memfs_first_child_bytes: () => 0,
    kernel_memfs_dir_entries_bytes: () => 0,
    kernel_memfs_data_bytes: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_epoll_table_bytes: () => 0,
    kernel_epoll_instance_bytes: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_table_bytes: () => 0,
    kernel_socket_used: () => 0,
    kernel_pipe_base: () => 0,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_pool_bytes: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_sig_slots_bytes: () => 0,
    kernel_signal_action_table_bytes: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_pending_bytes: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_process_runtime_state_bytes: () => 0,
    kernel_time_state_ptr: () => 0,
    kernel_time_state_bytes: () => 0,
    kernel_attach_runtime_state: () => {},
    supervisor_complete_futex_wait: vi.fn(() => 0),
    supervisor_futex_wait_completion_woken: () => 1,
    supervisor_futex_wait_completion_not_equal: () => 2,
    supervisor_futex_wait_completion_invalid_address: () => 4,
    supervisor_futex_wait_completion_signal_check: () => 5,
    supervisor_stdio_output_settle_kind_visible: () => 1,
    supervisor_stdio_output_settle_kind_inherited_child: () => 2,
    supervisor_stdio_pipe_read_progress_action_none: () => 0,
    supervisor_stdio_pipe_read_progress_action_snapshot: () => 1,
    supervisor_stdio_output_settle_max_passes: (kind: number) => {
      if (kind === 1) return 4;
      if (kind === 2) return 6;
      return -22;
    },
    supervisor_stdio_output_settle_stable_passes: (kind: number) => {
      if (kind === 1 || kind === 2) return 2;
      return -22;
    },
    supervisor_stdio_pipe_read_progress_action: (_vmPtr: number, _pipeSlot: number, drained: number) =>
      drained === 0 ? 0 : 1,
    supervisor_process_pipe_slot_for_fd(this: ProcessHandle["kernel"], vmPtr: number, fd: number) {
      const ofdId = this.process_fd_ofd_id(vmPtr, fd);
      if (ofdId <= 0 || ofdId >= this.abi_max_fds()) {
        return 0;
      }
      return this.kernel_ofd_fd_type(ofdId) === this.abi_fd_type_pipe()
        ? this.kernel_ofd_ino(ofdId)
        : 0;
    },
    supervisor_process_pipe_ofd_for_fd(this: ProcessHandle["kernel"], vmPtr: number, fd: number) {
      const ofdId = this.process_fd_ofd_id(vmPtr, fd);
      return ofdId > 0 && this.supervisor_process_pipe_slot_for_fd?.(vmPtr, fd) !== 0 ? ofdId : 0;
    },
    supervisor_process_pipe_host_fd_for_fd(this: ProcessHandle["kernel"], vmPtr: number, fd: number) {
      const ofdId = this.supervisor_process_pipe_ofd_for_fd?.(vmPtr, fd) ?? 0;
      return ofdId > 0 ? this.kernel_ofd_host_fd(ofdId) : -1;
    },
    supervisor_process_fd_is_open(this: ProcessHandle["kernel"], vmPtr: number, fd: number) {
      const ofdId = this.process_fd_ofd_id(vmPtr, fd);
      return ofdId > 0 && ofdId < this.abi_max_fds() ? 1 : 0;
    },
  } as unknown as ProcessHandle["kernel"]);
}

export function makeHostBoundaryThread(vmPtr: number): ThreadWorkerState {
  const thread = {
    vmPtr,
    stepCount: 0,
    totalStepBudget: 0,
    totalStepWallMs: 0,
    maxStepWallMs: 0,
    lastStepDurationMs: null,
    statusCount: 0,
    statusCounts: {},
    lastStatus: null,
    lastStepAtMs: null,
    lastStatusAtMs: null,
    lastStepProcessPtr: null,
    lastStepPc: null,
    lastStepSp: null,
    lastStepS11: null,
    hotStepPcs: {},
    lastStatusPc: null,
    lastStatusSp: null,
    lastStatusS11: null,
    hotStatusPcs: {},
  };
  setThreadHostIdentity(thread, 1, 0);
  setThreadInitialized(thread, true);
  setThreadReinitCount(thread, 0);
  clearThreadWaitingInWorkerStatus(thread);
  clearThreadOwnerAsyncStatus(thread);
  return thread;
}

export function makeHostBoundaryHandle(
  kernel: ProcessHandle["kernel"],
  worker: Worker,
): ProcessHandle {
  const handle = {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0x1234,
    guestMemoryPolicyLimitBytes: 0,
    maxMemoryPages: 0,
    forkMaxMemoryPages: 0,
    guestBase: 0,
    worker,
    memory: kernel.memory ?? new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel,
    kernelState: {
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
    },
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    ttyPtySlot: 0,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    stdioOwner: null,
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
      ofdSlotSnapshots: new Map(),
    },
    stdinQueue: [],
    stdinFlushPromise: null,
    outputBacklog: [],
    runtimeBridge: createRuntimeStateBridge(),
    sabPageCache: null,
    workers: new Set([worker]),
    transferDebugStats: createTransferDebugStats(),
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    exitInProgress: false,
    onExit: undefined,
    onError: undefined,
    onStdout: undefined,
    workerDebugLog: [],
  };
  setProcessDebugCommand(handle, "/bin/sh");
  setProcessLastDebugMessage(handle, undefined);
  seedProcessHandleStateFixture(handle);
  return handle;
}
