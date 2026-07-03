import type { KernelExports } from "../../../../src/abi.js";

const SUPERVISOR_RESUME_FLAG_TIMEOUT_EXPIRED = 1;
const SUPERVISOR_BLOCKED_RESUME_FLAG_KERNEL_WORKER_AVAILABLE = 1;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT = 0;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER = 1;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER = 2;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED = 3;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT = 4;
const SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DEFER = 0;
const SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DIRECT = 1;
const SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_NONE = 0;
const SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC = 1;
const SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_FLAG_KERNEL_WORKER_AVAILABLE = 1;
const SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_BLOCK = 0;
const SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET = 1;
const SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE = 2;
const SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_FORK_SHARED = 3;
const SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE_AND_FORK_SHARED = 4;
const SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET_AND_FORK_SHARED = 5;
const SUPERVISOR_BLOCKED_STATUS_TRANSITION_FLAG_KERNEL_WORKER_AVAILABLE = 1;
const SUPERVISOR_BLOCKED_STATUS_TRANSITION_FLAG_NETWORK_CONNECTIONS_AVAILABLE = 2;
const SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_SKIP = 0;
const SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_PUMP = 1;
const SUPERVISOR_STATE_SYNC_KIND_PIPE = 1;
const SUPERVISOR_STATE_SYNC_KIND_EPOLL = 2;
const SUPERVISOR_STATE_SYNC_KIND_SOCKET = 3;
const SUPERVISOR_STATE_SYNC_REQUEST_FLAG_WAKE_WAITERS = 1;
const SUPERVISOR_STATE_SYNC_RUN_FLAG_RUN = 1;
const SUPERVISOR_STATE_SYNC_RUN_FLAG_WAKE_WAITERS = 2;
const SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_SYNC = 1;
const SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_EPOLL_SYNC = 2;
const SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_SOCKET_SYNC = 3;
const SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_AND_EPOLL_SYNC = 4;
const SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_PIPE_WAIT = 1;
const SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_EPOLL_WAIT = 2;
const SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_SOCKET_RECV = 4;
const SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_SOCKET_SEND = 8;
const POLLFD_BYTES = 8;

export function installSupervisorBlockedResumeStub<T extends Partial<KernelExports>>(
  kernel: T,
): T & Pick<
  KernelExports,
  "supervisor_resume_blocked_syscall"
  | "supervisor_resume_flag_timeout_expired"
  | "supervisor_blocked_resume_writeback_count"
  | "supervisor_blocked_resume_writeback_ptr"
  | "supervisor_blocked_resume_writeback_len"
  | "supervisor_blocked_status_is_host_wait"
  | "supervisor_blocked_status_is_connect_wait"
  | "supervisor_blocked_status_is_epoll_wait"
  | "supervisor_blocked_status_is_pipe_wait"
  | "supervisor_blocked_status_is_wait4"
  | "supervisor_blocked_status_is_socket_wait"
  | "supervisor_blocked_status_needs_network_sidecar_restore"
  | "supervisor_blocked_status_syncs_socket_after_resume"
  | "supervisor_blocked_status_uses_fork_shared_state_sync"
  | "supervisor_process_status_is_lifecycle_handoff"
  | "supervisor_process_status_is_execve_spawn"
  | "supervisor_process_status_allows_pid_mismatch"
  | "supervisor_process_status_defers_kernel_state_adoption"
  | "supervisor_blocked_epoll_wait_timeout_ms"
  | "supervisor_blocked_poll_wait_timeout_ms"
  | "supervisor_blocked_poll_wait_has_socket"
  | "supervisor_blocked_pipe_wait_has_ready_events"
  | "supervisor_blocked_wait_may_depend_on_network"
  | "supervisor_blocked_wait_should_pump_network"
  | "supervisor_blocked_epoll_watches_pipe"
  | "supervisor_blocked_epoll_watched_ofd"
  | "supervisor_blocked_epoll_dependent_sync_ofd"
  | "supervisor_blocked_resume_delegation"
  | "supervisor_blocked_wait_active_child_pid"
  | "supervisor_blocked_pipe_resume_action_defer"
  | "supervisor_blocked_pipe_resume_action_direct"
  | "supervisor_blocked_pipe_resume_action"
  | "supervisor_blocked_pipe_sync_before_retry_action_none"
  | "supervisor_blocked_pipe_sync_before_retry_action_request_sync"
  | "supervisor_blocked_pipe_sync_before_retry_action_request_sync_and_drain_output"
  | "supervisor_blocked_pipe_sync_before_retry_action_schedule_retry"
  | "supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available"
  | "supervisor_blocked_pipe_sync_before_retry_action"
  | "supervisor_blocked_pipe_sync_before_retry_requested"
  | "supervisor_blocked_pipe_sync_before_retry_clear"
  | "supervisor_blocked_status_transition_action_block"
  | "supervisor_blocked_status_transition_action_sync_socket"
  | "supervisor_blocked_status_transition_action_sync_pipe"
  | "supervisor_blocked_status_transition_action_sync_fork_shared"
  | "supervisor_blocked_status_transition_action_sync_pipe_and_fork_shared"
  | "supervisor_blocked_status_transition_action_sync_socket_and_fork_shared"
  | "supervisor_blocked_status_transition_flag_kernel_worker_available"
  | "supervisor_blocked_status_transition_flag_network_connections_available"
  | "supervisor_blocked_status_transition_action"
  | "supervisor_blocked_wait_network_pump_action_skip"
  | "supervisor_blocked_wait_network_pump_action_pump"
  | "supervisor_blocked_wait_network_pump_action"
  | "supervisor_complete_blocked_wait_network_pump"
  | "supervisor_clear_blocked_wait_network_pump"
  | "supervisor_blocked_wait_wake_source_pipe_sync"
  | "supervisor_blocked_wait_wake_source_epoll_sync"
  | "supervisor_blocked_wait_wake_source_socket_sync"
  | "supervisor_blocked_wait_wake_source_pipe_and_epoll_sync"
  | "supervisor_blocked_wait_wake_target_pipe_wait"
  | "supervisor_blocked_wait_wake_target_epoll_wait"
  | "supervisor_blocked_wait_wake_target_socket_recv"
  | "supervisor_blocked_wait_wake_target_socket_send"
  | "supervisor_blocked_wait_wake_mask_for_sync"
  | "supervisor_blocked_wait_status_matches_wake_source"
  | "supervisor_state_sync_kind_pipe"
  | "supervisor_state_sync_kind_epoll"
  | "supervisor_state_sync_kind_socket"
  | "supervisor_state_sync_request_flag_wake_waiters"
  | "supervisor_state_sync_run_flag_run"
  | "supervisor_state_sync_run_flag_wake_waiters"
  | "supervisor_state_sync_request"
  | "supervisor_state_sync_next_run_flags"
  | "supervisor_state_sync_complete"
  | "supervisor_state_sync_clear"
  | "supervisor_process_held_ofd_refcount"
  | "supervisor_process_private_pipe_slot_mask"
  | "supervisor_process_private_pipe_source_mode"
  | "supervisor_process_private_pipe_target_mode"
  | "supervisor_process_held_pipe_slot_mask"
  | "supervisor_process_pipe_used_mask"
  | "supervisor_process_pipe_slot_mode"
  | "supervisor_process_pipe_read_open_count"
  | "supervisor_process_pipe_write_open_count"
  | "supervisor_process_epoll_held_ofd"
  | "supervisor_process_epoll_dependent_ofd"
  | "supervisor_process_sync_dependent_ofd"
  | "supervisor_process_epoll_dependent_sync_ofd"
  | "supervisor_process_epoll_depends_on_ofd"
> {
  const pipeSyncBeforeRetryRequested = new Set<number>();
  const networkPumpBackoffs = new Set<number>();
  const stateSyncRequests = new Map<string, { queuedRunFlags: number; inFlight: boolean }>();
  const toNumber = (value: bigint | number | undefined): number => (
    typeof value === "bigint" ? Number(value) : (value ?? 0)
  );
  const wakeMaskForSync = (source: number): number => {
    switch (source) {
      case SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_SYNC:
        return SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_PIPE_WAIT;
      case SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_EPOLL_SYNC:
        return SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_EPOLL_WAIT;
      case SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_SOCKET_SYNC:
        return SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_PIPE_WAIT
          | SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_EPOLL_WAIT
          | SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_SOCKET_RECV
          | SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_SOCKET_SEND;
      case SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_AND_EPOLL_SYNC:
        return SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_PIPE_WAIT
          | SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_EPOLL_WAIT;
      default:
        return -22;
    }
  };
  const wakeTargetForStatus = (status: number): number => {
    if (kernel.abi_status_pipe_wait?.() === status) {
      return SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_PIPE_WAIT;
    }
    if (kernel.abi_status_epoll_wait?.() === status) {
      return SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_EPOLL_WAIT;
    }
    if (kernel.abi_status_socket_recv?.() === status) {
      return SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_SOCKET_RECV;
    }
    if (kernel.abi_status_socket_send?.() === status) {
      return SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_SOCKET_SEND;
    }
    return 0;
  };
  const syscallMatches = (
    syscallNr: bigint,
    abiSyscall: (() => bigint | number) | undefined,
  ): boolean => {
    const value = abiSyscall?.();
    return value !== undefined && syscallNr === BigInt(value);
  };
  const blockedSyscallNr = (vmPtr: number): bigint =>
    BigInt(kernel.thread_blocked_syscall_nr?.(vmPtr) ?? 0n);
  const blockedSyscallArg = (vmPtr: number, index: number): number =>
    toNumber(kernel.thread_blocked_syscall_arg?.(vmPtr, index));
  const maxFds = (): number => Math.max(0, toNumber(kernel.abi_max_fds?.()) || 1024);
  const writebackRange = (
    vmPtr: number,
    blockedStatus: number,
    resumeResult: bigint | number,
    index: number,
  ): { guestPtr: number; length: number } | undefined => {
    const resumeLength = toNumber(resumeResult);
    if (resumeLength <= 0) {
      return undefined;
    }
    const syscallNr = blockedSyscallNr(vmPtr);
    if (blockedStatus === kernel.abi_status_pipe_wait?.()) {
      if (syscallMatches(syscallNr, kernel.abi_sys_ppoll) && index === 0) {
        const nfds = Math.min(blockedSyscallArg(vmPtr, 1), maxFds());
        return nfds > 0
          ? { guestPtr: blockedSyscallArg(vmPtr, 0), length: nfds * POLLFD_BYTES }
          : undefined;
      }
      if (syscallMatches(syscallNr, kernel.abi_sys_pselect6) && index >= 0 && index <= 2) {
        const nfds = Math.min(blockedSyscallArg(vmPtr, 0), maxFds());
        return nfds > 0
          ? {
              guestPtr: blockedSyscallArg(vmPtr, index + 1),
              length: Math.floor((nfds + 63) / 64) * 8,
            }
          : undefined;
      }
      if (syscallMatches(syscallNr, kernel.abi_sys_read) && index === 0) {
        return { guestPtr: blockedSyscallArg(vmPtr, 1), length: resumeLength };
      }
    }
    if (
      blockedStatus === kernel.abi_status_socket_recv?.()
      && index === 0
      && (
        syscallMatches(syscallNr, kernel.abi_sys_read)
        || syscallMatches(syscallNr, kernel.abi_sys_recvfrom)
      )
    ) {
      return { guestPtr: blockedSyscallArg(vmPtr, 1), length: resumeLength };
    }
    return undefined;
  };
  const compactWritebackRanges = (
    vmPtr: number,
    blockedStatus: number,
    resumeResult: bigint | number,
  ): Array<{ guestPtr: number; length: number }> => {
    const ranges: Array<{ guestPtr: number; length: number }> = [];
    for (let index = 0; index < 3; index += 1) {
      const range = writebackRange(vmPtr, blockedStatus, resumeResult, index);
      if (range !== undefined && range.guestPtr > 0 && range.length > 0) {
        ranges.push(range);
      }
    }
    return ranges;
  };
  const nextKernel = kernel as T & Pick<
    KernelExports,
    "supervisor_resume_blocked_syscall"
    | "supervisor_resume_flag_timeout_expired"
    | "supervisor_blocked_resume_writeback_count"
    | "supervisor_blocked_resume_writeback_ptr"
    | "supervisor_blocked_resume_writeback_len"
    | "supervisor_blocked_status_is_host_wait"
    | "supervisor_blocked_status_is_connect_wait"
    | "supervisor_blocked_status_is_epoll_wait"
    | "supervisor_blocked_status_is_pipe_wait"
    | "supervisor_blocked_status_is_wait4"
    | "supervisor_blocked_status_is_socket_wait"
    | "supervisor_blocked_status_needs_network_sidecar_restore"
    | "supervisor_blocked_status_syncs_socket_after_resume"
    | "supervisor_blocked_status_uses_fork_shared_state_sync"
    | "supervisor_blocked_epoll_wait_timeout_ms"
    | "supervisor_blocked_poll_wait_timeout_ms"
    | "supervisor_blocked_poll_wait_has_socket"
    | "supervisor_blocked_pipe_wait_has_ready_events"
    | "supervisor_blocked_wait_may_depend_on_network"
    | "supervisor_blocked_wait_should_pump_network"
    | "supervisor_blocked_epoll_watches_pipe"
    | "supervisor_blocked_epoll_watched_ofd"
    | "supervisor_blocked_epoll_dependent_sync_ofd"
    | "supervisor_blocked_resume_delegation"
    | "supervisor_blocked_wait_active_child_pid"
    | "supervisor_blocked_pipe_resume_action_defer"
    | "supervisor_blocked_pipe_resume_action_direct"
    | "supervisor_blocked_pipe_resume_action"
    | "supervisor_blocked_pipe_sync_before_retry_action_none"
    | "supervisor_blocked_pipe_sync_before_retry_action_request_sync"
    | "supervisor_blocked_pipe_sync_before_retry_action_request_sync_and_drain_output"
    | "supervisor_blocked_pipe_sync_before_retry_action_schedule_retry"
    | "supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available"
    | "supervisor_blocked_pipe_sync_before_retry_action"
    | "supervisor_blocked_pipe_sync_before_retry_requested"
    | "supervisor_blocked_pipe_sync_before_retry_clear"
    | "supervisor_blocked_status_transition_action_block"
    | "supervisor_blocked_status_transition_action_sync_socket"
    | "supervisor_blocked_status_transition_action_sync_pipe"
    | "supervisor_blocked_status_transition_action_sync_fork_shared"
    | "supervisor_blocked_status_transition_action_sync_pipe_and_fork_shared"
    | "supervisor_blocked_status_transition_action_sync_socket_and_fork_shared"
    | "supervisor_blocked_status_transition_flag_kernel_worker_available"
    | "supervisor_blocked_status_transition_flag_network_connections_available"
    | "supervisor_blocked_status_transition_action"
    | "supervisor_blocked_wait_network_pump_action_skip"
    | "supervisor_blocked_wait_network_pump_action_pump"
    | "supervisor_blocked_wait_network_pump_action"
    | "supervisor_complete_blocked_wait_network_pump"
    | "supervisor_clear_blocked_wait_network_pump"
    | "supervisor_blocked_wait_wake_source_pipe_sync"
    | "supervisor_blocked_wait_wake_source_epoll_sync"
    | "supervisor_blocked_wait_wake_source_socket_sync"
    | "supervisor_blocked_wait_wake_source_pipe_and_epoll_sync"
    | "supervisor_blocked_wait_wake_target_pipe_wait"
    | "supervisor_blocked_wait_wake_target_epoll_wait"
    | "supervisor_blocked_wait_wake_target_socket_recv"
    | "supervisor_blocked_wait_wake_target_socket_send"
    | "supervisor_blocked_wait_wake_mask_for_sync"
    | "supervisor_blocked_wait_status_matches_wake_source"
    | "supervisor_state_sync_kind_pipe"
    | "supervisor_state_sync_kind_epoll"
    | "supervisor_state_sync_kind_socket"
    | "supervisor_state_sync_request_flag_wake_waiters"
    | "supervisor_state_sync_run_flag_run"
    | "supervisor_state_sync_run_flag_wake_waiters"
    | "supervisor_state_sync_request"
    | "supervisor_state_sync_next_run_flags"
    | "supervisor_state_sync_complete"
    | "supervisor_state_sync_clear"
    | "supervisor_process_held_ofd_refcount"
    | "supervisor_process_private_pipe_slot_mask"
    | "supervisor_process_private_pipe_source_mode"
    | "supervisor_process_private_pipe_target_mode"
    | "supervisor_process_held_pipe_slot_mask"
    | "supervisor_process_pipe_used_mask"
    | "supervisor_process_pipe_slot_mode"
    | "supervisor_process_pipe_read_open_count"
    | "supervisor_process_pipe_write_open_count"
    | "supervisor_process_epoll_held_ofd"
    | "supervisor_process_epoll_dependent_ofd"
    | "supervisor_process_sync_dependent_ofd"
    | "supervisor_process_epoll_dependent_sync_ofd"
    | "supervisor_process_epoll_depends_on_ofd"
  >;
  const maxPipes = (): number => kernel.abi_max_pipes?.() ?? 64;
  const fdOfdId = (vmPtr: number, fd: number): number => (
    kernel.process_fd_ofd_id?.(vmPtr, fd) ?? 0
  );
  const processHeldOfdRefcount = (vmPtr: number, ofdId: number): number => {
    let count = 0;
    for (let fd = 0; fd < maxFds(); fd += 1) {
      if (fdOfdId(vmPtr, fd) === ofdId) {
        count += 1;
      }
    }
    return count;
  };
  const processOfdByType = (
    vmPtr: number,
    index: number,
    fdType: number | undefined,
  ): number => {
    if (fdType === undefined) {
      return 0;
    }
    let seen = 0;
    for (let fd = 0; fd < maxFds(); fd += 1) {
      const ofdId = fdOfdId(vmPtr, fd);
      if (
        ofdId > 0
        && (kernel.kernel_ofd_fd_type?.(ofdId) ?? -1) === fdType
      ) {
        if (seen === index) {
          return ofdId;
        }
        seen += 1;
      }
    }
    return 0;
  };
  const pipeSlotForOfd = (ofdId: number): number => {
    if (
      ofdId <= 0
      || (kernel.kernel_ofd_fd_type?.(ofdId) ?? -1) !== kernel.abi_fd_type_pipe?.()
    ) {
      return 0;
    }
    const slot = kernel.kernel_ofd_ino?.(ofdId) ?? 0;
    return slot > 0 && slot < maxPipes() ? slot : 0;
  };
  const processHeldPipeSlotMask = (vmPtr: number): bigint => {
    let mask = 0n;
    for (let fd = 0; fd < maxFds(); fd += 1) {
      const slot = pipeSlotForOfd(fdOfdId(vmPtr, fd));
      if (slot > 0) {
        mask |= 1n << BigInt(slot);
      }
    }
    return mask;
  };
  const pipeOpenCount = (
    vmPtr: number,
    pipeSlot: number,
    wantWriter: boolean,
  ): number => {
    let count = 0;
    for (let fd = 0; fd < maxFds(); fd += 1) {
      const ofdId = fdOfdId(vmPtr, fd);
      if (pipeSlotForOfd(ofdId) !== pipeSlot) {
        continue;
      }
      const hostFd = kernel.kernel_ofd_host_fd?.(ofdId) ?? 0;
      const writer = hostFd < 0;
      if (writer === wantWriter) {
        count += 1;
      }
    }
    return count;
  };
  const pipeSlotMode = (vmPtr: number, pipeSlot: number): number => {
    let mode = 0;
    for (let fd = 0; fd < maxFds(); fd += 1) {
      const ofdId = fdOfdId(vmPtr, fd);
      if (pipeSlotForOfd(ofdId) !== pipeSlot) {
        continue;
      }
      mode |= (kernel.kernel_ofd_host_fd?.(ofdId) ?? 0) < 0 ? 2 : 1;
    }
    return mode;
  };
  nextKernel.supervisor_blocked_wait_active_child_pid ??= () => 0;
  nextKernel.supervisor_process_held_ofd_refcount ??= processHeldOfdRefcount;
  nextKernel.supervisor_process_epoll_held_ofd ??= (vmPtr: number, index: number) =>
    processOfdByType(vmPtr, index, kernel.abi_fd_type_epoll?.());
  nextKernel.supervisor_process_epoll_dependent_ofd ??= () => 0;
  nextKernel.supervisor_process_sync_dependent_ofd ??= () => 0;
  nextKernel.supervisor_process_epoll_dependent_sync_ofd ??= () => 0;
  nextKernel.supervisor_process_epoll_depends_on_ofd ??= () => 0;
  nextKernel.supervisor_process_private_pipe_slot_mask ??= () => 0n;
  nextKernel.supervisor_process_private_pipe_source_mode ??= () => 0;
  nextKernel.supervisor_process_private_pipe_target_mode ??= () => 0;
  nextKernel.supervisor_process_held_pipe_slot_mask ??= processHeldPipeSlotMask;
  nextKernel.supervisor_process_pipe_used_mask ??= (vmPtr: number) => {
    const sharedPipeUsed = BigInt(kernel.kernel_pipe_used?.() ?? 0n);
    return sharedPipeUsed === 0n ? processHeldPipeSlotMask(vmPtr) | 1n : sharedPipeUsed;
  };
  nextKernel.supervisor_process_pipe_slot_mode ??= pipeSlotMode;
  nextKernel.supervisor_process_pipe_read_open_count ??= (vmPtr: number, pipeSlot: number) =>
    pipeOpenCount(vmPtr, pipeSlot, false);
  nextKernel.supervisor_process_pipe_write_open_count ??= (vmPtr: number, pipeSlot: number) =>
    pipeOpenCount(vmPtr, pipeSlot, true);
  nextKernel.supervisor_blocked_epoll_watched_ofd ??= () => 0;
  nextKernel.supervisor_blocked_epoll_dependent_sync_ofd ??= (vmPtr: number, index: number) =>
    nextKernel.supervisor_blocked_epoll_watched_ofd!(vmPtr, index);
  nextKernel.supervisor_blocked_epoll_watches_pipe ??= (vmPtr: number) => {
    for (let index = 0; index < maxFds(); index += 1) {
      const ofdId = nextKernel.supervisor_blocked_epoll_watched_ofd!(vmPtr, index);
      if (ofdId === 0) {
        return 0;
      }
      if ((kernel.kernel_ofd_fd_type?.(ofdId) ?? -1) === kernel.abi_fd_type_pipe?.()) {
        return 1;
      }
    }
    return 0;
  };
  nextKernel.supervisor_blocked_pipe_resume_action_defer ??= () =>
    SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DEFER;
  nextKernel.supervisor_blocked_pipe_resume_action_direct ??= () =>
    SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DIRECT;
  nextKernel.supervisor_blocked_pipe_resume_action ??= () =>
    SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DIRECT;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_action_none ??= () =>
    SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_NONE;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_action_request_sync ??= () =>
    SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_action_request_sync_and_drain_output ??= () =>
    2;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_action_schedule_retry ??= () =>
    3;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available ??= () =>
    SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_FLAG_KERNEL_WORKER_AVAILABLE;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_action ??= (vmPtr: number, flags: number) => {
    const kernelWorkerAvailable =
      (flags & SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_FLAG_KERNEL_WORKER_AVAILABLE) !== 0;
    if (!pipeSyncBeforeRetryRequested.has(vmPtr) && kernelWorkerAvailable) {
      pipeSyncBeforeRetryRequested.add(vmPtr);
      return SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC;
    }
    return SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_NONE;
  };
  nextKernel.supervisor_blocked_pipe_sync_before_retry_requested ??= (vmPtr: number) =>
    pipeSyncBeforeRetryRequested.has(vmPtr) ? 1 : 0;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_clear ??= (vmPtr: number) => {
    pipeSyncBeforeRetryRequested.delete(vmPtr);
    return 0;
  };
  nextKernel.supervisor_blocked_status_transition_action_block ??= () =>
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_BLOCK;
  nextKernel.supervisor_blocked_status_transition_action_sync_socket ??= () =>
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET;
  nextKernel.supervisor_blocked_status_transition_action_sync_pipe ??= () =>
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE;
  nextKernel.supervisor_blocked_status_transition_action_sync_fork_shared ??= () =>
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_FORK_SHARED;
  nextKernel.supervisor_blocked_status_transition_action_sync_pipe_and_fork_shared ??= () =>
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE_AND_FORK_SHARED;
  nextKernel.supervisor_blocked_status_transition_action_sync_socket_and_fork_shared ??= () =>
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET_AND_FORK_SHARED;
  nextKernel.supervisor_blocked_status_transition_flag_kernel_worker_available ??= () =>
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_FLAG_KERNEL_WORKER_AVAILABLE;
  nextKernel.supervisor_blocked_status_transition_flag_network_connections_available ??= () =>
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_FLAG_NETWORK_CONNECTIONS_AVAILABLE;
  nextKernel.supervisor_blocked_status_transition_action ??= (vmPtr: number, status: number, flags: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    const kernelWorkerAvailable =
      (flags & SUPERVISOR_BLOCKED_STATUS_TRANSITION_FLAG_KERNEL_WORKER_AVAILABLE) !== 0;
    if (!kernelWorkerAvailable) {
      return SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_BLOCK;
    }
    const pipeWaitStatus = kernel.abi_status_pipe_wait?.() === status;
    const networkConnectionsAvailable =
      (flags & SUPERVISOR_BLOCKED_STATUS_TRANSITION_FLAG_NETWORK_CONNECTIONS_AVAILABLE) !== 0;
    if (pipeWaitStatus && networkConnectionsAvailable && pollWaitHasSocket(vmPtr)) {
      return SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET;
    }
    if (kernel.abi_status_wait4?.() === status) {
      return SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE;
    }
    const finitePollTimeout = pipeWaitStatus
      && toNumber(nextKernel.supervisor_blocked_poll_wait_timeout_ms(vmPtr)) >= 0;
    const forkSharedStatus = [
      kernel.abi_status_pipe_wait?.(),
      kernel.abi_status_socket_recv?.(),
      kernel.abi_status_socket_send?.(),
      kernel.abi_status_flock_wait?.(),
    ].includes(status);
    if (!forkSharedStatus || finitePollTimeout) {
      return SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_BLOCK;
    }
    if (pipeWaitStatus) {
      return SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE_AND_FORK_SHARED;
    }
    if (kernel.abi_status_socket_recv?.() === status || kernel.abi_status_socket_send?.() === status) {
      return SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET_AND_FORK_SHARED;
    }
    return SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_FORK_SHARED;
  };
  nextKernel.supervisor_blocked_wait_network_pump_action_skip ??= () =>
    SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_SKIP;
  nextKernel.supervisor_blocked_wait_network_pump_action_pump ??= () =>
    SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_PUMP;
  nextKernel.supervisor_blocked_wait_network_pump_action ??= (vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    if (!networkPumpBackoffs.has(vmPtr)) {
      return SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_PUMP;
    }
    networkPumpBackoffs.delete(vmPtr);
    return SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_SKIP;
  };
  nextKernel.supervisor_complete_blocked_wait_network_pump ??= (vmPtr: number, changed: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    if (changed === 0) {
      networkPumpBackoffs.add(vmPtr);
    } else {
      networkPumpBackoffs.delete(vmPtr);
    }
    return 0;
  };
  nextKernel.supervisor_clear_blocked_wait_network_pump ??= (vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    networkPumpBackoffs.delete(vmPtr);
    return 0;
  };
  nextKernel.supervisor_blocked_wait_wake_source_pipe_sync ??= () =>
    SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_SYNC;
  nextKernel.supervisor_blocked_wait_wake_source_epoll_sync ??= () =>
    SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_EPOLL_SYNC;
  nextKernel.supervisor_blocked_wait_wake_source_socket_sync ??= () =>
    SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_SOCKET_SYNC;
  nextKernel.supervisor_blocked_wait_wake_source_pipe_and_epoll_sync ??= () =>
    SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_AND_EPOLL_SYNC;
  nextKernel.supervisor_blocked_wait_wake_target_pipe_wait ??= () =>
    SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_PIPE_WAIT;
  nextKernel.supervisor_blocked_wait_wake_target_epoll_wait ??= () =>
    SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_EPOLL_WAIT;
  nextKernel.supervisor_blocked_wait_wake_target_socket_recv ??= () =>
    SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_SOCKET_RECV;
  nextKernel.supervisor_blocked_wait_wake_target_socket_send ??= () =>
    SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_SOCKET_SEND;
  nextKernel.supervisor_blocked_wait_wake_mask_for_sync ??= (source: number) =>
    wakeMaskForSync(source);
  nextKernel.supervisor_blocked_wait_status_matches_wake_source ??= (status: number, source: number) => {
    const mask = wakeMaskForSync(source);
    if (mask < 0) {
      return mask;
    }
    const target = wakeTargetForStatus(status);
    return target !== 0 && (mask & target) !== 0 ? 1 : 0;
  };
  nextKernel.supervisor_state_sync_kind_pipe ??= () => SUPERVISOR_STATE_SYNC_KIND_PIPE;
  nextKernel.supervisor_state_sync_kind_epoll ??= () => SUPERVISOR_STATE_SYNC_KIND_EPOLL;
  nextKernel.supervisor_state_sync_kind_socket ??= () => SUPERVISOR_STATE_SYNC_KIND_SOCKET;
  nextKernel.supervisor_state_sync_request_flag_wake_waiters ??= () =>
    SUPERVISOR_STATE_SYNC_REQUEST_FLAG_WAKE_WAITERS;
  nextKernel.supervisor_state_sync_run_flag_run ??= () => SUPERVISOR_STATE_SYNC_RUN_FLAG_RUN;
  nextKernel.supervisor_state_sync_run_flag_wake_waiters ??= () =>
    SUPERVISOR_STATE_SYNC_RUN_FLAG_WAKE_WAITERS;
  nextKernel.supervisor_state_sync_request ??= (vmPtr: number, kind: number, flags: number) => {
    const key = `${vmPtr}:${kind}`;
    const request = stateSyncRequests.get(key) ?? { queuedRunFlags: 0, inFlight: false };
    request.queuedRunFlags |= SUPERVISOR_STATE_SYNC_RUN_FLAG_RUN;
    if ((flags & SUPERVISOR_STATE_SYNC_REQUEST_FLAG_WAKE_WAITERS) !== 0) {
      request.queuedRunFlags |= SUPERVISOR_STATE_SYNC_RUN_FLAG_WAKE_WAITERS;
    }
    const shouldStart = !request.inFlight;
    request.inFlight = true;
    stateSyncRequests.set(key, request);
    return shouldStart ? 1 : 0;
  };
  nextKernel.supervisor_state_sync_next_run_flags ??= (vmPtr: number, kind: number) => {
    const key = `${vmPtr}:${kind}`;
    const request = stateSyncRequests.get(key);
    if (!request) {
      return 0;
    }
    const runFlags = request.queuedRunFlags;
    request.queuedRunFlags = 0;
    return runFlags;
  };
  nextKernel.supervisor_state_sync_complete ??= (vmPtr: number, kind: number) => {
    const key = `${vmPtr}:${kind}`;
    const request = stateSyncRequests.get(key);
    if (!request) {
      return 0;
    }
    if (request.queuedRunFlags !== 0) {
      return 1;
    }
    stateSyncRequests.delete(key);
    return 0;
  };
  nextKernel.supervisor_state_sync_clear ??= (vmPtr: number, kind: number) => {
    stateSyncRequests.delete(`${vmPtr}:${kind}`);
    return 0;
  };
  nextKernel.supervisor_blocked_resume_writeback_count ??= (
    vmPtr: number,
    blockedStatus: number,
    resumeResult: bigint | number,
  ) => {
    if (vmPtr === 0) {
      return -14;
    }
    return compactWritebackRanges(vmPtr, blockedStatus, resumeResult).length;
  };
  nextKernel.supervisor_blocked_resume_writeback_ptr ??= (
    vmPtr: number,
    blockedStatus: number,
    resumeResult: bigint | number,
    index: number,
  ) => compactWritebackRanges(vmPtr, blockedStatus, resumeResult)[index]?.guestPtr ?? 0;
  nextKernel.supervisor_blocked_resume_writeback_len ??= (
    vmPtr: number,
    blockedStatus: number,
    resumeResult: bigint | number,
    index: number,
  ) => compactWritebackRanges(vmPtr, blockedStatus, resumeResult)[index]?.length ?? 0;
  nextKernel.supervisor_blocked_status_is_host_wait ??= (status: number) =>
    [
      kernel.abi_status_wait4?.(),
      kernel.abi_status_pipe_wait?.(),
      kernel.abi_status_epoll_wait?.(),
      kernel.abi_status_socket_recv?.(),
      kernel.abi_status_socket_send?.(),
      kernel.abi_status_flock_wait?.(),
      kernel.abi_status_connect?.(),
    ].some((candidate) => candidate !== undefined && Number(candidate) === status) ? 1 : 0;
  nextKernel.supervisor_blocked_status_is_connect_wait ??= (status: number) =>
    kernel.abi_status_connect?.() === status ? 1 : 0;
  nextKernel.supervisor_blocked_status_is_epoll_wait ??= (status: number) =>
    kernel.abi_status_epoll_wait?.() === status ? 1 : 0;
  nextKernel.supervisor_blocked_status_is_pipe_wait ??= (status: number) =>
    kernel.abi_status_pipe_wait?.() === status ? 1 : 0;
  nextKernel.supervisor_blocked_status_is_wait4 ??= (status: number) =>
    kernel.abi_status_wait4?.() === status ? 1 : 0;
  nextKernel.supervisor_blocked_status_is_socket_wait ??= (status: number) =>
    [
      kernel.abi_status_socket_recv?.(),
      kernel.abi_status_socket_send?.(),
    ].some((candidate) => candidate !== undefined && Number(candidate) === status) ? 1 : 0;
  nextKernel.supervisor_blocked_status_needs_network_sidecar_restore ??= (status: number) =>
    [
      kernel.abi_status_connect?.(),
      kernel.abi_status_socket_recv?.(),
      kernel.abi_status_socket_send?.(),
      kernel.abi_status_pipe_wait?.(),
      kernel.abi_status_epoll_wait?.(),
    ].some((candidate) => candidate !== undefined && Number(candidate) === status) ? 1 : 0;
  nextKernel.supervisor_blocked_status_syncs_socket_after_resume ??= (status: number) =>
    [
      kernel.abi_status_connect?.(),
      kernel.abi_status_socket_recv?.(),
      kernel.abi_status_socket_send?.(),
    ].some((candidate) => candidate !== undefined && Number(candidate) === status) ? 1 : 0;
  nextKernel.supervisor_blocked_status_uses_fork_shared_state_sync ??= (status: number) =>
    [
      kernel.abi_status_pipe_wait?.(),
      kernel.abi_status_socket_recv?.(),
      kernel.abi_status_socket_send?.(),
      kernel.abi_status_flock_wait?.(),
    ].some((candidate) => candidate !== undefined && Number(candidate) === status) ? 1 : 0;
  nextKernel.supervisor_process_status_is_lifecycle_handoff ??= (status: number) =>
    [
      kernel.abi_status_vfork_wait?.(),
      kernel.abi_status_execve_spawn?.(),
      kernel.abi_status_clone_spawn?.(),
    ].some((candidate) => candidate !== undefined && Number(candidate) === status) ? 1 : 0;
  nextKernel.supervisor_process_status_is_execve_spawn ??= (status: number) =>
    kernel.abi_status_execve_spawn?.() === status ? 1 : 0;
  nextKernel.supervisor_process_status_is_clone_spawn ??= (status: number) =>
    kernel.abi_status_clone_spawn?.() === status ? 1 : 0;
  nextKernel.supervisor_process_status_is_vfork_wait ??= (status: number) =>
    kernel.abi_status_vfork_wait?.() === status ? 1 : 0;
  nextKernel.supervisor_process_status_allows_pid_mismatch ??= (status: number) =>
    [
      kernel.abi_status_vfork_wait?.(),
      kernel.abi_status_execve_spawn?.(),
    ].some((candidate) => candidate !== undefined && Number(candidate) === status) ? 1 : 0;
  nextKernel.supervisor_process_status_defers_kernel_state_adoption ??= (status: number) =>
    kernel.abi_status_clone_spawn?.() === status ? 1 : 0;
  nextKernel.supervisor_blocked_epoll_wait_timeout_ms ??= (vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    if (kernel.thread_status?.(vmPtr) !== kernel.abi_status_epoll_wait?.()) {
      return -1;
    }
    return toNumber(kernel.thread_epoll_wait_timeout?.(vmPtr));
  };
  nextKernel.supervisor_blocked_poll_wait_timeout_ms ??= (vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    if (kernel.thread_status?.(vmPtr) !== kernel.abi_status_pipe_wait?.()) {
      return -1;
    }
    const syscallNr = blockedSyscallNr(vmPtr);
    if (
      !syscallMatches(syscallNr, kernel.abi_sys_ppoll)
      && !syscallMatches(syscallNr, kernel.abi_sys_pselect6)
    ) {
      return -1;
    }
    const timeout = kernel.thread_poll_wait_timeout?.(vmPtr);
    return timeout === undefined ? -1 : toNumber(timeout);
  };
  const pollWaitHasSocket = (vmPtr: number): boolean =>
    kernel.thread_status?.(vmPtr) === kernel.abi_status_pipe_wait?.()
    && kernel.thread_poll_wait_has_socket?.(vmPtr) === 1;
  const pipeWaitHasReadyEvents = (vmPtr: number): boolean =>
    kernel.thread_status?.(vmPtr) === kernel.abi_status_pipe_wait?.()
    && toNumber(kernel.thread_poll_wait_ready_count?.(vmPtr)) > 0;
  const waitMayDependOnNetwork = (vmPtr: number): boolean => {
    const status = kernel.thread_status?.(vmPtr);
    if (status === kernel.abi_status_pipe_wait?.()) {
      if (toNumber(nextKernel.supervisor_blocked_poll_wait_timeout_ms(vmPtr)) >= 0 && pollWaitHasSocket(vmPtr)) {
        return true;
      }
      if (pipeWaitHasReadyEvents(vmPtr)) {
        return false;
      }
      return false;
    }
    return status === kernel.abi_status_socket_recv?.()
      || status === kernel.abi_status_socket_send?.()
      || status === kernel.abi_status_connect?.();
  };
  nextKernel.supervisor_blocked_poll_wait_has_socket ??= (vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    return pollWaitHasSocket(vmPtr) ? 1 : 0;
  };
  nextKernel.supervisor_blocked_pipe_wait_has_ready_events ??= (vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    return pipeWaitHasReadyEvents(vmPtr) ? 1 : 0;
  };
  nextKernel.supervisor_blocked_wait_may_depend_on_network ??= (vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    return waitMayDependOnNetwork(vmPtr) ? 1 : 0;
  };
  nextKernel.supervisor_blocked_wait_should_pump_network ??= (vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    const status = kernel.thread_status?.(vmPtr);
    if (status === kernel.abi_status_pipe_wait?.()) {
      return waitMayDependOnNetwork(vmPtr) || !pipeWaitHasReadyEvents(vmPtr) ? 1 : 0;
    }
    return status === kernel.abi_status_socket_recv?.()
      || status === kernel.abi_status_socket_send?.()
      || status === kernel.abi_status_connect?.()
      ? 1
      : 0;
  };
  nextKernel.supervisor_blocked_resume_delegation = (vmPtr: number, flags: number) => {
    const kernelWorkerAvailable =
      (flags & SUPERVISOR_BLOCKED_RESUME_FLAG_KERNEL_WORKER_AVAILABLE) !== 0;
    const status = kernel.thread_status?.(vmPtr);
    if (kernelWorkerAvailable) {
      if (status === kernel.abi_status_wait4?.()) {
        return SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER;
      }
      if (
        status === kernel.abi_status_pipe_wait?.()
        || status === kernel.abi_status_connect?.()
        || status === kernel.abi_status_socket_recv?.()
        || status === kernel.abi_status_socket_send?.()
        || status === kernel.abi_status_flock_wait?.()
      ) {
        return SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER;
      }
      return SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED;
    }
    return status === kernel.abi_status_epoll_wait?.()
      ? SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT
      : SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT;
  };
  nextKernel.supervisor_blocked_resume_delegation_flag_kernel_worker_available = () =>
    SUPERVISOR_BLOCKED_RESUME_FLAG_KERNEL_WORKER_AVAILABLE;
  nextKernel.supervisor_blocked_resume_delegation_direct = () =>
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT;
  nextKernel.supervisor_blocked_resume_delegation_kernel_worker = () =>
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER;
  nextKernel.supervisor_blocked_resume_delegation_wait4_kernel_worker = () =>
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER;
  nextKernel.supervisor_blocked_resume_delegation_unsupported = () =>
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED;
  nextKernel.supervisor_blocked_resume_delegation_epoll_direct = () =>
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT;
  nextKernel.supervisor_resume_flag_timeout_expired = () =>
    SUPERVISOR_RESUME_FLAG_TIMEOUT_EXPIRED;
  nextKernel.supervisor_resume_blocked_syscall = (vmPtr: number, flags: number) => {
    const timeoutExpired = (flags & SUPERVISOR_RESUME_FLAG_TIMEOUT_EXPIRED) !== 0;
    const status = kernel.thread_status?.(vmPtr);
    if (status === kernel.abi_status_wait4?.() && kernel.thread_resume_wait4 !== undefined) {
      return kernel.thread_resume_wait4(vmPtr) ?? 0n;
    }
    if (
      timeoutExpired
      && (
        status === kernel.abi_status_connect?.()
        || status === kernel.abi_status_socket_recv?.()
        || status === kernel.abi_status_socket_send?.()
      )
    ) {
      kernel.thread_resume_socket_wait_timeout?.(vmPtr);
      return 0n;
    }
    if (timeoutExpired && status === kernel.abi_status_epoll_wait?.()) {
      kernel.thread_resume_epoll_wait_timeout?.(vmPtr);
      return 0n;
    }
    if (timeoutExpired && status === kernel.abi_status_pipe_wait?.()) {
      kernel.thread_resume_poll_wait_timeout?.(vmPtr);
      return 0n;
    }
    return kernel.thread_resume_blocked_syscall?.(vmPtr) ?? 0n;
  };
  return nextKernel;
}
