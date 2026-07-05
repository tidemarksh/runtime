import type { KernelExports } from "../../../../src/abi.js";

const SUPERVISOR_RESUME_FLAG_TIMEOUT_EXPIRED = 1;
const SUPERVISOR_ABI_VERSION = 1;
const SUPERVISOR_THREAD_STATE_SNAPSHOT_BYTES = 32;
const SUPERVISOR_THREAD_STATE_SNAPSHOT_MAGIC = 0x4854_5354;
const SUPERVISOR_RESUME_RECORD_BYTES = 48;
const SUPERVISOR_RESUME_RECORD_MAGIC = 0x5255_5354;
const SUPERVISOR_RESUME_KIND_FUTEX_WAKE = 1;
const SUPERVISOR_RESUME_KIND_RUNNABLE_THREAD = 2;
const SUPERVISOR_HOST_OP_RECORD_BYTES = 64;
const SUPERVISOR_HOST_OP_RECORD_MAGIC = 0x4f48_5354;
const SUPERVISOR_HOST_OP_KIND_SPAWN_THREAD_WORKER = 1;
const SUPERVISOR_HOST_OP_KIND_CREATE_PROCESS_MEMORY = 3;
const SUPERVISOR_HOST_OP_KIND_COPY_MEMORY_RANGES = 4;
const SUPERVISOR_HOST_OP_KIND_ARM_HOST_TIMER = 12;
const SUPERVISOR_HOST_OP_KIND_RETRY_BLOCKED_WORKER = 15;
const SUPERVISOR_HOST_COMPLETION_RECORD_BYTES = 64;
const SUPERVISOR_HOST_COMPLETION_RECORD_MAGIC = 0x4348_5354;
const SUPERVISOR_HOST_COMPLETION_OUTCOME_SUCCESS = 1;
const SUPERVISOR_HOST_COMPLETION_OUTCOME_FAILURE = 2;
const SUPERVISOR_PROCESS_HOST_OP_RECORD_BYTES = 96;
const SUPERVISOR_PROCESS_HOST_OP_RECORD_MAGIC = 0x4850_5354;
const SUPERVISOR_PROCESS_HOST_OP_KIND_SPAWN_PROCESS_WORKER = 1;
const SUPERVISOR_BLOCKED_RESUME_FLAG_KERNEL_WORKER_AVAILABLE = 1;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT = 0;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER = 1;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER = 2;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED = 3;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT = 4;
const SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DEFER = 0;
const SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DIRECT = 1;
const SUPERVISOR_PIPE_SLOT_MODE_READ = 1;
const SUPERVISOR_PIPE_SLOT_MODE_WRITE = 2;
const SUPERVISOR_PIPE_SLOT_MODE_CONTROL = 4;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_SHARED_STATE = 1;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_THREAD_STEP = 2;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_KERNEL_WORKER_APPLY = 3;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_KERNEL_WORKER_PROCESS = 4;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_KERNEL_WORKER_CANONICAL = 5;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SKIP = 1;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_APPLY_SLOT = 2;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_APPLY_CONTROL = 4;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_HEAD_TO_SOURCE = 8;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_TAIL_TO_SOURCE = 16;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_HEAD_ZERO = 32;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_FULL = 64;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_TAIL_DELTA = 128;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SYNC_GENERATION = 256;
const SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_STOP = 512;
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
const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PASSIVE_WAIT = 4;
const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PROCESS_GATE = 5;
const SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_SUPERVISOR_TIMER_ARMED = 1;
const SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_RPC_PENDING = 2;
const SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_OWNER_ASYNC = 4;
const SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_THREAD_INIT_PENDING = 8;
const SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_FORK_SHARED_SYNC_PENDING = 16;
const SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_RUNNING_WORKER = 32;
const SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_KERNEL_WORKER_AVAILABLE = 64;
const SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_PIPE_STATE_KERNEL_WORKER_OWNS = 128;
const SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_NETWORK_CONNECTIONS_AVAILABLE = 256;
const SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_WAIT_TIMER_ALREADY_CHECKED = 512;
const SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_SUPERVISOR_TIMER = 0;
const SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_RPC_PENDING = 1;
const SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_OWNER_ASYNC = 2;
const SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_THREAD_INIT_PENDING = 3;
const SUPERVISOR_BLOCKED_RETRY_ACTION_ARM_WAIT_TIMER = 4;
const SUPERVISOR_BLOCKED_RETRY_ACTION_PARK_ACTIVE_CHILD = 5;
const SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_WAIT4 = 6;
const SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_FORK_SHARED_SYNC = 7;
const SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_RUNNING_WORKER = 8;
const SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_PROCESS_GATE = 9;
const SUPERVISOR_BLOCKED_RETRY_ACTION_PUMP_NETWORK = 10;
const SUPERVISOR_BLOCKED_RETRY_ACTION_EPOLL_KERNEL_WORKER = 11;
const SUPERVISOR_BLOCKED_RETRY_ACTION_REQUEST_PIPE_SYNC = 12;
const SUPERVISOR_BLOCKED_RETRY_ACTION_REQUEST_PIPE_SYNC_AND_DRAIN_OUTPUT = 13;
const SUPERVISOR_BLOCKED_RETRY_ACTION_SCHEDULE_PIPE_SYNC_RETRY = 14;
const SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_FS = 15;
const SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_PASSIVE_WAIT = 16;
const SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_KERNEL_WORKER = 17;
const SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_WAIT4_KERNEL_WORKER = 18;
const SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_EPOLL_DIRECT = 19;
const SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_DIRECT = 20;
const SUPERVISOR_BLOCKED_RETRY_ACTION_UNSUPPORTED = 21;
const SUPERVISOR_BLOCKED_INITIAL_ACTION_NONE = 0;
const SUPERVISOR_BLOCKED_INITIAL_ACTION_DEFER_SUPERVISOR_TIMER = 1;
const SUPERVISOR_BLOCKED_INITIAL_ACTION_DEFER_RPC_PENDING = 2;
const SUPERVISOR_BLOCKED_INITIAL_ACTION_ARM_WAIT_TIMER = 3;
const SUPERVISOR_BLOCKED_INITIAL_ACTION_RETRY_NOW = 4;
const SUPERVISOR_BLOCKED_INITIAL_ACTION_SCHEDULE_RETRY = 5;
const SUPERVISOR_BLOCKED_RETRY_RESULT_ACTION_NONE = 0;
const SUPERVISOR_BLOCKED_RETRY_RESULT_ACTION_ARM_WAIT_TIMER = 1;
const SUPERVISOR_BLOCKED_RETRY_RESULT_ACTION_SCHEDULE_RETRY = 2;
const SUPERVISOR_BLOCKED_RETRY_RESULT_ACTION_SCHEDULE_RETRY_AND_PUMP = 3;
const SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_NONE = 0;
const SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_PIPE_RETRY_DIAGNOSTICS = 1;
const SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_PUMP_BLOCKED_OUTCOME = 2;
const SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_ACTIVE_CHILD_PIPE = 4;
const SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_ACTIVE_CHILD_WAIT4 = 8;
const SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_SUCCESS_LOG_EPOLL_READY = 16;
const SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_SUCCESS_LOG_WAIT4_RESUMED = 32;
const SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_WAKE_RUNTIME_BRIDGE_WHILE_RUNNING = 64;
const SUPERVISOR_BLOCKED_POST_NETWORK_PUMP_ACTION_SCHEDULE_RETRY = 22;
const SUPERVISOR_BLOCKED_POST_NETWORK_PUMP_ACTION_SCHEDULE_RETRY_WITH_HOST_PROGRESS = 23;
const SUPERVISOR_BLOCKED_STALE_RETRY_RESULT_ACTION_NONE = 0;
const SUPERVISOR_BLOCKED_STALE_RETRY_RESULT_ACTION_HANDLE_CURRENT_RESULT = 1;
const SUPERVISOR_BLOCKED_STALE_RETRY_RESULT_ACTION_HANDLE_CURRENT_RESULT_WAIT4 = 2;
const SUPERVISOR_BLOCKED_RPC_PIPE_SYNC_ACTION_NOT_PIPE = 0;
const SUPERVISOR_BLOCKED_RPC_PIPE_SYNC_ACTION_NONE = 1;
const SUPERVISOR_BLOCKED_RPC_PIPE_SYNC_ACTION_SYNC_SOCKET = 2;
const SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_FLAG_KERNEL_WORKER_AVAILABLE = 1;
const SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_FLAG_PROCESS_OWNER_MISMATCH = 2;
const SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_ACTION_NONE = 0;
const SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_ACTION_FORK_SHARED = 1;
const SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_ACTION_SOCKET = 2;
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
const SUPERVISOR_BLOCKED_WAIT_WAKE_FLAG_RPC_PENDING = 1;
const SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_SKIP = 0;
const SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_RETRY = 1;
const SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_CLEAR_PIPE_SYNC_AND_RETRY = 2;
const SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_LOG_EPOLL_AND_RETRY = 3;
const POLLFD_BYTES = 8;

export function installSupervisorBlockedResumeStub<T extends Partial<KernelExports>>(
  kernel: T,
): T & Pick<
  KernelExports,
  "supervisor_abi_version"
  | "supervisor_thread_state_snapshot_bytes"
  | "supervisor_thread_state_snapshot_magic"
  | "supervisor_resume_record_bytes"
  | "supervisor_resume_record_magic"
  | "supervisor_resume_kind_futex_wake"
  | "supervisor_resume_kind_runnable_thread"
  | "supervisor_host_op_record_bytes"
  | "supervisor_host_op_record_magic"
  | "supervisor_host_op_kind_spawn_thread_worker"
  | "supervisor_host_op_kind_create_process_memory"
  | "supervisor_host_op_kind_copy_memory_ranges"
  | "supervisor_host_op_kind_arm_host_timer"
  | "supervisor_host_op_kind_retry_blocked_worker"
  | "supervisor_host_completion_record_bytes"
  | "supervisor_host_completion_record_magic"
  | "supervisor_host_completion_outcome_success"
  | "supervisor_host_completion_outcome_failure"
  | "supervisor_process_host_op_record_bytes"
  | "supervisor_process_host_op_record_magic"
  | "supervisor_process_host_op_kind_spawn_process_worker"
  | "supervisor_resume_blocked_syscall"
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
  | "supervisor_vfork_child_owner_mismatch"
  | "supervisor_process_owner_matches"
  | "supervisor_process_owner_mismatch"
  | "supervisor_process_owner_absent"
  | "supervisor_process_owner_live_matches"
  | "supervisor_process_owner_missing_or_invalid"
  | "supervisor_process_identity_matches"
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
  | "supervisor_blocked_retry_action"
  | "supervisor_blocked_retry_action_flag_supervisor_timer_armed"
  | "supervisor_blocked_retry_action_flag_rpc_pending"
  | "supervisor_blocked_retry_action_flag_owner_async"
  | "supervisor_blocked_retry_action_flag_thread_init_pending"
  | "supervisor_blocked_retry_action_flag_fork_shared_sync_pending"
  | "supervisor_blocked_retry_action_flag_running_worker"
  | "supervisor_blocked_retry_action_flag_kernel_worker_available"
  | "supervisor_blocked_retry_action_flag_pipe_state_kernel_worker_owns"
  | "supervisor_blocked_retry_action_flag_network_connections_available"
  | "supervisor_blocked_retry_action_flag_wait_timer_already_checked"
  | "supervisor_blocked_retry_action_defer_supervisor_timer"
  | "supervisor_blocked_retry_action_defer_rpc_pending"
  | "supervisor_blocked_retry_action_defer_owner_async"
  | "supervisor_blocked_retry_action_defer_thread_init_pending"
  | "supervisor_blocked_retry_action_arm_wait_timer"
  | "supervisor_blocked_retry_action_park_active_child"
  | "supervisor_blocked_retry_action_resume_wait4"
  | "supervisor_blocked_retry_action_defer_fork_shared_sync"
  | "supervisor_blocked_retry_action_defer_running_worker"
  | "supervisor_blocked_retry_action_defer_process_gate"
  | "supervisor_blocked_retry_action_pump_network"
  | "supervisor_blocked_retry_action_epoll_kernel_worker"
  | "supervisor_blocked_retry_action_request_pipe_sync"
  | "supervisor_blocked_retry_action_request_pipe_sync_and_drain_output"
  | "supervisor_blocked_retry_action_schedule_pipe_sync_retry"
  | "supervisor_blocked_retry_action_resume_fs"
  | "supervisor_blocked_retry_action_defer_passive_wait"
  | "supervisor_blocked_retry_action_resume_kernel_worker"
  | "supervisor_blocked_retry_action_resume_wait4_kernel_worker"
  | "supervisor_blocked_retry_action_resume_epoll_direct"
  | "supervisor_blocked_retry_action_resume_direct"
  | "supervisor_blocked_retry_action_unsupported"
  | "supervisor_blocked_initial_action"
  | "supervisor_blocked_initial_action_none"
  | "supervisor_blocked_initial_action_defer_supervisor_timer"
  | "supervisor_blocked_initial_action_defer_rpc_pending"
  | "supervisor_blocked_initial_action_arm_wait_timer"
  | "supervisor_blocked_initial_action_retry_now"
  | "supervisor_blocked_initial_action_schedule_retry"
  | "supervisor_blocked_retry_result_action"
  | "supervisor_blocked_retry_result_action_none"
  | "supervisor_blocked_retry_result_action_arm_wait_timer"
  | "supervisor_blocked_retry_result_action_schedule_retry"
  | "supervisor_blocked_retry_result_action_schedule_retry_and_pump"
  | "supervisor_blocked_retry_status_action"
  | "supervisor_blocked_retry_status_action_none"
  | "supervisor_blocked_retry_status_action_pipe_retry_diagnostics"
  | "supervisor_blocked_retry_status_action_pump_blocked_outcome"
  | "supervisor_blocked_retry_status_action_active_child_pipe"
  | "supervisor_blocked_retry_status_action_active_child_wait4"
  | "supervisor_blocked_retry_status_action_success_log_epoll_ready"
  | "supervisor_blocked_retry_status_action_success_log_wait4_resumed"
  | "supervisor_blocked_retry_status_action_wake_runtime_bridge_while_running"
  | "supervisor_blocked_post_network_pump_action"
  | "supervisor_blocked_post_network_pump_action_schedule_retry"
  | "supervisor_blocked_post_network_pump_action_schedule_retry_with_host_progress"
  | "supervisor_blocked_stale_retry_result_action"
  | "supervisor_blocked_stale_retry_result_action_none"
  | "supervisor_blocked_stale_retry_result_action_handle_current_result"
  | "supervisor_blocked_stale_retry_result_action_handle_current_result_wait4"
  | "supervisor_blocked_rpc_pipe_sync_action"
  | "supervisor_blocked_rpc_pipe_sync_action_not_pipe"
  | "supervisor_blocked_rpc_pipe_sync_action_none"
  | "supervisor_blocked_rpc_pipe_sync_action_sync_socket"
  | "supervisor_blocked_rpc_preserve_canonical_eof"
  | "supervisor_blocked_rpc_post_resume_sync_action"
  | "supervisor_blocked_rpc_post_resume_sync_flag_kernel_worker_available"
  | "supervisor_blocked_rpc_post_resume_sync_flag_process_owner_mismatch"
  | "supervisor_blocked_rpc_post_resume_sync_action_none"
  | "supervisor_blocked_rpc_post_resume_sync_action_fork_shared"
  | "supervisor_blocked_rpc_post_resume_sync_action_socket"
  | "supervisor_blocked_wait_wake_source_pipe_sync"
  | "supervisor_blocked_wait_wake_source_epoll_sync"
  | "supervisor_blocked_wait_wake_source_socket_sync"
  | "supervisor_blocked_wait_wake_source_pipe_and_epoll_sync"
  | "supervisor_blocked_wait_wake_target_pipe_wait"
  | "supervisor_blocked_wait_wake_target_epoll_wait"
  | "supervisor_blocked_wait_wake_target_socket_recv"
  | "supervisor_blocked_wait_wake_target_socket_send"
  | "supervisor_blocked_wait_wake_flag_rpc_pending"
  | "supervisor_blocked_wait_wake_action_skip"
  | "supervisor_blocked_wait_wake_action_retry"
  | "supervisor_blocked_wait_wake_action_clear_pipe_sync_and_retry"
  | "supervisor_blocked_wait_wake_action_log_epoll_and_retry"
  | "supervisor_blocked_wait_wake_mask_for_sync"
  | "supervisor_blocked_wait_status_matches_wake_source"
  | "supervisor_blocked_wait_wake_action"
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
  | "supervisor_process_ofd_for_fd"
  | "supervisor_process_fd_flags_for_fd"
  | "supervisor_process_private_pipe_slot_mask"
  | "supervisor_process_private_pipe_source_mode"
  | "supervisor_process_private_pipe_target_mode"
  | "supervisor_process_held_pipe_slot_mask"
  | "supervisor_process_pipe_used_mask"
  | "supervisor_process_pipe_slot_mode"
  | "supervisor_process_pipe_read_open_count"
  | "supervisor_process_pipe_write_open_count"
  | "supervisor_pipe_reconcile_control"
  | "supervisor_pipe_slot_mode_read"
  | "supervisor_pipe_slot_mode_write"
  | "supervisor_pipe_slot_mode_control"
  | "supervisor_pipe_slot_snapshot_context_shared_state"
  | "supervisor_pipe_slot_snapshot_context_thread_step"
  | "supervisor_pipe_slot_snapshot_context_kernel_worker_apply"
  | "supervisor_pipe_slot_snapshot_context_kernel_worker_process"
  | "supervisor_pipe_slot_snapshot_context_kernel_worker_canonical"
  | "supervisor_pipe_slot_snapshot_action_skip"
  | "supervisor_pipe_slot_snapshot_action_apply_slot"
  | "supervisor_pipe_slot_snapshot_action_apply_control"
  | "supervisor_pipe_slot_snapshot_action_set_head_to_source"
  | "supervisor_pipe_slot_snapshot_action_set_tail_to_source"
  | "supervisor_pipe_slot_snapshot_action_set_head_zero"
  | "supervisor_pipe_slot_snapshot_action_copy_full"
  | "supervisor_pipe_slot_snapshot_action_copy_tail_delta"
  | "supervisor_pipe_slot_snapshot_action_sync_generation"
  | "supervisor_pipe_slot_snapshot_action_stop"
  | "supervisor_pipe_slot_snapshot_merge_action"
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
  const blockedWaitTimerRequired = (vmPtr: number): boolean => {
    const explicit = kernel.supervisor_blocked_wait_timer_required?.(vmPtr);
    if (explicit !== undefined) {
      return toNumber(explicit) !== 0;
    }
    const status = toNumber(kernel.thread_status?.(vmPtr));
    if (status === kernel.abi_status_epoll_wait?.()) {
      const timeout = kernel.thread_epoll_wait_timeout?.(vmPtr);
      if (timeout !== undefined && BigInt(timeout) >= 0n) {
        return true;
      }
      return BigInt(kernel.thread_epoll_timerfd_deadline_ticks?.(vmPtr) ?? 0n) !== 0n;
    }
    if (status === kernel.abi_status_pipe_wait?.()) {
      const timeout = kernel.thread_poll_wait_timeout?.(vmPtr);
      return timeout !== undefined
        && BigInt(timeout) >= 0n
        && toNumber(kernel.thread_poll_wait_ready_count?.(vmPtr)) <= 0;
    }
    if (
      status === kernel.abi_status_connect?.()
      || status === kernel.abi_status_socket_recv?.()
      || status === kernel.abi_status_socket_send?.()
    ) {
      const timeout = kernel.thread_socket_wait_timeout?.(vmPtr);
      return timeout !== undefined && BigInt(timeout) >= 0n;
    }
    return false;
  };
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
  const wakeActionForStatus = (status: number, source: number, flags: number): number => {
    const mask = wakeMaskForSync(source);
    if (mask < 0) {
      return mask;
    }
    const target = wakeTargetForStatus(status);
    if (target === 0 || (mask & target) === 0) {
      return SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_SKIP;
    }
    const rpcPending = (flags & SUPERVISOR_BLOCKED_WAIT_WAKE_FLAG_RPC_PENDING) !== 0;
    if (kernel.abi_status_pipe_wait?.() === status) {
      return rpcPending
        ? SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_SKIP
        : SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_CLEAR_PIPE_SYNC_AND_RETRY;
    }
    if (kernel.abi_status_epoll_wait?.() === status) {
      return rpcPending
        ? SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_SKIP
        : SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_LOG_EPOLL_AND_RETRY;
    }
    return SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_RETRY;
  };
  const retryStatusAction = (status: number): number => {
    if (kernel.abi_status_pipe_wait?.() === status) {
      return SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_PIPE_RETRY_DIAGNOSTICS
        | SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_PUMP_BLOCKED_OUTCOME
        | SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_ACTIVE_CHILD_PIPE;
    }
    if (kernel.abi_status_epoll_wait?.() === status) {
      return SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_SUCCESS_LOG_EPOLL_READY
        | SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_WAKE_RUNTIME_BRIDGE_WHILE_RUNNING;
    }
    if (kernel.abi_status_wait4?.() === status) {
      return SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_ACTIVE_CHILD_WAIT4
        | SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_SUCCESS_LOG_WAIT4_RESUMED;
    }
    return SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_NONE;
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
    | "supervisor_blocked_rpc_pipe_sync_action"
    | "supervisor_blocked_rpc_pipe_sync_action_not_pipe"
    | "supervisor_blocked_rpc_pipe_sync_action_none"
    | "supervisor_blocked_rpc_pipe_sync_action_sync_socket"
    | "supervisor_blocked_rpc_preserve_canonical_eof"
    | "supervisor_blocked_rpc_post_resume_sync_action"
    | "supervisor_blocked_rpc_post_resume_sync_flag_kernel_worker_available"
    | "supervisor_blocked_rpc_post_resume_sync_flag_process_owner_mismatch"
    | "supervisor_blocked_rpc_post_resume_sync_action_none"
    | "supervisor_blocked_rpc_post_resume_sync_action_fork_shared"
    | "supervisor_blocked_rpc_post_resume_sync_action_socket"
    | "supervisor_blocked_retry_action"
    | "supervisor_blocked_retry_action_flag_supervisor_timer_armed"
    | "supervisor_blocked_retry_action_flag_rpc_pending"
    | "supervisor_blocked_retry_action_flag_owner_async"
    | "supervisor_blocked_retry_action_flag_thread_init_pending"
    | "supervisor_blocked_retry_action_flag_fork_shared_sync_pending"
    | "supervisor_blocked_retry_action_flag_running_worker"
    | "supervisor_blocked_retry_action_flag_kernel_worker_available"
    | "supervisor_blocked_retry_action_flag_pipe_state_kernel_worker_owns"
    | "supervisor_blocked_retry_action_flag_network_connections_available"
    | "supervisor_blocked_retry_action_flag_wait_timer_already_checked"
    | "supervisor_blocked_retry_action_defer_supervisor_timer"
    | "supervisor_blocked_retry_action_defer_rpc_pending"
    | "supervisor_blocked_retry_action_defer_owner_async"
    | "supervisor_blocked_retry_action_defer_thread_init_pending"
    | "supervisor_blocked_retry_action_arm_wait_timer"
    | "supervisor_blocked_retry_action_park_active_child"
    | "supervisor_blocked_retry_action_resume_wait4"
    | "supervisor_blocked_retry_action_defer_fork_shared_sync"
    | "supervisor_blocked_retry_action_defer_running_worker"
    | "supervisor_blocked_retry_action_defer_process_gate"
    | "supervisor_blocked_retry_action_pump_network"
    | "supervisor_blocked_retry_action_epoll_kernel_worker"
    | "supervisor_blocked_retry_action_request_pipe_sync"
    | "supervisor_blocked_retry_action_request_pipe_sync_and_drain_output"
    | "supervisor_blocked_retry_action_schedule_pipe_sync_retry"
    | "supervisor_blocked_retry_action_resume_fs"
    | "supervisor_blocked_retry_action_defer_passive_wait"
    | "supervisor_blocked_retry_action_resume_kernel_worker"
    | "supervisor_blocked_retry_action_resume_wait4_kernel_worker"
    | "supervisor_blocked_retry_action_resume_epoll_direct"
    | "supervisor_blocked_retry_action_resume_direct"
    | "supervisor_blocked_retry_action_unsupported"
    | "supervisor_blocked_retry_status_action"
    | "supervisor_blocked_retry_status_action_none"
    | "supervisor_blocked_retry_status_action_pipe_retry_diagnostics"
    | "supervisor_blocked_retry_status_action_pump_blocked_outcome"
    | "supervisor_blocked_retry_status_action_active_child_pipe"
    | "supervisor_blocked_retry_status_action_active_child_wait4"
    | "supervisor_blocked_retry_status_action_success_log_epoll_ready"
    | "supervisor_blocked_retry_status_action_success_log_wait4_resumed"
    | "supervisor_blocked_retry_status_action_wake_runtime_bridge_while_running"
    | "supervisor_blocked_wait_wake_source_pipe_sync"
    | "supervisor_blocked_wait_wake_source_epoll_sync"
    | "supervisor_blocked_wait_wake_source_socket_sync"
    | "supervisor_blocked_wait_wake_source_pipe_and_epoll_sync"
    | "supervisor_blocked_wait_wake_target_pipe_wait"
    | "supervisor_blocked_wait_wake_target_epoll_wait"
    | "supervisor_blocked_wait_wake_target_socket_recv"
    | "supervisor_blocked_wait_wake_target_socket_send"
    | "supervisor_blocked_wait_wake_flag_rpc_pending"
    | "supervisor_blocked_wait_wake_action_skip"
    | "supervisor_blocked_wait_wake_action_retry"
    | "supervisor_blocked_wait_wake_action_clear_pipe_sync_and_retry"
    | "supervisor_blocked_wait_wake_action_log_epoll_and_retry"
    | "supervisor_blocked_wait_wake_mask_for_sync"
    | "supervisor_blocked_wait_status_matches_wake_source"
    | "supervisor_blocked_wait_wake_action"
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
    | "supervisor_process_ofd_for_fd"
    | "supervisor_process_fd_flags_for_fd"
    | "supervisor_process_private_pipe_slot_mask"
    | "supervisor_process_private_pipe_source_mode"
    | "supervisor_process_private_pipe_target_mode"
    | "supervisor_process_held_pipe_slot_mask"
    | "supervisor_process_pipe_used_mask"
    | "supervisor_process_pipe_slot_mode"
    | "supervisor_process_pipe_read_open_count"
    | "supervisor_process_pipe_write_open_count"
    | "supervisor_pipe_reconcile_control"
    | "supervisor_process_epoll_held_ofd"
    | "supervisor_process_epoll_dependent_ofd"
    | "supervisor_process_sync_dependent_ofd"
    | "supervisor_process_epoll_dependent_sync_ofd"
    | "supervisor_process_epoll_depends_on_ofd"
  >;
  nextKernel.supervisor_abi_version ??= () => SUPERVISOR_ABI_VERSION;
  nextKernel.supervisor_thread_state_snapshot_bytes ??= () =>
    SUPERVISOR_THREAD_STATE_SNAPSHOT_BYTES;
  nextKernel.supervisor_thread_state_snapshot_magic ??= () =>
    SUPERVISOR_THREAD_STATE_SNAPSHOT_MAGIC;
  nextKernel.supervisor_resume_record_bytes ??= () => SUPERVISOR_RESUME_RECORD_BYTES;
  nextKernel.supervisor_resume_record_magic ??= () => SUPERVISOR_RESUME_RECORD_MAGIC;
  nextKernel.supervisor_resume_kind_futex_wake ??= () => SUPERVISOR_RESUME_KIND_FUTEX_WAKE;
  nextKernel.supervisor_resume_kind_runnable_thread ??= () =>
    SUPERVISOR_RESUME_KIND_RUNNABLE_THREAD;
  nextKernel.supervisor_host_op_record_bytes ??= () => SUPERVISOR_HOST_OP_RECORD_BYTES;
  nextKernel.supervisor_host_op_record_magic ??= () => SUPERVISOR_HOST_OP_RECORD_MAGIC;
  nextKernel.supervisor_host_op_kind_spawn_thread_worker ??= () =>
    SUPERVISOR_HOST_OP_KIND_SPAWN_THREAD_WORKER;
  nextKernel.supervisor_host_op_kind_create_process_memory ??= () =>
    SUPERVISOR_HOST_OP_KIND_CREATE_PROCESS_MEMORY;
  nextKernel.supervisor_host_op_kind_copy_memory_ranges ??= () =>
    SUPERVISOR_HOST_OP_KIND_COPY_MEMORY_RANGES;
  nextKernel.supervisor_host_op_kind_arm_host_timer ??= () =>
    SUPERVISOR_HOST_OP_KIND_ARM_HOST_TIMER;
  nextKernel.supervisor_host_op_kind_retry_blocked_worker ??= () =>
    SUPERVISOR_HOST_OP_KIND_RETRY_BLOCKED_WORKER;
  nextKernel.supervisor_host_completion_record_bytes ??= () =>
    SUPERVISOR_HOST_COMPLETION_RECORD_BYTES;
  nextKernel.supervisor_host_completion_record_magic ??= () =>
    SUPERVISOR_HOST_COMPLETION_RECORD_MAGIC;
  nextKernel.supervisor_host_completion_outcome_success ??= () =>
    SUPERVISOR_HOST_COMPLETION_OUTCOME_SUCCESS;
  nextKernel.supervisor_host_completion_outcome_failure ??= () =>
    SUPERVISOR_HOST_COMPLETION_OUTCOME_FAILURE;
  nextKernel.supervisor_process_host_op_record_bytes ??= () =>
    SUPERVISOR_PROCESS_HOST_OP_RECORD_BYTES;
  nextKernel.supervisor_process_host_op_record_magic ??= () =>
    SUPERVISOR_PROCESS_HOST_OP_RECORD_MAGIC;
  nextKernel.supervisor_process_host_op_kind_spawn_process_worker ??= () =>
    SUPERVISOR_PROCESS_HOST_OP_KIND_SPAWN_PROCESS_WORKER;
  const maxPipes = (): number => kernel.abi_max_pipes?.() ?? 64;
  const fdOfdId = (vmPtr: number, fd: number): number => (
    kernel.process_fd_ofd_id?.(vmPtr, fd) ?? 0
  );
  const supervisorProcessOfdForFd = (vmPtr: number, fd: number): number => {
    const ofdId = fdOfdId(vmPtr, fd);
    return ofdId > 0 && ofdId < maxFds() ? ofdId : 0;
  };
  const supervisorProcessFdFlagsForFd = (vmPtr: number, fd: number): number => (
    fd >= 0 && fd < maxFds() ? kernel.process_fd_flags?.(vmPtr, fd) ?? 0 : 0
  );
  const supervisorOfdFdType = (ofdId: number): number => (
    ofdId > 0 && ofdId < maxFds() ? kernel.kernel_ofd_fd_type?.(ofdId) ?? 0 : 0
  );
  const supervisorOfdHostFd = (ofdId: number): number => (
    ofdId > 0 && ofdId < maxFds() ? kernel.kernel_ofd_host_fd?.(ofdId) ?? -1 : -1
  );
  const supervisorOfdStatusFlags = (ofdId: number): number => (
    ofdId > 0 && ofdId < maxFds() ? kernel.kernel_ofd_status_flags?.(ofdId) ?? 0 : 0
  );
  const supervisorOfdIno = (ofdId: number): number => (
    ofdId > 0 && ofdId < maxFds() ? kernel.kernel_ofd_ino?.(ofdId) ?? 0 : 0
  );
  const supervisorOfdRefcount = (ofdId: number): number => (
    ofdId > 0 && ofdId < maxFds() ? kernel.kernel_ofd_refcount?.(ofdId) ?? 0 : 0
  );
  const supervisorOfdReconcileRefcount = (ofdId: number, expectedRefcount: number): number => {
    if (ofdId <= 0 || ofdId >= maxFds()) {
      return 0;
    }
    let currentRefcount = kernel.kernel_ofd_refcount?.(ofdId) ?? 0;
    const expected = Math.trunc(expectedRefcount);
    if (expected < 0) {
      return currentRefcount;
    }
    while (currentRefcount < expected) {
      kernel.kernel_retain_ofd?.(ofdId);
      currentRefcount += 1;
    }
    while (currentRefcount > expected) {
      kernel.kernel_release_ofd?.(ofdId);
      currentRefcount -= 1;
    }
    return kernel.kernel_ofd_refcount?.(ofdId) ?? currentRefcount;
  };
  const supervisorOfdOffset = (ofdId: number): bigint => {
    if (ofdId <= 0 || ofdId >= maxFds()) {
      return 0n;
    }
    const offset = kernel.kernel_ofd_offset?.(ofdId) ?? 0n;
    return typeof offset === "bigint" ? offset : BigInt(offset);
  };
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
  const supervisorPipeReconcileControl = (
    pipeSlot: number,
    readOpen: number,
    writeOpen: number,
  ): number => {
    const maxPipeCount = maxPipes();
    if (pipeSlot <= 0 || pipeSlot >= maxPipeCount || readOpen < 0 || writeOpen < 0) {
      return 0;
    }
    const pipeRegionBytes = kernel.kernel_pipe_region_bytes?.() ?? 0;
    const slotBytes = pipeRegionBytes / maxPipeCount;
    if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
      return 0;
    }
    const slotOffset = (kernel.kernel_pipe_base?.() ?? 0) + pipeSlot * slotBytes;
    const view = new DataView(kernel.memory.buffer);
    view.setInt32(slotOffset + 8, Math.trunc(readOpen), true);
    view.setInt32(slotOffset + 12, Math.trunc(writeOpen), true);
    return 1;
  };
  const pipeCounterAhead = (sourceValue: number, targetValue: number): boolean =>
    sourceValue > targetValue
    || (
      sourceValue < targetValue
      && targetValue > 65_536 / 2
      && sourceValue < 65_536 / 2
    );
  const pipeGenerationAhead = (sourceValue: number, targetValue: number): boolean =>
    targetValue === 0
    || sourceValue > targetValue
    || (
      sourceValue < targetValue
      && targetValue > 0x80000000
      && sourceValue < 0x80000000
    );
  const supervisorPipeSlotSnapshotMergeAction = (
    context: number,
    mode: number,
    freshFlag: number,
    sourceHead: number,
    sourceTail: number,
    sourceReadOpen: number,
    sourceWriteOpen: number,
    sourceGeneration: number,
    targetHeadInput: number,
    targetTailInput: number,
    targetReadOpen: number,
    targetWriteOpen: number,
    targetGeneration: number,
  ): number => {
    const readMode = (mode & SUPERVISOR_PIPE_SLOT_MODE_READ) !== 0;
    const writeMode = (mode & SUPERVISOR_PIPE_SLOT_MODE_WRITE) !== 0;
    const controlMode = (mode & SUPERVISOR_PIPE_SLOT_MODE_CONTROL) !== 0;
    const fresh = freshFlag !== 0;
    const sharedState = context === SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_SHARED_STATE;
    const threadStep = context === SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_THREAD_STEP;
    const kernelWorkerApply = context === SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_KERNEL_WORKER_APPLY;
    const kernelWorkerProcess = context === SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_KERNEL_WORKER_PROCESS;
    const kernelWorkerCanonical = context === SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_KERNEL_WORKER_CANONICAL;
    const syncGenerationWithoutApply = sharedState || kernelWorkerProcess || kernelWorkerCanonical;
    let targetHead = targetHeadInput;
    let targetTail = targetTailInput;
    if (sourceGeneration !== 0 && sourceGeneration !== targetGeneration) {
      if (
        sourceHead === 0
        && sourceTail === 0
        && !pipeGenerationAhead(sourceGeneration, targetGeneration)
      ) {
        if (kernelWorkerCanonical) {
          if (targetReadOpen > 0 && targetWriteOpen > 0 && targetHead !== targetTail) {
            return SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SKIP
              | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_STOP;
          }
        } else if (targetHead !== targetTail && !fresh) {
          return SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SKIP
            | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_STOP;
        }
      }
      let action = SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_STOP;
      if (
        fresh
        || pipeGenerationAhead(sourceGeneration, targetGeneration)
        || (targetReadOpen <= 0 && targetWriteOpen <= 0)
      ) {
        action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_APPLY_SLOT
          | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SYNC_GENERATION;
      } else if (syncGenerationWithoutApply) {
        action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SYNC_GENERATION;
      }
      return action;
    }
    if (
      (
        fresh
        || ((kernelWorkerProcess || kernelWorkerCanonical) && targetReadOpen <= 0 && targetWriteOpen <= 0)
      )
      && sourceHead === 0
      && sourceTail === 0
      && targetHead === targetTail
      && sourceReadOpen > 0
      && sourceWriteOpen > 0
      && readMode
      && writeMode
    ) {
      return SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_APPLY_SLOT
        | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SYNC_GENERATION
        | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_STOP;
    }
    if (
      kernelWorkerProcess
      && sourceHead === 0
      && sourceTail === 0
      && (targetHead !== 0 || targetTail !== 0)
    ) {
      let action = SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SYNC_GENERATION
        | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_STOP;
      if (targetReadOpen <= 0 && targetWriteOpen <= 0) {
        action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_APPLY_SLOT;
      }
      return action;
    }
    if (
      kernelWorkerApply
      && sourceHead === 0
      && sourceTail === 0
      && (targetHead !== 0 || targetTail !== 0)
      && targetReadOpen <= 0
      && targetWriteOpen <= 0
    ) {
      return SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_APPLY_SLOT
        | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SYNC_GENERATION
        | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_STOP;
    }
    if (
      (threadStep || kernelWorkerApply)
      && targetReadOpen <= 0
      && targetWriteOpen <= 0
      && targetHead === targetTail
      && (sourceReadOpen > 0 || sourceWriteOpen > 0)
      && (writeMode || controlMode)
    ) {
      return SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_APPLY_SLOT
        | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SYNC_GENERATION
        | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_STOP;
    }
    let action = 0;
    if (controlMode && (sharedState || threadStep || kernelWorkerApply)) {
      action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_APPLY_CONTROL;
    }
    if (
      readMode
      && (!writeMode || kernelWorkerProcess)
      && sourceHead !== 0
      && sourceHead === sourceTail
      && targetTail !== sourceTail
      && pipeCounterAhead(sourceTail, targetTail)
    ) {
      action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_HEAD_TO_SOURCE
        | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_TAIL_TO_SOURCE;
      targetHead = sourceHead;
      targetTail = sourceTail;
    }
    if (
      writeMode
      && !readMode
      && sourceHead === 0
      && sourceTail !== 0
      && targetHead !== 0
      && (targetHead === targetTail || targetTail === 0)
      && pipeCounterAhead(sourceTail, targetTail)
    ) {
      action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_HEAD_ZERO;
      targetHead = 0;
    }
    if (sharedState) {
      let readHeadAdvanced = false;
      if (readMode) {
        if (pipeCounterAhead(sourceHead, targetHead) && (sourceHead !== sourceTail || targetTail === sourceTail)) {
          action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_HEAD_TO_SOURCE;
          targetHead = sourceHead;
          readHeadAdvanced = true;
        }
        if (pipeCounterAhead(sourceTail, targetTail) || (readHeadAdvanced && sourceHead !== sourceTail)) {
          action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_TAIL_TO_SOURCE
            | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_FULL;
          targetTail = sourceTail;
        } else if (
          sourceTail === targetTail
          && sourceHead !== sourceTail
          && !(readMode && pipeCounterAhead(targetHead, sourceHead))
        ) {
          action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_FULL;
        }
      }
      if (writeMode) {
        if (pipeCounterAhead(sourceTail, targetTail)) {
          action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_TAIL_TO_SOURCE
            | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_FULL;
        } else if (
          sourceTail === targetTail
          && sourceHead !== sourceTail
          && !(readMode && pipeCounterAhead(targetHead, sourceHead))
        ) {
          action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_FULL;
        }
      }
      return action | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SYNC_GENERATION;
    }
    if (kernelWorkerProcess) {
      if (
        readMode
        && pipeCounterAhead(sourceHead, targetHead)
        && (sourceHead !== sourceTail || targetTail === sourceTail)
      ) {
        action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_HEAD_TO_SOURCE;
      }
      if (writeMode && pipeCounterAhead(sourceTail, targetTail)) {
        action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_TAIL_TO_SOURCE
          | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_FULL;
      } else if (writeMode && sourceTail === targetTail && sourceHead !== sourceTail) {
        action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_FULL;
      }
      return action | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SYNC_GENERATION;
    }
    if (kernelWorkerCanonical) {
      if (readMode) {
        if (pipeCounterAhead(sourceHead, targetHead) && (sourceHead !== sourceTail || targetTail === sourceTail)) {
          action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_HEAD_TO_SOURCE;
          targetHead = sourceHead;
        }
        if (pipeCounterAhead(sourceTail, targetTail)) {
          action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_TAIL_TO_SOURCE
            | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_FULL;
        }
      }
      if (writeMode) {
        let repairedHead = false;
        if (pipeCounterAhead(sourceHead, targetHead) && (sourceHead !== sourceTail || targetTail === sourceTail)) {
          action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_HEAD_TO_SOURCE;
          repairedHead = true;
        }
        if (pipeCounterAhead(sourceTail, targetTail) || (repairedHead && sourceHead !== sourceTail)) {
          action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_TAIL_TO_SOURCE
            | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_FULL;
        } else if (sourceTail === targetTail && sourceHead !== sourceTail) {
          action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_FULL;
        }
      }
      return action | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SYNC_GENERATION;
    }
    if (readMode && pipeCounterAhead(sourceHead, targetHead) && (sourceHead !== sourceTail || targetTail === sourceTail)) {
      action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_HEAD_TO_SOURCE;
    }
    if (writeMode && pipeCounterAhead(sourceTail, targetTail)) {
      action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_TAIL_TO_SOURCE
        | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_TAIL_DELTA;
    } else if (
      writeMode
      && sourceTail === targetTail
      && sourceHead !== sourceTail
      && !(readMode && pipeCounterAhead(targetHead, sourceHead))
    ) {
      action |= SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_FULL;
    }
    return action | SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SYNC_GENERATION;
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
  nextKernel.supervisor_process_ofd_for_fd ??= supervisorProcessOfdForFd;
  nextKernel.supervisor_process_fd_flags_for_fd ??= supervisorProcessFdFlagsForFd;
  nextKernel.supervisor_ofd_fd_type ??= supervisorOfdFdType;
  nextKernel.supervisor_ofd_host_fd ??= supervisorOfdHostFd;
  nextKernel.supervisor_ofd_status_flags ??= supervisorOfdStatusFlags;
  nextKernel.supervisor_ofd_ino ??= supervisorOfdIno;
  nextKernel.supervisor_ofd_refcount ??= supervisorOfdRefcount;
  nextKernel.supervisor_ofd_reconcile_refcount ??= supervisorOfdReconcileRefcount;
  nextKernel.supervisor_ofd_offset ??= supervisorOfdOffset;
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
  nextKernel.supervisor_pipe_reconcile_control ??= supervisorPipeReconcileControl;
  nextKernel.supervisor_pipe_slot_mode_read ??= () => SUPERVISOR_PIPE_SLOT_MODE_READ;
  nextKernel.supervisor_pipe_slot_mode_write ??= () => SUPERVISOR_PIPE_SLOT_MODE_WRITE;
  nextKernel.supervisor_pipe_slot_mode_control ??= () => SUPERVISOR_PIPE_SLOT_MODE_CONTROL;
  nextKernel.supervisor_pipe_slot_snapshot_context_shared_state ??= () => SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_SHARED_STATE;
  nextKernel.supervisor_pipe_slot_snapshot_context_thread_step ??= () => SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_THREAD_STEP;
  nextKernel.supervisor_pipe_slot_snapshot_context_kernel_worker_apply ??= () =>
    SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_KERNEL_WORKER_APPLY;
  nextKernel.supervisor_pipe_slot_snapshot_context_kernel_worker_process ??= () =>
    SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_KERNEL_WORKER_PROCESS;
  nextKernel.supervisor_pipe_slot_snapshot_context_kernel_worker_canonical ??= () =>
    SUPERVISOR_PIPE_SLOT_SNAPSHOT_CONTEXT_KERNEL_WORKER_CANONICAL;
  nextKernel.supervisor_pipe_slot_snapshot_action_skip ??= () => SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SKIP;
  nextKernel.supervisor_pipe_slot_snapshot_action_apply_slot ??= () => SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_APPLY_SLOT;
  nextKernel.supervisor_pipe_slot_snapshot_action_apply_control ??= () => SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_APPLY_CONTROL;
  nextKernel.supervisor_pipe_slot_snapshot_action_set_head_to_source ??= () => SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_HEAD_TO_SOURCE;
  nextKernel.supervisor_pipe_slot_snapshot_action_set_tail_to_source ??= () => SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_TAIL_TO_SOURCE;
  nextKernel.supervisor_pipe_slot_snapshot_action_set_head_zero ??= () => SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SET_HEAD_ZERO;
  nextKernel.supervisor_pipe_slot_snapshot_action_copy_full ??= () => SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_FULL;
  nextKernel.supervisor_pipe_slot_snapshot_action_copy_tail_delta ??= () => SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_COPY_TAIL_DELTA;
  nextKernel.supervisor_pipe_slot_snapshot_action_sync_generation ??= () => SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_SYNC_GENERATION;
  nextKernel.supervisor_pipe_slot_snapshot_action_stop ??= () => SUPERVISOR_PIPE_SLOT_SNAPSHOT_ACTION_STOP;
  nextKernel.supervisor_pipe_slot_snapshot_merge_action ??= supervisorPipeSlotSnapshotMergeAction;
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
  nextKernel.supervisor_blocked_retry_action_flag_supervisor_timer_armed ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_SUPERVISOR_TIMER_ARMED;
  nextKernel.supervisor_blocked_retry_action_flag_rpc_pending ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_RPC_PENDING;
  nextKernel.supervisor_blocked_retry_action_flag_owner_async ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_OWNER_ASYNC;
  nextKernel.supervisor_blocked_retry_action_flag_thread_init_pending ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_THREAD_INIT_PENDING;
  nextKernel.supervisor_blocked_retry_action_flag_fork_shared_sync_pending ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_FORK_SHARED_SYNC_PENDING;
  nextKernel.supervisor_blocked_retry_action_flag_running_worker ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_RUNNING_WORKER;
  nextKernel.supervisor_blocked_retry_action_flag_kernel_worker_available ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_KERNEL_WORKER_AVAILABLE;
  nextKernel.supervisor_blocked_retry_action_flag_pipe_state_kernel_worker_owns ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_PIPE_STATE_KERNEL_WORKER_OWNS;
  nextKernel.supervisor_blocked_retry_action_flag_network_connections_available ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_NETWORK_CONNECTIONS_AVAILABLE;
  nextKernel.supervisor_blocked_retry_action_flag_wait_timer_already_checked ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_WAIT_TIMER_ALREADY_CHECKED;
  nextKernel.supervisor_blocked_retry_action_defer_supervisor_timer ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_SUPERVISOR_TIMER;
  nextKernel.supervisor_blocked_retry_action_defer_rpc_pending ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_RPC_PENDING;
  nextKernel.supervisor_blocked_retry_action_defer_owner_async ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_OWNER_ASYNC;
  nextKernel.supervisor_blocked_retry_action_defer_thread_init_pending ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_THREAD_INIT_PENDING;
  nextKernel.supervisor_blocked_retry_action_arm_wait_timer ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_ARM_WAIT_TIMER;
  nextKernel.supervisor_blocked_retry_action_park_active_child ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_PARK_ACTIVE_CHILD;
  nextKernel.supervisor_blocked_retry_action_resume_wait4 ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_WAIT4;
  nextKernel.supervisor_blocked_retry_action_defer_fork_shared_sync ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_FORK_SHARED_SYNC;
  nextKernel.supervisor_blocked_retry_action_defer_running_worker ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_RUNNING_WORKER;
  nextKernel.supervisor_blocked_retry_action_defer_process_gate ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_PROCESS_GATE;
  nextKernel.supervisor_blocked_retry_action_pump_network ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_PUMP_NETWORK;
  nextKernel.supervisor_blocked_retry_action_epoll_kernel_worker ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_EPOLL_KERNEL_WORKER;
  nextKernel.supervisor_blocked_retry_action_request_pipe_sync ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_REQUEST_PIPE_SYNC;
  nextKernel.supervisor_blocked_retry_action_request_pipe_sync_and_drain_output ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_REQUEST_PIPE_SYNC_AND_DRAIN_OUTPUT;
  nextKernel.supervisor_blocked_retry_action_schedule_pipe_sync_retry ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_SCHEDULE_PIPE_SYNC_RETRY;
  nextKernel.supervisor_blocked_retry_action_resume_fs ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_FS;
  nextKernel.supervisor_blocked_retry_action_defer_passive_wait ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_PASSIVE_WAIT;
  nextKernel.supervisor_blocked_retry_action_resume_kernel_worker ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_KERNEL_WORKER;
  nextKernel.supervisor_blocked_retry_action_resume_wait4_kernel_worker ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_WAIT4_KERNEL_WORKER;
  nextKernel.supervisor_blocked_retry_action_resume_epoll_direct ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_EPOLL_DIRECT;
  nextKernel.supervisor_blocked_retry_action_resume_direct ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_DIRECT;
  nextKernel.supervisor_blocked_retry_action_unsupported ??= () =>
    SUPERVISOR_BLOCKED_RETRY_ACTION_UNSUPPORTED;
  nextKernel.supervisor_blocked_initial_action_none ??= () =>
    SUPERVISOR_BLOCKED_INITIAL_ACTION_NONE;
  nextKernel.supervisor_blocked_initial_action_defer_supervisor_timer ??= () =>
    SUPERVISOR_BLOCKED_INITIAL_ACTION_DEFER_SUPERVISOR_TIMER;
  nextKernel.supervisor_blocked_initial_action_defer_rpc_pending ??= () =>
    SUPERVISOR_BLOCKED_INITIAL_ACTION_DEFER_RPC_PENDING;
  nextKernel.supervisor_blocked_initial_action_arm_wait_timer ??= () =>
    SUPERVISOR_BLOCKED_INITIAL_ACTION_ARM_WAIT_TIMER;
  nextKernel.supervisor_blocked_initial_action_retry_now ??= () =>
    SUPERVISOR_BLOCKED_INITIAL_ACTION_RETRY_NOW;
  nextKernel.supervisor_blocked_initial_action_schedule_retry ??= () =>
    SUPERVISOR_BLOCKED_INITIAL_ACTION_SCHEDULE_RETRY;
  nextKernel.supervisor_blocked_retry_result_action_none ??= () =>
    SUPERVISOR_BLOCKED_RETRY_RESULT_ACTION_NONE;
  nextKernel.supervisor_blocked_retry_result_action_arm_wait_timer ??= () =>
    SUPERVISOR_BLOCKED_RETRY_RESULT_ACTION_ARM_WAIT_TIMER;
  nextKernel.supervisor_blocked_retry_result_action_schedule_retry ??= () =>
    SUPERVISOR_BLOCKED_RETRY_RESULT_ACTION_SCHEDULE_RETRY;
  nextKernel.supervisor_blocked_retry_result_action_schedule_retry_and_pump ??= () =>
    SUPERVISOR_BLOCKED_RETRY_RESULT_ACTION_SCHEDULE_RETRY_AND_PUMP;
  nextKernel.supervisor_blocked_retry_status_action_none ??= () =>
    SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_NONE;
  nextKernel.supervisor_blocked_retry_status_action_pipe_retry_diagnostics ??= () =>
    SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_PIPE_RETRY_DIAGNOSTICS;
  nextKernel.supervisor_blocked_retry_status_action_pump_blocked_outcome ??= () =>
    SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_PUMP_BLOCKED_OUTCOME;
  nextKernel.supervisor_blocked_retry_status_action_active_child_pipe ??= () =>
    SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_ACTIVE_CHILD_PIPE;
  nextKernel.supervisor_blocked_retry_status_action_active_child_wait4 ??= () =>
    SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_ACTIVE_CHILD_WAIT4;
  nextKernel.supervisor_blocked_retry_status_action_success_log_epoll_ready ??= () =>
    SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_SUCCESS_LOG_EPOLL_READY;
  nextKernel.supervisor_blocked_retry_status_action_success_log_wait4_resumed ??= () =>
    SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_SUCCESS_LOG_WAIT4_RESUMED;
  nextKernel.supervisor_blocked_retry_status_action_wake_runtime_bridge_while_running ??= () =>
    SUPERVISOR_BLOCKED_RETRY_STATUS_ACTION_WAKE_RUNTIME_BRIDGE_WHILE_RUNNING;
  nextKernel.supervisor_blocked_retry_status_action ??= (status: number) =>
    retryStatusAction(status);
  nextKernel.supervisor_blocked_post_network_pump_action_schedule_retry ??= () =>
    SUPERVISOR_BLOCKED_POST_NETWORK_PUMP_ACTION_SCHEDULE_RETRY;
  nextKernel.supervisor_blocked_post_network_pump_action_schedule_retry_with_host_progress ??= () =>
    SUPERVISOR_BLOCKED_POST_NETWORK_PUMP_ACTION_SCHEDULE_RETRY_WITH_HOST_PROGRESS;
  nextKernel.supervisor_blocked_stale_retry_result_action_none ??= () =>
    SUPERVISOR_BLOCKED_STALE_RETRY_RESULT_ACTION_NONE;
  nextKernel.supervisor_blocked_stale_retry_result_action_handle_current_result ??= () =>
    SUPERVISOR_BLOCKED_STALE_RETRY_RESULT_ACTION_HANDLE_CURRENT_RESULT;
  nextKernel.supervisor_blocked_stale_retry_result_action_handle_current_result_wait4 ??= () =>
    SUPERVISOR_BLOCKED_STALE_RETRY_RESULT_ACTION_HANDLE_CURRENT_RESULT_WAIT4;
  nextKernel.supervisor_blocked_rpc_pipe_sync_action_not_pipe ??= () =>
    SUPERVISOR_BLOCKED_RPC_PIPE_SYNC_ACTION_NOT_PIPE;
  nextKernel.supervisor_blocked_rpc_pipe_sync_action_none ??= () =>
    SUPERVISOR_BLOCKED_RPC_PIPE_SYNC_ACTION_NONE;
  nextKernel.supervisor_blocked_rpc_pipe_sync_action_sync_socket ??= () =>
    SUPERVISOR_BLOCKED_RPC_PIPE_SYNC_ACTION_SYNC_SOCKET;
  nextKernel.supervisor_blocked_rpc_post_resume_sync_flag_kernel_worker_available ??= () =>
    SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_FLAG_KERNEL_WORKER_AVAILABLE;
  nextKernel.supervisor_blocked_rpc_post_resume_sync_flag_process_owner_mismatch ??= () =>
    SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_FLAG_PROCESS_OWNER_MISMATCH;
  nextKernel.supervisor_blocked_rpc_post_resume_sync_action_none ??= () =>
    SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_ACTION_NONE;
  nextKernel.supervisor_blocked_rpc_post_resume_sync_action_fork_shared ??= () =>
    SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_ACTION_FORK_SHARED;
  nextKernel.supervisor_blocked_rpc_post_resume_sync_action_socket ??= () =>
    SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_ACTION_SOCKET;
  nextKernel.supervisor_blocked_initial_action ??= (vmPtr: number, blockedStatus: number, flags: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    if ((flags & SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_SUPERVISOR_TIMER_ARMED) !== 0) {
      return SUPERVISOR_BLOCKED_INITIAL_ACTION_DEFER_SUPERVISOR_TIMER;
    }
    if ((flags & SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_RPC_PENDING) !== 0) {
      return SUPERVISOR_BLOCKED_INITIAL_ACTION_DEFER_RPC_PENDING;
    }
    if (blockedWaitTimerRequired(vmPtr)) {
      return SUPERVISOR_BLOCKED_INITIAL_ACTION_ARM_WAIT_TIMER;
    }
    if (blockedStatus === kernel.abi_status_epoll_wait?.()) {
      return toNumber(nextKernel.supervisor_blocked_epoll_wait_retry_required?.(vmPtr)) !== 0
        ? SUPERVISOR_BLOCKED_INITIAL_ACTION_SCHEDULE_RETRY
        : SUPERVISOR_BLOCKED_INITIAL_ACTION_NONE;
    }
    if (blockedStatus === kernel.abi_status_wait4?.()) {
      return SUPERVISOR_BLOCKED_INITIAL_ACTION_RETRY_NOW;
    }
    return SUPERVISOR_BLOCKED_INITIAL_ACTION_SCHEDULE_RETRY;
  };
  nextKernel.supervisor_blocked_retry_result_action ??= (vmPtr: number, resultStatus: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    if (blockedWaitTimerRequired(vmPtr)) {
      return SUPERVISOR_BLOCKED_RETRY_RESULT_ACTION_ARM_WAIT_TIMER;
    }
    if (resultStatus === kernel.abi_status_epoll_wait?.()) {
      return toNumber(nextKernel.supervisor_blocked_epoll_wait_retry_required?.(vmPtr)) !== 0
        ? SUPERVISOR_BLOCKED_RETRY_RESULT_ACTION_SCHEDULE_RETRY_AND_PUMP
        : SUPERVISOR_BLOCKED_RETRY_RESULT_ACTION_NONE;
    }
    return SUPERVISOR_BLOCKED_RETRY_RESULT_ACTION_SCHEDULE_RETRY;
  };
  nextKernel.supervisor_blocked_post_network_pump_action ??= (
    vmPtr: number,
    changed: number,
    flags: number,
  ) => {
    if (vmPtr === 0) {
      return -14;
    }
    if (changed === 0) {
      return SUPERVISOR_BLOCKED_POST_NETWORK_PUMP_ACTION_SCHEDULE_RETRY_WITH_HOST_PROGRESS;
    }
    if (kernel.thread_status?.(vmPtr) !== kernel.abi_status_pipe_wait?.() || !pollWaitHasSocket(vmPtr)) {
      return SUPERVISOR_BLOCKED_POST_NETWORK_PUMP_ACTION_SCHEDULE_RETRY;
    }
    return nextKernel.supervisor_blocked_retry_action?.(
      vmPtr,
      flags | SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_WAIT_TIMER_ALREADY_CHECKED,
    ) ?? SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_DIRECT;
  };
  nextKernel.supervisor_blocked_stale_retry_result_action ??= (vmPtr: number, blockedStatus: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    const currentStatus = kernel.thread_status?.(vmPtr);
    if (currentStatus === blockedStatus) {
      return SUPERVISOR_BLOCKED_STALE_RETRY_RESULT_ACTION_NONE;
    }
    if (currentStatus !== kernel.abi_status_ok?.() && currentStatus !== kernel.abi_status_fault?.()) {
      return SUPERVISOR_BLOCKED_STALE_RETRY_RESULT_ACTION_NONE;
    }
    if (blockedStatus === kernel.abi_status_wait4?.()) {
      return SUPERVISOR_BLOCKED_STALE_RETRY_RESULT_ACTION_HANDLE_CURRENT_RESULT_WAIT4;
    }
    return SUPERVISOR_BLOCKED_STALE_RETRY_RESULT_ACTION_HANDLE_CURRENT_RESULT;
  };
  nextKernel.supervisor_blocked_rpc_pipe_sync_action ??= (vmPtr: number, blockedStatus: number) => {
    if (blockedStatus !== kernel.abi_status_pipe_wait?.()) {
      return SUPERVISOR_BLOCKED_RPC_PIPE_SYNC_ACTION_NOT_PIPE;
    }
    if (vmPtr === 0) {
      return -14;
    }
    return pollWaitHasSocket(vmPtr)
      ? SUPERVISOR_BLOCKED_RPC_PIPE_SYNC_ACTION_SYNC_SOCKET
      : SUPERVISOR_BLOCKED_RPC_PIPE_SYNC_ACTION_NONE;
  };
  nextKernel.supervisor_blocked_rpc_preserve_canonical_eof ??= (blockedStatus: number) =>
    blockedStatus === kernel.abi_status_pipe_wait?.() ? 1 : 0;
  nextKernel.supervisor_blocked_rpc_post_resume_sync_action ??= (blockedStatus: number, flags: number) => {
    if ((flags & SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_FLAG_PROCESS_OWNER_MISMATCH) !== 0) {
      return SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_ACTION_NONE;
    }
    let action = SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_ACTION_NONE;
    if (
      (flags & SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_FLAG_KERNEL_WORKER_AVAILABLE) !== 0
      && blockedStatus !== kernel.abi_status_epoll_wait?.()
    ) {
      action |= SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_ACTION_FORK_SHARED;
    }
    if (toNumber(nextKernel.supervisor_blocked_status_syncs_socket_after_resume?.(blockedStatus)) !== 0) {
      action |= SUPERVISOR_BLOCKED_RPC_POST_RESUME_SYNC_ACTION_SOCKET;
    }
    return action;
  };
  nextKernel.supervisor_blocked_retry_action ??= (vmPtr: number, flags: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    if ((flags & SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_SUPERVISOR_TIMER_ARMED) !== 0) {
      return SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_SUPERVISOR_TIMER;
    }
    if ((flags & SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_RPC_PENDING) !== 0) {
      return SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_RPC_PENDING;
    }
    if ((flags & SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_OWNER_ASYNC) !== 0) {
      return SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_OWNER_ASYNC;
    }

    const status = toNumber(kernel.thread_status?.(vmPtr));
    const kernelWorkerAvailable =
      (flags & SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_KERNEL_WORKER_AVAILABLE) !== 0;
    if (
      status === kernel.abi_status_epoll_wait?.()
      && (flags & SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_THREAD_INIT_PENDING) !== 0
    ) {
      return SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_THREAD_INIT_PENDING;
    }
    if (
      (flags & SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_WAIT_TIMER_ALREADY_CHECKED) === 0
      && blockedWaitTimerRequired(vmPtr)
    ) {
      return SUPERVISOR_BLOCKED_RETRY_ACTION_ARM_WAIT_TIMER;
    }
    if (toNumber(nextKernel.supervisor_blocked_wait_active_child_pid?.(vmPtr)) > 0) {
      return SUPERVISOR_BLOCKED_RETRY_ACTION_PARK_ACTIVE_CHILD;
    }
    if (status === kernel.abi_status_wait4?.()) {
      return kernelWorkerAvailable
        ? SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_WAIT4_KERNEL_WORKER
        : SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_WAIT4;
    }
    if (
      (flags & SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_FORK_SHARED_SYNC_PENDING) !== 0
      && toNumber(nextKernel.supervisor_blocked_poll_wait_timeout_ms?.(vmPtr)) < 0
    ) {
      return SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_FORK_SHARED_SYNC;
    }
    if ((flags & SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_RUNNING_WORKER) !== 0) {
      return SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_RUNNING_WORKER;
    }

    const retryReason = toNumber(nextKernel.supervisor_blocked_wait_retry_reason?.(vmPtr, 0));
    if (retryReason === SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PROCESS_GATE) {
      return SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_PROCESS_GATE;
    }
    if (
      (flags & SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_NETWORK_CONNECTIONS_AVAILABLE) !== 0
      && toNumber(nextKernel.supervisor_blocked_wait_should_pump_network?.(vmPtr)) !== 0
      && toNumber(nextKernel.supervisor_blocked_wait_network_pump_action?.(vmPtr))
        === SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_PUMP
    ) {
      return SUPERVISOR_BLOCKED_RETRY_ACTION_PUMP_NETWORK;
    }

    if (status === kernel.abi_status_epoll_wait?.() && kernelWorkerAvailable) {
      return SUPERVISOR_BLOCKED_RETRY_ACTION_EPOLL_KERNEL_WORKER;
    }
    if (status === kernel.abi_status_pipe_wait?.()) {
      const pipeFlags =
        (flags & SUPERVISOR_BLOCKED_RETRY_ACTION_FLAG_PIPE_STATE_KERNEL_WORKER_OWNS) !== 0
          ? SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_FLAG_KERNEL_WORKER_AVAILABLE
          : 0;
      const pipeAction = toNumber(nextKernel.supervisor_blocked_pipe_sync_before_retry_action?.(vmPtr, pipeFlags));
      if (pipeAction === SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC) {
        return SUPERVISOR_BLOCKED_RETRY_ACTION_REQUEST_PIPE_SYNC;
      }
      if (pipeAction === 2) {
        return SUPERVISOR_BLOCKED_RETRY_ACTION_REQUEST_PIPE_SYNC_AND_DRAIN_OUTPUT;
      }
      if (pipeAction === 3) {
        return SUPERVISOR_BLOCKED_RETRY_ACTION_SCHEDULE_PIPE_SYNC_RETRY;
      }
    }
    if (status === kernel.abi_status_fs?.()) {
      return SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_FS;
    }
    if (retryReason === SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PASSIVE_WAIT) {
      return SUPERVISOR_BLOCKED_RETRY_ACTION_DEFER_PASSIVE_WAIT;
    }

    const delegationFlags = kernelWorkerAvailable
      ? SUPERVISOR_BLOCKED_RESUME_FLAG_KERNEL_WORKER_AVAILABLE
      : 0;
    const delegation = toNumber(nextKernel.supervisor_blocked_resume_delegation?.(vmPtr, delegationFlags));
    switch (delegation) {
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER:
        return SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_KERNEL_WORKER;
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER:
        return SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_WAIT4_KERNEL_WORKER;
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT:
        return SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_EPOLL_DIRECT;
      case SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED:
        return SUPERVISOR_BLOCKED_RETRY_ACTION_UNSUPPORTED;
      default:
        return SUPERVISOR_BLOCKED_RETRY_ACTION_RESUME_DIRECT;
    }
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
  nextKernel.supervisor_blocked_wait_wake_flag_rpc_pending ??= () =>
    SUPERVISOR_BLOCKED_WAIT_WAKE_FLAG_RPC_PENDING;
  nextKernel.supervisor_blocked_wait_wake_action_skip ??= () =>
    SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_SKIP;
  nextKernel.supervisor_blocked_wait_wake_action_retry ??= () =>
    SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_RETRY;
  nextKernel.supervisor_blocked_wait_wake_action_clear_pipe_sync_and_retry ??= () =>
    SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_CLEAR_PIPE_SYNC_AND_RETRY;
  nextKernel.supervisor_blocked_wait_wake_action_log_epoll_and_retry ??= () =>
    SUPERVISOR_BLOCKED_WAIT_WAKE_ACTION_LOG_EPOLL_AND_RETRY;
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
  nextKernel.supervisor_blocked_wait_wake_action ??= (
    status: number,
    source: number,
    flags: number,
  ) => wakeActionForStatus(status, source, flags);
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
  nextKernel.supervisor_vfork_child_owner_mismatch ??= () => 0;
  nextKernel.supervisor_process_owner_matches ??= (vmPtr: number, runtimePid: number) =>
    kernel.process_current_pid?.(vmPtr) === runtimePid ? 1 : 0;
  nextKernel.supervisor_process_owner_mismatch ??= (vmPtr: number, runtimePid: number) => {
    const currentPid = kernel.process_current_pid?.(vmPtr) ?? 0;
    return currentPid > 0 && currentPid !== runtimePid ? 1 : 0;
  };
  nextKernel.supervisor_process_owner_absent ??= (vmPtr: number) => {
    const currentPid = kernel.process_current_pid?.(vmPtr) ?? 0;
    const threadCount = kernel.thread_process_thread_count?.(vmPtr) ?? 0;
    return currentPid === 0 && threadCount === 0 ? 1 : 0;
  };
  nextKernel.supervisor_process_owner_live_matches ??= (vmPtr: number, runtimePid: number) => {
    const currentPid = kernel.process_current_pid?.(vmPtr) ?? 0;
    const threadCount = kernel.thread_process_thread_count?.(vmPtr) ?? 0;
    return currentPid === runtimePid && threadCount > 0 ? 1 : 0;
  };
  nextKernel.supervisor_process_owner_missing_or_invalid ??= (vmPtr: number) => {
    const currentPid = kernel.process_current_pid?.(vmPtr) ?? 0;
    return currentPid === 0 || currentPid > 0xffff ? 1 : 0;
  };
  nextKernel.supervisor_process_identity_matches ??= (
    vmPtr: number,
    runtimePid: number,
    processPtr: number,
  ) => {
    const currentPid = kernel.process_current_pid?.(vmPtr) ?? 0;
    const currentProcessPtr = kernel.thread_process_ptr?.(vmPtr) ?? 0;
    return currentPid === runtimePid && currentProcessPtr === processPtr ? 1 : 0;
  };
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
