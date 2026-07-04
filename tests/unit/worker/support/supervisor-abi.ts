import {
  requiredSupervisorRuntimeExportNames,
  type KernelExports,
} from "../../../../src/abi.js";
import {
  FS_OP_CLOSE,
  FS_OP_EXECVE,
  FS_OP_MKDIR,
  FS_OP_MMAP,
  FS_OP_OPENAT,
  FS_OP_RENAME,
  FS_OP_UNLINK,
} from "../../../../src/layout/fs-ring.js";

const DEFAULT_RESIDENT_PAGE_SIZE = 4096;

const DEFAULT_NUMERIC_EXPORTS: Record<string, number> = {
  supervisor_abi_version: 1,
  supervisor_debug_snapshot_bytes: 112,
  supervisor_thread_event_record_bytes: 80,
  supervisor_thread_event_record_magic: 0x5645_5354,
  supervisor_thread_event_kind_blocked: 1,
  supervisor_thread_event_kind_ready: 2,
  supervisor_thread_event_kind_faulted: 3,
  supervisor_thread_event_kind_exited: 4,
  supervisor_thread_event_kind_host_completion_arrived: 5,
  supervisor_thread_event_kind_timer_fired: 6,
  supervisor_thread_event_kind_network_readable: 7,
  supervisor_thread_event_kind_backing_store_ready: 8,
  supervisor_thread_state_snapshot_bytes: 32,
  supervisor_thread_state_snapshot_magic: 0x4854_5354,
  supervisor_resume_record_bytes: 48,
  supervisor_resume_record_magic: 0x5255_5354,
  supervisor_host_completion_record_bytes: 64,
  supervisor_host_completion_record_magic: 0x4348_5354,
  supervisor_host_completion_outcome_success: 1,
  supervisor_host_completion_outcome_failure: 2,
  supervisor_host_op_record_bytes: 64,
  supervisor_host_op_record_magic: 0x4f48_5354,
  supervisor_host_op_kind_spawn_thread_worker: 1,
  supervisor_host_op_kind_create_process_memory: 3,
  supervisor_host_op_kind_copy_memory_ranges: 4,
  supervisor_host_op_kind_retry_blocked_worker: 15,
  supervisor_process_host_op_record_bytes: 96,
  supervisor_process_host_op_record_magic: 0x4850_5354,
  supervisor_process_status_is_lifecycle_handoff: 0,
  supervisor_process_status_is_execve_spawn: 0,
  supervisor_process_status_is_clone_spawn: 0,
  supervisor_process_status_is_vfork_wait: 0,
  supervisor_process_status_allows_pid_mismatch: 0,
  supervisor_process_status_defers_kernel_state_adoption: 0,
  supervisor_vfork_child_owner_mismatch: 0,
  supervisor_process_owner_matches: 0,
  supervisor_process_owner_mismatch: 0,
  supervisor_process_owner_absent: 0,
  supervisor_process_owner_live_matches: 0,
  supervisor_process_owner_missing_or_invalid: 0,
  supervisor_process_identity_matches: 0,
  supervisor_fault_exit_decision_bytes: 88,
  supervisor_fault_exit_decision_magic: 0x5846_5354,
  supervisor_fault_exit_kind_thread_exit: 1,
  supervisor_fault_exit_kind_process_exit: 2,
  supervisor_fd_control_record_bytes: 96,
  supervisor_fd_control_record_magic: 0x4446_5354,
  supervisor_fd_entry_restore_action_apply: 1,
  supervisor_fd_entry_restore_action_skip: 2,
  supervisor_fd_entry_restore_flag_preserve_live_regular_file: 1,
  supervisor_fd_entry_restore_flag_preserve_live_epoll_dependent: 2,
  supervisor_ofd_slot_restore_flag_referenced_by_fd_entry: 1,
  supervisor_ofd_slot_restore_flag_trust_regular_file_offset: 2,
  supervisor_fork_shared_peer_flag_compatible: 1,
  supervisor_fork_shared_peer_flag_incompatible: 1 << 1,
  supervisor_fork_shared_peer_flag_pipe_peer: 1 << 2,
  supervisor_fork_shared_held_ofd_track_flag_shared: 1,
  supervisor_fork_shared_held_ofd_track_flag_host_stdio_pipe: 1 << 1,
  supervisor_fork_shared_held_ofd_track_flag_stdio_fd: 1 << 2,
  supervisor_fork_shared_held_ofd_track_flag_fd_entry_source: 1 << 3,
  supervisor_fork_shared_held_ofd_track_action: 0,
  supervisor_fork_shared_effect_sync_action: 0,
  supervisor_fs_control_record_bytes: 112,
  supervisor_fs_control_record_magic: 0x5346_5354,
  supervisor_signal_control_record_bytes: 120,
  supervisor_signal_control_record_magic: 0x4753_5354,
  supervisor_fs_response_flag_adopt_kernel_state: 1,
  supervisor_fs_response_flag_apply_guest_memory_writes: 2,
  supervisor_fs_response_flag_queue_thread_worker_writes: 4,
  supervisor_fs_response_flag_apply_local_metadata: 8,
  supervisor_fs_response_flag_clear_tombstones: 16,
  supervisor_fs_response_flag_fd_response_may_clear_live_fds: 32,
  supervisor_fs_response_flag_store_pending_kernel_state: 64,
  supervisor_fs_response_flag_resolved_to_execve_spawn: 128,
  supervisor_fs_response_flag_process_owner_matches: 256,
  supervisor_fs_pipe_refresh_action_none: 0,
  supervisor_fs_pipe_refresh_action_request_sync: 1,
  supervisor_fs_pipe_refresh_flag_kernel_worker_available: 1,
  supervisor_fs_resident_page_evict_not_found: 0,
  supervisor_fs_resident_page_evict_dirty_pinned: 1,
  supervisor_fs_resident_page_evict_evicted: 2,
  supervisor_resume_kind_futex_wake: 1,
  supervisor_resume_kind_runnable_thread: 2,
  supervisor_resume_flag_timeout_expired: 1,
  supervisor_blocked_resume_delegation_direct: 0,
  supervisor_blocked_resume_delegation_kernel_worker: 1,
  supervisor_blocked_resume_delegation_wait4_kernel_worker: 2,
  supervisor_blocked_resume_delegation_unsupported: 3,
  supervisor_blocked_resume_delegation_epoll_direct: 4,
  supervisor_blocked_resume_delegation_flag_kernel_worker_available: 1,
  supervisor_blocked_status_is_host_wait: 0,
  supervisor_blocked_status_is_connect_wait: 0,
  supervisor_blocked_status_is_epoll_wait: 0,
  supervisor_blocked_status_is_pipe_wait: 0,
  supervisor_blocked_status_is_wait4: 0,
  supervisor_blocked_status_is_socket_wait: 0,
  supervisor_blocked_status_needs_network_sidecar_restore: 0,
  supervisor_blocked_status_syncs_socket_after_resume: 0,
  supervisor_blocked_status_uses_fork_shared_state_sync: 0,
  supervisor_blocked_epoll_wait_timeout_ms: -1,
  supervisor_blocked_poll_wait_timeout_ms: -1,
  supervisor_blocked_poll_wait_has_socket: 0,
  supervisor_blocked_pipe_wait_has_ready_events: 0,
  supervisor_blocked_wait_may_depend_on_network: 0,
  supervisor_blocked_wait_should_pump_network: 0,
  supervisor_blocked_pipe_sync_before_retry_action_none: 0,
  supervisor_blocked_pipe_sync_before_retry_action_request_sync: 1,
  supervisor_blocked_pipe_sync_before_retry_action_request_sync_and_drain_output: 2,
  supervisor_blocked_pipe_sync_before_retry_action_schedule_retry: 3,
  supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available: 1,
  supervisor_blocked_status_transition_action_block: 0,
  supervisor_blocked_status_transition_action_sync_socket: 1,
  supervisor_blocked_status_transition_action_sync_pipe: 2,
  supervisor_blocked_status_transition_action_sync_fork_shared: 3,
  supervisor_blocked_status_transition_action_sync_pipe_and_fork_shared: 4,
  supervisor_blocked_status_transition_action_sync_socket_and_fork_shared: 5,
  supervisor_blocked_status_transition_flag_kernel_worker_available: 1,
  supervisor_blocked_status_transition_flag_network_connections_available: 2,
  supervisor_blocked_wait_network_pump_action_skip: 0,
  supervisor_blocked_wait_network_pump_action_pump: 1,
  supervisor_state_sync_kind_pipe: 1,
  supervisor_state_sync_kind_epoll: 2,
  supervisor_state_sync_kind_socket: 3,
  supervisor_state_sync_request_flag_wake_waiters: 1,
  supervisor_state_sync_run_flag_run: 1,
  supervisor_state_sync_run_flag_wake_waiters: 2,
  supervisor_blocked_wait_retry_reason_default: 0,
  supervisor_blocked_wait_retry_reason_immediate: 1,
  supervisor_blocked_wait_retry_reason_standard_defer: 2,
  supervisor_blocked_wait_retry_reason_active_child: 3,
  supervisor_blocked_wait_retry_reason_passive_wait: 4,
  supervisor_blocked_wait_retry_reason_process_gate: 5,
  supervisor_blocked_wait_retry_flag_host_progress: 1,
  supervisor_blocked_wait_retry_flag_active_child: 2,
  supervisor_blocked_wait_retry_flag_passive_wait: 4,
  supervisor_blocked_wait_retry_flag_process_gate: 8,
  supervisor_blocked_wait_retry_flag_thread_init_pending: 16,
  supervisor_blocked_retry_action_flag_supervisor_timer_armed: 1,
  supervisor_blocked_retry_action_flag_rpc_pending: 2,
  supervisor_blocked_retry_action_flag_owner_async: 4,
  supervisor_blocked_retry_action_flag_thread_init_pending: 8,
  supervisor_blocked_retry_action_flag_fork_shared_sync_pending: 16,
  supervisor_blocked_retry_action_flag_running_worker: 32,
  supervisor_blocked_retry_action_flag_kernel_worker_available: 64,
  supervisor_blocked_retry_action_flag_pipe_state_kernel_worker_owns: 128,
  supervisor_blocked_retry_action_flag_network_connections_available: 256,
  supervisor_blocked_retry_action_flag_wait_timer_already_checked: 512,
  supervisor_blocked_retry_action_defer_supervisor_timer: 0,
  supervisor_blocked_retry_action_defer_rpc_pending: 1,
  supervisor_blocked_retry_action_defer_owner_async: 2,
  supervisor_blocked_retry_action_defer_thread_init_pending: 3,
  supervisor_blocked_retry_action_arm_wait_timer: 4,
  supervisor_blocked_retry_action_park_active_child: 5,
  supervisor_blocked_retry_action_resume_wait4: 6,
  supervisor_blocked_retry_action_defer_fork_shared_sync: 7,
  supervisor_blocked_retry_action_defer_running_worker: 8,
  supervisor_blocked_retry_action_defer_process_gate: 9,
  supervisor_blocked_retry_action_pump_network: 10,
  supervisor_blocked_retry_action_epoll_kernel_worker: 11,
  supervisor_blocked_retry_action_request_pipe_sync: 12,
  supervisor_blocked_retry_action_request_pipe_sync_and_drain_output: 13,
  supervisor_blocked_retry_action_schedule_pipe_sync_retry: 14,
  supervisor_blocked_retry_action_resume_fs: 15,
  supervisor_blocked_retry_action_defer_passive_wait: 16,
  supervisor_blocked_retry_action_resume_kernel_worker: 17,
  supervisor_blocked_retry_action_resume_wait4_kernel_worker: 18,
  supervisor_blocked_retry_action_resume_epoll_direct: 19,
  supervisor_blocked_retry_action_resume_direct: 20,
  supervisor_blocked_retry_action_unsupported: 21,
  supervisor_blocked_wait_wake_source_pipe_sync: 1,
  supervisor_blocked_wait_wake_source_epoll_sync: 2,
  supervisor_blocked_wait_wake_source_socket_sync: 3,
  supervisor_blocked_wait_wake_source_pipe_and_epoll_sync: 4,
  supervisor_blocked_wait_wake_target_pipe_wait: 1,
  supervisor_blocked_wait_wake_target_epoll_wait: 2,
  supervisor_blocked_wait_wake_target_socket_recv: 4,
  supervisor_blocked_wait_wake_target_socket_send: 8,
  supervisor_stdio_output_settle_kind_visible: 1,
  supervisor_stdio_output_settle_kind_inherited_child: 2,
  supervisor_stdio_pipe_read_progress_action_none: 0,
  supervisor_stdio_pipe_read_progress_action_snapshot: 1,
  supervisor_blocked_wait_timer_required: 0,
  supervisor_vfork_execve_handoff_failure_wait_status: 127 << 8,
  supervisor_host_op_kind_arm_host_timer: 12,
  supervisor_futex_wait_completion_woken: 1,
  supervisor_futex_wait_completion_not_equal: 2,
  supervisor_futex_wait_completion_invalid_address: 4,
  supervisor_futex_wait_completion_signal_check: 5,
  supervisor_fs_metadata_full_sync_action_none: 0,
  supervisor_fs_metadata_full_sync_action_required: 1,
  supervisor_fs_local_metadata_action_none: 0,
  supervisor_fs_local_metadata_action_tombstone_primary: 1,
  supervisor_fs_local_metadata_action_forget_primary: 2,
  supervisor_fs_local_metadata_action_forget_secondary: 4,
};

export interface SupervisorRunnableResumeRecordFixture {
  vmPtr: number;
  tid?: number;
  processPtr?: number;
  status?: number;
  seq?: bigint;
  detail0?: bigint;
}

export interface SupervisorArmHostTimerHostOpFixture {
  vmPtr: number;
  delayMs: bigint;
  seq?: bigint;
  tid?: number;
  processPtr?: number;
  status?: number;
  deadlineTicks?: bigint;
}

export interface SupervisorBlockedRetryHostOpFixture {
  vmPtr: number;
  delayMs: bigint;
  reason: bigint;
  seq?: bigint;
  tid?: number;
  processPtr?: number;
  status?: number;
}

export interface SupervisorFaultExitDecisionRecordFixture {
  kind?: number;
  seq?: bigint;
  vmPtr: number;
  exitVmPtr?: number;
  tid?: number;
  processPtr?: number;
  remainingThreads?: number;
  liveThreads?: number;
  exitCode?: number;
  termSignal?: number;
  status?: number;
  faultSignal?: number;
  faultKind?: number;
  faultPc?: bigint;
  faultAddr?: bigint;
}

type SupervisorAbiStubOverrides = Partial<KernelExports> & {
  cloneSpawnParentVmPtr?: () => number;
  currentExports?: () => Partial<KernelExports> | undefined;
};

export function makeSupervisorAbiStubs(
  overrides: SupervisorAbiStubOverrides = {},
): Partial<KernelExports> {
  const cloneSpawnHostOpChildVmPtrs = new Map<number, number>();
  const networkPumpBackoffs = new Set<number>();
  const stateSyncRequests = new Map<string, { queuedRunFlags: number; inFlight: boolean }>();
  const ofdRestoreOffsetFloors = new Map<number, { hostFd: number; ino: number; offset: bigint }>();
  const currentExports = (receiver: KernelExports | undefined): Partial<KernelExports> =>
    receiver ?? overrides.currentExports?.() ?? overrides;
  const callNumber = (receiver: Partial<KernelExports>, name: keyof KernelExports, ...args: unknown[]): number => {
    const fn = (receiver[name] ?? overrides[name]) as ((...params: unknown[]) => unknown) | undefined;
    const value = fn?.(...args);
    return typeof value === "bigint" ? Number(value) : Number(value ?? 0);
  };
  const callOptionalNumber = (receiver: Partial<KernelExports>, name: keyof KernelExports, ...args: unknown[]): number | undefined => {
    const fn = (receiver[name] ?? overrides[name]) as ((...params: unknown[]) => unknown) | undefined;
    if (fn === undefined) {
      return undefined;
    }
    const value = fn(...args);
    return typeof value === "bigint" ? Number(value) : Number(value);
  };
  const callBigInt = (receiver: Partial<KernelExports>, name: keyof KernelExports, ...args: unknown[]): bigint => {
    const fn = (receiver[name] ?? overrides[name]) as ((...params: unknown[]) => unknown) | undefined;
    const value = fn?.(...args);
    return typeof value === "bigint" ? value : BigInt(Math.trunc(Number(value ?? 0)));
  };
  const callOptionalBigInt = (
    receiver: Partial<KernelExports>,
    name: keyof KernelExports,
    ...args: unknown[]
  ): bigint | undefined => {
    const fn = (receiver[name] ?? overrides[name]) as ((...params: unknown[]) => unknown) | undefined;
    if (fn === undefined) {
      return undefined;
    }
    const value = fn(...args);
    return typeof value === "bigint" ? value : BigInt(Math.trunc(Number(value)));
  };
  const statusFor = (receiver: Partial<KernelExports>, vmPtr: number): number =>
    callNumber(receiver, "thread_status", vmPtr);
  const pipeWaitStatusFor = (receiver: Partial<KernelExports>): number =>
    callNumber(receiver, "abi_status_pipe_wait");
  const pollWaitIsPollLike = (receiver: Partial<KernelExports>, vmPtr: number): boolean => {
    const syscallNr = callNumber(receiver, "thread_blocked_syscall_nr", vmPtr);
    return syscallNr !== 0
      && (
        syscallNr === callNumber(receiver, "abi_sys_ppoll")
        || syscallNr === callNumber(receiver, "abi_sys_pselect6")
      );
  };
  const supervisorPollWaitTimeoutMs = (receiver: Partial<KernelExports>, vmPtr: number): number => {
    if (vmPtr === 0) {
      return -14;
    }
    if (statusFor(receiver, vmPtr) !== pipeWaitStatusFor(receiver) || !pollWaitIsPollLike(receiver, vmPtr)) {
      return -1;
    }
    return callOptionalNumber(receiver, "thread_poll_wait_timeout", vmPtr) ?? -1;
  };
  const supervisorEpollWaitTimeoutMs = (receiver: Partial<KernelExports>, vmPtr: number): number => {
    if (vmPtr === 0) {
      return -14;
    }
    if (statusFor(receiver, vmPtr) !== callNumber(receiver, "abi_status_epoll_wait")) {
      return -1;
    }
    return callNumber(receiver, "thread_epoll_wait_timeout", vmPtr);
  };
  const statusMatchesAny = (
    receiver: Partial<KernelExports>,
    status: number,
    names: Array<keyof KernelExports>,
  ): boolean =>
    names.some((name) => callOptionalNumber(receiver, name) === status);
  const supervisorStatusIsHostWait = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, [
      "abi_status_wait4",
      "abi_status_pipe_wait",
      "abi_status_epoll_wait",
      "abi_status_socket_recv",
      "abi_status_socket_send",
      "abi_status_flock_wait",
      "abi_status_connect",
    ]);
  const supervisorStatusIsConnectWait = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, ["abi_status_connect"]);
  const supervisorStatusIsEpollWait = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, ["abi_status_epoll_wait"]);
  const supervisorStatusIsPipeWait = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, ["abi_status_pipe_wait"]);
  const supervisorStatusIsWait4 = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, ["abi_status_wait4"]);
  const supervisorStatusIsSocketWait = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, [
      "abi_status_socket_recv",
      "abi_status_socket_send",
    ]);
  const supervisorStatusNeedsNetworkSidecarRestore = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, [
      "abi_status_connect",
      "abi_status_socket_recv",
      "abi_status_socket_send",
      "abi_status_pipe_wait",
      "abi_status_epoll_wait",
    ]);
  const supervisorStatusSyncsSocketAfterResume = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, [
      "abi_status_connect",
      "abi_status_socket_recv",
      "abi_status_socket_send",
    ]);
  const supervisorStatusUsesForkSharedStateSync = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, [
      "abi_status_pipe_wait",
      "abi_status_socket_recv",
      "abi_status_socket_send",
      "abi_status_flock_wait",
    ]);
  const supervisorProcessStatusIsLifecycleHandoff = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, [
      "abi_status_vfork_wait",
      "abi_status_execve_spawn",
      "abi_status_clone_spawn",
    ]);
  const supervisorProcessStatusIsExecveSpawn = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, [
      "abi_status_execve_spawn",
    ]);
  const supervisorProcessStatusIsCloneSpawn = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, [
      "abi_status_clone_spawn",
    ]);
  const supervisorProcessStatusIsVforkWait = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, [
      "abi_status_vfork_wait",
    ]);
  const supervisorProcessStatusAllowsPidMismatch = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, [
      "abi_status_vfork_wait",
      "abi_status_execve_spawn",
    ]);
  const supervisorProcessStatusDefersKernelStateAdoption = (receiver: Partial<KernelExports>, status: number): boolean =>
    statusMatchesAny(receiver, status, [
      "abi_status_clone_spawn",
    ]);
  const supervisorWaitWakeMaskForSync = (source: number): number => {
    switch (source) {
      case DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_source_pipe_sync:
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_target_pipe_wait;
      case DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_source_epoll_sync:
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_target_epoll_wait;
      case DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_source_socket_sync:
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_target_pipe_wait
          | DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_target_epoll_wait
          | DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_target_socket_recv
          | DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_target_socket_send;
      case DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_source_pipe_and_epoll_sync:
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_target_pipe_wait
          | DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_target_epoll_wait;
      default:
        return -22;
    }
  };
  const supervisorWaitWakeTargetForStatus = (
    receiver: Partial<KernelExports>,
    status: number,
  ): number => {
    if (callOptionalNumber(receiver, "abi_status_pipe_wait") === status) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_target_pipe_wait;
    }
    if (callOptionalNumber(receiver, "abi_status_epoll_wait") === status) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_target_epoll_wait;
    }
    if (callOptionalNumber(receiver, "abi_status_socket_recv") === status) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_target_socket_recv;
    }
    if (callOptionalNumber(receiver, "abi_status_socket_send") === status) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_wake_target_socket_send;
    }
    return 0;
  };
  const supervisorWaitStatusMatchesWakeSource = (
    receiver: Partial<KernelExports>,
    status: number,
    source: number,
  ): number => {
    const mask = supervisorWaitWakeMaskForSync(source);
    if (mask < 0) {
      return mask;
    }
    const target = supervisorWaitWakeTargetForStatus(receiver, status);
    return target !== 0 && (mask & target) !== 0 ? 1 : 0;
  };
  const supervisorPollWaitHasSocket = (receiver: Partial<KernelExports>, vmPtr: number): boolean =>
    vmPtr !== 0
    && statusFor(receiver, vmPtr) === pipeWaitStatusFor(receiver)
    && callNumber(receiver, "thread_poll_wait_has_socket", vmPtr) === 1;
  const supervisorPipeWaitHasReadyEvents = (receiver: Partial<KernelExports>, vmPtr: number): boolean =>
    vmPtr !== 0
    && statusFor(receiver, vmPtr) === pipeWaitStatusFor(receiver)
    && callNumber(receiver, "thread_poll_wait_ready_count", vmPtr) > 0;
  const supervisorWaitMayDependOnNetwork = (receiver: Partial<KernelExports>, vmPtr: number): boolean => {
    const status = statusFor(receiver, vmPtr);
    if (status === pipeWaitStatusFor(receiver)) {
      if (supervisorPollWaitTimeoutMs(receiver, vmPtr) >= 0 && supervisorPollWaitHasSocket(receiver, vmPtr)) {
        return true;
      }
      if (supervisorPipeWaitHasReadyEvents(receiver, vmPtr)) {
        return false;
      }
      return false;
    }
    return status === callNumber(receiver, "abi_status_socket_recv")
      || status === callNumber(receiver, "abi_status_socket_send")
      || status === callNumber(receiver, "abi_status_connect");
  };
  const addRange = (ranges: Array<{ start: number; end: number }>, start: number, end: number, limit: number): void => {
    const boundedStart = Math.max(0, Math.min(start, limit));
    const boundedEnd = Math.max(boundedStart, Math.min(end, limit));
    if (boundedEnd > boundedStart) {
      ranges.push({ start: boundedStart, end: boundedEnd });
    }
  };
  const coalesceRanges = (ranges: Array<{ start: number; end: number }>): Array<{ start: number; end: number }> => {
    const sorted = ranges
      .filter((range) => range.end > range.start)
      .sort((left, right) => left.start - right.start || left.end - right.end);
    const coalesced: Array<{ start: number; end: number }> = [];
    for (const range of sorted) {
      const last = coalesced[coalesced.length - 1];
      if (last && range.start <= last.end) {
        last.end = Math.max(last.end, range.end);
      } else {
        coalesced.push({ ...range });
      }
    }
    return coalesced;
  };
  const processGuestBase = (receiver: Partial<KernelExports>): number => {
    const addressProfile = callNumber(receiver, "thread_spawn_address_profile" as keyof KernelExports, 0)
      || callNumber(receiver, "abi_guest_address_profile_target" as keyof KernelExports);
    const base = callNumber(receiver, "process_guest_base_for_address_profile", addressProfile);
    return base || 0x1000000;
  };
  const processMemoryRanges = (
    receiver: Partial<KernelExports>,
    memory: WebAssembly.Memory,
    vmPtr: number,
  ): Array<{ start: number; end: number }> => {
    const limit = memory.buffer.byteLength;
    const ranges: Array<{ start: number; end: number }> = [];
    const spawnKind = callNumber(receiver, "thread_spawn_kind", vmPtr);
    const execveKind = callNumber(receiver, "abi_spawn_kind_execve");
    const fullMemfs = spawnKind === execveKind;
    const heapPtr = callNumber(receiver, "kernel_heap_ptr");
    addRange(ranges, 0, heapPtr, limit);
    addRange(ranges, callNumber(receiver, "kernel_ofd_table_ptr"), callNumber(receiver, "kernel_ofd_table_ptr") + callNumber(receiver, "kernel_ofd_table_bytes"), limit);
    addRange(
      ranges,
      callNumber(receiver, "kernel_memfs_ptr"),
      callNumber(receiver, "kernel_memfs_ptr")
        + callNumber(receiver, "kernel_memfs_header_bytes")
        + callNumber(receiver, "kernel_memfs_inodes_bytes")
        + callNumber(receiver, "kernel_memfs_first_child_bytes")
        + callNumber(receiver, "kernel_memfs_dir_entries_bytes")
        + (fullMemfs ? callNumber(receiver, "kernel_memfs_data_bytes") : 0),
      limit,
    );
    addRange(ranges, callNumber(receiver, "kernel_memfs_data_ptr"), callNumber(receiver, "kernel_memfs_data_ptr") + callNumber(receiver, "kernel_memfs_data_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_epoll_table_ptr"), callNumber(receiver, "kernel_epoll_table_ptr") + callNumber(receiver, "kernel_epoll_table_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_socket_table_ptr"), callNumber(receiver, "kernel_socket_table_ptr") + callNumber(receiver, "kernel_socket_table_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_pipe_base"), callNumber(receiver, "kernel_pipe_base") + callNumber(receiver, "kernel_pipe_region_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_net_buf_base"), callNumber(receiver, "kernel_net_buf_base") + callNumber(receiver, "kernel_net_buf_pool_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_sig_slots_ptr"), callNumber(receiver, "kernel_sig_slots_ptr") + callNumber(receiver, "kernel_sig_slots_bytes") + callNumber(receiver, "kernel_signal_action_table_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_process_pending_ptr"), callNumber(receiver, "kernel_process_pending_ptr") + callNumber(receiver, "kernel_process_pending_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_process_runtime_state_ptr"), callNumber(receiver, "kernel_process_runtime_state_ptr") + callNumber(receiver, "kernel_process_runtime_state_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_time_state_ptr"), callNumber(receiver, "kernel_time_state_ptr") + callNumber(receiver, "kernel_time_state_bytes"), limit);
    if (spawnKind !== execveKind) {
      const guestBase = processGuestBase(receiver);
      const processPtr = callNumber(receiver, "thread_process_ptr", vmPtr);
      const processStateBytes = callNumber(receiver, "process_state_bytes") || 4096;
      addRange(ranges, guestBase, guestBase + processPtr + processStateBytes, limit);
      const spRaw = receiver.thread_sp?.(vmPtr) ?? overrides.thread_sp?.(vmPtr) ?? 0n;
      const sp = typeof spRaw === "bigint" ? Number(spRaw) : Number(spRaw);
      if (Number.isFinite(sp) && sp > 0) {
        const stackStartGuest = Math.max(0, Math.floor(sp / 65536) * 65536 - 1024 * 1024);
        addRange(ranges, guestBase + stackStartGuest, guestBase + stackStartGuest + 8 * 1024 * 1024, limit);
      }
    }
    return coalesceRanges(ranges);
  };
  const processMemoryInitialBytes = (
    receiver: Partial<KernelExports>,
    ranges: Array<{ start: number; end: number }>,
    vmPtr: number,
  ): number => {
    const spawnKind = callNumber(receiver, "thread_spawn_kind", vmPtr);
    const execveKind = callNumber(receiver, "abi_spawn_kind_execve");
    const guestBase = processGuestBase(receiver);
    if (spawnKind === execveKind) {
      const spRaw = receiver.thread_sp?.(vmPtr) ?? overrides.thread_sp?.(vmPtr) ?? 0n;
      const sp = typeof spRaw === "bigint" ? Number(spRaw) : Number(spRaw);
      const liveStackEnd = Number.isFinite(sp) && sp > 0 ? guestBase + sp + 8 * 1024 * 1024 : 0;
      return Math.max(64 * 1024 * 1024, callNumber(receiver, "kernel_heap_ptr"), guestBase + 0x10000, liveStackEnd);
    }
    const copyEnd = ranges.reduce((end, range) => Math.max(end, range.end), 0);
    return Math.max(copyEnd + 16 * 1024 * 1024, guestBase);
  };
  const processMemoryMaxPages = (
    receiver: Partial<KernelExports>,
    initialBytes: number,
    vmPtr: number,
  ): number => {
    return callNumber(receiver, "thread_spawn_process_max_memory_pages", vmPtr)
      || callNumber(receiver, "thread_process_max_memory_pages", vmPtr)
      || Math.ceil(initialBytes / 65536)
      || 1;
  };
  const processHeldOfdRefcount = (
    receiver: Partial<KernelExports>,
    vmPtr: number,
    targetOfdId: number,
  ): number => {
    const maxFds = callNumber(receiver, "abi_max_fds");
    if (targetOfdId <= 0 || targetOfdId >= maxFds) {
      return 0;
    }
    let count = 0;
    for (let fd = 0; fd < maxFds; fd += 1) {
      if (callNumber(receiver, "process_fd_ofd_id", vmPtr, fd) === targetOfdId) {
        count += 1;
      }
    }
    const maxThreads = callNumber(receiver, "abi_max_threads");
    for (let slot = 0; slot < maxThreads; slot += 1) {
      const threadVmPtr = callNumber(receiver, "process_thread_vm_ptr", vmPtr, slot);
      if (threadVmPtr === 0) {
        continue;
      }
      if (callNumber(receiver, "thread_blocked_ofd_id", threadVmPtr) === targetOfdId) {
        count += 1;
      }
    }
    return count;
  };
  const processHeldOfdIds = (receiver: Partial<KernelExports>, vmPtr: number): number[] => {
    const maxFds = callNumber(receiver, "abi_max_fds");
    const ofdIds: number[] = [];
    for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
      if (processHeldOfdRefcount(receiver, vmPtr, ofdId) > 0) {
        ofdIds.push(ofdId);
      }
    }
    return ofdIds;
  };
  const forkSharedOfdIdentityKey = (
    receiver: Partial<KernelExports>,
    ofdId: number,
  ): bigint => {
    const maxFds = callNumber(receiver, "abi_max_fds");
    if (ofdId <= 0 || ofdId >= maxFds) {
      return 0n;
    }
    const fdType = callNumber(receiver, "kernel_ofd_fd_type", ofdId);
    if (fdType <= 0) {
      return 0n;
    }
    const ino = callNumber(receiver, "kernel_ofd_ino", ofdId);
    const hostFd = callNumber(receiver, "kernel_ofd_host_fd", ofdId);
    const pipeType = callNumber(receiver, "abi_fd_type_pipe");
    const socketType = callOptionalNumber(receiver, "abi_fd_type_socket");
    const endpointToken = fdType === pipeType
      ? (hostFd >= 0 ? 1n : 2n)
      : socketType !== undefined && fdType === socketType
        ? BigInt.asUintN(64, BigInt(hostFd))
        : 0n;
    let hash = 0xcbf29ce484222325n;
    for (const value of [BigInt(fdType), BigInt(ino), endpointToken]) {
      hash ^= BigInt.asUintN(64, value);
      hash = BigInt.asUintN(64, hash * 0x100000001b3n);
    }
    return hash === 0n ? 0xcbf29ce484222325n : hash;
  };
  const forkSharedOfdPublishAction = (
    receiver: Partial<KernelExports>,
    ofdId: number,
    peerFlags: number,
  ): number => {
    const maxFds = callNumber(receiver, "abi_max_fds");
    if (ofdId <= 0 || ofdId >= maxFds) {
      return 0;
    }
    const fdType = callNumber(receiver, "kernel_ofd_fd_type", ofdId);
    if (
      fdType <= 0
      || (peerFlags & DEFAULT_NUMERIC_EXPORTS.supervisor_fork_shared_peer_flag_incompatible) !== 0
    ) {
      return 0;
    }
    const compatiblePeer = (peerFlags
      & DEFAULT_NUMERIC_EXPORTS.supervisor_fork_shared_peer_flag_compatible) !== 0;
    const pipePeer = (peerFlags & DEFAULT_NUMERIC_EXPORTS.supervisor_fork_shared_peer_flag_pipe_peer) !== 0;
    if (fdType === callNumber(receiver, "abi_fd_type_file")) {
      return compatiblePeer ? 1 : 0;
    }
    if (fdType === callNumber(receiver, "abi_fd_type_pipe")) {
      return compatiblePeer || pipePeer ? 1 : 0;
    }
    return 1;
  };
  const forkSharedEffectSyncAction = (
    hasEffect: number,
    hasTrackedState: number,
    sourceHoldsRegularFile: number,
    regularFileHasPeer: number,
  ): number => {
    if (hasEffect === 0 || hasTrackedState === 0) {
      return 0;
    }
    return sourceHoldsRegularFile !== 0 && regularFileHasPeer === 0 ? 0 : 1;
  };
  const fdTypeIsPipeLike = (receiver: Partial<KernelExports>, fdType: number): boolean =>
    fdType === callNumber(receiver, "abi_fd_type_pipe")
    || fdType === callOptionalNumber(receiver, "abi_fd_type_pty_master")
    || fdType === callOptionalNumber(receiver, "abi_fd_type_pty_slave");
  const processPipeSlotForFd = (
    receiver: Partial<KernelExports>,
    vmPtr: number,
    fd: number,
  ): number => {
    const maxFds = callNumber(receiver, "abi_max_fds");
    if (fd < 0 || fd >= maxFds) {
      return 0;
    }
    const ofdId = callNumber(receiver, "process_fd_ofd_id", vmPtr, fd);
    if (ofdId <= 0 || ofdId >= maxFds) {
      return 0;
    }
    const fdType = callNumber(receiver, "kernel_ofd_fd_type", ofdId);
    if (fdType === callNumber(receiver, "abi_fd_type_pipe")) {
      return callNumber(receiver, "kernel_ofd_ino", ofdId);
    }
    const ptySlaveType = callOptionalNumber(receiver, "abi_fd_type_pty_slave");
    const ptyMasterType = callOptionalNumber(receiver, "abi_fd_type_pty_master");
    if (fdType !== ptySlaveType && fdType !== ptyMasterType) {
      return 0;
    }
    const ptyIdx = callNumber(receiver, "kernel_ofd_ino", ofdId);
    const statusFlags = callOptionalNumber(receiver, "kernel_ofd_status_flags", ofdId) ?? 0;
    const oWronly = callOptionalNumber(receiver, "abi_o_wronly") ?? 1;
    const oRdwr = callOptionalNumber(receiver, "abi_o_rdwr") ?? 2;
    const fdWritesToStdioOutput = fd === 1 || fd === 2 || statusFlags === oWronly || statusFlags === oRdwr;
    if (fdType === ptySlaveType) {
      return fdWritesToStdioOutput
        ? (callOptionalNumber(receiver, "kernel_pty_output_pipe", ptyIdx) ?? 0)
        : (callOptionalNumber(receiver, "kernel_pty_input_pipe", ptyIdx) ?? 0);
    }
    return fdWritesToStdioOutput
      ? (callOptionalNumber(receiver, "kernel_pty_input_pipe", ptyIdx) ?? 0)
      : (callOptionalNumber(receiver, "kernel_pty_output_pipe", ptyIdx) ?? 0);
  };
  const processPipeOfdForFd = (
    receiver: Partial<KernelExports>,
    vmPtr: number,
    fd: number,
  ): number => {
    const maxFds = callNumber(receiver, "abi_max_fds");
    const ofdId = fd >= 0 && fd < maxFds
      ? callNumber(receiver, "process_fd_ofd_id", vmPtr, fd)
      : 0;
    return ofdId > 0 && ofdId < maxFds && processPipeSlotForFd(receiver, vmPtr, fd) !== 0
      ? ofdId
      : 0;
  };
  const regularFileOfdsShareIdentity = (
    receiver: Partial<KernelExports>,
    currentOfdId: number,
    candidateOfdId: number,
  ): boolean =>
    callNumber(receiver, "kernel_ofd_fd_type", currentOfdId) === callNumber(receiver, "abi_fd_type_file")
    && callNumber(receiver, "kernel_ofd_fd_type", candidateOfdId) === callNumber(receiver, "abi_fd_type_file")
    && callNumber(receiver, "kernel_ofd_host_fd", currentOfdId) === callNumber(receiver, "kernel_ofd_host_fd", candidateOfdId)
    && callNumber(receiver, "kernel_ofd_ino", currentOfdId) === callNumber(receiver, "kernel_ofd_ino", candidateOfdId);
  const supervisorFdEntryGuestWriteAction = (
    receiver: Partial<KernelExports>,
    vmPtr: number,
    fd: number,
    candidateOfdId: number,
    explicitOfdId: number,
  ): number => {
    const maxFds = callNumber(receiver, "abi_max_fds");
    if (fd < 0 || fd >= maxFds || candidateOfdId < 0 || candidateOfdId >= maxFds) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_apply;
    }
    const currentOfdId = callNumber(receiver, "process_fd_ofd_id", vmPtr, fd);
    if (currentOfdId <= 0 || currentOfdId >= maxFds || currentOfdId === candidateOfdId) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_apply;
    }
    const currentType = callNumber(receiver, "kernel_ofd_fd_type", currentOfdId);
    if (explicitOfdId === currentOfdId && fdTypeIsPipeLike(receiver, currentType)) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
    }
    if (
      candidateOfdId > 0
      && candidateOfdId < maxFds
      && callNumber(receiver, "kernel_ofd_refcount", currentOfdId) > 0
      && regularFileOfdsShareIdentity(receiver, currentOfdId, candidateOfdId)
      && callBigInt(receiver, "kernel_ofd_offset", candidateOfdId) < callBigInt(receiver, "kernel_ofd_offset", currentOfdId)
    ) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
    }
    return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_apply;
  };
  const supervisorOfdSlotWriteAction = (
    receiver: Partial<KernelExports>,
    ofdId: number,
    candidateType: number,
    candidateHostFd: number,
    candidateIno: number,
    candidateOffset: bigint | number,
  ): number => {
    const maxFds = callNumber(receiver, "abi_max_fds");
    if (ofdId <= 0 || ofdId >= maxFds) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
    }
    const currentType = callNumber(receiver, "kernel_ofd_fd_type", ofdId);
    if (currentType === 0 || callNumber(receiver, "kernel_ofd_refcount", ofdId) <= 0) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_apply;
    }
    if (candidateType === 0 || candidateType !== currentType) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
    }
    if (
      candidateHostFd !== callNumber(receiver, "kernel_ofd_host_fd", ofdId)
      || candidateIno !== callNumber(receiver, "kernel_ofd_ino", ofdId)
    ) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
    }
    const candidate = typeof candidateOffset === "bigint" ? candidateOffset : BigInt(Math.trunc(candidateOffset));
    if (
      currentType === callNumber(receiver, "abi_fd_type_file")
      && candidate < callBigInt(receiver, "kernel_ofd_offset", ofdId)
    ) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
    }
    return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_apply;
  };
  const supervisorOfdSlotRestoreAction = (
    receiver: Partial<KernelExports>,
    ofdId: number,
    candidateType: number,
    candidateHostFd: number,
    candidateIno: number,
    candidateOffset: bigint | number,
    syscallNr: bigint | number,
    flags: number,
  ): number => {
    const maxFds = callNumber(receiver, "abi_max_fds");
    if (ofdId <= 0 || ofdId >= maxFds) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
    }
    const currentType = callNumber(receiver, "kernel_ofd_fd_type", ofdId);
    if (currentType === 0 || callNumber(receiver, "kernel_ofd_refcount", ofdId) <= 0) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_apply;
    }
    const referencedByFdEntry = (flags
      & DEFAULT_NUMERIC_EXPORTS.supervisor_ofd_slot_restore_flag_referenced_by_fd_entry) !== 0;
    if (!referencedByFdEntry) {
      if (candidateType === 0 || candidateType !== currentType) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
      }
      if (
        candidateHostFd !== callNumber(receiver, "kernel_ofd_host_fd", ofdId)
        || candidateIno !== callNumber(receiver, "kernel_ofd_ino", ofdId)
      ) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
      }
    }
    if (
      (flags & DEFAULT_NUMERIC_EXPORTS.supervisor_ofd_slot_restore_flag_trust_regular_file_offset) !== 0
      || currentType !== callNumber(receiver, "abi_fd_type_file")
      || candidateType !== callNumber(receiver, "abi_fd_type_file")
      || candidateHostFd !== callNumber(receiver, "kernel_ofd_host_fd", ofdId)
      || candidateIno !== callNumber(receiver, "kernel_ofd_ino", ofdId)
    ) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_apply;
    }
    const currentOffset = callBigInt(receiver, "kernel_ofd_offset", ofdId);
    const candidate = typeof candidateOffset === "bigint" ? candidateOffset : BigInt(Math.trunc(candidateOffset));
    const floor = ofdRestoreOffsetFloors.get(ofdId);
    const maxSeen = floor?.hostFd === candidateHostFd && floor.ino === candidateIno
      ? floor.offset
      : currentOffset;
    const nextMax = candidate > currentOffset ? candidate : currentOffset;
    if (nextMax > maxSeen) {
      ofdRestoreOffsetFloors.set(ofdId, { hostFd: candidateHostFd, ino: candidateIno, offset: nextMax });
    }
    if (candidate < maxSeen) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
    }
    if (candidate >= currentOffset) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_apply;
    }
    const syscall = typeof syscallNr === "bigint" ? syscallNr : BigInt(Math.trunc(syscallNr));
    return syscall === BigInt(callNumber(receiver, "abi_sys_lseek"))
      ? DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_apply
      : DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
  };
  const supervisorRegularFileOffsetRestoreAction = (
    receiver: Partial<KernelExports>,
    ofdId: number,
    snapshotHostFd: number,
    snapshotIno: number,
    snapshotOffset: bigint | number,
  ): number => {
    const maxFds = callNumber(receiver, "abi_max_fds");
    if (ofdId <= 0 || ofdId >= maxFds) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
    }
    const snapshot = typeof snapshotOffset === "bigint" ? snapshotOffset : BigInt(Math.trunc(snapshotOffset));
    if (
      callNumber(receiver, "kernel_ofd_fd_type", ofdId) !== callNumber(receiver, "abi_fd_type_file")
      || callNumber(receiver, "kernel_ofd_refcount", ofdId) <= 0
      || callNumber(receiver, "kernel_ofd_host_fd", ofdId) !== snapshotHostFd
      || callNumber(receiver, "kernel_ofd_ino", ofdId) !== snapshotIno
      || callBigInt(receiver, "kernel_ofd_offset", ofdId) >= snapshot
    ) {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
    }
    return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_apply;
  };
  const processEpollHeldOfdIds = (receiver: Partial<KernelExports>, vmPtr: number): number[] => {
    const epollType = callNumber(receiver, "abi_fd_type_epoll");
    return processHeldOfdIds(receiver, vmPtr)
      .filter((ofdId) => callNumber(receiver, "kernel_ofd_fd_type", ofdId) === epollType);
  };
  const processEpollDependentOfdIds = (
    receiver: Partial<KernelExports>,
    vmPtr: number,
  ): number[] => {
    const memory = receiver.memory ?? overrides.memory;
    if (memory === undefined) {
      return [];
    }
    const maxFds = callNumber(receiver, "abi_max_fds");
    const tableBytes = callNumber(receiver, "kernel_epoll_table_bytes");
    const instanceBytes = callNumber(receiver, "kernel_epoll_instance_bytes");
    const tablePtr = callNumber(receiver, "kernel_epoll_table_ptr");
    const tablePointerBytes = tableBytes % 8 === 0
      && Math.floor((instanceBytes - 4) / (tableBytes >>> 3)) >= 28
      ? 8
      : 4;
    const tableEntries = Math.floor(tableBytes / tablePointerBytes);
    const entryStride = tableEntries > 0 ? Math.floor((instanceBytes - 4) / tableEntries) : 0;
    const countOffset = entryStride * tableEntries;
    const view = new DataView(memory.buffer);
    if (
      tableEntries <= 0
      || entryStride < 8
      || tablePtr < 0
      || tablePtr + tableBytes > view.byteLength
    ) {
      return [];
    }
    const dependentOfdIds = new Set<number>();
    for (const epollOfdId of processEpollHeldOfdIds(receiver, vmPtr)) {
      const epollIdx = callNumber(receiver, "kernel_ofd_ino", epollOfdId);
      if (epollIdx < 0 || epollIdx >= tableEntries) {
        continue;
      }
      const instancePtr = view.getUint32(tablePtr + epollIdx * tablePointerBytes, true);
      if (instancePtr === 0 || instancePtr + instanceBytes > view.byteLength) {
        continue;
      }
      const count = Math.min(view.getUint32(instancePtr + countOffset, true), tableEntries);
      for (let i = 0; i < count; i += 1) {
        const ofdId = view.getUint32(instancePtr + i * entryStride + 4, true);
        if (ofdId > 0 && ofdId < maxFds) {
          dependentOfdIds.add(ofdId);
        }
      }
    }
    return [...dependentOfdIds];
  };
  const processSyncDependentOfdIds = (
    receiver: Partial<KernelExports>,
    vmPtr: number,
  ): number[] => {
    const eventfdType = callNumber(receiver, "abi_fd_type_eventfd");
    const timerfdType = callNumber(receiver, "abi_fd_type_timerfd");
    return processHeldOfdIds(receiver, vmPtr)
      .filter((ofdId) => {
        const fdType = callNumber(receiver, "kernel_ofd_fd_type", ofdId);
        return fdType === eventfdType || fdType === timerfdType;
      });
  };
  const processEpollDependentSyncOfdIds = (
    receiver: Partial<KernelExports>,
    vmPtr: number,
  ): number[] => {
    const eventfdType = callNumber(receiver, "abi_fd_type_eventfd");
    const timerfdType = callNumber(receiver, "abi_fd_type_timerfd");
    return processEpollDependentOfdIds(receiver, vmPtr)
      .filter((ofdId) => {
        const fdType = callNumber(receiver, "kernel_ofd_fd_type", ofdId);
        return fdType === eventfdType || fdType === timerfdType;
      });
  };
  const syscallMayClearFdEntries = (
    receiver: Partial<KernelExports>,
    syscallNr: bigint | number,
  ): boolean => {
    const value = typeof syscallNr === "bigint" ? syscallNr : BigInt(Math.trunc(syscallNr));
    return value === BigInt(callNumber(receiver, "abi_sys_close"))
      || value === BigInt(callNumber(receiver, "abi_sys_close_range"))
      || value === BigInt(callNumber(receiver, "abi_sys_execve"));
  };
  let latestBackingFillSpan = { offset: 0, end: 0, length: 0 };
  const stubs = Object.fromEntries(
    requiredSupervisorRuntimeExportNames().map((name) => [
      name,
      () => DEFAULT_NUMERIC_EXPORTS[name] ?? 0,
    ]),
  );
  return {
    ...stubs,
    supervisor_blocked_poll_wait_timeout_ms(this: KernelExports | undefined, vmPtr: number) {
      return supervisorPollWaitTimeoutMs(currentExports(this), vmPtr);
    },
    supervisor_blocked_epoll_wait_timeout_ms(this: KernelExports | undefined, vmPtr: number) {
      return supervisorEpollWaitTimeoutMs(currentExports(this), vmPtr);
    },
    supervisor_blocked_status_is_host_wait(this: KernelExports | undefined, status: number) {
      return supervisorStatusIsHostWait(currentExports(this), status) ? 1 : 0;
    },
    supervisor_blocked_status_is_connect_wait(this: KernelExports | undefined, status: number) {
      return supervisorStatusIsConnectWait(currentExports(this), status) ? 1 : 0;
    },
    supervisor_blocked_status_is_epoll_wait(this: KernelExports | undefined, status: number) {
      return supervisorStatusIsEpollWait(currentExports(this), status) ? 1 : 0;
    },
    supervisor_blocked_status_is_pipe_wait(this: KernelExports | undefined, status: number) {
      return supervisorStatusIsPipeWait(currentExports(this), status) ? 1 : 0;
    },
    supervisor_blocked_status_is_wait4(this: KernelExports | undefined, status: number) {
      return supervisorStatusIsWait4(currentExports(this), status) ? 1 : 0;
    },
    supervisor_blocked_status_is_socket_wait(this: KernelExports | undefined, status: number) {
      return supervisorStatusIsSocketWait(currentExports(this), status) ? 1 : 0;
    },
    supervisor_blocked_status_needs_network_sidecar_restore(this: KernelExports | undefined, status: number) {
      return supervisorStatusNeedsNetworkSidecarRestore(currentExports(this), status) ? 1 : 0;
    },
    supervisor_blocked_status_syncs_socket_after_resume(this: KernelExports | undefined, status: number) {
      return supervisorStatusSyncsSocketAfterResume(currentExports(this), status) ? 1 : 0;
    },
    supervisor_blocked_status_uses_fork_shared_state_sync(this: KernelExports | undefined, status: number) {
      return supervisorStatusUsesForkSharedStateSync(currentExports(this), status) ? 1 : 0;
    },
    supervisor_process_status_is_lifecycle_handoff(this: KernelExports | undefined, status: number) {
      return supervisorProcessStatusIsLifecycleHandoff(currentExports(this), status) ? 1 : 0;
    },
    supervisor_process_status_is_execve_spawn(this: KernelExports | undefined, status: number) {
      return supervisorProcessStatusIsExecveSpawn(currentExports(this), status) ? 1 : 0;
    },
    supervisor_process_status_is_clone_spawn(this: KernelExports | undefined, status: number) {
      return supervisorProcessStatusIsCloneSpawn(currentExports(this), status) ? 1 : 0;
    },
    supervisor_process_status_is_vfork_wait(this: KernelExports | undefined, status: number) {
      return supervisorProcessStatusIsVforkWait(currentExports(this), status) ? 1 : 0;
    },
    supervisor_process_status_allows_pid_mismatch(this: KernelExports | undefined, status: number) {
      return supervisorProcessStatusAllowsPidMismatch(currentExports(this), status) ? 1 : 0;
    },
    supervisor_process_status_defers_kernel_state_adoption(this: KernelExports | undefined, status: number) {
      return supervisorProcessStatusDefersKernelStateAdoption(currentExports(this), status) ? 1 : 0;
    },
    supervisor_vfork_child_owner_mismatch(vmPtr: number, runtimePid: number) {
      const result = overrides.supervisor_vfork_child_owner_mismatch?.(vmPtr, runtimePid) ?? 0;
      return typeof result === "bigint" ? Number(result) : result;
    },
    supervisor_process_owner_matches(this: KernelExports | undefined, vmPtr: number, runtimePid: number) {
      const result = overrides.supervisor_process_owner_matches?.(vmPtr, runtimePid)
        ?? (callOptionalNumber(currentExports(this), "process_current_pid", vmPtr) === runtimePid ? 1 : 0);
      return typeof result === "bigint" ? Number(result) : result;
    },
    supervisor_process_owner_mismatch(this: KernelExports | undefined, vmPtr: number, runtimePid: number) {
      const currentPid = callOptionalNumber(currentExports(this), "process_current_pid", vmPtr) ?? 0;
      const result = overrides.supervisor_process_owner_mismatch?.(vmPtr, runtimePid)
        ?? (currentPid > 0 && currentPid !== runtimePid ? 1 : 0);
      return typeof result === "bigint" ? Number(result) : result;
    },
    supervisor_process_owner_absent(this: KernelExports | undefined, vmPtr: number) {
      const receiver = currentExports(this);
      const currentPid = callOptionalNumber(receiver, "process_current_pid", vmPtr) ?? 0;
      const threadCount = callOptionalNumber(receiver, "thread_process_thread_count", vmPtr) ?? 0;
      const result = overrides.supervisor_process_owner_absent?.(vmPtr)
        ?? (currentPid === 0 && threadCount === 0 ? 1 : 0);
      return typeof result === "bigint" ? Number(result) : result;
    },
    supervisor_process_owner_live_matches(this: KernelExports | undefined, vmPtr: number, runtimePid: number) {
      const receiver = currentExports(this);
      const currentPid = callOptionalNumber(receiver, "process_current_pid", vmPtr) ?? 0;
      const threadCount = callOptionalNumber(receiver, "thread_process_thread_count", vmPtr) ?? 0;
      const result = overrides.supervisor_process_owner_live_matches?.(vmPtr, runtimePid)
        ?? (currentPid === runtimePid && threadCount > 0 ? 1 : 0);
      return typeof result === "bigint" ? Number(result) : result;
    },
    supervisor_process_owner_missing_or_invalid(this: KernelExports | undefined, vmPtr: number) {
      const currentPid = callOptionalNumber(currentExports(this), "process_current_pid", vmPtr) ?? 0;
      const result = overrides.supervisor_process_owner_missing_or_invalid?.(vmPtr)
        ?? (currentPid === 0 || currentPid > 0xffff ? 1 : 0);
      return typeof result === "bigint" ? Number(result) : result;
    },
    supervisor_process_identity_matches(
      this: KernelExports | undefined,
      vmPtr: number,
      runtimePid: number,
      processPtr: number,
    ) {
      const receiver = currentExports(this);
      const currentPid = callOptionalNumber(receiver, "process_current_pid", vmPtr) ?? 0;
      const currentProcessPtr = callOptionalNumber(receiver, "thread_process_ptr", vmPtr) ?? 0;
      const result = overrides.supervisor_process_identity_matches?.(vmPtr, runtimePid, processPtr)
        ?? (currentPid === runtimePid && currentProcessPtr === processPtr ? 1 : 0);
      return typeof result === "bigint" ? Number(result) : result;
    },
    supervisor_blocked_poll_wait_has_socket(this: KernelExports | undefined, vmPtr: number) {
      if (vmPtr === 0) {
        return -14;
      }
      return supervisorPollWaitHasSocket(currentExports(this), vmPtr) ? 1 : 0;
    },
    supervisor_blocked_pipe_wait_has_ready_events(this: KernelExports | undefined, vmPtr: number) {
      if (vmPtr === 0) {
        return -14;
      }
      return supervisorPipeWaitHasReadyEvents(currentExports(this), vmPtr) ? 1 : 0;
    },
    supervisor_blocked_wait_may_depend_on_network(this: KernelExports | undefined, vmPtr: number) {
      if (vmPtr === 0) {
        return -14;
      }
      return supervisorWaitMayDependOnNetwork(currentExports(this), vmPtr) ? 1 : 0;
    },
    supervisor_blocked_wait_should_pump_network(this: KernelExports | undefined, vmPtr: number) {
      if (vmPtr === 0) {
        return -14;
      }
      const receiver = currentExports(this);
      const status = statusFor(receiver, vmPtr);
      if (status === pipeWaitStatusFor(receiver)) {
        return supervisorWaitMayDependOnNetwork(receiver, vmPtr)
          || !supervisorPipeWaitHasReadyEvents(receiver, vmPtr)
          ? 1
          : 0;
      }
      return status === callNumber(receiver, "abi_status_socket_recv")
        || status === callNumber(receiver, "abi_status_socket_send")
        || status === callNumber(receiver, "abi_status_connect")
        ? 1
        : 0;
    },
    supervisor_blocked_epoll_wait_retry_required(this: KernelExports | undefined, vmPtr: number) {
      if (vmPtr === 0) {
        return -14;
      }
      const receiver = currentExports(this);
      const status = statusFor(receiver, vmPtr);
      return status === callNumber(receiver, "abi_status_epoll_wait")
        && callNumber(receiver, "thread_process_thread_count", vmPtr) > 1
        ? 1
        : 0;
    },
    supervisor_blocked_resume_delegation(this: KernelExports | undefined, vmPtr: number, flags: number) {
      const kernelWorkerAvailable = (flags
        & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_flag_kernel_worker_available) !== 0;
      const receiver = currentExports(this);
      const status = Number(receiver?.thread_status?.(vmPtr) ?? overrides.thread_status?.(vmPtr) ?? 0);
      if (!kernelWorkerAvailable) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_direct;
      }
      if (vmPtr === 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_unsupported;
      }
      if (status === Number(receiver?.abi_status_wait4?.() ?? overrides.abi_status_wait4?.() ?? -1)) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_wait4_kernel_worker;
      }
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_kernel_worker;
    },
    supervisor_blocked_pipe_sync_before_retry_action(_vmPtr: number, flags: number) {
      if ((flags
        & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_pipe_sync_before_retry_action_request_sync;
      }
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_pipe_sync_before_retry_action_none;
    },
    supervisor_blocked_pipe_sync_before_retry_requested() {
      return 0;
    },
    supervisor_blocked_pipe_sync_before_retry_clear() {
      return 0;
    },
    supervisor_blocked_status_transition_action(this: KernelExports | undefined, vmPtr: number, status: number, flags: number) {
      if (vmPtr === 0) {
        return -14;
      }
      const receiver = currentExports(this);
      const kernelWorkerAvailable = (flags
        & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_status_transition_flag_kernel_worker_available) !== 0;
      if (!kernelWorkerAvailable) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_status_transition_action_block;
      }
      const pipeWaitStatus = supervisorStatusIsPipeWait(receiver, status);
      const networkConnectionsAvailable = (flags
        & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_status_transition_flag_network_connections_available) !== 0;
      if (
        pipeWaitStatus
        && networkConnectionsAvailable
        && supervisorPollWaitHasSocket(receiver, vmPtr)
      ) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_status_transition_action_sync_socket;
      }
      if (supervisorStatusIsWait4(receiver, status)) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_status_transition_action_sync_pipe;
      }
      const finitePollTimeout = pipeWaitStatus && supervisorPollWaitTimeoutMs(receiver, vmPtr) >= 0;
      if (!supervisorStatusUsesForkSharedStateSync(receiver, status) || finitePollTimeout) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_status_transition_action_block;
      }
      if (pipeWaitStatus) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_status_transition_action_sync_pipe_and_fork_shared;
      }
      if (supervisorStatusIsSocketWait(receiver, status)) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_status_transition_action_sync_socket_and_fork_shared;
      }
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_status_transition_action_sync_fork_shared;
    },
    supervisor_blocked_wait_network_pump_action(vmPtr: number) {
      if (vmPtr === 0) {
        return -14;
      }
      if (!networkPumpBackoffs.has(vmPtr)) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_network_pump_action_pump;
      }
      networkPumpBackoffs.delete(vmPtr);
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_network_pump_action_skip;
    },
    supervisor_complete_blocked_wait_network_pump(vmPtr: number, changed: number) {
      if (vmPtr === 0) {
        return -14;
      }
      if (changed === 0) {
        networkPumpBackoffs.add(vmPtr);
      } else {
        networkPumpBackoffs.delete(vmPtr);
      }
      return 0;
    },
    supervisor_clear_blocked_wait_network_pump(vmPtr: number) {
      if (vmPtr === 0) {
        return -14;
      }
      networkPumpBackoffs.delete(vmPtr);
      return 0;
    },
    supervisor_state_sync_request(_vmPtr: number, kind: number, flags: number) {
      const key = `${_vmPtr}:${kind}`;
      const request = stateSyncRequests.get(key) ?? { queuedRunFlags: 0, inFlight: false };
      request.queuedRunFlags |= DEFAULT_NUMERIC_EXPORTS.supervisor_state_sync_run_flag_run;
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_state_sync_request_flag_wake_waiters) !== 0) {
        request.queuedRunFlags |= DEFAULT_NUMERIC_EXPORTS.supervisor_state_sync_run_flag_wake_waiters;
      }
      const shouldStart = !request.inFlight;
      request.inFlight = true;
      stateSyncRequests.set(key, request);
      return shouldStart ? 1 : 0;
    },
    supervisor_state_sync_next_run_flags(_vmPtr: number, kind: number) {
      const key = `${_vmPtr}:${kind}`;
      const request = stateSyncRequests.get(key);
      if (!request) {
        return 0;
      }
      const flags = request.queuedRunFlags;
      request.queuedRunFlags = 0;
      return flags;
    },
    supervisor_state_sync_complete(_vmPtr: number, kind: number) {
      const key = `${_vmPtr}:${kind}`;
      const request = stateSyncRequests.get(key);
      if (!request) {
        return 0;
      }
      if (request.queuedRunFlags !== 0) {
        return 1;
      }
      stateSyncRequests.delete(key);
      return 0;
    },
    supervisor_state_sync_clear(_vmPtr: number, kind: number) {
      stateSyncRequests.delete(`${_vmPtr}:${kind}`);
      return 0;
    },
    supervisor_process_held_ofd_refcount(
      this: KernelExports | undefined,
      vmPtr: number,
      ofdId: number,
    ) {
      return processHeldOfdRefcount(currentExports(this), vmPtr, ofdId);
    },
    supervisor_process_holds_regular_file_ofd(
      this: KernelExports | undefined,
      vmPtr: number,
      ofdId: number,
    ) {
      const receiver = currentExports(this);
      return processHeldOfdRefcount(receiver, vmPtr, ofdId) > 0
        && callNumber(receiver, "kernel_ofd_fd_type", ofdId) === callNumber(receiver, "abi_fd_type_file")
        ? 1
        : 0;
    },
    supervisor_process_fd_is_open(this: KernelExports | undefined, vmPtr: number, fd: number) {
      const receiver = currentExports(this);
      const maxFds = callNumber(receiver, "abi_max_fds");
      if (fd < 0 || fd >= maxFds) {
        return 0;
      }
      const ofdId = callNumber(receiver, "process_fd_ofd_id", vmPtr, fd);
      return ofdId > 0 && ofdId < maxFds ? 1 : 0;
    },
    supervisor_process_ofd_for_fd(this: KernelExports | undefined, vmPtr: number, fd: number) {
      const receiver = currentExports(this);
      const maxFds = callNumber(receiver, "abi_max_fds");
      if (fd < 0 || fd >= maxFds) {
        return 0;
      }
      const ofdId = callNumber(receiver, "process_fd_ofd_id", vmPtr, fd);
      return ofdId > 0 && ofdId < maxFds ? ofdId : 0;
    },
    supervisor_process_fd_flags_for_fd(this: KernelExports | undefined, vmPtr: number, fd: number) {
      const receiver = currentExports(this);
      const maxFds = callNumber(receiver, "abi_max_fds");
      if (fd < 0 || fd >= maxFds) {
        return 0;
      }
      return callNumber(receiver, "process_fd_flags", vmPtr, fd);
    },
    supervisor_ofd_fd_type(this: KernelExports | undefined, ofdId: number) {
      const receiver = currentExports(this);
      if (ofdId <= 0 || ofdId >= callNumber(receiver, "abi_max_fds")) {
        return 0;
      }
      return callOptionalNumber(receiver, "kernel_ofd_fd_type", ofdId) ?? 0;
    },
    supervisor_ofd_host_fd(this: KernelExports | undefined, ofdId: number) {
      const receiver = currentExports(this);
      if (ofdId <= 0 || ofdId >= callNumber(receiver, "abi_max_fds")) {
        return -1;
      }
      return callOptionalNumber(receiver, "kernel_ofd_host_fd", ofdId) ?? -1;
    },
    supervisor_ofd_status_flags(this: KernelExports | undefined, ofdId: number) {
      const receiver = currentExports(this);
      if (ofdId <= 0 || ofdId >= callNumber(receiver, "abi_max_fds")) {
        return 0;
      }
      return callOptionalNumber(receiver, "kernel_ofd_status_flags", ofdId) ?? 0;
    },
    supervisor_ofd_ino(this: KernelExports | undefined, ofdId: number) {
      const receiver = currentExports(this);
      if (ofdId <= 0 || ofdId >= callNumber(receiver, "abi_max_fds")) {
        return 0;
      }
      return callOptionalNumber(receiver, "kernel_ofd_ino", ofdId) ?? 0;
    },
    supervisor_ofd_refcount(this: KernelExports | undefined, ofdId: number) {
      const receiver = currentExports(this);
      if (ofdId <= 0 || ofdId >= callNumber(receiver, "abi_max_fds")) {
        return 0;
      }
      return callOptionalNumber(receiver, "kernel_ofd_refcount", ofdId) ?? 0;
    },
    supervisor_ofd_reconcile_refcount(
      this: KernelExports | undefined,
      ofdId: number,
      expectedRefcount: number,
    ) {
      const receiver = currentExports(this);
      if (ofdId <= 0 || ofdId >= callNumber(receiver, "abi_max_fds")) {
        return 0;
      }
      let currentRefcount = callOptionalNumber(receiver, "kernel_ofd_refcount", ofdId) ?? 0;
      const expected = Math.trunc(expectedRefcount);
      if (expected < 0) {
        return currentRefcount;
      }
      const retain = (receiver.kernel_retain_ofd ?? overrides.kernel_retain_ofd) as
        | ((targetOfdId: number) => void)
        | undefined;
      const release = (receiver.kernel_release_ofd ?? overrides.kernel_release_ofd) as
        | ((targetOfdId: number) => void)
        | undefined;
      while (currentRefcount < expected) {
        retain?.(ofdId);
        currentRefcount += 1;
      }
      while (currentRefcount > expected) {
        release?.(ofdId);
        currentRefcount -= 1;
      }
      return callOptionalNumber(receiver, "kernel_ofd_refcount", ofdId) ?? currentRefcount;
    },
    supervisor_ofd_offset(this: KernelExports | undefined, ofdId: number) {
      const receiver = currentExports(this);
      if (ofdId <= 0 || ofdId >= callNumber(receiver, "abi_max_fds")) {
        return 0n;
      }
      return callOptionalBigInt(receiver, "kernel_ofd_offset", ofdId) ?? 0n;
    },
    supervisor_ofd_is_regular_file(this: KernelExports | undefined, ofdId: number) {
      const receiver = currentExports(this);
      return ofdId > 0
        && ofdId < callNumber(receiver, "abi_max_fds")
        && callNumber(receiver, "kernel_ofd_fd_type", ofdId) === callNumber(receiver, "abi_fd_type_file")
        ? 1
        : 0;
    },
    supervisor_pipe_sync_ofd_candidate(this: KernelExports | undefined, ofdId: number) {
      const receiver = currentExports(this);
      if (ofdId <= 0 || ofdId >= callNumber(receiver, "abi_max_fds")) {
        return 0;
      }
      return fdTypeIsPipeLike(receiver, callNumber(receiver, "kernel_ofd_fd_type", ofdId)) ? 1 : 0;
    },
    supervisor_process_pipe_slot_for_fd(
      this: KernelExports | undefined,
      vmPtr: number,
      fd: number,
    ) {
      return processPipeSlotForFd(currentExports(this), vmPtr, fd);
    },
    supervisor_process_pipe_ofd_for_fd(
      this: KernelExports | undefined,
      vmPtr: number,
      fd: number,
    ) {
      return processPipeOfdForFd(currentExports(this), vmPtr, fd);
    },
    supervisor_process_pipe_host_fd_for_fd(
      this: KernelExports | undefined,
      vmPtr: number,
      fd: number,
    ) {
      const receiver = currentExports(this);
      const ofdId = processPipeOfdForFd(receiver, vmPtr, fd);
      return ofdId > 0 ? callNumber(receiver, "kernel_ofd_host_fd", ofdId) : -1;
    },
    supervisor_fork_shared_held_ofd_track_flag_shared() {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fork_shared_held_ofd_track_flag_shared;
    },
    supervisor_fork_shared_held_ofd_track_flag_host_stdio_pipe() {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fork_shared_held_ofd_track_flag_host_stdio_pipe;
    },
    supervisor_fork_shared_held_ofd_track_flag_stdio_fd() {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fork_shared_held_ofd_track_flag_stdio_fd;
    },
    supervisor_fork_shared_held_ofd_track_flag_fd_entry_source() {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fork_shared_held_ofd_track_flag_fd_entry_source;
    },
    supervisor_fork_shared_held_ofd_track_action(
      this: KernelExports | undefined,
      ofdId: number,
      flags: number,
    ) {
      const receiver = currentExports(this);
      if (ofdId <= 0 || ofdId >= callNumber(receiver, "abi_max_fds")) {
        return 0;
      }
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_fork_shared_held_ofd_track_flag_host_stdio_pipe) !== 0) {
        return 0;
      }
      if (
        (flags & (
          DEFAULT_NUMERIC_EXPORTS.supervisor_fork_shared_held_ofd_track_flag_shared
          | DEFAULT_NUMERIC_EXPORTS.supervisor_fork_shared_held_ofd_track_flag_stdio_fd
          | DEFAULT_NUMERIC_EXPORTS.supervisor_fork_shared_held_ofd_track_flag_fd_entry_source
        )) !== 0
      ) {
        return 1;
      }
      return callNumber(receiver, "kernel_ofd_fd_type", ofdId) === callNumber(receiver, "abi_fd_type_file")
        ? 0
        : 1;
    },
    supervisor_fork_shared_ofd_identity_key(this: KernelExports | undefined, ofdId: number) {
      return forkSharedOfdIdentityKey(currentExports(this), ofdId);
    },
    supervisor_fork_shared_ofd_publish_action(
      this: KernelExports | undefined,
      ofdId: number,
      peerFlags: number,
    ) {
      return forkSharedOfdPublishAction(currentExports(this), ofdId, peerFlags);
    },
    supervisor_fork_shared_effect_sync_action(
      _hasEffect: number,
      _hasTrackedState: number,
      _sourceHoldsRegularFile: number,
      _regularFileHasPeer: number,
    ) {
      return forkSharedEffectSyncAction(
        _hasEffect,
        _hasTrackedState,
        _sourceHoldsRegularFile,
        _regularFileHasPeer,
      );
    },
    supervisor_fd_entry_restore_action(
      this: KernelExports | undefined,
      vmPtr: number,
      fd: number,
      snapshotOfdId: number,
      syscallNr: bigint | number,
      flags: number,
    ) {
      const receiver = currentExports(this);
      const maxFds = callNumber(receiver, "abi_max_fds");
      if (fd < 0 || fd >= maxFds) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
      }
      if (syscallMayClearFdEntries(receiver, syscallNr)) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_apply;
      }
      const currentOfdId = callNumber(receiver, "process_fd_ofd_id", vmPtr, fd);
      if (currentOfdId <= 0 || currentOfdId >= maxFds || currentOfdId === snapshotOfdId) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_apply;
      }
      if (
        (flags & DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_flag_preserve_live_regular_file) !== 0
        && callNumber(receiver, "kernel_ofd_fd_type", currentOfdId) === callNumber(receiver, "abi_fd_type_file")
        && callNumber(receiver, "kernel_ofd_refcount", currentOfdId) > 0
      ) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
      }
      if (
        (flags & DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_flag_preserve_live_epoll_dependent) !== 0
        && processEpollDependentOfdIds(receiver, vmPtr).includes(currentOfdId)
      ) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_skip;
      }
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fd_entry_restore_action_apply;
    },
    supervisor_fd_entry_guest_write_action(
      this: KernelExports | undefined,
      vmPtr: number,
      fd: number,
      candidateOfdId: number,
      explicitOfdId: number,
    ) {
      return supervisorFdEntryGuestWriteAction(
        currentExports(this),
        vmPtr,
        fd,
        candidateOfdId,
        explicitOfdId,
      );
    },
    supervisor_ofd_slot_write_action(
      this: KernelExports | undefined,
      ofdId: number,
      candidateType: number,
      candidateHostFd: number,
      candidateIno: number,
      candidateOffset: bigint | number,
    ) {
      return supervisorOfdSlotWriteAction(
        currentExports(this),
        ofdId,
        candidateType,
        candidateHostFd,
        candidateIno,
        candidateOffset,
      );
    },
    supervisor_ofd_slot_restore_flag_referenced_by_fd_entry() {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_ofd_slot_restore_flag_referenced_by_fd_entry;
    },
    supervisor_ofd_slot_restore_flag_trust_regular_file_offset() {
      return DEFAULT_NUMERIC_EXPORTS.supervisor_ofd_slot_restore_flag_trust_regular_file_offset;
    },
    supervisor_ofd_slot_restore_action(
      this: KernelExports | undefined,
      ofdId: number,
      candidateType: number,
      candidateHostFd: number,
      candidateIno: number,
      candidateOffset: bigint | number,
      syscallNr: bigint | number,
      flags: number,
    ) {
      return supervisorOfdSlotRestoreAction(
        currentExports(this),
        ofdId,
        candidateType,
        candidateHostFd,
        candidateIno,
        candidateOffset,
        syscallNr,
        flags,
      );
    },
    supervisor_regular_file_offset_restore_action(
      this: KernelExports | undefined,
      ofdId: number,
      snapshotHostFd: number,
      snapshotIno: number,
      snapshotOffset: bigint | number,
    ) {
      return supervisorRegularFileOffsetRestoreAction(
        currentExports(this),
        ofdId,
        snapshotHostFd,
        snapshotIno,
        snapshotOffset,
      );
    },
    supervisor_process_epoll_held_ofd(
      this: KernelExports | undefined,
      vmPtr: number,
      index: number,
    ) {
      return processEpollHeldOfdIds(currentExports(this), vmPtr)[index] ?? 0;
    },
    supervisor_process_epoll_dependent_ofd(
      this: KernelExports | undefined,
      vmPtr: number,
      index: number,
    ) {
      return processEpollDependentOfdIds(currentExports(this), vmPtr)[index] ?? 0;
    },
    supervisor_process_sync_dependent_ofd(
      this: KernelExports | undefined,
      vmPtr: number,
      index: number,
    ) {
      return processSyncDependentOfdIds(currentExports(this), vmPtr)[index] ?? 0;
    },
    supervisor_process_epoll_dependent_sync_ofd(
      this: KernelExports | undefined,
      vmPtr: number,
      index: number,
    ) {
      return processEpollDependentSyncOfdIds(currentExports(this), vmPtr)[index] ?? 0;
    },
    supervisor_process_epoll_depends_on_ofd(
      this: KernelExports | undefined,
      vmPtr: number,
      ofdId: number,
    ) {
      return processEpollDependentOfdIds(currentExports(this), vmPtr).includes(ofdId) ? 1 : 0;
    },
    supervisor_blocked_epoll_dependent_sync_ofd(
      this: KernelExports | undefined,
      vmPtr: number,
      index: number,
    ) {
      return processEpollDependentSyncOfdIds(currentExports(this), vmPtr)[index] ?? 0;
    },
    supervisor_blocked_wait_wake_mask_for_sync(_source: number) {
      return supervisorWaitWakeMaskForSync(_source);
    },
    supervisor_blocked_wait_status_matches_wake_source(
      this: KernelExports | undefined,
      status: number,
      source: number,
    ) {
      return supervisorWaitStatusMatchesWakeSource(currentExports(this), status, source);
    },
    supervisor_stdio_output_settle_max_passes(kind: number) {
      switch (kind) {
        case DEFAULT_NUMERIC_EXPORTS.supervisor_stdio_output_settle_kind_visible:
          return 4;
        case DEFAULT_NUMERIC_EXPORTS.supervisor_stdio_output_settle_kind_inherited_child:
          return 6;
        default:
          return -22;
      }
    },
    supervisor_stdio_output_settle_stable_passes(kind: number) {
      switch (kind) {
        case DEFAULT_NUMERIC_EXPORTS.supervisor_stdio_output_settle_kind_visible:
        case DEFAULT_NUMERIC_EXPORTS.supervisor_stdio_output_settle_kind_inherited_child:
          return 2;
        default:
          return -22;
      }
    },
    supervisor_stdio_pipe_read_progress_action(_vmPtr: number, _pipeSlot: number, drained: number) {
      return drained === 0
        ? DEFAULT_NUMERIC_EXPORTS.supervisor_stdio_pipe_read_progress_action_none
        : DEFAULT_NUMERIC_EXPORTS.supervisor_stdio_pipe_read_progress_action_snapshot;
    },
    supervisor_fs_response_disposition(
      this: KernelExports | undefined,
      vmPtr: number,
      runtimePid: number,
      opcode: number,
      result: bigint | number,
      ownerAsyncStatus: number,
      lastStatus: number,
    ) {
      const receiver = currentExports(this);
      const currentPid = callNumber(receiver, "process_current_pid", vmPtr) || runtimePid;
      const processOwnerMatches = currentPid <= 0 || runtimePid <= 0 || currentPid === runtimePid;
      const numericResult = Number(BigInt.asIntN(64, typeof result === "bigint" ? result : BigInt(result)));
      const failedExecveChildOwner = opcode === FS_OP_EXECVE && numericResult < 0 && !processOwnerMatches;
      const execveSpawn = callNumber(receiver, "abi_status_execve_spawn");
      const resolvedToExecveSpawn = opcode === FS_OP_EXECVE
        && execveSpawn !== 0
        && (
          callNumber(receiver, "thread_status", vmPtr) === execveSpawn
          || ownerAsyncStatus === execveSpawn
          || lastStatus === execveSpawn
        );
      let disposition = 0;
      if (processOwnerMatches) {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_process_owner_matches;
      }
      if (failedExecveChildOwner) {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_store_pending_kernel_state;
      } else {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_adopt_kernel_state;
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_apply_guest_memory_writes;
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_clear_tombstones;
      }
      if (!failedExecveChildOwner && processOwnerMatches) {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_apply_local_metadata;
      }
      if (!failedExecveChildOwner && opcode !== FS_OP_MMAP) {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_queue_thread_worker_writes;
      }
      if (opcode === FS_OP_CLOSE || opcode === FS_OP_EXECVE) {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_fd_response_may_clear_live_fds;
      }
      if (resolvedToExecveSpawn) {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_resolved_to_execve_spawn;
      }
      return disposition;
    },
    supervisor_fs_backing_fill_span_decision(
      _opcode: number,
      offsetRaw: bigint | number,
      lengthRaw: bigint | number,
      expectedSizeRaw: bigint | number,
    ) {
      const offset = Number(offsetRaw);
      const length = Number(lengthRaw);
      const expectedSize = Number(expectedSizeRaw);
      latestBackingFillSpan = { offset: 0, end: 0, length: 0 };
      if (
        !Number.isSafeInteger(offset)
        || !Number.isSafeInteger(length)
        || !Number.isSafeInteger(expectedSize)
        || offset < 0
        || length <= 0
        || expectedSize <= 0
        || offset >= expectedSize
      ) {
        return -22;
      }
      const alignedOffset = Math.floor(offset / DEFAULT_RESIDENT_PAGE_SIZE) * DEFAULT_RESIDENT_PAGE_SIZE;
      const requestedEnd = offset + length;
      const alignedEnd = Math.min(
        expectedSize,
        Math.ceil(requestedEnd / DEFAULT_RESIDENT_PAGE_SIZE) * DEFAULT_RESIDENT_PAGE_SIZE,
      );
      if (alignedEnd <= alignedOffset) {
        return -22;
      }
      latestBackingFillSpan = {
        offset: alignedOffset,
        end: alignedEnd,
        length: alignedEnd - alignedOffset,
      };
      return 0;
    },
    supervisor_fs_backing_fill_span_offset() {
      return latestBackingFillSpan.offset;
    },
    supervisor_fs_backing_fill_span_end() {
      return latestBackingFillSpan.end;
    },
    supervisor_fs_backing_fill_span_length() {
      return latestBackingFillSpan.length;
    },
    supervisor_fs_metadata_full_sync_decision(treeMutated: number, handledMetadataMutation: number) {
      return treeMutated !== 0 && handledMetadataMutation === 0
        ? DEFAULT_NUMERIC_EXPORTS.supervisor_fs_metadata_full_sync_action_required
        : DEFAULT_NUMERIC_EXPORTS.supervisor_fs_metadata_full_sync_action_none;
    },
    supervisor_fs_local_metadata_action(
      opcode: number,
      effects: number,
      result: bigint | number,
      hasPrimaryPath: number,
      hasSecondaryPath: number,
    ) {
      const signedResult = Number(BigInt.asIntN(64, typeof result === "bigint" ? result : BigInt(result)));
      if (signedResult < 0 || hasPrimaryPath === 0 || (effects & 1) === 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fs_local_metadata_action_none;
      }
      if (opcode === FS_OP_UNLINK) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fs_local_metadata_action_tombstone_primary;
      }
      if (opcode === FS_OP_OPENAT || opcode === FS_OP_MKDIR) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fs_local_metadata_action_forget_primary;
      }
      if (opcode === FS_OP_RENAME && hasSecondaryPath !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fs_local_metadata_action_forget_secondary;
      }
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fs_local_metadata_action_none;
    },
    supervisor_fs_pending_mmap_guest_write_ptr() {
      return 0;
    },
    supervisor_fs_pending_mmap_guest_write_len() {
      return 0;
    },
    supervisor_fs_pending_mmap_guest_write_snapshot_len() {
      return 0;
    },
    supervisor_fs_resident_page_slot_decision(_existingSlot: number, _emptySlot: number, _cleanEvictSlot: number) {
      if (_existingSlot >= 0) {
        return _existingSlot;
      }
      if (_emptySlot >= 0) {
        return _emptySlot;
      }
      if (_cleanEvictSlot >= 0) {
        return _cleanEvictSlot;
      }
      return -1;
    },
    supervisor_fs_resident_page_evict_status(_recordFound: number, _dirty: number) {
      if (_recordFound === 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fs_resident_page_evict_not_found;
      }
      if (_dirty !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fs_resident_page_evict_dirty_pinned;
      }
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fs_resident_page_evict_evicted;
    },
    supervisor_blocked_wait_retry_delay_ms: () => 0,
    supervisor_blocked_wait_active_child_pid(this: KernelExports | undefined, vmPtr: number) {
      const receiver = currentExports(this);
      const currentPid = Number(receiver?.process_current_pid?.(vmPtr) ?? overrides.process_current_pid?.(vmPtr) ?? 0);
      return Number(receiver?.process_active_child_pid?.(currentPid) ?? overrides.process_active_child_pid?.(currentPid) ?? 0);
    },
    supervisor_blocked_wait_retry_reason(this: KernelExports | undefined, vmPtr: number, flags: number) {
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_flag_process_gate) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_process_gate;
      }
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_flag_active_child) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_active_child;
      }
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_flag_passive_wait) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_passive_wait;
      }
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_flag_host_progress) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_immediate;
      }
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_flag_thread_init_pending) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_standard_defer;
      }
      const activeChildPid = Number(this?.supervisor_blocked_wait_active_child_pid?.(vmPtr) ?? 0);
      if (activeChildPid > 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_active_child;
      }
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_default;
    },
    supervisor_blocked_retry_action(this: KernelExports | undefined, vmPtr: number, flags: number) {
      const receiver = currentExports(this);
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_flag_supervisor_timer_armed) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_defer_supervisor_timer;
      }
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_flag_rpc_pending) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_defer_rpc_pending;
      }
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_flag_owner_async) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_defer_owner_async;
      }

      const status = Number(receiver?.thread_status?.(vmPtr) ?? overrides.thread_status?.(vmPtr) ?? 0);
      const kernelWorkerAvailable =
        (flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_flag_kernel_worker_available) !== 0;
      if (
        status === Number(receiver?.abi_status_epoll_wait?.() ?? overrides.abi_status_epoll_wait?.() ?? -1)
        && (flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_flag_thread_init_pending) !== 0
      ) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_defer_thread_init_pending;
      }
      if (
        (flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_flag_wait_timer_already_checked) === 0
        && Number(receiver?.supervisor_blocked_wait_timer_required?.(vmPtr) ?? 0) !== 0
      ) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_arm_wait_timer;
      }
      if (Number(receiver?.supervisor_blocked_wait_active_child_pid?.(vmPtr) ?? 0) > 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_park_active_child;
      }
      if (status === Number(receiver?.abi_status_wait4?.() ?? overrides.abi_status_wait4?.() ?? -1)) {
        return kernelWorkerAvailable
          ? DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_resume_wait4_kernel_worker
          : DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_resume_wait4;
      }
      if (
        (flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_flag_fork_shared_sync_pending) !== 0
        && Number(receiver?.supervisor_blocked_poll_wait_timeout_ms?.(vmPtr) ?? -1) < 0
      ) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_defer_fork_shared_sync;
      }
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_flag_running_worker) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_defer_running_worker;
      }

      const retryReason = Number(receiver?.supervisor_blocked_wait_retry_reason?.(vmPtr, 0) ?? 0);
      if (retryReason === DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_process_gate) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_defer_process_gate;
      }
      if (
        (flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_flag_network_connections_available) !== 0
        && Number(receiver?.supervisor_blocked_wait_should_pump_network?.(vmPtr) ?? 0) !== 0
        && Number(receiver?.supervisor_blocked_wait_network_pump_action?.(vmPtr) ?? 0)
          === DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_network_pump_action_pump
      ) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_pump_network;
      }
      if (
        status === Number(receiver?.abi_status_epoll_wait?.() ?? overrides.abi_status_epoll_wait?.() ?? -1)
        && kernelWorkerAvailable
      ) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_epoll_kernel_worker;
      }
      if (status === Number(receiver?.abi_status_pipe_wait?.() ?? overrides.abi_status_pipe_wait?.() ?? -1)) {
        const pipeFlags =
          (flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_flag_pipe_state_kernel_worker_owns) !== 0
            ? DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available
            : 0;
        const pipeAction = Number(receiver?.supervisor_blocked_pipe_sync_before_retry_action?.(vmPtr, pipeFlags) ?? 0);
        if (pipeAction === DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_pipe_sync_before_retry_action_request_sync) {
          return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_request_pipe_sync;
        }
        if (
          pipeAction
            === DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_pipe_sync_before_retry_action_request_sync_and_drain_output
        ) {
          return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_request_pipe_sync_and_drain_output;
        }
        if (pipeAction === DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_pipe_sync_before_retry_action_schedule_retry) {
          return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_schedule_pipe_sync_retry;
        }
      }
      if (status === Number(receiver?.abi_status_fs?.() ?? overrides.abi_status_fs?.() ?? -1)) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_resume_fs;
      }
      if (retryReason === DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_passive_wait) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_defer_passive_wait;
      }

      const delegationFlags = kernelWorkerAvailable
        ? DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_flag_kernel_worker_available
        : 0;
      const delegation = Number(receiver?.supervisor_blocked_resume_delegation?.(vmPtr, delegationFlags) ?? 0);
      if (delegation === DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_kernel_worker) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_resume_kernel_worker;
      }
      if (delegation === DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_wait4_kernel_worker) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_resume_wait4_kernel_worker;
      }
      if (delegation === DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_epoll_direct) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_resume_epoll_direct;
      }
      if (delegation === DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_unsupported) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_unsupported;
      }
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_retry_action_resume_direct;
    },
    supervisor_vfork_execve_rollback(this: KernelExports | undefined, vmPtr: number, childPid: number) {
      const receiver = currentExports(this);
      const waitStatus = Number(
        receiver?.supervisor_vfork_execve_handoff_failure_wait_status?.()
          ?? overrides.supervisor_vfork_execve_handoff_failure_wait_status?.()
          ?? DEFAULT_NUMERIC_EXPORTS.supervisor_vfork_execve_handoff_failure_wait_status,
      );
      receiver?.process_record_child_exit?.(childPid, waitStatus);
      receiver?.thread_restore_vfork_parent_without_cleanup?.(vmPtr, childPid);
      receiver?.thread_clear_vfork_runtime_state?.();
      return waitStatus;
    },
    supervisor_process_resume(this: KernelExports | undefined, vmPtr: number, outPtr: number, outLen: number) {
      const recordBytes = DEFAULT_NUMERIC_EXPORTS.supervisor_resume_record_bytes;
      if (outLen < recordBytes) {
        return -1;
      }
      const receiver = currentExports(this);
      const memory = receiver?.memory ?? overrides.memory;
      if (!memory) {
        return -1;
      }
      const status = Number(receiver?.thread_status?.(vmPtr) ?? overrides.thread_status?.(vmPtr) ?? 0);
      const okStatus = Number(receiver?.abi_status_ok?.() ?? overrides.abi_status_ok?.() ?? 0);
      if (status !== okStatus) {
        return 0;
      }
      return writeSupervisorRunnableResumeRecord(memory, outPtr, {
        vmPtr,
        tid: Number(receiver?.thread_tid?.(vmPtr) ?? overrides.thread_tid?.(vmPtr) ?? 1),
        processPtr: Number(receiver?.thread_process_ptr?.(vmPtr) ?? overrides.thread_process_ptr?.(vmPtr) ?? 0),
        status,
      });
    },
    supervisor_process_memory_host_op(this: KernelExports | undefined, vmPtr: number, index: number, outPtr: number, outLen: number) {
      const recordBytes = DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes;
      const receiver = currentExports(this);
      const memory = receiver?.memory ?? overrides.memory;
      if (!memory) {
        return -1;
      }
      if (outLen < recordBytes) {
        return -1;
      }
      const ranges = processMemoryRanges(receiver, memory, vmPtr);
      if (index === 0) {
        const initialBytes = processMemoryInitialBytes(receiver, ranges, vmPtr);
        return writeSupervisorSpawnThreadHostOpRecord(memory, outPtr, {
          vmPtr,
          childVmPtr: 0,
          kind: DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_kind_create_process_memory,
          tid: Number(receiver?.thread_spawn_child_pid?.(vmPtr) ?? overrides.thread_spawn_child_pid?.(vmPtr) ?? 1),
          processPtr: Number(receiver?.thread_process_ptr?.(vmPtr) ?? overrides.thread_process_ptr?.(vmPtr) ?? 0),
          status: Number(receiver?.thread_status?.(vmPtr) ?? overrides.thread_status?.(vmPtr) ?? 0),
          flags: BigInt(initialBytes),
          childTidPtr: BigInt(processMemoryMaxPages(receiver, initialBytes, vmPtr)),
        });
      }
      const range = ranges[index - 1];
      if (range !== undefined) {
        return writeSupervisorSpawnThreadHostOpRecord(memory, outPtr, {
          vmPtr,
          childVmPtr: 0,
          kind: DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_kind_copy_memory_ranges,
          tid: Number(receiver?.thread_spawn_child_pid?.(vmPtr) ?? overrides.thread_spawn_child_pid?.(vmPtr) ?? 1),
          processPtr: Number(receiver?.thread_process_ptr?.(vmPtr) ?? overrides.thread_process_ptr?.(vmPtr) ?? 0),
          status: Number(receiver?.thread_spawn_kind?.(vmPtr) ?? overrides.thread_spawn_kind?.(vmPtr) ?? 0),
          flags: BigInt(range.start),
          childTidPtr: BigInt(range.end),
        });
      }
      return 0;
    },
    supervisor_next_host_op_kind(this: KernelExports | undefined, kind: number, outPtr: number, outLen: number) {
      const receiver = currentExports(this);
      const spawnThreadKind = Number(
        receiver?.supervisor_host_op_kind_spawn_thread_worker?.()
          ?? overrides.supervisor_host_op_kind_spawn_thread_worker?.()
          ?? DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_kind_spawn_thread_worker,
      );
      if (kind !== spawnThreadKind) {
        return 0;
      }
      const parentVmPtr = overrides.cloneSpawnParentVmPtr?.();
      if (parentVmPtr === undefined) {
        return 0;
      }
      const cloneSpawnStatus = Number(
        receiver?.abi_status_clone_spawn?.() ?? overrides.abi_status_clone_spawn?.() ?? -1,
      );
      const currentStatus = Number(receiver?.thread_status?.(parentVmPtr) ?? overrides.thread_status?.(parentVmPtr) ?? 0);
      if (currentStatus !== cloneSpawnStatus) {
        return 0;
      }
      const memory = receiver?.memory ?? overrides.memory;
      if (!memory) {
        return -1;
      }
      const recordBytes = DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes;
      if (outLen < recordBytes) {
        return -1;
      }
      let childVmPtr = cloneSpawnHostOpChildVmPtrs.get(parentVmPtr);
      if (childVmPtr === undefined) {
        childVmPtr = Number(receiver?.thread_spawn_from_clone?.(parentVmPtr) ?? overrides.thread_spawn_from_clone?.(parentVmPtr) ?? 0);
        if (childVmPtr === 0) {
          return 0;
        }
        cloneSpawnHostOpChildVmPtrs.set(parentVmPtr, childVmPtr);
      }
      return writeSupervisorSpawnThreadHostOpRecord(memory, outPtr, {
        vmPtr: parentVmPtr,
        childVmPtr,
        kind: spawnThreadKind,
        tid: Number(receiver?.thread_tid?.(childVmPtr) ?? overrides.thread_tid?.(childVmPtr) ?? 1),
        processPtr: Number(receiver?.thread_process_ptr?.(parentVmPtr) ?? overrides.thread_process_ptr?.(parentVmPtr) ?? 0),
        status: Number(receiver?.abi_status_ok?.() ?? overrides.abi_status_ok?.() ?? 0),
      });
    },
    supervisor_resume_blocked_syscall(this: KernelExports | undefined, vmPtr: number, flags: number) {
      const receiver = currentExports(this);
      const status = Number(receiver?.thread_status?.(vmPtr) ?? overrides.thread_status?.(vmPtr) ?? 0);
      const timeoutFlag = Number(
        receiver?.supervisor_resume_flag_timeout_expired?.()
          ?? overrides.supervisor_resume_flag_timeout_expired?.()
          ?? DEFAULT_NUMERIC_EXPORTS.supervisor_resume_flag_timeout_expired,
      );
      const timeoutExpired = (flags & timeoutFlag) !== 0;
      if (status === Number(receiver?.abi_status_wait4?.() ?? overrides.abi_status_wait4?.() ?? -1)) {
        return receiver?.thread_resume_wait4?.(vmPtr) ?? overrides.thread_resume_wait4?.(vmPtr) ?? 0n;
      }
      if (
        timeoutExpired
        && (
          status === Number(receiver?.abi_status_connect?.() ?? overrides.abi_status_connect?.() ?? -1)
          || status === Number(receiver?.abi_status_socket_recv?.() ?? overrides.abi_status_socket_recv?.() ?? -1)
          || status === Number(receiver?.abi_status_socket_send?.() ?? overrides.abi_status_socket_send?.() ?? -1)
        )
      ) {
        receiver?.thread_resume_socket_wait_timeout?.(vmPtr);
        return 0n;
      }
      if (timeoutExpired && status === Number(receiver?.abi_status_epoll_wait?.() ?? overrides.abi_status_epoll_wait?.() ?? -1)) {
        receiver?.thread_resume_epoll_wait_timeout?.(vmPtr);
        return 0n;
      }
      if (timeoutExpired && status === Number(receiver?.abi_status_pipe_wait?.() ?? overrides.abi_status_pipe_wait?.() ?? -1)) {
        receiver?.thread_resume_poll_wait_timeout?.(vmPtr);
        return 0n;
      }
      return receiver?.thread_resume_blocked_syscall?.(vmPtr) ?? overrides.thread_resume_blocked_syscall?.(vmPtr) ?? 0n;
    },
    supervisor_fault_exit_decision(this: KernelExports | undefined, vmPtr: number, outPtr: number, outLen: number) {
      const recordBytes = DEFAULT_NUMERIC_EXPORTS.supervisor_fault_exit_decision_bytes;
      if (outLen < recordBytes) {
        return -1;
      }
      const receiver = currentExports(this);
      const memory = receiver?.memory ?? overrides.memory;
      if (!memory) {
        return -1;
      }
      return writeSupervisorFaultExitDecisionRecord(memory, outPtr, {
        vmPtr,
        exitVmPtr: vmPtr,
        processPtr: Number(receiver?.thread_process_ptr?.(vmPtr) ?? overrides.thread_process_ptr?.(vmPtr) ?? 0),
        remainingThreads: 0,
        liveThreads: 0,
        exitCode: Number(receiver?.thread_exit_code?.(vmPtr) ?? overrides.thread_exit_code?.(vmPtr) ?? 0),
        termSignal: Number(receiver?.thread_term_signal?.(vmPtr) ?? overrides.thread_term_signal?.(vmPtr) ?? 0),
        status: Number(receiver?.thread_status?.(vmPtr) ?? overrides.thread_status?.(vmPtr) ?? 0),
      });
    },
    ...overrides,
  } as Partial<KernelExports>;
}

export function writeSupervisorFaultExitDecisionRecord(
  memory: WebAssembly.Memory,
  outPtr: number,
  record: SupervisorFaultExitDecisionRecordFixture,
): number {
  const view = new DataView(memory.buffer);
  view.setUint32(outPtr, DEFAULT_NUMERIC_EXPORTS.supervisor_fault_exit_decision_magic, true);
  view.setUint32(outPtr + 4, 1, true);
  view.setUint32(outPtr + 8, DEFAULT_NUMERIC_EXPORTS.supervisor_fault_exit_decision_bytes, true);
  view.setUint32(
    outPtr + 12,
    record.kind ?? DEFAULT_NUMERIC_EXPORTS.supervisor_fault_exit_kind_process_exit,
    true,
  );
  view.setBigUint64(outPtr + 16, record.seq ?? 1n, true);
  view.setUint32(outPtr + 24, record.vmPtr, true);
  view.setUint32(outPtr + 28, record.exitVmPtr ?? record.vmPtr, true);
  view.setUint32(outPtr + 32, record.tid ?? 1, true);
  view.setUint32(outPtr + 36, record.processPtr ?? 0, true);
  view.setUint32(outPtr + 40, record.remainingThreads ?? 0, true);
  view.setUint32(outPtr + 44, record.liveThreads ?? 0, true);
  view.setInt32(outPtr + 48, record.exitCode ?? 0, true);
  view.setInt32(outPtr + 52, record.termSignal ?? 0, true);
  view.setUint32(outPtr + 56, record.status ?? 0, true);
  view.setUint32(outPtr + 60, record.faultSignal ?? 0, true);
  view.setUint32(outPtr + 64, record.faultKind ?? 0, true);
  view.setUint32(outPtr + 68, 0, true);
  view.setBigUint64(outPtr + 72, record.faultPc ?? 0n, true);
  view.setBigUint64(outPtr + 80, record.faultAddr ?? 0n, true);
  return DEFAULT_NUMERIC_EXPORTS.supervisor_fault_exit_decision_bytes;
}

export function writeSupervisorRunnableResumeRecord(
  memory: WebAssembly.Memory,
  outPtr: number,
  record: SupervisorRunnableResumeRecordFixture,
): number {
  const view = new DataView(memory.buffer);
  view.setUint32(outPtr, DEFAULT_NUMERIC_EXPORTS.supervisor_resume_record_magic, true);
  view.setUint32(outPtr + 4, DEFAULT_NUMERIC_EXPORTS.supervisor_abi_version, true);
  view.setUint32(outPtr + 8, DEFAULT_NUMERIC_EXPORTS.supervisor_resume_record_bytes, true);
  view.setUint32(outPtr + 12, DEFAULT_NUMERIC_EXPORTS.supervisor_resume_kind_runnable_thread, true);
  view.setBigUint64(outPtr + 16, record.seq ?? 1n, true);
  view.setUint32(outPtr + 24, record.vmPtr, true);
  view.setUint32(outPtr + 28, record.tid ?? 1, true);
  view.setUint32(outPtr + 32, record.processPtr ?? 0, true);
  view.setUint32(outPtr + 36, record.status ?? 0, true);
  view.setBigUint64(outPtr + 40, record.detail0 ?? 0n, true);
  return DEFAULT_NUMERIC_EXPORTS.supervisor_resume_record_bytes;
}

export interface SupervisorSpawnThreadHostOpFixture {
  vmPtr: number;
  childVmPtr: number;
  kind?: number;
  seq?: bigint;
  tid?: number;
  processPtr?: number;
  status?: number;
  flags?: bigint;
  childTidPtr?: bigint;
}

export function writeSupervisorSpawnThreadHostOpRecord(
  memory: WebAssembly.Memory,
  outPtr: number,
  record: SupervisorSpawnThreadHostOpFixture,
): number {
  const view = new DataView(memory.buffer);
  view.setUint32(outPtr, DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_magic, true);
  view.setUint32(outPtr + 4, DEFAULT_NUMERIC_EXPORTS.supervisor_abi_version, true);
  view.setUint32(outPtr + 8, DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes, true);
  view.setUint32(outPtr + 12, record.kind ?? DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_kind_spawn_thread_worker, true);
  view.setBigUint64(outPtr + 16, record.seq ?? 1n, true);
  view.setUint32(outPtr + 24, record.vmPtr, true);
  view.setUint32(outPtr + 28, record.childVmPtr, true);
  view.setUint32(outPtr + 32, record.tid ?? 1, true);
  view.setUint32(outPtr + 36, record.processPtr ?? 0, true);
  view.setUint32(outPtr + 40, record.status ?? 0, true);
  view.setUint32(outPtr + 44, 0, true);
  view.setBigUint64(outPtr + 48, record.flags ?? 0n, true);
  view.setBigUint64(outPtr + 56, record.childTidPtr ?? 0n, true);
  return DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes;
}

export function writeSupervisorArmHostTimerHostOpRecord(
  memory: WebAssembly.Memory,
  outPtr: number,
  record: SupervisorArmHostTimerHostOpFixture,
): number {
  const view = new DataView(memory.buffer);
  view.setUint32(outPtr, DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_magic, true);
  view.setUint32(outPtr + 4, DEFAULT_NUMERIC_EXPORTS.supervisor_abi_version, true);
  view.setUint32(outPtr + 8, DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes, true);
  view.setUint32(outPtr + 12, DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_kind_arm_host_timer, true);
  view.setBigUint64(outPtr + 16, record.seq ?? 1n, true);
  view.setUint32(outPtr + 24, record.vmPtr, true);
  view.setUint32(outPtr + 28, 0, true);
  view.setUint32(outPtr + 32, record.tid ?? 1, true);
  view.setUint32(outPtr + 36, record.processPtr ?? 0, true);
  view.setUint32(outPtr + 40, record.status ?? 0, true);
  view.setUint32(outPtr + 44, 0, true);
  view.setBigUint64(outPtr + 48, record.delayMs, true);
  view.setBigUint64(outPtr + 56, record.deadlineTicks ?? 0n, true);
  return DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes;
}

export function writeSupervisorBlockedRetryHostOpRecord(
  memory: WebAssembly.Memory,
  outPtr: number,
  record: SupervisorBlockedRetryHostOpFixture,
): number {
  const view = new DataView(memory.buffer);
  view.setUint32(outPtr, DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_magic, true);
  view.setUint32(outPtr + 4, DEFAULT_NUMERIC_EXPORTS.supervisor_abi_version, true);
  view.setUint32(outPtr + 8, DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes, true);
  view.setUint32(outPtr + 12, DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_kind_retry_blocked_worker, true);
  view.setBigUint64(outPtr + 16, record.seq ?? 1n, true);
  view.setUint32(outPtr + 24, record.vmPtr, true);
  view.setUint32(outPtr + 28, 0, true);
  view.setUint32(outPtr + 32, record.tid ?? 1, true);
  view.setUint32(outPtr + 36, record.processPtr ?? 0, true);
  view.setUint32(outPtr + 40, record.status ?? 0, true);
  view.setUint32(outPtr + 44, 0, true);
  view.setBigUint64(outPtr + 48, record.delayMs, true);
  view.setBigUint64(outPtr + 56, record.reason, true);
  return DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes;
}
