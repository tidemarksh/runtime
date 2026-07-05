import type { KernelExports } from "./abi.js";

export type SupervisorBlockedWaitRetryFlag =
  | "host-progress"
  | "active-child"
  | "passive-wait"
  | "process-gate"
  | "thread-init-pending";

export type SupervisorBlockedWaitRetryFlags = readonly SupervisorBlockedWaitRetryFlag[];

export type SupervisorBlockedPipeResumeAction =
  | "defer"
  | "direct";

export type SupervisorBlockedStatusTransitionAction =
  | "block"
  | "sync-socket"
  | "sync-pipe"
  | "sync-fork-shared"
  | "sync-pipe-and-fork-shared"
  | "sync-socket-and-fork-shared";

export type SupervisorBlockedRetryAction =
  | "defer-supervisor-timer"
  | "defer-rpc-pending"
  | "defer-owner-async"
  | "defer-thread-init-pending"
  | "arm-wait-timer"
  | "park-active-child"
  | "resume-wait4"
  | "defer-fork-shared-sync"
  | "defer-running-worker"
  | "defer-process-gate"
  | "pump-network"
  | "epoll-kernel-worker"
  | "request-pipe-sync"
  | "request-pipe-sync-and-drain-output"
  | "schedule-pipe-sync-retry"
  | "resume-fs"
  | "defer-passive-wait"
  | "resume-kernel-worker"
  | "resume-wait4-kernel-worker"
  | "resume-epoll-direct"
  | "resume-direct"
  | "unsupported";

export type SupervisorBlockedInitialAction =
  | "none"
  | "defer-supervisor-timer"
  | "defer-rpc-pending"
  | "arm-wait-timer"
  | "retry-now"
  | "schedule-retry";

export type SupervisorBlockedRetryResultAction =
  | "none"
  | "arm-wait-timer"
  | "schedule-retry"
  | "schedule-retry-and-pump";

export type SupervisorBlockedPostNetworkPumpAction =
  | SupervisorBlockedRetryAction
  | "schedule-retry"
  | "schedule-retry-with-host-progress";

export interface SupervisorBlockedRetryStatusActions {
  pipeRetryDiagnostics: boolean;
  pumpBlockedOutcome: boolean;
  activeChildPipe: boolean;
  activeChildWait4: boolean;
  successLogEpollReady: boolean;
  successLogWait4Resumed: boolean;
  wakeRuntimeBridgeWhileRunning: boolean;
}

export interface SupervisorWaitInWorkerStatusActions {
  blockEpollTimeout: boolean;
  parkFutex: boolean;
  blockWait4AndSyncPipe: boolean;
  syncSocket: boolean;
  syncEpoll: boolean;
  syncPipe: boolean;
}

export interface SupervisorWait4ChildExitBlockedParentActions {
  invalidateRpc: boolean;
  clearPipeSyncBeforeRetry: boolean;
}

export type SupervisorWait4RpcLocalStatusAction =
  | "skip"
  | "start";

export interface SupervisorSchedulerClearBlockedWorkerActions {
  clearPipeSyncBeforeRetry: boolean;
}

export type SupervisorSchedulerBlockWorkerAction =
  | "record-blocked"
  | "park-futex";

export type SupervisorSchedulerFutexWaiterAction =
  | "not-waiter"
  | "waiter";

export type SupervisorWorkerStatusTransitionAction =
  | "enqueue"
  | "connect-wait"
  | "park-futex"
  | "host-wait";

export type SupervisorBlockedStaleRetryResultAction =
  | "none"
  | "handle-current-result"
  | "handle-current-result-wait4";

export type SupervisorBlockedRpcPipeSyncAction =
  | "not-pipe"
  | "none"
  | "sync-socket";

export interface SupervisorBlockedRpcPostResumeSyncAction {
  syncForkShared: boolean;
  syncSocket: boolean;
}

export type SupervisorBlockedRetryActionFlag =
  | "supervisor-timer-armed"
  | "rpc-pending"
  | "owner-async"
  | "thread-init-pending"
  | "fork-shared-sync-pending"
  | "running-worker"
  | "kernel-worker-available"
  | "pipe-state-kernel-worker-owns"
  | "network-connections-available"
  | "wait-timer-already-checked";

export type SupervisorBlockedRetryActionFlags = readonly SupervisorBlockedRetryActionFlag[];

export type SupervisorBlockedWaitWakeSource =
  | "pipe-sync"
  | "epoll-sync"
  | "socket-sync"
  | "pipe-and-epoll-sync";

export type SupervisorBlockedWaitWakeAction =
  | "skip"
  | "retry"
  | "clear-pipe-sync-and-retry"
  | "log-epoll-and-retry";

export type SupervisorStateSyncKind =
  | "pipe"
  | "epoll"
  | "socket";

type SupervisorNumberConstExportName =
  | "supervisor_resume_flag_timeout_expired"
  | "supervisor_blocked_pipe_resume_action_defer"
  | "supervisor_blocked_pipe_resume_action_direct"
  | "supervisor_blocked_status_transition_action_block"
  | "supervisor_blocked_status_transition_action_sync_socket"
  | "supervisor_blocked_status_transition_action_sync_pipe"
  | "supervisor_blocked_status_transition_action_sync_fork_shared"
  | "supervisor_blocked_status_transition_action_sync_pipe_and_fork_shared"
  | "supervisor_blocked_status_transition_action_sync_socket_and_fork_shared"
  | "supervisor_blocked_status_transition_flag_kernel_worker_available"
  | "supervisor_blocked_status_transition_flag_network_connections_available"
  | "supervisor_state_sync_kind_pipe"
  | "supervisor_state_sync_kind_epoll"
  | "supervisor_state_sync_kind_socket"
  | "supervisor_state_sync_request_flag_wake_waiters"
  | "supervisor_state_sync_run_flag_run"
  | "supervisor_state_sync_run_flag_wake_waiters"
  | "supervisor_blocked_wait_retry_flag_host_progress"
  | "supervisor_blocked_wait_retry_flag_active_child"
  | "supervisor_blocked_wait_retry_flag_passive_wait"
  | "supervisor_blocked_wait_retry_flag_process_gate"
  | "supervisor_blocked_wait_retry_flag_thread_init_pending"
  | "supervisor_blocked_initial_action_none"
  | "supervisor_blocked_initial_action_defer_supervisor_timer"
  | "supervisor_blocked_initial_action_defer_rpc_pending"
  | "supervisor_blocked_initial_action_arm_wait_timer"
  | "supervisor_blocked_initial_action_retry_now"
  | "supervisor_blocked_initial_action_schedule_retry"
  | "supervisor_blocked_retry_result_action_none"
  | "supervisor_blocked_retry_result_action_arm_wait_timer"
  | "supervisor_blocked_retry_result_action_schedule_retry"
  | "supervisor_blocked_retry_result_action_schedule_retry_and_pump"
  | "supervisor_blocked_retry_status_action_none"
  | "supervisor_blocked_retry_status_action_pipe_retry_diagnostics"
  | "supervisor_blocked_retry_status_action_pump_blocked_outcome"
  | "supervisor_blocked_retry_status_action_active_child_pipe"
  | "supervisor_blocked_retry_status_action_active_child_wait4"
  | "supervisor_blocked_retry_status_action_success_log_epoll_ready"
  | "supervisor_blocked_retry_status_action_success_log_wait4_resumed"
  | "supervisor_blocked_retry_status_action_wake_runtime_bridge_while_running"
  | "supervisor_blocked_post_network_pump_action_schedule_retry"
  | "supervisor_blocked_post_network_pump_action_schedule_retry_with_host_progress"
  | "supervisor_blocked_stale_retry_result_action_none"
  | "supervisor_blocked_stale_retry_result_action_handle_current_result"
  | "supervisor_blocked_stale_retry_result_action_handle_current_result_wait4"
  | "supervisor_blocked_rpc_pipe_sync_action_not_pipe"
  | "supervisor_blocked_rpc_pipe_sync_action_none"
  | "supervisor_blocked_rpc_pipe_sync_action_sync_socket"
  | "supervisor_blocked_rpc_post_resume_sync_flag_kernel_worker_available"
  | "supervisor_blocked_rpc_post_resume_sync_flag_process_owner_mismatch"
  | "supervisor_blocked_rpc_post_resume_sync_action_none"
  | "supervisor_blocked_rpc_post_resume_sync_action_fork_shared"
  | "supervisor_blocked_rpc_post_resume_sync_action_socket"
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
  | "supervisor_blocked_wait_wake_source_pipe_sync"
  | "supervisor_blocked_wait_wake_source_epoll_sync"
  | "supervisor_blocked_wait_wake_source_socket_sync"
  | "supervisor_blocked_wait_wake_source_pipe_and_epoll_sync"
  | "supervisor_blocked_wait_wake_flag_rpc_pending"
  | "supervisor_blocked_wait_wake_action_skip"
  | "supervisor_blocked_wait_wake_action_retry"
  | "supervisor_blocked_wait_wake_action_clear_pipe_sync_and_retry"
  | "supervisor_blocked_wait_wake_action_log_epoll_and_retry"
  | "supervisor_wait_in_worker_status_action_none"
  | "supervisor_wait_in_worker_status_action_block_epoll_timeout"
  | "supervisor_wait_in_worker_status_action_park_futex"
  | "supervisor_wait_in_worker_status_action_block_wait4_and_sync_pipe"
  | "supervisor_wait_in_worker_status_action_sync_socket"
  | "supervisor_wait_in_worker_status_action_sync_epoll"
  | "supervisor_wait_in_worker_status_action_sync_pipe"
  | "supervisor_wait_in_worker_status_action_flag_running_worker"
  | "supervisor_wait_in_worker_status_action_flag_kernel_worker_available"
  | "supervisor_wait_in_worker_status_action_flag_epoll_state_kernel_owns"
  | "supervisor_wait4_child_exit_blocked_parent_action_none"
  | "supervisor_wait4_child_exit_blocked_parent_action_invalidate_rpc"
  | "supervisor_wait4_child_exit_blocked_parent_action_clear_pipe_sync_before_retry"
  | "supervisor_wait4_rpc_local_status_action_skip"
  | "supervisor_wait4_rpc_local_status_action_start"
  | "supervisor_scheduler_clear_blocked_action_none"
  | "supervisor_scheduler_clear_blocked_action_clear_pipe_sync_before_retry"
  | "supervisor_scheduler_clear_blocked_flag_clear_pipe_sync_before_retry"
  | "supervisor_scheduler_block_worker_action_record_blocked"
  | "supervisor_scheduler_block_worker_action_park_futex"
  | "supervisor_scheduler_futex_waiter_action_not_waiter"
  | "supervisor_scheduler_futex_waiter_action_waiter"
  | "supervisor_worker_status_transition_action_enqueue"
  | "supervisor_worker_status_transition_action_connect_wait"
  | "supervisor_worker_status_transition_action_park_futex"
  | "supervisor_worker_status_transition_action_host_wait";

function readSupervisorNumberConst(
  kernel: KernelExports,
  exportName: SupervisorNumberConstExportName,
): number {
  const readConst = kernel[exportName];
  if (readConst === undefined) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  return readConst.call(kernel);
}

export interface SupervisorStateSyncRun {
  run: boolean;
  wakeWaiters: boolean;
}

export interface SupervisorBlockedResumeWritebackRange {
  guestPtr: number;
  length: number;
}

export function resumeBlockedSyscallBySupervisor(
  kernel: KernelExports,
  vmPtr: number,
  timeoutExpired = false,
): bigint {
  const resume = kernel.supervisor_resume_blocked_syscall;
  if (resume === undefined) {
    throw new Error("kernel supervisor blocked syscall resume export is missing");
  }
  const flags = timeoutExpired
    ? readSupervisorNumberConst(kernel, "supervisor_resume_flag_timeout_expired")
    : 0;
  return BigInt(resume.call(kernel, vmPtr, flags));
}

function toNumber(value: bigint | number): number {
  return typeof value === "bigint" ? Number(value) : value;
}

export function supervisorBlockedResumeWritebacks(
  kernel: KernelExports,
  vmPtr: number,
  blockedStatus: number,
  resumeResult: bigint,
): SupervisorBlockedResumeWritebackRange[] {
  const countRanges = kernel.supervisor_blocked_resume_writeback_count;
  const readPtr = kernel.supervisor_blocked_resume_writeback_ptr;
  const readLen = kernel.supervisor_blocked_resume_writeback_len;
  if (countRanges === undefined || readPtr === undefined || readLen === undefined) {
    throw new Error("kernel supervisor blocked resume writeback exports are missing");
  }
  const count = countRanges.call(kernel, vmPtr, blockedStatus, resumeResult);
  if (count < 0) {
    throw new Error(`kernel supervisor blocked resume writeback count failed: ${count}`);
  }
  const ranges: SupervisorBlockedResumeWritebackRange[] = [];
  for (let index = 0; index < count; index += 1) {
    const guestPtr = toNumber(readPtr.call(kernel, vmPtr, blockedStatus, resumeResult, index));
    const length = toNumber(readLen.call(kernel, vmPtr, blockedStatus, resumeResult, index));
    if (
      Number.isFinite(guestPtr)
      && Number.isFinite(length)
      && guestPtr > 0
      && length > 0
    ) {
      ranges.push({ guestPtr, length });
    }
  }
  return ranges;
}

export function supervisorBlockedPollWaitTimeoutMs(
  kernel: KernelExports,
  vmPtr: number,
): number {
  const readTimeout = kernel.supervisor_blocked_poll_wait_timeout_ms;
  if (readTimeout === undefined) {
    throw new Error("kernel supervisor blocked poll wait timeout export is missing");
  }
  const result = toNumber(readTimeout.call(kernel, vmPtr));
  if (result < -1) {
    throw new Error(`kernel supervisor blocked poll wait timeout failed: ${result}`);
  }
  return result;
}

export function supervisorBlockedEpollWaitTimeoutMs(
  kernel: KernelExports,
  vmPtr: number,
): number {
  const readTimeout = kernel.supervisor_blocked_epoll_wait_timeout_ms;
  if (readTimeout === undefined) {
    throw new Error("kernel supervisor blocked epoll wait timeout export is missing");
  }
  const result = toNumber(readTimeout.call(kernel, vmPtr));
  if (result < -1) {
    throw new Error(`kernel supervisor blocked epoll wait timeout failed: ${result}`);
  }
  return result;
}

export function supervisorBlockedStatusIsHostWait(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorBlockedStatusFact(kernel, status, "supervisor_blocked_status_is_host_wait");
}

export function supervisorBlockedStatusIsConnectWait(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorBlockedStatusFact(kernel, status, "supervisor_blocked_status_is_connect_wait");
}

export function supervisorBlockedStatusIsEpollWait(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorBlockedStatusFact(kernel, status, "supervisor_blocked_status_is_epoll_wait");
}

export function supervisorBlockedStatusIsPipeWait(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorBlockedStatusFact(kernel, status, "supervisor_blocked_status_is_pipe_wait");
}

export function supervisorBlockedStatusIsWait4(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorBlockedStatusFact(kernel, status, "supervisor_blocked_status_is_wait4");
}

export function supervisorBlockedStatusIsSocketWait(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorBlockedStatusFact(kernel, status, "supervisor_blocked_status_is_socket_wait");
}

export function supervisorBlockedStatusNeedsNetworkSidecarRestore(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorBlockedStatusFact(kernel, status, "supervisor_blocked_status_needs_network_sidecar_restore");
}

export function supervisorBlockedStatusSyncsSocketAfterResume(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorBlockedStatusFact(kernel, status, "supervisor_blocked_status_syncs_socket_after_resume");
}

function supervisorBlockedStatusFact(
  kernel: KernelExports,
  status: number,
  exportName: "supervisor_blocked_status_is_host_wait"
    | "supervisor_blocked_status_is_connect_wait"
    | "supervisor_blocked_status_is_epoll_wait"
    | "supervisor_blocked_status_is_pipe_wait"
    | "supervisor_blocked_status_is_wait4"
    | "supervisor_blocked_status_is_socket_wait"
    | "supervisor_blocked_status_needs_network_sidecar_restore"
    | "supervisor_blocked_status_syncs_socket_after_resume",
): boolean {
  const readStatus = kernel[exportName];
  if (readStatus === undefined) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  const result = readStatus.call(kernel, status);
  if (result < 0) {
    throw new Error(`kernel supervisor status fact ${exportName} failed: ${result}`);
  }
  return result !== 0;
}

function supervisorBlockedBooleanFact(
  kernel: KernelExports,
  vmPtr: number,
  exportName:
    | "supervisor_blocked_poll_wait_has_socket"
    | "supervisor_blocked_pipe_wait_has_ready_events"
    | "supervisor_blocked_epoll_wait_retry_required",
): boolean {
  const readFact = kernel[exportName];
  if (readFact === undefined) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  const result = readFact.call(kernel, vmPtr);
  if (result < 0) {
    throw new Error(`kernel supervisor ${exportName} failed: ${result}`);
  }
  return result !== 0;
}

export function supervisorBlockedPollWaitHasSocket(
  kernel: KernelExports,
  vmPtr: number,
): boolean {
  return supervisorBlockedBooleanFact(kernel, vmPtr, "supervisor_blocked_poll_wait_has_socket");
}

export function supervisorBlockedPipeWaitHasReadyEvents(
  kernel: KernelExports,
  vmPtr: number,
): boolean {
  return supervisorBlockedBooleanFact(kernel, vmPtr, "supervisor_blocked_pipe_wait_has_ready_events");
}

export function supervisorBlockedEpollWaitRetryRequired(
  kernel: KernelExports,
  vmPtr: number,
): boolean {
  return supervisorBlockedBooleanFact(kernel, vmPtr, "supervisor_blocked_epoll_wait_retry_required");
}

function supervisorBlockedPipeResumeActionValue(
  kernel: KernelExports,
  action: SupervisorBlockedPipeResumeAction,
): number {
  switch (action) {
    case "direct":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_pipe_resume_action_direct");
    case "defer":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_pipe_resume_action_defer");
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

function normalizeSupervisorBlockedPipeResumeAction(
  kernel: KernelExports,
  result: number,
): SupervisorBlockedPipeResumeAction {
  const actions: SupervisorBlockedPipeResumeAction[] = [
    "defer",
    "direct",
  ];
  for (const action of actions) {
    if (result === supervisorBlockedPipeResumeActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor blocked pipe resume action is unknown: ${result}`);
}

export function supervisorBlockedPipeResumeAction(
  kernel: KernelExports,
  vmPtr: number,
): SupervisorBlockedPipeResumeAction {
  const readAction = kernel.supervisor_blocked_pipe_resume_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor blocked pipe resume action export is missing");
  }
  const result = readAction.call(kernel, vmPtr);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked pipe resume action failed: ${result}`);
  }
  return normalizeSupervisorBlockedPipeResumeAction(kernel, result);
}

export function supervisorBlockedPipeSyncBeforeRetryRequested(
  kernel: KernelExports,
  vmPtr: number,
): boolean {
  const readRequested = kernel.supervisor_blocked_pipe_sync_before_retry_requested;
  if (readRequested === undefined) {
    throw new Error("kernel supervisor blocked pipe sync-before-retry requested export is missing");
  }
  const result = readRequested.call(kernel, vmPtr);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked pipe sync-before-retry requested query failed: ${result}`);
  }
  return result !== 0;
}

export function supervisorBlockedPipeSyncBeforeRetryClear(
  kernel: KernelExports,
  vmPtr: number,
): void {
  const clear = kernel.supervisor_blocked_pipe_sync_before_retry_clear;
  if (clear === undefined) {
    throw new Error("kernel supervisor blocked pipe sync-before-retry clear export is missing");
  }
  const result = clear.call(kernel, vmPtr);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked pipe sync-before-retry clear failed: ${result}`);
  }
}

function supervisorBlockedStatusTransitionActionValue(
  kernel: KernelExports,
  action: SupervisorBlockedStatusTransitionAction,
): number {
  switch (action) {
    case "sync-socket":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_status_transition_action_sync_socket");
    case "sync-pipe":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_status_transition_action_sync_pipe");
    case "sync-fork-shared":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_status_transition_action_sync_fork_shared");
    case "sync-pipe-and-fork-shared":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_status_transition_action_sync_pipe_and_fork_shared");
    case "sync-socket-and-fork-shared":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_status_transition_action_sync_socket_and_fork_shared");
    case "block":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_status_transition_action_block");
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

function normalizeSupervisorBlockedStatusTransitionAction(
  kernel: KernelExports,
  result: number,
): SupervisorBlockedStatusTransitionAction {
  const actions: SupervisorBlockedStatusTransitionAction[] = [
    "block",
    "sync-socket",
    "sync-pipe",
    "sync-fork-shared",
    "sync-pipe-and-fork-shared",
    "sync-socket-and-fork-shared",
  ];
  for (const action of actions) {
    if (result === supervisorBlockedStatusTransitionActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor blocked status transition action is unknown: ${result}`);
}

function supervisorBlockedStatusTransitionFlags(
  kernel: KernelExports,
  options: {
    kernelWorkerAvailable: boolean;
    networkConnectionsAvailable: boolean;
  },
): number {
  let flags = 0;
  if (options.kernelWorkerAvailable) {
    flags |= readSupervisorNumberConst(
      kernel,
      "supervisor_blocked_status_transition_flag_kernel_worker_available",
    );
  }
  if (options.networkConnectionsAvailable) {
    flags |= readSupervisorNumberConst(
      kernel,
      "supervisor_blocked_status_transition_flag_network_connections_available",
    );
  }
  return flags;
}

export function supervisorBlockedStatusTransitionAction(
  kernel: KernelExports,
  vmPtr: number,
  status: number,
  options: {
    kernelWorkerAvailable: boolean;
    networkConnectionsAvailable: boolean;
  },
): SupervisorBlockedStatusTransitionAction {
  const readAction = kernel.supervisor_blocked_status_transition_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor blocked status transition action export is missing");
  }
  const result = readAction.call(
    kernel,
    vmPtr,
    status,
    supervisorBlockedStatusTransitionFlags(kernel, options),
  );
  if (result < 0) {
    throw new Error(`kernel supervisor blocked status transition action failed: ${result}`);
  }
  return normalizeSupervisorBlockedStatusTransitionAction(kernel, result);
}

export function supervisorCompleteBlockedWaitNetworkPump(
  kernel: KernelExports,
  vmPtr: number,
  changed: boolean,
): void {
  const complete = kernel.supervisor_complete_blocked_wait_network_pump;
  if (complete === undefined) {
    throw new Error("kernel supervisor blocked wait network pump completion export is missing");
  }
  const result = complete.call(kernel, vmPtr, changed ? 1 : 0);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked wait network pump completion failed: ${result}`);
  }
}

export function supervisorClearBlockedWaitNetworkPump(
  kernel: KernelExports,
  vmPtr: number,
): void {
  const clear = kernel.supervisor_clear_blocked_wait_network_pump;
  if (clear === undefined) {
    throw new Error("kernel supervisor blocked wait network pump clear export is missing");
  }
  const result = clear.call(kernel, vmPtr);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked wait network pump clear failed: ${result}`);
  }
}

function supervisorStateSyncKindValue(
  kernel: KernelExports,
  kind: SupervisorStateSyncKind,
): number {
  switch (kind) {
    case "pipe":
      return readSupervisorNumberConst(kernel, "supervisor_state_sync_kind_pipe");
    case "epoll":
      return readSupervisorNumberConst(kernel, "supervisor_state_sync_kind_epoll");
    case "socket":
      return readSupervisorNumberConst(kernel, "supervisor_state_sync_kind_socket");
    default: {
      const exhaustive: never = kind;
      return exhaustive;
    }
  }
}

function supervisorStateSyncRequestFlags(kernel: KernelExports, wakeWaiters: boolean): number {
  return wakeWaiters
    ? readSupervisorNumberConst(kernel, "supervisor_state_sync_request_flag_wake_waiters")
    : 0;
}

function supervisorStateSyncRunFlagRun(kernel: KernelExports): number {
  return readSupervisorNumberConst(kernel, "supervisor_state_sync_run_flag_run");
}

function supervisorStateSyncRunFlagWakeWaiters(kernel: KernelExports): number {
  return readSupervisorNumberConst(kernel, "supervisor_state_sync_run_flag_wake_waiters");
}

export function supervisorStateSyncRequest(
  kernel: KernelExports,
  vmPtr: number,
  kind: SupervisorStateSyncKind,
  wakeWaiters: boolean,
): boolean {
  const request = kernel.supervisor_state_sync_request;
  if (request === undefined) {
    throw new Error("kernel supervisor state-sync request export is missing");
  }
  const result = request.call(
    kernel,
    vmPtr,
    supervisorStateSyncKindValue(kernel, kind),
    supervisorStateSyncRequestFlags(kernel, wakeWaiters),
  );
  if (result < 0) {
    throw new Error(`kernel supervisor state-sync request failed: ${result}`);
  }
  return result !== 0;
}

export function supervisorStateSyncNextRun(
  kernel: KernelExports,
  vmPtr: number,
  kind: SupervisorStateSyncKind,
): SupervisorStateSyncRun {
  const nextRun = kernel.supervisor_state_sync_next_run_flags;
  if (nextRun === undefined) {
    throw new Error("kernel supervisor state-sync next-run export is missing");
  }
  const result = nextRun.call(kernel, vmPtr, supervisorStateSyncKindValue(kernel, kind));
  if (result < 0) {
    throw new Error(`kernel supervisor state-sync next-run failed: ${result}`);
  }
  return {
    run: (result & supervisorStateSyncRunFlagRun(kernel)) !== 0,
    wakeWaiters: (result & supervisorStateSyncRunFlagWakeWaiters(kernel)) !== 0,
  };
}

export function supervisorStateSyncComplete(
  kernel: KernelExports,
  vmPtr: number,
  kind: SupervisorStateSyncKind,
): boolean {
  const complete = kernel.supervisor_state_sync_complete;
  if (complete === undefined) {
    throw new Error("kernel supervisor state-sync complete export is missing");
  }
  const result = complete.call(kernel, vmPtr, supervisorStateSyncKindValue(kernel, kind));
  if (result < 0) {
    throw new Error(`kernel supervisor state-sync complete failed: ${result}`);
  }
  return result !== 0;
}

export function supervisorStateSyncClear(
  kernel: KernelExports,
  vmPtr: number,
  kind: SupervisorStateSyncKind,
): void {
  const clear = kernel.supervisor_state_sync_clear;
  if (clear === undefined) {
    throw new Error("kernel supervisor state-sync clear export is missing");
  }
  const result = clear.call(kernel, vmPtr, supervisorStateSyncKindValue(kernel, kind));
  if (result < 0) {
    throw new Error(`kernel supervisor state-sync clear failed: ${result}`);
  }
}

function supervisorRetryFlags(
  kernel: KernelExports,
  flags: SupervisorBlockedWaitRetryFlags,
): number {
  let result = 0;
  if (flags.includes("host-progress")) {
    result |= readSupervisorNumberConst(kernel, "supervisor_blocked_wait_retry_flag_host_progress");
  }
  if (flags.includes("active-child")) {
    result |= readSupervisorNumberConst(kernel, "supervisor_blocked_wait_retry_flag_active_child");
  }
  if (flags.includes("passive-wait")) {
    result |= readSupervisorNumberConst(kernel, "supervisor_blocked_wait_retry_flag_passive_wait");
  }
  if (flags.includes("process-gate")) {
    result |= readSupervisorNumberConst(kernel, "supervisor_blocked_wait_retry_flag_process_gate");
  }
  if (flags.includes("thread-init-pending")) {
    result |= readSupervisorNumberConst(kernel, "supervisor_blocked_wait_retry_flag_thread_init_pending");
  }
  return result;
}

export function supervisorBlockedWaitRetryFlags(
  kernel: KernelExports,
  flags: SupervisorBlockedWaitRetryFlags,
): number {
  return supervisorRetryFlags(kernel, flags);
}

function supervisorBlockedRetryActionFlagValue(
  kernel: KernelExports,
  flag: SupervisorBlockedRetryActionFlag,
): number {
  switch (flag) {
    case "supervisor-timer-armed":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_flag_supervisor_timer_armed");
    case "rpc-pending":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_flag_rpc_pending");
    case "owner-async":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_flag_owner_async");
    case "thread-init-pending":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_flag_thread_init_pending");
    case "fork-shared-sync-pending":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_flag_fork_shared_sync_pending");
    case "running-worker":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_flag_running_worker");
    case "kernel-worker-available":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_flag_kernel_worker_available");
    case "pipe-state-kernel-worker-owns":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_flag_pipe_state_kernel_worker_owns");
    case "network-connections-available":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_flag_network_connections_available");
    case "wait-timer-already-checked":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_flag_wait_timer_already_checked");
    default: {
      const exhaustive: never = flag;
      return exhaustive;
    }
  }
}

function supervisorBlockedRetryActionFlagsValue(
  kernel: KernelExports,
  flags: SupervisorBlockedRetryActionFlags,
): number {
  let result = 0;
  for (const flag of flags) {
    result |= supervisorBlockedRetryActionFlagValue(kernel, flag);
  }
  return result;
}

function supervisorBlockedRetryActionValue(
  kernel: KernelExports,
  action: SupervisorBlockedRetryAction,
): number {
  switch (action) {
    case "defer-supervisor-timer":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_defer_supervisor_timer");
    case "defer-rpc-pending":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_defer_rpc_pending");
    case "defer-owner-async":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_defer_owner_async");
    case "defer-thread-init-pending":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_defer_thread_init_pending");
    case "arm-wait-timer":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_arm_wait_timer");
    case "park-active-child":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_park_active_child");
    case "resume-wait4":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_resume_wait4");
    case "defer-fork-shared-sync":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_defer_fork_shared_sync");
    case "defer-running-worker":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_defer_running_worker");
    case "defer-process-gate":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_defer_process_gate");
    case "pump-network":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_pump_network");
    case "epoll-kernel-worker":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_epoll_kernel_worker");
    case "request-pipe-sync":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_request_pipe_sync");
    case "request-pipe-sync-and-drain-output":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_request_pipe_sync_and_drain_output");
    case "schedule-pipe-sync-retry":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_schedule_pipe_sync_retry");
    case "resume-fs":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_resume_fs");
    case "defer-passive-wait":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_defer_passive_wait");
    case "resume-kernel-worker":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_resume_kernel_worker");
    case "resume-wait4-kernel-worker":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_resume_wait4_kernel_worker");
    case "resume-epoll-direct":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_resume_epoll_direct");
    case "resume-direct":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_resume_direct");
    case "unsupported":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_action_unsupported");
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

function normalizeSupervisorBlockedRetryAction(
  kernel: KernelExports,
  result: number,
): SupervisorBlockedRetryAction {
  const actions: SupervisorBlockedRetryAction[] = [
    "defer-supervisor-timer",
    "defer-rpc-pending",
    "defer-owner-async",
    "defer-thread-init-pending",
    "arm-wait-timer",
    "park-active-child",
    "resume-wait4",
    "defer-fork-shared-sync",
    "defer-running-worker",
    "defer-process-gate",
    "pump-network",
    "epoll-kernel-worker",
    "request-pipe-sync",
    "request-pipe-sync-and-drain-output",
    "schedule-pipe-sync-retry",
    "resume-fs",
    "defer-passive-wait",
    "resume-kernel-worker",
    "resume-wait4-kernel-worker",
    "resume-epoll-direct",
    "resume-direct",
    "unsupported",
  ];
  for (const action of actions) {
    if (result === supervisorBlockedRetryActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor blocked retry action is unknown: ${result}`);
}

function supervisorBlockedInitialActionValue(
  kernel: KernelExports,
  action: SupervisorBlockedInitialAction,
): number {
  switch (action) {
    case "none":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_initial_action_none");
    case "defer-supervisor-timer":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_initial_action_defer_supervisor_timer");
    case "defer-rpc-pending":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_initial_action_defer_rpc_pending");
    case "arm-wait-timer":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_initial_action_arm_wait_timer");
    case "retry-now":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_initial_action_retry_now");
    case "schedule-retry":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_initial_action_schedule_retry");
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

function normalizeSupervisorBlockedInitialAction(
  kernel: KernelExports,
  result: number,
): SupervisorBlockedInitialAction {
  const actions: SupervisorBlockedInitialAction[] = [
    "none",
    "defer-supervisor-timer",
    "defer-rpc-pending",
    "arm-wait-timer",
    "retry-now",
    "schedule-retry",
  ];
  for (const action of actions) {
    if (result === supervisorBlockedInitialActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor blocked initial action is unknown: ${result}`);
}

export function supervisorBlockedInitialAction(
  kernel: KernelExports,
  vmPtr: number,
  blockedStatus: number,
  flags: SupervisorBlockedRetryActionFlags = [],
): SupervisorBlockedInitialAction {
  const readAction = kernel.supervisor_blocked_initial_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_initial_action");
  }
  const result = readAction.call(
    kernel,
    vmPtr,
    blockedStatus,
    supervisorBlockedRetryActionFlagsValue(kernel, flags),
  );
  if (result < 0) {
    throw new Error(`kernel supervisor blocked initial action failed: ${result}`);
  }
  return normalizeSupervisorBlockedInitialAction(kernel, result);
}

function supervisorBlockedRetryResultActionValue(
  kernel: KernelExports,
  action: SupervisorBlockedRetryResultAction,
): number {
  switch (action) {
    case "none":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_result_action_none");
    case "arm-wait-timer":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_result_action_arm_wait_timer");
    case "schedule-retry":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_result_action_schedule_retry");
    case "schedule-retry-and-pump":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_retry_result_action_schedule_retry_and_pump");
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

function normalizeSupervisorBlockedRetryResultAction(
  kernel: KernelExports,
  result: number,
): SupervisorBlockedRetryResultAction {
  const actions: SupervisorBlockedRetryResultAction[] = [
    "none",
    "arm-wait-timer",
    "schedule-retry",
    "schedule-retry-and-pump",
  ];
  for (const action of actions) {
    if (result === supervisorBlockedRetryResultActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor blocked retry result action is unknown: ${result}`);
}

export function supervisorBlockedRetryResultAction(
  kernel: KernelExports,
  vmPtr: number,
  resultStatus: number,
): SupervisorBlockedRetryResultAction {
  const readAction = kernel.supervisor_blocked_retry_result_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_retry_result_action");
  }
  const result = readAction.call(kernel, vmPtr, resultStatus);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked retry result action failed: ${result}`);
  }
  return normalizeSupervisorBlockedRetryResultAction(kernel, result);
}

export function supervisorBlockedRetryStatusActions(
  kernel: KernelExports,
  status: number,
): SupervisorBlockedRetryStatusActions {
  const readAction = kernel.supervisor_blocked_retry_status_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_retry_status_action");
  }
  const result = readAction.call(kernel, status);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked retry status action failed: ${result}`);
  }
  const pipeRetryDiagnostics = readSupervisorNumberConst(
    kernel,
    "supervisor_blocked_retry_status_action_pipe_retry_diagnostics",
  );
  const pumpBlockedOutcome = readSupervisorNumberConst(
    kernel,
    "supervisor_blocked_retry_status_action_pump_blocked_outcome",
  );
  const activeChildPipe = readSupervisorNumberConst(
    kernel,
    "supervisor_blocked_retry_status_action_active_child_pipe",
  );
  const activeChildWait4 = readSupervisorNumberConst(
    kernel,
    "supervisor_blocked_retry_status_action_active_child_wait4",
  );
  const successLogEpollReady = readSupervisorNumberConst(
    kernel,
    "supervisor_blocked_retry_status_action_success_log_epoll_ready",
  );
  const successLogWait4Resumed = readSupervisorNumberConst(
    kernel,
    "supervisor_blocked_retry_status_action_success_log_wait4_resumed",
  );
  const wakeRuntimeBridgeWhileRunning = readSupervisorNumberConst(
    kernel,
    "supervisor_blocked_retry_status_action_wake_runtime_bridge_while_running",
  );
  const known = readSupervisorNumberConst(kernel, "supervisor_blocked_retry_status_action_none")
    | pipeRetryDiagnostics
    | pumpBlockedOutcome
    | activeChildPipe
    | activeChildWait4
    | successLogEpollReady
    | successLogWait4Resumed
    | wakeRuntimeBridgeWhileRunning;
  if ((result & ~known) !== 0) {
    throw new Error(`kernel supervisor blocked retry status action is unknown: ${result}`);
  }
  return {
    pipeRetryDiagnostics: (result & pipeRetryDiagnostics) !== 0,
    pumpBlockedOutcome: (result & pumpBlockedOutcome) !== 0,
    activeChildPipe: (result & activeChildPipe) !== 0,
    activeChildWait4: (result & activeChildWait4) !== 0,
    successLogEpollReady: (result & successLogEpollReady) !== 0,
    successLogWait4Resumed: (result & successLogWait4Resumed) !== 0,
    wakeRuntimeBridgeWhileRunning: (result & wakeRuntimeBridgeWhileRunning) !== 0,
  };
}

function supervisorWaitInWorkerStatusActionFlags(
  kernel: KernelExports,
  options: {
    runningWorker: boolean;
    kernelWorkerAvailable: boolean;
    epollStateKernelOwns: boolean;
  },
): number {
  let flags = 0;
  if (options.runningWorker) {
    flags |= readSupervisorNumberConst(
      kernel,
      "supervisor_wait_in_worker_status_action_flag_running_worker",
    );
  }
  if (options.kernelWorkerAvailable) {
    flags |= readSupervisorNumberConst(
      kernel,
      "supervisor_wait_in_worker_status_action_flag_kernel_worker_available",
    );
  }
  if (options.epollStateKernelOwns) {
    flags |= readSupervisorNumberConst(
      kernel,
      "supervisor_wait_in_worker_status_action_flag_epoll_state_kernel_owns",
    );
  }
  return flags;
}

export function supervisorWaitInWorkerStatusActions(
  kernel: KernelExports,
  vmPtr: number,
  status: number,
  options: {
    runningWorker: boolean;
    kernelWorkerAvailable: boolean;
    epollStateKernelOwns: boolean;
  },
): SupervisorWaitInWorkerStatusActions {
  const readAction = kernel.supervisor_wait_in_worker_status_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_wait_in_worker_status_action");
  }
  const result = readAction.call(
    kernel,
    vmPtr,
    status,
    supervisorWaitInWorkerStatusActionFlags(kernel, options),
  );
  if (result < 0) {
    throw new Error(`kernel supervisor wait-in-worker status action failed: ${result}`);
  }
  const blockEpollTimeout = readSupervisorNumberConst(
    kernel,
    "supervisor_wait_in_worker_status_action_block_epoll_timeout",
  );
  const parkFutex = readSupervisorNumberConst(
    kernel,
    "supervisor_wait_in_worker_status_action_park_futex",
  );
  const blockWait4AndSyncPipe = readSupervisorNumberConst(
    kernel,
    "supervisor_wait_in_worker_status_action_block_wait4_and_sync_pipe",
  );
  const syncSocket = readSupervisorNumberConst(
    kernel,
    "supervisor_wait_in_worker_status_action_sync_socket",
  );
  const syncEpoll = readSupervisorNumberConst(
    kernel,
    "supervisor_wait_in_worker_status_action_sync_epoll",
  );
  const syncPipe = readSupervisorNumberConst(
    kernel,
    "supervisor_wait_in_worker_status_action_sync_pipe",
  );
  const known = readSupervisorNumberConst(kernel, "supervisor_wait_in_worker_status_action_none")
    | blockEpollTimeout
    | parkFutex
    | blockWait4AndSyncPipe
    | syncSocket
    | syncEpoll
    | syncPipe;
  if ((result & ~known) !== 0) {
    throw new Error(`kernel supervisor wait-in-worker status action is unknown: ${result}`);
  }
  return {
    blockEpollTimeout: (result & blockEpollTimeout) !== 0,
    parkFutex: (result & parkFutex) !== 0,
    blockWait4AndSyncPipe: (result & blockWait4AndSyncPipe) !== 0,
    syncSocket: (result & syncSocket) !== 0,
    syncEpoll: (result & syncEpoll) !== 0,
    syncPipe: (result & syncPipe) !== 0,
  };
}

export function supervisorWait4ChildExitBlockedParentActions(
  kernel: KernelExports,
  status: number,
): SupervisorWait4ChildExitBlockedParentActions {
  const readAction = kernel.supervisor_wait4_child_exit_blocked_parent_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_wait4_child_exit_blocked_parent_action");
  }
  const result = readAction.call(kernel, status);
  if (result < 0) {
    throw new Error(`kernel supervisor wait4 child-exit blocked parent action failed: ${result}`);
  }
  const invalidateRpc = readSupervisorNumberConst(
    kernel,
    "supervisor_wait4_child_exit_blocked_parent_action_invalidate_rpc",
  );
  const clearPipeSyncBeforeRetry = readSupervisorNumberConst(
    kernel,
    "supervisor_wait4_child_exit_blocked_parent_action_clear_pipe_sync_before_retry",
  );
  const known = readSupervisorNumberConst(
    kernel,
    "supervisor_wait4_child_exit_blocked_parent_action_none",
  ) | invalidateRpc | clearPipeSyncBeforeRetry;
  if ((result & ~known) !== 0) {
    throw new Error(`kernel supervisor wait4 child-exit blocked parent action is unknown: ${result}`);
  }
  return {
    invalidateRpc: (result & invalidateRpc) !== 0,
    clearPipeSyncBeforeRetry: (result & clearPipeSyncBeforeRetry) !== 0,
  };
}

function supervisorWait4RpcLocalStatusActionValue(
  kernel: KernelExports,
  action: SupervisorWait4RpcLocalStatusAction,
): number {
  switch (action) {
    case "skip":
      return readSupervisorNumberConst(kernel, "supervisor_wait4_rpc_local_status_action_skip");
    case "start":
      return readSupervisorNumberConst(kernel, "supervisor_wait4_rpc_local_status_action_start");
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

export function supervisorWait4RpcLocalStatusAction(
  kernel: KernelExports,
  status: number,
): SupervisorWait4RpcLocalStatusAction {
  const readAction = kernel.supervisor_wait4_rpc_local_status_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_wait4_rpc_local_status_action");
  }
  const result = readAction.call(kernel, status);
  if (result < 0) {
    throw new Error(`kernel supervisor wait4 RPC local status action failed: ${result}`);
  }
  for (const action of ["skip", "start"] as const) {
    if (result === supervisorWait4RpcLocalStatusActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor wait4 RPC local status action is unknown: ${result}`);
}

function supervisorSchedulerClearBlockedActionFlags(
  kernel: KernelExports,
  options: {
    clearPipeSyncBeforeRetry: boolean;
  },
): number {
  return options.clearPipeSyncBeforeRetry
    ? readSupervisorNumberConst(
        kernel,
        "supervisor_scheduler_clear_blocked_flag_clear_pipe_sync_before_retry",
      )
    : 0;
}

export function supervisorSchedulerClearBlockedWorkerActions(
  kernel: KernelExports,
  status: number,
  options: {
    clearPipeSyncBeforeRetry: boolean;
  },
): SupervisorSchedulerClearBlockedWorkerActions {
  const readAction = kernel.supervisor_scheduler_clear_blocked_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_scheduler_clear_blocked_action");
  }
  const result = readAction.call(
    kernel,
    status,
    supervisorSchedulerClearBlockedActionFlags(kernel, options),
  );
  if (result < 0) {
    throw new Error(`kernel supervisor scheduler clear-blocked action failed: ${result}`);
  }
  const clearPipeSyncBeforeRetry = readSupervisorNumberConst(
    kernel,
    "supervisor_scheduler_clear_blocked_action_clear_pipe_sync_before_retry",
  );
  const known = readSupervisorNumberConst(kernel, "supervisor_scheduler_clear_blocked_action_none")
    | clearPipeSyncBeforeRetry;
  if ((result & ~known) !== 0) {
    throw new Error(`kernel supervisor scheduler clear-blocked action is unknown: ${result}`);
  }
  return {
    clearPipeSyncBeforeRetry: (result & clearPipeSyncBeforeRetry) !== 0,
  };
}

function supervisorSchedulerBlockWorkerActionValue(
  kernel: KernelExports,
  action: SupervisorSchedulerBlockWorkerAction,
): number {
  switch (action) {
    case "record-blocked":
      return readSupervisorNumberConst(kernel, "supervisor_scheduler_block_worker_action_record_blocked");
    case "park-futex":
      return readSupervisorNumberConst(kernel, "supervisor_scheduler_block_worker_action_park_futex");
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

export function supervisorSchedulerBlockWorkerAction(
  kernel: KernelExports,
  status: number,
): SupervisorSchedulerBlockWorkerAction {
  const readAction = kernel.supervisor_scheduler_block_worker_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_scheduler_block_worker_action");
  }
  const result = readAction.call(kernel, status);
  if (result < 0) {
    throw new Error(`kernel supervisor scheduler block-worker action failed: ${result}`);
  }
  for (const action of ["record-blocked", "park-futex"] as const) {
    if (result === supervisorSchedulerBlockWorkerActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor scheduler block-worker action is unknown: ${result}`);
}

function supervisorSchedulerFutexWaiterActionValue(
  kernel: KernelExports,
  action: SupervisorSchedulerFutexWaiterAction,
): number {
  switch (action) {
    case "not-waiter":
      return readSupervisorNumberConst(kernel, "supervisor_scheduler_futex_waiter_action_not_waiter");
    case "waiter":
      return readSupervisorNumberConst(kernel, "supervisor_scheduler_futex_waiter_action_waiter");
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

export function supervisorSchedulerFutexWaiterAction(
  kernel: KernelExports,
  status: number,
): SupervisorSchedulerFutexWaiterAction {
  const readAction = kernel.supervisor_scheduler_futex_waiter_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_scheduler_futex_waiter_action");
  }
  const result = readAction.call(kernel, status);
  if (result < 0) {
    throw new Error(`kernel supervisor scheduler futex-waiter action failed: ${result}`);
  }
  for (const action of ["not-waiter", "waiter"] as const) {
    if (result === supervisorSchedulerFutexWaiterActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor scheduler futex-waiter action is unknown: ${result}`);
}

function supervisorWorkerStatusTransitionActionValue(
  kernel: KernelExports,
  action: SupervisorWorkerStatusTransitionAction,
): number {
  switch (action) {
    case "enqueue":
      return readSupervisorNumberConst(kernel, "supervisor_worker_status_transition_action_enqueue");
    case "connect-wait":
      return readSupervisorNumberConst(kernel, "supervisor_worker_status_transition_action_connect_wait");
    case "park-futex":
      return readSupervisorNumberConst(kernel, "supervisor_worker_status_transition_action_park_futex");
    case "host-wait":
      return readSupervisorNumberConst(kernel, "supervisor_worker_status_transition_action_host_wait");
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

export function supervisorWorkerStatusTransitionAction(
  kernel: KernelExports,
  status: number,
): SupervisorWorkerStatusTransitionAction {
  const readAction = kernel.supervisor_worker_status_transition_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_worker_status_transition_action");
  }
  const result = readAction.call(kernel, status);
  if (result < 0) {
    throw new Error(`kernel supervisor worker status transition action failed: ${result}`);
  }
  for (const action of ["enqueue", "connect-wait", "park-futex", "host-wait"] as const) {
    if (result === supervisorWorkerStatusTransitionActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor worker status transition action is unknown: ${result}`);
}

function supervisorBlockedPostNetworkPumpActionValue(
  kernel: KernelExports,
  action: Exclude<SupervisorBlockedPostNetworkPumpAction, SupervisorBlockedRetryAction>,
): number {
  switch (action) {
    case "schedule-retry":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_post_network_pump_action_schedule_retry");
    case "schedule-retry-with-host-progress":
      return readSupervisorNumberConst(
        kernel,
        "supervisor_blocked_post_network_pump_action_schedule_retry_with_host_progress",
      );
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

function normalizeSupervisorBlockedPostNetworkPumpAction(
  kernel: KernelExports,
  result: number,
): SupervisorBlockedPostNetworkPumpAction {
  const postNetworkActions: Exclude<SupervisorBlockedPostNetworkPumpAction, SupervisorBlockedRetryAction>[] = [
    "schedule-retry",
    "schedule-retry-with-host-progress",
  ];
  for (const action of postNetworkActions) {
    if (result === supervisorBlockedPostNetworkPumpActionValue(kernel, action)) {
      return action;
    }
  }
  return normalizeSupervisorBlockedRetryAction(kernel, result);
}

export function supervisorBlockedPostNetworkPumpAction(
  kernel: KernelExports,
  vmPtr: number,
  changed: boolean,
  flags: SupervisorBlockedRetryActionFlags = [],
): SupervisorBlockedPostNetworkPumpAction {
  const readAction = kernel.supervisor_blocked_post_network_pump_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_post_network_pump_action");
  }
  const result = readAction.call(
    kernel,
    vmPtr,
    changed ? 1 : 0,
    supervisorBlockedRetryActionFlagsValue(kernel, flags),
  );
  if (result < 0) {
    throw new Error(`kernel supervisor blocked post-network pump action failed: ${result}`);
  }
  return normalizeSupervisorBlockedPostNetworkPumpAction(kernel, result);
}

function supervisorBlockedStaleRetryResultActionValue(
  kernel: KernelExports,
  action: SupervisorBlockedStaleRetryResultAction,
): number {
  switch (action) {
    case "none":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_stale_retry_result_action_none");
    case "handle-current-result":
      return readSupervisorNumberConst(
        kernel,
        "supervisor_blocked_stale_retry_result_action_handle_current_result",
      );
    case "handle-current-result-wait4":
      return readSupervisorNumberConst(
        kernel,
        "supervisor_blocked_stale_retry_result_action_handle_current_result_wait4",
      );
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

function normalizeSupervisorBlockedStaleRetryResultAction(
  kernel: KernelExports,
  result: number,
): SupervisorBlockedStaleRetryResultAction {
  const actions: SupervisorBlockedStaleRetryResultAction[] = [
    "none",
    "handle-current-result",
    "handle-current-result-wait4",
  ];
  for (const action of actions) {
    if (result === supervisorBlockedStaleRetryResultActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor blocked stale retry result action is unknown: ${result}`);
}

export function supervisorBlockedStaleRetryResultAction(
  kernel: KernelExports,
  vmPtr: number,
  blockedStatus: number,
): SupervisorBlockedStaleRetryResultAction {
  const readAction = kernel.supervisor_blocked_stale_retry_result_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_stale_retry_result_action");
  }
  const result = readAction.call(kernel, vmPtr, blockedStatus);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked stale retry result action failed: ${result}`);
  }
  return normalizeSupervisorBlockedStaleRetryResultAction(kernel, result);
}

function supervisorBlockedRpcPipeSyncActionValue(
  kernel: KernelExports,
  action: SupervisorBlockedRpcPipeSyncAction,
): number {
  switch (action) {
    case "not-pipe":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_rpc_pipe_sync_action_not_pipe");
    case "none":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_rpc_pipe_sync_action_none");
    case "sync-socket":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_rpc_pipe_sync_action_sync_socket");
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

function normalizeSupervisorBlockedRpcPipeSyncAction(
  kernel: KernelExports,
  result: number,
): SupervisorBlockedRpcPipeSyncAction {
  const actions: SupervisorBlockedRpcPipeSyncAction[] = [
    "not-pipe",
    "none",
    "sync-socket",
  ];
  for (const action of actions) {
    if (result === supervisorBlockedRpcPipeSyncActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor blocked RPC pipe sync action is unknown: ${result}`);
}

export function supervisorBlockedRpcPipeSyncAction(
  kernel: KernelExports,
  vmPtr: number,
  blockedStatus: number,
): SupervisorBlockedRpcPipeSyncAction {
  const readAction = kernel.supervisor_blocked_rpc_pipe_sync_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_rpc_pipe_sync_action");
  }
  const result = readAction.call(kernel, vmPtr, blockedStatus);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked RPC pipe sync action failed: ${result}`);
  }
  return normalizeSupervisorBlockedRpcPipeSyncAction(kernel, result);
}

export function supervisorBlockedRpcPreserveCanonicalEof(
  kernel: KernelExports,
  blockedStatus: number,
): boolean {
  const readAction = kernel.supervisor_blocked_rpc_preserve_canonical_eof;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_rpc_preserve_canonical_eof");
  }
  const result = readAction.call(kernel, blockedStatus);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked RPC canonical EOF action failed: ${result}`);
  }
  return result !== 0;
}

function supervisorBlockedRpcPostResumeSyncFlags(
  kernel: KernelExports,
  options: {
    kernelWorkerAvailable: boolean;
    processOwnerMismatch: boolean;
  },
): number {
  let flags = 0;
  if (options.kernelWorkerAvailable) {
    flags |= readSupervisorNumberConst(
      kernel,
      "supervisor_blocked_rpc_post_resume_sync_flag_kernel_worker_available",
    );
  }
  if (options.processOwnerMismatch) {
    flags |= readSupervisorNumberConst(
      kernel,
      "supervisor_blocked_rpc_post_resume_sync_flag_process_owner_mismatch",
    );
  }
  return flags;
}

export function supervisorBlockedRpcPostResumeSyncAction(
  kernel: KernelExports,
  blockedStatus: number,
  options: {
    kernelWorkerAvailable: boolean;
    processOwnerMismatch: boolean;
  },
): SupervisorBlockedRpcPostResumeSyncAction {
  const readAction = kernel.supervisor_blocked_rpc_post_resume_sync_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_rpc_post_resume_sync_action");
  }
  const result = readAction.call(
    kernel,
    blockedStatus,
    supervisorBlockedRpcPostResumeSyncFlags(kernel, options),
  );
  if (result < 0) {
    throw new Error(`kernel supervisor blocked RPC post-resume sync action failed: ${result}`);
  }
  const none = readSupervisorNumberConst(kernel, "supervisor_blocked_rpc_post_resume_sync_action_none");
  const known =
    none
    | readSupervisorNumberConst(kernel, "supervisor_blocked_rpc_post_resume_sync_action_fork_shared")
    | readSupervisorNumberConst(kernel, "supervisor_blocked_rpc_post_resume_sync_action_socket");
  if ((result & ~known) !== 0) {
    throw new Error(`kernel supervisor blocked RPC post-resume sync action is unknown: ${result}`);
  }
  return {
    syncForkShared: (result & readSupervisorNumberConst(
      kernel,
      "supervisor_blocked_rpc_post_resume_sync_action_fork_shared",
    )) !== 0,
    syncSocket: (result & readSupervisorNumberConst(
      kernel,
      "supervisor_blocked_rpc_post_resume_sync_action_socket",
    )) !== 0,
  };
}

export function supervisorBlockedRetryAction(
  kernel: KernelExports,
  vmPtr: number,
  flags: SupervisorBlockedRetryActionFlags = [],
): SupervisorBlockedRetryAction {
  const readAction = kernel.supervisor_blocked_retry_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_retry_action");
  }
  const result = readAction.call(kernel, vmPtr, supervisorBlockedRetryActionFlagsValue(kernel, flags));
  if (result < 0) {
    throw new Error(`kernel supervisor blocked retry action failed: ${result}`);
  }
  return normalizeSupervisorBlockedRetryAction(kernel, result);
}

function supervisorBlockedWaitWakeSourceValue(
  kernel: KernelExports,
  source: SupervisorBlockedWaitWakeSource,
): number {
  switch (source) {
    case "pipe-sync":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_wait_wake_source_pipe_sync");
    case "epoll-sync":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_wait_wake_source_epoll_sync");
    case "socket-sync":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_wait_wake_source_socket_sync");
    case "pipe-and-epoll-sync":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_wait_wake_source_pipe_and_epoll_sync");
    default: {
      const exhaustive: never = source;
      return exhaustive;
    }
  }
}

export function supervisorBlockedWaitStatusMatchesWakeSource(
  kernel: KernelExports,
  status: number,
  source: SupervisorBlockedWaitWakeSource,
): boolean {
  const readMatch = kernel.supervisor_blocked_wait_status_matches_wake_source;
  if (readMatch === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_wait_status_matches_wake_source");
  }
  const result = readMatch.call(kernel, status, supervisorBlockedWaitWakeSourceValue(kernel, source));
  if (result < 0) {
    throw new Error(`kernel supervisor blocked wait status wake match failed: ${result}`);
  }
  return result !== 0;
}

function supervisorBlockedWaitWakeFlags(
  kernel: KernelExports,
  options: { rpcPending: boolean },
): number {
  let flags = 0;
  if (options.rpcPending) {
    flags |= readSupervisorNumberConst(kernel, "supervisor_blocked_wait_wake_flag_rpc_pending");
  }
  return flags;
}

export function supervisorBlockedWaitWakeAction(
  kernel: KernelExports,
  status: number,
  source: SupervisorBlockedWaitWakeSource,
  options: { rpcPending: boolean },
): SupervisorBlockedWaitWakeAction {
  const readAction = kernel.supervisor_blocked_wait_wake_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_wait_wake_action");
  }
  const result = readAction.call(
    kernel,
    status,
    supervisorBlockedWaitWakeSourceValue(kernel, source),
    supervisorBlockedWaitWakeFlags(kernel, options),
  );
  if (result < 0) {
    throw new Error(`kernel supervisor blocked wait wake action failed: ${result}`);
  }
  if (result === readSupervisorNumberConst(kernel, "supervisor_blocked_wait_wake_action_skip")) {
    return "skip";
  }
  if (result === readSupervisorNumberConst(kernel, "supervisor_blocked_wait_wake_action_retry")) {
    return "retry";
  }
  if (
    result === readSupervisorNumberConst(
      kernel,
      "supervisor_blocked_wait_wake_action_clear_pipe_sync_and_retry",
    )
  ) {
    return "clear-pipe-sync-and-retry";
  }
  if (
    result === readSupervisorNumberConst(
      kernel,
      "supervisor_blocked_wait_wake_action_log_epoll_and_retry",
    )
  ) {
    return "log-epoll-and-retry";
  }
  throw new Error(`kernel supervisor blocked wait wake action is unknown: ${result}`);
}

export function supervisorBlockedWaitStatusMatchesPipeWait(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorBlockedWaitStatusMatchesWakeSource(
    kernel,
    status,
    "pipe-sync",
  );
}

export function supervisorBlockedWaitStatusMatchesEpollWait(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorBlockedWaitStatusMatchesWakeSource(
    kernel,
    status,
    "epoll-sync",
  );
}

export function supervisorBlockedWaitStatusMatchesSocketWait(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorBlockedStatusIsSocketWait(kernel, status);
}

export function supervisorBlockedWaitTimerRequired(
  kernel: KernelExports,
  vmPtr: number,
): boolean {
  const required = kernel.supervisor_blocked_wait_timer_required;
  if (required === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_wait_timer_required");
  }
  const result = required.call(kernel, vmPtr);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked wait timer required check failed: ${result}`);
  }
  return result !== 0;
}
