import type { KernelExports } from "./abi.js";

export type SupervisorBlockedWaitRetryReason =
  | "default"
  | "immediate"
  | "standard-defer"
  | "active-child"
  | "passive-wait"
  | "process-gate";

export type SupervisorBlockedWaitRetryFlag =
  | "host-progress"
  | "active-child"
  | "passive-wait"
  | "process-gate"
  | "thread-init-pending";

export type SupervisorBlockedWaitRetryFlags = readonly SupervisorBlockedWaitRetryFlag[];

export type SupervisorBlockedResumeDelegation =
  | "direct"
  | "kernel-worker"
  | "wait4-kernel-worker"
  | "unsupported"
  | "epoll-direct";

export type SupervisorBlockedPipeResumeAction =
  | "defer"
  | "direct";

export type SupervisorBlockedPipeSyncBeforeRetryAction =
  | "none"
  | "request-sync"
  | "request-sync-and-drain-output"
  | "schedule-retry";

export type SupervisorBlockedStatusTransitionAction =
  | "block"
  | "sync-socket"
  | "sync-pipe"
  | "sync-fork-shared"
  | "sync-pipe-and-fork-shared"
  | "sync-socket-and-fork-shared";

export type SupervisorBlockedWaitNetworkPumpAction =
  | "skip"
  | "pump";

export type SupervisorBlockedWaitWakeSource =
  | "pipe-sync"
  | "epoll-sync"
  | "socket-sync"
  | "pipe-and-epoll-sync";

export type SupervisorStateSyncKind =
  | "pipe"
  | "epoll"
  | "socket";

type SupervisorNumberConstExportName =
  | "supervisor_resume_flag_timeout_expired"
  | "supervisor_blocked_resume_delegation_flag_kernel_worker_available"
  | "supervisor_blocked_resume_delegation_direct"
  | "supervisor_blocked_resume_delegation_kernel_worker"
  | "supervisor_blocked_resume_delegation_wait4_kernel_worker"
  | "supervisor_blocked_resume_delegation_unsupported"
  | "supervisor_blocked_resume_delegation_epoll_direct"
  | "supervisor_blocked_pipe_resume_action_defer"
  | "supervisor_blocked_pipe_resume_action_direct"
  | "supervisor_blocked_pipe_sync_before_retry_action_none"
  | "supervisor_blocked_pipe_sync_before_retry_action_request_sync"
  | "supervisor_blocked_pipe_sync_before_retry_action_request_sync_and_drain_output"
  | "supervisor_blocked_pipe_sync_before_retry_action_schedule_retry"
  | "supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available"
  | "supervisor_blocked_status_transition_action_block"
  | "supervisor_blocked_status_transition_action_sync_socket"
  | "supervisor_blocked_status_transition_action_sync_pipe"
  | "supervisor_blocked_status_transition_action_sync_fork_shared"
  | "supervisor_blocked_status_transition_action_sync_pipe_and_fork_shared"
  | "supervisor_blocked_status_transition_action_sync_socket_and_fork_shared"
  | "supervisor_blocked_status_transition_flag_kernel_worker_available"
  | "supervisor_blocked_status_transition_flag_network_connections_available"
  | "supervisor_blocked_wait_network_pump_action_skip"
  | "supervisor_blocked_wait_network_pump_action_pump"
  | "supervisor_state_sync_kind_pipe"
  | "supervisor_state_sync_kind_epoll"
  | "supervisor_state_sync_kind_socket"
  | "supervisor_state_sync_request_flag_wake_waiters"
  | "supervisor_state_sync_run_flag_run"
  | "supervisor_state_sync_run_flag_wake_waiters"
  | "supervisor_blocked_wait_retry_reason_default"
  | "supervisor_blocked_wait_retry_reason_immediate"
  | "supervisor_blocked_wait_retry_reason_standard_defer"
  | "supervisor_blocked_wait_retry_reason_active_child"
  | "supervisor_blocked_wait_retry_reason_passive_wait"
  | "supervisor_blocked_wait_retry_reason_process_gate"
  | "supervisor_blocked_wait_retry_flag_host_progress"
  | "supervisor_blocked_wait_retry_flag_active_child"
  | "supervisor_blocked_wait_retry_flag_passive_wait"
  | "supervisor_blocked_wait_retry_flag_process_gate"
  | "supervisor_blocked_wait_retry_flag_thread_init_pending"
  | "supervisor_blocked_wait_wake_source_pipe_sync"
  | "supervisor_blocked_wait_wake_source_epoll_sync"
  | "supervisor_blocked_wait_wake_source_socket_sync"
  | "supervisor_blocked_wait_wake_source_pipe_and_epoll_sync";

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

export interface SupervisorBlockedWaitRetryPlan {
  reason: SupervisorBlockedWaitRetryReason;
  delayMs: number;
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

export function supervisorBlockedStatusUsesForkSharedStateSync(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorBlockedStatusFact(kernel, status, "supervisor_blocked_status_uses_fork_shared_state_sync");
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
    | "supervisor_blocked_status_syncs_socket_after_resume"
    | "supervisor_blocked_status_uses_fork_shared_state_sync",
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
    | "supervisor_blocked_wait_should_pump_network"
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

export function supervisorBlockedWaitShouldPumpNetwork(
  kernel: KernelExports,
  vmPtr: number,
): boolean {
  return supervisorBlockedBooleanFact(kernel, vmPtr, "supervisor_blocked_wait_should_pump_network");
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

function supervisorBlockedPipeSyncBeforeRetryActionValue(
  kernel: KernelExports,
  action: SupervisorBlockedPipeSyncBeforeRetryAction,
): number {
  switch (action) {
    case "request-sync":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_pipe_sync_before_retry_action_request_sync");
    case "request-sync-and-drain-output":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_pipe_sync_before_retry_action_request_sync_and_drain_output");
    case "schedule-retry":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_pipe_sync_before_retry_action_schedule_retry");
    case "none":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_pipe_sync_before_retry_action_none");
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

function normalizeSupervisorBlockedPipeSyncBeforeRetryAction(
  kernel: KernelExports,
  result: number,
): SupervisorBlockedPipeSyncBeforeRetryAction {
  const actions: SupervisorBlockedPipeSyncBeforeRetryAction[] = [
    "none",
    "request-sync",
    "request-sync-and-drain-output",
    "schedule-retry",
  ];
  for (const action of actions) {
    if (result === supervisorBlockedPipeSyncBeforeRetryActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor blocked pipe sync-before-retry action is unknown: ${result}`);
}

export function supervisorBlockedPipeSyncBeforeRetryAction(
  kernel: KernelExports,
  vmPtr: number,
  kernelWorkerAvailable: boolean,
): SupervisorBlockedPipeSyncBeforeRetryAction {
  const readAction = kernel.supervisor_blocked_pipe_sync_before_retry_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor blocked pipe sync-before-retry action export is missing");
  }
  const kernelWorkerFlag = readSupervisorNumberConst(
    kernel,
    "supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available",
  );
  const result = readAction.call(kernel, vmPtr, kernelWorkerAvailable ? kernelWorkerFlag : 0);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked pipe sync-before-retry action failed: ${result}`);
  }
  return normalizeSupervisorBlockedPipeSyncBeforeRetryAction(kernel, result);
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

function supervisorBlockedWaitNetworkPumpActionValue(
  kernel: KernelExports,
  action: SupervisorBlockedWaitNetworkPumpAction,
): number {
  switch (action) {
    case "skip":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_wait_network_pump_action_skip");
    case "pump":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_wait_network_pump_action_pump");
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

function normalizeSupervisorBlockedWaitNetworkPumpAction(
  kernel: KernelExports,
  result: number,
): SupervisorBlockedWaitNetworkPumpAction {
  const actions: SupervisorBlockedWaitNetworkPumpAction[] = [
    "skip",
    "pump",
  ];
  for (const action of actions) {
    if (result === supervisorBlockedWaitNetworkPumpActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor blocked wait network pump action is unknown: ${result}`);
}

export function supervisorBlockedWaitNetworkPumpAction(
  kernel: KernelExports,
  vmPtr: number,
): SupervisorBlockedWaitNetworkPumpAction {
  const readAction = kernel.supervisor_blocked_wait_network_pump_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor blocked wait network pump action export is missing");
  }
  const result = readAction.call(kernel, vmPtr);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked wait network pump action failed: ${result}`);
  }
  return normalizeSupervisorBlockedWaitNetworkPumpAction(kernel, result);
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

function supervisorRetryReason(
  kernel: KernelExports,
  reason: SupervisorBlockedWaitRetryReason,
): number {
  switch (reason) {
    case "immediate":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_wait_retry_reason_immediate");
    case "standard-defer":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_wait_retry_reason_standard_defer");
    case "active-child":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_wait_retry_reason_active_child");
    case "passive-wait":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_wait_retry_reason_passive_wait");
    case "process-gate":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_wait_retry_reason_process_gate");
    case "default":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_wait_retry_reason_default");
    default: {
      const exhaustive: never = reason;
      return exhaustive;
    }
  }
}

function normalizeSupervisorBlockedWaitRetryReason(
  kernel: KernelExports,
  result: number,
): SupervisorBlockedWaitRetryReason {
  const reasons: SupervisorBlockedWaitRetryReason[] = [
    "default",
    "immediate",
    "standard-defer",
    "active-child",
    "passive-wait",
    "process-gate",
  ];
  for (const reason of reasons) {
    if (result === supervisorRetryReason(kernel, reason)) {
      return reason;
    }
  }
  throw new Error(`kernel supervisor blocked wait retry reason is unknown: ${result}`);
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

function supervisorBlockedResumeDelegationValue(
  kernel: KernelExports,
  delegation: SupervisorBlockedResumeDelegation,
): number {
  switch (delegation) {
    case "kernel-worker":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_resume_delegation_kernel_worker");
    case "wait4-kernel-worker":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_resume_delegation_wait4_kernel_worker");
    case "unsupported":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_resume_delegation_unsupported");
    case "epoll-direct":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_resume_delegation_epoll_direct");
    case "direct":
      return readSupervisorNumberConst(kernel, "supervisor_blocked_resume_delegation_direct");
    default: {
      const exhaustive: never = delegation;
      return exhaustive;
    }
  }
}

function normalizeSupervisorBlockedResumeDelegation(
  kernel: KernelExports,
  result: number,
): SupervisorBlockedResumeDelegation {
  const delegations: SupervisorBlockedResumeDelegation[] = [
    "direct",
    "kernel-worker",
    "wait4-kernel-worker",
    "unsupported",
    "epoll-direct",
  ];
  for (const delegation of delegations) {
    if (result === supervisorBlockedResumeDelegationValue(kernel, delegation)) {
      return delegation;
    }
  }
  throw new Error(`kernel supervisor blocked resume delegation is unknown: ${result}`);
}

export function supervisorBlockedResumeDelegation(
  kernel: KernelExports,
  vmPtr: number,
  kernelWorkerAvailable: boolean,
): SupervisorBlockedResumeDelegation {
  const readDelegation = kernel.supervisor_blocked_resume_delegation;
  if (readDelegation === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_resume_delegation");
  }
  const kernelWorkerFlag = readSupervisorNumberConst(
    kernel,
    "supervisor_blocked_resume_delegation_flag_kernel_worker_available",
  );
  const flags = kernelWorkerAvailable ? kernelWorkerFlag : 0;
  const result = readDelegation.call(kernel, vmPtr, flags);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked resume delegation failed: ${result}`);
  }
  return normalizeSupervisorBlockedResumeDelegation(kernel, result);
}

export function supervisorBlockedWaitRetryDelayMs(
  kernel: KernelExports,
  vmPtr: number,
  reason: SupervisorBlockedWaitRetryReason,
): number {
  const readDelay = kernel.supervisor_blocked_wait_retry_delay_ms;
  if (readDelay === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_wait_retry_delay_ms");
  }
  const result = readDelay.call(kernel, vmPtr, supervisorRetryReason(kernel, reason));
  if (result < 0) {
    throw new Error(`kernel supervisor blocked wait retry delay failed: ${result}`);
  }
  return result;
}

export function supervisorBlockedWaitRetryReason(
  kernel: KernelExports,
  vmPtr: number,
  flags: SupervisorBlockedWaitRetryFlags = [],
): SupervisorBlockedWaitRetryReason {
  const readReason = kernel.supervisor_blocked_wait_retry_reason;
  if (readReason === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_wait_retry_reason");
  }
  const result = readReason.call(kernel, vmPtr, supervisorRetryFlags(kernel, flags));
  if (result < 0) {
    throw new Error(`kernel supervisor blocked wait retry reason failed: ${result}`);
  }
  return normalizeSupervisorBlockedWaitRetryReason(kernel, result);
}

export function supervisorBlockedWaitRetryPlan(
  kernel: KernelExports,
  vmPtr: number,
  flags: SupervisorBlockedWaitRetryFlags = [],
): SupervisorBlockedWaitRetryPlan {
  const reason = supervisorBlockedWaitRetryReason(kernel, vmPtr, flags);
  return {
    reason,
    delayMs: supervisorBlockedWaitRetryDelayMs(kernel, vmPtr, reason),
  };
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
