import type { KernelExports } from "./abi.js";

const SUPERVISOR_RESUME_FLAG_TIMEOUT_EXPIRED = 1;
const SUPERVISOR_BLOCKED_RESUME_FLAG_KERNEL_WORKER_AVAILABLE = 1;
export const SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT = 0;
export const SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER = 1;
export const SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER = 2;
export const SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED = 3;
export const SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT = 4;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_DEFAULT = 0;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_IMMEDIATE = 1;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_STANDARD_DEFER = 2;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_ACTIVE_CHILD = 3;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PASSIVE_WAIT = 4;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PROCESS_GATE = 5;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE = 0;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_HOST_PROGRESS = 1;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_ACTIVE_CHILD = 1 << 1;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_PASSIVE_WAIT = 1 << 2;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_PROCESS_GATE = 1 << 3;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_THREAD_INIT_PENDING = 1 << 4;
export const SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DEFER = 0;
export const SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DIRECT = 1;
export const SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_NONE = 0;
export const SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC = 1;
export const SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC_AND_DRAIN_OUTPUT = 2;
export const SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_SCHEDULE_RETRY = 3;
const SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_FLAG_KERNEL_WORKER_AVAILABLE = 1;
export const SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_BLOCK = 0;
export const SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET = 1;
export const SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE = 2;
export const SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_FORK_SHARED = 3;
export const SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE_AND_FORK_SHARED = 4;
export const SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET_AND_FORK_SHARED = 5;
const SUPERVISOR_BLOCKED_STATUS_TRANSITION_FLAG_KERNEL_WORKER_AVAILABLE = 1;
const SUPERVISOR_BLOCKED_STATUS_TRANSITION_FLAG_NETWORK_CONNECTIONS_AVAILABLE = 1 << 1;
export const SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_SKIP = 0;
export const SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_PUMP = 1;
export const SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_SYNC = 1;
export const SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_EPOLL_SYNC = 2;
export const SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_SOCKET_SYNC = 3;
export const SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_AND_EPOLL_SYNC = 4;
export const SUPERVISOR_STATE_SYNC_KIND_PIPE = 1;
export const SUPERVISOR_STATE_SYNC_KIND_EPOLL = 2;
export const SUPERVISOR_STATE_SYNC_KIND_SOCKET = 3;
const SUPERVISOR_STATE_SYNC_REQUEST_FLAG_WAKE_WAITERS = 1;
const SUPERVISOR_STATE_SYNC_RUN_FLAG_RUN = 1;
const SUPERVISOR_STATE_SYNC_RUN_FLAG_WAKE_WAITERS = 1 << 1;

export type SupervisorBlockedWaitRetryReason =
  | typeof SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_DEFAULT
  | typeof SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_IMMEDIATE
  | typeof SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_STANDARD_DEFER
  | typeof SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_ACTIVE_CHILD
  | typeof SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PASSIVE_WAIT
  | typeof SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PROCESS_GATE;

export type SupervisorBlockedWaitRetryFlags = number;

export interface SupervisorBlockedWaitRetryPlan {
  reason: SupervisorBlockedWaitRetryReason;
  delayMs: number;
}

export type SupervisorBlockedResumeDelegation =
  | typeof SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT
  | typeof SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER
  | typeof SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER
  | typeof SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED
  | typeof SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT;

export type SupervisorBlockedPipeResumeAction =
  | typeof SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DEFER
  | typeof SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DIRECT;

export type SupervisorBlockedPipeSyncBeforeRetryAction =
  | typeof SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_NONE
  | typeof SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC
  | typeof SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC_AND_DRAIN_OUTPUT
  | typeof SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_SCHEDULE_RETRY;

export type SupervisorBlockedStatusTransitionAction =
  | typeof SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_BLOCK
  | typeof SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET
  | typeof SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE
  | typeof SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_FORK_SHARED
  | typeof SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE_AND_FORK_SHARED
  | typeof SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET_AND_FORK_SHARED;

export type SupervisorBlockedWaitNetworkPumpAction =
  | typeof SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_SKIP
  | typeof SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_PUMP;

export type SupervisorBlockedWaitWakeSource =
  | typeof SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_SYNC
  | typeof SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_EPOLL_SYNC
  | typeof SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_SOCKET_SYNC
  | typeof SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_AND_EPOLL_SYNC;

export type SupervisorStateSyncKind =
  | typeof SUPERVISOR_STATE_SYNC_KIND_PIPE
  | typeof SUPERVISOR_STATE_SYNC_KIND_EPOLL
  | typeof SUPERVISOR_STATE_SYNC_KIND_SOCKET;

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
  return BigInt(resume(vmPtr, timeoutExpired ? SUPERVISOR_RESUME_FLAG_TIMEOUT_EXPIRED : 0));
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
  const count = countRanges(vmPtr, blockedStatus, resumeResult);
  if (count < 0) {
    throw new Error(`kernel supervisor blocked resume writeback count failed: ${count}`);
  }
  const ranges: SupervisorBlockedResumeWritebackRange[] = [];
  for (let index = 0; index < count; index += 1) {
    const guestPtr = toNumber(readPtr(vmPtr, blockedStatus, resumeResult, index));
    const length = toNumber(readLen(vmPtr, blockedStatus, resumeResult, index));
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
  const result = toNumber(readTimeout(vmPtr));
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
  const result = toNumber(readTimeout(vmPtr));
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
  const result = readStatus(status);
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
  const result = readFact(vmPtr);
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
    case SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DIRECT:
      return kernel.supervisor_blocked_pipe_resume_action_direct?.() ?? action;
    case SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DEFER:
      return kernel.supervisor_blocked_pipe_resume_action_defer?.() ?? action;
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
    SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DEFER,
    SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DIRECT,
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
  const result = readAction(vmPtr);
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
    case SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC:
      return kernel.supervisor_blocked_pipe_sync_before_retry_action_request_sync?.() ?? action;
    case SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC_AND_DRAIN_OUTPUT:
      return kernel.supervisor_blocked_pipe_sync_before_retry_action_request_sync_and_drain_output?.() ?? action;
    case SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_SCHEDULE_RETRY:
      return kernel.supervisor_blocked_pipe_sync_before_retry_action_schedule_retry?.() ?? action;
    case SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_NONE:
      return kernel.supervisor_blocked_pipe_sync_before_retry_action_none?.() ?? action;
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
    SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_NONE,
    SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC,
    SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC_AND_DRAIN_OUTPUT,
    SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_SCHEDULE_RETRY,
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
  const kernelWorkerFlag = kernel.supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available?.()
    ?? SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_FLAG_KERNEL_WORKER_AVAILABLE;
  const result = readAction(vmPtr, kernelWorkerAvailable ? kernelWorkerFlag : 0);
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
  const result = readRequested(vmPtr);
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
  const result = clear(vmPtr);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked pipe sync-before-retry clear failed: ${result}`);
  }
}

function supervisorBlockedStatusTransitionActionValue(
  kernel: KernelExports,
  action: SupervisorBlockedStatusTransitionAction,
): number {
  switch (action) {
    case SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET:
      return kernel.supervisor_blocked_status_transition_action_sync_socket?.() ?? action;
    case SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE:
      return kernel.supervisor_blocked_status_transition_action_sync_pipe?.() ?? action;
    case SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_FORK_SHARED:
      return kernel.supervisor_blocked_status_transition_action_sync_fork_shared?.() ?? action;
    case SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE_AND_FORK_SHARED:
      return kernel.supervisor_blocked_status_transition_action_sync_pipe_and_fork_shared?.() ?? action;
    case SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET_AND_FORK_SHARED:
      return kernel.supervisor_blocked_status_transition_action_sync_socket_and_fork_shared?.() ?? action;
    case SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_BLOCK:
      return kernel.supervisor_blocked_status_transition_action_block?.() ?? action;
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
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_BLOCK,
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET,
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE,
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_FORK_SHARED,
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_PIPE_AND_FORK_SHARED,
    SUPERVISOR_BLOCKED_STATUS_TRANSITION_ACTION_SYNC_SOCKET_AND_FORK_SHARED,
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
    flags |= kernel.supervisor_blocked_status_transition_flag_kernel_worker_available?.()
      ?? SUPERVISOR_BLOCKED_STATUS_TRANSITION_FLAG_KERNEL_WORKER_AVAILABLE;
  }
  if (options.networkConnectionsAvailable) {
    flags |= kernel.supervisor_blocked_status_transition_flag_network_connections_available?.()
      ?? SUPERVISOR_BLOCKED_STATUS_TRANSITION_FLAG_NETWORK_CONNECTIONS_AVAILABLE;
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
  const result = readAction(
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
    case SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_SKIP:
      return kernel.supervisor_blocked_wait_network_pump_action_skip?.() ?? action;
    case SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_PUMP:
      return kernel.supervisor_blocked_wait_network_pump_action_pump?.() ?? action;
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
    SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_SKIP,
    SUPERVISOR_BLOCKED_WAIT_NETWORK_PUMP_ACTION_PUMP,
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
  const result = readAction(vmPtr);
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
  const result = complete(vmPtr, changed ? 1 : 0);
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
  const result = clear(vmPtr);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked wait network pump clear failed: ${result}`);
  }
}

function supervisorStateSyncKindValue(
  kernel: KernelExports,
  kind: SupervisorStateSyncKind,
): number {
  switch (kind) {
    case SUPERVISOR_STATE_SYNC_KIND_PIPE:
      return kernel.supervisor_state_sync_kind_pipe?.() ?? kind;
    case SUPERVISOR_STATE_SYNC_KIND_EPOLL:
      return kernel.supervisor_state_sync_kind_epoll?.() ?? kind;
    case SUPERVISOR_STATE_SYNC_KIND_SOCKET:
      return kernel.supervisor_state_sync_kind_socket?.() ?? kind;
    default: {
      const exhaustive: never = kind;
      return exhaustive;
    }
  }
}

function supervisorStateSyncRequestFlags(kernel: KernelExports, wakeWaiters: boolean): number {
  return wakeWaiters
    ? kernel.supervisor_state_sync_request_flag_wake_waiters?.()
      ?? SUPERVISOR_STATE_SYNC_REQUEST_FLAG_WAKE_WAITERS
    : 0;
}

function supervisorStateSyncRunFlagRun(kernel: KernelExports): number {
  return kernel.supervisor_state_sync_run_flag_run?.() ?? SUPERVISOR_STATE_SYNC_RUN_FLAG_RUN;
}

function supervisorStateSyncRunFlagWakeWaiters(kernel: KernelExports): number {
  return kernel.supervisor_state_sync_run_flag_wake_waiters?.()
    ?? SUPERVISOR_STATE_SYNC_RUN_FLAG_WAKE_WAITERS;
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
  const result = request(
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
  const result = nextRun(vmPtr, supervisorStateSyncKindValue(kernel, kind));
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
  const result = complete(vmPtr, supervisorStateSyncKindValue(kernel, kind));
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
  const result = clear(vmPtr, supervisorStateSyncKindValue(kernel, kind));
  if (result < 0) {
    throw new Error(`kernel supervisor state-sync clear failed: ${result}`);
  }
}

function supervisorRetryReason(
  kernel: KernelExports,
  reason: SupervisorBlockedWaitRetryReason,
): number {
  switch (reason) {
    case SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_IMMEDIATE:
      return kernel.supervisor_blocked_wait_retry_reason_immediate?.() ?? reason;
    case SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_STANDARD_DEFER:
      return kernel.supervisor_blocked_wait_retry_reason_standard_defer?.() ?? reason;
    case SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_ACTIVE_CHILD:
      return kernel.supervisor_blocked_wait_retry_reason_active_child?.() ?? reason;
    case SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PASSIVE_WAIT:
      return kernel.supervisor_blocked_wait_retry_reason_passive_wait?.() ?? reason;
    case SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PROCESS_GATE:
      return kernel.supervisor_blocked_wait_retry_reason_process_gate?.() ?? reason;
    case SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_DEFAULT:
      return kernel.supervisor_blocked_wait_retry_reason_default?.() ?? reason;
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
    SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_DEFAULT,
    SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_IMMEDIATE,
    SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_STANDARD_DEFER,
    SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_ACTIVE_CHILD,
    SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PASSIVE_WAIT,
    SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PROCESS_GATE,
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
  if ((flags & SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_HOST_PROGRESS) !== 0) {
    result |= kernel.supervisor_blocked_wait_retry_flag_host_progress?.()
      ?? SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_HOST_PROGRESS;
  }
  if ((flags & SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_ACTIVE_CHILD) !== 0) {
    result |= kernel.supervisor_blocked_wait_retry_flag_active_child?.()
      ?? SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_ACTIVE_CHILD;
  }
  if ((flags & SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_PASSIVE_WAIT) !== 0) {
    result |= kernel.supervisor_blocked_wait_retry_flag_passive_wait?.()
      ?? SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_PASSIVE_WAIT;
  }
  if ((flags & SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_PROCESS_GATE) !== 0) {
    result |= kernel.supervisor_blocked_wait_retry_flag_process_gate?.()
      ?? SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_PROCESS_GATE;
  }
  if ((flags & SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_THREAD_INIT_PENDING) !== 0) {
    result |= kernel.supervisor_blocked_wait_retry_flag_thread_init_pending?.()
      ?? SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_THREAD_INIT_PENDING;
  }
  return result;
}

function supervisorBlockedResumeDelegationValue(
  kernel: KernelExports,
  delegation: SupervisorBlockedResumeDelegation,
): number {
  switch (delegation) {
    case SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER:
      return kernel.supervisor_blocked_resume_delegation_kernel_worker?.() ?? delegation;
    case SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER:
      return kernel.supervisor_blocked_resume_delegation_wait4_kernel_worker?.() ?? delegation;
    case SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED:
      return kernel.supervisor_blocked_resume_delegation_unsupported?.() ?? delegation;
    case SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT:
      return kernel.supervisor_blocked_resume_delegation_epoll_direct?.() ?? delegation;
    case SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT:
      return kernel.supervisor_blocked_resume_delegation_direct?.() ?? delegation;
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
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT,
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER,
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER,
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED,
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT,
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
  const kernelWorkerFlag = kernel.supervisor_blocked_resume_delegation_flag_kernel_worker_available?.()
    ?? SUPERVISOR_BLOCKED_RESUME_FLAG_KERNEL_WORKER_AVAILABLE;
  const flags = kernelWorkerAvailable ? kernelWorkerFlag : 0;
  const result = readDelegation(vmPtr, flags);
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
  const result = readDelay(vmPtr, supervisorRetryReason(kernel, reason));
  if (result < 0) {
    throw new Error(`kernel supervisor blocked wait retry delay failed: ${result}`);
  }
  return result;
}

export function supervisorBlockedWaitRetryReason(
  kernel: KernelExports,
  vmPtr: number,
  flags: SupervisorBlockedWaitRetryFlags = SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE,
): SupervisorBlockedWaitRetryReason {
  const readReason = kernel.supervisor_blocked_wait_retry_reason;
  if (readReason === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_wait_retry_reason");
  }
  const result = readReason(vmPtr, supervisorRetryFlags(kernel, flags));
  if (result < 0) {
    throw new Error(`kernel supervisor blocked wait retry reason failed: ${result}`);
  }
  return normalizeSupervisorBlockedWaitRetryReason(kernel, result);
}

export function supervisorBlockedWaitRetryPlan(
  kernel: KernelExports,
  vmPtr: number,
  flags: SupervisorBlockedWaitRetryFlags = SUPERVISOR_BLOCKED_WAIT_RETRY_FLAG_NONE,
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
    case SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_SYNC:
      return kernel.supervisor_blocked_wait_wake_source_pipe_sync?.() ?? source;
    case SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_EPOLL_SYNC:
      return kernel.supervisor_blocked_wait_wake_source_epoll_sync?.() ?? source;
    case SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_SOCKET_SYNC:
      return kernel.supervisor_blocked_wait_wake_source_socket_sync?.() ?? source;
    case SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_AND_EPOLL_SYNC:
      return kernel.supervisor_blocked_wait_wake_source_pipe_and_epoll_sync?.() ?? source;
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
    SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_SYNC,
  );
}

export function supervisorBlockedWaitStatusMatchesEpollWait(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorBlockedWaitStatusMatchesWakeSource(
    kernel,
    status,
    SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_EPOLL_SYNC,
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
  const result = required(vmPtr);
  if (result < 0) {
    throw new Error(`kernel supervisor blocked wait timer required check failed: ${result}`);
  }
  return result !== 0;
}
