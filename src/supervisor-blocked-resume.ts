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
export const SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_SYNC = 1;
export const SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_EPOLL_SYNC = 2;
export const SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_SOCKET_SYNC = 3;
export const SUPERVISOR_BLOCKED_WAIT_WAKE_SOURCE_PIPE_AND_EPOLL_SYNC = 4;
export const SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_PIPE_WAIT = 1;
export const SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_EPOLL_WAIT = 1 << 1;
export const SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_SOCKET_RECV = 1 << 2;
export const SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_SOCKET_SEND = 1 << 3;
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

function supervisorBlockedBooleanFact(
  kernel: KernelExports,
  vmPtr: number,
  exportName:
    | "supervisor_blocked_poll_wait_has_socket"
    | "supervisor_blocked_pipe_wait_has_ready_events"
    | "supervisor_blocked_wait_may_depend_on_network"
    | "supervisor_blocked_wait_should_pump_network",
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

export function supervisorBlockedWaitMayDependOnNetwork(
  kernel: KernelExports,
  vmPtr: number,
): boolean {
  return supervisorBlockedBooleanFact(kernel, vmPtr, "supervisor_blocked_wait_may_depend_on_network");
}

export function supervisorBlockedWaitShouldPumpNetwork(
  kernel: KernelExports,
  vmPtr: number,
): boolean {
  return supervisorBlockedBooleanFact(kernel, vmPtr, "supervisor_blocked_wait_should_pump_network");
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

function supervisorBlockedWaitWakeTargetPipeWait(kernel: KernelExports): number {
  return kernel.supervisor_blocked_wait_wake_target_pipe_wait?.()
    ?? SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_PIPE_WAIT;
}

function supervisorBlockedWaitWakeTargetEpollWait(kernel: KernelExports): number {
  return kernel.supervisor_blocked_wait_wake_target_epoll_wait?.()
    ?? SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_EPOLL_WAIT;
}

function supervisorBlockedWaitWakeTargetSocketRecv(kernel: KernelExports): number {
  return kernel.supervisor_blocked_wait_wake_target_socket_recv?.()
    ?? SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_SOCKET_RECV;
}

function supervisorBlockedWaitWakeTargetSocketSend(kernel: KernelExports): number {
  return kernel.supervisor_blocked_wait_wake_target_socket_send?.()
    ?? SUPERVISOR_BLOCKED_WAIT_WAKE_TARGET_SOCKET_SEND;
}

export function supervisorBlockedWaitWakeMaskForSync(
  kernel: KernelExports,
  source: SupervisorBlockedWaitWakeSource,
): number {
  const readMask = kernel.supervisor_blocked_wait_wake_mask_for_sync;
  if (readMask === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_wait_wake_mask_for_sync");
  }
  const result = readMask(supervisorBlockedWaitWakeSourceValue(kernel, source));
  if (result < 0) {
    throw new Error(`kernel supervisor blocked wait wake mask failed: ${result}`);
  }
  return result;
}

export function supervisorBlockedWaitWakeStatuses(
  kernel: KernelExports,
  mask: number,
): Set<number> {
  const statuses = new Set<number>();
  if ((mask & supervisorBlockedWaitWakeTargetPipeWait(kernel)) !== 0) {
    statuses.add(kernel.abi_status_pipe_wait());
  }
  if ((mask & supervisorBlockedWaitWakeTargetEpollWait(kernel)) !== 0) {
    statuses.add(kernel.abi_status_epoll_wait());
  }
  if ((mask & supervisorBlockedWaitWakeTargetSocketRecv(kernel)) !== 0) {
    statuses.add(kernel.abi_status_socket_recv());
  }
  if ((mask & supervisorBlockedWaitWakeTargetSocketSend(kernel)) !== 0) {
    statuses.add(kernel.abi_status_socket_send());
  }
  return statuses;
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
