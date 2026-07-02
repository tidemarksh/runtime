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
