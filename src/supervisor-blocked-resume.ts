import type { KernelExports } from "./abi.js";

const SUPERVISOR_RESUME_FLAG_TIMEOUT_EXPIRED = 1;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_DEFAULT = 0;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_IMMEDIATE = 1;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_STANDARD_DEFER = 2;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_ACTIVE_CHILD = 3;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PASSIVE_WAIT = 4;
export const SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PROCESS_GATE = 5;

export type SupervisorBlockedWaitRetryReason =
  | typeof SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_DEFAULT
  | typeof SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_IMMEDIATE
  | typeof SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_STANDARD_DEFER
  | typeof SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_ACTIVE_CHILD
  | typeof SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PASSIVE_WAIT
  | typeof SUPERVISOR_BLOCKED_WAIT_RETRY_REASON_PROCESS_GATE;

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
