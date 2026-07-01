import type { ProcessHandle } from "../types.js";

const BLOCKED_ADDR_NOT_FOUND = 0xffff_ffff_ffff_ffffn;

type SupervisorFutexWaitCompletion =
  | "woken"
  | "not-equal"
  | "timed-out"
  | "invalid-address"
  | "signal-check";

type SupervisorFutexCompletionCodeExport =
  | "supervisor_futex_wait_completion_woken"
  | "supervisor_futex_wait_completion_not_equal"
  | "supervisor_futex_wait_completion_timed_out"
  | "supervisor_futex_wait_completion_invalid_address"
  | "supervisor_futex_wait_completion_signal_check";

const SUPERVISOR_FUTEX_COMPLETION_EXPORTS: Record<
  SupervisorFutexWaitCompletion,
  SupervisorFutexCompletionCodeExport
> = {
  "woken": "supervisor_futex_wait_completion_woken",
  "not-equal": "supervisor_futex_wait_completion_not_equal",
  "timed-out": "supervisor_futex_wait_completion_timed_out",
  "invalid-address": "supervisor_futex_wait_completion_invalid_address",
  "signal-check": "supervisor_futex_wait_completion_signal_check",
};

function threadRecordBlockedAddr(
  handle: ProcessHandle,
  vmPtr: number,
): bigint {
  return handle.kernel.thread_record_blocked_addr?.(vmPtr) ?? 0n;
}

function isKnownThreadRecordBlock(blockedAddr: bigint): boolean {
  return blockedAddr !== 0n && blockedAddr !== BLOCKED_ADDR_NOT_FOUND;
}

export function threadHasCanonicalFutexBlock(
  handle: ProcessHandle,
  vmPtr: number,
): boolean {
  const blockedAddr = threadRecordBlockedAddr(handle, vmPtr);
  if (!isKnownThreadRecordBlock(blockedAddr)) {
    return false;
  }
  if (handle.kernel.thread_status(vmPtr) === handle.kernel.abi_status_futex_wait()) {
    return true;
  }
  return (handle.kernel.thread_futex_wait_addr?.(vmPtr) ?? 0n) !== 0n;
}

function requireSupervisorCompletionCode(
  handle: ProcessHandle,
  completion: SupervisorFutexWaitCompletion,
): number {
  const completionExport = SUPERVISOR_FUTEX_COMPLETION_EXPORTS[completion];
  const readCode = handle.kernel[completionExport];
  if (readCode === undefined) {
    throw new Error(`kernel supervisor ABI missing ${completionExport}`);
  }
  return readCode();
}

export function completeSupervisorFutexWait(
  handle: ProcessHandle,
  vmPtr: number,
  completion: SupervisorFutexWaitCompletion,
): number {
  const complete = handle.kernel.supervisor_complete_futex_wait;
  if (complete === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_complete_futex_wait");
  }
  return complete(vmPtr, requireSupervisorCompletionCode(handle, completion));
}
