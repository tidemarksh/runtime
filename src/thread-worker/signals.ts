import type { KernelExports, StatusCodes } from "../abi.js";
import { clearSyscallTrace } from "./debug.js";

export function checkPendingSignalsAtBoundary(
  exports: KernelExports,
  vmPtr: number,
  statusCodes: StatusCodes,
): { status: number; delivered: boolean } {
  const beforePc = exports.thread_pc(vmPtr);
  const beforeSp = exports.thread_sp(vmPtr);
  clearSyscallTrace(exports);
  exports.thread_step(vmPtr, 0);
  const afterPc = exports.thread_pc(vmPtr);
  const afterSp = exports.thread_sp(vmPtr);
  const status = exports.thread_status(vmPtr);
  return {
    status,
    delivered: status === statusCodes.OK
      && (afterPc !== beforePc || afterSp !== beforeSp),
  };
}

export function hasPendingSignalForBlockedWait(
  exports: KernelExports,
  vmPtr: number,
): boolean {
  const supervisorDecision = exports.supervisor_signal_pending_for_blocked_wait;
  if (supervisorDecision === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_signal_pending_for_blocked_wait");
  }
  return supervisorDecision(vmPtr) === 1;
}
