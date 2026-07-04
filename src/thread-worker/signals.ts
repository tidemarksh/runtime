import type { KernelExports, StatusCodes } from "../abi.js";
import { clearSyscallTrace } from "./debug.js";

export function checkPendingSignalsAtBoundary(
  exports: KernelExports,
  vmPtr: number,
  statusCodes: StatusCodes,
): { status: number; delivered: boolean } {
  const supervisorBoundaryCheck = exports.supervisor_signal_boundary_check;
  if (supervisorBoundaryCheck === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_signal_boundary_check");
  }
  clearSyscallTrace(exports);
  const delivered = supervisorBoundaryCheck.call(exports, vmPtr);
  if (delivered < 0) {
    throw new Error(`kernel supervisor signal boundary check failed: ${delivered}`);
  }
  const status = exports.thread_status(vmPtr);
  return {
    status,
    delivered: status === statusCodes.OK && delivered === 1,
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
  return supervisorDecision.call(exports, vmPtr) === 1;
}
