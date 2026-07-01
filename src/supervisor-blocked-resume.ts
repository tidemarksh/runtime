import type { KernelExports } from "./abi.js";

const SUPERVISOR_RESUME_FLAG_TIMEOUT_EXPIRED = 1;

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
