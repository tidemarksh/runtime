import type { KernelExports } from "../abi.js";

type SupervisorProcessStatusFact =
  | "supervisor_process_status_is_lifecycle_handoff"
  | "supervisor_process_status_is_execve_spawn"
  | "supervisor_process_status_allows_pid_mismatch"
  | "supervisor_process_status_defers_kernel_state_adoption";

function supervisorProcessStatusFact(
  kernel: KernelExports,
  status: number,
  exportName: SupervisorProcessStatusFact,
): boolean {
  const readStatus = kernel[exportName];
  if (readStatus === undefined) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  const result = readStatus(status);
  if (result < 0) {
    throw new Error(`kernel supervisor process status fact ${exportName} failed: ${result}`);
  }
  return result !== 0;
}

export function supervisorProcessStatusIsLifecycleHandoff(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorProcessStatusFact(kernel, status, "supervisor_process_status_is_lifecycle_handoff");
}

export function supervisorProcessStatusAllowsPidMismatch(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorProcessStatusFact(kernel, status, "supervisor_process_status_allows_pid_mismatch");
}

export function supervisorProcessStatusIsExecveSpawn(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorProcessStatusFact(kernel, status, "supervisor_process_status_is_execve_spawn");
}

export function supervisorProcessStatusDefersKernelStateAdoption(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorProcessStatusFact(kernel, status, "supervisor_process_status_defers_kernel_state_adoption");
}
