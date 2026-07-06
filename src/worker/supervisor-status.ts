import type { KernelExports } from "../abi.js";

type SupervisorProcessStatusFact =
  | "supervisor_process_status_is_lifecycle_handoff"
  | "supervisor_process_status_is_execve_spawn"
  | "supervisor_process_status_is_clone_spawn"
  | "supervisor_process_status_is_vfork_wait"
  | "supervisor_process_status_allows_pid_mismatch"
  | "supervisor_process_status_defers_kernel_state_adoption";

type SupervisorProcessStatusNumberConst =
  | "supervisor_process_status_route_action_none"
  | "supervisor_process_status_route_action_apply_status_fd_state"
  | "supervisor_process_status_route_action_adopt_thread_kernel_state"
  | "supervisor_process_status_route_action_apply_fs_memory_writes"
  | "supervisor_process_status_route_action_skip_fs_ring_writes"
  | "supervisor_process_status_route_action_defer_local_pipe_drain"
  | "supervisor_process_status_route_flag_status_state_belongs_to_handle"
  | "supervisor_process_status_route_flag_kernel_worker_available";

export interface SupervisorProcessStatusRouteActions {
  applyStatusFdState: boolean;
  adoptThreadKernelState: boolean;
  applyFsMemoryWrites: boolean;
  skipFsRingWrites: boolean;
  deferLocalPipeDrain: boolean;
}

function supervisorProcessStatusFact(
  kernel: KernelExports,
  status: number,
  exportName: SupervisorProcessStatusFact,
): boolean {
  const readStatus = kernel[exportName];
  if (readStatus === undefined) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  const result = readStatus.call(kernel, status);
  if (result < 0) {
    throw new Error(`kernel supervisor process status fact ${exportName} failed: ${result}`);
  }
  return result !== 0;
}

function supervisorProcessStatusNumberConst(
  kernel: KernelExports,
  exportName: SupervisorProcessStatusNumberConst,
): number {
  const readConst = kernel[exportName];
  if (readConst === undefined) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  return readConst.call(kernel);
}

function supervisorProcessStatusRouteFlags(
  kernel: KernelExports,
  options: {
    statusStateBelongsToHandle: boolean;
    kernelWorkerAvailable: boolean;
  },
): number {
  let flags = 0;
  if (options.statusStateBelongsToHandle) {
    flags |= supervisorProcessStatusNumberConst(
      kernel,
      "supervisor_process_status_route_flag_status_state_belongs_to_handle",
    );
  }
  if (options.kernelWorkerAvailable) {
    flags |= supervisorProcessStatusNumberConst(
      kernel,
      "supervisor_process_status_route_flag_kernel_worker_available",
    );
  }
  return flags;
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

export function supervisorProcessStatusIsCloneSpawn(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorProcessStatusFact(kernel, status, "supervisor_process_status_is_clone_spawn");
}

export function supervisorProcessStatusIsVforkWait(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorProcessStatusFact(kernel, status, "supervisor_process_status_is_vfork_wait");
}

export function supervisorProcessStatusDefersKernelStateAdoption(
  kernel: KernelExports,
  status: number,
): boolean {
  return supervisorProcessStatusFact(kernel, status, "supervisor_process_status_defers_kernel_state_adoption");
}

export function supervisorProcessStatusRouteActions(
  kernel: KernelExports,
  status: number,
  options: {
    statusStateBelongsToHandle: boolean;
    kernelWorkerAvailable: boolean;
  },
): SupervisorProcessStatusRouteActions {
  const readAction = kernel.supervisor_process_status_route_action;
  if (readAction === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_process_status_route_action");
  }
  const result = readAction.call(kernel, status, supervisorProcessStatusRouteFlags(kernel, options));
  if (result < 0) {
    throw new Error(`kernel supervisor process status route action failed: ${result}`);
  }

  const applyStatusFdState = supervisorProcessStatusNumberConst(
    kernel,
    "supervisor_process_status_route_action_apply_status_fd_state",
  );
  const adoptThreadKernelState = supervisorProcessStatusNumberConst(
    kernel,
    "supervisor_process_status_route_action_adopt_thread_kernel_state",
  );
  const applyFsMemoryWrites = supervisorProcessStatusNumberConst(
    kernel,
    "supervisor_process_status_route_action_apply_fs_memory_writes",
  );
  const skipFsRingWrites = supervisorProcessStatusNumberConst(
    kernel,
    "supervisor_process_status_route_action_skip_fs_ring_writes",
  );
  const deferLocalPipeDrain = supervisorProcessStatusNumberConst(
    kernel,
    "supervisor_process_status_route_action_defer_local_pipe_drain",
  );
  const known = supervisorProcessStatusNumberConst(kernel, "supervisor_process_status_route_action_none")
    | applyStatusFdState
    | adoptThreadKernelState
    | applyFsMemoryWrites
    | skipFsRingWrites
    | deferLocalPipeDrain;
  if ((result & ~known) !== 0) {
    throw new Error(`kernel supervisor process status route action is unknown: ${result}`);
  }
  return {
    applyStatusFdState: (result & applyStatusFdState) !== 0,
    adoptThreadKernelState: (result & adoptThreadKernelState) !== 0,
    applyFsMemoryWrites: (result & applyFsMemoryWrites) !== 0,
    skipFsRingWrites: (result & skipFsRingWrites) !== 0,
    deferLocalPipeDrain: (result & deferLocalPipeDrain) !== 0,
  };
}
