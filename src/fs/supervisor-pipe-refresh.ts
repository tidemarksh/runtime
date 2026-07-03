import type { KernelExports } from "../abi.js";

export const SUPERVISOR_FS_PIPE_REFRESH_ACTION_NONE = 0;
export const SUPERVISOR_FS_PIPE_REFRESH_ACTION_REQUEST_SYNC = 1;

const SUPERVISOR_FS_PIPE_REFRESH_FLAG_KERNEL_WORKER_AVAILABLE = 1;

export type SupervisorFsPipeRefreshAction =
  | typeof SUPERVISOR_FS_PIPE_REFRESH_ACTION_NONE
  | typeof SUPERVISOR_FS_PIPE_REFRESH_ACTION_REQUEST_SYNC;

function supervisorFsPipeRefreshActionValue(
  kernel: KernelExports,
  action: SupervisorFsPipeRefreshAction,
): number {
  switch (action) {
    case SUPERVISOR_FS_PIPE_REFRESH_ACTION_NONE:
      return kernel.supervisor_fs_pipe_refresh_action_none?.() ?? action;
    case SUPERVISOR_FS_PIPE_REFRESH_ACTION_REQUEST_SYNC:
      return kernel.supervisor_fs_pipe_refresh_action_request_sync?.() ?? action;
    default: {
      const exhaustive: never = action;
      return exhaustive;
    }
  }
}

function normalizeSupervisorFsPipeRefreshAction(
  kernel: KernelExports,
  result: number,
): SupervisorFsPipeRefreshAction {
  const actions: SupervisorFsPipeRefreshAction[] = [
    SUPERVISOR_FS_PIPE_REFRESH_ACTION_NONE,
    SUPERVISOR_FS_PIPE_REFRESH_ACTION_REQUEST_SYNC,
  ];
  for (const action of actions) {
    if (result === supervisorFsPipeRefreshActionValue(kernel, action)) {
      return action;
    }
  }
  throw new Error(`kernel supervisor FS pipe refresh action is unknown: ${result}`);
}

export function supervisorFsPipeRefreshBeforeResumeAction(
  kernel: KernelExports,
  vmPtr: number,
  options: { kernelWorkerAvailable?: boolean } = {},
): SupervisorFsPipeRefreshAction {
  const decide = kernel.supervisor_fs_pipe_refresh_before_resume_action;
  if (typeof decide !== "function") {
    throw new Error("kernel supervisor FS pipe refresh action export is missing");
  }
  const kernelWorkerAvailableFlag =
    kernel.supervisor_fs_pipe_refresh_flag_kernel_worker_available?.()
    ?? SUPERVISOR_FS_PIPE_REFRESH_FLAG_KERNEL_WORKER_AVAILABLE;
  const flags = options.kernelWorkerAvailable === true ? kernelWorkerAvailableFlag : 0;
  const result = decide(vmPtr, flags);
  if (result < 0) {
    throw new Error(`kernel supervisor FS pipe refresh action failed: ${result}`);
  }
  return normalizeSupervisorFsPipeRefreshAction(kernel, result);
}
