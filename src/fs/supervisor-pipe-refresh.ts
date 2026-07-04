import type { KernelExports } from "../abi.js";

export type SupervisorFsPipeRefreshAction =
  | "none"
  | "request-sync";

function supervisorFsPipeRefreshNumericExport(
  kernel: KernelExports,
  name:
    | "supervisor_fs_pipe_refresh_action_none"
    | "supervisor_fs_pipe_refresh_action_request_sync"
    | "supervisor_fs_pipe_refresh_flag_kernel_worker_available",
): number {
  const read = kernel[name];
  if (typeof read !== "function") {
    throw new Error(`kernel supervisor FS pipe refresh ${name} export is missing`);
  }
  return read.call(kernel);
}

function supervisorFsPipeRefreshActionValue(
  kernel: KernelExports,
  action: SupervisorFsPipeRefreshAction,
): number {
  switch (action) {
    case "none":
      return supervisorFsPipeRefreshNumericExport(kernel, "supervisor_fs_pipe_refresh_action_none");
    case "request-sync":
      return supervisorFsPipeRefreshNumericExport(kernel, "supervisor_fs_pipe_refresh_action_request_sync");
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
    "none",
    "request-sync",
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
  const kernelWorkerAvailableFlag = supervisorFsPipeRefreshNumericExport(
    kernel,
    "supervisor_fs_pipe_refresh_flag_kernel_worker_available",
  );
  const flags = options.kernelWorkerAvailable === true ? kernelWorkerAvailableFlag : 0;
  const result = decide.call(kernel, vmPtr, flags);
  if (result < 0) {
    throw new Error(`kernel supervisor FS pipe refresh action failed: ${result}`);
  }
  return normalizeSupervisorFsPipeRefreshAction(kernel, result);
}
