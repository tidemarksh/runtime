export type KernelRpcResumeOutcome =
  | { kind: "completed"; status?: number; workerQueued?: true }
  | { kind: "blocked"; status: number }
  | { kind: "queued-in-worker" }
  | { kind: "stale" }
  | { kind: "process-gone" };

export function completedKernelRpcOutcome(
  status?: number,
  options: { workerQueued?: boolean } = {},
): KernelRpcResumeOutcome {
  if (status === undefined) {
    return options.workerQueued === true
      ? { kind: "completed", workerQueued: true }
      : { kind: "completed" };
  }
  return options.workerQueued === true
    ? { kind: "completed", status, workerQueued: true }
    : { kind: "completed", status };
}
