export type KernelRpcResumeOutcome =
  | { kind: "completed"; status?: number }
  | { kind: "blocked"; status: number }
  | { kind: "queued-in-worker" }
  | { kind: "stale" }
  | { kind: "process-gone" };

export function completedKernelRpcOutcome(
  status?: number,
): KernelRpcResumeOutcome {
  return status === undefined ? { kind: "completed" } : { kind: "completed", status };
}
