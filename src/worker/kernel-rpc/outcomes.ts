import type { ProcessHandle } from "../types.js";

export type KernelRpcResumeEffect =
  | { kind: "enqueue-worker" }
  | { kind: "handle-execve-spawn"; status: number }
  | { kind: "notify-host-effect-progress"; handle?: ProcessHandle };

type KernelRpcResumeOutcomeBase =
  | { kind: "completed"; status?: number }
  | { kind: "blocked"; status: number }
  | { kind: "queued-in-worker" }
  | { kind: "stale" }
  | { kind: "process-gone" };

export type KernelRpcResumeOutcome = KernelRpcResumeOutcomeBase & {
  effects?: KernelRpcResumeEffect[];
};

export function completedKernelRpcOutcome(
  status?: number,
  effects?: KernelRpcResumeEffect[],
): KernelRpcResumeOutcome {
  return status === undefined ? { kind: "completed", effects } : { kind: "completed", status, effects };
}
