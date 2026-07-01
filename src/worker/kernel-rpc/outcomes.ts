import type { ProcessHandle } from "../types.js";

export type KernelRpcResumeEffect =
  | { kind: "block-worker"; status: number }
  | { kind: "enqueue-worker" }
  | { kind: "handle-execve-spawn"; status: number }
  | { kind: "notify-host-effect-progress"; handle?: ProcessHandle }
  | { kind: "request-pipe-state-sync"; handle?: ProcessHandle; wakeWaiters?: boolean }
  | { kind: "settle-visible-process-output" }
  | { kind: "sync-blocked-fork-shared-state"; sourceVmPtr?: number; status: number }
  | { kind: "sync-blocked-socket-state"; status: number };

type KernelRpcResumeOutcomeBase =
  | { kind: "completed"; status?: number }
  | { kind: "blocked"; status: number }
  | { kind: "queued-in-worker" }
  | { kind: "stale" }
  | { kind: "process-gone" };

export type KernelRpcResumeOutcome = KernelRpcResumeOutcomeBase & {
  effects?: KernelRpcResumeEffect[];
};

export function kernelRpcResumeEffectNeedsContinuation(effect: KernelRpcResumeEffect): boolean {
  return effect.kind === "request-pipe-state-sync"
    || effect.kind === "sync-blocked-fork-shared-state"
    || effect.kind === "sync-blocked-socket-state";
}

export function kernelRpcResumeEffectsNeedContinuation(
  effects: KernelRpcResumeEffect[] | undefined,
): boolean {
  return effects !== undefined && effects.some(kernelRpcResumeEffectNeedsContinuation);
}

export function completedKernelRpcOutcome(
  status?: number,
  effects?: KernelRpcResumeEffect[],
): KernelRpcResumeOutcome {
  return status === undefined ? { kind: "completed", effects } : { kind: "completed", status, effects };
}
