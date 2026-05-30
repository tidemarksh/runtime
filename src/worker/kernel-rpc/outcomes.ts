import type { ProcessHandle } from "../types.js";

export type KernelRpcResumeEffect =
  | { kind: "block-worker"; status: number }
  | { kind: "enqueue-worker" }
  | { kind: "handle-blocked-retry-result"; status: number }
  | { kind: "kick-blocked-workers" }
  | { kind: "mark-runtime-fs-dirty"; handle: ProcessHandle }
  | { kind: "publish-runtime-kernel-state" }
  | { kind: "request-pipe-state-sync"; wakeWaiters?: boolean }
  | { kind: "settle-visible-process-output" }
  | {
      kind: "sync-fork-shared-state-local-after-fs";
      targets: ProcessHandle[];
      options?: { includeMemfs?: boolean; trackHeldOfds?: boolean; ofdIds?: number[] };
    }
  | {
      kind: "sync-shared-ofd-state-after-fs";
      targets: ProcessHandle[];
      sourceVmPtr: number;
    }
  | {
      kind: "publish-memfs-after-fs";
      targets: ProcessHandle[];
      preservePublishedRegularFileData: boolean;
      sourceVmPtr: number;
    };

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
