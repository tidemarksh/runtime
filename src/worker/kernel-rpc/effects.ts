import { readKernelRuntimeState } from "../../abi.js";
import type { ProcessHandle, ThreadWorkerState } from "../types.js";
import type { SyncCause } from "../contracts/sync.js";
import type {
  KernelRpcResumeEffect,
  KernelRpcResumeOutcome,
} from "./outcomes.js";

export interface KernelRpcResumeEffectPorts {
  blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  handleExecveSpawn(handle: ProcessHandle, worker: Worker, thread: ThreadWorkerState): Promise<void>;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  settleVisibleProcessOutput(
    handle: ProcessHandle,
    requestPipeStateSync: (handle: ProcessHandle, wakeWaiters?: boolean) => Promise<void>,
  ): Promise<unknown>;
  syncBlockedForkSharedStateViaKernelWorker(
    handle: ProcessHandle,
    sourceVmPtr: number | undefined,
    cause: SyncCause,
  ): Promise<void>;
  syncSocketStateViaKernelWorker(
    handle: ProcessHandle,
    cause: SyncCause,
  ): Promise<{ socketUsed: number; netBufUsed: bigint }>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
}

export interface KernelRpcResumeContinuationPorts {
  threadWorkerStates: WeakMap<Worker, unknown>;
  resumeBlockedSyscallLocally(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    blockedStatus: number,
    timeoutExpired?: boolean,
  ): Promise<void>;
}

function kernelRpcBlockedCause(status: number): SyncCause {
  return { kind: "blocked-rpc", status };
}

export async function applyKernelRpcResumeEffects(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  outcome: KernelRpcResumeOutcome,
  ports: KernelRpcResumeEffectPorts,
): Promise<KernelRpcResumeOutcome> {
  for (const effect of outcome.effects ?? []) {
    await applyKernelRpcResumeEffect(handle, worker, vmPtr, effect, ports);
  }
  const { effects: _effects, ...withoutEffects } = outcome;
  return withoutEffects;
}

export function kernelRpcOutcomeNeedsResumeContinuation(outcome: KernelRpcResumeOutcome): boolean {
  return outcome.kind === "blocked"
    && (outcome.effects ?? []).some(kernelRpcEffectNeedsResumeContinuation);
}

export async function continueBlockedRpcResumeAfterEffects(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  outcome: KernelRpcResumeOutcome,
  appliedOutcome: KernelRpcResumeOutcome,
  ports: KernelRpcResumeContinuationPorts,
): Promise<KernelRpcResumeOutcome> {
  if (!kernelRpcOutcomeNeedsResumeContinuation(outcome)) {
    return appliedOutcome;
  }
  const blocked = handle.blockedWorkers.get(worker);
  if (
    !blocked
    || blocked.vmPtr !== vmPtr
    || !handle.workers.has(worker)
    || !ports.threadWorkerStates.has(worker)
  ) {
    return appliedOutcome;
  }
  await ports.resumeBlockedSyscallLocally(
    handle,
    worker,
    vmPtr,
    blocked.status,
    blocked.timeoutExpired,
  );
  if (!handle.workers.has(worker) || !ports.threadWorkerStates.has(worker)) {
    return { kind: "process-gone" };
  }
  if (!handle.blockedWorkers.has(worker)) {
    return { kind: "queued-in-worker" };
  }
  const status = handle.kernel.thread_status?.(vmPtr);
  return status === undefined ? { kind: "stale" } : { kind: "blocked", status };
}

function kernelRpcEffectNeedsResumeContinuation(effect: KernelRpcResumeEffect): boolean {
  return effect.kind === "request-pipe-state-sync"
    || effect.kind === "sync-blocked-fork-shared-state"
    || effect.kind === "sync-blocked-socket-state";
}

async function applyKernelRpcResumeEffect(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  effect: KernelRpcResumeEffect,
  ports: KernelRpcResumeEffectPorts,
): Promise<void> {
  switch (effect.kind) {
    case "block-worker":
      ports.blockWorker(handle, worker, effect.status, vmPtr);
      return;
    case "enqueue-worker":
      ports.enqueueWorker(handle, worker);
      return;
    case "handle-execve-spawn": {
      const thread = ports.threadWorkerStates.get(worker);
      if (!thread || thread.vmPtr !== vmPtr) {
        return;
      }
      thread.ownerAsyncStatus = effect.status;
      try {
        await ports.handleExecveSpawn(handle, worker, thread);
      } finally {
        if (handle.workers.has(worker) && ports.threadWorkerStates.get(worker) === thread) {
          thread.ownerAsyncStatus = null;
        }
      }
      return;
    }
    case "request-pipe-state-sync":
      await ports.requestPipeStateSync(effect.handle ?? handle, effect.wakeWaiters);
      return;
    case "settle-visible-process-output":
      await ports.settleVisibleProcessOutput(handle, ports.requestPipeStateSync);
      return;
    case "sync-blocked-fork-shared-state":
      await ports.syncBlockedForkSharedStateViaKernelWorker(
        handle,
        effect.sourceVmPtr,
        kernelRpcBlockedCause(effect.status),
      );
      return;
    case "sync-blocked-socket-state":
      if (handle.networkConnections.size > 0) {
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        return;
      }
      await ports.syncSocketStateViaKernelWorker(handle, kernelRpcBlockedCause(effect.status));
      return;
    default: {
      const exhaustive: never = effect;
      throw new Error(`unhandled kernel-rpc effect: ${JSON.stringify(exhaustive)}`);
    }
  }
}
