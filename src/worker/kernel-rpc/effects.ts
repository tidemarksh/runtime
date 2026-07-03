import type { ProcessHandle, ThreadWorkerState } from "../types.js";
import {
  type KernelRpcResumeEffect,
  type KernelRpcResumeOutcome,
} from "./outcomes.js";

export interface KernelRpcResumeEffectPorts {
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  handleExecveSpawn(handle: ProcessHandle, worker: Worker, thread: ThreadWorkerState): Promise<void>;
  notifyHostEffectProgress(handle: ProcessHandle): void;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
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

async function applyKernelRpcResumeEffect(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  effect: KernelRpcResumeEffect,
  ports: KernelRpcResumeEffectPorts,
): Promise<void> {
  switch (effect.kind) {
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
    case "notify-host-effect-progress":
      ports.notifyHostEffectProgress(effect.handle ?? handle);
      return;
    default: {
      const exhaustive: never = effect;
      throw new Error(`unhandled kernel-rpc effect: ${JSON.stringify(exhaustive)}`);
    }
  }
}
