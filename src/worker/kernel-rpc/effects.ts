import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { readKernelRuntimeState } from "../../abi.js";
import type { ProcessHandle } from "../types.js";
import type { SyncCause } from "../contracts/sync.js";
import type {
  KernelRpcResumeEffect,
  KernelRpcResumeOutcome,
} from "./outcomes.js";

export interface KernelRpcResumeEffectPorts {
  blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  handleBlockedRetryResult(handle: ProcessHandle, worker: Worker, vmPtr: number, status: number): void;
  kickBlockedWorkers(handle: ProcessHandle): void;
  markRuntimeFsDirty(handle: ProcessHandle): void;
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
  syncForkSharedStateLocally(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options: { cause: SyncCause; includeMemfs?: boolean; trackHeldOfds?: boolean; ofdIds?: number[] },
  ): void;
  syncForkSharedStateViaKernelWorker(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options: {
      cause: SyncCause;
      includeMemfs?: boolean;
      trackHeldOfds?: boolean;
      deltaOfdIds?: boolean;
      memfsOnly?: boolean;
      preservePublishedRegularFileData?: boolean;
      sourceVmPtr?: number;
    },
  ): Promise<void>;
  syncPipeStateViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<bigint>;
  syncSocketStateViaKernelWorker(
    handle: ProcessHandle,
    cause: SyncCause,
  ): Promise<{ socketUsed: number; netBufUsed: bigint }>;
}

function kernelRpcEffectCause(name: string): SyncCause {
  return { kind: "syscall", nr: -1n, name };
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
    case "handle-blocked-retry-result":
      ports.handleBlockedRetryResult(handle, worker, vmPtr, effect.status);
      return;
    case "kick-blocked-workers":
      ports.kickBlockedWorkers(handle);
      return;
    case "mark-runtime-fs-dirty":
      ports.markRuntimeFsDirty(effect.handle);
      return;
    case "publish-runtime-kernel-state":
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      return;
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
    case "sync-fork-shared-state-local-after-fs":
      ports.syncForkSharedStateLocally(handle, effect.targets, {
        ...effect.options,
        cause: kernelRpcEffectCause("fs-local-fork-shared-sync"),
      });
      return;
    case "sync-shared-ofd-state-after-fs":
      await ports.syncForkSharedStateViaKernelWorker(handle, effect.targets, {
        cause: kernelRpcEffectCause("fs-shared-ofd-sync"),
        includeMemfs: false,
        trackHeldOfds: false,
        deltaOfdIds: true,
        sourceVmPtr: effect.sourceVmPtr,
      });
      return;
    case "publish-memfs-after-fs":
      await ports.syncForkSharedStateViaKernelWorker(handle, effect.targets, {
        cause: kernelRpcEffectCause("fs-memfs-publication"),
        includeMemfs: true,
        trackHeldOfds: false,
        memfsOnly: true,
        preservePublishedRegularFileData: effect.preservePublishedRegularFileData,
        sourceVmPtr: effect.sourceVmPtr,
      });
      await ports.syncPipeStateViaKernelWorker(handle, kernelRpcEffectCause("fs-memfs-pipe-refresh"));
      return;
    default: {
      const exhaustive: never = effect;
      throw new Error(`unhandled kernel-rpc effect: ${JSON.stringify(exhaustive)}`);
    }
  }
}
