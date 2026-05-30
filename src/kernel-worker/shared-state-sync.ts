import { createKernelWorkerForkSharedStateSync } from "./shared-state-sync/fork-shared.js";
import { createKernelWorkerLiveStateSync } from "./shared-state-sync/live-state.js";
import type { KernelWorkerSharedStateSyncDeps } from "./shared-state-sync/types.js";

export const KERNEL_WORKER_SHARED_STATE_SYNC_OWNERSHIP = "transitional-runtime-owner" as const;

export function createKernelWorkerSharedStateSync(deps: KernelWorkerSharedStateSyncDeps) {
  return {
    ...createKernelWorkerForkSharedStateSync(deps),
    ...createKernelWorkerLiveStateSync(deps),
  };
}
