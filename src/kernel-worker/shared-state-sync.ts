import { createKernelWorkerForkSharedStateSync } from "./shared-state-sync/fork-shared.js";
import { createKernelWorkerLiveStateSync } from "./shared-state-sync/live-state.js";
import type { KernelWorkerSharedStateSyncDeps } from "./shared-state-sync/types.js";

export function createKernelWorkerSharedStateSync(deps: KernelWorkerSharedStateSyncDeps) {
  return {
    ...createKernelWorkerForkSharedStateSync(deps),
    ...createKernelWorkerLiveStateSync(deps),
  };
}
