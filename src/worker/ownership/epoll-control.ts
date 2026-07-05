import type { KernelOwnershipHandle } from "../types.js";
import { processHasKernelWorkerOwnership } from "../kernel-worker-ownership.js";

export function kernelWorkerOwnsEpollState(handle: KernelOwnershipHandle): boolean {
  return processHasKernelWorkerOwnership(handle);
}
