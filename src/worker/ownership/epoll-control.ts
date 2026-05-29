import type { KernelOwnershipHandle } from "../types.js";

export function kernelWorkerOwnsEpollState(handle: KernelOwnershipHandle): boolean {
  return !!handle.kernelWorkerRequest && handle.pid > 0;
}
