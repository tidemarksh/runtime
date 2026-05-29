import type { KernelOwnershipHandle } from "../types.js";

export function kernelWorkerOwnsPipeState(handle: KernelOwnershipHandle): boolean {
  return !!handle.kernelWorkerRequest && handle.pid > 0;
}
