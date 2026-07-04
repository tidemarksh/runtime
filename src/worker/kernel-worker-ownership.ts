import type {
  KernelOwnershipHandle,
  KernelWorkerRequestFn,
} from "./types.js";

export function requireKernelWorkerOwnership(
  handle: KernelOwnershipHandle,
  operation: string,
): KernelWorkerRequestFn {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    throw new Error(`${operation} requires kernel-worker ownership`);
  }
  return handle.kernelWorkerRequest;
}
