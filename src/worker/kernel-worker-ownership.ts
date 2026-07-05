import type {
  KernelOwnershipHandle,
  KernelWorkerRequestFn,
} from "./types.js";
import { processKernelWorkerRequest } from "./process-kernel-worker-request-state.js";

export function requireKernelWorkerOwnership(
  handle: KernelOwnershipHandle,
  operation: string,
): KernelWorkerRequestFn {
  const request = processKernelWorkerRequest(handle);
  if (!request || handle.pid <= 0) {
    throw new Error(`${operation} requires kernel-worker ownership`);
  }
  return request;
}

export function processHasKernelWorkerOwnership(handle: KernelOwnershipHandle): boolean {
  return processKernelWorkerRequest(handle) !== undefined && handle.pid > 0;
}
