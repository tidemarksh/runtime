import type {
  KernelOwnershipHandle,
  KernelWorkerRequestFn,
} from "./types.js";

const processKernelWorkerRequests = new WeakMap<KernelOwnershipHandle, KernelWorkerRequestFn | undefined>();
const PROCESS_KERNEL_WORKER_REQUEST = Symbol.for("tidemark.worker.kernelWorkerRequest");

type KernelWorkerRequestStateCarrier = KernelOwnershipHandle & {
  [PROCESS_KERNEL_WORKER_REQUEST]?: KernelWorkerRequestFn;
};

export function processKernelWorkerRequest(handle: KernelOwnershipHandle): KernelWorkerRequestFn | undefined {
  if (processKernelWorkerRequests.has(handle)) {
    return processKernelWorkerRequests.get(handle);
  }
  const carrier = handle as KernelWorkerRequestStateCarrier;
  if (Object.prototype.hasOwnProperty.call(carrier, PROCESS_KERNEL_WORKER_REQUEST)) {
    const request = carrier[PROCESS_KERNEL_WORKER_REQUEST];
    processKernelWorkerRequests.set(handle, request);
    return request;
  }
  return undefined;
}

export function setProcessKernelWorkerRequest(
  handle: KernelOwnershipHandle,
  request: KernelWorkerRequestFn | undefined,
): void {
  processKernelWorkerRequests.set(handle, request);
  Object.defineProperty(handle, PROCESS_KERNEL_WORKER_REQUEST, {
    configurable: true,
    enumerable: false,
    value: request,
    writable: true,
  });
}

export function processHasKernelWorkerRequest(handle: KernelOwnershipHandle): boolean {
  return processKernelWorkerRequest(handle) !== undefined;
}
