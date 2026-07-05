import { setProcessKernelWorkerRequest } from "../../../../src/worker/process-kernel-worker-request-state.js";
import type { KernelOwnershipHandle, KernelWorkerRequestFn } from "../../../../src/worker/types.js";

type KernelWorkerRequestFixture = KernelOwnershipHandle & {
  kernelWorkerRequest?: KernelWorkerRequestFn;
};

export function seedProcessKernelWorkerRequestFixture(handle: unknown): void {
  const fixture = handle as KernelWorkerRequestFixture;
  if (!Object.prototype.hasOwnProperty.call(fixture, "kernelWorkerRequest")) {
    return;
  }
  const request = fixture.kernelWorkerRequest;
  delete fixture.kernelWorkerRequest;
  setProcessKernelWorkerRequest(fixture, request);
}
