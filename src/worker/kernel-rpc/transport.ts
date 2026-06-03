import type { KernelWorkerOutbound } from "../../messages.js";

export function expectKernelWorkerResponse<
  TExpected extends KernelWorkerOutbound["type"],
>(
  response: KernelWorkerOutbound,
  expected: TExpected,
  requestType: string,
): Extract<KernelWorkerOutbound, { type: TExpected }> {
  if (response.type !== expected) {
    throw new Error(`unexpected kernel-worker response for ${requestType}: ${response.type}`);
  }
  return response as Extract<KernelWorkerOutbound, { type: TExpected }>;
}
