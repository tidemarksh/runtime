import { expect, test, vi } from "vite-plus/test";
import { createWorkerKernelRpcBlockedRpcHelpers } from "../../../../src/worker/kernel-rpc/blocked-rpc.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

test("kernel-rpc blocked-rpc helper exposes a typed resume outcome without changing the legacy void call", async () => {
  const handle = {
    kernel: {
      thread_status: () => 21,
    },
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  const resumeBlockedSyscallLocally = vi.fn(async () => {});
  const helpers = createWorkerKernelRpcBlockedRpcHelpers({ resumeBlockedSyscallLocally });

  await expect(helpers.resumeBlockedSyscallLocally(handle, worker, 0x1234, 21)).resolves.toBeUndefined();
  await expect(helpers.resumeBlockedSyscallOutcome(handle, worker, 0x1234, 21)).resolves.toEqual({
    kind: "completed",
    status: 21,
  });
  expect(resumeBlockedSyscallLocally).toHaveBeenCalledTimes(2);
});
