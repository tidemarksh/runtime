import { expect, test, vi } from "vite-plus/test";
import { createWorkerKernelRpcFsHelpers } from "../../../../src/worker/kernel-rpc/fs.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

test("kernel-rpc fs helper exposes a typed resume outcome without changing the legacy void call", async () => {
  const handle = {
    kernel: {
      thread_status: () => 0,
    },
  } as unknown as ProcessHandle;
  const worker = {} as Worker;
  const resumeFsViaKernelWorker = vi.fn(async () => {});
  const helpers = createWorkerKernelRpcFsHelpers({ resumeFsViaKernelWorker });
  const effects = { memfsMutation: 1, memfsFileData: 2, sharedOfdState: 4 };

  await expect(helpers.resumeFsViaKernelWorker(handle, worker, 0x1234, effects)).resolves.toBeUndefined();
  await expect(helpers.resumeFsViaKernelWorkerOutcome(handle, worker, 0x1234, effects)).resolves.toEqual({
    kind: "completed",
    status: 0,
  });
  expect(resumeFsViaKernelWorker).toHaveBeenCalledTimes(2);
});
