import { expect, test, vi } from "vite-plus/test";
import {
  completeSupervisorFutexWait,
} from "../../../../src/worker/scheduler/futex.js";
import {
  makeHostBoundaryHandle,
  makeHostBoundaryKernel,
} from "./helpers.js";

test("host boundary submits supervisor futex timeout completion through kernel ABI", () => {
  const worker = { postMessage: vi.fn() } as unknown as Worker;
  const kernel = makeHostBoundaryKernel();
  const handle = makeHostBoundaryHandle(kernel, worker);

  const result = completeSupervisorFutexWait(handle, 0x1234, "timed-out");

  expect(result).toBe(0);
  expect(kernel.supervisor_complete_futex_wait).toHaveBeenCalledWith(0x1234, 3);
});
