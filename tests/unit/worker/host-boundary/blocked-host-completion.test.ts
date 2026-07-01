import { expect, test, vi } from "vite-plus/test";
import {
  clearStaleFutexRecordIfResolved,
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

test("host boundary clears stale resolved futex records via supervisor completion", () => {
  const worker = { postMessage: vi.fn() } as unknown as Worker;
  const kernel = makeHostBoundaryKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.thread_record_blocked_addr = () => 0xbeefn;
  kernel.thread_futex_wait_addr = () => 0n;
  const handle = makeHostBoundaryHandle(kernel, worker);

  expect(clearStaleFutexRecordIfResolved(handle, 0x1234)).toBe(true);

  expect(kernel.supervisor_complete_futex_wait).toHaveBeenCalledWith(0x1234, 1);
});
