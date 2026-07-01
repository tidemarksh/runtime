import { expect, test, vi } from "vite-plus/test";
import {
  completeSupervisorFutexWait,
} from "../../../../src/worker/scheduler/futex.js";
import {
  armSupervisorHostTimer,
  drainSupervisorArmHostTimerHostOp,
} from "../../../../src/worker/supervisor-host-op.js";
import {
  makeHostBoundaryHandle,
  makeHostBoundaryKernel,
} from "./helpers.js";

function writeArmHostTimerHostOpRecord(
  memory: WebAssembly.Memory,
  outPtr: number,
  vmPtr: number,
  delayMs: bigint,
): void {
  const view = new DataView(memory.buffer, outPtr, 64);
  view.setUint32(0, 0x4f48_5354, true);
  view.setUint32(4, 1, true);
  view.setUint32(8, 64, true);
  view.setUint32(12, 12, true);
  view.setBigUint64(16, 7n, true);
  view.setUint32(24, vmPtr, true);
  view.setUint32(28, 0, true);
  view.setUint32(32, 2, true);
  view.setUint32(36, 0x2000, true);
  view.setUint32(40, 22, true);
  view.setUint32(44, 0, true);
  view.setBigUint64(48, delayMs, true);
  view.setBigUint64(56, 0n, true);
}

test("host boundary submits supervisor futex timeout completion through kernel ABI", () => {
  const worker = { postMessage: vi.fn() } as unknown as Worker;
  const kernel = makeHostBoundaryKernel();
  const handle = makeHostBoundaryHandle(kernel, worker);

  const result = completeSupervisorFutexWait(handle, 0x1234, "timed-out");

  expect(result).toBe(0);
  expect(kernel.supervisor_complete_futex_wait).toHaveBeenCalledWith(0x1234, 3);
});

test("host boundary executes supervisor arm timer HostOp through completion ABI", async () => {
  vi.useFakeTimers();
  const worker = { postMessage: vi.fn() } as unknown as Worker;
  const kernel = makeHostBoundaryKernel();
  const handle = makeHostBoundaryHandle(kernel, worker);
  const memory = new WebAssembly.Memory({ initial: 6, maximum: 6, shared: true });
  kernel.memory = memory;
  handle.memory = memory;
  const completionViews: DataView[] = [];
  kernel.supervisor_abi_version = () => 1;
  kernel.supervisor_host_op_record_bytes = () => 64;
  kernel.supervisor_host_op_kind_arm_host_timer = () => 12;
  kernel.supervisor_host_completion_outcome_success = () => 1;
  kernel.supervisor_host_completion_outcome_failure = () => 2;
  kernel.supervisor_next_host_op = vi.fn((outPtr: number, outLen: number) => {
    expect(outLen).toBe(64);
    writeArmHostTimerHostOpRecord(kernel.memory!, outPtr, 0x1234, 5n);
    return 64;
  });
  kernel.supervisor_complete_host_op = vi.fn((completionPtr: number, completionLen: number) => {
    completionViews.push(new DataView(kernel.memory!.buffer, completionPtr, completionLen));
    return 0;
  });
  const runtimeEvents: string[] = [];
  const workerEvents: string[] = [];
  const hostTimer = drainSupervisorArmHostTimerHostOp(handle, 0x1234);

  expect(hostTimer).not.toBeNull();
  expect(hostTimer).not.toBeUndefined();
  armSupervisorHostTimer(handle, hostTimer!, {
    appendRuntimeLifecycleEvent: (_runtimeId, event) => runtimeEvents.push(event),
    appendWorkerDebugLog: (_handle, event) => workerEvents.push(event),
    failProcess: (_handle, error) => {
      throw error;
    },
    pumpProcess: vi.fn(),
  });

  await vi.advanceTimersByTimeAsync(5);

  expect(kernel.supervisor_complete_host_op).toHaveBeenCalledWith(expect.any(Number), 64);
  expect(completionViews).toHaveLength(1);
  expect(completionViews[0].getUint32(0, true)).toBe(0x4348_5354);
  expect(completionViews[0].getUint32(12, true)).toBe(12);
  expect(completionViews[0].getBigUint64(16, true)).toBe(7n);
  expect(completionViews[0].getUint32(24, true)).toBe(0x1234);
  expect(completionViews[0].getUint32(36, true)).toBe(1);
  expect(runtimeEvents[0]).toContain("supervisor-host-timer-arm");
  expect(workerEvents[0]).toContain("supervisor-host-timer-fired");
});
