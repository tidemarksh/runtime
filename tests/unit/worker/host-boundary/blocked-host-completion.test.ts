import { expect, test, vi } from "vite-plus/test";
import {
  handleFutexWait,
} from "../../../../src/thread-worker/blocking.js";
import {
  armSupervisorHostTimer,
  drainSupervisorArmHostTimerHostOp,
} from "../../../../src/worker/supervisor-host-op.js";
import {
  makeHostBoundaryHandle,
  makeHostBoundaryKernel,
} from "./helpers.js";
import { writeSupervisorArmHostTimerHostOpRecord } from "../support/supervisor-abi.js";

test("host boundary submits supervisor futex timeout completion through kernel ABI", () => {
  const waitSpy = vi.spyOn(Atomics, "wait");
  const worker = { postMessage: vi.fn() } as unknown as Worker;
  const kernel = makeHostBoundaryKernel();
  const handle = makeHostBoundaryHandle(kernel, worker);
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const waitAddr = 0x100;
  const timeoutPtr = 0x200;
  const expected = 7;
  let status = kernel.abi_status_futex_wait();
  let blockedAddr = BigInt(waitAddr);

  new Int32Array(memory.buffer)[waitAddr >>> 2] = expected;
  const view = new DataView(memory.buffer);
  view.setBigUint64(timeoutPtr, 0n, true);
  view.setBigUint64(timeoutPtr + 8, 0n, true);
  waitSpy.mockImplementation((view, index, value) => {
    expect(view.buffer).toBe(memory.buffer);
    expect(index).toBe(waitAddr >>> 2);
    expect(value).toBe(expected);
    return "timed-out";
  });

  kernel.memory = memory;
  handle.memory = memory;
  kernel.thread_status = () => status;
  kernel.thread_record_blocked_addr = () => blockedAddr;
  kernel.thread_futex_wait_addr = () => BigInt(waitAddr);
  kernel.thread_futex_wait_timeout = () => BigInt(timeoutPtr);
  kernel.thread_futex_wait_expected = () => expected;
  kernel.supervisor_signal_pending_for_blocked_wait = () => 0;
  kernel.supervisor_complete_futex_wait = vi.fn((_vmPtr: number, _completion: number) => {
    status = kernel.abi_status_ok();
    blockedAddr = 0n;
    return 0;
  });

  const handled = handleFutexWait({
    exports: kernel,
    statusCodes: { FUTEX_WAIT: kernel.abi_status_futex_wait() } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    runtimeBridge: null,
    threadStepBudget: 1000,
    eintrErrno: 4,
    futexWaitSliceMs: 0,
    postStatus: vi.fn(),
    reattachPublishedKernelRuntimeState: () => {},
    step: () => {},
  }, kernel.abi_status_futex_wait());

  expect(handled).toBe(false);
  expect(kernel.supervisor_complete_futex_wait).toHaveBeenCalledWith(0x1234, 3);
  waitSpy.mockRestore();
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
    return writeSupervisorArmHostTimerHostOpRecord(kernel.memory!, outPtr, {
      vmPtr: 0x1234,
      delayMs: 5n,
      seq: 7n,
      tid: 2,
      processPtr: 0x2000,
      status: 22,
    });
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
