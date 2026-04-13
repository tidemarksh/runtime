import { expect, test, vi } from "vite-plus/test";
import { checkPendingSignalsAtBoundary } from "../../../src/thread-worker/signals.js";

test("checkPendingSignalsAtBoundary reports delivered when step(0) advances the thread to OK", () => {
  const clearSyscallTrace = vi.fn();
  const clearSignalTrace = vi.fn();
  const clearCpuTrace = vi.fn();
  const threadStep = vi.fn(() => {
    pc = 0x4010n;
    sp = 0x7ff0n;
    status = 0;
  });

  let pc = 0x4000n;
  let sp = 0x8000n;
  let status = 22;

  const boundary = checkPendingSignalsAtBoundary({
    debug_clear_syscall_trace: clearSyscallTrace,
    debug_clear_signal_trace: clearSignalTrace,
    debug_clear_cpu_trace: clearCpuTrace,
    thread_pc: () => pc,
    thread_sp: () => sp,
    thread_step: threadStep,
    thread_status: () => status,
  } as never, 0x1234, { OK: 0 } as never);

  expect(clearSyscallTrace).toHaveBeenCalledTimes(1);
  expect(clearSignalTrace).toHaveBeenCalledTimes(1);
  expect(clearCpuTrace).toHaveBeenCalledTimes(1);
  expect(threadStep).toHaveBeenCalledWith(0x1234, 0);
  expect(boundary).toEqual({ status: 0, delivered: true });
});

test("checkPendingSignalsAtBoundary keeps delivered=false when status stays non-OK", () => {
  const threadStep = vi.fn(() => {
    pc = 0x4010n;
    sp = 0x7ff0n;
    status = 22;
  });

  let pc = 0x4000n;
  let sp = 0x8000n;
  let status = 22;

  const boundary = checkPendingSignalsAtBoundary({
    debug_clear_syscall_trace: () => {},
    debug_clear_signal_trace: () => {},
    debug_clear_cpu_trace: () => {},
    thread_pc: () => pc,
    thread_sp: () => sp,
    thread_step: threadStep,
    thread_status: () => status,
  } as never, 0x1234, { OK: 0 } as never);

  expect(boundary).toEqual({ status: 22, delivered: false });
});

test("checkPendingSignalsAtBoundary keeps delivered=false when step(0) does not move pc/sp", () => {
  const threadStep = vi.fn(() => {
    status = 0;
  });

  let pc = 0x4000n;
  let sp = 0x8000n;
  let status = 22;

  const boundary = checkPendingSignalsAtBoundary({
    debug_clear_syscall_trace: () => {},
    debug_clear_signal_trace: () => {},
    debug_clear_cpu_trace: () => {},
    thread_pc: () => pc,
    thread_sp: () => sp,
    thread_step: threadStep,
    thread_status: () => status,
  } as never, 0x1234, { OK: 0 } as never);

  expect(boundary).toEqual({ status: 0, delivered: false });
});
