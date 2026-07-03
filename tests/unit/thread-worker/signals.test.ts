// Thread-worker tests in this file validate signal-boundary behavior.
import { expect, test, vi } from "vite-plus/test";
import {
  checkPendingSignalsAtBoundary,
  hasPendingSignalForBlockedWait,
} from "../../../src/thread-worker/signals.js";

test("checkPendingSignalsAtBoundary reports delivered from supervisor boundary check", () => {
  const clearSyscallTrace = vi.fn();
  const clearSignalTrace = vi.fn();
  const clearCpuTrace = vi.fn();
  let status = 22;
  const supervisorBoundaryCheck = vi.fn(() => {
    status = 0;
    return 1;
  });

  const boundary = checkPendingSignalsAtBoundary({
    debug_clear_syscall_trace: clearSyscallTrace,
    debug_clear_signal_trace: clearSignalTrace,
    debug_clear_cpu_trace: clearCpuTrace,
    supervisor_signal_boundary_check: supervisorBoundaryCheck,
    thread_status: () => status,
  } as never, 0x1234, { OK: 0 } as never);

  expect(clearSyscallTrace).toHaveBeenCalledTimes(1);
  expect(clearSignalTrace).toHaveBeenCalledTimes(1);
  expect(clearCpuTrace).toHaveBeenCalledTimes(1);
  expect(supervisorBoundaryCheck).toHaveBeenCalledWith(0x1234);
  expect(boundary).toEqual({ status: 0, delivered: true });
});

test("checkPendingSignalsAtBoundary keeps delivered=false when status stays non-OK", () => {
  let status = 22;

  const boundary = checkPendingSignalsAtBoundary({
    debug_clear_syscall_trace: () => {},
    debug_clear_signal_trace: () => {},
    debug_clear_cpu_trace: () => {},
    supervisor_signal_boundary_check: () => 1,
    thread_status: () => status,
  } as never, 0x1234, { OK: 0 } as never);

  expect(boundary).toEqual({ status: 22, delivered: false });
});

test("checkPendingSignalsAtBoundary keeps delivered=false when supervisor reports no delivery", () => {
  let status = 22;

  const boundary = checkPendingSignalsAtBoundary({
    debug_clear_syscall_trace: () => {},
    debug_clear_signal_trace: () => {},
    debug_clear_cpu_trace: () => {},
    supervisor_signal_boundary_check: () => {
      status = 0;
      return 0;
    },
    thread_status: () => status,
  } as never, 0x1234, { OK: 0 } as never);

  expect(boundary).toEqual({ status: 0, delivered: false });
});

test("checkPendingSignalsAtBoundary requires supervisor boundary export", () => {
  expect(() => checkPendingSignalsAtBoundary({
    debug_clear_syscall_trace: () => {},
    debug_clear_signal_trace: () => {},
    debug_clear_cpu_trace: () => {},
    thread_status: () => 0,
  } as never, 0x1234, { OK: 0 } as never)).toThrow(
    "kernel supervisor ABI missing supervisor_signal_boundary_check",
  );
});

test("hasPendingSignalForBlockedWait reports supervisor blocked-wait decision", () => {
  const supervisorDecision = vi.fn(() => 0);

  expect(hasPendingSignalForBlockedWait({
    supervisor_signal_pending_for_blocked_wait: supervisorDecision,
  } as never, 0x1234)).toBe(false);

  expect(supervisorDecision).toHaveBeenCalledWith(0x1234);
});

test("hasPendingSignalForBlockedWait requires supervisor decision export", () => {
  expect(() => hasPendingSignalForBlockedWait({
    thread_sig_pending: () => 0n,
    thread_pending_signal_interrupts_blocked_syscall: () => 1,
  } as never, 0x1234)).toThrow(
    "kernel supervisor ABI missing supervisor_signal_pending_for_blocked_wait",
  );
});
