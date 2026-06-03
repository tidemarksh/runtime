// Thread-worker tests in this file validate typed blocking outcomes.
import { expect, test, vi } from "vite-plus/test";
import { createRuntimeStateBridge } from "../../../src/bridge/runtime-state.js";
import {
  handleFutexWait,
  handleRuntimeBridgeBlockedStatus,
  isRuntimeBridgeBlockedStatus,
  resumeBlockedSyscallStep,
} from "../../../src/thread-worker/blocking.js";
import { createThreadWorkerStatusOutcome } from "../../../src/thread-worker/outcomes.js";

test("isRuntimeBridgeBlockedStatus keeps only indefinite EPOLL_WAIT in the thread-worker", () => {
  const exports = {
    thread_epoll_wait_timeout: () => -1n,
    thread_process_thread_count: () => 1,
  } as never;
  const statusCodes = { EPOLL_WAIT: 22 } as never;

  expect(isRuntimeBridgeBlockedStatus(exports, 0x1234, 22, statusCodes)).toBe(false);

  exports.thread_epoll_wait_timeout = () => 0n;
  expect(isRuntimeBridgeBlockedStatus(exports, 0x1234, 22, statusCodes)).toBe(false);

  exports.thread_epoll_wait_timeout = () => 10n;
  expect(isRuntimeBridgeBlockedStatus(exports, 0x1234, 22, statusCodes)).toBe(false);

  exports.thread_epoll_wait_timeout = () => -1n;
  exports.thread_process_thread_count = () => 2;
  expect(isRuntimeBridgeBlockedStatus(exports, 0x1234, 22, statusCodes)).toBe(false);
});

test("resumeBlockedSyscallStep applies guest writes and returns a typed status outcome", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const bytes = new Uint8Array(memory.buffer);
  bytes[0x20] = "O".charCodeAt(0);
  bytes[0x21] = "K".charCodeAt(0);
  const events: string[] = [];
  const recordResumeBlockedDebug = vi.fn();

  const outcome = resumeBlockedSyscallStep({
    exports: {
      memory,
      thread_a0: () => 3n,
      thread_a1: () => 0x20n,
      thread_reg: () => 4n,
      thread_blocked_syscall_nr: () => 63n,
      thread_blocked_syscall_arg: (_vmPtr: number, index: number) => [3n, 0x20n, 4n][index] ?? 0n,
      thread_resume_blocked_syscall: () => {
        events.push("resume");
        return 2n;
      },
      thread_status: () => 0,
    } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    resumeBlockedSyscall: true,
    pipeResumeDebugEnabled: true,
    applyGuestMemoryWrites() {
      events.push("apply-writes");
    },
    recordResumeBlockedDebug,
  });

  expect(events).toEqual(["apply-writes", "resume"]);
  expect(outcome).toEqual(createThreadWorkerStatusOutcome(0));
  expect(recordResumeBlockedDebug).toHaveBeenCalledWith(
    'thread-resume-blocked:fd=3:buf=0x20:count=4:saved=63/3/0x20/4:ret=2:status=0:data="OK"',
  );
});

test("resumeBlockedSyscallStep skips work when the step is not a blocked-syscall resume", () => {
  const applyGuestMemoryWrites = vi.fn();
  const outcome = resumeBlockedSyscallStep({
    exports: {
      memory: new WebAssembly.Memory({ initial: 1 }),
      thread_resume_blocked_syscall: vi.fn(),
    } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    resumeBlockedSyscall: false,
    pipeResumeDebugEnabled: true,
    applyGuestMemoryWrites,
    recordResumeBlockedDebug: vi.fn(),
  });

  expect(outcome).toBeUndefined();
  expect(applyGuestMemoryWrites).not.toHaveBeenCalled();
});

test("handleFutexWait re-waits on the requeued futex address", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const i32 = new Int32Array(memory.buffer);
  const waitSpy = vi.spyOn(Atomics, "wait");
  const resumeWait = vi.fn(() => {
    status = 0;
  });
  const resumeWaitResult = vi.fn();
  const postStatus = vi.fn();

  const initialAddr = 0x100;
  const requeuedAddr = 0x200;
  const expected = 7;
  let status = 20;
  let blockedAddr = requeuedAddr;
  let waitCalls = 0;

  i32[initialAddr >>> 2] = expected;
  i32[requeuedAddr >>> 2] = expected;

  waitSpy.mockImplementation((view, index, value) => {
    waitCalls += 1;
    expect(view.buffer).toBe(memory.buffer);
    expect(value).toBe(expected);
    if (waitCalls === 1) {
      expect(index).toBe(initialAddr >>> 2);
      return "timed-out";
    }
    expect(index).toBe(requeuedAddr >>> 2);
    blockedAddr = 0;
    return "ok";
  });

  const handled = handleFutexWait({
    exports: {
      memory,
      thread_futex_wait_addr: () => BigInt(initialAddr),
      thread_futex_wait_timeout: () => 0n,
      thread_futex_wait_expected: () => expected,
      thread_record_blocked_addr: () => blockedAddr,
      thread_sig_pending: () => 0n,
      thread_resume_futex_wait: resumeWait,
      thread_resume_futex_wait_result: resumeWaitResult,
      thread_status: () => status,
    } as never,
    statusCodes: { FUTEX_WAIT: 20 } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    runtimeBridge: null,
    threadStepBudget: 1000,
    eintrErrno: 4,
    futexWaitSliceMs: 0,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
    step: () => {},
  }, 20);

  expect(handled).toBe(false);
  expect(waitCalls).toBe(2);
  expect(resumeWait).toHaveBeenCalledTimes(1);
  expect(resumeWaitResult).not.toHaveBeenCalled();
  expect(postStatus).toHaveBeenNthCalledWith(1, createThreadWorkerStatusOutcome(20, true));
  expect(postStatus).toHaveBeenNthCalledWith(2, createThreadWorkerStatusOutcome(0));

  waitSpy.mockRestore();
});

test("handleFutexWait resumes with EINTR when a signal becomes pending", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const i32 = new Int32Array(memory.buffer);
  const waitSpy = vi.spyOn(Atomics, "wait");
  const resumeWait = vi.fn();
  const resumeWaitResult = vi.fn(() => {
    status = 0;
  });
  const postStatus = vi.fn();

  const guestAddr = 0x100;
  const expected = 11;
  let status = 20;
  let blockedAddr = guestAddr;
  let sigPending = 0n;

  i32[guestAddr >>> 2] = expected;

  waitSpy.mockImplementation((view, index, value) => {
    expect(view.buffer).toBe(memory.buffer);
    expect(index).toBe(guestAddr >>> 2);
    expect(value).toBe(expected);
    sigPending = 1n;
    return "timed-out";
  });

  const handled = handleFutexWait({
    exports: {
      memory,
      thread_futex_wait_addr: () => BigInt(guestAddr),
      thread_futex_wait_timeout: () => 0n,
      thread_futex_wait_expected: () => expected,
      thread_record_blocked_addr: () => blockedAddr,
      thread_sig_pending: () => sigPending,
      thread_resume_futex_wait: resumeWait,
      thread_resume_futex_wait_result: (_vmPtr, result) => {
        blockedAddr = 0;
        resumeWaitResult(result);
      },
      thread_status: () => status,
      abi_eintr: () => -4,
    } as never,
    statusCodes: { FUTEX_WAIT: 20 } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    runtimeBridge: null,
    threadStepBudget: 1000,
    eintrErrno: 4,
    futexWaitSliceMs: 0,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
    step: () => {},
  }, 20);

  expect(handled).toBe(false);
  expect(resumeWait).not.toHaveBeenCalled();
  expect(resumeWaitResult).toHaveBeenCalledWith(BigInt(-4));
  expect(postStatus).toHaveBeenNthCalledWith(1, createThreadWorkerStatusOutcome(20, true));
  expect(postStatus).toHaveBeenNthCalledWith(2, createThreadWorkerStatusOutcome(0));

  waitSpy.mockRestore();
});

test("handleFutexWait resumes with EINTR when a signal wake notifies the futex", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const i32 = new Int32Array(memory.buffer);
  const waitSpy = vi.spyOn(Atomics, "wait");
  const resumeWait = vi.fn();
  const resumeWaitResult = vi.fn(() => {
    status = 0;
  });
  const postStatus = vi.fn();

  const guestAddr = 0x100;
  const expected = 17;
  let status = 20;
  let blockedAddr = guestAddr;
  let sigPending = 0n;

  i32[guestAddr >>> 2] = expected;

  waitSpy.mockImplementation((view, index, value) => {
    expect(view.buffer).toBe(memory.buffer);
    expect(index).toBe(guestAddr >>> 2);
    expect(value).toBe(expected);
    sigPending = 1n;
    return "ok";
  });

  const handled = handleFutexWait({
    exports: {
      memory,
      thread_futex_wait_addr: () => BigInt(guestAddr),
      thread_futex_wait_timeout: () => 0n,
      thread_futex_wait_expected: () => expected,
      thread_record_blocked_addr: () => blockedAddr,
      thread_sig_pending: () => sigPending,
      thread_resume_futex_wait: resumeWait,
      thread_resume_futex_wait_result: (_vmPtr, result) => {
        blockedAddr = 0;
        resumeWaitResult(result);
      },
      thread_status: () => status,
      abi_eintr: () => -4,
    } as never,
    statusCodes: { FUTEX_WAIT: 20 } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    runtimeBridge: null,
    threadStepBudget: 1000,
    eintrErrno: 4,
    futexWaitSliceMs: 0,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
    step: () => {},
  }, 20);

  expect(handled).toBe(false);
  expect(resumeWait).not.toHaveBeenCalled();
  expect(resumeWaitResult).toHaveBeenCalledWith(BigInt(-4));
  expect(postStatus).toHaveBeenNthCalledWith(1, createThreadWorkerStatusOutcome(20, true));
  expect(postStatus).toHaveBeenNthCalledWith(2, createThreadWorkerStatusOutcome(0));

  waitSpy.mockRestore();
});

test("handleFutexWait resumes with EINTR for process-directed pending signals", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const i32 = new Int32Array(memory.buffer);
  const waitSpy = vi.spyOn(Atomics, "wait");
  const resumeWait = vi.fn();
  const resumeWaitResult = vi.fn(() => {
    status = 0;
  });
  const postStatus = vi.fn();

  const guestAddr = 0x100;
  const expected = 19;
  let status = 20;
  let blockedAddr = guestAddr;
  let processSignalInterrupts = false;

  i32[guestAddr >>> 2] = expected;

  waitSpy.mockImplementation((view, index, value) => {
    expect(view.buffer).toBe(memory.buffer);
    expect(index).toBe(guestAddr >>> 2);
    expect(value).toBe(expected);
    processSignalInterrupts = true;
    return "timed-out";
  });

  const handled = handleFutexWait({
    exports: {
      memory,
      thread_futex_wait_addr: () => BigInt(guestAddr),
      thread_futex_wait_timeout: () => 0n,
      thread_futex_wait_expected: () => expected,
      thread_record_blocked_addr: () => blockedAddr,
      thread_sig_pending: () => 0n,
      thread_pending_signal_interrupts_blocked_syscall: () => processSignalInterrupts ? 1 : 0,
      thread_resume_futex_wait: resumeWait,
      thread_resume_futex_wait_result: (_vmPtr, result) => {
        blockedAddr = 0;
        resumeWaitResult(result);
      },
      thread_status: () => status,
      abi_eintr: () => -4,
    } as never,
    statusCodes: { FUTEX_WAIT: 20 } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    runtimeBridge: null,
    threadStepBudget: 1000,
    eintrErrno: 4,
    futexWaitSliceMs: 0,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
    step: () => {},
  }, 20);

  expect(handled).toBe(false);
  expect(resumeWait).not.toHaveBeenCalled();
  expect(resumeWaitResult).toHaveBeenCalledWith(BigInt(-4));
  expect(postStatus).toHaveBeenNthCalledWith(1, createThreadWorkerStatusOutcome(20, true));
  expect(postStatus).toHaveBeenNthCalledWith(2, createThreadWorkerStatusOutcome(0));

  waitSpy.mockRestore();
});

test("handleFutexWait treats missing thread-record sentinel as EAGAIN instead of requeueing", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const i32 = new Int32Array(memory.buffer);
  const waitSpy = vi.spyOn(Atomics, "wait");
  const resumeWait = vi.fn();
  const resumeWaitResult = vi.fn(() => {
    status = 0;
  });
  const postStatus = vi.fn();

  const guestAddr = 0x100;
  const expected = 29;
  let status = 20;
  let blockedAddr = guestAddr;

  i32[guestAddr >>> 2] = expected;

  waitSpy.mockImplementation((view, index, value) => {
    expect(view.buffer).toBe(memory.buffer);
    expect(index).toBe(guestAddr >>> 2);
    expect(value).toBe(expected);
    blockedAddr = 0xffff_ffff;
    return "timed-out";
  });

  const handled = handleFutexWait({
    exports: {
      memory,
      thread_futex_wait_addr: () => BigInt(guestAddr),
      thread_futex_wait_timeout: () => 0n,
      thread_futex_wait_expected: () => expected,
      thread_record_blocked_addr: () => blockedAddr,
      thread_sig_pending: () => 0n,
      thread_resume_futex_wait: resumeWait,
      thread_resume_futex_wait_result: (_vmPtr, result) => {
        blockedAddr = 0;
        resumeWaitResult(result);
      },
      thread_status: () => status,
      abi_eagain: () => -11,
    } as never,
    statusCodes: { FUTEX_WAIT: 20 } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    runtimeBridge: null,
    threadStepBudget: 1000,
    eintrErrno: 4,
    futexWaitSliceMs: 0,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
    step: () => {},
  }, 20);

  expect(handled).toBe(false);
  expect(resumeWait).not.toHaveBeenCalled();
  expect(resumeWaitResult).toHaveBeenCalledWith(BigInt(-11));
  expect(postStatus).toHaveBeenNthCalledWith(1, createThreadWorkerStatusOutcome(20, true));
  expect(postStatus).toHaveBeenNthCalledWith(2, createThreadWorkerStatusOutcome(0));

  waitSpy.mockRestore();
});

test("handleFutexWait exits when reattached kernel state changes the thread status", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const i32 = new Int32Array(memory.buffer);
  const waitSpy = vi.spyOn(Atomics, "wait");
  const resumeWait = vi.fn();
  const resumeWaitResult = vi.fn();
  const postStatus = vi.fn();

  const guestAddr = 0x100;
  const expected = 31;
  let status = 20;

  i32[guestAddr >>> 2] = expected;

  waitSpy.mockImplementation((view, index, value) => {
    expect(view.buffer).toBe(memory.buffer);
    expect(index).toBe(guestAddr >>> 2);
    expect(value).toBe(expected);
    status = 9;
    return "timed-out";
  });

  const handled = handleFutexWait({
    exports: {
      memory,
      thread_futex_wait_addr: () => BigInt(guestAddr),
      thread_futex_wait_timeout: () => 0n,
      thread_futex_wait_expected: () => expected,
      thread_record_blocked_addr: () => guestAddr,
      thread_sig_pending: () => 0n,
      thread_resume_futex_wait: resumeWait,
      thread_resume_futex_wait_result: resumeWaitResult,
      thread_status: () => status,
    } as never,
    statusCodes: { FUTEX_WAIT: 20 } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    runtimeBridge: null,
    threadStepBudget: 1000,
    eintrErrno: 4,
    futexWaitSliceMs: 0,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
    step: () => {},
  }, 20);

  expect(handled).toBe(false);
  expect(resumeWait).not.toHaveBeenCalled();
  expect(resumeWaitResult).not.toHaveBeenCalled();
  expect(postStatus).toHaveBeenNthCalledWith(1, createThreadWorkerStatusOutcome(20, true));
  expect(postStatus).toHaveBeenNthCalledWith(2, createThreadWorkerStatusOutcome(9));

  waitSpy.mockRestore();
});

test("handleRuntimeBridgeBlockedStatus treats epoll timeout=0 as an immediate timeout retry", () => {
  const bridge = createRuntimeStateBridge();
  const waitSpy = vi.spyOn(Atomics, "wait");
  const resumeTimeout = vi.fn(() => {
    status = 0;
  });
  const resumeBlocked = vi.fn();
  const postStatus = vi.fn();
  let status = 0;

  waitSpy.mockImplementation((_view, _index, _value, timeout) => {
    expect(timeout).toBe(0);
    return "timed-out";
  });

  handleRuntimeBridgeBlockedStatus({
    exports: {
      memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
      thread_epoll_wait_timeout: () => 0n,
      thread_pc: () => 0n,
      thread_sp: () => 0n,
      thread_step: () => {},
      thread_status: () => status,
      thread_resume_epoll_wait_timeout: resumeTimeout,
      thread_resume_blocked_syscall: resumeBlocked,
    } as never,
    statusCodes: { OK: 0, FUTEX_WAIT: 20, EPOLL_WAIT: 22 } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    runtimeBridge: bridge,
    threadStepBudget: 1000,
    eintrErrno: 4,
    futexWaitSliceMs: 0,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
    step: () => {},
  }, 22);

  expect(resumeTimeout).toHaveBeenCalledWith(0x1234);
  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(postStatus).toHaveBeenNthCalledWith(1, createThreadWorkerStatusOutcome(22, true));
  expect(postStatus).toHaveBeenNthCalledWith(2, createThreadWorkerStatusOutcome(0));

  waitSpy.mockRestore();
});

test("handleRuntimeBridgeBlockedStatus hands off EPOLL_WAIT timeout retries into the futex path", () => {
  const bridge = createRuntimeStateBridge();
  const waitSpy = vi.spyOn(Atomics, "wait");
  const resumeTimeout = vi.fn(() => {
    status = 20;
  });
  const resumeFutexWait = vi.fn(() => {
    status = 0;
  });
  const resumeBlocked = vi.fn();
  const postStatus = vi.fn();
  const step = vi.fn();
  let status = 22;

  waitSpy.mockImplementation((_view, _index, _value, timeout) => {
    expect(timeout).toBe(0);
    return "timed-out";
  });

  handleRuntimeBridgeBlockedStatus({
    exports: {
      memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
      thread_epoll_wait_timeout: () => 0n,
      thread_pc: () => 0n,
      thread_sp: () => 0n,
      thread_step: () => {},
      thread_status: () => status,
      thread_resume_epoll_wait_timeout: resumeTimeout,
      thread_resume_blocked_syscall: resumeBlocked,
      thread_futex_wait_addr: () => 0n,
      thread_futex_wait_timeout: () => 0n,
      thread_futex_wait_expected: () => 0,
      thread_record_blocked_addr: () => 0,
      thread_sig_pending: () => 0n,
      thread_resume_futex_wait: resumeFutexWait,
      thread_resume_futex_wait_result: () => {},
    } as never,
    statusCodes: { OK: 0, FUTEX_WAIT: 20, EPOLL_WAIT: 22 } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    runtimeBridge: bridge,
    threadStepBudget: 1000,
    eintrErrno: 4,
    futexWaitSliceMs: 0,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
    step,
  }, 22);

  expect(resumeTimeout).toHaveBeenCalledWith(0x1234);
  expect(resumeFutexWait).toHaveBeenCalledWith(0x1234);
  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(step).toHaveBeenCalledWith(1000);
  expect(postStatus).toHaveBeenCalledWith(createThreadWorkerStatusOutcome(22, true));

  waitSpy.mockRestore();
});
