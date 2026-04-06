import { expect, test, vi } from "vite-plus/test";
import { handleFutexWait } from "../../../src/thread-worker/blocking.js";

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
  expect(postStatus).toHaveBeenNthCalledWith(1, 20, true);
  expect(postStatus).toHaveBeenNthCalledWith(2, 0);

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
      abi_eintr: () => 4,
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
  expect(postStatus).toHaveBeenNthCalledWith(1, 20, true);
  expect(postStatus).toHaveBeenNthCalledWith(2, 0);

  waitSpy.mockRestore();
});
