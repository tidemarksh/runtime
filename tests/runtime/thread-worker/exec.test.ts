import { expect, test, vi } from "vite-plus/test";
import { stepThread } from "../../../src/thread-worker/exec.js";

test("stepThread forwards non-blocked fault status directly to postStatus", () => {
  const clearSyscallTrace = vi.fn();
  const threadStep = vi.fn();
  const postStatus = vi.fn();

  let status = 3;

  stepThread({
    exports: {
      debug_clear_syscall_trace: clearSyscallTrace,
      thread_step: threadStep,
      thread_status: () => status,
    } as never,
    statusCodes: {
      FUTEX_WAIT: 20,
      EPOLL_WAIT: 22,
    } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    threadStepBudget: 20_000,
    eintrErrno: 4,
    futexWaitSliceMs: 10,
    runtimeBridge: null,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
  }, 20_000);

  expect(clearSyscallTrace).toHaveBeenCalledTimes(1);
  expect(threadStep).toHaveBeenCalledWith(0x1234, 20_000);
  expect(postStatus).toHaveBeenCalledTimes(1);
  expect(postStatus).toHaveBeenCalledWith(3);
});

test("stepThread does not reroute OK through the blocked-status handlers", () => {
  const clearSyscallTrace = vi.fn();
  const threadStep = vi.fn();
  const postStatus = vi.fn();

  let status = 0;

  stepThread({
    exports: {
      debug_clear_syscall_trace: clearSyscallTrace,
      thread_step: threadStep,
      thread_status: () => status,
    } as never,
    statusCodes: {
      FUTEX_WAIT: 20,
      EPOLL_WAIT: 22,
    } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    threadStepBudget: 20_000,
    eintrErrno: 4,
    futexWaitSliceMs: 10,
    runtimeBridge: {} as never,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
  }, 0);

  expect(clearSyscallTrace).toHaveBeenCalledTimes(1);
  expect(threadStep).toHaveBeenCalledWith(0x1234, 0);
  expect(postStatus).toHaveBeenCalledTimes(1);
  expect(postStatus).toHaveBeenCalledWith(0);
});

test("stepThread resolves FUTEX_WAIT in-worker before reporting completion", () => {
  const clearSyscallTrace = vi.fn();
  const threadStep = vi.fn();
  const resumeFutexWait = vi.fn(() => {
    status = 0;
  });
  const postStatus = vi.fn();

  let status = 20;

  stepThread({
    exports: {
      memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
      debug_clear_syscall_trace: clearSyscallTrace,
      thread_step: threadStep,
      thread_status: () => status,
      thread_futex_wait_addr: () => 0n,
      thread_futex_wait_timeout: () => 0n,
      thread_futex_wait_expected: () => 0,
      thread_record_blocked_addr: () => 0,
      thread_sig_pending: () => 0n,
      thread_resume_futex_wait: resumeFutexWait,
      thread_resume_futex_wait_result: () => {},
    } as never,
    statusCodes: {
      OK: 0,
      FUTEX_WAIT: 20,
      EPOLL_WAIT: 22,
    } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    threadStepBudget: 20_000,
    eintrErrno: 4,
    futexWaitSliceMs: 10,
    runtimeBridge: null,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
  }, 20_000);

  expect(clearSyscallTrace).toHaveBeenCalledTimes(2);
  expect(threadStep).toHaveBeenCalledTimes(2);
  expect(threadStep).toHaveBeenNthCalledWith(1, 0x1234, 20_000);
  expect(threadStep).toHaveBeenNthCalledWith(2, 0x1234, 20_000);
  expect(resumeFutexWait).toHaveBeenCalledTimes(1);
  expect(postStatus).toHaveBeenCalledTimes(1);
  expect(postStatus).toHaveBeenCalledWith(0);
});
