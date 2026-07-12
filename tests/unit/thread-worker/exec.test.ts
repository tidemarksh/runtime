// Thread-worker tests in this file validate typed execution outcomes.
import { expect, test, vi } from "vite-plus/test";
import { createRuntimeStateBridge } from "../../../src/bridge/runtime-state.js";
import { stepThread } from "../../../src/thread-worker/exec.js";
import { createThreadWorkerStatusOutcome } from "../../../src/thread-worker/outcomes.js";

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
  expect(postStatus).toHaveBeenCalledWith(createThreadWorkerStatusOutcome(3));
});

test("stepThread returns syscall and invalidation statuses to the owner", () => {
  for (const status of [2, 10]) {
    const clearSyscallTrace = vi.fn();
    const threadStep = vi.fn();
    const postStatus = vi.fn();

    stepThread({
      exports: {
        debug_clear_syscall_trace: clearSyscallTrace,
        thread_step: threadStep,
        thread_status: () => status,
      } as never,
      statusCodes: {
        OK: 0,
        FAULT: 1,
        FS: 2,
        FUTEX_WAIT: 20,
        EPOLL_WAIT: 22,
        JIT_READY: 10,
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
    expect(postStatus).toHaveBeenCalledWith(createThreadWorkerStatusOutcome(status));
  }
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
    runtimeBridge: createRuntimeStateBridge(),
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
  }, 0);

  expect(clearSyscallTrace).toHaveBeenCalledTimes(1);
  expect(threadStep).toHaveBeenCalledWith(0x1234, 0);
  expect(postStatus).toHaveBeenCalledTimes(1);
  expect(postStatus).toHaveBeenCalledWith(createThreadWorkerStatusOutcome(0));
});

test("stepThread returns an already-woken FUTEX_WAIT to the owner before another guest step", () => {
  const clearSyscallTrace = vi.fn();
  const threadStep = vi.fn();
  const completeFutexWait = vi.fn(() => {
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
      thread_record_blocked_addr: () => 0n,
      thread_sig_pending: () => 0n,
      supervisor_futex_wait_completion_woken: () => 1,
      supervisor_futex_wait_completion_not_equal: () => 2,
      supervisor_futex_wait_completion_invalid_address: () => 4,
      supervisor_futex_wait_completion_signal_check: () => 5,
      supervisor_complete_futex_wait: completeFutexWait,
      supervisor_thread_take_completed_sync_effects: () => 0,
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

  expect(clearSyscallTrace).toHaveBeenCalledTimes(1);
  expect(threadStep).toHaveBeenCalledTimes(1);
  expect(threadStep).toHaveBeenCalledWith(0x1234, 20_000);
  expect(completeFutexWait).toHaveBeenCalledTimes(1);
  expect(completeFutexWait).toHaveBeenCalledWith(0x1234, 1);
  expect(postStatus).toHaveBeenCalledTimes(1);
  expect(postStatus).toHaveBeenCalledWith(createThreadWorkerStatusOutcome(0));
});

test("stepThread consumes the owner budget in worker quanta when no runtime status is published", () => {
  const clearSyscallTrace = vi.fn();
  const threadStep = vi.fn();
  const postStatus = vi.fn();
  const bridge = createRuntimeStateBridge();

  stepThread({
    exports: {
      debug_clear_syscall_trace: clearSyscallTrace,
      thread_step: threadStep,
      thread_status: () => 0,
      supervisor_thread_take_completed_sync_effects: () => 0,
    } as never,
    statusCodes: {
      OK: 0,
      FUTEX_WAIT: 20,
      EPOLL_WAIT: 22,
    } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    threadStepBudget: 1_000,
    eintrErrno: 4,
    futexWaitSliceMs: 10,
    runtimeBridge: bridge,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
  }, 2_500);

  expect(clearSyscallTrace).toHaveBeenCalledTimes(3);
  expect(threadStep).toHaveBeenNthCalledWith(1, 0x1234, 1_000);
  expect(threadStep).toHaveBeenNthCalledWith(2, 0x1234, 1_000);
  expect(threadStep).toHaveBeenNthCalledWith(3, 0x1234, 500);
  expect(postStatus).toHaveBeenCalledTimes(1);
  expect(postStatus).toHaveBeenCalledWith(createThreadWorkerStatusOutcome(0));
});

test("stepThread publishes OK early when another runtime status arrives during a long step", () => {
  const clearSyscallTrace = vi.fn();
  const bridge = createRuntimeStateBridge();
  const notify = new Int32Array(bridge.notifySab);
  const threadStep = vi.fn(() => {
    Atomics.add(notify, 0, 1);
  });
  const postStatus = vi.fn();

  stepThread({
    exports: {
      debug_clear_syscall_trace: clearSyscallTrace,
      thread_step: threadStep,
      thread_status: () => 0,
      supervisor_thread_take_completed_sync_effects: () => 0,
    } as never,
    statusCodes: {
      OK: 0,
      FUTEX_WAIT: 20,
      EPOLL_WAIT: 22,
    } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    threadStepBudget: 1_000,
    eintrErrno: 4,
    futexWaitSliceMs: 10,
    runtimeBridge: bridge,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
  }, 5_000);

  expect(clearSyscallTrace).toHaveBeenCalledTimes(1);
  expect(threadStep).toHaveBeenCalledTimes(1);
  expect(threadStep).toHaveBeenCalledWith(0x1234, 1_000);
  expect(postStatus).toHaveBeenCalledTimes(1);
  expect(postStatus).toHaveBeenCalledWith(createThreadWorkerStatusOutcome(0));
});

test("stepThread forwards consumed sync effects when yielding OK early", () => {
  const clearSyscallTrace = vi.fn();
  const threadStep = vi.fn();
  const takeCompletedSyncEffects = vi.fn(() => 4);
  const postStatus = vi.fn();

  stepThread({
    exports: {
      debug_clear_syscall_trace: clearSyscallTrace,
      thread_step: threadStep,
      thread_status: () => 0,
      supervisor_thread_take_completed_sync_effects: takeCompletedSyncEffects,
    } as never,
    statusCodes: {
      OK: 0,
      FUTEX_WAIT: 20,
      EPOLL_WAIT: 22,
    } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    threadStepBudget: 1_000,
    eintrErrno: 4,
    futexWaitSliceMs: 10,
    runtimeBridge: createRuntimeStateBridge(),
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
  }, 5_000);

  expect(clearSyscallTrace).toHaveBeenCalledTimes(1);
  expect(threadStep).toHaveBeenCalledWith(0x1234, 1_000);
  expect(takeCompletedSyncEffects).toHaveBeenCalledWith(0x1234, 0);
  expect(postStatus).toHaveBeenCalledTimes(1);
  expect(postStatus).toHaveBeenCalledWith(createThreadWorkerStatusOutcome(0, false, 4));
});
