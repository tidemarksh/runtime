import { expect, test, vi } from "vite-plus/test";
import { createPipePollScheduler } from "../../../../src/worker/scheduler/pipe-poll.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

function makeHandle(overrides: Partial<ProcessHandle> = {}): ProcessHandle {
  return {
    pid: 6,
    pendingChildExitRecords: 0,
    pendingChildSpawnRecords: 0,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    guestBase: 0,
    kernel: {
      abi_status_pipe_wait: () => 21,
      abi_sys_read: () => 63n,
      abi_max_pipes: () => 8,
      abi_fd_type_pipe: () => 6,
      thread_blocked_syscall_nr: () => 63n,
      thread_blocked_pipe_read_ready: () => 0,
      thread_blocked_pipe_write_ready: () => 0,
      thread_poll_wait_has_socket: () => 0,
      thread_pipe_read_eof_sync_pending: () => 0,
      thread_poll_hup_sync_pending: () => 0,
      thread_poll_wait_pipe_slots_mask: () => 0n,
      thread_poll_wait_first_pipe_revents: () => 0,
      process_fd_ofd_id: () => 1,
      kernel_ofd_fd_type: () => 6,
      kernel_ofd_ino: () => 1,
    } as never,
    ...overrides,
  } as ProcessHandle;
}

function makeScheduler(hasActiveChildProcess: (pid: number) => boolean) {
  return createPipePollScheduler({
    threadWorkerStates: new WeakMap(),
    appendRuntimeLifecycleEvent: vi.fn(),
    advanceGuestTimeByMs: vi.fn(),
    handleBlockedRetryResult: vi.fn(),
    scheduleBlockedRetry: vi.fn(),
    pipeChildExitRecordsAlreadyResumedForWait: vi.fn(() => true),
    hasActiveChildProcess,
  });
}

test("pipe wait stays passive after child-exit resume only while child work remains", () => {
  const activeScheduler = makeScheduler(() => true);
  const idleScheduler = makeScheduler(() => false);

  expect(activeScheduler.pipeWaitShouldRemainPassive(
    makeHandle(),
    21,
    0x1234,
    -1,
    null,
    0,
    "child-record",
  )).toBe(true);

  expect(idleScheduler.pipeWaitShouldRemainPassive(
    makeHandle(),
    21,
    0x1234,
    -1,
    null,
    0,
    "child-record",
  )).toBe(false);

  expect(idleScheduler.pipeWaitShouldRemainPassive(
    makeHandle({ pendingChildExitRecords: 1 }),
    21,
    0x1234,
    -1,
    null,
    0,
    "child-record",
  )).toBe(true);

  expect(idleScheduler.pipeWaitShouldRemainPassive(
    makeHandle({ pendingChildSpawnRecords: 1 }),
    21,
    0x1234,
    -1,
    null,
    0,
    "child-record",
  )).toBe(true);
});
