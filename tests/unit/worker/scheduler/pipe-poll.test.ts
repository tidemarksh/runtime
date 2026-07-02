import { expect, test, vi } from "vite-plus/test";
import { createPipePollScheduler } from "../../../../src/worker/scheduler/pipe-poll.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

function makeHandle(overrides: Partial<ProcessHandle> = {}): ProcessHandle {
  return {
    pid: 6,
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
    scheduleSupervisorBlockedRetry: vi.fn(),
    hasActiveChildProcess: (handle) => hasActiveChildProcess(handle.pid),
  });
}

test("pipe wait stays passive only while kernel reports active child work", () => {
  const activeScheduler = makeScheduler(() => true);
  const idleScheduler = makeScheduler(() => false);

  expect(activeScheduler.pipeWaitShouldRemainPassive(
    makeHandle(),
    21,
    0x1234,
    -1,
    0,
  )).toBe(true);

  expect(idleScheduler.pipeWaitShouldRemainPassive(
    makeHandle(),
    21,
    0x1234,
    -1,
    0,
  )).toBe(false);

  expect(idleScheduler.pipeWaitShouldRemainPassive(
    makeHandle({ pendingChildSpawnRecords: 1 }),
    21,
    0x1234,
    -1,
    0,
  )).toBe(false);
});

test("empty pselect pipe wait stays passive without child work", () => {
  const scheduler = makeScheduler(() => false);
  const base = makeHandle();
  const handle = makeHandle({
    kernel: {
      ...base.kernel,
      abi_sys_pselect6: () => 72n,
      thread_blocked_syscall_nr: () => 72n,
      thread_blocked_syscall_arg: () => 0n,
    } as never,
  });

  expect(scheduler.pipeWaitShouldRemainPassive(
    handle,
    21,
    0x1234,
    -1,
    0,
  )).toBe(true);
});

test("empty pselect passive parking does not depend on local child state", () => {
  const scheduler = makeScheduler(() => false);
  const base = makeHandle();
  const handle = makeHandle({
    kernel: {
      ...base.kernel,
      abi_sys_pselect6: () => 72n,
      thread_blocked_syscall_nr: () => 72n,
      thread_blocked_syscall_arg: () => 0n,
    } as never,
  });

  expect(scheduler.pipeWaitShouldRemainPassive(
    handle,
    21,
    0x1234,
    -1,
    0,
  )).toBe(true);
});

test("pselect with a watched fd does not use empty-set passive parking", () => {
  const scheduler = makeScheduler(() => false);
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const view = new DataView(memory.buffer);
  view.setUint8(0x20, 1 << 3);
  const base = makeHandle();
  const handle = makeHandle({
    memory,
    kernel: {
      ...base.kernel,
      abi_sys_pselect6: () => 72n,
      thread_blocked_syscall_nr: () => 72n,
      thread_blocked_syscall_arg: (_vmPtr: number, index: number) => {
        switch (index) {
          case 0:
            return 8n;
          case 1:
            return 0x20n;
          default:
            return 0n;
        }
      },
    } as never,
  });

  expect(scheduler.pipeWaitShouldRemainPassive(
    handle,
    21,
    0x1234,
    -1,
    0,
  )).toBe(false);
});
