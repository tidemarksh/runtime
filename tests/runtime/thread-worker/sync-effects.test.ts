import { expect, test } from "vite-plus/test";
import {
  loadSyscallNumbers,
  readCompletedSyncEffects,
} from "../../../src/thread-worker/sync-effects.js";

const SYNC_EFFECT_PIPE_STATE = 1 << 0;
const SYNC_EFFECT_SOCKET_STATE = 1 << 1;
const SYNC_EFFECT_EPOLL_STATE = 1 << 2;
const SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE = 1 << 3;
const SYNC_EFFECT_PROCESS_LIFECYCLE = 1 << 4;
const SYNC_EFFECT_SIGNAL_WAITERS = 1 << 5;
const SYNC_EFFECT_FORK_SHARED_OFD_STATE = 1 << 6;

function makeKernelExports(syscallNr: bigint | null) {
  return {
    abi_sys_close: () => 57n,
    abi_sys_pipe2: () => 59n,
    abi_sys_dup: () => 23n,
    abi_sys_dup3: () => 24n,
    abi_sys_eventfd2: () => 19n,
    abi_sys_execve: () => 221n,
    abi_sys_kill: () => 129n,
    debug_syscall_trace_nr: () => syscallNr ?? 0n,
    debug_completed_sync_effects: () => 0,
  } as const;
}

const statusCodes = {
  OK: 0,
} as const;

test("readCompletedSyncEffects marks close/dup/dup3 as fork-shared OFD mutations", () => {
  for (const syscallNr of [57n, 23n, 24n]) {
    const exports = makeKernelExports(syscallNr);
    const syscallNumbers = loadSyscallNumbers(exports);

    expect(readCompletedSyncEffects(exports, statusCodes, statusCodes.OK, syscallNumbers)).toBe(
      SYNC_EFFECT_PIPE_STATE
        | SYNC_EFFECT_SOCKET_STATE
        | SYNC_EFFECT_EPOLL_STATE
        | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
        | SYNC_EFFECT_FORK_SHARED_OFD_STATE,
    );
  }
});

test("readCompletedSyncEffects marks pipe2 and eventfd2 as ownership-affecting sync points", () => {
  const pipe2Exports = makeKernelExports(59n);
  const pipe2Syscalls = loadSyscallNumbers(pipe2Exports);
  expect(readCompletedSyncEffects(pipe2Exports, statusCodes, statusCodes.OK, pipe2Syscalls)).toBe(
    SYNC_EFFECT_PIPE_STATE | SYNC_EFFECT_EPOLL_STATE | SYNC_EFFECT_FORK_SHARED_OFD_STATE,
  );

  const eventfd2Exports = makeKernelExports(19n);
  const eventfd2Syscalls = loadSyscallNumbers(eventfd2Exports);
  expect(readCompletedSyncEffects(eventfd2Exports, statusCodes, statusCodes.OK, eventfd2Syscalls)).toBe(
    SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_FORK_SHARED_OFD_STATE,
  );
});

test("readCompletedSyncEffects keeps lifecycle and signal wakeups distinct from fd ownership sync", () => {
  const execveExports = makeKernelExports(221n);
  const execveSyscalls = loadSyscallNumbers(execveExports);
  expect(readCompletedSyncEffects(execveExports, statusCodes, statusCodes.OK, execveSyscalls)).toBe(
    SYNC_EFFECT_PROCESS_LIFECYCLE,
  );

  const killExports = makeKernelExports(129n);
  const killSyscalls = loadSyscallNumbers(killExports);
  expect(readCompletedSyncEffects(killExports, statusCodes, statusCodes.OK, killSyscalls)).toBe(
    SYNC_EFFECT_SIGNAL_WAITERS,
  );
});
