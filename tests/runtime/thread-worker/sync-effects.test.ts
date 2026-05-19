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
const SYNC_EFFECT_PROCESS_MEMFS = 1 << 7;

function makeKernelExports(syscallNr: bigint | null, fdTraceOfdType?: number) {
  return {
    abi_fd_type_file: () => 1,
    abi_fd_type_pipe: () => 2,
    abi_fd_type_epoll: () => 3,
    abi_fd_type_socket: () => 4,
    abi_sys_read: () => 63n,
    abi_sys_openat: () => 56n,
    abi_sys_write: () => 64n,
    abi_sys_pwrite64: () => 68n,
    abi_sys_lseek: () => 62n,
    abi_sys_close: () => 57n,
    abi_sys_pipe2: () => 59n,
    abi_sys_dup: () => 23n,
    abi_sys_dup3: () => 24n,
    abi_sys_fcntl: () => 25n,
    abi_sys_ioctl: () => 29n,
    abi_sys_readv: () => 65n,
    abi_sys_writev: () => 66n,
    abi_sys_socket: () => 198n,
    abi_sys_bind: () => 200n,
    abi_sys_listen: () => 201n,
    abi_sys_accept4: () => 242n,
    abi_sys_eventfd2: () => 19n,
    abi_sys_epoll_create1: () => 20n,
    abi_sys_socketpair: () => 199n,
    abi_sys_epoll_ctl: () => 21n,
    abi_sys_execve: () => 221n,
    abi_sys_setpgid: () => 154n,
    abi_sys_setsid: () => 157n,
    abi_sys_kill: () => 129n,
    abi_sys_tkill: () => 130n,
    abi_sys_tgkill: () => 131n,
    debug_fd_syscall_trace_len: () => (fdTraceOfdType === undefined ? 0 : 1),
    debug_fd_syscall_trace_nr: () => syscallNr ?? 0n,
    debug_fd_syscall_trace_arg0_ofd_type: () => fdTraceOfdType ?? 0,
    debug_syscall_trace_nr: () => syscallNr ?? 0n,
    debug_completed_sync_effects: () => 0,
  } as const;
}

const statusCodes = {
  OK: 0,
} as const;

test("readCompletedSyncEffects marks openat as a fork-shared OFD mutation without extra state sync", () => {
  const exports = makeKernelExports(56n);
  const syscallNumbers = loadSyscallNumbers(exports);

  expect(readCompletedSyncEffects(exports, statusCodes, statusCodes.OK, syscallNumbers)).toBe(
    SYNC_EFFECT_FORK_SHARED_OFD_STATE,
  );
});

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

test("readCompletedSyncEffects does not treat stream I/O as fork-shared mutations", () => {
  for (const syscallNr of [63n, 64n, 62n, 65n, 66n]) {
    const exports = makeKernelExports(syscallNr);
    const syscallNumbers = loadSyscallNumbers(exports);
    expect(readCompletedSyncEffects(exports, statusCodes, statusCodes.OK, syscallNumbers)).toBe(
      SYNC_EFFECT_PIPE_STATE
        | SYNC_EFFECT_SOCKET_STATE
        | SYNC_EFFECT_EPOLL_STATE
        | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE,
    );
  }
});

test("readCompletedSyncEffects marks fcntl/ioctl as fd-state sync without process MemFS sync", () => {
  for (const syscallNr of [25n, 29n]) {
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

test("readCompletedSyncEffects keeps regular-file stream I/O off the pipe/socket sync path", () => {
  for (const syscallNr of [63n, 62n, 65n]) {
    const exports = makeKernelExports(syscallNr, 1);
    const syscallNumbers = loadSyscallNumbers(exports);
    expect(readCompletedSyncEffects(exports, statusCodes, statusCodes.OK, syscallNumbers)).toBe(
      SYNC_EFFECT_FORK_SHARED_OFD_STATE,
    );
  }
});

test("readCompletedSyncEffects marks regular-file writes as process MemFS mutations", () => {
  for (const syscallNr of [64n, 66n, 68n]) {
    const exports = makeKernelExports(syscallNr, 1);
    const syscallNumbers = loadSyscallNumbers(exports);
    expect(readCompletedSyncEffects(exports, statusCodes, statusCodes.OK, syscallNumbers)).toBe(
      SYNC_EFFECT_FORK_SHARED_OFD_STATE | SYNC_EFFECT_PROCESS_MEMFS,
    );
  }
});

test("readCompletedSyncEffects uses explicit thread effects for stream I/O when present", () => {
  const explicitReadExports = {
    ...makeKernelExports(63n),
    thread_take_completed_sync_effects: () => SYNC_EFFECT_FORK_SHARED_OFD_STATE,
  };
  expect(readCompletedSyncEffects(
    explicitReadExports,
    statusCodes,
    statusCodes.OK,
    loadSyscallNumbers(explicitReadExports),
    0x1234,
  )).toBe(SYNC_EFFECT_FORK_SHARED_OFD_STATE);

  const explicitPipeWriteExports = {
    ...makeKernelExports(64n),
    thread_take_completed_sync_effects: () => SYNC_EFFECT_PIPE_STATE,
  };
  expect(readCompletedSyncEffects(
    explicitPipeWriteExports,
    statusCodes,
    statusCodes.OK,
    loadSyscallNumbers(explicitPipeWriteExports),
    0x1234,
  )).toBe(SYNC_EFFECT_PIPE_STATE);
});

test("readCompletedSyncEffects strips pipe effects from explicit regular-file stream I/O", () => {
  const explicitRegularReadExports = {
    ...makeKernelExports(63n, 1),
    thread_take_completed_sync_effects: () => SYNC_EFFECT_PIPE_STATE | SYNC_EFFECT_EPOLL_STATE,
  };
  expect(readCompletedSyncEffects(
    explicitRegularReadExports,
    statusCodes,
    statusCodes.OK,
    loadSyscallNumbers(explicitRegularReadExports),
    0x1234,
  )).toBe(SYNC_EFFECT_FORK_SHARED_OFD_STATE);

  const explicitRegularWriteExports = {
    ...makeKernelExports(64n, 1),
    thread_take_completed_sync_effects: () => SYNC_EFFECT_PIPE_STATE,
  };
  expect(readCompletedSyncEffects(
    explicitRegularWriteExports,
    statusCodes,
    statusCodes.OK,
    loadSyscallNumbers(explicitRegularWriteExports),
    0x1234,
  )).toBe(SYNC_EFFECT_FORK_SHARED_OFD_STATE | SYNC_EFFECT_PROCESS_MEMFS);
});

test("readCompletedSyncEffects uses explicit thread effects without harness syscall trace", () => {
  const exports = {
    ...makeKernelExports(null),
    debug_completed_sync_effects: undefined,
    debug_fd_syscall_trace_len: undefined,
    debug_fd_syscall_trace_nr: undefined,
    debug_fd_syscall_trace_arg0_ofd_type: undefined,
    debug_syscall_trace_nr: undefined,
    thread_take_completed_sync_effects: () => (
      SYNC_EFFECT_PIPE_STATE | SYNC_EFFECT_EPOLL_STATE | SYNC_EFFECT_FORK_SHARED_OFD_STATE
    ),
  };
  expect(readCompletedSyncEffects(
    exports,
    statusCodes,
    statusCodes.OK,
    loadSyscallNumbers(exports),
    0x1234,
  )).toBe(SYNC_EFFECT_PIPE_STATE | SYNC_EFFECT_EPOLL_STATE | SYNC_EFFECT_FORK_SHARED_OFD_STATE);
});

test("readCompletedSyncEffects keeps passive socket ops out of fork-shared sync", () => {
  for (const syscallNr of [200n, 201n]) {
    const exports = makeKernelExports(syscallNr);
    const syscallNumbers = loadSyscallNumbers(exports);
    expect(readCompletedSyncEffects(exports, statusCodes, statusCodes.OK, syscallNumbers)).toBe(
      SYNC_EFFECT_SOCKET_STATE | SYNC_EFFECT_EPOLL_STATE,
    );
  }
});

test("readCompletedSyncEffects marks pipe2, eventfd2, and epoll_create1 as ownership-affecting sync points", () => {
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

  const epollCreate1Exports = makeKernelExports(20n);
  const epollCreate1Syscalls = loadSyscallNumbers(epollCreate1Exports);
  expect(readCompletedSyncEffects(epollCreate1Exports, statusCodes, statusCodes.OK, epollCreate1Syscalls)).toBe(
    SYNC_EFFECT_EPOLL_STATE | SYNC_EFFECT_FORK_SHARED_OFD_STATE,
  );

  const socketExports = makeKernelExports(198n);
  const socketSyscalls = loadSyscallNumbers(socketExports);
  expect(readCompletedSyncEffects(socketExports, statusCodes, statusCodes.OK, socketSyscalls)).toBe(
    SYNC_EFFECT_SOCKET_STATE | SYNC_EFFECT_EPOLL_STATE | SYNC_EFFECT_FORK_SHARED_OFD_STATE,
  );

  const accept4Exports = makeKernelExports(242n);
  const accept4Syscalls = loadSyscallNumbers(accept4Exports);
  expect(readCompletedSyncEffects(accept4Exports, statusCodes, statusCodes.OK, accept4Syscalls)).toBe(
    SYNC_EFFECT_SOCKET_STATE | SYNC_EFFECT_EPOLL_STATE | SYNC_EFFECT_FORK_SHARED_OFD_STATE,
  );

  const socketpairExports = makeKernelExports(199n);
  const socketpairSyscalls = loadSyscallNumbers(socketpairExports);
  expect(readCompletedSyncEffects(socketpairExports, statusCodes, statusCodes.OK, socketpairSyscalls)).toBe(
    SYNC_EFFECT_SOCKET_STATE | SYNC_EFFECT_EPOLL_STATE | SYNC_EFFECT_FORK_SHARED_OFD_STATE,
  );
});

test("readCompletedSyncEffects keeps lifecycle and signal wakeups distinct from fd ownership sync", () => {
  const execveExports = makeKernelExports(221n);
  const execveSyscalls = loadSyscallNumbers(execveExports);
  expect(readCompletedSyncEffects(execveExports, statusCodes, statusCodes.OK, execveSyscalls)).toBe(
    SYNC_EFFECT_PIPE_STATE
      | SYNC_EFFECT_SOCKET_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
      | SYNC_EFFECT_FORK_SHARED_OFD_STATE
      | SYNC_EFFECT_PROCESS_LIFECYCLE,
  );

  const killExports = makeKernelExports(129n);
  const killSyscalls = loadSyscallNumbers(killExports);
  expect(readCompletedSyncEffects(killExports, statusCodes, statusCodes.OK, killSyscalls)).toBe(
    SYNC_EFFECT_SIGNAL_WAITERS,
  );
});

test("readCompletedSyncEffects normalizes aggregated fork-shared effects for non-mutating syscalls", () => {
  const exports = {
    ...makeKernelExports(64n),
    debug_completed_sync_effects: () => (
      SYNC_EFFECT_PIPE_STATE
      | SYNC_EFFECT_SOCKET_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
      | SYNC_EFFECT_FORK_SHARED_OFD_STATE
    ),
  };
  const syscallNumbers = loadSyscallNumbers(exports);
  expect(readCompletedSyncEffects(exports, statusCodes, statusCodes.OK, syscallNumbers)).toBe(
    SYNC_EFFECT_PIPE_STATE
      | SYNC_EFFECT_SOCKET_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE,
  );
});

test("readCompletedSyncEffects prefers traced syscall effects over aggregated carry-over on OK", () => {
  const exports = {
    ...makeKernelExports(64n),
    debug_completed_sync_effects: () => (
      SYNC_EFFECT_PIPE_STATE
      | SYNC_EFFECT_SOCKET_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
      | SYNC_EFFECT_PROCESS_LIFECYCLE
      | SYNC_EFFECT_SIGNAL_WAITERS
      | SYNC_EFFECT_FORK_SHARED_OFD_STATE
    ),
  };
  const syscallNumbers = loadSyscallNumbers(exports);
  expect(readCompletedSyncEffects(exports, statusCodes, statusCodes.OK, syscallNumbers)).toBe(
    SYNC_EFFECT_PIPE_STATE
      | SYNC_EFFECT_SOCKET_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE,
  );
});

test("readCompletedSyncEffects uses aggregated effects even when OK has no traced syscall", () => {
  const exports = {
    ...makeKernelExports(null),
    debug_completed_sync_effects: () => (
      SYNC_EFFECT_PIPE_STATE
      | SYNC_EFFECT_SOCKET_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
      | SYNC_EFFECT_PROCESS_LIFECYCLE
      | SYNC_EFFECT_SIGNAL_WAITERS
      | SYNC_EFFECT_FORK_SHARED_OFD_STATE
    ),
  };
  const syscallNumbers = loadSyscallNumbers(exports);
  expect(readCompletedSyncEffects(exports, statusCodes, statusCodes.OK, syscallNumbers)).toBe(
    SYNC_EFFECT_PIPE_STATE
      | SYNC_EFFECT_SOCKET_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
      | SYNC_EFFECT_PROCESS_LIFECYCLE
      | SYNC_EFFECT_SIGNAL_WAITERS
      | SYNC_EFFECT_FORK_SHARED_OFD_STATE,
  );
});
