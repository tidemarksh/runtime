import type { KernelExports, StatusCodes } from "../abi.js";

const SYNC_EFFECT_PIPE_STATE = 1 << 0;
const SYNC_EFFECT_SOCKET_STATE = 1 << 1;
const SYNC_EFFECT_EPOLL_STATE = 1 << 2;
const SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE = 1 << 3;
const SYNC_EFFECT_PROCESS_LIFECYCLE = 1 << 4;
const SYNC_EFFECT_SIGNAL_WAITERS = 1 << 5;
const SYNC_EFFECT_FORK_SHARED_OFD_STATE = 1 << 6;
const SYNC_EFFECT_PROCESS_MEMFS = 1 << 7;

interface FdTraceSnapshot {
  syscallNr: bigint;
  arg0OfdType: number;
  arg1: bigint | null;
  result: bigint | null;
}

export interface SyscallNumbers {
  read: bigint | null;
  openat: bigint | null;
  write: bigint | null;
  pwrite64: bigint | null;
  lseek: bigint | null;
  execve: bigint | null;
  close: bigint | null;
  closeRange: bigint | null;
  pipe2: bigint | null;
  epollCtl: bigint | null;
  dup: bigint | null;
  dup3: bigint | null;
  fcntl: bigint | null;
  ioctl: bigint | null;
  readv: bigint | null;
  writev: bigint | null;
  socket: bigint | null;
  bind: bigint | null;
  listen: bigint | null;
  connect: bigint | null;
  accept4: bigint | null;
  sendto: bigint | null;
  recvfrom: bigint | null;
  sendmsg: bigint | null;
  recvmsg: bigint | null;
  setsockopt: bigint | null;
  shutdown: bigint | null;
  eventfd2: bigint | null;
  epollCreate1: bigint | null;
  socketpair: bigint | null;
  setpgid: bigint | null;
  setsid: bigint | null;
  kill: bigint | null;
  tkill: bigint | null;
  tgkill: bigint | null;
}

function readOptionalU64(value: bigint | number | undefined): bigint | null {
  if (value === undefined) {
    return null;
  }
  return typeof value === "bigint" ? value : BigInt(value);
}

function matchesSyscall(syscallNr: bigint, candidate: bigint | null): boolean {
  return candidate !== null && syscallNr === candidate;
}

function readLatestFdTrace(
  exports: KernelExports,
  syscallNr: bigint,
): FdTraceSnapshot | null {
  const len = exports.debug_fd_syscall_trace_len?.() ?? 0;
  for (let index = len - 1; index >= 0; index -= 1) {
    const tracedNr = readOptionalU64(exports.debug_fd_syscall_trace_nr?.(index));
    if (tracedNr === null || tracedNr !== syscallNr) {
      continue;
    }
    return {
      syscallNr: tracedNr,
      arg0OfdType: exports.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0,
      arg1: readOptionalU64(exports.debug_fd_syscall_trace_arg1?.(index)),
      result: readOptionalU64(exports.debug_fd_syscall_trace_result?.(index)),
    };
  }
  return null;
}

function readLatestAnyFdTrace(exports: KernelExports): FdTraceSnapshot | null {
  const len = exports.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return null;
  }
  const index = len - 1;
  const syscallNr = readOptionalU64(exports.debug_fd_syscall_trace_nr?.(index));
  if (syscallNr === null) {
    return null;
  }
  return {
    syscallNr,
    arg0OfdType: exports.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0,
    arg1: readOptionalU64(exports.debug_fd_syscall_trace_arg1?.(index)),
    result: readOptionalU64(exports.debug_fd_syscall_trace_result?.(index)),
  };
}

function regularFileFdTrace(
  exports: KernelExports,
  syscallNr: bigint,
): boolean {
  const fdTrace = readLatestFdTrace(exports, syscallNr);
  if (fdTrace === null) {
    return false;
  }
  return fdTrace.arg0OfdType === exports.abi_fd_type_file();
}

function latestFdTraceType(exports: KernelExports, syscallNr: bigint): number {
  return readLatestFdTrace(exports, syscallNr)?.arg0OfdType ?? 0;
}

function latestFdTraceFailed(exports: KernelExports, syscallNr: bigint): boolean {
  const result = readLatestFdTrace(exports, syscallNr)?.result;
  return result !== null && result !== undefined && BigInt.asIntN(64, result) < 0n;
}

function isRegularFileWritingSyscall(
  syscallNr: bigint,
  syscallNumbers: SyscallNumbers,
): boolean {
  return matchesSyscall(syscallNr, syscallNumbers.write)
    || matchesSyscall(syscallNr, syscallNumbers.writev)
    || matchesSyscall(syscallNr, syscallNumbers.pwrite64);
}

function fcntlCommandNeedsFdStateSync(command: bigint | null): boolean {
  if (command === null) {
    return true;
  }
  switch (command) {
    case 1n: // F_GETFD
    case 2n: // F_SETFD: FD-table-only in process memory.
    case 3n: // F_GETFL
      return false;
    default:
      return true;
  }
}

function ioctlRequestNeedsFdStateSync(request: bigint | null): boolean {
  if (request === null) {
    return true;
  }
  return request === 0x5421n; // FIONBIO changes O_NONBLOCK on the open file description.
}

function syscallMutatesForkSharedOfdState(
  syscallNr: bigint,
  syscallNumbers: SyscallNumbers,
): boolean {
  return matchesSyscall(syscallNr, syscallNumbers.openat)
    || matchesSyscall(syscallNr, syscallNumbers.close)
    || matchesSyscall(syscallNr, syscallNumbers.dup)
    || matchesSyscall(syscallNr, syscallNumbers.dup3)
    || matchesSyscall(syscallNr, syscallNumbers.pipe2)
    || matchesSyscall(syscallNr, syscallNumbers.socket)
    || matchesSyscall(syscallNr, syscallNumbers.accept4)
    || matchesSyscall(syscallNr, syscallNumbers.eventfd2)
    || matchesSyscall(syscallNr, syscallNumbers.epollCreate1);
}

function normalizeForkSharedSyncEffects(
  effects: number,
  syscallNr: bigint | null,
  syscallNumbers: SyscallNumbers | null,
): number {
  if (
    syscallNr === null
    || syscallNumbers === null
    || syscallMutatesForkSharedOfdState(syscallNr, syscallNumbers)
  ) {
    return effects;
  }
  return effects & ~SYNC_EFFECT_FORK_SHARED_OFD_STATE;
}

export function loadSyscallNumbers(exports: KernelExports): SyscallNumbers {
  return {
    read: readOptionalU64(exports.abi_sys_read?.()),
    openat: readOptionalU64(exports.abi_sys_openat?.()),
    write: readOptionalU64(exports.abi_sys_write?.()),
    pwrite64: readOptionalU64(exports.abi_sys_pwrite64?.()),
    lseek: readOptionalU64(exports.abi_sys_lseek?.()),
    execve: readOptionalU64(exports.abi_sys_execve?.()),
    close: readOptionalU64(exports.abi_sys_close?.()),
    closeRange: readOptionalU64(exports.abi_sys_close_range?.()),
    pipe2: readOptionalU64(exports.abi_sys_pipe2?.()),
    epollCtl: readOptionalU64(exports.abi_sys_epoll_ctl?.()),
    dup: readOptionalU64(exports.abi_sys_dup?.()),
    dup3: readOptionalU64(exports.abi_sys_dup3?.()),
    fcntl: readOptionalU64(exports.abi_sys_fcntl?.()),
    ioctl: readOptionalU64(exports.abi_sys_ioctl?.()),
    readv: readOptionalU64(exports.abi_sys_readv?.()),
    writev: readOptionalU64(exports.abi_sys_writev?.()),
    socket: readOptionalU64(exports.abi_sys_socket?.()),
    bind: readOptionalU64(exports.abi_sys_bind?.()),
    listen: readOptionalU64(exports.abi_sys_listen?.()),
    connect: readOptionalU64(exports.abi_sys_connect?.()),
    accept4: readOptionalU64(exports.abi_sys_accept4?.()),
    sendto: readOptionalU64(exports.abi_sys_sendto?.()),
    recvfrom: readOptionalU64(exports.abi_sys_recvfrom?.()),
    sendmsg: readOptionalU64(exports.abi_sys_sendmsg?.()),
    recvmsg: readOptionalU64(exports.abi_sys_recvmsg?.()),
    setsockopt: readOptionalU64(exports.abi_sys_setsockopt?.()),
    shutdown: readOptionalU64(exports.abi_sys_shutdown?.()),
    eventfd2: readOptionalU64(exports.abi_sys_eventfd2?.()),
    epollCreate1: readOptionalU64(exports.abi_sys_epoll_create1?.()),
    socketpair: readOptionalU64(exports.abi_sys_socketpair?.()),
    setpgid: readOptionalU64(exports.abi_sys_setpgid?.()),
    setsid: readOptionalU64(exports.abi_sys_setsid?.()),
    kill: readOptionalU64(exports.abi_sys_kill?.()),
    tkill: readOptionalU64(exports.abi_sys_tkill?.()),
    tgkill: readOptionalU64(exports.abi_sys_tgkill?.()),
  };
}

export function readCompletedSyncEffects(
  exports: KernelExports,
  statusCodes: StatusCodes,
  status: number,
  syscallNumbers: SyscallNumbers | null,
  vmPtr = 0,
): number {
  const readSyscallNr = (): bigint | null => {
    const traced = exports.debug_syscall_trace_nr?.();
    if (traced !== undefined && traced !== 0n) {
      return traced;
    }
    return readLatestAnyFdTrace(exports)?.syscallNr ?? null;
  };
  const threadCompletedEffects = exports.thread_take_completed_sync_effects?.(vmPtr);
  const hasThreadCompletedEffects = threadCompletedEffects !== undefined;
  const aggregatedEffects = threadCompletedEffects
    ?? exports.debug_completed_sync_effects?.()
    ?? 0;
  const completedEffects = aggregatedEffects;
  if (status !== statusCodes.OK || syscallNumbers === null) {
    return completedEffects === 0
      ? 0
      : normalizeForkSharedSyncEffects(completedEffects, readSyscallNr(), syscallNumbers);
  }

  const syscallNr = readSyscallNr();

  if (syscallNr === null) {
    return completedEffects === 0
      ? 0
      : normalizeForkSharedSyncEffects(completedEffects, null, syscallNumbers);
  }

  if (
    matchesSyscall(syscallNr, syscallNumbers.read)
    || matchesSyscall(syscallNr, syscallNumbers.write)
    || matchesSyscall(syscallNr, syscallNumbers.pwrite64)
    || matchesSyscall(syscallNr, syscallNumbers.lseek)
    || matchesSyscall(syscallNr, syscallNumbers.readv)
    || matchesSyscall(syscallNr, syscallNumbers.writev)
  ) {
    if (hasThreadCompletedEffects) {
      if (regularFileFdTrace(exports, syscallNr)) {
        const regularFileEffects = completedEffects
          & ~(
            SYNC_EFFECT_PIPE_STATE
            | SYNC_EFFECT_SOCKET_STATE
            | SYNC_EFFECT_EPOLL_STATE
            | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
          );
        if (isRegularFileWritingSyscall(syscallNr, syscallNumbers)) {
          return regularFileEffects | SYNC_EFFECT_FORK_SHARED_OFD_STATE | SYNC_EFFECT_PROCESS_MEMFS;
        }
        return regularFileEffects | SYNC_EFFECT_FORK_SHARED_OFD_STATE;
      }
      return completedEffects;
    }
    if (regularFileFdTrace(exports, syscallNr)) {
      if (isRegularFileWritingSyscall(syscallNr, syscallNumbers)) {
        return SYNC_EFFECT_FORK_SHARED_OFD_STATE | SYNC_EFFECT_PROCESS_MEMFS;
      }
      return SYNC_EFFECT_FORK_SHARED_OFD_STATE;
    }
    return SYNC_EFFECT_PIPE_STATE
      | SYNC_EFFECT_SOCKET_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE;
  }

  if (
    matchesSyscall(syscallNr, syscallNumbers.fcntl)
    || matchesSyscall(syscallNr, syscallNumbers.ioctl)
  ) {
    const fdTrace = readLatestFdTrace(exports, syscallNr);
    if (latestFdTraceFailed(exports, syscallNr)) {
      return completedEffects & ~SYNC_EFFECT_FORK_SHARED_OFD_STATE;
    }
    const needsFdStateSync = fdTrace === null
      ? true
      : matchesSyscall(syscallNr, syscallNumbers.fcntl)
        ? fcntlCommandNeedsFdStateSync(fdTrace.arg1)
        : ioctlRequestNeedsFdStateSync(fdTrace.arg1);
    if (!needsFdStateSync) {
      return 0;
    }
    const mayMutateForkSharedOfd = fdTrace === null || fdTrace.arg1 === null
      ? true
      : matchesSyscall(syscallNr, syscallNumbers.fcntl)
        ? fdTrace.arg1 === 4n // F_SETFL changes open file description status flags.
        : fdTrace.arg1 === 0x5421n; // FIONBIO changes O_NONBLOCK via ioctl.
    return SYNC_EFFECT_PIPE_STATE
      | SYNC_EFFECT_SOCKET_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
      | (mayMutateForkSharedOfd ? SYNC_EFFECT_FORK_SHARED_OFD_STATE : 0);
  }

  if (matchesSyscall(syscallNr, syscallNumbers.openat)) {
    return SYNC_EFFECT_FORK_SHARED_OFD_STATE;
  }

  if (
    matchesSyscall(syscallNr, syscallNumbers.close)
    || matchesSyscall(syscallNr, syscallNumbers.dup)
    || matchesSyscall(syscallNr, syscallNumbers.dup3)
  ) {
    if (latestFdTraceFailed(exports, syscallNr)) {
      return completedEffects & ~SYNC_EFFECT_FORK_SHARED_OFD_STATE;
    }
    const fdType = latestFdTraceType(exports, syscallNr);
    if (fdType === exports.abi_fd_type_file()) {
      return SYNC_EFFECT_FORK_SHARED_OFD_STATE;
    }
    if (fdType === exports.abi_fd_type_socket()) {
      return SYNC_EFFECT_SOCKET_STATE
        | SYNC_EFFECT_EPOLL_STATE
        | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
        | SYNC_EFFECT_FORK_SHARED_OFD_STATE;
    }
    if (fdType === exports.abi_fd_type_pipe()) {
      return SYNC_EFFECT_PIPE_STATE
        | SYNC_EFFECT_EPOLL_STATE
        | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
        | SYNC_EFFECT_FORK_SHARED_OFD_STATE;
    }
    return SYNC_EFFECT_PIPE_STATE
      | SYNC_EFFECT_SOCKET_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
      | SYNC_EFFECT_FORK_SHARED_OFD_STATE;
  }

  if (matchesSyscall(syscallNr, syscallNumbers.closeRange)) {
    return SYNC_EFFECT_PIPE_STATE
      | SYNC_EFFECT_SOCKET_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
      | SYNC_EFFECT_FORK_SHARED_OFD_STATE;
  }

  if (matchesSyscall(syscallNr, syscallNumbers.pipe2)) {
    return SYNC_EFFECT_PIPE_STATE | SYNC_EFFECT_EPOLL_STATE | SYNC_EFFECT_FORK_SHARED_OFD_STATE;
  }

  if (
    matchesSyscall(syscallNr, syscallNumbers.socket)
    || matchesSyscall(syscallNr, syscallNumbers.socketpair)
    || matchesSyscall(syscallNr, syscallNumbers.accept4)
  ) {
    return SYNC_EFFECT_SOCKET_STATE | SYNC_EFFECT_EPOLL_STATE | SYNC_EFFECT_FORK_SHARED_OFD_STATE;
  }

  if (
    matchesSyscall(syscallNr, syscallNumbers.bind)
    || matchesSyscall(syscallNr, syscallNumbers.listen)
    || matchesSyscall(syscallNr, syscallNumbers.connect)
    || matchesSyscall(syscallNr, syscallNumbers.sendto)
    || matchesSyscall(syscallNr, syscallNumbers.recvfrom)
    || matchesSyscall(syscallNr, syscallNumbers.sendmsg)
    || matchesSyscall(syscallNr, syscallNumbers.recvmsg)
    || matchesSyscall(syscallNr, syscallNumbers.setsockopt)
    || matchesSyscall(syscallNr, syscallNumbers.shutdown)
  ) {
    return SYNC_EFFECT_SOCKET_STATE | SYNC_EFFECT_EPOLL_STATE;
  }

  if (matchesSyscall(syscallNr, syscallNumbers.eventfd2)) {
    return SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_FORK_SHARED_OFD_STATE;
  }

  if (matchesSyscall(syscallNr, syscallNumbers.epollCreate1)) {
    return SYNC_EFFECT_EPOLL_STATE | SYNC_EFFECT_FORK_SHARED_OFD_STATE;
  }

  if (matchesSyscall(syscallNr, syscallNumbers.epollCtl)) {
    return SYNC_EFFECT_EPOLL_STATE;
  }

  if (matchesSyscall(syscallNr, syscallNumbers.execve)) {
    return SYNC_EFFECT_PIPE_STATE
      | SYNC_EFFECT_SOCKET_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
      | SYNC_EFFECT_FORK_SHARED_OFD_STATE
      | SYNC_EFFECT_PROCESS_LIFECYCLE;
  }

  if (
    matchesSyscall(syscallNr, syscallNumbers.setpgid)
    || matchesSyscall(syscallNr, syscallNumbers.setsid)
  ) {
    return SYNC_EFFECT_PROCESS_LIFECYCLE;
  }

  if (
    matchesSyscall(syscallNr, syscallNumbers.kill)
    || matchesSyscall(syscallNr, syscallNumbers.tkill)
    || matchesSyscall(syscallNr, syscallNumbers.tgkill)
  ) {
    return SYNC_EFFECT_SIGNAL_WAITERS;
  }

  return completedEffects === 0
    ? 0
    : normalizeForkSharedSyncEffects(completedEffects, syscallNr, syscallNumbers);
}
