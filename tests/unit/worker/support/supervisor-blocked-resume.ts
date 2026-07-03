import type { KernelExports } from "../../../../src/abi.js";

const SUPERVISOR_RESUME_FLAG_TIMEOUT_EXPIRED = 1;
const SUPERVISOR_BLOCKED_RESUME_FLAG_KERNEL_WORKER_AVAILABLE = 1;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT = 0;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER = 1;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER = 2;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED = 3;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT = 4;
const SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DEFER = 0;
const SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DIRECT = 1;
const SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_NONE = 0;
const SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC = 1;
const SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_FLAG_KERNEL_WORKER_AVAILABLE = 1;
const SUPERVISOR_STATE_SYNC_KIND_PIPE = 1;
const SUPERVISOR_STATE_SYNC_KIND_EPOLL = 2;
const SUPERVISOR_STATE_SYNC_KIND_SOCKET = 3;
const SUPERVISOR_STATE_SYNC_REQUEST_FLAG_WAKE_WAITERS = 1;
const SUPERVISOR_STATE_SYNC_RUN_FLAG_RUN = 1;
const SUPERVISOR_STATE_SYNC_RUN_FLAG_WAKE_WAITERS = 2;
const POLLFD_BYTES = 8;

export function installSupervisorBlockedResumeStub<T extends Partial<KernelExports>>(
  kernel: T,
): T & Pick<
  KernelExports,
  "supervisor_resume_blocked_syscall"
  | "supervisor_blocked_resume_writeback_count"
  | "supervisor_blocked_resume_writeback_ptr"
  | "supervisor_blocked_resume_writeback_len"
  | "supervisor_blocked_poll_wait_timeout_ms"
  | "supervisor_blocked_poll_wait_has_socket"
  | "supervisor_blocked_pipe_wait_has_ready_events"
  | "supervisor_blocked_wait_may_depend_on_network"
  | "supervisor_blocked_wait_should_pump_network"
  | "supervisor_blocked_resume_delegation"
  | "supervisor_blocked_wait_active_child_pid"
  | "supervisor_blocked_pipe_resume_action_defer"
  | "supervisor_blocked_pipe_resume_action_direct"
  | "supervisor_blocked_pipe_resume_action"
  | "supervisor_blocked_pipe_sync_before_retry_action_none"
  | "supervisor_blocked_pipe_sync_before_retry_action_request_sync"
  | "supervisor_blocked_pipe_sync_before_retry_action_request_sync_and_drain_output"
  | "supervisor_blocked_pipe_sync_before_retry_action_schedule_retry"
  | "supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available"
  | "supervisor_blocked_pipe_sync_before_retry_action"
  | "supervisor_blocked_pipe_sync_before_retry_requested"
  | "supervisor_blocked_pipe_sync_before_retry_clear"
  | "supervisor_state_sync_kind_pipe"
  | "supervisor_state_sync_kind_epoll"
  | "supervisor_state_sync_kind_socket"
  | "supervisor_state_sync_request_flag_wake_waiters"
  | "supervisor_state_sync_run_flag_run"
  | "supervisor_state_sync_run_flag_wake_waiters"
  | "supervisor_state_sync_request"
  | "supervisor_state_sync_next_run_flags"
  | "supervisor_state_sync_complete"
  | "supervisor_state_sync_clear"
> {
  const pipeSyncBeforeRetryRequested = new Set<number>();
  const stateSyncRequests = new Map<string, { queuedRunFlags: number; inFlight: boolean }>();
  const toNumber = (value: bigint | number | undefined): number => (
    typeof value === "bigint" ? Number(value) : (value ?? 0)
  );
  const syscallMatches = (
    syscallNr: bigint,
    abiSyscall: (() => bigint | number) | undefined,
  ): boolean => {
    const value = abiSyscall?.();
    return value !== undefined && syscallNr === BigInt(value);
  };
  const blockedSyscallNr = (vmPtr: number): bigint =>
    BigInt(kernel.thread_blocked_syscall_nr?.(vmPtr) ?? 0n);
  const blockedSyscallArg = (vmPtr: number, index: number): number =>
    toNumber(kernel.thread_blocked_syscall_arg?.(vmPtr, index));
  const maxFds = (): number => Math.max(0, toNumber(kernel.abi_max_fds?.()) || 1024);
  const writebackRange = (
    vmPtr: number,
    blockedStatus: number,
    resumeResult: bigint | number,
    index: number,
  ): { guestPtr: number; length: number } | undefined => {
    const resumeLength = toNumber(resumeResult);
    if (resumeLength <= 0) {
      return undefined;
    }
    const syscallNr = blockedSyscallNr(vmPtr);
    if (blockedStatus === kernel.abi_status_pipe_wait?.()) {
      if (syscallMatches(syscallNr, kernel.abi_sys_ppoll) && index === 0) {
        const nfds = Math.min(blockedSyscallArg(vmPtr, 1), maxFds());
        return nfds > 0
          ? { guestPtr: blockedSyscallArg(vmPtr, 0), length: nfds * POLLFD_BYTES }
          : undefined;
      }
      if (syscallMatches(syscallNr, kernel.abi_sys_pselect6) && index >= 0 && index <= 2) {
        const nfds = Math.min(blockedSyscallArg(vmPtr, 0), maxFds());
        return nfds > 0
          ? {
              guestPtr: blockedSyscallArg(vmPtr, index + 1),
              length: Math.floor((nfds + 63) / 64) * 8,
            }
          : undefined;
      }
      if (syscallMatches(syscallNr, kernel.abi_sys_read) && index === 0) {
        return { guestPtr: blockedSyscallArg(vmPtr, 1), length: resumeLength };
      }
    }
    if (
      blockedStatus === kernel.abi_status_socket_recv?.()
      && index === 0
      && (
        syscallMatches(syscallNr, kernel.abi_sys_read)
        || syscallMatches(syscallNr, kernel.abi_sys_recvfrom)
      )
    ) {
      return { guestPtr: blockedSyscallArg(vmPtr, 1), length: resumeLength };
    }
    return undefined;
  };
  const compactWritebackRanges = (
    vmPtr: number,
    blockedStatus: number,
    resumeResult: bigint | number,
  ): Array<{ guestPtr: number; length: number }> => {
    const ranges: Array<{ guestPtr: number; length: number }> = [];
    for (let index = 0; index < 3; index += 1) {
      const range = writebackRange(vmPtr, blockedStatus, resumeResult, index);
      if (range !== undefined && range.guestPtr > 0 && range.length > 0) {
        ranges.push(range);
      }
    }
    return ranges;
  };
  const nextKernel = kernel as T & Pick<
    KernelExports,
    "supervisor_resume_blocked_syscall"
    | "supervisor_blocked_resume_writeback_count"
    | "supervisor_blocked_resume_writeback_ptr"
    | "supervisor_blocked_resume_writeback_len"
    | "supervisor_blocked_poll_wait_timeout_ms"
    | "supervisor_blocked_poll_wait_has_socket"
    | "supervisor_blocked_pipe_wait_has_ready_events"
    | "supervisor_blocked_wait_may_depend_on_network"
    | "supervisor_blocked_wait_should_pump_network"
    | "supervisor_blocked_resume_delegation"
    | "supervisor_blocked_wait_active_child_pid"
    | "supervisor_blocked_pipe_resume_action_defer"
    | "supervisor_blocked_pipe_resume_action_direct"
    | "supervisor_blocked_pipe_resume_action"
    | "supervisor_blocked_pipe_sync_before_retry_action_none"
    | "supervisor_blocked_pipe_sync_before_retry_action_request_sync"
    | "supervisor_blocked_pipe_sync_before_retry_action_request_sync_and_drain_output"
    | "supervisor_blocked_pipe_sync_before_retry_action_schedule_retry"
    | "supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available"
    | "supervisor_blocked_pipe_sync_before_retry_action"
    | "supervisor_blocked_pipe_sync_before_retry_requested"
    | "supervisor_blocked_pipe_sync_before_retry_clear"
    | "supervisor_state_sync_kind_pipe"
    | "supervisor_state_sync_kind_epoll"
    | "supervisor_state_sync_kind_socket"
    | "supervisor_state_sync_request_flag_wake_waiters"
    | "supervisor_state_sync_run_flag_run"
    | "supervisor_state_sync_run_flag_wake_waiters"
    | "supervisor_state_sync_request"
    | "supervisor_state_sync_next_run_flags"
    | "supervisor_state_sync_complete"
    | "supervisor_state_sync_clear"
  >;
  nextKernel.supervisor_blocked_wait_active_child_pid ??= () => 0;
  nextKernel.supervisor_blocked_pipe_resume_action_defer ??= () =>
    SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DEFER;
  nextKernel.supervisor_blocked_pipe_resume_action_direct ??= () =>
    SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DIRECT;
  nextKernel.supervisor_blocked_pipe_resume_action ??= () =>
    SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DIRECT;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_action_none ??= () =>
    SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_NONE;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_action_request_sync ??= () =>
    SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_action_request_sync_and_drain_output ??= () =>
    2;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_action_schedule_retry ??= () =>
    3;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available ??= () =>
    SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_FLAG_KERNEL_WORKER_AVAILABLE;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_action ??= (vmPtr: number, flags: number) => {
    const kernelWorkerAvailable =
      (flags & SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_FLAG_KERNEL_WORKER_AVAILABLE) !== 0;
    if (!pipeSyncBeforeRetryRequested.has(vmPtr) && kernelWorkerAvailable) {
      pipeSyncBeforeRetryRequested.add(vmPtr);
      return SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_REQUEST_SYNC;
    }
    return SUPERVISOR_BLOCKED_PIPE_SYNC_BEFORE_RETRY_ACTION_NONE;
  };
  nextKernel.supervisor_blocked_pipe_sync_before_retry_requested ??= (vmPtr: number) =>
    pipeSyncBeforeRetryRequested.has(vmPtr) ? 1 : 0;
  nextKernel.supervisor_blocked_pipe_sync_before_retry_clear ??= (vmPtr: number) => {
    pipeSyncBeforeRetryRequested.delete(vmPtr);
    return 0;
  };
  nextKernel.supervisor_state_sync_kind_pipe ??= () => SUPERVISOR_STATE_SYNC_KIND_PIPE;
  nextKernel.supervisor_state_sync_kind_epoll ??= () => SUPERVISOR_STATE_SYNC_KIND_EPOLL;
  nextKernel.supervisor_state_sync_kind_socket ??= () => SUPERVISOR_STATE_SYNC_KIND_SOCKET;
  nextKernel.supervisor_state_sync_request_flag_wake_waiters ??= () =>
    SUPERVISOR_STATE_SYNC_REQUEST_FLAG_WAKE_WAITERS;
  nextKernel.supervisor_state_sync_run_flag_run ??= () => SUPERVISOR_STATE_SYNC_RUN_FLAG_RUN;
  nextKernel.supervisor_state_sync_run_flag_wake_waiters ??= () =>
    SUPERVISOR_STATE_SYNC_RUN_FLAG_WAKE_WAITERS;
  nextKernel.supervisor_state_sync_request ??= (vmPtr: number, kind: number, flags: number) => {
    const key = `${vmPtr}:${kind}`;
    const request = stateSyncRequests.get(key) ?? { queuedRunFlags: 0, inFlight: false };
    request.queuedRunFlags |= SUPERVISOR_STATE_SYNC_RUN_FLAG_RUN;
    if ((flags & SUPERVISOR_STATE_SYNC_REQUEST_FLAG_WAKE_WAITERS) !== 0) {
      request.queuedRunFlags |= SUPERVISOR_STATE_SYNC_RUN_FLAG_WAKE_WAITERS;
    }
    const shouldStart = !request.inFlight;
    request.inFlight = true;
    stateSyncRequests.set(key, request);
    return shouldStart ? 1 : 0;
  };
  nextKernel.supervisor_state_sync_next_run_flags ??= (vmPtr: number, kind: number) => {
    const key = `${vmPtr}:${kind}`;
    const request = stateSyncRequests.get(key);
    if (!request) {
      return 0;
    }
    const runFlags = request.queuedRunFlags;
    request.queuedRunFlags = 0;
    return runFlags;
  };
  nextKernel.supervisor_state_sync_complete ??= (vmPtr: number, kind: number) => {
    const key = `${vmPtr}:${kind}`;
    const request = stateSyncRequests.get(key);
    if (!request) {
      return 0;
    }
    if (request.queuedRunFlags !== 0) {
      return 1;
    }
    stateSyncRequests.delete(key);
    return 0;
  };
  nextKernel.supervisor_state_sync_clear ??= (vmPtr: number, kind: number) => {
    stateSyncRequests.delete(`${vmPtr}:${kind}`);
    return 0;
  };
  nextKernel.supervisor_blocked_resume_writeback_count ??= (
    vmPtr: number,
    blockedStatus: number,
    resumeResult: bigint | number,
  ) => {
    if (vmPtr === 0) {
      return -14;
    }
    return compactWritebackRanges(vmPtr, blockedStatus, resumeResult).length;
  };
  nextKernel.supervisor_blocked_resume_writeback_ptr ??= (
    vmPtr: number,
    blockedStatus: number,
    resumeResult: bigint | number,
    index: number,
  ) => compactWritebackRanges(vmPtr, blockedStatus, resumeResult)[index]?.guestPtr ?? 0;
  nextKernel.supervisor_blocked_resume_writeback_len ??= (
    vmPtr: number,
    blockedStatus: number,
    resumeResult: bigint | number,
    index: number,
  ) => compactWritebackRanges(vmPtr, blockedStatus, resumeResult)[index]?.length ?? 0;
  nextKernel.supervisor_blocked_poll_wait_timeout_ms ??= (vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    if (kernel.thread_status?.(vmPtr) !== kernel.abi_status_pipe_wait?.()) {
      return -1;
    }
    const syscallNr = blockedSyscallNr(vmPtr);
    if (
      !syscallMatches(syscallNr, kernel.abi_sys_ppoll)
      && !syscallMatches(syscallNr, kernel.abi_sys_pselect6)
    ) {
      return -1;
    }
    const timeout = kernel.thread_poll_wait_timeout?.(vmPtr);
    return timeout === undefined ? -1 : toNumber(timeout);
  };
  const pollWaitHasSocket = (vmPtr: number): boolean =>
    kernel.thread_status?.(vmPtr) === kernel.abi_status_pipe_wait?.()
    && kernel.thread_poll_wait_has_socket?.(vmPtr) === 1;
  const pipeWaitHasReadyEvents = (vmPtr: number): boolean =>
    kernel.thread_status?.(vmPtr) === kernel.abi_status_pipe_wait?.()
    && toNumber(kernel.thread_poll_wait_ready_count?.(vmPtr)) > 0;
  const waitMayDependOnNetwork = (vmPtr: number): boolean => {
    const status = kernel.thread_status?.(vmPtr);
    if (status === kernel.abi_status_pipe_wait?.()) {
      if (toNumber(nextKernel.supervisor_blocked_poll_wait_timeout_ms(vmPtr)) >= 0 && pollWaitHasSocket(vmPtr)) {
        return true;
      }
      if (pipeWaitHasReadyEvents(vmPtr)) {
        return false;
      }
      return false;
    }
    return status === kernel.abi_status_socket_recv?.()
      || status === kernel.abi_status_socket_send?.()
      || status === kernel.abi_status_connect?.();
  };
  nextKernel.supervisor_blocked_poll_wait_has_socket ??= (vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    return pollWaitHasSocket(vmPtr) ? 1 : 0;
  };
  nextKernel.supervisor_blocked_pipe_wait_has_ready_events ??= (vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    return pipeWaitHasReadyEvents(vmPtr) ? 1 : 0;
  };
  nextKernel.supervisor_blocked_wait_may_depend_on_network ??= (vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    return waitMayDependOnNetwork(vmPtr) ? 1 : 0;
  };
  nextKernel.supervisor_blocked_wait_should_pump_network ??= (vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    const status = kernel.thread_status?.(vmPtr);
    if (status === kernel.abi_status_pipe_wait?.()) {
      return waitMayDependOnNetwork(vmPtr) || !pipeWaitHasReadyEvents(vmPtr) ? 1 : 0;
    }
    return status === kernel.abi_status_socket_recv?.()
      || status === kernel.abi_status_socket_send?.()
      || status === kernel.abi_status_connect?.()
      ? 1
      : 0;
  };
  nextKernel.supervisor_blocked_resume_delegation = (vmPtr: number, flags: number) => {
    const kernelWorkerAvailable =
      (flags & SUPERVISOR_BLOCKED_RESUME_FLAG_KERNEL_WORKER_AVAILABLE) !== 0;
    const status = kernel.thread_status?.(vmPtr);
    if (kernelWorkerAvailable) {
      if (status === kernel.abi_status_wait4?.()) {
        return SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER;
      }
      if (
        status === kernel.abi_status_pipe_wait?.()
        || status === kernel.abi_status_connect?.()
        || status === kernel.abi_status_socket_recv?.()
        || status === kernel.abi_status_socket_send?.()
        || status === kernel.abi_status_flock_wait?.()
      ) {
        return SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER;
      }
      return SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED;
    }
    return status === kernel.abi_status_epoll_wait?.()
      ? SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT
      : SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT;
  };
  nextKernel.supervisor_blocked_resume_delegation_flag_kernel_worker_available = () =>
    SUPERVISOR_BLOCKED_RESUME_FLAG_KERNEL_WORKER_AVAILABLE;
  nextKernel.supervisor_blocked_resume_delegation_direct = () =>
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT;
  nextKernel.supervisor_blocked_resume_delegation_kernel_worker = () =>
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER;
  nextKernel.supervisor_blocked_resume_delegation_wait4_kernel_worker = () =>
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER;
  nextKernel.supervisor_blocked_resume_delegation_unsupported = () =>
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED;
  nextKernel.supervisor_blocked_resume_delegation_epoll_direct = () =>
    SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT;
  nextKernel.supervisor_resume_blocked_syscall = (vmPtr: number, flags: number) => {
    const timeoutExpired = (flags & SUPERVISOR_RESUME_FLAG_TIMEOUT_EXPIRED) !== 0;
    const status = kernel.thread_status?.(vmPtr);
    if (status === kernel.abi_status_wait4?.() && kernel.thread_resume_wait4 !== undefined) {
      return kernel.thread_resume_wait4(vmPtr) ?? 0n;
    }
    if (
      timeoutExpired
      && (
        status === kernel.abi_status_connect?.()
        || status === kernel.abi_status_socket_recv?.()
        || status === kernel.abi_status_socket_send?.()
      )
    ) {
      kernel.thread_resume_socket_wait_timeout?.(vmPtr);
      return 0n;
    }
    if (timeoutExpired && status === kernel.abi_status_epoll_wait?.()) {
      kernel.thread_resume_epoll_wait_timeout?.(vmPtr);
      return 0n;
    }
    if (timeoutExpired && status === kernel.abi_status_pipe_wait?.()) {
      kernel.thread_resume_poll_wait_timeout?.(vmPtr);
      return 0n;
    }
    return kernel.thread_resume_blocked_syscall?.(vmPtr) ?? 0n;
  };
  return nextKernel;
}
