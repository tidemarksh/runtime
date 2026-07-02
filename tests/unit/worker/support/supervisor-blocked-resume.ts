import type { KernelExports } from "../../../../src/abi.js";

const SUPERVISOR_RESUME_FLAG_TIMEOUT_EXPIRED = 1;
const SUPERVISOR_BLOCKED_RESUME_FLAG_KERNEL_WORKER_AVAILABLE = 1;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_DIRECT = 0;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_KERNEL_WORKER = 1;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_WAIT4_KERNEL_WORKER = 2;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_UNSUPPORTED = 3;
const SUPERVISOR_BLOCKED_RESUME_DELEGATION_EPOLL_DIRECT = 4;

export function installSupervisorBlockedResumeStub<T extends Partial<KernelExports>>(
  kernel: T,
): T & Pick<KernelExports, "supervisor_resume_blocked_syscall" | "supervisor_blocked_resume_delegation"> {
  const nextKernel = kernel as T & Pick<
    KernelExports,
    "supervisor_resume_blocked_syscall" | "supervisor_blocked_resume_delegation"
  >;
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
