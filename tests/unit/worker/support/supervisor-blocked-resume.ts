import type { KernelExports } from "../../../../src/abi.js";

const SUPERVISOR_RESUME_FLAG_TIMEOUT_EXPIRED = 1;

export function installSupervisorBlockedResumeStub<T extends Partial<KernelExports>>(
  kernel: T,
): T & Pick<KernelExports, "supervisor_resume_blocked_syscall"> {
  const nextKernel = kernel as T & Pick<KernelExports, "supervisor_resume_blocked_syscall">;
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
