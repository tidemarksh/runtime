import type { ProcessHandle } from "../types.js";

export interface SocketWaitSchedulerDeps {
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  logNetworkDebug(event: string): void;
  scheduleBlockedRetry(handle: ProcessHandle, worker: Worker, delayMs?: number, timeoutExpired?: boolean): void;
}

export interface SocketWaitScheduler {
  socketWaitTimeoutMs(handle: ProcessHandle, status: number, vmPtr: number): number;
  formatFirstSocketWaitDebug(handle: ProcessHandle, vmPtr: number): string;
  blockedWaitMayDependOnNetwork(handle: ProcessHandle, status: number, vmPtr: number): boolean;
  blockedWaitShouldPumpNetwork(handle: ProcessHandle, status: number, vmPtr: number): boolean;
  networkInboundAvailable(handle: ProcessHandle, vmPtr: number): boolean;
  scheduleSocketBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    blocked: { timeoutDeadlineMs: number | null },
    timeoutMs: number,
  ): void;
}

export function createSocketWaitScheduler(deps: SocketWaitSchedulerDeps): SocketWaitScheduler {
  function socketWaitTimeoutMs(handle: ProcessHandle, status: number, vmPtr: number): number {
    if (
      status !== handle.kernel.abi_status_connect()
      && status !== handle.kernel.abi_status_socket_recv()
      && status !== handle.kernel.abi_status_socket_send()
    ) {
      return -1;
    }
    const timeout = handle.kernel.thread_socket_wait_timeout?.(vmPtr);
    return timeout === undefined ? -1 : Number(timeout);
  }

  function formatFirstSocketWaitDebug(handle: ProcessHandle, vmPtr: number): string {
    const packed = handle.kernel.thread_poll_wait_first_socket_revents?.(vmPtr);
    if (packed === undefined) {
      return "sock=n/a";
    }
    const fd = packed & 0xff;
    const interest = (packed >>> 8) & 0xffff;
    const revents = (packed >>> 24) & 0xff;
    try {
      const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
      if (
        ofdId <= 0
        || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_socket()
      ) {
        return `sock=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}`;
      }
      const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd) ?? -1;
      const state = socketIdx >= 0 ? handle.kernel.thread_socket_state?.(socketIdx) : undefined;
      const available = socketIdx >= 0 ? handle.kernel.thread_socket_recv_available?.(socketIdx) : undefined;
      const peerIdx = socketIdx >= 0 ? handle.kernel.thread_socket_peer_idx?.(socketIdx) : undefined;
      return `sock=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}/idx${socketIdx}/st${state ?? "n/a"}/avail${available ?? "n/a"}/peer${peerIdx ?? "n/a"}/conn${handle.networkConnections.has(fd) ? 1 : 0}`;
    } catch (error) {
      return `sock=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/err=${JSON.stringify(error instanceof Error ? error.message : String(error))}`;
    }
  }

  function pollWaitTimeoutMs(handle: ProcessHandle, status: number, vmPtr: number): number {
    if (status !== handle.kernel.abi_status_pipe_wait()) {
      return -1;
    }
    const blockedSyscall = handle.kernel.thread_blocked_syscall_nr?.(vmPtr);
    const currentSyscall = handle.kernel.thread_a7?.(vmPtr);
    const syscall = blockedSyscall !== undefined && BigInt(blockedSyscall) !== 0n
      ? BigInt(blockedSyscall)
      : currentSyscall === undefined ? null : BigInt(currentSyscall);
    if (syscall === null) {
      return -1;
    }
    const ppoll = handle.kernel.abi_sys_ppoll?.();
    const pselect6 = handle.kernel.abi_sys_pselect6?.();
    if (
      (ppoll === undefined || syscall !== BigInt(ppoll))
      && (pselect6 === undefined || syscall !== BigInt(pselect6))
    ) {
      return -1;
    }
    const timeout = handle.kernel.thread_poll_wait_timeout?.(vmPtr);
    return timeout === undefined ? -1 : Number(timeout);
  }

  function blockedWaitMayDependOnNetwork(handle: ProcessHandle, status: number, vmPtr: number): boolean {
    const pollWaitHasSocket = pollWaitTimeoutMs(handle, status, vmPtr) >= 0
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) === 1;
    const readyCount = handle.kernel.thread_poll_wait_ready_count?.(vmPtr);
    const socketRevents = handle.kernel.thread_poll_wait_first_socket_revents?.(vmPtr);
    deps.appendWorkerDebugLog(
      handle,
      `net-wait:${handle.pid}:status=${status}:poll=${pollWaitTimeoutMs(handle, status, vmPtr)}:hasSocket=${pollWaitHasSocket ? 1 : 0}:ready=${readyCount ?? "n/a"}:sock=0x${(socketRevents ?? 0).toString(16)}:conns=${handle.networkConnections.size}`,
    );
    deps.logNetworkDebug(`wait pid=${handle.pid} status=${status} poll=${pollWaitTimeoutMs(handle, status, vmPtr)} hasSocket=${pollWaitHasSocket ? 1 : 0} ready=${readyCount ?? "n/a"} sock=0x${(socketRevents ?? 0).toString(16)} conns=${handle.networkConnections.size}`);
    if (pollWaitHasSocket) {
      return true;
    }
    if (readyCount !== undefined && readyCount > 0) {
      return false;
    }
    return status === handle.kernel.abi_status_socket_recv()
      || status === handle.kernel.abi_status_socket_send()
      || status === handle.kernel.abi_status_connect();
  }

  function blockedWaitShouldPumpNetwork(handle: ProcessHandle, status: number, vmPtr: number): boolean {
    if (handle.networkConnections.size === 0) {
      return false;
    }
    if (blockedWaitMayDependOnNetwork(handle, status, vmPtr)) {
      return true;
    }
    const readyCount = status === handle.kernel.abi_status_pipe_wait()
      ? handle.kernel.thread_poll_wait_ready_count?.(vmPtr)
      : undefined;
    return status === handle.kernel.abi_status_pipe_wait()
      && (readyCount === undefined || readyCount <= 0);
  }

  function networkInboundAvailable(handle: ProcessHandle, vmPtr: number): boolean {
    if (handle.networkConnections.size === 0) {
      return false;
    }
    for (const fd of handle.networkConnections.keys()) {
      const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd) ?? -1;
      if (socketIdx >= 0 && (handle.kernel.thread_socket_recv_available?.(socketIdx) ?? 0) > 0) {
        return true;
      }
    }
    return false;
  }

  function scheduleSocketBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    blocked: { timeoutDeadlineMs: number | null },
    timeoutMs: number,
  ): void {
    if (timeoutMs < 0) {
      blocked.timeoutDeadlineMs = null;
      deps.scheduleBlockedRetry(handle, worker, 1);
      return;
    }
    const now = Date.now();
    if (blocked.timeoutDeadlineMs === null) {
      blocked.timeoutDeadlineMs = now + timeoutMs;
    }
    const remaining = blocked.timeoutDeadlineMs - now;
    if (remaining <= 0) {
      deps.scheduleBlockedRetry(handle, worker, 0, true);
      return;
    }
    deps.scheduleBlockedRetry(handle, worker, Math.min(10, remaining));
  }

  return {
    socketWaitTimeoutMs,
    formatFirstSocketWaitDebug,
    blockedWaitMayDependOnNetwork,
    blockedWaitShouldPumpNetwork,
    networkInboundAvailable,
    scheduleSocketBlockedRetry,
  };
}
