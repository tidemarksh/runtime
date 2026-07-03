import type { ProcessHandle } from "../types.js";
import {
  supervisorBlockedPollWaitHasSocket,
  supervisorBlockedPollWaitTimeoutMs,
  supervisorBlockedWaitMayDependOnNetwork,
  supervisorBlockedWaitShouldPumpNetwork,
} from "../../supervisor-blocked-resume.js";

export interface SocketWaitSchedulerDeps {
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  logNetworkDebug(event: string): void;
}

export interface SocketWaitScheduler {
  formatFirstSocketWaitDebug(handle: ProcessHandle, vmPtr: number): string;
  blockedWaitMayDependOnNetwork(handle: ProcessHandle, status: number, vmPtr: number): boolean;
  blockedWaitShouldPumpNetwork(handle: ProcessHandle, status: number, vmPtr: number): boolean;
  networkInboundAvailable(handle: ProcessHandle, vmPtr: number): boolean;
}

export function createSocketWaitScheduler(deps: SocketWaitSchedulerDeps): SocketWaitScheduler {
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

  function pollWaitTimeoutMs(handle: ProcessHandle, _status: number, vmPtr: number): number {
    return supervisorBlockedPollWaitTimeoutMs(handle.kernel, vmPtr);
  }

  function blockedWaitMayDependOnNetwork(handle: ProcessHandle, status: number, vmPtr: number): boolean {
    const pollTimeoutMs = pollWaitTimeoutMs(handle, status, vmPtr);
    const pollWaitHasSocket = supervisorBlockedPollWaitHasSocket(handle.kernel, vmPtr);
    const mayDependOnNetwork = supervisorBlockedWaitMayDependOnNetwork(handle.kernel, vmPtr);
    const readyCount = handle.kernel.thread_poll_wait_ready_count?.(vmPtr);
    const socketRevents = handle.kernel.thread_poll_wait_first_socket_revents?.(vmPtr);
    deps.appendWorkerDebugLog(
      handle,
      `net-wait:${handle.pid}:status=${status}:poll=${pollTimeoutMs}:hasSocket=${pollWaitHasSocket ? 1 : 0}:ready=${readyCount ?? "n/a"}:sock=0x${(socketRevents ?? 0).toString(16)}:conns=${handle.networkConnections.size}`,
    );
    deps.logNetworkDebug(`wait pid=${handle.pid} status=${status} poll=${pollTimeoutMs} hasSocket=${pollWaitHasSocket ? 1 : 0} ready=${readyCount ?? "n/a"} sock=0x${(socketRevents ?? 0).toString(16)} conns=${handle.networkConnections.size}`);
    return mayDependOnNetwork;
  }

  function blockedWaitShouldPumpNetwork(handle: ProcessHandle, status: number, vmPtr: number): boolean {
    if (handle.networkConnections.size === 0) {
      return false;
    }
    if (blockedWaitMayDependOnNetwork(handle, status, vmPtr)) {
      return true;
    }
    return supervisorBlockedWaitShouldPumpNetwork(handle.kernel, vmPtr);
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

  return {
    formatFirstSocketWaitDebug,
    blockedWaitMayDependOnNetwork,
    blockedWaitShouldPumpNetwork,
    networkInboundAvailable,
  };
}
