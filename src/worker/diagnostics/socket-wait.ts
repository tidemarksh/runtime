import type { ProcessHandle } from "../types.js";
import { supervisorProcessOfdForFd } from "../ownership/fd-table.js";
import { supervisorOfdFdType } from "../ownership/ofd-metadata.js";

export interface SocketWaitSchedulerDeps {}

export interface SocketWaitScheduler {
  formatFirstSocketWaitDebug(handle: ProcessHandle, vmPtr: number): string;
}

export function createSocketWaitScheduler(_deps: SocketWaitSchedulerDeps): SocketWaitScheduler {
  function formatFirstSocketWaitDebug(handle: ProcessHandle, vmPtr: number): string {
    const packed = handle.kernel.thread_poll_wait_first_socket_revents?.(vmPtr);
    if (packed === undefined) {
      return "sock=n/a";
    }
    const fd = packed & 0xff;
    const interest = (packed >>> 8) & 0xffff;
    const revents = (packed >>> 24) & 0xff;
    try {
      const ofdId = supervisorProcessOfdForFd(handle, vmPtr, fd);
      if (
        ofdId <= 0
        || supervisorOfdFdType(handle, ofdId) !== handle.kernel.abi_fd_type_socket()
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

  return {
    formatFirstSocketWaitDebug,
  };
}
