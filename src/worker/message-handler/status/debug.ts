import type { ProcessHandle } from "../../types.js";
import type { ThreadWorkerStatusMessage } from "../types.js";

export const STATUS_SOCKET_DEBUG_LOG_ENABLED = ((globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.TIDEMARK_WORKER_DEBUG_LOG ?? "") === "1"
  || ((globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.TIDEMARK_WORKER_DEBUG_NETWORK ?? "") === "1";

export function appendStatusSocketDebug(handle: ProcessHandle, event: string): void {
  if (!STATUS_SOCKET_DEBUG_LOG_ENABLED) {
    return;
  }
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
}

const statusSocketMetaDebug = new WeakMap<ProcessHandle, Map<number, string>>();

export function recordStatusSocketMetaTransition(
  handle: ProcessHandle,
  source: string,
  fd: number,
  socketIdx: number,
  state: number,
  type: number,
  ext: number,
  buf: number,
  recv: number,
): void {
  if (!STATUS_SOCKET_DEBUG_LOG_ENABLED) {
    return;
  }
  let byFd = statusSocketMetaDebug.get(handle);
  if (byFd === undefined) {
    byFd = new Map();
    statusSocketMetaDebug.set(handle, byFd);
  }
  const next = `${socketIdx}/${state}/${type}/${ext}/${buf}/${recv}`;
  const previous = byFd.get(fd);
  if (previous === next) {
    return;
  }
  byFd.set(fd, next);
  appendStatusSocketDebug(handle, `socket-meta-transition:${handle.pid}:source=${source}:fd=${fd}:prev=${previous ?? "none"}:next=${next}`);
}

export function formatStatusFdEntries(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  limit = 8,
): string {
  if (!msg.fdEntries || msg.fdEntries.length === 0) {
    return "none";
  }
  const maxFds = handle.kernel.abi_max_fds();
  return msg.fdEntries
    .filter((entry) => entry.fd >= 0 && entry.fd < limit && entry.ofdId !== 0)
    .map((entry) => {
      const hasValidOfd = entry.ofdId > 0 && entry.ofdId < maxFds;
      const type = hasValidOfd ? handle.kernel.kernel_ofd_fd_type(entry.ofdId) : -1;
      const ino = hasValidOfd ? handle.kernel.kernel_ofd_ino(entry.ofdId) : -1;
      const hostFd = hasValidOfd ? handle.kernel.kernel_ofd_host_fd(entry.ofdId) : -1;
      const offset = hasValidOfd && handle.kernel.kernel_ofd_offset
        ? handle.kernel.kernel_ofd_offset(entry.ofdId)
        : -1n;
      return `${entry.fd}->${entry.ofdId}/${entry.fdFlags}/${type}/${ino}/${hostFd}/${offset.toString()}`;
    })
    .join(" ");
}

export function shouldRecordStatusFdLifecycle(
  handle: ProcessHandle,
  status: number,
  msg: ThreadWorkerStatusMessage,
): boolean {
  if (!msg.fdEntries || msg.fdEntries.length === 0) {
    return false;
  }
  if (status === handle.kernel.abi_status_ok()) {
    return false;
  }
  if (
    status === handle.kernel.abi_status_pipe_wait()
    || status === handle.kernel.abi_status_wait4()
    || handle.stdioOwner !== null
  ) {
    return true;
  }
  return msg.fdEntries.some((entry) => entry.fd >= 4 && entry.ofdId !== 0);
}

export function formatLifecycleFdTrace(trace: string, maxLength = 360): string {
  return trace.length <= maxLength
    ? trace
    : `${trace.slice(0, maxLength)}...`;
}

export function pipeSlotLifecycleDebugEnabled(): boolean {
  return ((globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_PIPE_SLOT_DEBUG ?? "") === "1";
}

export function formatBytesAscii(bytes: Uint8Array, limit = 32): string {
  return [...bytes.subarray(0, limit)]
    .map((byte) => byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : ".")
    .join("");
}
