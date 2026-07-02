import type { ProcessHandle } from "./types.js";
import {
  SOCKET_SHUT_RD_WR_MASK,
  SOCKET_SLOT_EXTERNAL_PEER_OFFSET,
  SOCKET_SLOT_RECV_BUF_IDX_OFFSET,
  SOCKET_SLOT_SHUTDOWN_MASK_OFFSET,
  SOCKET_SLOT_STATE_OFFSET,
  SOCKET_STATE_SHUTDOWN,
} from "../layout/socket.js";
const NETWORK_CLOSE_DEBUG_LOG_ENABLED = ((globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.TIDEMARK_WORKER_DEBUG_LOG ?? "") === "1"
  || ((globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.TIDEMARK_WORKER_DEBUG_NETWORK ?? "") === "1";

function appendNetworkCloseDebug(handle: ProcessHandle, event: string): void {
  if (!NETWORK_CLOSE_DEBUG_LOG_ENABLED) {
    return;
  }
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
}

function socketSlotBytes(handle: ProcessHandle): number {
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const socketTableBytes = handle.kernel.kernel_socket_table_bytes?.() ?? 0;
  const bytes = maxSockets > 0 ? socketTableBytes / maxSockets : 0;
  return Number.isInteger(bytes) && bytes > SOCKET_SLOT_SHUTDOWN_MASK_OFFSET + 4 ? bytes : 0;
}

function patchSocketSlotShutdown(memory: Uint8Array, absolute: number): void {
  if (absolute < 0 || absolute + SOCKET_SLOT_SHUTDOWN_MASK_OFFSET + 4 > memory.byteLength) {
    return;
  }
  const view = new DataView(memory.buffer);
  view.setUint32(absolute + SOCKET_SLOT_STATE_OFFSET, SOCKET_STATE_SHUTDOWN, true);
  view.setUint32(absolute + SOCKET_SLOT_EXTERNAL_PEER_OFFSET, 0, true);
  view.setUint32(absolute + SOCKET_SLOT_SHUTDOWN_MASK_OFFSET, SOCKET_SHUT_RD_WR_MASK, true);
}

function socketTablePtrs(handle: ProcessHandle): number[] {
  const ptrs = [handle.kernelState.socketTablePtr];
  const activePtr = handle.kernel.kernel_socket_table_ptr?.();
  if (activePtr !== undefined && !ptrs.includes(activePtr)) {
    ptrs.push(activePtr);
  }
  return ptrs;
}

export function forceNetworkClosedSocketSlot(handle: ProcessHandle, vmPtr: number, fd: number): void {
  const bytes = socketSlotBytes(handle);
  if (bytes <= 0) {
    appendNetworkCloseDebug(handle, `net-force-close:${handle.pid}:fd=${fd}:skip=slot-bytes`);
    return;
  }
  const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd) ?? -1;
  if (socketIdx < 0) {
    appendNetworkCloseDebug(handle, `net-force-close:${handle.pid}:fd=${fd}:skip=socket-idx`);
    return;
  }
  const beforeState = handle.kernel.thread_socket_state?.(socketIdx) ?? -1;
  const memory = new Uint8Array(handle.memory.buffer);
  const tablePtrs = socketTablePtrs(handle);
  for (const tablePtr of tablePtrs) {
    patchSocketSlotShutdown(memory, tablePtr + socketIdx * bytes);
  }

  const peerIdx = handle.kernel.thread_socket_peer_idx?.(socketIdx) ?? -1;
  if (peerIdx >= 0) {
    for (const tablePtr of tablePtrs) {
      patchSocketSlotShutdown(memory, tablePtr + peerIdx * bytes);
    }
  }
  const afterState = handle.kernel.thread_socket_state?.(socketIdx) ?? -1;
  appendNetworkCloseDebug(
    handle,
    `net-force-close:${handle.pid}:fd=${fd}:idx=${socketIdx}:peer=${peerIdx}:bytes=${bytes}:ptrs=${tablePtrs.join(",")}:state=${beforeState}/${afterState}`,
  );
}

export function forceNetworkClosedFd(handle: ProcessHandle, vmPtr: number, fd: number): void {
  const supervisorCloseSocket = handle.kernel.supervisor_socket_force_shutdown
    ?? handle.kernel.supervisor_socket_close_external_peer;
  if (supervisorCloseSocket) {
    supervisorCloseSocket(vmPtr, fd);
    return;
  }
  const closeSocket = handle.kernel.thread_socket_force_shutdown
    ?? handle.kernel.thread_socket_close_external_peer;
  closeSocket?.(vmPtr, fd);
  forceNetworkClosedSocketSlot(handle, vmPtr, fd);
}

export function forceNetworkClosedFds(handle: ProcessHandle, vmPtr = handle.vmPtr): void {
  if (handle.networkClosedFds.size === 0) {
    return;
  }
  for (const fd of handle.networkClosedFds) {
    forceNetworkClosedFd(handle, vmPtr, fd);
  }
}

export function snapshotNetworkSocketSlot(
  handle: ProcessHandle,
  vmPtr: number,
  fd: number,
): { offset: number; data: Uint8Array } | null {
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const bytes = socketSlotBytes(handle);
  const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd) ?? -1;
  if (socketIdx < 0 || socketIdx >= maxSockets || bytes <= 0) {
    return null;
  }
  const offset = socketIdx * bytes;
  const tablePtr = handle.kernel.kernel_socket_table_ptr?.() ?? handle.kernelState.socketTablePtr;
  const absolute = tablePtr + offset;
  return {
    offset,
    data: new Uint8Array(handle.memory.buffer).slice(absolute, absolute + bytes),
  };
}

export function restoreNetworkSocketSlotSnapshot(
  handle: ProcessHandle,
  slot: { offset: number; data: Uint8Array },
): void {
  const memory = new Uint8Array(handle.memory.buffer);
  for (const tablePtr of socketTablePtrs(handle)) {
    memory.set(slot.data, tablePtr + slot.offset);
  }
}

export function networkSocketSlotRecvBufferIndex(slot: { data: Uint8Array }): number {
  if (slot.data.byteLength < SOCKET_SLOT_RECV_BUF_IDX_OFFSET + 4) {
    return -1;
  }
  return new DataView(
    slot.data.buffer,
    slot.data.byteOffset,
    slot.data.byteLength,
  ).getInt32(SOCKET_SLOT_RECV_BUF_IDX_OFFSET, true);
}
