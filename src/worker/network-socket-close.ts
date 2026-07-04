import type { ProcessHandle } from "./types.js";
import { requireKernelSocketCount } from "../kernel-socket-layout.js";
import {
  SOCKET_SLOT_RECV_BUF_IDX_OFFSET,
  SOCKET_SLOT_SHUTDOWN_MASK_OFFSET,
} from "../layout/socket.js";
import { kernelSocketIndexForFd } from "./ownership/socket-control.js";

function socketSlotBytes(handle: ProcessHandle): number {
  const maxSockets = requireKernelSocketCount(handle.kernel);
  const socketTableBytes = handle.kernel.kernel_socket_table_bytes();
  const bytes = maxSockets > 0 ? socketTableBytes / maxSockets : 0;
  return Number.isInteger(bytes) && bytes > SOCKET_SLOT_SHUTDOWN_MASK_OFFSET + 4 ? bytes : 0;
}

function socketTablePtrs(handle: ProcessHandle): number[] {
  const ptrs = [handle.kernelState.socketTablePtr];
  const activePtr = handle.kernel.kernel_socket_table_ptr?.();
  if (activePtr !== undefined && !ptrs.includes(activePtr)) {
    ptrs.push(activePtr);
  }
  return ptrs;
}

function requireSupervisorSocketForceShutdown(handle: ProcessHandle) {
  const forceShutdown = handle.kernel.supervisor_socket_force_shutdown;
  if (typeof forceShutdown !== "function") {
    throw new Error("kernel supervisor ABI missing supervisor_socket_force_shutdown");
  }
  return forceShutdown;
}

export function forceNetworkClosedFd(handle: ProcessHandle, vmPtr: number, fd: number): void {
  requireSupervisorSocketForceShutdown(handle)(vmPtr, fd);
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
  const maxSockets = requireKernelSocketCount(handle.kernel);
  const bytes = socketSlotBytes(handle);
  const socketIdx = kernelSocketIndexForFd(handle, vmPtr, fd);
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
