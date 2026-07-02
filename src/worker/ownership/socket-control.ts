import type { RuntimeNetworkBridge } from "../../bridge/network.js";
import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerSocketStateSnapshot,
} from "../../messages.js";
import { OFD_TYPE_OFFSET } from "../../layout/ofd.js";
import type { KernelOwnershipHandle, ProcessHandle } from "../types.js";

export function kernelWorkerOwnsSocketState(handle: KernelOwnershipHandle): boolean {
  return !!handle.kernelWorkerRequest && handle.pid > 0;
}

export function runtimeNetworkFds(handle: ProcessHandle): Set<number> {
  return new Set([
    ...(handle.networkConnections?.keys() ?? []),
    ...(handle.networkPendingConnectFds ?? []),
    ...(handle.networkClosedFds ?? []),
    ...(handle.networkClosedSocketSlots?.keys() ?? []),
    ...(handle.networkSocketSlotWatermarks?.keys() ?? []),
    ...(handle.networkRecvPayloadWatermarks?.keys() ?? []),
  ]);
}

export function socketOfdForFd(handle: ProcessHandle, vmPtr: number, fd: number): number {
  return handle.kernel.process_fd_ofd_id?.(vmPtr, fd) ?? 0;
}

function supervisorHeldSocketOfdIds(handle: ProcessHandle, vmPtr: number): number[] {
  const query = handle.kernel.supervisor_process_socket_held_ofd;
  if (!query || vmPtr === 0) {
    return [];
  }
  const maxFds = handle.kernel.abi_max_fds();
  const ofdIds: number[] = [];
  for (let index = 0; index < maxFds; index += 1) {
    const ofdId = query(vmPtr, index);
    if (ofdId === 0) {
      break;
    }
    if (ofdId > 0 && ofdId < maxFds) {
      ofdIds.push(ofdId);
    }
  }
  return ofdIds;
}

export function networkConnectionOfds(handle: ProcessHandle): Map<number, number> {
  handle.networkConnectionOfds ??= new Map();
  return handle.networkConnectionOfds;
}

type SocketStatusDebug = (handle: ProcessHandle, event: string) => void;

export interface PreservedNetworkSocketSlot {
  offset: number;
  data: Uint8Array;
  socketMask: number;
}

export function socketTablePtrs(handle: ProcessHandle): number[] {
  const ptrs = [handle.kernelState.socketTablePtr];
  const activePtr = handle.kernel.kernel_socket_table_ptr?.();
  if (activePtr !== undefined && !ptrs.includes(activePtr)) {
    ptrs.push(activePtr);
  }
  return ptrs;
}

function activeSocketTablePtr(handle: ProcessHandle): number {
  return handle.kernel.kernel_socket_table_ptr?.() ?? handle.kernelState.socketTablePtr;
}

export function restoreNetworkOfdSlotWatermarks(
  handle: ProcessHandle,
  appendStatusSocketDebug?: SocketStatusDebug,
): void {
  const watermarks = handle.networkOfdSlotWatermarks;
  if (!watermarks || watermarks.size === 0) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  for (const ofdId of networkConnectionOfds(handle).values()) {
    const slot = watermarks.get(ofdId);
    if (!slot || slot.length !== slotBytes || ofdId <= 0 || ofdId >= maxFds) {
      continue;
    }
    if (handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_socket()) {
      continue;
    }
    memory.set(slot, handle.kernelState.ofdTablePtr + ofdId * slotBytes);
    appendStatusSocketDebug?.(handle, `net-status-ofd-restore:${handle.pid}:ofd=${ofdId}`);
  }
}

function clearNetworkRecvWatermarks(
  handle: ProcessHandle,
  fd: number,
  appendStatusSocketDebug?: SocketStatusDebug,
): void {
  if (handle.networkRecvPayloadWatermarks?.has(fd)) {
    appendStatusSocketDebug?.(handle, `net-status-watermark-clear:${handle.pid}:fd=${fd}`);
  }
  handle.networkRecvHeadWatermarks.delete(fd);
  handle.networkRecvTailWatermarks.delete(fd);
  handle.networkRecvPayloadWatermarks ??= new Map();
  handle.networkRecvPayloadWatermarks.delete(fd);
  handle.networkSocketSlotWatermarks ??= new Map();
  handle.networkSocketSlotWatermarks.delete(fd);
}

function moveNetworkRecvWatermarks(
  handle: ProcessHandle,
  fromFd: number,
  toFd: number,
  appendStatusSocketDebug?: SocketStatusDebug,
): void {
  if (fromFd === toFd) {
    return;
  }
  const head = handle.networkRecvHeadWatermarks.get(fromFd);
  const tail = handle.networkRecvTailWatermarks.get(fromFd);
  handle.networkRecvPayloadWatermarks ??= new Map();
  const payload = handle.networkRecvPayloadWatermarks.get(fromFd);
  handle.networkSocketSlotWatermarks ??= new Map();
  const socketSlot = handle.networkSocketSlotWatermarks.get(fromFd);
  clearNetworkRecvWatermarks(handle, fromFd, appendStatusSocketDebug);
  if (head !== undefined) {
    handle.networkRecvHeadWatermarks.set(toFd, head);
  }
  if (tail !== undefined) {
    handle.networkRecvTailWatermarks.set(toFd, tail);
  }
  if (payload !== undefined) {
    handle.networkRecvPayloadWatermarks.set(toFd, payload);
  }
  if (socketSlot !== undefined) {
    handle.networkSocketSlotWatermarks.set(toFd, socketSlot);
  }
}

export function pruneStaleNetworkConnectionsFromFdEntries(
  handle: ProcessHandle,
  vmPtr: number,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  callbacks: {
    appendWorkerDebugLog: (handle: ProcessHandle, event: string) => void;
    failProcess: (handle: ProcessHandle, error: Error) => void;
    appendStatusSocketDebug?: SocketStatusDebug;
  },
): void {
  if (!fdEntries || vmPtr === 0 || handle.networkConnections.size === 0) {
    return;
  }
  const ofds = networkConnectionOfds(handle);
  const maxFds = handle.kernel.abi_max_fds();
  const findFdForOfd = (ofdId: number, preferredFd: number): number | null => {
    if (ofdId <= 0) {
      return null;
    }
    if (handle.kernel.process_fd_ofd_id(vmPtr, preferredFd) === ofdId) {
      return preferredFd;
    }
    for (let fd = 0; fd < maxFds; fd += 1) {
      if (fd !== preferredFd && handle.kernel.process_fd_ofd_id(vmPtr, fd) === ofdId) {
        return fd;
      }
    }
    return null;
  };
  for (const [fd, connection] of [...handle.networkConnections]) {
    const fdEntry = fdEntries.find((entry) => entry.fd === fd);
    const expectedOfd = ofds.get(fd);
    const currentOfd = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (expectedOfd === undefined) {
      if (currentOfd > 0) {
        ofds.set(fd, currentOfd);
        continue;
      }
    } else if (currentOfd === expectedOfd) {
      continue;
    } else {
      const replacementFd = findFdForOfd(expectedOfd, fd);
      if (replacementFd !== null) {
        if (replacementFd !== fd) {
          handle.networkConnections.delete(fd);
          handle.networkConnections.set(replacementFd, connection);
          ofds.delete(fd);
          ofds.set(replacementFd, expectedOfd);
          moveNetworkRecvWatermarks(handle, fd, replacementFd, callbacks.appendStatusSocketDebug);
          callbacks.appendWorkerDebugLog(handle, `net-status-rekey:${handle.pid}:fd=${fd}->${replacementFd}:ofd=${expectedOfd}`);
        }
        continue;
      }
      if (
        currentOfd > 0
        && handle.kernel.kernel_ofd_fd_type(currentOfd) === handle.kernel.abi_fd_type_socket()
        && !handle.networkPendingConnectFds.has(fd)
      ) {
        ofds.set(fd, currentOfd);
        callbacks.appendWorkerDebugLog(handle, `net-status-rekey:${handle.pid}:fd=${fd}:ofd=${expectedOfd}->${currentOfd}`);
        continue;
      }
    }
    if (
      !handle.networkClosedFds.has(fd)
      && !(fdEntry?.ofdId === 0 && currentOfd === 0)
    ) {
      callbacks.appendWorkerDebugLog(handle, `net-status-keep:${handle.pid}:fd=${fd}:ofd=${expectedOfd ?? "unknown"}->${currentOfd}`);
      continue;
    }
    handle.networkConnections.delete(fd);
    ofds.delete(fd);
    handle.networkPendingConnectFds.delete(fd);
    clearNetworkRecvWatermarks(handle, fd, callbacks.appendStatusSocketDebug);
    callbacks.appendWorkerDebugLog(handle, `net-status-close:${handle.pid}:fd=${fd}:ofd=${expectedOfd ?? "unknown"}->${currentOfd}`);
    void Promise.resolve(connection.close?.()).catch((error) => {
      callbacks.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    });
  }
}

export function isSocketOfd(handle: ProcessHandle, ofdId: number): boolean {
  if (ofdId <= 0) {
    return false;
  }
  const fdType = handle.kernel.kernel_ofd_fd_type?.(ofdId);
  const socketType = handle.kernel.abi_fd_type_socket?.();
  return fdType === undefined || socketType === undefined || fdType === socketType;
}

export function findSocketFdForOfd(
  handle: ProcessHandle,
  vmPtr: number,
  ofdId: number,
  preferredFd: number,
): number | null {
  if (isSocketOfd(handle, ofdId) && socketOfdForFd(handle, vmPtr, preferredFd) === ofdId) {
    return preferredFd;
  }
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  for (let fd = 0; fd < maxFds; fd += 1) {
    if (fd !== preferredFd && socketOfdForFd(handle, vmPtr, fd) === ofdId && isSocketOfd(handle, ofdId)) {
      return fd;
    }
  }
  return null;
}

export function restoreRuntimeNetworkSidecarForHandle(
  handle: ProcessHandle,
  processHandles: Map<number, ProcessHandle>,
  vmPtr: number,
): RuntimeNetworkBridge | undefined {
  const relatedHandles = [
    ...handle.kernelShare.members,
    ...processHandles.values(),
  ].filter((member) => member !== handle && member.runtimeId === handle.runtimeId);

  if (handle.networkBridge) {
    // Keep the existing bridge, but still restore fd sidecars below.
  } else {
    for (const member of relatedHandles) {
      if (member.networkBridge) {
        handle.networkBridge = member.networkBridge;
        break;
      }
    }
  }

  if (handle.networkBridge) {
    for (const member of relatedHandles) {
      for (const [sourceFd, connection] of member.networkConnections) {
        const sourceOfd = socketOfdForFd(member, member.vmPtr, sourceFd);
        if (!isSocketOfd(member, sourceOfd)) {
          continue;
        }
        const targetFd = findSocketFdForOfd(handle, vmPtr, sourceOfd, sourceFd);
        if (
          targetFd === null
          || handle.networkConnections.has(targetFd)
          || handle.networkClosedFds.has(targetFd)
        ) {
          continue;
        }
        handle.networkConnections.set(targetFd, connection);
        networkConnectionOfds(handle).set(targetFd, sourceOfd);
        handle.networkPendingConnectFds.delete(targetFd);
        handle.networkClosedSocketSlots.delete(targetFd);
      }
    }
  }
  return handle.networkBridge;
}

export function snapshotSocketStateForThread(handle: ProcessHandle): KernelWorkerSocketStateSnapshot | undefined {
  if (
    handle.kernelState.socketUsed === 0
    && handle.kernelState.netBufUsed === 0n
    && (handle.networkConnections?.size ?? 0) === 0
    && (handle.networkPendingConnectFds?.size ?? 0) === 0
    && (handle.networkClosedFds?.size ?? 0) === 0
    && (handle.networkClosedSocketSlots?.size ?? 0) === 0
    && (handle.networkSocketSlotWatermarks?.size ?? 0) === 0
    && (handle.networkRecvPayloadWatermarks?.size ?? 0) === 0
  ) {
    return undefined;
  }
  const socketTableBytes = handle.kernel.kernel_socket_table_bytes?.() ?? 0;
  const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes?.() ?? 0;
  if (socketTableBytes <= 0 || netBufPoolBytes <= 0) {
    return undefined;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  return {
    socketTable: memory.slice(
      handle.kernelState.socketTablePtr,
      handle.kernelState.socketTablePtr + socketTableBytes,
    ),
    netBufPool: memory.slice(
      handle.kernelState.netBufBase,
      handle.kernelState.netBufBase + netBufPoolBytes,
    ),
    socketUsed: handle.kernelState.socketUsed | handle.kernel.kernel_socket_used(),
    netBufUsed: handle.kernelState.netBufUsed | handle.kernel.kernel_net_buf_used(),
  };
}

export function collectRuntimeNetworkSocketOfdIds(handles: Iterable<ProcessHandle>): Set<number> {
  const ofdIds = new Set<number>();
  for (const handle of handles) {
    const maxFds = handle.kernel.abi_max_fds();
    const addOfd = (ofdId: number): void => {
      if (
        ofdId > 0
        && ofdId < maxFds
        && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_socket()
      ) {
        ofdIds.add(ofdId);
      }
    };
    for (const ofdId of handle.networkConnectionOfds?.values() ?? []) {
      addOfd(ofdId);
    }
    const networkFds = runtimeNetworkFds(handle);
    for (const fd of networkFds) {
      if (fd >= 0 && fd < maxFds && handle.vmPtr !== 0) {
        addOfd(handle.kernel.process_fd_ofd_id(handle.vmPtr, fd));
      }
    }
    for (const [ofdId, slot] of handle.networkOfdSlotWatermarks ?? []) {
      if (ofdId <= 0 || ofdId >= maxFds) {
        continue;
      }
      const view = new DataView(slot.buffer, slot.byteOffset, slot.byteLength);
      if (view.getInt32(OFD_TYPE_OFFSET, true) === handle.kernel.abi_fd_type_socket()) {
        ofdIds.add(ofdId);
      }
    }
  }
  return ofdIds;
}

export function hasKernelSocketFdState(handle: ProcessHandle): boolean {
  if (handle.vmPtr === 0) {
    return false;
  }
  const supervisorSocketOfdIds = supervisorHeldSocketOfdIds(handle, handle.vmPtr);
  if (supervisorSocketOfdIds.length > 0) {
    return true;
  }
  const maxFds = handle.kernel.abi_max_fds();
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (
      ofdId > 0
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_socket()
    ) {
      return true;
    }
  }
  return false;
}

export function hasRuntimeNetworkSocketState(handle: ProcessHandle): boolean {
  return (handle.networkConnections?.size ?? 0) > 0
    || (handle.networkPendingConnectFds?.size ?? 0) > 0
    || handle.networkClosedFds.size > 0
    || handle.networkClosedSocketSlots.size > 0
    || (handle.networkSocketSlotWatermarks?.size ?? 0) > 0
    || (handle.networkRecvPayloadWatermarks?.size ?? 0) > 0
    || hasKernelSocketFdState(handle);
}

export function preserveNetworkSocketSlots(
  handle: ProcessHandle,
  vmPtr: number,
  socketTableBytes: number,
): PreservedNetworkSocketSlot[] {
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const socketSlotBytes = maxSockets > 0 ? socketTableBytes / maxSockets : 0;
  if (!Number.isInteger(socketSlotBytes) || socketSlotBytes <= 0) {
    return [];
  }
  const preserved: PreservedNetworkSocketSlot[] = [];
  const preservedOffsets = new Set<number>();
  const memory = new Uint8Array(handle.memory.buffer);
  const preserveSocketIdx = (socketIdx: number): void => {
    if (socketIdx < 0 || socketIdx >= maxSockets) {
      return;
    }
    const offset = socketIdx * socketSlotBytes;
    if (preservedOffsets.has(offset)) {
      return;
    }
    const absolute = activeSocketTablePtr(handle) + offset;
    preservedOffsets.add(offset);
    preserved.push({
      offset,
      data: memory.slice(absolute, absolute + socketSlotBytes),
      socketMask: 1 << socketIdx,
    });
  };
  for (const fd of runtimeNetworkFds(handle)) {
    const closedSlot = handle.networkClosedSocketSlots.get(fd);
    if (closedSlot) {
      if (!preservedOffsets.has(closedSlot.offset)) {
        preservedOffsets.add(closedSlot.offset);
        preserved.push({
          offset: closedSlot.offset,
          data: closedSlot.data,
          socketMask: 1 << Math.floor(closedSlot.offset / socketSlotBytes),
        });
      }
      continue;
    }
    handle.networkSocketSlotWatermarks ??= new Map();
    const watermarkedSlot = handle.networkSocketSlotWatermarks.get(fd);
    if (watermarkedSlot && !preservedOffsets.has(watermarkedSlot.offset)) {
      preservedOffsets.add(watermarkedSlot.offset);
      preserved.push({
        offset: watermarkedSlot.offset,
        data: watermarkedSlot.data,
        socketMask: 1 << Math.floor(watermarkedSlot.offset / socketSlotBytes),
      });
    }
    const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd);
    if (socketIdx === undefined || socketIdx < 0 || socketIdx >= maxSockets) {
      handle.networkClosedFds.delete(fd);
      continue;
    }
    preserveSocketIdx(socketIdx);
    preserveSocketIdx(handle.kernel.thread_socket_peer_idx?.(socketIdx) ?? -1);
  }
  return preserved;
}

export function restoreNetworkSocketSlots(
  memory: Uint8Array,
  socketTablePtrs: number[],
  preserved: PreservedNetworkSocketSlot[],
): void {
  for (const slot of preserved) {
    for (const socketTablePtr of socketTablePtrs) {
      memory.set(slot.data, socketTablePtr + slot.offset);
    }
  }
}
