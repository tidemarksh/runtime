import type { RuntimeNetworkBridge } from "../../network.js";
import type { KernelOwnershipHandle, ProcessHandle } from "../types.js";

const OFD_TYPE_OFFSET = 0;

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

export function networkConnectionOfds(handle: ProcessHandle): Map<number, number> {
  handle.networkConnectionOfds ??= new Map();
  return handle.networkConnectionOfds;
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
    if (handle.vmPtr !== 0) {
      for (let fd = 0; fd < maxFds; fd += 1) {
        addOfd(handle.kernel.process_fd_ofd_id(handle.vmPtr, fd));
      }
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
