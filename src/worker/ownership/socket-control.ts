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
