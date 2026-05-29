import type { ProcessHandle } from "../types.js";
import { collectProcessHeldOfds } from "./ofd-table.js";
import { hasPipePeerInOtherKernelShareMember } from "./pipe-control.js";

function forkSharedOfdIdentityKey(handle: ProcessHandle, ofdId: number): string | null {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return null;
  }
  const type = handle.kernel.kernel_ofd_fd_type(ofdId);
  if (type <= 0) {
    return null;
  }
  const ino = handle.kernel.kernel_ofd_ino(ofdId);
  const hostFd = handle.kernel.kernel_ofd_host_fd(ofdId);
  if (type === handle.kernel.abi_fd_type_pipe()) {
    const endpoint = hostFd >= 0 ? "read" : "write";
    return `${type}:${ino}:${endpoint}`;
  }
  if (type === handle.kernel.abi_fd_type_socket?.()) {
    return `${type}:${ino}:${hostFd}`;
  }
  return `${type}:${ino}`;
}

export function sourceForkSharedOfdPeerState(
  handle: ProcessHandle,
  ofdId: number,
): { compatiblePeer: boolean; incompatiblePeer: boolean } {
  const sourceKey = forkSharedOfdIdentityKey(handle, ofdId);
  if (sourceKey === null) {
    return { compatiblePeer: false, incompatiblePeer: true };
  }
  let compatiblePeer = false;
  let incompatiblePeer = false;
  for (const member of handle.kernelShare.members) {
    if (member === handle || !collectProcessHeldOfds(member).has(ofdId)) {
      continue;
    }
    const peerKey = forkSharedOfdIdentityKey(member, ofdId);
    if (peerKey === sourceKey) {
      compatiblePeer = true;
    } else {
      incompatiblePeer = true;
    }
  }
  return { compatiblePeer, incompatiblePeer };
}

export function sourceOfdCanPublishForkSharedState(handle: ProcessHandle, ofdId: number): boolean {
  if (!handle.kernelShare.sharedOfdIds.has(ofdId)) {
    return false;
  }
  const { compatiblePeer, incompatiblePeer } = sourceForkSharedOfdPeerState(handle, ofdId);
  if (incompatiblePeer) {
    return false;
  }
  const fdType = handle.kernel.kernel_ofd_fd_type(ofdId);
  const fileType = handle.kernel.abi_fd_type_file?.();
  if (fileType !== undefined && fdType === fileType) {
    return compatiblePeer;
  }
  if (fdType === handle.kernel.abi_fd_type_pipe()) {
    return compatiblePeer || hasPipePeerInOtherKernelShareMember(handle, ofdId);
  }
  return true;
}

export function targetHasIncompatibleForkSharedOfd(
  source: ProcessHandle,
  target: ProcessHandle,
  ofdIds: Iterable<number>,
): boolean {
  if (target.vmPtr === 0) {
    return false;
  }
  const trackedOfdIds = new Set(ofdIds);
  if (trackedOfdIds.size === 0) {
    return false;
  }
  const maxFds = target.kernel.abi_max_fds();
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = target.kernel.process_fd_ofd_id(target.vmPtr, fd);
    if (
      ofdId <= 0
      || ofdId >= maxFds
      || !trackedOfdIds.has(ofdId)
      || !source.kernelShare.sharedOfdIds.has(ofdId)
    ) {
      continue;
    }
    const sourceKey = forkSharedOfdIdentityKey(source, ofdId);
    const targetKey = forkSharedOfdIdentityKey(target, ofdId);
    if (sourceKey !== null && targetKey !== null && sourceKey !== targetKey) {
      return true;
    }
  }
  return false;
}

export function targetHoldsSyncedRegularFileOfd(target: ProcessHandle, ofdIds: ReadonlySet<number>): boolean {
  if (target.vmPtr === 0 || ofdIds.size === 0) {
    return false;
  }
  const fileType = target.kernel.abi_fd_type_file();
  for (const ofdId of collectProcessHeldOfds(target)) {
    if (
      ofdIds.has(ofdId)
      && target.kernel.kernel_ofd_fd_type(ofdId) === fileType
    ) {
      return true;
    }
  }
  return false;
}

export function isRegularFileOfd(handle: ProcessHandle, ofdId: number): boolean {
  return ofdId > 0
    && ofdId < handle.kernel.abi_max_fds()
    && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_file();
}
