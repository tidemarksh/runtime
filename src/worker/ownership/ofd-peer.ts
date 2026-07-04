import type { ProcessHandle } from "../types.js";
import {
  collectProcessHeldOfds,
  forkSharedHeldOfdTrackAction,
  trackForkSharedOfds,
} from "./ofd-table.js";
import {
  hasPipePeerInOtherKernelShareMember,
  isProcessHostStdioOfd,
  isProcessHostStdioPipeOfd,
} from "./pipe-control.js";

function forkSharedOfdIdentityKey(handle: ProcessHandle, ofdId: number): string | null {
  const readIdentityKey = handle.kernel.supervisor_fork_shared_ofd_identity_key;
  if (!readIdentityKey) {
    throw new Error("kernel supervisor ABI missing supervisor_fork_shared_ofd_identity_key");
  }
  const key = BigInt(readIdentityKey.call(handle.kernel, ofdId));
  return key === 0n ? null : key.toString();
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
  const publishAction = handle.kernel.supervisor_fork_shared_ofd_publish_action;
  const compatibleFlag = handle.kernel.supervisor_fork_shared_peer_flag_compatible;
  const incompatibleFlag = handle.kernel.supervisor_fork_shared_peer_flag_incompatible;
  const pipePeerFlag = handle.kernel.supervisor_fork_shared_peer_flag_pipe_peer;
  if (!publishAction || !compatibleFlag || !incompatibleFlag || !pipePeerFlag) {
    throw new Error("kernel supervisor ABI missing fork-shared OFD publish policy exports");
  }
  let peerFlags = 0;
  if (compatiblePeer) {
    peerFlags |= compatibleFlag.call(handle.kernel);
  }
  if (incompatiblePeer) {
    peerFlags |= incompatibleFlag.call(handle.kernel);
  }
  if (hasPipePeerInOtherKernelShareMember(handle, ofdId)) {
    peerFlags |= pipePeerFlag.call(handle.kernel);
  }
  return publishAction.call(handle.kernel, ofdId, peerFlags) !== 0;
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
  for (const ofdId of ofdIds) {
    if (processHoldsRegularFileOfd(target, target.vmPtr, ofdId)) {
      return true;
    }
  }
  return false;
}

function processHoldsRegularFileOfd(handle: ProcessHandle, vmPtr: number, ofdId: number): boolean {
  if (vmPtr === 0 || ofdId === 0) {
    return false;
  }
  const holdsRegularFileOfd = handle.kernel.supervisor_process_holds_regular_file_ofd;
  if (!holdsRegularFileOfd) {
    throw new Error("kernel supervisor ABI missing supervisor_process_holds_regular_file_ofd");
  }
  return holdsRegularFileOfd.call(handle.kernel, vmPtr, ofdId) !== 0;
}

function processFdEntriesIncludeOfd(handle: ProcessHandle, vmPtr: number, ofdId: number): boolean {
  if (vmPtr === 0 || ofdId === 0) {
    return false;
  }
  const maxFds = handle.kernel.abi_max_fds();
  for (let fd = 0; fd < maxFds; fd += 1) {
    if (handle.kernel.process_fd_ofd_id(vmPtr, fd) === ofdId) {
      return true;
    }
  }
  return false;
}

function sourceRegularFileForkSharedPeerFacts(
  handle: ProcessHandle,
  vmPtr: number,
): { sourceHoldsRegularFile: boolean; regularFileHasPeer: boolean } {
  if (vmPtr === 0) {
    return { sourceHoldsRegularFile: false, regularFileHasPeer: false };
  }
  let sourceHoldsRegularFile = false;
  for (const ofdId of handle.kernelShare.sharedOfdIds) {
    if (
      !processFdEntriesIncludeOfd(handle, vmPtr, ofdId)
      || !processHoldsRegularFileOfd(handle, vmPtr, ofdId)
    ) {
      continue;
    }
    sourceHoldsRegularFile = true;
    for (const peer of handle.kernelShare.members) {
      if (peer === handle || peer.vmPtr === 0) {
        continue;
      }
      if (
        processFdEntriesIncludeOfd(peer, peer.vmPtr, ofdId)
        && processHoldsRegularFileOfd(peer, peer.vmPtr, ofdId)
      ) {
        return { sourceHoldsRegularFile: true, regularFileHasPeer: true };
      }
    }
  }
  return { sourceHoldsRegularFile, regularFileHasPeer: false };
}

export function shouldRequestForkSharedStateForEffect(
  handle: ProcessHandle,
  vmPtr: number,
  hasEffect: boolean,
  hasTrackedState: boolean,
): boolean {
  const decide = handle.kernel.supervisor_fork_shared_effect_sync_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_fork_shared_effect_sync_action");
  }
  const { sourceHoldsRegularFile, regularFileHasPeer } = sourceRegularFileForkSharedPeerFacts(handle, vmPtr);
  return decide.call(
    handle.kernel,
    hasEffect ? 1 : 0,
    hasTrackedState ? 1 : 0,
    sourceHoldsRegularFile ? 1 : 0,
    regularFileHasPeer ? 1 : 0,
  ) !== 0;
}

export function collectTrackableForkSharedOfds(handle: ProcessHandle): Set<number> {
  const stdioOfdIds = new Set<number>();
  const stdioFdOfdIds = new Set<number>();
  for (const [fd, stdioPipeSlot] of [
    [0, handle.stdinPipe],
    [1, handle.stdoutPipe],
    [2, handle.stderrPipe],
  ] as const) {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId !== 0) {
      stdioFdOfdIds.add(ofdId);
    }
    if (
      ofdId !== 0
      && stdioPipeSlot > 0
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
      && handle.kernel.kernel_ofd_ino(ofdId) === stdioPipeSlot
    ) {
      stdioOfdIds.add(ofdId);
    }
  }
  const tracked = new Set<number>();
  for (const ofdId of handle.kernelShare.sharedOfdIds) {
    if (
      ofdId > 0
      && ofdId < handle.kernel.abi_max_fds()
    ) {
      tracked.add(ofdId);
    }
  }
  for (const ofdId of collectProcessHeldOfds(handle)) {
    if (forkSharedHeldOfdTrackAction(handle, ofdId, {
      shared: handle.kernelShare.sharedOfdIds.has(ofdId),
      hostStdioPipe: stdioOfdIds.has(ofdId),
      stdioFd: stdioFdOfdIds.has(ofdId),
    })) {
      tracked.add(ofdId);
    }
  }
  return tracked;
}

export function collectTracedForkSharedOfdIds(
  handle: ProcessHandle,
  sourceVmPtr: number,
): number[] {
  const len = handle.kernel.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return [];
  }
  const traced = new Set<number>();
  const sampleToPid = (sample: bigint | number | undefined): number => {
    if (sample === undefined) {
      return 0;
    }
    return Number(BigInt.asIntN(32, BigInt(sample) & 0xffffffffn));
  };
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
    if (traceVmPtr !== 0 && sourceVmPtr !== 0 && traceVmPtr !== sourceVmPtr) {
      continue;
    }
    const tracePid = sampleToPid(handle.kernel.debug_fd_syscall_trace_pid_sample0?.(index));
    if (tracePid > 0 && tracePid !== handle.pid) {
      continue;
    }
    const arg0OfdId = handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0;
    const arg2OfdId = handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? 0;
    if (arg0OfdId > 0) {
      traced.add(arg0OfdId);
    }
    if (arg2OfdId > 0) {
      traced.add(arg2OfdId);
    }
  }
  return [...traced];
}

export function trackHeldForkSharedOfds(handle: ProcessHandle, vmPtr: number): void {
  trackForkSharedOfds(handle, vmPtr, false);
  for (const ofdId of collectProcessHeldOfds(handle)) {
    if (forkSharedHeldOfdTrackAction(handle, ofdId, {
      shared: handle.kernelShare.sharedOfdIds.has(ofdId),
      hostStdioPipe: isProcessHostStdioOfd(handle, vmPtr, ofdId)
        || isProcessHostStdioPipeOfd(handle, ofdId),
      stdioFd: false,
    })) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
    }
  }
}

export function isRegularFileOfd(handle: ProcessHandle, ofdId: number): boolean {
  return ofdId > 0
    && ofdId < handle.kernel.abi_max_fds()
    && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_file();
}
