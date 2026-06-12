import type { ProcessHandle } from "../types.js";
import { PIPE_WRITE_OPEN_OFFSET } from "../../layout/pipe.js";
import { pipeSlotWriteOpen as pipeControlSlotWriteOpen } from "../ownership/pipe-control.js";

export interface KernelRpcPipeDeps {
  processHandles: Map<number, ProcessHandle>;
}

function blockedPipeReaderOfdId(handle: ProcessHandle, vmPtr: number): number {
  const pinnedOfdId = handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0;
  if (pinnedOfdId > 0) {
    return pinnedOfdId;
  }

  const candidateFds: number[] = [];
  const packedPollPipe = handle.kernel.thread_poll_wait_first_pipe_revents?.(vmPtr);
  if (packedPollPipe !== undefined && packedPollPipe !== 0) {
    candidateFds.push(packedPollPipe & 0xff);
  }
  const blockedFd = handle.kernel.thread_blocked_fd(vmPtr);
  if (blockedFd >= 0) {
    candidateFds.push(blockedFd);
  }
  const syscallFd = Number(handle.kernel.thread_a0(vmPtr));
  if (syscallFd >= 0) {
    candidateFds.push(syscallFd);
  }

  const seen = new Set<number>();
  for (const fd of candidateFds) {
    if (seen.has(fd)) {
      continue;
    }
    seen.add(fd);
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId > 0
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
      && handle.kernel.kernel_ofd_host_fd(ofdId) >= 0
    ) {
      return ofdId;
    }
  }
  return 0;
}

function currentPipeSlotWriteOpen(handle: ProcessHandle, pipeSlot: number): number | null {
  const maxPipes = handle.kernel.abi_max_pipes?.();
  const pipeRegionBytes = handle.kernel.kernel_pipe_region_bytes?.();
  const pipeBase = handle.kernel.kernel_pipe_base?.();
  if (
    maxPipes === undefined
    || pipeRegionBytes === undefined
    || pipeBase === undefined
  ) {
    return pipeControlSlotWriteOpen(handle, pipeSlot);
  }
  const slotBytes = pipeRegionBytes / maxPipes;
  if (
    pipeSlot <= 0
    || pipeSlot >= maxPipes
    || !Number.isInteger(slotBytes)
    || slotBytes <= PIPE_WRITE_OPEN_OFFSET + 4
  ) {
    return pipeControlSlotWriteOpen(handle, pipeSlot);
  }
  const offset = pipeBase + pipeSlot * slotBytes;
  const view = new DataView(handle.memory.buffer);
  if (offset < 0 || offset + PIPE_WRITE_OPEN_OFFSET + 4 > view.byteLength) {
    return pipeControlSlotWriteOpen(handle, pipeSlot);
  }
  return view.getInt32(offset + PIPE_WRITE_OPEN_OFFSET, true);
}

export function activeChildMayHoldBlockedPipeWriter(
  deps: KernelRpcPipeDeps,
  handle: ProcessHandle,
  vmPtr: number,
): boolean {
  const blockedOfdId = blockedPipeReaderOfdId(handle, vmPtr);
  if (
    blockedOfdId <= 0
    || handle.kernel.kernel_ofd_fd_type(blockedOfdId) !== handle.kernel.abi_fd_type_pipe()
    || handle.kernel.kernel_ofd_host_fd(blockedOfdId) < 0
  ) {
    return false;
  }
  const pipeSlot = handle.kernel.kernel_ofd_ino(blockedOfdId);
  if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
    return false;
  }
  const canonicalWriteOpen = currentPipeSlotWriteOpen(handle, pipeSlot);
  if (canonicalWriteOpen !== null && canonicalWriteOpen <= 0) {
    return false;
  }
  const processFdTableMayHoldPipeWriter = (candidate: ProcessHandle): boolean => {
    if (candidate.vmPtr === 0) {
      return false;
    }
    const maxFds = candidate.kernel.abi_max_fds();
    for (let fd = 0; fd < maxFds; fd += 1) {
      const ofdId = candidate.kernel.process_fd_ofd_id(candidate.vmPtr, fd);
      if (
        ofdId > 0
        && ofdId < maxFds
        && candidate.kernel.kernel_ofd_fd_type(ofdId) === candidate.kernel.abi_fd_type_pipe()
        && candidate.kernel.kernel_ofd_ino(ofdId) === pipeSlot
        && candidate.kernel.kernel_ofd_host_fd(ofdId) < 0
      ) {
        return true;
      }
    }
    return false;
  };
  for (const candidate of deps.processHandles.values()) {
    const ownsVisibleOutputForHandle = candidate.stdioOwner === handle
      || candidate.stdioOwner?.pid === handle.pid;
    if (
      candidate.exitInProgress
      || deps.processHandles.get(candidate.pid) !== candidate
      || candidate.workers.size === 0
      || (candidate.parentPid !== handle.pid && !ownsVisibleOutputForHandle)
    ) {
      continue;
    }
    if (processFdTableMayHoldPipeWriter(candidate)) {
      return true;
    }
    for (const ofdId of candidate.kernelShare.sharedOfdIds) {
      if (
        ofdId > 0
        && ofdId < candidate.kernel.abi_max_fds()
        && candidate.kernel.kernel_ofd_fd_type(ofdId) === candidate.kernel.abi_fd_type_pipe()
        && candidate.kernel.kernel_ofd_ino(ofdId) === pipeSlot
        && candidate.kernel.kernel_ofd_host_fd(ofdId) < 0
      ) {
        return true;
      }
    }
  }
  return false;
}
