import type { KernelOwnershipHandle, ProcessHandle } from "../types.js";
import { collectProcessHeldOfds } from "./ofd-table.js";

const PIPE_HEADER_BYTES = 32;
const PIPE_READ_OPEN_OFFSET = 8;
const PIPE_WRITE_OPEN_OFFSET = 12;

export function kernelWorkerOwnsPipeState(handle: KernelOwnershipHandle): boolean {
  return !!handle.kernelWorkerRequest && handle.pid > 0;
}

export function hostStdioPipeSlotsForFd(handle: ProcessHandle, fd: 0 | 1 | 2): Set<number> {
  const owner = handle.stdioOwner;
  const slotsByFd = fd === 0
    ? [
        handle.stdinPipe,
        handle.hostStdio.stdinPipe,
        owner?.stdinPipe ?? 0,
        owner?.hostStdio.stdinPipe ?? 0,
      ]
    : fd === 1
      ? [
          handle.stdoutPipe,
          handle.hostStdio.stdoutPipe,
          owner?.stdoutPipe ?? 0,
          owner?.hostStdio.stdoutPipe ?? 0,
        ]
      : [
          handle.stderrPipe,
          handle.hostStdio.stderrPipe,
          owner?.stderrPipe ?? 0,
          owner?.hostStdio.stderrPipe ?? 0,
        ];
  return new Set(slotsByFd.filter((slot) => slot > 0));
}

export function hostStdioPipeSlots(handle: ProcessHandle): Set<number> {
  return new Set([
    ...hostStdioPipeSlotsForFd(handle, 0),
    ...hostStdioPipeSlotsForFd(handle, 1),
    ...hostStdioPipeSlotsForFd(handle, 2),
  ]);
}

export function isProcessHostStdioOfd(handle: ProcessHandle, vmPtr: number, ofdId: number): boolean {
  for (const fd of [0, 1, 2] as const) {
    const fdOfdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    const stdioPipeSlots = hostStdioPipeSlotsForFd(handle, fd);
    if (
      fdOfdId === ofdId
      && stdioPipeSlots.size > 0
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
      && stdioPipeSlots.has(handle.kernel.kernel_ofd_ino(ofdId))
    ) {
      return true;
    }
  }
  return false;
}

export function isProcessHostStdioPipeOfd(handle: ProcessHandle, ofdId: number): boolean {
  if (
    ofdId <= 0
    || ofdId >= handle.kernel.abi_max_fds()
    || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
  ) {
    return false;
  }
  const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
  return pipeSlot > 0 && hostStdioPipeSlots(handle).has(pipeSlot);
}

export function expandPipePeerOfdIds(handle: ProcessHandle, ofdIds: Iterable<number>): number[] {
  const pipeSlots = new Set<number>();
  for (const ofdId of ofdIds) {
    if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
      continue;
    }
    if (handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()) {
      const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
      if (pipeSlot > 0) {
        pipeSlots.add(pipeSlot);
      }
    }
  }
  if (pipeSlots.size === 0) {
    return [];
  }

  const peers = new Set<number>();
  for (const member of handle.kernelShare.members) {
    for (const ofdId of collectProcessHeldOfds(member)) {
      if (
        member.kernel.kernel_ofd_fd_type(ofdId) === member.kernel.abi_fd_type_pipe()
        && pipeSlots.has(member.kernel.kernel_ofd_ino(ofdId))
      ) {
        peers.add(ofdId);
      }
    }
  }
  return [...peers];
}

export function pipeSlotForOfd(handle: ProcessHandle, ofdId: number): number {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return 0;
  }
  if (handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()) {
    return 0;
  }
  return handle.kernel.kernel_ofd_ino(ofdId);
}

export function hasPipePeerInOtherKernelShareMember(handle: ProcessHandle, ofdId: number): boolean {
  const pipeSlot = pipeSlotForOfd(handle, ofdId);
  if (pipeSlot <= 0) {
    return false;
  }
  for (const member of handle.kernelShare.members) {
    if (member === handle) {
      continue;
    }
    for (const peerOfdId of collectProcessHeldOfds(member)) {
      if (
        member.kernel.kernel_ofd_fd_type(peerOfdId) === member.kernel.abi_fd_type_pipe()
        && member.kernel.kernel_ofd_ino(peerOfdId) === pipeSlot
      ) {
        return true;
      }
    }
  }
  return false;
}

export function liveProcessPipeUsedBits(handle: ProcessHandle): bigint {
  if (handle.vmPtr === 0) {
    return 0n;
  }
  let pipeUsed = 0n;
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  const maxPipes = handle.kernel.abi_max_pipes?.() ?? 64;
  if (
    maxFds > 0
    && typeof handle.kernel.process_fd_ofd_id === "function"
    && typeof handle.kernel.kernel_ofd_fd_type === "function"
    && typeof handle.kernel.abi_fd_type_pipe === "function"
    && typeof handle.kernel.kernel_ofd_ino === "function"
  ) {
    for (let fd = 0; fd < maxFds; fd += 1) {
      const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
      if (
        ofdId <= 0
        || ofdId >= maxFds
        || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
      ) {
        continue;
      }
      const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
      if (pipeSlot > 0 && pipeSlot < maxPipes) {
        pipeUsed |= 1n << BigInt(pipeSlot);
      }
    }
  }
  return pipeUsed;
}

export function composeHandlePipeUsed(handle: ProcessHandle, sharedPipeUsed: bigint): bigint {
  let pipeUsed = sharedPipeUsed | 1n;
  for (const pipeSlot of hostStdioPipeSlots(handle)) {
    if (pipeSlot > 0) {
      pipeUsed |= 1n << BigInt(pipeSlot);
    }
  }
  return pipeUsed;
}

export function preserveHostStdioPipeControlForStep(handle: ProcessHandle, vmPtr: number): void {
  const maxPipes = handle.kernel.abi_max_pipes?.() ?? 0;
  const pipeRegionBytes = handle.kernel.kernel_pipe_region_bytes?.() ?? 0;
  const slotBytes = maxPipes > 0 ? pipeRegionBytes / maxPipes : 0;
  if (!Number.isInteger(slotBytes) || slotBytes <= PIPE_HEADER_BYTES) {
    return;
  }
  const view = new DataView(handle.memory.buffer);
  const preserve = (pipeSlot: number, readOpenMin: number, writeOpenMin: number): void => {
    if (pipeSlot <= 0 || pipeSlot >= maxPipes) {
      return;
    }
    const offset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
    if (readOpenMin > 0) {
      const readOpen = view.getInt32(offset + PIPE_READ_OPEN_OFFSET, true);
      if (readOpen < readOpenMin) {
        view.setInt32(offset + PIPE_READ_OPEN_OFFSET, readOpenMin, true);
      }
    }
    if (writeOpenMin > 0) {
      const writeOpen = view.getInt32(offset + PIPE_WRITE_OPEN_OFFSET, true);
      if (writeOpen < writeOpenMin) {
        view.setInt32(offset + PIPE_WRITE_OPEN_OFFSET, writeOpenMin, true);
      }
    }
  };

  for (const [fd, readOpenMin, writeOpenMin] of [
    [0, 0, 1],
    [1, 1, 0],
    [2, 1, 0],
  ] as const) {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId <= 0
      || ofdId >= handle.kernel.abi_max_fds()
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      continue;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    if (hostStdioPipeSlotsForFd(handle, fd).has(pipeSlot)) {
      preserve(pipeSlot, readOpenMin, writeOpenMin);
    }
  }
}
