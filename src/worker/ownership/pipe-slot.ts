import type { KernelExports } from "../../abi.js";
import type { ProcessHandle } from "../types.js";

function supervisorMaskToBigInt(mask: bigint | number): bigint {
  return typeof mask === "bigint" ? mask : BigInt(mask);
}

function requireSupervisorOfdPipeMask(
  kernel: KernelExports,
): (ofdId: number) => bigint | number {
  const readMask = kernel.supervisor_ofd_pipe_slot_mask;
  if (!readMask) {
    throw new Error("kernel supervisor ABI missing supervisor_ofd_pipe_slot_mask");
  }
  return readMask.bind(kernel);
}

function requireSupervisorOfdPipeMode(
  kernel: KernelExports,
): (ofdId: number, pipeSlot: number) => number {
  const readMode = kernel.supervisor_ofd_pipe_slot_mode;
  if (!readMode) {
    throw new Error("kernel supervisor ABI missing supervisor_ofd_pipe_slot_mode");
  }
  return readMode.bind(kernel);
}

function requireSupervisorOfdPipeGenerationSlot(
  kernel: KernelExports,
): (ofdId: number) => number {
  const readGenerationSlot = kernel.supervisor_ofd_pipe_generation_slot;
  if (!readGenerationSlot) {
    throw new Error("kernel supervisor ABI missing supervisor_ofd_pipe_generation_slot");
  }
  return readGenerationSlot.bind(kernel);
}

function requireSupervisorProcessPipeSlotForFd(
  kernel: KernelExports,
): (vmPtr: number, fd: number) => number {
  const readSlot = kernel.supervisor_process_pipe_slot_for_fd;
  if (!readSlot) {
    throw new Error("kernel supervisor ABI missing supervisor_process_pipe_slot_for_fd");
  }
  return readSlot.bind(kernel);
}

function requireSupervisorProcessPipeOfdForFd(
  kernel: KernelExports,
): (vmPtr: number, fd: number) => number {
  const readOfd = kernel.supervisor_process_pipe_ofd_for_fd;
  if (!readOfd) {
    throw new Error("kernel supervisor ABI missing supervisor_process_pipe_ofd_for_fd");
  }
  return readOfd.bind(kernel);
}

function requireSupervisorProcessPipeHostFdForFd(
  kernel: KernelExports,
): (vmPtr: number, fd: number) => number {
  const readHostFd = kernel.supervisor_process_pipe_host_fd_for_fd;
  if (!readHostFd) {
    throw new Error("kernel supervisor ABI missing supervisor_process_pipe_host_fd_for_fd");
  }
  return readHostFd.bind(kernel);
}

export function supervisorOfdPipeSlotMask(handle: ProcessHandle, ofdId: number): bigint {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return 0n;
  }
  return supervisorMaskToBigInt(requireSupervisorOfdPipeMask(handle.kernel)(ofdId));
}

export function supervisorOfdPipeSlots(handle: ProcessHandle, ofdId: number): number[] {
  const maxPipes = handle.kernel.abi_max_pipes();
  const mask = supervisorOfdPipeSlotMask(handle, ofdId);
  const pipeSlots: number[] = [];
  for (let pipeSlot = 1; pipeSlot < maxPipes && pipeSlot < 64; pipeSlot += 1) {
    if ((mask & (1n << BigInt(pipeSlot))) !== 0n) {
      pipeSlots.push(pipeSlot);
    }
  }
  return pipeSlots;
}

export function supervisorOfdPipeSlotMode(
  handle: ProcessHandle,
  ofdId: number,
  pipeSlot: number,
): number {
  if (
    ofdId <= 0
    || ofdId >= handle.kernel.abi_max_fds()
    || pipeSlot <= 0
    || pipeSlot >= handle.kernel.abi_max_pipes()
  ) {
    return 0;
  }
  return requireSupervisorOfdPipeMode(handle.kernel)(ofdId, pipeSlot);
}

export function supervisorOfdPipeGenerationSlot(handle: ProcessHandle, ofdId: number): number {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return 0;
  }
  const pipeSlot = requireSupervisorOfdPipeGenerationSlot(handle.kernel)(ofdId);
  return pipeSlot > 0 && pipeSlot < handle.kernel.abi_max_pipes() ? pipeSlot : 0;
}

export function supervisorOfdUsesPipeSlot(
  handle: ProcessHandle,
  ofdId: number,
  pipeSlot: number,
): boolean {
  if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
    return false;
  }
  return (supervisorOfdPipeSlotMask(handle, ofdId) & (1n << BigInt(pipeSlot))) !== 0n;
}

export function supervisorProcessPipeSlotForFd(
  handle: ProcessHandle,
  vmPtr: number,
  fd: number,
): number {
  if (vmPtr === 0 || fd < 0 || fd >= handle.kernel.abi_max_fds()) {
    return 0;
  }
  const pipeSlot = requireSupervisorProcessPipeSlotForFd(handle.kernel)(vmPtr, fd);
  return pipeSlot > 0 && pipeSlot < handle.kernel.abi_max_pipes() ? pipeSlot : 0;
}

export function supervisorProcessPipeOfdForFd(
  handle: ProcessHandle,
  vmPtr: number,
  fd: number,
): number {
  if (vmPtr === 0 || fd < 0 || fd >= handle.kernel.abi_max_fds()) {
    return 0;
  }
  const ofdId = requireSupervisorProcessPipeOfdForFd(handle.kernel)(vmPtr, fd);
  return ofdId > 0 && ofdId < handle.kernel.abi_max_fds() ? ofdId : 0;
}

export function supervisorProcessPipeHostFdForFd(
  handle: ProcessHandle,
  vmPtr: number,
  fd: number,
): number {
  if (vmPtr === 0 || fd < 0 || fd >= handle.kernel.abi_max_fds()) {
    return -1;
  }
  return requireSupervisorProcessPipeHostFdForFd(handle.kernel)(vmPtr, fd);
}

export function supervisorOfdPipeSlotModeMap(
  handle: ProcessHandle,
  ofdId: number,
): Map<number, number> {
  const modes = new Map<number, number>();
  for (const pipeSlot of supervisorOfdPipeSlots(handle, ofdId)) {
    const mode = supervisorOfdPipeSlotMode(handle, ofdId, pipeSlot);
    if (mode !== 0) {
      modes.set(pipeSlot, mode);
    }
  }
  return modes;
}
