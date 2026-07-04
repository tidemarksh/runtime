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
