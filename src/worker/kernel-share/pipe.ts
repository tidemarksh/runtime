import { attachKernelRuntimeState } from "../../abi.js";
import type { KernelSyncHandle, ProcessHandle } from "../types.js";

const PIPE_CONTROL_START = 8;
const PIPE_DATA_START = 32;
const PIPE_OFF_READ_OPEN = 8;
const PIPE_OFF_WRITE_OPEN = 12;

function collectLocalPipeSlotsForOfd(handle: ProcessHandle, ofdId: number): number[] {
  const fdType = handle.kernel.kernel_ofd_fd_type(ofdId);
  if (fdType === handle.kernel.abi_fd_type_pipe()) {
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    return pipeSlot > 0 ? [pipeSlot] : [];
  }

  if (fdType === handle.kernel.abi_fd_type_pty_slave?.()) {
    const ptyIdx = handle.kernel.kernel_ofd_ino(ofdId);
    const accessMode = handle.kernel.kernel_ofd_status_flags(ofdId) & 0x3;
    const pipeSlot = accessMode === handle.kernel.abi_o_wronly()
      ? (handle.kernel.kernel_pty_output_pipe?.(ptyIdx) ?? 0)
      : (handle.kernel.kernel_pty_input_pipe?.(ptyIdx) ?? 0);
    return pipeSlot > 0 ? [pipeSlot] : [];
  }

  if (fdType === handle.kernel.abi_fd_type_pty_master?.()) {
    const ptyIdx = handle.kernel.kernel_ofd_ino(ofdId);
    const accessMode = handle.kernel.kernel_ofd_status_flags(ofdId) & 0x3;
    const pipeSlot = accessMode === handle.kernel.abi_o_wronly()
      ? (handle.kernel.kernel_pty_input_pipe?.(ptyIdx) ?? 0)
      : (handle.kernel.kernel_pty_output_pipe?.(ptyIdx) ?? 0);
    return pipeSlot > 0 ? [pipeSlot] : [];
  }

  return [];
}

export function snapshotLocalPipeSlots(
  handle: ProcessHandle,
  ofdIds: Iterable<number>,
): Map<number, Uint8Array> {
  const maxPipes = handle.kernel.abi_max_pipes();
  const regionBytes = handle.kernel.kernel_pipe_region_bytes();
  const slotBytes = regionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid pipe region layout: bytes=${regionBytes} maxPipes=${maxPipes}`);
  }

  const snapshots = new Map<number, Uint8Array>();
  for (const ofdId of ofdIds) {
    for (const pipeSlot of collectLocalPipeSlotsForOfd(handle, ofdId)) {
      if (pipeSlot < 0 || pipeSlot >= maxPipes || snapshots.has(pipeSlot)) {
        continue;
      }
      const slotOffset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
      snapshots.set(pipeSlot, new Uint8Array(handle.memory.buffer, slotOffset, slotBytes).slice());
    }
  }
  return snapshots;
}

export function restoreLocalPipeSlots(
  handle: KernelSyncHandle,
  preservedPipeSlots: ReadonlyMap<number, Uint8Array> | null,
  options: { includeControl?: boolean } = {},
): void {
  if (!preservedPipeSlots || preservedPipeSlots.size === 0) {
    return;
  }
  const maxPipes = handle.kernel.abi_max_pipes();
  const regionBytes = handle.kernel.kernel_pipe_region_bytes();
  const slotBytes = regionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid pipe region layout: bytes=${regionBytes} maxPipes=${maxPipes}`);
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  for (const [pipeSlot, slot] of preservedPipeSlots) {
    const slotOffset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
    if (options.includeControl === true) {
      bytes.set(slot, slotOffset);
      continue;
    }
    bytes.set(slot.subarray(0, PIPE_CONTROL_START), slotOffset);
    bytes.set(slot.subarray(PIPE_DATA_START), slotOffset + PIPE_DATA_START);
  }
}

export function reconcileKernelSharePipeControl(handle: ProcessHandle): void {
  const members = [...handle.kernelShare.members];
  if (members.length === 0) {
    return;
  }
  const maxPipes = handle.kernel.abi_max_pipes();
  const readOpen = new Int32Array(maxPipes);
  const writeOpen = new Int32Array(maxPipes);
  const readKeys = new Set<string>();
  const writeKeys = new Set<string>();
  let pipeUsed = 1n;

  for (const member of members) {
    const maxFds = member.kernel.abi_max_fds();
    for (let fd = 0; fd < maxFds; fd += 1) {
      const ofdId = member.kernel.process_fd_ofd_id(member.vmPtr, fd);
      if (ofdId <= 0 || ofdId >= maxFds) {
        continue;
      }
      if (member.kernel.kernel_ofd_fd_type(ofdId) !== member.kernel.abi_fd_type_pipe()) {
        continue;
      }
      const pipeSlot = member.kernel.kernel_ofd_ino(ofdId);
      if (pipeSlot <= 0 || pipeSlot >= maxPipes) {
        continue;
      }
      pipeUsed |= 1n << BigInt(pipeSlot);
      const hostFd = member.kernel.kernel_ofd_host_fd(ofdId);
      const key = `${member.pid}:${pipeSlot}:${ofdId}`;
      if (hostFd >= 0) {
        readKeys.add(key);
      } else {
        writeKeys.add(key);
      }
    }
  }

  for (const key of readKeys) {
    const [, pipeSlot] = key.split(":");
    readOpen[Number(pipeSlot)] += 1;
  }
  for (const key of writeKeys) {
    const [, pipeSlot] = key.split(":");
    writeOpen[Number(pipeSlot)] += 1;
  }

  const slotBytes = 32 + 65_536;
  for (const member of members) {
    const view = new DataView(member.memory.buffer);
    for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
      if ((pipeUsed & (1n << BigInt(pipeSlot))) === 0n) {
        continue;
      }
      const slotOffset = member.kernelState.pipeBase + pipeSlot * slotBytes;
      view.setInt32(slotOffset + PIPE_OFF_READ_OPEN, readOpen[pipeSlot] ?? 0, true);
      view.setInt32(slotOffset + PIPE_OFF_WRITE_OPEN, writeOpen[pipeSlot] ?? 0, true);
    }
    member.kernelState = {
      ...member.kernelState,
      pipeUsed,
    };
    attachKernelRuntimeState(member.kernel, member.kernelState);
  }
}

export function formatPipeCounterDebug(handle: ProcessHandle, fd: number): string {
  const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
  if (ofdId === 0) {
    return `${fd}:ofd=0`;
  }
  const fdType = handle.kernel.kernel_ofd_fd_type(ofdId);
  let pipeSlot = 0;
  if (fdType === handle.kernel.abi_fd_type_pipe()) {
    pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
  } else if (fdType === handle.kernel.abi_fd_type_pty_slave?.()) {
    const ptyIdx = handle.kernel.kernel_ofd_ino(ofdId);
    const accessMode = handle.kernel.kernel_ofd_status_flags(ofdId) & 0x3;
    pipeSlot = accessMode === handle.kernel.abi_o_wronly()
      ? (handle.kernel.kernel_pty_output_pipe?.(ptyIdx) ?? 0)
      : (handle.kernel.kernel_pty_input_pipe?.(ptyIdx) ?? 0);
  } else if (fdType === handle.kernel.abi_fd_type_pty_master?.()) {
    const ptyIdx = handle.kernel.kernel_ofd_ino(ofdId);
    const accessMode = handle.kernel.kernel_ofd_status_flags(ofdId) & 0x3;
    pipeSlot = accessMode === handle.kernel.abi_o_wronly()
      ? (handle.kernel.kernel_pty_input_pipe?.(ptyIdx) ?? 0)
      : (handle.kernel.kernel_pty_output_pipe?.(ptyIdx) ?? 0);
  }
  if (pipeSlot === 0) {
    return `${fd}:ofd=${ofdId}:type=${fdType}:slot=0`;
  }
  const base = handle.kernelState.pipeBase + pipeSlot * (32 + 65_536);
  const i32 = new Int32Array(handle.memory.buffer);
  const u32 = new Uint32Array(handle.memory.buffer);
  return [
    `${fd}:ofd=${ofdId}`,
    `type=${fdType}`,
    `slot=${pipeSlot}`,
    `ro=${Atomics.load(i32, (base + 8) >>> 2)}`,
    `wo=${Atomics.load(i32, (base + 12) >>> 2)}`,
    `head=${Atomics.load(u32, base >>> 2)}`,
    `tail=${Atomics.load(u32, (base + 4) >>> 2)}`,
  ].join(":");
}

export function formatOpenFdSummary(handle: ProcessHandle, limit = 16): string {
  return Array.from({ length: limit }, (_value, fd) => {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId === 0) {
      return null;
    }
    return `${fd}->${ofdId}/${handle.kernel.process_fd_flags(handle.vmPtr, fd)}/${handle.kernel.kernel_ofd_fd_type(ofdId)}/${handle.kernel.kernel_ofd_ino(ofdId)}/${handle.kernel.kernel_ofd_host_fd(ofdId)}`;
  }).filter(Boolean).join(" ");
}
