import type { KernelWorkerFdEntrySnapshot } from "../../messages.js";
import { OFD_OFFSET_OFFSET } from "../../layout/ofd.js";
import { PIPE_GENERATION_OFFSET } from "../../layout/pipe.js";
import {
  collectProcessHeldOfds,
} from "../ownership/ofd-table.js";
import type { ProcessHandle } from "./types.js";

function currentPipeGeneration(handle: ProcessHandle, pipeSlot: number): number | null {
  const pipeSlotBytes = handle.kernel.kernel_pipe_region_bytes() / handle.kernel.abi_max_pipes();
  if (
    !Number.isInteger(pipeSlotBytes)
    || pipeSlotBytes <= PIPE_GENERATION_OFFSET
    || pipeSlot <= 0
    || pipeSlot >= handle.kernel.abi_max_pipes()
  ) {
    return null;
  }
  return new DataView(handle.memory.buffer).getUint32(
    handle.kernelState.pipeBase + pipeSlot * pipeSlotBytes + PIPE_GENERATION_OFFSET,
    true,
  );
}

function pipeOfdGenerationDiffers(handle: ProcessHandle, ofdId: number): boolean {
  if (handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()) {
    return false;
  }
  const pipeGeneration = currentPipeGeneration(handle, handle.kernel.kernel_ofd_ino(ofdId));
  return pipeGeneration !== null && handle.kernel.kernel_ofd_offset(ofdId) !== BigInt(pipeGeneration);
}

function localOfdSlotDiffersFromHandle(handle: ProcessHandle, ofdId: number, slot: Uint8Array): boolean {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return false;
  }
  const view = new DataView(slot.buffer, slot.byteOffset, slot.byteLength);
  const preservedType = view.getInt32(0, true);
  if (preservedType === 0) {
    return false;
  }
  const currentType = handle.kernel.kernel_ofd_fd_type(ofdId);
  if (currentType === 0 || currentType !== preservedType) {
    return true;
  }
  if (pipeOfdGenerationDiffers(handle, ofdId)) {
    return true;
  }
  return handle.kernel.kernel_ofd_host_fd(ofdId) !== view.getInt32(4, true)
    || handle.kernel.kernel_ofd_ino(ofdId) !== view.getUint32(24, true);
}

export function snapshotLocalOfdSlots(
  handle: ProcessHandle,
  ofdIds: Iterable<number>,
): Map<number, Uint8Array> {
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }
  const snapshots = new Map<number, Uint8Array>();
  for (const ofdId of ofdIds) {
    if (ofdId <= 0 || ofdId >= maxFds || snapshots.has(ofdId)) {
      continue;
    }
    const fdType = handle.kernel.kernel_ofd_fd_type(ofdId);
    if (fdType <= 0 || fdType > 64) {
      continue;
    }
    const slotOffset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
    snapshots.set(
      ofdId,
      new Uint8Array(handle.memory.buffer, slotOffset, slotBytes).slice(),
    );
  }
  return snapshots;
}

export function restoreIncompatibleLocalOfdSlots(
  handle: ProcessHandle,
  preservedOfdSlots: ReadonlyMap<number, Uint8Array>,
): void {
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  const view = new DataView(handle.memory.buffer);
  for (const [ofdId, slot] of preservedOfdSlots) {
    if (slot.length !== slotBytes || !localOfdSlotDiffersFromHandle(handle, ofdId, slot)) {
      continue;
    }
    const ofdSlotOffset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
    bytes.set(slot, ofdSlotOffset);
    if (
      handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
    ) {
      const pipeGeneration = currentPipeGeneration(handle, handle.kernel.kernel_ofd_ino(ofdId));
      if (pipeGeneration !== null) {
        view.setBigInt64(ofdSlotOffset + OFD_OFFSET_OFFSET, BigInt(pipeGeneration), true);
      }
    }
  }
}

export function restoreLocalPipeSlotGenerations(
  handle: ProcessHandle,
  preservedPipeSlots: ReadonlyMap<number, Uint8Array>,
): void {
  if (preservedPipeSlots.size === 0) {
    return;
  }
  const maxPipes = handle.kernel.abi_max_pipes();
  const pipeSlotBytes = handle.kernel.kernel_pipe_region_bytes() / maxPipes;
  if (!Number.isInteger(pipeSlotBytes) || pipeSlotBytes <= PIPE_GENERATION_OFFSET) {
    return;
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  for (const [pipeSlot, slot] of preservedPipeSlots) {
    if (
      pipeSlot <= 0
      || pipeSlot >= maxPipes
      || slot.length !== pipeSlotBytes
    ) {
      continue;
    }
    bytes.set(
      slot.subarray(PIPE_GENERATION_OFFSET, PIPE_GENERATION_OFFSET + 4),
      handle.kernelState.pipeBase + pipeSlot * pipeSlotBytes + PIPE_GENERATION_OFFSET,
    );
  }
}

export function alignPipeOfdGenerationOffsets(
  handle: ProcessHandle,
  ofdIds: Iterable<number>,
): void {
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= OFD_OFFSET_OFFSET) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }

  const view = new DataView(handle.memory.buffer);
  for (const ofdId of ofdIds) {
    if (
      ofdId <= 0
      || ofdId >= maxFds
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      continue;
    }
    const pipeGeneration = currentPipeGeneration(handle, handle.kernel.kernel_ofd_ino(ofdId));
    if (pipeGeneration === null || handle.kernel.kernel_ofd_offset(ofdId) === BigInt(pipeGeneration)) {
      continue;
    }
    view.setBigInt64(
      handle.kernelState.ofdTablePtr + ofdId * slotBytes + OFD_OFFSET_OFFSET,
      BigInt(pipeGeneration),
      true,
    );
  }
}

export function reconcileInheritedSharedOfdsAfterVforkExecve(
  handle: ProcessHandle,
  inheritedSharedOfdIds: Iterable<number>,
): void {
  const heldByExistingMembers = new Set<number>();
  for (const member of handle.kernelShare.members) {
    for (const ofdId of collectProcessHeldOfds(member)) {
      heldByExistingMembers.add(ofdId);
    }
  }
  for (const ofdId of inheritedSharedOfdIds) {
    if (heldByExistingMembers.has(ofdId)) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
    } else {
      handle.kernelShare.sharedOfdIds.delete(ofdId);
    }
  }
}

export function trackVforkExecveChildSharedOfds(
  childHandle: ProcessHandle,
  childExecFdEntries: readonly KernelWorkerFdEntrySnapshot[],
): void {
  for (const entry of childExecFdEntries) {
    if (entry.ofdId > 0 && entry.ofdId < childHandle.kernel.abi_max_fds()) {
      childHandle.kernelShare.sharedOfdIds.add(entry.ofdId);
    }
  }
  for (const ofdId of collectProcessHeldOfds(childHandle)) {
    childHandle.kernelShare.sharedOfdIds.add(ofdId);
  }
}
