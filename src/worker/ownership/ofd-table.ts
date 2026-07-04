import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
} from "../../messages.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../layout/ofd.js";
import { PIPE_GENERATION_OFFSET } from "../../layout/pipe.js";
import type { KernelSyncHandle, ProcessHandle } from "../types.js";
import { supervisorProcessOfdForFd } from "./fd-table.js";
import {
  supervisorProcessPipeOfdForFd,
  supervisorProcessPipeSlotForFd,
} from "./pipe-slot.js";

function requireSupervisorProcessHeldOfdRefcount(
  handle: ProcessHandle,
): (vmPtr: number, ofdId: number) => number {
  const readSupervisorHeldOfdRefcount = handle.kernel.supervisor_process_held_ofd_refcount;
  if (!readSupervisorHeldOfdRefcount) {
    throw new Error("kernel supervisor ABI missing supervisor_process_held_ofd_refcount");
  }
  return readSupervisorHeldOfdRefcount.bind(handle.kernel);
}

function requireSupervisorRestoreActionApply(handle: ProcessHandle): number {
  const apply = handle.kernel.supervisor_fd_entry_restore_action_apply;
  if (!apply) {
    throw new Error("kernel supervisor ABI missing supervisor_fd_entry_restore_action_apply");
  }
  return apply.call(handle.kernel);
}

function requireSupervisorRegularFileOffsetRestoreAction(
  handle: ProcessHandle,
): (
  ofdId: number,
  snapshotHostFd: number,
  snapshotIno: number,
  snapshotOffset: bigint | number,
) => number {
  const decide = handle.kernel.supervisor_regular_file_offset_restore_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_regular_file_offset_restore_action");
  }
  return decide.bind(handle.kernel);
}

export function collectProcessHeldOfds(handle: ProcessHandle): Set<number> {
  const maxFds = handle.kernel.abi_max_fds();
  const heldOfds = new Set<number>();
  const supervisorHeldOfdRefcount = requireSupervisorProcessHeldOfdRefcount(handle);
  for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
    if (supervisorHeldOfdRefcount(handle.vmPtr, ofdId) > 0) {
      heldOfds.add(ofdId);
    }
  }
  return heldOfds;
}

export function collectProcessHeldOfdRefcounts(handle: ProcessHandle): Int32Array {
  const maxFds = handle.kernel.abi_max_fds();
  const heldOfdRefcounts = new Int32Array(maxFds);
  const supervisorHeldOfdRefcount = requireSupervisorProcessHeldOfdRefcount(handle);
  for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
    const refcount = supervisorHeldOfdRefcount(handle.vmPtr, ofdId);
    if (refcount > 0) {
      heldOfdRefcounts[ofdId] = refcount;
    }
  }
  return heldOfdRefcounts;
}

function collectSupervisorProcessOfdIds(
  handle: ProcessHandle,
  nextOfd: ((vmPtr: number, index: number) => number) | undefined,
  exportName: string,
): number[] {
  if (!nextOfd) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  const maxFds = handle.kernel.abi_max_fds();
  const ofdIds: number[] = [];
  for (let index = 0; index < maxFds; index += 1) {
    const ofdId = nextOfd.call(handle.kernel, handle.vmPtr, index);
    if (ofdId === 0) {
      break;
    }
    if (ofdId > 0 && ofdId < maxFds) {
      ofdIds.push(ofdId);
    }
  }
  return ofdIds;
}

export function collectHeldEpollOfdIds(handle: ProcessHandle): number[] {
  return collectSupervisorProcessOfdIds(
    handle,
    handle.kernel.supervisor_process_epoll_held_ofd,
    "supervisor_process_epoll_held_ofd",
  );
}

export function collectProcessSyncDependentOfdIds(handle: ProcessHandle): number[] {
  return collectSupervisorProcessOfdIds(
    handle,
    handle.kernel.supervisor_process_sync_dependent_ofd,
    "supervisor_process_sync_dependent_ofd",
  );
}

export function collectHeldEpollDependentSyncOfdIds(handle: ProcessHandle): number[] {
  return collectSupervisorProcessOfdIds(
    handle,
    handle.kernel.supervisor_process_epoll_dependent_sync_ofd,
    "supervisor_process_epoll_dependent_sync_ofd",
  );
}

export function processEpollDependsOnOfd(handle: ProcessHandle, ofdId: number): boolean {
  const dependsOnOfd = handle.kernel.supervisor_process_epoll_depends_on_ofd;
  if (!dependsOnOfd) {
    throw new Error("kernel supervisor ABI missing supervisor_process_epoll_depends_on_ofd");
  }
  return dependsOnOfd.call(handle.kernel, handle.vmPtr, ofdId) !== 0;
}

export function snapshotOfdSlotsForThread(
  handle: ProcessHandle,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
): KernelWorkerOfdSlotSnapshot[] | undefined {
  if (!fdEntries || fdEntries.length === 0) {
    return undefined;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes?.() ?? 0;
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return undefined;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  const ofdSlots: KernelWorkerOfdSlotSnapshot[] = [];
  for (const ofdId of [...new Set(fdEntries.map((entry) => entry.ofdId))]) {
    if (ofdId <= 0 || ofdId >= maxFds) {
      continue;
    }
    const watermarked = handle.networkOfdSlotWatermarks?.get(ofdId);
    if (watermarked && watermarked.length === slotBytes) {
      ofdSlots.push({
        ofdId,
        slot: watermarked.slice(),
      });
      continue;
    }
    const offset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
    ofdSlots.push({
      ofdId,
      slot: memory.slice(offset, offset + slotBytes),
    });
  }
  return ofdSlots.length > 0 ? ofdSlots : undefined;
}

export function restoreRegularFileOffsetsForStep(handle: ProcessHandle): void {
  const offsets = handle.regularFileOfdOffsets;
  if (!offsets || offsets.size === 0) {
    return;
  }
  const restoreAction = requireSupervisorRegularFileOffsetRestoreAction(handle);
  const applyAction = requireSupervisorRestoreActionApply(handle);
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
  }
  const view = new DataView(handle.memory.buffer);
  for (const [ofdId, snapshot] of offsets) {
    if (ofdId <= 0 || ofdId >= maxFds) {
      continue;
    }
    if (restoreAction(ofdId, snapshot.hostFd, snapshot.ino, snapshot.offset) !== applyAction) {
      continue;
    }
    view.setBigInt64(handle.kernelState.ofdTablePtr + ofdId * slotBytes + OFD_OFFSET_OFFSET, snapshot.offset, true);
  }
}

export function hasPlausibleOfdState(handle: KernelSyncHandle, ofdId: number): boolean {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return false;
  }
  const type = handle.kernel.kernel_ofd_fd_type(ofdId);
  return type > 0 && type <= 64;
}

export function activeKernelShareOfdIds(handle: ProcessHandle): number[] {
  const heldByAnyMember = new Set<number>();
  for (const member of handle.kernelShare.members) {
    for (const ofdId of collectProcessHeldOfds(member)) {
      heldByAnyMember.add(ofdId);
    }
  }

  const active: number[] = [];
  for (const ofdId of handle.kernelShare.sharedOfdIds) {
    if (heldByAnyMember.has(ofdId)) {
      active.push(ofdId);
    }
  }
  return active.sort((left, right) => left - right);
}

export function staleKernelShareOfdIds(handle: ProcessHandle): number[] {
  const heldByAnyMember = new Set<number>();
  for (const member of handle.kernelShare.members) {
    for (const ofdId of collectProcessHeldOfds(member)) {
      heldByAnyMember.add(ofdId);
    }
  }

  const stale: number[] = [];
  for (const ofdId of handle.kernelShare.sharedOfdIds) {
    if (!heldByAnyMember.has(ofdId)) {
      stale.push(ofdId);
    }
  }
  return stale.sort((left, right) => left - right);
}

export function forkSharedHeldOfdTrackAction(
  handle: ProcessHandle,
  ofdId: number,
  facts: { shared: boolean; hostStdioPipe: boolean; stdioFd: boolean; fdEntrySource?: boolean },
): boolean {
  const decide = handle.kernel.supervisor_fork_shared_held_ofd_track_action;
  const sharedFlag = handle.kernel.supervisor_fork_shared_held_ofd_track_flag_shared;
  const hostStdioPipeFlag = handle.kernel.supervisor_fork_shared_held_ofd_track_flag_host_stdio_pipe;
  const stdioFdFlag = handle.kernel.supervisor_fork_shared_held_ofd_track_flag_stdio_fd;
  const fdEntrySourceFlag = handle.kernel.supervisor_fork_shared_held_ofd_track_flag_fd_entry_source;
  if (!decide || !sharedFlag || !hostStdioPipeFlag || !stdioFdFlag || !fdEntrySourceFlag) {
    throw new Error("kernel supervisor ABI missing fork-shared held OFD track action exports");
  }
  let flags = 0;
  if (facts.shared) {
    flags |= sharedFlag.call(handle.kernel);
  }
  if (facts.hostStdioPipe) {
    flags |= hostStdioPipeFlag.call(handle.kernel);
  }
  if (facts.stdioFd) {
    flags |= stdioFdFlag.call(handle.kernel);
  }
  if (facts.fdEntrySource) {
    flags |= fdEntrySourceFlag.call(handle.kernel);
  }
  return decide.call(handle.kernel, ofdId, flags) !== 0;
}

export function trackForkSharedOfds(
  handle: ProcessHandle,
  vmPtr: number,
  retainRefcounts: boolean,
): number[] {
  const stdioOfdIds = new Set<number>();
  const stdioPipeSlotByFd = new Map<number, number>([
    [0, handle.stdinPipe],
    [1, handle.stdoutPipe],
    [2, handle.stderrPipe],
  ]);
  for (const [fd, stdioPipeSlot] of stdioPipeSlotByFd) {
    const ofdId = supervisorProcessPipeOfdForFd(handle, vmPtr, fd);
    if (
      ofdId !== 0
      && stdioPipeSlot > 0
      && supervisorProcessPipeSlotForFd(handle, vmPtr, fd) === stdioPipeSlot
    ) {
      stdioOfdIds.add(ofdId);
    }
  }
  const retained = new Set<number>();
  for (let fd = 0; fd < handle.kernel.abi_max_fds(); fd++) {
    const ofdId = supervisorProcessOfdForFd(handle, vmPtr, fd);
    if (ofdId === 0) {
      continue;
    }
    if (!forkSharedHeldOfdTrackAction(handle, ofdId, {
      shared: handle.kernelShare.sharedOfdIds.has(ofdId),
      hostStdioPipe: stdioOfdIds.has(ofdId),
      stdioFd: fd >= 0 && fd <= 2,
      fdEntrySource: true,
    })) {
      continue;
    }
    retained.add(ofdId);
    handle.kernelShare.sharedOfdIds.add(ofdId);
    if (retainRefcounts) {
      handle.kernel.kernel_retain_ofd(ofdId);
    }
  }
  return [...retained].sort((left, right) => left - right);
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

export function snapshotRuntimeNetworkOfdSlots(
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
  const memory = new Uint8Array(handle.memory.buffer);
  for (const ofdId of ofdIds) {
    if (ofdId <= 0 || ofdId >= maxFds || snapshots.has(ofdId)) {
      continue;
    }
    if (handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_socket()) {
      const slotOffset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
      snapshots.set(ofdId, memory.slice(slotOffset, slotOffset + slotBytes));
      continue;
    }
    const watermarked = handle.networkOfdSlotWatermarks?.get(ofdId);
    if (watermarked && watermarked.length === slotBytes) {
      snapshots.set(ofdId, watermarked.slice());
    }
  }
  return snapshots;
}

export function restoreRuntimeNetworkOfdSlots(
  handle: ProcessHandle,
  preservedOfdSlots: ReadonlyMap<number, Uint8Array>,
): void {
  if (preservedOfdSlots.size === 0) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  for (const [ofdId, slot] of preservedOfdSlots) {
    if (ofdId <= 0 || ofdId >= maxFds || slot.length !== slotBytes) {
      continue;
    }
    const view = new DataView(slot.buffer, slot.byteOffset, slot.byteLength);
    if (view.getInt32(OFD_TYPE_OFFSET, true) !== handle.kernel.abi_fd_type_socket()) {
      continue;
    }
    bytes.set(slot, handle.kernelState.ofdTablePtr + ofdId * slotBytes);
  }
}

export function currentPipeGeneration(handle: ProcessHandle, pipeSlot: number): number | null {
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

export function syncPipeOfdGenerationsForSlot(handle: ProcessHandle, pipeSlot: number): void {
  const pipeGeneration = currentPipeGeneration(handle, pipeSlot);
  if (pipeGeneration === null || pipeGeneration === 0) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= OFD_INO_OFFSET + 4) {
    return;
  }
  const view = new DataView(handle.memory.buffer);
  for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
    const ofdOffset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
    if (
      view.getInt32(ofdOffset + OFD_TYPE_OFFSET, true) === handle.kernel.abi_fd_type_pipe()
      && view.getUint32(ofdOffset + OFD_INO_OFFSET, true) === pipeSlot
    ) {
      view.setBigInt64(ofdOffset + OFD_OFFSET_OFFSET, BigInt(pipeGeneration), true);
    }
  }
}

export function pipeOfdGenerationDiffers(handle: ProcessHandle, ofdId: number): boolean {
  if (handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()) {
    return false;
  }
  const pipeGeneration = currentPipeGeneration(handle, handle.kernel.kernel_ofd_ino(ofdId));
  return pipeGeneration !== null && handle.kernel.kernel_ofd_offset(ofdId) !== BigInt(pipeGeneration);
}

export function localOfdSlotDiffersFromHandle(handle: ProcessHandle, ofdId: number, slot: Uint8Array): boolean {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return false;
  }
  const view = new DataView(slot.buffer, slot.byteOffset, slot.byteLength);
  const sourceType = view.getInt32(OFD_TYPE_OFFSET, true);
  if (sourceType === 0) {
    return false;
  }
  const targetType = handle.kernel.kernel_ofd_fd_type(ofdId);
  if (targetType === 0 || targetType !== sourceType) {
    return true;
  }
  if (pipeOfdGenerationDiffers(handle, ofdId)) {
    return true;
  }
  return handle.kernel.kernel_ofd_host_fd(ofdId) !== view.getInt32(OFD_HOST_FD_OFFSET, true)
    || handle.kernel.kernel_ofd_ino(ofdId) !== view.getUint32(OFD_INO_OFFSET, true);
}

export function restoreIncompatibleLocalOfdSlots(
  handle: ProcessHandle,
  preservedOfdSlots: ReadonlyMap<number, Uint8Array>,
): void {
  if (preservedOfdSlots.size === 0) {
    return;
  }
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
