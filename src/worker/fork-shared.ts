import type { ProcessHandle, ThreadWorkerState, VforkSnapshot } from "./types.js";
import type { KernelWorkerFdEntrySnapshot } from "../messages.js";
import { getThreadActivityState } from "./thread-activity-state.js";
import {
  activeKernelShareOfdIds,
  collectProcessHeldOfdRefcounts,
  collectProcessHeldOfds,
  hasPlausibleOfdState,
  trackForkSharedOfds,
} from "./ownership/ofd-table.js";
import { supervisorOfdReconcileRefcount } from "./ownership/ofd-metadata.js";
import {
  deleteKernelShareOfdSlotSnapshot,
  deleteKernelShareSharedOfdId,
  getKernelShareOfdSlotSnapshot,
  kernelShareMemberList,
  kernelShareSharedOfdIdList,
  snapshotLocalPipeSlots,
  setKernelShareOfdSlotSnapshot,
} from "./kernel-share.js";
import { snapshotProcessFdEntries } from "./ownership/fd-table.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_TYPE_OFFSET,
} from "../layout/ofd.js";

function requireSupervisorRestoreActionSkip(handle: ProcessHandle): number {
  const skip = handle.kernel.supervisor_fd_entry_restore_action_skip;
  if (!skip) {
    throw new Error("kernel supervisor ABI missing supervisor_fd_entry_restore_action_skip");
  }
  return skip.call(handle.kernel);
}

function requireSupervisorOfdSlotWriteAction(
  handle: ProcessHandle,
): (
  ofdId: number,
  candidateType: number,
  candidateHostFd: number,
  candidateIno: number,
  candidateOffset: bigint | number,
) => number {
  const decide = handle.kernel.supervisor_ofd_slot_write_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_ofd_slot_write_action");
  }
  return decide.bind(handle.kernel);
}

function ofdSlotWriteShouldBeSkipped(
  handle: ProcessHandle,
  ofdId: number,
  slot: Uint8Array,
  decide: ReturnType<typeof requireSupervisorOfdSlotWriteAction>,
  skipAction: number,
): boolean {
  const view = new DataView(slot.buffer, slot.byteOffset, slot.byteLength);
  return decide(
    ofdId,
    view.getInt32(OFD_TYPE_OFFSET, true),
    view.getInt32(OFD_HOST_FD_OFFSET, true),
    view.getUint32(OFD_INO_OFFSET, true),
    view.getBigInt64(OFD_OFFSET_OFFSET, true),
  ) === skipAction;
}

export function snapshotKernelShareOfdSlots(
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
  const members = [handle, ...kernelShareMemberList(handle).filter((member) => member !== handle)];
  for (const ofdId of ofdIds) {
    if (ofdId <= 0 || ofdId >= maxFds || snapshots.has(ofdId)) {
      continue;
    }
    const heldMembers = members.filter((member) => collectProcessHeldOfds(member).has(ofdId));
    for (const member of [...heldMembers, ...members.filter((member) => !heldMembers.includes(member))]) {
      if (!hasPlausibleOfdState(member, ofdId)) {
        continue;
      }
      const slotOffset = member.kernelState.ofdTablePtr + ofdId * slotBytes;
      const slot = new Uint8Array(member.memory.buffer, slotOffset, slotBytes).slice();
      snapshots.set(ofdId, slot);
      setKernelShareOfdSlotSnapshot(handle, ofdId, slot);
      break;
    }
    if (!snapshots.has(ofdId)) {
      const cached = getKernelShareOfdSlotSnapshot(handle, ofdId);
      if (cached?.length === slotBytes) {
        snapshots.set(ofdId, cached.slice());
      }
    }
  }
  return snapshots;
}

export function syncKernelShareOfdReservations(handle: ProcessHandle): void {
  const activeOfdIds = activeKernelShareOfdIds(handle);
  const reservedOfdIds = [...new Set([...kernelShareSharedOfdIdList(handle), ...activeOfdIds])];
  for (const member of kernelShareMemberList(handle)) {
    for (const ofdId of reservedOfdIds) {
      member.kernel.kernel_reserve_ofd?.(ofdId);
    }
  }
}

export function pruneStaleKernelShareOfdIds(handle: ProcessHandle, staleOfdIds: Iterable<number>): void {
  for (const ofdId of staleOfdIds) {
    for (const member of kernelShareMemberList(handle)) {
      member.kernel.kernel_unreserve_ofd?.(ofdId);
    }
    deleteKernelShareSharedOfdId(handle, ofdId);
    deleteKernelShareOfdSlotSnapshot(handle, ofdId);
  }
}

export function mergeKernelShareOfdStateInto(handle: ProcessHandle, sharedOfdIds: Iterable<number>): void {
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }

  const slotCopies = new Map<number, Uint8Array>();
  const sourceFirstMembers = [
    handle,
    ...kernelShareMemberList(handle).filter((member) => member !== handle),
  ];
  for (const ofdId of sharedOfdIds) {
    for (const member of sourceFirstMembers) {
      if (
        slotCopies.has(ofdId)
        || !collectProcessHeldOfds(member).has(ofdId)
        || !hasPlausibleOfdState(member, ofdId)
      ) {
        continue;
      }
      const slotOffset = member.kernelState.ofdTablePtr + ofdId * slotBytes;
      slotCopies.set(ofdId, new Uint8Array(member.memory.buffer, slotOffset, slotBytes).slice());
      break;
    }
  }

  const targetTable = new Uint8Array(handle.memory.buffer);
  const decideSlotWrite = requireSupervisorOfdSlotWriteAction(handle);
  const skipAction = requireSupervisorRestoreActionSkip(handle);
  for (const [ofdId, slotBytesView] of slotCopies) {
    if (ofdSlotWriteShouldBeSkipped(handle, ofdId, slotBytesView, decideSlotWrite, skipAction)) {
      continue;
    }
    const slotOffset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
    targetTable.set(slotBytesView, slotOffset);
  }
}

export function reconcileForkSharedOfdRefcounts(handle: ProcessHandle, sharedOfdIds: Iterable<number>): void {
  const maxFds = handle.kernel.abi_max_fds();
  const sharedOfdIdSet = new Set(sharedOfdIds);
  const expectedRefcounts = new Int32Array(maxFds);

  for (const member of kernelShareMemberList(handle)) {
    const memberRefcounts = collectProcessHeldOfdRefcounts(member);
    for (const ofdId of sharedOfdIdSet) {
      if (ofdId <= 0 || ofdId >= maxFds) {
        continue;
      }
      expectedRefcounts[ofdId] += memberRefcounts[ofdId] ?? 0;
    }
  }

  for (const ofdId of sharedOfdIdSet) {
    if (ofdId <= 0 || ofdId >= maxFds) {
      continue;
    }
    const expectedRefcount = expectedRefcounts[ofdId] ?? 0;
    supervisorOfdReconcileRefcount(handle, ofdId, expectedRefcount);
  }
}

export function captureVforkSnapshot(
  handle: ProcessHandle,
  thread: ThreadWorkerState,
): VforkSnapshot {
  const ofdIds = trackForkSharedOfds(handle, thread.vmPtr, true);
  const activity = getThreadActivityState(thread);
  const parentFdEntries = snapshotVforkParentFdEntries(handle, thread.vmPtr);
  const parentHeldOfdIds = parentSnapshotHeldOfdIds(handle, parentFdEntries);
  return {
    vmPtr: thread.vmPtr,
    childPid: null,
    nextPid: null,
    pgid: null,
    sid: null,
    parentPid: handle.pid,
    parentProcessPtr: activity.lastStepProcessPtr ?? handle.kernel.thread_process_ptr(thread.vmPtr),
    parentPc: activity.lastStepPc ?? handle.kernel.thread_pc(thread.vmPtr),
    parentSp: activity.lastStepSp ?? handle.kernel.thread_sp(thread.vmPtr),
    parentS11: activity.lastStepS11 ?? (handle.kernel.thread_s11?.(thread.vmPtr) ?? 0n),
    inheritedSharedOfdIds: ofdIds,
    parentFdEntries,
    parentHeldOfdIds,
    parentOfdSlots: snapshotKernelShareOfdSlots(handle, parentHeldOfdIds),
    parentPipeSlots: snapshotLocalPipeSlots(handle, parentHeldOfdIds),
  };
}

export function snapshotVforkParentFdEntries(
  handle: ProcessHandle,
  vmPtr: number,
): KernelWorkerFdEntrySnapshot[] {
  const readOfdId = handle.kernel.thread_vfork_parent_fd_ofd_id;
  const readFlags = handle.kernel.thread_vfork_parent_fd_flags;
  const readGeneration = handle.kernel.thread_vfork_parent_fd_generation;
  if (!readOfdId || !readFlags || !readGeneration) {
    return snapshotProcessFdEntries(handle, vmPtr);
  }
  const fdEntries: KernelWorkerFdEntrySnapshot[] = [];
  for (let fd = 0; fd < handle.kernel.abi_max_fds(); fd += 1) {
    fdEntries.push({
      fd,
      ofdId: readOfdId.call(handle.kernel, fd),
      fdFlags: readFlags.call(handle.kernel, fd),
      generation: readGeneration.call(handle.kernel, fd),
    });
  }
  return fdEntries;
}

function parentSnapshotHeldOfdIds(
  handle: ProcessHandle,
  parentFdEntries: readonly KernelWorkerFdEntrySnapshot[],
): number[] {
  const maxFds = handle.kernel.abi_max_fds();
  return [...new Set([
    ...collectProcessHeldOfds(handle),
    ...parentFdEntries
      .map((entry) => entry.ofdId)
      .filter((ofdId) => ofdId > 0 && ofdId < maxFds),
  ])];
}
