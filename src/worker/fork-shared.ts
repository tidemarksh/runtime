import type { KernelSyncHandle, ProcessHandle, ThreadWorkerState, VforkSnapshot } from "./types.js";
import { trackForkSharedOfds } from "./kernel-share.js";

const OFD_TYPE_OFFSET = 0;
const OFD_HOST_FD_OFFSET = 4;
const OFD_OFFSET_OFFSET = 16;
const OFD_INO_OFFSET = 24;

function boundedThreadSlotCount(handle: ProcessHandle): number {
  const maxThreads = handle.kernel.abi_max_threads();
  try {
    const processThreadCount = handle.kernel.thread_process_thread_count?.(handle.vmPtr) ?? maxThreads;
    if (!Number.isFinite(processThreadCount) || processThreadCount <= 0) {
      return 0;
    }
    // Kernel thread slots are sparse and keyed by tid-1, so thread_count is a
    // population count, not a dense slot upper bound.
    return maxThreads;
  } catch {
    return maxThreads;
  }
}

function plausibleThreadVmPtr(handle: ProcessHandle, vmPtr: number): boolean {
  if (!Number.isInteger(vmPtr) || vmPtr <= 0) {
    return false;
  }
  const byteLength = handle.memory.buffer.byteLength;
  return vmPtr < byteLength;
}

export function collectProcessHeldOfds(handle: ProcessHandle): Set<number> {
  const maxFds = handle.kernel.abi_max_fds();
  const heldOfds = new Set<number>();

  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId !== 0 && ofdId < maxFds) {
      heldOfds.add(ofdId);
    }
  }

  for (let slot = 0; slot < boundedThreadSlotCount(handle); slot += 1) {
    let threadVmPtr = 0;
    try {
      threadVmPtr = handle.kernel.process_thread_vm_ptr(handle.vmPtr, slot);
    } catch {
      continue;
    }
    if (threadVmPtr === 0) {
      continue;
    }
    if (!plausibleThreadVmPtr(handle, threadVmPtr)) {
      continue;
    }
    let blockedOfdId = 0;
    try {
      blockedOfdId = handle.kernel.thread_blocked_ofd_id?.(threadVmPtr) ?? 0;
    } catch {
      continue;
    }
    if (blockedOfdId !== 0 && blockedOfdId < maxFds) {
      heldOfds.add(blockedOfdId);
    }
  }

  for (const blocked of (handle.blockedWorkers?.values() ?? [])) {
    let blockedOfdId = 0;
    try {
      blockedOfdId = handle.kernel.thread_blocked_ofd_id?.(blocked.vmPtr) ?? 0;
    } catch {
      continue;
    }
    if (blockedOfdId !== 0 && blockedOfdId < maxFds) {
      heldOfds.add(blockedOfdId);
    }
  }

  return heldOfds;
}

function collectProcessHeldOfdRefcounts(handle: ProcessHandle): Int32Array {
  const maxFds = handle.kernel.abi_max_fds();
  const heldOfdRefcounts = new Int32Array(maxFds);

  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId !== 0 && ofdId < maxFds) {
      heldOfdRefcounts[ofdId] += 1;
    }
  }

  for (let slot = 0; slot < boundedThreadSlotCount(handle); slot += 1) {
    let threadVmPtr = 0;
    try {
      threadVmPtr = handle.kernel.process_thread_vm_ptr(handle.vmPtr, slot);
    } catch {
      continue;
    }
    if (threadVmPtr === 0 || !plausibleThreadVmPtr(handle, threadVmPtr)) {
      continue;
    }
    let blockedOfdId = 0;
    try {
      blockedOfdId = handle.kernel.thread_blocked_ofd_id?.(threadVmPtr) ?? 0;
    } catch {
      continue;
    }
    if (blockedOfdId !== 0 && blockedOfdId < maxFds) {
      heldOfdRefcounts[blockedOfdId] += 1;
    }
  }

  for (const blocked of (handle.blockedWorkers?.values() ?? [])) {
    let blockedOfdId = 0;
    try {
      blockedOfdId = handle.kernel.thread_blocked_ofd_id?.(blocked.vmPtr) ?? 0;
    } catch {
      continue;
    }
    if (blockedOfdId !== 0 && blockedOfdId < maxFds) {
      heldOfdRefcounts[blockedOfdId] += 1;
    }
  }

  return heldOfdRefcounts;
}

export function collectHeldOfdIdsByType(handle: ProcessHandle, types: number[]): number[] {
  const held = collectProcessHeldOfds(handle);
  const ofdIds: number[] = [];
  for (const ofdId of held) {
    if (types.includes(handle.kernel.kernel_ofd_fd_type(ofdId))) {
      ofdIds.push(ofdId);
    }
  }
  return ofdIds;
}

export function collectHeldEpollOfdIds(handle: ProcessHandle): number[] {
  const held = collectProcessHeldOfds(handle);
  const epollOfdIds: number[] = [];
  for (const ofdId of held) {
    if (handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_epoll()) {
      epollOfdIds.push(ofdId);
    }
  }
  return epollOfdIds;
}

export function hasPlausibleOfdState(handle: KernelSyncHandle, ofdId: number): boolean {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return false;
  }
  const type = handle.kernel.kernel_ofd_fd_type(ofdId);
  return type > 0 && type <= 64;
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
  const members = [handle, ...[...handle.kernelShare.members].filter((member) => member !== handle)];
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
      handle.kernelShare.ofdSlotSnapshots.set(ofdId, slot);
      break;
    }
    if (!snapshots.has(ofdId)) {
      const cached = handle.kernelShare.ofdSlotSnapshots.get(ofdId);
      if (cached?.length === slotBytes) {
        snapshots.set(ofdId, cached.slice());
      }
    }
  }
  return snapshots;
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

export function syncKernelShareOfdReservations(handle: ProcessHandle): void {
  const activeOfdIds = activeKernelShareOfdIds(handle);
  const reservedOfdIds = [...new Set([...handle.kernelShare.sharedOfdIds, ...activeOfdIds])];
  for (const member of handle.kernelShare.members) {
    for (const ofdId of reservedOfdIds) {
      member.kernel.kernel_reserve_ofd?.(ofdId);
    }
  }
}

export function pruneStaleKernelShareOfdIds(handle: ProcessHandle, staleOfdIds: Iterable<number>): void {
  for (const ofdId of staleOfdIds) {
    for (const member of handle.kernelShare.members) {
      member.kernel.kernel_unreserve_ofd?.(ofdId);
    }
    handle.kernelShare.sharedOfdIds.delete(ofdId);
    handle.kernelShare.ofdSlotSnapshots.delete(ofdId);
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
    ...[...handle.kernelShare.members].filter((member) => member !== handle),
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
  for (const [ofdId, slotBytesView] of slotCopies) {
    if (slotWouldRollBackRegularFileOffset(handle, ofdId, slotBytesView)) {
      continue;
    }
    const slotOffset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
    targetTable.set(slotBytesView, slotOffset);
  }
}

function slotWouldRollBackRegularFileOffset(
  handle: ProcessHandle,
  ofdId: number,
  slot: Uint8Array,
): boolean {
  const targetType = handle.kernel.kernel_ofd_fd_type(ofdId);
  if (targetType !== handle.kernel.abi_fd_type_file()) {
    return false;
  }
  const view = new DataView(slot.buffer, slot.byteOffset, slot.byteLength);
  return view.getInt32(OFD_TYPE_OFFSET, true) === targetType
    && view.getUint32(OFD_INO_OFFSET, true) === handle.kernel.kernel_ofd_ino(ofdId)
    && view.getInt32(OFD_HOST_FD_OFFSET, true) === handle.kernel.kernel_ofd_host_fd(ofdId)
    && handle.kernel.kernel_ofd_refcount(ofdId) > 0
    && view.getBigInt64(OFD_OFFSET_OFFSET, true) < handle.kernel.kernel_ofd_offset(ofdId);
}

export function reconcileForkSharedOfdRefcounts(handle: ProcessHandle, sharedOfdIds: Iterable<number>): void {
  const maxFds = handle.kernel.abi_max_fds();
  const sharedOfdIdSet = new Set(sharedOfdIds);
  const expectedRefcounts = new Int32Array(maxFds);

  for (const member of handle.kernelShare.members) {
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
    let currentRefcount = handle.kernel.kernel_ofd_refcount(ofdId);
    const expectedRefcount = expectedRefcounts[ofdId] ?? 0;
    while (currentRefcount < expectedRefcount) {
      handle.kernel.kernel_retain_ofd(ofdId);
      currentRefcount += 1;
    }
    while (currentRefcount > expectedRefcount) {
      handle.kernel.kernel_release_ofd(ofdId);
      currentRefcount -= 1;
    }
  }
}

export function captureVforkSnapshot(
  handle: ProcessHandle,
  thread: Pick<ThreadWorkerState, "vmPtr" | "lastStepProcessPtr" | "lastStepPc" | "lastStepSp" | "lastStepS11">,
): VforkSnapshot {
  const ofdIds = trackForkSharedOfds(handle, thread.vmPtr, true);
  return {
    vmPtr: thread.vmPtr,
    childPid: null,
    nextPid: null,
    pgid: null,
    sid: null,
    parentPid: handle.pid,
    parentProcessPtr: thread.lastStepProcessPtr ?? handle.kernel.thread_process_ptr(thread.vmPtr),
    parentPc: thread.lastStepPc ?? handle.kernel.thread_pc(thread.vmPtr),
    parentSp: thread.lastStepSp ?? handle.kernel.thread_sp(thread.vmPtr),
    parentS11: thread.lastStepS11 ?? (handle.kernel.thread_s11?.(thread.vmPtr) ?? 0n),
    inheritedSharedOfdIds: ofdIds,
  };
}
