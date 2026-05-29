import type { KernelSyncHandle, ProcessHandle } from "../types.js";

export function boundedThreadSlotCount(handle: ProcessHandle): number {
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

export function plausibleThreadVmPtr(handle: ProcessHandle, vmPtr: number): boolean {
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

export function collectProcessHeldOfdRefcounts(handle: ProcessHandle): Int32Array {
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
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId !== 0
      && stdioPipeSlot > 0
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
      && handle.kernel.kernel_ofd_ino(ofdId) === stdioPipeSlot
    ) {
      stdioOfdIds.add(ofdId);
    }
  }
  const retained = new Set<number>();
  for (let fd = 0; fd < handle.kernel.abi_max_fds(); fd++) {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (ofdId === 0) {
      continue;
    }
    if (stdioOfdIds.has(ofdId)) {
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

export function retainForkSharedOfds(handle: ProcessHandle): void {
  trackForkSharedOfds(handle, handle.vmPtr, true);
}
