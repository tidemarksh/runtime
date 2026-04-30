import type { ProcessHandle } from "../types.js";

export function adoptKernelShare(parent: ProcessHandle, child: ProcessHandle): void {
  child.kernelShare = parent.kernelShare;
  child.kernelShare.members.add(child);
}

export function detachKernelShare(handle: ProcessHandle): void {
  handle.kernelShare.members.delete(handle);
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
