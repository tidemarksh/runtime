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
  const stdioPipeSlots = new Set<number>([handle.stdinPipe, handle.stdoutPipe, handle.stderrPipe]
    .filter((pipeSlot) => pipeSlot > 0));
  const retained = new Set<number>();
  for (let fd = 0; fd < handle.kernel.abi_max_fds(); fd++) {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (ofdId === 0 || retained.has(ofdId)) {
      continue;
    }
    if (
      handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
      && stdioPipeSlots.has(handle.kernel.kernel_ofd_ino(ofdId))
    ) {
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
