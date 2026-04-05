import type { KernelExports } from "../abi.js";

export function collectLiveThreadVmPtrs(kernel: KernelExports, vmPtr: number): Set<number> {
  const liveVmPtrs = new Set<number>();
  for (let slot = 0; slot < kernel.abi_max_threads(); slot += 1) {
    const liveVmPtr = kernel.process_thread_vm_ptr(vmPtr, slot);
    if (liveVmPtr !== 0) {
      liveVmPtrs.add(liveVmPtr);
    }
  }
  return liveVmPtrs;
}

export function processHasLiveThreads(kernel: KernelExports, vmPtr: number): boolean {
  for (let slot = 0; slot < kernel.abi_max_threads(); slot += 1) {
    if (kernel.process_thread_vm_ptr(vmPtr, slot) !== 0) {
      return true;
    }
  }
  return false;
}
