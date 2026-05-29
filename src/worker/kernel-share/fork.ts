import type { ProcessHandle } from "../types.js";

export function adoptKernelShare(parent: ProcessHandle, child: ProcessHandle): void {
  child.kernelShare = parent.kernelShare;
  child.kernelShare.members.add(child);
}

export function detachKernelShare(handle: ProcessHandle): void {
  handle.kernelShare.members.delete(handle);
}
