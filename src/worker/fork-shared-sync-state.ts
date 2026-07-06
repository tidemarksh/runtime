import type {
  KernelShareGroup,
  ProcessHandle,
} from "./types.js";
import { processKernelShareGroup } from "./kernel-share.js";

const handleForkSharedSyncPromises = new WeakMap<ProcessHandle, Promise<void>>();
const kernelShareForkSharedSyncPromises = new WeakMap<KernelShareGroup, Promise<void>>();

export function getHandleForkSharedSync(
  handle: ProcessHandle,
): Promise<void> | null {
  return handleForkSharedSyncPromises.get(handle) ?? null;
}

export function setHandleForkSharedSync(
  handle: ProcessHandle,
  promise: Promise<void> | null,
): void {
  if (promise === null) {
    handleForkSharedSyncPromises.delete(handle);
    return;
  }
  handleForkSharedSyncPromises.set(handle, promise);
}

export function getKernelShareForkSharedSync(
  kernelShare: KernelShareGroup,
): Promise<void> | null {
  return kernelShareForkSharedSyncPromises.get(kernelShare) ?? null;
}

export function setKernelShareForkSharedSync(
  kernelShare: KernelShareGroup,
  promise: Promise<void> | null,
): void {
  if (promise === null) {
    kernelShareForkSharedSyncPromises.delete(kernelShare);
    return;
  }
  kernelShareForkSharedSyncPromises.set(kernelShare, promise);
}

export function getProcessKernelShareForkSharedSync(
  handle: ProcessHandle,
): Promise<void> | null {
  return getKernelShareForkSharedSync(processKernelShareGroup(handle));
}

export function setProcessKernelShareForkSharedSync(
  handle: ProcessHandle,
  promise: Promise<void> | null,
): void {
  setKernelShareForkSharedSync(processKernelShareGroup(handle), promise);
}

export function getActiveForkSharedSync(
  handle: ProcessHandle,
): Promise<void> | null {
  return getProcessKernelShareForkSharedSync(handle) ?? getHandleForkSharedSync(handle);
}

export function setActiveForkSharedSync(
  handle: ProcessHandle,
  promise: Promise<void>,
): void {
  setHandleForkSharedSync(handle, promise);
  setProcessKernelShareForkSharedSync(handle, promise);
}

export function clearForkSharedSyncIfCurrent(
  handle: ProcessHandle,
  promise: Promise<void>,
): void {
  if (getHandleForkSharedSync(handle) === promise) {
    setHandleForkSharedSync(handle, null);
  }
  if (getProcessKernelShareForkSharedSync(handle) === promise) {
    setProcessKernelShareForkSharedSync(handle, null);
  }
}
