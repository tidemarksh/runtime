import { ensureMemoryCapacity } from "../kernel-scratch.js";
import type {
  KernelShareGroup,
  KernelSyncHandle,
  ProcessHandle,
} from "../types.js";

export function copyMemoryRange(
  sourceMemory: WebAssembly.Memory,
  sourceOffset: number,
  targetMemory: WebAssembly.Memory,
  targetOffset: number,
  byteLength: number,
): void {
  if (byteLength <= 0) {
    return;
  }
  ensureMemoryCapacity(targetMemory, targetOffset + byteLength);
  const source = new Uint8Array(sourceMemory.buffer, sourceOffset, byteLength);
  new Uint8Array(targetMemory.buffer).set(source, targetOffset);
}

export function createKernelShareGroup(): KernelShareGroup {
  return {
    members: new Set(),
    sharedOfdIds: new Set(),
    ofdSlotSnapshots: new Map(),
  };
}

export function kernelShareRepresentative(handle: ProcessHandle): ProcessHandle | null {
  const representative = handle.kernelShare.members.values().next().value as ProcessHandle | undefined;
  return representative ?? null;
}

export function kernelShareOtherMembers(handle: ProcessHandle): ProcessHandle[] {
  return [...handle.kernelShare.members].filter((member) => member !== handle);
}

export function copyKernelRegion(
  source: KernelSyncHandle,
  sourceOffset: number,
  target: KernelSyncHandle,
  targetOffset: number,
  byteLength: number,
): void {
  copyMemoryRange(source.memory, sourceOffset, target.memory, targetOffset, byteLength);
}

export function clearKernelRegion(
  target: KernelSyncHandle,
  targetOffset: number,
  byteLength: number,
): void {
  if (byteLength <= 0 || targetOffset < 0) {
    return;
  }
  const available = target.memory.buffer.byteLength - targetOffset;
  if (available <= 0) {
    return;
  }
  new Uint8Array(target.memory.buffer, targetOffset, Math.min(byteLength, available)).fill(0);
}
