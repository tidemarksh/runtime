import { ensureMemoryCapacity } from "../kernel-scratch.js";
import type {
  KernelShareGroup,
  KernelSyncHandle,
  ProcessHandle,
} from "../types.js";

const kernelShareGroups = new WeakMap<ProcessHandle, KernelShareGroup>();
const PROCESS_KERNEL_SHARE_GROUP = Symbol.for("tidemark.worker.kernelShareGroup");

type LegacyKernelShareSeed = {
  kernelShare?: KernelShareGroup;
};

type KernelShareStateCarrier = ProcessHandle & {
  [PROCESS_KERNEL_SHARE_GROUP]?: KernelShareGroup;
};

function consumeLegacyKernelShareGroup(handle: ProcessHandle): KernelShareGroup | null {
  const legacy = handle as LegacyKernelShareSeed;
  if (legacy.kernelShare === undefined) {
    return null;
  }
  const group = legacy.kernelShare;
  delete legacy.kernelShare;
  return group;
}

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

export function processKernelShareGroup(handle: ProcessHandle): KernelShareGroup {
  const group = kernelShareGroups.get(handle);
  if (group !== undefined) {
    return group;
  }
  const carrier = handle as KernelShareStateCarrier;
  if (Object.prototype.hasOwnProperty.call(carrier, PROCESS_KERNEL_SHARE_GROUP)) {
    const sharedGroup = carrier[PROCESS_KERNEL_SHARE_GROUP];
    if (sharedGroup !== undefined) {
      kernelShareGroups.set(handle, sharedGroup);
      return sharedGroup;
    }
  }
  const legacy = consumeLegacyKernelShareGroup(handle);
  if (legacy !== null) {
    setProcessKernelShareGroup(handle, legacy);
    return legacy;
  }
  const created = createKernelShareGroup();
  setProcessKernelShareGroup(handle, created);
  return created;
}

export function setProcessKernelShareGroup(handle: ProcessHandle, group: KernelShareGroup): void {
  consumeLegacyKernelShareGroup(handle);
  kernelShareGroups.set(handle, group);
  Object.defineProperty(handle, PROCESS_KERNEL_SHARE_GROUP, {
    configurable: true,
    enumerable: false,
    value: group,
    writable: true,
  });
}

export function addKernelShareMember(handle: ProcessHandle, member: ProcessHandle = handle): void {
  processKernelShareGroup(handle).members.add(member);
}

export function deleteKernelShareMember(handle: ProcessHandle, member: ProcessHandle = handle): void {
  processKernelShareGroup(handle).members.delete(member);
}

export function kernelShareMembers(handle: ProcessHandle): Set<ProcessHandle> {
  return processKernelShareGroup(handle).members;
}

export function kernelShareMemberList(handle: ProcessHandle): ProcessHandle[] {
  return [...kernelShareMembers(handle)];
}

export function kernelShareRepresentative(handle: ProcessHandle): ProcessHandle | null {
  const representative = kernelShareMembers(handle).values().next().value as ProcessHandle | undefined;
  return representative ?? null;
}

export function kernelShareOtherMembers(handle: ProcessHandle): ProcessHandle[] {
  return kernelShareMemberList(handle).filter((member) => member !== handle);
}

export function kernelShareSharedOfdIds(handle: ProcessHandle): Set<number> {
  return processKernelShareGroup(handle).sharedOfdIds;
}

export function kernelShareSharedOfdIdList(handle: ProcessHandle): number[] {
  return [...kernelShareSharedOfdIds(handle)];
}

export function addKernelShareSharedOfdId(handle: ProcessHandle, ofdId: number): void {
  kernelShareSharedOfdIds(handle).add(ofdId);
}

export function deleteKernelShareSharedOfdId(handle: ProcessHandle, ofdId: number): void {
  kernelShareSharedOfdIds(handle).delete(ofdId);
}

export function kernelShareHasSharedOfdId(handle: ProcessHandle, ofdId: number): boolean {
  return kernelShareSharedOfdIds(handle).has(ofdId);
}

export function setKernelShareOfdSlotSnapshot(
  handle: ProcessHandle,
  ofdId: number,
  slot: Uint8Array,
): void {
  processKernelShareGroup(handle).ofdSlotSnapshots.set(ofdId, slot);
}

export function getKernelShareOfdSlotSnapshot(handle: ProcessHandle, ofdId: number): Uint8Array | undefined {
  return processKernelShareGroup(handle).ofdSlotSnapshots.get(ofdId);
}

export function deleteKernelShareOfdSlotSnapshot(handle: ProcessHandle, ofdId: number): void {
  processKernelShareGroup(handle).ofdSlotSnapshots.delete(ofdId);
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
