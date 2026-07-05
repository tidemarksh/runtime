import type { ProcessHandle, RuntimeMemoryOwnership } from "./types.js";

const memoryOwnershipStates = new WeakMap<ProcessHandle, RuntimeMemoryOwnership>();

type LegacyMemoryOwnershipSeed = {
  memoryOwnership?: RuntimeMemoryOwnership;
};

export function processMemoryOwnership(handle: ProcessHandle): RuntimeMemoryOwnership | undefined {
  const state = memoryOwnershipStates.get(handle);
  if (state !== undefined) {
    return state;
  }
  const legacyOwnership = (handle as LegacyMemoryOwnershipSeed).memoryOwnership;
  if (legacyOwnership !== undefined) {
    memoryOwnershipStates.set(handle, legacyOwnership);
    delete (handle as LegacyMemoryOwnershipSeed).memoryOwnership;
  }
  return legacyOwnership;
}

export function setProcessMemoryOwnership(
  handle: ProcessHandle,
  ownership: RuntimeMemoryOwnership | undefined,
): void {
  if (ownership === undefined) {
    memoryOwnershipStates.delete(handle);
    return;
  }
  memoryOwnershipStates.set(handle, ownership);
}

export function copyProcessMemoryOwnership(target: ProcessHandle, source: ProcessHandle): void {
  const ownership = processMemoryOwnership(source);
  setProcessMemoryOwnership(target, ownership === undefined ? undefined : { ...ownership });
}
