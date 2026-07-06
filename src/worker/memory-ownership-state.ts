import type { ProcessHandle, RuntimeMemoryOwnership } from "./types.js";

const memoryOwnershipStates = new WeakMap<ProcessHandle, RuntimeMemoryOwnership>();

export function processMemoryOwnership(handle: ProcessHandle): RuntimeMemoryOwnership | undefined {
  return memoryOwnershipStates.get(handle);
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
