import type { ProcessHandle } from "./types.js";

const pendingChildSpawnRecords = new WeakMap<ProcessHandle, number>();

export function pendingChildSpawnRecordCount(handle: ProcessHandle): number {
  return pendingChildSpawnRecords.get(handle) ?? 0;
}

export function hasPendingChildSpawnRecords(handle: ProcessHandle): boolean {
  return pendingChildSpawnRecordCount(handle) > 0;
}

export function setPendingChildSpawnRecords(handle: ProcessHandle, count: number): void {
  if (count <= 0) {
    pendingChildSpawnRecords.delete(handle);
    return;
  }
  pendingChildSpawnRecords.set(handle, count);
}

export function beginPendingChildSpawnRecord(handle: ProcessHandle): number {
  const count = pendingChildSpawnRecordCount(handle) + 1;
  pendingChildSpawnRecords.set(handle, count);
  return count;
}

export function finishPendingChildSpawnRecord(handle: ProcessHandle): number {
  const count = pendingChildSpawnRecordCount(handle);
  if (count <= 1) {
    pendingChildSpawnRecords.delete(handle);
    return 0;
  }
  const nextCount = count - 1;
  pendingChildSpawnRecords.set(handle, nextCount);
  return nextCount;
}
