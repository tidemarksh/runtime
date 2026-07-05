import type { KernelWorkerPipeSlotSnapshot } from "../messages.js";
import type { ProcessHandle } from "./types.js";

const pendingPipeSlotSnapshots = new WeakMap<ProcessHandle, KernelWorkerPipeSlotSnapshot[]>();

function pendingSnapshots(handle: ProcessHandle): KernelWorkerPipeSlotSnapshot[] {
  let snapshots = pendingPipeSlotSnapshots.get(handle);
  if (!snapshots) {
    snapshots = [];
    pendingPipeSlotSnapshots.set(handle, snapshots);
  }
  return snapshots;
}

function clearIfEmpty(handle: ProcessHandle, snapshots: KernelWorkerPipeSlotSnapshot[]): void {
  if (snapshots.length === 0) {
    pendingPipeSlotSnapshots.delete(handle);
  }
}

export function appendPendingPipeSlotSnapshots(
  handle: ProcessHandle,
  snapshots: readonly KernelWorkerPipeSlotSnapshot[],
): void {
  if (snapshots.length === 0) {
    return;
  }
  pendingSnapshots(handle).push(...snapshots);
}

export function replacePendingPipeSlotSnapshotForSlot(
  handle: ProcessHandle,
  snapshot: KernelWorkerPipeSlotSnapshot,
): void {
  const snapshots = pendingSnapshots(handle);
  const existingIndex = snapshots.findIndex((pending) => pending.pipeSlot === snapshot.pipeSlot);
  if (existingIndex >= 0) {
    snapshots.splice(existingIndex, 1);
  }
  snapshots.push(snapshot);
}

export function snapshotPendingPipeSlotSnapshots(
  handle: ProcessHandle,
): KernelWorkerPipeSlotSnapshot[] {
  return pendingPipeSlotSnapshots.get(handle)?.slice() ?? [];
}

export function pendingPipeSlotSnapshotCount(handle: ProcessHandle): number {
  return pendingPipeSlotSnapshots.get(handle)?.length ?? 0;
}

export function dropPendingPipeSlotSnapshotPrefix(
  handle: ProcessHandle,
  count: number,
): void {
  if (count <= 0) {
    return;
  }
  const snapshots = pendingPipeSlotSnapshots.get(handle);
  if (!snapshots) {
    return;
  }
  snapshots.splice(0, count);
  clearIfEmpty(handle, snapshots);
}
