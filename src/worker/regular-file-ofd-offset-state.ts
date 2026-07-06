import type { ProcessHandle } from "./types.js";

export interface RegularFileOfdOffsetSnapshot {
  ino: number;
  hostFd: number;
  offset: bigint;
}

const regularFileOfdOffsets = new WeakMap<ProcessHandle, Map<number, RegularFileOfdOffsetSnapshot>>();

function offsetSnapshots(handle: ProcessHandle): Map<number, RegularFileOfdOffsetSnapshot> {
  let snapshots = regularFileOfdOffsets.get(handle);
  if (!snapshots) {
    snapshots = new Map();
    regularFileOfdOffsets.set(handle, snapshots);
  }
  return snapshots;
}

export function recordRegularFileOfdOffsetSnapshot(
  handle: ProcessHandle,
  ofdId: number,
  snapshot: RegularFileOfdOffsetSnapshot,
): void {
  offsetSnapshots(handle).set(ofdId, snapshot);
}

export function regularFileOfdOffsetSnapshotEntries(
  handle: ProcessHandle,
): Array<[number, RegularFileOfdOffsetSnapshot]> {
  return [...(regularFileOfdOffsets.get(handle)?.entries() ?? [])];
}
