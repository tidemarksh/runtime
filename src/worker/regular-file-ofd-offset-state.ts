import type { ProcessHandle } from "./types.js";
import { OFD_OFFSET_OFFSET } from "../layout/ofd.js";

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

export function deleteRegularFileOfdOffsetSnapshot(
  handle: ProcessHandle,
  ofdId: number,
): void {
  regularFileOfdOffsets.get(handle)?.delete(ofdId);
}

export function regularFileOfdOffsetSnapshotEntries(
  handle: ProcessHandle,
): Array<[number, RegularFileOfdOffsetSnapshot]> {
  return [...(regularFileOfdOffsets.get(handle)?.entries() ?? [])];
}

export function writeRegularFileOfdOffsetState(
  handle: ProcessHandle,
  ofdId: number,
  slotBytes: number,
  offset: bigint,
): void {
  const view = new DataView(handle.memory.buffer);
  view.setBigInt64(
    handle.kernelState.ofdTablePtr + ofdId * slotBytes + OFD_OFFSET_OFFSET,
    offset,
    true,
  );

  const floorPtr = handle.kernelState.ofdOffsetFloorPtr
    ?? handle.kernel.kernel_ofd_offset_floor_ptr?.();
  const floorBytes = handle.kernelState.ofdOffsetFloorBytes
    ?? handle.kernel.kernel_ofd_offset_floor_bytes?.();
  const floorOffset = (floorPtr ?? 0) + ofdId * BigUint64Array.BYTES_PER_ELEMENT;
  if (
    floorPtr === undefined
    || floorPtr <= 0
    || floorBytes === undefined
    || floorBytes < (ofdId + 1) * BigUint64Array.BYTES_PER_ELEMENT
    || floorOffset + BigUint64Array.BYTES_PER_ELEMENT > handle.memory.buffer.byteLength
  ) {
    return;
  }
  view.setBigUint64(floorOffset, BigInt.asUintN(64, offset), true);
}
