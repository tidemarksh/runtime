import {
  PIPE_GENERATION_OFFSET as PIPE_DEBUG_GENERATION_OFFSET,
  PIPE_HEAD_OFFSET as PIPE_DEBUG_HEAD_OFFSET,
  PIPE_HEADER_BYTES as PIPE_DEBUG_HEADER_BYTES,
  PIPE_READ_OPEN_OFFSET as PIPE_DEBUG_READ_OPEN_OFFSET,
  PIPE_TAIL_OFFSET as PIPE_DEBUG_TAIL_OFFSET,
  PIPE_WRITE_OPEN_OFFSET as PIPE_DEBUG_WRITE_OPEN_OFFSET,
} from "../../layout/pipe.js";
import type { KernelWorkerPipeSlotSnapshot } from "../../messages.js";

export function int32AtomicView(buffer: ArrayBufferLike): Int32Array | null {
  return buffer instanceof SharedArrayBuffer ? new Int32Array(buffer) : null;
}

export function loadPipeU32(view: DataView, atomicView: Int32Array | null, byteOffset: number): number {
  if (atomicView !== null && byteOffset % 4 === 0) {
    return Atomics.load(atomicView, byteOffset / 4) >>> 0;
  }
  return view.getUint32(byteOffset, true);
}

export function loadPipeI32(view: DataView, atomicView: Int32Array | null, byteOffset: number): number {
  if (atomicView !== null && byteOffset % 4 === 0) {
    return Atomics.load(atomicView, byteOffset / 4);
  }
  return view.getInt32(byteOffset, true);
}

export function formatPipeSnapshotDebug(
  label: string,
  snapshots: readonly KernelWorkerPipeSlotSnapshot[],
  limit = 8,
): string {
  if (snapshots.length === 0) {
    return `${label}=none`;
  }
  return `${label}=${snapshots.slice(0, limit).map((snapshot) => {
    const view = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
    const data = snapshot.slot.subarray(PIPE_DEBUG_HEADER_BYTES, PIPE_DEBUG_HEADER_BYTES + 24);
    const ascii = [...data]
      .map((byte) => byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : ".")
      .join("");
    return [
      `slot${snapshot.pipeSlot}`,
      `m${snapshot.mode ?? -1}`,
      `f${snapshot.fresh ? 1 : 0}`,
      `h${view.getUint32(PIPE_DEBUG_HEAD_OFFSET, true)}`,
      `t${view.getUint32(PIPE_DEBUG_TAIL_OFFSET, true)}`,
      `ro${view.getInt32(PIPE_DEBUG_READ_OPEN_OFFSET, true)}`,
      `wo${view.getInt32(PIPE_DEBUG_WRITE_OPEN_OFFSET, true)}`,
      `g${view.getUint32(PIPE_DEBUG_GENERATION_OFFSET, true)}`,
      `d=${JSON.stringify(ascii)}`,
    ].join("/");
  }).join(",")}${snapshots.length > limit ? `,+${snapshots.length - limit}` : ""}`;
}
