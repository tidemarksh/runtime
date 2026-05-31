/**
 * SAB ring buffer for FS request/response between thread-worker and kernel Worker.
 *
 * SharedArrayBuffer layout matches kernel/src/fs/ring_buffer.rs.
 * Thread-worker submits requests, kernel Worker processes them.
 * Communication is via Atomics on the SharedArrayBuffer.
 */

import {
  FS_RING_COMPLETE_HEAD_OFFSET,
  FS_RING_COMPLETE_QUEUE_OFFSET,
  FS_RING_COMPLETE_QUEUE_SIZE,
  FS_RING_COMPLETE_TAIL_OFFSET,
  FS_RING_HEADER_BYTES,
  FS_RING_NOTIFY_OFFSET,
  FS_RING_REQUEST_BYTES,
  FS_RING_RESPONSE_BYTES,
  FS_RING_SUBMIT_HEAD_OFFSET,
  FS_RING_SUBMIT_QUEUE_SIZE,
  FS_RING_SUBMIT_TAIL_OFFSET,
  FS_RING_TOTAL_BYTES,
} from "../layout/fs-ring.js";

const SQ_SIZE = FS_RING_SUBMIT_QUEUE_SIZE;
const CQ_SIZE = FS_RING_COMPLETE_QUEUE_SIZE;
const REQ_SIZE = FS_RING_REQUEST_BYTES;
const RESP_SIZE = FS_RING_RESPONSE_BYTES;
export const RING_BUFFER_SIZE = FS_RING_TOTAL_BYTES;

// Header offsets (exported for kernel Worker integration)
export const OFF_SUBMIT_HEAD = FS_RING_SUBMIT_HEAD_OFFSET;
export const OFF_SUBMIT_TAIL = FS_RING_SUBMIT_TAIL_OFFSET;
export const OFF_COMPLETE_HEAD = FS_RING_COMPLETE_HEAD_OFFSET;
export const OFF_COMPLETE_TAIL = FS_RING_COMPLETE_TAIL_OFFSET;
export const OFF_NOTIFY = FS_RING_NOTIFY_OFFSET;
export const OFF_SQ_BASE = FS_RING_HEADER_BYTES;
export const OFF_CQ_BASE = FS_RING_COMPLETE_QUEUE_OFFSET;

/**
 * Create a SharedArrayBuffer for the FS ring buffer.
 */
export function createFsRingBuffer(): SharedArrayBuffer {
  return new SharedArrayBuffer(RING_BUFFER_SIZE);
}

/**
 * Wait for a notification from the kernel Worker (thread-worker side).
 * Blocks until the kernel Worker signals completion.
 */
export function waitForCompletion(sab: SharedArrayBuffer, baseOffset: number): void {
  const i32 = new Int32Array(sab, baseOffset);
  const notifyIdx = OFF_NOTIFY / 4;

  // Wait until notify word becomes non-zero
  while (Atomics.load(i32, notifyIdx) === 0) {
    Atomics.wait(i32, notifyIdx, 0);
  }
  // Reset notify word
  Atomics.store(i32, notifyIdx, 0);
}

/**
 * Signal the thread-worker that completions are available (kernel Worker side).
 */
export function notifyCompletion(sab: SharedArrayBuffer, baseOffset: number): void {
  const i32 = new Int32Array(sab, baseOffset);
  const notifyIdx = OFF_NOTIFY / 4;
  Atomics.store(i32, notifyIdx, 1);
  Atomics.notify(i32, notifyIdx);
}

/**
 * Read the number of pending completions.
 */
export function pendingCompletions(sab: SharedArrayBuffer, baseOffset: number): number {
  const u32 = new Uint32Array(sab, baseOffset);
  const head = Atomics.load(u32, OFF_COMPLETE_HEAD / 4);
  const tail = Atomics.load(u32, OFF_COMPLETE_TAIL / 4);
  return (head - tail) >>> 0;
}

/**
 * Read a completion result from the ring buffer (thread-worker side).
 * Returns the result (i64 as number), or null if no completions.
 */
export function readCompletion(sab: SharedArrayBuffer, baseOffset: number): number | null {
  const u32 = new Uint32Array(sab, baseOffset);
  const head = Atomics.load(u32, OFF_COMPLETE_HEAD / 4);
  const tail = Atomics.load(u32, OFF_COMPLETE_TAIL / 4);

  if (head === tail) return null;

  const idx = tail % CQ_SIZE;
  const dv = new DataView(sab, baseOffset + OFF_CQ_BASE + idx * RESP_SIZE);
  const result = Number(dv.getBigInt64(0, true));

  Atomics.store(u32, OFF_COMPLETE_TAIL / 4, tail + 1);
  return result;
}
