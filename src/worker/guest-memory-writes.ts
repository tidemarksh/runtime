import type { GuestMemoryWriteSnapshot } from "../messages.js";
import type { ThreadWorkerState } from "./types.js";

function endOf(write: GuestMemoryWriteSnapshot): number {
  return write.guestPtr + write.bytes.length;
}

function cloneWrite(write: GuestMemoryWriteSnapshot): GuestMemoryWriteSnapshot {
  return {
    guestPtr: write.guestPtr,
    bytes: write.bytes.slice(),
  };
}

function mergeWrites(
  left: GuestMemoryWriteSnapshot,
  right: GuestMemoryWriteSnapshot,
): GuestMemoryWriteSnapshot | null {
  const leftEnd = endOf(left);
  const rightEnd = endOf(right);
  if (right.guestPtr > leftEnd || left.guestPtr > rightEnd) {
    return null;
  }
  const start = Math.min(left.guestPtr, right.guestPtr);
  const end = Math.max(leftEnd, rightEnd);
  const bytes = new Uint8Array(end - start);
  bytes.set(left.bytes, left.guestPtr - start);
  bytes.set(right.bytes, right.guestPtr - start);
  return { guestPtr: start, bytes };
}

export function appendPendingGuestMemoryWrites(
  thread: ThreadWorkerState,
  writes: readonly GuestMemoryWriteSnapshot[],
): void {
  for (const write of writes) {
    if (write.bytes.length === 0 || !Number.isFinite(write.guestPtr)) {
      continue;
    }
    let next = cloneWrite(write);
    for (let index = 0; index < thread.pendingGuestMemoryWrites.length;) {
      const merged = mergeWrites(thread.pendingGuestMemoryWrites[index]!, next);
      if (merged === null) {
        index += 1;
        continue;
      }
      next = merged;
      thread.pendingGuestMemoryWrites.splice(index, 1);
    }
    thread.pendingGuestMemoryWrites.push(next);
    thread.pendingGuestMemoryWrites.sort((left, right) => left.guestPtr - right.guestPtr);
  }
}
