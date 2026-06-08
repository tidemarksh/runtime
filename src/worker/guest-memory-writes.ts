import type { GuestMemoryWriteSnapshot, GuestWriteWatchRegion } from "../messages.js";
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

export function formatGuestMemoryWriteWatchOverlap(
  source: string,
  regions: readonly GuestWriteWatchRegion[] | undefined,
  guestPtr: number,
  bytes: Uint8Array,
): string | null {
  if (!regions || regions.length === 0 || bytes.length === 0 || !Number.isFinite(guestPtr)) {
    return null;
  }
  const writeEnd = guestPtr + bytes.length;
  for (const region of regions) {
    const regionEnd = region.end;
    const overlapStart = Math.max(guestPtr, region.start);
    const overlapEnd = Math.min(writeEnd, regionEnd);
    if (overlapStart >= overlapEnd) {
      continue;
    }
    const sampleStart = Math.max(0, overlapStart - guestPtr);
    const sampleEnd = Math.min(bytes.length, sampleStart + 16, overlapEnd - guestPtr);
    const sample = Array.from(bytes.subarray(sampleStart, sampleEnd))
      .map((value) => value.toString(16).padStart(2, "0"))
      .join("");
    return `${source}:slot=${region.slot}:ptr=0x${guestPtr.toString(16)}`
      + `:len=${bytes.length}:overlap=0x${overlapStart.toString(16)}`
      + `..0x${overlapEnd.toString(16)}:sample=${sample}`;
  }
  return null;
}
