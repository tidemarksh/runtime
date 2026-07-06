import type { GuestWriteWatchRegion } from "../messages.js";
export { appendPendingGuestMemoryWrites } from "./thread-step-payload.js";

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
