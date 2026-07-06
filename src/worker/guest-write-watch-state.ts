import type { GuestWriteWatchRegion } from "../messages.js";
import type { ProcessHandle } from "./types.js";

const guestWriteWatchStates = new WeakMap<ProcessHandle, GuestWriteWatchRegion[]>();

export function guestWriteWatchRegions(handle: ProcessHandle): GuestWriteWatchRegion[] {
  return guestWriteWatchStates.get(handle) ?? [];
}

export function setGuestWriteWatchRegions(
  handle: ProcessHandle,
  regions: readonly GuestWriteWatchRegion[],
): void {
  const next = regions.map((region) => ({ ...region }));
  if (next.length === 0) {
    guestWriteWatchStates.delete(handle);
    return;
  }
  guestWriteWatchStates.set(handle, next);
}

export function clearGuestWriteWatchRegions(handle: ProcessHandle): void {
  guestWriteWatchStates.delete(handle);
}

export function copyGuestWriteWatchRegions(target: ProcessHandle, source: ProcessHandle): void {
  setGuestWriteWatchRegions(target, guestWriteWatchRegions(source));
}
