import type { ProcessHandle } from "./types.js";

const stdinFlushPromises = new WeakMap<ProcessHandle, Promise<void>>();

type LegacyStdinFlushSeed = {
  stdinFlushPromise?: Promise<void> | null;
};

export function stdinFlushPromise(handle: ProcessHandle): Promise<void> | null {
  const promise = stdinFlushPromises.get(handle);
  if (promise !== undefined) {
    return promise;
  }
  const legacyPromise = (handle as LegacyStdinFlushSeed).stdinFlushPromise;
  if (legacyPromise !== undefined) {
    delete (handle as LegacyStdinFlushSeed).stdinFlushPromise;
    if (legacyPromise !== null) {
      stdinFlushPromises.set(handle, legacyPromise);
    }
    return legacyPromise;
  }
  return null;
}

export function setStdinFlushPromise(
  handle: ProcessHandle,
  promise: Promise<void> | null,
): void {
  delete (handle as LegacyStdinFlushSeed).stdinFlushPromise;
  if (promise === null) {
    stdinFlushPromises.delete(handle);
    return;
  }
  stdinFlushPromises.set(handle, promise);
}
