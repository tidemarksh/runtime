import type { ProcessHandle } from "./types.js";

const stdinFlushPromises = new WeakMap<ProcessHandle, Promise<void>>();

export function stdinFlushPromise(handle: ProcessHandle): Promise<void> | null {
  return stdinFlushPromises.get(handle) ?? null;
}

export function setStdinFlushPromise(
  handle: ProcessHandle,
  promise: Promise<void> | null,
): void {
  if (promise === null) {
    stdinFlushPromises.delete(handle);
    return;
  }
  stdinFlushPromises.set(handle, promise);
}
