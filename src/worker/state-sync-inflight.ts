import type { ProcessHandle } from "./types.js";

export type RuntimeStateSyncKind = "pipe" | "epoll" | "socket";

const stateSyncPromises = new WeakMap<ProcessHandle, Map<RuntimeStateSyncKind, Promise<void>>>();

function promiseMap(handle: ProcessHandle): Map<RuntimeStateSyncKind, Promise<void>> {
  let promises = stateSyncPromises.get(handle);
  if (!promises) {
    promises = new Map();
    stateSyncPromises.set(handle, promises);
  }
  return promises;
}

export function getRuntimeStateSyncPromise(
  handle: ProcessHandle,
  kind: RuntimeStateSyncKind,
): Promise<void> | null {
  return stateSyncPromises.get(handle)?.get(kind) ?? null;
}

export function setRuntimeStateSyncPromise(
  handle: ProcessHandle,
  kind: RuntimeStateSyncKind,
  promise: Promise<void>,
): void {
  promiseMap(handle).set(kind, promise);
}

export function clearRuntimeStateSyncPromise(
  handle: ProcessHandle,
  kind: RuntimeStateSyncKind,
): void {
  const promises = stateSyncPromises.get(handle);
  if (!promises) {
    return;
  }
  promises.delete(kind);
  if (promises.size === 0) {
    stateSyncPromises.delete(handle);
  }
}

export function runtimeStateSyncInFlight(
  handle: ProcessHandle,
  kind: RuntimeStateSyncKind,
): boolean {
  return getRuntimeStateSyncPromise(handle, kind) !== null;
}
