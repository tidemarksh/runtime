import type { ProcessHandle } from "./types.js";

const workerDebugLogs = new WeakMap<ProcessHandle, string[]>();

export function workerDebugLog(handle: ProcessHandle): string[] {
  let log = workerDebugLogs.get(handle);
  if (!log) {
    log = [];
    workerDebugLogs.set(handle, log);
  }
  return log;
}

export function appendWorkerDebugLogEntry(
  handle: ProcessHandle,
  event: string,
  maxEntries = 4096,
): void {
  const log = workerDebugLog(handle);
  log.push(event);
  if (log.length > maxEntries) {
    log.splice(0, log.length - maxEntries);
  }
}
