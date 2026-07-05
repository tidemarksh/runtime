import type { ProcessHandle } from "./types.js";

const workerDebugLogs = new WeakMap<ProcessHandle, string[]>();

type LegacyWorkerDebugLogSeed = {
  workerDebugLog?: string[];
};

export function workerDebugLog(handle: ProcessHandle): string[] {
  let log = workerDebugLogs.get(handle);
  if (!log) {
    const legacyLog = (handle as LegacyWorkerDebugLogSeed).workerDebugLog;
    log = Array.isArray(legacyLog) ? legacyLog : [];
    workerDebugLogs.set(handle, log);
    if (legacyLog !== undefined) {
      delete (handle as LegacyWorkerDebugLogSeed).workerDebugLog;
    }
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
