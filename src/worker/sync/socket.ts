import type { ProcessHandle } from "../types.js";

export const socketSyncDebugLogEnabled = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_WORKER_DEBUG_LOG ?? "") === "1"
  || ((globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_WORKER_DEBUG_NETWORK ?? "") === "1";

export function appendSocketSyncDebug(handle: ProcessHandle, event: string): void {
  if (!socketSyncDebugLogEnabled) {
    return;
  }
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
}
