import type { ProcessHandle } from "../types.js";

const FORK_SHARED_MEMFS_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_PUBLICATION_DEBUG ?? "") === "1";

export function appendForkSharedMemfsDebug(handle: ProcessHandle, event: string): void {
  if (!FORK_SHARED_MEMFS_DEBUG_ENABLED) {
    return;
  }
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
}
