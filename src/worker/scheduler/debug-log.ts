import type { ProcessHandle } from "../types.js";
import { workerDebugLog } from "../worker-debug-log.js";

const WORKER_DEBUG_LOG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_WORKER_DEBUG_LOG ?? "") === "1";

export function appendWorkerDebugLog(handle: ProcessHandle, event: string): void {
  if (!WORKER_DEBUG_LOG_ENABLED) {
    return;
  }
  workerDebugLog(handle).push(event);
  if (workerDebugLog(handle).length > 4096) {
    workerDebugLog(handle).splice(0, workerDebugLog(handle).length - 4096);
  }
}
