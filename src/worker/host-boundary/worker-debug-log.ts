import type { ProcessHandle } from "../types.js";
import { workerDebugLog } from "../worker-debug-log.js";

const WORKER_DEBUG_LOG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_WORKER_DEBUG_LOG ?? "") === "1";

const ALWAYS_ON_SUPERVISOR_TRACE_PREFIXES = [
  "supervisor-blocked-retry-timer-arm",
  "supervisor-blocked-retry-timer-fired",
  "supervisor-timer-fired-event",
  "supervisor-host-timer-fired",
  "supervisor-thread-event",
  "supervisor-resume",
  "blocked-rpc-start",
  "blocked-rpc-stage",
  "blocked-rpc-done",
  "blocked-rpc-stale",
  "blocked-rpc-defer-pipe-supervisor",
  "pipe-retry-host-boundary",
  "blocked-retry-host-boundary",
  "epoll-retry-resume-host-boundary",
  "epoll-retry-resume-rpc",
];

function shouldRecordSupervisorTrace(event: string): boolean {
  return ALWAYS_ON_SUPERVISOR_TRACE_PREFIXES.some((prefix) => event.startsWith(`${prefix}:`));
}

export function appendWorkerDebugLog(handle: ProcessHandle, event: string): void {
  if (!WORKER_DEBUG_LOG_ENABLED && !shouldRecordSupervisorTrace(event)) {
    return;
  }
  workerDebugLog(handle).push(event);
  if (workerDebugLog(handle).length > 4096) {
    workerDebugLog(handle).splice(0, workerDebugLog(handle).length - 4096);
  }
}
