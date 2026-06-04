import {
  describeForkSharedSyncPromise,
  getForkSharedSyncDebugRecord,
} from "../fork-shared-sync-debug.js";
import type { ProcessHandle } from "../types.js";

function forkSharedSyncSourceIsStale(handle: ProcessHandle, promise: Promise<void>): boolean {
  const record = getForkSharedSyncDebugRecord(promise);
  if (record === null) {
    return false;
  }
  const owner = [...handle.kernelShare.members].find((member) => member.pid === record.ownerPid);
  if (owner === undefined) {
    return true;
  }
  try {
    return owner.kernel.thread_process_ptr(record.sourceVmPtr) === 0;
  } catch {
    return false;
  }
}

export function clearStaleForkSharedSyncGate(
  handle: ProcessHandle,
  promise: Promise<void>,
): string | null {
  if (!forkSharedSyncSourceIsStale(handle, promise)) {
    return null;
  }
  if (handle.kernelShare.forkSharedSyncPromise === promise) {
    handle.kernelShare.forkSharedSyncPromise = null;
  }
  if (handle.forkSharedSyncPromise === promise) {
    handle.forkSharedSyncPromise = null;
  }
  return `fork-shared-sync-stale-source:${describeForkSharedSyncPromise(promise)}`;
}
