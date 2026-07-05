import {
  describeForkSharedSyncPromise,
  getForkSharedSyncDebugRecord,
} from "./fork-shared-sync-debug.js";
import { clearForkSharedSyncIfCurrent } from "./fork-shared-sync-state.js";
import type { ProcessHandle } from "./types.js";

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

function forkSharedSyncHasNoTargets(promise: Promise<void>): boolean {
  const record = getForkSharedSyncDebugRecord(promise);
  return record !== null && record.targetPids.length === 0;
}

export function clearStaleForkSharedSyncGate(
  handle: ProcessHandle,
  promise: Promise<void>,
): string | null {
  const sourceIsStale = forkSharedSyncSourceIsStale(handle, promise);
  const hasNoTargets = forkSharedSyncHasNoTargets(promise);
  if (!sourceIsStale && !hasNoTargets) {
    return null;
  }
  clearForkSharedSyncIfCurrent(handle, promise);
  if (sourceIsStale) {
    return `fork-shared-sync-stale-source:${describeForkSharedSyncPromise(promise)}`;
  }
  if (hasNoTargets) {
    return `fork-shared-sync-empty-targets:${describeForkSharedSyncPromise(promise)}`;
  }
  return `fork-shared-sync-stale-source:${describeForkSharedSyncPromise(promise)}`;
}
