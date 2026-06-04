export interface ForkSharedSyncDebugRecord {
  id: number;
  ownerPid: number;
  sourceVmPtr: number;
  reason: string;
  targetPids: number[];
}

const forkSharedSyncDebugRecords = new WeakMap<Promise<void>, ForkSharedSyncDebugRecord>();
let nextForkSharedSyncDebugId = 1;

export function registerForkSharedSyncPromise(
  promise: Promise<void>,
  record: Omit<ForkSharedSyncDebugRecord, "id">,
): number {
  const id = nextForkSharedSyncDebugId++;
  forkSharedSyncDebugRecords.set(promise, { ...record, id });
  return id;
}

export function describeForkSharedSyncPromise(promise: Promise<void>): string {
  const record = forkSharedSyncDebugRecords.get(promise);
  if (!record) {
    return "unknown";
  }
  return [
    `id=${record.id}`,
    `owner=${record.ownerPid}`,
    `src=0x${record.sourceVmPtr.toString(16)}`,
    `reason=${record.reason}`,
    `targets=${record.targetPids.join(".") || "none"}`,
  ].join(":");
}

export function getForkSharedSyncDebugRecord(promise: Promise<void>): ForkSharedSyncDebugRecord | null {
  return forkSharedSyncDebugRecords.get(promise) ?? null;
}
