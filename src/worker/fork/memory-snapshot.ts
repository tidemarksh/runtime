import type { ThreadWorkerMemoryPreserveRange } from "../../messages.js";
import { supervisorProcessIdentityMatches } from "../process-identity.js";
import type { ProcessHandle, ThreadWorkerState } from "./types.js";
import { getThreadActivityState } from "../thread-activity-state.js";

export type ParentProcessSnapshot = {
  processPtr: number;
  processState: Uint8Array;
  pid: number;
  ppid: number;
};

export type MemoryRange = {
  start: number;
  end: number;
};

export type ForkMemorySnapshot = {
  ranges: Array<MemoryRange & { bytes: Uint8Array }>;
  endOffset: number;
};

export function forkMemoryPreserveRanges(snapshot: ForkMemorySnapshot): ThreadWorkerMemoryPreserveRange[] {
  return snapshot.ranges.map((range) => ({
    start: range.start,
    end: range.end,
  }));
}

export function captureParentProcessSnapshot(
  handle: ProcessHandle,
  parentThread: ThreadWorkerState,
): ParentProcessSnapshot {
  const activity = getThreadActivityState(parentThread);
  const processPtr = activity.lastStepProcessPtr ?? handle.kernel.thread_process_ptr(parentThread.vmPtr);
  const processStateBytes = handle.kernel.process_state_bytes();
  const guestBytes = new Uint8Array(handle.memory.buffer);
  return {
    processPtr,
    processState: guestBytes.slice(
      handle.guestBase + processPtr,
      handle.guestBase + processPtr + processStateBytes,
    ),
    pid: handle.pid,
    ppid: handle.parentPid ?? 0,
  };
}

export function restoreParentProcessSnapshot(
  handle: ProcessHandle,
  parentThread: ThreadWorkerState,
  snapshot: ParentProcessSnapshot,
): void {
  new Uint8Array(handle.memory.buffer).set(
    snapshot.processState,
    handle.guestBase + snapshot.processPtr,
  );
  handle.kernel.thread_clear_spawn_handoff(parentThread.vmPtr);
  handle.kernel.thread_restore_fork_parent?.(
    parentThread.vmPtr,
    snapshot.processPtr,
    snapshot.pid,
    snapshot.ppid,
  );
  if (!supervisorProcessIdentityMatches(handle.kernel, parentThread.vmPtr, snapshot.pid, snapshot.processPtr)) {
    const currentPid = handle.kernel.process_current_pid(parentThread.vmPtr);
    const currentProcessPtr = handle.kernel.thread_process_ptr(parentThread.vmPtr);
    throw new Error(
      `plain-fork parent restore drift vm=0x${parentThread.vmPtr.toString(16)}`
        + ` expected_pid=${snapshot.pid} actual_pid=${currentPid}`
        + ` expected_process_ptr=0x${snapshot.processPtr.toString(16)}`
        + ` actual_process_ptr=0x${currentProcessPtr.toString(16)}`,
    );
  }
}

export function captureForkMemorySnapshotFromRanges(
  handle: ProcessHandle,
  ranges: Array<MemoryRange>,
): ForkMemorySnapshot {
  const source = new Uint8Array(handle.memory.buffer);
  return {
    ranges: ranges.map((range) => {
      if (range.start < 0 || range.end > source.byteLength || range.end <= range.start) {
        throw new Error(`supervisor fork memory range out of bounds: ${range.start}-${range.end}`);
      }
      return {
        ...range,
        bytes: source.slice(range.start, range.end),
      };
    }),
    endOffset: ranges.reduce((end, range) => Math.max(end, range.end), 0),
  };
}

export function applyForkMemorySnapshot(snapshot: ForkMemorySnapshot, target: WebAssembly.Memory): void {
  const targetBytes = new Uint8Array(target.buffer);
  for (const range of snapshot.ranges) {
    targetBytes.set(range.bytes, range.start);
  }
}

export function overwriteForkMemorySnapshotStrict(
  snapshot: ForkMemorySnapshot,
  start: number,
  bytes: Uint8Array,
  context: string,
): void {
  const end = start + bytes.byteLength;
  for (const range of snapshot.ranges) {
    if (start < range.start || end > range.end) {
      continue;
    }
    range.bytes.set(bytes, start - range.start);
    return;
  }
  throw new Error(`supervisor fork memory plan missing ${context} range: ${start}-${end}`);
}

function forkSnapshotCopiedBytes(snapshot: ForkMemorySnapshot): number {
  return snapshot.ranges.reduce((total, range) => total + range.bytes.byteLength, 0);
}

function formatForkSnapshotRanges(snapshot: ForkMemorySnapshot): string {
  return snapshot.ranges
    .map((range) => `${range.start}-${range.end}:${range.bytes.byteLength}`)
    .join(",");
}

function formatNullableBytes(value: number | bigint | string | null | undefined): string {
  return value === null || value === undefined ? "null" : String(value);
}

export function appendPlainForkMemoryDebug(
  handle: ProcessHandle,
  childPid: number,
  snapshot: ForkMemorySnapshot,
  childMemoryBytes: number,
): void {
  const ownership = handle.memoryOwnership;
  const processCommittedBytes = Math.max(0, handle.memory.buffer.byteLength - handle.guestBase);
  handle.workerDebugLog.push([
    "plain-fork-memory",
    `parent=${handle.pid}`,
    `child=${childPid}`,
    `parentMemoryBytes=${handle.memory.buffer.byteLength}`,
    `childInitialBytes=${childMemoryBytes}`,
    `snapshotRanges=${snapshot.ranges.length}`,
    `snapshotBytes=${forkSnapshotCopiedBytes(snapshot)}`,
    `snapshotEnd=${snapshot.endOffset}`,
    `snapshotRangeSpans=${formatForkSnapshotRanges(snapshot)}`,
    `guestBase=${handle.guestBase}`,
    `guestMemoryPolicyLimitBytes=${handle.guestMemoryPolicyLimitBytes}`,
    `guestBackingBytes=${Math.max(0, handle.memory.buffer.byteLength - handle.guestBase)}`,
    `guestAddressProfile=${formatNullableBytes(ownership?.guestAddressProfile)}`,
    `guestAddressLimitBytes=${formatNullableBytes(ownership?.guestAddressLimitBytes)}`,
    `guestMemoryPolicyLimitBytes=${formatNullableBytes(ownership?.guestMemoryPolicyLimitBytes)}`,
    `processCommittedBytes=${processCommittedBytes}`,
    `processMaxMemoryBytes=${handle.maxMemoryPages * 65536}`,
    `forkProcessMaxMemoryBytes=${handle.forkMaxMemoryPages * 65536}`,
    `pageCacheDataCapacityBytes=${formatNullableBytes(ownership?.pageCacheDataCapacityBytes ?? handle.sabPageCache?.byteLength ?? 0)}`,
    `publishedMetadataCapacityBytes=${formatNullableBytes(ownership?.publishedMetadataCapacityBytes)}`,
    `fsRpcRingCapacityBytes=${formatNullableBytes(ownership?.fsRpcRingCapacityBytes)}`,
    `kernelWorkerPrivateMemoryBytes=${formatNullableBytes(ownership?.kernelWorkerPrivateMemoryBytes)}`,
    `artifactMaterializationBudgetBytes=${formatNullableBytes(ownership?.artifactMaterializationBudgetBytes)}`,
    `artifactMaterializedBytes=${formatNullableBytes(ownership?.artifactMaterializedBytes)}`,
  ].join(":"));
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
}
