import type { ThreadWorkerMemoryPreserveRange } from "../../messages.js";
import type { ProcessHandle, ThreadWorkerState } from "./types.js";

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

const PROT_NONE = 0;
const MAP_ANONYMOUS = 0x20;
const FORK_STACK_BELOW_SP_BYTES = 1024 * 1024;
export const FORK_STACK_COPY_WINDOW_BYTES = 8 * 1024 * 1024;

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
  const processPtr = parentThread.lastStepProcessPtr ?? handle.kernel.thread_process_ptr(parentThread.vmPtr);
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
  const currentPid = handle.kernel.process_current_pid(parentThread.vmPtr);
  const currentProcessPtr = handle.kernel.thread_process_ptr(parentThread.vmPtr);
  if (currentPid !== snapshot.pid || currentProcessPtr !== snapshot.processPtr) {
    throw new Error(
      `plain-fork parent restore drift vm=0x${parentThread.vmPtr.toString(16)}`
        + ` expected_pid=${snapshot.pid} actual_pid=${currentPid}`
        + ` expected_process_ptr=0x${snapshot.processPtr.toString(16)}`
        + ` actual_process_ptr=0x${currentProcessPtr.toString(16)}`,
    );
  }
}

function addForkCopyRange(ranges: MemoryRange[], start: number, end: number, limit: number): void {
  const boundedStart = Math.max(0, Math.min(start, limit));
  const boundedEnd = Math.max(boundedStart, Math.min(end, limit));
  if (boundedEnd > boundedStart) {
    ranges.push({ start: boundedStart, end: boundedEnd });
  }
}

function coalesceRanges(ranges: MemoryRange[]): MemoryRange[] {
  const sorted = ranges
    .filter((range) => range.end > range.start)
    .sort((left, right) => left.start - right.start || left.end - right.end);
  const coalesced: MemoryRange[] = [];
  for (const range of sorted) {
    const last = coalesced[coalesced.length - 1];
    if (last && range.start <= last.end) {
      last.end = Math.max(last.end, range.end);
    } else {
      coalesced.push({ ...range });
    }
  }
  return coalesced;
}

function isSparseAnonymousReservation(handle: ProcessHandle, index: number): boolean {
  const prot = handle.kernel.debug_memory_vma_prot?.(index);
  const flags = handle.kernel.debug_memory_vma_flags?.(index);
  const fd = handle.kernel.debug_memory_vma_fd?.(index);
  if (prot === undefined || flags === undefined || fd === undefined) {
    return false;
  }
  return prot === PROT_NONE && fd < 0 && (flags & MAP_ANONYMOUS) !== 0;
}

export function captureForkMemorySnapshot(handle: ProcessHandle, stackPointer = 0n): ForkMemorySnapshot {
  const source = new Uint8Array(handle.memory.buffer);
  const ranges: MemoryRange[] = [];
  const limit = source.byteLength;
  addForkCopyRange(ranges, 0, Math.max(handle.kernelState.heapPtr, handle.kernel.kernel_heap_ptr()), limit);
  addForkCopyRange(
    ranges,
    handle.kernelState.ofdTablePtr,
    handle.kernelState.ofdTablePtr + handle.kernel.kernel_ofd_table_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.memfsPtr,
    handle.kernelState.memfsPtr
      + handle.kernelState.memfsHeaderBytes
      + handle.kernelState.memfsInodesBytes
      + handle.kernelState.memfsFirstChildBytes
      + handle.kernelState.memfsDirEntriesBytes,
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernel.kernel_memfs_ptr(),
    handle.kernel.kernel_memfs_ptr() + handle.kernel.kernel_memfs_header_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernel.kernel_memfs_inodes_ptr(),
    handle.kernel.kernel_memfs_inodes_ptr() + handle.kernel.kernel_memfs_inodes_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernel.kernel_memfs_first_child_ptr(),
    handle.kernel.kernel_memfs_first_child_ptr() + handle.kernel.kernel_memfs_first_child_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernel.kernel_memfs_dir_entries_ptr(),
    handle.kernel.kernel_memfs_dir_entries_ptr() + handle.kernel.kernel_memfs_dir_entries_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernel.kernel_memfs_data_ptr(),
    handle.kernel.kernel_memfs_data_ptr() + Math.min(
      handle.kernel.memfs_data_used?.() ?? handle.kernel.kernel_memfs_data_bytes(),
      handle.kernel.kernel_memfs_data_bytes(),
    ),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.epollTablePtr,
    handle.kernelState.epollTablePtr + handle.kernel.kernel_epoll_table_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.socketTablePtr,
    handle.kernelState.socketTablePtr + handle.kernel.kernel_socket_table_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.pipeBase,
    handle.kernelState.pipeBase + handle.kernel.kernel_pipe_region_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.netBufBase,
    handle.kernelState.netBufBase + handle.kernel.kernel_net_buf_pool_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.sigSlotsPtr,
    handle.kernelState.sigSlotsPtr
      + handle.kernel.kernel_sig_slots_bytes()
      + handle.kernel.kernel_signal_action_table_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.processPendingPtr,
    handle.kernelState.processPendingPtr + handle.kernel.kernel_process_pending_bytes(),
    limit,
  );
  if (handle.kernelState.signalfdStatePtr !== undefined && handle.kernel.kernel_signalfd_state_bytes) {
    addForkCopyRange(
      ranges,
      handle.kernelState.signalfdStatePtr,
      handle.kernelState.signalfdStatePtr + handle.kernel.kernel_signalfd_state_bytes(),
      limit,
    );
  }
  addForkCopyRange(
    ranges,
    handle.kernelState.processRuntimeStatePtr,
    handle.kernelState.processRuntimeStatePtr + handle.kernel.kernel_process_runtime_state_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.timeStatePtr,
    handle.kernelState.timeStatePtr + handle.kernel.kernel_time_state_bytes(),
    limit,
  );

  const lowGuestEnd = Math.max(
    Number(handle.kernel.debug_memory_brk_current?.() ?? 0),
    handle.kernel.thread_process_ptr(handle.vmPtr) + handle.kernel.process_state_bytes(),
  );
  addForkCopyRange(ranges, handle.guestBase, handle.guestBase + lowGuestEnd, limit);

  const vmaLen = handle.kernel.debug_memory_vma_len?.() ?? 0;
  for (let index = 0; index < vmaLen; index += 1) {
    if (isSparseAnonymousReservation(handle, index)) {
      continue;
    }
    const start = Number(handle.kernel.debug_memory_vma_start?.(index) ?? 0);
    const end = Number(handle.kernel.debug_memory_vma_end?.(index) ?? 0);
    addForkCopyRange(ranges, handle.guestBase + start, handle.guestBase + end, limit);
  }

  const stackGuestPtr = Number(stackPointer);
  const guestBackingBytes = Math.max(0, source.byteLength - handle.guestBase);
  if (Number.isFinite(stackGuestPtr) && stackGuestPtr > 0 && stackGuestPtr < guestBackingBytes) {
    const stackStartGuest = Math.max(
      0,
      Math.floor(stackGuestPtr / 65536) * 65536 - FORK_STACK_BELOW_SP_BYTES,
    );
    const stackEndGuest = Math.min(guestBackingBytes, stackStartGuest + FORK_STACK_COPY_WINDOW_BYTES);
    addForkCopyRange(ranges, handle.guestBase + stackStartGuest, handle.guestBase + stackEndGuest, limit);
  }

  const merged = coalesceRanges(ranges);
  return {
    ranges: merged.map((range) => ({
      ...range,
      bytes: source.slice(range.start, range.end),
    })),
    endOffset: merged.reduce((end, range) => Math.max(end, range.end), 0),
  };
}

export function applyForkMemorySnapshot(snapshot: ForkMemorySnapshot, target: WebAssembly.Memory): void {
  const targetBytes = new Uint8Array(target.buffer);
  for (const range of snapshot.ranges) {
    targetBytes.set(range.bytes, range.start);
  }
}

export function overwriteForkMemorySnapshot(
  snapshot: ForkMemorySnapshot,
  start: number,
  bytes: Uint8Array,
): void {
  const end = start + bytes.byteLength;
  for (const range of snapshot.ranges) {
    if (start < range.start || end > range.end) {
      continue;
    }
    range.bytes.set(bytes, start - range.start);
    return;
  }
  snapshot.ranges.push({
    start,
    end,
    bytes: bytes.slice(),
  });
  snapshot.ranges = snapshot.ranges.sort((left, right) => left.start - right.start || left.end - right.end);
  snapshot.endOffset = Math.max(snapshot.endOffset, end);
}

export function forkSnapshotContainsBytes(
  snapshot: ForkMemorySnapshot,
  start: number,
  bytes: Uint8Array,
): boolean {
  const end = start + bytes.byteLength;
  for (const range of snapshot.ranges) {
    if (start < range.start || end > range.end) {
      continue;
    }
    const offset = start - range.start;
    for (let index = 0; index < bytes.byteLength; index += 1) {
      if (range.bytes[offset + index] !== bytes[index]) {
        return false;
      }
    }
    return true;
  }
  return false;
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
