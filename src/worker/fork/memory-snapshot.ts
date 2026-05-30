import { MAX_INODES, readInodeRecord } from "../../fs/sab-page-cache.js";
import {
  MEMFS_S_IFMT,
  MEMFS_S_IFREG,
  memfsFileHasInlineData,
} from "../../kernel-worker/common.js";
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
    const start = Number(handle.kernel.debug_memory_vma_start?.(index) ?? 0);
    const end = Number(handle.kernel.debug_memory_vma_end?.(index) ?? 0);
    addForkCopyRange(ranges, handle.guestBase + start, handle.guestBase + end, limit);
  }

  const stackGuestPtr = Number(stackPointer);
  const stackTop = handle.guestBase + handle.ramSize;
  if (Number.isFinite(stackGuestPtr) && stackGuestPtr > 0 && stackGuestPtr < handle.ramSize) {
    const stackStartGuest = Math.max(
      0,
      Math.floor(stackGuestPtr / 65536) * 65536 - 1024 * 1024,
    );
    addForkCopyRange(ranges, handle.guestBase + stackStartGuest, stackTop, limit);
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

export function appendPlainForkMemoryDebug(
  handle: ProcessHandle,
  childPid: number,
  snapshot: ForkMemorySnapshot,
  childMemoryBytes: number,
): void {
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
    `ramSize=${handle.ramSize}`,
    `maxMemoryBytes=${handle.maxMemoryPages * 65536}`,
    `forkMaxMemoryBytes=${handle.forkMaxMemoryPages * 65536}`,
  ].join(":"));
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
}

export function normalizePageCacheBackedExecutableFilesForFork(handle: ProcessHandle): void {
  const sab = handle.sabPageCache;
  if (!sab) {
    return;
  }

  let candidates = 0;
  let externalized = 0;
  let alreadyExternal = 0;
  let bytes = 0;
  const sampledInodes: string[] = [];
  for (let ino = 1; ino < MAX_INODES; ino += 1) {
    const published = readInodeRecord(sab, ino);
    if (
      !published
      || (published.mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG
      || (published.mode & 0o111) === 0
      || published.size <= 0
      || published.dataOff === 0
    ) {
      continue;
    }

    const mode = handle.kernel.memfs_inode_mode(ino);
    if (
      (mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG
      || Number(handle.kernel.memfs_file_size(ino)) !== published.size
    ) {
      continue;
    }

    candidates += 1;
    bytes += published.size;
    if (sampledInodes.length < 8) {
      sampledInodes.push(`${ino}:${published.size}`);
    }
    if (!memfsFileHasInlineData(handle.kernel, ino)) {
      alreadyExternal += 1;
      continue;
    }

    const externalizedResult = Number(handle.kernel.memfs_set_file_external(ino, BigInt(published.size)));
    if (externalizedResult < 0) {
      continue;
    }
    const modeResult = Number(handle.kernel.memfs_set_mode(ino, mode));
    if (modeResult < 0) {
      continue;
    }
    externalized += 1;
  }

  if (candidates > 0) {
    handle.workerDebugLog.push(
      [
        "plain-fork-exec-files-external",
        `candidates=${candidates}`,
        `externalized=${externalized}`,
        `alreadyExternal=${alreadyExternal}`,
        `bytes=${bytes}`,
        `inos=${sampledInodes.join(".")}`,
      ].join(":"),
    );
    if (handle.workerDebugLog.length > 4096) {
      handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
    }
  }
}
