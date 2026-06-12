import {
  kernelOfdStateBytes,
  kernelOfdStatePtr,
  kernelSocketStateBytes,
  kernelSocketStatePtr,
  type KernelExports,
  type KernelRuntimeState,
} from "../abi.js";
import {
  MEMFS_HEADER_BYTES_FALLBACK,
  MEMFS_HEADER_OFF_DATA_PTR,
  MEMFS_HEADER_OFF_DATA_USED,
  MEMFS_HEADER_OFF_DIR_ENTRIES_PTR,
  MEMFS_HEADER_OFF_FIRST_CHILD_PTR,
  MEMFS_HEADER_OFF_INODES_PTR,
  MEMFS_INODE_OFF_DATA_CAP,
  MEMFS_INODE_OFF_DATA_OFFSET,
  MEMFS_INODE_OFF_MODE,
  MEMFS_INODE_OFF_SIZE,
} from "../layout/memfs.js";
import type { ThreadWorkerMemoryPreserveRange } from "../messages.js";

const MEMORY_SNAPSHOT_CHUNK_BYTES = 4 * 1024 * 1024;
const MEMFS_S_IFMT = 0o170000;
const MEMFS_S_IFREG = 0o100000;

interface MemorySnapshotChunk {
  start: number;
  bytes: Uint8Array;
}

interface MemorySnapshotRange {
  chunks: MemorySnapshotChunk[];
}

export interface ThreadWorkerPrepareMemorySnapshot {
  prefix: MemorySnapshotRange | null;
  overlays: MemorySnapshotRange[];
}

interface PrepareMemorySnapshotOptions {
  includeMemfsData?: boolean;
  preserveRanges?: readonly ThreadWorkerMemoryPreserveRange[];
}

function snapshotMemoryRange(
  memory: WebAssembly.Memory,
  start: number,
  length: number,
): MemorySnapshotRange | null {
  if (length <= 0 || start < 0 || start >= memory.buffer.byteLength) {
    return null;
  }
  const end = Math.min(memory.buffer.byteLength, start + length);
  if (end <= start) {
    return null;
  }
  const chunks: MemorySnapshotChunk[] = [];
  for (let offset = start; offset < end; offset += MEMORY_SNAPSHOT_CHUNK_BYTES) {
    const chunkEnd = Math.min(end, offset + MEMORY_SNAPSHOT_CHUNK_BYTES);
    chunks.push({
      start: offset,
      bytes: new Uint8Array(memory.buffer, offset, chunkEnd - offset).slice(),
    });
  }
  return { chunks };
}

function normalizePreserveRanges(
  memory: WebAssembly.Memory,
  ranges: readonly ThreadWorkerMemoryPreserveRange[] | undefined,
): ThreadWorkerMemoryPreserveRange[] {
  if (!ranges || ranges.length === 0) {
    return [];
  }
  const byteLength = memory.buffer.byteLength;
  const normalized = ranges
    .map((range) => ({
      start: Math.max(0, Math.min(byteLength, Math.trunc(range.start))),
      end: Math.max(0, Math.min(byteLength, Math.trunc(range.end))),
    }))
    .filter((range) => Number.isFinite(range.start) && Number.isFinite(range.end) && range.end > range.start)
    .sort((left, right) => left.start - right.start || left.end - right.end);
  const coalesced: ThreadWorkerMemoryPreserveRange[] = [];
  for (const range of normalized) {
    const previous = coalesced[coalesced.length - 1];
    if (previous && range.start <= previous.end) {
      previous.end = Math.max(previous.end, range.end);
      continue;
    }
    coalesced.push({ ...range });
  }
  return coalesced;
}

function restoreMemoryRange(
  memory: WebAssembly.Memory,
  range: MemorySnapshotRange | null,
): void {
  if (!range || range.chunks.length === 0) {
    return;
  }
  const bytes = new Uint8Array(memory.buffer);
  for (const chunk of range.chunks) {
    if (chunk.bytes.byteLength === 0) {
      continue;
    }
    bytes.set(chunk.bytes, chunk.start);
  }
}

function snapshotOverlayRange(
  memory: WebAssembly.Memory,
  preserveBytes: number,
  start: number,
  length: number,
): MemorySnapshotRange | null {
  if (length <= 0) {
    return null;
  }
  const rangeStart = Math.max(0, start);
  const rangeEnd = start + length;
  if (rangeEnd <= preserveBytes) {
    return null;
  }
  return snapshotMemoryRange(
    memory,
    Math.max(rangeStart, preserveBytes),
    rangeEnd - Math.max(rangeStart, preserveBytes),
  );
}

type MemfsLayoutSnapshot = {
  headerBytes: number;
  inodesBytes: number;
  firstChildBytes: number;
  dirEntriesBytes: number;
  dataBytes: number;
  dataUsed: number;
  inodesPtr: number;
  firstChildPtr: number;
  dirEntriesPtr: number;
  dataPtr: number;
};

function computeReferencedMemfsDataUsed(
  memory: WebAssembly.Memory,
  kernelState: KernelRuntimeState,
  layout: MemfsLayoutSnapshot,
): number {
  const maxInodes = Math.floor(layout.firstChildBytes / 4);
  if (maxInodes <= 0) {
    return Math.min(layout.dataUsed, layout.dataBytes);
  }
  const inodeBytes = Math.floor(layout.inodesBytes / maxInodes);
  if (inodeBytes < 32) {
    return Math.min(layout.dataUsed, layout.dataBytes);
  }
  const inodeLimit = Math.min(
    Math.floor(layout.inodesBytes / inodeBytes),
    Math.floor(layout.firstChildBytes / 4),
  );
  if (
    inodeLimit <= 0
    || layout.inodesPtr <= 0
    || layout.inodesPtr + inodeLimit * inodeBytes > memory.buffer.byteLength
  ) {
    return Math.min(layout.dataUsed, layout.dataBytes);
  }
  const inodeView = new DataView(memory.buffer);
  let dataUsed = layout.dataUsed;
  for (let ino = 1; ino < inodeLimit; ino += 1) {
    const inodeBase = layout.inodesPtr + ino * inodeBytes;
    const mode = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_MODE, true);
    if ((mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
      continue;
    }
    const dataCap = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, true);
    if (dataCap === 0) {
      continue;
    }
    const size = Number(inodeView.getBigUint64(inodeBase + MEMFS_INODE_OFF_SIZE, true));
    const dataOffset = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_DATA_OFFSET, true);
    dataUsed = Math.max(dataUsed, dataOffset + Math.min(Math.max(size, 0), dataCap));
  }
  return Math.min(dataUsed, layout.dataBytes);
}

function readMemfsLayoutSnapshot(
  memory: WebAssembly.Memory,
  exports: Pick<
    KernelExports,
    | "memory"
    | "kernel_memfs_header_bytes"
    | "kernel_memfs_inodes_ptr"
    | "kernel_memfs_inodes_bytes"
    | "kernel_memfs_first_child_ptr"
    | "kernel_memfs_first_child_bytes"
    | "kernel_memfs_dir_entries_ptr"
    | "kernel_memfs_dir_entries_bytes"
    | "kernel_memfs_data_ptr"
    | "kernel_memfs_data_bytes"
    | "memfs_data_used"
  > | null | undefined,
  kernelState: KernelRuntimeState,
): MemfsLayoutSnapshot {
  const exportsMatchMemory = exports?.memory?.buffer === memory.buffer;
  const liveExports = exportsMatchMemory ? exports : null;
  const headerBytes = liveExports?.kernel_memfs_header_bytes?.() ?? MEMFS_HEADER_BYTES_FALLBACK;
  const view = kernelState.memfsPtr > 0 && kernelState.memfsPtr + headerBytes <= memory.buffer.byteLength
    ? new DataView(memory.buffer, kernelState.memfsPtr, headerBytes)
    : null;
  return {
    headerBytes,
    inodesBytes: liveExports?.kernel_memfs_inodes_bytes?.() ?? kernelState.memfsInodesBytes ?? 0,
    firstChildBytes: liveExports?.kernel_memfs_first_child_bytes?.() ?? kernelState.memfsFirstChildBytes ?? 0,
    dirEntriesBytes: liveExports?.kernel_memfs_dir_entries_bytes?.() ?? kernelState.memfsDirEntriesBytes ?? 0,
    dataBytes: liveExports?.kernel_memfs_data_bytes?.() ?? kernelState.memfsDataBytes ?? 0,
    dataUsed: liveExports?.memfs_data_used?.()
      ?? view?.getUint32(MEMFS_HEADER_OFF_DATA_USED, true)
      ?? 0,
    inodesPtr: liveExports?.kernel_memfs_inodes_ptr?.()
      ?? view?.getUint32(MEMFS_HEADER_OFF_INODES_PTR, true)
      ?? 0,
    firstChildPtr: liveExports?.kernel_memfs_first_child_ptr?.()
      ?? view?.getUint32(MEMFS_HEADER_OFF_FIRST_CHILD_PTR, true)
      ?? 0,
    dirEntriesPtr: liveExports?.kernel_memfs_dir_entries_ptr?.()
      ?? view?.getUint32(MEMFS_HEADER_OFF_DIR_ENTRIES_PTR, true)
      ?? 0,
    dataPtr: liveExports?.kernel_memfs_data_ptr?.()
      ?? view?.getUint32(MEMFS_HEADER_OFF_DATA_PTR, true)
      ?? 0,
  };
}

export function snapshotThreadWorkerPrepareMemory(
  memory: WebAssembly.Memory,
  exports: Pick<
    KernelExports,
    | "memory"
    | "kernel_memfs_header_bytes"
    | "kernel_memfs_inodes_ptr"
    | "kernel_memfs_inodes_bytes"
    | "kernel_memfs_first_child_ptr"
    | "kernel_memfs_first_child_bytes"
    | "kernel_memfs_dir_entries_ptr"
    | "kernel_memfs_dir_entries_bytes"
    | "kernel_memfs_data_ptr"
    | "kernel_memfs_data_bytes"
    | "memfs_data_used"
    | "kernel_ofd_table_bytes"
    | "kernel_ofd_state_ptr"
    | "kernel_ofd_state_bytes"
    | "kernel_ofd_reserved_ptr"
    | "kernel_ofd_reserved_bytes"
    | "kernel_ofd_offset_floor_ptr"
    | "kernel_ofd_offset_floor_bytes"
    | "kernel_epoll_table_bytes"
    | "kernel_socket_table_bytes"
    | "kernel_socket_state_ptr"
    | "kernel_socket_state_bytes"
    | "kernel_pipe_region_bytes"
    | "kernel_net_buf_pool_bytes"
    | "kernel_sig_slots_bytes"
    | "kernel_process_pending_bytes"
    | "kernel_signalfd_state_bytes"
    | "kernel_process_runtime_state_bytes"
    | "kernel_time_state_bytes"
  > | null | undefined,
  kernelState: KernelRuntimeState | null | undefined,
  preserveBytes: number,
  options: PrepareMemorySnapshotOptions = {},
): ThreadWorkerPrepareMemorySnapshot {
  const normalizedPreserveBytes = Math.max(0, Math.min(preserveBytes, memory.buffer.byteLength));
  const overlays: MemorySnapshotRange[] = [];
  const explicitPreserveRanges = normalizePreserveRanges(memory, options.preserveRanges);
  if (explicitPreserveRanges.length > 0) {
    for (const range of explicitPreserveRanges) {
      const snapshot = snapshotMemoryRange(memory, range.start, range.end - range.start);
      if (snapshot) {
        overlays.push(snapshot);
      }
    }
    return {
      prefix: null,
      overlays,
    };
  }
  const includeMemfsData = options.includeMemfsData ?? true;
  if (kernelState) {
    const snapshotKernelRegion = (start: number, length: number): void => {
      const snapshot = snapshotOverlayRange(memory, normalizedPreserveBytes, start, length);
      if (snapshot) {
        overlays.push(snapshot);
      }
    };
    const memfsLayout = readMemfsLayoutSnapshot(memory, exports, kernelState);
    const ofdTableBytes = exports?.kernel_ofd_table_bytes?.() ?? 0;
    const ofdStateBytes = exports
      ? kernelOfdStateBytes(exports, kernelState)
      : kernelState.ofdStateBytes ?? 0;
    const hasOfdStateExport = Boolean(
      exports?.kernel_ofd_state_ptr
        || exports?.kernel_ofd_state_bytes
        || (kernelState.ofdStatePtr !== undefined && kernelState.ofdStatePtr > 0 && kernelState.ofdStatePtr !== kernelState.ofdTablePtr)
        || (ofdTableBytes > 0 && ofdStateBytes > ofdTableBytes)
        || (kernelState.ofdOffsetFloorBytes ?? 0) > 0,
    );
    if (hasOfdStateExport) {
      snapshotKernelRegion(kernelOfdStatePtr(kernelState), ofdStateBytes);
    } else {
      snapshotKernelRegion(kernelState.ofdTablePtr, ofdTableBytes);
      snapshotKernelRegion(exports?.kernel_ofd_reserved_ptr?.() ?? 0, exports?.kernel_ofd_reserved_bytes?.() ?? 0);
      snapshotKernelRegion(
        exports?.kernel_ofd_offset_floor_ptr?.() ?? kernelState.ofdOffsetFloorPtr ?? 0,
        exports?.kernel_ofd_offset_floor_bytes?.() ?? kernelState.ofdOffsetFloorBytes ?? 0,
      );
    }
    snapshotKernelRegion(kernelState.epollTablePtr, exports?.kernel_epoll_table_bytes?.() ?? kernelState.epollTableBytes ?? 0);
    const socketTableBytes = exports?.kernel_socket_table_bytes?.() ?? 0;
    const socketStateBytes = exports
      ? kernelSocketStateBytes(exports, kernelState)
      : kernelState.socketStateBytes ?? 0;
    const hasSocketStateExport = Boolean(
      exports?.kernel_socket_state_ptr
        || exports?.kernel_socket_state_bytes
        || (kernelState.socketStatePtr !== undefined && kernelState.socketStatePtr > 0 && kernelState.socketStatePtr !== kernelState.socketTablePtr)
        || (socketTableBytes > 0 && socketStateBytes > socketTableBytes),
    );
    snapshotKernelRegion(
      hasSocketStateExport ? kernelSocketStatePtr(kernelState) : kernelState.socketTablePtr,
      hasSocketStateExport ? socketStateBytes : socketTableBytes,
    );
    snapshotKernelRegion(kernelState.pipeBase, exports?.kernel_pipe_region_bytes?.() ?? kernelState.pipeRegionBytes ?? 0);
    snapshotKernelRegion(kernelState.netBufBase, exports?.kernel_net_buf_pool_bytes?.() ?? kernelState.netBufPoolBytes ?? 0);
    snapshotKernelRegion(kernelState.sigSlotsPtr, exports?.kernel_sig_slots_bytes?.() ?? kernelState.sigSlotsBytes ?? 0);
    snapshotKernelRegion(kernelState.processPendingPtr, exports?.kernel_process_pending_bytes?.() ?? kernelState.processPendingBytes ?? 0);
    snapshotKernelRegion(kernelState.signalfdStatePtr ?? 0, exports?.kernel_signalfd_state_bytes?.() ?? kernelState.signalfdStateBytes ?? 0);
    snapshotKernelRegion(kernelState.processRuntimeStatePtr, exports?.kernel_process_runtime_state_bytes?.() ?? kernelState.processRuntimeStateBytes ?? 0);
    snapshotKernelRegion(kernelState.timeStatePtr, exports?.kernel_time_state_bytes?.() ?? kernelState.timeStateBytes ?? 0);
    const memfsHeaderSnapshot = snapshotOverlayRange(
      memory,
      normalizedPreserveBytes,
      kernelState.memfsPtr,
      memfsLayout.headerBytes,
    );
    if (memfsHeaderSnapshot) {
      overlays.push(memfsHeaderSnapshot);
    }
    const memfsInodesSnapshot = snapshotOverlayRange(
      memory,
      normalizedPreserveBytes,
      memfsLayout.inodesPtr,
      memfsLayout.inodesBytes,
    );
    if (memfsInodesSnapshot) {
      overlays.push(memfsInodesSnapshot);
    }
    const memfsFirstChildSnapshot = snapshotOverlayRange(
      memory,
      normalizedPreserveBytes,
      memfsLayout.firstChildPtr,
      memfsLayout.firstChildBytes,
    );
    if (memfsFirstChildSnapshot) {
      overlays.push(memfsFirstChildSnapshot);
    }
    const memfsDirEntriesSnapshot = snapshotOverlayRange(
      memory,
      normalizedPreserveBytes,
      memfsLayout.dirEntriesPtr,
      memfsLayout.dirEntriesBytes,
    );
    if (memfsDirEntriesSnapshot) {
      overlays.push(memfsDirEntriesSnapshot);
    }
    if (includeMemfsData) {
      const memfsDataSnapshot = snapshotOverlayRange(
        memory,
        normalizedPreserveBytes,
        memfsLayout.dataPtr,
        computeReferencedMemfsDataUsed(memory, kernelState, memfsLayout),
      );
      if (memfsDataSnapshot) {
        overlays.push(memfsDataSnapshot);
      }
    }
  }
  return {
    prefix: snapshotMemoryRange(memory, 0, normalizedPreserveBytes),
    overlays,
  };
}

export function restoreThreadWorkerPrepareMemory(
  memory: WebAssembly.Memory,
  snapshot: ThreadWorkerPrepareMemorySnapshot,
): void {
  restoreMemoryRange(memory, snapshot.prefix);
  for (const overlay of snapshot.overlays) {
    restoreMemoryRange(memory, overlay);
  }
}
