import type { KernelExports, KernelRuntimeState } from "../abi.js";

const MEMORY_SNAPSHOT_CHUNK_BYTES = 4 * 1024 * 1024;
const MAX_HEADER_SNAPSHOT_BYTES = 4096;
const MEMFS_HEADER_BYTES_FALLBACK = 32;
const MEMFS_HEADER_OFF_DATA_USED = 8;
const MEMFS_HEADER_OFF_INODES_PTR = 16;
const MEMFS_HEADER_OFF_FIRST_CHILD_PTR = 20;
const MEMFS_HEADER_OFF_DIR_ENTRIES_PTR = 24;
const MEMFS_HEADER_OFF_DATA_PTR = 28;
const MEMFS_S_IFMT = 0o170000;
const MEMFS_S_IFREG = 0o100000;
const MEMFS_INODE_OFF_MODE = 0;
const MEMFS_INODE_OFF_SIZE = 16;
const MEMFS_INODE_OFF_DATA_OFFSET = 24;
const MEMFS_INODE_OFF_DATA_CAP = 28;

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

function deriveMemStateBytes(state: KernelRuntimeState): number {
  const nextPointers = [
    state.processRuntimeStatePtr,
    state.timeStatePtr,
    state.sigSlotsPtr,
    state.processPendingPtr,
    state.epollTablePtr,
    state.socketTablePtr,
    state.pipeBase,
    state.netBufBase,
    state.ofdTablePtr,
    state.memfsPtr,
  ]
    .filter((ptr) => ptr > state.memStatePtr)
    .sort((left, right) => left - right);
  const nextPtr = nextPointers[0] ?? 0;
  if (nextPtr <= state.memStatePtr) {
    return 0;
  }
  return Math.min(MAX_HEADER_SNAPSHOT_BYTES, nextPtr - state.memStatePtr);
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
  > | null | undefined,
  kernelState: KernelRuntimeState | null | undefined,
  preserveBytes: number,
  options: PrepareMemorySnapshotOptions = {},
): ThreadWorkerPrepareMemorySnapshot {
  const normalizedPreserveBytes = Math.max(0, Math.min(preserveBytes, memory.buffer.byteLength));
  const overlays: MemorySnapshotRange[] = [];
  const includeMemfsData = options.includeMemfsData ?? true;
  if (kernelState) {
    const heapUsedSnapshot = snapshotOverlayRange(
      memory,
      normalizedPreserveBytes,
      kernelState.heapBase,
      Math.max(0, kernelState.heapPtr - kernelState.heapBase),
    );
    if (heapUsedSnapshot) {
      overlays.push(heapUsedSnapshot);
    }
    const memfsLayout = readMemfsLayoutSnapshot(memory, exports, kernelState);
    const memStateSnapshot = snapshotOverlayRange(
      memory,
      normalizedPreserveBytes,
      kernelState.memStatePtr,
      deriveMemStateBytes(kernelState),
    );
    if (memStateSnapshot) {
      overlays.push(memStateSnapshot);
    }
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
