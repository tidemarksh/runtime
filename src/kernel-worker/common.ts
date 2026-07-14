import type { KernelExports, KernelRuntimeState } from "../abi.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import { growKernelMemory } from "../wasm/kernel-memory.js";
import {
  MEMFS_HEADER_OFF_DATA_CAP,
  MEMFS_HEADER_OFF_DATA_USED,
  MEMFS_HEADER_OFF_DIR_ENTRY_COUNT,
  MEMFS_HEADER_OFF_INODE_COUNT,
  readMemfsHeaderPointer,
  writeMemfsHeaderPointer,
  MEMFS_INODE_OFF_DATA_CAP,
  MEMFS_INODE_OFF_DATA_OFFSET,
  MEMFS_INODE_OFF_MODE,
  MEMFS_INODE_OFF_SIZE,
} from "../layout/memfs.js";
export {
  MEMFS_S_IFMT,
  MEMFS_S_IFREG,
  memfsFileHasInlineData,
} from "../fs/memfs-inline-data.js";
import { MEMFS_S_IFMT, MEMFS_S_IFREG } from "../fs/memfs-inline-data.js";

export type MemfsLayout = {
  inodeCount: number;
  dirEntryCount: number;
  dataUsed: number;
  dataCap: number;
  inodesPtr: number;
  firstChildPtr: number;
  dirEntriesPtr: number;
  dataPtr: number;
};

export function normalizePermMode(mode: number | undefined, fallback: number): number {
  return (mode ?? fallback) & 0o7777;
}

export function normalizePath(path: string): string {
  const normalized = `/${path.split("/").filter(Boolean).join("/")}`;
  return normalized === "//" ? "/" : normalized || "/";
}

export function pathParts(path: string): string[] {
  return normalizePath(path).split("/").filter(Boolean);
}

export function cloneSnapshotEntry(entry: FsSnapshotEntry): FsSnapshotEntry {
  if (entry.kind === "file") {
    return {
      kind: "file",
      path: entry.path,
      ino: entry.ino,
      data: entry.data ? new Uint8Array(entry.data) : undefined,
      size: entry.size ?? entry.data?.length ?? 0,
      mode: entry.mode,
      backingExtent: entry.backingExtent ? { ...entry.backingExtent } : undefined,
    };
  }
  if (entry.kind === "symlink") {
    return {
      kind: "symlink",
      path: entry.path,
      ino: entry.ino,
      target: entry.target ? new Uint8Array(entry.target) : undefined,
      size: entry.size ?? entry.target?.length ?? 0,
      mode: entry.mode,
    };
  }
  return {
    kind: "dir",
    path: entry.path,
    ino: entry.ino,
    mode: entry.mode,
  };
}

export function scratchBaseFor(targetExports: KernelExports): number {
  const heapScratch = targetExports.kernel_heap_ptr() + 0x10000;
  const memfsDataEnd = typeof targetExports.kernel_memfs_data_ptr === "function"
    && typeof targetExports.kernel_memfs_data_bytes === "function"
    ? targetExports.kernel_memfs_data_ptr() + targetExports.kernel_memfs_data_bytes() + 0x10000
    : 0;
  return (Math.max(heapScratch, memfsDataEnd) + 15) & ~15;
}

export function ensureMemoryCapacityFor(memory: WebAssembly.Memory, endOffset: number): void {
  const pageSize = 65536;
  const currentBytes = memory.buffer.byteLength;
  if (endOffset <= currentBytes) {
    return;
  }
  const additionalPages = Math.ceil((endOffset - currentBytes) / pageSize);
  growKernelMemory(memory, additionalPages);
}

export function writeScratchFor(targetExports: KernelExports, bytes: Uint8Array, offset = 0): number {
  const scratch = scratchBaseFor(targetExports) + offset;
  ensureMemoryCapacityFor(targetExports.memory, scratch + bytes.length);
  const mem = new Uint8Array(targetExports.memory.buffer);
  mem.set(bytes, scratch);
  return scratch;
}

export function copyMemoryRange(
  sourceMemory: WebAssembly.Memory,
  sourceOffset: number,
  targetMemory: WebAssembly.Memory,
  targetOffset: number,
  byteLength: number,
): void {
  if (byteLength <= 0) {
    return;
  }
  ensureMemoryCapacityFor(targetMemory, targetOffset + byteLength);
  const source = new Uint8Array(sourceMemory.buffer, sourceOffset, byteLength);
  new Uint8Array(targetMemory.buffer).set(source, targetOffset);
}

export function clearMemoryRange(
  targetMemory: WebAssembly.Memory,
  targetOffset: number,
  byteLength: number,
): void {
  if (byteLength <= 0) {
    return;
  }
  ensureMemoryCapacityFor(targetMemory, targetOffset + byteLength);
  new Uint8Array(targetMemory.buffer, targetOffset, byteLength).fill(0);
}

export function readMemfsLayout(
  memory: WebAssembly.Memory,
  memfsPtr: number,
  exports?: Partial<Pick<
    KernelExports,
    | "kernel_memfs_header_bytes"
    | "kernel_memfs_inodes_ptr"
    | "kernel_memfs_first_child_ptr"
    | "kernel_memfs_dir_entries_ptr"
    | "kernel_memfs_data_ptr"
  >>,
): MemfsLayout {
  const headerBytes = exports?.kernel_memfs_header_bytes?.() ?? 32;
  const view = new DataView(memory.buffer, memfsPtr, headerBytes);
  return {
    inodeCount: view.getUint32(0, true),
    dirEntryCount: view.getUint32(4, true),
    dataUsed: view.getUint32(8, true),
    dataCap: view.getUint32(12, true),
    inodesPtr: exports?.kernel_memfs_inodes_ptr?.() ?? readMemfsHeaderPointer(view, headerBytes, 0),
    firstChildPtr: exports?.kernel_memfs_first_child_ptr?.() ?? readMemfsHeaderPointer(view, headerBytes, 1),
    dirEntriesPtr: exports?.kernel_memfs_dir_entries_ptr?.() ?? readMemfsHeaderPointer(view, headerBytes, 2),
    dataPtr: exports?.kernel_memfs_data_ptr?.() ?? readMemfsHeaderPointer(view, headerBytes, 3),
  };
}

export function computeReferencedMemfsDataUsed(
  targetExports: KernelExports,
  layout = readMemfsLayout(targetExports.memory, targetExports.kernel_memfs_ptr(), targetExports),
): number {
  const maxInodes = targetExports.kernel_memfs_first_child_bytes() / 4;
  if (maxInodes <= 0) {
    return Math.min(layout.dataUsed, layout.dataCap);
  }
  const inodeBytes = targetExports.kernel_memfs_inodes_bytes() / maxInodes;
  if (inodeBytes < 32) {
    return Math.min(layout.dataUsed, layout.dataCap);
  }
  const inodeLimit = Math.min(layout.inodeCount, maxInodes);
  const inodeView = new DataView(targetExports.memory.buffer);
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
  return Math.min(dataUsed, layout.dataCap);
}

export function normalizeMetadataOnlyMemfsState(
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  sourceLayout: MemfsLayout,
  targetLayout: MemfsLayout,
  dataUsed = 8,
  options: { clearRegularData?: boolean } = {},
): void {
  const headerBytes = targetExports.kernel_memfs_header_bytes();
  const header = new DataView(targetExports.memory.buffer, targetState.memfsPtr, headerBytes);
  header.setUint32(MEMFS_HEADER_OFF_INODE_COUNT, sourceLayout.inodeCount, true);
  header.setUint32(MEMFS_HEADER_OFF_DIR_ENTRY_COUNT, sourceLayout.dirEntryCount, true);
  header.setUint32(MEMFS_HEADER_OFF_DATA_USED, Math.min(Math.max(dataUsed, 8), targetLayout.dataCap), true);
  header.setUint32(MEMFS_HEADER_OFF_DATA_CAP, targetLayout.dataCap, true);
  writeMemfsHeaderPointer(header, headerBytes, 0, targetLayout.inodesPtr);
  writeMemfsHeaderPointer(header, headerBytes, 1, targetLayout.firstChildPtr);
  writeMemfsHeaderPointer(header, headerBytes, 2, targetLayout.dirEntriesPtr);
  writeMemfsHeaderPointer(header, headerBytes, 3, targetLayout.dataPtr);

  const maxInodes = targetExports.kernel_memfs_first_child_bytes() / 4;
  const inodeBytes = targetExports.kernel_memfs_inodes_bytes() / maxInodes;
  const inodeView = new DataView(targetExports.memory.buffer);
  if (options.clearRegularData === false) {
    return;
  }

  for (let ino = 1; ino < sourceLayout.inodeCount; ino++) {
    const inodeBase = targetLayout.inodesPtr + ino * inodeBytes;
    const mode = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_MODE, true);
    if ((mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
      continue;
    }
    inodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_OFFSET, 0, true);
    inodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, 0, true);
  }
}
