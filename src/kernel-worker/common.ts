import type { KernelExports, KernelRuntimeState } from "../abi.js";
import type { FsSnapshotEntry } from "../fs/types.js";

const materializedMemfsFiles = new WeakMap<KernelExports, Set<number>>();

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

export const MEMFS_S_IFMT = 0o170000;
export const MEMFS_S_IFREG = 0o100000;
const MEMFS_INODE_OFF_SIZE = 16;
const MEMFS_HEADER_OFF_INODE_COUNT = 0;
const MEMFS_HEADER_OFF_DIR_ENTRY_COUNT = 4;
const MEMFS_HEADER_OFF_DATA_USED = 8;
const MEMFS_HEADER_OFF_DATA_CAP = 12;
const MEMFS_HEADER_OFF_INODES_PTR = 16;
const MEMFS_HEADER_OFF_FIRST_CHILD_PTR = 20;
const MEMFS_HEADER_OFF_DIR_ENTRIES_PTR = 24;
const MEMFS_HEADER_OFF_DATA_PTR = 28;
const MEMFS_INODE_OFF_MODE = 0;
const MEMFS_INODE_OFF_DATA_OFFSET = 24;
const MEMFS_INODE_OFF_DATA_CAP = 28;

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
  return targetExports.kernel_heap_ptr() + 0x10000;
}

export function ensureMemoryCapacityFor(memory: WebAssembly.Memory, endOffset: number): void {
  const pageSize = 65536;
  const currentBytes = memory.buffer.byteLength;
  if (endOffset <= currentBytes) {
    return;
  }
  const additionalPages = Math.ceil((endOffset - currentBytes) / pageSize);
  memory.grow(additionalPages);
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

export function markMaterializedMemfsFile(targetExports: KernelExports, ino: number): void {
  let inos = materializedMemfsFiles.get(targetExports);
  if (!inos) {
    inos = new Set<number>();
    materializedMemfsFiles.set(targetExports, inos);
  }
  inos.add(ino);
}

export function clearMaterializedMemfsFiles(targetExports: KernelExports): void {
  materializedMemfsFiles.delete(targetExports);
}

export function readMemfsLayout(memory: WebAssembly.Memory, memfsPtr: number): MemfsLayout {
  const view = new DataView(memory.buffer, memfsPtr, 32);
  return {
    inodeCount: view.getUint32(0, true),
    dirEntryCount: view.getUint32(4, true),
    dataUsed: view.getUint32(8, true),
    dataCap: view.getUint32(12, true),
    inodesPtr: view.getUint32(16, true),
    firstChildPtr: view.getUint32(20, true),
    dirEntriesPtr: view.getUint32(24, true),
    dataPtr: view.getUint32(28, true),
  };
}

export function computeReferencedMemfsDataUsed(
  targetExports: KernelExports,
  layout = readMemfsLayout(targetExports.memory, targetExports.kernel_memfs_ptr()),
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

export function readMemfsFileDataDirect(targetExports: KernelExports, ino: number): Uint8Array | null {
  const layout = readMemfsLayout(targetExports.memory, targetExports.kernel_memfs_ptr());
  const maxInodes = targetExports.kernel_memfs_first_child_bytes() / 4;
  const inodeBytes = targetExports.kernel_memfs_inodes_bytes() / maxInodes;
  const inodeBase = layout.inodesPtr + ino * inodeBytes;
  const inodeView = new DataView(targetExports.memory.buffer);
  const mode = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_MODE, true);
  if ((mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
    return null;
  }
  const size = Number(inodeView.getBigUint64(inodeBase + MEMFS_INODE_OFF_SIZE, true));
  if (size <= 0) {
    return new Uint8Array();
  }
  const dataOffset = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_DATA_OFFSET, true);
  const dataCap = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, true);
  if (dataCap === 0 || dataOffset + size > layout.dataCap) {
    return null;
  }
  const dataStart = layout.dataPtr + dataOffset;
  return new Uint8Array(targetExports.memory.buffer.slice(dataStart, dataStart + size));
}

export function memfsFileHasInlineData(targetExports: KernelExports, ino: number): boolean {
  if (
    typeof targetExports.kernel_memfs_ptr !== "function"
    || typeof targetExports.kernel_memfs_first_child_bytes !== "function"
    || typeof targetExports.kernel_memfs_inodes_bytes !== "function"
  ) {
    return false;
  }
  const layout = readMemfsLayout(targetExports.memory, targetExports.kernel_memfs_ptr());
  const maxInodes = targetExports.kernel_memfs_first_child_bytes() / 4;
  const inodeBytes = targetExports.kernel_memfs_inodes_bytes() / maxInodes;
  const inodeBase = layout.inodesPtr + ino * inodeBytes;
  const inodeView = new DataView(targetExports.memory.buffer);
  const mode = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_MODE, true);
  if ((mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
    return false;
  }
  return inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, true) !== 0;
}

export function normalizeMetadataOnlyMemfsState(
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  sourceLayout: MemfsLayout,
  targetLayout: MemfsLayout,
): void {
  const header = new DataView(targetExports.memory.buffer, targetState.memfsPtr, 32);
  header.setUint32(MEMFS_HEADER_OFF_INODE_COUNT, sourceLayout.inodeCount, true);
  header.setUint32(MEMFS_HEADER_OFF_DIR_ENTRY_COUNT, sourceLayout.dirEntryCount, true);
  header.setUint32(MEMFS_HEADER_OFF_DATA_USED, 8, true);
  header.setUint32(MEMFS_HEADER_OFF_DATA_CAP, targetLayout.dataCap, true);
  header.setUint32(MEMFS_HEADER_OFF_INODES_PTR, targetLayout.inodesPtr, true);
  header.setUint32(MEMFS_HEADER_OFF_FIRST_CHILD_PTR, targetLayout.firstChildPtr, true);
  header.setUint32(MEMFS_HEADER_OFF_DIR_ENTRIES_PTR, targetLayout.dirEntriesPtr, true);
  header.setUint32(MEMFS_HEADER_OFF_DATA_PTR, targetLayout.dataPtr, true);

  const maxInodes = targetExports.kernel_memfs_first_child_bytes() / 4;
  const inodeBytes = targetExports.kernel_memfs_inodes_bytes() / maxInodes;
  const inodeView = new DataView(targetExports.memory.buffer);
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
