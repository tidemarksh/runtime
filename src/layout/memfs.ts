export const MEMFS_HEADER_BYTES_FALLBACK = 32;
export const MEMFS_HEADER_OFF_INODE_COUNT = 0;
export const MEMFS_HEADER_OFF_DIR_ENTRY_COUNT = 4;
export const MEMFS_HEADER_OFF_DATA_USED = 8;
export const MEMFS_HEADER_OFF_DATA_CAP = 12;
export const MEMFS_HEADER_OFF_INODES_PTR = 16;
export const MEMFS_HEADER_OFF_FIRST_CHILD_PTR = 20;
export const MEMFS_HEADER_OFF_DIR_ENTRIES_PTR = 24;
export const MEMFS_HEADER_OFF_DATA_PTR = 28;

export function memfsHeaderPointerOffset(headerBytes: number, pointerIndex: number): number {
  return 16 + pointerIndex * (headerBytes >= 48 ? 8 : 4);
}

export function readMemfsHeaderPointer(
  view: DataView,
  headerBytes: number,
  pointerIndex: number,
): number {
  const offset = memfsHeaderPointerOffset(headerBytes, pointerIndex);
  return headerBytes >= 48
    ? Number(view.getBigUint64(offset, true))
    : view.getUint32(offset, true);
}

export function writeMemfsHeaderPointer(
  view: DataView,
  headerBytes: number,
  pointerIndex: number,
  value: number,
): void {
  const offset = memfsHeaderPointerOffset(headerBytes, pointerIndex);
  if (headerBytes >= 48) {
    view.setBigUint64(offset, BigInt(value), true);
  } else {
    view.setUint32(offset, value, true);
  }
}

export const MEMFS_INODE_OFF_MODE = 0;
export const MEMFS_INODE_OFF_SIZE = 16;
export const MEMFS_INODE_OFF_DATA_OFFSET = 24;
export const MEMFS_INODE_OFF_DATA_CAP = 28;
