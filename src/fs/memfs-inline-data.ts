import type { KernelExports } from "../abi.js";
import { MEMFS_INODE_OFF_DATA_CAP, MEMFS_INODE_OFF_MODE } from "../layout/memfs.js";

export const MEMFS_S_IFMT = 0o170000;
export const MEMFS_S_IFREG = 0o100000;

export function memfsFileHasInlineData(targetExports: KernelExports, ino: number): boolean {
  if (
    typeof targetExports.kernel_memfs_ptr !== "function"
    || typeof targetExports.kernel_memfs_first_child_bytes !== "function"
    || typeof targetExports.kernel_memfs_inodes_bytes !== "function"
  ) {
    return false;
  }
  const memfsPtr = targetExports.kernel_memfs_ptr();
  const layoutView = new DataView(targetExports.memory.buffer, memfsPtr, 32);
  const inodesPtr = layoutView.getUint32(16, true);
  const maxInodes = targetExports.kernel_memfs_first_child_bytes() / 4;
  const inodeBytes = targetExports.kernel_memfs_inodes_bytes() / maxInodes;
  const inodeBase = inodesPtr + ino * inodeBytes;
  const inodeView = new DataView(targetExports.memory.buffer);
  const mode = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_MODE, true);
  if ((mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
    return false;
  }
  return inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, true) !== 0;
}
