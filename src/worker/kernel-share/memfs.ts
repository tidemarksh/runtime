import type { KernelSyncHandle } from "../types.js";
import { clearKernelRegion, copyKernelRegion } from "./common.js";

type MemfsLayout = {
  inodeCount: number;
  inodesPtr: number;
  firstChildPtr: number;
  dirEntriesPtr: number;
  dataUsed: number;
  dataPtr: number;
  dataCap: number;
};

const MEMFS_S_IFMT = 0o170000;
const MEMFS_S_IFREG = 0o100000;
const MEMFS_INODE_OFF_MODE = 0;
const MEMFS_INODE_OFF_SIZE = 16;
const MEMFS_INODE_OFF_DATA_OFFSET = 24;
const MEMFS_INODE_OFF_DATA_CAP = 28;

function readMemfsLayout(handle: KernelSyncHandle): MemfsLayout {
  const view = new DataView(handle.memory.buffer, handle.kernelState.memfsPtr, 32);
  return {
    inodeCount: view.getUint32(0, true),
    dataUsed: view.getUint32(8, true),
    dataCap: view.getUint32(12, true),
    inodesPtr: view.getUint32(16, true),
    firstChildPtr: view.getUint32(20, true),
    dirEntriesPtr: view.getUint32(24, true),
    dataPtr: view.getUint32(28, true),
  };
}

function computeReferencedMemfsDataUsed(handle: KernelSyncHandle, layout = readMemfsLayout(handle)): number {
  const maxInodes = handle.kernel.kernel_memfs_first_child_bytes() / 4;
  if (maxInodes <= 0) {
    return Math.min(layout.dataUsed, layout.dataCap);
  }
  const inodeBytes = handle.kernel.kernel_memfs_inodes_bytes() / maxInodes;
  if (inodeBytes < 32) {
    return Math.min(layout.dataUsed, layout.dataCap);
  }
  const inodeLimit = Math.min(layout.inodeCount, maxInodes);
  const inodeView = new DataView(handle.memory.buffer);
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

export function clearMemfsState(target: KernelSyncHandle): void {
  const layout = readMemfsLayout(target);
  clearKernelRegion(
    target,
    layout.inodesPtr,
    target.kernel.kernel_memfs_inodes_bytes(),
  );
  clearKernelRegion(
    target,
    layout.firstChildPtr,
    target.kernel.kernel_memfs_first_child_bytes(),
  );
  clearKernelRegion(
    target,
    layout.dirEntriesPtr,
    target.kernel.kernel_memfs_dir_entries_bytes(),
  );
  clearKernelRegion(
    target,
    layout.dataPtr,
    layout.dataCap,
  );
  clearKernelRegion(
    target,
    target.kernelState.memfsPtr,
    target.kernel.kernel_memfs_header_bytes(),
  );
}

export function syncMemfsState(source: KernelSyncHandle, target: KernelSyncHandle): void {
  const sourceLayout = readMemfsLayout(source);
  const targetLayout = readMemfsLayout(target);
  const sourceDataUsed = computeReferencedMemfsDataUsed(source, sourceLayout);
  copyKernelRegion(
    source,
    sourceLayout.inodesPtr,
    target,
    targetLayout.inodesPtr,
    source.kernel.kernel_memfs_inodes_bytes(),
  );
  copyKernelRegion(
    source,
    sourceLayout.firstChildPtr,
    target,
    targetLayout.firstChildPtr,
    source.kernel.kernel_memfs_first_child_bytes(),
  );
  copyKernelRegion(
    source,
    sourceLayout.dirEntriesPtr,
    target,
    targetLayout.dirEntriesPtr,
    source.kernel.kernel_memfs_dir_entries_bytes(),
  );
  copyKernelRegion(
    source,
    sourceLayout.dataPtr,
    target,
    targetLayout.dataPtr,
    Math.min(sourceDataUsed, targetLayout.dataCap),
  );
  clearKernelRegion(
    target,
    targetLayout.dataPtr + Math.min(sourceDataUsed, targetLayout.dataCap),
    Math.max(0, targetLayout.dataCap - Math.min(sourceDataUsed, targetLayout.dataCap)),
  );
  copyKernelRegion(
    source,
    source.kernelState.memfsPtr,
    target,
    target.kernelState.memfsPtr,
    source.kernel.kernel_memfs_header_bytes(),
  );
  const targetHeader = new DataView(target.memory.buffer, target.kernelState.memfsPtr, 32);
  targetHeader.setUint32(0, sourceLayout.inodeCount, true);
  targetHeader.setUint32(8, Math.min(sourceDataUsed, targetLayout.dataCap), true);
  targetHeader.setUint32(12, targetLayout.dataCap, true);
  targetHeader.setUint32(16, targetLayout.inodesPtr, true);
  targetHeader.setUint32(20, targetLayout.firstChildPtr, true);
  targetHeader.setUint32(24, targetLayout.dirEntriesPtr, true);
  targetHeader.setUint32(28, targetLayout.dataPtr, true);
}
