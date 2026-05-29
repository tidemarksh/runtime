import type { KernelExports, KernelRuntimeState } from "../../abi.js";
import {
  clearMaterializedMemfsFiles,
  clearMemoryRange,
  computeReferencedMemfsDataUsed,
  copyMemoryRange,
  ensureMemoryCapacityFor,
  MEMFS_S_IFMT,
  MEMFS_S_IFREG,
  normalizeMetadataOnlyMemfsState,
  readMemfsLayout,
} from "../common.js";
import {
  MEMFS_INODE_OFF_DATA_CAP,
  MEMFS_INODE_OFF_DATA_OFFSET,
  MEMFS_INODE_OFF_MODE,
  MEMFS_INODE_OFF_SIZE,
} from "../../layout/memfs.js";

const MEMFS_S_IFLNK = 0o120000;

export function createKernelWorkerMemfsPrimitives() {
  function preserveMetadataOnlySymlinkData(
    sourceExports: KernelExports,
    targetExports: KernelExports,
    sourceLayout: ReturnType<typeof readMemfsLayout>,
    targetLayout: ReturnType<typeof readMemfsLayout>,
  ): number {
    const maxInodes = targetExports.kernel_memfs_first_child_bytes() / 4;
    if (maxInodes <= 0) {
      return 8;
    }
    const inodeBytes = targetExports.kernel_memfs_inodes_bytes() / maxInodes;
    if (inodeBytes < 32) {
      return 8;
    }

    const sourceBytes = new Uint8Array(sourceExports.memory.buffer);
    const targetBytes = new Uint8Array(targetExports.memory.buffer);
    const targetInodeView = new DataView(targetExports.memory.buffer);
    let dataUsed = 8;
    const inodeLimit = Math.min(sourceLayout.inodeCount, maxInodes);

    for (let ino = 1; ino < inodeLimit; ino += 1) {
      const inodeBase = targetLayout.inodesPtr + ino * inodeBytes;
      const mode = targetInodeView.getUint32(inodeBase + MEMFS_INODE_OFF_MODE, true);
      const fileType = mode & MEMFS_S_IFMT;
      if (fileType === MEMFS_S_IFREG) {
        targetInodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_OFFSET, 0, true);
        targetInodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, 0, true);
        continue;
      }
      if (fileType !== MEMFS_S_IFLNK) {
        continue;
      }

      const size = Number(targetInodeView.getBigUint64(inodeBase + MEMFS_INODE_OFF_SIZE, true));
      const sourceDataOffset = targetInodeView.getUint32(inodeBase + MEMFS_INODE_OFF_DATA_OFFSET, true);
      const sourceDataCap = targetInodeView.getUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, true);
      if (
        size <= 0
        || sourceDataCap < size
        || sourceDataOffset + size > sourceLayout.dataCap
        || dataUsed + size > targetLayout.dataCap
      ) {
        targetInodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_OFFSET, 0, true);
        targetInodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, 0, true);
        continue;
      }

      ensureMemoryCapacityFor(targetExports.memory, targetLayout.dataPtr + dataUsed + size);
      targetBytes.set(
        sourceBytes.subarray(
          sourceLayout.dataPtr + sourceDataOffset,
          sourceLayout.dataPtr + sourceDataOffset + size,
        ),
        targetLayout.dataPtr + dataUsed,
      );
      targetInodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_OFFSET, dataUsed, true);
      targetInodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, size, true);
      dataUsed += size;
    }

    return dataUsed;
  }

  function copyMemfsState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    options: { includeData?: boolean } = {},
  ): void {
    const includeData = options.includeData ?? true;
    const sourceLayout = readMemfsLayout(sourceExports.memory, sourceState.memfsPtr);
    const targetLayout = readMemfsLayout(targetExports.memory, targetState.memfsPtr);
    const sourceDataUsed = includeData
      ? computeReferencedMemfsDataUsed(sourceExports, sourceLayout)
      : sourceLayout.dataUsed;
    copyMemoryRange(
      sourceExports.memory,
      sourceLayout.inodesPtr,
      targetExports.memory,
      targetLayout.inodesPtr,
      sourceExports.kernel_memfs_inodes_bytes(),
    );
    copyMemoryRange(
      sourceExports.memory,
      sourceLayout.firstChildPtr,
      targetExports.memory,
      targetLayout.firstChildPtr,
      sourceExports.kernel_memfs_first_child_bytes(),
    );
    copyMemoryRange(
      sourceExports.memory,
      sourceLayout.dirEntriesPtr,
      targetExports.memory,
      targetLayout.dirEntriesPtr,
      sourceExports.kernel_memfs_dir_entries_bytes(),
    );
    if (includeData) {
      const dataBytes = Math.min(sourceDataUsed, targetLayout.dataCap);
      copyMemoryRange(
        sourceExports.memory,
        sourceLayout.dataPtr,
        targetExports.memory,
        targetLayout.dataPtr,
        dataBytes,
      );
      clearMemoryRange(
        targetExports.memory,
        targetLayout.dataPtr + dataBytes,
        Math.max(0, targetLayout.dataCap - dataBytes),
      );
    } else {
      clearMaterializedMemfsFiles(targetExports);
      clearMemoryRange(
        targetExports.memory,
        targetExports.kernel_memfs_data_ptr(),
        targetExports.kernel_memfs_data_bytes(),
      );
    }
    copyMemoryRange(
      sourceExports.memory,
      sourceState.memfsPtr,
      targetExports.memory,
      targetState.memfsPtr,
      sourceExports.kernel_memfs_header_bytes(),
    );
    const targetHeader = new DataView(targetExports.memory.buffer, targetState.memfsPtr, 32);
    targetHeader.setUint32(0, sourceLayout.inodeCount, true);
    targetHeader.setUint32(4, sourceLayout.dirEntryCount, true);
    targetHeader.setUint32(8, includeData ? Math.min(sourceDataUsed, targetLayout.dataCap) : 8, true);
    targetHeader.setUint32(12, targetLayout.dataCap, true);
    targetHeader.setUint32(16, targetLayout.inodesPtr, true);
    targetHeader.setUint32(20, targetLayout.firstChildPtr, true);
    targetHeader.setUint32(24, targetLayout.dirEntriesPtr, true);
    targetHeader.setUint32(28, targetLayout.dataPtr, true);
    if (!includeData) {
      const dataUsed = preserveMetadataOnlySymlinkData(sourceExports, targetExports, sourceLayout, targetLayout);
      normalizeMetadataOnlyMemfsState(targetExports, targetState, sourceLayout, targetLayout, dataUsed);
    }
  }

  function clearMemfsState(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void {
    clearMaterializedMemfsFiles(targetExports);
    clearMemoryRange(
      targetExports.memory,
      targetExports.kernel_memfs_inodes_ptr(),
      targetExports.kernel_memfs_inodes_bytes(),
    );
    clearMemoryRange(
      targetExports.memory,
      targetExports.kernel_memfs_first_child_ptr(),
      targetExports.kernel_memfs_first_child_bytes(),
    );
    clearMemoryRange(
      targetExports.memory,
      targetExports.kernel_memfs_dir_entries_ptr(),
      targetExports.kernel_memfs_dir_entries_bytes(),
    );
    clearMemoryRange(
      targetExports.memory,
      targetExports.kernel_memfs_data_ptr(),
      targetExports.kernel_memfs_data_bytes(),
    );
    clearMemoryRange(
      targetExports.memory,
      targetState.memfsPtr,
      targetExports.kernel_memfs_header_bytes(),
    );
  }

  return {
    clearMemfsState,
    copyMemfsState,
  };
}
