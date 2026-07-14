import {
  attachKernelRuntimeState,
  type KernelExports,
  type KernelRuntimeState,
} from "../../abi.js";
import {
  clearMemoryRange,
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
  function copyActiveTable(
    sourceExports: KernelExports,
    sourcePtr: number,
    sourceTotalBytes: number,
    sourceCount: number,
    targetExports: KernelExports,
    targetPtr: number,
    targetTotalBytes: number,
    targetCount: number,
    maxEntries: number,
  ): void {
    if (maxEntries <= 0) {
      return;
    }
    const sourceEntryBytes = sourceTotalBytes / maxEntries;
    const targetEntryBytes = targetTotalBytes / maxEntries;
    if (!Number.isInteger(sourceEntryBytes) || sourceEntryBytes !== targetEntryBytes) {
      throw new Error("incompatible MemFS table layout");
    }
    const copiedEntries = Math.min(Math.max(sourceCount, 0), maxEntries);
    const previousTargetEntries = Math.min(Math.max(targetCount, 0), maxEntries);
    if (previousTargetEntries > copiedEntries) {
      clearMemoryRange(
        targetExports.memory,
        targetPtr + copiedEntries * targetEntryBytes,
        (previousTargetEntries - copiedEntries) * targetEntryBytes,
      );
    }
    copyMemoryRange(
      sourceExports.memory,
      sourcePtr,
      targetExports.memory,
      targetPtr,
      copiedEntries * sourceEntryBytes,
    );
  }

  function preserveCopiedInlineData(
    sourceExports: KernelExports,
    targetExports: KernelExports,
    sourceLayout: ReturnType<typeof readMemfsLayout>,
    targetLayout: ReturnType<typeof readMemfsLayout>,
    preserveRegularData: boolean,
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
        if (!preserveRegularData) {
          targetInodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_OFFSET, 0, true);
          targetInodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, 0, true);
          continue;
        }
      } else if (fileType !== MEMFS_S_IFLNK) {
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
    const sourceLayout = readMemfsLayout(sourceExports.memory, sourceState.memfsPtr, sourceExports);
    const targetLayout = readMemfsLayout(targetExports.memory, targetState.memfsPtr, targetExports);
    const maxInodes = sourceExports.kernel_memfs_first_child_bytes() / 4;
    copyActiveTable(
      sourceExports,
      sourceLayout.inodesPtr,
      sourceExports.kernel_memfs_inodes_bytes(),
      sourceLayout.inodeCount,
      targetExports,
      targetLayout.inodesPtr,
      targetExports.kernel_memfs_inodes_bytes(),
      targetLayout.inodeCount,
      maxInodes,
    );
    copyActiveTable(
      sourceExports,
      sourceLayout.firstChildPtr,
      sourceExports.kernel_memfs_first_child_bytes(),
      sourceLayout.inodeCount,
      targetExports,
      targetLayout.firstChildPtr,
      targetExports.kernel_memfs_first_child_bytes(),
      targetLayout.inodeCount,
      maxInodes,
    );
    copyActiveTable(
      sourceExports,
      sourceLayout.dirEntriesPtr,
      sourceExports.kernel_memfs_dir_entries_bytes(),
      sourceLayout.dirEntryCount,
      targetExports,
      targetLayout.dirEntriesPtr,
      targetExports.kernel_memfs_dir_entries_bytes(),
      targetLayout.dirEntryCount,
      maxInodes,
    );
    clearMemoryRange(
      targetExports.memory,
      targetLayout.dataPtr,
      Math.min(targetLayout.dataUsed, targetLayout.dataCap),
    );
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
    targetHeader.setUint32(8, 8, true);
    targetHeader.setUint32(12, targetLayout.dataCap, true);
    targetHeader.setUint32(16, targetLayout.inodesPtr, true);
    targetHeader.setUint32(20, targetLayout.firstChildPtr, true);
    targetHeader.setUint32(24, targetLayout.dirEntriesPtr, true);
    targetHeader.setUint32(28, targetLayout.dataPtr, true);
    const preserveRegularData = options.includeData === true;
    const dataUsed = preserveCopiedInlineData(
      sourceExports,
      targetExports,
      sourceLayout,
      targetLayout,
      preserveRegularData,
    );
    normalizeMetadataOnlyMemfsState(
      targetExports,
      targetState,
      sourceLayout,
      targetLayout,
      dataUsed,
      { clearRegularData: !preserveRegularData },
    );
    attachKernelRuntimeState(targetExports, targetState);
  }

  function clearMemfsState(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void {
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
