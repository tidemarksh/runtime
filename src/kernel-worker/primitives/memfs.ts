import type { KernelExports, KernelRuntimeState } from "../../abi.js";
import {
  clearMaterializedMemfsFiles,
  clearMemoryRange,
  computeReferencedMemfsDataUsed,
  copyMemoryRange,
  normalizeMetadataOnlyMemfsState,
  readMemfsLayout,
} from "../common.js";

export function createKernelWorkerMemfsPrimitives() {
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
      normalizeMetadataOnlyMemfsState(targetExports, targetState, sourceLayout, targetLayout);
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
