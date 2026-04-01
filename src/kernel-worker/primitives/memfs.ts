import type { KernelExports, KernelRuntimeState } from "../../abi.js";
import {
  clearMaterializedMemfsFiles,
  clearMemoryRange,
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
    copyMemoryRange(
      sourceExports.memory,
      sourceLayout.inodesPtr,
      targetExports.memory,
      sourceLayout.inodesPtr,
      sourceExports.kernel_memfs_inodes_bytes(),
    );
    copyMemoryRange(
      sourceExports.memory,
      sourceLayout.firstChildPtr,
      targetExports.memory,
      sourceLayout.firstChildPtr,
      sourceExports.kernel_memfs_first_child_bytes(),
    );
    copyMemoryRange(
      sourceExports.memory,
      sourceLayout.dirEntriesPtr,
      targetExports.memory,
      sourceLayout.dirEntriesPtr,
      sourceExports.kernel_memfs_dir_entries_bytes(),
    );
    if (includeData) {
      copyMemoryRange(
        sourceExports.memory,
        sourceLayout.dataPtr,
        targetExports.memory,
        sourceLayout.dataPtr,
        sourceLayout.dataCap,
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
    if (!includeData) {
      normalizeMetadataOnlyMemfsState(targetExports, targetState, sourceLayout);
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
