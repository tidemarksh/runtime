import type { KernelSyncHandle } from "../types.js";
import { clearKernelRegion, copyKernelRegion } from "./common.js";

export function clearMemfsState(target: KernelSyncHandle): void {
  clearKernelRegion(
    target,
    target.kernel.kernel_memfs_inodes_ptr(),
    target.kernel.kernel_memfs_inodes_bytes(),
  );
  clearKernelRegion(
    target,
    target.kernel.kernel_memfs_first_child_ptr(),
    target.kernel.kernel_memfs_first_child_bytes(),
  );
  clearKernelRegion(
    target,
    target.kernel.kernel_memfs_dir_entries_ptr(),
    target.kernel.kernel_memfs_dir_entries_bytes(),
  );
  clearKernelRegion(
    target,
    target.kernel.kernel_memfs_data_ptr(),
    target.kernel.kernel_memfs_data_bytes(),
  );
  clearKernelRegion(
    target,
    target.kernelState.memfsPtr,
    target.kernel.kernel_memfs_header_bytes(),
  );
}

export function syncMemfsState(source: KernelSyncHandle, target: KernelSyncHandle): void {
  copyKernelRegion(
    source,
    source.kernel.kernel_memfs_inodes_ptr(),
    target,
    target.kernel.kernel_memfs_inodes_ptr(),
    source.kernel.kernel_memfs_inodes_bytes(),
  );
  copyKernelRegion(
    source,
    source.kernel.kernel_memfs_first_child_ptr(),
    target,
    target.kernel.kernel_memfs_first_child_ptr(),
    source.kernel.kernel_memfs_first_child_bytes(),
  );
  copyKernelRegion(
    source,
    source.kernel.kernel_memfs_dir_entries_ptr(),
    target,
    target.kernel.kernel_memfs_dir_entries_ptr(),
    source.kernel.kernel_memfs_dir_entries_bytes(),
  );
  copyKernelRegion(
    source,
    source.kernel.kernel_memfs_data_ptr(),
    target,
    target.kernel.kernel_memfs_data_ptr(),
    source.kernel.kernel_memfs_data_bytes(),
  );
  copyKernelRegion(
    source,
    source.kernelState.memfsPtr,
    target,
    target.kernelState.memfsPtr,
    source.kernel.kernel_memfs_header_bytes(),
  );
}
