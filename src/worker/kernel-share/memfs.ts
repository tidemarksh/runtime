import type { KernelSyncHandle } from "../types.js";
import { clearKernelRegion, copyKernelRegion } from "./common.js";

type MemfsLayout = {
  inodesPtr: number;
  firstChildPtr: number;
  dirEntriesPtr: number;
  dataPtr: number;
  dataCap: number;
};

function readMemfsLayout(handle: KernelSyncHandle): MemfsLayout {
  const view = new DataView(handle.memory.buffer, handle.kernelState.memfsPtr, 32);
  return {
    dataCap: view.getUint32(12, true),
    inodesPtr: view.getUint32(16, true),
    firstChildPtr: view.getUint32(20, true),
    dirEntriesPtr: view.getUint32(24, true),
    dataPtr: view.getUint32(28, true),
  };
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
    Math.min(sourceLayout.dataCap, targetLayout.dataCap),
  );
  copyKernelRegion(
    source,
    source.kernelState.memfsPtr,
    target,
    target.kernelState.memfsPtr,
    source.kernel.kernel_memfs_header_bytes(),
  );
}
