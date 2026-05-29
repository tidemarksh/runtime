import {
  kernelShareOtherMembers,
  restoreProcSelfExe,
  snapshotProcSelfExe,
} from "../../kernel-share.js";
import type { ProcessHandle } from "../../types.js";

export function applyThreadMemfsKernelWriteToForkShareMembers(
  handle: ProcessHandle,
  ptr: number,
  bytes: Uint8Array,
): void {
  const targetOffset = memfsKernelWriteOffset(handle, ptr);
  if (targetOffset === null) {
    return;
  }
  for (const member of kernelShareOtherMembers(handle)) {
    const targetPtr = memfsKernelWriteTargetPtr(member, targetOffset);
    if (targetPtr === null) {
      continue;
    }
    const targetMemory = new Uint8Array(member.memory.buffer);
    if (targetPtr < 0 || targetPtr + bytes.length > targetMemory.byteLength) {
      continue;
    }
    const procSelfExe = snapshotProcSelfExe(member);
    targetMemory.set(bytes, targetPtr);
    restoreProcSelfExe(member, procSelfExe);
  }
}

function memfsKernelWriteOffset(
  handle: ProcessHandle,
  ptr: number,
): { region: "header" | "inodes" | "firstChild" | "dirEntries" | "data"; offset: number } | null {
  const regions: Array<ReturnType<typeof memfsKernelWriteOffset> & { start: number; length: number }> = [
    { region: "header", start: handle.kernelState.memfsPtr, length: handle.kernelState.memfsHeaderBytes, offset: 0 },
    { region: "inodes", start: handle.kernel.kernel_memfs_inodes_ptr?.() ?? 0, length: handle.kernelState.memfsInodesBytes, offset: 0 },
    { region: "firstChild", start: handle.kernel.kernel_memfs_first_child_ptr?.() ?? 0, length: handle.kernelState.memfsFirstChildBytes, offset: 0 },
    { region: "dirEntries", start: handle.kernel.kernel_memfs_dir_entries_ptr?.() ?? 0, length: handle.kernelState.memfsDirEntriesBytes, offset: 0 },
    { region: "data", start: handle.kernel.kernel_memfs_data_ptr?.() ?? 0, length: handle.kernelState.memfsDataBytes, offset: 0 },
  ];
  for (const region of regions) {
    if (region.start > 0 && region.length > 0 && ptr >= region.start && ptr < region.start + region.length) {
      return { region: region.region, offset: ptr - region.start };
    }
  }
  return null;
}

function memfsKernelWriteTargetPtr(
  handle: ProcessHandle,
  sourceOffset: NonNullable<ReturnType<typeof memfsKernelWriteOffset>>,
): number | null {
  switch (sourceOffset.region) {
    case "header":
      return handle.kernelState.memfsPtr + sourceOffset.offset;
    case "inodes":
      return handle.kernel.kernel_memfs_inodes_ptr === undefined
        ? null
        : handle.kernel.kernel_memfs_inodes_ptr() + sourceOffset.offset;
    case "firstChild":
      return handle.kernel.kernel_memfs_first_child_ptr === undefined
        ? null
        : handle.kernel.kernel_memfs_first_child_ptr() + sourceOffset.offset;
    case "dirEntries":
      return handle.kernel.kernel_memfs_dir_entries_ptr === undefined
        ? null
        : handle.kernel.kernel_memfs_dir_entries_ptr() + sourceOffset.offset;
    case "data":
      return handle.kernel.kernel_memfs_data_ptr === undefined
        ? null
        : handle.kernel.kernel_memfs_data_ptr() + sourceOffset.offset;
  }
}
