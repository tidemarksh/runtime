import type { KernelSyncHandle } from "../types.js";
import type { KernelExports } from "../../abi.js";
import {
  MEMFS_INODE_OFF_DATA_CAP,
  MEMFS_INODE_OFF_DATA_OFFSET,
  MEMFS_INODE_OFF_MODE,
  MEMFS_INODE_OFF_SIZE,
} from "../../layout/memfs.js";
import { clearKernelRegion, copyKernelRegion } from "./common.js";
import { ensureKernelMemoryCapacity, kernelScratchBase } from "../kernel-scratch.js";

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
const MEMFS_S_IFLNK = 0o120000;
const encoder = new TextEncoder();

type ProcfsMemfsExports = Pick<
  KernelExports,
  | "memfs_resolve_path"
  | "memfs_inode_mode"
  | "memfs_read_link"
  | "memfs_unlink"
  | "memfs_create_symlink"
>;

function writeMemfsScratch(handle: KernelSyncHandle, bytes: Uint8Array, offset = 0): number {
  const scratch = kernelScratchBase(handle.kernel) + offset;
  ensureKernelMemoryCapacity(handle.kernel, scratch + bytes.length);
  new Uint8Array(handle.memory.buffer).set(bytes, scratch);
  return scratch;
}

function resolveMemfsPath(handle: KernelSyncHandle, path: string): number {
  const memfs = handle.kernel as Partial<ProcfsMemfsExports>;
  if (typeof memfs.memfs_resolve_path !== "function") {
    return 0;
  }
  const normalized = `/${path.split("/").filter(Boolean).join("/")}`;
  const bytes = encoder.encode(normalized || "/");
  const ptr = writeMemfsScratch(handle, bytes);
  return memfs.memfs_resolve_path(ptr, bytes.length);
}

export function snapshotProcSelfExe(handle: KernelSyncHandle): Uint8Array | null {
  const memfs = handle.kernel as Partial<ProcfsMemfsExports>;
  if (
    typeof memfs.memfs_inode_mode !== "function"
    || typeof memfs.memfs_read_link !== "function"
  ) {
    return null;
  }
  const ino = resolveMemfsPath(handle, "/proc/self/exe");
  if (ino === 0 || (memfs.memfs_inode_mode(ino) & MEMFS_S_IFMT) !== MEMFS_S_IFLNK) {
    return null;
  }
  const bufPtr = kernelScratchBase(handle.kernel) + 0x34000;
  const bufLen = 4096;
  ensureKernelMemoryCapacity(handle.kernel, bufPtr + bufLen);
  const len = Number(memfs.memfs_read_link(ino, bufPtr, bufLen));
  if (len <= 0) {
    return null;
  }
  return new Uint8Array(handle.memory.buffer.slice(bufPtr, bufPtr + len));
}

export function restoreProcSelfExe(handle: KernelSyncHandle, target: Uint8Array | null): void {
  const memfs = handle.kernel as Partial<ProcfsMemfsExports>;
  if (
    typeof memfs.memfs_unlink !== "function"
    || typeof memfs.memfs_create_symlink !== "function"
  ) {
    return;
  }
  const selfIno = resolveMemfsPath(handle, "/proc/self");
  if (selfIno === 0) {
    return;
  }
  const exeName = encoder.encode("exe");
  const exeNamePtr = writeMemfsScratch(handle, exeName, 0x200);
  memfs.memfs_unlink(selfIno, exeNamePtr, exeName.length);
  if (!target || target.length === 0) {
    return;
  }
  const targetPtr = writeMemfsScratch(handle, target, 0x300);
  memfs.memfs_create_symlink(selfIno, exeNamePtr, exeName.length, targetPtr, target.length);
}

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

function preserveMetadataOnlySymlinkData(
  source: KernelSyncHandle,
  target: KernelSyncHandle,
  sourceLayout: MemfsLayout,
  targetLayout: MemfsLayout,
): number {
  const maxInodes = target.kernel.kernel_memfs_first_child_bytes() / 4;
  if (maxInodes <= 0) {
    return 8;
  }
  const inodeBytes = target.kernel.kernel_memfs_inodes_bytes() / maxInodes;
  if (inodeBytes < 32) {
    return 8;
  }

  const sourceBytes = new Uint8Array(source.memory.buffer);
  const targetBytes = new Uint8Array(target.memory.buffer);
  const targetInodeView = new DataView(target.memory.buffer);
  const inodeLimit = Math.min(sourceLayout.inodeCount, maxInodes);
  let dataUsed = 8;

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
      || sourceLayout.dataPtr + sourceDataOffset + size > source.memory.buffer.byteLength
      || dataUsed + size > targetLayout.dataCap
    ) {
      targetInodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_OFFSET, 0, true);
      targetInodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, 0, true);
      continue;
    }

    ensureKernelMemoryCapacity(target.kernel, targetLayout.dataPtr + dataUsed + size);
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
  const procSelfExe = snapshotProcSelfExe(target);
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
  clearKernelRegion(
    target,
    targetLayout.dataPtr,
    targetLayout.dataCap,
  );
  const dataUsed = preserveMetadataOnlySymlinkData(source, target, sourceLayout, targetLayout);
  copyKernelRegion(
    source,
    source.kernelState.memfsPtr,
    target,
    target.kernelState.memfsPtr,
    source.kernel.kernel_memfs_header_bytes(),
  );
  const targetHeader = new DataView(target.memory.buffer, target.kernelState.memfsPtr, 32);
  targetHeader.setUint32(0, sourceLayout.inodeCount, true);
  targetHeader.setUint32(8, dataUsed, true);
  targetHeader.setUint32(12, targetLayout.dataCap, true);
  targetHeader.setUint32(16, targetLayout.inodesPtr, true);
  targetHeader.setUint32(20, targetLayout.firstChildPtr, true);
  targetHeader.setUint32(24, targetLayout.dirEntriesPtr, true);
  targetHeader.setUint32(28, targetLayout.dataPtr, true);
  restoreProcSelfExe(target, procSelfExe);
}
