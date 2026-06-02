import { attachKernelRuntimeState, type KernelExports, type KernelRuntimeState } from "../../abi.js";
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
  writeMemfsLayoutHeader,
} from "../common.js";
import {
  MEMFS_INODE_OFF_DATA_CAP,
  MEMFS_INODE_OFF_DATA_OFFSET,
  MEMFS_INODE_OFF_MODE,
  MEMFS_INODE_OFF_SIZE,
} from "../../layout/memfs.js";

const MEMFS_S_IFLNK = 0o120000;
const EXEC_PROT = 4;

function targetGuestBase(targetExports: KernelExports): number {
  const profile = targetExports.abi_guest_address_profile_target?.() ?? 0;
  const base = targetExports.process_guest_base_for_address_profile?.(profile) ?? 0n;
  return Number(base);
}

function formatSample(bytes: Uint8Array, offset: number): string {
  return [...bytes.subarray(offset, Math.min(bytes.length, offset + 16))]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function recordMemfsCopyExecOverlap(
  targetExports: KernelExports,
  label: string,
  targetOffset: number,
  byteLength: number,
  sourceBytes?: Uint8Array,
): void {
  if (byteLength <= 0) {
    return;
  }
  const guestBase = targetGuestBase(targetExports);
  if (!Number.isFinite(guestBase) || guestBase <= 0 || targetOffset < guestBase) {
    return;
  }
  const guestStart = BigInt(targetOffset - guestBase);
  const guestEnd = guestStart + BigInt(byteLength);
  const vmaLen = targetExports.debug_memory_vma_len?.() ?? 0;
  for (let index = 0; index < vmaLen; index += 1) {
    const prot = targetExports.debug_memory_vma_prot?.(index) ?? 0;
    if ((prot & EXEC_PROT) === 0) {
      continue;
    }
    const vmaStart = BigInt(targetExports.debug_memory_vma_start?.(index) ?? 0n);
    const vmaEnd = BigInt(targetExports.debug_memory_vma_end?.(index) ?? 0n);
    if (vmaStart >= vmaEnd || guestEnd <= vmaStart || guestStart >= vmaEnd) {
      continue;
    }
    const overlapStart = guestStart > vmaStart ? guestStart : vmaStart;
    const overlapEnd = guestEnd < vmaEnd ? guestEnd : vmaEnd;
    const sourceOffset = Number(overlapStart - guestStart);
    const sample = sourceBytes ? formatSample(sourceBytes, sourceOffset) : "clear";
    console.log([
      "kernel-worker-memfs-copy-exec-overlap",
      `label=${label}`,
      `target=0x${targetOffset.toString(16)}`,
      `guest=0x${guestStart.toString(16)}`,
      `len=${byteLength}`,
      `overlap=0x${overlapStart.toString(16)}-0x${overlapEnd.toString(16)}`,
      `vma=0x${vmaStart.toString(16)}-0x${vmaEnd.toString(16)}`,
      `prot=${prot}`,
      `fd=${targetExports.debug_memory_vma_fd?.(index) ?? "n/a"}`,
      `sample=${sample}`,
    ].join(":"));
  }
}

export function createKernelWorkerMemfsPrimitives() {
  function readRuntimeMemfsLayout(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): ReturnType<typeof readMemfsLayout> {
    const memfsPtr = typeof targetExports.kernel_memfs_ptr === "function"
      ? targetExports.kernel_memfs_ptr()
      : targetState.memfsPtr;
    const headerBytes = targetState.memfsHeaderBytes
      || targetExports.kernel_memfs_header_bytes?.()
      || 32;
    const layout = readMemfsLayout(targetExports.memory, memfsPtr, headerBytes);
    return {
      ...layout,
      dataCap: typeof targetExports.kernel_memfs_data_bytes === "function"
        ? targetExports.kernel_memfs_data_bytes()
        : targetState.memfsDataBytes || layout.dataCap,
      inodesPtr: typeof targetExports.kernel_memfs_inodes_ptr === "function"
        ? targetExports.kernel_memfs_inodes_ptr()
        : layout.inodesPtr,
      firstChildPtr: typeof targetExports.kernel_memfs_first_child_ptr === "function"
        ? targetExports.kernel_memfs_first_child_ptr()
        : layout.firstChildPtr,
      dirEntriesPtr: typeof targetExports.kernel_memfs_dir_entries_ptr === "function"
        ? targetExports.kernel_memfs_dir_entries_ptr()
        : layout.dirEntriesPtr,
      dataPtr: typeof targetExports.kernel_memfs_data_ptr === "function"
        ? targetExports.kernel_memfs_data_ptr()
        : layout.dataPtr,
    };
  }

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
    attachKernelRuntimeState(sourceExports, sourceState);
    const sourceLayout = readRuntimeMemfsLayout(sourceExports, sourceState);
    const sourceDataUsed = includeData
      ? computeReferencedMemfsDataUsed(sourceExports, sourceLayout)
      : sourceLayout.dataUsed;
    attachKernelRuntimeState(targetExports, targetState);
    const targetLayout = readRuntimeMemfsLayout(targetExports, targetState);
    const sourceInodesBytes = sourceState.memfsInodesBytes || sourceExports.kernel_memfs_inodes_bytes();
    const sourceFirstChildBytes = sourceState.memfsFirstChildBytes || sourceExports.kernel_memfs_first_child_bytes();
    const sourceDirEntriesBytes = sourceState.memfsDirEntriesBytes || sourceExports.kernel_memfs_dir_entries_bytes();
    const sourceHeaderBytes = sourceState.memfsHeaderBytes || sourceExports.kernel_memfs_header_bytes();
    const targetDataBytes = targetState.memfsDataBytes || targetExports.kernel_memfs_data_bytes();
    recordMemfsCopyExecOverlap(
      targetExports,
      "memfs-inodes",
      targetLayout.inodesPtr,
      sourceInodesBytes,
      new Uint8Array(sourceExports.memory.buffer, sourceLayout.inodesPtr, sourceInodesBytes),
    );
    copyMemoryRange(
      sourceExports.memory,
      sourceLayout.inodesPtr,
      targetExports.memory,
      targetLayout.inodesPtr,
      sourceInodesBytes,
    );
    recordMemfsCopyExecOverlap(
      targetExports,
      "memfs-first-child",
      targetLayout.firstChildPtr,
      sourceFirstChildBytes,
      new Uint8Array(sourceExports.memory.buffer, sourceLayout.firstChildPtr, sourceFirstChildBytes),
    );
    copyMemoryRange(
      sourceExports.memory,
      sourceLayout.firstChildPtr,
      targetExports.memory,
      targetLayout.firstChildPtr,
      sourceFirstChildBytes,
    );
    recordMemfsCopyExecOverlap(
      targetExports,
      "memfs-dir-entries",
      targetLayout.dirEntriesPtr,
      sourceDirEntriesBytes,
      new Uint8Array(sourceExports.memory.buffer, sourceLayout.dirEntriesPtr, sourceDirEntriesBytes),
    );
    copyMemoryRange(
      sourceExports.memory,
      sourceLayout.dirEntriesPtr,
      targetExports.memory,
      targetLayout.dirEntriesPtr,
      sourceDirEntriesBytes,
    );
    if (includeData) {
      const dataBytes = Math.min(sourceDataUsed, targetLayout.dataCap);
      recordMemfsCopyExecOverlap(
        targetExports,
        "memfs-data",
        targetLayout.dataPtr,
        dataBytes,
        new Uint8Array(sourceExports.memory.buffer, sourceLayout.dataPtr, dataBytes),
      );
      copyMemoryRange(
        sourceExports.memory,
        sourceLayout.dataPtr,
        targetExports.memory,
        targetLayout.dataPtr,
        dataBytes,
      );
      recordMemfsCopyExecOverlap(
        targetExports,
        "memfs-data-clear-tail",
        targetLayout.dataPtr + dataBytes,
        Math.max(0, targetLayout.dataCap - dataBytes),
      );
      clearMemoryRange(
        targetExports.memory,
        targetLayout.dataPtr + dataBytes,
        Math.max(0, targetLayout.dataCap - dataBytes),
      );
    } else {
      clearMaterializedMemfsFiles(targetExports);
      recordMemfsCopyExecOverlap(
        targetExports,
        "memfs-data-clear-all",
        targetLayout.dataPtr,
        targetDataBytes,
      );
      clearMemoryRange(
        targetExports.memory,
        targetLayout.dataPtr,
        targetDataBytes,
      );
    }
    recordMemfsCopyExecOverlap(
      targetExports,
      "memfs-header",
      targetState.memfsPtr,
      sourceHeaderBytes,
      new Uint8Array(sourceExports.memory.buffer, sourceState.memfsPtr, sourceHeaderBytes),
    );
    copyMemoryRange(
      sourceExports.memory,
      sourceState.memfsPtr,
      targetExports.memory,
      targetState.memfsPtr,
      sourceHeaderBytes,
    );
    writeMemfsLayoutHeader(
      targetExports.memory,
      targetState.memfsPtr,
      targetState.memfsHeaderBytes || targetExports.kernel_memfs_header_bytes(),
      {
        ...targetLayout,
        inodeCount: sourceLayout.inodeCount,
        dirEntryCount: sourceLayout.dirEntryCount,
        dataUsed: includeData ? Math.min(sourceDataUsed, targetLayout.dataCap) : 8,
      },
    );
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
      targetState.memfsDataBytes || targetExports.kernel_memfs_data_bytes(),
    );
    clearMemoryRange(
      targetExports.memory,
      targetState.memfsPtr,
      targetState.memfsHeaderBytes || targetExports.kernel_memfs_header_bytes(),
    );
  }

  return {
    clearMemfsState,
    copyMemfsState,
  };
}
