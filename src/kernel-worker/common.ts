import type { KernelExports, KernelRuntimeState } from "../abi.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import { growKernelMemory } from "../wasm/kernel-memory.js";
import {
  MEMFS_HEADER_OFF_DATA_CAP,
  MEMFS_HEADER_OFF_DATA_PTR,
  MEMFS_HEADER_OFF_DATA_USED,
  MEMFS_HEADER_OFF_DIR_ENTRIES_PTR,
  MEMFS_HEADER_OFF_DIR_ENTRY_COUNT,
  MEMFS_HEADER_OFF_FIRST_CHILD_PTR,
  MEMFS_HEADER_OFF_INODES_PTR,
  MEMFS_HEADER_OFF_INODE_COUNT,
  MEMFS_HEADER_BYTES_FALLBACK,
  MEMFS_HEADER64_BYTES_FALLBACK,
  MEMFS_HEADER64_OFF_DATA_PTR,
  MEMFS_HEADER64_OFF_DIR_ENTRIES_PTR,
  MEMFS_HEADER64_OFF_FIRST_CHILD_PTR,
  MEMFS_HEADER64_OFF_INODES_PTR,
  MEMFS_INODE_OFF_DATA_CAP,
  MEMFS_INODE_OFF_DATA_OFFSET,
  MEMFS_INODE_OFF_MODE,
  MEMFS_INODE_OFF_SIZE,
} from "../layout/memfs.js";
export {
  MEMFS_S_IFMT,
  MEMFS_S_IFREG,
  memfsFileHasInlineData,
} from "../fs/memfs-inline-data.js";
import { MEMFS_S_IFMT, MEMFS_S_IFREG } from "../fs/memfs-inline-data.js";

const materializedMemfsFiles = new WeakMap<KernelExports, Set<number>>();
const scratchReservations = new WeakMap<KernelExports, number>();
const memoryExports = new WeakMap<WebAssembly.Memory, KernelExports>();
const SCRATCH_GUARD_BYTES = 0x10000;
const SCRATCH_RESERVATION_BYTES = 0x1000;
const KERNEL_WORKER_COMMON_COPY_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_KERNEL_WORKER_COMMON_COPY_DEBUG ?? "") === "1";
const EXEC_PROT = 4;
const COMMON_COPY_TRACE_CAP = 128;
const commonCopyTraceCounts = new WeakMap<KernelExports, number>();
const textDecoder = new TextDecoder();

export type MemfsLayout = {
  inodeCount: number;
  dirEntryCount: number;
  dataUsed: number;
  dataCap: number;
  inodesPtr: number;
  firstChildPtr: number;
  dirEntriesPtr: number;
  dataPtr: number;
};

function usesMemfsHeader64(headerBytes: number | undefined): boolean {
  return (headerBytes ?? MEMFS_HEADER_BYTES_FALLBACK) >= MEMFS_HEADER64_BYTES_FALLBACK;
}

function readMemfsHeaderPtr(view: DataView, headerBytes: number | undefined, offset32: number, offset64: number): number {
  if (!usesMemfsHeader64(headerBytes)) {
    return view.getUint32(offset32, true);
  }
  const value = view.getBigUint64(offset64, true);
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`memfs pointer exceeds JS safe integer: ${value.toString()}`);
  }
  return Number(value);
}

function writeMemfsHeaderPtr(view: DataView, headerBytes: number | undefined, offset32: number, offset64: number, value: number): void {
  if (!usesMemfsHeader64(headerBytes)) {
    view.setUint32(offset32, value, true);
    return;
  }
  view.setBigUint64(offset64, BigInt(value), true);
}

function targetGuestBase(targetExports: KernelExports): number {
  const profile = targetExports.abi_guest_address_profile_target?.() ?? 0;
  const base = targetExports.process_guest_base_for_address_profile?.(profile) ?? 0n;
  return Number(base);
}

function sampleHex(bytes: Uint8Array, offset: number, length = 16): string {
  return [...bytes.subarray(offset, Math.min(bytes.length, offset + length))]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function asciiSample(bytes: Uint8Array, offset: number, length = 32): string {
  return [...bytes.subarray(offset, Math.min(bytes.length, offset + length))]
    .map((byte) => (byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : "."))
    .join("");
}

function readVmaPath(targetExports: KernelExports, index: number): string {
  const len = targetExports.debug_memory_vma_path_len?.(index) ?? 0;
  if (len <= 0) {
    return "";
  }
  const bytes: number[] = [];
  for (let offset = 0; offset < Math.min(len, 160); offset += 1) {
    const byte = targetExports.debug_memory_vma_path_byte?.(index, offset) ?? 0;
    if (byte === 0) {
      break;
    }
    bytes.push(byte & 0xff);
  }
  return textDecoder.decode(new Uint8Array(bytes));
}

function rememberKernelExports(targetExports: KernelExports): void {
  memoryExports.set(targetExports.memory, targetExports);
}

function targetExportsForMemory(memory: WebAssembly.Memory): KernelExports | undefined {
  return memoryExports.get(memory);
}

function recordKernelWorkerCommonCopyIfExecOverlap(
  targetExports: KernelExports | undefined,
  label: string,
  targetOffset: number,
  byteLength: number,
  sourceBytes?: Uint8Array,
): void {
  if (!targetExports || !KERNEL_WORKER_COMMON_COPY_DEBUG_ENABLED || byteLength <= 0) {
    return;
  }
  const current = commonCopyTraceCounts.get(targetExports) ?? 0;
  if (current >= COMMON_COPY_TRACE_CAP) {
    if (current === COMMON_COPY_TRACE_CAP) {
      console.log(`kernel-worker-common-copy-exec-overlap-suppressed:label=${label}`);
    }
    commonCopyTraceCounts.set(targetExports, current + 1);
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
    const sampleOffset = Number(overlapStart - guestStart);
    const sampleSource = sourceBytes ?? new Uint8Array(Math.min(16, Math.max(0, byteLength - sampleOffset)));
    const sample = sampleHex(sampleSource, sourceBytes ? sampleOffset : 0, 16);
    const ascii = asciiSample(sampleSource, sourceBytes ? sampleOffset : 0, 32);
    console.log([
      "kernel-worker-common-copy-exec-overlap",
      `label=${label}`,
      `target=0x${targetOffset.toString(16)}`,
      `guest=0x${guestStart.toString(16)}`,
      `len=${byteLength}`,
      `overlap=0x${overlapStart.toString(16)}-0x${overlapEnd.toString(16)}`,
      `vma=0x${vmaStart.toString(16)}-0x${vmaEnd.toString(16)}`,
      `prot=${prot}`,
      `fd=${targetExports.debug_memory_vma_fd?.(index) ?? "n/a"}`,
      `off=0x${BigInt(targetExports.debug_memory_vma_offset?.(index) ?? 0).toString(16)}`,
      `path=${JSON.stringify(readVmaPath(targetExports, index))}`,
      `file=0x${BigInt.asUintN(64, BigInt(targetExports.debug_memory_vma_file_sample?.(index, overlapStart) ?? 0)).toString(16)}/${targetExports.debug_memory_vma_file_sample_len?.(index, overlapStart) ?? 0}`,
      `sample=${sample}`,
      `ascii=${JSON.stringify(ascii)}`,
    ].join(":"));
    commonCopyTraceCounts.set(targetExports, current + 1);
  }
  const ownedLen = targetExports.debug_kernel_owned_guest_range_len?.() ?? 0;
  for (let index = 0; index < ownedLen; index += 1) {
    const ownedStart = BigInt(targetExports.debug_kernel_owned_guest_range_start?.(index) ?? 0n);
    const ownedEnd = BigInt(targetExports.debug_kernel_owned_guest_range_end?.(index) ?? 0n);
    if (ownedStart >= ownedEnd || guestEnd <= ownedStart || guestStart >= ownedEnd) {
      continue;
    }
    const overlapStart = guestStart > ownedStart ? guestStart : ownedStart;
    const overlapEnd = guestEnd < ownedEnd ? guestEnd : ownedEnd;
    const sampleOffset = Number(overlapStart - guestStart);
    const sampleSource = sourceBytes ?? new Uint8Array(Math.min(16, Math.max(0, byteLength - sampleOffset)));
    console.log([
      "kernel-worker-common-copy-kernel-owned-overlap",
      `label=${label}`,
      `target=0x${targetOffset.toString(16)}`,
      `guest=0x${guestStart.toString(16)}`,
      `len=${byteLength}`,
      `overlap=0x${overlapStart.toString(16)}-0x${overlapEnd.toString(16)}`,
      `owned=0x${ownedStart.toString(16)}-0x${ownedEnd.toString(16)}`,
      `sample=${sampleHex(sampleSource, sourceBytes ? sampleOffset : 0, 16)}`,
      `ascii=${JSON.stringify(asciiSample(sampleSource, sourceBytes ? sampleOffset : 0, 32))}`,
    ].join(":"));
    commonCopyTraceCounts.set(targetExports, current + 1);
  }
}

export function normalizePermMode(mode: number | undefined, fallback: number): number {
  return (mode ?? fallback) & 0o7777;
}

export function normalizePath(path: string): string {
  const normalized = `/${path.split("/").filter(Boolean).join("/")}`;
  return normalized === "//" ? "/" : normalized || "/";
}

export function pathParts(path: string): string[] {
  return normalizePath(path).split("/").filter(Boolean);
}

export function cloneSnapshotEntry(entry: FsSnapshotEntry): FsSnapshotEntry {
  if (entry.kind === "file") {
    return {
      kind: "file",
      path: entry.path,
      ino: entry.ino,
      data: entry.data ? new Uint8Array(entry.data) : undefined,
      size: entry.size ?? entry.data?.length ?? 0,
      mode: entry.mode,
    };
  }
  if (entry.kind === "symlink") {
    return {
      kind: "symlink",
      path: entry.path,
      ino: entry.ino,
      target: entry.target ? new Uint8Array(entry.target) : undefined,
      size: entry.size ?? entry.target?.length ?? 0,
      mode: entry.mode,
    };
  }
  return {
    kind: "dir",
    path: entry.path,
    ino: entry.ino,
    mode: entry.mode,
  };
}

export function scratchBaseFor(targetExports: KernelExports): number {
  rememberKernelExports(targetExports);
  const heapScratch = targetExports.kernel_heap_ptr() + SCRATCH_GUARD_BYTES;
  const memfsEnds = [
    typeof targetExports.kernel_memfs_ptr === "function"
      && typeof targetExports.kernel_memfs_header_bytes === "function"
      ? targetExports.kernel_memfs_ptr() + targetExports.kernel_memfs_header_bytes()
      : 0,
    typeof targetExports.kernel_memfs_inodes_ptr === "function"
      && typeof targetExports.kernel_memfs_inodes_bytes === "function"
      ? targetExports.kernel_memfs_inodes_ptr() + targetExports.kernel_memfs_inodes_bytes()
      : 0,
    typeof targetExports.kernel_memfs_first_child_ptr === "function"
      && typeof targetExports.kernel_memfs_first_child_bytes === "function"
      ? targetExports.kernel_memfs_first_child_ptr() + targetExports.kernel_memfs_first_child_bytes()
      : 0,
    typeof targetExports.kernel_memfs_dir_entries_ptr === "function"
      && typeof targetExports.kernel_memfs_dir_entries_bytes === "function"
      ? targetExports.kernel_memfs_dir_entries_ptr() + targetExports.kernel_memfs_dir_entries_bytes()
      : 0,
    typeof targetExports.kernel_memfs_data_ptr === "function"
      && typeof targetExports.kernel_memfs_data_bytes === "function"
      ? targetExports.kernel_memfs_data_ptr() + targetExports.kernel_memfs_data_bytes()
      : 0,
  ];
  const exportedSafeBase = Math.max(
    heapScratch,
    ...memfsEnds.map((end) => end > 0 ? end + SCRATCH_GUARD_BYTES : 0),
  );
  const reservedBase = scratchReservations.get(targetExports);
  if (reservedBase !== undefined && reservedBase >= exportedSafeBase) {
    return reservedBase;
  }
  const hasMemfsRegion = memfsEnds.some((end) => end > 0);
  const memoryEndSafeBase = !hasMemfsRegion && exportedSafeBase < targetExports.memory.buffer.byteLength
    ? targetExports.memory.buffer.byteLength + SCRATCH_GUARD_BYTES
    : 0;
  const nextBase = (Math.max(exportedSafeBase, memoryEndSafeBase) + 15) & ~15;
  scratchReservations.set(targetExports, nextBase);
  return nextBase;
}

export function ensureMemoryCapacityFor(memory: WebAssembly.Memory, endOffset: number): void {
  const pageSize = 65536;
  const currentBytes = memory.buffer.byteLength;
  if (endOffset <= currentBytes) {
    return;
  }
  const additionalPages = Math.ceil((endOffset - currentBytes) / pageSize);
  growKernelMemory(memory, additionalPages);
}

export function writeScratchFor(targetExports: KernelExports, bytes: Uint8Array, offset = 0): number {
  rememberKernelExports(targetExports);
  const scratch = scratchBaseFor(targetExports) + offset;
  ensureMemoryCapacityFor(targetExports.memory, scratch + Math.max(bytes.length, SCRATCH_RESERVATION_BYTES));
  recordKernelWorkerCommonCopyIfExecOverlap(targetExports, "writeScratchFor", scratch, bytes.length, bytes);
  const mem = new Uint8Array(targetExports.memory.buffer);
  mem.set(bytes, scratch);
  return scratch;
}

export function copyMemoryRange(
  sourceMemory: WebAssembly.Memory,
  sourceOffset: number,
  targetMemory: WebAssembly.Memory,
  targetOffset: number,
  byteLength: number,
): void {
  if (byteLength <= 0) {
    return;
  }
  ensureMemoryCapacityFor(targetMemory, targetOffset + byteLength);
  const source = new Uint8Array(sourceMemory.buffer, sourceOffset, byteLength);
  recordKernelWorkerCommonCopyIfExecOverlap(targetExportsForMemory(targetMemory), "copyMemoryRange", targetOffset, byteLength, source);
  new Uint8Array(targetMemory.buffer).set(source, targetOffset);
}

export function clearMemoryRange(
  targetMemory: WebAssembly.Memory,
  targetOffset: number,
  byteLength: number,
): void {
  if (byteLength <= 0) {
    return;
  }
  ensureMemoryCapacityFor(targetMemory, targetOffset + byteLength);
  recordKernelWorkerCommonCopyIfExecOverlap(targetExportsForMemory(targetMemory), "clearMemoryRange", targetOffset, byteLength);
  new Uint8Array(targetMemory.buffer, targetOffset, byteLength).fill(0);
}

export function markMaterializedMemfsFile(targetExports: KernelExports, ino: number): void {
  let inos = materializedMemfsFiles.get(targetExports);
  if (!inos) {
    inos = new Set<number>();
    materializedMemfsFiles.set(targetExports, inos);
  }
  inos.add(ino);
}

export function clearMaterializedMemfsFiles(targetExports: KernelExports): void {
  materializedMemfsFiles.delete(targetExports);
}

export function readMemfsLayout(memory: WebAssembly.Memory, memfsPtr: number, headerBytes = MEMFS_HEADER_BYTES_FALLBACK): MemfsLayout {
  const view = new DataView(memory.buffer, memfsPtr, Math.max(headerBytes, MEMFS_HEADER_BYTES_FALLBACK));
  return {
    inodeCount: view.getUint32(MEMFS_HEADER_OFF_INODE_COUNT, true),
    dirEntryCount: view.getUint32(MEMFS_HEADER_OFF_DIR_ENTRY_COUNT, true),
    dataUsed: view.getUint32(MEMFS_HEADER_OFF_DATA_USED, true),
    dataCap: view.getUint32(MEMFS_HEADER_OFF_DATA_CAP, true),
    inodesPtr: readMemfsHeaderPtr(view, headerBytes, MEMFS_HEADER_OFF_INODES_PTR, MEMFS_HEADER64_OFF_INODES_PTR),
    firstChildPtr: readMemfsHeaderPtr(view, headerBytes, MEMFS_HEADER_OFF_FIRST_CHILD_PTR, MEMFS_HEADER64_OFF_FIRST_CHILD_PTR),
    dirEntriesPtr: readMemfsHeaderPtr(view, headerBytes, MEMFS_HEADER_OFF_DIR_ENTRIES_PTR, MEMFS_HEADER64_OFF_DIR_ENTRIES_PTR),
    dataPtr: readMemfsHeaderPtr(view, headerBytes, MEMFS_HEADER_OFF_DATA_PTR, MEMFS_HEADER64_OFF_DATA_PTR),
  };
}

export function writeMemfsLayoutHeader(
  memory: WebAssembly.Memory,
  memfsPtr: number,
  headerBytes: number,
  layout: MemfsLayout,
): void {
  const view = new DataView(memory.buffer, memfsPtr, Math.max(headerBytes, MEMFS_HEADER_BYTES_FALLBACK));
  view.setUint32(MEMFS_HEADER_OFF_INODE_COUNT, layout.inodeCount, true);
  view.setUint32(MEMFS_HEADER_OFF_DIR_ENTRY_COUNT, layout.dirEntryCount, true);
  view.setUint32(MEMFS_HEADER_OFF_DATA_USED, layout.dataUsed, true);
  view.setUint32(MEMFS_HEADER_OFF_DATA_CAP, layout.dataCap, true);
  writeMemfsHeaderPtr(view, headerBytes, MEMFS_HEADER_OFF_INODES_PTR, MEMFS_HEADER64_OFF_INODES_PTR, layout.inodesPtr);
  writeMemfsHeaderPtr(view, headerBytes, MEMFS_HEADER_OFF_FIRST_CHILD_PTR, MEMFS_HEADER64_OFF_FIRST_CHILD_PTR, layout.firstChildPtr);
  writeMemfsHeaderPtr(view, headerBytes, MEMFS_HEADER_OFF_DIR_ENTRIES_PTR, MEMFS_HEADER64_OFF_DIR_ENTRIES_PTR, layout.dirEntriesPtr);
  writeMemfsHeaderPtr(view, headerBytes, MEMFS_HEADER_OFF_DATA_PTR, MEMFS_HEADER64_OFF_DATA_PTR, layout.dataPtr);
}

export function computeReferencedMemfsDataUsed(
  targetExports: KernelExports,
  layout = readMemfsLayout(targetExports.memory, targetExports.kernel_memfs_ptr()),
): number {
  const maxInodes = targetExports.kernel_memfs_first_child_bytes() / 4;
  if (maxInodes <= 0) {
    return Math.min(layout.dataUsed, layout.dataCap);
  }
  const inodeBytes = targetExports.kernel_memfs_inodes_bytes() / maxInodes;
  if (inodeBytes < 32) {
    return Math.min(layout.dataUsed, layout.dataCap);
  }
  const inodeLimit = Math.min(layout.inodeCount, maxInodes);
  const inodeView = new DataView(targetExports.memory.buffer);
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

export function readMemfsFileDataDirect(targetExports: KernelExports, ino: number): Uint8Array | null {
  const layout = readMemfsLayout(
    targetExports.memory,
    targetExports.kernel_memfs_ptr(),
    targetExports.kernel_memfs_header_bytes(),
  );
  const maxInodes = targetExports.kernel_memfs_first_child_bytes() / 4;
  const inodeBytes = targetExports.kernel_memfs_inodes_bytes() / maxInodes;
  const inodeBase = layout.inodesPtr + ino * inodeBytes;
  const inodeView = new DataView(targetExports.memory.buffer);
  const mode = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_MODE, true);
  if ((mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
    return null;
  }
  const size = Number(inodeView.getBigUint64(inodeBase + MEMFS_INODE_OFF_SIZE, true));
  if (size <= 0) {
    return new Uint8Array();
  }
  const dataOffset = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_DATA_OFFSET, true);
  const dataCap = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, true);
  if (dataCap === 0 || dataOffset + size > layout.dataCap) {
    return null;
  }
  const dataStart = layout.dataPtr + dataOffset;
  return new Uint8Array(targetExports.memory.buffer.slice(dataStart, dataStart + size));
}

export function normalizeMetadataOnlyMemfsState(
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  sourceLayout: MemfsLayout,
  targetLayout: MemfsLayout,
  dataUsed = 8,
): void {
  writeMemfsLayoutHeader(
    targetExports.memory,
    targetState.memfsPtr,
    targetState.memfsHeaderBytes || targetExports.kernel_memfs_header_bytes(),
    {
      ...targetLayout,
      inodeCount: sourceLayout.inodeCount,
      dirEntryCount: sourceLayout.dirEntryCount,
      dataUsed: Math.min(Math.max(dataUsed, 8), targetLayout.dataCap),
    },
  );

  const maxInodes = targetExports.kernel_memfs_first_child_bytes() / 4;
  const inodeBytes = targetExports.kernel_memfs_inodes_bytes() / maxInodes;
  const inodeView = new DataView(targetExports.memory.buffer);
  for (let ino = 1; ino < sourceLayout.inodeCount; ino++) {
    const inodeBase = targetLayout.inodesPtr + ino * inodeBytes;
    const mode = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_MODE, true);
    if ((mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
      continue;
    }
    inodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_OFFSET, 0, true);
    inodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, 0, true);
  }
}
