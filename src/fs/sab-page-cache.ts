/**
 * SAB Page Cache — SharedArrayBuffer-based file data and inode metadata.
 *
 * §6.3: kernel Worker publishes file data and inode metadata here.
 *        thread-workers read directly (kernel bypass for read/stat/openat O_RDONLY).
 *
 * Layout:
 *   [Header: 64 bytes]
 *     +0  AtomicU32  dataUsed       — next free offset in inline data/name region
 *     +4  AtomicU32  inodeCount     — number of published inodes
 *     +8  AtomicU32  dirEpoch       — directory structure epoch (for path resolution invalidation)
 *     +12 AtomicU32  residentClock  — resident page LRU clock
 *     +16 [padding 48 bytes]
 *
 *   [Inode table: MAX_INODES * 40 bytes, starting at offset 64]
 *     Per-inode entry (40 bytes):
 *       +0  AtomicU32  version    — seqlock (odd = writing, even = stable)
 *       +4  AtomicU32  data_off   — reserved legacy inline data offset; zero for Phase 7.5 files
 *       +8  AtomicU32  data_size  — file size in bytes
 *       +12 AtomicU32  mode       — inode mode (S_IFREG, S_IFDIR, etc.)
 *       +16 AtomicU32  parent_ino — parent directory inode
 *       +20 AtomicU32  name_off   — offset of name in data region
 *       +24 AtomicU32  name_len   — name length
 *       +28 AtomicU32  next_sibling — next sibling inode (0 = none)
 *       +32 AtomicU32  file_object_id — backing file object/page-map identity
 *       +36 AtomicU32  generation — backing/page-map generation
 *
 *   [Data region: up to resident frame region]
 *     Append-only name strings.
 *
 *   [Resident frame region: residentPageCount * 4096 bytes]
 *     Fixed reusable clean/dirty resident page frames, one per record slot.
 *
 *   [Resident record table: residentPageCount * 40 bytes]
 *     File-object/page-index/generation records for the fixed frames.
 */

export const MAX_INODES = 65536;
export const HEADER_SIZE = 64;
export const INODE_ENTRY_SIZE = 40;
export const INODE_TABLE_SIZE = MAX_INODES * INODE_ENTRY_SIZE;
export const INODE_TABLE_OFFSET = HEADER_SIZE;
export const DATA_REGION_OFFSET = HEADER_SIZE + INODE_TABLE_SIZE;

// Header field offsets (byte)
export const OFF_DATA_USED = 0;
export const OFF_INODE_COUNT = 4;
export const OFF_DIR_EPOCH = 8;
export const OFF_RESIDENT_CLOCK = 12;

// Per-inode field offsets (byte, relative to inode entry start)
export const INODE_OFF_VERSION = 0;
export const INODE_OFF_DATA_OFF = 4;
export const INODE_OFF_DATA_SIZE = 8;
export const INODE_OFF_MODE = 12;
export const INODE_OFF_PARENT = 16;
export const INODE_OFF_NAME_OFF = 20;
export const INODE_OFF_NAME_LEN = 24;
export const INODE_OFF_NEXT_SIBLING = 28;
export const INODE_OFF_FILE_OBJECT_ID = 32;
export const INODE_OFF_GENERATION = 36;

export const DEFAULT_RESIDENT_PAGE_SIZE = 4096;
const RESIDENT_PAGE_RECORD_BYTES = 40;
const DEFAULT_METADATA_DATA_REGION_BYTES = 1024 * 1024;
const RESIDENT_PAGE_ALLOCATION_QUANTUM = DEFAULT_RESIDENT_PAGE_SIZE + RESIDENT_PAGE_RECORD_BYTES;
const RESIDENT_PAGE_RECORD_VERSION = 0;
const RESIDENT_PAGE_RECORD_FILE_OBJECT_ID = 4;
const RESIDENT_PAGE_RECORD_PAGE_INDEX = 8;
const RESIDENT_PAGE_RECORD_DATA_OFF = 12;
const RESIDENT_PAGE_RECORD_VALID_BYTES = 16;
const RESIDENT_PAGE_RECORD_GENERATION = 20;
const RESIDENT_PAGE_RECORD_FLAGS = 24;
const RESIDENT_PAGE_RECORD_LAST_USED = 28;
const RESIDENT_PAGE_RECORD_DIRTY_START = 32;
const RESIDENT_PAGE_RECORD_DIRTY_END = 36;
const RESIDENT_PAGE_RECORD_FLAG_VALID = 1;
const RESIDENT_PAGE_RECORD_FLAG_DIRTY = 2;

export type FileObjectId = number;

export interface FileBackingStat {
  fileObjectId: FileObjectId;
  size: number;
  generation: number;
}

export interface FileBackingSource {
  readonly byteLength: number;
  readRange(offset: number, length: number): Uint8Array | null;
}

export interface ResidentPageCacheMiss {
  status: "miss";
  missingPageIndex: number;
}

export interface ResidentPageCacheHit {
  status: "hit";
  bytes: Uint8Array;
}

export type ResidentPageCacheReadResult =
  | ResidentPageCacheMiss
  | ResidentPageCacheHit;

export type ResidentBackingRangeReader = (ino: number, offset: number, length: number) => Uint8Array | null;

export interface ResidentPageFillResult {
  status: "filled" | "already-resident" | "dirty-pinned" | "not-found";
  fileObjectId: FileObjectId;
  pageIndex: number;
  frameIndex?: number;
}

interface MemoryFileObject {
  data: Uint8Array | null;
  source: FileBackingSource | null;
  patches: Array<{ offset: number; data: Uint8Array }>;
  sizeOverride: number | null;
  generation: number;
}

interface FileBackingObjectOptions {
  fileObjectId?: FileObjectId;
  generation?: number;
}

interface FileBackingMutationOptions {
  generation?: number;
}

export class MemoryFileBackingStore {
  private nextFileObjectId = 1;
  private readonly files = new Map<FileObjectId, MemoryFileObject>();

  createFile(data: Uint8Array = new Uint8Array(), options: FileBackingObjectOptions = {}): FileBackingStat {
    const fileObjectId = this.allocateFileObjectId(options.fileObjectId);
    this.files.set(fileObjectId, {
      data: new Uint8Array(data),
      source: null,
      patches: [],
      sizeOverride: null,
      generation: this.resolveGeneration(options.generation),
    });
    return this.stat(fileObjectId)!;
  }

  createSourceFile(source: FileBackingSource, options: FileBackingObjectOptions = {}): FileBackingStat {
    if (!Number.isInteger(source.byteLength) || source.byteLength < 0 || source.byteLength > 0xffffffff) {
      throw new RangeError(`invalid file source size: ${source.byteLength}`);
    }
    const fileObjectId = this.allocateFileObjectId(options.fileObjectId);
    this.files.set(fileObjectId, {
      data: null,
      source,
      patches: [],
      sizeOverride: null,
      generation: this.resolveGeneration(options.generation),
    });
    return this.stat(fileObjectId)!;
  }

  stat(fileObjectId: FileObjectId): FileBackingStat | null {
    const file = this.files.get(fileObjectId);
    if (!file) {
      return null;
    }
    return {
      fileObjectId,
      size: this.fileSize(file),
      generation: file.generation,
    };
  }

  read(fileObjectId: FileObjectId, offset: number, length: number): Uint8Array {
    const file = this.files.get(fileObjectId);
    const size = file ? this.fileSize(file) : 0;
    if (!file || length <= 0 || offset < 0 || offset >= size) {
      return new Uint8Array();
    }
    const readLen = Math.min(length, size - offset);
    const out = new Uint8Array(readLen);
    const covered = new Uint8Array(readLen);
    const baseSize = file.data?.byteLength ?? file.source?.byteLength ?? 0;
    if (file.data) {
      const copyLen = Math.max(0, Math.min(readLen, file.data.byteLength - offset));
      if (copyLen > 0) {
        out.set(file.data.subarray(offset, offset + copyLen));
        covered.fill(1, 0, copyLen);
      }
    } else {
      const bytes = file.source?.readRange(offset, readLen);
      if (bytes) {
        const copyLen = Math.min(bytes.byteLength, readLen);
        out.set(bytes.subarray(0, copyLen));
        covered.fill(1, 0, copyLen);
      }
    }
    if (offset + readLen > baseSize) {
      covered.fill(1, Math.max(0, baseSize - offset), readLen);
    }
    const readEnd = offset + readLen;
    for (const patch of file.patches) {
      const patchEnd = patch.offset + patch.data.length;
      const copyStart = Math.max(offset, patch.offset);
      const copyEnd = Math.min(readEnd, patchEnd);
      if (copyEnd <= copyStart) {
        continue;
      }
      out.set(
        patch.data.subarray(copyStart - patch.offset, copyEnd - patch.offset),
        copyStart - offset,
      );
      covered.fill(1, copyStart - offset, copyEnd - offset);
    }
    for (let index = 0; index < covered.length; index += 1) {
      if (covered[index] === 0) {
        return new Uint8Array();
      }
    }
    return out;
  }

  replace(fileObjectId: FileObjectId, data: Uint8Array, options: FileBackingMutationOptions = {}): FileBackingStat {
    const file = this.files.get(fileObjectId);
    if (!file) {
      throw new Error(`file object not found: ${fileObjectId}`);
    }
    file.data = new Uint8Array(data);
    file.source = null;
    file.patches = [];
    file.sizeOverride = null;
    this.applyMutationGeneration(file, options.generation);
    return this.stat(fileObjectId)!;
  }

  write(fileObjectId: FileObjectId, offset: number, data: Uint8Array, options: FileBackingMutationOptions = {}): FileBackingStat {
    const file = this.files.get(fileObjectId);
    if (!file) {
      throw new Error(`file object not found: ${fileObjectId}`);
    }
    if (!Number.isInteger(offset) || offset < 0 || offset > 0xffffffff) {
      throw new RangeError(`invalid file write offset: ${offset}`);
    }
    const end = offset + data.length;
    if (!Number.isInteger(end) || end > 0xffffffff) {
      throw new RangeError(`invalid file write range: ${offset}+${data.length}`);
    }
    if (data.length === 0) {
      return this.stat(fileObjectId)!;
    }
    file.patches.push({ offset, data: new Uint8Array(data) });
    if (file.sizeOverride !== null && end > file.sizeOverride) {
      file.sizeOverride = end;
    }
    this.applyMutationGeneration(file, options.generation);
    return this.stat(fileObjectId)!;
  }

  truncate(fileObjectId: FileObjectId, size: number, options: FileBackingMutationOptions = {}): FileBackingStat {
    const file = this.files.get(fileObjectId);
    if (!file) {
      throw new Error(`file object not found: ${fileObjectId}`);
    }
    if (!Number.isInteger(size) || size < 0 || size > 0xffffffff) {
      throw new RangeError(`invalid file size: ${size}`);
    }
    const previousSize = this.fileSize(file);
    let trimmed = false;
    if (file.data && file.data.byteLength > size) {
      file.data = file.data.slice(0, size);
      trimmed = true;
    }
    const nextPatches: MemoryFileObject["patches"] = [];
    for (const patch of file.patches) {
      if (patch.offset >= size) {
        trimmed = true;
        continue;
      }
      const patchEnd = patch.offset + patch.data.length;
      if (patchEnd > size) {
        nextPatches.push({ offset: patch.offset, data: patch.data.slice(0, size - patch.offset) });
        trimmed = true;
        continue;
      }
      nextPatches.push(patch);
    }
    file.patches = nextPatches;
    file.sizeOverride = size;
    if (previousSize !== size || trimmed || options.generation !== undefined) {
      this.applyMutationGeneration(file, options.generation);
    }
    return this.stat(fileObjectId)!;
  }

  private allocateFileObjectId(fileObjectId: FileObjectId | undefined): FileObjectId {
    if (fileObjectId === undefined) {
      return this.nextFileObjectId++;
    }
    if (!Number.isInteger(fileObjectId) || fileObjectId <= 0 || fileObjectId > 0xffffffff) {
      throw new RangeError(`invalid file object id: ${fileObjectId}`);
    }
    if (this.files.has(fileObjectId)) {
      throw new Error(`file object already exists: ${fileObjectId}`);
    }
    this.nextFileObjectId = Math.max(this.nextFileObjectId, fileObjectId + 1);
    return fileObjectId;
  }

  private resolveGeneration(generation: number | undefined): number {
    if (generation === undefined) {
      return 1;
    }
    if (!Number.isInteger(generation) || generation <= 0 || generation > 0xffffffff) {
      throw new RangeError(`invalid file object generation: ${generation}`);
    }
    return generation;
  }

  private applyMutationGeneration(file: MemoryFileObject, generation: number | undefined): void {
    file.generation = generation === undefined
      ? file.generation + 1
      : this.resolveGeneration(generation);
  }

  private fileSize(file: MemoryFileObject): number {
    if (file.sizeOverride !== null) {
      return file.sizeOverride;
    }
    let size = file.data?.byteLength ?? file.source?.byteLength ?? 0;
    for (const patch of file.patches) {
      size = Math.max(size, patch.offset + patch.data.length);
    }
    return size;
  }
}

/**
 * Get the byte offset for a given inode entry in the SAB.
 */
export function inodeOffset(ino: number): number {
  return INODE_TABLE_OFFSET + ino * INODE_ENTRY_SIZE;
}

/**
 * Publish (or update) an inode's metadata to the SAB page cache.
 * File bytes live in resident page records or backing objects.
 *
 * Uses seqlock protocol:
 *   1. Write the name to the data region (append-only)
 *   2. version = odd (writing)
 *   3. Update metadata atomics
 *   4. version = even (stable)
 */
export function publishInode(
  sab: SharedArrayBuffer,
  ino: number,
  meta: {
    mode: number;
    size: number;
    parentIno: number;
    name: Uint8Array;
    nextSibling: number;
    fileObjectId?: number;
    generation?: number;
  },
): void {
  const u32 = new Uint32Array(sab);
  const u8 = new Uint8Array(sab);
  const base = inodeOffset(ino);
  const base32 = base >>> 2;
  const dataRegionEnd = dataRegionLimit(sab);
  const fileObjectId = meta.fileObjectId ?? (meta.size > 0 ? ino : 0);
  const generation = meta.generation ?? 1;

  function allocateDataRegion(byteLength: number): number {
    if (byteLength <= 0) {
      return 0;
    }
    const currentUsed = Atomics.load(u32, OFF_DATA_USED >>> 2);
    const start = DATA_REGION_OFFSET + currentUsed;
    const end = start + byteLength;
    if (end > dataRegionEnd) {
      return 0;
    }
    Atomics.store(u32, OFF_DATA_USED >>> 2, currentUsed + byteLength);
    return start;
  }

  // Allocate space for name
  let nameOff = 0;
  if (meta.name.length > 0) {
    nameOff = allocateDataRegion(meta.name.length);
    if (nameOff !== 0) {
      u8.set(meta.name, nameOff);
    }
  }

  // Seqlock: version → odd (writing)
  const currentVersion = Atomics.load(u32, base32 + (INODE_OFF_VERSION >>> 2));
  const oddVersion = (currentVersion | 1) + (currentVersion & 1 ? 1 : 0);
  const currentFileObjectId = Atomics.load(u32, base32 + (INODE_OFF_FILE_OBJECT_ID >>> 2));
  const currentGeneration = Atomics.load(u32, base32 + (INODE_OFF_GENERATION >>> 2));
  if (currentVersion === 0 || currentFileObjectId !== fileObjectId || currentGeneration !== generation) {
    invalidateResidentPages(sab, ino);
  }
  Atomics.store(u32, base32 + (INODE_OFF_VERSION >>> 2), oddVersion);

  // Update metadata
  Atomics.store(u32, base32 + (INODE_OFF_DATA_OFF >>> 2), 0);
  Atomics.store(u32, base32 + (INODE_OFF_DATA_SIZE >>> 2), meta.size);
  Atomics.store(u32, base32 + (INODE_OFF_MODE >>> 2), meta.mode);
  Atomics.store(u32, base32 + (INODE_OFF_PARENT >>> 2), meta.parentIno);
  Atomics.store(u32, base32 + (INODE_OFF_NAME_OFF >>> 2), nameOff);
  Atomics.store(u32, base32 + (INODE_OFF_NAME_LEN >>> 2), meta.name.length);
  Atomics.store(u32, base32 + (INODE_OFF_NEXT_SIBLING >>> 2), meta.nextSibling);
  Atomics.store(u32, base32 + (INODE_OFF_FILE_OBJECT_ID >>> 2), fileObjectId);
  Atomics.store(u32, base32 + (INODE_OFF_GENERATION >>> 2), generation);

  // Seqlock: version → even (stable)
  Atomics.store(u32, base32 + (INODE_OFF_VERSION >>> 2), oddVersion + 1);

  // Update inode count if this is a new inode
  const inodeCount = Atomics.load(u32, OFF_INODE_COUNT >>> 2);
  if (ino >= inodeCount) {
    Atomics.store(u32, OFF_INODE_COUNT >>> 2, ino + 1);
  }
}

function residentPageRecordCount(sab: SharedArrayBuffer): number {
  const available = Math.max(0, sab.byteLength - DATA_REGION_OFFSET);
  const residentAvailable = Math.max(0, available - metadataDataRegionCapacity(sab));
  return Math.floor(residentAvailable / RESIDENT_PAGE_ALLOCATION_QUANTUM);
}

export function hasResidentPageRecordCapacity(sab: SharedArrayBuffer): boolean {
  return residentPageRecordCount(sab) > 0;
}

function residentPageRecordTableOffset(sab: SharedArrayBuffer): number {
  return sab.byteLength - residentPageRecordCount(sab) * RESIDENT_PAGE_RECORD_BYTES;
}

function residentPageFrameRegionOffset(sab: SharedArrayBuffer): number {
  return residentPageRecordTableOffset(sab) - residentPageRecordCount(sab) * DEFAULT_RESIDENT_PAGE_SIZE;
}

function dataRegionLimit(sab: SharedArrayBuffer): number {
  return DATA_REGION_OFFSET + metadataDataRegionCapacity(sab);
}

function metadataDataRegionCapacity(sab: SharedArrayBuffer): number {
  const available = Math.max(0, sab.byteLength - DATA_REGION_OFFSET);
  if (available >= DEFAULT_METADATA_DATA_REGION_BYTES + RESIDENT_PAGE_ALLOCATION_QUANTUM) {
    return DEFAULT_METADATA_DATA_REGION_BYTES;
  }
  return available % RESIDENT_PAGE_ALLOCATION_QUANTUM;
}

function residentPageRecordOffset(sab: SharedArrayBuffer, index: number): number {
  return residentPageRecordTableOffset(sab) + index * RESIDENT_PAGE_RECORD_BYTES;
}

function residentPageFrameOffset(sab: SharedArrayBuffer, index: number): number {
  return residentPageFrameRegionOffset(sab) + index * DEFAULT_RESIDENT_PAGE_SIZE;
}

interface ResidentPageRecordState {
  fileObjectId: number;
  pageIndex: number;
  dataOff: number;
  validBytes: number;
  generation: number;
  flags: number;
  dirtyStart: number;
  dirtyEnd: number;
}

function readStableResidentPageRecord(
  sab: SharedArrayBuffer,
  index: number,
): ResidentPageRecordState | null {
  const u32 = new Uint32Array(sab);
  const base32 = residentPageRecordOffset(sab, index) >>> 2;
  const v1 = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2));
  if (v1 === 0 || (v1 & 1) !== 0) {
    return null;
  }
  const fileObjectId = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_FILE_OBJECT_ID >>> 2));
  const pageIndex = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_PAGE_INDEX >>> 2));
  const dataOff = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_DATA_OFF >>> 2));
  const validBytes = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_VALID_BYTES >>> 2));
  const generation = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_GENERATION >>> 2));
  const flags = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2));
  const dirtyStart = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_DIRTY_START >>> 2));
  const dirtyEnd = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_DIRTY_END >>> 2));
  const v2 = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2));
  if (v1 !== v2) {
    return null;
  }
  return {
    fileObjectId,
    pageIndex,
    dataOff,
    validBytes,
    generation,
    flags,
    dirtyStart,
    dirtyEnd,
  };
}

function nextResidentClock(sab: SharedArrayBuffer): number {
  const u32 = new Uint32Array(sab);
  return Atomics.add(u32, OFF_RESIDENT_CLOCK >>> 2, 1) + 1;
}

function findResidentPageRecordIndex(
  sab: SharedArrayBuffer,
  fileObjectId: number,
  generation: number,
  pageIndex: number,
): number {
  const count = residentPageRecordCount(sab);
  for (let index = 0; index < count; index += 1) {
    const record = readStableResidentPageRecord(sab, index);
    if (
      record
      && (record.flags & RESIDENT_PAGE_RECORD_FLAG_VALID) !== 0
      && record.fileObjectId === fileObjectId
      && record.generation === generation
      && record.pageIndex === pageIndex
    ) {
      return index;
    }
  }
  return -1;
}

export interface ResidentPagePublishSlotDecisionInput {
  existingSlot: number;
  emptySlot: number;
  cleanEvictSlot: number;
}

export type ResidentPageEvictStatus = "evicted" | "dirty-pinned" | "not-found";

export interface ResidentPageEvictionDecisionInput {
  recordFound: boolean;
  dirty: boolean;
}

export interface ResidentPageTableAuthority {
  selectPublishSlot(input: ResidentPagePublishSlotDecisionInput): number;
  decideEviction(input: ResidentPageEvictionDecisionInput): ResidentPageEvictStatus;
}

function allocateResidentPageRecordIndex(
  sab: SharedArrayBuffer,
  fileObjectId: number,
  generation: number,
  pageIndex: number,
  authority: ResidentPageTableAuthority,
): number {
  const existing = findResidentPageRecordIndex(sab, fileObjectId, generation, pageIndex);
  const u32 = new Uint32Array(sab);
  const count = residentPageRecordCount(sab);
  let emptyIndex = -1;
  let oldestIndex = -1;
  let oldestLastUsed = 0xffffffff;
  if (existing < 0) {
    for (let index = 0; index < count; index += 1) {
      const base32 = residentPageRecordOffset(sab, index) >>> 2;
      const flags = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2));
      if ((flags & RESIDENT_PAGE_RECORD_FLAG_VALID) === 0) {
        emptyIndex = index;
        break;
      }
      if ((flags & RESIDENT_PAGE_RECORD_FLAG_DIRTY) !== 0) {
        continue;
      }
      const lastUsed = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_LAST_USED >>> 2));
      if (lastUsed < oldestLastUsed) {
        oldestIndex = index;
        oldestLastUsed = lastUsed;
      }
    }
  }
  const selected = authority.selectPublishSlot({
    existingSlot: existing,
    emptySlot: emptyIndex,
    cleanEvictSlot: oldestIndex,
  });
  if (selected < 0 || selected >= count) {
    return -1;
  }
  if (selected === oldestIndex && existing < 0 && emptyIndex < 0) {
    clearResidentPageRecord(sab, selected);
  }
  return selected;
}

function clearResidentPageRecord(sab: SharedArrayBuffer, recordIndex: number): void {
  const u32 = new Uint32Array(sab);
  const base32 = residentPageRecordOffset(sab, recordIndex) >>> 2;
  const currentVersion = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2));
  const oddVersion = (currentVersion | 1) + (currentVersion & 1 ? 1 : 0);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2), oddVersion);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2), 0);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_DIRTY_START >>> 2), 0);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_DIRTY_END >>> 2), 0);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2), oddVersion + 1);
}

function invalidateResidentPages(sab: SharedArrayBuffer, ino: number): void {
  const meta = readInodeMeta(sab, ino);
  if (!meta || meta.fileObjectId === 0) {
    return;
  }
  const u32 = new Uint32Array(sab);
  const count = residentPageRecordCount(sab);
  for (let index = 0; index < count; index += 1) {
    const base32 = residentPageRecordOffset(sab, index) >>> 2;
    const flags = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2));
    if (
      (flags & RESIDENT_PAGE_RECORD_FLAG_VALID) !== 0
      && Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_FILE_OBJECT_ID >>> 2)) === meta.fileObjectId
    ) {
      clearResidentPageRecord(sab, index);
    }
  }
}

export function evictResidentPage(
  sab: SharedArrayBuffer,
  ino: number,
  pageIndex: number,
  authority: ResidentPageTableAuthority,
): "evicted" | "dirty-pinned" | "not-found" {
  if (
    !Number.isInteger(ino)
    || ino <= 0
    || ino >= MAX_INODES
    || !Number.isInteger(pageIndex)
    || pageIndex < 0
  ) {
    return "not-found";
  }
  const meta = readInodeMeta(sab, ino);
  if (!meta || meta.fileObjectId === 0) {
    return authority.decideEviction({ recordFound: false, dirty: false });
  }
  const recordIndex = findResidentPageRecordIndex(sab, meta.fileObjectId, meta.generation, pageIndex);
  if (recordIndex < 0) {
    return authority.decideEviction({ recordFound: false, dirty: false });
  }
  const u32 = new Uint32Array(sab);
  const base32 = residentPageRecordOffset(sab, recordIndex) >>> 2;
  const flags = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2));
  const status = authority.decideEviction({
    recordFound: true,
    dirty: (flags & RESIDENT_PAGE_RECORD_FLAG_DIRTY) !== 0,
  });
  if (status !== "evicted") {
    return status;
  }
  clearResidentPageRecord(sab, recordIndex);
  return "evicted";
}

export function publishResidentPage(
  sab: SharedArrayBuffer,
  ino: number,
  pageIndex: number,
  data: Uint8Array,
  options: {
    dirty?: boolean;
    dirtyStart?: number;
    dirtyEnd?: number;
    authority: ResidentPageTableAuthority;
  },
): boolean {
  if (
    !Number.isInteger(ino)
    || ino <= 0
    || ino >= MAX_INODES
    || !Number.isInteger(pageIndex)
    || pageIndex < 0
    || data.length <= 0
    || data.length > DEFAULT_RESIDENT_PAGE_SIZE
  ) {
    return false;
  }
  const meta = readInodeMeta(sab, ino);
  if (!meta || meta.fileObjectId === 0) {
    return false;
  }
  const recordIndex = allocateResidentPageRecordIndex(
    sab,
    meta.fileObjectId,
    meta.generation,
    pageIndex,
    options.authority,
  );
  if (recordIndex < 0) {
    return false;
  }
  const dataOff = residentPageFrameOffset(sab, recordIndex);
  const dirty = options.dirty === true;
  const dirtyStart = dirty ? Math.max(0, Math.min(data.length, options.dirtyStart ?? 0)) : 0;
  const dirtyEnd = dirty ? Math.max(dirtyStart, Math.min(data.length, options.dirtyEnd ?? data.length)) : 0;

  const u32 = new Uint32Array(sab);
  const base32 = residentPageRecordOffset(sab, recordIndex) >>> 2;
  const currentVersion = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2));
  const oddVersion = (currentVersion | 1) + (currentVersion & 1 ? 1 : 0);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2), oddVersion);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2), 0);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_DIRTY_START >>> 2), 0);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_DIRTY_END >>> 2), 0);

  const u8 = new Uint8Array(sab);
  u8.fill(0, dataOff, dataOff + DEFAULT_RESIDENT_PAGE_SIZE);
  u8.set(data, dataOff);

  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_FILE_OBJECT_ID >>> 2), meta.fileObjectId);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_PAGE_INDEX >>> 2), pageIndex);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_DATA_OFF >>> 2), dataOff);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_VALID_BYTES >>> 2), data.length);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_GENERATION >>> 2), meta.generation);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_LAST_USED >>> 2), nextResidentClock(sab));
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_DIRTY_START >>> 2), dirtyStart);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_DIRTY_END >>> 2), dirtyEnd);
  Atomics.store(
    u32,
    base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2),
    RESIDENT_PAGE_RECORD_FLAG_VALID | (dirty ? RESIDENT_PAGE_RECORD_FLAG_DIRTY : 0),
  );
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2), oddVersion + 1);
  return true;
}

function readResidentFileDataWithDirtyPolicy(
  sab: SharedArrayBuffer,
  ino: number,
  offset: number,
  length: number,
  includeDirty: boolean,
): Uint8Array | null {
  if (length <= 0) {
    return new Uint8Array();
  }
  const out = new Uint8Array(length);
  const u32 = new Uint32Array(sab);
  const u8 = new Uint8Array(sab);
  const meta = readInodeMeta(sab, ino);
  if (!meta || meta.fileObjectId === 0) {
    return null;
  }
  let copied = 0;
  while (copied < length) {
    const absolute = offset + copied;
    const pageIndex = Math.floor(absolute / DEFAULT_RESIDENT_PAGE_SIZE);
    const pageOffset = absolute % DEFAULT_RESIDENT_PAGE_SIZE;
    const copyLen = Math.min(length - copied, DEFAULT_RESIDENT_PAGE_SIZE - pageOffset);
    const recordIndex = findResidentPageRecordIndex(sab, meta.fileObjectId, meta.generation, pageIndex);
    if (recordIndex < 0) {
      return null;
    }
    const base32 = residentPageRecordOffset(sab, recordIndex) >>> 2;
    const record = readStableResidentPageRecord(sab, recordIndex);
    if (
      !record
      || record.fileObjectId !== meta.fileObjectId
      || record.generation !== meta.generation
      || record.pageIndex !== pageIndex
      || (record.flags & RESIDENT_PAGE_RECORD_FLAG_VALID) === 0
      || (!includeDirty && (record.flags & RESIDENT_PAGE_RECORD_FLAG_DIRTY) !== 0)
      || record.dataOff < residentPageFrameRegionOffset(sab)
      || record.dataOff + record.validBytes > residentPageRecordTableOffset(sab)
      || pageOffset + copyLen > record.validBytes
    ) {
      return null;
    }
    out.set(u8.subarray(record.dataOff + pageOffset, record.dataOff + pageOffset + copyLen), copied);
    Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_LAST_USED >>> 2), nextResidentClock(sab));
    copied += copyLen;
  }
  return out;
}

export function readResidentFileData(
  sab: SharedArrayBuffer,
  ino: number,
  offset: number,
  length: number,
): Uint8Array | null {
  return readResidentFileDataWithDirtyPolicy(sab, ino, offset, length, true);
}

export function readCleanResidentFileData(
  sab: SharedArrayBuffer,
  ino: number,
  offset: number,
  length: number,
): Uint8Array | null {
  return readResidentFileDataWithDirtyPolicy(sab, ino, offset, length, false);
}

export function debugResidentPageCountForInode(sab: SharedArrayBuffer, ino: number): number {
  if (!Number.isInteger(ino) || ino <= 0 || ino >= MAX_INODES) {
    return 0;
  }
  const meta = readInodeMeta(sab, ino);
  if (!meta || meta.fileObjectId === 0) {
    return 0;
  }
  const u32 = new Uint32Array(sab);
  const count = residentPageRecordCount(sab);
  let residentPages = 0;
  for (let index = 0; index < count; index += 1) {
    const base32 = residentPageRecordOffset(sab, index) >>> 2;
    const flags = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2));
    if (
      (flags & RESIDENT_PAGE_RECORD_FLAG_VALID) !== 0
      && Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_FILE_OBJECT_ID >>> 2)) === meta.fileObjectId
      && Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_GENERATION >>> 2)) === meta.generation
    ) {
      residentPages += 1;
    }
  }
  return residentPages;
}

interface ResidentPageSnapshot {
  pageIndex: number;
  bytes: Uint8Array;
  dirty: boolean;
  dirtyStart: number;
  dirtyEnd: number;
}

export interface DirtyResidentWritebackRange {
  offset: number;
  bytes: Uint8Array;
}

export interface DirtyResidentWriteDecisionInput {
  ino: number;
  offset: number;
  length: number;
  currentSize: number;
  currentFileObjectId: number;
  currentGeneration: number;
  existingDirtyPages: number;
  newDirtyPages: number;
  residentPageCapacity: number;
}

export interface DirtyResidentTruncateDecisionInput {
  ino: number;
  size: number;
  currentSize: number;
  currentFileObjectId: number;
  currentGeneration: number;
  retainedDirtyPages: number;
  residentPageCapacity: number;
}

export interface DirtyResidentPageDecision {
  result: number;
  nextSize: number;
  fileObjectId: number;
  generation: number;
}

export interface ResidentDirtyPageAuthority extends ResidentPageTableAuthority {
  decideWrite(input: DirtyResidentWriteDecisionInput): DirtyResidentPageDecision;
  decideTruncate(input: DirtyResidentTruncateDecisionInput): DirtyResidentPageDecision;
}

function collectResidentPageSnapshots(
  sab: SharedArrayBuffer,
  meta: { fileObjectId: number; generation: number },
  dirtyOnly: boolean,
): ResidentPageSnapshot[] {
  if (meta.fileObjectId === 0) {
    return [];
  }
  const u8 = new Uint8Array(sab);
  const count = residentPageRecordCount(sab);
  const snapshots: ResidentPageSnapshot[] = [];
  for (let index = 0; index < count; index += 1) {
    const record = readStableResidentPageRecord(sab, index);
    const dirty = (record?.flags ?? 0) & RESIDENT_PAGE_RECORD_FLAG_DIRTY;
    if (
      !record
      || (record.flags & RESIDENT_PAGE_RECORD_FLAG_VALID) === 0
      || (dirtyOnly && dirty === 0)
      || record.fileObjectId !== meta.fileObjectId
      || record.generation !== meta.generation
    ) {
      continue;
    }
    if (
      record.dataOff < residentPageFrameRegionOffset(sab)
      || record.validBytes > DEFAULT_RESIDENT_PAGE_SIZE
      || record.dataOff + record.validBytes > residentPageRecordTableOffset(sab)
    ) {
      continue;
    }
    snapshots.push({
      pageIndex: record.pageIndex,
      bytes: new Uint8Array(u8.slice(record.dataOff, record.dataOff + record.validBytes)),
      dirty: dirty !== 0,
      dirtyStart: record.dirtyStart,
      dirtyEnd: record.dirtyEnd,
    });
  }
  return snapshots;
}

function readResidentPageSnapshot(
  sab: SharedArrayBuffer,
  meta: { fileObjectId: number; generation: number },
  pageIndex: number,
): ResidentPageSnapshot | null {
  if (meta.fileObjectId === 0) {
    return null;
  }
  const recordIndex = findResidentPageRecordIndex(sab, meta.fileObjectId, meta.generation, pageIndex);
  if (recordIndex < 0) {
    return null;
  }
  const record = readStableResidentPageRecord(sab, recordIndex);
  if (
    !record
    || (record.flags & RESIDENT_PAGE_RECORD_FLAG_VALID) === 0
    || record.fileObjectId !== meta.fileObjectId
    || record.generation !== meta.generation
    || record.pageIndex !== pageIndex
    || record.dataOff < residentPageFrameRegionOffset(sab)
    || record.validBytes > DEFAULT_RESIDENT_PAGE_SIZE
    || record.dataOff + record.validBytes > residentPageRecordTableOffset(sab)
  ) {
    return null;
  }
  const u8 = new Uint8Array(sab);
  const dirty = (record.flags & RESIDENT_PAGE_RECORD_FLAG_DIRTY) !== 0;
  return {
    pageIndex: record.pageIndex,
    bytes: new Uint8Array(u8.slice(record.dataOff, record.dataOff + record.validBytes)),
    dirty,
    dirtyStart: record.dirtyStart,
    dirtyEnd: record.dirtyEnd,
  };
}

function updateInodeForResidentPages(
  sab: SharedArrayBuffer,
  ino: number,
  record: NonNullable<ReturnType<typeof readInodeRecord>>,
  size: number,
  fileObjectId: number,
  generation: number,
): boolean {
  const u32 = new Uint32Array(sab);
  const base = inodeOffset(ino);
  if (base < INODE_TABLE_OFFSET || base + INODE_ENTRY_SIZE > sab.byteLength) {
    return false;
  }
  const base32 = base >>> 2;
  const oldNameOff = Atomics.load(u32, base32 + (INODE_OFF_NAME_OFF >>> 2));
  const oldNameLen = Atomics.load(u32, base32 + (INODE_OFF_NAME_LEN >>> 2));
  const currentVersion = Atomics.load(u32, base32 + (INODE_OFF_VERSION >>> 2));
  const oddVersion = (currentVersion | 1) + (currentVersion & 1 ? 1 : 0);
  if (record.fileObjectId !== fileObjectId || record.generation !== generation) {
    invalidateResidentPages(sab, ino);
  }
  Atomics.store(u32, base32 + (INODE_OFF_VERSION >>> 2), oddVersion);
  Atomics.store(u32, base32 + (INODE_OFF_DATA_OFF >>> 2), 0);
  Atomics.store(u32, base32 + (INODE_OFF_DATA_SIZE >>> 2), size);
  Atomics.store(u32, base32 + (INODE_OFF_MODE >>> 2), record.mode);
  Atomics.store(u32, base32 + (INODE_OFF_PARENT >>> 2), record.parentIno);
  Atomics.store(u32, base32 + (INODE_OFF_NAME_OFF >>> 2), oldNameLen > 0 ? oldNameOff : 0);
  Atomics.store(u32, base32 + (INODE_OFF_NAME_LEN >>> 2), oldNameLen);
  Atomics.store(u32, base32 + (INODE_OFF_NEXT_SIBLING >>> 2), record.nextSibling);
  Atomics.store(u32, base32 + (INODE_OFF_FILE_OBJECT_ID >>> 2), fileObjectId);
  Atomics.store(u32, base32 + (INODE_OFF_GENERATION >>> 2), generation);
  Atomics.store(u32, base32 + (INODE_OFF_VERSION >>> 2), oddVersion + 1);
  return true;
}

function dirtyResidentPageCount(sab: SharedArrayBuffer, meta: { fileObjectId: number; generation: number }): number {
  if (meta.fileObjectId === 0) {
    return 0;
  }
  let dirtyPages = 0;
  const count = residentPageRecordCount(sab);
  for (let index = 0; index < count; index += 1) {
    const record = readStableResidentPageRecord(sab, index);
    if (
      record
      && (record.flags & RESIDENT_PAGE_RECORD_FLAG_VALID) !== 0
      && (record.flags & RESIDENT_PAGE_RECORD_FLAG_DIRTY) !== 0
      && record.fileObjectId === meta.fileObjectId
      && record.generation === meta.generation
    ) {
      dirtyPages += 1;
    }
  }
  return dirtyPages;
}

function readExistingPageBytes(
  sab: SharedArrayBuffer,
  ino: number,
  record: NonNullable<ReturnType<typeof readInodeRecord>>,
  pageIndex: number,
  nextPageLen: number,
  readBackingRange: ResidentBackingRangeReader | undefined,
): Uint8Array | null {
  const pageStart = pageIndex * DEFAULT_RESIDENT_PAGE_SIZE;
  const bytes = new Uint8Array(nextPageLen);
  const existingLen = Math.max(0, Math.min(nextPageLen, record.size - pageStart));
  if (existingLen === 0) {
    return bytes;
  }
  const resident = readResidentFileData(sab, ino, pageStart, existingLen);
  if (resident) {
    bytes.set(resident);
    return bytes;
  }
  const backing = readBackingRange?.(ino, pageStart, existingLen);
  if (!backing || backing.length !== existingLen) {
    return null;
  }
  bytes.set(backing);
  return bytes;
}

export function hasDirtyResidentPagesForInode(sab: SharedArrayBuffer, ino: number): boolean {
  const meta = readInodeMeta(sab, ino);
  return !!(meta && collectResidentPageSnapshots(sab, meta, true).length > 0);
}

export function markDirtyResidentFileDataClean(
  sab: SharedArrayBuffer,
  ino: number,
  fileObjectId: number,
  generation: number,
  authority: ResidentPageTableAuthority,
): boolean {
  if (
    !Number.isInteger(ino)
    || ino <= 0
    || ino >= MAX_INODES
    || !Number.isInteger(fileObjectId)
    || fileObjectId <= 0
    || !Number.isInteger(generation)
    || generation <= 0
  ) {
    return false;
  }
  const record = readInodeRecord(sab, ino);
  const meta = readInodeMeta(sab, ino);
  if (!record || !meta || meta.fileObjectId === 0) {
    return false;
  }
  const residentPages = collectResidentPageSnapshots(sab, meta, false)
    .filter((page) => page.pageIndex * DEFAULT_RESIDENT_PAGE_SIZE < record.size);
  if (residentPages.length > residentPageRecordCount(sab)) {
    return false;
  }
  if (!updateInodeForResidentPages(sab, ino, record, record.size, fileObjectId, generation)) {
    return false;
  }
  for (const page of residentPages) {
    const pageStart = page.pageIndex * DEFAULT_RESIDENT_PAGE_SIZE;
    const validBytes = Math.min(page.bytes.length, Math.max(0, record.size - pageStart));
    if (validBytes <= 0) {
      continue;
    }
    if (!publishResidentPage(sab, ino, page.pageIndex, page.bytes.subarray(0, validBytes), { authority })) {
      return false;
    }
  }
  return true;
}

export function writeDirtyResidentFileData(
  sab: SharedArrayBuffer,
  ino: number,
  offset: number,
  data: Uint8Array,
  readBackingRange?: ResidentBackingRangeReader,
  authority?: ResidentDirtyPageAuthority,
): number {
  if (!Number.isInteger(ino) || ino <= 0 || ino >= MAX_INODES || offset < 0) {
    return -22;
  }
  if (data.length === 0) {
    return 0;
  }
  const record = readInodeRecord(sab, ino);
  if (!record) {
    return -2;
  }
  const end = offset + data.length;
  const nextSize = Math.max(record.size, end);
  if (!Number.isFinite(nextSize) || nextSize > 0xffffffff) {
    return -28;
  }
  const meta = readInodeMeta(sab, ino);
  const residentPages = new Map<number, ResidentPageSnapshot>();
  const touchedPages = new Set<number>();
  const fillStart = offset > record.size ? record.size : offset;
  const firstPage = Math.floor(fillStart / DEFAULT_RESIDENT_PAGE_SIZE);
  const lastPage = Math.floor((end - 1) / DEFAULT_RESIDENT_PAGE_SIZE);
  let newDirtyPageCount = 0;
  for (let pageIndex = firstPage; pageIndex <= lastPage; pageIndex += 1) {
    const pageStart = pageIndex * DEFAULT_RESIDENT_PAGE_SIZE;
    const pageLen = Math.min(DEFAULT_RESIDENT_PAGE_SIZE, nextSize - pageStart);
    const writeStart = Math.max(offset, pageStart);
    const writeEnd = Math.min(end, pageStart + pageLen);
    const writesPage = writeEnd > writeStart;
    const pageWriteStart = writesPage ? writeStart - pageStart : 0;
    const pageWriteEnd = writesPage ? writeEnd - pageStart : 0;
    const existing = meta ? readResidentPageSnapshot(sab, meta, pageIndex) : null;
    let pageBytes: Uint8Array;
    if (writesPage && pageWriteStart === 0 && pageWriteEnd === pageLen) {
      pageBytes = new Uint8Array(pageLen);
    } else if (existing) {
      pageBytes = existing.bytes.length === pageLen ? new Uint8Array(existing.bytes) : new Uint8Array(pageLen);
      pageBytes.set(existing.bytes.subarray(0, Math.min(existing.bytes.length, pageLen)));
    } else {
      const read = readExistingPageBytes(sab, ino, record, pageIndex, pageLen, readBackingRange);
      if (!read) {
        return -5;
      }
      pageBytes = read;
    }
    if (!existing?.dirty) {
      newDirtyPageCount += 1;
    }
    let dirtyStart = existing?.dirty ? existing.dirtyStart : pageLen;
    let dirtyEnd = existing?.dirty ? existing.dirtyEnd : 0;
    if (offset > record.size) {
      const holeStart = Math.max(record.size, pageStart);
      const holeEnd = Math.min(offset, pageStart + pageLen);
      if (holeEnd > holeStart) {
        dirtyStart = Math.min(dirtyStart, holeStart - pageStart);
        dirtyEnd = Math.max(dirtyEnd, holeEnd - pageStart);
      }
    }
    if (writesPage) {
      pageBytes.set(data.subarray(writeStart - offset, writeEnd - offset), pageWriteStart);
      dirtyStart = Math.min(dirtyStart, pageWriteStart);
      dirtyEnd = Math.max(dirtyEnd, pageWriteEnd);
    }
    if (dirtyEnd <= dirtyStart) {
      continue;
    }
    residentPages.set(pageIndex, {
      pageIndex,
      bytes: pageBytes,
      dirty: true,
      dirtyStart,
      dirtyEnd,
    });
    touchedPages.add(pageIndex);
  }

  const existingDirtyPageCount = meta ? dirtyResidentPageCount(sab, meta) : 0;
  if (!authority) {
    return -95;
  }
  const decision = authority.decideWrite({
    ino,
    offset,
    length: data.length,
    currentSize: record.size,
    currentFileObjectId: record.fileObjectId,
    currentGeneration: record.generation,
    existingDirtyPages: existingDirtyPageCount,
    newDirtyPages: newDirtyPageCount,
    residentPageCapacity: residentPageRecordCount(sab),
  });
  if (decision.result < 0) {
    return decision.result;
  }
  if (
    !updateInodeForResidentPages(
      sab,
      ino,
      record,
      decision.nextSize,
      decision.fileObjectId,
      decision.generation,
    )
  ) {
    return -28;
  }
  for (const page of residentPages.values()) {
    if (!page.dirty || page.bytes.length === 0 || !touchedPages.has(page.pageIndex)) {
      continue;
    }
    const published = publishResidentPage(sab, ino, page.pageIndex, page.bytes, {
      dirty: true,
      dirtyStart: page.dirtyStart,
      dirtyEnd: page.dirtyEnd,
      authority,
    });
    if (!published) {
      return -28;
    }
  }
  return decision.result;
}

export function truncateDirtyResidentFileData(
  sab: SharedArrayBuffer,
  ino: number,
  size: number,
  readBackingRange?: ResidentBackingRangeReader,
  authority?: ResidentDirtyPageAuthority,
): number {
  if (!Number.isInteger(ino) || ino <= 0 || ino >= MAX_INODES || size < 0 || size > 0xffffffff) {
    return -22;
  }
  const record = readInodeRecord(sab, ino);
  if (!record) {
    return -2;
  }
  const oldMeta = readInodeMeta(sab, ino);
  const residentPages = new Map<number, ResidentPageSnapshot>();
  for (const page of oldMeta ? collectResidentPageSnapshots(sab, oldMeta, true) : []) {
    if (page.pageIndex * DEFAULT_RESIDENT_PAGE_SIZE < size) {
      residentPages.set(page.pageIndex, page);
    }
  }
  const affectedPages = new Set<number>();
  if (size > record.size && size > 0) {
    const firstPage = Math.floor(record.size / DEFAULT_RESIDENT_PAGE_SIZE);
    const lastPage = Math.floor((size - 1) / DEFAULT_RESIDENT_PAGE_SIZE);
    for (let pageIndex = firstPage; pageIndex <= lastPage; pageIndex += 1) {
      affectedPages.add(pageIndex);
    }
  } else if (size > 0 && size < record.size && size % DEFAULT_RESIDENT_PAGE_SIZE !== 0) {
    affectedPages.add(Math.floor(size / DEFAULT_RESIDENT_PAGE_SIZE));
  }
  for (const pageIndex of affectedPages) {
    const pageStart = pageIndex * DEFAULT_RESIDENT_PAGE_SIZE;
    const validBytes = Math.min(DEFAULT_RESIDENT_PAGE_SIZE, size - pageStart);
    if (validBytes <= 0) {
      continue;
    }
    const existing = residentPages.get(pageIndex);
    let pageBytes: Uint8Array;
    if (existing) {
      pageBytes = existing.bytes.length === validBytes ? new Uint8Array(existing.bytes) : new Uint8Array(validBytes);
      pageBytes.set(existing.bytes.subarray(0, Math.min(existing.bytes.length, validBytes)));
    } else {
      const read = readExistingPageBytes(sab, ino, record, pageIndex, validBytes, readBackingRange);
      if (!read) {
        return -5;
      }
      pageBytes = read;
    }
    const dirtyStart = size < record.size
      ? 0
      : Math.max(0, Math.min(validBytes, Math.max(record.size, pageStart) - pageStart));
    residentPages.set(pageIndex, {
      pageIndex,
      bytes: pageBytes,
      dirty: true,
      dirtyStart: existing?.dirty ? Math.min(existing.dirtyStart, dirtyStart) : dirtyStart,
      dirtyEnd: validBytes,
    });
  }
  const retainedDirtyPages = [...residentPages.values()].filter((page) => page.pageIndex * DEFAULT_RESIDENT_PAGE_SIZE < size);
  if (!authority) {
    return -95;
  }
  const decision = authority.decideTruncate({
    ino,
    size,
    currentSize: record.size,
    currentFileObjectId: record.fileObjectId,
    currentGeneration: record.generation,
    retainedDirtyPages: retainedDirtyPages.length,
    residentPageCapacity: residentPageRecordCount(sab),
  });
  if (decision.result < 0) {
    return decision.result;
  }
  if (
    !updateInodeForResidentPages(
      sab,
      ino,
      record,
      decision.nextSize,
      decision.fileObjectId,
      decision.generation,
    )
  ) {
    return -28;
  }
  for (const page of retainedDirtyPages) {
    const pageStart = page.pageIndex * DEFAULT_RESIDENT_PAGE_SIZE;
    if (pageStart >= size) {
      continue;
    }
    const validBytes = Math.min(page.bytes.length, size - pageStart);
    if (validBytes <= 0) {
      continue;
    }
    const published = publishResidentPage(sab, ino, page.pageIndex, page.bytes.subarray(0, validBytes), {
      dirty: true,
      dirtyStart: Math.min(page.dirtyStart, validBytes),
      dirtyEnd: Math.min(page.dirtyEnd, validBytes),
      authority,
    });
    if (!published) {
      return -28;
    }
  }
  return decision.result;
}

export function readDirtyResidentFileData(
  sab: SharedArrayBuffer,
  ino: number,
  readBackingRange?: ResidentBackingRangeReader,
): Uint8Array | null {
  const record = readInodeRecord(sab, ino);
  if (!record) {
    return null;
  }
  if (record.size === 0) {
    return new Uint8Array();
  }
  return readDirtyResidentFileRangeData(sab, ino, 0, record.size, readBackingRange);
}

export function collectDirtyResidentFileWritebackRanges(
  sab: SharedArrayBuffer,
  ino: number,
): DirtyResidentWritebackRange[] | null {
  const record = readInodeRecord(sab, ino);
  const meta = readInodeMeta(sab, ino);
  if (!record || !meta || meta.fileObjectId === 0) {
    return null;
  }
  const dirtyPages = collectResidentPageSnapshots(sab, meta, true);
  if (dirtyPages.length > residentPageRecordCount(sab)) {
    return null;
  }
  const ranges: DirtyResidentWritebackRange[] = [];
  for (const page of dirtyPages) {
    const pageStart = page.pageIndex * DEFAULT_RESIDENT_PAGE_SIZE;
    if (pageStart >= record.size) {
      continue;
    }
    const validBytes = Math.min(page.bytes.length, Math.max(0, record.size - pageStart));
    const dirtyStart = Math.min(Math.max(0, page.dirtyStart), validBytes);
    const dirtyEnd = Math.min(Math.max(dirtyStart, page.dirtyEnd), validBytes);
    if (dirtyEnd <= dirtyStart) {
      continue;
    }
    ranges.push({
      offset: pageStart + dirtyStart,
      bytes: page.bytes.slice(dirtyStart, dirtyEnd),
    });
  }
  return ranges;
}

export function readDirtyResidentFileRangeData(
  sab: SharedArrayBuffer,
  ino: number,
  offset: number,
  length: number,
  readBackingRange?: ResidentBackingRangeReader,
): Uint8Array | null {
  const record = readInodeRecord(sab, ino);
  if (
    !record
    || !Number.isInteger(offset)
    || !Number.isInteger(length)
    || offset < 0
    || length <= 0
    || offset >= record.size
  ) {
    return length === 0 ? new Uint8Array() : null;
  }
  const readLen = Math.min(length, record.size - offset);
  const meta = readInodeMeta(sab, ino);
  if (!meta || meta.fileObjectId === 0) {
    return null;
  }
  const dirtyPages = collectResidentPageSnapshots(sab, meta, true);
  const out = new Uint8Array(readLen);
  const base = readBackingRange?.(ino, offset, readLen) ?? null;
  if (base && base.length >= readLen) {
    out.set(base.subarray(0, readLen));
  } else if (dirtyPages.length === 0) {
    return null;
  }
  const covered = new Uint8Array(readLen);
  for (const page of dirtyPages) {
    const pageStart = page.pageIndex * DEFAULT_RESIDENT_PAGE_SIZE;
    const pageEnd = pageStart + page.bytes.length;
    const copyStart = Math.max(offset, pageStart);
    const copyEnd = Math.min(offset + readLen, pageEnd);
    if (copyEnd <= copyStart) {
      continue;
    }
    const outStart = copyStart - offset;
    const pageOffset = copyStart - pageStart;
    const copyLen = copyEnd - copyStart;
    out.set(page.bytes.subarray(pageOffset, pageOffset + copyLen), outStart);
    covered.fill(1, outStart, outStart + copyLen);
  }
  if (!base || base.length < readLen) {
    for (let index = 0; index < covered.length; index += 1) {
      if (covered[index] === 0) {
        return null;
      }
    }
  }
  return out;
}

/**
 * Read an inode's metadata from the SAB page cache with seqlock validation.
 * Returns null if the inode doesn't exist or a torn read is detected after retries.
 *
 * Thread-worker side — kernel bypass for stat/fstat.
 */
export function readInodeMeta(
  sab: SharedArrayBuffer,
  ino: number,
): { dataOff: number; size: number; mode: number; fileObjectId: number; generation: number } | null {
  if (!Number.isInteger(ino) || ino < 0 || ino >= MAX_INODES) {
    return null;
  }
  const u32 = new Uint32Array(sab);
  const base = inodeOffset(ino);
  if (base < INODE_TABLE_OFFSET || base + INODE_ENTRY_SIZE > sab.byteLength) {
    return null;
  }
  const base32 = base >>> 2;

  for (let retry = 0; retry < 4; retry++) {
    const v1 = Atomics.load(u32, base32 + (INODE_OFF_VERSION >>> 2));
    if (v1 === 0) return null; // inode not published
    if (v1 & 1) continue; // odd = writing, retry

    const dataOff = Atomics.load(u32, base32 + (INODE_OFF_DATA_OFF >>> 2));
    const size = Atomics.load(u32, base32 + (INODE_OFF_DATA_SIZE >>> 2));
    const mode = Atomics.load(u32, base32 + (INODE_OFF_MODE >>> 2));
    const fileObjectId = Atomics.load(u32, base32 + (INODE_OFF_FILE_OBJECT_ID >>> 2));
    const generation = Atomics.load(u32, base32 + (INODE_OFF_GENERATION >>> 2));

    const v2 = Atomics.load(u32, base32 + (INODE_OFF_VERSION >>> 2));
    if (v1 === v2) {
      if (size < 0 || dataOff < 0) {
        return null;
      }
      if (size > 0 && dataOff !== 0) {
        return null;
      }
      return { dataOff, size, mode, fileObjectId, generation };
    }
    // torn read — retry
  }
  return null;
}

export function readInodeRecord(
  sab: SharedArrayBuffer,
  ino: number,
): {
  dataOff: number;
  size: number;
  mode: number;
  parentIno: number;
  name: Uint8Array;
  nextSibling: number;
  fileObjectId: number;
  generation: number;
} | null {
  if (!Number.isInteger(ino) || ino < 0 || ino >= MAX_INODES) {
    return null;
  }
  const u32 = new Uint32Array(sab);
  const u8 = new Uint8Array(sab);
  const base = inodeOffset(ino);
  if (base < INODE_TABLE_OFFSET || base + INODE_ENTRY_SIZE > sab.byteLength) {
    return null;
  }
  const base32 = base >>> 2;

  for (let retry = 0; retry < 4; retry++) {
    const v1 = Atomics.load(u32, base32 + (INODE_OFF_VERSION >>> 2));
    if (v1 === 0) return null;
    if (v1 & 1) continue;

    const dataOff = Atomics.load(u32, base32 + (INODE_OFF_DATA_OFF >>> 2));
    const size = Atomics.load(u32, base32 + (INODE_OFF_DATA_SIZE >>> 2));
    const mode = Atomics.load(u32, base32 + (INODE_OFF_MODE >>> 2));
    const parentIno = Atomics.load(u32, base32 + (INODE_OFF_PARENT >>> 2));
    const nameOff = Atomics.load(u32, base32 + (INODE_OFF_NAME_OFF >>> 2));
    const nameLen = Atomics.load(u32, base32 + (INODE_OFF_NAME_LEN >>> 2));
    const nextSibling = Atomics.load(u32, base32 + (INODE_OFF_NEXT_SIBLING >>> 2));
    const fileObjectId = Atomics.load(u32, base32 + (INODE_OFF_FILE_OBJECT_ID >>> 2));
    const generation = Atomics.load(u32, base32 + (INODE_OFF_GENERATION >>> 2));
    const name = nameLen > 0 ? u8.slice(nameOff, nameOff + nameLen) : new Uint8Array();

    const v2 = Atomics.load(u32, base32 + (INODE_OFF_VERSION >>> 2));
    if (v1 === v2) {
      if (size < 0 || dataOff < 0 || nameOff < 0 || nameLen < 0) {
        return null;
      }
      if (size > 0 && dataOff !== 0) {
        return null;
      }
      if (nameLen > 0) {
        if (nameOff < DATA_REGION_OFFSET || nameOff + nameLen > sab.byteLength) {
          return null;
        }
      }
      return {
        dataOff,
        size,
        mode,
        parentIno,
        name,
        nextSibling,
        fileObjectId,
        generation,
      };
    }
  }
  return null;
}

/**
 * Bump the directory epoch counter.
 * Called by kernel Worker after directory mutations (mkdir, unlink, rename).
 * Thread-workers check this to invalidate cached path resolutions.
 */
export function bumpDirEpoch(sab: SharedArrayBuffer): void {
  const u32 = new Uint32Array(sab);
  Atomics.add(u32, OFF_DIR_EPOCH >>> 2, 1);
}

export function resetPageCache(sab: SharedArrayBuffer): void {
  new Uint8Array(sab).fill(0);
}

/**
 * Read the current directory epoch.
 */
export function readDirEpoch(sab: SharedArrayBuffer): number {
  const u32 = new Uint32Array(sab);
  return Atomics.load(u32, OFF_DIR_EPOCH >>> 2);
}
