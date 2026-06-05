/**
 * SAB Page Cache — SharedArrayBuffer-based file data and inode metadata.
 *
 * §6.3: kernel Worker publishes file data and inode metadata here.
 *        thread-workers read directly (kernel bypass for read/stat/openat O_RDONLY).
 *
 * Layout:
 *   [Header: 64 bytes]
 *     +0  AtomicU32  dataUsed       — next free offset in data region
 *     +4  AtomicU32  inodeCount     — number of published inodes
 *     +8  AtomicU32  dirEpoch       — directory structure epoch (for path resolution invalidation)
 *     +12 [padding 52 bytes]
 *
 *   [Inode table: MAX_INODES * 32 bytes, starting at offset 64]
 *     Per-inode entry (32 bytes):
 *       +0  AtomicU32  version    — seqlock (odd = writing, even = stable)
 *       +4  AtomicU32  data_off   — offset into data region
 *       +8  AtomicU32  data_size  — file size in bytes
 *       +12 AtomicU32  mode       — inode mode (S_IFREG, S_IFDIR, etc.)
 *       +16 AtomicU32  parent_ino — parent directory inode
 *       +20 AtomicU32  name_off   — offset of name in data region
 *       +24 AtomicU32  name_len   — name length
 *       +28 AtomicU32  next_sibling — next sibling inode (0 = none)
 *
 *   [Data region: remainder of SAB]
 *     Append-only file data blocks + name strings.
 */

export const MAX_INODES = 65536;
export const HEADER_SIZE = 64;
export const INODE_ENTRY_SIZE = 32;
export const INODE_TABLE_SIZE = MAX_INODES * INODE_ENTRY_SIZE;
export const INODE_TABLE_OFFSET = HEADER_SIZE;
export const DATA_REGION_OFFSET = HEADER_SIZE + INODE_TABLE_SIZE;

// Header field offsets (byte)
export const OFF_DATA_USED = 0;
export const OFF_INODE_COUNT = 4;
export const OFF_DIR_EPOCH = 8;

// Per-inode field offsets (byte, relative to inode entry start)
export const INODE_OFF_VERSION = 0;
export const INODE_OFF_DATA_OFF = 4;
export const INODE_OFF_DATA_SIZE = 8;
export const INODE_OFF_MODE = 12;
export const INODE_OFF_PARENT = 16;
export const INODE_OFF_NAME_OFF = 20;
export const INODE_OFF_NAME_LEN = 24;
export const INODE_OFF_NEXT_SIBLING = 28;

const DEFAULT_RESIDENT_PAGE_SIZE = 4096;
const RESIDENT_PAGE_RECORD_BYTES = 32;
const RESIDENT_PAGE_RECORD_VERSION = 0;
const RESIDENT_PAGE_RECORD_INO = 4;
const RESIDENT_PAGE_RECORD_PAGE_INDEX = 8;
const RESIDENT_PAGE_RECORD_DATA_OFF = 12;
const RESIDENT_PAGE_RECORD_VALID_BYTES = 16;
const RESIDENT_PAGE_RECORD_FLAGS = 20;
const RESIDENT_PAGE_RECORD_FLAG_VALID = 1;

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

export interface ResidentPageFillResult {
  status: "filled" | "already-resident" | "dirty-pinned" | "not-found";
  fileObjectId: FileObjectId;
  pageIndex: number;
  frameIndex?: number;
}

interface MemoryFileObject {
  data: Uint8Array | null;
  source: FileBackingSource | null;
  generation: number;
}

export class MemoryFileBackingStore {
  private nextFileObjectId = 1;
  private readonly files = new Map<FileObjectId, MemoryFileObject>();

  createFile(data: Uint8Array = new Uint8Array()): FileBackingStat {
    const fileObjectId = this.nextFileObjectId++;
    this.files.set(fileObjectId, {
      data: new Uint8Array(data),
      source: null,
      generation: 1,
    });
    return this.stat(fileObjectId)!;
  }

  createSourceFile(source: FileBackingSource): FileBackingStat {
    if (!Number.isInteger(source.byteLength) || source.byteLength < 0 || source.byteLength > 0xffffffff) {
      throw new RangeError(`invalid file source size: ${source.byteLength}`);
    }
    const fileObjectId = this.nextFileObjectId++;
    this.files.set(fileObjectId, {
      data: null,
      source,
      generation: 1,
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
    if (file.data) {
      return file.data.slice(offset, offset + readLen);
    }
    const bytes = file.source?.readRange(offset, readLen);
    if (!bytes) {
      return new Uint8Array();
    }
    return bytes.byteLength <= readLen ? bytes : bytes.slice(0, readLen);
  }

  write(fileObjectId: FileObjectId, offset: number, data: Uint8Array): FileBackingStat {
    if (!Number.isInteger(offset) || offset < 0) {
      throw new RangeError(`invalid file write offset: ${offset}`);
    }
    const file = this.files.get(fileObjectId);
    if (!file) {
      throw new Error(`file object not found: ${fileObjectId}`);
    }
    const fileData = this.materializeFile(fileObjectId, file);
    const end = offset + data.length;
    if (!Number.isFinite(end) || end > 0xffffffff) {
      throw new RangeError(`file write exceeds supported size: ${end}`);
    }
    if (end > fileData.length) {
      const next = new Uint8Array(end);
      next.set(fileData);
      file.data = next;
    }
    file.data!.set(data, offset);
    file.generation += 1;
    return this.stat(fileObjectId)!;
  }

  truncate(fileObjectId: FileObjectId, size: number): FileBackingStat {
    if (!Number.isInteger(size) || size < 0 || size > 0xffffffff) {
      throw new RangeError(`invalid file size: ${size}`);
    }
    const file = this.files.get(fileObjectId);
    if (!file) {
      throw new Error(`file object not found: ${fileObjectId}`);
    }
    const fileData = this.materializeFile(fileObjectId, file);
    if (size !== fileData.length) {
      const next = new Uint8Array(size);
      next.set(fileData.subarray(0, Math.min(size, fileData.length)));
      file.data = next;
      file.generation += 1;
    }
    return this.stat(fileObjectId)!;
  }

  private fileSize(file: MemoryFileObject): number {
    return file.data?.byteLength ?? file.source?.byteLength ?? 0;
  }

  private materializeFile(fileObjectId: FileObjectId, file: MemoryFileObject): Uint8Array {
    if (file.data) {
      return file.data;
    }
    const size = this.fileSize(file);
    if (size === 0) {
      file.data = new Uint8Array();
      file.source = null;
      return file.data;
    }
    const bytes = file.source?.readRange(0, size);
    if (!bytes || bytes.byteLength !== size) {
      throw new Error(`file source unavailable: ${fileObjectId}`);
    }
    file.data = new Uint8Array(bytes);
    file.source = null;
    return file.data;
  }
}

interface ResidentPageRecord {
  fileObjectId: FileObjectId;
  pageIndex: number;
  generation: number;
  frameIndex: number;
  validBytes: number;
  dirty: boolean;
  lastUsed: number;
}

export class ResidentSabPageCache {
  private readonly u8: Uint8Array;
  private readonly pageSize: number;
  private readonly frameCount: number;
  private readonly recordsByKey = new Map<string, ResidentPageRecord>();
  private readonly recordsByFrame = new Map<number, ResidentPageRecord>();
  private clock = 1;

  constructor(sab: SharedArrayBuffer, options: { pageSize?: number } = {}) {
    this.pageSize = options.pageSize ?? DEFAULT_RESIDENT_PAGE_SIZE;
    if (!Number.isInteger(this.pageSize) || this.pageSize <= 0) {
      throw new RangeError(`invalid resident page size: ${this.pageSize}`);
    }
    this.u8 = new Uint8Array(sab);
    this.frameCount = Math.floor(Math.max(0, sab.byteLength - DATA_REGION_OFFSET) / this.pageSize);
  }

  residentCapacityBytes(): number {
    return this.frameCount * this.pageSize;
  }

  residentPageCount(): number {
    return this.recordsByKey.size;
  }

  readResidentRange(
    stat: FileBackingStat,
    offset: number,
    length: number,
  ): ResidentPageCacheReadResult {
    if (length <= 0 || offset >= stat.size) {
      return { status: "hit", bytes: new Uint8Array() };
    }
    const readLen = Math.min(length, stat.size - offset);
    const out = new Uint8Array(readLen);
    let copied = 0;
    while (copied < readLen) {
      const absolute = offset + copied;
      const pageIndex = Math.floor(absolute / this.pageSize);
      const pageOffset = absolute % this.pageSize;
      const copyLen = Math.min(readLen - copied, this.pageSize - pageOffset);
      const record = this.recordFor(stat.fileObjectId, pageIndex);
      if (!record || record.generation !== stat.generation || record.dirty) {
        return { status: "miss", missingPageIndex: pageIndex };
      }
      if (pageOffset + copyLen > record.validBytes) {
        return { status: "miss", missingPageIndex: pageIndex };
      }
      out.set(
        this.u8.subarray(
          this.frameOffset(record.frameIndex) + pageOffset,
          this.frameOffset(record.frameIndex) + pageOffset + copyLen,
        ),
        copied,
      );
      record.lastUsed = this.clock++;
      copied += copyLen;
    }
    return { status: "hit", bytes: out };
  }

  fillPageFromBacking(
    backing: MemoryFileBackingStore,
    fileObjectId: FileObjectId,
    pageIndex: number,
  ): ResidentPageFillResult {
    const stat = backing.stat(fileObjectId);
    if (!stat) {
      return { status: "not-found", fileObjectId, pageIndex };
    }
    const current = this.recordFor(fileObjectId, pageIndex);
    if (current && current.generation === stat.generation && !current.dirty) {
      current.lastUsed = this.clock++;
      return {
        status: "already-resident",
        fileObjectId,
        pageIndex,
        frameIndex: current.frameIndex,
      };
    }
    if (current) {
      this.removeRecord(current);
    }
    const frameIndex = this.allocateFrame();
    if (frameIndex === null) {
      return { status: "dirty-pinned", fileObjectId, pageIndex };
    }
    const pageStart = pageIndex * this.pageSize;
    const bytes = backing.read(fileObjectId, pageStart, this.pageSize);
    const frameStart = this.frameOffset(frameIndex);
    this.u8.fill(0, frameStart, frameStart + this.pageSize);
    this.u8.set(bytes, frameStart);
    const record: ResidentPageRecord = {
      fileObjectId,
      pageIndex,
      generation: stat.generation,
      frameIndex,
      validBytes: bytes.length,
      dirty: false,
      lastUsed: this.clock++,
    };
    this.setRecord(record);
    return { status: "filled", fileObjectId, pageIndex, frameIndex };
  }

  readThroughCache(
    backing: MemoryFileBackingStore,
    fileObjectId: FileObjectId,
    offset: number,
    length: number,
  ): Uint8Array {
    const stat = backing.stat(fileObjectId);
    if (!stat || length <= 0 || offset >= stat.size) {
      return new Uint8Array();
    }
    const readLen = Math.min(length, stat.size - offset);
    const out = new Uint8Array(readLen);
    let copied = 0;
    while (copied < readLen) {
      const currentStat = backing.stat(fileObjectId);
      if (!currentStat) {
        break;
      }
      const absolute = offset + copied;
      const pageOffset = absolute % this.pageSize;
      const copyLen = Math.min(readLen - copied, this.pageSize - pageOffset);
      const hit = this.readResidentRange(currentStat, absolute, copyLen);
      if (hit.status === "miss") {
        const filled = this.fillPageFromBacking(backing, fileObjectId, hit.missingPageIndex);
        if (filled.status !== "filled" && filled.status !== "already-resident") {
          throw new Error(`failed to fill resident page: ${filled.status}`);
        }
        continue;
      }
      out.set(hit.bytes, copied);
      copied += hit.bytes.length;
    }
    return out;
  }

  writeThroughBacking(
    backing: MemoryFileBackingStore,
    fileObjectId: FileObjectId,
    offset: number,
    data: Uint8Array,
  ): FileBackingStat {
    const stat = backing.write(fileObjectId, offset, data);
    this.invalidateFileRange(fileObjectId, offset, data.length);
    return stat;
  }

  truncateBacking(
    backing: MemoryFileBackingStore,
    fileObjectId: FileObjectId,
    size: number,
  ): FileBackingStat {
    const stat = backing.truncate(fileObjectId, size);
    this.invalidateFile(fileObjectId);
    return stat;
  }

  evictPage(fileObjectId: FileObjectId, pageIndex: number): "evicted" | "dirty-pinned" | "missing" {
    const record = this.recordFor(fileObjectId, pageIndex);
    if (!record) {
      return "missing";
    }
    if (record.dirty) {
      return "dirty-pinned";
    }
    this.removeRecord(record);
    return "evicted";
  }

  markResidentPageDirty(fileObjectId: FileObjectId, pageIndex: number): boolean {
    const record = this.recordFor(fileObjectId, pageIndex);
    if (!record) {
      return false;
    }
    record.dirty = true;
    record.lastUsed = this.clock++;
    return true;
  }

  writeResidentPageBytes(fileObjectId: FileObjectId, pageIndex: number, offset: number, data: Uint8Array): boolean {
    const record = this.recordFor(fileObjectId, pageIndex);
    if (!record || offset < 0 || offset + data.length > this.pageSize) {
      return false;
    }
    this.u8.set(data, this.frameOffset(record.frameIndex) + offset);
    record.validBytes = Math.max(record.validBytes, offset + data.length);
    record.dirty = true;
    record.lastUsed = this.clock++;
    return true;
  }

  flushDirtyPage(
    backing: MemoryFileBackingStore,
    fileObjectId: FileObjectId,
    pageIndex: number,
  ): FileBackingStat | null {
    const record = this.recordFor(fileObjectId, pageIndex);
    if (!record || !record.dirty) {
      return backing.stat(fileObjectId);
    }
    const frameStart = this.frameOffset(record.frameIndex);
    const stat = backing.write(
      fileObjectId,
      pageIndex * this.pageSize,
      this.u8.slice(frameStart, frameStart + record.validBytes),
    );
    record.generation = stat.generation;
    record.dirty = false;
    record.lastUsed = this.clock++;
    return stat;
  }

  private frameOffset(frameIndex: number): number {
    return DATA_REGION_OFFSET + frameIndex * this.pageSize;
  }

  private pageKey(fileObjectId: FileObjectId, pageIndex: number): string {
    return `${fileObjectId}:${pageIndex}`;
  }

  private recordFor(fileObjectId: FileObjectId, pageIndex: number): ResidentPageRecord | undefined {
    return this.recordsByKey.get(this.pageKey(fileObjectId, pageIndex));
  }

  private setRecord(record: ResidentPageRecord): void {
    this.recordsByKey.set(this.pageKey(record.fileObjectId, record.pageIndex), record);
    this.recordsByFrame.set(record.frameIndex, record);
  }

  private removeRecord(record: ResidentPageRecord): void {
    this.recordsByKey.delete(this.pageKey(record.fileObjectId, record.pageIndex));
    this.recordsByFrame.delete(record.frameIndex);
  }

  private allocateFrame(): number | null {
    for (let frameIndex = 0; frameIndex < this.frameCount; frameIndex += 1) {
      if (!this.recordsByFrame.has(frameIndex)) {
        return frameIndex;
      }
    }
    let evictable: ResidentPageRecord | null = null;
    for (const record of this.recordsByFrame.values()) {
      if (record.dirty) {
        continue;
      }
      if (!evictable || record.lastUsed < evictable.lastUsed) {
        evictable = record;
      }
    }
    if (!evictable) {
      return null;
    }
    const frameIndex = evictable.frameIndex;
    this.removeRecord(evictable);
    return frameIndex;
  }

  private invalidateFile(fileObjectId: FileObjectId): void {
    for (const record of [...this.recordsByKey.values()]) {
      if (record.fileObjectId === fileObjectId) {
        this.removeRecord(record);
      }
    }
  }

  private invalidateFileRange(fileObjectId: FileObjectId, offset: number, length: number): void {
    if (length <= 0) {
      return;
    }
    const startPage = Math.floor(offset / this.pageSize);
    const endPage = Math.floor((offset + length - 1) / this.pageSize);
    for (let pageIndex = startPage; pageIndex <= endPage; pageIndex += 1) {
      const record = this.recordFor(fileObjectId, pageIndex);
      if (record) {
        this.removeRecord(record);
      }
    }
  }
}

/**
 * Get the byte offset for a given inode entry in the SAB.
 */
export function inodeOffset(ino: number): number {
  return INODE_TABLE_OFFSET + ino * INODE_ENTRY_SIZE;
}

/**
 * Publish (or update) an inode's metadata and file data to the SAB page cache.
 * Called by the kernel Worker after MemFS writes.
 *
 * Uses seqlock protocol:
 *   1. Write data to data region (append-only)
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
  },
  fileData: Uint8Array | null,
): void {
  const u32 = new Uint32Array(sab);
  const u8 = new Uint8Array(sab);
  const base = inodeOffset(ino);
  const base32 = base >>> 2;
  const dataRegionEnd = dataRegionLimit(sab);

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

  // Allocate space in data region for file data
  let dataOff = 0;
  if (fileData && fileData.length > 0) {
    dataOff = allocateDataRegion(fileData.length);
    if (dataOff === 0) {
      return;
    }
    u8.set(fileData, dataOff);
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
  invalidateResidentPages(sab, ino);
  Atomics.store(u32, base32 + (INODE_OFF_VERSION >>> 2), oddVersion);

  // Update metadata
  Atomics.store(u32, base32 + (INODE_OFF_DATA_OFF >>> 2), dataOff);
  Atomics.store(u32, base32 + (INODE_OFF_DATA_SIZE >>> 2), meta.size);
  Atomics.store(u32, base32 + (INODE_OFF_MODE >>> 2), meta.mode);
  Atomics.store(u32, base32 + (INODE_OFF_PARENT >>> 2), meta.parentIno);
  Atomics.store(u32, base32 + (INODE_OFF_NAME_OFF >>> 2), nameOff);
  Atomics.store(u32, base32 + (INODE_OFF_NAME_LEN >>> 2), meta.name.length);
  Atomics.store(u32, base32 + (INODE_OFF_NEXT_SIBLING >>> 2), meta.nextSibling);

  // Seqlock: version → even (stable)
  Atomics.store(u32, base32 + (INODE_OFF_VERSION >>> 2), oddVersion + 1);

  // Update inode count if this is a new inode
  const inodeCount = Atomics.load(u32, OFF_INODE_COUNT >>> 2);
  if (ino >= inodeCount) {
    Atomics.store(u32, OFF_INODE_COUNT >>> 2, ino + 1);
  }
}

function allocateDataRegion(sab: SharedArrayBuffer, byteLength: number): number {
  if (byteLength <= 0) {
    return 0;
  }
  const u32 = new Uint32Array(sab);
  const currentUsed = Atomics.load(u32, OFF_DATA_USED >>> 2);
  const start = DATA_REGION_OFFSET + currentUsed;
  const end = start + byteLength;
  if (end > dataRegionLimit(sab)) {
    return 0;
  }
  Atomics.store(u32, OFF_DATA_USED >>> 2, currentUsed + byteLength);
  return start;
}

function residentPageRecordCount(sab: SharedArrayBuffer): number {
  const available = Math.max(0, sab.byteLength - DATA_REGION_OFFSET);
  return Math.floor(available / (DEFAULT_RESIDENT_PAGE_SIZE + RESIDENT_PAGE_RECORD_BYTES));
}

export function hasResidentPageRecordCapacity(sab: SharedArrayBuffer): boolean {
  return residentPageRecordCount(sab) > 0;
}

function residentPageRecordTableOffset(sab: SharedArrayBuffer): number {
  return sab.byteLength - residentPageRecordCount(sab) * RESIDENT_PAGE_RECORD_BYTES;
}

function dataRegionLimit(sab: SharedArrayBuffer): number {
  return residentPageRecordTableOffset(sab);
}

function residentPageRecordOffset(sab: SharedArrayBuffer, index: number): number {
  return residentPageRecordTableOffset(sab) + index * RESIDENT_PAGE_RECORD_BYTES;
}

function findResidentPageRecordIndex(sab: SharedArrayBuffer, ino: number, pageIndex: number): number {
  const u32 = new Uint32Array(sab);
  const count = residentPageRecordCount(sab);
  for (let index = 0; index < count; index += 1) {
    const base32 = residentPageRecordOffset(sab, index) >>> 2;
    const flags = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2));
    if (
      (flags & RESIDENT_PAGE_RECORD_FLAG_VALID) !== 0
      && Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_INO >>> 2)) === ino
      && Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_PAGE_INDEX >>> 2)) === pageIndex
    ) {
      return index;
    }
  }
  return -1;
}

function allocateResidentPageRecordIndex(sab: SharedArrayBuffer, ino: number, pageIndex: number): number {
  const existing = findResidentPageRecordIndex(sab, ino, pageIndex);
  if (existing >= 0) {
    return existing;
  }
  const u32 = new Uint32Array(sab);
  const count = residentPageRecordCount(sab);
  for (let index = 0; index < count; index += 1) {
    const base32 = residentPageRecordOffset(sab, index) >>> 2;
    const flags = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2));
    if ((flags & RESIDENT_PAGE_RECORD_FLAG_VALID) === 0) {
      return index;
    }
  }
  return -1;
}

function clearResidentPageRecord(sab: SharedArrayBuffer, recordIndex: number): void {
  const u32 = new Uint32Array(sab);
  const base32 = residentPageRecordOffset(sab, recordIndex) >>> 2;
  const currentVersion = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2));
  const oddVersion = (currentVersion | 1) + (currentVersion & 1 ? 1 : 0);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2), oddVersion);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2), 0);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2), oddVersion + 1);
}

function reusableResidentPageDataOff(sab: SharedArrayBuffer, recordIndex: number): number {
  const u32 = new Uint32Array(sab);
  const base32 = residentPageRecordOffset(sab, recordIndex) >>> 2;
  const flags = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2));
  if ((flags & RESIDENT_PAGE_RECORD_FLAG_VALID) !== 0) {
    return 0;
  }
  const dataOff = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_DATA_OFF >>> 2));
  if (dataOff < DATA_REGION_OFFSET || dataOff + DEFAULT_RESIDENT_PAGE_SIZE > dataRegionLimit(sab)) {
    return 0;
  }
  return dataOff;
}

function invalidateResidentPages(sab: SharedArrayBuffer, ino: number): void {
  const u32 = new Uint32Array(sab);
  const count = residentPageRecordCount(sab);
  for (let index = 0; index < count; index += 1) {
    const base32 = residentPageRecordOffset(sab, index) >>> 2;
    const flags = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2));
    if (
      (flags & RESIDENT_PAGE_RECORD_FLAG_VALID) !== 0
      && Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_INO >>> 2)) === ino
    ) {
      clearResidentPageRecord(sab, index);
    }
  }
}

export function evictResidentPage(
  sab: SharedArrayBuffer,
  ino: number,
  pageIndex: number,
): "evicted" | "not-found" {
  if (
    !Number.isInteger(ino)
    || ino <= 0
    || ino >= MAX_INODES
    || !Number.isInteger(pageIndex)
    || pageIndex < 0
  ) {
    return "not-found";
  }
  const recordIndex = findResidentPageRecordIndex(sab, ino, pageIndex);
  if (recordIndex < 0) {
    return "not-found";
  }
  clearResidentPageRecord(sab, recordIndex);
  return "evicted";
}

export function publishResidentPage(
  sab: SharedArrayBuffer,
  ino: number,
  pageIndex: number,
  data: Uint8Array,
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
  const recordIndex = allocateResidentPageRecordIndex(sab, ino, pageIndex);
  if (recordIndex < 0) {
    return false;
  }
  const dataOff = reusableResidentPageDataOff(sab, recordIndex)
    || allocateDataRegion(sab, DEFAULT_RESIDENT_PAGE_SIZE);
  if (dataOff === 0) {
    return false;
  }
  const u8 = new Uint8Array(sab);
  u8.fill(0, dataOff, dataOff + DEFAULT_RESIDENT_PAGE_SIZE);
  u8.set(data, dataOff);

  const u32 = new Uint32Array(sab);
  const base32 = residentPageRecordOffset(sab, recordIndex) >>> 2;
  const currentVersion = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2));
  const oddVersion = (currentVersion | 1) + (currentVersion & 1 ? 1 : 0);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2), oddVersion);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_INO >>> 2), ino);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_PAGE_INDEX >>> 2), pageIndex);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_DATA_OFF >>> 2), dataOff);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_VALID_BYTES >>> 2), data.length);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2), RESIDENT_PAGE_RECORD_FLAG_VALID);
  Atomics.store(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2), oddVersion + 1);
  return true;
}

export function readResidentFileData(
  sab: SharedArrayBuffer,
  ino: number,
  offset: number,
  length: number,
): Uint8Array | null {
  if (length <= 0) {
    return new Uint8Array();
  }
  const out = new Uint8Array(length);
  const u32 = new Uint32Array(sab);
  const u8 = new Uint8Array(sab);
  let copied = 0;
  while (copied < length) {
    const absolute = offset + copied;
    const pageIndex = Math.floor(absolute / DEFAULT_RESIDENT_PAGE_SIZE);
    const pageOffset = absolute % DEFAULT_RESIDENT_PAGE_SIZE;
    const copyLen = Math.min(length - copied, DEFAULT_RESIDENT_PAGE_SIZE - pageOffset);
    const recordIndex = findResidentPageRecordIndex(sab, ino, pageIndex);
    if (recordIndex < 0) {
      return null;
    }
    const base32 = residentPageRecordOffset(sab, recordIndex) >>> 2;
    const v1 = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2));
    if (v1 === 0 || (v1 & 1) !== 0) {
      return null;
    }
    const dataOff = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_DATA_OFF >>> 2));
    const validBytes = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_VALID_BYTES >>> 2));
    const flags = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_FLAGS >>> 2));
    const v2 = Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_VERSION >>> 2));
    if (
      v1 !== v2
      || (flags & RESIDENT_PAGE_RECORD_FLAG_VALID) === 0
      || dataOff < DATA_REGION_OFFSET
      || dataOff + validBytes > dataRegionLimit(sab)
      || pageOffset + copyLen > validBytes
    ) {
      return null;
    }
    out.set(u8.subarray(dataOff + pageOffset, dataOff + pageOffset + copyLen), copied);
    copied += copyLen;
  }
  return out;
}

export function debugResidentPageCountForInode(sab: SharedArrayBuffer, ino: number): number {
  if (!Number.isInteger(ino) || ino <= 0 || ino >= MAX_INODES) {
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
      && Atomics.load(u32, base32 + (RESIDENT_PAGE_RECORD_INO >>> 2)) === ino
    ) {
      residentPages += 1;
    }
  }
  return residentPages;
}

export function writeFileData(
  sab: SharedArrayBuffer,
  ino: number,
  offset: number,
  data: Uint8Array,
): number {
  if (!Number.isInteger(ino) || ino <= 0 || ino >= MAX_INODES || offset < 0) {
    return -22;
  }
  if (data.length === 0) {
    return 0;
  }

  const u32 = new Uint32Array(sab);
  const u8 = new Uint8Array(sab);
  const base = inodeOffset(ino);
  if (base < INODE_TABLE_OFFSET || base + INODE_ENTRY_SIZE > sab.byteLength) {
    return -22;
  }
  const base32 = base >>> 2;
  const oldNameOff = Atomics.load(u32, base32 + (INODE_OFF_NAME_OFF >>> 2));
  const oldNameLen = Atomics.load(u32, base32 + (INODE_OFF_NAME_LEN >>> 2));
  const oldRecord = readInodeRecord(sab, ino);
  const oldSize = oldRecord?.size ?? 0;
  const oldDataOff = oldRecord?.dataOff ?? 0;
  const end = offset + data.length;
  const nextSize = Math.max(oldSize, end);
  if (!Number.isFinite(nextSize) || nextSize > 0xffffffff) {
    return -28;
  }

  let dataOff = oldDataOff;
  if (
    oldDataOff >= DATA_REGION_OFFSET
    && end <= oldSize
    && oldDataOff + end <= sab.byteLength
  ) {
    u8.set(data, oldDataOff + offset);
  } else if (
    oldDataOff >= DATA_REGION_OFFSET
    && offset === oldSize
    && oldDataOff + oldSize === DATA_REGION_OFFSET + Atomics.load(u32, OFF_DATA_USED >>> 2)
    && oldDataOff + nextSize <= sab.byteLength
  ) {
    u8.set(data, oldDataOff + offset);
    Atomics.store(u32, OFF_DATA_USED >>> 2, oldDataOff + nextSize - DATA_REGION_OFFSET);
  } else {
    dataOff = allocateDataRegion(sab, nextSize);
    if (dataOff === 0) {
      return -28;
    }
    if (oldRecord && oldRecord.dataOff >= DATA_REGION_OFFSET && oldRecord.size > 0) {
      u8.set(readFileData(sab, oldRecord.dataOff, oldRecord.size, 0, oldRecord.size), dataOff);
    }
    u8.set(data, dataOff + offset);
  }

  const currentVersion = Atomics.load(u32, base32 + (INODE_OFF_VERSION >>> 2));
  const oddVersion = (currentVersion | 1) + (currentVersion & 1 ? 1 : 0);
  invalidateResidentPages(sab, ino);
  Atomics.store(u32, base32 + (INODE_OFF_VERSION >>> 2), oddVersion);
  Atomics.store(u32, base32 + (INODE_OFF_DATA_OFF >>> 2), dataOff);
  Atomics.store(u32, base32 + (INODE_OFF_DATA_SIZE >>> 2), nextSize);
  Atomics.store(u32, base32 + (INODE_OFF_MODE >>> 2), oldRecord?.mode ?? 0o100644);
  Atomics.store(u32, base32 + (INODE_OFF_PARENT >>> 2), oldRecord?.parentIno ?? 0);
  Atomics.store(u32, base32 + (INODE_OFF_NAME_OFF >>> 2), oldNameLen > 0 ? oldNameOff : 0);
  Atomics.store(u32, base32 + (INODE_OFF_NAME_LEN >>> 2), oldNameLen);
  Atomics.store(u32, base32 + (INODE_OFF_NEXT_SIBLING >>> 2), oldRecord?.nextSibling ?? 0);
  Atomics.store(u32, base32 + (INODE_OFF_VERSION >>> 2), oddVersion + 1);

  const inodeCount = Atomics.load(u32, OFF_INODE_COUNT >>> 2);
  if (ino >= inodeCount) {
    Atomics.store(u32, OFF_INODE_COUNT >>> 2, ino + 1);
  }
  return data.length;
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
): { dataOff: number; size: number; mode: number } | null {
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

    const v2 = Atomics.load(u32, base32 + (INODE_OFF_VERSION >>> 2));
    if (v1 === v2) {
      if (size < 0 || dataOff < 0) {
        return null;
      }
      if (size > 0 && dataOff !== 0) {
        if (dataOff < DATA_REGION_OFFSET) {
          return null;
        }
        if (dataOff + size > sab.byteLength) {
          return null;
        }
      }
      return { dataOff, size, mode };
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
    const name = nameLen > 0 ? u8.slice(nameOff, nameOff + nameLen) : new Uint8Array();

    const v2 = Atomics.load(u32, base32 + (INODE_OFF_VERSION >>> 2));
    if (v1 === v2) {
      if (size < 0 || dataOff < 0 || nameOff < 0 || nameLen < 0) {
        return null;
      }
      if (size > 0 && dataOff !== 0) {
        if (dataOff < DATA_REGION_OFFSET || dataOff + size > sab.byteLength) {
          return null;
        }
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
      };
    }
  }
  return null;
}

/**
 * Read file data from the SAB page cache.
 * Caller should first use readInodeMeta to get dataOff and size.
 *
 * Thread-worker side — kernel bypass for read/pread64.
 */
export function readFileData(
  sab: SharedArrayBuffer,
  dataOff: number,
  size: number,
  dstOffset: number,
  dstLen: number,
): Uint8Array {
  const u8 = new Uint8Array(sab);
  if (dstLen <= 0 || dstOffset < 0 || dstOffset >= size) {
    return new Uint8Array();
  }
  const readLen = Math.min(size - dstOffset, dstLen);
  const start = dataOff + dstOffset;
  return u8.slice(start, start + readLen);
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
