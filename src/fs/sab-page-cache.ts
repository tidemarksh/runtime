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
  const dataRegionEnd = sab.byteLength;

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
  if (end > sab.byteLength) {
    return 0;
  }
  Atomics.store(u32, OFF_DATA_USED >>> 2, currentUsed + byteLength);
  return start;
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
