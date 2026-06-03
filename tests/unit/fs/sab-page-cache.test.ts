import { expect, test } from "vite-plus/test";
import {
  DATA_REGION_OFFSET,
  MAX_INODES,
  MemoryFileBackingStore,
  publishInode,
  ResidentSabPageCache,
  readFileData,
  readInodeMeta,
  readInodeRecord,
  writeFileData,
} from "../../../src/fs/sab-page-cache.js";
import { createKernelEnv } from "../../../src/wasm/env.js";

test("publishInode exposes file metadata and bytes when capacity exists", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 128);
  const name = new TextEncoder().encode("file");
  const fileData = new Uint8Array(8).fill(0x41);

  publishInode(sab, 2, {
    mode: 0o100755,
    size: fileData.length,
    parentIno: 1,
    name,
    nextSibling: 0,
  }, fileData);

  const meta = readInodeMeta(sab, 2);
  const record = readInodeRecord(sab, 2);
  expect(meta).not.toBeNull();
  expect(record).not.toBeNull();
  expect(meta?.size).toBe(fileData.length);
  expect(meta?.mode).toBe(0o100755);
  expect(meta?.dataOff).toBeGreaterThanOrEqual(DATA_REGION_OFFSET);
  expect(record?.name).toEqual(name);
});

test("page-cache read/stat report miss when inode metadata is out of cache bounds", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 16);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const env = createKernelEnv(memory, { sabPageCache: sab });
  const name = new TextEncoder().encode("file");
  const fileData = new Uint8Array(64).fill(0x41);

  publishInode(sab, 2, {
    mode: 0o100755,
    size: fileData.length,
    parentIno: 1,
    name,
    nextSibling: 0,
  }, fileData);

  expect(env.tidemark_page_cache_read(2, 0n, 0, 8)).toBe(-2n);
  expect(env.tidemark_page_cache_stat(2, 16)).toBe(0);
});

test("page-cache supports kernel-sized high inode numbers", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 64);
  const ino = MAX_INODES - 1;
  const name = new TextEncoder().encode("high-ino");
  const fileData = new Uint8Array([0x68, 0x69]);

  publishInode(sab, ino, {
    mode: 0o100644,
    size: fileData.length,
    parentIno: 1,
    name,
    nextSibling: 0,
  }, fileData);

  const meta = readInodeMeta(sab, ino);
  const record = readInodeRecord(sab, ino);
  expect(meta).not.toBeNull();
  expect(record).not.toBeNull();
  expect(meta?.size).toBe(2);
  expect(record?.name).toEqual(name);
});

test("readFileData honors byte offsets", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 256);
  const name = new TextEncoder().encode("file");
  const fileData = Uint8Array.from({ length: 64 }, (_value, index) => index);

  publishInode(sab, 2, {
    mode: 0o100644,
    size: fileData.length,
    parentIno: 1,
    name,
    nextSibling: 0,
  }, fileData);

  const meta = readInodeMeta(sab, 2);
  expect(meta).not.toBeNull();
  expect(readFileData(sab, meta!.dataOff, meta!.size, 16, 8)).toEqual(fileData.slice(16, 24));
  expect(readFileData(sab, meta!.dataOff, meta!.size, 60, 16)).toEqual(fileData.slice(60));
});

test("writeFileData appends in place and preserves inode metadata", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 256);
  const name = new TextEncoder().encode("file");
  const initial = new Uint8Array([1, 2, 3, 4]);
  const append = new Uint8Array([5, 6, 7]);

  publishInode(sab, 2, {
    mode: 0o100644,
    size: initial.length,
    parentIno: 1,
    name,
    nextSibling: 9,
  }, initial);
  const before = readInodeRecord(sab, 2);
  expect(before).not.toBeNull();

  expect(writeFileData(sab, 2, initial.length, append)).toBe(append.length);
  const moved = readInodeRecord(sab, 2);
  expect(moved).not.toBeNull();
  expect(moved?.dataOff).toBeGreaterThan(before!.dataOff);

  const secondAppend = new Uint8Array([8, 9]);
  expect(writeFileData(sab, 2, initial.length + append.length, secondAppend)).toBe(secondAppend.length);

  const after = readInodeRecord(sab, 2);
  expect(after).not.toBeNull();
  expect(after?.dataOff).toBe(moved?.dataOff);
  expect(after?.size).toBe(initial.length + append.length + secondAppend.length);
  expect(after?.mode).toBe(0o100644);
  expect(after?.parentIno).toBe(1);
  expect(after?.name).toEqual(name);
  expect(after?.nextSibling).toBe(9);
  expect(readFileData(sab, after!.dataOff, after!.size, 0, after!.size))
    .toEqual(new Uint8Array([...initial, ...append, ...secondAppend]));
});

test("resident cache reads a backing file larger than resident capacity at arbitrary offsets", () => {
  const pageSize = 16;
  const residentBytes = pageSize * 2;
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + residentBytes);
  const backing = new MemoryFileBackingStore();
  const data = Uint8Array.from({ length: 96 }, (_value, index) => index);
  const file = backing.createFile(data);
  const cache = new ResidentSabPageCache(sab, { pageSize });

  expect(file.size).toBeGreaterThan(cache.residentCapacityBytes());
  expect(cache.readThroughCache(backing, file.fileObjectId, 0, 7))
    .toEqual(data.slice(0, 7));
  expect(cache.readThroughCache(backing, file.fileObjectId, 14, 10))
    .toEqual(data.slice(14, 24));
  expect(cache.readThroughCache(backing, file.fileObjectId, 47, 37))
    .toEqual(data.slice(47, 84));
});

test("resident cache reports miss then fill then direct clean hit", () => {
  const pageSize = 16;
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + pageSize);
  const backing = new MemoryFileBackingStore();
  const data = Uint8Array.from({ length: 32 }, (_value, index) => 0x80 + index);
  const file = backing.createFile(data);
  const cache = new ResidentSabPageCache(sab, { pageSize });

  const firstRead = cache.readResidentRange(file, 4, 8);
  expect(firstRead.status).toBe("miss");
  expect(firstRead.status === "miss" ? firstRead.missingPageIndex : -1).toBe(0);

  expect(cache.fillPageFromBacking(backing, file.fileObjectId, 0).status).toBe("filled");
  const secondRead = cache.readResidentRange(file, 4, 8);
  expect(secondRead.status).toBe("hit");
  expect(secondRead.status === "hit" ? secondRead.bytes : new Uint8Array())
    .toEqual(data.slice(4, 12));
});

test("resident cache evicts clean pages and reloads them from backing store", () => {
  const pageSize = 16;
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + pageSize);
  const backing = new MemoryFileBackingStore();
  const data = Uint8Array.from({ length: 48 }, (_value, index) => 0x30 + index);
  const file = backing.createFile(data);
  const cache = new ResidentSabPageCache(sab, { pageSize });

  expect(cache.fillPageFromBacking(backing, file.fileObjectId, 0).status).toBe("filled");
  expect(cache.fillPageFromBacking(backing, file.fileObjectId, 1).status).toBe("filled");
  expect(cache.readResidentRange(file, 0, 4).status).toBe("miss");
  expect(cache.readThroughCache(backing, file.fileObjectId, 0, 20))
    .toEqual(data.slice(0, 20));
});

test("backing writes append truncate and sparse writes preserve size data and generation", () => {
  const pageSize = 8;
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + pageSize * 2);
  const backing = new MemoryFileBackingStore();
  const file = backing.createFile(new Uint8Array([1, 2, 3, 4, 5, 6]));
  const cache = new ResidentSabPageCache(sab, { pageSize });

  const append = cache.writeThroughBacking(backing, file.fileObjectId, 6, new Uint8Array([7, 8, 9, 10]));
  expect(append.size).toBe(10);
  expect(append.generation).toBe(file.generation + 1);
  expect(cache.readThroughCache(backing, file.fileObjectId, 0, append.size))
    .toEqual(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));

  const write = cache.writeThroughBacking(backing, file.fileObjectId, 7, new Uint8Array([17, 18, 19, 20]));
  expect(write.size).toBe(11);
  expect(write.generation).toBe(append.generation + 1);
  expect(cache.readThroughCache(backing, file.fileObjectId, 0, write.size))
    .toEqual(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 17, 18, 19, 20]));

  const sparse = cache.writeThroughBacking(backing, file.fileObjectId, 14, new Uint8Array([21, 22, 23]));
  expect(sparse.size).toBe(17);
  expect(sparse.generation).toBe(write.generation + 1);
  expect(cache.readThroughCache(backing, file.fileObjectId, 0, sparse.size))
    .toEqual(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 17, 18, 19, 20, 0, 0, 0, 21, 22, 23]));

  const truncated = cache.truncateBacking(backing, file.fileObjectId, 9);
  expect(truncated.size).toBe(9);
  expect(truncated.generation).toBe(sparse.generation + 1);
  expect(cache.readThroughCache(backing, file.fileObjectId, 0, 32))
    .toEqual(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 17, 18]));
});

test("dirty resident pages are pinned until flushed before clean eviction", () => {
  const pageSize = 8;
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + pageSize);
  const backing = new MemoryFileBackingStore();
  const file = backing.createFile(Uint8Array.from({ length: 16 }, (_value, index) => index));
  const cache = new ResidentSabPageCache(sab, { pageSize });

  expect(cache.fillPageFromBacking(backing, file.fileObjectId, 0).status).toBe("filled");
  expect(cache.writeResidentPageBytes(file.fileObjectId, 0, 2, new Uint8Array([77, 78]))).toBe(true);
  expect(cache.evictPage(file.fileObjectId, 0)).toBe("dirty-pinned");
  expect(cache.fillPageFromBacking(backing, file.fileObjectId, 1).status).toBe("dirty-pinned");

  const flushed = cache.flushDirtyPage(backing, file.fileObjectId, 0);
  expect(flushed?.generation).toBe(file.generation + 1);
  expect(cache.evictPage(file.fileObjectId, 0)).toBe("evicted");
  expect(cache.fillPageFromBacking(backing, file.fileObjectId, 1).status).toBe("filled");
  expect(cache.readThroughCache(backing, file.fileObjectId, 0, 8))
    .toEqual(new Uint8Array([0, 1, 77, 78, 4, 5, 6, 7]));
});
