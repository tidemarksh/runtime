import { expect, test } from "vite-plus/test";
import {
  DATA_REGION_OFFSET,
  debugResidentPageCountForInode,
  evictResidentPage,
  MAX_INODES,
  MemoryFileBackingStore,
  publishInode,
  readFileData,
  readInodeMeta,
  readInodeRecord,
  writeFileData,
} from "../../../src/fs/sab-page-cache.js";
import { createKernelEnv } from "../../../src/wasm/env.js";

test("memory atomic notify accepts wasm64 BigInt pointers", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const env = createKernelEnv(memory);

  expect(env.tidemark_memory_atomic_notify(0n, 1n)).toBe(0);
  expect(env.tidemark_memory_atomic_notify(BigInt(memory.buffer.byteLength), 1n)).toBe(0);
});

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

test("page-cache read can use kernel-worker backing data when resident bytes are absent", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 128);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const name = new TextEncoder().encode("external");
  const backing = new TextEncoder().encode("abcdef");
  const calls: Array<[number, number, number]> = [];

  publishInode(sab, 2, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name,
    nextSibling: 0,
  }, null);

  const env = createKernelEnv(memory, {
    sabPageCache: sab,
    readFileDataByInode: (ino, offset, length) => {
      calls.push([ino, offset, length]);
      return backing.subarray(offset, Math.min(backing.length, offset + length));
    },
  });

  expect(env.tidemark_page_cache_read(2, 2n, 32, 3)).toBe(3n);
  expect(calls).toEqual([[2, 2, 3]]);
  expect(Array.from(new Uint8Array(memory.buffer, 32, 3))).toEqual(Array.from(backing.subarray(2, 5)));
});

test("page-cache backing read reports miss when backing returns no bytes before eof", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 128);
  const memory = new WebAssembly.Memory({ initial: 1 });

  publishInode(sab, 2, {
    mode: 0o100644,
    size: 6,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
  }, null);

  const env = createKernelEnv(memory, {
    sabPageCache: sab,
    readFileDataByInode: () => new Uint8Array(),
  });

  expect(env.tidemark_page_cache_read(2, 2n, 32, 3)).toBe(-2n);
});

test("page-cache backing read fills a shared resident page for a later direct hit", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 8192);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const backing = new TextEncoder().encode("abcdef");
  const calls: Array<[number, number, number]> = [];

  publishInode(sab, 2, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
  }, null);

  const fillingEnv = createKernelEnv(memory, {
    sabPageCache: sab,
    readFileDataByInode: (ino, offset, length) => {
      calls.push([ino, offset, length]);
      return backing.subarray(offset, Math.min(backing.length, offset + length));
    },
  });

  expect(fillingEnv.tidemark_page_cache_read(2, 2n, 32, 3)).toBe(3n);
  expect(Array.from(new Uint8Array(memory.buffer, 32, 3))).toEqual(Array.from(backing.subarray(2, 5)));
  expect(calls).toEqual([[2, 0, backing.length]]);
  expect(debugResidentPageCountForInode(sab, 2)).toBe(1);

  new Uint8Array(memory.buffer, 32, 3).fill(0);
  const directEnv = createKernelEnv(memory, { sabPageCache: sab });

  expect(directEnv.tidemark_page_cache_read(2, 2n, 32, 3)).toBe(3n);
  expect(Array.from(new Uint8Array(memory.buffer, 32, 3))).toEqual(Array.from(backing.subarray(2, 5)));

  publishInode(sab, 2, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
  }, null);
  expect(debugResidentPageCountForInode(sab, 2)).toBe(0);
  expect(directEnv.tidemark_page_cache_read(2, 2n, 32, 3)).toBe(-2n);
});

test("page-cache clean resident eviction reloads from backing store", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 8192);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const backing = new TextEncoder().encode("abcdef");
  const calls: Array<[number, number, number]> = [];
  const readFileDataByInode = (ino: number, offset: number, length: number): Uint8Array => {
    calls.push([ino, offset, length]);
    return backing.subarray(offset, Math.min(backing.length, offset + length));
  };

  publishInode(sab, 2, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
  }, null);

  const fillingEnv = createKernelEnv(memory, {
    sabPageCache: sab,
    readFileDataByInode,
  });

  expect(fillingEnv.tidemark_page_cache_read(2, 1n, 32, 4)).toBe(4n);
  expect(calls).toEqual([[2, 0, backing.length]]);
  expect(Array.from(new Uint8Array(memory.buffer, 32, 4))).toEqual(Array.from(backing.subarray(1, 5)));
  expect(debugResidentPageCountForInode(sab, 2)).toBe(1);

  expect(evictResidentPage(sab, 2, 0)).toBe("evicted");
  expect(debugResidentPageCountForInode(sab, 2)).toBe(0);

  new Uint8Array(memory.buffer, 32, 4).fill(0);
  const directMissEnv = createKernelEnv(memory, { sabPageCache: sab });

  expect(directMissEnv.tidemark_page_cache_read(2, 1n, 32, 4)).toBe(-2n);

  const refillingEnv = createKernelEnv(memory, {
    sabPageCache: sab,
    readFileDataByInode,
  });

  expect(refillingEnv.tidemark_page_cache_read(2, 1n, 32, 4)).toBe(4n);
  expect(calls).toEqual([[2, 0, backing.length], [2, 0, backing.length]]);
  expect(Array.from(new Uint8Array(memory.buffer, 32, 4))).toEqual(Array.from(backing.subarray(1, 5)));
  expect(debugResidentPageCountForInode(sab, 2)).toBe(1);

  new Uint8Array(memory.buffer, 32, 4).fill(0);
  const directHitEnv = createKernelEnv(memory, { sabPageCache: sab });

  expect(directHitEnv.tidemark_page_cache_read(2, 1n, 32, 4)).toBe(4n);
  expect(Array.from(new Uint8Array(memory.buffer, 32, 4))).toEqual(Array.from(backing.subarray(1, 5)));
});

test("page-cache backing read fills every resident page in a multi-page request", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 16384);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const backing = new Uint8Array(6000);
  for (let offset = 0; offset < backing.length; offset += 1) {
    backing[offset] = (offset * 13 + 5) & 0xff;
  }
  const calls: Array<[number, number, number]> = [];

  publishInode(sab, 2, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
  }, null);

  const fillingEnv = createKernelEnv(memory, {
    sabPageCache: sab,
    readFileDataByInode: (ino, offset, length) => {
      calls.push([ino, offset, length]);
      return backing.subarray(offset, Math.min(backing.length, offset + length));
    },
  });

  expect(fillingEnv.tidemark_page_cache_read(2, 512n, 64, 5000)).toBe(5000n);
  expect(calls).toEqual([
    [2, 0, 4096],
    [2, 4096, backing.length - 4096],
  ]);
  expect(Array.from(new Uint8Array(memory.buffer, 64, 5000))).toEqual(Array.from(backing.subarray(512, 5512)));
  expect(debugResidentPageCountForInode(sab, 2)).toBe(2);

  new Uint8Array(memory.buffer, 64, 5000).fill(0);
  const directEnv = createKernelEnv(memory, { sabPageCache: sab });

  expect(directEnv.tidemark_page_cache_read(2, 512n, 64, 5000)).toBe(5000n);
  expect(Array.from(new Uint8Array(memory.buffer, 64, 5000))).toEqual(Array.from(backing.subarray(512, 5512)));
  expect(calls).toHaveLength(2);
});

test("page-cache zero-length read on kernel-worker backing data returns zero", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 128);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const name = new TextEncoder().encode("external");
  const backing = new TextEncoder().encode("abcdef");
  let calls = 0;

  publishInode(sab, 2, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name,
    nextSibling: 0,
  }, null);

  const env = createKernelEnv(memory, {
    sabPageCache: sab,
    readFileDataByInode: () => {
      calls += 1;
      return backing;
    },
  });

  expect(env.tidemark_page_cache_read(2, 0n, 32, 0)).toBe(0n);
  expect(calls).toBe(0);
});

test("page-cache write can update backing data when resident capacity is smaller than the file", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 8);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const backing = new Uint8Array(128);
  const source = Uint8Array.from({ length: 64 }, (_value, index) => 0x40 + index);
  new Uint8Array(memory.buffer, 256, source.length).set(source);

  publishInode(sab, 2, {
    mode: 0o100644,
    size: 0,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
  }, null);

  const env = createKernelEnv(memory, {
    sabPageCache: sab,
    readFileDataByInode: (_ino, offset, length) => backing.subarray(offset, offset + length),
    writeFileDataByInode: (ino, offset, data) => {
      backing.set(data, offset);
      publishInode(sab, ino, {
        mode: 0o100644,
        size: offset + data.length,
        parentIno: 1,
        name: new TextEncoder().encode("external"),
        nextSibling: 0,
      }, null);
      return data.length;
    },
  });

  expect(env.tidemark_page_cache_write(2, 32n, 256, source.length)).toBe(BigInt(source.length));
  expect(env.tidemark_page_cache_read(2, 32n, 512, source.length)).toBe(BigInt(source.length));
  expect(new Uint8Array(memory.buffer, 512, source.length)).toEqual(source);
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

test("source-backed file materializes for backing writes and truncates", () => {
  const backing = new MemoryFileBackingStore();
  const data = new TextEncoder().encode("abcdef");
  const calls: Array<[number, number]> = [];
  const file = backing.createSourceFile({
    byteLength: data.length,
    readRange(offset, length) {
      calls.push([offset, length]);
      return data.subarray(offset, Math.min(data.length, offset + length));
    },
  });

  const written = backing.write(file.fileObjectId, 3, new TextEncoder().encode("XYZ"));
  expect(written.size).toBe(6);
  expect(written.generation).toBe(file.generation + 1);
  expect(backing.read(file.fileObjectId, 0, 16)).toEqual(new TextEncoder().encode("abcXYZ"));
  expect(calls).toEqual([[0, data.length]]);

  const truncated = backing.truncate(file.fileObjectId, 4);
  expect(truncated.size).toBe(4);
  expect(truncated.generation).toBe(written.generation + 1);
  expect(backing.read(file.fileObjectId, 0, 16)).toEqual(new TextEncoder().encode("abcX"));
  expect(calls).toEqual([[0, data.length]]);
});

test("live resident records use file object generation and fixed reusable frames", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 4096 + 32);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const backing = new Uint8Array(8192);
  for (let offset = 0; offset < backing.length; offset += 1) {
    backing[offset] = (offset * 17 + 3) & 0xff;
  }

  publishInode(sab, 2, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
    fileObjectId: 44,
    generation: 7,
  }, null);

  const env = createKernelEnv(memory, {
    sabPageCache: sab,
    readFileDataByInode: (_ino, offset, length) => backing.subarray(offset, offset + length),
  });

  expect(env.tidemark_page_cache_read(2, 128n, 64, 16)).toBe(16n);
  expect(debugResidentPageCountForInode(sab, 2)).toBe(1);
  expect(new Uint8Array(memory.buffer, 64, 16)).toEqual(backing.slice(128, 144));

  new Uint8Array(memory.buffer, 64, 16).fill(0);
  expect(env.tidemark_page_cache_read(2, 4096n, 64, 16)).toBe(16n);
  expect(debugResidentPageCountForInode(sab, 2)).toBe(1);
  expect(new Uint8Array(memory.buffer, 64, 16)).toEqual(backing.slice(4096, 4112));

  new Uint8Array(memory.buffer, 64, 16).fill(0);
  const directEnv = createKernelEnv(memory, { sabPageCache: sab });
  expect(directEnv.tidemark_page_cache_read(2, 128n, 64, 16)).toBe(-2n);
  expect(directEnv.tidemark_page_cache_read(2, 4096n, 64, 16)).toBe(16n);

  publishInode(sab, 2, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
    fileObjectId: 44,
    generation: 8,
  }, null);
  expect(debugResidentPageCountForInode(sab, 2)).toBe(0);
  expect(directEnv.tidemark_page_cache_read(2, 4096n, 64, 16)).toBe(-2n);
});
