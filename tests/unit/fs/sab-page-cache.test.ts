import { expect, test } from "vite-plus/test";
import {
  DATA_REGION_OFFSET,
  debugResidentPageCountForInode,
  evictResidentPage,
  MAX_INODES,
  MemoryFileBackingStore,
  publishInode,
  publishResidentPage,
  readInodeMeta,
  readInodeRecord,
} from "../../../src/fs/sab-page-cache.js";
import { createKernelEnv } from "../../../src/wasm/env.js";

test("memory atomic notify accepts wasm64 BigInt pointers", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const env = createKernelEnv(memory);

  expect(env.tidemark_memory_atomic_notify(0n, 1n)).toBe(0);
  expect(env.tidemark_memory_atomic_notify(BigInt(memory.buffer.byteLength), 1n)).toBe(0);
});

test("page-cache imports reject unsafe Memory64 pointers without truncation", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 8192);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const env = createKernelEnv(memory, { sabPageCache: sab });
  const bytes = new TextEncoder().encode("abcdef");

  publishInode(sab, 2, {
    mode: 0o100644,
    size: bytes.length,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
    fileObjectId: 2,
    generation: 1,
  });
  expect(publishResidentPage(sab, 2, 0, bytes)).toBe(true);

  const unsafePtr = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
  expect(env.tidemark_page_cache_read(2, 0n, unsafePtr, bytes.length)).toBe(-14n);
  expect(env.tidemark_page_cache_write(2, 0n, unsafePtr, bytes.length)).toBe(-14n);
  expect(env.tidemark_page_cache_stat(2, unsafePtr)).toBe(0);
  expect(Array.from(new Uint8Array(memory.buffer, 0, bytes.length))).toEqual(new Array(bytes.length).fill(0));
});

test("page-cache imports accept safe Memory64 BigInt pointers", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 8192);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const env = createKernelEnv(memory, { sabPageCache: sab });
  const bytes = new TextEncoder().encode("abcdef");

  publishInode(sab, 2, {
    mode: 0o100644,
    size: bytes.length,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
    fileObjectId: 2,
    generation: 1,
  });
  expect(publishResidentPage(sab, 2, 0, bytes)).toBe(true);

  expect(env.tidemark_page_cache_read(2, 0n, 32n, bytes.length)).toBe(BigInt(bytes.length));
  expect(Array.from(new Uint8Array(memory.buffer, 32, bytes.length))).toEqual(Array.from(bytes));
  expect(env.tidemark_page_cache_stat(2, 64n)).toBe(1);
  expect(new DataView(memory.buffer).getBigUint64(72, true)).toBe(BigInt(bytes.length));
});

test("page-cache flush import delegates dirty resident writeback", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 128);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const calls: number[] = [];
  const env = createKernelEnv(memory, {
    sabPageCache: sab,
    flushResidentFileDataByInode: (ino) => {
      calls.push(ino);
      return ino === 2;
    },
  });

  expect(env.tidemark_page_cache_flush(2)).toBe(0);
  expect(env.tidemark_page_cache_flush(3)).toBe(-5);
  expect(calls).toEqual([2, 3]);
});

test("page-cache flush import rejects unowned writeback", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const env = createKernelEnv(memory);

  expect(env.tidemark_page_cache_flush(2)).toBe(-95);
});

test("publishInode exposes file metadata without inline file bytes", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 128);
  const name = new TextEncoder().encode("file");
  const fileData = new Uint8Array(8).fill(0x41);

  publishInode(sab, 2, {
    mode: 0o100755,
    size: fileData.length,
    parentIno: 1,
    name,
    nextSibling: 0,
  });

  const meta = readInodeMeta(sab, 2);
  const record = readInodeRecord(sab, 2);
  expect(meta).not.toBeNull();
  expect(record).not.toBeNull();
  expect(meta?.size).toBe(fileData.length);
  expect(meta?.mode).toBe(0o100755);
  expect(meta?.dataOff).toBe(0);
  expect(meta?.fileObjectId).toBe(2);
  expect(record?.name).toEqual(name);
});

test("page-cache read reports resident miss while stat reports metadata-only inode", () => {
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
  });

  expect(env.tidemark_page_cache_read(2, 0n, 0, 8)).toBe(-2n);
  expect(env.tidemark_page_cache_stat(2, 16)).toBe(1);
});

test("page-cache read reports miss without using backing data when resident bytes are absent", () => {
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
  });

  const env = createKernelEnv(memory, {
    sabPageCache: sab,
    readFileDataByInode: (ino, offset, length) => {
      calls.push([ino, offset, length]);
      return backing.subarray(offset, Math.min(backing.length, offset + length));
    },
  });

  expect(env.tidemark_page_cache_read(2, 2n, 32, 3)).toBe(-2n);
  expect(calls).toEqual([]);
});

test("page-cache read reports miss when no resident page exists before eof", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 128);
  const memory = new WebAssembly.Memory({ initial: 1 });
  let calls = 0;

  publishInode(sab, 2, {
    mode: 0o100644,
    size: 6,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
  });

  const env = createKernelEnv(memory, {
    sabPageCache: sab,
    readFileDataByInode: () => {
      calls += 1;
      return new Uint8Array();
    },
  });

  expect(env.tidemark_page_cache_read(2, 2n, 32, 3)).toBe(-2n);
  expect(calls).toBe(0);
});

test("page-cache read uses resident pages filled by the status path", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 8192);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const backing = new TextEncoder().encode("abcdef");

  publishInode(sab, 2, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
    fileObjectId: 2,
    generation: 1,
  });

  expect(publishResidentPage(sab, 2, 0, backing)).toBe(true);

  const directEnv = createKernelEnv(memory, { sabPageCache: sab });
  expect(directEnv.tidemark_page_cache_read(2, 2n, 32, 3)).toBe(3n);
  expect(Array.from(new Uint8Array(memory.buffer, 32, 3))).toEqual(Array.from(backing.subarray(2, 5)));
  expect(debugResidentPageCountForInode(sab, 2)).toBe(1);

  new Uint8Array(memory.buffer, 32, 3).fill(0);

  expect(directEnv.tidemark_page_cache_read(2, 2n, 32, 3)).toBe(3n);
  expect(Array.from(new Uint8Array(memory.buffer, 32, 3))).toEqual(Array.from(backing.subarray(2, 5)));

  publishInode(sab, 2, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
    fileObjectId: 2,
    generation: 2,
  });
  expect(debugResidentPageCountForInode(sab, 2)).toBe(0);
  expect(directEnv.tidemark_page_cache_read(2, 2n, 32, 3)).toBe(-2n);
});

test("page-cache clean resident eviction returns miss until status path refills", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 8192);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const backing = new TextEncoder().encode("abcdef");

  publishInode(sab, 2, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
    fileObjectId: 2,
    generation: 1,
  });

  expect(publishResidentPage(sab, 2, 0, backing)).toBe(true);
  const directEnv = createKernelEnv(memory, { sabPageCache: sab });

  expect(directEnv.tidemark_page_cache_read(2, 1n, 32, 4)).toBe(4n);
  expect(Array.from(new Uint8Array(memory.buffer, 32, 4))).toEqual(Array.from(backing.subarray(1, 5)));
  expect(debugResidentPageCountForInode(sab, 2)).toBe(1);

  expect(evictResidentPage(sab, 2, 0)).toBe("evicted");
  expect(debugResidentPageCountForInode(sab, 2)).toBe(0);

  new Uint8Array(memory.buffer, 32, 4).fill(0);
  const directMissEnv = createKernelEnv(memory, { sabPageCache: sab });

  expect(directMissEnv.tidemark_page_cache_read(2, 1n, 32, 4)).toBe(-2n);

  expect(publishResidentPage(sab, 2, 0, backing)).toBe(true);

  expect(directEnv.tidemark_page_cache_read(2, 1n, 32, 4)).toBe(4n);
  expect(Array.from(new Uint8Array(memory.buffer, 32, 4))).toEqual(Array.from(backing.subarray(1, 5)));
  expect(debugResidentPageCountForInode(sab, 2)).toBe(1);

  new Uint8Array(memory.buffer, 32, 4).fill(0);
  const directHitEnv = createKernelEnv(memory, { sabPageCache: sab });

  expect(directHitEnv.tidemark_page_cache_read(2, 1n, 32, 4)).toBe(4n);
  expect(Array.from(new Uint8Array(memory.buffer, 32, 4))).toEqual(Array.from(backing.subarray(1, 5)));
});

test("page-cache read can span resident pages filled by the status path", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 16384);
  const memory = new WebAssembly.Memory({ initial: 1 });
  const backing = new Uint8Array(6000);
  for (let offset = 0; offset < backing.length; offset += 1) {
    backing[offset] = (offset * 13 + 5) & 0xff;
  }

  publishInode(sab, 2, {
    mode: 0o100644,
    size: backing.length,
    parentIno: 1,
    name: new TextEncoder().encode("external"),
    nextSibling: 0,
    fileObjectId: 2,
    generation: 1,
  });

  expect(publishResidentPage(sab, 2, 0, backing.subarray(0, 4096))).toBe(true);
  expect(publishResidentPage(sab, 2, 1, backing.subarray(4096))).toBe(true);

  const directEnv = createKernelEnv(memory, { sabPageCache: sab });
  expect(directEnv.tidemark_page_cache_read(2, 512n, 64, 5000)).toBe(5000n);
  expect(Array.from(new Uint8Array(memory.buffer, 64, 5000))).toEqual(Array.from(backing.subarray(512, 5512)));
  expect(debugResidentPageCountForInode(sab, 2)).toBe(2);

  new Uint8Array(memory.buffer, 64, 5000).fill(0);

  expect(directEnv.tidemark_page_cache_read(2, 512n, 64, 5000)).toBe(5000n);
  expect(Array.from(new Uint8Array(memory.buffer, 64, 5000))).toEqual(Array.from(backing.subarray(512, 5512)));
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
  });

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

test("page-cache write publishes dirty resident data without updating backing data", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 8192);
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
  });

  const env = createKernelEnv(memory, {
    sabPageCache: sab,
    readFileDataByInode: (_ino, offset, length) => backing.subarray(offset, offset + length),
  });

  expect(env.tidemark_page_cache_write(2, 32n, 256, source.length)).toBe(BigInt(source.length));
  expect(env.tidemark_page_cache_read(2, 32n, 512, source.length)).toBe(BigInt(source.length));
  expect(new Uint8Array(memory.buffer, 512, source.length)).toEqual(source);
  expect(backing.slice(32, 32 + source.length)).toEqual(new Uint8Array(source.length));
  expect(evictResidentPage(sab, 2, 0)).toBe("dirty-pinned");

  expect(env.tidemark_page_cache_truncate(2, 40n)).toBe(0);
  new Uint8Array(memory.buffer, 512, source.length).fill(0);
  expect(env.tidemark_page_cache_read(2, 32n, 512, source.length)).toBe(8n);
  expect(new Uint8Array(memory.buffer, 512, 8)).toEqual(source.subarray(0, 8));
  expect(env.tidemark_page_cache_read(2, 40n, 512, 1)).toBe(0n);
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
  });

  const meta = readInodeMeta(sab, ino);
  const record = readInodeRecord(sab, ino);
  expect(meta).not.toBeNull();
  expect(record).not.toBeNull();
  expect(meta?.size).toBe(2);
  expect(record?.name).toEqual(name);
});

test("source-backed file replacement does not materialize the previous source", () => {
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

  const replacement = new TextEncoder().encode("XYZ");
  const replaced = backing.replace(file.fileObjectId, replacement);
  expect(replaced.size).toBe(replacement.length);
  expect(replaced.generation).toBe(file.generation + 1);
  expect(backing.read(file.fileObjectId, 0, 16)).toEqual(replacement);
  expect(calls).toEqual([]);
});

test("live resident records use file object generation and fixed reusable frames", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 4096 + 40);
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
  });

  expect(publishResidentPage(sab, 2, 0, backing.subarray(0, 4096))).toBe(true);

  const env = createKernelEnv(memory, { sabPageCache: sab });
  expect(env.tidemark_page_cache_read(2, 128n, 64, 16)).toBe(16n);
  expect(debugResidentPageCountForInode(sab, 2)).toBe(1);
  expect(new Uint8Array(memory.buffer, 64, 16)).toEqual(backing.slice(128, 144));

  expect(publishResidentPage(sab, 2, 1, backing.subarray(4096))).toBe(true);
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
  });
  expect(debugResidentPageCountForInode(sab, 2)).toBe(0);
  expect(directEnv.tidemark_page_cache_read(2, 4096n, 64, 16)).toBe(-2n);
});
