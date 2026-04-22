import { expect, test } from "vite-plus/test";
import {
  DATA_REGION_OFFSET,
  MAX_INODES,
  publishInode,
  readInodeMeta,
  readInodeRecord,
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
