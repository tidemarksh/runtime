import { expect, test } from "vite-plus/test";
import {
  DATA_REGION_OFFSET,
  publishInode,
  readInodeMeta,
  readInodeRecord,
} from "../../../src/fs/sab-page-cache.js";
import { createKernelEnv } from "../../../src/wasm/env.js";

test("publishInode falls back to metadata-only when file bytes exceed page-cache capacity", () => {
  const sab = new SharedArrayBuffer(DATA_REGION_OFFSET + 16);
  const name = new TextEncoder().encode("file");
  const fileData = new Uint8Array(64).fill(0x41);

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
  expect(meta?.dataOff).toBe(0);
  expect(record?.name).toEqual(name);
});

test("page-cache read reports miss for metadata-only file while stat still works", () => {
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
  expect(env.tidemark_page_cache_stat(2, 16)).toBe(1);

  const view = new DataView(memory.buffer);
  expect(view.getUint32(16, true)).toBe(0o100755);
  expect(view.getBigUint64(24, true)).toBe(64n);
});
