import { expect, test } from "vite-plus/test";
import type { KernelExports } from "../../../src/abi.js";
import {
  preloadSnapshotEntries,
  readMemfsFile,
  resolveMemfsPath,
} from "../../../src/worker/process-fs.js";
import { instantiateKernelWithSharedMemory } from "../../../src/wasm/kernel-memory.js";
import { createDiagnosticKernelPageCache } from "../../support/kernel-page-cache-env.js";
import { readKernelWasm } from "../../support/runtime-workload.js";

async function loadKernel(): Promise<KernelExports> {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    throw new Error("kernel wasm unavailable");
  }
  const module = await WebAssembly.compile(wasmBytes);
  const pageCache = createDiagnosticKernelPageCache();
  const { instance } = await instantiateKernelWithSharedMemory(module, {
    initialPages: 1024,
    maximumPages: 32768,
  }, pageCache.envOptions);
  const exports = instance.exports as unknown as KernelExports;
  const heapBase = (exports as { __heap_base?: { value: number | bigint } }).__heap_base?.value ?? 0x200000;
  exports.kernel_init(Number(heapBase));
  return exports;
}

test("preloadSnapshotEntries round-trips large file bytes with the real kernel memfs", async () => {
  if (!readKernelWasm()) {
    return;
  }

  const exports = await loadKernel();
  const data = new Uint8Array(1024 * 1024).map((_, index) => (index * 17) & 0xff);

  preloadSnapshotEntries(exports, [
    { kind: "dir", path: "/bin", mode: 0o755 },
    { kind: "file", path: "/bin/large.bin", data, mode: 0o755 },
  ]);

  const ino = resolveMemfsPath(exports, "/bin/large.bin");
  expect(ino).toBeGreaterThan(1);
  expect(readMemfsFile(exports, ino)).toEqual(data);
}, 20_000);

test("preloadSnapshotEntries preserves dynamic interpreter file size with the real kernel memfs", async () => {
  if (!readKernelWasm()) {
    return;
  }

  const exports = await loadKernel();
  const interpreter = new Uint8Array(4096).map((_, index) => (index * 31) & 0xff);

  preloadSnapshotEntries(exports, [
    { kind: "file", path: "/lib/ld-linux-riscv64-lp64d.so.1", data: interpreter, mode: 0o755 },
  ]);

  const ino = resolveMemfsPath(exports, "/lib/ld-linux-riscv64-lp64d.so.1");
  expect(ino).toBeGreaterThan(1);
  expect(Number(exports.memfs_file_size(ino))).toBe(interpreter.length);
  expect(readMemfsFile(exports, ino)).toEqual(interpreter);
}, 20_000);
