import { expect, test } from "vite-plus/test";
import type { KernelExports } from "../../../src/abi.js";
import {
  preloadSnapshotEntries,
  readMemfsFile,
  resolveMemfsPath,
} from "../../../src/worker/process-fs.js";
import { createKernelEnv } from "../../../src/wasm/env.js";
import { readKernelWasm } from "../../support/runtime-workload.js";

async function loadKernel(): Promise<KernelExports> {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    throw new Error("kernel wasm unavailable");
  }
  const module = await WebAssembly.compile(wasmBytes);
  const memory = new WebAssembly.Memory({
    initial: 1024,
    maximum: 32768,
    shared: true,
  });
  const instance = await WebAssembly.instantiate(module, {
    env: createKernelEnv(memory),
  });
  const exports = instance.exports as unknown as KernelExports;
  const heapBase = (exports as { __heap_base?: { value: number } }).__heap_base?.value ?? 0x200000;
  exports.kernel_init(heapBase);
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
});
