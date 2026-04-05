import { expect, test } from "vite-plus/test";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { KernelExports } from "../../../src/abi.js";
import {
  preloadSnapshotEntries,
  readMemfsFile,
  resolveMemfsPath,
} from "../../../src/worker/process-fs.js";
import { createKernelEnv } from "../../../src/wasm/env.js";

const KERNEL_WASM_PATH = resolve(
  __dirname,
  "../../../../kernel/target/wasm32-unknown-unknown/release/tidemark_kernel.wasm",
);

async function loadKernel(): Promise<KernelExports> {
  const wasmBytes = readFileSync(KERNEL_WASM_PATH);
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
  if (!existsSync(KERNEL_WASM_PATH)) {
    console.log("skip: tidemark_kernel.wasm not found (build kernel release wasm first)");
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
