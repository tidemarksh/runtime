/**
 * Kernel WASM integration test.
 *
 * Loads the kernel .wasm, instantiates it directly (no Workers),
 * runs a minimal RISC-V ELF, and verifies exit code = 0.
 */

import { expect, test } from "vite-plus/test";
import { readFileSync, readdirSync, existsSync } from "node:fs";
import { resolve } from "node:path";
import { readStatusCodes } from "../src/abi.js";
import { createKernelEnv } from "../src/wasm-env.js";

const KERNEL_WASM_PATH = resolve(
  __dirname,
  "../../kernel/target/wasm32-unknown-unknown/release/tidemark_kernel.wasm",
);

async function loadKernel(opts?: { memory?: WebAssembly.Memory }) {
  const wasmBytes = readFileSync(KERNEL_WASM_PATH);
  const module = await WebAssembly.compile(wasmBytes);
  // Kernel WASM imports env.memory. Kernel data at 48MB (--global-base),
  // so memory must be at least 48MB + kernel data (~4KB) = ~49MB.
  // Use 64MB (1024 pages) as a safe default.
  const memory = opts?.memory ?? new WebAssembly.Memory({
    initial: 1024,   // 64 MB
    maximum: 32768,  // 2 GB
    shared: true,
  });
  const instance = await WebAssembly.instantiate(module, {
    env: createKernelEnv(memory),
  });
  const exports = instance.exports as unknown as KernelExports;
  return { exports, memory, module };
}

function findAddElf(): Uint8Array | null {
  const buildDir = resolve(
    __dirname,
    "../../kernel/target/wasm32-wasip1/debug/build",
  );
  if (!existsSync(buildDir)) return null;

  for (const entry of readdirSync(buildDir)) {
    const addElf = resolve(buildDir, entry, "out/rv64ui/add.elf");
    if (existsSync(addElf)) {
      return new Uint8Array(readFileSync(addElf));
    }
  }
  return null;
}

test("kernel WASM loads and exports constants", async () => {
  const { exports } = await loadKernel();
  const status = readStatusCodes(exports);

  expect(status.OK).toBe(0);
  expect(status.FAULT).toBe(3);
  expect(exports.abi_page_size()).toBe(4096);

  // kernel_init should not crash. heapBase must be >= __heap_base (kernel data end).
  const heapBase0 = (exports as any).__heap_base?.value ?? 0x200000;
  exports.kernel_init(heapBase0);
});

test("kernel_init + process_create + thread_step runs to exit", async () => {
  const { exports, memory } = await loadKernel();

  const elfData = findAddElf();
  if (!elfData) {
    console.log("skip: add.elf not found (run kernel tests first)");
    return;
  }

  // heapBase must be >= __heap_base (kernel data segments end at ~48MB)
  const heapBase1 = (exports as any).__heap_base?.value ?? 0x200000;
  exports.kernel_init(heapBase1);

  // Copy ELF to WASM memory (re-read buffer after potential grow)
  const ramSize = memory.buffer.byteLength;
  const elfOffset = ramSize - 256 * 1024;
  const memView = new Uint8Array(memory.buffer);
  memView.set(elfData, elfOffset);

  const vmPtr = exports.process_create(elfOffset, elfData.length, ramSize, 1024 * 1024);
  expect(vmPtr).toBeGreaterThan(0);

  const STATUS_OK = exports.abi_status_ok();
  const STATUS_FAULT = exports.abi_status_fault();
  let totalSteps = 0;

  while (totalSteps < 50_000_000) {
    const remaining = exports.thread_step(vmPtr, 500_000);
    totalSteps += 500_000 - remaining;
    const s = exports.thread_status(vmPtr);

    if (s === STATUS_FAULT) {
      expect(exports.thread_exit_code(vmPtr)).toBe(0);
      return;
    }
    if (s !== STATUS_OK) break;
  }

  throw new Error(`Did not exit within ${totalSteps} instructions`);
});
