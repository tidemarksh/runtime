/**
 * Kernel WASM integration test.
 *
 * Loads the kernel .wasm, instantiates it directly (no Workers),
 * runs a minimal RISC-V ELF, and verifies exit code = 0.
 */

import { expect, test } from "vite-plus/test";
import { readStatusCodes } from "../../../src/abi.js";
import { createKernelEnv } from "../../../src/wasm/env.js";
import { findRiscvElf, logSkip, readKernelWasm } from "../../support/runtime-workload.js";

async function loadKernel(opts?: { memory?: WebAssembly.Memory }) {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return null;
  }
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

test("kernel WASM loads and exports constants", async () => {
  const loaded = await loadKernel();
  if (!loaded) {
    return;
  }
  const { exports } = loaded;
  const status = readStatusCodes(exports);

  expect(status.OK).toBe(0);
  expect(status.FAULT).toBe(3);
  expect(exports.abi_page_size()).toBe(4096);

  // kernel_init should not crash. heapBase must be >= __heap_base (kernel data end).
  const heapBase0 = (exports as any).__heap_base?.value ?? 0x200000;
  exports.kernel_init(heapBase0);
});

test("kernel_init + process_create + thread_step runs to exit", async () => {
  const loaded = await loadKernel();
  if (!loaded) {
    return;
  }
  const { exports, memory } = loaded;

  const elfData = findRiscvElf("add");
  if (!elfData) {
    logSkip("add.elf not found");
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
