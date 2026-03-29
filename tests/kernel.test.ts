/**
 * Kernel WASM integration test.
 *
 * Loads the kernel .wasm, instantiates it directly (no Workers),
 * runs a minimal RISC-V ELF, and verifies exit code = 0.
 */

import { expect, test } from "vite-plus/test";
import { readFileSync, readdirSync, existsSync } from "node:fs";
import { resolve } from "node:path";
import type { KernelExports } from "../src/abi.js";
import { readStatusCodes } from "../src/abi.js";

const KERNEL_WASM_PATH = resolve(
  __dirname,
  "../../kernel/target/wasm32-unknown-unknown/release/tidemark_kernel.wasm",
);

async function loadKernel(opts?: { memory?: WebAssembly.Memory }) {
  const wasmBytes = readFileSync(KERNEL_WASM_PATH);
  const module = await WebAssembly.compile(wasmBytes);
  // Kernel WASM imports env.memory (shared). Create a default if not provided.
  const memory = opts?.memory ?? new WebAssembly.Memory({
    initial: 256,    // 16 MB
    maximum: 32768,  // 2 GB
    shared: true,
  });
  const instance = await WebAssembly.instantiate(module, {
    env: { memory },
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

  // kernel_init should not crash
  exports.kernel_init(0x200000);
});

test("kernel_init + process_create + thread_step runs to exit", async () => {
  const { exports, memory } = await loadKernel();

  const elfData = findAddElf();
  if (!elfData) {
    console.log("skip: add.elf not found (run kernel tests first)");
    return;
  }

  // Grow memory to 16 MB before init
  const targetPages = 256; // 16 MB
  const currentPages = memory.buffer.byteLength / 65536;
  if (currentPages < targetPages) {
    memory.grow(targetPages - currentPages);
  }

  exports.kernel_init(0x200000);

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

test("go version (GOMAXPROCS=1)", async () => {
  const goPath = resolve(__dirname, "../../core/images/go");
  if (!existsSync(goPath)) {
    console.log("skip: Go binary not found at", goPath);
    return;
  }

  const elfData = new Uint8Array(readFileSync(goPath));

  // Go binary's BSS extends to ~45MB. ELF file data is ~19MB. Need:
  //   guest segments (45MB) + ELF staging (19MB) + kernel heap (4MB) + margin
  // Use 1280 pages (80 MB).
  const memory = new WebAssembly.Memory({
    initial: 1280,
    maximum: 32768,
    shared: true,
  });
  const { exports } = await loadKernel({ memory });

  const ramSize = memory.buffer.byteLength;

  // Memory layout for Go (BSS extends to ~45MB):
  //   [guest segments 0..45MB] [gap] [ELF file + argv] [kernel heap] [stack]
  // Need heap above BSS end. Use ramSize - 4MB for heap, place ELF just below.
  // Stack lives at ramSize - 4096 (set by process_create).
  const heapBase = ramSize - 4 * 1024 * 1024;
  exports.kernel_init(heapBase);

  // Place ELF file data above Go's BSS end (~45MB) but below kernel heap
  // BSS end ≈ 0x2C40000 (45MB). ELF size ≈ 19MB. Need 19MB of space.
  // elfOffset must be > 45MB and elfOffset + elfSize < heapBase.
  const elfOffset = (heapBase - elfData.length - 65536) & ~0xFFF; // page-align
  if (elfOffset < 0x2D00000) {
    console.log(`skip: not enough memory for ELF placement (need > 45MB, got ${elfOffset})`);
    return;
  }
  console.log(`ramSize=${ramSize} (${ramSize/1024/1024}MB), elfSize=${elfData.length}, elfOffset=${elfOffset} (0x${elfOffset.toString(16)}), heapBase=0x${heapBase.toString(16)}`);
  const memView = new Uint8Array(memory.buffer);
  memView.set(elfData, elfOffset);

  // Write argv/envp between ELF data and BSS end (safe zone above 45MB)
  const encoder = new TextEncoder();
  let strOffset = elfOffset - 4096; // just below ELF file data, above BSS end
  function writeGuestStr(s: string): number {
    const bytes = encoder.encode(s + "\0");
    memView.set(bytes, strOffset);
    const addr = strOffset;
    strOffset += bytes.length;
    return addr;
  }

  exports.process_clear_args();
  exports.process_push_arg(writeGuestStr("go"));
  exports.process_push_arg(writeGuestStr("version"));
  exports.process_push_env(writeGuestStr("GOMAXPROCS=1"));

  // Debug: check ELF validity before full process_create
  const debugInspect = (exports as any).debug_inspect_elf(elfOffset, elfData.length, ramSize) as bigint;
  console.log(`debug_inspect_elf: ${debugInspect} (0x${debugInspect.toString(16)})`);
  // Also verify ELF magic is intact at elfOffset
  console.log(`ELF magic at elfOffset: ${Array.from(memView.slice(elfOffset, elfOffset+4)).map(b => b.toString(16)).join(' ')}`);

  const vmPtr = exports.process_create(elfOffset, elfData.length, ramSize, 4 * 1024 * 1024);
  console.log(`process_create returned: ${vmPtr} (0x${vmPtr.toString(16)})`);
  expect(vmPtr).toBeGreaterThan(0);

  const STATUS_OK = exports.abi_status_ok();
  const STATUS_FAULT = exports.abi_status_fault();
  const STATUS_SLEEP = exports.abi_status_sleep();

  const BUDGET_PER_STEP = 1_000_000;
  const MAX_INSTRUCTIONS = 500_000_000;
  let totalSteps = 0;

  while (totalSteps < MAX_INSTRUCTIONS) {
    const remaining = exports.thread_step(vmPtr, BUDGET_PER_STEP);
    totalSteps += BUDGET_PER_STEP - remaining;
    const s = exports.thread_status(vmPtr);

    if (s === STATUS_FAULT) {
      const code = exports.thread_exit_code(vmPtr);
      expect(code).toBe(0);
      console.log(`go version exited with code ${code} after ${totalSteps} instructions`);
      return;
    }

    // STATUS_OK, STATUS_SLEEP, and others (FS, FUTEX_WAIT, etc.): continue stepping
  }

  throw new Error(`go version did not exit within ${totalSteps} instructions`);
});
