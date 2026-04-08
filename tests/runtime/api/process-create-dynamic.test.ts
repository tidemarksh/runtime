import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, test } from "vite-plus/test";
import type { KernelExports } from "../../../src/abi.js";
import { createKernelEnv } from "../../../src/wasm/env.js";
import { preloadSnapshotEntries } from "../../../src/worker/process-fs.js";
import { buildDynamicStartupSnapshot } from "../../support/dynamic-startup-harness.js";
import { luaDynamicStartupEntry } from "../../workloads/lua/dynamic-startup.js";
import { perlDynamicStartupEntry } from "../../workloads/perl/dynamic-startup.js";
import { phpDynamicStartupEntry } from "../../workloads/php/dynamic-startup.js";

const KERNEL_WASM_PATH = resolve(
  __dirname,
  "../../../../kernel/target/wasm32-unknown-unknown/release/tidemark_kernel.wasm",
);

async function loadKernel() {
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
  const now = Date.now();
  exports.set_base_time(
    BigInt(Math.floor(now / 1000)),
    BigInt((now % 1000) * 1_000_000),
  );
  return { exports, memory };
}

type DynamicEntry = typeof luaDynamicStartupEntry;

function minimalInterpSnapshot(entry: DynamicEntry) {
  const interpSource = entry.snapshotSources.find(
    (source) => source.kind === "file" && source.guestPath === "/lib/ld-linux-riscv64-lp64d.so.1",
  );
  if (!interpSource || !existsSync(interpSource.hostPath)) {
    return null;
  }
  return [{
    kind: "file" as const,
    path: "/lib/ld-linux-riscv64-lp64d.so.1",
    data: new Uint8Array(readFileSync(interpSource.hostPath)),
    mode: 0o755,
  }];
}

async function runProcessCreate(entry: DynamicEntry, preloadMode: "minimal" | "full") {
  if (!existsSync(entry.hostExecutablePath)) {
    return null;
  }
  const { exports, memory } = await loadKernel();
  const ramSize = entry.ramSize;
  const snapshot = preloadMode === "minimal"
    ? minimalInterpSnapshot(entry)
    : buildDynamicStartupSnapshot(entry);
  if (!snapshot) {
    return null;
  }

  preloadSnapshotEntries(exports, snapshot);
  const elfData = new Uint8Array(readFileSync(entry.hostExecutablePath));
  const elfOffset = memory.buffer.byteLength - Math.max(elfData.length + 0x20_000, 0x40_000);
  new Uint8Array(memory.buffer).set(elfData, elfOffset);
  exports.process_clear_args();
  const encoder = new TextEncoder();
  let scratchPtr = 0x10_000;
  for (const arg of entry.argv) {
    const bytes = encoder.encode(`${arg}\0`);
    new Uint8Array(memory.buffer).set(bytes, scratchPtr);
    exports.process_push_arg(scratchPtr);
    scratchPtr += bytes.length + 16;
  }
  for (const env of entry.env) {
    const bytes = encoder.encode(`${env}\0`);
    new Uint8Array(memory.buffer).set(bytes, scratchPtr);
    exports.process_push_env(scratchPtr);
    scratchPtr += bytes.length + 16;
  }
  const vmPtr = exports.process_create(elfOffset, elfData.length, ramSize, 1024 * 1024);
  return {
    vmPtr,
    lastError: exports.process_create_last_error?.() ?? null,
  };
}

for (const entry of [luaDynamicStartupEntry, phpDynamicStartupEntry, perlDynamicStartupEntry] as const) {
  test(`kernel process_create minimal interp: ${entry.id}`, async () => {
    const result = await runProcessCreate(entry, "minimal");
    if (!result) {
      return;
    }
    expect(
      result.vmPtr,
      `process_create minimal failed: ${entry.id} lastError=${result.lastError ?? "<unavailable>"}`,
    ).toBeGreaterThan(0);
  });

  test(`kernel process_create full preload: ${entry.id}`, async () => {
    const result = await runProcessCreate(entry, "full");
    if (!result) {
      return;
    }
    expect(
      result.vmPtr,
      `process_create full failed: ${entry.id} lastError=${result.lastError ?? "<unavailable>"}`,
    ).toBeGreaterThan(0);
  });
}
