import { expect, test } from "vitest";
import type { KernelExports } from "../../../src/abi.js";
import { scratchBaseFor } from "../../../src/kernel-worker/common.js";

test("scratchBaseFor stays past MemFS metadata regions", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 16, shared: true });
  const exports = {
    memory,
    kernel_heap_ptr: () => 0x1000,
    kernel_memfs_ptr: () => 0x20000,
    kernel_memfs_header_bytes: () => 32,
    kernel_memfs_inodes_ptr: () => 0x21000,
    kernel_memfs_inodes_bytes: () => 0x4000,
    kernel_memfs_first_child_ptr: () => 0x25000,
    kernel_memfs_first_child_bytes: () => 0x1000,
    kernel_memfs_dir_entries_ptr: () => 0x26000,
    kernel_memfs_dir_entries_bytes: () => 0x8000,
    kernel_memfs_data_ptr: () => 0x18000,
    kernel_memfs_data_bytes: () => 0x1000,
  } as unknown as KernelExports;

  expect(scratchBaseFor(exports)).toBeGreaterThanOrEqual(0x2e000 + 0x10000);
});
