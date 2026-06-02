import { expect, test } from "vite-plus/test";
import { growKernelMemory } from "../../../src/wasm/kernel-memory.js";

test("growKernelMemory grows i32 shared memory", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 4, shared: true });

  growKernelMemory(memory, 2);

  expect(memory.buffer.byteLength).toBe(3 * 65536);
});

test("growKernelMemory grows i64 shared memory", () => {
  const memory = new WebAssembly.Memory({
    initial: 1n,
    maximum: 4n,
    shared: true,
    address: "i64",
  } as unknown as WebAssembly.MemoryDescriptor);

  growKernelMemory(memory, 2);

  expect(memory.buffer.byteLength).toBe(3 * 65536);
});
