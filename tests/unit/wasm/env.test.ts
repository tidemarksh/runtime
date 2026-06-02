import { test, expect } from "vitest";
import { createKernelEnv } from "../../../src/wasm/env.js";

test("tidemark_memory_atomic_notify accepts Memory64 bigint arguments", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const env = createKernelEnv(memory);
  const view = new Int32Array(memory.buffer);

  view[4] = 1;

  expect(env.tidemark_memory_atomic_notify(16n, 1n)).toBe(0);
});
