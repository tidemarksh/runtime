// Thread-worker tests in this file validate session-boundary behavior.
import { expect, test } from "vite-plus/test";
import {
  shouldPrepareThreadWorker,
  type ThreadWorkerPrepareInputs,
} from "../../../src/thread-worker/session.js";

function makeInputs(
  overrides: Partial<ThreadWorkerPrepareInputs> = {},
): ThreadWorkerPrepareInputs {
  return {
    wasmModule: {} as WebAssembly.Module,
    memory: {} as WebAssembly.Memory,
    sabPageCache: null,
    ...overrides,
  };
}

test("shouldPrepareThreadWorker prepares the first attached worker instance", () => {
  expect(shouldPrepareThreadWorker(null, makeInputs())).toBe(true);
});

test("shouldPrepareThreadWorker skips re-instantiation when module, memory, and page cache are unchanged", () => {
  const inputs = makeInputs();
  expect(shouldPrepareThreadWorker(inputs, inputs)).toBe(false);
});

test("shouldPrepareThreadWorker re-instantiates for changed worker imports", () => {
  const current = makeInputs({
    wasmModule: {} as WebAssembly.Module,
    memory: {} as WebAssembly.Memory,
    sabPageCache: new SharedArrayBuffer(64),
  });

  expect(shouldPrepareThreadWorker(current, makeInputs({
    wasmModule: {} as WebAssembly.Module,
    memory: current.memory,
    sabPageCache: current.sabPageCache,
  }))).toBe(true);
  expect(shouldPrepareThreadWorker(current, makeInputs({
    wasmModule: current.wasmModule,
    memory: {} as WebAssembly.Memory,
    sabPageCache: current.sabPageCache,
  }))).toBe(true);
  expect(shouldPrepareThreadWorker(current, makeInputs({
    wasmModule: current.wasmModule,
    memory: current.memory,
    sabPageCache: null,
  }))).toBe(true);
});
