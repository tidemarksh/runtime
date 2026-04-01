import { expect, test } from "vite-plus/test";
import { assertSharedMemoryBridge, detectBridgeSupport } from "../../../src/bridge.js";

const FakeWorker = (() => {}) as unknown as typeof Worker;

test("assertSharedMemoryBridge does not require crossOriginIsolated in Node-like environments", () => {
  const env = {
    Worker: FakeWorker,
    Atomics,
    SharedArrayBuffer,
    navigator: {},
  } as const;

  expect(detectBridgeSupport(env).sharedArrayBuffer).toBe(true);
  expect(() => assertSharedMemoryBridge(env)).not.toThrow();
});

test("assertSharedMemoryBridge requires crossOriginIsolated in browser-like environments", () => {
  const env = {
    Worker: FakeWorker,
    Atomics,
    SharedArrayBuffer,
    crossOriginIsolated: false,
    navigator: {
      serviceWorker: {},
      storage: {
        getDirectory: () => Promise.resolve({}),
      },
    },
  } as const;

  expect(() => assertSharedMemoryBridge(env)).toThrow(
    "crossOriginIsolated is required for SharedArrayBuffer in browsers",
  );
});
