import { expect, test } from "vite-plus/test";
import {
  assertSharedMemoryBridge,
  detectBridgeSupport,
  diagnoseBridgeSupport,
} from "../../../src/bridge.js";

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
  expect(diagnoseBridgeSupport(env)).toMatchObject({
    requiresCrossOriginIsolation: false,
    sharedMemoryReady: true,
    opfsPersistentStorageReady: false,
    errors: [],
    warnings: ["OPFS is unavailable; persistent storage is disabled"],
  });
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
  expect(diagnoseBridgeSupport(env)).toMatchObject({
    requiresCrossOriginIsolation: true,
    sharedMemoryReady: false,
    opfsPersistentStorageReady: true,
    errors: ["crossOriginIsolated is required for SharedArrayBuffer in browsers"],
    warnings: [],
  });
});

test("diagnoseBridgeSupport reports missing Worker and SharedArrayBuffer independently", () => {
  const env = {
    Atomics: {},
    navigator: {},
  } as const;

  expect(diagnoseBridgeSupport(env)).toMatchObject({
    requiresCrossOriginIsolation: false,
    sharedMemoryReady: false,
    opfsPersistentStorageReady: false,
    errors: [
      "Worker is unavailable in this environment",
      "Atomics.wait is unavailable in this environment",
      "SharedArrayBuffer is unavailable in this environment",
    ],
    warnings: ["OPFS is unavailable; persistent storage is disabled"],
  });
});
