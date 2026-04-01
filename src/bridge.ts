import { detectWasmFeatures, type WasmFeatureSupport } from "./wasm/features.js";

export interface RuntimeBridgeSupport {
  worker: boolean;
  atomicsWait: boolean;
  sharedArrayBuffer: boolean;
  crossOriginIsolated: boolean;
  opfs: boolean;
  serviceWorker: boolean;
  wasm: WasmFeatureSupport;
}

type BridgeEnv = Pick<
  typeof globalThis,
  "Atomics" | "SharedArrayBuffer" | "Worker" | "crossOriginIsolated" | "navigator"
>;

function requiresCrossOriginIsolation(env: Partial<BridgeEnv>): boolean {
  return typeof env.crossOriginIsolated !== "undefined"
    || typeof env.navigator?.serviceWorker !== "undefined"
    || typeof env.navigator?.storage?.getDirectory === "function";
}

export function detectBridgeSupport(env: Partial<BridgeEnv> = globalThis): RuntimeBridgeSupport {
  const worker = typeof env.Worker === "function";
  const atomicsWait = typeof env.Atomics?.wait === "function";
  const sharedArrayBuffer = typeof env.SharedArrayBuffer === "function";
  const crossOriginIsolated = env.crossOriginIsolated === true;
  const opfs = typeof env.navigator?.storage?.getDirectory === "function";
  const serviceWorker = typeof env.navigator?.serviceWorker !== "undefined";

  return {
    worker,
    atomicsWait,
    sharedArrayBuffer,
    crossOriginIsolated,
    opfs,
    serviceWorker,
    wasm: detectWasmFeatures(),
  };
}

export function assertSharedMemoryBridge(env: Partial<BridgeEnv> = globalThis): void {
  const support = detectBridgeSupport(env);
  if (!support.worker) {
    throw new Error("Worker is unavailable in this environment");
  }
  if (!support.atomicsWait) {
    throw new Error("Atomics.wait is unavailable in this environment");
  }
  if (!support.sharedArrayBuffer) {
    throw new Error("SharedArrayBuffer is unavailable in this environment");
  }
  if (requiresCrossOriginIsolation(env) && !support.crossOriginIsolated) {
    throw new Error("crossOriginIsolated is required for SharedArrayBuffer in browsers");
  }
}
