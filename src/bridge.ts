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

export interface RuntimeBridgeDiagnostics {
  support: RuntimeBridgeSupport;
  requiresCrossOriginIsolation: boolean;
  sharedMemoryReady: boolean;
  opfsPersistentStorageReady: boolean;
  errors: string[];
  warnings: string[];
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

export function diagnoseBridgeSupport(env: Partial<BridgeEnv> = globalThis): RuntimeBridgeDiagnostics {
  const support = detectBridgeSupport(env);
  const needsCrossOriginIsolation = requiresCrossOriginIsolation(env);
  const errors: string[] = [];
  const warnings: string[] = [];

  if (!support.worker) {
    errors.push("Worker is unavailable in this environment");
  }
  if (!support.atomicsWait) {
    errors.push("Atomics.wait is unavailable in this environment");
  }
  if (!support.sharedArrayBuffer) {
    errors.push("SharedArrayBuffer is unavailable in this environment");
  }
  if (needsCrossOriginIsolation && !support.crossOriginIsolated) {
    errors.push("crossOriginIsolated is required for SharedArrayBuffer in browsers");
  }
  if (!support.opfs) {
    warnings.push("OPFS is unavailable; persistent storage is disabled");
  }

  return {
    support,
    requiresCrossOriginIsolation: needsCrossOriginIsolation,
    sharedMemoryReady: errors.length === 0,
    opfsPersistentStorageReady: support.opfs,
    errors,
    warnings,
  };
}

export function assertSharedMemoryBridge(env: Partial<BridgeEnv> = globalThis): void {
  const diagnostics = diagnoseBridgeSupport(env);
  if (diagnostics.errors.length > 0) {
    throw new Error(diagnostics.errors[0]);
  }
}
