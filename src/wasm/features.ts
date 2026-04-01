export interface WasmFeatureSupport {
  memory64: boolean;
  tailCalls: boolean;
  exceptionHandling: boolean;
}

const MEMORY64_MODULE = new Uint8Array([
  0, 97, 115, 109, 1, 0, 0, 0, 5, 3, 1, 4, 1,
]);

const TAIL_CALLS_MODULE = new Uint8Array([
  0, 97, 115, 109, 1, 0, 0, 0, 1, 4, 1, 96, 0, 0, 3, 3,
  2, 0, 0, 10, 9, 2, 2, 0, 11, 4, 0, 18, 0, 11, 0, 17,
  4, 110, 97, 109, 101, 1, 4, 1, 0, 1, 103, 4, 4, 1, 0, 1,
  116,
]);

const EXCEPTION_HANDLING_MODULE = new Uint8Array([
  0, 97, 115, 109, 1, 0, 0, 0, 1, 8, 2, 96, 1, 127, 0, 96,
  0, 0, 3, 2, 1, 1, 13, 3, 1, 0, 0, 10, 8, 1, 6, 0,
  65, 0, 8, 0, 11,
]);

function validate(bytes: Uint8Array): boolean {
  try {
    return WebAssembly.validate(bytes.slice().buffer);
  } catch {
    return false;
  }
}

export function detectWasmFeatures(): WasmFeatureSupport {
  return {
    memory64: validate(MEMORY64_MODULE),
    tailCalls: validate(TAIL_CALLS_MODULE),
    exceptionHandling: validate(EXCEPTION_HANDLING_MODULE)
      || typeof (WebAssembly as typeof WebAssembly & { Tag?: unknown }).Tag === "function",
  };
}
