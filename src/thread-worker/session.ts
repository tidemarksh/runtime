export interface ThreadWorkerPrepareInputs {
  wasmModule: WebAssembly.Module;
  memory: WebAssembly.Memory;
  sabPageCache: SharedArrayBuffer | null;
}

export function shouldPrepareThreadWorker(
  current: ThreadWorkerPrepareInputs | null,
  next: ThreadWorkerPrepareInputs,
): boolean {
  if (current === null) {
    return true;
  }
  return current.wasmModule !== next.wasmModule
    || current.memory !== next.memory
    || current.sabPageCache !== next.sabPageCache;
}
