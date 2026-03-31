export interface KernelEnvImports {
  [key: string]: WebAssembly.ImportValue;
  memory: WebAssembly.Memory;
  tidemark_memory_atomic_notify(ptr: number, maxWake: number): number;
}

export function createKernelEnv(memory: WebAssembly.Memory): KernelEnvImports {
  return {
    memory,
    tidemark_memory_atomic_notify(ptr, maxWake) {
      return Atomics.notify(new Int32Array(memory.buffer), ptr >>> 2, maxWake);
    },
  };
}
