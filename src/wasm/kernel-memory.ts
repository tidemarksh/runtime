import { createKernelEnv, type KernelEnvOptions } from "./env.js";

type KernelMemoryAddress = "i64" | "i32";

export interface KernelSharedMemorySizing {
  initialPages: number;
  maximumPages: number;
}

export interface KernelSharedMemoryInstance {
  instance: WebAssembly.Instance;
  memory: WebAssembly.Memory;
}

function normalizePageCount(value: number): number {
  if (!Number.isFinite(value)) {
    return 1;
  }
  return Math.max(Math.trunc(value), 1);
}

function createSharedKernelMemory(
  sizing: KernelSharedMemorySizing,
  address: KernelMemoryAddress,
): WebAssembly.Memory {
  const initial = normalizePageCount(sizing.initialPages);
  const maximum = Math.max(normalizePageCount(sizing.maximumPages), initial);
  if (address === "i64") {
    return new WebAssembly.Memory({
      initial: BigInt(initial),
      maximum: BigInt(maximum),
      shared: true,
      address: "i64",
    } as unknown as WebAssembly.MemoryDescriptor);
  }
  return new WebAssembly.Memory({
    initial,
    maximum,
    shared: true,
  });
}

function isMemoryAddressMismatch(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  return /cannot import i64 memory as i32|cannot import i32 memory as i64/i.test(error.message);
}

export async function instantiateKernelWithSharedMemory(
  module: WebAssembly.Module,
  sizing: KernelSharedMemorySizing,
  options: KernelEnvOptions = {},
  prepareMemory?: (memory: WebAssembly.Memory) => void,
): Promise<KernelSharedMemoryInstance> {
  let i32Error: unknown;
  try {
    const memory = createSharedKernelMemory(sizing, "i32");
    prepareMemory?.(memory);
    const instance = await WebAssembly.instantiate(module, {
      env: createKernelEnv(memory, options),
    });
    return { instance, memory };
  } catch (error) {
    if (!isMemoryAddressMismatch(error)) {
      throw error;
    }
    i32Error = error;
  }

  try {
    const memory = createSharedKernelMemory(sizing, "i64");
    prepareMemory?.(memory);
    const instance = await WebAssembly.instantiate(module, {
      env: createKernelEnv(memory, options),
    });
    return { instance, memory };
  } catch (error) {
    if (i32Error instanceof Error && error instanceof Error) {
      error.message = `${error.message}; i32 memory attempt: ${i32Error.message}`;
    }
    throw error;
  }
}
