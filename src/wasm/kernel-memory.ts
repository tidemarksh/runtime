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

function requiresBigIntMemoryDelta(error: unknown): boolean {
  return error instanceof Error && /BigInt/i.test(error.message);
}

function memoryAddressAttemptOrder(sizing: KernelSharedMemorySizing): KernelMemoryAddress[] {
  const maximum = normalizePageCount(sizing.maximumPages);
  return maximum > 65536 ? ["i64", "i32"] : ["i32", "i64"];
}

export function growKernelMemory(memory: WebAssembly.Memory, deltaPages: number): void {
  if (deltaPages <= 0) {
    return;
  }
  try {
    memory.grow(deltaPages);
  } catch (error) {
    if (!requiresBigIntMemoryDelta(error)) {
      throw error;
    }
    (memory.grow as unknown as (delta: bigint) => bigint)(BigInt(deltaPages));
  }
}

export async function instantiateKernelWithSharedMemory(
  module: WebAssembly.Module,
  sizing: KernelSharedMemorySizing,
  options: KernelEnvOptions = {},
  prepareMemory?: (memory: WebAssembly.Memory) => void,
): Promise<KernelSharedMemoryInstance> {
  let firstMismatchError: unknown;
  let firstAddress: KernelMemoryAddress | null = null;
  for (const address of memoryAddressAttemptOrder(sizing)) {
    try {
      const memory = createSharedKernelMemory(sizing, address);
      prepareMemory?.(memory);
      const instance = await WebAssembly.instantiate(module, {
        env: createKernelEnv(memory, options),
      });
      return { instance, memory };
    } catch (error) {
      if (!isMemoryAddressMismatch(error)) {
        throw error;
      }
      if (firstMismatchError === undefined) {
        firstMismatchError = error;
        firstAddress = address;
      }
    }
  }

  const error = firstMismatchError instanceof Error
    ? firstMismatchError
    : new Error("kernel memory address mismatch");
  if (firstAddress !== null) {
    error.message = `${error.message}; first memory address attempt: ${firstAddress}`;
  }
  throw error;
}
