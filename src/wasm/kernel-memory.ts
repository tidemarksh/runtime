import { createKernelEnv, type KernelEnvOptions } from "./env.js";

type KernelMemoryAddress = "i64" | "i32";

const MEMORY_GROW_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_MEMORY_GROW_DEBUG ?? "") === "1";
const MEMORY_GROW_DEBUG_THRESHOLD_BYTES = 64 * 1024 * 1024;
let nextMemoryGrowDebugId = 1;
const memoryGrowDebugIds = new WeakMap<WebAssembly.Memory, number>();
const memoryGrowDebugNextThresholds = new WeakMap<WebAssembly.Memory, number>();

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

function debugMemoryGrow(memory: WebAssembly.Memory, beforeBytes: number, afterBytes: number, deltaPages: number): void {
  if (!MEMORY_GROW_DEBUG_ENABLED) {
    return;
  }
  let nextThreshold = memoryGrowDebugNextThresholds.get(memory) ?? 0;
  if (afterBytes < nextThreshold) {
    return;
  }
  const id = memoryGrowDebugIds.get(memory) ?? nextMemoryGrowDebugId++;
  memoryGrowDebugIds.set(memory, id);
  nextThreshold = Math.ceil((afterBytes + 1) / MEMORY_GROW_DEBUG_THRESHOLD_BYTES)
    * MEMORY_GROW_DEBUG_THRESHOLD_BYTES;
  memoryGrowDebugNextThresholds.set(memory, nextThreshold);
  const stack = new Error().stack
    ?.split("\n")
    .slice(2, 6)
    .map((line) => line.trim())
    .join(" | ")
    ?? "n/a";
  console.error(
    `[tidemark-memory-grow] id=${id}:before=${beforeBytes}:after=${afterBytes}:deltaPages=${deltaPages}:stack=${stack}`,
  );
}

export function growKernelMemory(memory: WebAssembly.Memory, deltaPages: number): void {
  if (deltaPages <= 0) {
    return;
  }
  const beforeBytes = memory.buffer.byteLength;
  try {
    memory.grow(deltaPages);
  } catch (error) {
    if (!requiresBigIntMemoryDelta(error)) {
      throw error;
    }
    (memory.grow as unknown as (delta: bigint) => bigint)(BigInt(deltaPages));
  }
  debugMemoryGrow(memory, beforeBytes, memory.buffer.byteLength, deltaPages);
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
