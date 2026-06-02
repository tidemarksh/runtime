import type { KernelExports } from "../abi.js";

const KERNEL_STACK_WINDOW_BYTES = 1024 * 1024;
const KERNEL_STACK_TOP_BASE = KERNEL_STACK_WINDOW_BYTES;
const MAX_KERNEL_STACK_SLOTS = 32;

type KernelExportsWithStackGlobal = KernelExports & {
  __stack_pointer?: WebAssembly.Global;
};

export const MAIN_THREAD_KERNEL_STACK_SLOT = 0;
export const KERNEL_WORKER_STACK_SLOT = 1;
export const THREAD_WORKER_KERNEL_STACK_SLOT_BASE = 2;

export function kernelStackTopForSlot(slot: number): number {
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_KERNEL_STACK_SLOTS) {
    throw new Error(`invalid kernel stack slot: ${slot}`);
  }
  return KERNEL_STACK_TOP_BASE + slot * KERNEL_STACK_WINDOW_BYTES;
}

export function kernelHeapBase(exports: KernelExports, fallback: number): number {
  const value = exports.__heap_base?.value;
  if (typeof value === "bigint") {
    const normalized = Number(value);
    if (!Number.isSafeInteger(normalized)) {
      throw new Error(`kernel __heap_base is outside JS safe integer range: ${value}`);
    }
    return normalized;
  }
  return typeof value === "number" ? value : fallback;
}

export function configureKernelInstanceStack(exports: KernelExports, slot: number): void {
  const stackPointer = (exports as KernelExportsWithStackGlobal).__stack_pointer;
  if (!(stackPointer instanceof WebAssembly.Global)) {
    throw new Error("kernel wasm export __stack_pointer is unavailable");
  }
  const stackTop = kernelStackTopForSlot(slot);
  if (typeof stackPointer.value === "bigint") {
    (stackPointer as WebAssembly.Global & { value: bigint }).value = BigInt(stackTop);
    return;
  }
  (stackPointer as WebAssembly.Global & { value: number }).value = stackTop;
}
