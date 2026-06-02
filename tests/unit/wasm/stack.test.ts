import { expect, test } from "vite-plus/test";
import type { KernelExports } from "../../../src/abi.js";
import {
  configureKernelInstanceStack,
  KERNEL_WORKER_STACK_SLOT,
  kernelHeapBase,
  kernelStackTopForSlot,
} from "../../../src/wasm/stack.js";

test("configureKernelInstanceStack writes i32 stack pointer globals", () => {
  const stackPointer = new WebAssembly.Global({ value: "i32", mutable: true }, 0);

  configureKernelInstanceStack({
    __stack_pointer: stackPointer,
  } as unknown as KernelExports, KERNEL_WORKER_STACK_SLOT);

  expect(stackPointer.value).toBe(kernelStackTopForSlot(KERNEL_WORKER_STACK_SLOT));
});

test("configureKernelInstanceStack writes i64 stack pointer globals", () => {
  const stackPointer = new WebAssembly.Global({ value: "i64", mutable: true }, 0n);

  configureKernelInstanceStack({
    __stack_pointer: stackPointer,
  } as unknown as KernelExports, KERNEL_WORKER_STACK_SLOT);

  expect(stackPointer.value).toBe(BigInt(kernelStackTopForSlot(KERNEL_WORKER_STACK_SLOT)));
});

test("kernelHeapBase reads i32 and i64 heap globals as numbers", () => {
  expect(kernelHeapBase({
    __heap_base: new WebAssembly.Global({ value: "i32", mutable: false }, 0x200000),
  } as unknown as KernelExports, 0x100000)).toBe(0x200000);

  expect(kernelHeapBase({
    __heap_base: new WebAssembly.Global({ value: "i64", mutable: false }, 0x300000n),
  } as unknown as KernelExports, 0x100000)).toBe(0x300000);
});
