import type { KernelExports } from "../abi.js";

export function alignUp(value: number, align: number): number {
  return (value + align - 1) & ~(align - 1);
}

export function ensureMemoryCapacity(memory: WebAssembly.Memory, endOffset: number): void {
  if (endOffset <= memory.buffer.byteLength) {
    return;
  }
  const requiredPages = Math.ceil(endOffset / 65536);
  const currentPages = memory.buffer.byteLength / 65536;
  memory.grow(requiredPages - currentPages);
}

export function kernelScratchBase(exports: KernelExports): number {
  return exports.kernel_heap_ptr() + 0x10000;
}

export function ensureKernelMemoryCapacity(exports: KernelExports, endOffset: number): void {
  ensureMemoryCapacity(exports.memory, endOffset);
}

export function writeKernelScratch(exports: KernelExports, bytes: Uint8Array, offset = 0): number {
  const scratch = kernelScratchBase(exports) + offset;
  ensureKernelMemoryCapacity(exports, scratch + bytes.length);
  new Uint8Array(exports.memory.buffer).set(bytes, scratch);
  return scratch;
}

export function detachedKernelScratchBase(
  memory: WebAssembly.Memory,
  guestBase: number,
  guestUsedBytes: number,
): number {
  return alignUp(Math.max(memory.buffer.byteLength, guestBase + guestUsedBytes) + 0x10000, 16);
}

export function writeDetachedKernelScratch(
  exports: KernelExports,
  scratchBase: number,
  bytes: Uint8Array,
  offset = 0,
): number {
  const scratch = scratchBase + offset;
  ensureKernelMemoryCapacity(exports, scratch + bytes.length);
  new Uint8Array(exports.memory.buffer).set(bytes, scratch);
  return scratch;
}

export function copyKernelPrivateRegion(
  source: WebAssembly.Memory,
  target: WebAssembly.Memory,
  endOffset: number,
): void {
  ensureMemoryCapacity(target, endOffset);
  new Uint8Array(target.buffer).set(new Uint8Array(source.buffer, 0, endOffset), 0);
}

export function copyLinearMemoryImage(
  source: WebAssembly.Memory,
  target: WebAssembly.Memory,
  byteLength = source.buffer.byteLength,
): void {
  ensureMemoryCapacity(target, byteLength);
  new Uint8Array(target.buffer).set(new Uint8Array(source.buffer, 0, byteLength), 0);
}
