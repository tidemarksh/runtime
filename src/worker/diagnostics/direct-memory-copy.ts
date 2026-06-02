import type { KernelExports } from "../../abi.js";
import type { KernelSyncHandle, ProcessHandle } from "../types.js";

type DiagnosticHandle = Pick<KernelSyncHandle, "kernel" | "pid">
  & Partial<Pick<ProcessHandle, "guestBase" | "runtimeId" | "workerDebugLog">>;

const EXEC_PROT = 4;
const TRACE_CAP_PER_HANDLE = 128;
const traceCounts = new WeakMap<object, number>();
const decoder = new TextDecoder();
const DIRECT_MEMORY_COPY_DIAGNOSTICS_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_STRICT_GATE_DIAGNOSTICS ?? "") === "1"
  || ((globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_DIRECT_MEMORY_COPY_DEBUG ?? "") === "1";

function readVmaPath(kernel: KernelExports, index: number): string {
  const len = kernel.debug_memory_vma_path_len?.(index) ?? 0;
  if (len <= 0) {
    return "";
  }
  const bytes: number[] = [];
  for (let offset = 0; offset < Math.min(len, 160); offset += 1) {
    const byte = kernel.debug_memory_vma_path_byte?.(index, offset) ?? 0;
    if (byte === 0) {
      break;
    }
    bytes.push(byte & 0xff);
  }
  return decoder.decode(new Uint8Array(bytes));
}

function sampleHex(bytes: Uint8Array, offset: number, length: number): string {
  const end = Math.min(bytes.length, offset + length);
  return [...bytes.subarray(offset, end)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function asciiSample(bytes: Uint8Array, offset: number, length: number): string {
  const end = Math.min(bytes.length, offset + length);
  return [...bytes.subarray(offset, end)]
    .map((byte) => (byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : "."))
    .join("");
}

function sampleU32Le(bytes: Uint8Array, offset: number): number {
  let value = 0;
  const end = Math.min(bytes.length, offset + 4);
  for (let index = offset; index < end; index += 1) {
    value |= bytes[index] << ((index - offset) * 8);
  }
  return value >>> 0;
}

function pushTrace(handle: DiagnosticHandle, event: string): void {
  if (!handle.workerDebugLog) {
    return;
  }
  const current = traceCounts.get(handle as object) ?? 0;
  if (current >= TRACE_CAP_PER_HANDLE) {
    if (current === TRACE_CAP_PER_HANDLE) {
      handle.workerDebugLog.push(`direct-memory-copy-overlap-suppressed:pid=${handle.pid}`);
    }
    traceCounts.set(handle as object, current + 1);
    return;
  }
  traceCounts.set(handle as object, current + 1);
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
}

export function recordDirectMemoryCopyOverlap(
  handle: DiagnosticHandle,
  source: string,
  targetOffset: number,
  bytes: Uint8Array,
): void {
  if (!DIRECT_MEMORY_COPY_DIAGNOSTICS_ENABLED) {
    return;
  }
  if (!handle.workerDebugLog || handle.guestBase === undefined || bytes.length === 0) {
    return;
  }
  const guestPtr = targetOffset - handle.guestBase;
  if (!Number.isFinite(guestPtr) || guestPtr < 0) {
    return;
  }
  const writeStart = BigInt(guestPtr);
  const writeEnd = writeStart + BigInt(bytes.length);
  const vmaLen = handle.kernel.debug_memory_vma_len?.() ?? 0;
  for (let index = 0; index < vmaLen; index += 1) {
    const prot = handle.kernel.debug_memory_vma_prot?.(index) ?? 0;
    const vmaStart = BigInt(handle.kernel.debug_memory_vma_start?.(index) ?? 0n);
    const vmaEnd = BigInt(handle.kernel.debug_memory_vma_end?.(index) ?? 0n);
    if (vmaStart >= vmaEnd || writeEnd <= vmaStart || writeStart >= vmaEnd) {
      continue;
    }
    const overlapStart = writeStart > vmaStart ? writeStart : vmaStart;
    const overlapEnd = writeEnd < vmaEnd ? writeEnd : vmaEnd;
    const byteOffset = Number(overlapStart - writeStart);
    const copiedU32 = sampleU32Le(bytes, byteOffset);
    const fileSample = BigInt.asUintN(
      64,
      BigInt(handle.kernel.debug_memory_vma_file_sample?.(index, overlapStart) ?? 0),
    );
    const fileSampleLen = handle.kernel.debug_memory_vma_file_sample_len?.(index, overlapStart) ?? 0;
    const fileU32 = Number(fileSample & 0xffffffffn) >>> 0;
    const event = [
      "direct-memory-copy-vma-overlap",
      `pid=${handle.pid}`,
      `src=${source}`,
      `target=0x${targetOffset.toString(16)}`,
      `guest=0x${guestPtr.toString(16)}`,
      `len=${bytes.length}`,
      `overlap=0x${overlapStart.toString(16)}-0x${overlapEnd.toString(16)}`,
      `vma=0x${vmaStart.toString(16)}-0x${vmaEnd.toString(16)}`,
      `prot=${prot}`,
      `exec=${(prot & EXEC_PROT) !== 0 ? 1 : 0}`,
      `fd=${handle.kernel.debug_memory_vma_fd?.(index) ?? "n/a"}`,
      `off=0x${BigInt(handle.kernel.debug_memory_vma_offset?.(index) ?? 0).toString(16)}`,
      `path=${JSON.stringify(readVmaPath(handle.kernel, index))}`,
      `copied=0x${copiedU32.toString(16)}`,
      `file=0x${fileSample.toString(16)}/${fileSampleLen}`,
      `mismatch=${fileSampleLen > 0 && copiedU32 !== fileU32 ? 1 : 0}`,
      `sample=${sampleHex(bytes, byteOffset, 16)}`,
      `ascii=${JSON.stringify(asciiSample(bytes, byteOffset, 32))}`,
    ].join(":");
    pushTrace(handle, event);
  }
  const ownedLen = handle.kernel.debug_kernel_owned_guest_range_len?.() ?? 0;
  for (let index = 0; index < ownedLen; index += 1) {
    const ownedStart = BigInt(handle.kernel.debug_kernel_owned_guest_range_start?.(index) ?? 0n);
    const ownedEnd = BigInt(handle.kernel.debug_kernel_owned_guest_range_end?.(index) ?? 0n);
    if (ownedStart >= ownedEnd || writeEnd <= ownedStart || writeStart >= ownedEnd) {
      continue;
    }
    const overlapStart = writeStart > ownedStart ? writeStart : ownedStart;
    const overlapEnd = writeEnd < ownedEnd ? writeEnd : ownedEnd;
    const byteOffset = Number(overlapStart - writeStart);
    const event = [
      "direct-memory-copy-kernel-owned-overlap",
      `pid=${handle.pid}`,
      `src=${source}`,
      `target=0x${targetOffset.toString(16)}`,
      `guest=0x${guestPtr.toString(16)}`,
      `len=${bytes.length}`,
      `overlap=0x${overlapStart.toString(16)}-0x${overlapEnd.toString(16)}`,
      `owned=0x${ownedStart.toString(16)}-0x${ownedEnd.toString(16)}`,
      `sample=${sampleHex(bytes, byteOffset, 16)}`,
      `ascii=${JSON.stringify(asciiSample(bytes, byteOffset, 32))}`,
    ].join(":");
    pushTrace(handle, event);
  }
}
