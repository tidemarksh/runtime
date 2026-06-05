import {
  readInodeMeta,
  readCleanResidentFileData,
} from "../fs/sab-page-cache.js";

export interface KernelEnvImports {
  [key: string]: WebAssembly.ImportValue;
  memory: WebAssembly.Memory;
  tidemark_memory_atomic_notify(ptr: number | bigint, maxWake: number | bigint): number;
  tidemark_page_cache_read(ino: number, offset: bigint, dstPtr: number | bigint, dstLen: number): bigint;
  tidemark_page_cache_stat(ino: number, outPtr: number | bigint): number;
  tidemark_page_cache_flush(ino: number): number;
  tidemark_page_cache_truncate(ino: number, size: bigint): number;
  tidemark_page_cache_write(ino: number, offset: bigint, srcPtr: number | bigint, srcLen: number): bigint;
}

export interface KernelEnvOptions {
  sabPageCache?: SharedArrayBuffer | null;
  flushResidentFileDataByInode?: (ino: number) => boolean;
  truncateResidentFileDataByInode?: (ino: number, size: number) => number;
  writeResidentFileDataByInode?: (ino: number, offset: number, data: Uint8Array) => number;
}

function normalizeWasmPointer(ptr: number | bigint): number | null {
  if (typeof ptr === "bigint") {
    if (ptr < 0n || ptr > BigInt(Number.MAX_SAFE_INTEGER)) {
      return null;
    }
    return Number(ptr);
  }
  if (!Number.isSafeInteger(ptr) || ptr < 0) {
    return null;
  }
  return ptr;
}

function normalizeMemoryRange(
  memory: WebAssembly.Memory,
  ptr: number | bigint,
  length: number,
): { ptr: number; length: number } | null {
  if (!Number.isSafeInteger(length) || length < 0) {
    return null;
  }
  const ptrNumber = normalizeWasmPointer(ptr);
  if (ptrNumber === null) {
    return null;
  }
  const end = ptrNumber + length;
  if (!Number.isSafeInteger(end) || end > memory.buffer.byteLength) {
    return null;
  }
  return { ptr: ptrNumber, length };
}

export function createKernelEnv(
  memory: WebAssembly.Memory,
  options: KernelEnvOptions = {},
): KernelEnvImports {
  const sabPageCache = options.sabPageCache ?? null;
  const flushResidentFileDataByInode = options.flushResidentFileDataByInode;
  const truncateResidentFileDataByInode = options.truncateResidentFileDataByInode;
  const writeResidentFileDataByInode = options.writeResidentFileDataByInode;
  return {
    memory,
    tidemark_memory_atomic_notify(ptr, maxWake) {
      const ptrNumber = typeof ptr === "bigint" ? Number(ptr) : ptr;
      const maxWakeNumber = typeof maxWake === "bigint" ? Number(maxWake) : maxWake;
      const i32 = new Int32Array(memory.buffer);
      const index = Math.trunc(ptrNumber / Int32Array.BYTES_PER_ELEMENT);
      if (
        !Number.isSafeInteger(ptrNumber)
        || !Number.isFinite(maxWakeNumber)
        || index < 0
        || index >= i32.length
      ) {
        return 0;
      }
      return Atomics.notify(i32, index, Math.max(0, Math.trunc(maxWakeNumber)));
    },
    tidemark_page_cache_read(ino, offset, dstPtr, dstLen) {
      if (dstLen <= 0) {
        return 0n;
      }
      const offsetNumber = Number(offset);
      if (!Number.isFinite(offsetNumber) || offsetNumber < 0) {
        return -22n;
      }
      if (!sabPageCache) {
        return -2n;
      }
      const meta = readInodeMeta(sabPageCache, ino);
      if (!meta) {
        return -2n;
      }
      if (offsetNumber >= meta.size) {
        return 0n;
      }
      if (meta.dataOff === 0 && meta.size > 0) {
        const readLen = Math.min(dstLen, meta.size - offsetNumber);
        const dstRange = normalizeMemoryRange(memory, dstPtr, readLen);
        if (!dstRange) {
          return -14n;
        }
        const resident = readCleanResidentFileData(sabPageCache, ino, offsetNumber, readLen);
        if (resident) {
          new Uint8Array(memory.buffer, dstRange.ptr, resident.length).set(resident);
          return BigInt(resident.length);
        }
        return -2n;
      }
      return -2n;
    },
    tidemark_page_cache_stat(ino, outPtr) {
      if (!sabPageCache) {
        return 0;
      }
      const meta = readInodeMeta(sabPageCache, ino);
      if (!meta) {
        return 0;
      }
      const outRange = normalizeMemoryRange(memory, outPtr, 16);
      if (!outRange) {
        return 0;
      }
      const view = new DataView(memory.buffer);
      view.setUint32(outRange.ptr, meta.mode, true);
      view.setBigUint64(outRange.ptr + 8, BigInt(meta.size), true);
      return 1;
    },
    tidemark_page_cache_flush(ino) {
      if (!sabPageCache || !flushResidentFileDataByInode) {
        return -95;
      }
      return flushResidentFileDataByInode(ino) ? 0 : -5;
    },
    tidemark_page_cache_truncate(ino, size) {
      if (!sabPageCache) {
        return -95;
      }
      const sizeNumber = Number(size);
      if (!Number.isFinite(sizeNumber) || sizeNumber < 0 || sizeNumber > 0xffffffff) {
        return -22;
      }
      if (!truncateResidentFileDataByInode) {
        return -95;
      }
      return truncateResidentFileDataByInode(ino, sizeNumber);
    },
    tidemark_page_cache_write(ino, offset, srcPtr, srcLen) {
      if (srcLen <= 0) {
        return 0n;
      }
      if (!sabPageCache || !writeResidentFileDataByInode) {
        return -95n;
      }
      const offsetNumber = Number(offset);
      if (!Number.isFinite(offsetNumber) || offsetNumber < 0) {
        return -22n;
      }
      const srcRange = normalizeMemoryRange(memory, srcPtr, srcLen);
      if (!srcRange) {
        return -14n;
      }
      const source = new Uint8Array(memory.buffer, srcRange.ptr, srcRange.length);
      return BigInt(writeResidentFileDataByInode(ino, offsetNumber, source));
    },
  };
}
