import { readInodeMeta, writeFileData } from "../fs/sab-page-cache.js";

export interface KernelEnvImports {
  [key: string]: WebAssembly.ImportValue;
  memory: WebAssembly.Memory;
  tidemark_memory_atomic_notify(ptr: number | bigint, maxWake: number | bigint): number;
  tidemark_page_cache_read(ino: number, offset: bigint, dstPtr: number | bigint, dstLen: number): bigint;
  tidemark_page_cache_stat(ino: number, outPtr: number | bigint): number;
  tidemark_page_cache_write(ino: number, offset: bigint, srcPtr: number | bigint, srcLen: number): bigint;
}

function toHostNumber(value: number | bigint): number {
  return typeof value === "bigint" ? Number(value) : value;
}

export interface KernelEnvOptions {
  sabPageCache?: SharedArrayBuffer | null;
  pageCacheMemoryWriteObserver?: (event: {
    source: "page-cache-read" | "page-cache-stat";
    ino: number;
    offset: number;
    ptr: number;
    length: number;
    bytes?: Uint8Array;
  }) => void;
}

export function createKernelEnv(
  memory: WebAssembly.Memory,
  options: KernelEnvOptions = {},
): KernelEnvImports {
  const sabPageCache = options.sabPageCache ?? null;
  return {
    memory,
    tidemark_memory_atomic_notify(ptr, maxWake) {
      const ptrNumber = toHostNumber(ptr);
      const maxWakeNumber = toHostNumber(maxWake);
      return Atomics.notify(new Int32Array(memory.buffer), ptrNumber >>> 2, maxWakeNumber);
    },
    tidemark_page_cache_read(ino, offset, dstPtr, dstLen) {
      if (!sabPageCache || dstLen <= 0) {
        return 0n;
      }
      const dstPtrNumber = toHostNumber(dstPtr);
      const meta = readInodeMeta(sabPageCache, ino);
      if (!meta) {
        return -2n;
      }
      if (meta.dataOff === 0 && meta.size > 0) {
        return -2n;
      }
      const offsetNumber = Number(offset);
      if (!Number.isFinite(offsetNumber) || offsetNumber < 0 || offsetNumber >= meta.size) {
        return 0n;
      }
      const readLen = Math.min(dstLen, meta.size - offsetNumber);
      if (!Number.isFinite(dstPtrNumber) || dstPtrNumber < 0 || dstPtrNumber + readLen > memory.buffer.byteLength) {
        return -14n;
      }
      const source = new Uint8Array(sabPageCache, meta.dataOff + offsetNumber, readLen);
      new Uint8Array(memory.buffer, dstPtrNumber, readLen).set(source);
      options.pageCacheMemoryWriteObserver?.({
        source: "page-cache-read",
        ino,
        offset: offsetNumber,
        ptr: dstPtrNumber,
        length: readLen,
        bytes: source,
      });
      return BigInt(readLen);
    },
    tidemark_page_cache_stat(ino, outPtr) {
      if (!sabPageCache) {
        return 0;
      }
      const outPtrNumber = toHostNumber(outPtr);
      const meta = readInodeMeta(sabPageCache, ino);
      if (!meta) {
        return 0;
      }
      if (!Number.isFinite(outPtrNumber) || outPtrNumber < 0 || outPtrNumber + 16 > memory.buffer.byteLength) {
        return 0;
      }
      const view = new DataView(memory.buffer);
      view.setUint32(outPtrNumber, meta.mode, true);
      view.setBigUint64(outPtrNumber + 8, BigInt(meta.size), true);
      options.pageCacheMemoryWriteObserver?.({
        source: "page-cache-stat",
        ino,
        offset: 0,
        ptr: outPtrNumber,
        length: 16,
      });
      return 1;
    },
    tidemark_page_cache_write(ino, offset, srcPtr, srcLen) {
      if (!sabPageCache || srcLen <= 0) {
        return 0n;
      }
      const srcPtrNumber = toHostNumber(srcPtr);
      const offsetNumber = Number(offset);
      if (!Number.isFinite(offsetNumber) || offsetNumber < 0) {
        return -22n;
      }
      if (!Number.isFinite(srcPtrNumber) || srcPtrNumber < 0 || srcPtrNumber + srcLen > memory.buffer.byteLength) {
        return -14n;
      }
      const source = new Uint8Array(memory.buffer, srcPtrNumber, srcLen);
      return BigInt(writeFileData(sabPageCache, ino, offsetNumber, source));
    },
  };
}
