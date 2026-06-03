import { readInodeMeta, writeFileData } from "../fs/sab-page-cache.js";

export interface KernelEnvImports {
  [key: string]: WebAssembly.ImportValue;
  memory: WebAssembly.Memory;
  tidemark_memory_atomic_notify(ptr: number, maxWake: number): number;
  tidemark_page_cache_read(ino: number, offset: bigint, dstPtr: number, dstLen: number): bigint;
  tidemark_page_cache_stat(ino: number, outPtr: number): number;
  tidemark_page_cache_write(ino: number, offset: bigint, srcPtr: number, srcLen: number): bigint;
}

export interface KernelEnvOptions {
  sabPageCache?: SharedArrayBuffer | null;
  readFileDataByInode?: (ino: number, offset: number, length: number) => Uint8Array | null;
}

export function createKernelEnv(
  memory: WebAssembly.Memory,
  options: KernelEnvOptions = {},
): KernelEnvImports {
  const sabPageCache = options.sabPageCache ?? null;
  const readFileDataByInode = options.readFileDataByInode;
  return {
    memory,
    tidemark_memory_atomic_notify(ptr, maxWake) {
      return Atomics.notify(new Int32Array(memory.buffer), ptr >>> 2, maxWake);
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
        const backing = readFileDataByInode?.(ino, offsetNumber, dstLen);
        if (!backing) {
          return -2n;
        }
        const readLen = Math.min(backing.length, dstLen);
        new Uint8Array(memory.buffer, dstPtr, readLen).set(backing.subarray(0, readLen));
        return BigInt(readLen);
      }
      const meta = readInodeMeta(sabPageCache, ino);
      if (!meta) {
        const backing = readFileDataByInode?.(ino, offsetNumber, dstLen);
        if (!backing) {
          return -2n;
        }
        const readLen = Math.min(backing.length, dstLen);
        new Uint8Array(memory.buffer, dstPtr, readLen).set(backing.subarray(0, readLen));
        return BigInt(readLen);
      }
      if (offsetNumber >= meta.size) {
        return 0n;
      }
      if (meta.dataOff === 0 && meta.size > 0) {
        const backing = readFileDataByInode?.(ino, offsetNumber, dstLen);
        if (!backing) {
          return -2n;
        }
        const readLen = Math.min(backing.length, dstLen);
        new Uint8Array(memory.buffer, dstPtr, readLen).set(backing.subarray(0, readLen));
        return BigInt(readLen);
      }
      const readLen = Math.min(dstLen, meta.size - offsetNumber);
      const source = new Uint8Array(sabPageCache, meta.dataOff + offsetNumber, readLen);
      new Uint8Array(memory.buffer, dstPtr, readLen).set(source);
      return BigInt(readLen);
    },
    tidemark_page_cache_stat(ino, outPtr) {
      if (!sabPageCache) {
        return 0;
      }
      const meta = readInodeMeta(sabPageCache, ino);
      if (!meta) {
        return 0;
      }
      const view = new DataView(memory.buffer);
      view.setUint32(outPtr, meta.mode, true);
      view.setBigUint64(outPtr + 8, BigInt(meta.size), true);
      return 1;
    },
    tidemark_page_cache_write(ino, offset, srcPtr, srcLen) {
      if (!sabPageCache || srcLen <= 0) {
        return 0n;
      }
      const offsetNumber = Number(offset);
      if (!Number.isFinite(offsetNumber) || offsetNumber < 0) {
        return -22n;
      }
      if (srcPtr < 0 || srcPtr + srcLen > memory.buffer.byteLength) {
        return -14n;
      }
      const source = new Uint8Array(memory.buffer, srcPtr, srcLen);
      return BigInt(writeFileData(sabPageCache, ino, offsetNumber, source));
    },
  };
}
