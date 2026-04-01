import { readInodeMeta } from "../fs/sab-page-cache.js";

export interface KernelEnvImports {
  [key: string]: WebAssembly.ImportValue;
  memory: WebAssembly.Memory;
  tidemark_memory_atomic_notify(ptr: number, maxWake: number): number;
  tidemark_page_cache_read(ino: number, offset: bigint, dstPtr: number, dstLen: number): bigint;
  tidemark_page_cache_stat(ino: number, outPtr: number): number;
}

export interface KernelEnvOptions {
  sabPageCache?: SharedArrayBuffer | null;
}

export function createKernelEnv(
  memory: WebAssembly.Memory,
  options: KernelEnvOptions = {},
): KernelEnvImports {
  const sabPageCache = options.sabPageCache ?? null;
  return {
    memory,
    tidemark_memory_atomic_notify(ptr, maxWake) {
      return Atomics.notify(new Int32Array(memory.buffer), ptr >>> 2, maxWake);
    },
    tidemark_page_cache_read(ino, offset, dstPtr, dstLen) {
      if (!sabPageCache || dstLen <= 0) {
        return 0n;
      }
      const meta = readInodeMeta(sabPageCache, ino);
      if (!meta) {
        return -2n;
      }
      const offsetNumber = Number(offset);
      if (!Number.isFinite(offsetNumber) || offsetNumber < 0 || offsetNumber >= meta.size) {
        return 0n;
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
  };
}
