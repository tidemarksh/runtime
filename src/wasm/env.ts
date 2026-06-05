import {
  hasResidentPageRecordCapacity,
  publishResidentPage,
  readInodeMeta,
  readResidentFileData,
  truncateDirtyResidentFileData,
  writeDirtyResidentFileData,
} from "../fs/sab-page-cache.js";

export interface KernelEnvImports {
  [key: string]: WebAssembly.ImportValue;
  memory: WebAssembly.Memory;
  tidemark_memory_atomic_notify(ptr: number | bigint, maxWake: number | bigint): number;
  tidemark_page_cache_read(ino: number, offset: bigint, dstPtr: number, dstLen: number): bigint;
  tidemark_page_cache_stat(ino: number, outPtr: number): number;
  tidemark_page_cache_truncate(ino: number, size: bigint): number;
  tidemark_page_cache_write(ino: number, offset: bigint, srcPtr: number, srcLen: number): bigint;
}

export interface KernelEnvOptions {
  sabPageCache?: SharedArrayBuffer | null;
  readFileDataByInode?: (ino: number, offset: number, length: number) => Uint8Array | null;
  writeResidentFileDataByInode?: (ino: number, offset: number, data: Uint8Array) => number;
}

function copyBackingRead(
  memory: WebAssembly.Memory,
  dstPtr: number,
  dstLen: number,
  backing: Uint8Array | null | undefined,
): bigint {
  if (!backing || backing.length === 0) {
    return -2n;
  }
  const readLen = Math.min(backing.length, dstLen);
  new Uint8Array(memory.buffer, dstPtr, readLen).set(backing.subarray(0, readLen));
  return BigInt(readLen);
}

export function createKernelEnv(
  memory: WebAssembly.Memory,
  options: KernelEnvOptions = {},
): KernelEnvImports {
  const sabPageCache = options.sabPageCache ?? null;
  const readFileDataByInode = options.readFileDataByInode;
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
        const backing = readFileDataByInode?.(ino, offsetNumber, dstLen);
        return copyBackingRead(memory, dstPtr, dstLen, backing);
      }
      const meta = readInodeMeta(sabPageCache, ino);
      if (!meta) {
        const backing = readFileDataByInode?.(ino, offsetNumber, dstLen);
        return copyBackingRead(memory, dstPtr, dstLen, backing);
      }
      if (offsetNumber >= meta.size) {
        return 0n;
      }
      if (meta.dataOff === 0 && meta.size > 0) {
        const readLen = Math.min(dstLen, meta.size - offsetNumber);
        const resident = readResidentFileData(sabPageCache, ino, offsetNumber, readLen);
        if (resident) {
          new Uint8Array(memory.buffer, dstPtr, resident.length).set(resident);
          return BigInt(resident.length);
        }
        const pageSize = 4096;
        const firstPage = Math.floor(offsetNumber / pageSize);
        const lastPage = Math.floor((offsetNumber + readLen - 1) / pageSize);
        let filledRequestedRange = hasResidentPageRecordCapacity(sabPageCache);
        for (let pageIndex = firstPage; filledRequestedRange && pageIndex <= lastPage; pageIndex += 1) {
          const pageStart = pageIndex * pageSize;
          const pageLen = Math.min(pageSize, meta.size - pageStart);
          if (readResidentFileData(sabPageCache, ino, pageStart, pageLen)) {
            continue;
          }
          const page = readFileDataByInode?.(ino, pageStart, pageLen);
          filledRequestedRange = !!(page && page.length > 0 && publishResidentPage(sabPageCache, ino, pageIndex, page));
        }
        if (filledRequestedRange) {
          const filled = readResidentFileData(sabPageCache, ino, offsetNumber, readLen);
          if (filled) {
            new Uint8Array(memory.buffer, dstPtr, filled.length).set(filled);
            return BigInt(filled.length);
          }
        }
        const backing = readFileDataByInode?.(ino, offsetNumber, readLen);
        return copyBackingRead(memory, dstPtr, readLen, backing);
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
    tidemark_page_cache_truncate(ino, size) {
      if (!sabPageCache) {
        return -95;
      }
      const sizeNumber = Number(size);
      if (!Number.isFinite(sizeNumber) || sizeNumber < 0 || sizeNumber > 0xffffffff) {
        return -22;
      }
      return truncateDirtyResidentFileData(sabPageCache, ino, sizeNumber);
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
      if (writeResidentFileDataByInode) {
        return BigInt(writeResidentFileDataByInode(ino, offsetNumber, source));
      }
      return BigInt(writeDirtyResidentFileData(sabPageCache, ino, offsetNumber, source, readFileDataByInode));
    },
  };
}
