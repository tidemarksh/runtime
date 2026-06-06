import type { KernelEnvOptions } from "../../src/wasm/env.js";
import { publishInode } from "../../src/fs/sab-page-cache.js";

const encoder = new TextEncoder();

export interface DiagnosticKernelPageCache {
  sabPageCache: SharedArrayBuffer;
  envOptions: KernelEnvOptions;
}

function resizeBytes(current: Uint8Array | undefined, size: number): Uint8Array {
  const next = new Uint8Array(size);
  if (current) {
    next.set(current.subarray(0, Math.min(current.length, size)));
  }
  return next;
}

export function createDiagnosticKernelPageCache(byteLength = 8 * 1024 * 1024): DiagnosticKernelPageCache {
  const sabPageCache = new SharedArrayBuffer(byteLength);
  const files = new Map<number, Uint8Array>();
  const generations = new Map<number, number>();

  const publishFile = (ino: number, size: number): void => {
    const generation = (generations.get(ino) ?? 0) + 1;
    generations.set(ino, generation);
    publishInode(sabPageCache, ino, {
      mode: 0o100644,
      size,
      parentIno: 1,
      name: encoder.encode(`ino-${ino}`),
      nextSibling: 0,
      fileObjectId: ino,
      generation,
    });
  };

  return {
    sabPageCache,
    envOptions: {
      sabPageCache,
      readResidentFileDataByInode(ino, offset, length) {
        const data = files.get(ino);
        if (!data || offset < 0 || length < 0 || offset + length > data.length) {
          return null;
        }
        return data.slice(offset, offset + length);
      },
      flushResidentFileDataByInode() {
        return true;
      },
      truncateResidentFileDataByInode(ino, size) {
        if (!Number.isInteger(size) || size < 0 || size > 0xffffffff) {
          return -22;
        }
        files.set(ino, resizeBytes(files.get(ino), size));
        publishFile(ino, size);
        return 0;
      },
      writeResidentFileDataByInode(ino, offset, data) {
        if (!Number.isInteger(offset) || offset < 0 || offset > 0xffffffff) {
          return -22;
        }
        const end = offset + data.length;
        if (!Number.isInteger(end) || end > 0xffffffff) {
          return -28;
        }
        const next = resizeBytes(files.get(ino), Math.max(files.get(ino)?.length ?? 0, end));
        next.set(data, offset);
        files.set(ino, next);
        publishFile(ino, next.length);
        return data.length;
      },
    },
  };
}
