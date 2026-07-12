import type { KernelWorkerBackingSource } from "./backing-source.js";

interface NodeFsStat {
  dev: number;
  ino: number;
  size: number;
  isFile(): boolean;
}

interface NodeFsSync {
  closeSync(fd: number): void;
  openSync(path: string, flags: string): number;
  readSync(fd: number, buffer: Uint8Array, offset: number, length: number, position: number): number;
  statSync(path: string): NodeFsStat;
}

interface CachedOpenFile {
  fd: number;
  dev: number;
  ino: number;
  size: number;
  lastUsed: number;
}

let nodeFsSyncPromise: Promise<NodeFsSync> | null = null;
let openFileCacheClock = 0;

const MAX_OPEN_HOST_FILE_BACKING_SOURCE_FILES = 64;
const HOST_FILE_BACKING_SOURCE_READ_WINDOW_BYTES = 1024 * 1024;
const openFileCache = new Map<string, CachedOpenFile>();

function loadNodeFsSync(): Promise<NodeFsSync> {
  nodeFsSyncPromise ??= import("node:fs").then((fs) => ({
    closeSync: fs.closeSync,
    openSync: fs.openSync,
    readSync: fs.readSync,
    statSync: fs.statSync,
  })).catch((error) => {
    nodeFsSyncPromise = null;
    throw new Error(
      `host-file backing sources require node:fs: ${error instanceof Error ? error.message : String(error)}`,
    );
  });
  return nodeFsSyncPromise;
}

function closeCachedOpenFile(fs: NodeFsSync, hostPath: string, cached: CachedOpenFile): void {
  openFileCache.delete(hostPath);
  fs.closeSync(cached.fd);
}

function closeAllCachedOpenFiles(fs: NodeFsSync): void {
  for (const [hostPath, cached] of openFileCache) {
    closeCachedOpenFile(fs, hostPath, cached);
  }
}

function cachedOpenFileMatches(cached: CachedOpenFile, stat: NodeFsStat): boolean {
  return cached.dev === stat.dev
    && cached.ino === stat.ino
    && cached.size === stat.size;
}

function evictLeastRecentlyUsedOpenFiles(fs: NodeFsSync): void {
  while (openFileCache.size > MAX_OPEN_HOST_FILE_BACKING_SOURCE_FILES) {
    let oldestPath: string | null = null;
    let oldestEntry: CachedOpenFile | null = null;
    for (const [hostPath, cached] of openFileCache) {
      if (oldestEntry === null || cached.lastUsed < oldestEntry.lastUsed) {
        oldestPath = hostPath;
        oldestEntry = cached;
      }
    }
    if (oldestPath === null || oldestEntry === null) {
      return;
    }
    closeCachedOpenFile(fs, oldestPath, oldestEntry);
  }
}

function acquireCachedReadFd(fs: NodeFsSync, hostPath: string, stat: NodeFsStat): number {
  const cached = openFileCache.get(hostPath);
  if (cached && cachedOpenFileMatches(cached, stat)) {
    cached.lastUsed = ++openFileCacheClock;
    return cached.fd;
  }
  if (cached) {
    closeCachedOpenFile(fs, hostPath, cached);
  }
  const fd = fs.openSync(hostPath, "r");
  openFileCache.set(hostPath, {
    fd,
    dev: stat.dev,
    ino: stat.ino,
    size: stat.size,
    lastUsed: ++openFileCacheClock,
  });
  evictLeastRecentlyUsedOpenFiles(fs);
  return fd;
}

function invalidateCachedReadFd(fs: NodeFsSync, hostPath: string, fd: number): void {
  const cached = openFileCache.get(hostPath);
  if (!cached || cached.fd !== fd) {
    return;
  }
  closeCachedOpenFile(fs, hostPath, cached);
}

class NodeHostFileBackingSource implements KernelWorkerBackingSource {
  readonly eagerPrewarm = true;

  private readonly stat: NodeFsStat;
  private cachedWindow: { offset: number; data: Uint8Array } | null = null;

  constructor(
    private readonly fs: NodeFsSync,
    private readonly hostPath: string,
    readonly byteLength: number,
  ) {
    if (!Number.isSafeInteger(byteLength) || byteLength < 0) {
      throw new RangeError(`invalid host-file backing source byte length: ${byteLength}`);
    }
    this.stat = this.fs.statSync(hostPath);
    if (!this.stat.isFile()) {
      throw new Error(`host-file backing source is not a regular file: ${hostPath}`);
    }
    if (this.stat.size !== byteLength) {
      throw new Error(
        `host-file backing source size changed: ${hostPath} expected=${byteLength} actual=${this.stat.size}`,
      );
    }
  }

  readRange(offset: number, length: number, options?: { readAhead?: boolean }): Uint8Array | null {
    if (
      !Number.isInteger(offset)
      || !Number.isInteger(length)
      || offset < 0
      || length < 0
      || offset + length > this.byteLength
    ) {
      return null;
    }
    if (length === 0) {
      return new Uint8Array();
    }
    const cached = this.cachedWindow;
    if (
      cached
      && offset >= cached.offset
      && offset + length <= cached.offset + cached.data.byteLength
    ) {
      return cached.data.slice(offset - cached.offset, offset - cached.offset + length);
    }
    const fd = acquireCachedReadFd(this.fs, this.hostPath, this.stat);
    try {
      const readLength = options?.readAhead === false || length >= HOST_FILE_BACKING_SOURCE_READ_WINDOW_BYTES
        ? length
        : Math.min(
            this.byteLength - offset,
            Math.max(length, HOST_FILE_BACKING_SOURCE_READ_WINDOW_BYTES),
          );
      const buffer = new Uint8Array(readLength);
      const bytesRead = this.fs.readSync(fd, buffer, 0, readLength, offset);
      if (bytesRead !== readLength) {
        return null;
      }
      if (readLength > length) {
        this.cachedWindow = { offset, data: buffer };
        return buffer.slice(0, length);
      }
      this.cachedWindow = null;
      return buffer;
    } catch (error) {
      invalidateCachedReadFd(this.fs, this.hostPath, fd);
      throw error;
    }
  }
}

export async function createNodeHostFileBackingSource(
  hostPath: string,
  byteLength: number,
): Promise<KernelWorkerBackingSource> {
  return new NodeHostFileBackingSource(await loadNodeFsSync(), hostPath, byteLength);
}

export async function closeNodeHostFileBackingSourceOpenFilesForTest(): Promise<void> {
  closeAllCachedOpenFiles(await loadNodeFsSync());
}
