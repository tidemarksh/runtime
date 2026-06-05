import type { KernelWorkerBackingSource } from "./fs-state.js";

type NodeFsSync = Pick<typeof import("node:fs"), "closeSync" | "openSync" | "readSync" | "statSync">;

let nodeFsSyncPromise: Promise<NodeFsSync> | null = null;

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

class NodeHostFileBackingSource implements KernelWorkerBackingSource {
  constructor(
    private readonly fs: NodeFsSync,
    private readonly hostPath: string,
    readonly byteLength: number,
  ) {
    if (!Number.isSafeInteger(byteLength) || byteLength < 0) {
      throw new RangeError(`invalid host-file backing source byte length: ${byteLength}`);
    }
    const stat = this.fs.statSync(hostPath);
    if (!stat.isFile()) {
      throw new Error(`host-file backing source is not a regular file: ${hostPath}`);
    }
    if (stat.size !== byteLength) {
      throw new Error(
        `host-file backing source size changed: ${hostPath} expected=${byteLength} actual=${stat.size}`,
      );
    }
  }

  readRange(offset: number, length: number): Uint8Array | null {
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
    const fd = this.fs.openSync(this.hostPath, "r");
    try {
      const buffer = new Uint8Array(length);
      const bytesRead = this.fs.readSync(fd, buffer, 0, length, offset);
      return bytesRead === length ? buffer : null;
    } finally {
      this.fs.closeSync(fd);
    }
  }
}

export async function createNodeHostFileBackingSource(
  hostPath: string,
  byteLength: number,
): Promise<KernelWorkerBackingSource> {
  return new NodeHostFileBackingSource(await loadNodeFsSync(), hostPath, byteLength);
}
