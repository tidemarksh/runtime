import type { KernelExports } from "../../abi.js";
import type { FsSnapshotEntry } from "../../fs/types.js";
import { publishInode, readFileData, readInodeMeta, resetPageCache } from "../../fs/sab-page-cache.js";
import {
  ensureMemoryCapacityFor,
  memfsFileHasInlineData,
  normalizePath,
  normalizePermMode,
  readMemfsFileDataDirect,
  scratchBaseFor,
  writeScratchFor,
} from "../common.js";

interface KernelWorkerFsSnapshotDeps {
  getCanonicalExports(): KernelExports;
  getSabPageCache(): SharedArrayBuffer;
  seedEntries: Map<string, FsSnapshotEntry>;
  encoder: TextEncoder;
  fanoutCanonicalMemfs(includeData?: boolean): void;
  publishRootInode(mode?: number): void;
  resolvePath(path: string): number;
  readdir(path: string): string[];
  readFile(path: string): Uint8Array;
  writeFile(path: string, data: Uint8Array, mode?: number, fanout?: boolean): void;
  mkdir(path: string, mode?: number, fanout?: boolean): void;
  upsertSymlinkSeed(path: string, target: Uint8Array, mode?: number): void;
}

export function createKernelWorkerFsSnapshotHelpers(deps: KernelWorkerFsSnapshotDeps) {
  const decoder = new TextDecoder();

  function publishSnapshotFile(
    targetExports: KernelExports,
    path: string,
    data: Uint8Array | null,
    size: number,
    mode?: number,
  ): void {
    const normalized = normalizePath(path);
    const parts = normalized.split("/").filter(Boolean);
    const name = parts.pop();
    if (!name) {
      return;
    }
    const parentPath = parts.length === 0 ? "/" : `/${parts.join("/")}`;
    deps.mkdir(parentPath, 0o755, false);
    const parentIno = deps.resolvePath(parentPath);
    if (parentIno === 0) {
      return;
    }
    const nameBytes = deps.encoder.encode(name);
    const namePtr = writeScratchFor(targetExports, nameBytes, 0x8000);
    const ino = targetExports.memfs_create_file(parentIno, namePtr, nameBytes.length);
    if (ino === 0) {
      return;
    }
    const perms = normalizePermMode(mode, 0o644);
    const publishedSize = data?.length ?? size;
    if (data !== null) {
      if (!memfsFileHasInlineData(targetExports, ino)) {
        const sizeResult = Number(targetExports.memfs_set_file_size_shallow(ino, BigInt(data.length)));
        if (sizeResult < 0) {
          throw new Error(`failed to materialize snapshot file: ${path}`);
        }
      } else {
        const truncateResult = Number(targetExports.memfs_truncate_file(ino, BigInt(data.length)));
        if (truncateResult < 0) {
          throw new Error(`failed to materialize snapshot file: ${path}`);
        }
      }
      if (memfsFileHasInlineData(targetExports, ino) && data.length > 0) {
        const dataPtr = writeScratchFor(targetExports, data, 0xa000);
        const written = Number(targetExports.memfs_write_file_at(ino, 0n, dataPtr, data.length));
        if (written < 0 || written !== data.length) {
          throw new Error(`failed to write snapshot file: ${path}`);
        }
      }
    } else {
      const externalized = Number(targetExports.memfs_set_file_external(ino, BigInt(size)));
      if (externalized < 0) {
        throw new Error(`failed to externalize snapshot file: ${path}`);
      }
    }
    const modeResult = Number(targetExports.memfs_set_mode(ino, perms));
    if (modeResult < 0) {
      throw new Error(`failed to set snapshot file mode: ${path}`);
    }
    deps.seedEntries.set(normalized, {
      kind: "file",
      path: normalized,
      size: publishedSize,
      mode: perms,
    });
    publishInode(deps.getSabPageCache(), ino, {
      mode: 0o100000 | perms,
      size: publishedSize,
      parentIno,
      name: nameBytes,
      nextSibling: 0,
    }, data);
  }

  function resolvePathIn(targetExports: KernelExports, path: string): number {
    const normalized = normalizePath(path);
    const pathBytes = deps.encoder.encode(normalized);
    const pathPtr = writeScratchFor(targetExports, pathBytes, 0x8000);
    return targetExports.memfs_resolve_path(pathPtr, pathBytes.length);
  }

  function readFileIn(targetExports: KernelExports, path: string): Uint8Array {
    const ino = resolvePathIn(targetExports, path);
    if (ino === 0) {
      throw new Error(`file not found: ${path}`);
    }
    const size = Number(targetExports.memfs_file_size(ino));
    const bufPtr = scratchBaseFor(targetExports) + 0xc000;
    ensureMemoryCapacityFor(targetExports.memory, bufPtr + Math.max(size, 1));
    const read = Number(targetExports.memfs_read_file_to(ino, 0n, bufPtr, size));
    if (read < 0) {
      throw new Error(`failed to read file: ${path}`);
    }
    return new Uint8Array(targetExports.memory.buffer.slice(bufPtr, bufPtr + read));
  }

  function readdirIn(targetExports: KernelExports, path: string): string[] {
    const ino = resolvePathIn(targetExports, path);
    if (ino === 0) {
      throw new Error(`directory not found: ${path}`);
    }
    const bufPtr = scratchBaseFor(targetExports) + 0x18000;
    const bufLen = 64 * 1024;
    ensureMemoryCapacityFor(targetExports.memory, bufPtr + bufLen);
    const byteLen = Number(targetExports.memfs_readdir_names(ino, bufPtr, bufLen));
    if (byteLen < 0) {
      throw new Error(`failed to read directory: ${path}`);
    }
    const bytes = new Uint8Array(targetExports.memory.buffer, bufPtr, byteLen);
    const names: string[] = [];
    let start = 0;
    for (let i = 0; i < bytes.length; i += 1) {
      if (bytes[i] !== 0) {
        continue;
      }
      if (i > start) {
        names.push(decoder.decode(new Uint8Array(bytes.slice(start, i))));
      }
      start = i + 1;
    }
    return names;
  }

  function readSnapshotFileData(targetExports: KernelExports, path: string, ino: number): Uint8Array | undefined {
    const direct = readMemfsFileDataDirect(targetExports, ino);
    if (direct) {
      return direct;
    }
    const cached = readInodeMeta(deps.getSabPageCache(), ino);
    if (cached && cached.dataOff !== 0 && (cached.mode & 0o170000) === 0o100000) {
      return readFileData(deps.getSabPageCache(), cached.dataOff, cached.size, 0, cached.size);
    }
    try {
      return readFileIn(targetExports, path);
    } catch {
      return undefined;
    }
  }

  function readLink(targetExports: KernelExports, path: string): Uint8Array | null {
    const ino = resolvePathIn(targetExports, path);
    if (ino === 0 || (targetExports.memfs_inode_mode(ino) & 0o170000) !== 0o120000) {
      return null;
    }
    const bufPtr = scratchBaseFor(targetExports) + 0x8000;
    const bufLen = 4096;
    ensureMemoryCapacityFor(targetExports.memory, bufPtr + bufLen);
    const len = Number(targetExports.memfs_read_link(ino, bufPtr, bufLen));
    if (len <= 0) {
      return null;
    }
    return new Uint8Array(targetExports.memory.buffer.slice(bufPtr, bufPtr + len));
  }

  function snapshotFsTree(
    targetExports: KernelExports,
    ino: number,
    path: string,
    out: FsSnapshotEntry[],
    includeData: boolean,
  ): void {
    const normalized = normalizePath(path);
    const mode = targetExports.memfs_inode_mode(ino);
    const perms = mode & 0o7777;
    const fileType = mode & 0o170000;

    if (fileType === 0o040000) {
      out.push({ kind: "dir", path: normalized, ino, mode: perms });
      for (const name of readdirIn(targetExports, normalized).sort()) {
        const childPath = normalized === "/" ? `/${name}` : `${normalized}/${name}`;
        const childIno = resolvePathIn(targetExports, childPath);
        if (childIno === 0) {
          continue;
        }
        snapshotFsTree(targetExports, childIno, childPath, out, includeData);
      }
      return;
    }

    if (fileType === 0o120000) {
      const target = readLink(targetExports, normalized) ?? new Uint8Array();
      out.push({
        kind: "symlink",
        path: normalized,
        ino,
        target,
        size: target.length,
        mode: perms,
      });
      return;
    }

    if (fileType !== 0o100000) {
      return;
    }

    out.push({
      kind: "file",
      path: normalized,
      ino,
      data: includeData ? readSnapshotFileData(targetExports, normalized, ino) : undefined,
      size: Number(targetExports.memfs_file_size(ino)),
      mode: perms,
    });
  }

  function snapshotFsEntries(targetExports: KernelExports, includeData: boolean): FsSnapshotEntry[] {
    const entries: FsSnapshotEntry[] = [];
    snapshotFsTree(targetExports, 1, "/", entries, includeData);
    return entries;
  }

  function snapshotCanonicalFs(includeData: boolean): FsSnapshotEntry[] {
    return snapshotFsEntries(deps.getCanonicalExports(), includeData);
  }

  function rebuildPageCacheFromExports(targetExports: KernelExports): void {
    const entries = snapshotFsEntries(targetExports, true);
    const rootMode = targetExports.memfs_inode_mode(1) & 0o7777;
    resetPageCache(deps.getSabPageCache());
    deps.publishRootInode(rootMode);

    for (const entry of entries) {
      if (entry.path === "/") {
        continue;
      }
      const normalized = normalizePath(entry.path);
      const name = normalized.split("/").pop();
      if (!name || entry.ino === undefined) {
        continue;
      }
      const parentPath = normalized.split("/").slice(0, -1).join("/") || "/";
      const parentIno = deps.resolvePath(parentPath);
      if (parentIno === 0) {
        continue;
      }
      const perms = normalizePermMode(
        entry.mode,
        entry.kind === "dir" ? 0o755 : entry.kind === "symlink" ? 0o777 : 0o644,
      );
      const fileType = entry.kind === "dir"
        ? 0o040000
        : entry.kind === "symlink"
          ? 0o120000
          : 0o100000;
      publishInode(deps.getSabPageCache(), entry.ino, {
        mode: fileType | perms,
        size: entry.size ?? entry.data?.length ?? 0,
        parentIno,
        name: deps.encoder.encode(name),
        nextSibling: 0,
      }, entry.kind === "file" ? (entry.data ?? new Uint8Array()) : null);
    }
  }

  function rebuildPageCacheFromCanonical(): void {
    rebuildPageCacheFromExports(deps.getCanonicalExports());
  }

  function loadFsSnapshot(entries: FsSnapshotEntry[]): void {
    const targetExports = deps.getCanonicalExports();
    targetExports.kernel_reset_memfs();
    resetPageCache(deps.getSabPageCache());
    deps.seedEntries.clear();

    const rootEntry = entries.find((entry) => entry.kind === "dir" && normalizePath(entry.path) === "/");
    const rootMode = normalizePermMode(rootEntry?.mode, 0o755);
    deps.seedEntries.set("/", { kind: "dir", path: "/", mode: rootMode });
    const rootModeResult = Number(targetExports.memfs_set_mode(1, rootMode));
    if (rootModeResult < 0) {
      throw new Error("failed to set mode on root directory");
    }
    deps.publishRootInode(rootMode);

    const ordered = entries
      .filter((entry) => !(entry.kind === "dir" && normalizePath(entry.path) === "/"))
      .sort((left, right) => {
        if (left.kind !== right.kind) {
          return left.kind === "dir" ? -1 : 1;
        }
        return left.path.localeCompare(right.path);
      });

    for (const entry of ordered) {
      if (entry.kind === "dir") {
        deps.mkdir(entry.path, entry.mode ?? 0o755, false);
        continue;
      }
      if (entry.kind === "symlink") {
        const normalized = normalizePath(entry.path);
        const parts = normalized.split("/").filter(Boolean);
        const name = parts.pop();
        if (!name) {
          continue;
        }
        const parentPath = parts.length === 0 ? "/" : `/${parts.join("/")}`;
        const parentIno = deps.resolvePath(parentPath);
        if (parentIno === 0) {
          continue;
        }
      const nameBytes = deps.encoder.encode(name);
      const namePtr = writeScratchFor(targetExports, nameBytes, 0x8000);
      const target = entry.target ?? new Uint8Array();
      const targetPtr = writeScratchFor(targetExports, target, 0x9000);
      targetExports.memfs_create_symlink(parentIno, namePtr, nameBytes.length, targetPtr, target.length);
      deps.upsertSymlinkSeed(normalized, target, entry.mode ?? 0o777);
      continue;
    }
      if (entry.data !== undefined) {
        publishSnapshotFile(targetExports, entry.path, entry.data, entry.data.length, entry.mode);
        continue;
      }
      publishSnapshotFile(targetExports, entry.path, null, entry.size ?? 0, entry.mode);
    }

    deps.fanoutCanonicalMemfs(false);
  }

  return {
    loadFsSnapshot,
    rebuildPageCacheFromExports,
    rebuildPageCacheFromCanonical,
    snapshotCanonicalFs,
  };
}
