import type { KernelExports } from "../../abi.js";
import type { FsSnapshotEntry } from "../../fs/types.js";
import { publishInode, resetPageCache } from "../../fs/sab-page-cache.js";
import { normalizePath, normalizePermMode, readMemfsFileDataDirect } from "../common.js";

interface KernelWorkerFsSnapshotDeps {
  getCanonicalExports(): KernelExports;
  getSabPageCache(): SharedArrayBuffer;
  seedEntries: Map<string, FsSnapshotEntry>;
  encoder: TextEncoder;
  fanoutCanonicalMemfs(): void;
  publishRootInode(mode?: number): void;
  resolvePath(path: string): number;
  readdir(path: string): string[];
  readFile(path: string): Uint8Array;
  writeFile(path: string, data: Uint8Array, mode?: number, fanout?: boolean): void;
  mkdir(path: string, mode?: number, fanout?: boolean): void;
}

export function createKernelWorkerFsSnapshotHelpers(deps: KernelWorkerFsSnapshotDeps) {
  function readLink(path: string): Uint8Array | null {
    const targetExports = deps.getCanonicalExports();
    const ino = deps.resolvePath(path);
    if (ino === 0 || (targetExports.memfs_inode_mode(ino) & 0o170000) !== 0o120000) {
      return null;
    }
    const bufPtr = targetExports.kernel_heap_ptr() + 0x18000;
    const bufLen = 4096;
    const len = Number(targetExports.memfs_read_link(ino, bufPtr, bufLen));
    if (len <= 0) {
      return null;
    }
    return new Uint8Array(targetExports.memory.buffer.slice(bufPtr, bufPtr + len));
  }

  function snapshotFsTree(
    ino: number,
    path: string,
    out: FsSnapshotEntry[],
    includeData: boolean,
  ): void {
    const targetExports = deps.getCanonicalExports();
    const normalized = normalizePath(path);
    const mode = targetExports.memfs_inode_mode(ino);
    const perms = mode & 0o7777;
    const fileType = mode & 0o170000;

    if (fileType === 0o040000) {
      out.push({ kind: "dir", path: normalized, ino, mode: perms });
      for (const name of deps.readdir(normalized).sort()) {
        const childPath = normalized === "/" ? `/${name}` : `${normalized}/${name}`;
        const childIno = deps.resolvePath(childPath);
        if (childIno === 0) {
          continue;
        }
        snapshotFsTree(childIno, childPath, out, includeData);
      }
      return;
    }

    if (fileType === 0o120000) {
      const target = readLink(normalized) ?? new Uint8Array();
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
      data: includeData ? (readMemfsFileDataDirect(targetExports, ino) ?? deps.readFile(normalized)) : undefined,
      size: Number(targetExports.memfs_file_size(ino)),
      mode: perms,
    });
  }

  function snapshotCanonicalFs(includeData: boolean): FsSnapshotEntry[] {
    const entries: FsSnapshotEntry[] = [];
    snapshotFsTree(1, "/", entries, includeData);
    return entries;
  }

  function rebuildPageCacheFromCanonical(): void {
    const entries = snapshotCanonicalFs(true);
    const targetExports = deps.getCanonicalExports();
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
        const namePtr = targetExports.kernel_heap_ptr() + 0x18000;
        new Uint8Array(targetExports.memory.buffer).set(nameBytes, namePtr);
        const target = entry.target ?? new Uint8Array();
        const targetPtr = namePtr + 0x1000;
        new Uint8Array(targetExports.memory.buffer).set(target, targetPtr);
        targetExports.memfs_create_symlink(parentIno, namePtr, nameBytes.length, targetPtr, target.length);
        continue;
      }
      deps.writeFile(entry.path, entry.data ?? new Uint8Array(), entry.mode ?? 0o644, false);
    }

    deps.fanoutCanonicalMemfs();
  }

  return {
    loadFsSnapshot,
    rebuildPageCacheFromCanonical,
    snapshotCanonicalFs,
  };
}
