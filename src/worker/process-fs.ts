import type { KernelExports } from "../abi.js";
import { readFileData, readInodeMeta } from "../fs/sab-page-cache.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import {
  alignUp,
  ensureKernelMemoryCapacity,
  kernelScratchBase,
  writeDetachedKernelScratch,
  writeKernelScratch,
} from "./kernel-scratch.js";
import type { ProcessHandle } from "./types.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function copyBytes(view: Uint8Array): Uint8Array {
  const copy = new Uint8Array(view.byteLength);
  copy.set(view);
  return copy;
}

export function normalizePath(path: string): string {
  const normalized = `/${path.split("/").filter(Boolean).join("/")}`;
  return normalized === "//" ? "/" : normalized || "/";
}

interface CloneSnapshotOptions {
  stripFileData?: boolean;
}

export function cloneSnapshotEntry(
  entry: FsSnapshotEntry,
  options: CloneSnapshotOptions = {},
): FsSnapshotEntry {
  if (entry.kind === "file") {
    const data = entry.data && !options.stripFileData
      ? new Uint8Array(entry.data)
      : undefined;
    return {
      kind: "file",
      path: entry.path,
      ino: entry.ino,
      data,
      size: entry.size ?? entry.data?.length ?? 0,
      mode: entry.mode,
      backingExtent: entry.backingExtent ? { ...entry.backingExtent } : undefined,
    };
  }
  if (entry.kind === "symlink") {
    return {
      kind: "symlink",
      path: entry.path,
      ino: entry.ino,
      target: entry.target ? new Uint8Array(entry.target) : undefined,
      size: entry.size ?? entry.target?.length ?? 0,
      mode: entry.mode,
    };
  }
  return {
    kind: "dir",
    path: entry.path,
    ino: entry.ino,
    mode: entry.mode,
  };
}

export function cloneSnapshotEntries(
  entries: FsSnapshotEntry[],
  options: CloneSnapshotOptions = {},
): FsSnapshotEntry[] {
  return entries.map((entry) => cloneSnapshotEntry(entry, options));
}

function writeSnapshotScratch(
  exports: KernelExports,
  scratchBase: number | undefined,
  bytes: Uint8Array,
  offset = 0,
): number {
  if (scratchBase === undefined) {
    return writeKernelScratch(exports, bytes, offset);
  }
  const dynamicBase = Math.max(scratchBase, kernelScratchBase(exports));
  return writeDetachedKernelScratch(exports, dynamicBase, bytes, offset);
}

function replaceFileData(
  exports: KernelExports,
  ino: number,
  data: Uint8Array,
  scratchBase?: number,
): boolean {
  const truncateResult = Number(exports.memfs_truncate_file(ino, BigInt(data.length)));
  if (truncateResult < 0) {
    return false;
  }
  if (data.length > 0) {
    const dataPtr = writeSnapshotScratch(exports, scratchBase, data);
    const written = Number(exports.memfs_write_file_at(ino, 0n, dataPtr, data.length));
    if (written < 0 || written !== data.length) {
      return false;
    }
  }
  return true;
}

export function preloadSnapshotEntries(
  exports: KernelExports,
  entries: FsSnapshotEntry[],
  scratchBase?: number,
): void {
  if (entries.length === 0) {
    return;
  }

  const ordered = [...entries].sort((left, right) => {
    if (left.ino !== undefined && right.ino !== undefined && left.ino !== right.ino) {
      return left.ino - right.ino;
    }
    if (left.kind !== right.kind) {
      return left.kind === "dir" ? -1 : 1;
    }
    return left.path.localeCompare(right.path);
  });

  let scratchOffset = 0;
  const pathInos = new Map<string, number>([["/", 1]]);
  const allocScratch = (bytes: Uint8Array): number => {
    const ptr = writeSnapshotScratch(exports, scratchBase, bytes, scratchOffset);
    scratchOffset = alignUp(scratchOffset + bytes.length, 16);
    return ptr;
  };

  for (const entry of ordered) {
    const normalizedPath = normalizePath(entry.path);
    const parts = entry.path.split("/").filter(Boolean);
    if (parts.length === 0) {
      continue;
    }

    const name = parts.pop()!;
    let parentIno = 1;
    let parentPath = "/";
    for (const dir of parts) {
      const nextPath = parentPath === "/" ? `/${dir}` : `${parentPath}/${dir}`;
      const cachedDirIno = pathInos.get(nextPath);
      if (cachedDirIno !== undefined) {
        parentIno = cachedDirIno;
        parentPath = nextPath;
        continue;
      }
      const dirBytes = encoder.encode(dir);
      const dirPtr = allocScratch(dirBytes);
      parentIno = exports.memfs_ensure_dir(parentIno, dirPtr, dirBytes.length, 0o755);
      if (parentIno === 0) {
        break;
      }
      pathInos.set(nextPath, parentIno);
      parentPath = nextPath;
    }
    if (parentIno === 0) {
      continue;
    }

    const nameBytes = encoder.encode(name);
    const namePtr = allocScratch(nameBytes);
    if (entry.kind === "dir") {
      const existingIno = pathInos.get(normalizedPath) ?? 0;
      if (existingIno === 0 && entry.ino !== undefined && entry.ino > 1) {
        const reserveResult = Number(exports.memfs_reserve_inode_until(entry.ino));
        if (reserveResult < 0) {
          continue;
        }
      }
      const ino = exports.memfs_ensure_dir(parentIno, namePtr, nameBytes.length, entry.mode ?? 0o755);
      if (ino !== 0) {
        pathInos.set(normalizedPath, ino);
      }
      continue;
    }

    if (entry.kind === "symlink") {
      const existingIno = pathInos.get(normalizedPath) ?? 0;
      if (existingIno === 0 && entry.ino !== undefined && entry.ino > 1) {
        const reserveResult = Number(exports.memfs_reserve_inode_until(entry.ino));
        if (reserveResult < 0) {
          continue;
        }
      }
      const target = entry.target ?? new Uint8Array();
      const targetPtr = allocScratch(target);
      const ino = exports.memfs_create_symlink(parentIno, namePtr, nameBytes.length, targetPtr, target.length);
      if (ino !== 0) {
        pathInos.set(normalizedPath, ino);
      }
      continue;
    }

    const data = entry.data;
    const size = entry.size ?? data?.length ?? 0;
    const mode = entry.mode ?? 0o644;
    const existingIno = pathInos.get(normalizedPath) ?? 0;
    if (existingIno === 0 && entry.ino !== undefined && entry.ino > 1) {
      const reserveResult = Number(exports.memfs_reserve_inode_until(entry.ino));
      if (reserveResult < 0) {
        continue;
      }
    }
    const ino = existingIno !== 0
      ? existingIno
      : exports.memfs_create_file(parentIno, namePtr, nameBytes.length);
    if (ino === 0) {
      continue;
    }
    pathInos.set(normalizedPath, ino);

    if (data) {
      if (!replaceFileData(exports, ino, data, scratchBase)) {
        continue;
      }
    } else {
      const externalResult = Number(exports.memfs_set_file_external(ino, BigInt(size)));
      if (externalResult < 0) {
        continue;
      }
    }
    exports.memfs_set_mode(ino, mode);
  }
}

export function resolveMemfsPath(
  exports: KernelExports,
  path: string,
  scratchBase?: number,
): number {
  const bytes = encoder.encode(normalizePath(path));
  const scratch = writeSnapshotScratch(exports, scratchBase, bytes);
  return exports.memfs_resolve_path(scratch, bytes.length);
}

export function resolveProcessCwdIno(exports: KernelExports, cwd: string | undefined): number {
  if (!cwd) {
    return 1;
  }
  const ino = resolveMemfsPath(exports, cwd);
  if (ino === 0) {
    throw new Error(`cwd does not exist: ${cwd}`);
  }
  const mode = exports.memfs_inode_mode(ino);
  if ((mode & 0o170000) !== 0o040000) {
    throw new Error(`cwd is not a directory: ${cwd}`);
  }
  return ino;
}

export function readMemfsDirectoryNames(exports: KernelExports, dirIno: number): string[] {
  const bufPtr = kernelScratchBase(exports) + 0x20000;
  const bufLen = 64 * 1024;
  ensureKernelMemoryCapacity(exports, bufPtr + bufLen);
  const byteLen = Number(exports.memfs_readdir_names(dirIno, bufPtr, bufLen));
  if (byteLen < 0) {
    throw new Error(`memfs_readdir_names failed for ino=${dirIno}`);
  }

  const bytes = copyBytes(new Uint8Array(exports.memory.buffer, bufPtr, byteLen));
  const names: string[] = [];
  let start = 0;
  for (let i = 0; i < bytes.length; i += 1) {
    if (bytes[i] !== 0) {
      continue;
    }
    if (i > start) {
      names.push(decoder.decode(bytes.subarray(start, i)));
    }
    start = i + 1;
  }
  return names.sort();
}

export function readMemfsFile(
  exports: KernelExports,
  ino: number,
  pageCache?: SharedArrayBuffer | null,
): Uint8Array | undefined {
  const size = Number(exports.memfs_file_size(ino));
  if (size <= 0) {
    return new Uint8Array();
  }
  if (pageCache) {
    const cached = readInodeMeta(pageCache, ino);
    if (
      cached
      && cached.dataOff !== 0
      && cached.size === size
      && (cached.mode & 0o170000) === 0o100000
    ) {
      return readFileData(pageCache, cached.dataOff, cached.size, 0, cached.size);
    }
  }

  const bufPtr = kernelScratchBase(exports) + 0x30000;
  ensureKernelMemoryCapacity(exports, bufPtr + size);
  const read = Number(exports.memfs_read_file_to(ino, 0n, bufPtr, size));
  if (read < 0) {
    return undefined;
  }
  return new Uint8Array(exports.memory.buffer.slice(bufPtr, bufPtr + read));
}

export function readMemfsSymlink(exports: KernelExports, ino: number): Uint8Array {
  const bufPtr = kernelScratchBase(exports) + 0x34000;
  const bufLen = 4096;
  ensureKernelMemoryCapacity(exports, bufPtr + bufLen);
  const read = Number(exports.memfs_read_link(ino, bufPtr, bufLen));
  if (read < 0) {
    throw new Error(`memfs_read_link failed for ino=${ino}`);
  }
  return new Uint8Array(exports.memory.buffer.slice(bufPtr, bufPtr + read));
}

function snapshotMemfsTree(
  exports: KernelExports,
  ino: number,
  path: string,
  out: FsSnapshotEntry[],
  pageCache?: SharedArrayBuffer | null,
): void {
  const normalized = normalizePath(path);
  const mode = exports.memfs_inode_mode(ino);
  const perms = mode & 0o7777;
  const fileType = mode & 0o170000;

  if (fileType === 0o040000) {
    out.push({ kind: "dir", path: normalized, ino, mode: perms });
    for (const name of readMemfsDirectoryNames(exports, ino)) {
      const childPath = normalized === "/" ? `/${name}` : `${normalized}/${name}`;
      const childIno = resolveMemfsPath(exports, childPath);
      if (childIno === 0) {
        continue;
      }
      snapshotMemfsTree(exports, childIno, childPath, out, pageCache);
    }
    return;
  }

  if (fileType === 0o120000) {
    const target = readMemfsSymlink(exports, ino);
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

  const data = readMemfsFile(exports, ino, pageCache);
  const size = Number(exports.memfs_file_size(ino));
  out.push({
    kind: "file",
    path: normalized,
    ino,
    data,
    size: data?.length ?? size,
    mode: perms,
  });
}

export function snapshotProcessFs(handle: ProcessHandle): FsSnapshotEntry[] {
  const entries: FsSnapshotEntry[] = [];
  snapshotMemfsTree(handle.kernel, 1, "/", entries, handle.sabPageCache);
  return entries;
}

export function snapshotEntriesEqual(left: FsSnapshotEntry[], right: FsSnapshotEntry[]): boolean {
  if (left.length !== right.length) {
    return false;
  }

  for (let i = 0; i < left.length; i += 1) {
    const a = left[i];
    const b = right[i];
    if (
      !a
      || !b
      || a.kind !== b.kind
      || a.path !== b.path
      || a.mode !== b.mode
      || a.ino !== b.ino
      || a.size !== b.size
    ) {
      return false;
    }
    if (a.kind === "symlink" && b.kind === "symlink") {
      const aTarget = a.target ?? new Uint8Array();
      const bTarget = b.target ?? new Uint8Array();
      if (aTarget.length !== bTarget.length) {
        return false;
      }
      for (let j = 0; j < aTarget.length; j += 1) {
        if (aTarget[j] !== bTarget[j]) {
          return false;
        }
      }
    }
    if (a.kind === "file" && b.kind === "file") {
      const aData = a.data ?? new Uint8Array();
      const bData = b.data ?? new Uint8Array();
      if (aData.length !== bData.length) {
        return false;
      }
      for (let j = 0; j < aData.length; j += 1) {
        if (aData[j] !== bData[j]) {
          return false;
        }
      }
    }
  }

  return true;
}

export function applyFsEntryToHandle(handle: ProcessHandle, entry: FsSnapshotEntry): void {
  const exports = handle.kernel;
  const parts = normalizePath(entry.path).split("/").filter(Boolean);
  if (parts.length === 0) {
    return;
  }

  const name = parts.pop()!;
  let parentIno = 1;
  for (const dir of parts) {
    const dirBytes = encoder.encode(dir);
    const dirPtr = writeKernelScratch(exports, dirBytes);
    parentIno = exports.memfs_ensure_dir(parentIno, dirPtr, dirBytes.length, 0o755);
    if (parentIno === 0) {
      return;
    }
  }

  const nameBytes = encoder.encode(name);
  const namePtr = writeKernelScratch(exports, nameBytes);
  const existingIno = resolveMemfsPath(exports, entry.path);
  if (entry.kind === "dir") {
    if (existingIno === 0 && entry.ino !== undefined && entry.ino > 1) {
      const reserveResult = Number(exports.memfs_reserve_inode_until(entry.ino));
      if (reserveResult < 0) {
        return;
      }
    }
    exports.memfs_ensure_dir(parentIno, namePtr, nameBytes.length, entry.mode ?? 0o755);
    return;
  }

  if (entry.kind === "symlink") {
    if (existingIno === 0 && entry.ino !== undefined && entry.ino > 1) {
      const reserveResult = Number(exports.memfs_reserve_inode_until(entry.ino));
      if (reserveResult < 0) {
        return;
      }
    }
    const target = entry.target ?? new Uint8Array();
    const targetPtr = writeKernelScratch(exports, target);
    exports.memfs_create_symlink(parentIno, namePtr, nameBytes.length, targetPtr, target.length);
    return;
  }

  const data = entry.data;
  const size = entry.size ?? data?.length ?? 0;
  const mode = entry.mode ?? 0o644;
  if (existingIno === 0 && entry.ino !== undefined && entry.ino > 1) {
    const reserveResult = Number(exports.memfs_reserve_inode_until(entry.ino));
    if (reserveResult < 0) {
      return;
    }
  }
  const ino = existingIno !== 0
    ? existingIno
    : exports.memfs_create_file(parentIno, namePtr, nameBytes.length);
  if (ino === 0) {
    return;
  }

  if (data) {
    if (!replaceFileData(exports, ino, data)) {
      return;
    }
  } else {
    const sizeResult = Number(exports.memfs_set_file_size_shallow(ino, BigInt(size)));
    if (sizeResult < 0) {
      return;
    }
  }
  exports.memfs_set_mode(ino, mode);
}
