import type { KernelExports } from "../abi.js";
import type { FsSnapshotEntry, RuntimeStat } from "../fs/types.js";
import { bumpDirEpoch, publishInode, readInodeRecord } from "../fs/sab-page-cache.js";
import {
  ensureMemoryCapacityFor,
  markMaterializedMemfsFile,
  MEMFS_S_IFMT,
  MEMFS_S_IFREG,
  normalizePath,
  normalizePermMode,
  pathParts,
  scratchBaseFor,
  writeScratchFor,
} from "./common.js";
import { createKernelWorkerFsSnapshotHelpers } from "./fs-state/snapshot.js";

interface KernelWorkerFsStateDeps {
  getCanonicalExports(): KernelExports;
  getSabPageCache(): SharedArrayBuffer;
  processKernels: Map<number, KernelExports>;
  seedEntries: Map<string, FsSnapshotEntry>;
  encoder: TextEncoder;
  decoder: TextDecoder;
  fanoutCanonicalMemfs(includeData?: boolean): void;
  publishRootInode(mode?: number): void;
  resolvePathIn(targetExports: KernelExports, path: string): number;
  upsertDirSeed(path: string, mode?: number): void;
  upsertFileSeed(path: string, data: Uint8Array, mode?: number): void;
}

export function createKernelWorkerFsState(deps: KernelWorkerFsStateDeps) {
  const MEMFS_S_IFDIR = 0o040000;

  function replaceFileData(targetExports: KernelExports, ino: number, data: Uint8Array): void {
    const truncateResult = Number(targetExports.memfs_truncate_file(ino, BigInt(data.length)));
    if (truncateResult < 0) {
      const mode = targetExports.memfs_inode_mode(ino);
      throw new Error(`failed to resize inode: ino=${ino} mode=${mode.toString(8)} newSize=${data.length}`);
    }
    if (data.length > 0) {
      const dataPtr = writeScratchFor(targetExports, data);
      const written = Number(targetExports.memfs_write_file_at(ino, 0n, dataPtr, data.length));
      if (written < 0 || written !== data.length) {
        throw new Error(`failed to write inode: ${ino}`);
      }
    }
    markMaterializedMemfsFile(targetExports, ino);
  }

  function ensureDirectory(path: string, mode = 0o755): number {
    const targetExports = deps.getCanonicalExports();
    const parts = pathParts(path);
    let parentIno = 1;
    const perms = normalizePermMode(mode, 0o755);

    for (const dir of parts) {
      const dirBytes = deps.encoder.encode(dir);
      const dirPtr = writeScratchFor(targetExports, dirBytes);
      const prevParent = parentIno;
      parentIno = targetExports.memfs_ensure_dir(parentIno, dirPtr, dirBytes.length, perms);
      if (parentIno === 0) {
        throw new Error(`failed to create directory: ${path}`);
      }
      publishInode(deps.getSabPageCache(), parentIno, {
        mode: 0o40000 | perms,
        size: 0,
        parentIno: prevParent,
        name: dirBytes,
        nextSibling: 0,
      }, null);
    }

    if (parts.length > 0) {
      bumpDirEpoch(deps.getSabPageCache());
    }

    return parentIno;
  }

  function resolvePath(path: string): number {
    return deps.resolvePathIn(deps.getCanonicalExports(), path);
  }

  function writeFile(path: string, data: Uint8Array, mode = 0o644, fanout = true): void {
    const targetExports = deps.getCanonicalExports();
    const normalized = normalizePath(path);
    const parts = pathParts(normalized);
    const fileName = parts.pop();
    if (!fileName) {
      throw new Error(`invalid file path: ${path}`);
    }

    const parentPath = parts.length === 0 ? "/" : `/${parts.join("/")}`;
    const parentIno = ensureDirectory(parentPath);
    const fileMode = normalizePermMode(mode, 0o644);

    const existingIno = resolvePath(normalized);
    const fileBytes = deps.encoder.encode(fileName);
    const namePtr = writeScratchFor(targetExports, fileBytes);

    const ino = existingIno !== 0
      ? existingIno
      : targetExports.memfs_create_file(parentIno, namePtr, fileBytes.length);
    if (ino === 0) {
      throw new Error(`failed to create file: ${path}`);
    }

    try {
      replaceFileData(targetExports, ino, data);
    } catch (error) {
      throw new Error(`failed to write file ${normalized}: ${error instanceof Error ? error.message : String(error)}`);
    }
    const chmodResult = Number(targetExports.memfs_set_mode(ino, fileMode));
    if (chmodResult < 0) {
      throw new Error(`failed to set mode on file: ${path}`);
    }

    publishInode(deps.getSabPageCache(), ino, {
      mode: 0o100000 | fileMode,
      size: data.length,
      parentIno,
      name: fileBytes,
      nextSibling: 0,
    }, data);
    bumpDirEpoch(deps.getSabPageCache());
    deps.upsertFileSeed(normalized, data, fileMode);
    if (fanout) {
      deps.fanoutCanonicalMemfs();
    }
  }

  function ensurePublishedInodeInTarget(
    targetExports: KernelExports,
    ino: number,
    published: ReturnType<typeof readInodeRecord>,
    seen: Set<number> = new Set(),
  ): void {
    if (ino <= 0 || seen.has(ino)) {
      return;
    }
    seen.add(ino);
    if (ino === 1) {
      return;
    }
    if ((targetExports.memfs_inode_mode(ino) & MEMFS_S_IFMT) !== 0) {
      return;
    }
    if (!published || published.name.length === 0) {
      return;
    }

    const parentIno = published.parentIno > 0 ? published.parentIno : 1;
    if (parentIno !== ino) {
      ensurePublishedInodeInTarget(
        targetExports,
        parentIno,
        readInodeRecord(deps.getSabPageCache(), parentIno),
        seen,
      );
    }

    const reserveResult = Number(targetExports.memfs_reserve_inode_until(ino));
    if (reserveResult < 0) {
      throw new Error(`failed to reserve inode: ${ino}`);
    }
    const namePtr = writeScratchFor(targetExports, published.name);
    const perms = normalizePermMode(published.mode, 0o644);
    const fileType = published.mode & MEMFS_S_IFMT;

    if (fileType === MEMFS_S_IFDIR) {
      const created = targetExports.memfs_ensure_dir(parentIno, namePtr, published.name.length, perms);
      if (created === 0) {
        throw new Error(`failed to ensure directory inode: ${ino}`);
      }
      return;
    }

    if (fileType !== MEMFS_S_IFREG) {
      return;
    }

    const created = targetExports.memfs_create_file(parentIno, namePtr, published.name.length);
    if (created === 0) {
      throw new Error(`failed to create inode: ${ino}`);
    }
    const sizeResult = Number(targetExports.memfs_set_file_size_shallow(ino, BigInt(published.size)));
    if (sizeResult < 0) {
      throw new Error(`failed to seed inode metadata: ${ino}`);
    }
    const modeResult = Number(targetExports.memfs_set_mode(ino, perms));
    if (modeResult < 0) {
      throw new Error(`failed to set inode mode: ${ino}`);
    }
  }

  function readFileByInode(sourceExports: KernelExports, ino: number): Uint8Array {
    const size = Number(sourceExports.memfs_file_size(ino));
    if (size <= 0) {
      return new Uint8Array();
    }

    const bufPtr = scratchBaseFor(sourceExports) + 0x4000;
    ensureMemoryCapacityFor(sourceExports.memory, bufPtr + size);
    const read = Number(sourceExports.memfs_read_file_to(ino, 0n, bufPtr, size));
    if (read < 0) {
      throw new Error(`failed to read inode: ${ino}`);
    }

    return new Uint8Array(sourceExports.memory.buffer.slice(bufPtr, bufPtr + read));
  }

  function writeFileByInode(targetExports: KernelExports, ino: number, data: Uint8Array): void {
    replaceFileData(targetExports, ino, data);
  }

  function syncFileDataIntoTarget(
    targetExports: KernelExports,
    ino: number,
    mode: number,
    data: Uint8Array,
    published: ReturnType<typeof readInodeRecord>,
  ): void {
    ensurePublishedInodeInTarget(targetExports, ino, published);
    writeFileByInode(targetExports, ino, data);
    const modeResult = Number(targetExports.memfs_set_mode(ino, mode));
    if (modeResult < 0) {
      throw new Error(`failed to set inode mode: ${ino}`);
    }
  }

  function syncFileDataMutation(sourceExports: KernelExports, ino: number): boolean {
    const canonicalExports = deps.getCanonicalExports();
    const mode = sourceExports.memfs_inode_mode(ino);
    if ((mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
      return false;
    }

    const published = readInodeRecord(deps.getSabPageCache(), ino) ?? {
      dataOff: 0,
      size: 0,
      mode,
      parentIno: 0,
      name: new Uint8Array(),
      nextSibling: 0,
    };

    const data = readFileByInode(sourceExports, ino);
    ensurePublishedInodeInTarget(canonicalExports, ino, published);
    writeFileByInode(canonicalExports, ino, data);
    publishInode(deps.getSabPageCache(), ino, {
      mode,
      size: data.length,
      parentIno: published.parentIno,
      name: published.name,
      nextSibling: published.nextSibling,
    }, data);

    syncFileDataIntoTarget(canonicalExports, ino, mode, data, published);
    for (const memberExports of deps.processKernels.values()) {
      if (memberExports === sourceExports) {
        continue;
      }
      syncFileDataIntoTarget(memberExports, ino, mode, data, published);
    }

    return true;
  }

  function readFile(path: string): Uint8Array {
    const targetExports = deps.getCanonicalExports();
    const ino = resolvePath(path);
    if (ino === 0) {
      throw new Error(`file not found: ${path}`);
    }

    const size = Number(targetExports.memfs_file_size(ino));
    const bufPtr = scratchBaseFor(targetExports) + 0x4000;
    ensureMemoryCapacityFor(targetExports.memory, bufPtr + size);
    const read = Number(targetExports.memfs_read_file_to(ino, 0n, bufPtr, size));
    if (read < 0) {
      throw new Error(`failed to read file: ${path}`);
    }

    return new Uint8Array(targetExports.memory.buffer.slice(bufPtr, bufPtr + read));
  }

  function readdir(path: string): string[] {
    const targetExports = deps.getCanonicalExports();
    const ino = resolvePath(path);
    if (ino === 0) {
      throw new Error(`directory not found: ${path}`);
    }

    const bufPtr = scratchBaseFor(targetExports) + 0x8000;
    const bufLen = 64 * 1024;
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
        names.push(deps.decoder.decode(bytes.subarray(start, i)));
      }
      start = i + 1;
    }
    return names;
  }

  function mkdir(path: string, mode = 0o755, fanout = true): void {
    const normalized = normalizePath(path);
    ensureDirectory(normalized, mode);
    deps.upsertDirSeed(normalized, mode);
    if (fanout) {
      deps.fanoutCanonicalMemfs();
    }
  }
  const {
    loadFsSnapshot,
    rebuildPageCacheFromCanonical,
    snapshotCanonicalFs,
  } = createKernelWorkerFsSnapshotHelpers({
    getCanonicalExports: deps.getCanonicalExports,
    getSabPageCache: deps.getSabPageCache,
    seedEntries: deps.seedEntries,
    encoder: deps.encoder,
    publishRootInode: deps.publishRootInode,
    resolvePath,
    readdir,
    readFile,
    writeFile,
    mkdir,
    fanoutCanonicalMemfs: deps.fanoutCanonicalMemfs,
  });

  function statPath(path: string): RuntimeStat {
    const targetExports = deps.getCanonicalExports();
    const ino = resolvePath(path);
    if (ino === 0) {
      throw new Error(`path not found: ${path}`);
    }

    return {
      ino,
      mode: targetExports.memfs_inode_mode(ino),
      nlink: targetExports.memfs_inode_nlink(ino),
      uid: targetExports.memfs_inode_uid(ino),
      gid: targetExports.memfs_inode_gid(ino),
      size: Number(targetExports.memfs_file_size(ino)),
    };
  }

  return {
    ensureDirectory,
    loadFsSnapshot,
    mkdir,
    readFile,
    readdir,
    rebuildPageCacheFromCanonical,
    resolvePath,
    snapshotCanonicalFs,
    statPath,
    syncFileDataMutation,
    writeFile,
  };
}
