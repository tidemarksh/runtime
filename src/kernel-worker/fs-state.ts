import type { KernelExports } from "../abi.js";
import type { FileBackingStat } from "../fs/sab-page-cache.js";
import type { FsSnapshotBackingExtent, FsSnapshotEntry, RuntimeStat } from "../fs/types.js";
import {
  collectDirtyResidentFileWritebackRanges,
  bumpDirEpoch,
  hasDirtyResidentPagesForInode,
  markDirtyResidentFileDataClean,
  MAX_INODES,
  MemoryFileBackingStore,
  publishInode,
  readDirtyResidentFileData,
  readDirtyResidentFileRangeData,
  readInodeRecord,
  truncateDirtyResidentFileData,
  writeDirtyResidentFileData,
} from "../fs/sab-page-cache.js";
import {
  ensureMemoryCapacityFor,
  MEMFS_S_IFMT,
  MEMFS_S_IFREG,
  normalizePath,
  normalizePermMode,
  pathParts,
  scratchBaseFor,
  writeScratchFor,
} from "./common.js";
import {
  ChunkedBackingSource,
  Uint8ArrayBackingSource,
  type KernelWorkerBackingSource,
} from "./backing-source.js";
import {
  fetchRemoteBackingSourceRange,
} from "./remote-backing-source.js";
import {
  readOpfsBackingSourceRange,
  type OpfsBackingFileHandle,
} from "./opfs-backing-source.js";
import { createKernelWorkerFsSnapshotHelpers } from "./fs-state/snapshot.js";

export const KERNEL_WORKER_FS_STATE_OWNERSHIP = "transitional-runtime-owner" as const;

type AsyncBackingSource =
  | { kind: "remote"; byteLength: number; url: string }
  | { kind: "opfs"; byteLength: number; handle: OpfsBackingFileHandle };

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
  upsertFileSeed(
    path: string,
    data: Uint8Array,
    mode?: number,
    options?: { size?: number },
  ): void;
  upsertSymlinkSeed(path: string, target: Uint8Array, mode?: number): void;
}

export function createKernelWorkerFsState(deps: KernelWorkerFsStateDeps) {
  const MEMFS_S_IFDIR = 0o040000;
  const backingStore = new MemoryFileBackingStore();
  const fileObjectIdsByIno = new Map<number, number>();
  const backingSourcesByIdentity = new Map<string, KernelWorkerBackingSource>();
  const asyncBackingSourcesByIdentity = new Map<string, AsyncBackingSource>();
  const backingExtentsByIno = new Map<number, FsSnapshotBackingExtent>();

  function replaceBackingFile(ino: number, data: Uint8Array): FileBackingStat {
    backingExtentsByIno.delete(ino);
    const fileObjectId = fileObjectIdsByIno.get(ino);
    if (fileObjectId === undefined) {
      const stat = backingStore.createFile(data);
      fileObjectIdsByIno.set(ino, stat.fileObjectId);
      return stat;
    }
    return backingStore.replace(fileObjectId, data);
  }

  function writeBackingFileRange(ino: number, offset: number, data: Uint8Array): FileBackingStat {
    backingExtentsByIno.delete(ino);
    const fileObjectId = fileObjectIdsByIno.get(ino);
    if (fileObjectId === undefined) {
      const stat = backingStore.createFile();
      fileObjectIdsByIno.set(ino, stat.fileObjectId);
      return backingStore.write(stat.fileObjectId, offset, data);
    }
    return backingStore.write(fileObjectId, offset, data);
  }

  function truncateBackingFile(ino: number, size: number): FileBackingStat {
    backingExtentsByIno.delete(ino);
    const fileObjectId = fileObjectIdsByIno.get(ino);
    if (fileObjectId === undefined) {
      const stat = backingStore.createFile();
      fileObjectIdsByIno.set(ino, stat.fileObjectId);
      return backingStore.truncate(stat.fileObjectId, size);
    }
    return backingStore.truncate(fileObjectId, size);
  }

  function backingMetaForInode(ino: number): Pick<FileBackingStat, "fileObjectId" | "generation"> | undefined {
    const fileObjectId = fileObjectIdsByIno.get(ino);
    if (fileObjectId === undefined) {
      return undefined;
    }
    const stat = backingStore.stat(fileObjectId);
    return stat ? { fileObjectId: stat.fileObjectId, generation: stat.generation } : undefined;
  }

  function registerBackingSource(sourceIdentity: string, data: Uint8Array): void {
    if (sourceIdentity.length === 0) {
      throw new Error("backing source identity must not be empty");
    }
    backingSourcesByIdentity.set(sourceIdentity, new Uint8ArrayBackingSource(data));
  }

  function registerBackingSourceRange(
    sourceIdentity: string,
    byteLength: number,
    offset: number,
    data: Uint8Array,
  ): void {
    if (sourceIdentity.length === 0) {
      throw new Error("backing source identity must not be empty");
    }
    const existing = backingSourcesByIdentity.get(sourceIdentity);
    const source = existing instanceof ChunkedBackingSource && existing.byteLength === byteLength
      ? existing
      : new ChunkedBackingSource(byteLength);
    source.putRange(offset, data);
    backingSourcesByIdentity.set(sourceIdentity, source);
  }

  function registerRemoteBackingSource(sourceIdentity: string, byteLength: number, url: string): void {
    if (sourceIdentity.length === 0) {
      throw new Error("backing source identity must not be empty");
    }
    if (!Number.isInteger(byteLength) || byteLength < 0) {
      throw new RangeError(`invalid remote backing source byte length: ${byteLength}`);
    }
    asyncBackingSourcesByIdentity.set(sourceIdentity, { kind: "remote", byteLength, url });
  }

  function registerOpfsBackingSource(
    sourceIdentity: string,
    byteLength: number,
    handle: OpfsBackingFileHandle,
  ): void {
    if (sourceIdentity.length === 0) {
      throw new Error("backing source identity must not be empty");
    }
    if (!Number.isInteger(byteLength) || byteLength < 0) {
      throw new RangeError(`invalid OPFS backing source byte length: ${byteLength}`);
    }
    asyncBackingSourcesByIdentity.set(sourceIdentity, { kind: "opfs", byteLength, handle });
  }

  function registerBackingSourceObject(sourceIdentity: string, source: KernelWorkerBackingSource): void {
    if (sourceIdentity.length === 0) {
      throw new Error("backing source identity must not be empty");
    }
    if (!Number.isInteger(source.byteLength) || source.byteLength < 0) {
      throw new RangeError(`invalid backing source byte length: ${source.byteLength}`);
    }
    backingSourcesByIdentity.set(sourceIdentity, source);
  }

  function sourceRangeForInode(
    ino: number,
    fileOffset: number,
    length: number,
    expectedSize: number,
  ): { sourceIdentity: string; byteLength: number; offset: number; length: number } | null {
    const extent = backingExtentsByIno.get(ino);
    if (
      !extent
      || expectedSize < 0
      || !Number.isInteger(fileOffset)
      || !Number.isInteger(length)
      || fileOffset < 0
      || length <= 0
      || fileOffset >= expectedSize
    ) {
      return null;
    }
    const sourceOffset = Math.floor(extent.offset ?? 0);
    const extentLength = Math.floor(extent.length ?? expectedSize);
    if (
      sourceOffset < 0
      || extentLength < 0
      || extentLength !== expectedSize
    ) {
      return null;
    }
    return {
      sourceIdentity: extent.sourceIdentity,
      byteLength: sourceOffset + extentLength,
      offset: sourceOffset + fileOffset,
      length: Math.min(length, expectedSize - fileOffset),
    };
  }

  function registerSnapshotBackingExtent(ino: number, backingExtent: FsSnapshotBackingExtent | undefined): void {
    if (!backingExtent) {
      backingExtentsByIno.delete(ino);
      return;
    }
    backingExtentsByIno.set(ino, { ...backingExtent });
  }

  function clearSeedBackingExtentForInode(ino: number): void {
    if (!Number.isInteger(ino) || ino <= 0) {
      return;
    }
    const canonicalExports = deps.getCanonicalExports();
    for (const [path, entry] of deps.seedEntries) {
      if (entry.kind !== "file" || !entry.backingExtent) {
        continue;
      }
      if (deps.resolvePathIn(canonicalExports, path) !== ino) {
        continue;
      }
      const { backingExtent: _backingExtent, ...nextEntry } = entry;
      deps.seedEntries.set(path, nextEntry);
    }
  }

  function materializeBackingSourceForInode(ino: number, expectedSize: number): Uint8Array | null {
    const data = readBackingSourceRangeForInode(ino, 0, expectedSize, expectedSize);
    if (!data) {
      return null;
    }
    replaceBackingFile(ino, data);
    return readBackingFileByInode(ino);
  }

  function readBackingSourceRangeForInode(
    ino: number,
    fileOffset: number,
    length: number,
    expectedSize: number,
  ): Uint8Array | null {
    const extent = backingExtentsByIno.get(ino);
    if (
      !extent
      || expectedSize < 0
      || !Number.isInteger(fileOffset)
      || !Number.isInteger(length)
      || fileOffset < 0
      || length <= 0
      || fileOffset >= expectedSize
    ) {
      return null;
    }
    const source = backingSourcesByIdentity.get(extent.sourceIdentity);
    if (!source) {
      return null;
    }
    const sourceOffset = Math.floor(extent.offset ?? 0);
    const extentLength = Math.floor(extent.length ?? expectedSize);
    if (
      sourceOffset < 0
      || extentLength < 0
      || extentLength !== expectedSize
      || sourceOffset + extentLength > source.byteLength
    ) {
      return null;
    }
    const readLength = Math.min(length, expectedSize - fileOffset);
    return source.readRange(sourceOffset + fileOffset, readLength);
  }

  async function ensureBackingSourceRangeForInode(
    ino: number,
    fileOffset: number,
    length: number,
    expectedSize: number,
  ): Promise<boolean> {
    if (readBackingSourceRangeForInode(ino, fileOffset, length, expectedSize) !== null) {
      return true;
    }
    const range = sourceRangeForInode(ino, fileOffset, length, expectedSize);
    if (!range) {
      return false;
    }
    const asyncSource = asyncBackingSourcesByIdentity.get(range.sourceIdentity);
    if (!asyncSource || asyncSource.byteLength < range.offset + range.length) {
      return false;
    }
    const fetched = asyncSource.kind === "remote"
      ? await fetchRemoteBackingSourceRange(asyncSource.url, asyncSource.byteLength, range.offset, range.length)
      : await readOpfsBackingSourceRange(asyncSource.handle, asyncSource.byteLength, range.offset, range.length);
    registerBackingSourceRange(range.sourceIdentity, fetched.byteLength, fetched.offset, fetched.data);
    return readBackingSourceRangeForInode(ino, fileOffset, length, expectedSize) !== null;
  }

  function createSourceBackedFileForInode(ino: number, expectedSize: number): number | null {
    const extent = backingExtentsByIno.get(ino);
    if (!extent || expectedSize < 0) {
      return null;
    }
    const source = backingSourcesByIdentity.get(extent.sourceIdentity);
    if (!source) {
      return null;
    }
    const sourceOffset = Math.floor(extent.offset ?? 0);
    const extentLength = Math.floor(extent.length ?? expectedSize);
    if (
      sourceOffset < 0
      || extentLength < 0
      || extentLength !== expectedSize
      || sourceOffset + extentLength > source.byteLength
    ) {
      return null;
    }
    const stat = backingStore.createSourceFile({
      byteLength: expectedSize,
      readRange(offset, length) {
        if (
          !Number.isInteger(offset)
          || !Number.isInteger(length)
          || offset < 0
          || length < 0
          || offset + length > expectedSize
        ) {
          return null;
        }
        return source.readRange(sourceOffset + offset, length);
      },
    });
    fileObjectIdsByIno.set(ino, stat.fileObjectId);
    return stat.fileObjectId;
  }

  function residentPageMapIsNewerThanBacking(ino: number): boolean {
    if (hasDirtyResidentPagesForInode(deps.getSabPageCache(), ino)) {
      return true;
    }
    const published = readInodeRecord(deps.getSabPageCache(), ino);
    if (!published || published.fileObjectId === 0) {
      return false;
    }
    const fileObjectId = fileObjectIdsByIno.get(ino);
    const stat = fileObjectId === undefined ? null : backingStore.stat(fileObjectId);
    return !!stat && (published.generation !== stat.generation || published.size !== stat.size);
  }

  function readBackingFileByInode(ino: number): Uint8Array | null {
    if (residentPageMapIsNewerThanBacking(ino)) {
      return readDirtyResidentFileData(deps.getSabPageCache(), ino, readCleanBackingFileRangeByInode);
    }
    const fileObjectId = fileObjectIdsByIno.get(ino);
    if (fileObjectId === undefined) {
      return null;
    }
    const stat = backingStore.stat(fileObjectId);
    if (!stat) {
      return null;
    }
    return backingStore.read(fileObjectId, 0, stat.size);
  }

  function readCleanBackingFileRangeByInode(ino: number, offset: number, length: number): Uint8Array | null {
    let fileObjectId = fileObjectIdsByIno.get(ino);
    if (fileObjectId === undefined) {
      const size = Number(deps.getCanonicalExports().memfs_file_size(ino));
      return readBackingSourceRangeForInode(ino, offset, length, size);
    }
    if (
      fileObjectId === undefined
      || !Number.isInteger(offset)
      || !Number.isInteger(length)
      || offset < 0
      || length <= 0
    ) {
      return null;
    }
    const stat = backingStore.stat(fileObjectId);
    if (!stat) {
      return null;
    }
    const published = readInodeRecord(deps.getSabPageCache(), ino);
    const publishedSize = published?.size ?? stat.size;
    if (offset >= publishedSize) {
      return null;
    }
    if (offset >= stat.size) {
      return publishedSize > stat.size && offset + length <= publishedSize ? new Uint8Array(length) : null;
    }
    const backingLen = Math.min(length, stat.size - offset);
    const data = backingStore.read(fileObjectId, offset, backingLen);
    if (data.length !== backingLen) {
      return null;
    }
    if (backingLen === length) {
      return data;
    }
    if (publishedSize <= stat.size) {
      return data;
    }
    if (offset + length > publishedSize) {
      return null;
    }
    const out = new Uint8Array(length);
    out.set(data);
    return out;
  }

  function readBackingFileRangeByInode(ino: number, offset: number, length: number): Uint8Array | null {
    if (residentPageMapIsNewerThanBacking(ino)) {
      return readDirtyResidentFileRangeData(
        deps.getSabPageCache(),
        ino,
        offset,
        length,
        readCleanBackingFileRangeByInode,
      );
    }
    return readCleanBackingFileRangeByInode(ino, offset, length);
  }

  function writeDirtyResidentFileByInode(
    ino: number,
    offset: number,
    data: Uint8Array,
    sourceExports: KernelExports = deps.getCanonicalExports(),
  ): number {
    if (!Number.isInteger(ino) || ino <= 0 || ino >= MAX_INODES || offset < 0) {
      return -22;
    }
    if (data.length === 0) {
      return 0;
    }
    try {
      const fileObjectId = fileObjectIdsByIno.get(ino);
      const expectedSize = Number(sourceExports.memfs_file_size(ino));
      const sourceMode = sourceExports.memfs_inode_mode(ino);
      const sourceIsRegular = (sourceMode & MEMFS_S_IFMT) === MEMFS_S_IFREG;
      if (fileObjectId === undefined) {
        const sourceFileObjectId = createSourceBackedFileForInode(ino, expectedSize);
        const published = readInodeRecord(deps.getSabPageCache(), ino);
        const sourceTruncatedEmpty = sourceIsRegular && expectedSize === 0;
        if (
          sourceFileObjectId === null
          && !published?.dataOff
          && (published?.size ?? expectedSize) > 0
          && !sourceTruncatedEmpty
        ) {
          return -95;
        }
      }
      const published = readInodeRecord(deps.getSabPageCache(), ino);
      if (!published) {
        publishInode(deps.getSabPageCache(), ino, {
          mode: sourceMode || 0o100644,
          size: expectedSize,
          parentIno: 0,
          name: new Uint8Array(),
          nextSibling: 0,
          ...backingMetaForInode(ino),
        });
      } else if (sourceIsRegular && published.size !== expectedSize) {
        publishInode(deps.getSabPageCache(), ino, {
          mode: sourceMode || published.mode,
          size: expectedSize,
          parentIno: published.parentIno,
          name: published.name,
          nextSibling: published.nextSibling,
          ...backingMetaForInode(ino),
        });
      }
      const written = writeDirtyResidentFileData(deps.getSabPageCache(), ino, offset, data, readBackingFileRangeByInode);
      if (written >= 0) {
        clearSeedBackingExtentForInode(ino);
      }
      return written;
    } catch {
      return -5;
    }
  }

  function flushDirtyResidentFileByInode(ino: number): boolean {
    if (!Number.isInteger(ino) || ino <= 0 || ino >= MAX_INODES) {
      return false;
    }
    const sab = deps.getSabPageCache();
    const hasDirtyPages = hasDirtyResidentPagesForInode(sab, ino);
    if (!hasDirtyPages && !residentPageMapIsNewerThanBacking(ino)) {
      return true;
    }
    const record = readInodeRecord(sab, ino);
    if (!record) {
      return false;
    }
    const ranges = hasDirtyPages ? collectDirtyResidentFileWritebackRanges(sab, ino) : [];
    if (!ranges) {
      return false;
    }
    let stat: FileBackingStat | null = null;
    for (const range of ranges) {
      stat = writeBackingFileRange(ino, range.offset, range.bytes);
    }
    stat = truncateBackingFile(ino, record.size);
    const cleaned = markDirtyResidentFileDataClean(sab, ino, stat.fileObjectId, stat.generation);
    if (cleaned) {
      backingExtentsByIno.delete(ino);
      clearSeedBackingExtentForInode(ino);
    }
    return cleaned;
  }

  function truncateDirtyResidentFileByInode(ino: number, size: number): number {
    if (!Number.isInteger(ino) || ino <= 0 || ino >= MAX_INODES || size < 0 || size > 0xffffffff) {
      return -22;
    }
    try {
      const expectedSize = Number(deps.getCanonicalExports().memfs_file_size(ino));
      if (fileObjectIdsByIno.get(ino) === undefined) {
        createSourceBackedFileForInode(ino, expectedSize);
      }
      if (!readInodeRecord(deps.getSabPageCache(), ino)) {
        publishInode(deps.getSabPageCache(), ino, {
          mode: deps.getCanonicalExports().memfs_inode_mode(ino) || 0o100644,
          size: expectedSize,
          parentIno: 0,
          name: new Uint8Array(),
          nextSibling: 0,
          ...backingMetaForInode(ino),
        });
      }
      const truncated = truncateDirtyResidentFileData(
        deps.getSabPageCache(),
        ino,
        size,
        readCleanBackingFileRangeByInode,
      );
      if (truncated >= 0) {
        clearSeedBackingExtentForInode(ino);
      }
      return truncated;
    } catch {
      return -5;
    }
  }

  function hasBackingFile(ino: number): boolean {
    return readBackingFileByInode(ino) !== null;
  }

  function replaceFileDataExternal(
    targetExports: KernelExports,
    ino: number,
    meta: {
      mode: number;
      size: number;
      parentIno: number;
      name: Uint8Array;
      nextSibling: number;
    },
    data: Uint8Array,
  ): void {
    const pageCache = deps.getSabPageCache();
    const stat = replaceBackingFile(ino, data);
    publishInode(pageCache, ino, {
      ...meta,
      fileObjectId: stat.fileObjectId,
      generation: stat.generation,
    });
    const published = readInodeRecord(pageCache, ino);
    if (!published || published.size !== data.length) {
      throw new Error(`failed to publish inode metadata: ino=${ino} dataBytes=${data.length}`);
    }
    const externalized = Number(targetExports.memfs_set_file_external(ino, BigInt(data.length)));
    if (externalized < 0) {
      throw new Error(`failed to externalize inode: ${ino}`);
    }
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
      });
    }

    if (parts.length > 0) {
      bumpDirEpoch(deps.getSabPageCache());
    }

    return parentIno;
  }

  function resolvePath(path: string): number {
    return deps.resolvePathIn(deps.getCanonicalExports(), path);
  }

  function readLinkTarget(targetExports: KernelExports, ino: number): string | null {
    if ((targetExports.memfs_inode_mode(ino) & MEMFS_S_IFMT) !== 0o120000) {
      return null;
    }
    const bufPtr = scratchBaseFor(targetExports) + 0x8000;
    const bufLen = 4096;
    ensureMemoryCapacityFor(targetExports.memory, bufPtr + bufLen);
    const len = Number(targetExports.memfs_read_link(ino, bufPtr, bufLen));
    if (len <= 0) {
      return null;
    }
    return deps.decoder.decode(new Uint8Array(targetExports.memory.buffer, bufPtr, len));
  }

  function resolvePathFollowingSymlinks(path: string): number {
    const targetExports = deps.getCanonicalExports();
    let resolvedParts: string[] = [];
    let remainingParts = pathParts(normalizePath(path));
    let follows = 0;

    while (remainingParts.length > 0) {
      const part = remainingParts.shift()!;
      const candidatePath = normalizePath(`/${[...resolvedParts, part].join("/")}`);
      const ino = resolvePath(candidatePath);
      if (ino === 0) {
        return 0;
      }

      const target = readLinkTarget(targetExports, ino);
      if (!target) {
        resolvedParts.push(part);
        continue;
      }
      follows += 1;
      if (follows > 40) {
        return 0;
      }

      const targetPath = target.startsWith("/")
        ? normalizePath(target)
        : normalizePath(`/${[...resolvedParts, target].join("/")}`);
      remainingParts = [...pathParts(targetPath), ...remainingParts];
      resolvedParts = [];
    }

    return resolvePath(normalizePath(`/${resolvedParts.join("/")}`));
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
      const published = readInodeRecord(deps.getSabPageCache(), ino);
      replaceFileDataExternal(targetExports, ino, {
        mode: 0o100000 | fileMode,
        size: data.length,
        parentIno,
        name: fileBytes,
        nextSibling: published?.nextSibling ?? 0,
      }, data);
    } catch (error) {
      throw new Error(`failed to write file ${normalized}: ${error instanceof Error ? error.message : String(error)}`);
    }
    const chmodResult = Number(targetExports.memfs_set_mode(ino, fileMode));
    if (chmodResult < 0) {
      throw new Error(`failed to set mode on file: ${path}`);
    }

    bumpDirEpoch(deps.getSabPageCache());
    deps.upsertFileSeed(normalized, data, fileMode, {
      size: data.length,
    });
    if (fanout) {
      deps.fanoutCanonicalMemfs();
    }
  }

  function symlink(path: string, target: Uint8Array, mode = 0o777, fanout = true): void {
    const targetExports = deps.getCanonicalExports();
    const normalized = normalizePath(path);
    const parts = pathParts(normalized);
    const linkName = parts.pop();
    if (!linkName) {
      throw new Error(`invalid symlink path: ${path}`);
    }
    const parentPath = parts.length === 0 ? "/" : `/${parts.join("/")}`;
    const parentIno = ensureDirectory(parentPath);
    const parentPathIno = resolvePath(parentPath);
    const parentMode = parentIno !== 0 ? targetExports.memfs_inode_mode(parentIno) : 0;
    const existingIno = resolvePath(normalized);
    if (existingIno !== 0) {
      return;
    }
    const nameBytes = deps.encoder.encode(linkName);
    const namePtr = writeScratchFor(targetExports, nameBytes);
    const targetPtr = writeScratchFor(targetExports, target, 0x1000);
    const lookupIno = targetExports.memfs_lookup(parentIno, namePtr, nameBytes.length);
    const lookupMode = lookupIno !== 0 ? targetExports.memfs_inode_mode(lookupIno) : 0;
    const ino = targetExports.memfs_create_symlink(parentIno, namePtr, nameBytes.length, targetPtr, target.length);
    if (ino === 0) {
      const postExistingIno = resolvePath(normalized);
      throw new Error(
        `failed to create symlink: ${path}`
        + ` parentPath=${parentPath}`
        + ` parentIno=${parentIno}`
        + ` parentPathIno=${parentPathIno}`
        + ` parentMode=${parentMode.toString(8)}`
        + ` existingIno=${existingIno}`
        + ` lookupIno=${lookupIno}`
        + ` lookupMode=${lookupMode.toString(8)}`
        + ` postExistingIno=${postExistingIno}`
        + ` targetLen=${target.length}`
        + ` inodeCount=${targetExports.memfs_inode_count()}`
        + ` dirEntryCount=${targetExports.memfs_dir_entry_count()}`
        + ` dataUsed=${targetExports.memfs_data_used()}`
        + ` dataCap=${targetExports.memfs_data_cap()}`
        + ` lastStage=${targetExports.memfs_last_create_symlink_stage()}`
        + ` lastArg0=${targetExports.memfs_last_create_symlink_arg0().toString()}`
        + ` lastArg1=${targetExports.memfs_last_create_symlink_arg1().toString()}`,
      );
    }
    publishInode(deps.getSabPageCache(), ino, {
      mode: 0o120000 | normalizePermMode(mode, 0o777),
      size: target.length,
      parentIno,
      name: nameBytes,
      nextSibling: 0,
    });
    bumpDirEpoch(deps.getSabPageCache());
    deps.upsertSymlinkSeed(normalized, target, mode);
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

  function syncExternalFileDataIntoTarget(
    targetExports: KernelExports,
    ino: number,
    mode: number,
    size: number,
    published: ReturnType<typeof readInodeRecord>,
  ): void {
    ensurePublishedInodeInTarget(targetExports, ino, published);
    const externalized = Number(targetExports.memfs_set_file_external(ino, BigInt(size)));
    if (externalized < 0) {
      throw new Error(`failed to externalize inode: ${ino}`);
    }
    const modeResult = Number(targetExports.memfs_set_mode(ino, mode));
    if (modeResult < 0) {
      throw new Error(`failed to set inode mode: ${ino}`);
    }
  }

  function trySyncExternalFileDataIntoTarget(
    targetExports: KernelExports,
    ino: number,
    mode: number,
    size: number,
    published: ReturnType<typeof readInodeRecord>,
  ): boolean {
    try {
      syncExternalFileDataIntoTarget(targetExports, ino, mode, size, published);
      return true;
    } catch {
      return false;
    }
  }

  function tryEnsurePublishedInodeInTarget(
    targetExports: KernelExports,
    ino: number,
    published: ReturnType<typeof readInodeRecord>,
  ): boolean {
    try {
      ensurePublishedInodeInTarget(targetExports, ino, published);
      return true;
    } catch {
      return false;
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
        fileObjectId: 0,
        generation: 0,
      };

    try {
      ensurePublishedInodeInTarget(canonicalExports, ino, published);
      if ((canonicalExports.memfs_inode_mode(ino) & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
        return false;
      }
      const size = Number(sourceExports.memfs_file_size(ino));
      if (residentPageMapIsNewerThanBacking(ino)) {
        clearSeedBackingExtentForInode(ino);
        syncExternalFileDataIntoTarget(canonicalExports, ino, mode, size, published);
        for (const memberExports of deps.processKernels.values()) {
          trySyncExternalFileDataIntoTarget(memberExports, ino, mode, size, published);
        }
        return true;
      }
      return false;
    } catch {
      return false;
    }
  }

  function syncFileDataMutationByPath(sourceExports: KernelExports, path: string): boolean {
    const normalized = normalizePath(path);
    const ino = deps.resolvePathIn(sourceExports, normalized);
    if (ino === 0) {
      return false;
    }
    return syncFileDataMutation(sourceExports, ino);
  }

  function restorePublishedRegularFileDataFromPageCache(targetExports = deps.getCanonicalExports()): void {
    const sab = deps.getSabPageCache();

    for (let ino = 1; ino < MAX_INODES; ino += 1) {
      const published = readInodeRecord(sab, ino);
      if (
        !published
        || (published.mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG
        || published.size <= 0
        || (!hasBackingFile(ino) && !hasDirtyResidentPagesForInode(sab, ino))
      ) {
        continue;
      }

      const mode = targetExports.memfs_inode_mode(ino);
      if ((mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
        continue;
      }
      if (Number(targetExports.memfs_file_size(ino)) !== published.size) {
        continue;
      }

      syncExternalFileDataIntoTarget(targetExports, ino, mode, published.size, published);
    }
  }

  function syncMetadataMutation(sourceExports: KernelExports, path: string): boolean {
    if (!path.startsWith("/")) {
      return false;
    }

    const normalized = normalizePath(path);
    const ino = deps.resolvePathIn(sourceExports, normalized);
    const parts = pathParts(normalized);
    const name = parts.pop();
    if (!name) {
      return false;
    }
    const parentPath = parts.length === 0 ? "/" : `/${parts.join("/")}`;
    const nameBytes = deps.encoder.encode(name);

    const removePathInTarget = (targetExports: KernelExports): boolean => {
      const parentIno = deps.resolvePathIn(targetExports, parentPath);
      if (parentIno === 0) {
        return true;
      }
      const namePtr = writeScratchFor(targetExports, nameBytes);
      const unlinkResult = Number(targetExports.memfs_unlink(parentIno, namePtr, nameBytes.length));
      if (unlinkResult === 0 || unlinkResult === -2) {
        return true;
      }
      const rmdirResult = Number(targetExports.memfs_rmdir(parentIno, namePtr, nameBytes.length));
      return rmdirResult === 0 || rmdirResult === -2;
    };

    if (ino === 0) {
      const removedCanonical = removePathInTarget(deps.getCanonicalExports());
      let removedMembers = true;
      for (const memberExports of deps.processKernels.values()) {
        removedMembers = removePathInTarget(memberExports) && removedMembers;
      }
      if (removedCanonical || removedMembers) {
        bumpDirEpoch(deps.getSabPageCache());
      }
      return removedCanonical && removedMembers;
    }

    const mode = sourceExports.memfs_inode_mode(ino);
    const fileType = mode & MEMFS_S_IFMT;
    if (fileType !== MEMFS_S_IFDIR && fileType !== MEMFS_S_IFREG) {
      return false;
    }
    const parentIno = deps.resolvePathIn(sourceExports, parentPath);
    if (parentIno === 0) {
      return false;
    }
    let published = readInodeRecord(deps.getSabPageCache(), ino);
    const hasDirtyResidentPages = hasDirtyResidentPagesForInode(deps.getSabPageCache(), ino);
    const sourceSize = fileType === MEMFS_S_IFREG ? Number(sourceExports.memfs_file_size(ino)) : 0;
    const size = hasDirtyResidentPages && published ? published.size : sourceSize;
    const publishedMeta = published && (published.size === size || hasDirtyResidentPages)
      ? { fileObjectId: published.fileObjectId, generation: published.generation }
      : undefined;
    const backingMeta = hasDirtyResidentPages
      ? publishedMeta
      : backingMetaForInode(ino) ?? publishedMeta;
    if (
      !published
      || published.mode !== mode
      || published.size !== size
      || published.parentIno !== parentIno
      || deps.decoder.decode(published.name) !== name
    ) {
      publishInode(deps.getSabPageCache(), ino, {
        mode,
        size,
        parentIno,
        name: nameBytes,
        nextSibling: published?.nextSibling ?? 0,
        ...backingMeta,
      });
      published = readInodeRecord(deps.getSabPageCache(), ino);
      if (!published) {
        return false;
      }
    }

    if (!tryEnsurePublishedInodeInTarget(deps.getCanonicalExports(), ino, published)) {
      return false;
    }
    for (const memberExports of deps.processKernels.values()) {
      // A live sibling may already have advanced beyond this inode while
      // leaving a hole. The fast path cannot reconstruct that exact inode in
      // the sibling, but forcing a full fanout would roll unrelated live MemFS
      // state backward. Keep canonical authoritative and let the sibling retain
      // its own inherited namespace until it synchronizes from its own process.
      tryEnsurePublishedInodeInTarget(memberExports, ino, published);
    }
    bumpDirEpoch(deps.getSabPageCache());
    return true;
  }

  function readFile(path: string): Uint8Array {
    const targetExports = deps.getCanonicalExports();
    const ino = resolvePathFollowingSymlinks(path);
    if (ino === 0) {
      throw new Error(`file not found: ${path}`);
    }

    const mode = targetExports.memfs_inode_mode(ino);
    if ((mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
      throw new Error(`path is not a regular file: ${path}`);
    }
    const size = Number(targetExports.memfs_file_size(ino));
    if (size === 0) {
      return new Uint8Array();
    }
    const backingData = readBackingFileByInode(ino) ?? materializeBackingSourceForInode(ino, size);
    if (backingData && backingData.length === size) {
      return backingData;
    }
    throw new Error(`failed to read authoritative regular file data: ${path}`);
  }

  async function readFileAsync(path: string): Promise<Uint8Array> {
    const targetExports = deps.getCanonicalExports();
    const ino = resolvePathFollowingSymlinks(path);
    if (ino === 0) {
      throw new Error(`file not found: ${path}`);
    }

    const size = Number(targetExports.memfs_file_size(ino));
    const backingData = readBackingFileByInode(ino) ?? materializeBackingSourceForInode(ino, size);
    if (backingData && backingData.length === size) {
      return backingData;
    }
    if (size > 0 && await ensureBackingSourceRangeForInode(ino, 0, size, size)) {
      const materialized = materializeBackingSourceForInode(ino, size);
      if (materialized && materialized.length === size) {
        return materialized;
      }
    }
    return readFile(path);
  }

  function readlink(path: string): Uint8Array {
    const targetExports = deps.getCanonicalExports();
    const ino = resolvePath(path);
    if (ino === 0) {
      throw new Error(`symlink not found: ${path}`);
    }
    if ((targetExports.memfs_inode_mode(ino) & MEMFS_S_IFMT) !== 0o120000) {
      throw new Error(`path is not a symlink: ${path}`);
    }
    const bufPtr = scratchBaseFor(targetExports) + 0x8000;
    const bufLen = 4096;
    ensureMemoryCapacityFor(targetExports.memory, bufPtr + bufLen);
    const len = Number(targetExports.memfs_read_link(ino, bufPtr, bufLen));
    if (len < 0) {
      throw new Error(`failed to read symlink: ${path}`);
    }
    return new Uint8Array(targetExports.memory.buffer.slice(bufPtr, bufPtr + len));
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

    const bytes = new Uint8Array(byteLen);
    bytes.set(new Uint8Array(targetExports.memory.buffer, bufPtr, byteLen));
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
    rebuildPageCacheFromExports,
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
    registerSnapshotBackingExtent,
    publishFileBacking: replaceBackingFile,
    readBackingFileByInode,
    readBackingFileRangeByInode,
    mkdir,
    upsertSymlinkSeed: deps.upsertSymlinkSeed,
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
    readFileAsync,
    readlink,
    readdir,
    registerBackingSource,
    registerBackingSourceRange,
    registerBackingSourceObject,
    registerRemoteBackingSource,
    registerOpfsBackingSource,
    ensureBackingSourceRangeForInode,
    readBackingFileByInode,
    readBackingFileRangeByInode,
    flushDirtyResidentFileByInode,
    truncateDirtyResidentFileByInode,
    writeDirtyResidentFileByInode,
    rebuildPageCacheFromExports,
    rebuildPageCacheFromCanonical,
    resolvePath,
    snapshotCanonicalFs,
    statPath,
    restorePublishedRegularFileDataFromPageCache,
    syncFileDataMutation,
    syncFileDataMutationByPath,
    syncMetadataMutation,
    symlink,
    writeFile,
  };
}
