import type { KernelExports } from "../abi.js";
import type {
  DirtyResidentPageDecision,
  FileBackingStat,
  ResidentDirtyPageAuthority,
} from "../fs/sab-page-cache.js";
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
import { supervisorResidentPageTableAuthority } from "../fs/supervisor-page-table.js";
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

type AsyncBackingSource =
  | { kind: "remote"; byteLength: number; url: string }
  | { kind: "opfs"; byteLength: number; handle: OpfsBackingFileHandle };

type BackingObjectOperation = "source" | "materialize" | "replace" | "flush-write" | "flush-truncate";

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

  function replaceBackingFile(
    ino: number,
    data: Uint8Array,
    op: Extract<BackingObjectOperation, "materialize" | "replace"> = "replace",
    sourceExports: KernelExports = deps.getCanonicalExports(),
  ): FileBackingStat {
    backingExtentsByIno.delete(ino);
    const decision = supervisorBackingObject(sourceExports, op, ino);
    fileObjectIdsByIno.set(ino, decision.fileObjectId);
    if (backingStore.stat(decision.fileObjectId)) {
      return backingStore.replace(decision.fileObjectId, data, {
        generation: decision.generation,
      });
    }
    return backingStore.createFile(data, decision);
  }

  function writeBackingFileRange(ino: number, offset: number, data: Uint8Array): FileBackingStat {
    backingExtentsByIno.delete(ino);
    const decision = supervisorBackingObject(deps.getCanonicalExports(), "flush-write", ino);
    fileObjectIdsByIno.set(ino, decision.fileObjectId);
    if (!backingStore.stat(decision.fileObjectId)) {
      backingStore.createFile(new Uint8Array(), decision);
    }
    return backingStore.write(decision.fileObjectId, offset, data, {
      generation: decision.generation,
    });
  }

  function truncateBackingFile(ino: number, size: number): FileBackingStat {
    backingExtentsByIno.delete(ino);
    const decision = supervisorBackingObject(deps.getCanonicalExports(), "flush-truncate", ino);
    fileObjectIdsByIno.set(ino, decision.fileObjectId);
    if (!backingStore.stat(decision.fileObjectId)) {
      backingStore.createFile(new Uint8Array(), decision);
    }
    return backingStore.truncate(decision.fileObjectId, size, {
      generation: decision.generation,
    });
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
    if (!extent) {
      return null;
    }
    return supervisorBackingSourceRange(
      deps.getCanonicalExports(),
      ino,
      fileOffset,
      length,
      expectedSize,
      extent,
      0,
      false,
    );
  }

  function registerSnapshotBackingExtent(ino: number, backingExtent: FsSnapshotBackingExtent | undefined): void {
    if (!backingExtent) {
      backingExtentsByIno.delete(ino);
      return;
    }
    backingExtentsByIno.set(ino, { ...backingExtent });
  }

  function supervisorNumber(value: bigint | number | undefined): number {
    const numeric = typeof value === "bigint" ? Number(value) : value;
    if (typeof numeric !== "number" || !Number.isSafeInteger(numeric)) {
      throw new RangeError(`invalid supervisor fs decision value: ${String(value)}`);
    }
    return numeric;
  }

  function supervisorNumericExport(sourceExports: KernelExports, name: string): number {
    const read = (sourceExports as unknown as Record<string, unknown>)[name];
    if (typeof read !== "function") {
      throw new Error(`missing ${name} export`);
    }
    return supervisorNumber((read as () => bigint | number)());
  }

  function supervisorDirtyPageOpWrite(sourceExports: KernelExports): number {
    return supervisorNumericExport(sourceExports, "supervisor_fs_dirty_page_op_write");
  }

  function supervisorDirtyPageOpTruncate(sourceExports: KernelExports): number {
    return supervisorNumericExport(sourceExports, "supervisor_fs_dirty_page_op_truncate");
  }

  function supervisorDirtyPageOpFlush(sourceExports: KernelExports): number {
    return supervisorNumericExport(sourceExports, "supervisor_fs_dirty_page_op_flush");
  }

  function supervisorDirtyPageWritebackRequiredFlag(sourceExports: KernelExports): number {
    return supervisorNumericExport(sourceExports, "supervisor_fs_dirty_page_flag_writeback_required");
  }

  function supervisorRangeI64(value: number | undefined, fallback: number): bigint {
    const resolved = value ?? fallback;
    return Number.isSafeInteger(resolved) ? BigInt(resolved) : -1n;
  }

  function supervisorBackingObject(
    sourceExports: KernelExports,
    op: BackingObjectOperation,
    ino: number,
  ): Pick<FileBackingStat, "fileObjectId" | "generation"> {
    const decide = sourceExports.supervisor_fs_backing_object_decision;
    if (typeof decide !== "function") {
      throw new Error("missing supervisor_fs_backing_object_decision export");
    }
    const published = readInodeRecord(deps.getSabPageCache(), ino);
    const backing = backingMetaForInode(ino);
    const opNumber = (() => {
      switch (op) {
        case "source":
          return supervisorNumericExport(sourceExports, "supervisor_fs_backing_object_op_source");
        case "materialize":
          return supervisorNumericExport(sourceExports, "supervisor_fs_backing_object_op_materialize");
        case "replace":
          return supervisorNumericExport(sourceExports, "supervisor_fs_backing_object_op_replace");
        case "flush-write":
          return supervisorNumericExport(sourceExports, "supervisor_fs_backing_object_op_flush_write");
        case "flush-truncate":
          return supervisorNumericExport(sourceExports, "supervisor_fs_backing_object_op_flush_truncate");
      }
    })();
    const result = supervisorNumber(decide(
      opNumber,
      ino,
      BigInt(published?.fileObjectId ?? 0),
      BigInt(published?.generation ?? 0),
      BigInt(backing?.fileObjectId ?? 0),
      BigInt(backing?.generation ?? 0),
    ));
    if (result < 0) {
      throw new Error(`supervisor backing object decision failed: ${result}`);
    }
    const fileObjectId = supervisorNumericExport(sourceExports, "supervisor_fs_backing_object_file_object_id");
    const generation = supervisorNumericExport(sourceExports, "supervisor_fs_backing_object_generation");
    if (fileObjectId <= 0 || generation <= 0) {
      throw new Error("invalid supervisor backing object identity");
    }
    return { fileObjectId, generation };
  }

  function supervisorBackingSourceRange(
    sourceExports: KernelExports,
    ino: number,
    fileOffset: number,
    length: number,
    expectedSize: number,
    extent: FsSnapshotBackingExtent,
    sourceByteLength: number,
    requireSourceBounds: boolean,
  ): { sourceIdentity: string; byteLength: number; offset: number; length: number } | null {
    const decide = sourceExports.supervisor_fs_backing_source_range_decision;
    if (typeof decide !== "function") {
      throw new Error("missing supervisor_fs_backing_source_range_decision export");
    }
    const decision = supervisorNumber(decide(
      ino,
      supervisorRangeI64(fileOffset, -1),
      supervisorRangeI64(length, -1),
      supervisorRangeI64(expectedSize, -1),
      supervisorRangeI64(extent.offset, 0),
      supervisorRangeI64(extent.length, expectedSize),
      supervisorRangeI64(sourceByteLength, -1),
      requireSourceBounds ? 1 : 0,
    ));
    const none = supervisorNumericExport(sourceExports, "supervisor_fs_backing_source_range_none");
    if (decision === none) {
      return null;
    }
    const read = supervisorNumericExport(sourceExports, "supervisor_fs_backing_source_range_read");
    if (decision !== read) {
      throw new Error(`unknown supervisor backing source range decision: ${decision}`);
    }
    const byteLength = supervisorNumericExport(sourceExports, "supervisor_fs_backing_source_range_source_byte_length");
    const offset = supervisorNumericExport(sourceExports, "supervisor_fs_backing_source_range_source_offset");
    const rangeLength = supervisorNumericExport(sourceExports, "supervisor_fs_backing_source_range_read_length");
    if (byteLength < 0 || offset < 0 || rangeLength < 0) {
      throw new Error("invalid supervisor backing source range");
    }
    return {
      sourceIdentity: extent.sourceIdentity,
      byteLength,
      offset,
      length: rangeLength,
    };
  }

  function supervisorCleanBackingRange(
    sourceExports: KernelExports,
    ino: number,
    offset: number,
    length: number,
    publishedSize: number,
    backingSize: number,
  ): {
    mode: "read" | "read-zero-tail" | "zero";
    readOffset: number;
    readLength: number;
    outputLength: number;
  } | null {
    const decide = sourceExports.supervisor_fs_clean_backing_range_decision;
    if (typeof decide !== "function") {
      throw new Error("missing supervisor_fs_clean_backing_range_decision export");
    }
    const decision = supervisorNumber(decide(
      ino,
      supervisorRangeI64(offset, -1),
      supervisorRangeI64(length, -1),
      supervisorRangeI64(publishedSize, -1),
      supervisorRangeI64(backingSize, -1),
    ));
    const none = supervisorNumericExport(sourceExports, "supervisor_fs_clean_backing_range_none");
    if (decision === none) {
      return null;
    }
    const read = supervisorNumericExport(sourceExports, "supervisor_fs_clean_backing_range_read");
    const readZeroTail = supervisorNumericExport(sourceExports, "supervisor_fs_clean_backing_range_read_zero_tail");
    const zero = supervisorNumericExport(sourceExports, "supervisor_fs_clean_backing_range_zero");
    let mode: "read" | "read-zero-tail" | "zero";
    if (decision === read) {
      mode = "read";
    } else if (decision === readZeroTail) {
      mode = "read-zero-tail";
    } else if (decision === zero) {
      mode = "zero";
    } else {
      throw new Error(`unknown supervisor clean backing range decision: ${decision}`);
    }
    const readOffset = supervisorNumericExport(sourceExports, "supervisor_fs_clean_backing_range_read_offset");
    const readLength = supervisorNumericExport(sourceExports, "supervisor_fs_clean_backing_range_read_length");
    const outputLength = supervisorNumericExport(sourceExports, "supervisor_fs_clean_backing_range_output_length");
    if (readOffset < 0 || readLength < 0 || outputLength < 0) {
      throw new Error("invalid supervisor clean backing range");
    }
    return {
      mode,
      readOffset,
      readLength,
      outputLength,
    };
  }

  function readSupervisorDirtyPageDecision(
    sourceExports: KernelExports,
    result: bigint | number,
  ): DirtyResidentPageDecision {
    const resultNumber = supervisorNumber(result);
    if (resultNumber < 0) {
      return {
        result: resultNumber,
        nextSize: 0,
        fileObjectId: 0,
        generation: 0,
      };
    }
    return {
      result: resultNumber,
      nextSize: supervisorNumericExport(sourceExports, "supervisor_fs_dirty_page_decision_next_size"),
      fileObjectId: supervisorNumericExport(sourceExports, "supervisor_fs_dirty_page_decision_file_object_id"),
      generation: supervisorNumericExport(sourceExports, "supervisor_fs_dirty_page_decision_generation"),
    };
  }

  function supervisorDirtyPageAuthority(sourceExports: KernelExports): ResidentDirtyPageAuthority {
    const decide = sourceExports.supervisor_fs_dirty_page_decision;
    if (typeof decide !== "function") {
      throw new Error("missing supervisor_fs_dirty_page_decision export");
    }
    const pageTableAuthority = supervisorResidentPageTableAuthority(sourceExports);
    return {
      selectPublishSlot: pageTableAuthority.selectPublishSlot,
      decideEviction: pageTableAuthority.decideEviction,
      decideWrite(input) {
        return readSupervisorDirtyPageDecision(
          sourceExports,
          decide(
            supervisorDirtyPageOpWrite(sourceExports),
            input.ino,
            BigInt(input.offset),
            BigInt(input.length),
            BigInt(input.currentSize),
            BigInt(input.currentFileObjectId),
            BigInt(input.currentGeneration),
            input.existingDirtyPages,
            input.newDirtyPages,
            input.residentPageCapacity,
            0,
          ),
        );
      },
      decideTruncate(input) {
        return readSupervisorDirtyPageDecision(
          sourceExports,
          decide(
            supervisorDirtyPageOpTruncate(sourceExports),
            input.ino,
            0n,
            BigInt(input.size),
            BigInt(input.currentSize),
            BigInt(input.currentFileObjectId),
            BigInt(input.currentGeneration),
            0,
            input.retainedDirtyPages,
            input.residentPageCapacity,
            0,
          ),
        );
      },
    };
  }

  function supervisorDirtyFlushRequired(
    sourceExports: KernelExports,
    ino: number,
    record: { size: number; fileObjectId: number; generation: number },
    hasDirtyPages: boolean,
    pageMapNewer: boolean,
  ): boolean {
    const decide = sourceExports.supervisor_fs_dirty_page_decision;
    if (typeof decide !== "function") {
      throw new Error("missing supervisor_fs_dirty_page_decision export");
    }
    const result = supervisorNumber(decide(
      supervisorDirtyPageOpFlush(sourceExports),
      ino,
      0n,
      0n,
      BigInt(record.size),
      BigInt(record.fileObjectId),
      BigInt(record.generation),
      0,
      hasDirtyPages ? 1 : 0,
      0,
      pageMapNewer ? 1 : 0,
    ));
    if (result < 0) {
      throw new Error(`supervisor dirty flush decision failed: ${result}`);
    }
    const flags = supervisorNumericExport(sourceExports, "supervisor_fs_dirty_page_decision_flags");
    const writebackRequired = supervisorDirtyPageWritebackRequiredFlag(sourceExports);
    return (flags & writebackRequired) !== 0;
  }

  function supervisorDirtyPrepare(
    sourceExports: KernelExports,
    input: {
      ino: number;
      op: number;
      expectedSize: number;
      published: ReturnType<typeof readInodeRecord>;
      sourceIsRegular: boolean;
      sourceFileObjectId: number | null;
      hasDirtyResidentPages: boolean;
    },
  ): { result: number; publishMode: "none" | "missing" | "size-update" } {
    const decide = sourceExports.supervisor_fs_dirty_prepare_decision;
    if (typeof decide !== "function") {
      throw new Error("missing supervisor_fs_dirty_prepare_decision export");
    }
    const result = supervisorNumber(decide(
      input.ino,
      input.op,
      BigInt(input.expectedSize),
      input.published ? 1 : 0,
      BigInt(input.published?.size ?? 0),
      BigInt(input.published?.fileObjectId ?? 0),
      BigInt(input.published?.dataOff ?? 0),
      input.sourceIsRegular ? 1 : 0,
      BigInt(input.sourceFileObjectId ?? 0),
      input.hasDirtyResidentPages ? 1 : 0,
    ));
    if (result < 0) {
      return { result, publishMode: "none" };
    }
    const publishMode = supervisorNumericExport(sourceExports, "supervisor_fs_dirty_prepare_publish_mode");
    const none = supervisorNumericExport(sourceExports, "supervisor_fs_dirty_prepare_publish_none");
    const missing = supervisorNumericExport(sourceExports, "supervisor_fs_dirty_prepare_publish_missing");
    const sizeUpdate = supervisorNumericExport(sourceExports, "supervisor_fs_dirty_prepare_publish_size_update");
    if (publishMode === none) {
      return { result, publishMode: "none" };
    }
    if (publishMode === missing) {
      return { result, publishMode: "missing" };
    }
    if (publishMode === sizeUpdate) {
      return { result, publishMode: "size-update" };
    }
    throw new Error(`unknown supervisor dirty prepare publish mode: ${publishMode}`);
  }

  function supervisorDataSyncAction(
    sourceExports: KernelExports,
    input: {
      kind: "export" | "restore";
      ino: number;
      sourceMode: number;
      sourceSize: number;
      targetMode: number;
      targetSize: number;
      published: ReturnType<typeof readInodeRecord>;
      hasDirtyResidentPages: boolean;
      pageMapNewer: boolean;
      hasBackingFile: boolean;
    },
  ): "none" | "externalize" {
    const decide = sourceExports.supervisor_fs_data_sync_decision;
    if (typeof decide !== "function") {
      throw new Error("missing supervisor_fs_data_sync_decision export");
    }
    const kind = input.kind === "restore"
      ? supervisorNumericExport(sourceExports, "supervisor_fs_data_sync_kind_restore")
      : supervisorNumericExport(sourceExports, "supervisor_fs_data_sync_kind_export");
    const decision = supervisorNumber(decide(
      kind,
      input.ino,
      input.sourceMode,
      BigInt(input.sourceSize),
      input.targetMode,
      BigInt(input.targetSize),
      input.published ? 1 : 0,
      input.published?.mode ?? 0,
      BigInt(input.published?.size ?? 0),
      BigInt(input.published?.fileObjectId ?? 0),
      input.hasDirtyResidentPages ? 1 : 0,
      input.pageMapNewer ? 1 : 0,
      input.hasBackingFile ? 1 : 0,
    ));
    if (decision < 0) {
      return "none";
    }
    const none = supervisorNumericExport(sourceExports, "supervisor_fs_sync_action_none");
    if (decision === none) {
      return "none";
    }
    const externalize = supervisorNumericExport(sourceExports, "supervisor_fs_sync_action_externalize");
    if (decision === externalize) {
      return "externalize";
    }
    throw new Error(`unknown supervisor fs data sync action: ${decision}`);
  }

  function supervisorMetadataSync(
    sourceExports: KernelExports,
    input: {
      ino: number;
      sourcePresent: boolean;
      sourceMode: number;
      sourceSize: number;
      parentPresent: boolean;
      parentIno: number;
      published: ReturnType<typeof readInodeRecord>;
      publishedNameMatches: boolean;
      hasDirtyResidentPages: boolean;
      backingPresent: boolean;
    },
  ): { action: "none" | "remove" | "ensure" | "publish-ensure"; size: number; backingSource: "none" | "published" | "backing" } {
    const decide = sourceExports.supervisor_fs_metadata_sync_decision;
    if (typeof decide !== "function") {
      throw new Error("missing supervisor_fs_metadata_sync_decision export");
    }
    const decision = supervisorNumber(decide(
      input.ino,
      input.sourcePresent ? 1 : 0,
      input.sourceMode,
      BigInt(input.sourceSize),
      input.parentPresent ? 1 : 0,
      input.parentIno,
      input.published ? 1 : 0,
      input.published?.mode ?? 0,
      BigInt(input.published?.size ?? 0),
      input.published?.parentIno ?? 0,
      input.publishedNameMatches ? 1 : 0,
      input.hasDirtyResidentPages ? 1 : 0,
      input.backingPresent ? 1 : 0,
    ));
    if (decision < 0) {
      return { action: "none", size: 0, backingSource: "none" };
    }

    const none = supervisorNumericExport(sourceExports, "supervisor_fs_metadata_sync_action_none");
    const remove = supervisorNumericExport(sourceExports, "supervisor_fs_metadata_sync_action_remove");
    const ensure = supervisorNumericExport(sourceExports, "supervisor_fs_metadata_sync_action_ensure");
    const publishEnsure = supervisorNumericExport(sourceExports, "supervisor_fs_metadata_sync_action_publish_ensure");
    let action: "none" | "remove" | "ensure" | "publish-ensure";
    if (decision === none) {
      action = "none";
    } else if (decision === remove) {
      action = "remove";
    } else if (decision === ensure) {
      action = "ensure";
    } else if (decision === publishEnsure) {
      action = "publish-ensure";
    } else {
      throw new Error(`unknown supervisor fs metadata sync action: ${decision}`);
    }

    const backing = supervisorNumericExport(sourceExports, "supervisor_fs_metadata_sync_backing_source");
    const backingNone = supervisorNumericExport(sourceExports, "supervisor_fs_metadata_backing_none");
    const backingPublished = supervisorNumericExport(sourceExports, "supervisor_fs_metadata_backing_published");
    const backingBacking = supervisorNumericExport(sourceExports, "supervisor_fs_metadata_backing_backing");
    let backingSource: "none" | "published" | "backing";
    if (backing === backingNone) {
      backingSource = "none";
    } else if (backing === backingPublished) {
      backingSource = "published";
    } else if (backing === backingBacking) {
      backingSource = "backing";
    } else {
      throw new Error(`unknown supervisor fs metadata backing source: ${backing}`);
    }

    return {
      action,
      size: supervisorNumericExport(sourceExports, "supervisor_fs_metadata_sync_size"),
      backingSource,
    };
  }

  function supervisorPageTableUsesResident(
    sourceExports: KernelExports,
    ino: number,
    hasDirtyPages: boolean,
    published: ReturnType<typeof readInodeRecord>,
    backingStat: FileBackingStat | null,
  ): boolean {
    const decide = sourceExports.supervisor_fs_page_table_source_decision;
    if (typeof decide !== "function") {
      throw new Error("missing supervisor_fs_page_table_source_decision export");
    }
    const decision = supervisorNumber(decide(
      ino,
      hasDirtyPages ? 1 : 0,
      BigInt(published?.fileObjectId ?? 0),
      BigInt(published?.generation ?? 0),
      BigInt(published?.size ?? 0),
      BigInt(backingStat?.fileObjectId ?? 0),
      BigInt(backingStat?.generation ?? 0),
      BigInt(backingStat?.size ?? 0),
    ));
    if (decision < 0) {
      throw new Error(`supervisor page-table source decision failed: ${decision}`);
    }
    const backing = supervisorNumericExport(sourceExports, "supervisor_fs_page_table_source_backing");
    const resident = supervisorNumericExport(sourceExports, "supervisor_fs_page_table_source_resident");
    if (decision === resident) {
      return true;
    }
    if (decision === backing) {
      return false;
    }
    throw new Error(`unknown supervisor page-table source decision: ${decision}`);
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
    replaceBackingFile(ino, data, "materialize");
    return readBackingFileByInode(ino);
  }

  function readBackingSourceRangeForInode(
    ino: number,
    fileOffset: number,
    length: number,
    expectedSize: number,
  ): Uint8Array | null {
    const extent = backingExtentsByIno.get(ino);
    if (!extent) {
      return null;
    }
    const source = backingSourcesByIdentity.get(extent.sourceIdentity);
    if (!source) {
      return null;
    }
    const range = supervisorBackingSourceRange(
      deps.getCanonicalExports(),
      ino,
      fileOffset,
      length,
      expectedSize,
      extent,
      source.byteLength,
      true,
    );
    if (!range) {
      return null;
    }
    return source.readRange(range.offset, range.length);
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
    if (!extent) {
      return null;
    }
    const source = backingSourcesByIdentity.get(extent.sourceIdentity);
    if (!source) {
      return null;
    }
    const range = supervisorBackingSourceRange(
      deps.getCanonicalExports(),
      ino,
      0,
      expectedSize,
      expectedSize,
      extent,
      source.byteLength,
      true,
    );
    if (!range || range.length !== expectedSize) {
      return null;
    }
    const decision = supervisorBackingObject(deps.getCanonicalExports(), "source", ino);
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
        return source.readRange(range.offset + offset, length);
      },
    }, decision);
    fileObjectIdsByIno.set(ino, stat.fileObjectId);
    return stat.fileObjectId;
  }

  function residentPageMapIsNewerThanBacking(ino: number): boolean {
    const hasDirtyPages = hasDirtyResidentPagesForInode(deps.getSabPageCache(), ino);
    const published = readInodeRecord(deps.getSabPageCache(), ino);
    const fileObjectId = fileObjectIdsByIno.get(ino);
    const backingStat = fileObjectId === undefined ? null : backingStore.stat(fileObjectId);
    return supervisorPageTableUsesResident(
      deps.getCanonicalExports(),
      ino,
      hasDirtyPages,
      published,
      backingStat,
    );
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
    const fileObjectId = fileObjectIdsByIno.get(ino);
    if (fileObjectId === undefined) {
      const size = Number(deps.getCanonicalExports().memfs_file_size(ino));
      return readBackingSourceRangeForInode(ino, offset, length, size);
    }
    const stat = backingStore.stat(fileObjectId);
    if (!stat) {
      return null;
    }
    const published = readInodeRecord(deps.getSabPageCache(), ino);
    const publishedSize = published?.size ?? stat.size;
    const decision = supervisorCleanBackingRange(
      deps.getCanonicalExports(),
      ino,
      offset,
      length,
      publishedSize,
      stat.size,
    );
    if (!decision) {
      return null;
    }
    if (decision.mode === "zero") {
      return new Uint8Array(decision.outputLength);
    }
    const data = backingStore.read(fileObjectId, decision.readOffset, decision.readLength);
    if (data.length !== decision.readLength) {
      return null;
    }
    if (decision.mode === "read") {
      return data;
    }
    const out = new Uint8Array(decision.outputLength);
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
      let sourceFileObjectId: number | null = fileObjectId ?? null;
      if (fileObjectId === undefined) {
        sourceFileObjectId = createSourceBackedFileForInode(ino, expectedSize);
      }
      const published = readInodeRecord(deps.getSabPageCache(), ino);
      const prepare = supervisorDirtyPrepare(sourceExports, {
        ino,
        op: supervisorDirtyPageOpWrite(sourceExports),
        expectedSize,
        published,
        sourceIsRegular,
        sourceFileObjectId,
        hasDirtyResidentPages: hasDirtyResidentPagesForInode(deps.getSabPageCache(), ino),
      });
      if (prepare.result < 0) {
        return prepare.result;
      }
      if (prepare.publishMode === "missing") {
        publishInode(deps.getSabPageCache(), ino, {
          mode: sourceMode || 0o100644,
          size: expectedSize,
          parentIno: 0,
          name: new Uint8Array(),
          nextSibling: 0,
          ...backingMetaForInode(ino),
        });
      } else if (prepare.publishMode === "size-update" && published) {
        publishInode(deps.getSabPageCache(), ino, {
          mode: sourceMode || published.mode,
          size: expectedSize,
          parentIno: published.parentIno,
          name: published.name,
          nextSibling: published.nextSibling,
          ...backingMetaForInode(ino),
        });
      }
      const written = writeDirtyResidentFileData(
        deps.getSabPageCache(),
        ino,
        offset,
        data,
        readBackingFileRangeByInode,
        supervisorDirtyPageAuthority(sourceExports),
      );
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
    const pageMapNewer = residentPageMapIsNewerThanBacking(ino);
    const record = readInodeRecord(sab, ino);
    const supervisorRecord = record ?? {
      size: Number(deps.getCanonicalExports().memfs_file_size(ino)),
      fileObjectId: 0,
      generation: 0,
    };
    let shouldFlush = false;
    try {
      shouldFlush = supervisorDirtyFlushRequired(
        deps.getCanonicalExports(),
        ino,
        supervisorRecord,
        hasDirtyPages,
        pageMapNewer,
      );
    } catch {
      return false;
    }
    if (!record) {
      return !shouldFlush;
    }
    if (!shouldFlush) {
      return true;
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
    const cleaned = markDirtyResidentFileDataClean(
      sab,
      ino,
      stat.fileObjectId,
      stat.generation,
      supervisorResidentPageTableAuthority(deps.getCanonicalExports()),
    );
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
      const sourceMode = deps.getCanonicalExports().memfs_inode_mode(ino);
      const sourceIsRegular = (sourceMode & MEMFS_S_IFMT) === MEMFS_S_IFREG;
      let sourceFileObjectId: number | null = fileObjectIdsByIno.get(ino) ?? null;
      if (sourceFileObjectId === null) {
        sourceFileObjectId = createSourceBackedFileForInode(ino, expectedSize);
      }
      const published = readInodeRecord(deps.getSabPageCache(), ino);
      const canonicalExports = deps.getCanonicalExports();
      const prepare = supervisorDirtyPrepare(canonicalExports, {
        ino,
        op: supervisorDirtyPageOpTruncate(canonicalExports),
        expectedSize,
        published,
        sourceIsRegular,
        sourceFileObjectId,
        hasDirtyResidentPages: hasDirtyResidentPagesForInode(deps.getSabPageCache(), ino),
      });
      if (prepare.result < 0) {
        return prepare.result;
      }
      if (prepare.publishMode === "missing") {
        publishInode(deps.getSabPageCache(), ino, {
          mode: sourceMode || 0o100644,
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
        supervisorDirtyPageAuthority(deps.getCanonicalExports()),
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
    const stat = replaceBackingFile(ino, data, "replace", targetExports);
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
    const publishedRecord = readInodeRecord(deps.getSabPageCache(), ino);
    const published = publishedRecord ?? {
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
      const canonicalMode = canonicalExports.memfs_inode_mode(ino);
      const targetMode = (canonicalMode & MEMFS_S_IFMT) === MEMFS_S_IFREG
        ? canonicalMode
        : publishedRecord?.mode ?? canonicalMode;
      const size = (mode & MEMFS_S_IFMT) === MEMFS_S_IFREG
        ? Number(sourceExports.memfs_file_size(ino))
        : 0;
      const canonicalSize = (canonicalMode & MEMFS_S_IFMT) === MEMFS_S_IFREG
        ? Number(canonicalExports.memfs_file_size(ino))
        : publishedRecord?.size ?? 0;
      const action = supervisorDataSyncAction(sourceExports, {
        kind: "export",
        ino,
        sourceMode: mode,
        sourceSize: size,
        targetMode,
        targetSize: canonicalSize,
        published: publishedRecord,
        hasDirtyResidentPages: hasDirtyResidentPagesForInode(deps.getSabPageCache(), ino),
        pageMapNewer: residentPageMapIsNewerThanBacking(ino),
        hasBackingFile: hasBackingFile(ino),
      });
      if (action === "externalize") {
        clearSeedBackingExtentForInode(ino);
        ensurePublishedInodeInTarget(canonicalExports, ino, published);
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
    for (let ino = 1; ino < MAX_INODES; ino += 1) {
      restorePublishedRegularFileDataForInode(ino, targetExports);
    }
  }

  function restorePublishedRegularFileDataForInode(
    ino: number,
    targetExports = deps.getCanonicalExports(),
  ): boolean {
    const sab = deps.getSabPageCache();
    const published = readInodeRecord(sab, ino);
    const mode = targetExports.memfs_inode_mode(ino);
    const size = Number(targetExports.memfs_file_size(ino));
    const action = supervisorDataSyncAction(targetExports, {
      kind: "restore",
      ino,
      sourceMode: 0,
      sourceSize: 0,
      targetMode: mode,
      targetSize: size,
      published,
      hasDirtyResidentPages: hasDirtyResidentPagesForInode(sab, ino),
      pageMapNewer: false,
      hasBackingFile: hasBackingFile(ino),
    });
    if (action !== "externalize" || !published) {
      return false;
    }

    syncExternalFileDataIntoTarget(targetExports, ino, mode, published.size, published);
    return true;
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
      const decision = supervisorMetadataSync(sourceExports, {
        ino,
        sourcePresent: false,
        sourceMode: 0,
        sourceSize: 0,
        parentPresent: false,
        parentIno: 0,
        published: null,
        publishedNameMatches: false,
        hasDirtyResidentPages: false,
        backingPresent: false,
      });
      if (decision.action !== "remove") {
        return false;
      }
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
    const backingMeta = backingMetaForInode(ino);
    const decision = supervisorMetadataSync(sourceExports, {
      ino,
      sourcePresent: true,
      sourceMode: mode,
      sourceSize,
      parentPresent: true,
      parentIno,
      published,
      publishedNameMatches: published ? deps.decoder.decode(published.name) === name : false,
      hasDirtyResidentPages,
      backingPresent: backingMeta !== undefined,
    });
    if (decision.action !== "ensure" && decision.action !== "publish-ensure") {
      return false;
    }
    const publishedMeta = published && decision.backingSource === "published"
      ? { fileObjectId: published.fileObjectId, generation: published.generation }
      : undefined;
    const selectedBackingMeta = decision.backingSource === "backing"
      ? backingMeta
      : publishedMeta;
    if (decision.action === "publish-ensure") {
      publishInode(deps.getSabPageCache(), ino, {
        mode,
        size: decision.size,
        parentIno,
        name: nameBytes,
        nextSibling: published?.nextSibling ?? 0,
        ...selectedBackingMeta,
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
    restorePublishedRegularFileDataForInode,
    syncFileDataMutation,
    syncFileDataMutationByPath,
    syncMetadataMutation,
    symlink,
    writeFile,
  };
}
