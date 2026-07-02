// Kernel-worker tests in this file validate transitional runtime-owned bridge behavior.
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test, vi } from "vite-plus/test";
import type { KernelExports } from "../../../src/abi.js";
import {
  DATA_REGION_OFFSET,
  evictResidentPage,
  publishInode,
  readInodeRecord,
  readResidentFileData,
} from "../../../src/fs/sab-page-cache.js";
import { createSha256VerifiedBackingSource } from "../../../src/kernel-worker/backing-source.js";
import { createKernelWorkerFsState } from "../../../src/kernel-worker/fs-state.js";
import { createNodeHostFileBackingSource } from "../../../src/kernel-worker/host-file-backing-source.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

type InodeRecord = {
  ino: number;
  path: string;
  kind: "dir" | "file" | "symlink";
  mode: number;
  data: Uint8Array;
  materialized: boolean;
};

type FakeKernelExports = KernelExports & {
  opLog: string[];
  fileData(path: string): Uint8Array;
  fileIno(path: string): number;
  linkTarget(path: string): string | null;
  overwriteFile(path: string, data: Uint8Array): void;
  createSymlink(path: string, target: Uint8Array): number;
  setHeapPtr(ptr: number): void;
};

function normalizePath(path: string): string {
  const normalized = `/${path.split("/").filter(Boolean).join("/")}`;
  return normalized || "/";
}

function decodeGuestBytes(memory: WebAssembly.Memory, ptr: number, len: number): string {
  return decoder.decode(new Uint8Array(memory.buffer, ptr, len));
}

function expectBytesEqual(actual: Uint8Array, expected: Uint8Array): void {
  expect(actual.byteLength).toBe(expected.byteLength);
  for (let index = 0; index < expected.byteLength; index += 1) {
    if (actual[index] !== expected[index]) {
      throw new Error(`byte mismatch at ${index}: expected ${expected[index]} got ${actual[index]}`);
    }
  }
}

function makeKernelExports(): FakeKernelExports {
  const memory = new WebAssembly.Memory({ initial: 512, maximum: 1024, shared: true });
  const recordsByPath = new Map<string, InodeRecord>();
  const recordsByIno = new Map<number, InodeRecord>();
  const opLog: string[] = [];
  let nextIno = 2;
  let reservedIno: number | null = null;
  let heapPtr = 0x10000;
  let lastDirtyPageDecision = {
    result: 0,
    nextSize: 0,
    fileObjectId: 0,
    generation: 0,
    flags: 0,
  };

  function putRecord(record: InodeRecord): void {
    recordsByPath.set(record.path, record);
    recordsByIno.set(record.ino, record);
  }

  function resetRoot(): void {
    recordsByPath.clear();
    recordsByIno.clear();
    nextIno = 2;
    reservedIno = null;
    putRecord({
      ino: 1,
      path: "/",
      kind: "dir",
      mode: 0o040755,
      data: new Uint8Array(),
      materialized: false,
    });
  }

  function allocateIno(): number {
    if (reservedIno !== null) {
      const ino = reservedIno;
      reservedIno = null;
      if (ino >= nextIno) {
        nextIno = ino + 1;
      }
      return ino;
    }
    return nextIno++;
  }

  resetRoot();

  function supervisorDirtyPageDecision(
    op: number,
    ino: number,
    offset: bigint | number,
    lengthOrSize: bigint | number,
    currentSize: bigint | number,
    currentFileObjectId: bigint | number,
    currentGeneration: bigint | number,
    existingDirtyPages: number,
    deltaDirtyPages: number,
    residentPageCapacity: number,
    pageMapNewer: number,
  ): number {
    const offsetNumber = Number(offset);
    const lengthOrSizeNumber = Number(lengthOrSize);
    const currentSizeNumber = Number(currentSize);
    const currentFileObjectIdNumber = Number(currentFileObjectId);
    const currentGenerationNumber = Number(currentGeneration);
    lastDirtyPageDecision = {
      result: 0,
      nextSize: 0,
      fileObjectId: 0,
      generation: 0,
      flags: 0,
    };
    if (ino <= 0 || ino >= 65536) {
      lastDirtyPageDecision.result = -22;
      return -22;
    }
    if (op === 1) {
      const end = offsetNumber + lengthOrSizeNumber;
      if (lengthOrSizeNumber <= 0 || !Number.isSafeInteger(end) || end > 0xffffffff) {
        lastDirtyPageDecision.result = -22;
        return -22;
      }
      if (existingDirtyPages + deltaDirtyPages > residentPageCapacity) {
        lastDirtyPageDecision.result = -28;
        return -28;
      }
      lastDirtyPageDecision = {
        result: lengthOrSizeNumber,
        nextSize: Math.max(currentSizeNumber, end),
        fileObjectId: currentFileObjectIdNumber || ino,
        generation: currentFileObjectIdNumber === 0
          ? currentGenerationNumber + 1
          : currentGenerationNumber,
        flags: 1 | 2,
      };
      return lengthOrSizeNumber;
    }
    if (op === 2) {
      if (lengthOrSizeNumber < 0 || lengthOrSizeNumber > 0xffffffff) {
        lastDirtyPageDecision.result = -22;
        return -22;
      }
      if (deltaDirtyPages > residentPageCapacity) {
        lastDirtyPageDecision.result = -28;
        return -28;
      }
      lastDirtyPageDecision = {
        result: 0,
        nextSize: lengthOrSizeNumber,
        fileObjectId: currentFileObjectIdNumber || ino,
        generation: currentGenerationNumber + 1,
        flags: 1 | 2,
      };
      return 0;
    }
    if (op === 3) {
      lastDirtyPageDecision = {
        result: 0,
        nextSize: currentSizeNumber,
        fileObjectId: currentFileObjectIdNumber,
        generation: currentGenerationNumber,
        flags: deltaDirtyPages !== 0 || pageMapNewer !== 0 ? 4 : 0,
      };
      return 0;
    }
    lastDirtyPageDecision.result = -22;
    return -22;
  }

  function supervisorPageTableSourceDecision(
    ino: number,
    hasDirtyPages: number,
    publishedFileObjectId: bigint | number,
    publishedGeneration: bigint | number,
    publishedSize: bigint | number,
    backingFileObjectId: bigint | number,
    backingGeneration: bigint | number,
    backingSize: bigint | number,
  ): number {
    const publishedFileObjectIdNumber = Number(publishedFileObjectId);
    const publishedGenerationNumber = Number(publishedGeneration);
    const publishedSizeNumber = Number(publishedSize);
    const backingFileObjectIdNumber = Number(backingFileObjectId);
    const backingGenerationNumber = Number(backingGeneration);
    const backingSizeNumber = Number(backingSize);
    if (ino <= 0 || ino >= 65536) {
      return -22;
    }
    if (hasDirtyPages !== 0) {
      return 1;
    }
    if (publishedFileObjectIdNumber === 0 || backingFileObjectIdNumber === 0) {
      return 0;
    }
    return (
      publishedFileObjectIdNumber !== backingFileObjectIdNumber
      || publishedGenerationNumber !== backingGenerationNumber
      || publishedSizeNumber !== backingSizeNumber
    )
      ? 1
      : 0;
  }

  const exports = {
    memory,
    kernel_heap_ptr: () => heapPtr,
    kernel_reset_memfs: () => {
      resetRoot();
    },
    memfs_resolve_path: (ptr: number, len: number) => {
      const path = normalizePath(decodeGuestBytes(memory, ptr, len));
      return recordsByPath.get(path)?.ino ?? 0;
    },
    memfs_lookup: (parentIno: number, namePtr: number, nameLen: number) => {
      const parent = recordsByIno.get(parentIno);
      if (!parent || parent.kind !== "dir") {
        return 0;
      }
      const name = decodeGuestBytes(memory, namePtr, nameLen);
      const path = parent.path === "/" ? `/${name}` : `${parent.path}/${name}`;
      return recordsByPath.get(path)?.ino ?? 0;
    },
    memfs_inode_mode: (ino: number) => recordsByIno.get(ino)?.mode ?? 0,
    memfs_file_size: (ino: number) => BigInt(recordsByIno.get(ino)?.data.length ?? 0),
    memfs_reserve_inode_until: (ino: number) => {
      reservedIno = ino;
      if (ino >= nextIno) {
        nextIno = ino + 1;
      }
      return 0n;
    },
    memfs_ensure_dir: (parentIno: number, namePtr: number, nameLen: number, mode: number) => {
      const parent = recordsByIno.get(parentIno);
      if (!parent || parent.kind !== "dir") {
        return 0;
      }
      const name = decodeGuestBytes(memory, namePtr, nameLen);
      const path = parent.path === "/" ? `/${name}` : `${parent.path}/${name}`;
      const existing = recordsByPath.get(path);
      if (existing) {
        return existing.ino;
      }
      const record: InodeRecord = {
        ino: allocateIno(),
        path,
        kind: "dir",
        mode: 0o040000 | mode,
        data: new Uint8Array(),
        materialized: false,
      };
      putRecord(record);
      return record.ino;
    },
    memfs_create_file: (parentIno: number, namePtr: number, nameLen: number) => {
      const parent = recordsByIno.get(parentIno);
      if (!parent || parent.kind !== "dir") {
        return 0;
      }
      const name = decodeGuestBytes(memory, namePtr, nameLen);
      const path = parent.path === "/" ? `/${name}` : `${parent.path}/${name}`;
      const existing = recordsByPath.get(path);
      if (existing) {
        return existing.ino;
      }
      const record: InodeRecord = {
        ino: allocateIno(),
        path,
        kind: "file",
        mode: 0o100644,
        data: new Uint8Array(),
        materialized: false,
      };
      putRecord(record);
      return record.ino;
    },
    memfs_unlink: (parentIno: number, namePtr: number, nameLen: number) => {
      const parent = recordsByIno.get(parentIno);
      if (!parent || parent.kind !== "dir") {
        return -2n;
      }
      const name = decodeGuestBytes(memory, namePtr, nameLen);
      const path = parent.path === "/" ? `/${name}` : `${parent.path}/${name}`;
      const existing = recordsByPath.get(path);
      if (!existing || existing.kind === "dir") {
        return -2n;
      }
      recordsByPath.delete(path);
      recordsByIno.delete(existing.ino);
      opLog.push(`unlink:${path}`);
      return 0n;
    },
    memfs_rmdir: (parentIno: number, namePtr: number, nameLen: number) => {
      const parent = recordsByIno.get(parentIno);
      if (!parent || parent.kind !== "dir") {
        return -2n;
      }
      const name = decodeGuestBytes(memory, namePtr, nameLen);
      const path = parent.path === "/" ? `/${name}` : `${parent.path}/${name}`;
      const existing = recordsByPath.get(path);
      if (!existing || existing.kind !== "dir") {
        return -2n;
      }
      recordsByPath.delete(path);
      recordsByIno.delete(existing.ino);
      opLog.push(`rmdir:${path}`);
      return 0n;
    },
    memfs_create_symlink: (parentIno: number, namePtr: number, nameLen: number, targetPtr: number, targetLen: number) => {
      const parent = recordsByIno.get(parentIno);
      if (!parent || parent.kind !== "dir") {
        return 0;
      }
      const name = decodeGuestBytes(memory, namePtr, nameLen);
      const path = parent.path === "/" ? `/${name}` : `${parent.path}/${name}`;
      const existing = recordsByPath.get(path);
      if (existing) {
        return 0;
      }
      const record: InodeRecord = {
        ino: allocateIno(),
        path,
        kind: "symlink",
        mode: 0o120777,
        data: new Uint8Array(memory.buffer.slice(targetPtr, targetPtr + targetLen)),
        materialized: false,
      };
      putRecord(record);
      return record.ino;
    },
    memfs_set_file_size_shallow: (ino: number, size: bigint) => {
      const record = recordsByIno.get(ino);
      opLog.push(`shallow:${record?.path ?? ino}:${size}`);
      if (!record || record.kind !== "file" || record.materialized) {
        return -1n;
      }
      const next = Number(size);
      if (record.data.length === next) {
        return size;
      }
      if (record.data.length > next) {
        record.data = record.data.slice(0, next);
        return size;
      }
      const grown = new Uint8Array(next);
      grown.set(record.data);
      record.data = grown;
      return size;
    },
    memfs_set_file_external: (ino: number, size: bigint) => {
      const record = recordsByIno.get(ino);
      opLog.push(`external:${record?.path ?? ino}:${size}`);
      if (!record || record.kind !== "file") {
        return -1n;
      }
      record.data = new Uint8Array(Number(size));
      record.materialized = false;
      return 0n;
    },
    memfs_truncate_file: (ino: number, size: bigint) => {
      const record = recordsByIno.get(ino);
      opLog.push(`truncate:${record?.path ?? ino}:${size}`);
      if (!record || record.kind !== "file") {
        return -1n;
      }
      const nextSize = Number(size);
      const next = new Uint8Array(nextSize);
      next.set(record.data.subarray(0, nextSize));
      record.data = next;
      record.materialized = true;
      return 0n;
    },
    memfs_write_file_at: (ino: number, offset: bigint, ptr: number, len: number) => {
      const record = recordsByIno.get(ino);
      opLog.push(`write:${record?.path ?? ino}:${len}`);
      if (!record || record.kind !== "file") {
        return -1n;
      }
      const required = Number(offset) + len;
      if (record.data.length < required) {
        const next = new Uint8Array(required);
        next.set(record.data);
        record.data = next;
      }
      record.data.set(new Uint8Array(memory.buffer, ptr, len), Number(offset));
      record.materialized = true;
      return BigInt(len);
    },
    memfs_read_file_to: (ino: number, offset: bigint, ptr: number, len: number) => {
      const record = recordsByIno.get(ino);
      if (!record || record.kind !== "file") {
        return -1n;
      }
      const start = Number(offset);
      const available = Math.max(0, record.data.length - start);
      const readLen = Math.min(len, available);
      new Uint8Array(memory.buffer, ptr, readLen).set(record.data.subarray(start, start + readLen));
      return BigInt(readLen);
    },
    memfs_readdir_names: (ino: number, ptr: number, len: number) => {
      const record = recordsByIno.get(ino);
      if (!record || record.kind !== "dir") {
        return -1n;
      }
      const names = [...recordsByPath.values()]
        .filter((child) => child.path !== record.path && parentPath(child.path) === record.path)
        .map((child) => child.path.split("/").pop() ?? "")
        .sort();
      let offset = 0;
      const bytes = new Uint8Array(memory.buffer, ptr, len);
      for (const name of names) {
        const nameBytes = encoder.encode(name);
        if (offset + nameBytes.length + 1 > len) {
          break;
        }
        bytes.set(nameBytes, offset);
        offset += nameBytes.length;
        bytes[offset] = 0;
        offset += 1;
      }
      return BigInt(offset);
    },
    memfs_set_mode: (ino: number, mode: number) => {
      const record = recordsByIno.get(ino);
      if (!record) {
        return -1n;
      }
      const kindMask = record.kind === "dir" ? 0o040000 : 0o100000;
      record.mode = kindMask | mode;
      return 0n;
    },
    supervisor_fs_dirty_page_op_write: () => 1,
    supervisor_fs_dirty_page_op_truncate: () => 2,
    supervisor_fs_dirty_page_op_flush: () => 3,
    supervisor_fs_dirty_page_flag_update_inode: () => 1,
    supervisor_fs_dirty_page_flag_publish_dirty: () => 2,
    supervisor_fs_dirty_page_flag_writeback_required: () => 4,
    supervisor_fs_page_table_source_backing: () => 0,
    supervisor_fs_page_table_source_resident: () => 1,
    supervisor_fs_page_table_source_decision: supervisorPageTableSourceDecision,
    supervisor_fs_dirty_page_decision: supervisorDirtyPageDecision,
    supervisor_fs_dirty_page_decision_next_size: () => BigInt(lastDirtyPageDecision.nextSize),
    supervisor_fs_dirty_page_decision_file_object_id: () => BigInt(lastDirtyPageDecision.fileObjectId),
    supervisor_fs_dirty_page_decision_generation: () => BigInt(lastDirtyPageDecision.generation),
    supervisor_fs_dirty_page_decision_flags: () => lastDirtyPageDecision.flags,
    opLog,
    fileData: (path: string) => {
      const record = recordsByPath.get(normalizePath(path));
      if (!record || record.kind !== "file") {
        return new Uint8Array();
      }
      return new Uint8Array(record.data);
    },
    fileIno: (path: string) => recordsByPath.get(normalizePath(path))?.ino ?? 0,
    linkTarget: (path: string) => {
      const record = recordsByPath.get(normalizePath(path));
      if (!record || record.kind !== "symlink") {
        return null;
      }
      return decoder.decode(record.data);
    },
    overwriteFile: (path: string, data: Uint8Array) => {
      const record = recordsByPath.get(normalizePath(path));
      if (!record || record.kind !== "file") {
        throw new Error(`missing file: ${path}`);
      }
      record.data = new Uint8Array(data);
      record.materialized = true;
    },
    createSymlink: (path: string, target: Uint8Array) => {
      const normalized = normalizePath(path);
      const parts = normalized.split("/").filter(Boolean);
      const name = parts.pop();
      if (!name) {
        throw new Error(`invalid symlink path: ${path}`);
      }
      const parent = parts.length === 0 ? "/" : `/${parts.join("/")}`;
      const parentRecord = recordsByPath.get(parent);
      if (!parentRecord || parentRecord.kind !== "dir") {
        throw new Error(`missing parent directory: ${parent}`);
      }
      const record: InodeRecord = {
        ino: allocateIno(),
        path: parent === "/" ? `/${name}` : `${parent}/${name}`,
        kind: "symlink",
        mode: 0o120777,
        data: new Uint8Array(target),
        materialized: false,
      };
      putRecord(record);
      return record.ino;
    },
    setHeapPtr: (ptr: number) => {
      heapPtr = ptr;
    },
  };

  return exports as unknown as FakeKernelExports;
}

function parentPath(path: string): string {
  const normalized = normalizePath(path);
  return normalized.split("/").slice(0, -1).join("/") || "/";
}

function makeFsState(pageCacheDataCapacityBytes = 16 * 1024 * 1024) {
  const sabPageCache = new SharedArrayBuffer(DATA_REGION_OFFSET + pageCacheDataCapacityBytes);
  const canonicalExports = makeKernelExports();
  const memberExports = makeKernelExports();
  const processKernels = new Map<number, KernelExports>([[2, memberExports]]);
  const seedEntries = new Map<string, import("../../../src/fs/types.js").FsSnapshotEntry>();
  const fsState = createKernelWorkerFsState({
    getCanonicalExports: () => canonicalExports,
    getSabPageCache: () => sabPageCache,
    processKernels,
    seedEntries,
    encoder,
    decoder,
    fanoutCanonicalMemfs: () => {},
    publishRootInode: (mode = 0o755) => {
      publishInode(sabPageCache, 1, {
        mode: 0o040000 | mode,
        size: 0,
        parentIno: 1,
        name: new Uint8Array(),
        nextSibling: 0,
      });
    },
    resolvePathIn: (targetExports, path) => targetExports.memfs_resolve_path(
      (() => {
        const bytes = encoder.encode(normalizePath(path));
        const ptr = targetExports.kernel_heap_ptr() + 0x10000;
        new Uint8Array(targetExports.memory.buffer).set(bytes, ptr);
        return ptr;
      })(),
      normalizePath(path).length,
    ),
    upsertDirSeed: (path, mode = 0o755) => {
      seedEntries.set(normalizePath(path), { kind: "dir", path: normalizePath(path), mode });
    },
    upsertFileSeed: (path, data, mode = 0o644, options = {}) => {
      seedEntries.set(normalizePath(path), {
        kind: "file",
        path: normalizePath(path),
        size: options.size ?? data.length,
        mode,
      });
    },
    upsertSymlinkSeed: (path, target, mode = 0o777) => {
      seedEntries.set(normalizePath(path), {
        kind: "symlink",
        path: normalizePath(path),
        target,
        size: target.length,
        mode,
      });
    },
  });
  return { canonicalExports, memberExports, fsState, sabPageCache, seedEntries };
}

test("kernel-worker loadFsSnapshot keeps provided file data page-cache-backed while seed is metadata-only", () => {
  const { canonicalExports, fsState, sabPageCache, seedEntries } = makeFsState();
  const data = new Uint8Array(128 * 1024).map((_, index) => index & 0xff);

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", data, mode: 0o755 },
  ]);

  const ino = canonicalExports.fileIno("/lib/runtime.bin");
  expect(ino).toBeGreaterThan(1);
  expectBytesEqual(fsState.readFile("/lib/runtime.bin"), data);
  expect(canonicalExports.opLog).toContain(`shallow:/lib/runtime.bin:${data.length}`);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("truncate:/lib/runtime.bin:"))).toBe(false);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("write:/lib/runtime.bin:"))).toBe(false);
  expect(readInodeRecord(sabPageCache, ino)?.size).toBe(data.length);
  expect(seedEntries.get("/lib/runtime.bin")).toEqual({
    kind: "file",
    path: "/lib/runtime.bin",
    size: data.length,
    mode: 0o755,
  });
});

test("kernel-worker loadFsSnapshot keeps large provided file data page-cache-backed", () => {
  const { canonicalExports, fsState, sabPageCache, seedEntries } = makeFsState();
  const data = new Uint8Array((256 * 1024) + 17).map((_, index) => (index * 17) & 0xff);

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/usr/lib/runtime-large.bin", data, mode: 0o755 },
  ]);

  const ino = canonicalExports.fileIno("/usr/lib/runtime-large.bin");
  expect(ino).toBeGreaterThan(1);
  expectBytesEqual(fsState.readFile("/usr/lib/runtime-large.bin"), data);
  expect(canonicalExports.opLog).toContain(`shallow:/usr/lib/runtime-large.bin:${data.length}`);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("truncate:/usr/lib/runtime-large.bin:"))).toBe(false);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("write:/usr/lib/runtime-large.bin:"))).toBe(false);
  expect(readInodeRecord(sabPageCache, ino)?.size).toBe(data.length);
  expect(seedEntries.get("/usr/lib/runtime-large.bin")).toEqual({
    kind: "file",
    path: "/usr/lib/runtime-large.bin",
    size: data.length,
    mode: 0o755,
  });
});

test("kernel-worker loadFsSnapshot preserves metadata-only file sizes and backing identity as external state", () => {
  const { canonicalExports, fsState, sabPageCache, seedEntries } = makeFsState();
  const backingExtent = {
    sourceIdentity: "artifact:runtime.bin",
    digest: "sha256:runtime",
    offset: 0,
    length: 128 * 1024,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: 128 * 1024, mode: 0o755, backingExtent },
  ]);

  const ino = canonicalExports.fileIno("/lib/runtime.bin");
  expect(ino).toBeGreaterThan(1);
  expect(canonicalExports.fileData("/lib/runtime.bin").length).toBe(128 * 1024);
  expect(canonicalExports.opLog).toContain("external:/lib/runtime.bin:131072");
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("truncate:/lib/runtime.bin:"))).toBe(false);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("write:/lib/runtime.bin:"))).toBe(false);
  expect(readInodeRecord(sabPageCache, ino)?.size).toBe(128 * 1024);
  expect(seedEntries.get("/lib/runtime.bin")).toEqual({
    kind: "file",
    path: "/lib/runtime.bin",
    size: 128 * 1024,
    mode: 0o755,
    backingExtent,
  });
});

test("kernel-worker reads metadata-only snapshot files from registered backing source", () => {
  const { fsState } = makeFsState();
  const source = encoder.encode("prefix:artifact-bytes:suffix");
  const backingExtent = {
    sourceIdentity: "artifact:runtime.bin",
    offset: "prefix:".length,
    length: "artifact-bytes".length,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);
  fsState.registerBackingSource(backingExtent.sourceIdentity, source);

  expect(new TextDecoder().decode(fsState.readFile("/lib/runtime.bin"))).toBe("artifact-bytes");
  expect(new TextDecoder().decode(fsState.readBackingFileRangeByInode(
    fsState.resolvePath("/lib/runtime.bin"),
    9,
    5,
  ) ?? new Uint8Array())).toBe("bytes");
});

test("kernel-worker source-backed range read does not materialize the whole file", () => {
  const { fsState } = makeFsState();
  const source = encoder.encode("prefix:artifact-bytes:suffix");
  const backingExtent = {
    sourceIdentity: "artifact:runtime.bin",
    offset: "prefix:".length,
    length: "artifact-bytes".length,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);
  fsState.registerBackingSource(backingExtent.sourceIdentity, source);

  const ino = fsState.resolvePath("/lib/runtime.bin");
  const range = fsState.readBackingFileRangeByInode(ino, 9, 5);
  expect(new TextDecoder().decode(range ?? new Uint8Array())).toBe("bytes");
  expect(range?.buffer).toBe(source.buffer);
  expect(fsState.readBackingFileByInode(ino)).toBeNull();
});

test("kernel-worker reads host-file backed snapshot ranges without whole-file materialization", async () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-fs-state-host-source-"));
  try {
    const { fsState } = makeFsState();
    const source = encoder.encode("prefix:artifact-bytes:suffix");
    const hostFile = join(root, "runtime.bin");
    writeFileSync(hostFile, source);
    const backingExtent = {
      sourceIdentity: "artifact:runtime.bin",
      offset: "prefix:".length,
      length: "artifact-bytes".length,
    };

    fsState.loadFsSnapshot([
      { kind: "dir", path: "/", mode: 0o755 },
      { kind: "file", path: "/lib/runtime.bin", size: backingExtent.length, mode: 0o644, backingExtent },
    ]);
    fsState.registerBackingSourceObject(
      backingExtent.sourceIdentity,
      await createNodeHostFileBackingSource(hostFile, source.byteLength),
    );

    const ino = fsState.resolvePath("/lib/runtime.bin");
    expect(new TextDecoder().decode(fsState.readBackingFileRangeByInode(ino, 9, 5) ?? new Uint8Array()))
      .toBe("bytes");
    expect(fsState.readBackingFileByInode(ino)).toBeNull();
    expect(new TextDecoder().decode(fsState.readFile("/lib/runtime.bin"))).toBe("artifact-bytes");
    expect(new TextDecoder().decode(fsState.readBackingFileByInode(ino) ?? new Uint8Array())).toBe("artifact-bytes");
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("kernel-worker reads content-addressed backing sources after digest verification", async () => {
  const { fsState } = makeFsState();
  const source = encoder.encode("prefix:artifact-bytes:suffix");
  const backingExtent = {
    sourceIdentity: "sha256:8cdcc8c061d35ba62f2629549a178f433a8d7a38e6beaaafd912cad355e0d775",
    digest: "sha256:8cdcc8c061d35ba62f2629549a178f433a8d7a38e6beaaafd912cad355e0d775",
    offset: "prefix:".length,
    length: "artifact-bytes".length,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);
  fsState.registerBackingSourceObject(
    backingExtent.sourceIdentity,
    await createSha256VerifiedBackingSource(source, backingExtent.digest),
  );

  const ino = fsState.resolvePath("/lib/runtime.bin");
  expect(new TextDecoder().decode(fsState.readBackingFileRangeByInode(ino, 9, 5) ?? new Uint8Array()))
    .toBe("bytes");
  expect(new TextDecoder().decode(fsState.readFile("/lib/runtime.bin"))).toBe("artifact-bytes");
});

test("kernel-worker rejects content-addressed backing sources with mismatched digests", async () => {
  await expect(
    createSha256VerifiedBackingSource(encoder.encode("artifact-bytes"), "sha256:".padEnd(71, "0")),
  ).rejects.toThrow("backing source digest mismatch");
});

test("kernel-worker partial write to source-backed snapshot file is dirty resident authoritative", () => {
  const { fsState } = makeFsState();
  const source = encoder.encode("prefix:abcdef:suffix");
  const backingExtent = {
    sourceIdentity: "artifact:runtime.bin",
    offset: "prefix:".length,
    length: "abcdef".length,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);
  fsState.registerBackingSource(backingExtent.sourceIdentity, source);

  const ino = fsState.resolvePath("/lib/runtime.bin");
  expect(fsState.writeDirtyResidentFileByInode(ino, 3, encoder.encode("XYZ"))).toBe(3);
  expect(new TextDecoder().decode(fsState.readBackingFileRangeByInode(ino, 0, 3) ?? new Uint8Array())).toBe("abc");
  expect(new TextDecoder().decode(fsState.readBackingFileByInode(ino) ?? new Uint8Array())).toBe("abcXYZ");
  expect(new TextDecoder().decode(fsState.readFile("/lib/runtime.bin"))).toBe("abcXYZ");
  const metadataSnapshot = fsState.snapshotCanonicalFs(false).find((entry) => (
    entry.kind === "file" && entry.path === "/lib/runtime.bin"
  ));
  expect(metadataSnapshot?.kind === "file" ? metadataSnapshot.backingExtent : undefined).toBeUndefined();
  const dataSnapshot = fsState.snapshotCanonicalFs(true).find((entry) => (
    entry.kind === "file" && entry.path === "/lib/runtime.bin"
  ));
  expect(dataSnapshot?.kind === "file" ? dataSnapshot.data : undefined).toEqual(encoder.encode("abcXYZ"));
  expect(dataSnapshot?.kind === "file" ? dataSnapshot.backingExtent : undefined).toBeUndefined();
});

test("kernel-worker dirty resident source-backed range reads fetch missing clean pages", () => {
  const { fsState } = makeFsState();
  const pageSize = 4096;
  const source = new Uint8Array(pageSize * 3);
  for (let offset = 0; offset < source.length; offset += 1) {
    source[offset] = (offset * 13 + 7) & 0xff;
  }
  const dirty = new Uint8Array(pageSize);
  dirty.fill(0xa5);
  const backingExtent = {
    sourceIdentity: "artifact:large.bin",
    offset: 0,
    length: source.length,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/large.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);
  fsState.registerBackingSourceRange(backingExtent.sourceIdentity, source.length, 0, source.subarray(0, pageSize));
  fsState.registerBackingSourceRange(
    backingExtent.sourceIdentity,
    source.length,
    pageSize * 2,
    source.subarray(pageSize * 2),
  );

  const ino = fsState.resolvePath("/lib/large.bin");
  expect(fsState.writeDirtyResidentFileByInode(ino, 0, dirty)).toBe(pageSize);
  expect(fsState.writeDirtyResidentFileByInode(ino, pageSize * 2, dirty)).toBe(pageSize);
  expect(fsState.readBackingFileRangeByInode(ino, pageSize, 16)).toBeNull();

  fsState.registerBackingSourceRange(
    backingExtent.sourceIdentity,
    source.length,
    pageSize,
    source.subarray(pageSize, pageSize + 16),
  );
  expect(fsState.readBackingFileRangeByInode(ino, pageSize, 16)).toEqual(source.subarray(pageSize, pageSize + 16));
});

test("kernel-worker sparse dirty resident writes read source bytes then zero holes", () => {
  const { fsState } = makeFsState();
  const source = encoder.encode("abcdef");
  const backingExtent = {
    sourceIdentity: "artifact:sparse.bin",
    offset: 0,
    length: source.length,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/sparse.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);
  fsState.registerBackingSource(backingExtent.sourceIdentity, source);

  const ino = fsState.resolvePath("/lib/sparse.bin");
  const offset = 8192;
  const data = encoder.encode("XYZ");
  expect(fsState.writeDirtyResidentFileByInode(ino, offset, data)).toBe(data.length);
  expect(new TextDecoder().decode(fsState.readBackingFileRangeByInode(ino, 0, source.length) ?? new Uint8Array()))
    .toBe("abcdef");
  expect(fsState.readBackingFileRangeByInode(ino, source.length, 16)).toEqual(new Uint8Array(16));
  expect(new TextDecoder().decode(fsState.readBackingFileRangeByInode(ino, offset, data.length) ?? new Uint8Array()))
    .toBe("XYZ");
});

test("kernel-worker applies supervisor dirty write denial without publishing resident pages", () => {
  const { canonicalExports, fsState, sabPageCache } = makeFsState();

  fsState.writeFile("/lib/runtime.bin", encoder.encode("abcdef"), 0o644, false);
  const ino = canonicalExports.fileIno("/lib/runtime.bin");
  canonicalExports.supervisor_fs_dirty_page_decision = () => -28;

  expect(fsState.writeDirtyResidentFileByInode(ino, 3, encoder.encode("XYZ"))).toBe(-28);
  expect(new TextDecoder().decode(fsState.readFile("/lib/runtime.bin"))).toBe("abcdef");
  expect(evictResidentPage(sabPageCache, ino, 0)).toBe("not-found");
});

test("kernel-worker follows supervisor page-table source selection", () => {
  const { canonicalExports, fsState } = makeFsState();

  fsState.writeFile("/lib/runtime.bin", encoder.encode("abcdef"), 0o644, false);
  const ino = canonicalExports.fileIno("/lib/runtime.bin");
  expect(fsState.writeDirtyResidentFileByInode(ino, 3, encoder.encode("XYZ"))).toBe(3);
  expect(new TextDecoder().decode(fsState.readBackingFileByInode(ino) ?? new Uint8Array()))
    .toBe("abcXYZ");

  canonicalExports.supervisor_fs_page_table_source_decision = () => (
    canonicalExports.supervisor_fs_page_table_source_backing?.() ?? 0
  );
  expect(new TextDecoder().decode(fsState.readBackingFileByInode(ino) ?? new Uint8Array()))
    .toBe("abcdef");
});

test("kernel-worker does not write back when supervisor dirty flush says no-op", () => {
  const { canonicalExports, fsState, sabPageCache } = makeFsState();

  fsState.writeFile("/lib/runtime.bin", encoder.encode("abcdef"), 0o644, false);
  const ino = canonicalExports.fileIno("/lib/runtime.bin");
  expect(fsState.writeDirtyResidentFileByInode(ino, 3, encoder.encode("XYZ"))).toBe(3);
  const decide = canonicalExports.supervisor_fs_dirty_page_decision;
  canonicalExports.supervisor_fs_dirty_page_decision = (...args) => {
    if (args[0] === canonicalExports.supervisor_fs_dirty_page_op_flush?.()) {
      return 0;
    }
    return decide?.(...args) ?? -22;
  };
  canonicalExports.supervisor_fs_dirty_page_decision_flags = () => 0;

  expect(fsState.flushDirtyResidentFileByInode(ino)).toBe(true);
  expect(evictResidentPage(sabPageCache, ino, 0)).toBe("dirty-pinned");
  expect(new TextDecoder().decode(fsState.readFile("/lib/runtime.bin"))).toBe("abcXYZ");
});

test("kernel-worker routes empty dirty flush through supervisor no-op decision", () => {
  const { canonicalExports, fsState } = makeFsState();
  const decide = canonicalExports.supervisor_fs_dirty_page_decision;
  let flushDecisions = 0;
  canonicalExports.supervisor_fs_dirty_page_decision = (...args) => {
    if (args[0] === canonicalExports.supervisor_fs_dirty_page_op_flush?.()) {
      flushDecisions += 1;
    }
    return decide?.(...args) ?? -22;
  };

  expect(fsState.flushDirtyResidentFileByInode(2)).toBe(true);
  expect(flushDecisions).toBe(1);
});

test("kernel-worker flushes dirty resident pages to backing and leaves clean resident pages", () => {
  const { canonicalExports, fsState, sabPageCache } = makeFsState();

  fsState.writeFile("/lib/runtime.bin", encoder.encode("abcdef"), 0o644, false);
  const ino = canonicalExports.fileIno("/lib/runtime.bin");
  expect(ino).toBeGreaterThan(1);
  expect(fsState.writeDirtyResidentFileByInode(ino, 3, encoder.encode("XYZ"))).toBe(3);
  expect(evictResidentPage(sabPageCache, ino, 0)).toBe("dirty-pinned");

  expect(fsState.flushDirtyResidentFileByInode(ino)).toBe(true);
  expect(new TextDecoder().decode(readResidentFileData(sabPageCache, ino, 0, 6) ?? new Uint8Array()))
    .toBe("abcXYZ");
  expect(evictResidentPage(sabPageCache, ino, 0)).toBe("evicted");
  expect(new TextDecoder().decode(fsState.readFile("/lib/runtime.bin"))).toBe("abcXYZ");
});

test("kernel-worker flushes source-backed dirty resident ranges without full source materialization", () => {
  const { fsState, sabPageCache } = makeFsState();
  const source = encoder.encode("abcdef");
  const sourceReads: Array<[number, number]> = [];
  const backingExtent = {
    sourceIdentity: "artifact:runtime.bin",
    offset: 0,
    length: source.length,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);
  fsState.registerBackingSourceObject(backingExtent.sourceIdentity, {
    byteLength: source.length,
    readRange(offset, length) {
      sourceReads.push([offset, length]);
      return source.subarray(offset, Math.min(source.length, offset + length));
    },
  });

  const ino = fsState.resolvePath("/lib/runtime.bin");
  expect(fsState.writeDirtyResidentFileByInode(ino, 3, encoder.encode("XYZ"))).toBe(3);
  expect(sourceReads).toEqual([[0, source.length]]);
  sourceReads.length = 0;

  expect(fsState.flushDirtyResidentFileByInode(ino)).toBe(true);
  expect(sourceReads).toEqual([]);
  expect(new TextDecoder().decode(readResidentFileData(sabPageCache, ino, 0, 6) ?? new Uint8Array()))
    .toBe("abcXYZ");
  expect(evictResidentPage(sabPageCache, ino, 0)).toBe("evicted");

  expect(new TextDecoder().decode(fsState.readBackingFileRangeByInode(ino, 0, 6) ?? new Uint8Array()))
    .toBe("abcXYZ");
  expect(sourceReads).toEqual([[0, source.length]]);
});

test("kernel-worker flush truncates source-backed dirty resident files to inode size", () => {
  const { fsState, sabPageCache } = makeFsState();
  const source = encoder.encode("abcdef");
  const sourceReads: Array<[number, number]> = [];
  const backingExtent = {
    sourceIdentity: "artifact:runtime.bin",
    offset: 0,
    length: source.length,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);
  fsState.registerBackingSourceObject(backingExtent.sourceIdentity, {
    byteLength: source.length,
    readRange(offset, length) {
      sourceReads.push([offset, length]);
      return source.subarray(offset, Math.min(source.length, offset + length));
    },
  });

  const ino = fsState.resolvePath("/lib/runtime.bin");
  expect(fsState.truncateDirtyResidentFileByInode(ino, 3)).toBe(0);
  expect(sourceReads).toEqual([[0, 3]]);
  sourceReads.length = 0;

  expect(fsState.flushDirtyResidentFileByInode(ino)).toBe(true);
  expect(sourceReads).toEqual([]);
  expect(new TextDecoder().decode(readResidentFileData(sabPageCache, ino, 0, 3) ?? new Uint8Array()))
    .toBe("abc");
  expect(evictResidentPage(sabPageCache, ino, 0)).toBe("evicted");

  expect(new TextDecoder().decode(fsState.readBackingFileRangeByInode(ino, 0, 6) ?? new Uint8Array()))
    .toBe("abc");
  expect(fsState.readBackingFileRangeByInode(ino, 3, 1)).toBeNull();
  expect(sourceReads).toEqual([[0, 3]]);
});

test("kernel-worker reads registered partial source ranges without full source materialization", () => {
  const { fsState } = makeFsState();
  const backingExtent = {
    sourceIdentity: "artifact:runtime.bin",
    offset: 0,
    length: "artifact-bytes".length,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);
  fsState.registerBackingSourceRange(
    backingExtent.sourceIdentity,
    backingExtent.length,
    9,
    encoder.encode("bytes"),
  );

  const ino = fsState.resolvePath("/lib/runtime.bin");
  expect(new TextDecoder().decode(fsState.readBackingFileRangeByInode(ino, 9, 5) ?? new Uint8Array())).toBe("bytes");
  expect(fsState.readBackingFileRangeByInode(ino, 0, backingExtent.length)).toBeNull();
  expect(fsState.readBackingFileByInode(ino)).toBeNull();
});

test("kernel-worker demand-fills OPFS source ranges into registered backing source", async () => {
  const { fsState } = makeFsState();
  const source = encoder.encode("prefix:artifact-bytes:suffix");
  const backingExtent = {
    sourceIdentity: "opfs:runtime.bin",
    offset: "prefix:".length,
    length: "artifact-bytes".length,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);
  fsState.registerOpfsBackingSource(backingExtent.sourceIdentity, source.length, {
    getFile: vi.fn(async () => new Blob([source])),
  });

  const ino = fsState.resolvePath("/lib/runtime.bin");
  expect(fsState.readBackingFileRangeByInode(ino, 9, 5)).toBeNull();
  await expect(fsState.ensureBackingSourceRangeForInode(ino, 9, 5, backingExtent.length)).resolves.toBe(true);
  expect(new TextDecoder().decode(fsState.readBackingFileRangeByInode(ino, 9, 5) ?? new Uint8Array())).toBe("bytes");
  expect(fsState.readBackingFileByInode(ino)).toBeNull();
});

test("kernel-worker async readFile materializes OPFS-backed metadata-only files", async () => {
  const { fsState } = makeFsState();
  const source = encoder.encode("prefix:artifact-bytes:suffix");
  const backingExtent = {
    sourceIdentity: "opfs:runtime.bin",
    offset: "prefix:".length,
    length: "artifact-bytes".length,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);
  fsState.registerOpfsBackingSource(backingExtent.sourceIdentity, source.length, {
    getFile: vi.fn(async () => new Blob([source])),
  });

  const ino = fsState.resolvePath("/lib/runtime.bin");
  expect(fsState.readBackingFileByInode(ino)).toBeNull();
  await expect(fsState.readFileAsync("/lib/runtime.bin"))
    .resolves.toEqual(encoder.encode("artifact-bytes"));
  expect(new TextDecoder().decode(fsState.readBackingFileByInode(ino) ?? new Uint8Array()))
    .toBe("artifact-bytes");
});

test("kernel-worker snapshot export preserves source backing identity", () => {
  const { fsState } = makeFsState();
  const backingExtent = {
    sourceIdentity: "artifact:runtime.bin",
    digest: "sha256:runtime",
    offset: 32,
    length: 4096,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);

  expect(fsState.snapshotCanonicalFs(false)).toEqual(expect.arrayContaining([
    expect.objectContaining({
      kind: "file",
      path: "/lib/runtime.bin",
      size: backingExtent.length,
      backingExtent,
    }),
  ]));
});

test("kernel-worker snapshot data export reads registered backing sources", () => {
  const { fsState } = makeFsState();
  const source = encoder.encode("prefix:artifact-bytes:suffix");
  const backingExtent = {
    sourceIdentity: "artifact:runtime.bin",
    offset: "prefix:".length,
    length: "artifact-bytes".length,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);
  fsState.registerBackingSource(backingExtent.sourceIdentity, source);

  const exported = fsState.snapshotCanonicalFs(true).find((entry) => (
    entry.kind === "file" && entry.path === "/lib/runtime.bin"
  ));
  expect(exported?.kind === "file" ? exported.data : undefined).toEqual(encoder.encode("artifact-bytes"));
  expect(exported?.backingExtent).toEqual(backingExtent);
});

test("kernel-worker loadFsSnapshot restores symlinks into seed entries", () => {
  const { canonicalExports, fsState, seedEntries } = makeFsState();
  const target = encoder.encode("/toolchain/current/bin/node");

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/toolchain", mode: 0o755 },
    { kind: "symlink", path: "/toolchain/node", target, mode: 0o777 },
  ]);

  expect(canonicalExports.fileIno("/toolchain/node")).toBeGreaterThan(1);
  expect(canonicalExports.linkTarget("/toolchain/node")).toBe("/toolchain/current/bin/node");
  expect(seedEntries.get("/toolchain/node")).toEqual({
    kind: "symlink",
    path: "/toolchain/node",
    target,
    size: target.length,
    mode: 0o777,
  });
});

test("kernel-worker syncFileDataMutation exports dirty resident metadata without whole-file rewrite", () => {
  const { canonicalExports, memberExports, fsState } = makeFsState();
  const initialData = encoder.encode("abcdefghijklmnop");
  const nextData = encoder.encode("abc");

  fsState.writeFile("/lib/runtime.bin", initialData, 0o755, false);
  const ino = canonicalExports.fileIno("/lib/runtime.bin");
  expect(ino).toBeGreaterThan(1);

  fsState.syncFileDataMutation(canonicalExports, ino);
  memberExports.opLog.length = 0;
  canonicalExports.overwriteFile("/lib/runtime.bin", nextData);
  expect(fsState.truncateDirtyResidentFileByInode(ino, nextData.length)).toBe(0);
  expect(fsState.writeDirtyResidentFileByInode(ino, 0, nextData)).toBe(nextData.length);

  const changed = fsState.syncFileDataMutation(canonicalExports, ino);

  expect(changed).toBe(true);
  expect(memberExports.opLog).toContain("external:/lib/runtime.bin:3");
  expect(memberExports.opLog.some((entry) => entry.startsWith("truncate:/lib/runtime.bin:3"))).toBe(false);
  expect(memberExports.opLog.some((entry) => entry.startsWith("write:/lib/runtime.bin:3"))).toBe(false);
  expect(canonicalExports.opLog).toContain("external:/lib/runtime.bin:3");
});

test("kernel-worker syncFileDataMutation clears stale source backing extent on export", () => {
  const { canonicalExports, fsState } = makeFsState();
  const source = encoder.encode("prefix:abcdef:suffix");
  const backingExtent = {
    sourceIdentity: "artifact:runtime.bin",
    offset: "prefix:".length,
    length: "abcdef".length,
  };

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: backingExtent.length, mode: 0o644, backingExtent },
  ]);
  fsState.registerBackingSource(backingExtent.sourceIdentity, source);
  const ino = canonicalExports.fileIno("/lib/runtime.bin");
  expect(ino).toBeGreaterThan(1);
  expect(fsState.snapshotCanonicalFs(false).find((entry) => (
    entry.kind === "file" && entry.path === "/lib/runtime.bin"
  ))).toEqual(expect.objectContaining({ backingExtent }));

  expect(fsState.truncateDirtyResidentFileByInode(ino, "mutated".length)).toBe(0);
  canonicalExports.overwriteFile("/lib/runtime.bin", encoder.encode("mutated"));
  expect(fsState.writeDirtyResidentFileByInode(ino, 0, encoder.encode("mutated"))).toBe("mutated".length);
  expect(fsState.syncFileDataMutation(canonicalExports, ino)).toBe(true);

  const metadataSnapshot = fsState.snapshotCanonicalFs(false).find((entry) => (
    entry.kind === "file" && entry.path === "/lib/runtime.bin"
  ));
  expect(metadataSnapshot?.kind === "file" ? metadataSnapshot.backingExtent : undefined).toBeUndefined();
  const dataSnapshot = fsState.snapshotCanonicalFs(true).find((entry) => (
    entry.kind === "file" && entry.path === "/lib/runtime.bin"
  ));
  expect(dataSnapshot?.kind === "file" ? dataSnapshot.data : undefined).toEqual(encoder.encode("mutated"));
  expect(dataSnapshot?.kind === "file" ? dataSnapshot.backingExtent : undefined).toBeUndefined();
});

test("kernel-worker restorePublishedRegularFileDataFromPageCache externalizes without rematerializing bytes", () => {
  const { canonicalExports, fsState } = makeFsState();
  const publishedData = encoder.encode("published-data");
  const staleInlineData = encoder.encode("stale-inline!!");

  expect(staleInlineData.length).toBe(publishedData.length);
  fsState.writeFile("/lib/runtime.bin", publishedData, 0o755, false);
  canonicalExports.overwriteFile("/lib/runtime.bin", staleInlineData);
  canonicalExports.opLog.length = 0;

  fsState.restorePublishedRegularFileDataFromPageCache();

  expect(canonicalExports.opLog).toContain(`external:/lib/runtime.bin:${publishedData.length}`);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("truncate:/lib/runtime.bin:"))).toBe(false);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("write:/lib/runtime.bin:"))).toBe(false);
  expectBytesEqual(fsState.readFile("/lib/runtime.bin"), publishedData);
});

test("kernel-worker restorePublishedRegularFileDataForInode only externalizes the selected inode", () => {
  const { canonicalExports, fsState } = makeFsState();
  const runtimeData = encoder.encode("published-data");
  const toolData = encoder.encode("tool-data");

  fsState.writeFile("/lib/runtime.bin", runtimeData, 0o755, false);
  fsState.writeFile("/lib/tool.bin", toolData, 0o755, false);
  const toolIno = canonicalExports.fileIno("/lib/tool.bin");
  canonicalExports.overwriteFile("/lib/runtime.bin", encoder.encode("stale-inline!!"));
  canonicalExports.overwriteFile("/lib/tool.bin", encoder.encode("stale-bin"));
  canonicalExports.opLog.length = 0;

  expect(fsState.restorePublishedRegularFileDataForInode(toolIno)).toBe(true);

  expect(canonicalExports.opLog).toContain(`external:/lib/tool.bin:${toolData.length}`);
  expect(canonicalExports.opLog).not.toContain(`external:/lib/runtime.bin:${runtimeData.length}`);
  expect(fsState.restorePublishedRegularFileDataForInode(9999)).toBe(false);
});

test("kernel-worker syncMetadataMutation materializes absolute-path file creation without full fanout", () => {
  const { canonicalExports, memberExports, fsState, sabPageCache } = makeFsState();

  fsState.mkdir("/work", 0o755, false);
  const name = encoder.encode("new.txt");
  const ptr = canonicalExports.kernel_heap_ptr() + 0x12000;
  new Uint8Array(canonicalExports.memory.buffer).set(name, ptr);
  expect(canonicalExports.memfs_create_file(canonicalExports.fileIno("/work"), ptr, name.length)).toBeGreaterThan(1);

  const changed = fsState.syncMetadataMutation(canonicalExports, "/work/new.txt");
  const ino = canonicalExports.fileIno("/work/new.txt");

  expect(changed).toBe(true);
  expect(ino).toBeGreaterThan(1);
  expect(memberExports.fileIno("/work/new.txt")).toBe(ino);
  expect(readInodeRecord(sabPageCache, ino)).not.toBeNull();
});

test("kernel-worker syncMetadataMutation updates existing published file size", () => {
  const { canonicalExports, memberExports, fsState, sabPageCache } = makeFsState();

  const workName = encoder.encode("work");
  const workPtr = memberExports.kernel_heap_ptr() + 0x12000;
  new Uint8Array(memberExports.memory.buffer).set(workName, workPtr);
  const workIno = memberExports.memfs_ensure_dir(1, workPtr, workName.length, 0o755);
  expect(workIno).toBeGreaterThan(1);
  publishInode(sabPageCache, workIno, {
    mode: 0o040755,
    size: 0,
    parentIno: 1,
    name: workName,
    nextSibling: 0,
  });

  const name = encoder.encode("out.txt");
  const ptr = memberExports.kernel_heap_ptr() + 0x13000;
  new Uint8Array(memberExports.memory.buffer).set(name, ptr);
  const ino = memberExports.memfs_create_file(workIno, ptr, name.length);
  expect(ino).toBeGreaterThan(1);
  publishInode(sabPageCache, ino, {
    mode: 0o100644,
    size: 4096,
    parentIno: workIno,
    name,
    nextSibling: 0,
  });

  expect(fsState.syncMetadataMutation(memberExports, "/work/out.txt")).toBe(true);

  expect(canonicalExports.fileIno("/work/out.txt")).toBe(ino);
  expect(readInodeRecord(sabPageCache, ino)?.size).toBe(0);
});

test("kernel-worker dirty write uses process-local truncated inode metadata", () => {
  const { memberExports, fsState, sabPageCache } = makeFsState();

  const workName = encoder.encode("work");
  const workPtr = memberExports.kernel_heap_ptr() + 0x12000;
  new Uint8Array(memberExports.memory.buffer).set(workName, workPtr);
  const workIno = memberExports.memfs_ensure_dir(1, workPtr, workName.length, 0o755);
  expect(workIno).toBeGreaterThan(1);

  const name = encoder.encode("out.txt");
  const ptr = memberExports.kernel_heap_ptr() + 0x13000;
  new Uint8Array(memberExports.memory.buffer).set(name, ptr);
  const ino = memberExports.memfs_create_file(workIno, ptr, name.length);
  expect(ino).toBeGreaterThan(1);

  publishInode(sabPageCache, ino, {
    mode: 0o100644,
    size: 4096,
    parentIno: workIno,
    name,
    nextSibling: 0,
  });

  const data = encoder.encode("dynamic-backing-ok\n");
  expect(fsState.writeDirtyResidentFileByInode(ino, 0, data, memberExports)).toBe(data.length);
  const record = readInodeRecord(sabPageCache, ino);
  expect(record?.size).toBe(data.length);
  expect(readResidentFileData(sabPageCache, ino, 0, data.length)).toEqual(data);
});

test("kernel-worker syncFileDataMutation externalizes matching published resident file metadata", () => {
  const { canonicalExports, memberExports, fsState, sabPageCache } = makeFsState();

  const workName = encoder.encode("work");
  const workPtr = memberExports.kernel_heap_ptr() + 0x12000;
  new Uint8Array(memberExports.memory.buffer).set(workName, workPtr);
  const workIno = memberExports.memfs_ensure_dir(1, workPtr, workName.length, 0o755);
  expect(workIno).toBeGreaterThan(1);
  publishInode(sabPageCache, workIno, {
    mode: 0o040755,
    size: 0,
    parentIno: 1,
    name: workName,
    nextSibling: 0,
  });

  const name = encoder.encode("out.txt");
  const ptr = memberExports.kernel_heap_ptr() + 0x13000;
  new Uint8Array(memberExports.memory.buffer).set(name, ptr);
  const ino = memberExports.memfs_create_file(workIno, ptr, name.length);
  expect(ino).toBeGreaterThan(1);

  const size = 65391;
  publishInode(sabPageCache, ino, {
    mode: 0o100644,
    size,
    parentIno: workIno,
    name,
    nextSibling: 0,
    fileObjectId: ino,
    generation: 2,
  });
  expect(memberExports.memfs_set_file_size_shallow(ino, BigInt(size))).toBe(BigInt(size));
  canonicalExports.opLog.length = 0;
  memberExports.opLog.length = 0;

  expect(fsState.syncFileDataMutation(memberExports, ino)).toBe(true);

  expect(canonicalExports.opLog).toContain(`external:/work/out.txt:${size}`);
  expect(memberExports.opLog).toContain(`external:/work/out.txt:${size}`);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("write:/work/out.txt:"))).toBe(false);
  expect(memberExports.opLog.some((entry) => entry.startsWith("write:/work/out.txt:"))).toBe(false);
});

test("kernel-worker metadata sync preserves dirty resident file data", () => {
  const { canonicalExports, memberExports, fsState, sabPageCache } = makeFsState();

  const workName = encoder.encode("work");
  const workPtr = memberExports.kernel_heap_ptr() + 0x12000;
  new Uint8Array(memberExports.memory.buffer).set(workName, workPtr);
  const workIno = memberExports.memfs_ensure_dir(1, workPtr, workName.length, 0o755);
  expect(workIno).toBeGreaterThan(1);
  publishInode(sabPageCache, workIno, {
    mode: 0o040755,
    size: 0,
    parentIno: 1,
    name: workName,
    nextSibling: 0,
  });

  const name = encoder.encode("out.txt");
  const ptr = memberExports.kernel_heap_ptr() + 0x13000;
  new Uint8Array(memberExports.memory.buffer).set(name, ptr);
  const ino = memberExports.memfs_create_file(workIno, ptr, name.length);
  expect(ino).toBeGreaterThan(1);
  publishInode(sabPageCache, ino, {
    mode: 0o100644,
    size: 0,
    parentIno: workIno,
    name,
    nextSibling: 0,
  });

  const dirty = encoder.encode("dirty-after-metadata\n");
  expect(fsState.writeDirtyResidentFileByInode(ino, 0, dirty, memberExports)).toBe(dirty.length);
  const dirtyRecord = readInodeRecord(sabPageCache, ino);
  expect(dirtyRecord?.generation).toBeGreaterThan(1);
  expect(readResidentFileData(sabPageCache, ino, 0, dirty.length)).toEqual(dirty);

  expect(Number(memberExports.memfs_set_mode(ino, 0o600))).toBe(0);
  expect(fsState.syncMetadataMutation(memberExports, "/work/out.txt")).toBe(true);

  const after = readInodeRecord(sabPageCache, ino);
  expect(after?.mode).toBe(0o100600);
  expect(after?.generation).toBe(dirtyRecord?.generation);
  expect(readResidentFileData(sabPageCache, ino, 0, dirty.length)).toEqual(dirty);
  expectBytesEqual(fsState.readFile("/work/out.txt"), dirty);
  const exported = fsState.snapshotCanonicalFs(true).find((entry) => (
    entry.kind === "file" && entry.path === "/work/out.txt"
  ));
  expect(exported?.kind === "file" ? exported.data : undefined).toEqual(dirty);
  expect(canonicalExports.fileIno("/work/out.txt")).toBe(ino);
});

test("kernel-worker syncMetadataMutation removes absolute-path entries without full fanout", () => {
  const { canonicalExports, memberExports, fsState } = makeFsState();

  fsState.writeFile("/work/remove.txt", encoder.encode("payload"), 0o644, false);
  const ino = canonicalExports.fileIno("/work/remove.txt");
  expect(ino).toBeGreaterThan(1);
  expect(memberExports.fileIno("/work/remove.txt")).toBe(0);
  fsState.syncMetadataMutation(canonicalExports, "/work/remove.txt");
  expect(memberExports.fileIno("/work/remove.txt")).toBe(ino);

  const parentIno = canonicalExports.fileIno("/work");
  const name = encoder.encode("remove.txt");
  const ptr = canonicalExports.kernel_heap_ptr() + 0x13000;
  new Uint8Array(canonicalExports.memory.buffer).set(name, ptr);
  expect(canonicalExports.memfs_unlink(parentIno, ptr, name.length)).toBe(0n);

  const changed = fsState.syncMetadataMutation(canonicalExports, "/work/remove.txt");

  expect(changed).toBe(true);
  expect(canonicalExports.fileIno("/work/remove.txt")).toBe(0);
  expect(memberExports.fileIno("/work/remove.txt")).toBe(0);
});

test("kernel-worker writeFile keeps files page-cache-backed instead of materializing canonical memfs data", () => {
  const { canonicalExports, fsState, seedEntries } = makeFsState();
  const data = new Uint8Array(128 * 1024).map((_, index) => (index * 13) & 0xff);

  fsState.writeFile("/usr/lib/runtime.bin", data, 0o644, false);
  const roundTrip = fsState.readFile("/usr/lib/runtime.bin");

  expect(canonicalExports.opLog).toContain(`external:/usr/lib/runtime.bin:${data.length}`);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("shallow:/usr/lib/runtime.bin:"))).toBe(false);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("truncate:/usr/lib/runtime.bin:"))).toBe(false);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("write:/usr/lib/runtime.bin:"))).toBe(false);
  expect(roundTrip.length).toBe(data.length);
  expect(roundTrip.subarray(0, 64)).toEqual(data.subarray(0, 64));
  expect(roundTrip.subarray(data.length - 64)).toEqual(data.subarray(data.length - 64));
  expect(seedEntries.get("/usr/lib/runtime.bin")).toEqual({
    kind: "file",
    path: "/usr/lib/runtime.bin",
    size: data.length,
    mode: 0o644,
  });
});

test("kernel-worker writeFile keeps backing data independent from resident page-cache capacity", () => {
  const { fsState } = makeFsState(32);
  const data = new Uint8Array(128).fill(0x5a);

  expect(() => fsState.writeFile("/usr/lib/runtime.bin", data, 0o644, false)).not.toThrow();
  expectBytesEqual(fsState.readFile("/usr/lib/runtime.bin"), data);
});

test("kernel-worker readFile falls back to page-cache data for externalized large files", () => {
  const { canonicalExports, fsState } = makeFsState();
  const data = new Uint8Array((1024 * 1024) + 17).map((_, index) => (index * 7) & 0xff);

  fsState.writeFile("/usr/bin/sqv", data, 0o755, false);
  canonicalExports.memfs_read_file_to = () => -1n;

  const roundTrip = fsState.readFile("/usr/bin/sqv");

  expect(roundTrip.length).toBe(data.length);
  expect(roundTrip.subarray(0, 128)).toEqual(data.subarray(0, 128));
  expect(roundTrip.subarray(data.length - 128)).toEqual(data.subarray(data.length - 128));
});

test("kernel-worker readFile uses backing data when memfs and resident page-cache data are unavailable", () => {
  const { canonicalExports, fsState, sabPageCache } = makeFsState();
  const data = new Uint8Array(128 * 1024).map((_, index) => (index * 11) & 0xff);

  fsState.writeFile("/usr/bin/sqv", data, 0o755, false);
  const ino = canonicalExports.fileIno("/usr/bin/sqv");
  publishInode(sabPageCache, ino, {
    mode: 0o100755,
    size: data.length,
    parentIno: canonicalExports.fileIno("/usr/bin"),
    name: encoder.encode("sqv"),
    nextSibling: 0,
  });
  canonicalExports.memfs_read_file_to = () => -1n;

  expectBytesEqual(fsState.readFile("/usr/bin/sqv"), data);
});

test("kernel-worker readFile rejects stale canonical memfs bytes without authoritative backing", () => {
  const { canonicalExports, fsState, sabPageCache } = makeFsState();
  const stale = encoder.encode("stale-canonical");

  fsState.mkdir("/lib", 0o755, false);
  const parentIno = canonicalExports.fileIno("/lib");
  const name = encoder.encode("runtime.bin");
  const namePtr = canonicalExports.kernel_heap_ptr() + 0x12000;
  new Uint8Array(canonicalExports.memory.buffer).set(name, namePtr);
  const ino = canonicalExports.memfs_create_file(parentIno, namePtr, name.length);
  canonicalExports.overwriteFile("/lib/runtime.bin", stale);
  publishInode(sabPageCache, ino, {
    mode: 0o100644,
    size: stale.length,
    parentIno,
    name,
    nextSibling: 0,
    fileObjectId: ino,
    generation: 1,
  });

  expect(() => fsState.readFile("/lib/runtime.bin"))
    .toThrow("failed to read authoritative regular file data: /lib/runtime.bin");
});

test("kernel-worker snapshot data export rejects stale canonical memfs bytes without authoritative backing", () => {
  const { canonicalExports, fsState, sabPageCache } = makeFsState();
  const stale = encoder.encode("stale-canonical");

  fsState.mkdir("/lib", 0o755, false);
  const parentIno = canonicalExports.fileIno("/lib");
  const name = encoder.encode("runtime.bin");
  const namePtr = canonicalExports.kernel_heap_ptr() + 0x12000;
  new Uint8Array(canonicalExports.memory.buffer).set(name, namePtr);
  const ino = canonicalExports.memfs_create_file(parentIno, namePtr, name.length);
  canonicalExports.overwriteFile("/lib/runtime.bin", stale);
  publishInode(sabPageCache, ino, {
    mode: 0o100644,
    size: stale.length,
    parentIno,
    name,
    nextSibling: 0,
    fileObjectId: ino,
    generation: 1,
  });

  expect(() => fsState.snapshotCanonicalFs(true))
    .toThrow("failed to read authoritative regular file data: /lib/runtime.bin");
});

test("kernel-worker readFile rejects stale seed data without authoritative backing", () => {
  const { canonicalExports, fsState, sabPageCache, seedEntries } = makeFsState();
  const stale = encoder.encode("stale-seed-data");

  fsState.mkdir("/lib", 0o755, false);
  const parentIno = canonicalExports.fileIno("/lib");
  const name = encoder.encode("runtime.bin");
  const namePtr = canonicalExports.kernel_heap_ptr() + 0x12000;
  new Uint8Array(canonicalExports.memory.buffer).set(name, namePtr);
  const ino = canonicalExports.memfs_create_file(parentIno, namePtr, name.length);
  canonicalExports.overwriteFile("/lib/runtime.bin", stale);
  canonicalExports.memfs_read_file_to = () => -1n;
  seedEntries.set("/lib/runtime.bin", {
    kind: "file",
    path: "/lib/runtime.bin",
    data: stale,
    size: stale.length,
    mode: 0o644,
  });
  publishInode(sabPageCache, ino, {
    mode: 0o100644,
    size: stale.length,
    parentIno,
    name,
    nextSibling: 0,
    fileObjectId: ino,
    generation: 1,
  });

  expect(() => fsState.readFile("/lib/runtime.bin"))
    .toThrow("failed to read authoritative regular file data: /lib/runtime.bin");
});

test("kernel-worker backing range read returns only the requested byte range", () => {
  const { canonicalExports, fsState } = makeFsState();
  const data = new Uint8Array(128 * 1024).map((_, index) => (index * 13) & 0xff);

  fsState.writeFile("/usr/bin/rustc", data, 0o755, false);
  const ino = canonicalExports.fileIno("/usr/bin/rustc");

  expect(fsState.readBackingFileRangeByInode(ino, 4093, 32)).toEqual(data.subarray(4093, 4093 + 32));
  expect(fsState.readBackingFileRangeByInode(ino, data.length - 8, 32)).toEqual(data.subarray(data.length - 8));
  expect(fsState.readBackingFileRangeByInode(ino, data.length, 32)).toBeNull();
  expect(fsState.readBackingFileRangeByInode(ino, -1, 32)).toBeNull();
});

test("kernel-worker symlink uses basename scratch independently from resolvePath scratch", () => {
  const { canonicalExports, fsState } = makeFsState();

  fsState.mkdir("/usr/lib/riscv64-linux-gnu", 0o755, false);
  const parentIno = canonicalExports.fileIno("/usr/lib/riscv64-linux-gnu");
  const rawName = encoder.encode("/usr/lib/ri");
  const rawTarget = encoder.encode("stale-target");
  const rawNamePtr = canonicalExports.kernel_heap_ptr() + 0x10000;
  const rawTargetPtr = rawNamePtr + 0x1000;
  new Uint8Array(canonicalExports.memory.buffer).set(rawName, rawNamePtr);
  new Uint8Array(canonicalExports.memory.buffer).set(rawTarget, rawTargetPtr);
  expect(canonicalExports.memfs_create_symlink(parentIno, rawNamePtr, rawName.length, rawTargetPtr, rawTarget.length))
    .toBeGreaterThan(1);

  fsState.symlink(
    "/usr/lib/riscv64-linux-gnu/libX11.so.6",
    encoder.encode("libX11.so.6.4.0"),
    0o777,
    false,
  );

  expect(canonicalExports.fileIno("/usr/lib/riscv64-linux-gnu/libX11.so.6")).toBeGreaterThan(1);
});

test("kernel-worker snapshotCanonicalFs grows scratch when heap pointer advances", () => {
  const { canonicalExports, fsState } = makeFsState();

  fsState.mkdir("/work", 0o755, false);
  fsState.writeFile("/work/hello.txt", encoder.encode("hello"), 0o644, false);

  canonicalExports.setHeapPtr((512 * 65536) - 0x19000);

  expect(() => fsState.snapshotCanonicalFs(false)).not.toThrow();
  expect(fsState.snapshotCanonicalFs(false).some((entry) => entry.path === "/work/hello.txt")).toBe(true);
});
