// Kernel-worker tests in this file validate transitional runtime-owned bridge behavior.
import { expect, test } from "vite-plus/test";
import type { KernelExports } from "../../../src/abi.js";
import { DATA_REGION_OFFSET, publishInode, readInodeRecord } from "../../../src/fs/sab-page-cache.js";
import { createKernelWorkerFsState } from "../../../src/kernel-worker/fs-state.js";

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

function makeKernelExports(sabPageCache?: SharedArrayBuffer): FakeKernelExports {
  const memory = new WebAssembly.Memory({ initial: 512, maximum: 1024, shared: true });
  const recordsByPath = new Map<string, InodeRecord>();
  const recordsByIno = new Map<number, InodeRecord>();
  const opLog: string[] = [];
  let nextIno = 2;
  let reservedIno: number | null = null;
  let heapPtr = 0x10000;

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
      if (!record.materialized && sabPageCache) {
        const published = readInodeRecord(sabPageCache, ino);
        if (published && published.dataOff !== 0 && published.size > 0) {
          const start = Number(offset);
          const available = Math.max(0, published.size - start);
          const readLen = Math.min(len, available);
          const bytes = new Uint8Array(sabPageCache, published.dataOff + start, readLen);
          new Uint8Array(memory.buffer, ptr, readLen).set(bytes);
          return BigInt(readLen);
        }
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
    opLog,
    fileData: (path: string) => {
      const record = recordsByPath.get(normalizePath(path));
      if (!record || record.kind !== "file") {
        return new Uint8Array();
      }
      if (!record.materialized && sabPageCache) {
        const published = readInodeRecord(sabPageCache, record.ino);
        if (published && published.dataOff !== 0 && published.size > 0) {
          return new Uint8Array(new Uint8Array(sabPageCache).slice(
            published.dataOff,
            published.dataOff + published.size,
          ));
        }
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

function makeFsState() {
  const sabPageCache = new SharedArrayBuffer(DATA_REGION_OFFSET + (16 * 1024 * 1024));
  const canonicalExports = makeKernelExports(sabPageCache);
  const memberExports = makeKernelExports(sabPageCache);
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
      }, null);
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
    upsertFileSeed: (path, data, mode = 0o644) => {
      seedEntries.set(normalizePath(path), { kind: "file", path: normalizePath(path), data, size: data.length, mode });
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

test("kernel-worker loadFsSnapshot materializes provided file data while publishing page cache metadata", () => {
  const { canonicalExports, fsState, sabPageCache } = makeFsState();
  const data = new Uint8Array(128 * 1024).map((_, index) => index & 0xff);

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", data, mode: 0o755 },
  ]);

  const ino = canonicalExports.fileIno("/lib/runtime.bin");
  expect(ino).toBeGreaterThan(1);
  expect(canonicalExports.fileData("/lib/runtime.bin")).toEqual(data);
  expect(canonicalExports.opLog).not.toContain(`external:/lib/runtime.bin:${data.length}`);
  expect(canonicalExports.opLog).toContain(`truncate:/lib/runtime.bin:${data.length}`);
  expect(canonicalExports.opLog).toContain(`write:/lib/runtime.bin:${data.length}`);
  expect(readInodeRecord(sabPageCache, ino)?.size).toBe(data.length);
});

test("kernel-worker loadFsSnapshot preserves metadata-only file sizes as external state", () => {
  const { canonicalExports, fsState, sabPageCache } = makeFsState();

  fsState.loadFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "file", path: "/lib/runtime.bin", size: 128 * 1024, mode: 0o755 },
  ]);

  const ino = canonicalExports.fileIno("/lib/runtime.bin");
  expect(ino).toBeGreaterThan(1);
  expect(canonicalExports.fileData("/lib/runtime.bin").length).toBe(128 * 1024);
  expect(canonicalExports.opLog).toContain("external:/lib/runtime.bin:131072");
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("truncate:/lib/runtime.bin:"))).toBe(false);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("write:/lib/runtime.bin:"))).toBe(false);
  expect(readInodeRecord(sabPageCache, ino)?.size).toBe(128 * 1024);
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

test("kernel-worker syncFileDataMutation moves canonical/member files back to page-cache-backed external state", () => {
  const { canonicalExports, memberExports, fsState } = makeFsState();
  const initialData = encoder.encode("abcdefghijklmnop");
  const nextData = encoder.encode("abc");

  fsState.writeFile("/lib/runtime.bin", initialData, 0o755, false);
  const ino = canonicalExports.fileIno("/lib/runtime.bin");
  expect(ino).toBeGreaterThan(1);

  fsState.syncFileDataMutation(canonicalExports, ino);
  memberExports.opLog.length = 0;
  canonicalExports.overwriteFile("/lib/runtime.bin", nextData);

  const changed = fsState.syncFileDataMutation(canonicalExports, ino);

  expect(changed).toBe(true);
  expect(memberExports.fileData("/lib/runtime.bin")).toEqual(nextData);
  expect(memberExports.opLog).toContain("external:/lib/runtime.bin:3");
  expect(memberExports.opLog.some((entry) => entry.startsWith("truncate:/lib/runtime.bin:3"))).toBe(false);
  expect(memberExports.opLog.some((entry) => entry.startsWith("write:/lib/runtime.bin:3"))).toBe(false);
  expect(canonicalExports.opLog).toContain("external:/lib/runtime.bin:3");
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

test("kernel-worker writeFile keeps large files page-cache-backed instead of materializing canonical memfs data", () => {
  const { canonicalExports, fsState } = makeFsState();
  const data = new Uint8Array((8 * 1024 * 1024) + (1024 * 1024)).map((_, index) => (index * 13) & 0xff);

  fsState.writeFile("/usr/lib/large.bin", data, 0o644, false);
  const roundTrip = fsState.readFile("/usr/lib/large.bin");

  expect(canonicalExports.opLog).toContain(`shallow:/usr/lib/large.bin:${data.length}`);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("truncate:/usr/lib/large.bin:"))).toBe(false);
  expect(canonicalExports.opLog.some((entry) => entry.startsWith("write:/usr/lib/large.bin:"))).toBe(false);
  expect(roundTrip.length).toBe(data.length);
  expect(roundTrip.subarray(0, 64)).toEqual(data.subarray(0, 64));
  expect(roundTrip.subarray(data.length - 64)).toEqual(data.subarray(data.length - 64));
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

test("kernel-worker readFile falls back to seed data when page-cache data is unavailable", () => {
  const { canonicalExports, fsState, sabPageCache } = makeFsState();
  const data = new Uint8Array((1024 * 1024) + 17).map((_, index) => (index * 11) & 0xff);

  fsState.writeFile("/usr/bin/sqv", data, 0o755, false);
  const ino = canonicalExports.fileIno("/usr/bin/sqv");
  publishInode(sabPageCache, ino, {
    mode: 0o100755,
    size: data.length,
    parentIno: canonicalExports.fileIno("/usr/bin"),
    name: encoder.encode("sqv"),
    nextSibling: 0,
  }, null);
  canonicalExports.memfs_read_file_to = () => -1n;

  const roundTrip = fsState.readFile("/usr/bin/sqv");

  expect(roundTrip.length).toBe(data.length);
  expect(roundTrip.subarray(0, 128)).toEqual(data.subarray(0, 128));
  expect(roundTrip.subarray(data.length - 128)).toEqual(data.subarray(data.length - 128));
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
