import { expect, test } from "vite-plus/test";
import {
  applyFsEntryToHandle,
  preloadSnapshotEntries,
  resolveProcessCwdIno,
} from "../../../src/worker/process-fs.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

type InodeRecord = {
  ino: number;
  path: string;
  kind: "dir" | "file";
  mode: number;
  data: Uint8Array;
};

type FakeMemfsKernel = ProcessHandle["kernel"] & {
  createFileCalls: string[];
  recordsByPath: Map<string, InodeRecord>;
  writePtrs: number[];
};

function normalizePath(path: string): string {
  const normalized = `/${path.split("/").filter(Boolean).join("/")}`;
  return normalized || "/";
}

function decodeGuestBytes(memory: WebAssembly.Memory, ptr: number, len: number): string {
  return decoder.decode(new Uint8Array(memory.buffer, ptr, len));
}

function makeMemfsKernel(): FakeMemfsKernel {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const recordsByPath = new Map<string, InodeRecord>();
  const createFileCalls: string[] = [];
  const writePtrs: number[] = [];
  const recordsByIno = new Map<number, InodeRecord>();
  let nextIno = 2;

  function putRecord(record: InodeRecord): void {
    recordsByPath.set(record.path, record);
    recordsByIno.set(record.ino, record);
  }

  putRecord({
    ino: 1,
    path: "/",
    kind: "dir",
    mode: 0o040755,
    data: new Uint8Array(),
  });

  function ensureDir(path: string, mode = 0o755): InodeRecord {
    const normalized = normalizePath(path);
    const existing = recordsByPath.get(normalized);
    if (existing) {
      return existing;
    }
    const record: InodeRecord = {
      ino: nextIno++,
      path: normalized,
      kind: "dir",
      mode: 0o040000 | mode,
      data: new Uint8Array(),
    };
    putRecord(record);
    return record;
  }

  return {
    memory,
    kernel_heap_ptr: () => 0x10000,
    memfs_resolve_path: (ptr: number, len: number) => {
      const path = normalizePath(decodeGuestBytes(memory, ptr, len));
      return recordsByPath.get(path)?.ino ?? 0;
    },
    memfs_inode_mode: (ino: number) => recordsByIno.get(ino)?.mode ?? 0,
    memfs_file_size: (ino: number) => BigInt(recordsByIno.get(ino)?.data.length ?? 0),
    memfs_reserve_inode_until: (ino: number) => {
      if (ino >= nextIno) {
        nextIno = ino + 1;
      }
      return 0n;
    },
    memfs_ensure_dir: (parentIno: number, namePtr: number, nameLen: number, mode: number) => {
      const parent = recordsByIno.get(parentIno);
      if (!parent) {
        return 0;
      }
      const name = decodeGuestBytes(memory, namePtr, nameLen);
      return ensureDir(parent.path === "/" ? `/${name}` : `${parent.path}/${name}`, mode).ino;
    },
    memfs_create_file: (parentIno: number, namePtr: number, nameLen: number) => {
      const parent = recordsByIno.get(parentIno);
      if (!parent) {
        return 0;
      }
      const name = decodeGuestBytes(memory, namePtr, nameLen);
      const path = parent.path === "/" ? `/${name}` : `${parent.path}/${name}`;
      createFileCalls.push(path);
      const existing = recordsByPath.get(path);
      if (existing) {
        return existing.ino;
      }
      const record: InodeRecord = {
        ino: nextIno++,
        path,
        kind: "file",
        mode: 0o100644,
        data: new Uint8Array(),
      };
      putRecord(record);
      return record.ino;
    },
    memfs_truncate_file: (ino: number, size: bigint) => {
      const record = recordsByIno.get(ino);
      if (!record || record.kind !== "file") {
        return -1n;
      }
      record.data = new Uint8Array(Number(size));
      return size;
    },
    memfs_set_file_size_shallow: (ino: number, size: bigint) => {
      const record = recordsByIno.get(ino);
      if (!record || record.kind !== "file") {
        return -1n;
      }
      const nextSize = Number(size);
      if (record.data.length === nextSize) {
        return size;
      }
      if (record.data.length > nextSize) {
        record.data = record.data.slice(0, nextSize);
        return size;
      }
      const next = new Uint8Array(nextSize);
      next.set(record.data);
      record.data = next;
      return size;
    },
    memfs_write_file_at: (ino: number, offset: bigint, ptr: number, len: number) => {
      const record = recordsByIno.get(ino);
      if (!record || record.kind !== "file") {
        return -1n;
      }
      writePtrs.push(ptr);
      const required = Number(offset) + len;
      if (record.data.length < required) {
        const next = new Uint8Array(required);
        next.set(record.data);
        record.data = next;
      }
      record.data.set(new Uint8Array(memory.buffer, ptr, len), Number(offset));
      return BigInt(len);
    },
    memfs_set_mode: (ino: number, mode: number) => {
      const record = recordsByIno.get(ino);
      if (!record) {
        return;
      }
      const kindMask = record.kind === "dir" ? 0o040000 : 0o100000;
      record.mode = kindMask | mode;
    },
    createFileCalls,
    recordsByPath,
    writePtrs,
  } as unknown as FakeMemfsKernel;
}

function makeHandle(kernel: FakeMemfsKernel): ProcessHandle {
  return {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0x1234,
    ramSize: 0,
    guestBase: 0,
    worker: {} as Worker,
    memory: kernel.memory,
    wasmModule: {} as WebAssembly.Module,
    kernel,
    kernelState: {
      heapBase: 0x200000,
      heapPtr: 0x210000,
      ofdTablePtr: 0x220000,
      memfsPtr: 0x230000,
      memStatePtr: 0x240000,
      epollTablePtr: 0x250000,
      socketTablePtr: 0x260000,
      socketUsed: 0,
      pipeBase: 0x270000,
      pipeUsed: 0n,
      netBufBase: 0x280000,
      netBufUsed: 0n,
      sigSlotsPtr: 0xc00000,
      processPendingPtr: 0xc10000,
      processRuntimeStatePtr: 0xc20000,
      timeStatePtr: 0xc30000,
    },
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    hostStdio: { stdinPipe: 0, stdoutPipe: 0, stderrPipe: 0, refCount: 1 },
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    stdinQueue: [],
    outputBacklog: [],
    debugCommand: "process-fs",
    runtimeBridge: {} as ProcessHandle["runtimeBridge"],
    sabPageCache: null,
    workers: new Set(),
    readyWorkers: [],
    runningWorker: null,
    blockedWorkers: new Map(),
    pipeSyncPromise: null,
    pipeSyncQueued: false,
    pipeWaiterWakeQueued: false,
    epollSyncPromise: null,
    epollSyncQueued: false,
    epollWaiterWakeQueued: false,
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    socketSyncPromise: null,
    socketSyncQueued: false,
    socketWaiterWakeQueued: false,
    vforkSnapshot: null,
    vforkWorker: null,
    workerDebugLog: [],
  } as unknown as ProcessHandle;
}

test("resolveProcessCwdIno accepts directories and rejects missing or non-directory paths", () => {
  const kernel = makeMemfsKernel();
  kernel.memfs_ensure_dir(1, (() => {
    const bytes = encoder.encode("work");
    new Uint8Array(kernel.memory.buffer).set(bytes, 0x20000);
    return 0x20000;
  })(), 4, 0o755);
  const fileIno = kernel.memfs_create_file(1, (() => {
    const bytes = encoder.encode("note.txt");
    new Uint8Array(kernel.memory.buffer).set(bytes, 0x20020);
    return 0x20020;
  })(), 8);

  expect(resolveProcessCwdIno(kernel, undefined)).toBe(1);
  expect(resolveProcessCwdIno(kernel, "/work")).toBeGreaterThan(1);
  expect(() => resolveProcessCwdIno(kernel, "/missing")).toThrow("cwd does not exist: /missing");
  expect(() => resolveProcessCwdIno(kernel, "/note.txt")).toThrow("cwd is not a directory: /note.txt");
  expect(fileIno).toBeGreaterThan(1);
});

test("applyFsEntryToHandle replaces existing file bytes and updates mode", () => {
  const kernel = makeMemfsKernel();
  const handle = makeHandle(kernel);
  const workIno = kernel.memfs_ensure_dir(1, (() => {
    const bytes = encoder.encode("work");
    new Uint8Array(kernel.memory.buffer).set(bytes, 0x20100);
    return 0x20100;
  })(), 4, 0o755);
  const fileIno = kernel.memfs_create_file(workIno, (() => {
    const bytes = encoder.encode("note.txt");
    new Uint8Array(kernel.memory.buffer).set(bytes, 0x20120);
    return 0x20120;
  })(), 8);
  kernel.memfs_truncate_file(fileIno, 10n);
  kernel.memfs_write_file_at(fileIno, 0n, (() => {
    const bytes = encoder.encode("0123456789");
    new Uint8Array(kernel.memory.buffer).set(bytes, 0x20140);
    return 0x20140;
  })(), 10);

  applyFsEntryToHandle(handle, {
    kind: "file",
    path: "/work/note.txt",
    data: encoder.encode("hi"),
    mode: 0o600,
  });

  const updated = kernel.recordsByPath.get("/work/note.txt");
  expect(updated?.data).toEqual(encoder.encode("hi"));
  expect(updated?.mode).toBe(0o100600);
});

test("preloadSnapshotEntries resizes files before writing full file data", () => {
  const kernel = makeMemfsKernel();
  let truncateCalls = 0;
  kernel.memfs_truncate_file = () => {
    truncateCalls += 1;
    return 0n;
  };

  preloadSnapshotEntries(kernel, [
    { kind: "dir", path: "/lib", mode: 0o755 },
    { kind: "file", path: "/lib/libc.so.6", data: encoder.encode("abcdef"), mode: 0o755 },
  ]);

  expect(truncateCalls).toBe(1);
  expect(kernel.recordsByPath.get("/lib/libc.so.6")?.data).toEqual(encoder.encode("abcdef"));
});

test("applyFsEntryToHandle creates missing parent directories for new files", () => {
  const kernel = makeMemfsKernel();
  const handle = makeHandle(kernel);

  applyFsEntryToHandle(handle, {
    kind: "dir",
    path: "/workspace/cache",
    mode: 0o750,
  });
  applyFsEntryToHandle(handle, {
    kind: "file",
    path: "/workspace/cache/index.bin",
    data: encoder.encode("seed"),
    mode: 0o640,
  });

  const dirRecord = kernel.recordsByPath.get("/workspace/cache");
  expect(dirRecord?.kind).toBe("dir");
  expect(dirRecord?.path).toBe("/workspace/cache");
  expect(kernel.createFileCalls).toHaveLength(1);
  expect(kernel.createFileCalls[0]?.startsWith("/workspace/cache/")).toBe(true);
});

test("preloadSnapshotEntries reuses post-resize scratch for successive file payloads", () => {
  const kernel = makeMemfsKernel();

  preloadSnapshotEntries(kernel, [
    { kind: "dir", path: "/bin", mode: 0o755 },
    { kind: "file", path: "/bin/app", data: encoder.encode("alpha"), mode: 0o755 },
    { kind: "file", path: "/bin/helper", data: encoder.encode("beta"), mode: 0o755 },
  ]);

  expect(kernel.writePtrs).toHaveLength(2);
  expect(kernel.writePtrs[1]).toBe(kernel.writePtrs[0]);
});
