import { afterEach, beforeEach, expect, test, vi } from "vite-plus/test";
import { Runtime } from "../../../src/index.js";
import type { FsSnapshotEntry } from "../../../src/fs/types.js";
import type { RuntimeStat } from "../../../src/fs/types.js";
import type { KernelWorkerInbound, KernelWorkerOutbound } from "../../../src/messages.js";
import type { ProcessHandle } from "../../../src/worker.js";
import * as workerModule from "../../../src/worker.js";

function normalizePath(path: string): string {
  const normalized = `/${path.split("/").filter(Boolean).join("/")}`;
  return normalized || "/";
}

class FakeKernelWorker {
  private listeners = new Set<(event: MessageEvent<KernelWorkerOutbound>) => void>();
  readonly postedMessages: KernelWorkerInbound[] = [];
  readonly files = new Map<string, Uint8Array>();
  readonly fileSizes = new Map<string, number>();
  readonly fileModes = new Map<string, number>();
  readonly externalFiles = new Set<string>();
  readonly backingExtents = new Map<string, NonNullable<FsSnapshotEntry["backingExtent"]>>();
  readonly backingSources = new Map<string, Uint8Array>();
  readonly symlinks = new Map<string, Uint8Array>();
  readonly symlinkModes = new Map<string, number>();
  readonly dirModes = new Map<string, number>([["/", 0o755]]);
  readonly dirs = new Set<string>(["/"]);
  terminated = false;

  constructor(
    readonly _url: URL,
    readonly _options: WorkerOptions,
  ) {}

  addEventListener(
    type: "message",
    listener: (event: MessageEvent<KernelWorkerOutbound>) => void,
  ): void {
    if (type === "message") {
      this.listeners.add(listener);
    }
  }

  removeEventListener(
    type: "message",
    listener: (event: MessageEvent<KernelWorkerOutbound>) => void,
  ): void {
    if (type === "message") {
      this.listeners.delete(listener);
    }
  }

  postMessage(message: KernelWorkerInbound): void {
    this.postedMessages.push(message);
    queueMicrotask(() => this.respond(message));
  }

  terminate(): void {
    this.terminated = true;
  }

  private emit(message: KernelWorkerOutbound): void {
    const event = { data: message } as MessageEvent<KernelWorkerOutbound>;
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  private ensureDir(path: string, mode = 0o755): void {
    const parts = normalizePath(path).split("/").filter(Boolean);
    let current = "";
    for (const part of parts) {
      current += `/${part}`;
      this.dirs.add(current);
      if (!this.dirModes.has(current)) {
        this.dirModes.set(current, mode);
      }
    }
  }

  private stat(path: string): RuntimeStat {
    const normalized = normalizePath(path);
    if (this.files.has(normalized)) {
      const size = this.fileSizes.get(normalized) ?? this.files.get(normalized)?.length ?? 0;
      return {
        ino: normalized.length,
        mode: 0o100000 | (this.fileModes.get(normalized) ?? 0o644),
        nlink: 1,
        uid: 0,
        gid: 0,
        size,
      };
    }
    if (this.symlinks.has(normalized)) {
      return {
        ino: normalized.length,
        mode: 0o120000 | (this.symlinkModes.get(normalized) ?? 0o777),
        nlink: 1,
        uid: 0,
        gid: 0,
        size: this.symlinks.get(normalized)?.length ?? 0,
      };
    }
    if (this.dirs.has(normalized)) {
      return {
        ino: normalized.length || 1,
        mode: 0o040000 | (this.dirModes.get(normalized) ?? 0o755),
        nlink: 2,
        uid: 0,
        gid: 0,
        size: 0,
      };
    }
    throw new Error(`ENOENT: ${path}`);
  }

  private readdir(path: string): string[] {
    const normalized = normalizePath(path);
    const prefix = normalized === "/" ? "/" : `${normalized}/`;
    const children = new Set<string>();

    for (const dir of this.dirs) {
      if (dir === normalized || !dir.startsWith(prefix)) {
        continue;
      }
      const rest = dir.slice(prefix.length);
      if (rest.length > 0 && !rest.includes("/")) {
        children.add(rest);
      }
    }

    for (const file of this.files.keys()) {
      if (!file.startsWith(prefix)) {
        continue;
      }
      const rest = file.slice(prefix.length);
      if (rest.length > 0 && !rest.includes("/")) {
        children.add(rest);
      }
    }
    for (const symlink of this.symlinks.keys()) {
      if (!symlink.startsWith(prefix)) {
        continue;
      }
      const rest = symlink.slice(prefix.length);
      if (rest.length > 0 && !rest.includes("/")) {
        children.add(rest);
      }
    }

    return [...children].sort();
  }

  private snapshotEntries(): FsSnapshotEntry[] {
    const entries: FsSnapshotEntry[] = [...this.dirs]
      .sort()
      .map((path) => ({
        kind: "dir" as const,
        path,
        mode: this.dirModes.get(path) ?? 0o755,
      }));

    for (const [path, data] of [...this.files.entries()].sort(([left], [right]) => left.localeCompare(right))) {
      const external = this.externalFiles.has(path);
      entries.push(external
        ? {
            kind: "file" as const,
            path,
            size: this.fileSizes.get(path) ?? data.length,
            mode: this.fileModes.get(path) ?? 0o644,
            backingExtent: this.backingExtents.get(path),
          }
        : {
            kind: "file" as const,
            path,
            data: new Uint8Array(data),
            mode: this.fileModes.get(path) ?? 0o644,
          });
    }
    for (const [path, target] of [...this.symlinks.entries()].sort(([left], [right]) => left.localeCompare(right))) {
      entries.push({
        kind: "symlink" as const,
        path,
        target: new Uint8Array(target),
        mode: this.symlinkModes.get(path) ?? 0o777,
      });
    }
    return entries;
  }

  private loadSnapshot(entries: FsSnapshotEntry[]): void {
    this.files.clear();
    this.fileSizes.clear();
    this.fileModes.clear();
    this.externalFiles.clear();
    this.backingExtents.clear();
    this.symlinks.clear();
    this.symlinkModes.clear();
    this.dirModes.clear();
    this.dirs.clear();
    this.dirs.add("/");
    this.dirModes.set("/", 0o755);

    for (const entry of entries) {
      const normalized = normalizePath(entry.path);
      if (entry.kind === "dir") {
        this.ensureDir(normalized, entry.mode ?? 0o755);
        this.dirModes.set(normalized, entry.mode ?? 0o755);
        continue;
      }
      if (entry.kind === "symlink") {
        this.ensureDir(normalized.split("/").slice(0, -1).join("/"));
        this.symlinks.set(normalized, new Uint8Array(entry.target ?? new Uint8Array()));
        this.symlinkModes.set(normalized, entry.mode ?? 0o777);
        continue;
      }
      this.ensureDir(normalized.split("/").slice(0, -1).join("/"));
      this.files.set(normalized, new Uint8Array(entry.data ?? new Uint8Array()));
      this.fileSizes.set(normalized, entry.data?.length ?? entry.size ?? 0);
      this.fileModes.set(normalized, entry.mode ?? 0o644);
      if (entry.data === undefined) {
        this.externalFiles.add(normalized);
        if (entry.backingExtent) {
          this.backingExtents.set(normalized, { ...entry.backingExtent });
        }
      } else {
        this.externalFiles.delete(normalized);
        this.backingExtents.delete(normalized);
      }
    }
  }

  private materializeBackingSource(path: string): Uint8Array | null {
    const extent = this.backingExtents.get(path);
    if (!extent) {
      return null;
    }
    const source = this.backingSources.get(extent.sourceIdentity);
    if (!source) {
      return null;
    }
    const size = this.fileSizes.get(path) ?? 0;
    const offset = Math.floor(extent.offset ?? 0);
    const length = Math.floor(extent.length ?? size);
    if (offset < 0 || length !== size || offset + length > source.byteLength) {
      return null;
    }
    const data = source.slice(offset, offset + length);
    this.files.set(path, data);
    this.fileSizes.set(path, data.length);
    this.externalFiles.delete(path);
    return data;
  }

  private respond(message: KernelWorkerInbound): void {
    switch (message.type) {
      case "init-kernel":
        this.emit({
          type: "ready",
          requestId: message.requestId,
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
            sigSlotsPtr: 0x290000,
            processPendingPtr: 0x2a0000,
            processRuntimeStatePtr: 0x2b0000,
            timeStatePtr: 0x2c0000,
          },
        });
        break;
      case "write-file":
        this.ensureDir(normalizePath(message.path).split("/").slice(0, -1).join("/"));
        this.files.set(normalizePath(message.path), new Uint8Array(message.data));
        this.fileSizes.set(normalizePath(message.path), message.data.length);
        this.fileModes.set(normalizePath(message.path), message.mode ?? 0o644);
        this.externalFiles.delete(normalizePath(message.path));
        this.emit({ type: "file-written", requestId: message.requestId });
        break;
      case "write-files":
        for (const entry of message.entries) {
          this.ensureDir(normalizePath(entry.path).split("/").slice(0, -1).join("/"));
          this.files.set(normalizePath(entry.path), new Uint8Array(entry.data));
          this.fileSizes.set(normalizePath(entry.path), entry.data.length);
          this.fileModes.set(normalizePath(entry.path), entry.mode ?? 0o644);
          this.externalFiles.delete(normalizePath(entry.path));
        }
        this.emit({ type: "files-written", requestId: message.requestId });
        break;
      case "apply-file-layer":
        for (const entry of message.entries) {
          if (entry.kind === "dir") {
            this.ensureDir(entry.path, entry.mode ?? 0o755);
            continue;
          }
          this.ensureDir(normalizePath(entry.path).split("/").slice(0, -1).join("/"));
          if (entry.kind === "file") {
            this.files.set(normalizePath(entry.path), new Uint8Array(entry.data));
            this.fileSizes.set(normalizePath(entry.path), entry.data.length);
            this.fileModes.set(normalizePath(entry.path), entry.mode ?? 0o644);
            this.externalFiles.delete(normalizePath(entry.path));
            continue;
          }
          this.symlinks.set(normalizePath(entry.path), new Uint8Array(entry.target));
          this.symlinkModes.set(normalizePath(entry.path), entry.mode ?? 0o777);
        }
        this.emit({ type: "file-layer-applied", requestId: message.requestId });
        break;
      case "symlink":
        this.ensureDir(normalizePath(message.path).split("/").slice(0, -1).join("/"));
        this.symlinks.set(normalizePath(message.path), new Uint8Array(message.target));
        this.symlinkModes.set(normalizePath(message.path), message.mode ?? 0o777);
        this.emit({ type: "symlink-created", requestId: message.requestId });
        break;
      case "readlink": {
        const target = this.symlinks.get(normalizePath(message.path));
        if (!target) {
          this.emit({
            type: "error",
            requestId: message.requestId,
            message: `EINVAL: ${message.path}`,
          });
          break;
        }
        this.emit({ type: "readlink-result", requestId: message.requestId, target: new Uint8Array(target) });
        break;
      }
      case "read-file": {
        const normalized = normalizePath(message.path);
        const data = this.externalFiles.has(normalized)
          ? this.materializeBackingSource(normalized)
          : this.files.get(normalized);
        if (!data) {
          this.emit({
            type: "error",
            requestId: message.requestId,
            message: `ENOENT: ${message.path}`,
          });
          break;
        }
        this.emit({
          type: "file-read",
          requestId: message.requestId,
          data: new Uint8Array(data),
        });
        break;
      }
      case "mkdir":
        this.ensureDir(message.path, message.mode ?? 0o755);
        this.emit({ type: "dir-created", requestId: message.requestId });
        break;
      case "stat":
        this.emit({
          type: "stat-result",
          requestId: message.requestId,
          stat: this.stat(message.path),
        });
        break;
      case "readdir":
        this.emit({
          type: "readdir-result",
          requestId: message.requestId,
          entries: this.readdir(message.path),
        });
        break;
      case "snapshot-fs":
        this.emit({
          type: "snapshot-fs-result",
          requestId: message.requestId,
          entries: this.snapshotEntries(),
        });
        break;
      case "load-fs-snapshot":
        this.loadSnapshot(message.entries);
        this.emit({ type: "fs-loaded", requestId: message.requestId });
        break;
      case "register-fs-backing-source":
        this.backingSources.set(message.sourceIdentity, new Uint8Array(message.data));
        this.emit({ type: "fs-backing-source-registered", requestId: message.requestId });
        break;
      case "register-fs-backing-source-range":
        this.backingSources.set(message.sourceIdentity, new Uint8Array(message.data));
        this.emit({ type: "fs-backing-source-registered", requestId: message.requestId });
        break;
    }
  }
}

const createdWorkers: FakeKernelWorker[] = [];
const originalWorker = globalThis.Worker;

beforeEach(() => {
  createdWorkers.length = 0;
  globalThis.Worker = class extends FakeKernelWorker {
    constructor(url: URL, options: WorkerOptions) {
      super(url, options);
      createdWorkers.push(this);
    }
  } as unknown as typeof Worker;

  vi.spyOn(WebAssembly, "compile").mockResolvedValue({} as WebAssembly.Module);
});

afterEach(() => {
  vi.restoreAllMocks();
  globalThis.Worker = originalWorker;
});

test("Runtime.fs routes kernel-worker RPCs with unique request ids", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });

  await Promise.all([
    runtime.fs.mkdir("/workspace"),
    runtime.fs.writeFile("/workspace/main.c", new TextEncoder().encode("int main() {}\n"), 0o755),
  ]);

  const file = await runtime.fs.readFile("/workspace/main.c");
  const stat = await runtime.fs.stat("/workspace/main.c");
  const entries = await runtime.fs.readdir("/workspace");

  expect(new TextDecoder().decode(file)).toBe("int main() {}\n");
  expect(stat.size).toBe(14);
  expect(stat.mode).toBe(0o100755);
  expect(entries).toEqual(["main.c"]);

  const requestIds = createdWorkers[0].postedMessages.map((msg) => msg.requestId);
  expect(requestIds).toHaveLength(new Set(requestIds).size);

  runtime.destroy();
  expect(createdWorkers[0].terminated).toBe(true);
});

test("Runtime.fs reads only kernel-worker canonical FS state", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });
  await expect(runtime.fs.readFile("/workspace/generated.txt")).rejects.toThrow(
    "ENOENT: /workspace/generated.txt",
  );

  const worker = createdWorkers[0];
  const readIndex = worker.postedMessages.findIndex((msg) => msg.type === "read-file");
  expect(readIndex).toBeGreaterThan(-1);
  expect(worker.postedMessages.some((msg) => msg.type === "load-fs-snapshot")).toBe(false);
  expect(worker.postedMessages.some((msg) => msg.type === "snapshot-fs")).toBe(false);

  runtime.destroy();
});

test("Runtime.applyFsSnapshot seeds kernel-worker FS in a single RPC", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });

  await runtime.applyFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/toolchain", mode: 0o755 },
    {
      kind: "file",
      path: "/toolchain/marker.txt",
      data: new TextEncoder().encode("toolchain"),
      mode: 0o644,
    },
  ]);

  const file = await runtime.fs.readFile("/toolchain/marker.txt");
  expect(new TextDecoder().decode(file)).toBe("toolchain");
  expect(createdWorkers[0].postedMessages.some((msg) => msg.type === "load-fs-snapshot")).toBe(true);

  runtime.destroy();
});

test("Runtime.applyFsSnapshot preserves metadata-only file size in canonical snapshot state", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });
  const backingExtent = {
    sourceIdentity: "artifact:toolchain/archive.bin",
    digest: "sha256:archive",
    offset: 4096,
    length: 128 * 1024,
  };

  await runtime.applyFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/toolchain", mode: 0o755 },
    {
      kind: "file",
      path: "/toolchain/archive.bin",
      size: 128 * 1024,
      mode: 0o644,
      backingExtent,
    },
  ]);

  await expect(runtime.fs.stat("/toolchain/archive.bin")).resolves.toMatchObject({
    mode: 0o100644,
    size: 128 * 1024,
  });
  const loadMessage = createdWorkers[0].postedMessages.find((msg) => msg.type === "load-fs-snapshot");
  expect(loadMessage).toBeDefined();
  expect(loadMessage).toMatchObject({
    entries: expect.arrayContaining([
      expect.objectContaining({
        kind: "file",
        path: "/toolchain/archive.bin",
        size: 128 * 1024,
        backingExtent,
      }),
    ]),
  });

  runtime.destroy();
});

test("Runtime reads metadata-only snapshot files from registered backing source", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });
  const source = new TextEncoder().encode("prefix:artifact-bytes:suffix");
  const backingExtent = {
    sourceIdentity: "artifact:toolchain/archive.bin",
    offset: "prefix:".length,
    length: "artifact-bytes".length,
  };

  await runtime.applyFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/toolchain", mode: 0o755 },
    {
      kind: "file",
      path: "/toolchain/archive.bin",
      size: backingExtent.length,
      mode: 0o644,
      backingExtent,
    },
  ]);
  await runtime.registerFsBackingSource(backingExtent.sourceIdentity, source);

  await expect(runtime.fs.readFile("/toolchain/archive.bin"))
    .resolves.toEqual(new TextEncoder().encode("artifact-bytes"));
  expect(createdWorkers[0].postedMessages.some((msg) => msg.type === "register-fs-backing-source")).toBe(true);

  runtime.destroy();
});

test("Runtime registers prefetched remote backing source ranges through kernel-worker range source", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });
  const full = new TextEncoder().encode("prefix:artifact-bytes:suffix");
  const fetch = vi.fn(async () => ({
    ok: true,
    status: 200,
    arrayBuffer: async () => full.buffer.slice(full.byteOffset, full.byteOffset + full.byteLength),
  }));

  await runtime.registerFsRemoteBackingSourceRange(
    "remote:toolchain/archive.bin",
    full.length,
    "https://example.test/archive.bin",
    "prefix:".length,
    "artifact-bytes".length,
    { fetch },
  );

  const message = createdWorkers[0].postedMessages.find((msg) => msg.type === "register-fs-backing-source-range");
  expect(message).toMatchObject({
    type: "register-fs-backing-source-range",
    sourceIdentity: "remote:toolchain/archive.bin",
    byteLength: full.length,
    offset: "prefix:".length,
  });
  expect(message?.type === "register-fs-backing-source-range" ? message.data : undefined)
    .toEqual(new TextEncoder().encode("artifact-bytes"));

  runtime.destroy();
});

test("Runtime registers prefetched OPFS backing source ranges through kernel-worker range source", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });
  const full = new TextEncoder().encode("prefix:artifact-bytes:suffix");
  const handle = {
    getFile: vi.fn(async () => new Blob([full])),
  };

  await runtime.registerFsOpfsBackingSourceRange(
    "opfs:toolchain/archive.bin",
    full.length,
    handle,
    "prefix:".length,
    "artifact-bytes".length,
  );

  const message = createdWorkers[0].postedMessages.find((msg) => (
    msg.type === "register-fs-backing-source-range"
    && msg.sourceIdentity === "opfs:toolchain/archive.bin"
  ));
  expect(message).toMatchObject({
    type: "register-fs-backing-source-range",
    sourceIdentity: "opfs:toolchain/archive.bin",
    byteLength: full.length,
    offset: "prefix:".length,
  });
  expect(message?.type === "register-fs-backing-source-range" ? message.data : undefined)
    .toEqual(new TextEncoder().encode("artifact-bytes"));

  runtime.destroy();
});

test("Runtime.applyFsSnapshot does not retain provided file data in runtime state", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });
  const data = new TextEncoder().encode("snapshot body");

  await runtime.applyFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/toolchain", mode: 0o755 },
    {
      kind: "file",
      path: "/toolchain/data.txt",
      data,
      mode: 0o644,
    },
  ]);

  const worker = createdWorkers[0];
  worker.postedMessages.length = 0;
  await expect(runtime.fs.readFile("/toolchain/data.txt")).resolves.toEqual(data);
  expect(worker.postedMessages.some((msg) => msg.type === "read-file")).toBe(true);

  runtime.destroy();
});

test("Runtime.applyFsSnapshot reads a file larger than default resident page-cache capacity", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });
  const data = Uint8Array.from(
    { length: (16 * 1024 * 1024) + 17 },
    (_value, index) => (index * 31) & 0xff,
  );

  await runtime.applyFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/toolchain", mode: 0o755 },
    {
      kind: "file",
      path: "/toolchain/large.bin",
      data,
      mode: 0o644,
    },
  ]);

  const worker = createdWorkers[0];
  worker.postedMessages.length = 0;
  const roundTrip = await runtime.fs.readFile("/toolchain/large.bin");

  expect(roundTrip.length).toBe(data.length);
  expect(roundTrip.subarray(0, 128)).toEqual(data.subarray(0, 128));
  expect(roundTrip.subarray(data.length - 128)).toEqual(data.subarray(data.length - 128));
  expect(worker.postedMessages.some((msg) => msg.type === "read-file")).toBe(true);

  runtime.destroy();
});

test("Runtime.applyFileLayer applies dirs files and symlinks through one kernel-worker RPC", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });

  await runtime.applyFileLayer([
    { kind: "dir", path: "/usr/bin", mode: 0o755 },
    {
      kind: "file",
      path: "/usr/bin/tool",
      data: new TextEncoder().encode("tool"),
      mode: 0o755,
    },
    {
      kind: "symlink",
      path: "/bin/tool",
      target: new TextEncoder().encode("../usr/bin/tool"),
      mode: 0o777,
    },
  ]);

  await expect(runtime.fs.readFile("/usr/bin/tool")).resolves.toEqual(new TextEncoder().encode("tool"));
  await expect(runtime.fs.readlink("/bin/tool")).resolves.toEqual(new TextEncoder().encode("../usr/bin/tool"));

  const worker = createdWorkers[0];
  expect(worker.postedMessages.filter((msg) => msg.type === "apply-file-layer")).toHaveLength(1);
  expect(worker.postedMessages.filter((msg) => msg.type === "mkdir")).toHaveLength(0);
  expect(worker.postedMessages.filter((msg) => msg.type === "write-files")).toHaveLength(0);
  expect(worker.postedMessages.filter((msg) => msg.type === "symlink")).toHaveLength(0);

  runtime.destroy();
});

test("Runtime.spawn delegates canonical FS hydration to createProcess", async () => {
  const createProcessSpy = vi.spyOn(workerModule, "createProcess").mockResolvedValue({
    pid: 1,
    outputBacklog: [],
  } as unknown as ProcessHandle);
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });
  const kernelWorker = createdWorkers[0];

  await runtime.applyFsSnapshot([
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/workspace", mode: 0o755 },
    {
      kind: "file",
      path: "/workspace/generated.txt",
      data: new TextEncoder().encode("from kernel worker\n"),
      mode: 0o644,
    },
  ]);
  kernelWorker.postedMessages.length = 0;

  await runtime.spawn(new Uint8Array([1, 2, 3]));

  expect(kernelWorker.postedMessages.some((msg) => msg.type === "load-fs-snapshot")).toBe(false);
  expect(kernelWorker.postedMessages.some((msg) => msg.type === "snapshot-fs")).toBe(false);

  expect(createProcessSpy).toHaveBeenCalledTimes(1);
  const options = createProcessSpy.mock.calls[0]?.[2];
  expect(options).toMatchObject({
    runtimeId: expect.any(Number),
    pid: 1,
    kernelWorkerRequest: expect.any(Function),
  });
  expect(options && "preloadEntries" in options).toBe(false);

  runtime.destroy();
});

test("Runtime.fs writeFile overwrites canonical file bytes and updates stat on repeated writes", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });

  await runtime.fs.mkdir("/workspace");
  await runtime.fs.writeFile("/workspace/data.txt", new TextEncoder().encode("abcdef"), 0o644);
  await runtime.fs.writeFile("/workspace/data.txt", new TextEncoder().encode("xy"), 0o600);

  const file = await runtime.fs.readFile("/workspace/data.txt");
  const stat = await runtime.fs.stat("/workspace/data.txt");

  expect(new TextDecoder().decode(file)).toBe("xy");
  expect(stat.size).toBe(2);
  expect(stat.mode).toBe(0o100600);

  runtime.destroy();
});

test("Runtime.applyFsSnapshot replaces canonical snapshot contents instead of merging stale paths", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });

  try {
    await runtime.applyFsSnapshot([
      { kind: "dir", path: "/", mode: 0o755 },
      { kind: "dir", path: "/workspace", mode: 0o755 },
      {
        kind: "file",
        path: "/workspace/old.txt",
        data: new TextEncoder().encode("old"),
        mode: 0o644,
      },
    ]);
    await runtime.applyFsSnapshot([
      { kind: "dir", path: "/", mode: 0o755 },
      { kind: "dir", path: "/workspace", mode: 0o755 },
      {
        kind: "file",
        path: "/workspace/new.txt",
        data: new TextEncoder().encode("new"),
        mode: 0o600,
      },
    ]);

    await expect(runtime.fs.readFile("/workspace/old.txt")).rejects.toThrow("ENOENT: /workspace/old.txt");
    await expect(runtime.fs.readdir("/workspace")).resolves.toEqual(["new.txt"]);
    await expect(runtime.fs.readFile("/workspace/new.txt")).resolves.toEqual(new TextEncoder().encode("new"));
    await expect(runtime.fs.stat("/workspace/new.txt")).resolves.toMatchObject({
      mode: 0o100600,
      size: 3,
    });
  } finally {
    runtime.destroy();
  }
});

test("Runtime.applyFsSnapshot can replace a nested path without leaving stale directories behind", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });

  try {
    await runtime.applyFsSnapshot([
      { kind: "dir", path: "/", mode: 0o755 },
      { kind: "dir", path: "/workspace", mode: 0o755 },
      { kind: "dir", path: "/workspace/old", mode: 0o755 },
      {
        kind: "file",
        path: "/workspace/old/data.txt",
        data: new TextEncoder().encode("old"),
        mode: 0o644,
      },
    ]);
    await runtime.applyFsSnapshot([
      { kind: "dir", path: "/", mode: 0o755 },
      { kind: "dir", path: "/workspace", mode: 0o755 },
      { kind: "dir", path: "/workspace/new", mode: 0o750 },
      {
        kind: "file",
        path: "/workspace/new/data.txt",
        data: new TextEncoder().encode("new"),
        mode: 0o600,
      },
    ]);

    await expect(runtime.fs.readFile("/workspace/old/data.txt")).rejects.toThrow("ENOENT: /workspace/old/data.txt");
    await expect(runtime.fs.readdir("/workspace")).resolves.toEqual(["new"]);
    await expect(runtime.fs.readdir("/workspace/new")).resolves.toEqual(["data.txt"]);
    await expect(runtime.fs.stat("/workspace/new")).resolves.toMatchObject({
      mode: 0o040750,
    });
    await expect(runtime.fs.stat("/workspace/new/data.txt")).resolves.toMatchObject({
      mode: 0o100600,
      size: 3,
    });
  } finally {
    runtime.destroy();
  }
});

test("Runtime.applyFsSnapshot can replace rename/unlink-like paths without disturbing unrelated entries", async () => {
  const runtime = await Runtime.create({ wasmBytes: new Uint8Array([0x00]) });

  try {
    await runtime.applyFsSnapshot([
      { kind: "dir", path: "/", mode: 0o755 },
      { kind: "dir", path: "/workspace", mode: 0o755 },
      { kind: "dir", path: "/workspace/tmp", mode: 0o755 },
      {
        kind: "file",
        path: "/workspace/tmp/old.txt",
        data: new TextEncoder().encode("old"),
        mode: 0o644,
      },
      {
        kind: "file",
        path: "/workspace/keep.txt",
        data: new TextEncoder().encode("keep"),
        mode: 0o600,
      },
    ]);
    await runtime.applyFsSnapshot([
      { kind: "dir", path: "/", mode: 0o755 },
      { kind: "dir", path: "/workspace", mode: 0o755 },
      { kind: "dir", path: "/workspace/final", mode: 0o750 },
      {
        kind: "file",
        path: "/workspace/final/new.txt",
        data: new TextEncoder().encode("new"),
        mode: 0o640,
      },
      {
        kind: "file",
        path: "/workspace/keep.txt",
        data: new TextEncoder().encode("keep"),
        mode: 0o600,
      },
    ]);

    await expect(runtime.fs.readFile("/workspace/tmp/old.txt")).rejects.toThrow("ENOENT: /workspace/tmp/old.txt");
    await expect(runtime.fs.readFile("/workspace/keep.txt")).resolves.toEqual(new TextEncoder().encode("keep"));
    await expect(runtime.fs.readdir("/workspace")).resolves.toEqual(["final", "keep.txt"]);
    await expect(runtime.fs.stat("/workspace/final/new.txt")).resolves.toMatchObject({
      mode: 0o100640,
      size: 3,
    });
  } finally {
    runtime.destroy();
  }
});
