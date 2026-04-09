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
  readonly fileModes = new Map<string, number>();
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
      const data = this.files.get(normalized)!;
      return {
        ino: normalized.length,
        mode: 0o100000 | (this.fileModes.get(normalized) ?? 0o644),
        nlink: 1,
        uid: 0,
        gid: 0,
        size: data.length,
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
      entries.push({
        kind: "file",
        path,
        data: new Uint8Array(data),
        mode: this.fileModes.get(path) ?? 0o644,
      });
    }
    return entries;
  }

  private loadSnapshot(entries: FsSnapshotEntry[]): void {
    this.files.clear();
    this.fileModes.clear();
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
      this.ensureDir(normalized.split("/").slice(0, -1).join("/"));
      this.files.set(normalized, new Uint8Array(entry.data ?? new Uint8Array()));
      this.fileModes.set(normalized, entry.mode ?? 0o644);
    }
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
        this.fileModes.set(normalizePath(message.path), message.mode ?? 0o644);
        this.emit({ type: "file-written", requestId: message.requestId });
        break;
      case "read-file": {
        const data = this.files.get(normalizePath(message.path));
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
