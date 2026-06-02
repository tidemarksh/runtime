import {
  clearRuntimeFailureSnapshot,
  createProcess,
  debugRuntimeLastFailureSnapshot,
  debugRuntimeProcessHandles,
  debugRuntimeWorkerLogFocus,
  destroyProcessTree,
  destroyRuntimeProcesses,
  debugRuntimeExecutionStats,
  flushRuntimeFsStateFromDirtyHandle,
  getRuntimeFsState,
  publishRuntimeFsState,
  replaceRuntimeFsSnapshot,
  destroyWorkerPool,
  injectHttpRequest,
  type ProcessHandle,
  precompileModule,
  writeProcessStdin,
} from "./worker.js";
import type { RuntimeStat } from "./fs/types.js";
import type {
  KernelWorkerInbound,
  KernelWorkerOutbound,
  KernelWorkerRequest,
} from "./messages.js";
import {
  addWorkerMessageListener,
  removeWorkerMessageListener,
  setWorkerErrorHandler,
  setWorkerMessageErrorHandler,
} from "./bridge/compat.js";
import type { RuntimeNetworkBridge } from "./bridge/network.js";
import type { FsSnapshotEntry } from "./fs/types.js";
import { FS_RING_TOTAL_BYTES } from "./layout/fs-ring.js";

export interface RuntimeOptions {
  wasmBytes: BufferSource;
  /** Deprecated compatibility alias for guest memory policy limit. */
  ramSize?: number;
  guestMemoryPolicyLimitBytes?: number;
  pageCacheDataCapacityBytes?: number;
  /** Deprecated compatibility alias for kernelWorkerMaxMemoryPages. */
  maxKernelMemoryPages?: number;
  kernelWorkerMaxMemoryPages?: number;
  maxProcessMemoryPages?: number;
  maxForkMemoryPages?: number;
  fsRpcRingCapacityBytes?: number;
  artifactMaterializationBudgetBytes?: number;
  networkBridge?: RuntimeNetworkBridge;
}

const MIN_RAM_SIZE_BYTES = 16 * 1024 * 1024;
const DEFAULT_MAX_MEMORY_PAGES = 32768;

interface RuntimeMemoryConfig {
  guestMemoryPolicyLimitBytes: number;
  pageCacheDataCapacityBytes: number;
  kernelWorkerMaxMemoryPages: number;
  maxProcessMemoryPages: number;
  maxForkMemoryPages: number;
  fsRpcRingCapacityBytes: number;
  artifactMaterializationBudgetBytes: number | null;
}

function normalizeOptionalByteBudget(value: number | undefined): number | null {
  if (value === undefined) {
    return null;
  }
  return Math.max(Math.trunc(value), 0);
}

function normalizeFsRpcRingCapacityBytes(value: number | undefined): number {
  if (value === undefined) {
    return FS_RING_TOTAL_BYTES;
  }
  const normalized = Math.max(Math.trunc(value), 0);
  if (normalized !== FS_RING_TOTAL_BYTES) {
    throw new Error(
      `fsRpcRingCapacityBytes is fixed by kernel ABI at ${FS_RING_TOTAL_BYTES} bytes; got ${normalized}`,
    );
  }
  return normalized;
}

function normalizeRuntimePath(path: string): string {
  const normalized = `/${path.split("/").filter(Boolean).join("/")}`;
  return normalized === "//" ? "/" : normalized || "/";
}

function withoutSnapshotFileData(entries: FsSnapshotEntry[]): FsSnapshotEntry[] {
  return entries.map((entry) => {
    if (entry.kind !== "file" || entry.data === undefined) {
      return entry;
    }
    return {
      kind: "file",
      path: entry.path,
      ino: entry.ino,
      size: entry.size ?? entry.data.byteLength,
      mode: entry.mode,
    };
  });
}

export interface RuntimeFs {
  writeFile(path: string, data: Uint8Array, mode?: number): Promise<void>;
  symlink(path: string, target: Uint8Array, mode?: number): Promise<void>;
  readFile(path: string): Promise<Uint8Array>;
  readlink(path: string): Promise<Uint8Array>;
  mkdir(path: string, mode?: number): Promise<void>;
  stat(path: string): Promise<RuntimeStat>;
  readdir(path: string): Promise<string[]>;
}

export type RuntimeFileLayerEntry =
  | { kind: "dir"; path: string; mode?: number }
  | { kind: "file"; path: string; data: Uint8Array; mode?: number }
  | { kind: "symlink"; path: string; target: Uint8Array; mode?: number };

export interface RuntimeFileLayer {
  id?: string;
  entries: RuntimeFileLayerEntry[];
}

export interface SpawnOptions {
  cwd?: string;
  stdio?: "pipe" | "pty";
  stdin?: "pipe" | "closed";
}

export interface GuestWriteWatchRegion {
  slot: number;
  start: number;
  end: number;
}

function toKernelU64(value: number): bigint {
  return BigInt(Math.trunc(value));
}

export class Runtime {
  private static nextRuntimeId = 1;

  readonly fs: RuntimeFs = {
    writeFile: async (path, data, mode) => this.writeFile(path, data, mode),
    symlink: async (path, target, mode) => this.symlink(path, target, mode),
    readFile: async (path) => this.readFile(path),
    readlink: async (path) => this.readlink(path),
    mkdir: async (path, mode) => this.mkdir(path, mode),
    stat: async (path) => this.stat(path),
    readdir: async (path) => this.readdir(path),
  };

  private readonly wasmBytes: BufferSource;
  private readonly memoryConfig: RuntimeMemoryConfig;
  private readonly networkBridge: RuntimeNetworkBridge | undefined;
  private readonly runtimeId: number;
  private readonly processes = new Map<number, ProcessHandle>();
  private readonly kernelRequests = new Map<number, {
    resolve: (value: KernelWorkerOutbound) => void;
    reject: (reason?: unknown) => void;
  }>();

  private nextPid = 1;
  private nextKernelRequestId = 1;
  private kernelWorker: Worker | null = null;
  private wasmModule: WebAssembly.Module | null = null;
  private sabPageCache: SharedArrayBuffer | null = null;

  private constructor(options: RuntimeOptions) {
    this.wasmBytes = options.wasmBytes;
    const maxProcessMemoryPages = Math.max(
      options.maxProcessMemoryPages ?? DEFAULT_MAX_MEMORY_PAGES,
      1,
    );
    this.memoryConfig = {
      guestMemoryPolicyLimitBytes: Math.max(
        options.guestMemoryPolicyLimitBytes ?? options.ramSize ?? MIN_RAM_SIZE_BYTES,
        MIN_RAM_SIZE_BYTES,
      ),
      pageCacheDataCapacityBytes: Math.max(
        options.pageCacheDataCapacityBytes ?? MIN_RAM_SIZE_BYTES,
        MIN_RAM_SIZE_BYTES,
      ),
      kernelWorkerMaxMemoryPages: Math.max(
        options.kernelWorkerMaxMemoryPages ?? options.maxKernelMemoryPages ?? DEFAULT_MAX_MEMORY_PAGES,
        1,
      ),
      maxProcessMemoryPages,
      maxForkMemoryPages: Math.max(
        options.maxForkMemoryPages ?? maxProcessMemoryPages,
        maxProcessMemoryPages,
      ),
      fsRpcRingCapacityBytes: normalizeFsRpcRingCapacityBytes(options.fsRpcRingCapacityBytes),
      artifactMaterializationBudgetBytes: normalizeOptionalByteBudget(
        options.artifactMaterializationBudgetBytes,
      ),
    };
    this.networkBridge = options.networkBridge;
    this.runtimeId = Runtime.nextRuntimeId++;
  }

  static async create(options: RuntimeOptions): Promise<Runtime> {
    const runtime = new Runtime(options);
    await runtime.initKernelWorker();
    return runtime;
  }

  private maybeUnrefWorker(worker: Worker): void {
    const nodeWorker = worker as Worker & { unref?: () => void };
    nodeWorker.unref?.();
  }

  private async initKernelWorker(): Promise<void> {
    this.wasmModule = await precompileModule(this.wasmBytes);
    this.sabPageCache = new SharedArrayBuffer(this.memoryConfig.pageCacheDataCapacityBytes);

    const workerUrl = new URL("./kernel-worker.mjs", import.meta.url);
    this.kernelWorker = new Worker(workerUrl, { type: "module" });
    this.maybeUnrefWorker(this.kernelWorker);
    addWorkerMessageListener(this.kernelWorker, this.handleKernelWorkerMessage);
    const worker = this.kernelWorker;
    const cleanup = (): void => {
      if (!worker) {
        return;
      }
      setWorkerErrorHandler(worker, null);
      setWorkerMessageErrorHandler(worker, null);
    };

    try {
      const response = await new Promise<Extract<KernelWorkerOutbound, { type: "ready" }>>((resolve, reject) => {
        const timeout = setTimeout(() => {
          cleanup();
          reject(new Error(`kernel worker init timed out: ${workerUrl.href}`));
        }, 10_000);
        const fail = (detail: string): void => {
          clearTimeout(timeout);
          cleanup();
          reject(new Error(`kernel worker init failed: ${detail} (${workerUrl.href})`));
        };
        setWorkerErrorHandler(worker, (event) => {
          const detail = [
            event.message,
            event.filename ? `at ${event.filename}:${event.lineno}:${event.colno}` : null,
            event.error instanceof Error ? (event.error.stack ?? event.error.message) : null,
          ].filter((part): part is string => !!part).join(" ");
          fail(detail || "worker error");
          event.preventDefault?.();
        });
        setWorkerMessageErrorHandler(worker, (event) => {
          fail(`worker messageerror: ${event.type}`);
          event.preventDefault?.();
        });
        this.sendKernelRequest<Extract<KernelWorkerOutbound, { type: "ready" }>>({
          type: "init-kernel",
          wasmModule: this.wasmModule!,
          sabPageCache: this.sabPageCache!,
          maxMemoryPages: this.memoryConfig.kernelWorkerMaxMemoryPages,
        }).then((value) => {
          clearTimeout(timeout);
          cleanup();
          resolve(value);
        }, (error) => {
          clearTimeout(timeout);
          cleanup();
          reject(error);
        });
      });
      void response;
    } catch (error) {
      removeWorkerMessageListener(worker, this.handleKernelWorkerMessage);
      worker.terminate();
      this.kernelWorker = null;
      throw error;
    }
  }

  private readonly handleKernelWorkerMessage = (
    event: MessageEvent<KernelWorkerOutbound>,
  ): void => {
    const response = event.data;
    const pending = this.kernelRequests.get(response.requestId);
    if (!pending) {
      return;
    }
    this.kernelRequests.delete(response.requestId);
    if (response.type === "error") {
      pending.reject(new Error(response.message));
      return;
    }
    pending.resolve(response);
  };

  private sendKernelRequest<T extends KernelWorkerOutbound>(
    msg: KernelWorkerRequest,
  ): Promise<T> {
    if (!this.kernelWorker) {
      return Promise.reject(new Error("kernel worker not initialized"));
    }

    const requestId = this.nextKernelRequestId++;
    return new Promise<T>((resolve, reject) => {
      this.kernelRequests.set(requestId, {
        resolve: (value) => resolve(value as T),
        reject,
      });
      const payload: KernelWorkerInbound = { ...msg, requestId };
      this.kernelWorker!.postMessage(payload);
    });
  }

  async spawn(
    elfData: Uint8Array,
    argv: string[] = [],
    env: string[] = [],
    options: SpawnOptions = {},
  ): Promise<Process> {
    const pid = this.nextPid++;

    const handle = await createProcess(this.wasmBytes, elfData, {
      runtimeId: this.runtimeId,
      pid,
      ramSize: this.memoryConfig.guestMemoryPolicyLimitBytes,
      argv,
      env,
      cwd: options.cwd,
      sabPageCache: this.sabPageCache,
      maxMemoryPages: this.memoryConfig.maxProcessMemoryPages,
      forkMaxMemoryPages: this.memoryConfig.maxForkMemoryPages,
      kernelWorkerPrivateMemoryBytes: this.memoryConfig.kernelWorkerMaxMemoryPages * 65536,
      fsRpcRingCapacityBytes: this.memoryConfig.fsRpcRingCapacityBytes,
      artifactMaterializationBudgetBytes: this.memoryConfig.artifactMaterializationBudgetBytes ?? undefined,
      networkBridge: this.networkBridge,
      stdio: options.stdio ?? "pipe",
      stdin: options.stdin ?? "pipe",
      hydrateFsFromKernelWorker: true,
      kernelWorkerRequest: (msg) => this.sendKernelRequest(msg),
    });

    const proc = new Process(pid, handle);
    handle.onExit = (code) => {
      this.processes.delete(pid);
      proc.exitCode = code;
      proc.exited = true;
      proc._onExit?.(code);
    };
    handle.onError = (error) => {
      this.processes.delete(pid);
      proc.exitCode = 1;
      proc.exited = true;
      proc.fatalError = error;
      proc._onError?.(error);
    };

    this.processes.set(pid, handle);
    return proc;
  }

  async applyFsSnapshot(entries: FsSnapshotEntry[]): Promise<void> {
    await this.sendKernelRequest<Extract<KernelWorkerOutbound, { type: "fs-loaded" }>>({
      type: "load-fs-snapshot",
      entries,
    });
    const runtimeEntries = withoutSnapshotFileData(entries);
    publishRuntimeFsState(this.runtimeId, runtimeEntries);
    replaceRuntimeFsSnapshot(this.runtimeId, runtimeEntries);
  }

  async writeFile(path: string, data: Uint8Array, mode?: number): Promise<void> {
    await this.sendKernelRequest<Extract<KernelWorkerOutbound, { type: "file-written" }>>({
      type: "write-file",
      path,
      data,
      mode,
    });
    await this.refreshRuntimeFsSnapshot();
  }

  async writeFiles(entries: Array<{ path: string; data: Uint8Array; mode?: number }>): Promise<void> {
    if (entries.length === 0) {
      return;
    }
    await this.sendKernelRequest<Extract<KernelWorkerOutbound, { type: "files-written" }>>({
      type: "write-files",
      entries,
    });
    await this.refreshRuntimeFsSnapshot();
  }

  async applyFileLayer(layer: RuntimeFileLayer | RuntimeFileLayerEntry[]): Promise<void> {
    const entries = Array.isArray(layer) ? layer : layer.entries;
    await this.sendKernelRequest<Extract<KernelWorkerOutbound, { type: "file-layer-applied" }>>({
      type: "apply-file-layer",
      entries,
    });
    await this.refreshRuntimeFsSnapshot();
  }

  async symlink(path: string, target: Uint8Array, mode?: number): Promise<void> {
    await this.sendKernelRequest<Extract<KernelWorkerOutbound, { type: "symlink-created" }>>({
      type: "symlink",
      path,
      target,
      mode,
    });
    await this.refreshRuntimeFsSnapshot();
  }

  async readFile(path: string): Promise<Uint8Array> {
    const normalized = normalizeRuntimePath(path);
    const flushed = flushRuntimeFsStateFromDirtyHandle(this.runtimeId);
    const currentSnapshot = flushed ?? getRuntimeFsState(this.runtimeId);
    const snapshotEntry = currentSnapshot?.entries.find((candidate) => (
      candidate.kind === "file"
      && normalizeRuntimePath(candidate.path) === normalized
    ));
    if (snapshotEntry?.kind === "file" && snapshotEntry.data) {
      return new Uint8Array(snapshotEntry.data);
    }
    try {
      const response = await this.sendKernelRequest<
        Extract<KernelWorkerOutbound, { type: "file-read" }>
      >({
        type: "read-file",
        path,
      });
      return response.data;
    } catch (error) {
      const snapshot = getRuntimeFsState(this.runtimeId);
      const entry = snapshot?.entries.find((candidate) => (
        candidate.kind === "file"
        && normalizeRuntimePath(candidate.path) === normalized
      ));
      if (entry?.kind === "file" && entry.data) {
        return new Uint8Array(entry.data);
      }
      throw error;
    }
  }

  async readlink(path: string): Promise<Uint8Array> {
    const response = await this.sendKernelRequest<
      Extract<KernelWorkerOutbound, { type: "readlink-result" }>
    >({
      type: "readlink",
      path,
    });
    return response.target;
  }

  async mkdir(path: string, mode?: number): Promise<void> {
    await this.sendKernelRequest<Extract<KernelWorkerOutbound, { type: "dir-created" }>>({
      type: "mkdir",
      path,
      mode,
    });
    await this.refreshRuntimeFsSnapshot();
  }

  private async refreshRuntimeFsSnapshot(): Promise<void> {
    const response = await this.sendKernelRequest<
      Extract<KernelWorkerOutbound, { type: "snapshot-fs-result" }>
    >({
      type: "snapshot-fs",
    });
    publishRuntimeFsState(this.runtimeId, response.entries);
    replaceRuntimeFsSnapshot(this.runtimeId, response.entries);
  }

  async stat(path: string): Promise<RuntimeStat> {
    const response = await this.sendKernelRequest<
      Extract<KernelWorkerOutbound, { type: "stat-result" }>
    >({
      type: "stat",
      path,
    });
    return response.stat;
  }

  async readdir(path: string): Promise<string[]> {
    const response = await this.sendKernelRequest<
      Extract<KernelWorkerOutbound, { type: "readdir-result" }>
    >({
      type: "readdir",
      path,
    });
    return response.entries;
  }

  async debugPageCachePath(path: string): Promise<{
    path: string;
    ino: number;
    dataOff: number;
    size: number;
    mode: number;
  } | null> {
    const response = await this.sendKernelRequest<
      Extract<KernelWorkerOutbound, { type: "page-cache-path-debugged" }>
    >({
      type: "debug-page-cache-path",
      path,
    });
    return response.record;
  }

  async debugPageCacheStats(): Promise<{
    byteLength: number;
    dataUsed: number;
    inodeCount: number;
    dirEpoch: number;
  }> {
    const response = await this.sendKernelRequest<
      Extract<KernelWorkerOutbound, { type: "page-cache-stats-debugged" }>
    >({
      type: "debug-page-cache-stats",
    });
    return response.stats;
  }

  onStdout(pid: number, cb: (data: Uint8Array) => void): void {
    const handle = this.processes.get(pid);
    if (!handle) {
      throw new Error(`unknown pid: ${pid}`);
    }
    handle.onStdout = cb;
    for (const chunk of handle.outputBacklog) {
      cb(chunk);
    }
    handle.outputBacklog.length = 0;
  }

  writeStdin(pid: number, data: Uint8Array): void {
    const proc = this.processes.get(pid);
    if (!proc) {
      throw new Error(`unknown pid: ${pid}`);
    }
    writeProcessStdin(proc, data);
  }

  async injectHttp(port: number, request: string): Promise<string> {
    for (const handle of this.processes.values()) {
      try {
        return await injectHttpRequest(handle, port, request);
      } catch (error) {
        if (!(error instanceof Error) || !error.message.includes("no listening process")) {
          throw error;
        }
      }
    }
    throw new Error(`no listening process on port ${port}`);
  }

  kill(pid: number, signal = 15): void {
    const handle = this.processes.get(pid);
    if (!handle) {
      return;
    }
    destroyProcessTree(pid, this.runtimeId, signal);
    this.processes.delete(pid);
  }

  destroy(): void {
    destroyRuntimeProcesses(this.runtimeId);
    clearRuntimeFailureSnapshot(this.runtimeId);
    this.processes.clear();

    for (const pending of this.kernelRequests.values()) {
      pending.reject(new Error("runtime destroyed"));
    }
    this.kernelRequests.clear();

    if (this.kernelWorker) {
      removeWorkerMessageListener(this.kernelWorker, this.handleKernelWorkerMessage);
      this.kernelWorker.terminate();
      this.kernelWorker = null;
    }
    destroyWorkerPool();
  }

  debugSnapshot(): string {
    return debugRuntimeProcessHandles(this.runtimeId);
  }

  debugExecutionStats() {
    return debugRuntimeExecutionStats(this.runtimeId);
  }

  debugLastFailureSnapshot() {
    return debugRuntimeLastFailureSnapshot(this.runtimeId);
  }

  debugWorkerLogFocus() {
    return debugRuntimeWorkerLogFocus(this.runtimeId);
  }
}

export class Process {
  exitCode = -1;
  exited = false;
  fatalError: Error | null = null;
  _onExit?: (code: number) => void;
  _onError?: (error: Error) => void;

  constructor(
    readonly pid: number,
    private readonly handle: ProcessHandle,
  ) {}

  onExit(cb: (code: number) => void): void {
    this._onExit = cb;
    if (this.exited) {
      cb(this.exitCode);
    }
  }

  onStdout(cb: (data: Uint8Array) => void): void {
    this.handle.onStdout = cb;
    for (const chunk of this.handle.outputBacklog) {
      cb(chunk);
    }
    this.handle.outputBacklog.length = 0;
  }

  writeStdin(_data: Uint8Array): void {
    writeProcessStdin(this.handle, _data);
  }

  debugInfo(): string | undefined {
    return this.handle.lastDebugMessage;
  }

  configureGuestWriteWatch(regions: GuestWriteWatchRegion[]): void {
    this.handle.guestWriteWatchRegions = regions.map((region) => ({ ...region }));
    this.handle.kernel.debug_clear_guest_write_watch_trace?.();
    for (const region of this.handle.guestWriteWatchRegions) {
      this.handle.kernel.debug_configure_guest_write_watch?.(
        region.slot,
        toKernelU64(region.start),
        toKernelU64(region.end),
      );
    }
  }

  clearGuestWriteWatch(): void {
    this.handle.guestWriteWatchRegions = [];
    this.handle.kernel.debug_clear_guest_write_watch_trace?.();
  }

  waitForExit(): Promise<number> {
    if (this.fatalError) {
      return Promise.reject(this.fatalError);
    }
    if (this.exited) {
      return Promise.resolve(this.exitCode);
    }
    return new Promise((resolve, reject) => {
      this._onExit = resolve;
      this._onError = reject;
    });
  }
}

export type { FsSnapshotEntry, RuntimeStat } from "./fs/types.js";
export type { KernelExports, StatusCodes, SpawnKindCodes } from "./abi.js";
export type {
  RuntimeNetworkBridge,
  RuntimeNetworkConnection,
  RuntimeNetworkConnectRequest,
  RuntimeNetworkConnectResult,
  RuntimeNetworkEndpoint,
} from "./bridge/network.js";
export { readSpawnKindCodes, readStatusCodes } from "./abi.js";
export { assertSharedMemoryBridge, detectBridgeSupport, diagnoseBridgeSupport } from "./bridge.js";
export { detectWasmFeatures } from "./wasm/features.js";
