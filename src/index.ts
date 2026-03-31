import {
  clearRuntimeFsState,
  createProcess,
  debugRuntimeProcessHandles,
  destroyProcessTree,
  destroyRuntimeProcesses,
  destroyWorkerPool,
  flushRuntimeFsStateFromDirtyHandle,
  getRuntimeFsState,
  injectHttpRequest,
  primeRuntimeFsState,
  publishRuntimeFsState,
  type ProcessHandle,
  precompileModule,
  writeProcessStdin,
} from "./worker.js";
import type { RuntimeStat } from "./fs-types.js";
import type {
  KernelWorkerInbound,
  KernelWorkerOutbound,
  KernelWorkerRequest,
} from "./messages.js";
import { addWorkerMessageListener, removeWorkerMessageListener } from "./worker-compat.js";

export interface RuntimeOptions {
  wasmBytes: BufferSource;
  ramSize?: number;
}

export interface RuntimeFs {
  writeFile(path: string, data: Uint8Array, mode?: number): Promise<void>;
  readFile(path: string): Promise<Uint8Array>;
  mkdir(path: string, mode?: number): Promise<void>;
  stat(path: string): Promise<RuntimeStat>;
  readdir(path: string): Promise<string[]>;
}

export interface SpawnOptions {
  cwd?: string;
}

export class Runtime {
  private static nextRuntimeId = 1;

  readonly fs: RuntimeFs = {
    writeFile: async (path, data, mode) => this.writeFile(path, data, mode),
    readFile: async (path) => this.readFile(path),
    mkdir: async (path, mode) => this.mkdir(path, mode),
    stat: async (path) => this.stat(path),
    readdir: async (path) => this.readdir(path),
  };

  private readonly wasmBytes: BufferSource;
  private readonly ramSize: number;
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
  private kernelFsVersion = 0;

  private constructor(options: RuntimeOptions) {
    this.wasmBytes = options.wasmBytes;
    this.ramSize = Math.max(options.ramSize ?? 16 * 1024 * 1024, 64 * 1024 * 1024);
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
    this.sabPageCache = new SharedArrayBuffer(Math.max(64 * 1024 * 1024, this.ramSize));

    const workerUrl = new URL("./kernel-worker.mjs", import.meta.url);
    this.kernelWorker = new Worker(workerUrl, { type: "module" });
    this.maybeUnrefWorker(this.kernelWorker);
    addWorkerMessageListener(this.kernelWorker, this.handleKernelWorkerMessage);

    const response = await this.sendKernelRequest<Extract<KernelWorkerOutbound, { type: "ready" }>>({
      type: "init-kernel",
      wasmModule: this.wasmModule,
      sabPageCache: this.sabPageCache,
    });
    void response;
    await this.refreshRuntimeFsStateFromKernelWorker(false);
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

  private async syncKernelWorkerFs(): Promise<void> {
    flushRuntimeFsStateFromDirtyHandle(this.runtimeId);
    const state = getRuntimeFsState(this.runtimeId);
    if (!state || state.version <= this.kernelFsVersion) {
      return;
    }
    await this.sendKernelRequest<Extract<KernelWorkerOutbound, { type: "fs-loaded" }>>({
      type: "load-fs-snapshot",
      entries: state.entries,
    });
    this.kernelFsVersion = state.version;
  }

  private async refreshRuntimeFsStateFromKernelWorker(bumpVersion: boolean): Promise<void> {
    const snapshot = await this.sendKernelRequest<
      Extract<KernelWorkerOutbound, { type: "snapshot-fs-result" }>
    >({
      type: "snapshot-fs",
    });
    this.kernelFsVersion = bumpVersion
      ? publishRuntimeFsState(this.runtimeId, snapshot.entries)
      : primeRuntimeFsState(this.runtimeId, snapshot.entries);
  }

  async spawn(
    elfData: Uint8Array,
    argv: string[] = [],
    env: string[] = [],
    options: SpawnOptions = {},
  ): Promise<Process> {
    const pid = this.nextPid++;
    await this.syncKernelWorkerFs();

    const handle = await createProcess(this.wasmBytes, elfData, {
      runtimeId: this.runtimeId,
      pid,
      ramSize: this.ramSize,
      argv,
      env,
      cwd: options.cwd,
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

  async applyFsSnapshot(entries: import("./fs-types.js").FsSnapshotEntry[]): Promise<void> {
    await this.sendKernelRequest<Extract<KernelWorkerOutbound, { type: "fs-loaded" }>>({
      type: "load-fs-snapshot",
      entries,
    });
    this.kernelFsVersion = publishRuntimeFsState(this.runtimeId, entries);
  }

  async writeFile(path: string, data: Uint8Array, mode?: number): Promise<void> {
    await this.syncKernelWorkerFs();
    await this.sendKernelRequest<Extract<KernelWorkerOutbound, { type: "file-written" }>>({
      type: "write-file",
      path,
      data,
      mode,
    });
    await this.refreshRuntimeFsStateFromKernelWorker(true);
  }

  async readFile(path: string): Promise<Uint8Array> {
    await this.syncKernelWorkerFs();
    const response = await this.sendKernelRequest<
      Extract<KernelWorkerOutbound, { type: "file-read" }>
    >({
      type: "read-file",
      path,
    });
    return response.data;
  }

  async mkdir(path: string, mode?: number): Promise<void> {
    await this.syncKernelWorkerFs();
    await this.sendKernelRequest<Extract<KernelWorkerOutbound, { type: "dir-created" }>>({
      type: "mkdir",
      path,
      mode,
    });
    await this.refreshRuntimeFsStateFromKernelWorker(true);
  }

  async stat(path: string): Promise<RuntimeStat> {
    await this.syncKernelWorkerFs();
    const response = await this.sendKernelRequest<
      Extract<KernelWorkerOutbound, { type: "stat-result" }>
    >({
      type: "stat",
      path,
    });
    return response.stat;
  }

  async readdir(path: string): Promise<string[]> {
    await this.syncKernelWorkerFs();
    const response = await this.sendKernelRequest<
      Extract<KernelWorkerOutbound, { type: "readdir-result" }>
    >({
      type: "readdir",
      path,
    });
    return response.entries;
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
    clearRuntimeFsState(this.runtimeId);
    destroyWorkerPool();
  }

  debugSnapshot(): string {
    return debugRuntimeProcessHandles(this.runtimeId);
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

export type { FsSnapshotEntry, RuntimeStat } from "./fs-types.js";
export type { KernelExports, StatusCodes, SpawnKindCodes } from "./abi.js";
export { readSpawnKindCodes, readStatusCodes } from "./abi.js";
export { assertSharedMemoryBridge, detectBridgeSupport } from "./bridge.js";
export { detectWasmFeatures } from "./wasm-features.js";
