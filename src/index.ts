/**
 * Runtime: VM lifecycle management and public API.
 *
 * §6.1: Launches a dedicated kernel Worker with PRIVATE memory.
 *        All kernel state operations (MemFS, process creation) go through the kernel Worker.
 *        Guest processes get their own shared WebAssembly.Memory.
 * §4.2: FS API is Promise-based.
 *
 * Usage:
 *   const runtime = await Runtime.create({ wasmBytes });
 *   await runtime.writeFile("/hello.c", source);
 *   const proc = await runtime.spawn(elfData, ["hello"], ["PATH=/"]);
 *   const code = await proc.waitForExit();
 */

import { createProcess, destroyWorkerPool, precompileModule, type ProcessHandle } from "./worker.js";
import type { KernelWorkerInbound, KernelWorkerOutbound } from "./messages.js";
import type { KernelRuntimeState } from "./abi.js";

export interface RuntimeOptions {
  wasmBytes: BufferSource;
  ramSize?: number;
}

export class Runtime {
  private wasmBytes: BufferSource;
  private ramSize: number;
  private processes: Map<number, ProcessHandle> = new Map();
  private nextPid = 1;

  /** Dedicated kernel Worker (§6.1: owns private Memory for kernel state). */
  private kernelWorker: Worker | null = null;
  private kernelState: KernelRuntimeState | null = null;
  private wasmModule: WebAssembly.Module | null = null;
  /** §6.3: SAB page cache shared between all Workers (file data, pipe buffers, FS ring). */
  private sabPageCache: SharedArrayBuffer | null = null;

  private constructor(options: RuntimeOptions) {
    this.wasmBytes = options.wasmBytes;
    this.ramSize = options.ramSize ?? 16 * 1024 * 1024;
  }

  static async create(options: RuntimeOptions): Promise<Runtime> {
    const runtime = new Runtime(options);
    await runtime.initKernelWorker();
    return runtime;
  }

  /**
   * §6.1: Launch a dedicated kernel Worker with private Memory.
   * The kernel Worker hosts the only kernel WASM instance and owns all
   * kernel state (MemFS, OFD, VMA, signal, pipe, epoll, socket).
   */
  private async initKernelWorker(): Promise<void> {
    this.wasmModule = await precompileModule(this.wasmBytes);

    // §6.3: Create SAB page cache (shared between all Workers).
    // Initial size: 16MB for file data blocks + pipe buffers + FS ring buffers.
    // Kernel Worker publishes inode/data here; thread-workers read directly.
    this.sabPageCache = new SharedArrayBuffer(16 * 1024 * 1024);

    const workerUrl = new URL("./kernel-worker.js", import.meta.url);
    this.kernelWorker = new Worker(workerUrl, { type: "module" });

    // Send init message and wait for ready
    const kernelState = await this.sendKernelMessage<
      Extract<KernelWorkerOutbound, { type: "ready" }>
    >({
      type: "init-kernel",
      wasmModule: this.wasmModule,
      sabPageCache: this.sabPageCache,
    });
    this.kernelState = kernelState.kernelState;
  }

  async spawn(
    elfData: Uint8Array,
    argv: string[] = [],
    env: string[] = [],
  ): Promise<Process> {
    // Get files written to kernel Worker's MemFS for replication
    const fileList = await this.sendKernelMessage<
      Extract<KernelWorkerOutbound, { type: "file-list" }>
    >({ type: "list-files" });

    const pid = this.nextPid++;
    const handle = await createProcess(this.wasmBytes, elfData, {
      ramSize: this.ramSize,
      argv,
      env,
      kernelState: this.kernelState!,
      sabPageCache: this.sabPageCache!,
      preloadFiles: fileList.files,
      onExit: (code) => {
        this.processes.delete(pid);
        proc.exitCode = code;
        proc.exited = true;
        proc._onExit?.(code);
      },
    });
    handle.pid = pid;
    this.processes.set(pid, handle);

    const proc = new Process(pid, handle);
    return proc;
  }

  /**
   * §6.1: Write a file to MemFS via the kernel Worker (private memory).
   * The kernel Worker owns MemFS — main thread delegates via message.
   */
  async writeFile(path: string, data: Uint8Array): Promise<void> {
    await this.sendKernelMessage<
      Extract<KernelWorkerOutbound, { type: "file-written" }>
    >({
      type: "write-file",
      path,
      data,
    });
  }

  /**
   * §6.1: Read a file from MemFS via the kernel Worker.
   */
  async readFile(path: string): Promise<Uint8Array> {
    const response = await this.sendKernelMessage<
      Extract<KernelWorkerOutbound, { type: "file-read" }>
    >({
      type: "read-file",
      path,
    });
    return response.data;
  }

  kill(pid: number): void {
    const handle = this.processes.get(pid);
    if (handle) {
      for (const w of handle.workers) {
        w.terminate();
      }
      handle.worker.terminate();
      this.processes.delete(pid);
    }
  }

  destroy(): void {
    for (const [pid] of this.processes) {
      this.kill(pid);
    }
    destroyWorkerPool();
    this.kernelWorker?.terminate();
    this.kernelWorker = null;
  }

  /**
   * Send a message to the kernel Worker and wait for the response.
   */
  private sendKernelMessage<T extends KernelWorkerOutbound>(
    msg: KernelWorkerInbound,
  ): Promise<T> {
    return new Promise((resolve, reject) => {
      if (!this.kernelWorker) {
        reject(new Error("kernel Worker not initialized"));
        return;
      }
      const handler = (event: MessageEvent<KernelWorkerOutbound>) => {
        this.kernelWorker!.removeEventListener("message", handler);
        const response = event.data;
        if (response.type === "error") {
          reject(new Error((response as Extract<KernelWorkerOutbound, { type: "error" }>).message));
        } else {
          resolve(response as T);
        }
      };
      this.kernelWorker.addEventListener("message", handler);
      this.kernelWorker.postMessage(msg);
    });
  }
}

export class Process {
  pid: number;
  exitCode: number = -1;
  exited: boolean = false;
  _onExit?: (code: number) => void;

  private handle: ProcessHandle;

  constructor(pid: number, handle: ProcessHandle) {
    this.pid = pid;
    this.handle = handle;
  }

  onExit(cb: (code: number) => void): void {
    this._onExit = cb;
    if (this.exited) cb(this.exitCode);
  }

  onStdout(cb: (data: Uint8Array) => void): void {
    this.handle.onStdout = cb;
  }

  writeStdin(_data: Uint8Array): void {
    // TODO: implement via pipe SAB
  }

  waitForExit(): Promise<number> {
    if (this.exited) return Promise.resolve(this.exitCode);
    return new Promise((resolve) => {
      this._onExit = resolve;
    });
  }
}

export type { KernelExports, StatusCodes, SpawnKindCodes } from "./abi.js";
export { readStatusCodes, readSpawnKindCodes } from "./abi.js";
