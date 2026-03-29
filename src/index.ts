/**
 * Runtime: VM lifecycle management and public API.
 *
 * §6.1: Launches a kernel Worker (dedicated) and manages process lifecycle.
 * §4.2: FS API is Promise-based.
 *
 * Usage:
 *   const runtime = await Runtime.create({ wasmBytes });
 *   await runtime.writeFile("/hello.c", source);
 *   const proc = await runtime.spawn(elfData, ["hello"], ["PATH=/"]);
 *   const code = await proc.waitForExit();
 */

import type { KernelExports } from "./abi.js";
import { createProcess, destroyWorkerPool, precompileModule, type ProcessHandle } from "./worker.js";

export interface RuntimeOptions {
  wasmBytes: BufferSource;
  ramSize?: number;
}

export class Runtime {
  private wasmBytes: BufferSource;
  private ramSize: number;
  private processes: Map<number, ProcessHandle> = new Map();
  private nextPid = 1;

  /** Host kernel instance for pre-spawn MemFS operations. */
  private hostKernel: KernelExports | null = null;
  private hostMemory: WebAssembly.Memory | null = null;

  private constructor(options: RuntimeOptions) {
    this.wasmBytes = options.wasmBytes;
    this.ramSize = options.ramSize ?? 16 * 1024 * 1024;
  }

  static async create(options: RuntimeOptions): Promise<Runtime> {
    const runtime = new Runtime(options);
    await runtime.initHostKernel();
    return runtime;
  }

  /**
   * Initialize a host kernel WASM instance for pre-spawn MemFS operations.
   * This runs in the main thread and is used for writeFile/readFile before
   * any process is spawned. Once the kernel Worker is fully operational,
   * these operations will be delegated there.
   */
  private async initHostKernel(): Promise<void> {
    const module = await precompileModule(this.wasmBytes);
    this.hostMemory = new WebAssembly.Memory({
      initial: 256, // 16 MB
      maximum: 32768,
      shared: true,
    });
    const instance = await WebAssembly.instantiate(module, {
      env: { memory: this.hostMemory },
    });
    this.hostKernel = instance.exports as unknown as KernelExports;
    this.hostKernel.kernel_init(0x200000);

    // Set wall clock base time
    const now = Date.now();
    this.hostKernel.set_base_time(
      BigInt(Math.floor(now / 1000)),
      BigInt((now % 1000) * 1_000_000),
    );
  }

  async spawn(
    elfData: Uint8Array,
    argv: string[] = [],
    env: string[] = [],
  ): Promise<Process> {
    const pid = this.nextPid++;
    const handle = await createProcess(this.wasmBytes, elfData, {
      ramSize: this.ramSize,
      argv,
      env,
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
   * Write a file to the MemFS. Can be called before spawning processes
   * to stage files (ELF binaries, source code, config).
   */
  async writeFile(path: string, data: Uint8Array): Promise<void> {
    const k = this.hostKernel!;
    const mem = new Uint8Array(k.memory.buffer);
    const encoder = new TextEncoder();
    const scratch = 0x300000; // scratch area above heap base

    // Parse path into parent directories + filename
    const parts = path.split("/").filter(Boolean);
    const filename = parts.pop();
    if (!filename) throw new Error(`Invalid path: ${path}`);

    // Ensure parent directories
    let parentIno = 1; // root
    for (const dir of parts) {
      const dirBytes = encoder.encode(dir);
      mem.set(dirBytes, scratch);
      parentIno = k.memfs_ensure_dir(parentIno, scratch, dirBytes.length, 0o755);
      if (parentIno === 0) throw new Error(`Failed to mkdir: ${dir}`);
    }

    // Write file with data
    const nameBytes = encoder.encode(filename);
    mem.set(nameBytes, scratch);
    const dataOffset = scratch + nameBytes.length + 64;
    mem.set(data, dataOffset);

    const ino = k.memfs_create_file_with_data(
      parentIno,
      scratch,
      nameBytes.length,
      dataOffset,
      data.length,
    );
    if (ino === 0) throw new Error(`Failed to write file: ${path}`);
  }

  /**
   * Read a file from the MemFS.
   */
  async readFile(path: string): Promise<Uint8Array> {
    const k = this.hostKernel!;
    const mem = new Uint8Array(k.memory.buffer);
    const encoder = new TextEncoder();
    const scratch = 0x300000;

    const pathBytes = encoder.encode(path);
    mem.set(pathBytes, scratch);

    const ino = k.memfs_resolve_path(scratch, pathBytes.length);
    if (ino === 0) throw new Error(`File not found: ${path}`);

    const size = Number(k.memfs_file_size(ino));
    const bufOffset = scratch + 1024;
    const bytesRead = Number(k.memfs_read_file_to(ino, 0n, bufOffset, size));
    if (bytesRead < 0) throw new Error(`Failed to read: ${path}`);

    return new Uint8Array(mem.buffer.slice(bufOffset, bufOffset + bytesRead));
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
