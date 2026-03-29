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

import { createProcess, destroyWorkerPool, type ProcessHandle } from "./worker.js";

export interface RuntimeOptions {
  wasmBytes: BufferSource;
  ramSize?: number;
}

export class Runtime {
  private wasmBytes: BufferSource;
  private ramSize: number;
  private processes: Map<number, ProcessHandle> = new Map();
  private nextPid = 1;

  private constructor(options: RuntimeOptions) {
    this.wasmBytes = options.wasmBytes;
    this.ramSize = options.ramSize ?? 16 * 1024 * 1024;
  }

  static async create(options: RuntimeOptions): Promise<Runtime> {
    return new Runtime(options);
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

  /** Write a file to the MemFS before spawning processes. */
  async writeFile(_path: string, _data: Uint8Array): Promise<void> {
    // TODO: delegate to kernel Worker via KernelWorkerInbound "write-file"
    // For now, this is a no-op placeholder. Full implementation requires
    // the kernel Worker to be running and a message channel established.
  }

  /** Read a file from the MemFS. */
  async readFile(_path: string): Promise<Uint8Array> {
    // TODO: delegate to kernel Worker via KernelWorkerInbound "read-file"
    return new Uint8Array();
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
