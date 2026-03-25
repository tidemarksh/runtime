/**
 * Runtime: VM lifecycle management and public API.
 *
 * Usage:
 *   const runtime = await Runtime.create({ wasmBytes });
 *   const proc = await runtime.spawn(elfData, ["hello"], []);
 *   proc.onExit = (code) => console.log("exit", code);
 */

import { createProcess, type ProcessHandle } from "./worker.js";

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

    // Start execution
    handle.worker.postMessage({ type: "step", budget: 500_000 });

    return proc;
  }

  /** Write a file to the MemFS before spawning processes. */
  async writeFile(path: string, data: Uint8Array): Promise<void> {
    // TODO: implement via kernel WASM exports (memfs::create_file_with_data)
  }

  /** Read a file from the MemFS. */
  async readFile(path: string): Promise<Uint8Array> {
    // TODO: implement via kernel WASM exports
    return new Uint8Array();
  }

  kill(pid: number, signal: number): void {
    const handle = this.processes.get(pid);
    if (handle) {
      handle.worker.terminate();
      this.processes.delete(pid);
    }
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

  writeStdin(data: Uint8Array): void {
    // TODO: implement via pipe
  }

  waitForExit(): Promise<number> {
    if (this.exited) return Promise.resolve(this.exitCode);
    return new Promise((resolve) => {
      this._onExit = resolve;
    });
  }
}

export type { KernelExports, StatusCodes } from "./abi.js";
export { readStatusCodes } from "./abi.js";
