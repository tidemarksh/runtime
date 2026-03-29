/**
 * Worker message types (discriminated union).
 * Used for postMessage between main thread, thread-workers, and kernel worker.
 */

import type { KernelRuntimeState } from "./abi.js";

export type WorkerMessage =
  | {
    type: "init";
    wasmModule: WebAssembly.Module;
    memory: WebAssembly.Memory;
    vmPtr: number;
    kernelState: KernelRuntimeState;
  }
  | { type: "step"; budget: number; kernelState: KernelRuntimeState }
  | {
    type: "status";
    status: number;
    exitCode: number;
    pc: bigint;
    kernelState: KernelRuntimeState;
  }
  | { type: "spawn-thread"; vmPtr: number }
  | { type: "spawn-process"; elfData: Uint8Array; argv: string[]; env: string[] }
  | { type: "exit"; code: number };
