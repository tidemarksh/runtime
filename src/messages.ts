/**
 * Worker message types (discriminated union).
 * Used for postMessage between main thread, thread-workers, and kernel worker.
 */

export type WorkerMessage =
  | { type: "init"; wasmModule: WebAssembly.Module; memory: WebAssembly.Memory; vmPtr: number }
  | { type: "step"; budget: number }
  | { type: "status"; status: number; exitCode: number; pc: bigint }
  | { type: "spawn-thread"; vmPtr: number }
  | { type: "spawn-process"; elfData: Uint8Array; argv: string[]; env: string[] }
  | { type: "exit"; code: number };
