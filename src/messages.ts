/**
 * Worker message types (discriminated union).
 * §6.4: All STATUS codes map to specific message flows.
 *
 * Flows:
 *   main → kernel-worker: KernelWorkerMessage
 *   main → thread-worker: ThreadWorkerInbound
 *   thread-worker → main: ThreadWorkerOutbound
 */

import type { KernelRuntimeState } from "./abi.js";

// =========================================================================
// thread-worker inbound (main → thread-worker)
// =========================================================================

export type ThreadWorkerInbound =
  | {
      type: "init";
      wasmModule: WebAssembly.Module;
      memory: WebAssembly.Memory;
      vmPtr: number;
      kernelState: KernelRuntimeState;
      fsRingBuffer: SharedArrayBuffer;
      /** §6.3: SAB page cache for direct file data reads (kernel bypass). */
      sabPageCache: SharedArrayBuffer;
    }
  | { type: "step"; budget: number; kernelState: KernelRuntimeState }
  | { type: "wake-sleep" };

// =========================================================================
// thread-worker outbound (thread-worker → main)
// =========================================================================

export type ThreadWorkerOutbound =
  | {
      type: "status";
      status: number;
      exitCode: number;
      pc: bigint;
      vmPtr: number;
      kernelState: KernelRuntimeState;
    }
  | { type: "exit"; code: number; vmPtr: number };

// =========================================================================
// kernel-worker messages (main ↔ kernel-worker)
// =========================================================================

export type KernelWorkerInbound =
  | {
      type: "init-kernel";
      wasmModule: WebAssembly.Module;
      /** §6.3: SAB page cache shared between all Workers. */
      sabPageCache: SharedArrayBuffer;
    }
  | {
      type: "create-process";
      elfData: Uint8Array;
      argv: string[];
      env: string[];
      ramSize: number;
    }
  | { type: "write-file"; path: string; data: Uint8Array }
  | { type: "read-file"; path: string }
  | { type: "child-exit"; pid: number; waitStatus: number };

export type KernelWorkerOutbound =
  | { type: "ready"; kernelState: KernelRuntimeState }
  | {
      type: "process-created";
      vmPtr: number;
      pid: number;
      kernelState: KernelRuntimeState;
    }
  | { type: "file-written" }
  | { type: "file-read"; data: Uint8Array }
  | { type: "error"; message: string };

