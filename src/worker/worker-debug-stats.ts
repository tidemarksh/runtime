import type { ProcessHandle } from "./types.js";
import {
  createTransferDebugStats,
  createWorkerMessageDebugStats,
  type TransferDebugStats,
  type WorkerMessageDebugStats,
} from "./types/debug.js";

const workerMessageDebugStatsByHandle = new WeakMap<ProcessHandle, WorkerMessageDebugStats>();
const transferDebugStatsByHandle = new WeakMap<ProcessHandle, TransferDebugStats>();

export function workerMessageDebugStats(handle: ProcessHandle): WorkerMessageDebugStats {
  let stats = workerMessageDebugStatsByHandle.get(handle);
  if (!stats) {
    stats = createWorkerMessageDebugStats();
    workerMessageDebugStatsByHandle.set(handle, stats);
  }
  return stats;
}

export function transferDebugStats(handle: ProcessHandle): TransferDebugStats {
  let stats = transferDebugStatsByHandle.get(handle);
  if (!stats) {
    stats = createTransferDebugStats();
    transferDebugStatsByHandle.set(handle, stats);
  }
  return stats;
}
