import type { ProcessHandle } from "./types.js";
import {
  createTransferDebugStats,
  createWorkerMessageDebugStats,
  type TransferDebugStats,
  type WorkerMessageDebugStats,
} from "./types/debug.js";

const workerMessageDebugStatsByHandle = new WeakMap<ProcessHandle, WorkerMessageDebugStats>();
const transferDebugStatsByHandle = new WeakMap<ProcessHandle, TransferDebugStats>();

type LegacyWorkerDebugStatsSeed = {
  workerMessageDebugStats?: WorkerMessageDebugStats;
  transferDebugStats?: TransferDebugStats;
};

export function workerMessageDebugStats(handle: ProcessHandle): WorkerMessageDebugStats {
  let stats = workerMessageDebugStatsByHandle.get(handle);
  if (!stats) {
    const legacyStats = (handle as LegacyWorkerDebugStatsSeed).workerMessageDebugStats;
    stats = legacyStats ?? createWorkerMessageDebugStats();
    workerMessageDebugStatsByHandle.set(handle, stats);
    if (legacyStats !== undefined) {
      delete (handle as LegacyWorkerDebugStatsSeed).workerMessageDebugStats;
    }
  }
  return stats;
}

export function transferDebugStats(handle: ProcessHandle): TransferDebugStats {
  let stats = transferDebugStatsByHandle.get(handle);
  if (!stats) {
    const legacyStats = (handle as LegacyWorkerDebugStatsSeed).transferDebugStats;
    stats = legacyStats ?? createTransferDebugStats();
    transferDebugStatsByHandle.set(handle, stats);
    if (legacyStats !== undefined) {
      delete (handle as LegacyWorkerDebugStatsSeed).transferDebugStats;
    }
  }
  return stats;
}
