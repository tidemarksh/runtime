import type { ProcessHandle } from "./types.js";

export function hasActiveChildProcess(
  processHandles: ReadonlyMap<number, ProcessHandle>,
  parentPid: number,
): boolean {
  for (const candidate of processHandles.values()) {
    if (candidate.parentPid !== parentPid) {
      continue;
    }
    if (candidate.exitInProgress) {
      continue;
    }
    return true;
  }
  return false;
}
