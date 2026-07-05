import type { ProcessHandle } from "./types.js";

const exitInProgressStates = new WeakMap<ProcessHandle, boolean>();

type LegacyExitInProgressSeed = {
  exitInProgress?: boolean;
};

export function processExitInProgress(handle: ProcessHandle): boolean {
  const state = exitInProgressStates.get(handle);
  if (state !== undefined) {
    return state;
  }
  const legacyState = (handle as LegacyExitInProgressSeed).exitInProgress;
  if (legacyState !== undefined) {
    if (legacyState) {
      exitInProgressStates.set(handle, true);
    }
    delete (handle as LegacyExitInProgressSeed).exitInProgress;
    return legacyState;
  }
  return false;
}

export function setProcessExitInProgress(handle: ProcessHandle, inProgress: boolean): void {
  delete (handle as LegacyExitInProgressSeed).exitInProgress;
  if (inProgress) {
    exitInProgressStates.set(handle, true);
    return;
  }
  exitInProgressStates.delete(handle);
}

export function beginProcessExit(handle: ProcessHandle): boolean {
  if (processExitInProgress(handle)) {
    return false;
  }
  setProcessExitInProgress(handle, true);
  return true;
}
