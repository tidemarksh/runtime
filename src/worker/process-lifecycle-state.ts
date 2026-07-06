import type { ProcessHandle } from "./types.js";

const exitInProgressStates = new WeakMap<ProcessHandle, boolean>();

export function processExitInProgress(handle: ProcessHandle): boolean {
  return exitInProgressStates.get(handle) ?? false;
}

export function setProcessExitInProgress(handle: ProcessHandle, inProgress: boolean): void {
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
