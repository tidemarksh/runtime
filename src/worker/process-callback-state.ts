import type { ProcessHandle } from "./types.js";

type OnExitCallback = (code: number) => void;
type OnErrorCallback = (error: Error) => void;
type OnStdoutCallback = (data: Uint8Array) => void;

const processExitCallbacks = new WeakMap<ProcessHandle, OnExitCallback>();
const processErrorCallbacks = new WeakMap<ProcessHandle, OnErrorCallback>();
const processStdoutCallbacks = new WeakMap<ProcessHandle, OnStdoutCallback>();

export function processOnExit(handle: ProcessHandle): OnExitCallback | undefined {
  return processExitCallbacks.get(handle);
}

export function setProcessOnExit(
  handle: ProcessHandle,
  callback: OnExitCallback | null | undefined,
): void {
  if (callback === undefined || callback === null) {
    processExitCallbacks.delete(handle);
    return;
  }
  processExitCallbacks.set(handle, callback);
}

export function takeProcessOnExit(handle: ProcessHandle): OnExitCallback | undefined {
  const callback = processOnExit(handle);
  setProcessOnExit(handle, undefined);
  return callback;
}

export function processOnError(handle: ProcessHandle): OnErrorCallback | undefined {
  return processErrorCallbacks.get(handle);
}

export function setProcessOnError(
  handle: ProcessHandle,
  callback: OnErrorCallback | null | undefined,
): void {
  if (callback === undefined || callback === null) {
    processErrorCallbacks.delete(handle);
    return;
  }
  processErrorCallbacks.set(handle, callback);
}

export function processOnStdout(handle: ProcessHandle): OnStdoutCallback | undefined {
  return processStdoutCallbacks.get(handle);
}

export function setProcessOnStdout(
  handle: ProcessHandle,
  callback: OnStdoutCallback | null | undefined,
): void {
  if (callback === undefined || callback === null) {
    processStdoutCallbacks.delete(handle);
    return;
  }
  processStdoutCallbacks.set(handle, callback);
}
