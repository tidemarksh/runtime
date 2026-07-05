import type { ProcessHandle } from "./types.js";

type OnExitCallback = (code: number) => void;
type OnErrorCallback = (error: Error) => void;
type OnStdoutCallback = (data: Uint8Array) => void;

const processExitCallbacks = new WeakMap<ProcessHandle, OnExitCallback>();
const processErrorCallbacks = new WeakMap<ProcessHandle, OnErrorCallback>();
const processStdoutCallbacks = new WeakMap<ProcessHandle, OnStdoutCallback>();

type LegacyProcessCallbackSeed = {
  onExit?: OnExitCallback | null;
  onError?: OnErrorCallback | null;
  onStdout?: OnStdoutCallback | null;
};

function consumeLegacyExitCallback(handle: ProcessHandle): OnExitCallback | undefined {
  const legacyCallback = (handle as LegacyProcessCallbackSeed).onExit;
  if (legacyCallback !== undefined) {
    delete (handle as LegacyProcessCallbackSeed).onExit;
    if (legacyCallback !== null) {
      processExitCallbacks.set(handle, legacyCallback);
      return legacyCallback;
    }
  }
  return undefined;
}

function consumeLegacyErrorCallback(handle: ProcessHandle): OnErrorCallback | undefined {
  const legacyCallback = (handle as LegacyProcessCallbackSeed).onError;
  if (legacyCallback !== undefined) {
    delete (handle as LegacyProcessCallbackSeed).onError;
    if (legacyCallback !== null) {
      processErrorCallbacks.set(handle, legacyCallback);
      return legacyCallback;
    }
  }
  return undefined;
}

function consumeLegacyStdoutCallback(handle: ProcessHandle): OnStdoutCallback | undefined {
  const legacyCallback = (handle as LegacyProcessCallbackSeed).onStdout;
  if (legacyCallback !== undefined) {
    delete (handle as LegacyProcessCallbackSeed).onStdout;
    if (legacyCallback !== null) {
      processStdoutCallbacks.set(handle, legacyCallback);
      return legacyCallback;
    }
  }
  return undefined;
}

export function processOnExit(handle: ProcessHandle): OnExitCallback | undefined {
  return processExitCallbacks.get(handle) ?? consumeLegacyExitCallback(handle);
}

export function setProcessOnExit(
  handle: ProcessHandle,
  callback: OnExitCallback | null | undefined,
): void {
  delete (handle as LegacyProcessCallbackSeed).onExit;
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
  return processErrorCallbacks.get(handle) ?? consumeLegacyErrorCallback(handle);
}

export function setProcessOnError(
  handle: ProcessHandle,
  callback: OnErrorCallback | null | undefined,
): void {
  delete (handle as LegacyProcessCallbackSeed).onError;
  if (callback === undefined || callback === null) {
    processErrorCallbacks.delete(handle);
    return;
  }
  processErrorCallbacks.set(handle, callback);
}

export function processOnStdout(handle: ProcessHandle): OnStdoutCallback | undefined {
  return processStdoutCallbacks.get(handle) ?? consumeLegacyStdoutCallback(handle);
}

export function setProcessOnStdout(
  handle: ProcessHandle,
  callback: OnStdoutCallback | null | undefined,
): void {
  delete (handle as LegacyProcessCallbackSeed).onStdout;
  if (callback === undefined || callback === null) {
    processStdoutCallbacks.delete(handle);
    return;
  }
  processStdoutCallbacks.set(handle, callback);
}
