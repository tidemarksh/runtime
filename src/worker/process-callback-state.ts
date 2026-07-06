import type { ProcessHandle } from "./types.js";

type OnExitCallback = (code: number) => void;
type OnErrorCallback = (error: Error) => void;
type OnStdoutCallback = (data: Uint8Array) => void;
type ProcessExitWaiters = {
  error?: Error;
  errorWaiters: OnErrorCallback[];
  exitCode?: number;
  exitWaiters: OnExitCallback[];
};

const processExitCallbacks = new WeakMap<ProcessHandle, OnExitCallback>();
const processErrorCallbacks = new WeakMap<ProcessHandle, OnErrorCallback>();
const processStdoutCallbacks = new WeakMap<ProcessHandle, OnStdoutCallback>();
const processExitWaiters = new WeakMap<ProcessHandle, ProcessExitWaiters>();
const runtimeProcessExitCallbacks = new Map<string, OnExitCallback>();
const runtimeProcessErrorCallbacks = new Map<string, OnErrorCallback>();
const runtimeProcessExitWaiters = new Map<string, ProcessExitWaiters>();

function runtimeProcessKey(runtimeId: number, pid: number): string {
  return `${runtimeId}:${pid}`;
}

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

function ensureProcessExitWaiters(handle: ProcessHandle): ProcessExitWaiters {
  let waiters = processExitWaiters.get(handle);
  if (!waiters) {
    waiters = {
      errorWaiters: [],
      exitWaiters: [],
    };
    processExitWaiters.set(handle, waiters);
  }
  return waiters;
}

function addExitWaiter(
  waiters: ProcessExitWaiters,
  onExit: OnExitCallback,
  onError: OnErrorCallback,
): void {
  if (waiters.error) {
    onError(waiters.error);
    return;
  }
  if (waiters.exitCode !== undefined) {
    onExit(waiters.exitCode);
    return;
  }
  waiters.exitWaiters.push(onExit);
  waiters.errorWaiters.push(onError);
}

function resolveExitWaiters(waiters: ProcessExitWaiters, code: number): void {
  waiters.exitCode = code;
  waiters.error = undefined;
  const exitWaiters = waiters.exitWaiters.splice(0);
  waiters.errorWaiters.length = 0;
  for (const onExit of exitWaiters) {
    onExit(code);
  }
}

function rejectExitWaiters(waiters: ProcessExitWaiters, error: Error): void {
  waiters.error = error;
  waiters.exitCode = undefined;
  const errorWaiters = waiters.errorWaiters.splice(0);
  waiters.exitWaiters.length = 0;
  for (const onError of errorWaiters) {
    onError(error);
  }
}

export function addProcessExitWaiter(
  handle: ProcessHandle,
  onExit: OnExitCallback,
  onError: OnErrorCallback,
): void {
  addExitWaiter(ensureProcessExitWaiters(handle), onExit, onError);
}

export function resolveProcessExitWaiters(handle: ProcessHandle, code: number): void {
  resolveExitWaiters(ensureProcessExitWaiters(handle), code);
}

export function rejectProcessExitWaiters(handle: ProcessHandle, error: Error): void {
  rejectExitWaiters(ensureProcessExitWaiters(handle), error);
}

function ensureRuntimeProcessExitWaiters(runtimeId: number, pid: number): ProcessExitWaiters {
  const key = runtimeProcessKey(runtimeId, pid);
  let waiters = runtimeProcessExitWaiters.get(key);
  if (!waiters) {
    waiters = {
      errorWaiters: [],
      exitWaiters: [],
    };
    runtimeProcessExitWaiters.set(key, waiters);
  }
  return waiters;
}

export function addRuntimeProcessExitWaiter(
  runtimeId: number,
  pid: number,
  onExit: OnExitCallback,
  onError: OnErrorCallback,
): void {
  addExitWaiter(ensureRuntimeProcessExitWaiters(runtimeId, pid), onExit, onError);
}

export function resolveRuntimeProcessExitWaiters(handle: ProcessHandle, code: number): void {
  const key = runtimeProcessKey(handle.runtimeId, handle.pid);
  const waiters = runtimeProcessExitWaiters.get(key);
  if (!waiters) {
    return;
  }
  resolveExitWaiters(waiters, code);
  runtimeProcessExitWaiters.delete(key);
}

export function rejectRuntimeProcessExitWaiters(handle: ProcessHandle, error: Error): void {
  const key = runtimeProcessKey(handle.runtimeId, handle.pid);
  const waiters = runtimeProcessExitWaiters.get(key);
  if (!waiters) {
    return;
  }
  rejectExitWaiters(waiters, error);
  runtimeProcessExitWaiters.delete(key);
}

export function setRuntimeProcessOnExit(
  runtimeId: number,
  pid: number,
  callback: OnExitCallback | null | undefined,
): void {
  const key = runtimeProcessKey(runtimeId, pid);
  if (callback === undefined || callback === null) {
    runtimeProcessExitCallbacks.delete(key);
    return;
  }
  runtimeProcessExitCallbacks.set(key, callback);
}

export function takeRuntimeProcessOnExit(handle: ProcessHandle): OnExitCallback | undefined {
  const key = runtimeProcessKey(handle.runtimeId, handle.pid);
  const callback = runtimeProcessExitCallbacks.get(key);
  runtimeProcessExitCallbacks.delete(key);
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

export function setRuntimeProcessOnError(
  runtimeId: number,
  pid: number,
  callback: OnErrorCallback | null | undefined,
): void {
  const key = runtimeProcessKey(runtimeId, pid);
  if (callback === undefined || callback === null) {
    runtimeProcessErrorCallbacks.delete(key);
    return;
  }
  runtimeProcessErrorCallbacks.set(key, callback);
}

export function takeRuntimeProcessOnError(handle: ProcessHandle): OnErrorCallback | undefined {
  const key = runtimeProcessKey(handle.runtimeId, handle.pid);
  const callback = runtimeProcessErrorCallbacks.get(key);
  runtimeProcessErrorCallbacks.delete(key);
  return callback;
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
