import type { ProcessHandle } from "./types.js";

interface ProcessMemfsState {
  dirty: boolean;
  publishPending: boolean;
  deletedPaths: Set<string> | null;
}

const processMemfsStates = new WeakMap<ProcessHandle, ProcessMemfsState>();

function memfsState(handle: ProcessHandle): ProcessMemfsState {
  let state = processMemfsStates.get(handle);
  if (!state) {
    state = {
      dirty: false,
      publishPending: false,
      deletedPaths: null,
    };
    processMemfsStates.set(handle, state);
  }
  return state;
}

function maybeMemfsState(handle: ProcessHandle): ProcessMemfsState | undefined {
  return processMemfsStates.get(handle);
}

function clearIfIdle(handle: ProcessHandle, state: ProcessMemfsState): void {
  if (!state.dirty && !state.publishPending && (state.deletedPaths === null || state.deletedPaths.size === 0)) {
    processMemfsStates.delete(handle);
  }
}

export function isProcessMemfsDirty(handle: ProcessHandle): boolean {
  return maybeMemfsState(handle)?.dirty ?? false;
}

export function setProcessMemfsDirty(handle: ProcessHandle, dirty: boolean): void {
  const state = memfsState(handle);
  state.dirty = dirty;
  clearIfIdle(handle, state);
}

export function isProcessMemfsPublishPending(handle: ProcessHandle): boolean {
  return maybeMemfsState(handle)?.publishPending ?? false;
}

export function setProcessMemfsPublishPending(
  handle: ProcessHandle,
  publishPending: boolean,
): void {
  const state = memfsState(handle);
  state.publishPending = publishPending;
  clearIfIdle(handle, state);
}

export function processMemfsDeletedPathSet(handle: ProcessHandle): Set<string> {
  const state = memfsState(handle);
  if (state.deletedPaths === null) {
    state.deletedPaths = new Set();
  }
  return state.deletedPaths;
}

export function forgetProcessMemfsDeletedPath(handle: ProcessHandle, path: string): void {
  const state = maybeMemfsState(handle);
  if (!state?.deletedPaths) {
    return;
  }
  state.deletedPaths.delete(path);
  clearIfIdle(handle, state);
}

export function processMemfsDeletedPaths(handle: ProcessHandle): string[] {
  return [...(maybeMemfsState(handle)?.deletedPaths ?? [])];
}
