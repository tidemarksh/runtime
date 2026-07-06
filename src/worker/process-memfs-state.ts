import type { ProcessHandle } from "./types.js";

interface ProcessMemfsState {
  dirty: boolean;
  publishPending: boolean;
  changedPaths: Set<string> | null;
  changedInos: Set<number> | null;
  deletedPaths: Set<string> | null;
}

const processMemfsStates = new WeakMap<ProcessHandle, ProcessMemfsState>();

function memfsState(handle: ProcessHandle): ProcessMemfsState {
  let state = processMemfsStates.get(handle);
  if (!state) {
    state = {
      dirty: false,
      publishPending: false,
      changedPaths: null,
      changedInos: null,
      deletedPaths: null,
    };
    processMemfsStates.set(handle, state);
  }
  return state;
}

function maybeMemfsState(handle: ProcessHandle): ProcessMemfsState | undefined {
  return processMemfsStates.get(handle);
}

function setIsEmpty<T>(set: Set<T> | null): boolean {
  return set === null || set.size === 0;
}

function clearIfIdle(handle: ProcessHandle, state: ProcessMemfsState): void {
  if (
    !state.dirty
    && !state.publishPending
    && setIsEmpty(state.changedPaths)
    && setIsEmpty(state.changedInos)
    && setIsEmpty(state.deletedPaths)
  ) {
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

export function rememberProcessMemfsChangedPath(handle: ProcessHandle, path: string): void {
  const state = memfsState(handle);
  if (state.changedPaths === null) {
    state.changedPaths = new Set();
  }
  state.changedPaths.add(path);
}

export function rememberProcessMemfsChangedIno(handle: ProcessHandle, ino: number): void {
  if (ino <= 0) {
    return;
  }
  const state = memfsState(handle);
  if (state.changedInos === null) {
    state.changedInos = new Set();
  }
  state.changedInos.add(ino);
}

export function processMemfsChangedPaths(handle: ProcessHandle): string[] {
  return [...(maybeMemfsState(handle)?.changedPaths ?? [])];
}

export function processMemfsChangedInos(handle: ProcessHandle): number[] {
  return [...(maybeMemfsState(handle)?.changedInos ?? [])];
}

export function clearProcessMemfsChanges(handle: ProcessHandle): void {
  const state = maybeMemfsState(handle);
  if (!state) {
    return;
  }
  state.changedPaths?.clear();
  state.changedInos?.clear();
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
