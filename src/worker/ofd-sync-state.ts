import type { ProcessHandle } from "./types.js";

interface OfdSyncState {
  promise: Promise<void> | null;
  queuedIds: Set<number>;
}

const ofdSyncStates = new WeakMap<ProcessHandle, OfdSyncState>();

function ofdSyncState(handle: ProcessHandle): OfdSyncState {
  let state = ofdSyncStates.get(handle);
  if (!state) {
    state = {
      promise: null,
      queuedIds: new Set(),
    };
    ofdSyncStates.set(handle, state);
  }
  return state;
}

function clearIfIdle(handle: ProcessHandle, state: OfdSyncState): void {
  if (state.promise === null && state.queuedIds.size === 0) {
    ofdSyncStates.delete(handle);
  }
}

export function getOfdSyncPromise(handle: ProcessHandle): Promise<void> | null {
  return ofdSyncStates.get(handle)?.promise ?? null;
}

export function setOfdSyncPromise(
  handle: ProcessHandle,
  promise: Promise<void> | null,
): void {
  const state = ofdSyncState(handle);
  state.promise = promise;
  clearIfIdle(handle, state);
}

export function enqueueOfdSyncIds(
  handle: ProcessHandle,
  ofdIds: Iterable<number>,
): number {
  const state = ofdSyncState(handle);
  for (const ofdId of ofdIds) {
    state.queuedIds.add(ofdId);
  }
  const size = state.queuedIds.size;
  clearIfIdle(handle, state);
  return size;
}

export function queuedOfdSyncIdCount(handle: ProcessHandle): number {
  return ofdSyncStates.get(handle)?.queuedIds.size ?? 0;
}

export function takeQueuedOfdSyncIds(handle: ProcessHandle): number[] {
  const state = ofdSyncStates.get(handle);
  if (!state || state.queuedIds.size === 0) {
    return [];
  }
  const ofdIds = [...state.queuedIds];
  state.queuedIds.clear();
  clearIfIdle(handle, state);
  return ofdIds;
}
