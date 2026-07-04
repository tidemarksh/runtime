import type { ThreadWorkerState } from "./types.js";

interface ThreadHostGateState {
  ownerAsyncStatus: number | null;
  waitingInWorkerStatus: number | null;
}

const threadHostGateStates = new WeakMap<ThreadWorkerState, ThreadHostGateState>();

function hostGateState(thread: ThreadWorkerState): ThreadHostGateState {
  let state = threadHostGateStates.get(thread);
  if (!state) {
    state = {
      ownerAsyncStatus: null,
      waitingInWorkerStatus: null,
    };
    threadHostGateStates.set(thread, state);
  }
  return state;
}

function clearIfIdle(thread: ThreadWorkerState, state: ThreadHostGateState): void {
  if (state.ownerAsyncStatus === null && state.waitingInWorkerStatus === null) {
    threadHostGateStates.delete(thread);
  }
}

export function getThreadOwnerAsyncStatus(
  thread: ThreadWorkerState | null | undefined,
): number | null {
  return thread === null || thread === undefined
    ? null
    : threadHostGateStates.get(thread)?.ownerAsyncStatus ?? null;
}

export function setThreadOwnerAsyncStatus(
  thread: ThreadWorkerState,
  status: number,
): void {
  hostGateState(thread).ownerAsyncStatus = status;
}

export function clearThreadOwnerAsyncStatus(thread: ThreadWorkerState): void {
  const state = threadHostGateStates.get(thread);
  if (!state) {
    return;
  }
  state.ownerAsyncStatus = null;
  clearIfIdle(thread, state);
}

export function getThreadWaitingInWorkerStatus(
  thread: ThreadWorkerState | null | undefined,
): number | null {
  return thread === null || thread === undefined
    ? null
    : threadHostGateStates.get(thread)?.waitingInWorkerStatus ?? null;
}

export function setThreadWaitingInWorkerStatus(
  thread: ThreadWorkerState,
  status: number,
): void {
  hostGateState(thread).waitingInWorkerStatus = status;
}

export function clearThreadWaitingInWorkerStatus(thread: ThreadWorkerState): void {
  const state = threadHostGateStates.get(thread);
  if (!state) {
    return;
  }
  state.waitingInWorkerStatus = null;
  clearIfIdle(thread, state);
}

export function clearThreadHostGateState(thread: ThreadWorkerState): void {
  threadHostGateStates.delete(thread);
}
