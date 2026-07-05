import type { ThreadWorkerState } from "./types.js";

interface ThreadHostGateState {
  ownerAsyncStatus: number | null;
  waitingInWorkerStatus: number | null;
  resumeWorkersOnReady: Worker[];
  runningGeneration: number | null;
  initialized: boolean;
  reinitCount: number;
}

const threadHostGateStates = new WeakMap<ThreadWorkerState, ThreadHostGateState>();

function hostGateState(thread: ThreadWorkerState): ThreadHostGateState {
  let state = threadHostGateStates.get(thread);
  if (!state) {
    state = {
      ownerAsyncStatus: null,
      waitingInWorkerStatus: null,
      resumeWorkersOnReady: [],
      runningGeneration: null,
      initialized: false,
      reinitCount: 0,
    };
    threadHostGateStates.set(thread, state);
  }
  return state;
}

function clearIfIdle(thread: ThreadWorkerState, state: ThreadHostGateState): void {
  if (
    state.ownerAsyncStatus === null
    && state.waitingInWorkerStatus === null
    && state.resumeWorkersOnReady.length === 0
    && state.runningGeneration === null
    && !state.initialized
    && state.reinitCount === 0
  ) {
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

export function appendThreadResumeWorkerOnReady(
  thread: ThreadWorkerState,
  worker: Worker,
): void {
  hostGateState(thread).resumeWorkersOnReady.push(worker);
}

export function setThreadResumeWorkersOnReady(
  thread: ThreadWorkerState,
  workers: readonly Worker[],
): void {
  const state = hostGateState(thread);
  state.resumeWorkersOnReady = workers.slice();
  clearIfIdle(thread, state);
}

export function takeThreadResumeWorkersOnReady(
  thread: ThreadWorkerState | null | undefined,
): Worker[] {
  if (thread === null || thread === undefined) {
    return [];
  }
  const state = threadHostGateStates.get(thread);
  if (!state || state.resumeWorkersOnReady.length === 0) {
    return [];
  }
  const workers = state.resumeWorkersOnReady.splice(0);
  clearIfIdle(thread, state);
  return workers;
}

export function threadResumeWorkersOnReadyCount(
  thread: ThreadWorkerState | null | undefined,
): number {
  return thread === null || thread === undefined
    ? 0
    : threadHostGateStates.get(thread)?.resumeWorkersOnReady.length ?? 0;
}

export function snapshotThreadResumeWorkersOnReady(
  thread: ThreadWorkerState | null | undefined,
): Worker[] {
  return thread === null || thread === undefined
    ? []
    : threadHostGateStates.get(thread)?.resumeWorkersOnReady.slice() ?? [];
}

export function getThreadRunningGeneration(
  thread: ThreadWorkerState | null | undefined,
): number | null {
  return thread === null || thread === undefined
    ? null
    : threadHostGateStates.get(thread)?.runningGeneration ?? null;
}

export function setThreadRunningGeneration(
  thread: ThreadWorkerState,
  generation: number,
): void {
  hostGateState(thread).runningGeneration = generation;
}

export function clearThreadRunningGeneration(thread: ThreadWorkerState): void {
  const state = threadHostGateStates.get(thread);
  if (!state) {
    return;
  }
  state.runningGeneration = null;
  clearIfIdle(thread, state);
}

export function isThreadInitialized(
  thread: ThreadWorkerState | null | undefined,
): boolean {
  return thread !== null
    && thread !== undefined
    && (threadHostGateStates.get(thread)?.initialized ?? false);
}

export function setThreadInitialized(
  thread: ThreadWorkerState,
  initialized: boolean,
): void {
  const state = hostGateState(thread);
  state.initialized = initialized;
  clearIfIdle(thread, state);
}

export function getThreadReinitCount(
  thread: ThreadWorkerState | null | undefined,
): number {
  return thread === null || thread === undefined
    ? 0
    : threadHostGateStates.get(thread)?.reinitCount ?? 0;
}

export function setThreadReinitCount(
  thread: ThreadWorkerState,
  reinitCount: number,
): void {
  const state = hostGateState(thread);
  state.reinitCount = reinitCount;
  clearIfIdle(thread, state);
}

export function clearThreadHostGateState(thread: ThreadWorkerState): void {
  threadHostGateStates.delete(thread);
}
