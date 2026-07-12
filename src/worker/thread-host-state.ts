import type { ThreadWorkerState } from "./types.js";
import type { SupervisorCloneSpawnHostOp } from "./supervisor-host-op.js";

interface ThreadHostGateState {
  ownerAsyncStatus: number | null;
  ownerAsyncDetail: string | null;
  ownerAsyncStartedAtMs: number | null;
  waitingInWorkerStatus: number | null;
  resumeWorkersOnReady: Worker[];
  runningGeneration: number | null;
  initialized: boolean;
  reinitCount: number;
  ownerPid: number;
  generation: number;
  cloneSpawnHostOp: SupervisorCloneSpawnHostOp | null;
}

const threadHostGateStates = new WeakMap<ThreadWorkerState, ThreadHostGateState>();

function hostGateState(thread: ThreadWorkerState): ThreadHostGateState {
  let state = threadHostGateStates.get(thread);
  if (!state) {
    state = {
      ownerAsyncStatus: null,
      ownerAsyncDetail: null,
      ownerAsyncStartedAtMs: null,
      waitingInWorkerStatus: null,
      resumeWorkersOnReady: [],
      runningGeneration: null,
      initialized: false,
      reinitCount: 0,
      ownerPid: 0,
      generation: 0,
      cloneSpawnHostOp: null,
    };
    threadHostGateStates.set(thread, state);
  }
  return state;
}

function clearIfIdle(thread: ThreadWorkerState, state: ThreadHostGateState): void {
  if (
    state.ownerAsyncStatus === null
    && state.ownerAsyncDetail === null
    && state.ownerAsyncStartedAtMs === null
    && state.waitingInWorkerStatus === null
    && state.resumeWorkersOnReady.length === 0
    && state.runningGeneration === null
    && !state.initialized
    && state.reinitCount === 0
    && state.ownerPid === 0
    && state.generation === 0
    && state.cloneSpawnHostOp === null
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
  detail = `status:${status}`,
): void {
  const state = hostGateState(thread);
  state.ownerAsyncStatus = status;
  state.ownerAsyncDetail = detail;
  state.ownerAsyncStartedAtMs = Date.now();
}

export function setThreadOwnerAsyncDetail(
  thread: ThreadWorkerState,
  detail: string,
): void {
  const state = threadHostGateStates.get(thread);
  if (state?.ownerAsyncStatus === null || state === undefined) {
    return;
  }
  state.ownerAsyncDetail = detail;
}

export function getThreadOwnerAsyncDetail(
  thread: ThreadWorkerState | null | undefined,
): { detail: string | null; startedAtMs: number | null } | null {
  if (thread === null || thread === undefined) {
    return null;
  }
  const state = threadHostGateStates.get(thread);
  if (state?.ownerAsyncStatus === null || state === undefined) {
    return null;
  }
  return {
    detail: state.ownerAsyncDetail,
    startedAtMs: state.ownerAsyncStartedAtMs,
  };
}

export function clearThreadOwnerAsyncStatus(thread: ThreadWorkerState): void {
  const state = threadHostGateStates.get(thread);
  if (!state) {
    return;
  }
  state.ownerAsyncStatus = null;
  state.ownerAsyncDetail = null;
  state.ownerAsyncStartedAtMs = null;
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

export function setThreadCloneSpawnHostOp(
  thread: ThreadWorkerState,
  hostOp: SupervisorCloneSpawnHostOp,
): void {
  hostGateState(thread).cloneSpawnHostOp = hostOp;
}

export function takeThreadCloneSpawnHostOp(
  thread: ThreadWorkerState | null | undefined,
): SupervisorCloneSpawnHostOp | null {
  if (thread === null || thread === undefined) {
    return null;
  }
  const state = threadHostGateStates.get(thread);
  if (!state || state.cloneSpawnHostOp === null) {
    return null;
  }
  const hostOp = state.cloneSpawnHostOp;
  state.cloneSpawnHostOp = null;
  clearIfIdle(thread, state);
  return hostOp;
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

export function getThreadOwnerPid(
  thread: ThreadWorkerState | null | undefined,
): number {
  return thread === null || thread === undefined
    ? 0
    : threadHostGateStates.get(thread)?.ownerPid ?? 0;
}

export function setThreadOwnerPid(thread: ThreadWorkerState, ownerPid: number): void {
  const state = hostGateState(thread);
  state.ownerPid = ownerPid;
  clearIfIdle(thread, state);
}

export function getThreadGeneration(
  thread: ThreadWorkerState | null | undefined,
): number {
  return thread === null || thread === undefined
    ? 0
    : threadHostGateStates.get(thread)?.generation ?? 0;
}

export function setThreadGeneration(thread: ThreadWorkerState, generation: number): void {
  const state = hostGateState(thread);
  state.generation = generation;
  clearIfIdle(thread, state);
}

export function setThreadHostIdentity(
  thread: ThreadWorkerState,
  ownerPid: number,
  generation: number,
): void {
  const state = hostGateState(thread);
  state.ownerPid = ownerPid;
  state.generation = generation;
  clearIfIdle(thread, state);
}

export function clearThreadHostGateState(thread: ThreadWorkerState): void {
  threadHostGateStates.delete(thread);
}
