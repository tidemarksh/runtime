import type { ThreadWorkerState } from "./types.js";

const HOT_PC_COUNTER_LIMIT = 32;
const RECENT_ACTIVITY_LIMIT = 16;

export interface ThreadWorkerActivityState {
  stepCount: number;
  totalStepBudget: number;
  totalStepWallMs: number;
  maxStepWallMs: number;
  lastStepDurationMs: number | null;
  statusCount: number;
  statusCounts: Record<string, number>;
  lastStatus: number | null;
  recentStatuses: ThreadWorkerStatusSnapshot[];
  lastStepAtMs: number | null;
  lastStatusAtMs: number | null;
  lastStepProcessPtr: number | null;
  lastStepPc: bigint | null;
  lastStepSp: bigint | null;
  lastStepS11: bigint | null;
  hotStepPcs: Record<string, number>;
  recentSteps: ThreadWorkerStepSnapshot[];
  lastStatusPc: bigint | null;
  lastStatusSp: bigint | null;
  lastStatusS11: bigint | null;
  hotStatusPcs: Record<string, number>;
}

export interface ThreadWorkerStepSnapshot {
  pc: bigint;
  sp: bigint;
  s11: bigint;
  atMs: number;
}

export interface ThreadWorkerStatusSnapshot {
  status: number;
  pc: bigint;
  syscallNr: bigint | null;
  fdTraceSyscallNr: bigint | null;
  fdTraceResult: bigint | null;
  futexWaitAddr: bigint | null;
  futexBlockedAddr: bigint | null;
  atMs: number;
}

const threadActivityStates = new WeakMap<ThreadWorkerState, ThreadWorkerActivityState>();

function createThreadActivityState(): ThreadWorkerActivityState {
  return {
    stepCount: 0,
    totalStepBudget: 0,
    totalStepWallMs: 0,
    maxStepWallMs: 0,
    lastStepDurationMs: null,
    statusCount: 0,
    statusCounts: {},
    lastStatus: null,
    recentStatuses: [],
    lastStepAtMs: null,
    lastStatusAtMs: null,
    lastStepProcessPtr: null,
    lastStepPc: null,
    lastStepSp: null,
    lastStepS11: null,
    hotStepPcs: {},
    recentSteps: [],
    lastStatusPc: null,
    lastStatusSp: null,
    lastStatusS11: null,
    hotStatusPcs: {},
  };
}

export function getThreadActivityState(thread: ThreadWorkerState): ThreadWorkerActivityState {
  let state = threadActivityStates.get(thread);
  if (!state) {
    state = createThreadActivityState();
    threadActivityStates.set(thread, state);
  }
  return state;
}

export function maybeThreadActivityState(
  thread: ThreadWorkerState | null | undefined,
): ThreadWorkerActivityState | undefined {
  return thread === null || thread === undefined
    ? undefined
    : threadActivityStates.get(thread);
}

export function resetThreadActivityState(thread: ThreadWorkerState): ThreadWorkerActivityState {
  const state = createThreadActivityState();
  threadActivityStates.set(thread, state);
  return state;
}

export function clearThreadActivityState(thread: ThreadWorkerState): void {
  threadActivityStates.delete(thread);
}

export function incrementBoundedActivityCounter(
  counters: Record<string, number>,
  key: string,
): void {
  counters[key] = (counters[key] ?? 0) + 1;
  const entries = Object.entries(counters);
  if (entries.length <= HOT_PC_COUNTER_LIMIT) {
    return;
  }
  entries
    .sort(([, leftCount], [, rightCount]) => leftCount - rightCount)
    .slice(0, entries.length - HOT_PC_COUNTER_LIMIT)
    .forEach(([entryKey]) => {
      delete counters[entryKey];
    });
}

export function pushRecentStepActivity(
  state: ThreadWorkerActivityState,
  snapshot: ThreadWorkerStepSnapshot,
): void {
  state.recentSteps.push(snapshot);
  if (state.recentSteps.length > RECENT_ACTIVITY_LIMIT) {
    state.recentSteps.splice(0, state.recentSteps.length - RECENT_ACTIVITY_LIMIT);
  }
}

export function pushRecentStatusActivity(
  state: ThreadWorkerActivityState,
  snapshot: ThreadWorkerStatusSnapshot,
): void {
  state.recentStatuses.push(snapshot);
  if (state.recentStatuses.length > RECENT_ACTIVITY_LIMIT) {
    state.recentStatuses.splice(0, state.recentStatuses.length - RECENT_ACTIVITY_LIMIT);
  }
}
