import type { HostBlockedWorkerState } from "./types.js";

interface HostBlockedEffectState {
  rpcPending: boolean;
  rpcGeneration: number;
  pipeCanonicalEofReady: boolean;
  lastPipeRetryLifecycleEvent: string | null;
  lastWait4RetryLifecycleEvent: string | null;
}

const hostBlockedEffectStates = new WeakMap<HostBlockedWorkerState, HostBlockedEffectState>();

function effectState(blocked: HostBlockedWorkerState): HostBlockedEffectState {
  let state = hostBlockedEffectStates.get(blocked);
  if (!state) {
    state = {
      rpcPending: false,
      rpcGeneration: 0,
      pipeCanonicalEofReady: false,
      lastPipeRetryLifecycleEvent: null,
      lastWait4RetryLifecycleEvent: null,
    };
    hostBlockedEffectStates.set(blocked, state);
  }
  return state;
}

function maybeEffectState(
  blocked: HostBlockedWorkerState | null | undefined,
): HostBlockedEffectState | undefined {
  return blocked === null || blocked === undefined
    ? undefined
    : hostBlockedEffectStates.get(blocked);
}

export function isBlockedRpcPending(
  blocked: HostBlockedWorkerState | null | undefined,
): boolean {
  return maybeEffectState(blocked)?.rpcPending ?? false;
}

export function setBlockedRpcPending(
  blocked: HostBlockedWorkerState,
  pending: boolean,
): void {
  effectState(blocked).rpcPending = pending;
}

export function getBlockedRpcGeneration(
  blocked: HostBlockedWorkerState | null | undefined,
): number {
  return maybeEffectState(blocked)?.rpcGeneration ?? 0;
}

export function setBlockedRpcGeneration(
  blocked: HostBlockedWorkerState,
  generation: number,
): void {
  effectState(blocked).rpcGeneration = generation;
}

export function beginBlockedRpc(blocked: HostBlockedWorkerState): number {
  const state = effectState(blocked);
  state.rpcPending = true;
  state.rpcGeneration += 1;
  return state.rpcGeneration;
}

export function finishBlockedRpc(
  blocked: HostBlockedWorkerState,
  rpcGeneration: number | null,
): boolean {
  const state = effectState(blocked);
  if (rpcGeneration !== null && state.rpcGeneration !== rpcGeneration) {
    return false;
  }
  state.rpcPending = false;
  return true;
}

export function invalidateBlockedRpc(blocked: HostBlockedWorkerState): number {
  const state = effectState(blocked);
  state.rpcPending = false;
  state.rpcGeneration += 1;
  return state.rpcGeneration;
}

export function isPipeCanonicalEofReady(
  blocked: HostBlockedWorkerState | null | undefined,
): boolean {
  return maybeEffectState(blocked)?.pipeCanonicalEofReady ?? false;
}

export function setPipeCanonicalEofReady(
  blocked: HostBlockedWorkerState,
  ready: boolean,
): void {
  effectState(blocked).pipeCanonicalEofReady = ready;
}

export function getLastPipeRetryLifecycleEvent(
  blocked: HostBlockedWorkerState | null | undefined,
): string | null {
  return maybeEffectState(blocked)?.lastPipeRetryLifecycleEvent ?? null;
}

export function setLastPipeRetryLifecycleEvent(
  blocked: HostBlockedWorkerState,
  event: string | null,
): void {
  effectState(blocked).lastPipeRetryLifecycleEvent = event;
}

export function getLastWait4RetryLifecycleEvent(
  blocked: HostBlockedWorkerState | null | undefined,
): string | null {
  return maybeEffectState(blocked)?.lastWait4RetryLifecycleEvent ?? null;
}

export function setLastWait4RetryLifecycleEvent(
  blocked: HostBlockedWorkerState,
  event: string | null,
): void {
  effectState(blocked).lastWait4RetryLifecycleEvent = event;
}

export function copyHostBlockedEffectState(
  target: HostBlockedWorkerState,
  source: HostBlockedWorkerState,
): void {
  const sourceState = maybeEffectState(source);
  if (!sourceState) {
    return;
  }
  hostBlockedEffectStates.set(target, { ...sourceState });
}
