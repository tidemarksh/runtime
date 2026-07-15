import type { HostBlockedWorkerState } from "./types.js";

interface HostBlockedEffectState {
  rpcPending: boolean;
  rpcGeneration: number;
  rpcDeferredRetryPending: boolean;
  passiveUntilWake: boolean;
  epollPassiveUntilWake: boolean;
  epollWakeRetryPending: boolean;
  epollWakeRetryGeneration: number;
  epollDeferredRetrySignature: string | null;
  epollDeferredRetryWakeGeneration: number;
  pipeCanonicalEofReady: boolean;
  waitTimerAlreadyChecked: boolean;
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
      rpcDeferredRetryPending: false,
      passiveUntilWake: false,
      epollPassiveUntilWake: false,
      epollWakeRetryPending: false,
      epollWakeRetryGeneration: 0,
      epollDeferredRetrySignature: null,
      epollDeferredRetryWakeGeneration: 0,
      pipeCanonicalEofReady: false,
      waitTimerAlreadyChecked: false,
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
  state.rpcDeferredRetryPending = false;
  state.rpcGeneration += 1;
  return state.rpcGeneration;
}

export function markBlockedRpcDeferredRetry(blocked: HostBlockedWorkerState): void {
  effectState(blocked).rpcDeferredRetryPending = true;
}

export function consumeBlockedRpcDeferredRetry(blocked: HostBlockedWorkerState): boolean {
  const state = effectState(blocked);
  if (!state.rpcDeferredRetryPending) {
    return false;
  }
  state.rpcDeferredRetryPending = false;
  return true;
}

export function isBlockedPassiveUntilWake(
  blocked: HostBlockedWorkerState | null | undefined,
): boolean {
  return maybeEffectState(blocked)?.passiveUntilWake ?? false;
}

export function setBlockedPassiveUntilWake(
  blocked: HostBlockedWorkerState,
  passive: boolean,
): void {
  effectState(blocked).passiveUntilWake = passive;
}

export function isEpollPassiveUntilWake(
  blocked: HostBlockedWorkerState | null | undefined,
): boolean {
  return maybeEffectState(blocked)?.epollPassiveUntilWake ?? false;
}

export function setEpollPassiveUntilWake(
  blocked: HostBlockedWorkerState,
  passive: boolean,
): void {
  effectState(blocked).epollPassiveUntilWake = passive;
}

export function isEpollWakeRetryPending(
  blocked: HostBlockedWorkerState | null | undefined,
): boolean {
  return maybeEffectState(blocked)?.epollWakeRetryPending ?? false;
}

export function setEpollWakeRetryPending(
  blocked: HostBlockedWorkerState,
  pending: boolean,
): void {
  const state = effectState(blocked);
  state.epollWakeRetryPending = pending;
  if (pending) {
    state.epollWakeRetryGeneration += 1;
  }
}

export function getEpollWakeRetryGeneration(
  blocked: HostBlockedWorkerState | null | undefined,
): number {
  return maybeEffectState(blocked)?.epollWakeRetryGeneration ?? 0;
}

export function getEpollDeferredRetryWakeGeneration(
  blocked: HostBlockedWorkerState | null | undefined,
): number {
  return maybeEffectState(blocked)?.epollDeferredRetryWakeGeneration ?? 0;
}

export function getEpollDeferredRetrySignature(
  blocked: HostBlockedWorkerState | null | undefined,
): string | null {
  return maybeEffectState(blocked)?.epollDeferredRetrySignature ?? null;
}

export function setEpollDeferredRetrySignature(
  blocked: HostBlockedWorkerState,
  signature: string | null,
): void {
  const state = effectState(blocked);
  state.epollDeferredRetrySignature = signature;
  state.epollDeferredRetryWakeGeneration = signature === null
    ? 0
    : state.epollWakeRetryGeneration;
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

export function setWaitTimerAlreadyChecked(
  blocked: HostBlockedWorkerState,
  checked: boolean,
): void {
  effectState(blocked).waitTimerAlreadyChecked = checked;
}

export function consumeWaitTimerAlreadyChecked(
  blocked: HostBlockedWorkerState | null | undefined,
): boolean {
  const state = maybeEffectState(blocked);
  if (state === undefined || !state.waitTimerAlreadyChecked) {
    return false;
  }
  state.waitTimerAlreadyChecked = false;
  return true;
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
