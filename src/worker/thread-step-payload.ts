import type { KernelRuntimeState } from "../abi.js";
import type {
  GuestMemoryWriteSnapshot,
  KernelWorkerPipeSlotSnapshot,
} from "../messages.js";
import type { ThreadWorkerState } from "./types.js";

interface ThreadStepPayloadState {
  pendingKernelStateForNextStep?: KernelRuntimeState;
  pendingPipeSlotsForNextStep: KernelWorkerPipeSlotSnapshot[];
  pendingGuestMemoryWrites: GuestMemoryWriteSnapshot[];
}

const threadStepPayloadStates = new WeakMap<ThreadWorkerState, ThreadStepPayloadState>();

function stepPayloadState(thread: ThreadWorkerState): ThreadStepPayloadState {
  let state = threadStepPayloadStates.get(thread);
  if (!state) {
    state = {
      pendingPipeSlotsForNextStep: [],
      pendingGuestMemoryWrites: [],
    };
    threadStepPayloadStates.set(thread, state);
  }
  return state;
}

function clearIfIdle(thread: ThreadWorkerState, state: ThreadStepPayloadState): void {
  if (
    state.pendingKernelStateForNextStep === undefined
    && state.pendingPipeSlotsForNextStep.length === 0
    && state.pendingGuestMemoryWrites.length === 0
  ) {
    threadStepPayloadStates.delete(thread);
  }
}

function endOf(write: GuestMemoryWriteSnapshot): number {
  return write.guestPtr + write.bytes.length;
}

function cloneWrite(write: GuestMemoryWriteSnapshot): GuestMemoryWriteSnapshot {
  return {
    guestPtr: write.guestPtr,
    bytes: write.bytes.slice(),
  };
}

function mergeWrites(
  left: GuestMemoryWriteSnapshot,
  right: GuestMemoryWriteSnapshot,
): GuestMemoryWriteSnapshot | null {
  const leftEnd = endOf(left);
  const rightEnd = endOf(right);
  if (right.guestPtr > leftEnd || left.guestPtr > rightEnd) {
    return null;
  }
  const start = Math.min(left.guestPtr, right.guestPtr);
  const end = Math.max(leftEnd, rightEnd);
  const bytes = new Uint8Array(end - start);
  bytes.set(left.bytes, left.guestPtr - start);
  bytes.set(right.bytes, right.guestPtr - start);
  return { guestPtr: start, bytes };
}

export function setPendingKernelStateForNextStep(
  thread: ThreadWorkerState,
  kernelState: KernelRuntimeState,
): void {
  stepPayloadState(thread).pendingKernelStateForNextStep = kernelState;
}

export function getPendingKernelStateForNextStep(
  thread: ThreadWorkerState | null | undefined,
): KernelRuntimeState | undefined {
  return thread === null || thread === undefined
    ? undefined
    : threadStepPayloadStates.get(thread)?.pendingKernelStateForNextStep;
}

export function takePendingKernelStateForNextStep(
  thread: ThreadWorkerState | null | undefined,
): KernelRuntimeState | undefined {
  if (thread === null || thread === undefined) {
    return undefined;
  }
  const state = threadStepPayloadStates.get(thread);
  if (!state) {
    return undefined;
  }
  const kernelState = state.pendingKernelStateForNextStep;
  state.pendingKernelStateForNextStep = undefined;
  clearIfIdle(thread, state);
  return kernelState;
}

export function appendPendingPipeSlotsForNextStep(
  thread: ThreadWorkerState,
  pipeSlots: readonly KernelWorkerPipeSlotSnapshot[],
): void {
  if (pipeSlots.length === 0) {
    return;
  }
  stepPayloadState(thread).pendingPipeSlotsForNextStep.push(...pipeSlots);
}

export function takePendingPipeSlotsForNextStep(
  thread: ThreadWorkerState | null | undefined,
): KernelWorkerPipeSlotSnapshot[] {
  if (thread === null || thread === undefined) {
    return [];
  }
  const state = threadStepPayloadStates.get(thread);
  if (!state || state.pendingPipeSlotsForNextStep.length === 0) {
    return [];
  }
  const pipeSlots = state.pendingPipeSlotsForNextStep.splice(0);
  clearIfIdle(thread, state);
  return pipeSlots;
}

export function pendingPipeSlotsForNextStepCount(
  thread: ThreadWorkerState | null | undefined,
): number {
  return thread === null || thread === undefined
    ? 0
    : threadStepPayloadStates.get(thread)?.pendingPipeSlotsForNextStep.length ?? 0;
}

export function snapshotPendingPipeSlotsForNextStep(
  thread: ThreadWorkerState | null | undefined,
): KernelWorkerPipeSlotSnapshot[] {
  if (thread === null || thread === undefined) {
    return [];
  }
  const pipeSlots = threadStepPayloadStates.get(thread)?.pendingPipeSlotsForNextStep ?? [];
  return pipeSlots.map((slot) => ({ ...slot }));
}

export function appendPendingGuestMemoryWrites(
  thread: ThreadWorkerState,
  writes: readonly GuestMemoryWriteSnapshot[],
): void {
  const state = stepPayloadState(thread);
  const pendingGuestMemoryWrites = state.pendingGuestMemoryWrites;
  for (const write of writes) {
    if (write.bytes.length === 0 || !Number.isFinite(write.guestPtr)) {
      continue;
    }
    let next = cloneWrite(write);
    for (let index = 0; index < pendingGuestMemoryWrites.length;) {
      const merged = mergeWrites(pendingGuestMemoryWrites[index]!, next);
      if (merged === null) {
        index += 1;
        continue;
      }
      next = merged;
      pendingGuestMemoryWrites.splice(index, 1);
    }
    pendingGuestMemoryWrites.push(next);
    pendingGuestMemoryWrites.sort((left, right) => left.guestPtr - right.guestPtr);
  }
  clearIfIdle(thread, state);
}

export function takePendingGuestMemoryWritesForNextStep(
  thread: ThreadWorkerState | null | undefined,
): GuestMemoryWriteSnapshot[] {
  if (thread === null || thread === undefined) {
    return [];
  }
  const state = threadStepPayloadStates.get(thread);
  if (!state || state.pendingGuestMemoryWrites.length === 0) {
    return [];
  }
  const writes = state.pendingGuestMemoryWrites.splice(0);
  clearIfIdle(thread, state);
  return writes;
}

export function pendingGuestMemoryWritesForNextStepCount(
  thread: ThreadWorkerState | null | undefined,
): number {
  return thread === null || thread === undefined
    ? 0
    : threadStepPayloadStates.get(thread)?.pendingGuestMemoryWrites.length ?? 0;
}

export function snapshotPendingGuestMemoryWritesForNextStep(
  thread: ThreadWorkerState | null | undefined,
): GuestMemoryWriteSnapshot[] {
  if (thread === null || thread === undefined) {
    return [];
  }
  const writes = threadStepPayloadStates.get(thread)?.pendingGuestMemoryWrites ?? [];
  return writes.map(cloneWrite);
}

export function clearThreadStepPayloadState(thread: ThreadWorkerState): void {
  threadStepPayloadStates.delete(thread);
}
