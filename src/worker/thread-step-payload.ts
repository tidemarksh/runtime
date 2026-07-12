import type { KernelRuntimeState } from "../abi.js";
import type {
  GuestMemoryWriteSnapshot,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPipeSlotSnapshot,
} from "../messages.js";
import type { ThreadWorkerExitedThreadPublication } from "./contracts/thread-worker.js";
import type { ThreadWorkerState } from "./types.js";

interface ThreadStepPayloadState {
  pendingKernelStateForNextStep?: KernelRuntimeState;
  pendingAuthoritativeFdsForNextStep: Set<number>;
  pendingPipeSlotsForNextStep: KernelWorkerPipeSlotSnapshot[];
  pendingGuestMemoryWrites: GuestMemoryWriteSnapshot[];
  pendingExitedThreads: ThreadWorkerExitedThreadPublication[];
  postedFdEntriesForNextStatus?: Map<number, KernelWorkerFdEntrySnapshot>;
  postedOfdSlotsForNextStatus?: Map<number, Uint8Array>;
}

const threadStepPayloadStates = new WeakMap<ThreadWorkerState, ThreadStepPayloadState>();

export interface ChangedOfdSlotsSincePosted {
  authoritativeOfdIds: Set<number>;
  postedOfdSlots: ReadonlyMap<number, Uint8Array>;
}

export interface ChangedFdEntriesSincePosted {
  authoritativeFds: Set<number>;
  postedFdEntries: ReadonlyMap<number, KernelWorkerFdEntrySnapshot>;
}

function stepPayloadState(thread: ThreadWorkerState): ThreadStepPayloadState {
  let state = threadStepPayloadStates.get(thread);
  if (!state) {
    state = {
      pendingAuthoritativeFdsForNextStep: new Set(),
      pendingPipeSlotsForNextStep: [],
      pendingGuestMemoryWrites: [],
      pendingExitedThreads: [],
    };
    threadStepPayloadStates.set(thread, state);
  }
  return state;
}

function clearIfIdle(thread: ThreadWorkerState, state: ThreadStepPayloadState): void {
  if (
    state.pendingKernelStateForNextStep === undefined
    && state.pendingAuthoritativeFdsForNextStep.size === 0
    && state.pendingPipeSlotsForNextStep.length === 0
    && state.pendingGuestMemoryWrites.length === 0
    && state.pendingExitedThreads.length === 0
    && state.postedFdEntriesForNextStatus === undefined
    && state.postedOfdSlotsForNextStatus === undefined
  ) {
    threadStepPayloadStates.delete(thread);
  }
}

export function appendPendingExitedThreadForNextStep(
  thread: ThreadWorkerState,
  publication: ThreadWorkerExitedThreadPublication,
): void {
  const pending = stepPayloadState(thread).pendingExitedThreads;
  if (!pending.some((item) => item.vmPtr === publication.vmPtr && item.tid === publication.tid)) {
    pending.push(publication);
  }
}

export function takePendingExitedThreadsForNextStep(
  thread: ThreadWorkerState | null | undefined,
): ThreadWorkerExitedThreadPublication[] {
  if (thread === null || thread === undefined) {
    return [];
  }
  const state = threadStepPayloadStates.get(thread);
  if (!state || state.pendingExitedThreads.length === 0) {
    return [];
  }
  const pending = state.pendingExitedThreads;
  state.pendingExitedThreads = [];
  clearIfIdle(thread, state);
  return pending;
}

export function recordPostedFdEntriesForNextStatus(
  thread: ThreadWorkerState,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
): void {
  stepPayloadState(thread).postedFdEntriesForNextStatus = new Map(
    (fdEntries ?? []).map((entry) => [entry.fd, { ...entry }]),
  );
}

export function takeChangedFdEntriesWithBaselineSincePosted(
  thread: ThreadWorkerState,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
): ChangedFdEntriesSincePosted | undefined {
  const state = threadStepPayloadStates.get(thread);
  const baseline = state?.postedFdEntriesForNextStatus;
  if (!state || baseline === undefined) {
    return undefined;
  }
  if (fdEntries === undefined) {
    return {
      authoritativeFds: new Set(),
      postedFdEntries: baseline,
    };
  }
  state.postedFdEntriesForNextStatus = new Map(
    fdEntries.map((entry) => [entry.fd, { ...entry }]),
  );
  const changed = new Set<number>();
  for (const entry of fdEntries) {
    const before = baseline.get(entry.fd);
    if (
      before === undefined
      || before.ofdId !== entry.ofdId
      || before.fdFlags !== entry.fdFlags
      || before.generation !== entry.generation
    ) {
      changed.add(entry.fd);
    }
  }
  return {
    authoritativeFds: changed,
    postedFdEntries: baseline,
  };
}

export function takeChangedFdEntriesSincePosted(
  thread: ThreadWorkerState,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
): Set<number> | undefined {
  return takeChangedFdEntriesWithBaselineSincePosted(thread, fdEntries)?.authoritativeFds;
}

export function recordPostedOfdSlotsForNextStatus(
  thread: ThreadWorkerState,
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[] | undefined,
): void {
  const state = stepPayloadState(thread);
  state.postedOfdSlotsForNextStatus = new Map(
    (ofdSlots ?? []).map((snapshot) => [snapshot.ofdId, snapshot.slot.slice()]),
  );
}

export function takeChangedOfdSlotsWithBaselineSincePosted(
  thread: ThreadWorkerState,
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[] | undefined,
): ChangedOfdSlotsSincePosted | undefined {
  const state = threadStepPayloadStates.get(thread);
  const baseline = state?.postedOfdSlotsForNextStatus;
  if (!state || baseline === undefined) {
    return undefined;
  }
  if (ofdSlots === undefined) {
    return {
      authoritativeOfdIds: new Set(),
      postedOfdSlots: baseline,
    };
  }
  state.postedOfdSlotsForNextStatus = new Map(
    ofdSlots.map((snapshot) => [snapshot.ofdId, snapshot.slot.slice()]),
  );

  const changed = new Set<number>();
  for (const snapshot of ofdSlots) {
    const before = baseline.get(snapshot.ofdId);
    if (before === undefined) {
      changed.add(snapshot.ofdId);
      continue;
    }
    if (before.byteLength !== snapshot.slot.byteLength) {
      changed.add(snapshot.ofdId);
      continue;
    }
    if (before.some((value, index) => value !== snapshot.slot[index])) {
      changed.add(snapshot.ofdId);
    }
  }
  return {
    authoritativeOfdIds: changed,
    postedOfdSlots: baseline,
  };
}

export function takeChangedOfdSlotsSincePosted(
  thread: ThreadWorkerState,
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[] | undefined,
): Set<number> | undefined {
  return takeChangedOfdSlotsWithBaselineSincePosted(thread, ofdSlots)?.authoritativeOfdIds;
}

export function appendPendingAuthoritativeFdForNextStep(
  thread: ThreadWorkerState,
  fd: number,
): void {
  if (!Number.isInteger(fd) || fd < 0) {
    return;
  }
  stepPayloadState(thread).pendingAuthoritativeFdsForNextStep.add(fd);
}

export function takePendingAuthoritativeFdEntriesForNextStep(
  thread: ThreadWorkerState | null | undefined,
  fdEntries: KernelWorkerFdEntrySnapshot[] | undefined,
): KernelWorkerFdEntrySnapshot[] | undefined {
  if (thread === null || thread === undefined) {
    return fdEntries;
  }
  const state = threadStepPayloadStates.get(thread);
  if (!state || state.pendingAuthoritativeFdsForNextStep.size === 0) {
    return fdEntries;
  }
  const authoritativeFds = new Set(state.pendingAuthoritativeFdsForNextStep);
  state.pendingAuthoritativeFdsForNextStep.clear();
  clearIfIdle(thread, state);
  return fdEntries?.map((entry) => authoritativeFds.has(entry.fd) && entry.monotonicGeneration !== true
    ? {
        fd: entry.fd,
        ofdId: entry.ofdId,
        fdFlags: entry.fdFlags,
        ...(entry.authorityEpoch !== undefined
          ? { authorityEpoch: entry.authorityEpoch }
          : {}),
      }
    : entry);
}

export function snapshotPendingAuthoritativeFdsForNextStep(
  thread: ThreadWorkerState | null | undefined,
): number[] {
  return thread === null || thread === undefined
    ? []
    : [...(threadStepPayloadStates.get(thread)?.pendingAuthoritativeFdsForNextStep ?? [])];
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
