import type { ProcessHandle } from "./types.js";

interface SupervisorControlSeqState {
  fd: bigint;
  fs: bigint;
  signal: bigint;
}

type SupervisorControlSeqKind = keyof SupervisorControlSeqState;

const supervisorControlSeqStates = new WeakMap<ProcessHandle, SupervisorControlSeqState>();

function controlSeqState(handle: ProcessHandle): SupervisorControlSeqState {
  let state = supervisorControlSeqStates.get(handle);
  if (!state) {
    state = {
      fd: 0n,
      fs: 0n,
      signal: 0n,
    };
    supervisorControlSeqStates.set(handle, state);
  }
  return state;
}

function lastSupervisorControlSeq(
  handle: ProcessHandle,
  kind: SupervisorControlSeqKind,
): bigint {
  return supervisorControlSeqStates.get(handle)?.[kind] ?? 0n;
}

function setLastSupervisorControlSeq(
  handle: ProcessHandle,
  kind: SupervisorControlSeqKind,
  seq: bigint,
): void {
  controlSeqState(handle)[kind] = seq;
}

export function lastSupervisorFdControlSeq(handle: ProcessHandle): bigint {
  return lastSupervisorControlSeq(handle, "fd");
}

export function setLastSupervisorFdControlSeq(handle: ProcessHandle, seq: bigint): void {
  setLastSupervisorControlSeq(handle, "fd", seq);
}

export function lastSupervisorFsControlSeq(handle: ProcessHandle): bigint {
  return lastSupervisorControlSeq(handle, "fs");
}

export function setLastSupervisorFsControlSeq(handle: ProcessHandle, seq: bigint): void {
  setLastSupervisorControlSeq(handle, "fs", seq);
}

export function lastSupervisorSignalControlSeq(handle: ProcessHandle): bigint {
  return lastSupervisorControlSeq(handle, "signal");
}

export function setLastSupervisorSignalControlSeq(handle: ProcessHandle, seq: bigint): void {
  setLastSupervisorControlSeq(handle, "signal", seq);
}
