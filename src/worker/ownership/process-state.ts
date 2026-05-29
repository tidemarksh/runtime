import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import type { ProcessHandle, ThreadWorkerState } from "../types.js";

export type ProcessStateSnapshot = {
  offset: number;
  bytes: Uint8Array;
};

export type HandleProcessStateSnapshot = {
  handle: ProcessHandle;
  snapshot: ProcessStateSnapshot | null;
};

export function createWorkerOwnershipGuard(
  handle: ProcessHandle,
  worker: Worker,
  thread: ThreadWorkerState,
  threadWorkerStates: Pick<WeakMap<Worker, ThreadWorkerState>, "get">,
): () => boolean {
  const threadGeneration = thread.generation;
  return () => {
    const current = threadWorkerStates.get(worker);
    return current?.generation === threadGeneration
      && current.ownerPid === handle.pid
      && handle.workers.has(worker);
  };
}

export function snapshotProcessState(handle: ProcessHandle): ProcessStateSnapshot | null {
  if (handle.vmPtr === 0) {
    return null;
  }
  const processPtr = handle.kernel.thread_process_ptr(handle.vmPtr);
  const processBytes = handle.kernel.process_state_bytes();
  const offset = handle.guestBase + processPtr;
  if (
    processPtr === 0
    || processBytes <= 0
    || offset < 0
    || offset + processBytes > handle.memory.buffer.byteLength
  ) {
    return null;
  }
  return {
    offset,
    bytes: new Uint8Array(handle.memory.buffer, offset, processBytes).slice(),
  };
}

export function restoreProcessState(
  handle: ProcessHandle,
  snapshot: ProcessStateSnapshot | null,
  options: { preserveCurrentFdTable?: boolean } = {},
): void {
  if (snapshot === null || snapshot.offset + snapshot.bytes.length > handle.memory.buffer.byteLength) {
    return;
  }
  const target = new Uint8Array(handle.memory.buffer, snapshot.offset, snapshot.bytes.length);
  const preservedFdTable = options.preserveCurrentFdTable === true
    ? target.slice(0, Math.min(snapshot.bytes.length, handle.kernel.abi_max_fds() * 8))
    : null;
  target.set(snapshot.bytes);
  if (preservedFdTable !== null) {
    target.set(preservedFdTable, 0);
  }
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

export function snapshotProcessStates(handles: Iterable<ProcessHandle>): HandleProcessStateSnapshot[] {
  const snapshots: HandleProcessStateSnapshot[] = [];
  const seen = new Set<ProcessHandle>();
  for (const handle of handles) {
    if (seen.has(handle)) {
      continue;
    }
    seen.add(handle);
    snapshots.push({
      handle,
      snapshot: snapshotProcessState(handle),
    });
  }
  return snapshots;
}

export function restoreProcessStates(snapshots: readonly HandleProcessStateSnapshot[]): void {
  for (const { handle, snapshot } of snapshots) {
    restoreProcessState(handle, snapshot, { preserveCurrentFdTable: true });
  }
}
