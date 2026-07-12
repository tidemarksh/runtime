import { processHasWorker } from "../process-worker-state.js";
import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import type { ProcessHandle, ThreadWorkerState } from "../types.js";
import {
  getThreadGeneration,
  getThreadOwnerPid,
} from "../thread-host-state.js";

export type ProcessStateSnapshot = {
  processPtr: number;
  offset: number;
  bytes: Uint8Array;
};

const PROCESS_RESERVATION_SLOT_COUNT = 64;
const PROCESS_ATOMIC_U32_BYTES = 4;
const PROCESS_RUNTIME_SYNC_STATE_BYTES = PROCESS_ATOMIC_U32_BYTES
  + (PROCESS_RESERVATION_SLOT_COUNT * PROCESS_ATOMIC_U32_BYTES * 2);
const PROCESS_STABLE_METADATA_BYTES = 9 * 4;

export type HandleProcessStateSnapshot = {
  handle: ProcessHandle;
  snapshot: ProcessStateSnapshot | null;
};

export type ProcessIdentitySnapshot = {
  processPtr: number;
  pid: number;
  ppid: number;
  nextPid: number;
};

export function createWorkerOwnershipGuard(
  handle: ProcessHandle,
  worker: Worker,
  thread: ThreadWorkerState,
  threadWorkerStates: Pick<WeakMap<Worker, ThreadWorkerState>, "get">,
): () => boolean {
  const threadGeneration = getThreadGeneration(thread);
  return () => {
    const current = threadWorkerStates.get(worker);
    return current !== undefined
      && getThreadGeneration(current) === threadGeneration
      && getThreadOwnerPid(current) === handle.pid
      && processHasWorker(handle, worker);
  };
}

export function snapshotProcessIdentity(
  handle: ProcessHandle,
  vmPtr: number,
): ProcessIdentitySnapshot | null {
  if (vmPtr === 0) {
    return null;
  }
  const processPtr = handle.kernel.thread_process_ptr(vmPtr);
  const pid = handle.kernel.process_current_pid(vmPtr);
  const ppid = handle.kernel.process_current_ppid(vmPtr);
  const nextPid = handle.kernel.process_next_pid?.();
  if (
    processPtr === 0
    || pid <= 0
    || (handle.pid > 0 && pid !== handle.pid)
    || ppid < 0
    || nextPid === undefined
    || nextPid <= 0
  ) {
    return null;
  }
  return { processPtr, pid, ppid, nextPid };
}

export function restoreProcessIdentity(
  handle: ProcessHandle,
  vmPtr: number,
  snapshot: ProcessIdentitySnapshot | null,
): void {
  if (
    snapshot === null
    || vmPtr === 0
    || (handle.pid > 0 && snapshot.pid !== handle.pid)
  ) {
    return;
  }
  if (handle.kernel.thread_process_ptr(vmPtr) !== snapshot.processPtr) {
    return;
  }
  if (
    handle.kernel.process_current_pid(vmPtr) === snapshot.pid
    && handle.kernel.process_current_ppid(vmPtr) === snapshot.ppid
  ) {
    return;
  }
  handle.kernel.process_restore_current_identity(
    vmPtr,
    snapshot.pid,
    snapshot.ppid,
    snapshot.nextPid,
  );
}

export function snapshotProcessState(handle: ProcessHandle): ProcessStateSnapshot | null {
  if (handle.vmPtr === 0) {
    return null;
  }
  const pid = handle.kernel.process_current_pid(handle.vmPtr);
  if (handle.pid > 0 && pid !== handle.pid) {
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
    processPtr,
    offset,
    bytes: new Uint8Array(handle.memory.buffer, offset, processBytes).slice(),
  };
}

export function restoreProcessState(
  handle: ProcessHandle,
  snapshot: ProcessStateSnapshot | null,
  options: { preserveCurrentFdTable?: boolean } = {},
): void {
  if (
    snapshot === null
    || handle.vmPtr === 0
    || handle.kernel.thread_process_ptr(handle.vmPtr) !== snapshot.processPtr
    || snapshot.offset + snapshot.bytes.length > handle.memory.buffer.byteLength
  ) {
    return;
  }
  const target = new Uint8Array(handle.memory.buffer, snapshot.offset, snapshot.bytes.length);
  // ProcessState places live thread synchronization between FD state and stable metadata.
  const fdStateBytes = Math.min(snapshot.bytes.length, handle.kernel.abi_max_fds() * 12);
  const preservedFdState = options.preserveCurrentFdTable === true
    ? target.slice(0, fdStateBytes)
    : null;
  const runtimeSyncEnd = Math.min(
    snapshot.bytes.length,
    fdStateBytes + PROCESS_RUNTIME_SYNC_STATE_BYTES,
  );
  const preservedRuntimeSyncState = target.slice(fdStateBytes, runtimeSyncEnd);
  const threadStateOffset = Math.min(
    snapshot.bytes.length,
    runtimeSyncEnd + PROCESS_STABLE_METADATA_BYTES,
  );
  const preservedThreadState = target.slice(threadStateOffset);
  target.set(snapshot.bytes);
  if (preservedFdState !== null) {
    target.set(preservedFdState, 0);
  }
  target.set(preservedRuntimeSyncState, fdStateBytes);
  target.set(preservedThreadState, threadStateOffset);
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
