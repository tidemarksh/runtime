import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import { adoptKernelStateForHandle } from "./kernel-state.js";
import { describeSyncCause, type SyncCause } from "./effects.js";
import type { ProcessHandle } from "../types.js";

const FORK_SHARED_MEMFS_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_PUBLICATION_DEBUG ?? "") === "1";

type ProcessStateSnapshot = {
  offset: number;
  bytes: Uint8Array;
};

export function appendForkSharedMemfsDebug(handle: ProcessHandle, event: string): void {
  if (!FORK_SHARED_MEMFS_DEBUG_ENABLED) {
    return;
  }
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
}

function snapshotProcessState(handle: ProcessHandle): ProcessStateSnapshot | null {
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

function restoreProcessState(handle: ProcessHandle, snapshot: ProcessStateSnapshot | null): void {
  if (snapshot === null) {
    return;
  }
  if (snapshot.offset + snapshot.bytes.length > handle.memory.buffer.byteLength) {
    return;
  }
  new Uint8Array(handle.memory.buffer, snapshot.offset, snapshot.bytes.length).set(snapshot.bytes);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

export async function syncProcessMemfsViaKernelWorker(
  handle: ProcessHandle,
  cause: SyncCause,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }
  const processStateSnapshot = snapshotProcessState(handle);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  appendForkSharedMemfsDebug(handle, `process-memfs-sync:${handle.pid}:cause=${describeSyncCause(cause)}`);

  const response = await handle.kernelWorkerRequest({
    type: "sync-process-memfs",
    pid: handle.pid,
    kernelState: handle.kernelState,
  });
  if (response.type !== "process-memfs-synced") {
    throw new Error(`unexpected kernel-worker response for sync-process-memfs: ${response.type}`);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  restoreProcessState(handle, processStateSnapshot);
  handle.processMemfsDirty = false;
}

export function syncProcessMemfsForWorkerRuntime(handle: ProcessHandle): Promise<void> {
  return syncProcessMemfsViaKernelWorker(handle, {
    kind: "syscall",
    nr: -1n,
    name: "process-memfs-sync",
  });
}
