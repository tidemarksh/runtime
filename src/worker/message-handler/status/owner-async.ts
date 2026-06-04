import { recordWorkerMessageDebugEvent, type ProcessHandle, type ThreadWorkerState } from "../../types.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "../types.js";
import { applyStatusFdEntries } from "./fd-entries.js";
import { pruneStatusNetworkConnectionsFromFdEntries } from "./network-recv.js";
import { applyStatusOfdSlots } from "./ofd-slots.js";
import {
  applyThreadGuestMemoryWrites,
  applyThreadKernelMemoryWrites,
} from "./socket-state.js";

export function handleOwnerAsyncStatusReturn(args: {
  handle: ProcessHandle;
  worker: Worker;
  thread: ThreadWorkerState;
  msg: ThreadWorkerStatusMessage;
  statusBelongsToHandle: boolean;
  deps: WorkerMessageHandlerDeps;
}): boolean {
  const {
    handle,
    worker,
    thread,
    msg,
    statusBelongsToHandle,
    deps,
  } = args;
  const ownerAsyncStatus = thread.ownerAsyncStatus ?? null;
  if (ownerAsyncStatus === null) {
    return false;
  }
  if (msg.status !== ownerAsyncStatus) {
    return false;
  }
  if (handle.runningWorker === worker && thread.runningGeneration === msg.generation) {
    handle.runningWorker = null;
    thread.runningGeneration = null;
  }
  if (statusBelongsToHandle) {
    applyThreadKernelMemoryWrites(
      handle,
      msg,
      deps.appendRuntimeLifecycleEvent,
      { skipFsRingWrites: msg.status === handle.kernel.abi_status_fs() },
    );
    applyThreadGuestMemoryWrites(handle, msg, deps.appendRuntimeLifecycleEvent);
    applyStatusOfdSlots(handle, msg);
    applyStatusFdEntries(handle, msg);
    pruneStatusNetworkConnectionsFromFdEntries(handle, msg, deps);
  }
  deps.appendWorkerDebugLog(
    handle,
    `status-owner-async:0x${thread.vmPtr.toString(16)}`
      + `:owner=${thread.ownerAsyncStatus}:status=${msg.status}`
      + `:ownerMatch=${statusBelongsToHandle ? 1 : 0}`,
  );
  recordWorkerMessageDebugEvent(
    handle.workerMessageDebugStats,
    "ownerAsyncStatusReturns",
    `status-owner-async:${handle.pid}:0x${thread.vmPtr.toString(16)}:${msg.status}`,
  );
  return true;
}
