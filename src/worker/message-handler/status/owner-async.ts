import { applyThreadFdEntries } from "../../ownership/fd-table.js";
import { applyThreadOfdSlots } from "../../ownership/ofd-slot.js";
import { pruneStaleNetworkConnectionsFromFdEntries } from "../../ownership/socket-control.js";
import { recordWorkerMessageDebugEvent, type ProcessHandle, type ThreadWorkerState } from "../../types.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "../types.js";
import { appendStatusSocketDebug } from "./debug.js";
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
  if ((thread.ownerAsyncStatus ?? null) === null) {
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
    applyThreadOfdSlots(handle, msg, appendStatusSocketDebug);
    applyThreadFdEntries(handle, msg.vmPtr, msg.fdEntries, appendStatusSocketDebug);
    pruneStaleNetworkConnectionsFromFdEntries(handle, msg.vmPtr, msg.fdEntries, {
      appendStatusSocketDebug,
      appendWorkerDebugLog: deps.appendWorkerDebugLog,
      failProcess: deps.failProcess,
    });
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
