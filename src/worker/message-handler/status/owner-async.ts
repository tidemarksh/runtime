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
import {
  readSupervisorFaultExitDecision,
  supervisorFaultExitKindProcessExit,
} from "../../supervisor-shadow.js";
import {
  clearThreadOwnerAsyncStatus,
  getThreadOwnerAsyncStatus,
} from "../../thread-host-state.js";
import { getThreadActivityState } from "../../thread-activity-state.js";

function completeFaultedProcessFromStaleOwnerAsyncOk(args: {
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
  if (!statusBelongsToHandle) {
    return false;
  }
  const okStatus = handle.kernel.abi_status_ok?.();
  const faultStatus = handle.kernel.abi_status_fault?.();
  if (
    okStatus === undefined
    || faultStatus === undefined
    || msg.status !== okStatus
    || handle.kernel.thread_status?.(thread.vmPtr) !== faultStatus
  ) {
    return false;
  }

  const supervisorExitDecision = readSupervisorFaultExitDecision(handle, thread.vmPtr);
  if (supervisorExitDecision === undefined) {
    deps.failProcess(
      handle,
      new Error(`supervisor fault exit decision required for vm=0x${thread.vmPtr.toString(16)}`),
    );
    return true;
  }
  if (supervisorExitDecision.kind !== supervisorFaultExitKindProcessExit(handle.kernel)) {
    return false;
  }

  if (handle.runningWorker === worker) {
    handle.runningWorker = null;
  }
  clearThreadOwnerAsyncStatus(thread);
  thread.runningGeneration = null;

  const exitCode = supervisorExitDecision.exitCode;
  const termSignal = supervisorExitDecision.termSignal;
  const exitVmPtr = supervisorExitDecision.exitVmPtr;
  const remainingThreads = supervisorExitDecision.remainingThreads;
  deps.appendWorkerDebugLog(
    handle,
    `status-owner-async-ok-fault-complete:0x${thread.vmPtr.toString(16)}`
      + `:exitVm=0x${exitVmPtr.toString(16)}`
      + `:decision=${supervisorExitDecision.seq.toString()}`
      + `:exit=${exitCode}:${termSignal}:threads=${remainingThreads}`,
  );
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `complete-exit-request:owner-async-ok-fault:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
      + `:exitVm=0x${exitVmPtr.toString(16)}`
      + `:decision=${supervisorExitDecision.seq.toString()}`
      + `:exit=${exitCode}:${termSignal}:threads=${remainingThreads}`,
  );
  void deps.completeProcessExit(handle, exitCode, termSignal, exitVmPtr).catch((error) => {
    deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
  });
  return true;
}

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
  const ownerAsyncStatus = getThreadOwnerAsyncStatus(thread);
  if (ownerAsyncStatus === null) {
    return false;
  }
  if (msg.status !== ownerAsyncStatus) {
    return false;
  }
  const fsStatus = handle.kernel.abi_status_fs?.();
  const generationMatches = msg.generation === undefined
    || thread.runningGeneration === msg.generation;
  const activity = getThreadActivityState(thread);
  if (
    fsStatus !== undefined
    && msg.status === fsStatus
    && generationMatches
    && activity.stepCount > activity.statusCount
  ) {
    clearThreadOwnerAsyncStatus(thread);
    deps.appendWorkerDebugLog(
      handle,
      `status-owner-async-stale-unanswered-fs:0x${thread.vmPtr.toString(16)}:status=${msg.status}`,
    );
    return false;
  }
  const okStatus = handle.kernel.abi_status_ok?.();
  if (completeFaultedProcessFromStaleOwnerAsyncOk(args)) {
    return true;
  }
  if (
    okStatus !== undefined
    && msg.status === okStatus
    && generationMatches
    && activity.stepCount > activity.statusCount
  ) {
    clearThreadOwnerAsyncStatus(thread);
    deps.appendWorkerDebugLog(
      handle,
      `status-owner-async-stale-unanswered-ok:0x${thread.vmPtr.toString(16)}:status=${msg.status}`,
    );
    return false;
  }
  const sleepStatus = handle.kernel.abi_status_sleep?.();
  if (sleepStatus !== undefined && msg.status === sleepStatus) {
    clearThreadOwnerAsyncStatus(thread);
    deps.appendWorkerDebugLog(
      handle,
      `status-owner-async-stale-sleep:0x${thread.vmPtr.toString(16)}:status=${msg.status}`,
    );
    return false;
  }
  if (handle.runningWorker === worker && thread.runningGeneration === msg.generation) {
    handle.runningWorker = null;
    thread.runningGeneration = null;
  }
  const blocked = handle.hostBlockedWorkers.get(worker);
  if (
    statusBelongsToHandle
    && blocked
    && blocked.vmPtr === thread.vmPtr
    && blocked.rpcPending
    && blocked.status !== msg.status
  ) {
    deps.clearBlockedWorker(handle, worker);
    deps.appendWorkerDebugLog(
      handle,
      `status-owner-async-clear-stale-blocked-rpc:0x${thread.vmPtr.toString(16)}`
        + `:blocked=${blocked.status}:status=${msg.status}`,
    );
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
      + `:owner=${ownerAsyncStatus}:status=${msg.status}`
      + `:ownerMatch=${statusBelongsToHandle ? 1 : 0}`,
  );
  recordWorkerMessageDebugEvent(
    handle.workerMessageDebugStats,
    "ownerAsyncStatusReturns",
    `status-owner-async:${handle.pid}:0x${thread.vmPtr.toString(16)}:${msg.status}`,
  );
  if (
    statusBelongsToHandle
    && fsStatus !== undefined
    && msg.status === fsStatus
  ) {
    deps.pumpProcess(handle);
  }
  if (okStatus !== undefined && msg.status === okStatus) {
    deps.pumpProcess(handle);
  }
  return true;
}
