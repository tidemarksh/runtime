import {
  readKernelRuntimeState,
} from "../../abi.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";

export interface Wait4SchedulerDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
}

export interface Wait4Scheduler {
  formatChildExitRecords(handle: ProcessHandle): string;
  childExitRecordsAlreadyApplied(handle: ProcessHandle, recordsKey: string): boolean;
  markChildExitRecordsApplied(handle: ProcessHandle, recordsKey: string): void;
  applyLocalChildExitRecords(handle: ProcessHandle, options?: { force?: boolean }): void;
}

export function createWait4Scheduler(
  deps: Wait4SchedulerDeps,
  appliedChildExitRecordKeys: Map<string, Set<string>>,
): Wait4Scheduler {
  function formatChildExitRecords(handle: ProcessHandle): string {
    return [...handle.childExitSyncRecords]
      .map(([childPid, waitStatus]) => `${childPid}/${waitStatus}`)
      .join(".");
  }

  function childExitRecordsAlreadyApplied(handle: ProcessHandle, recordsKey: string): boolean {
    return appliedChildExitRecordKeys.get(`${handle.runtimeId}:${handle.pid}`)?.has(recordsKey) ?? false;
  }

  function markChildExitRecordsApplied(handle: ProcessHandle, recordsKey: string): void {
    const processKey = `${handle.runtimeId}:${handle.pid}`;
    let records = appliedChildExitRecordKeys.get(processKey);
    if (records === undefined) {
      records = new Set();
      appliedChildExitRecordKeys.set(processKey, records);
    }
    records.add(recordsKey);
  }

  function applyLocalChildExitRecords(handle: ProcessHandle, options: { force?: boolean } = {}): void {
    if (handle.childExitSyncRecords.size === 0) {
      return;
    }
    const recordsKey = formatChildExitRecords(handle);
    if (!options.force && childExitRecordsAlreadyApplied(handle, recordsKey)) {
      return;
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `wait4-apply-records:${handle.pid}:${recordsKey}`,
    );
    for (const [childPid, waitStatus] of handle.childExitSyncRecords) {
      if (handle.kernel.process_record_child_exit_for_parent && handle.pid > 0) {
        handle.kernel.process_record_child_exit_for_parent(handle.pid, childPid, waitStatus);
      } else {
        handle.kernel.process_record_child_exit(childPid, waitStatus);
      }
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    markChildExitRecordsApplied(handle, recordsKey);
  }

  return {
    formatChildExitRecords,
    childExitRecordsAlreadyApplied,
    markChildExitRecordsApplied,
    applyLocalChildExitRecords,
  };
}
