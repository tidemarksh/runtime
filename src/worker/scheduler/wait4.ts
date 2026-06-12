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

function formatProcessThreadSlots(handle: ProcessHandle): string {
  const slots: string[] = [];
  for (let slot = 0; slot < handle.kernel.abi_max_threads(); slot += 1) {
    const tid = handle.kernel.process_thread_tid(handle.vmPtr, slot);
    const state = handle.kernel.process_thread_state(handle.vmPtr, slot);
    const threadVmPtr = handle.kernel.process_thread_vm_ptr(handle.vmPtr, slot);
    if (tid === 0 && state === 0 && threadVmPtr === 0) {
      continue;
    }
    slots.push(`${slot}:${tid}/${state}/0x${threadVmPtr.toString(16)}`);
  }
  return slots.join(",");
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
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `wait4-apply-records-before:${handle.pid}:${recordsKey}`
        + `:count=${handle.kernel.thread_process_thread_count(handle.vmPtr)}`
        + `:threads=[${formatProcessThreadSlots(handle)}]`,
    );
    for (const [childPid, waitStatus] of handle.childExitSyncRecords) {
      if (handle.kernel.process_record_child_exit_for_parent && handle.pid > 0) {
        handle.kernel.process_record_child_exit_for_parent(handle.pid, childPid, waitStatus);
      } else {
        handle.kernel.process_record_child_exit(childPid, waitStatus);
      }
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `wait4-apply-records-after:${handle.pid}:${recordsKey}`
        + `:count=${handle.kernel.thread_process_thread_count(handle.vmPtr)}`
        + `:threads=[${formatProcessThreadSlots(handle)}]`,
    );
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
