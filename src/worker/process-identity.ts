import { collectLiveThreadVmPtrs } from "./live-threads.js";
import type { ProcessHandle, ThreadWorkerState } from "./types.js";

interface ProcessIdentityAssertionOptions {
  status?: number;
  threadVmPtr?: number;
  workerLogTail?: string[];
  skipVmPtrs?: Iterable<number>;
}

export function supervisorVforkChildOwnerMismatch(
  kernel: ProcessHandle["kernel"],
  vmPtr: number,
  runtimePid: number,
): boolean {
  if (vmPtr === 0 || runtimePid <= 0) {
    return false;
  }
  const readMismatch = kernel.supervisor_vfork_child_owner_mismatch;
  if (typeof readMismatch !== "function") {
    throw new Error("kernel supervisor ABI missing supervisor_vfork_child_owner_mismatch");
  }
  const result = readMismatch(vmPtr, runtimePid);
  if (result < 0) {
    throw new Error(`kernel supervisor vfork child owner mismatch failed: ${result}`);
  }
  return result !== 0;
}

export function supervisorProcessOwnerMatches(
  kernel: ProcessHandle["kernel"],
  vmPtr: number,
  runtimePid: number,
): boolean {
  if (vmPtr === 0 || runtimePid <= 0) {
    return false;
  }
  const readMatch = kernel.supervisor_process_owner_matches;
  if (typeof readMatch !== "function") {
    throw new Error("kernel supervisor ABI missing supervisor_process_owner_matches");
  }
  const result = readMatch(vmPtr, runtimePid);
  if (result < 0) {
    throw new Error(`kernel supervisor process owner match failed: ${result}`);
  }
  return result !== 0;
}

export function supervisorProcessOwnerMismatch(
  kernel: ProcessHandle["kernel"],
  vmPtr: number,
  runtimePid: number,
): boolean {
  if (vmPtr === 0 || runtimePid <= 0) {
    return false;
  }
  const readMismatch = kernel.supervisor_process_owner_mismatch;
  if (typeof readMismatch !== "function") {
    throw new Error("kernel supervisor ABI missing supervisor_process_owner_mismatch");
  }
  const result = readMismatch(vmPtr, runtimePid);
  if (result < 0) {
    throw new Error(`kernel supervisor process owner mismatch failed: ${result}`);
  }
  return result !== 0;
}

export function supervisorProcessOwnerAbsent(
  kernel: ProcessHandle["kernel"],
  vmPtr: number,
): boolean {
  if (vmPtr === 0) {
    return false;
  }
  const readAbsent = kernel.supervisor_process_owner_absent;
  if (typeof readAbsent !== "function") {
    throw new Error("kernel supervisor ABI missing supervisor_process_owner_absent");
  }
  const result = readAbsent(vmPtr);
  if (result < 0) {
    throw new Error(`kernel supervisor process owner absent check failed: ${result}`);
  }
  return result !== 0;
}

export function supervisorProcessOwnerLiveMatches(
  kernel: ProcessHandle["kernel"],
  vmPtr: number,
  runtimePid: number,
): boolean {
  if (vmPtr === 0 || runtimePid <= 0) {
    return false;
  }
  const readMatch = kernel.supervisor_process_owner_live_matches;
  if (typeof readMatch !== "function") {
    throw new Error("kernel supervisor ABI missing supervisor_process_owner_live_matches");
  }
  const result = readMatch(vmPtr, runtimePid);
  if (result < 0) {
    throw new Error(`kernel supervisor live process owner match failed: ${result}`);
  }
  return result !== 0;
}

export function supervisorProcessOwnerMissingOrInvalid(
  kernel: ProcessHandle["kernel"],
  vmPtr: number,
): boolean {
  const readInvalid = kernel.supervisor_process_owner_missing_or_invalid;
  if (typeof readInvalid !== "function") {
    throw new Error("kernel supervisor ABI missing supervisor_process_owner_missing_or_invalid");
  }
  const result = readInvalid(vmPtr);
  if (result < 0) {
    throw new Error(`kernel supervisor process owner validity check failed: ${result}`);
  }
  return result !== 0;
}

export function supervisorProcessIdentityMatches(
  kernel: ProcessHandle["kernel"],
  vmPtr: number,
  runtimePid: number,
  processPtr: number,
): boolean {
  if (vmPtr === 0 || runtimePid <= 0 || processPtr === 0) {
    return false;
  }
  const readMatch = kernel.supervisor_process_identity_matches;
  if (typeof readMatch !== "function") {
    throw new Error("kernel supervisor ABI missing supervisor_process_identity_matches");
  }
  const result = readMatch(vmPtr, runtimePid, processPtr);
  if (result < 0) {
    throw new Error(`kernel supervisor process identity match failed: ${result}`);
  }
  return result !== 0;
}

export function assertHandleWorkerProcessIdentity(
  handle: ProcessHandle,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
  context: string,
  options: ProcessIdentityAssertionOptions = {},
): void {
  if (handle.pid <= 0 || handle.pendingChildSpawnRecords > 0) {
    return;
  }

  const skipVmPtrs = new Set(options.skipVmPtrs ?? []);
  const ownedWorkerVmPtrs = new Set<number>();
  const liveVmPtrs = new Map<string, number>();
  let anchorVmPtr: number | null = null;
  let workerIndex = 0;

  for (const worker of handle.workers) {
    const threadState = threadWorkerStates.get(worker);
    if (!threadState || threadState.vmPtr === 0) {
      continue;
    }
    ownedWorkerVmPtrs.add(threadState.vmPtr);
    if (
      anchorVmPtr === null
      && supervisorProcessOwnerMatches(handle.kernel, threadState.vmPtr, handle.pid)
    ) {
      anchorVmPtr = threadState.vmPtr;
    }
    liveVmPtrs.set(`worker${workerIndex}`, threadState.vmPtr);
    workerIndex += 1;
  }

  if (handle.vmPtr !== 0) {
    const includeHandleVmPtr = ownedWorkerVmPtrs.has(handle.vmPtr)
      || (ownedWorkerVmPtrs.size === 0 && handle.pendingChildSpawnRecords === 0);
    if (includeHandleVmPtr) {
      if (
        anchorVmPtr === null
        && supervisorProcessOwnerMatches(handle.kernel, handle.vmPtr, handle.pid)
      ) {
        anchorVmPtr = handle.vmPtr;
      }
      liveVmPtrs.set("handle", handle.vmPtr);
    }
  }

  const liveThreadVmPtrs = anchorVmPtr === null
    ? null
    : collectLiveThreadVmPtrs(handle.kernel, anchorVmPtr);
  if (liveThreadVmPtrs === null) {
    return;
  }

  for (const [label, vmPtr] of liveVmPtrs) {
    if (skipVmPtrs.has(vmPtr)) {
      continue;
    }
    if (!liveThreadVmPtrs.has(vmPtr)) {
      continue;
    }
    const processPtr = handle.kernel.thread_process_ptr(vmPtr);
    if (processPtr === 0) {
      continue;
    }
    if (supervisorProcessOwnerMatches(handle.kernel, vmPtr, handle.pid)) {
      continue;
    }
    const actualPid = handle.kernel.process_current_pid(vmPtr);
    const workerLogTail = options.workerLogTail ?? [];
    throw new Error(
      `${context} process identity drift expected_pid=${handle.pid} actual_pid=${actualPid}`
        + (options.status !== undefined ? ` status=${options.status}` : "")
        + (options.threadVmPtr !== undefined ? ` thread_vm=0x${options.threadVmPtr.toString(16)}` : "")
        + ` label=${label}`
        + ` vm=0x${vmPtr.toString(16)}`
        + ` process_ptr=0x${processPtr.toString(16)}`
        + ` last_debug=${JSON.stringify(handle.lastDebugMessage ?? "n/a")}`
        + (workerLogTail.length > 0 ? ` worker_log_tail=[${workerLogTail.join(", ")}]` : ""),
    );
  }
}
