import { createWorkerControlMessageHandler } from "./message-handler/control.js";
import { readProcessPidField } from "./diagnostics/format.js";
import { createWorkerStatusMessageHandler } from "./message-handler/status/route.js";
import {
  supervisorProcessOwnerAbsent,
  supervisorProcessOwnerLiveMatches,
  supervisorProcessOwnerMatches,
  supervisorProcessOwnerMissingOrInvalid,
} from "./process-identity.js";
import { supervisorProcessStatusAllowsPidMismatch } from "./supervisor-status.js";
import type { WorkerMessageHandlerDeps } from "./message-handler/types.js";
import { recordWorkerMessageDebugEvent, type ProcessHandle } from "./types.js";
import type { ThreadWorkerOutbound } from "../messages.js";
import {
  getThreadGeneration,
  getThreadOwnerPid,
  setThreadOwnerPid,
} from "./thread-host-state.js";

export function createWorkerMessageHandler(deps: WorkerMessageHandlerDeps) {
  const handleWorkerControlMessage = createWorkerControlMessageHandler(deps);
  const handleWorkerStatusMessage = createWorkerStatusMessageHandler(deps);

  function failOwnershipInvariant(
    handle: ProcessHandle,
    worker: Worker,
    msg: ThreadWorkerOutbound,
    detail: string,
  ): void {
    recordWorkerMessageDebugEvent(
      handle.workerMessageDebugStats,
      "ownershipInvariantDrops",
      `ownership-invariant:${handle.pid}:${msg.type}:${detail}`,
    );
    const thread = deps.threadWorkerStates.get(worker);
    const vmPtr = thread?.vmPtr ?? 0;
    const workerLogTail = handle.workerDebugLog.slice(-12).join(", ");
    const plainForkLog = handle.workerDebugLog
      .filter((entry) => entry.includes("plain-fork"))
      .slice(-12)
      .join(", ");
    const processPtr = vmPtr !== 0 ? handle.kernel.thread_process_ptr(vmPtr) : 0;
    const processPidField = vmPtr !== 0 ? handle.kernel.process_current_pid(vmPtr) : 0;
    const processPpidField = vmPtr !== 0 ? handle.kernel.process_current_ppid(vmPtr) : 0;
    const processThreadCount = vmPtr !== 0 ? handle.kernel.thread_process_thread_count(vmPtr) : 0;
    deps.failProcess(
      handle,
      new Error(
        `thread worker ownership invariant failed pid=${handle.pid}`
          + ` msg=${msg.type}${msg.type === "status" ? `@${msg.status}` : ""}`
          + ` vm=0x${vmPtr.toString(16)} ${detail}`
          + ` process_ptr=0x${processPtr.toString(16)}`
          + ` process_pid_field=${processPidField}`
          + ` process_ppid_field=${processPpidField}`
          + ` process_thread_count=${processThreadCount}`
          + ` last_debug=${JSON.stringify(handle.lastDebugMessage ?? "n/a")}`
          + ` worker_log_tail=[${workerLogTail}]`
          + ` plain_fork_log=[${plainForkLog}]`,
      ),
    );
  }

  function assertWorkerOwnershipInvariant(
    handle: ProcessHandle,
    worker: Worker,
    msg: ThreadWorkerOutbound,
  ): boolean {
    const thread = deps.threadWorkerStates.get(worker);
    if (!thread) {
      return true;
    }
    const threadGeneration = getThreadGeneration(thread);
    if ("generation" in msg && msg.generation !== threadGeneration) {
      recordWorkerMessageDebugEvent(
        handle.workerMessageDebugStats,
        "staleGenerationDrops",
        `stale-generation:${handle.pid}:${msg.type}:${msg.generation}->${threadGeneration}`,
      );
      deps.appendWorkerDebugLog(
        handle,
        `stale-message:0x${thread.vmPtr.toString(16)}:${msg.type}:${msg.generation}->${threadGeneration}`,
      );
      return false;
    }
    const ownerPid = getThreadOwnerPid(thread);
    if (ownerPid !== handle.pid) {
      const readyVmPtr = msg.type === "ready" && "vmPtr" in msg ? msg.vmPtr : 0;
      if (readyVmPtr === thread.vmPtr && handle.pid > 0) {
        if (supervisorProcessOwnerLiveMatches(handle.kernel, thread.vmPtr, handle.pid)) {
          const currentPid = handle.kernel.process_current_pid(thread.vmPtr);
          const processThreadCount = handle.kernel.thread_process_thread_count(thread.vmPtr);
          const previousOwnerPid = ownerPid;
          setThreadOwnerPid(thread, handle.pid);
          recordWorkerMessageDebugEvent(
            handle.workerMessageDebugStats,
            "readyRehydrateAdopts",
            `ready-owner-adopt:${handle.pid}:0x${thread.vmPtr.toString(16)}`
              + `:previous=${previousOwnerPid}:current=${currentPid}:threads=${processThreadCount}`,
          );
          deps.appendWorkerDebugLog(
            handle,
            `ready-owner-adopt:0x${thread.vmPtr.toString(16)}`
              + `:pid=${handle.pid}:previous=${previousOwnerPid}`,
          );
        } else {
          failOwnershipInvariant(
            handle,
            worker,
            msg,
            `owner_pid=${ownerPid} handle_pid=${handle.pid}`,
          );
          return false;
        }
      } else {
        failOwnershipInvariant(
          handle,
          worker,
          msg,
          `owner_pid=${ownerPid} handle_pid=${handle.pid}`,
        );
        return false;
      }
    }
    if ("vmPtr" in msg && msg.vmPtr !== thread.vmPtr) {
      failOwnershipInvariant(
        handle,
        worker,
        msg,
        `worker_vm_ptr=${msg.vmPtr.toString(16)} expected_vm_ptr=${thread.vmPtr.toString(16)}`,
      );
      return false;
    }
    const isActiveVforkWorker = handle.vforkSnapshot !== null && handle.vforkWorker === worker;
    const messageAllowsTransientKernelPidMismatch = isActiveVforkWorker || (
      msg.type === "status"
      && supervisorProcessStatusAllowsPidMismatch(handle.kernel, msg.status)
    );
    if (messageAllowsTransientKernelPidMismatch) {
      return true;
    }
    if (handle.pid > 0) {
      const messageProcessPidField = "debug" in msg
        ? readProcessPidField(msg.debug)
        : null;
      if (
        msg.type === "status"
        && msg.status === handle.kernel.abi_status_fault()
        && (
          supervisorProcessOwnerMissingOrInvalid(handle.kernel, thread.vmPtr)
          || messageProcessPidField === handle.pid
        )
      ) {
        return true;
      }
      const processOwnerAbsent = supervisorProcessOwnerAbsent(handle.kernel, thread.vmPtr);
      if (
        msg.type === "ready"
        && processOwnerAbsent
        && messageProcessPidField === handle.pid
      ) {
        const currentPid = handle.kernel.process_current_pid(thread.vmPtr);
        const processThreadCount = handle.kernel.thread_process_thread_count(thread.vmPtr);
        recordWorkerMessageDebugEvent(
          handle.workerMessageDebugStats,
          "readyRehydrateAdopts",
          `ready-rehydrate:${handle.pid}:0x${thread.vmPtr.toString(16)}`
            + `:field=${messageProcessPidField}:current=${currentPid}:threads=${processThreadCount}`,
        );
        deps.appendWorkerDebugLog(
          handle,
          `ready-rehydrate:0x${thread.vmPtr.toString(16)}:pid=${handle.pid}`,
        );
        return true;
      }
      if (msg.type === "status" && processOwnerAbsent) {
        const currentPid = handle.kernel.process_current_pid(thread.vmPtr);
        const processThreadCount = handle.kernel.thread_process_thread_count(thread.vmPtr);
        recordWorkerMessageDebugEvent(
          handle.workerMessageDebugStats,
          "staleProcessStatusDrops",
          `stale-process-status:${handle.pid}:0x${thread.vmPtr.toString(16)}:${msg.status}`
            + `:field=${messageProcessPidField ?? "n/a"}`
            + `:current=${currentPid}:threads=${processThreadCount}`,
        );
        deps.appendWorkerDebugLog(
          handle,
          `stale-process-status:0x${thread.vmPtr.toString(16)}:${msg.status}:pid=${handle.pid}`,
        );
        return false;
      }
      if (!supervisorProcessOwnerMatches(handle.kernel, thread.vmPtr, handle.pid)) {
        const currentPid = handle.kernel.process_current_pid(thread.vmPtr);
        failOwnershipInvariant(handle, worker, msg, `kernel_pid=${currentPid} expected_pid=${handle.pid}`);
        return false;
      }
    }
    return true;
  }

  return function handleWorkerMessage(
    handle: ProcessHandle,
    worker: Worker,
    msg: ThreadWorkerOutbound,
  ): void {
    recordWorkerMessageDebugEvent(handle.workerMessageDebugStats, "received");
    if (msg.type === "status") {
      recordWorkerMessageDebugEvent(handle.workerMessageDebugStats, "statusReceived");
    } else {
      recordWorkerMessageDebugEvent(handle.workerMessageDebugStats, "controlReceived");
    }
    const messageProcessPidField = "debug" in msg ? readProcessPidField(msg.debug) : null;
    const thread = deps.threadWorkerStates.get(worker);
    const isActiveVforkWorker = thread !== undefined
      && handle.vforkSnapshot !== null
      && handle.vforkWorker === worker;
    const workerOwnerMatchesHandle = thread !== undefined && getThreadOwnerPid(thread) === handle.pid;
    if (
      !isActiveVforkWorker
      && !workerOwnerMatchesHandle
      && handle.pid > 0
      && messageProcessPidField !== null
      && messageProcessPidField > 0
      && messageProcessPidField !== handle.pid
    ) {
      const rerouteHandle = deps.processHandles.get(messageProcessPidField);
      if (rerouteHandle && rerouteHandle !== handle) {
        recordWorkerMessageDebugEvent(
          handle.workerMessageDebugStats,
          "rerouted",
          `reroute:${handle.pid}->${messageProcessPidField}:${msg.type}`,
        );
        deps.appendWorkerDebugLog(
          handle,
          `reroute-message:pid=${handle.pid}->${messageProcessPidField}:type=${msg.type}`,
        );
        handleWorkerMessage(rerouteHandle, worker, msg);
        return;
      }
    }
    if (
      !isActiveVforkWorker
      && handle.pid > 0
      && thread !== undefined
      && getThreadOwnerPid(thread) > 0
      && getThreadOwnerPid(thread) !== handle.pid
    ) {
      const ownerPid = getThreadOwnerPid(thread);
      const ownerHandle = deps.processHandles.get(ownerPid);
      if (ownerHandle && ownerHandle !== handle) {
        recordWorkerMessageDebugEvent(
          handle.workerMessageDebugStats,
          "rerouted",
          `reroute-owner:${handle.pid}->${ownerPid}:${msg.type}`,
        );
        deps.appendWorkerDebugLog(
          handle,
          `reroute-owner-message:pid=${handle.pid}->${ownerPid}:type=${msg.type}`,
        );
        handleWorkerMessage(ownerHandle, worker, msg);
        return;
      }
    }
    if (handle.pid > 0 && deps.processHandles.get(handle.pid) !== handle) {
      recordWorkerMessageDebugEvent(
        handle.workerMessageDebugStats,
        "inactiveHandleDrops",
        `inactive-handle:${handle.pid}:${msg.type}`,
      );
      return;
    }
    if (handle.exitInProgress) {
      recordWorkerMessageDebugEvent(
        handle.workerMessageDebugStats,
        "exitInProgressDrops",
        `exit-in-progress:${handle.pid}:${msg.type}`,
      );
      return;
    }
    if (!assertWorkerOwnershipInvariant(handle, worker, msg)) {
      return;
    }

    if (handleWorkerControlMessage(handle, worker, msg)) {
      return;
    }
    handleWorkerStatusMessage(handle, worker, msg as Extract<ThreadWorkerOutbound, { type: "status" }>);
  };
}
