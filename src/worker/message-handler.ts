import { createWorkerControlMessageHandler } from "./message-handler/control.js";
import { readProcessPidField } from "./message-handler/debug-summary.js";
import { createWorkerStatusMessageHandler } from "./message-handler/status.js";
import type { WorkerMessageHandlerDeps } from "./message-handler/types.js";
import { recordWorkerMessageDebugEvent, type ProcessHandle } from "./types.js";
import type { ThreadWorkerOutbound } from "../messages.js";

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
    if ("generation" in msg && msg.generation !== thread.generation) {
      recordWorkerMessageDebugEvent(
        handle.workerMessageDebugStats,
        "staleGenerationDrops",
        `stale-generation:${handle.pid}:${msg.type}:${msg.generation}->${thread.generation}`,
      );
      deps.appendWorkerDebugLog(
        handle,
        `stale-message:0x${thread.vmPtr.toString(16)}:${msg.type}:${msg.generation}->${thread.generation}`,
      );
      return false;
    }
    if (thread.ownerPid !== handle.pid) {
      failOwnershipInvariant(
        handle,
        worker,
        msg,
        `owner_pid=${thread.ownerPid} handle_pid=${handle.pid}`,
      );
      return false;
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
      && (
        msg.status === handle.kernel.abi_status_vfork_wait()
        || msg.status === handle.kernel.abi_status_execve_spawn()
      )
    );
    if (messageAllowsTransientKernelPidMismatch) {
      return true;
    }
    if (handle.pid > 0) {
      const currentPid = handle.kernel.process_current_pid(thread.vmPtr);
      const messageProcessPidField = msg.type === "status" && "debug" in msg
        ? readProcessPidField(msg.debug)
        : null;
      if (
        msg.type === "status"
        && msg.status === handle.kernel.abi_status_fault()
        && (currentPid === 0 || currentPid > 0xffff || messageProcessPidField === handle.pid)
      ) {
        return true;
      }
      const processThreadCount = handle.kernel.thread_process_thread_count(thread.vmPtr);
      if (msg.type === "status" && currentPid === 0 && processThreadCount === 0) {
        recordWorkerMessageDebugEvent(
          handle.workerMessageDebugStats,
          "staleProcessStatusDrops",
          `stale-process-status:${handle.pid}:0x${thread.vmPtr.toString(16)}:${msg.status}`,
        );
        deps.appendWorkerDebugLog(
          handle,
          `stale-process-status:0x${thread.vmPtr.toString(16)}:${msg.status}:pid=${handle.pid}`,
        );
        return false;
      }
      if (currentPid !== handle.pid) {
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
    if (
      handle.pid > 0
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
