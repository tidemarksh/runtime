import { attachKernelRuntimeState, readKernelRuntimeState } from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { collectHeldEpollOfdIds } from "../ownership/ofd-table.js";
import { kernelWorkerOwnsEpollState } from "../ownership/epoll-control.js";
import { kernelWorkerOwnsPipeState } from "../ownership/pipe-control.js";
import {
  drainProcessOutput,
  flushStdin,
} from "../stdio.js";
import { adoptKernelStateForHandle } from "../sync.js";
import type { ProcessHandle } from "../types.js";
import type { ThreadWorkerOutbound } from "../../messages.js";
import type { WorkerMessageHandlerDeps } from "./types.js";
import { readProcessPidField } from "../diagnostics/format.js";
import { createWorkerOwnershipGuard } from "../ownership/process-state.js";
import {
  supervisorProcessIdentityMatches,
  supervisorProcessOwnerLiveMatches,
} from "../process-identity.js";
import { submitSupervisorShadowStatusEvent } from "../supervisor-shadow.js";
import { supervisorProcessStatusIsCloneSpawn } from "../supervisor-status.js";
import {
  clearThreadOwnerAsyncStatus,
  clearThreadWaitingInWorkerStatus,
  getThreadOwnerPid,
  getThreadReinitCount,
  getThreadOwnerAsyncStatus,
  setThreadInitialized,
  takeThreadResumeWorkersOnReady,
  threadResumeWorkersOnReadyCount,
} from "../thread-host-state.js";
import { getThreadActivityState } from "../thread-activity-state.js";

function formatDebugError(error: unknown): string {
  return error instanceof Error ? (error.stack ?? error.message) : String(error);
}

function safeThreadErrorDebug(
  handle: ProcessHandle,
  deps: WorkerMessageHandlerDeps,
  vmPtr: number,
): {
  threadCount: string;
  liveThreads: string[];
  threadSlots: string[];
} {
  try {
    const threadCount = handle.kernel.thread_process_thread_count(vmPtr);
    const liveThreads: string[] = [];
    for (let slot = 0; slot < threadCount; slot += 1) {
      const threadVmPtr = handle.kernel.process_thread_vm_ptr(vmPtr, slot);
      if (threadVmPtr === 0) {
        continue;
      }
      liveThreads.push(`0x${threadVmPtr.toString(16)}`);
    }
    const threadSlots = Array.from({ length: Math.min(handle.kernel.abi_max_threads(), 8) }, (_value, slot) => {
      const candidateVmPtr = handle.kernel.process_thread_vm_ptr(vmPtr, slot);
      return candidateVmPtr === 0 ? `${slot}=0x0` : `${slot}=0x${candidateVmPtr.toString(16)}`;
    });
    return {
      threadCount: String(threadCount),
      liveThreads,
      threadSlots,
    };
  } catch (error) {
    const detail = formatDebugError(error);
    return {
      threadCount: `unavailable:${detail}`,
      liveThreads: [],
      threadSlots: [`debug-error=${JSON.stringify(detail)}`],
    };
  }
}

export function createWorkerControlMessageHandler(deps: WorkerMessageHandlerDeps) {
  return function handleWorkerControlMessage(
    handle: ProcessHandle,
    worker: Worker,
    msg: ThreadWorkerOutbound,
  ): boolean {
    if (msg.type === "ready") {
      const thread = deps.threadWorkerStates.get(worker);
      if (!thread) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `clone-child-ready-no-thread:${handle.pid}:vm=0x${msg.vmPtr.toString(16)}`
            + `:generation=${msg.generation}`,
        );
        return true;
      }
      const workerStillOwnedByHandle = createWorkerOwnershipGuard(
        handle,
        worker,
        thread,
        deps.threadWorkerStates,
      );
      deps.appendWorkerDebugLog(
        handle,
        `ready-route:0x${thread.vmPtr.toString(16)}`
          + `:owner_pid=${getThreadOwnerPid(thread)}`
          + `:handle_pid=${handle.pid}`
          + `:generation=${msg.generation}`,
      );
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `clone-child-ready-route:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
          + `:owner=${getThreadOwnerPid(thread)}:generation=${msg.generation}`
          + `:resume=${threadResumeWorkersOnReadyCount(thread)}`,
      );
      const readyProcessPidField = readProcessPidField(msg.debug);
      if (readyProcessPidField !== null && readyProcessPidField !== handle.pid) {
        if (
          getThreadOwnerPid(thread) === handle.pid
          && supervisorProcessOwnerLiveMatches(handle.kernel, thread.vmPtr, handle.pid)
        ) {
          const processThreadCount = handle.kernel.thread_process_thread_count(thread.vmPtr);
          deps.appendWorkerDebugLog(
            handle,
            `ready-pid-mismatch-stale-debug:0x${thread.vmPtr.toString(16)}`
              + `:expected_pid=${handle.pid}`
              + `:actual_pid=${readyProcessPidField}`
              + `:threads=${processThreadCount}`,
          );
        } else {
          deps.appendWorkerDebugLog(
            handle,
            `ready-pid-mismatch:0x${thread.vmPtr.toString(16)}`
              + `:expected_pid=${handle.pid}`
              + `:actual_pid=${readyProcessPidField}`
              + `:reinit_count=${getThreadReinitCount(thread)}`,
          );
          if (getThreadReinitCount(thread) < 1) {
            deps.attachThreadWorker(handle, {
              worker,
              vmPtr: thread.vmPtr,
              forcePrepare: true,
            });
            return true;
          }
          deps.failProcess(
            handle,
            new Error(
              `ready pid mismatch persisted vm=0x${thread.vmPtr.toString(16)}`
                + ` expected_pid=${handle.pid} actual_pid=${readyProcessPidField}`
                + (msg.debug ? ` ready_debug=${JSON.stringify(msg.debug)}` : ""),
            ),
          );
          return true;
        }
      }
      const vforkSnapshot = handle.vforkSnapshot;
      if (
        vforkSnapshot
        && handle.vforkWorker === null
        && thread.vmPtr === vforkSnapshot.vmPtr
      ) {
        const currentPid = handle.kernel.process_current_pid(thread.vmPtr);
        const currentProcessPtr = handle.kernel.thread_process_ptr(thread.vmPtr);
        deps.appendWorkerDebugLog(
          handle,
          `vfork-parent-ready-check:0x${thread.vmPtr.toString(16)}`
            + `:expected_pid=${vforkSnapshot.parentPid}`
            + `:actual_pid=${currentPid}`
            + `:expected_process_ptr=0x${vforkSnapshot.parentProcessPtr.toString(16)}`
            + `:actual_process_ptr=0x${currentProcessPtr.toString(16)}`
            + (msg.debug ? `:${msg.debug}` : ""),
        );
        if (!supervisorProcessIdentityMatches(
          handle.kernel,
          thread.vmPtr,
          vforkSnapshot.parentPid,
          vforkSnapshot.parentProcessPtr,
        )) {
          deps.failProcess(
            handle,
            new Error(
              `vfork parent replacement worker drift vm=0x${thread.vmPtr.toString(16)}`
                + ` expected_pid=${vforkSnapshot.parentPid} actual_pid=${currentPid}`
                + ` expected_process_ptr=0x${vforkSnapshot.parentProcessPtr.toString(16)}`
                + ` actual_process_ptr=0x${currentProcessPtr.toString(16)}`
                + (msg.debug ? ` ready_debug=${JSON.stringify(msg.debug)}` : ""),
            ),
          );
          return true;
        }
        deps.clearVforkSnapshot(handle);
      }
      const resumeWorkersOnReady = takeThreadResumeWorkersOnReady(thread);
      const releaseCloneSpawnResumeWorkers = (): void => {
        for (const resumeWorker of resumeWorkersOnReady) {
          const resumeThread = deps.threadWorkerStates.get(resumeWorker);
          const ownerAsyncStatus = getThreadOwnerAsyncStatus(resumeThread);
          if (
            resumeThread !== undefined
            && ownerAsyncStatus !== null
            && supervisorProcessStatusIsCloneSpawn(handle.kernel, ownerAsyncStatus)
          ) {
            clearThreadOwnerAsyncStatus(resumeThread);
          }
        }
      };
      const readyKernelState = thread.vmPtr === handle.vmPtr
        ? msg.kernelState
        : readKernelRuntimeState(handle.kernel);
      handle.kernelState = adoptKernelStateForHandle(handle, readyKernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState);
      setThreadInitialized(thread, true);
      clearThreadWaitingInWorkerStatus(thread);
      deps.clearBlockedWorker(handle, worker);
      const activity = getThreadActivityState(thread);
      const readyThreadEventResult = submitSupervisorShadowStatusEvent(handle, {
        sequence: activity.statusCount,
        vmPtr: thread.vmPtr,
        pid: handle.pid,
        tid: handle.kernel.thread_tid?.(thread.vmPtr) ?? 0,
        status: handle.kernel.abi_status_ok(),
        syncEffects: 0,
        blockedReason: 0,
        pc: handle.kernel.thread_pc(thread.vmPtr),
        sp: handle.kernel.thread_sp(thread.vmPtr),
      });
      if (readyThreadEventResult !== undefined && readyThreadEventResult < 0) {
        deps.appendWorkerDebugLog(handle, `supervisor-ready-event:submit-error:${readyThreadEventResult}`);
      }
      const wroteToPipe = flushStdin(handle);
      const drainedPipe = kernelWorkerOwnsPipeState(handle) ? false : drainProcessOutput(handle);
      const readySyncs: Promise<void>[] = [];
      if (wroteToPipe || drainedPipe) {
        readySyncs.push(deps.requestPipeStateSync(handle, true));
      }
      if (
        kernelWorkerOwnsEpollState(handle)
        && collectHeldEpollOfdIds(handle).length > 0
      ) {
        readySyncs.push(deps.requestEpollStateSync(handle, false));
      }
      if (readySyncs.length > 0) {
        void Promise.all(readySyncs)
          .then(() => {
            if (!workerStillOwnedByHandle()) {
              return;
            }
            releaseCloneSpawnResumeWorkers();
            deps.enqueueWorker(handle, worker);
            for (const resumeWorker of resumeWorkersOnReady) {
              if (handle.workers.has(resumeWorker)) {
                deps.enqueueWorker(handle, resumeWorker);
              }
            }
          })
          .catch((error) => {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return true;
      }
      releaseCloneSpawnResumeWorkers();
      deps.enqueueWorker(handle, worker);
      for (const resumeWorker of resumeWorkersOnReady) {
        if (handle.workers.has(resumeWorker)) {
          deps.enqueueWorker(handle, resumeWorker);
        }
      }
      return true;
    }

    if (msg.type === "status") {
      return false;
    }

    if (msg.type === "error") {
      handle.lastDebugMessage = msg.message;
      if (handle.runningWorker === worker) {
        handle.runningWorker = null;
      }
      deps.clearBlockedWorker(handle, worker);
      const {
        threadCount,
        liveThreads,
        threadSlots,
      } = safeThreadErrorDebug(handle, deps, msg.vmPtr);
      const attachedWorkers = [...handle.workers]
        .map((candidate) => deps.threadWorkerStates.get(candidate)?.vmPtr ?? 0)
        .filter((candidateVmPtr) => candidateVmPtr !== 0)
        .map((candidateVmPtr) => `0x${candidateVmPtr.toString(16)}`);
      deps.failProcess(
        handle,
        new Error(
          `pid=${handle.pid} thread vm=0x${msg.vmPtr.toString(16)} thread_count=${threadCount}`
            + ` live_threads=[${liveThreads.join(", ")}]`
            + ` attached_workers=[${attachedWorkers.join(", ")}]`
            + ` thread_slots=[${threadSlots.join(", ")}]`
            + ` ${msg.message}`,
        ),
      );
    }

    return true;
  };
}
