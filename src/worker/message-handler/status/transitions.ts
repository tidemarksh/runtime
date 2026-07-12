import {
  hostExecutionState,
  markRegularFileFsInFlight,
} from "../../host-execution-state.js";
import { processHasKernelWorkerOwnership } from "../../kernel-worker-ownership.js";
import {
  deleteProcessWorker,
  processHasWorker,
  processWorkerCount,
  processWorkerList,
} from "../../process-worker-state.js";
import { readKernelRuntimeState } from "../../../abi.js";
import { publishKernelRuntimeState, wakeRuntimeBridge } from "../../../bridge/runtime-state.js";
import {
  acquireWorker,
  releaseWorker,
} from "../../pool.js";
import { kernelShareOtherMembers } from "../../kernel-share.js";
import {
  assertHandleWorkerProcessIdentity,
  supervisorProcessOwnerMismatch,
} from "../../process-identity.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../../types.js";
import { executeHostBlockedStatusTransition } from "../../host-boundary/blocked-status-transition.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "../types.js";
import type { KernelRpcResumeOutcome } from "../../kernel-rpc/outcomes.js";
import { createWorkerOwnershipGuard } from "../../ownership/process-state.js";
import {
  refreshPipeControlFromKernelShare,
  snapshotProcessPipeSlotsForThreadStep,
} from "../../ownership/pipe-control.js";
import {
  supervisorFsPipeRefreshBeforeResumeAction,
} from "../../../fs/supervisor-pipe-refresh.js";
import { restoreRuntimeNetworkSidecarForHandle } from "../../ownership/socket-control.js";
import { networkConnections } from "../../network-socket-state.js";
import { workerDebugLog } from "../../worker-debug-log.js";
import { formatCompactDebugSuffix } from "../../diagnostics/format.js";
import {
  pollSupervisorFdControlDiagnostic,
  pollSupervisorFsControlDiagnostic,
  pollSupervisorSignalControlDiagnostic,
  readSupervisorFaultExitDecision,
  submitSupervisorShadowStatusEvent,
  supervisorFaultExitKindProcessExit,
} from "../../supervisor-shadow.js";
import {
  armSupervisorHostTimer,
  drainSupervisorArmHostTimerHostOp,
  drainSupervisorCloneSpawnHostOp,
  type SupervisorCloneSpawnHostOp,
} from "../../supervisor-host-op.js";
import {
  supervisorBlockedStatusNeedsNetworkSidecarRestore,
  supervisorBlockedWaitTimerRequired,
  supervisorWorkerSleepStatusActions,
  supervisorWorkerStatusSyncActions,
  supervisorWorkerStatusTransitionAction,
} from "../../../supervisor-blocked-resume.js";
import {
  supervisorProcessStatusAllowsPidMismatch,
  supervisorProcessStatusIsCloneSpawn,
  supervisorProcessStatusIsExecveSpawn,
  supervisorProcessStatusIsVforkWait,
} from "../../supervisor-status.js";
import {
  appendThreadResumeWorkerOnReady,
  clearThreadOwnerAsyncStatus,
  clearThreadWaitingInWorkerStatus,
  setThreadOwnerAsyncDetail,
  getThreadOwnerAsyncStatus,
  setThreadCloneSpawnHostOp,
  setThreadOwnerAsyncStatus,
  setThreadWaitingInWorkerStatus,
} from "../../thread-host-state.js";
import {
  appendPendingExitedThreadForNextStep,
  appendPendingPipeSlotsForNextStep,
} from "../../thread-step-payload.js";
import { isBlockedRpcPending } from "../../host-blocked-state.js";

const CLONE_SPAWN_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_CLONE_SPAWN_DEBUG ?? "") === "1";
const FS_LIFECYCLE_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_LIFECYCLE_DEBUG ?? "") === "1";

const cloneSpawnAsyncTokens = new WeakMap<ThreadWorkerState, number>();

interface StatusTransitionArgs {
  handle: ProcessHandle;
  worker: Worker;
  thread: ThreadWorkerState;
  msg: ThreadWorkerStatusMessage;
  status: number;
  wroteToPipe: boolean;
  drainedPipe: boolean;
  completedSyncEffects: number;
}

function appendFsTransitionTiming(
  deps: WorkerMessageHandlerDeps,
  handle: ProcessHandle,
  phase: string,
  vmPtr: number,
  detail = "",
): void {
  if (!FS_LIFECYCLE_DEBUG_ENABLED) {
    return;
  }
  const event = `fs-transition-${phase}:${handle.pid}:0x${vmPtr.toString(16)}:t=${Date.now()}${detail}`;
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
  console.error(`[tidemark-fs] ${event}`);
}

function beginRegularFileFsTransitionGate(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
): () => void {
  const pendingFd = handle.kernel.supervisor_fs_pending_regular_file_fd;
  const pendingOfd = handle.kernel.supervisor_fs_pending_regular_file_ofd;
  if (typeof pendingFd !== "function" || typeof pendingOfd !== "function") {
    return () => {};
  }
  const fd = pendingFd.call(handle.kernel, vmPtr);
  const ofdId = pendingOfd.call(handle.kernel, vmPtr);
  return markRegularFileFsInFlight(
    handle,
    fd >= 0 && ofdId > 0
      ? {
        worker,
        vmPtr,
        fd,
        ofdId,
      }
      : {
        worker,
        vmPtr,
        fd: -1,
        ofdId: 0,
      },
  );
}

const VFORK_CHILD_FAULT_DEBUG_KEYS = [
  "pc",
  "pc_insn",
  "pc_vma",
  "pc_vma_prev",
  "pc_vma_next",
  "ra",
  "sp",
  "s11",
  "syscall",
  "syscall_a7",
  "syscall_a0",
  "syscall_a1",
  "syscall_a2",
  "syscall_a3",
  "syscall_a4",
  "syscall_a5",
  "process_pid_field",
  "process_thread_count",
  "fault_pc",
  "fault_insn",
  "fault_vma",
  "fault_vma_prev",
  "fault_vma_next",
  "fault_addr",
  "fault_signal",
  "fault_code",
  "mem_brk_base",
  "mem_brk_current",
] as const;

function formatProcessThreadSlots(handle: ProcessHandle, vmPtr: number): string {
  const slots: string[] = [];
  for (let slot = 0; slot < handle.kernel.abi_max_threads(); slot += 1) {
    const tid = handle.kernel.process_thread_tid?.(vmPtr, slot) ?? 0;
    const state = handle.kernel.process_thread_state?.(vmPtr, slot) ?? 0;
    const threadVmPtr = handle.kernel.process_thread_vm_ptr?.(vmPtr, slot) ?? 0;
    if (tid === 0 && state === 0 && threadVmPtr === 0) {
      continue;
    }
    slots.push(`${slot}:${tid}/${state}/0x${threadVmPtr.toString(16)}`);
  }
  return slots.join(",");
}

function formatFaultThreadSummary(handle: ProcessHandle, label: string, vmPtr: number): string {
  return `${label}=0x${vmPtr.toString(16)}`
    + `/status=${handle.kernel.thread_status?.(vmPtr) ?? -1}`
    + `/exit=${handle.kernel.thread_exit_code?.(vmPtr) ?? 0}:${handle.kernel.thread_term_signal?.(vmPtr) ?? 0}`
    + `/sig=${handle.kernel.thread_fault_signal?.(vmPtr) ?? 0}`
    + `/kind=${handle.kernel.thread_fault_kind?.(vmPtr) ?? 0}`
    + `/pc=0x${(handle.kernel.thread_fault_pc?.(vmPtr) ?? 0n).toString(16)}`
    + `/addr=0x${(handle.kernel.thread_fault_addr?.(vmPtr) ?? 0n).toString(16)}`;
}

function workerHasPendingBlockedRpc(handle: ProcessHandle, worker: Worker): boolean {
  const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
  return isBlockedRpcPending(blocked);
}

function submitSupervisorConnectReadyEvent(
  deps: WorkerMessageHandlerDeps,
  handle: ProcessHandle,
  vmPtr: number,
  status: number | undefined,
): void {
  const okStatus = handle.kernel.abi_status_ok?.();
  if (okStatus === undefined || status !== okStatus) {
    return;
  }
  try {
    const result = submitSupervisorShadowStatusEvent(handle, {
      vmPtr,
      pid: handle.pid,
      tid: handle.kernel.thread_tid?.(vmPtr) ?? 0,
      status,
      syncEffects: 0,
      blockedReason: 0,
      pc: handle.kernel.thread_pc?.(vmPtr) ?? 0n,
      sp: handle.kernel.thread_sp?.(vmPtr) ?? 0n,
    });
    if (result !== undefined && result < 0) {
      deps.appendWorkerDebugLog(handle, `supervisor-connect-ready-event:submit-error:${result}`);
    }
  } catch (error) {
    deps.appendWorkerDebugLog(
      handle,
      `supervisor-connect-ready-event:submit-error:${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

export function createWorkerStatusTransitionHandler(deps: WorkerMessageHandlerDeps) {
  function consumeKernelRpcResumeOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    outcome: KernelRpcResumeOutcome,
  ): void {
    if (
      outcome.kind === "completed"
      && outcome.status === handle.kernel.abi_status_ok()
      && outcome.workerQueued !== true
      && !hostExecutionState(handle).hostBlockedWorkers.has(worker)
      && processHasWorker(handle, worker)
      && deps.threadWorkerStates.has(worker)
    ) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `kernel-rpc-completed-stale-enqueue:${handle.pid}:vm=0x${vmPtr.toString(16)}:status=${outcome.status}`,
      );
      deps.requestThreadWorkerBlockedResume(handle, worker);
      deps.enqueueWorker(handle, worker);
    }
    switch (outcome.kind) {
      case "completed":
      case "queued-in-worker":
      case "process-gone":
      case "stale":
        return;
      case "blocked": {
        const blocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
        if (blocked && blocked.vmPtr === vmPtr) {
          blocked.status = outcome.status;
        }
        return;
      }
      default: {
        const exhaustive: never = outcome;
        throw new Error(`unhandled kernel-rpc outcome: ${JSON.stringify(exhaustive)}`);
      }
    }
  }

  return function handleStatusTransition({
    handle,
    worker,
    thread,
    msg,
    status,
    wroteToPipe,
    drainedPipe,
    completedSyncEffects,
  }: StatusTransitionArgs): void {
    const assertHandleProcessIdentity = (context: string): void => {
      assertHandleWorkerProcessIdentity(handle, deps.threadWorkerStates, context, {
        status,
        threadVmPtr: thread.vmPtr,
        workerLogTail: workerDebugLog(handle).slice(-16),
        skipVmPtrs: [hostExecutionState(handle).vforkSnapshot?.vmPtr ?? 0].filter((vmPtr) => vmPtr !== 0),
      });
    };
    const workerStillOwnedByHandle = createWorkerOwnershipGuard(
      handle,
      worker,
      thread,
      deps.threadWorkerStates,
    );
    if (!supervisorProcessStatusAllowsPidMismatch(handle.kernel, status)) {
      assertHandleProcessIdentity("handleStatusTransition:start");
    }
    const supervisorFdControlDiagnostic = pollSupervisorFdControlDiagnostic(handle);
    if (supervisorFdControlDiagnostic !== undefined) {
      deps.appendWorkerDebugLog(handle, supervisorFdControlDiagnostic);
    }
    const supervisorFsControlDiagnostic = pollSupervisorFsControlDiagnostic(handle);
    if (supervisorFsControlDiagnostic !== undefined) {
      deps.appendWorkerDebugLog(handle, supervisorFsControlDiagnostic);
    }
    const supervisorSignalControlDiagnostic = pollSupervisorSignalControlDiagnostic(handle);
    if (supervisorSignalControlDiagnostic !== undefined) {
      deps.appendWorkerDebugLog(handle, supervisorSignalControlDiagnostic);
    }

    const enqueueWorkerAfterOk = (releaseOwnerAsync = false): void => {
      if (workerHasPendingBlockedRpc(handle, worker)) {
        setTimeout(() => {
          if (!workerStillOwnedByHandle()) {
            deps.pumpProcess(handle);
            return;
          }
          enqueueWorkerAfterOk(releaseOwnerAsync);
        }, 1);
        return;
      }
      if (releaseOwnerAsync) {
        if (getThreadOwnerAsyncStatus(thread) !== status) {
          deps.pumpProcess(handle);
          return;
        }
        clearThreadOwnerAsyncStatus(thread);
      }
      deps.enqueueWorker(handle, worker);
    };

    const syncSocketAfterNetworkPump = (networkChanged: boolean): Promise<void> => {
      if (!networkChanged) {
        return Promise.resolve();
      }
      return deps.requestSocketStateSync(handle, true);
    };
    const statusSyncActions = supervisorWorkerStatusSyncActions(handle.kernel, status, completedSyncEffects, {
      wroteOrDrainedPipe: wroteToPipe || drainedPipe,
    });

    if (status === handle.kernel.abi_status_ok()) {
      const shouldSyncPipeState = statusSyncActions.syncPipeState;
      const shouldSyncSocketState = statusSyncActions.syncSocketState;
      const shouldSyncLifecycle = statusSyncActions.syncLifecycle;
      const shouldWakeSignalWaiters = statusSyncActions.wakeSignalWaiters;
      if (shouldSyncPipeState || shouldSyncSocketState || shouldSyncLifecycle) {
        deps.appendWorkerDebugLog(handle, `ok-sync-start:0x${thread.vmPtr.toString(16)}`);
        setThreadOwnerAsyncStatus(thread, status, "ok-sync:start");
        const activeVforkWorkerOwnsThread = (): boolean => {
          const executionState = hostExecutionState(handle);
          const vforkSnapshot = executionState.vforkSnapshot;
          return vforkSnapshot !== null
            && executionState.vforkWorker === worker
            && vforkSnapshot.vmPtr === thread.vmPtr;
        };
        const finishIfWorkerDetached = (): boolean => {
          const ownerMismatch = supervisorProcessOwnerMismatch(handle.kernel, thread.vmPtr, handle.pid);
          if (workerStillOwnedByHandle() && (!ownerMismatch || activeVforkWorkerOwnsThread())) {
            return false;
          }
          clearThreadOwnerAsyncStatus(thread);
          deps.pumpProcess(handle);
          return true;
        };
        void (async () => {
          try {
            const drainedHiddenOutputBeforePipeSync = shouldSyncPipeState
              ? deps.drainHiddenProcessOutput(handle)
              : false;
            if (shouldSyncPipeState) {
              setThreadOwnerAsyncDetail(thread, "ok-sync:pipe");
              await deps.requestPipeStateSync(handle, true);
            }
            if (finishIfWorkerDetached()) {
              return;
            }
            assertHandleProcessIdentity("handleStatusTransition:after-pipe-sync");
            if (
              shouldSyncPipeState
              && !drainedHiddenOutputBeforePipeSync
              && deps.drainHiddenProcessOutput(handle)
            ) {
              setThreadOwnerAsyncDetail(thread, "ok-sync:pipe-hidden-output");
              await deps.requestPipeStateSync(handle, false);
              if (finishIfWorkerDetached()) {
                return;
              }
              assertHandleProcessIdentity("handleStatusTransition:after-hidden-output-drain");
            }
            if (shouldSyncSocketState) {
              setThreadOwnerAsyncDetail(thread, "ok-sync:socket");
              await deps.requestSocketStateSync(handle, true);
            }
            if (finishIfWorkerDetached()) {
              return;
            }
            assertHandleProcessIdentity("handleStatusTransition:after-socket-sync");
            if (shouldSyncLifecycle) {
              setThreadOwnerAsyncDetail(thread, "ok-sync:lifecycle");
              await deps.syncProcessLifecycleViaKernelWorker(handle, thread.vmPtr);
            }
            if (finishIfWorkerDetached()) {
              return;
            }
            assertHandleProcessIdentity("handleStatusTransition:after-lifecycle-sync");
            if (shouldSyncLifecycle) {
              deps.pruneDetachedProcessWorkers(handle, thread.vmPtr, worker);
              assertHandleProcessIdentity("handleStatusTransition:after-prune-detached");
            }
            deps.appendWorkerDebugLog(handle, `ok-sync-done:0x${thread.vmPtr.toString(16)}`);
            setThreadOwnerAsyncDetail(thread, "ok-sync:network");
            const networkChanged = networkConnections(handle).size > 0
              ? deps.pumpNetworkConnections(handle, thread.vmPtr)
              : false;
            await syncSocketAfterNetworkPump(await networkChanged);
            if (!workerStillOwnedByHandle()) {
              clearThreadOwnerAsyncStatus(thread);
              deps.pumpProcess(handle);
              return;
            }
            if (shouldWakeSignalWaiters) {
              wakeRuntimeBridge(handle.runtimeBridge);
            }
            deps.notifyWorkerStatusProgress(handle);
            if (shouldSyncPipeState) {
              for (const member of kernelShareOtherMembers(handle)) {
                deps.notifyWorkerStatusProgress(member);
                deps.pumpProcess(member);
              }
            }
            assertHandleProcessIdentity("handleStatusTransition:before-enqueue");
            setThreadOwnerAsyncDetail(thread, "ok-sync:enqueue");
            enqueueWorkerAfterOk(true);
          } catch (error) {
            clearThreadOwnerAsyncStatus(thread);
            if (!workerStillOwnedByHandle()) {
              deps.pumpProcess(handle);
              return;
            }
            deps.appendWorkerDebugLog(handle, `ok-sync-err:0x${thread.vmPtr.toString(16)}`);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          }
        })();
        return;
      }
      if (shouldSyncLifecycle) {
        deps.pruneDetachedProcessWorkers(handle, thread.vmPtr, worker);
      }
      if (shouldWakeSignalWaiters) {
        wakeRuntimeBridge(handle.runtimeBridge);
      }
      if (networkConnections(handle).size > 0) {
        void deps.pumpNetworkConnections(handle, thread.vmPtr)
          .then((networkChanged) => syncSocketAfterNetworkPump(networkChanged))
          .then(() => {
            if (!workerStillOwnedByHandle()) {
              return;
            }
            deps.notifyWorkerStatusProgress(handle);
            enqueueWorkerAfterOk();
          })
          .catch((error) => {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      deps.notifyWorkerStatusProgress(handle);
      enqueueWorkerAfterOk();
      return;
    }

    if (status === handle.kernel.abi_status_fault()) {
      const vforkSnapshot = hostExecutionState(handle).vforkSnapshot;
      if (
        vforkSnapshot !== null
        && hostExecutionState(handle).vforkWorker === worker
        && vforkSnapshot.vmPtr === thread.vmPtr
      ) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `vfork-child-fault:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:child=${vforkSnapshot.childPid ?? "none"}`
            + `:kernel_child=${handle.kernel.thread_vfork_child_pid?.() ?? "n/a"}`
            + `:exit=${msg.exitCode}:${msg.termSignal}`
            + formatCompactDebugSuffix(msg.debug, VFORK_CHILD_FAULT_DEBUG_KEYS),
        );
      }
      if (msg.debug) {
        deps.appendWorkerDebugLog(handle, `fault_debug:0x${thread.vmPtr.toString(16)}:${JSON.stringify(msg.debug)}`);
      }
      const supervisorExitDecision = readSupervisorFaultExitDecision(handle, thread.vmPtr);
      if (supervisorExitDecision === undefined) {
        deps.failProcess(
          handle,
          new Error(`supervisor fault exit decision required for vm=0x${thread.vmPtr.toString(16)}`),
        );
        return;
      }
      const exitCode = supervisorExitDecision.exitCode;
      const termSignal = supervisorExitDecision.termSignal;
      if (deps.resumeVforkParentAfterChildExit(handle, worker, thread.vmPtr, exitCode, termSignal)) {
        return;
      }
      const currentPid = handle.kernel.process_current_pid(thread.vmPtr);
      const remainingThreads = supervisorExitDecision.remainingThreads;
      const hasLiveThreads = supervisorExitDecision.liveThreads !== 0;
      const shouldExitProcess = supervisorExitDecision.kind === supervisorFaultExitKindProcessExit(handle.kernel);
      const faultProcessPtr = handle.kernel.thread_process_ptr(thread.vmPtr);
      const ownerPid = handle.kernel.process_current_pid(handle.vmPtr);
      const ownerProcessPtr = handle.kernel.thread_process_ptr(handle.vmPtr);
      const ownerThreadCount = handle.kernel.thread_process_thread_count(handle.vmPtr);
      const processTableDebug = `:fault_pid=${currentPid}:fault_process=0x${faultProcessPtr.toString(16)}`
        + `:fault_count=${remainingThreads}`
        + `:owner_pid=${ownerPid}:owner_process=0x${ownerProcessPtr.toString(16)}`
        + `:owner_count=${ownerThreadCount}:owner_threads=[${formatProcessThreadSlots(handle, handle.vmPtr)}]`;
      deps.appendWorkerDebugLog(handle, `fault:0x${thread.vmPtr.toString(16)} remaining=${remainingThreads}`);
      if (shouldExitProcess) {
        const exitVmPtr = supervisorExitDecision.exitVmPtr;
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `complete-exit-request:transitions-fault:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:exitVm=0x${exitVmPtr.toString(16)}`
            + `:decision=${supervisorExitDecision.seq.toString()}`
            + `:status=${status}:msg=${msg.status}:exit=${exitCode}:${termSignal}`
            + `:current=${handle.kernel.thread_status?.(thread.vmPtr) ?? -1}:remaining=${remainingThreads}:live=${hasLiveThreads ? 1 : 0}`
            + processTableDebug
            + `:${formatFaultThreadSummary(handle, "faultVm", thread.vmPtr)}`
            + `:${formatFaultThreadSummary(handle, "exitVm", exitVmPtr)}`,
        );
        void deps.completeProcessExit(handle, exitCode, termSignal, exitVmPtr).catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      } else {
        const exitedTid = handle.kernel.thread_tid?.(thread.vmPtr) ?? 0;
        if (exitedTid > 0) {
          for (const remainingWorker of processWorkerList(handle)) {
            if (remainingWorker === worker) {
              continue;
            }
            const remainingThread = deps.threadWorkerStates.get(remainingWorker);
            if (remainingThread !== undefined) {
              appendPendingExitedThreadForNextStep(remainingThread, {
                vmPtr: thread.vmPtr,
                tid: exitedTid,
              });
            }
          }
        }
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `fault-worker-delete:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:status=${status}:msg=${msg.status}:exit=${msg.exitCode}:${msg.termSignal}`
            + `:current=${handle.kernel.thread_status?.(thread.vmPtr) ?? -1}`
            + `:remaining=${remainingThreads}:live=${hasLiveThreads ? 1 : 0}`
            + processTableDebug
            + `:threads=[${formatProcessThreadSlots(handle, thread.vmPtr)}]`,
        );
        if (hostExecutionState(handle).runningWorker === worker) {
          hostExecutionState(handle).runningWorker = null;
        }
        deleteProcessWorker(handle, worker);
        deps.threadWorkerStates.delete(worker);
        releaseWorker(worker);
        if (statusSyncActions.syncPipeState) {
          void deps.requestPipeStateSync(handle, true)
            .then(() => {
              deps.notifyWorkerStatusProgress(handle);
              deps.pumpProcess(handle);
            })
            .catch((error) => {
              deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
            });
          return;
        }
        deps.notifyWorkerStatusProgress(handle);
        deps.pumpProcess(handle);
      }
      return;
    }

    if (statusSyncActions.syncPipeState) {
      void deps.requestPipeStateSync(handle, true).catch((error) => {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    }
    if (supervisorBlockedStatusNeedsNetworkSidecarRestore(handle.kernel, status)) {
      restoreRuntimeNetworkSidecarForHandle(handle, deps.processHandles, thread.vmPtr);
    }
    const workerStatusTransitionAction = supervisorWorkerStatusTransitionAction(handle.kernel, status);

    if (
      networkConnections(handle).size > 0
      && workerStatusTransitionAction !== "connect-wait"
    ) {
      void deps.pumpNetworkConnections(handle, thread.vmPtr)
        .then(async (changed) => {
          if (!changed) {
            return;
          }
          await deps.requestSocketStateSync(handle, true);
          if (!workerStillOwnedByHandle()) {
            return;
          }
          deps.notifyWorkerStatusProgress(handle);
          deps.pumpProcess(handle);
        })
        .catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
    }

    if (status === handle.kernel.abi_status_sleep()) {
      const sleepActions = supervisorWorkerSleepStatusActions(
        handle.kernel,
        thread.vmPtr,
        status,
        completedSyncEffects,
      );
      if (sleepActions.yieldThread) {
        deps.appendWorkerDebugLog(handle, `yield:0x${thread.vmPtr.toString(16)}`);
        const clearYieldingWorker = (): void => {
          if (hostExecutionState(handle).runningWorker === worker) {
            hostExecutionState(handle).runningWorker = null;
          }
          clearThreadWaitingInWorkerStatus(thread);
        };
        if (sleepActions.syncPipeBeforeYieldClear) {
          void deps.requestPipeStateSync(handle, false)
            .then(() => {
              if (!workerStillOwnedByHandle()) {
                return;
              }
              clearYieldingWorker();
              deps.notifyWorkerStatusProgress(handle);
              deps.pumpProcess(handle);
            })
            .catch((error) => {
              if (!workerStillOwnedByHandle()) {
                return;
              }
              deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
            });
          return;
        }
        clearYieldingWorker();
        deps.notifyWorkerStatusProgress(handle);
        deps.pumpProcess(handle);
        return;
      }
      if (!sleepActions.armHostTimer) {
        throw new Error(`kernel supervisor did not select a sleep status action for status ${status}`);
      }
      const sec = handle.kernel.thread_sleep_seconds(thread.vmPtr);
      const nsec = handle.kernel.thread_sleep_nanoseconds(thread.vmPtr);
      deps.appendWorkerDebugLog(handle, `sleep:0x${thread.vmPtr.toString(16)}@${sec.toString()}s+${nsec.toString()}ns`);
      const hostTimer = drainSupervisorArmHostTimerHostOp(handle, thread.vmPtr);
      if (hostTimer === undefined) {
        throw new Error("kernel supervisor ABI missing arm host timer HostOp exports");
      }
      if (hostExecutionState(handle).runningWorker === worker) {
        hostExecutionState(handle).runningWorker = null;
      }
      setThreadWaitingInWorkerStatus(thread, status);
      if (hostTimer !== null) {
        armSupervisorHostTimer(handle, hostTimer, deps);
      }
      deps.notifyWorkerStatusProgress(handle);
      deps.pumpProcess(handle);
      return;
    }

    if (status === handle.kernel.abi_status_fs()) {
      deps.appendWorkerDebugLog(handle, `fs:0x${thread.vmPtr.toString(16)}`);
      const traceFsTransitionTiming = FS_LIFECYCLE_DEBUG_ENABLED;
      const fsTransitionStartMs = traceFsTransitionTiming ? Date.now() : 0;
      const clearRegularFileFsTransitionGate = beginRegularFileFsTransitionGate(
        handle,
        worker,
        thread.vmPtr,
      );
      if (traceFsTransitionTiming) {
        appendFsTransitionTiming(deps, handle, "entry", thread.vmPtr);
      }
      const pipeRefreshAction = supervisorFsPipeRefreshBeforeResumeAction(handle.kernel, thread.vmPtr, {
        kernelWorkerAvailable: processHasKernelWorkerOwnership(handle),
      });
      if (traceFsTransitionTiming) {
        appendFsTransitionTiming(
          deps,
          handle,
          "pipe-refresh",
          thread.vmPtr,
          `:action=${pipeRefreshAction}:dt=${Date.now() - fsTransitionStartMs}`,
        );
      }
      if (pipeRefreshAction === "request-sync") {
        setThreadOwnerAsyncStatus(thread, status);
        void deps.requestPipeStateSync(handle, false)
          .then(() => {
            if (traceFsTransitionTiming) {
              appendFsTransitionTiming(
                deps,
                handle,
                "pipe-sync-done",
                thread.vmPtr,
                `:dt=${Date.now() - fsTransitionStartMs}`,
              );
            }
            refreshPipeControlFromKernelShare(handle);
            const pipeSlotsForNextStep = snapshotProcessPipeSlotsForThreadStep(handle, thread.vmPtr);
            if (pipeSlotsForNextStep.length > 0) {
              appendPendingPipeSlotsForNextStep(thread, pipeSlotsForNextStep);
            }
            if (!workerStillOwnedByHandle()) {
              clearThreadOwnerAsyncStatus(thread);
              clearRegularFileFsTransitionGate();
              return;
            }
            const resumeFs = deps.resumeFsViaKernelWorkerOutcome ?? deps.resumeFsViaKernelWorker;
            const resumeStartMs = traceFsTransitionTiming ? Date.now() : 0;
            if (traceFsTransitionTiming) {
              appendFsTransitionTiming(
                deps,
                handle,
                "resume-start",
                thread.vmPtr,
                `:dt=${resumeStartMs - fsTransitionStartMs}`,
              );
            }
            return resumeFs(handle, worker, thread.vmPtr)
              .then((outcome) => {
                if (traceFsTransitionTiming) {
                  appendFsTransitionTiming(
                    deps,
                    handle,
                    "resume-done",
                    thread.vmPtr,
                    `:dt=${Date.now() - resumeStartMs}:total=${Date.now() - fsTransitionStartMs}`,
                  );
                }
                if (outcome !== undefined) {
                  consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
                }
              })
              .then(() => undefined)
              .finally(() => {
                clearThreadOwnerAsyncStatus(thread);
                clearRegularFileFsTransitionGate();
                if (traceFsTransitionTiming) {
                  appendFsTransitionTiming(
                    deps,
                    handle,
                    "finally-pump",
                    thread.vmPtr,
                    `:total=${Date.now() - fsTransitionStartMs}`,
                  );
                }
                deps.pumpProcess(handle);
              });
          })
          .catch((error) => {
            clearThreadOwnerAsyncStatus(thread);
            clearRegularFileFsTransitionGate();
            if (!workerStillOwnedByHandle()) {
              return;
            }
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      setThreadOwnerAsyncStatus(thread, status);
      const resumeFs = deps.resumeFsViaKernelWorkerOutcome ?? deps.resumeFsViaKernelWorker;
      const resumeStartMs = traceFsTransitionTiming ? Date.now() : 0;
      if (traceFsTransitionTiming) {
        appendFsTransitionTiming(
          deps,
          handle,
          "resume-start",
          thread.vmPtr,
          `:dt=${resumeStartMs - fsTransitionStartMs}`,
        );
      }
      void resumeFs(handle, worker, thread.vmPtr)
        .then((outcome) => {
          if (traceFsTransitionTiming) {
            appendFsTransitionTiming(
              deps,
              handle,
              "resume-done",
              thread.vmPtr,
              `:dt=${Date.now() - resumeStartMs}:total=${Date.now() - fsTransitionStartMs}`,
            );
          }
          if (outcome !== undefined) {
            consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
          }
        })
        .finally(() => {
          clearThreadOwnerAsyncStatus(thread);
          clearRegularFileFsTransitionGate();
          if (traceFsTransitionTiming) {
            appendFsTransitionTiming(
              deps,
              handle,
              "finally-pump",
              thread.vmPtr,
              `:total=${Date.now() - fsTransitionStartMs}`,
            );
          }
          deps.pumpProcess(handle);
        })
        .catch((error) => {
          if (!workerStillOwnedByHandle()) {
            return;
          }
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }

    if (workerStatusTransitionAction === "connect-wait") {
      if (supervisorBlockedWaitTimerRequired(handle.kernel, thread.vmPtr)) {
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        deps.pumpProcess(handle);
        return;
      }
      if (restoreRuntimeNetworkSidecarForHandle(handle, deps.processHandles, thread.vmPtr)) {
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
        const connectResult = deps.resolveNetworkConnect(handle, thread.vmPtr);
        void connectResult
          .then(async (result) => {
            await deps.requestSocketStateSync(handle, false);
            const outcome = await deps.resumeConnectViaKernelWorker(handle, worker, thread.vmPtr, result);
            if (outcome !== undefined) {
              consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
            }
            if (outcome === undefined || outcome.kind === "completed") {
              submitSupervisorConnectReadyEvent(
                deps,
                handle,
                thread.vmPtr,
                outcome?.status ?? handle.kernel.thread_status?.(thread.vmPtr),
              );
            }
            await deps.commitNetworkConnect(handle, thread.vmPtr, result);
          })
          .then(() => {
            deps.notifyWorkerStatusProgress(handle);
            deps.enqueueWorker(handle, worker);
          })
          .catch((error) => {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        deps.pumpProcess(handle);
        return;
      }
      deps.appendWorkerDebugLog(
        handle,
        `connect-no-bridge-resume-start:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
          + `:kernelWorker=${processHasKernelWorkerOwnership(handle) ? 1 : 0}`
          + `:status=${handle.kernel.thread_status?.(thread.vmPtr) ?? "n/a"}`
          + `:a0=${handle.kernel.thread_a0?.(thread.vmPtr)?.toString() ?? "n/a"}`
          + `:blockedFd=${handle.kernel.thread_blocked_fd?.(thread.vmPtr) ?? "n/a"}`,
      );
      void deps.resumeConnectViaKernelWorker(
        handle,
        worker,
        thread.vmPtr,
        BigInt(-handle.kernel.abi_econnrefused()),
      )
        .then((outcome) => {
          deps.appendWorkerDebugLog(
            handle,
            `connect-no-bridge-resume-done:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
              + `:outcome=${outcome?.kind ?? "none"}`
              + `:status=${handle.kernel.thread_status?.(thread.vmPtr) ?? "n/a"}`
              + `:a0=${handle.kernel.thread_a0?.(thread.vmPtr)?.toString() ?? "n/a"}`,
          );
          if (outcome !== undefined) {
            consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
          }
          if (outcome === undefined || outcome.kind === "completed") {
            submitSupervisorConnectReadyEvent(
              deps,
              handle,
              thread.vmPtr,
              outcome?.status ?? handle.kernel.thread_status?.(thread.vmPtr),
            );
          }
          deps.notifyWorkerStatusProgress(handle);
          deps.enqueueWorker(handle, worker);
        })
        .catch((error) => {
          deps.appendWorkerDebugLog(
            handle,
            `connect-no-bridge-resume-err:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
              + `:${error instanceof Error ? error.message : String(error)}`,
          );
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      deps.pumpProcess(handle);
      return;
    }

    if (supervisorProcessStatusIsCloneSpawn(handle.kernel, status)) {
      if (msg.debug) {
        deps.appendWorkerDebugLog(
          handle,
          `clone_spawn_debug:0x${thread.vmPtr.toString(16)}:${JSON.stringify(msg.debug)}`,
        );
      }
      if (CLONE_SPAWN_DEBUG_ENABLED) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `clone-spawn-enter:${handle.pid}:0x${thread.vmPtr.toString(16)}:status=${status}`,
        );
      }
      let childVmPtr = 0;
      let cloneSpawnHostOp: SupervisorCloneSpawnHostOp | null = null;
      const spawnCloneChild = (): boolean => {
        const hostOp = drainSupervisorCloneSpawnHostOp(handle, thread.vmPtr);
        if (hostOp === null || hostOp === undefined) {
          childVmPtr = 0;
        } else {
          cloneSpawnHostOp = hostOp;
          childVmPtr = hostOp.childVmPtr;
          deps.appendWorkerDebugLog(
            handle,
            `clone_spawn_hostop:parent=0x${hostOp.parentVmPtr.toString(16)}`
              + `:child=0x${hostOp.childVmPtr.toString(16)}`
              + `:tid=${hostOp.tid}:seq=${hostOp.seq.toString()}`,
          );
        }
        if (CLONE_SPAWN_DEBUG_ENABLED) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `clone-spawn-after-kernel:${handle.pid}:parent=0x${thread.vmPtr.toString(16)}`
              + `:child=0x${childVmPtr.toString(16)}`
              + `:threads=[${formatProcessThreadSlots(handle, thread.vmPtr)}]`,
          );
        }
        if (childVmPtr !== 0) {
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          return true;
        }
        const currentStatus = handle.kernel.thread_status?.(thread.vmPtr);
        if (CLONE_SPAWN_DEBUG_ENABLED) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `clone-spawn-zero:${handle.pid}:0x${thread.vmPtr.toString(16)}`
              + `:status=${status}:current=${currentStatus ?? "n/a"}`,
          );
        }
        if (currentStatus !== undefined && currentStatus !== status) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `clone-spawn-stale-current:${handle.pid}:0x${thread.vmPtr.toString(16)}:${status}->${currentStatus}`,
          );
          deps.appendWorkerDebugLog(
            handle,
            `clone_spawn_stale:0x${thread.vmPtr.toString(16)}:${status}->${currentStatus}`,
          );
          deps.notifyWorkerStatusProgress(handle);
          deps.pumpProcess(handle);
          return false;
        }
        if (hostOp !== undefined) {
          throw new Error(`supervisor clone spawn HostOp missing for pid=${handle.pid}`);
        }
        throw new Error(`kernel supervisor ABI missing clone spawn HostOp exports for pid=${handle.pid}`);
      };
      const finishCloneSpawn = (): void => {
        if (CLONE_SPAWN_DEBUG_ENABLED) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `clone-spawn-finish:${handle.pid}:parent=0x${thread.vmPtr.toString(16)}`
              + `:child=0x${childVmPtr.toString(16)}`
              + `:threads=[${formatProcessThreadSlots(handle, thread.vmPtr)}]`,
          );
        }
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        deps.assertProcessStandardFdIntegrity(handle, "clone-spawn");
        publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
        const childWorker = acquireWorker();
        deps.attachThreadWorker(handle, { worker: childWorker, vmPtr: childVmPtr });
        const childThread = deps.threadWorkerStates.get(childWorker);
        if (!childThread) {
          throw new Error(`child thread state missing for pid=${handle.pid} vm=0x${childVmPtr.toString(16)}`);
        }
        if (cloneSpawnHostOp === null) {
          throw new Error(`clone spawn HostOp record missing for pid=${handle.pid}`);
        }
        setThreadCloneSpawnHostOp(childThread, cloneSpawnHostOp);
        setThreadOwnerAsyncStatus(thread, status);
        appendThreadResumeWorkerOnReady(childThread, worker);
        deps.notifyWorkerStatusProgress(handle);
        if (CLONE_SPAWN_DEBUG_ENABLED) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `clone-spawn-attached:${handle.pid}:parent=0x${thread.vmPtr.toString(16)}`
              + `:child=0x${childVmPtr.toString(16)}:workers=${processWorkerCount(handle)}`
              + `:threads=[${formatProcessThreadSlots(handle, thread.vmPtr)}]`,
          );
        }
      };

      setThreadOwnerAsyncStatus(thread, status);
      const cloneSpawnAsyncToken = (cloneSpawnAsyncTokens.get(thread) ?? 0) + 1;
      cloneSpawnAsyncTokens.set(thread, cloneSpawnAsyncToken);
      const isCurrentCloneSpawnAsync = (): boolean =>
        cloneSpawnAsyncTokens.get(thread) === cloneSpawnAsyncToken
        && getThreadOwnerAsyncStatus(thread) === status;
      if (CLONE_SPAWN_DEBUG_ENABLED) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `clone-spawn-sync-start:${handle.pid}:0x${thread.vmPtr.toString(16)}`
            + `:child=pending`,
        );
      }
      void deps.syncProcessLifecycleViaKernelWorker(handle, thread.vmPtr)
        .then(() => {
          if (CLONE_SPAWN_DEBUG_ENABLED) {
            deps.appendRuntimeLifecycleEvent(
              handle.runtimeId,
              `clone-spawn-sync-done:${handle.pid}:0x${thread.vmPtr.toString(16)}`
                + `:owned=${workerStillOwnedByHandle() ? 1 : 0}`
                + `:threads=[${formatProcessThreadSlots(handle, thread.vmPtr)}]`,
            );
          }
          if (!workerStillOwnedByHandle()) {
            return;
          }
          if (!isCurrentCloneSpawnAsync()) {
            if (CLONE_SPAWN_DEBUG_ENABLED) {
              deps.appendRuntimeLifecycleEvent(
                handle.runtimeId,
                `clone-spawn-sync-stale:${handle.pid}:0x${thread.vmPtr.toString(16)}`
                  + `:token=${cloneSpawnAsyncToken}`,
              );
            }
            return;
          }
          if (!spawnCloneChild()) {
            clearThreadOwnerAsyncStatus(thread);
            return;
          }
          finishCloneSpawn();
        })
        .catch((error) => {
          const currentCloneSpawnAsync = isCurrentCloneSpawnAsync();
          if (currentCloneSpawnAsync) {
            clearThreadOwnerAsyncStatus(thread);
          }
          if (CLONE_SPAWN_DEBUG_ENABLED) {
            deps.appendRuntimeLifecycleEvent(
              handle.runtimeId,
              `clone-spawn-sync-error:${handle.pid}:0x${thread.vmPtr.toString(16)}`
                + `:${error instanceof Error ? error.message : String(error)}`,
            );
          }
          if (!workerStillOwnedByHandle()) {
            return;
          }
          if (!currentCloneSpawnAsync) {
            return;
          }
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }

    if (supervisorProcessStatusIsExecveSpawn(handle.kernel, status)) {
      setThreadOwnerAsyncStatus(thread, status);
      void deps.handleExecveSpawn(handle, worker, thread)
        .finally(() => {
          if (workerStillOwnedByHandle()) {
            clearThreadOwnerAsyncStatus(thread);
          }
        })
        .catch((error) => {
          if (!workerStillOwnedByHandle()) {
            return;
          }
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }

    if (supervisorProcessStatusIsVforkWait(handle.kernel, status)) {
      const executionState = hostExecutionState(handle);
      let vforkSnapshot = executionState.vforkSnapshot;
      if (vforkSnapshot === null) {
        vforkSnapshot = deps.captureVforkSnapshot(handle, thread);
        executionState.vforkSnapshot = vforkSnapshot;
        executionState.vforkWorker = worker;
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `vfork-wait-snapshot:capture:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:child=${vforkSnapshot.childPid ?? "none"}`
            + `:kernel_child=${handle.kernel.thread_vfork_child_pid()}`
            + `:pc=${handle.kernel.thread_pc?.(thread.vmPtr)?.toString(16) ?? "n/a"}`
            + `:a0=${handle.kernel.thread_a0?.(thread.vmPtr)?.toString() ?? "n/a"}`
            + `:a7=${handle.kernel.thread_a7?.(thread.vmPtr)?.toString() ?? "n/a"}`,
        );
      } else {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `vfork-wait-snapshot:reuse:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:snapshot=0x${vforkSnapshot.vmPtr.toString(16)}`
            + `:child=${vforkSnapshot.childPid ?? "none"}`
            + `:worker=${executionState.vforkWorker === worker ? "match" : "mismatch"}`
            + `:kernel_child=${handle.kernel.thread_vfork_child_pid()}`,
        );
      }
      if (vforkSnapshot.childPid === null) {
        void deps.syncForkSharedStateViaKernelWorker(
          handle,
          kernelShareOtherMembers(handle),
          {
            cause: { kind: "syscall", nr: -1n, name: "vfork-wait-preserve-source" },
            includeMemfs: false,
            preserveSourceRefcounts: true,
            sourceVmPtr: thread.vmPtr,
          },
        )
          .then(() => {
            if (!processHasWorker(handle, worker) || !deps.threadWorkerStates.has(worker)) {
              return;
            }
            const latestSnapshot = hostExecutionState(handle).vforkSnapshot;
            if (!latestSnapshot || latestSnapshot.vmPtr !== thread.vmPtr) {
              return;
            }
            if (latestSnapshot.childPid === null) {
              const provisionalChildPid = handle.kernel.thread_vfork_child_pid();
              if (provisionalChildPid > 0) {
                deps.appendRuntimeLifecycleEvent(
                  handle.runtimeId,
                  `vfork-wait-canonicalize:start:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
                    + `:child=${provisionalChildPid}`,
                );
                void deps.canonicalizeActiveVforkChildViaKernelWorker(
                  handle,
                  worker,
                  thread.vmPtr,
                  provisionalChildPid,
                ).then((outcome) => {
                  deps.appendRuntimeLifecycleEvent(
                    handle.runtimeId,
                    `vfork-wait-canonicalize:done:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
                      + `:child=${provisionalChildPid}:outcome=${outcome?.kind ?? "none"}`
                      + `:pc=${handle.kernel.thread_pc?.(thread.vmPtr)?.toString(16) ?? "n/a"}`
                      + `:a0=${handle.kernel.thread_a0?.(thread.vmPtr)?.toString() ?? "n/a"}`
                      + `:a7=${handle.kernel.thread_a7?.(thread.vmPtr)?.toString() ?? "n/a"}`,
                  );
                  if (outcome !== undefined) {
                    consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
                  }
                });
                return;
              }
            }
            deps.enqueueWorker(handle, worker);
          })
          .catch((error) => {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      deps.enqueueWorker(handle, worker);
      return;
    }

    if (workerStatusTransitionAction === "park-futex") {
      const hostTimer = drainSupervisorArmHostTimerHostOp(handle, thread.vmPtr);
      if (hostTimer === undefined) {
        throw new Error("kernel supervisor ABI missing arm host timer HostOp exports");
      }
      if (hostExecutionState(handle).runningWorker === worker) {
        hostExecutionState(handle).runningWorker = null;
      }
      deps.parkSupervisorFutexWait(
        handle,
        worker,
        thread.vmPtr,
        "status-futex-supervisor-owned",
      );
      if (hostTimer !== null) {
        armSupervisorHostTimer(handle, hostTimer, deps);
      }
      deps.pumpProcess(handle);
      return;
    }

    if (workerStatusTransitionAction === "host-wait") {
      void executeHostBlockedStatusTransition(deps, handle, worker, thread.vmPtr, status)
        .then((outcome) => {
          consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
        })
        .catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }

    deps.enqueueWorker(handle, worker);
  };
}
