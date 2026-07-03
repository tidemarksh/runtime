import { readKernelRuntimeState } from "../../../abi.js";
import { publishKernelRuntimeState, wakeRuntimeBridge } from "../../../bridge/runtime-state.js";
import {
  acquireWorker,
  releaseWorker,
} from "../../pool.js";
import { kernelShareOtherMembers } from "../../kernel-share.js";
import { assertHandleWorkerProcessIdentity } from "../../process-identity.js";
import type {
  HostBlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../../types.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "../types.js";
import { resolveWorkerMessageHandlerSyncEffects } from "../sync-effects.js";
import type { KernelRpcResumeOutcome } from "../../kernel-rpc/outcomes.js";
import { createWorkerOwnershipGuard } from "../../ownership/process-state.js";
import {
  refreshPipeControlFromKernelShare,
  shouldRefreshPipeStateBeforeFsResume,
  snapshotProcessPipeSlotsForThreadStep,
} from "../../ownership/pipe-control.js";
import { restoreRuntimeNetworkSidecarForHandle } from "../../ownership/socket-control.js";
import { formatCompactDebugSuffix } from "../../diagnostics/format.js";
import {
  pollSupervisorFdControlDiagnostic,
  pollSupervisorFsControlDiagnostic,
  pollSupervisorSignalControlDiagnostic,
  readSupervisorFaultExitDecision,
  supervisorFaultExitKindProcessExit,
} from "../../supervisor-shadow.js";
import {
  armSupervisorHostTimer,
  drainSupervisorArmHostTimerHostOp,
  drainSupervisorCloneSpawnHostOp,
} from "../../supervisor-host-op.js";
import {
  supervisorBlockedPipeWaitHasReadyEvents,
  supervisorBlockedPollWaitHasSocket,
  supervisorBlockedPollWaitTimeoutMs,
  supervisorBlockedStatusIsConnectWait,
  supervisorBlockedStatusUsesForkSharedStateSync,
  supervisorBlockedStatusIsHostWait,
  supervisorBlockedStatusIsPipeWait,
  supervisorBlockedStatusIsWait4,
  supervisorBlockedStatusNeedsNetworkSidecarRestore,
  supervisorBlockedWaitStatusMatchesPipeWait,
  supervisorBlockedWaitStatusMatchesSocketWait,
  supervisorBlockedWaitTimerRequired,
} from "../../../supervisor-blocked-resume.js";
import {
  supervisorProcessStatusAllowsPidMismatch,
  supervisorProcessStatusIsCloneSpawn,
  supervisorProcessStatusIsExecveSpawn,
  supervisorProcessStatusIsVforkWait,
} from "../../supervisor-status.js";

const CLONE_SPAWN_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_CLONE_SPAWN_DEBUG ?? "") === "1";

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

function beginStatusTransitionRpc(blocked: HostBlockedWorkerState): number {
  blocked.rpcPending = true;
  blocked.rpcGeneration += 1;
  return blocked.rpcGeneration;
}

function finishStatusTransitionRpc(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  rpcGeneration: number,
): HostBlockedWorkerState | null {
  const latestBlocked = handle.hostBlockedWorkers.get(worker);
  if (!latestBlocked || latestBlocked.vmPtr !== vmPtr) {
    return null;
  }
  if (latestBlocked.rpcGeneration === rpcGeneration) {
    latestBlocked.rpcPending = false;
  }
  return latestBlocked;
}

function hasPendingBlockedRpc(handle: ProcessHandle): boolean {
  for (const blocked of handle.hostBlockedWorkers.values()) {
    if (blocked.rpcPending) {
      return true;
    }
  }
  return false;
}

export function createWorkerStatusTransitionHandler(deps: WorkerMessageHandlerDeps) {
  const syncEffects = resolveWorkerMessageHandlerSyncEffects(deps);
  function consumeKernelRpcResumeOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    outcome: KernelRpcResumeOutcome,
  ): void {
    switch (outcome.kind) {
      case "completed":
      case "queued-in-worker":
      case "process-gone":
      case "stale":
        return;
      case "blocked": {
        const blocked = handle.hostBlockedWorkers.get(worker);
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
        workerLogTail: handle.workerDebugLog.slice(-16),
        skipVmPtrs: [handle.vforkSnapshot?.vmPtr ?? 0].filter((vmPtr) => vmPtr !== 0),
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

    const enqueueWorkerAfterOk = (): void => {
      if (hasPendingBlockedRpc(handle)) {
        setTimeout(() => {
          if (!workerStillOwnedByHandle()) {
            deps.pumpProcess(handle);
            return;
          }
          enqueueWorkerAfterOk();
        }, 1);
        return;
      }
      deps.enqueueWorker(handle, worker);
    };

    const syncSocketAfterNetworkPump = (networkChanged: boolean): Promise<void> => {
      if (!networkChanged) {
        return Promise.resolve();
      }
      return deps.requestSocketStateSync(handle, true);
    };

    if (status === handle.kernel.abi_status_ok()) {
      const shouldSyncPipeState = wroteToPipe
        || drainedPipe
        || (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0;
      const shouldSyncSocketState = (completedSyncEffects & syncEffects.syncEffectSocketState) !== 0;
      const shouldSyncLifecycle = (completedSyncEffects & syncEffects.syncEffectProcessLifecycle) !== 0;
      const shouldWakeSignalWaiters = (completedSyncEffects & syncEffects.syncEffectSignalWaiters) !== 0;
      if (
        handle.kernelWorkerRequest
        && handle.pid > 0
        && (shouldSyncPipeState || shouldSyncSocketState || shouldSyncLifecycle)
      ) {
        deps.appendWorkerDebugLog(handle, `ok-sync-start:0x${thread.vmPtr.toString(16)}`);
        thread.ownerAsyncStatus = status;
        const syncPipeState = shouldSyncPipeState
          ? deps.requestPipeStateSync(handle, true)
          : Promise.resolve();
        const syncSocketState = shouldSyncSocketState
          ? deps.requestSocketStateSync(handle, true)
          : Promise.resolve();
        void syncPipeState
          .then(() => {
            assertHandleProcessIdentity("handleStatusTransition:after-pipe-sync");
          })
          .then(() => syncSocketState)
          .then(() => {
            assertHandleProcessIdentity("handleStatusTransition:after-socket-sync");
          })
          .then(() => (shouldSyncLifecycle
            ? deps.syncProcessLifecycleViaKernelWorker(handle, thread.vmPtr)
            : Promise.resolve()))
          .then(() => {
            assertHandleProcessIdentity("handleStatusTransition:after-lifecycle-sync");
            if (!workerStillOwnedByHandle()) {
              return;
            }
            if (shouldSyncLifecycle) {
              deps.pruneDetachedProcessWorkers(handle, thread.vmPtr, worker);
              assertHandleProcessIdentity("handleStatusTransition:after-prune-detached");
            }
            deps.appendWorkerDebugLog(handle, `ok-sync-done:0x${thread.vmPtr.toString(16)}`);
            return handle.networkConnections.size > 0
              ? deps.pumpNetworkConnections(handle, thread.vmPtr)
              : Promise.resolve(false);
          })
          .then((networkChanged) => syncSocketAfterNetworkPump(networkChanged ?? false))
          .then(() => {
            thread.ownerAsyncStatus = null;
            if (!workerStillOwnedByHandle()) {
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
            enqueueWorkerAfterOk();
          })
          .catch((error) => {
            thread.ownerAsyncStatus = null;
            if (!workerStillOwnedByHandle()) {
              deps.pumpProcess(handle);
              return;
            }
            deps.appendWorkerDebugLog(handle, `ok-sync-err:0x${thread.vmPtr.toString(16)}`);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (shouldSyncLifecycle) {
        deps.pruneDetachedProcessWorkers(handle, thread.vmPtr, worker);
      }
      if (shouldWakeSignalWaiters) {
        wakeRuntimeBridge(handle.runtimeBridge);
      }
      if (handle.networkConnections.size > 0) {
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
      if (
        handle.vforkSnapshot !== null
        && handle.vforkWorker === worker
        && handle.vforkSnapshot.vmPtr === thread.vmPtr
      ) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `vfork-child-fault:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:child=${handle.vforkSnapshot.childPid ?? "none"}`
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
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `fault-worker-delete:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:status=${status}:msg=${msg.status}:exit=${msg.exitCode}:${msg.termSignal}`
            + `:current=${handle.kernel.thread_status?.(thread.vmPtr) ?? -1}`
            + `:remaining=${remainingThreads}:live=${hasLiveThreads ? 1 : 0}`
            + processTableDebug
            + `:threads=[${formatProcessThreadSlots(handle, thread.vmPtr)}]`,
        );
        if (handle.runningWorker === worker) {
          handle.runningWorker = null;
        }
        handle.workers.delete(worker);
        deps.threadWorkerStates.delete(worker);
        releaseWorker(worker);
        if (wroteToPipe || drainedPipe || (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0) {
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

    const shouldSyncPipeState = wroteToPipe
      || drainedPipe
      || (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0;
    const pendingHostResume = status === handle.kernel.abi_status_fs();
    if (shouldSyncPipeState && !pendingHostResume) {
      void deps.requestPipeStateSync(handle, true).catch((error) => {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    }
    if (supervisorBlockedStatusNeedsNetworkSidecarRestore(handle.kernel, status)) {
      restoreRuntimeNetworkSidecarForHandle(handle, deps.processHandles, thread.vmPtr);
    }

    if (
      handle.networkConnections.size > 0
      && !supervisorBlockedStatusIsConnectWait(handle.kernel, status)
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
      const sec = handle.kernel.thread_sleep_seconds(thread.vmPtr);
      const nsec = handle.kernel.thread_sleep_nanoseconds(thread.vmPtr);
      if (sec === 0n && nsec === 0n) {
        deps.appendWorkerDebugLog(handle, `yield:0x${thread.vmPtr.toString(16)}`);
        const clearYieldingWorker = (): void => {
          if (handle.runningWorker === worker) {
            handle.runningWorker = null;
          }
          thread.waitingInWorkerStatus = null;
        };
        const shouldSyncPipeState = (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0;
        if (handle.kernelWorkerRequest && handle.pid > 0 && shouldSyncPipeState) {
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
      deps.appendWorkerDebugLog(handle, `sleep:0x${thread.vmPtr.toString(16)}@${sec.toString()}s+${nsec.toString()}ns`);
      const hostTimer = drainSupervisorArmHostTimerHostOp(handle, thread.vmPtr);
      if (hostTimer === undefined) {
        throw new Error("kernel supervisor ABI missing arm host timer HostOp exports");
      }
      if (handle.runningWorker === worker) {
        handle.runningWorker = null;
      }
      thread.waitingInWorkerStatus = status;
      if (hostTimer !== null) {
        armSupervisorHostTimer(handle, hostTimer, deps);
      }
      deps.notifyWorkerStatusProgress(handle);
      deps.pumpProcess(handle);
      return;
    }

    if (status === handle.kernel.abi_status_fs()) {
      deps.appendWorkerDebugLog(handle, `fs:0x${thread.vmPtr.toString(16)}`);
      if (shouldRefreshPipeStateBeforeFsResume(handle, thread.vmPtr)) {
        thread.ownerAsyncStatus = status;
        void deps.requestPipeStateSync(handle, false)
          .then(() => {
            refreshPipeControlFromKernelShare(handle);
            const pipeSlotsForNextStep = snapshotProcessPipeSlotsForThreadStep(handle, thread.vmPtr);
            if (pipeSlotsForNextStep.length > 0) {
              thread.pendingPipeSlotsForNextStep.push(...pipeSlotsForNextStep);
            }
            if (!workerStillOwnedByHandle()) {
              thread.ownerAsyncStatus = null;
              return;
            }
            const resumeFs = deps.resumeFsViaKernelWorkerOutcome ?? deps.resumeFsViaKernelWorker;
            return resumeFs(handle, worker, thread.vmPtr)
              .then((outcome) => {
                if (outcome !== undefined) {
                  consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
                }
              })
              .then(() => undefined)
              .finally(() => {
                thread.ownerAsyncStatus = null;
                deps.pumpProcess(handle);
              });
          })
          .catch((error) => {
            thread.ownerAsyncStatus = null;
            if (!workerStillOwnedByHandle()) {
              return;
            }
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (!handle.kernelWorkerRequest || handle.pid <= 0) {
        const resumeFs = deps.resumeFsViaKernelWorkerOutcome ?? deps.resumeFsViaKernelWorker;
        void resumeFs(handle, worker, thread.vmPtr)
          .then((outcome) => {
            if (outcome !== undefined) {
              consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
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
      thread.ownerAsyncStatus = status;
      const resumeFs = deps.resumeFsViaKernelWorkerOutcome ?? deps.resumeFsViaKernelWorker;
      void resumeFs(handle, worker, thread.vmPtr)
        .then((outcome) => {
          if (outcome !== undefined) {
            consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
          }
        })
        .finally(() => {
          thread.ownerAsyncStatus = null;
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

    if (supervisorBlockedStatusIsConnectWait(handle.kernel, status)) {
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
      if (!handle.kernelWorkerRequest || handle.pid <= 0) {
        void deps.resumeConnectViaKernelWorker(
          handle,
          worker,
          thread.vmPtr,
          BigInt(-handle.kernel.abi_econnrefused()),
        ).then((outcome) => {
          if (outcome !== undefined) {
            consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
          }
        }).catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
        deps.notifyWorkerStatusProgress(handle);
        deps.enqueueWorker(handle, worker);
        return;
      }
      void deps.requestSocketStateSync(handle, false)
        .then(() => deps.resumeConnectViaKernelWorker(
          handle,
          worker,
          thread.vmPtr,
          BigInt(-handle.kernel.abi_econnrefused()),
        ))
        .then((outcome) => {
          if (outcome !== undefined) {
            consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
          }
          deps.notifyWorkerStatusProgress(handle);
          deps.enqueueWorker(handle, worker);
        })
        .catch((error) => {
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
      const spawnCloneChild = (): boolean => {
        const hostOp = drainSupervisorCloneSpawnHostOp(handle, thread.vmPtr);
        if (hostOp === null || hostOp === undefined) {
          childVmPtr = 0;
        } else {
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
        thread.ownerAsyncStatus = status;
        childThread.resumeWorkersOnReady.push(worker);
        deps.notifyWorkerStatusProgress(handle);
        if (CLONE_SPAWN_DEBUG_ENABLED) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `clone-spawn-attached:${handle.pid}:parent=0x${thread.vmPtr.toString(16)}`
              + `:child=0x${childVmPtr.toString(16)}:workers=${handle.workers.size}`
              + `:threads=[${formatProcessThreadSlots(handle, thread.vmPtr)}]`,
          );
        }
      };

      if (handle.kernelWorkerRequest && handle.pid > 0) {
        thread.ownerAsyncStatus = status;
        const cloneSpawnAsyncToken = (cloneSpawnAsyncTokens.get(thread) ?? 0) + 1;
        cloneSpawnAsyncTokens.set(thread, cloneSpawnAsyncToken);
        const isCurrentCloneSpawnAsync = (): boolean =>
          cloneSpawnAsyncTokens.get(thread) === cloneSpawnAsyncToken
          && (thread.ownerAsyncStatus ?? null) === status;
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
              thread.ownerAsyncStatus = null;
              return;
            }
            finishCloneSpawn();
          })
          .catch((error) => {
            const currentCloneSpawnAsync = isCurrentCloneSpawnAsync();
            if (currentCloneSpawnAsync) {
              thread.ownerAsyncStatus = null;
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

      if (!spawnCloneChild()) {
        return;
      }
      finishCloneSpawn();
      return;
    }

    if (supervisorProcessStatusIsExecveSpawn(handle.kernel, status)) {
      thread.ownerAsyncStatus = status;
      void deps.handleExecveSpawn(handle, worker, thread)
        .finally(() => {
          if (workerStillOwnedByHandle()) {
            thread.ownerAsyncStatus = null;
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
      if (handle.vforkSnapshot === null) {
        handle.vforkSnapshot = deps.captureVforkSnapshot(handle, thread);
        handle.vforkWorker = worker;
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `vfork-wait-snapshot:capture:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:child=${handle.vforkSnapshot.childPid ?? "none"}`
            + `:kernel_child=${handle.kernel.thread_vfork_child_pid()}`,
        );
      } else {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `vfork-wait-snapshot:reuse:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:snapshot=0x${handle.vforkSnapshot.vmPtr.toString(16)}`
            + `:child=${handle.vforkSnapshot.childPid ?? "none"}`
            + `:worker=${handle.vforkWorker === worker ? "match" : "mismatch"}`
            + `:kernel_child=${handle.kernel.thread_vfork_child_pid()}`,
        );
      }
      if (
        handle.kernelWorkerRequest
        && handle.pid > 0
        && handle.vforkSnapshot.childPid === null
      ) {
        deps.pumpProcess(handle);
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
            if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
              return;
            }
            if (!handle.vforkSnapshot || handle.vforkSnapshot.vmPtr !== thread.vmPtr) {
              return;
            }
            if (handle.vforkSnapshot.childPid === null) {
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
                      + `:child=${provisionalChildPid}:outcome=${outcome?.kind ?? "none"}`,
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
      if (
        handle.kernelWorkerRequest
        && handle.pid > 0
        && handle.vforkSnapshot.childPid === null
      ) {
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
                + `:child=${provisionalChildPid}:outcome=${outcome?.kind ?? "none"}`,
            );
            if (outcome !== undefined) {
              consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
            }
          });
          return;
        }
      }
      deps.enqueueWorker(handle, worker);
      return;
    }

    if (status === handle.kernel.abi_status_futex_wait()) {
      const hostTimer = drainSupervisorArmHostTimerHostOp(handle, thread.vmPtr);
      if (hostTimer === undefined) {
        throw new Error("kernel supervisor ABI missing arm host timer HostOp exports");
      }
      if (handle.runningWorker === worker) {
        handle.runningWorker = null;
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

    if (supervisorBlockedStatusIsHostWait(handle.kernel, status)) {
      const pipeWaitStatus = supervisorBlockedStatusIsPipeWait(handle.kernel, status);
      const finitePollTimeout = pipeWaitStatus
        && supervisorBlockedPollWaitTimeoutMs(handle.kernel, thread.vmPtr) >= 0;
      const pipeWaitHasSocket = pipeWaitStatus
        && handle.networkConnections.size > 0
        && supervisorBlockedPollWaitHasSocket(handle.kernel, thread.vmPtr);
      if (
        pipeWaitHasSocket
        && handle.kernelWorkerRequest
        && handle.pid > 0
      ) {
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        const blocked = handle.hostBlockedWorkers.get(worker);
        const rpcGeneration = blocked ? beginStatusTransitionRpc(blocked) : -1;
        void deps.requestSocketStateSync(handle, true)
          .then(() => {
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            deps.notifyWorkerStatusProgress(handle);
            deps.pumpProcess(handle);
          })
          .catch((error) => {
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (
        supervisorBlockedStatusIsWait4(handle.kernel, status)
        && handle.kernelWorkerRequest
        && handle.pid > 0
      ) {
        if (handle.runningWorker === worker) {
          handle.runningWorker = null;
        }
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        if (handle.runningWorker === worker) {
          handle.runningWorker = null;
        }
        const blocked = handle.hostBlockedWorkers.get(worker);
        const rpcGeneration = blocked ? beginStatusTransitionRpc(blocked) : -1;
        void deps.requestPipeStateSync(handle, true)
          .then(() => {
            const latestBlocked = finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            if (!latestBlocked) {
              return;
            }
            deps.notifyWorkerStatusProgress(handle);
            deps.pumpProcess(handle);
          })
          .catch((error) => {
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (
        supervisorBlockedStatusUsesForkSharedStateSync(handle.kernel, status)
        && !pipeWaitHasSocket
        && !finitePollTimeout
        && handle.kernelWorkerRequest
        && handle.pid > 0
      ) {
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        const blocked = handle.hostBlockedWorkers.get(worker);
        const rpcGeneration = blocked ? beginStatusTransitionRpc(blocked) : -1;
        const syncPipeState = supervisorBlockedWaitStatusMatchesPipeWait(handle.kernel, status)
          ? deps.requestPipeStateSync(handle, false)
          : Promise.resolve();
        const socketWaitStatus = supervisorBlockedWaitStatusMatchesSocketWait(handle.kernel, status);
        const syncSocketState = socketWaitStatus
          ? deps.requestSocketStateSync(handle, true)
          : Promise.resolve();
        void syncPipeState
          .then(() => syncSocketState)
          .then(() => {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-start:${handle.pid}:status=${status}`);
            return deps.syncBlockedForkSharedStateViaKernelWorker(
              handle,
              thread.vmPtr,
              { kind: "blocked-rpc", status },
            );
          })
          .then(() => {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-done:${handle.pid}:status=${status}`);
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
              deps.clearBlockedWorker(handle, worker);
              return;
            }
            if (
              pipeWaitStatus
              && !supervisorBlockedPipeWaitHasReadyEvents(handle.kernel, thread.vmPtr)
            ) {
              deps.pumpProcess(handle);
              return;
            }
            deps.notifyWorkerStatusProgress(handle);
            deps.pumpProcess(handle);
          })
          .catch((error) => {
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
        return;
      }
      deps.blockWorker(handle, worker, status, thread.vmPtr);
      deps.pumpProcess(handle);
      return;
    }

    deps.enqueueWorker(handle, worker);
  };
}
