import { hostExecutionState } from "../host-execution-state.js";
import {
  processHostStdio,
  processStdioOwner,
} from "../process-stdio-state.js";
import { processWorkerList } from "../process-worker-state.js";
import {
  publishKernelRuntimeState,
} from "../../bridge/runtime-state.js";
import {
  supervisorBlockedPipeSyncBeforeRetryClear,
  supervisorBlockedStatusIsEpollWait,
  supervisorBlockedStatusIsPipeWait,
  supervisorBlockedWaitStatusMatchesWakeSource,
  supervisorStateSyncClear,
  supervisorStateSyncComplete,
  supervisorStateSyncNextRun,
  supervisorStateSyncRequest,
  type SupervisorBlockedWaitWakeSource,
} from "../../supervisor-blocked-resume.js";
import {
  collectHeldEpollDependentSyncOfdIds,
  collectHeldEpollOfdIds,
  collectProcessSyncDependentOfdIds,
} from "../ownership/ofd-table.js";
import { kernelWorkerOwnsEpollState } from "../ownership/epoll-control.js";
import { processHasKernelWorkerOwnership } from "../kernel-worker-ownership.js";
import { composeHandlePipeUsed } from "../ownership/pipe-control.js";
import { supervisorProcessPipeSlotForFd } from "../ownership/pipe-slot.js";
import {
  assertHandleWorkerProcessIdentity,
  supervisorVforkChildOwnerMismatch,
} from "../process-identity.js";
import {
  clearRuntimeStateSyncPromise,
  getRuntimeStateSyncPromise,
  setRuntimeStateSyncPromise,
} from "../state-sync-inflight.js";
import {
  enqueueOfdSyncIds,
  getOfdSyncPromise,
  queuedOfdSyncIdCount,
  setOfdSyncPromise,
  takeQueuedOfdSyncIds,
} from "../ofd-sync-state.js";
import { isBlockedRpcPending } from "../host-blocked-state.js";
import { getThreadWaitingInWorkerStatus } from "../thread-host-state.js";
import {
  syncEpollStateViaKernelWorker,
  syncOfdStateViaKernelWorker,
  syncPipeStateViaKernelWorker,
  syncSocketStateViaKernelWorker,
  applySocketStateFromKernelWorker,
} from "../sync.js";
import { kernelShareMemberList } from "../kernel-share.js";
import type { SyncCause } from "../contracts/sync.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import { transferDebugStats } from "../worker-debug-stats.js";
import type { WorkerSchedulerStateSyncHelpers } from "./worker-scheduler-types.js";

interface WorkerSchedulerStateSyncDeps {
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  drainProcessOutput(handle: ProcessHandle): boolean;
  drainHiddenProcessOutput(handle: ProcessHandle): boolean;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  pumpProcess?(handle: ProcessHandle): void;
  retryHostBlockedWorker(handle: ProcessHandle, worker: Worker): void;
}

export function createWorkerSchedulerStateSyncHelpers(
  deps: WorkerSchedulerStateSyncDeps,
): WorkerSchedulerStateSyncHelpers {
  function assertProcessIdentity(handle: ProcessHandle, context: string): void {
    assertHandleWorkerProcessIdentity(handle, deps.threadWorkerStates, context);
  }

  function visibleOutputHandle(handle: ProcessHandle): ProcessHandle {
    return processStdioOwner(handle) ?? handle;
  }

  function hasActiveVforkChildOwnerMismatch(handle: ProcessHandle): boolean {
    const executionState = hostExecutionState(handle);
    if (executionState.vforkWorker == null || handle.pid <= 0) {
      return false;
    }
    const thread = deps.threadWorkerStates.get(executionState.vforkWorker);
    if (!thread || thread.vmPtr === 0) {
      return false;
    }
    return supervisorVforkChildOwnerMismatch(handle.kernel, thread.vmPtr, handle.pid);
  }

  function shouldDrainHiddenInheritedOutput(
    handle: ProcessHandle,
    visibleHandle: ProcessHandle,
  ): boolean {
    if (visibleHandle === handle) {
      return false;
    }
    const visibleSlots = new Set(
      [
        processHostStdio(visibleHandle).stdoutPipe,
        processHostStdio(visibleHandle).stderrPipe,
      ].filter((slot) => slot > 0),
    );
    const fdPipeSlot = (fd: number): number => supervisorProcessPipeSlotForFd(handle, handle.vmPtr, fd);
    const hiddenSlots = [fdPipeSlot(1), fdPipeSlot(2)]
      .filter((slot, index, slots) => slot > 0 && slots.indexOf(slot) === index);
    if (hiddenSlots.length === 0) {
      return false;
    }
    return hiddenSlots.some((slot) => visibleSlots.has(slot));
  }

  function wakeBlockedWaitersInKernelShare(
    handle: ProcessHandle,
    wakeSource: SupervisorBlockedWaitWakeSource,
    pipeUsed?: bigint,
  ): void {
    for (const member of kernelShareMemberList(handle)) {
      if (pipeUsed !== undefined) {
        member.kernelState = {
          ...member.kernelState,
          pipeUsed: composeHandlePipeUsed(member, pipeUsed),
        };
      }
      const shouldWake = processWorkerList(member).some((candidate) => {
        const state = deps.threadWorkerStates.get(candidate);
        const waitingInWorkerStatus = getThreadWaitingInWorkerStatus(state);
        return state !== undefined
          && waitingInWorkerStatus !== null
          && supervisorBlockedWaitStatusMatchesWakeSource(
            member.kernel,
            waitingInWorkerStatus,
            wakeSource,
          );
      });
      if (shouldWake) {
        publishKernelRuntimeState(member.runtimeBridge, member.kernelState, true);
      }
      for (const [blockedWorker, blocked] of hostExecutionState(member).hostBlockedWorkers) {
        if (supervisorBlockedWaitStatusMatchesWakeSource(member.kernel, blocked.status, wakeSource)) {
          if (supervisorBlockedStatusIsPipeWait(member.kernel, blocked.status)) {
            if (isBlockedRpcPending(blocked)) {
              continue;
            }
            supervisorBlockedPipeSyncBeforeRetryClear(member.kernel, blocked.vmPtr);
          }
          if (supervisorBlockedStatusIsEpollWait(member.kernel, blocked.status)) {
            if (isBlockedRpcPending(blocked)) {
              continue;
            }
            const vmPtr = typeof blocked.vmPtr === "number"
              ? `0x${blocked.vmPtr.toString(16)}`
              : "n/a";
            deps.appendRuntimeLifecycleEvent(
              member.runtimeId,
              `epoll-wake-retry:${member.pid}:vm=${vmPtr}:status=${blocked.status}`,
            );
          }
          const runningBeforePump = hostExecutionState(member).runningWorker;
          deps.pumpProcess?.(member);
          if (!hostExecutionState(member).hostBlockedWorkers.has(blockedWorker)) {
            continue;
          }
          if (runningBeforePump === null && hostExecutionState(member).runningWorker !== null) {
            continue;
          }
          deps.retryHostBlockedWorker(member, blockedWorker);
        }
      }
    }
  }

  function wakePipeAndEpollWaitersInKernelShare(handle: ProcessHandle, pipeUsed: bigint): void {
    wakeBlockedWaitersInKernelShare(
      handle,
      "pipe-and-epoll-sync",
      pipeUsed,
    );
  }

  function wakePipeWaitersInKernelShare(handle: ProcessHandle, pipeUsed: bigint): void {
    wakeBlockedWaitersInKernelShare(
      handle,
      "pipe-sync",
      pipeUsed,
    );
  }

  function retryEpollWaitersInKernelShare(handle: ProcessHandle): void {
    wakeBlockedWaitersInKernelShare(
      handle,
      "epoll-sync",
    );
  }

  function syscallSyncCause(name: string): SyncCause {
    return { kind: "syscall", nr: -1n, name };
  }

  function blockedRpcSyncCause(status: number): SyncCause {
    return { kind: "blocked-rpc", status };
  }

  function wakeSocketWaitersInKernelShare(
    handle: ProcessHandle,
    socketUsed: number,
    netBufUsed: bigint,
  ): void {
    for (const member of kernelShareMemberList(handle)) {
      member.kernelState = {
        ...member.kernelState,
        socketUsed,
        netBufUsed,
      };
    }
    wakeBlockedWaitersInKernelShare(
      handle,
      "socket-sync",
    );
  }

  function requestEpollStateSync(handle: ProcessHandle, retryWaiters = false): Promise<void> {
    if (hasActiveVforkChildOwnerMismatch(handle)) {
      return Promise.resolve();
    }
    if (!kernelWorkerOwnsEpollState(handle)) {
      return Promise.resolve();
    }
    if (collectHeldEpollOfdIds(handle).length === 0) {
      if (retryWaiters) {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `epoll-sync-skip-no-ofd:${handle.pid}:retry=1`);
        retryEpollWaitersInKernelShare(handle);
      }
      return Promise.resolve();
    }

    const shouldStart = supervisorStateSyncRequest(
      handle.kernel,
      handle.vmPtr,
      "epoll",
      retryWaiters,
    );
    const activeSync = getRuntimeStateSyncPromise(handle, "epoll");
    if (activeSync !== null) {
      return activeSync;
    }
    if (!shouldStart) {
      return Promise.resolve();
    }

    const syncPromise = (async () => {
      try {
        for (;;) {
          const run = supervisorStateSyncNextRun(
            handle.kernel,
            handle.vmPtr,
            "epoll",
          );
          if (!run.run) {
            break;
          }
          const shouldRetry = run.wakeWaiters;
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `epoll-sync-start:${handle.pid}:retry=${shouldRetry ? 1 : 0}`);
          await syncEpollStateViaKernelWorker(
            handle,
            shouldRetry
              ? blockedRpcSyncCause(handle.kernel.abi_status_epoll_wait())
              : syscallSyncCause("epoll-state-sync"),
          );
          assertProcessIdentity(handle, "requestEpollStateSync");
          if (shouldRetry) {
            retryEpollWaitersInKernelShare(handle);
          }
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `epoll-sync-done:${handle.pid}:retry=${shouldRetry ? 1 : 0}`);
          if (!supervisorStateSyncComplete(
            handle.kernel,
            handle.vmPtr,
            "epoll",
          )) {
            break;
          }
        }
      } catch (error) {
        supervisorStateSyncClear(handle.kernel, handle.vmPtr, "epoll");
        throw error;
      }
    })().finally(() => {
      clearRuntimeStateSyncPromise(handle, "epoll");
    });
    setRuntimeStateSyncPromise(handle, "epoll", syncPromise);

    return syncPromise;
  }

  function requestEpollDependentOfdStateSync(
    handle: ProcessHandle,
    additionalOfdIds: readonly number[] = [],
    options: { mode?: "push" | "pull" } = {},
  ): Promise<void> {
    if (hasActiveVforkChildOwnerMismatch(handle)) {
      return Promise.resolve();
    }
    if (handle.pid <= 0) {
      return Promise.resolve();
    }

    const dependentOfdIds = new Set([
      ...collectProcessSyncDependentOfdIds(handle),
      ...collectHeldEpollDependentSyncOfdIds(handle),
      ...additionalOfdIds,
    ]);
    if (options.mode === "pull") {
      const ofdIds = [...dependentOfdIds];
      if (ofdIds.length === 0) {
        return Promise.resolve();
      }
      return (async () => {
        const activeOfdSync = getOfdSyncPromise(handle);
        if (activeOfdSync !== null) {
          await activeOfdSync;
        }
        await syncOfdStateViaKernelWorker(
          handle,
          ofdIds,
          syscallSyncCause("epoll-dependent-ofd-sync"),
          { mode: "pull" },
        );
        assertProcessIdentity(handle, "requestEpollDependentOfdStateSync");
      })();
    }
    enqueueOfdSyncIds(handle, dependentOfdIds);
    if (queuedOfdSyncIdCount(handle) === 0) {
      return Promise.resolve();
    }
    const activeOfdSync = getOfdSyncPromise(handle);
    if (activeOfdSync !== null) {
      return activeOfdSync;
    }

    const ofdSyncPromise = (async () => {
      while (queuedOfdSyncIdCount(handle) > 0) {
        const ofdIds = takeQueuedOfdSyncIds(handle);
        await syncOfdStateViaKernelWorker(handle, ofdIds, syscallSyncCause("epoll-dependent-ofd-sync"));
        assertProcessIdentity(handle, "requestEpollDependentOfdStateSync");
      }
    })().finally(() => {
      setOfdSyncPromise(handle, null);
    });
    setOfdSyncPromise(handle, ofdSyncPromise);

    return ofdSyncPromise;
  }

  function requestPipeStateSync(handle: ProcessHandle, wakeWaiters = false): Promise<void> {
    transferDebugStats(handle).pipeSyncRequests += 1;
    if (hasActiveVforkChildOwnerMismatch(handle)) {
      return Promise.resolve();
    }
    if (!processHasKernelWorkerOwnership(handle)) {
      return Promise.resolve();
    }

    const shouldStart = supervisorStateSyncRequest(
      handle.kernel,
      handle.vmPtr,
      "pipe",
      wakeWaiters,
    );
    const activeSync = getRuntimeStateSyncPromise(handle, "pipe");
    if (activeSync !== null) {
      return activeSync;
    }
    if (!shouldStart) {
      return Promise.resolve();
    }

    const syncPromise = (async () => {
      try {
        for (;;) {
          const run = supervisorStateSyncNextRun(
            handle.kernel,
            handle.vmPtr,
            "pipe",
          );
          if (!run.run) {
            break;
          }
          const shouldWake = run.wakeWaiters;
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-start:${handle.pid}:wake=${shouldWake ? 1 : 0}`);
          transferDebugStats(handle).pipeSyncKernelCalls += 1;
          const pipeUsed = await syncPipeStateViaKernelWorker(
            handle,
            shouldWake
              ? blockedRpcSyncCause(handle.kernel.abi_status_pipe_wait())
              : syscallSyncCause("pipe-state-sync"),
          );
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-canonical:${handle.pid}:used=${pipeUsed.toString()}`);
          assertProcessIdentity(handle, "requestPipeStateSync");
          const visibleHandle = visibleOutputHandle(handle);
          const drainedHiddenOutput = shouldDrainHiddenInheritedOutput(handle, visibleHandle)
            ? deps.drainHiddenProcessOutput(handle)
            : false;
          const drainedVisibleOutput = drainedHiddenOutput
            ? false
            : deps.drainProcessOutput(visibleHandle);
          if (drainedHiddenOutput) {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-hidden-drain:${handle.pid}`);
            transferDebugStats(handle).pipeSyncKernelCalls += 1;
            const refreshedPipeUsed = await syncPipeStateViaKernelWorker(
              handle,
              syscallSyncCause("pipe-hidden-output-drain"),
            );
            assertProcessIdentity(handle, "requestPipeStateSync:after-hidden-drain");
            handle.kernelState = {
              ...handle.kernelState,
              pipeUsed: refreshedPipeUsed,
            };
          }
          if (
            drainedVisibleOutput
            && processHasKernelWorkerOwnership(visibleHandle)
          ) {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-visible-drain:${visibleHandle.pid}`);
            transferDebugStats(handle).pipeSyncVisibleDrainCalls += 1;
            transferDebugStats(visibleHandle).pipeSyncKernelCalls += 1;
            const visiblePipeUsed = await syncPipeStateViaKernelWorker(
              visibleHandle,
              syscallSyncCause("pipe-visible-output-drain"),
            );
            assertProcessIdentity(visibleHandle, "requestPipeStateSync:after-visible-drain");
            visibleHandle.kernelState = {
              ...visibleHandle.kernelState,
              pipeUsed: visiblePipeUsed,
            };
          }
          if (shouldWake) {
            wakePipeWaitersInKernelShare(handle, pipeUsed);
            if (kernelWorkerOwnsEpollState(handle)) {
              await requestEpollDependentOfdStateSync(handle);
              await requestEpollStateSync(handle, true);
            } else {
              wakeBlockedWaitersInKernelShare(
                handle,
                "epoll-sync",
                pipeUsed,
              );
            }
          }
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-done:${handle.pid}:wake=${shouldWake ? 1 : 0}`);
          if (!supervisorStateSyncComplete(
            handle.kernel,
            handle.vmPtr,
            "pipe",
          )) {
            break;
          }
        }
      } catch (error) {
        supervisorStateSyncClear(handle.kernel, handle.vmPtr, "pipe");
        throw error;
      }
    })().finally(() => {
      clearRuntimeStateSyncPromise(handle, "pipe");
    });
    setRuntimeStateSyncPromise(handle, "pipe", syncPromise);

    return syncPromise;
  }

  function requestSocketStateSync(handle: ProcessHandle, wakeWaiters = false): Promise<void> {
    transferDebugStats(handle).socketSyncRequests += 1;
    if (hasActiveVforkChildOwnerMismatch(handle)) {
      return Promise.resolve();
    }
    if (!processHasKernelWorkerOwnership(handle)) {
      return Promise.resolve();
    }

    const shouldStart = supervisorStateSyncRequest(
      handle.kernel,
      handle.vmPtr,
      "socket",
      wakeWaiters,
    );
    const activeSync = getRuntimeStateSyncPromise(handle, "socket");
    if (activeSync !== null) {
      return activeSync;
    }
    if (!shouldStart) {
      return Promise.resolve();
    }

    const syncPromise = (async () => {
      try {
        for (;;) {
          const run = supervisorStateSyncNextRun(
            handle.kernel,
            handle.vmPtr,
            "socket",
          );
          if (!run.run) {
            break;
          }
          const shouldWake = run.wakeWaiters;
          transferDebugStats(handle).socketSyncKernelCalls += 1;
          const { socketUsed, netBufUsed, socketState } = await syncSocketStateViaKernelWorker(
            handle,
            shouldWake
              ? blockedRpcSyncCause(handle.kernel.abi_status_socket_recv())
              : syscallSyncCause("socket-state-sync"),
          );
          assertProcessIdentity(handle, "requestSocketStateSync");
          if (shouldWake) {
            if (socketState) {
              for (const member of kernelShareMemberList(handle)) {
                if (member !== handle && processHasKernelWorkerOwnership(member)) {
                  applySocketStateFromKernelWorker(member, socketState);
                }
              }
            }
            wakeSocketWaitersInKernelShare(handle, socketUsed, netBufUsed);
          }
          if (!supervisorStateSyncComplete(
            handle.kernel,
            handle.vmPtr,
            "socket",
          )) {
            break;
          }
        }
      } catch (error) {
        supervisorStateSyncClear(handle.kernel, handle.vmPtr, "socket");
        throw error;
      }
    })().finally(() => {
      clearRuntimeStateSyncPromise(handle, "socket");
    });
    setRuntimeStateSyncPromise(handle, "socket", syncPromise);

    return syncPromise;
  }

  return {
    requestEpollDependentOfdStateSync,
    requestEpollStateSync,
    requestPipeStateSync,
    requestSocketStateSync,
    wakePipeAndEpollWaitersInKernelShare,
  };
}
