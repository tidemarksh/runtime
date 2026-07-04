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
import { composeHandlePipeUsed, kernelWorkerOwnsPipeState } from "../ownership/pipe-control.js";
import { supervisorProcessPipeSlotForFd } from "../ownership/pipe-slot.js";
import { kernelWorkerOwnsSocketState } from "../ownership/socket-control.js";
import {
  assertHandleWorkerProcessIdentity,
  supervisorVforkChildOwnerMismatch,
} from "../process-identity.js";
import {
  syncEpollStateViaKernelWorker,
  syncOfdStateViaKernelWorker,
  syncPipeStateViaKernelWorker,
  syncSocketStateViaKernelWorker,
  applySocketStateFromKernelWorker,
} from "../sync.js";
import type { SyncCause } from "../contracts/sync.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type { WorkerSchedulerStateSyncHelpers } from "../scheduler/types.js";

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
    return handle.stdioOwner ?? handle;
  }

  function hasActiveVforkChildOwnerMismatch(handle: ProcessHandle): boolean {
    if (handle.vforkWorker == null || handle.pid <= 0) {
      return false;
    }
    const thread = deps.threadWorkerStates.get(handle.vforkWorker);
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
        visibleHandle.hostStdio.stdoutPipe,
        visibleHandle.hostStdio.stderrPipe,
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
    for (const member of handle.kernelShare.members) {
      if (pipeUsed !== undefined) {
        member.kernelState = {
          ...member.kernelState,
          pipeUsed: composeHandlePipeUsed(member, pipeUsed),
        };
      }
      const shouldWake = [...member.workers].some((candidate) => {
        const state = deps.threadWorkerStates.get(candidate);
        return state !== undefined
          && state.waitingInWorkerStatus !== null
          && supervisorBlockedWaitStatusMatchesWakeSource(
            member.kernel,
            state.waitingInWorkerStatus,
            wakeSource,
          );
      });
      if (shouldWake) {
        publishKernelRuntimeState(member.runtimeBridge, member.kernelState, true);
      }
      for (const [blockedWorker, blocked] of member.hostBlockedWorkers) {
        if (supervisorBlockedWaitStatusMatchesWakeSource(member.kernel, blocked.status, wakeSource)) {
          if (supervisorBlockedStatusIsPipeWait(member.kernel, blocked.status)) {
            if (blocked.rpcPending) {
              continue;
            }
            supervisorBlockedPipeSyncBeforeRetryClear(member.kernel, blocked.vmPtr);
          }
          if (supervisorBlockedStatusIsEpollWait(member.kernel, blocked.status)) {
            if (blocked.rpcPending) {
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
          const runningBeforePump = member.runningWorker;
          deps.pumpProcess?.(member);
          if (!member.hostBlockedWorkers.has(blockedWorker)) {
            continue;
          }
          if (runningBeforePump === null && member.runningWorker !== null) {
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
    for (const member of handle.kernelShare.members) {
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
    if (handle.epollSyncPromise !== null) {
      return handle.epollSyncPromise;
    }
    if (!shouldStart) {
      return Promise.resolve();
    }

    handle.epollSyncPromise = (async () => {
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
      handle.epollSyncPromise = null;
    });

    return handle.epollSyncPromise;
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
        if (handle.ofdSyncPromise !== null) {
          await handle.ofdSyncPromise;
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
    for (const ofdId of dependentOfdIds) {
      handle.ofdSyncQueuedIds.add(ofdId);
    }
    if (handle.ofdSyncQueuedIds.size === 0) {
      return Promise.resolve();
    }
    if (handle.ofdSyncPromise !== null) {
      return handle.ofdSyncPromise;
    }

    handle.ofdSyncPromise = (async () => {
      while (handle.ofdSyncQueuedIds.size > 0) {
        const ofdIds = [...handle.ofdSyncQueuedIds];
        handle.ofdSyncQueuedIds.clear();
        await syncOfdStateViaKernelWorker(handle, ofdIds, syscallSyncCause("epoll-dependent-ofd-sync"));
        assertProcessIdentity(handle, "requestEpollDependentOfdStateSync");
      }
    })().finally(() => {
      handle.ofdSyncPromise = null;
    });

    return handle.ofdSyncPromise;
  }

  function requestPipeStateSync(handle: ProcessHandle, wakeWaiters = false): Promise<void> {
    handle.transferDebugStats.pipeSyncRequests += 1;
    if (hasActiveVforkChildOwnerMismatch(handle)) {
      return Promise.resolve();
    }

    const shouldStart = supervisorStateSyncRequest(
      handle.kernel,
      handle.vmPtr,
      "pipe",
      wakeWaiters,
    );
    if (handle.pipeSyncPromise !== null) {
      return handle.pipeSyncPromise;
    }
    if (!shouldStart) {
      return Promise.resolve();
    }

    handle.pipeSyncPromise = (async () => {
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
          handle.transferDebugStats.pipeSyncKernelCalls += 1;
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
            handle.transferDebugStats.pipeSyncKernelCalls += 1;
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
            && kernelWorkerOwnsPipeState(visibleHandle)
          ) {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-visible-drain:${visibleHandle.pid}`);
            handle.transferDebugStats.pipeSyncVisibleDrainCalls += 1;
            visibleHandle.transferDebugStats.pipeSyncKernelCalls += 1;
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
      handle.pipeSyncPromise = null;
    });

    return handle.pipeSyncPromise;
  }

  function requestSocketStateSync(handle: ProcessHandle, wakeWaiters = false): Promise<void> {
    handle.transferDebugStats.socketSyncRequests += 1;
    if (hasActiveVforkChildOwnerMismatch(handle)) {
      return Promise.resolve();
    }

    const shouldStart = supervisorStateSyncRequest(
      handle.kernel,
      handle.vmPtr,
      "socket",
      wakeWaiters,
    );
    if (handle.socketSyncPromise !== null) {
      return handle.socketSyncPromise;
    }
    if (!shouldStart) {
      return Promise.resolve();
    }

    handle.socketSyncPromise = (async () => {
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
          handle.transferDebugStats.socketSyncKernelCalls += 1;
          const { socketUsed, netBufUsed, socketState } = await syncSocketStateViaKernelWorker(
            handle,
            shouldWake
              ? blockedRpcSyncCause(handle.kernel.abi_status_socket_recv())
              : syscallSyncCause("socket-state-sync"),
          );
          assertProcessIdentity(handle, "requestSocketStateSync");
          if (shouldWake) {
            if (socketState) {
              for (const member of handle.kernelShare.members) {
                if (member !== handle && kernelWorkerOwnsSocketState(member)) {
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
      handle.socketSyncPromise = null;
    });

    return handle.socketSyncPromise;
  }

  return {
    requestEpollDependentOfdStateSync,
    requestEpollStateSync,
    requestPipeStateSync,
    requestSocketStateSync,
    wakePipeAndEpollWaitersInKernelShare,
  };
}
