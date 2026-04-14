import {
  publishKernelRuntimeState,
  wakeRuntimeBridge,
} from "../../bridge/runtime-state.js";
import { collectHeldEpollOfdIds, collectHeldOfdIdsByType } from "../fork-shared.js";
import {
  kernelWorkerOwnsEpollState,
  kernelWorkerOwnsPipeState,
  kernelWorkerOwnsSocketState,
} from "../kernel-share.js";
import {
  composeHandlePipeUsed,
  syncEpollStateViaKernelWorker,
  syncOfdStateViaKernelWorker,
  syncPipeStateViaKernelWorker,
  syncSocketStateViaKernelWorker,
} from "../sync.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type { WorkerSchedulerStateSyncHelpers } from "./types.js";

interface WorkerSchedulerStateSyncDeps {
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  drainProcessOutput(handle: ProcessHandle): boolean;
  drainHiddenProcessOutput(handle: ProcessHandle): boolean;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  retryBlockedWorker(handle: ProcessHandle, worker: Worker): void;
}

export function createWorkerSchedulerStateSyncHelpers(
  deps: WorkerSchedulerStateSyncDeps,
): WorkerSchedulerStateSyncHelpers {
  function visibleOutputHandle(handle: ProcessHandle): ProcessHandle {
    return handle.stdioOwner ?? handle;
  }

  function wakeBlockedStatusesInKernelShare(
    handle: ProcessHandle,
    statuses: number[],
    pipeUsed?: bigint,
  ): void {
    const blockedStatuses = new Set(statuses);
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
          && blockedStatuses.has(state.waitingInWorkerStatus);
      });
      if (shouldWake) {
        publishKernelRuntimeState(member.runtimeBridge, member.kernelState, true);
      }
      for (const [blockedWorker, blocked] of member.blockedWorkers) {
        if (blockedStatuses.has(blocked.status)) {
          deps.retryBlockedWorker(member, blockedWorker);
        }
      }
    }
  }

  function wakePipeAndEpollWaitersInKernelShare(handle: ProcessHandle, pipeUsed: bigint): void {
    wakeBlockedStatusesInKernelShare(
      handle,
      [
        handle.kernel.abi_status_pipe_wait(),
        handle.kernel.abi_status_epoll_wait(),
      ],
      pipeUsed,
    );
  }

  function wakePipeWaitersInKernelShare(handle: ProcessHandle, pipeUsed: bigint): void {
    wakeBlockedStatusesInKernelShare(handle, [handle.kernel.abi_status_pipe_wait()], pipeUsed);
  }

  function retryEpollWaitersInKernelShare(handle: ProcessHandle): void {
    wakeBlockedStatusesInKernelShare(handle, [handle.kernel.abi_status_epoll_wait()]);
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
    wakeBlockedStatusesInKernelShare(
      handle,
      [
        handle.kernel.abi_status_socket_recv(),
        handle.kernel.abi_status_socket_send(),
      ],
    );
  }

  function requestEpollStateSync(handle: ProcessHandle, retryWaiters = false): Promise<void> {
    if (!kernelWorkerOwnsEpollState(handle)) {
      if (retryWaiters) {
        retryEpollWaitersInKernelShare(handle);
      }
      return Promise.resolve();
    }
    if (collectHeldEpollOfdIds(handle).length === 0) {
      if (retryWaiters) {
        retryEpollWaitersInKernelShare(handle);
      }
      return Promise.resolve();
    }

    handle.epollSyncQueued = true;
    if (retryWaiters) {
      handle.epollWaiterWakeQueued = true;
    }
    if (handle.epollSyncPromise !== null) {
      return handle.epollSyncPromise;
    }

    handle.epollSyncPromise = (async () => {
      while (handle.epollSyncQueued) {
        handle.epollSyncQueued = false;
        const shouldRetry = handle.epollWaiterWakeQueued;
        handle.epollWaiterWakeQueued = false;
        await syncEpollStateViaKernelWorker(handle);
        if (shouldRetry) {
          retryEpollWaitersInKernelShare(handle);
        }
      }
    })().finally(() => {
      handle.epollSyncPromise = null;
    });

    return handle.epollSyncPromise;
  }

  function requestEpollDependentOfdStateSync(handle: ProcessHandle): Promise<void> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return Promise.resolve();
    }

    for (const ofdId of collectHeldOfdIdsByType(handle, [9, 10])) {
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
        await syncOfdStateViaKernelWorker(handle, ofdIds);
      }
    })().finally(() => {
      handle.ofdSyncPromise = null;
    });

    return handle.ofdSyncPromise;
  }

  function requestPipeStateSync(handle: ProcessHandle, wakeWaiters = false): Promise<void> {
    if (!kernelWorkerOwnsPipeState(handle)) {
      if (wakeWaiters) {
        wakeRuntimeBridge(handle.runtimeBridge);
      }
      return Promise.resolve();
    }

    handle.pipeSyncQueued = true;
    if (wakeWaiters) {
      handle.pipeWaiterWakeQueued = true;
    }
    if (handle.pipeSyncPromise !== null) {
      return handle.pipeSyncPromise;
    }

    handle.pipeSyncPromise = (async () => {
      while (handle.pipeSyncQueued) {
        handle.pipeSyncQueued = false;
        const shouldWake = handle.pipeWaiterWakeQueued;
        handle.pipeWaiterWakeQueued = false;
        const pipeUsed = await syncPipeStateViaKernelWorker(handle);
        const visibleHandle = visibleOutputHandle(handle);
        if (
          visibleHandle !== handle
          && kernelWorkerOwnsPipeState(visibleHandle)
          && visibleHandle.kernelWorkerRequest
          && visibleHandle.pid > 0
        ) {
          const visiblePipeUsed = await syncPipeStateViaKernelWorker(visibleHandle);
          visibleHandle.kernelState = {
            ...visibleHandle.kernelState,
            pipeUsed: visiblePipeUsed,
          };
        }
        const drainedHiddenOutput = visibleHandle !== handle
          ? deps.drainHiddenProcessOutput(handle)
          : false;
        const drainedVisibleOutput = deps.drainProcessOutput(visibleHandle);
        if (
          drainedVisibleOutput
          && kernelWorkerOwnsPipeState(visibleHandle)
          && visibleHandle.kernelWorkerRequest
          && visibleHandle.pid > 0
        ) {
          const visiblePipeUsed = await syncPipeStateViaKernelWorker(visibleHandle);
          visibleHandle.kernelState = {
            ...visibleHandle.kernelState,
            pipeUsed: visiblePipeUsed,
          };
        }
        deps.appendRuntimeLifecycleEvent(
          visibleHandle.runtimeId,
          `pipe-sync-drain:${handle.pid}->${visibleHandle.pid}:${drainedHiddenOutput || drainedVisibleOutput ? 1 : 0}`,
        );
        if (shouldWake) {
          wakePipeWaitersInKernelShare(handle, pipeUsed);
          if (kernelWorkerOwnsEpollState(handle)) {
            await requestEpollDependentOfdStateSync(handle);
            await requestEpollStateSync(handle, true);
          } else {
            wakeBlockedStatusesInKernelShare(
              handle,
              [handle.kernel.abi_status_epoll_wait()],
              pipeUsed,
            );
          }
        }
      }
    })().finally(() => {
      handle.pipeSyncPromise = null;
    });

    return handle.pipeSyncPromise;
  }

  function requestSocketStateSync(handle: ProcessHandle, wakeWaiters = false): Promise<void> {
    if (!kernelWorkerOwnsSocketState(handle)) {
      if (wakeWaiters) {
        wakeSocketWaitersInKernelShare(handle, handle.kernelState.socketUsed, handle.kernelState.netBufUsed);
      }
      return Promise.resolve();
    }

    handle.socketSyncQueued = true;
    if (wakeWaiters) {
      handle.socketWaiterWakeQueued = true;
    }
    if (handle.socketSyncPromise !== null) {
      return handle.socketSyncPromise;
    }

    handle.socketSyncPromise = (async () => {
      while (handle.socketSyncQueued) {
        handle.socketSyncQueued = false;
        const shouldWake = handle.socketWaiterWakeQueued;
        handle.socketWaiterWakeQueued = false;
        const { socketUsed, netBufUsed } = await syncSocketStateViaKernelWorker(handle);
        if (shouldWake) {
          wakeSocketWaitersInKernelShare(handle, socketUsed, netBufUsed);
        }
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
