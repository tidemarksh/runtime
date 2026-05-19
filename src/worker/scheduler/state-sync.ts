import {
  readKernelRuntimeState,
} from "../../abi.js";
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
import { assertHandleWorkerProcessIdentity } from "../process-identity.js";
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
  function assertProcessIdentity(handle: ProcessHandle, context: string): void {
    assertHandleWorkerProcessIdentity(handle, deps.threadWorkerStates, context);
  }

  function visibleOutputHandle(handle: ProcessHandle): ProcessHandle {
    return handle.stdioOwner ?? handle;
  }

  function hasActiveVforkChildOwnerMismatch(handle: ProcessHandle): boolean {
    if (handle.vforkSnapshot == null || handle.vforkWorker == null || handle.pid <= 0) {
      return false;
    }
    const thread = deps.threadWorkerStates.get(handle.vforkWorker);
    if (!thread || thread.vmPtr === 0) {
      return false;
    }
    try {
      const currentPid = handle.kernel.process_current_pid(thread.vmPtr);
      return currentPid > 0 && currentPid !== handle.pid;
    } catch {
      return false;
    }
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
    const fdPipeSlot = (fd: number): number => {
      const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
      if (
        ofdId === 0
        || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
      ) {
        return 0;
      }
      return handle.kernel.kernel_ofd_ino(ofdId);
    };
    const hiddenSlots = [fdPipeSlot(1), fdPipeSlot(2)]
      .filter((slot, index, slots) => slot > 0 && slots.indexOf(slot) === index);
    if (hiddenSlots.length === 0) {
      return false;
    }
    return hiddenSlots.some((slot) => visibleSlots.has(slot));
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
          if (blocked.status === member.kernel.abi_status_pipe_wait()) {
            blocked.pipeSyncedBeforePassive = false;
          }
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
        handle.kernel.abi_status_pipe_wait(),
        handle.kernel.abi_status_epoll_wait(),
        handle.kernel.abi_status_socket_recv(),
        handle.kernel.abi_status_socket_send(),
      ],
    );
  }

  function requestEpollStateSync(handle: ProcessHandle, retryWaiters = false): Promise<void> {
    if (hasActiveVforkChildOwnerMismatch(handle)) {
      return Promise.resolve();
    }
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
        assertProcessIdentity(handle, "requestEpollStateSync");
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
    if (hasActiveVforkChildOwnerMismatch(handle)) {
      return Promise.resolve();
    }
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
        assertProcessIdentity(handle, "requestEpollDependentOfdStateSync");
      }
    })().finally(() => {
      handle.ofdSyncPromise = null;
    });

    return handle.ofdSyncPromise;
  }

  function requestPipeStateSync(handle: ProcessHandle, wakeWaiters = false): Promise<void> {
    if (hasActiveVforkChildOwnerMismatch(handle)) {
      return Promise.resolve();
    }
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
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-start:${handle.pid}:wake=${shouldWake ? 1 : 0}`);
        const pipeUsed = await syncPipeStateViaKernelWorker(handle);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-canonical:${handle.pid}:used=${pipeUsed.toString()}`);
        assertProcessIdentity(handle, "requestPipeStateSync");
        const visibleHandle = visibleOutputHandle(handle);
        const drainedHiddenOutput = shouldDrainHiddenInheritedOutput(handle, visibleHandle)
          ? deps.drainHiddenProcessOutput(handle)
          : false;
        const drainedVisibleOutput = deps.drainProcessOutput(visibleHandle);
        if (drainedHiddenOutput) {
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-hidden-drain:${handle.pid}`);
          const refreshedPipeUsed = await syncPipeStateViaKernelWorker(handle);
          assertProcessIdentity(handle, "requestPipeStateSync:after-hidden-drain");
          handle.kernelState = {
            ...handle.kernelState,
            pipeUsed: refreshedPipeUsed,
          };
        }
        if (
          drainedVisibleOutput
          && kernelWorkerOwnsPipeState(visibleHandle)
          && visibleHandle.kernelWorkerRequest
          && visibleHandle.pid > 0
        ) {
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-visible-drain:${visibleHandle.pid}`);
          const visiblePipeUsed = await syncPipeStateViaKernelWorker(visibleHandle);
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
            wakeBlockedStatusesInKernelShare(
              handle,
              [handle.kernel.abi_status_epoll_wait()],
              pipeUsed,
            );
          }
        }
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-done:${handle.pid}:wake=${shouldWake ? 1 : 0}`);
      }
    })().finally(() => {
      handle.pipeSyncPromise = null;
    });

    return handle.pipeSyncPromise;
  }

  function requestSocketStateSync(handle: ProcessHandle, wakeWaiters = false): Promise<void> {
    if (hasActiveVforkChildOwnerMismatch(handle)) {
      return Promise.resolve();
    }
    if (!kernelWorkerOwnsSocketState(handle)) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
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
        assertProcessIdentity(handle, "requestSocketStateSync");
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
