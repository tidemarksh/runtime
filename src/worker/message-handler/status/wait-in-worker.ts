import { hostExecutionState } from "../../host-execution-state.js";
import { processHasKernelWorkerOwnership } from "../../kernel-worker-ownership.js";
import { publishKernelRuntimeState } from "../../../bridge/runtime-state.js";
import { kernelWorkerOwnsEpollState } from "../../ownership/epoll-control.js";
import { drainProcessOutput } from "../../stdio.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../../types.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "../types.js";
import { formatCompactDebugSuffix } from "../../diagnostics/format.js";
import { createWorkerOwnershipGuard } from "../../ownership/process-state.js";
import {
  armSupervisorHostTimer,
  drainSupervisorArmHostTimerHostOp,
} from "../../supervisor-host-op.js";
import {
  supervisorWaitInWorkerStatusActions,
} from "../../../supervisor-blocked-resume.js";
import {
  clearThreadRunningGeneration,
  clearThreadWaitingInWorkerStatus,
  setThreadWaitingInWorkerStatus,
} from "../../thread-host-state.js";

export function createWorkerWaitInWorkerStatusHandler(deps: WorkerMessageHandlerDeps) {
  return function handleWaitInWorkerStatus(
    handle: ProcessHandle,
    worker: Worker,
    thread: ThreadWorkerState,
    msg: ThreadWorkerStatusMessage,
    isRunningWorker: boolean,
  ): void {
    const workerStillOwnedByHandle = createWorkerOwnershipGuard(
      handle,
      worker,
      thread,
      deps.threadWorkerStates,
    );
    const kernelWorkerAvailable = processHasKernelWorkerOwnership(handle);
    const waitActions = supervisorWaitInWorkerStatusActions(
      handle.kernel,
      thread.vmPtr,
      msg.status,
      {
        runningWorker: isRunningWorker,
        kernelWorkerAvailable,
        epollStateKernelOwns: kernelWorkerOwnsEpollState(handle),
      },
    );
    deps.appendWorkerDebugLog(
      handle,
      `wait:0x${thread.vmPtr.toString(16)}@${msg.status}${formatCompactDebugSuffix(msg.debug)}`,
    );
    if (msg.debug?.includes("thread-futex-wait")) {
      deps.appendWorkerDebugLog(
        handle,
        `futex_wait_debug:0x${thread.vmPtr.toString(16)}:${msg.debug}`,
      );
    }
    deps.clearBlockedWorker(handle, worker);
    const drainedPipe = waitActions.drainLocalPipe ? drainProcessOutput(handle) : false;
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState);

    if (waitActions.blockEpollTimeout) {
      clearThreadWaitingInWorkerStatus(thread);
      if (isRunningWorker) {
        hostExecutionState(handle).runningWorker = null;
      }
      deps.blockWorker(handle, worker, msg.status, thread.vmPtr);
      deps.notifyWorkerStatusProgress(handle);
      deps.pumpProcess(handle);
      return;
    }

    if (waitActions.parkFutex) {
      clearThreadWaitingInWorkerStatus(thread);
      const hostTimer = drainSupervisorArmHostTimerHostOp(handle, thread.vmPtr);
      if (hostTimer === undefined) {
        throw new Error("kernel supervisor ABI missing arm host timer HostOp exports");
      }
      if (isRunningWorker) {
        hostExecutionState(handle).runningWorker = null;
        clearThreadRunningGeneration(thread);
      }
      deps.parkSupervisorFutexWait(
        handle,
        worker,
        thread.vmPtr,
        "wait-in-worker-futex-supervisor-owned",
      );
      if (hostTimer !== null) {
        armSupervisorHostTimer(handle, hostTimer, deps);
      }
      deps.notifyWorkerStatusProgress(handle);
      deps.pumpProcess(handle);
      return;
    }

    setThreadWaitingInWorkerStatus(thread, msg.status);
    if (isRunningWorker) {
      hostExecutionState(handle).runningWorker = null;
      clearThreadRunningGeneration(thread);
    }
    if (
      hostExecutionState(handle).vforkSnapshot !== null
      && hostExecutionState(handle).vforkWorker !== null
      && hostExecutionState(handle).vforkWorker !== worker
    ) {
      deps.appendWorkerDebugLog(
        handle,
        `wait-vfork-hold:0x${thread.vmPtr.toString(16)}@${msg.status}${formatCompactDebugSuffix(msg.debug)}`,
      );
      return;
    }

    if (waitActions.blockWait4AndSyncPipe) {
      clearThreadWaitingInWorkerStatus(thread);
      deps.blockWorker(handle, worker, msg.status, thread.vmPtr);
      void deps.requestPipeStateSync(handle, true)
        .then(() => {
          if (!workerStillOwnedByHandle()) {
            return;
          }
          publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
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

    const continueWaitInWorker = (): void => {
      if (drainedPipe) {
        void deps.requestPipeStateSync(handle, true).catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      }
      deps.notifyWorkerStatusProgress(handle);
      deps.pumpProcess(handle);
    };

    if (waitActions.syncEpoll) {
      const syncSocketState = waitActions.syncSocket
        ? deps.requestSocketStateSync(handle, false)
        : Promise.resolve();
      void syncSocketState
        .then(() => deps.requestEpollStateSync(handle, false))
        .then(() => {
          if (!workerStillOwnedByHandle()) {
            return;
          }
          publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
          continueWaitInWorker();
        })
        .catch((error) => {
          if (!workerStillOwnedByHandle()) {
            return;
          }
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }
    if (waitActions.syncSocket) {
      void deps.requestSocketStateSync(handle, false)
        .then(() => {
          if (!workerStillOwnedByHandle()) {
            return;
          }
          publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
          continueWaitInWorker();
        })
        .catch((error) => {
          if (!workerStillOwnedByHandle()) {
            return;
          }
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }
    if (waitActions.syncPipe) {
      void deps.requestPipeStateSync(handle, true)
        .then(() => {
          if (!workerStillOwnedByHandle()) {
            return;
          }
          publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
          continueWaitInWorker();
        })
        .catch((error) => {
          if (!workerStillOwnedByHandle()) {
            return;
          }
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }

    continueWaitInWorker();
  };
}
