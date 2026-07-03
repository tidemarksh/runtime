import { publishKernelRuntimeState } from "../../../bridge/runtime-state.js";
import { kernelWorkerOwnsEpollState } from "../../ownership/epoll-control.js";
import { kernelWorkerOwnsPipeState } from "../../ownership/pipe-control.js";
import { kernelWorkerOwnsSocketState } from "../../ownership/socket-control.js";
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
  supervisorBlockedWaitStatusMatchesEpollWait,
  supervisorBlockedWaitStatusMatchesPipeWait,
  supervisorBlockedWaitStatusMatchesSocketWait,
} from "../../../supervisor-blocked-resume.js";

export function createWorkerWaitInWorkerStatusHandler(deps: WorkerMessageHandlerDeps) {
  return function handleWaitInWorkerStatus(
    handle: ProcessHandle,
    worker: Worker,
    thread: ThreadWorkerState,
    msg: ThreadWorkerStatusMessage,
    isRunningWorker: boolean,
  ): void {
    const epollWaitStatus = handle.kernel.abi_status_epoll_wait();
    const workerStillOwnedByHandle = createWorkerOwnershipGuard(
      handle,
      worker,
      thread,
      deps.threadWorkerStates,
    );
    const epollTimeoutMs = msg.status === epollWaitStatus
      ? Number(handle.kernel.thread_epoll_wait_timeout(thread.vmPtr))
      : null;
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
    const drainedPipe = kernelWorkerOwnsPipeState(handle) ? false : drainProcessOutput(handle);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState);

    if (msg.status === epollWaitStatus && epollTimeoutMs === 0) {
      thread.waitingInWorkerStatus = null;
      if (isRunningWorker) {
        handle.runningWorker = null;
      }
      deps.blockWorker(handle, worker, msg.status, thread.vmPtr);
      deps.notifyWorkerStatusProgress(handle);
      deps.pumpProcess(handle);
      return;
    }

    if (msg.status === handle.kernel.abi_status_futex_wait()) {
      thread.waitingInWorkerStatus = null;
      const hostTimer = drainSupervisorArmHostTimerHostOp(handle, thread.vmPtr);
      if (hostTimer === undefined) {
        throw new Error("kernel supervisor ABI missing arm host timer HostOp exports");
      }
      if (isRunningWorker) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
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

    thread.waitingInWorkerStatus = msg.status;
    if (isRunningWorker) {
      handle.runningWorker = null;
      thread.runningGeneration = null;
    }
    if (
      handle.vforkSnapshot !== null
      && handle.vforkWorker !== null
      && handle.vforkWorker !== worker
    ) {
      deps.appendWorkerDebugLog(
        handle,
        `wait-vfork-hold:0x${thread.vmPtr.toString(16)}@${msg.status}${formatCompactDebugSuffix(msg.debug)}`,
      );
      return;
    }

    const wait4Status = handle.kernel.abi_status_wait4?.() ?? null;
    if (
      msg.status === wait4Status
      && handle.kernelWorkerRequest
      && handle.pid > 0
    ) {
      thread.waitingInWorkerStatus = null;
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

    const socketWaitStatus = kernelWorkerOwnsSocketState(handle)
      && supervisorBlockedWaitStatusMatchesSocketWait(handle.kernel, msg.status);
    const waitInWorkerNeedsEpollSync = supervisorBlockedWaitStatusMatchesEpollWait(handle.kernel, msg.status)
      || socketWaitStatus;
    if (isRunningWorker && kernelWorkerOwnsEpollState(handle) && waitInWorkerNeedsEpollSync) {
      const syncSocketState = socketWaitStatus
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
    if (
      isRunningWorker
      && socketWaitStatus
    ) {
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
    if (
      isRunningWorker
      && kernelWorkerOwnsPipeState(handle)
      && supervisorBlockedWaitStatusMatchesPipeWait(handle.kernel, msg.status)
    ) {
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
