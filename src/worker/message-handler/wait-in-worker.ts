import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { kernelWorkerOwnsEpollState } from "../ownership/epoll-control.js";
import { kernelWorkerOwnsPipeState } from "../ownership/pipe-control.js";
import { kernelWorkerOwnsSocketState } from "../ownership/socket-control.js";
import { drainProcessOutput } from "../stdio.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "./types.js";
import { formatCompactDebugSuffix } from "./debug-summary.js";
import { createWorkerOwnershipGuard } from "../ownership/process-state.js";

export function createWorkerWaitInWorkerStatusHandler(deps: WorkerMessageHandlerDeps) {
  function clearRunningWorkerAlreadyWaiting(handle: ProcessHandle): void {
    const runningWorker = handle.runningWorker;
    if (runningWorker == null) {
      return;
    }
    const runningThread = deps.threadWorkerStates.get(runningWorker);
    if ((runningThread?.waitingInWorkerStatus ?? null) === null) {
      return;
    }
    handle.runningWorker = null;
    if (runningThread) {
      runningThread.runningGeneration = null;
      deps.appendWorkerDebugLog(
        handle,
        `running-worker-cleared:wait-in-worker:0x${runningThread.vmPtr.toString(16)}`,
      );
    }
  }

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
    deps.clearBlockedWorker(handle, worker);
    const drainedPipe = kernelWorkerOwnsPipeState(handle) ? false : drainProcessOutput(handle);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState);

    if (msg.status === epollWaitStatus && epollTimeoutMs === 0) {
      thread.waitingInWorkerStatus = null;
      if (isRunningWorker) {
        handle.runningWorker = null;
      }
      deps.blockWorker(handle, worker, msg.status, thread.vmPtr);
      deps.kickBlockedWorkers(handle);
      deps.pumpProcess(handle);
      return;
    }

    thread.waitingInWorkerStatus = msg.status;
    thread.waitingInWorkerReconcileDeferred = true;
    if (isRunningWorker) {
      handle.runningWorker = null;
      thread.runningGeneration = null;
    }
    clearRunningWorkerAlreadyWaiting(handle);
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

    const continueWaitInWorker = (): void => {
      if (drainedPipe) {
        void deps.requestPipeStateSync(handle, true).catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      }
      deps.kickBlockedWorkers(handle);
      deps.pumpProcess(handle);
    };

    const waitInWorkerNeedsEpollSync = msg.status === epollWaitStatus
      || (
        kernelWorkerOwnsSocketState(handle)
        && (
          msg.status === handle.kernel.abi_status_socket_recv()
          || msg.status === handle.kernel.abi_status_socket_send()
        )
      );
    if (isRunningWorker && kernelWorkerOwnsEpollState(handle) && waitInWorkerNeedsEpollSync) {
      const shouldSyncSocket = kernelWorkerOwnsSocketState(handle)
        && (
          msg.status === handle.kernel.abi_status_socket_recv()
          || msg.status === handle.kernel.abi_status_socket_send()
        );
      const syncSocketState = shouldSyncSocket
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
      && kernelWorkerOwnsSocketState(handle)
      && (
        msg.status === handle.kernel.abi_status_socket_recv()
        || msg.status === handle.kernel.abi_status_socket_send()
      )
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

    continueWaitInWorker();
  };
}
