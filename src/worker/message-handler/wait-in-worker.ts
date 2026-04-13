import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import {
  kernelWorkerOwnsEpollState,
  kernelWorkerOwnsPipeState,
  kernelWorkerOwnsSocketState,
} from "../kernel-share.js";
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

export function createWorkerWaitInWorkerStatusHandler(deps: WorkerMessageHandlerDeps) {
  return function handleWaitInWorkerStatus(
    handle: ProcessHandle,
    worker: Worker,
    thread: ThreadWorkerState,
    msg: ThreadWorkerStatusMessage,
    isRunningWorker: boolean,
  ): void {
    const epollWaitStatus = handle.kernel.abi_status_epoll_wait();
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
      deps.pumpProcess(handle);
      return;
    }

    thread.waitingInWorkerStatus = msg.status;

    const continueWaitInWorker = (): void => {
      if (drainedPipe) {
        void deps.requestPipeStateSync(handle, true).catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      }
      if (isRunningWorker) {
        handle.runningWorker = null;
      }
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
          publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
          continueWaitInWorker();
        })
        .catch((error) => {
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
          publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
          continueWaitInWorker();
        })
        .catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }

    continueWaitInWorker();
  };
}
