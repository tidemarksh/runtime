import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import {
  kernelWorkerOwnsEpollState,
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

export function createWorkerWaitInWorkerStatusHandler(deps: WorkerMessageHandlerDeps) {
  return function handleWaitInWorkerStatus(
    handle: ProcessHandle,
    worker: Worker,
    thread: ThreadWorkerState,
    msg: ThreadWorkerStatusMessage,
    isRunningWorker: boolean,
  ): void {
    deps.appendWorkerDebugLog(handle, `wait:0x${thread.vmPtr.toString(16)}@${msg.status}`);
    thread.waitingInWorkerStatus = msg.status;
    deps.clearBlockedWorker(handle, worker);
    const drainedPipe = drainProcessOutput(handle);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState);

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

    const waitInWorkerNeedsEpollSync = msg.status === handle.kernel.abi_status_epoll_wait()
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
