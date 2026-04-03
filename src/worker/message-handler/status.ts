import {
  attachKernelRuntimeState,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import {
  drainProcessOutput,
  flushStdin,
} from "../stdio.js";
import { adoptKernelState } from "../sync.js";
import type { ProcessHandle } from "../types.js";
import { createWorkerOwnedStateSyncHandler } from "./owned-state-sync.js";
import { createWorkerStatusTransitionHandler } from "./transitions.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "./types.js";
import { createWorkerWaitInWorkerStatusHandler } from "./wait-in-worker.js";

export function createWorkerStatusMessageHandler(deps: WorkerMessageHandlerDeps) {
  const handleWaitInWorkerStatus = createWorkerWaitInWorkerStatusHandler(deps);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps);
  const handleStatusTransition = createWorkerStatusTransitionHandler(deps);

  return function handleWorkerStatusMessage(
    handle: ProcessHandle,
    worker: Worker,
    msg: ThreadWorkerStatusMessage,
  ): void {
    const thread = deps.threadWorkerStates.get(worker);
    if (!thread) {
      return;
    }
    thread.waitingInWorkerStatus = null;
    handle.lastDebugMessage = msg.debug;

    const isRunningWorker = handle.runningWorker === worker;
    handle.kernelState = adoptKernelState(handle.kernelState, msg.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    if (msg.waitInWorker) {
      handleWaitInWorkerStatus(handle, worker, thread, msg, isRunningWorker);
      return;
    }
    deps.clearBlockedWorker(handle, worker);
    const wroteToPipe = flushStdin(handle);
    const drainedPipe = drainProcessOutput(handle);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    if (isRunningWorker) {
      handle.runningWorker = null;
    }

    const status = msg.status;
    const completedSyncEffects = msg.syncEffects ?? (
      status === handle.kernel.abi_status_ok() ? deps.statusOkCompletedSyncEffects : 0
    );
    const continueAfterOwnedStateSync = (): void => {
      handleStatusTransition({
        handle,
        worker,
        thread,
        msg,
        status,
        wroteToPipe,
        drainedPipe,
        completedSyncEffects,
      });
    };
    if (maybeHandleOwnedStateSync({
      handle,
      status,
      completedSyncEffects,
      isRunningWorker,
      wroteToPipe,
      drainedPipe,
      continueAfterOwnedStateSync,
    })) {
      return;
    }
    continueAfterOwnedStateSync();
  };
}
