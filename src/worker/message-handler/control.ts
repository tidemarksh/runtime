import { attachKernelRuntimeState } from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { collectHeldEpollOfdIds } from "../fork-shared.js";
import { kernelWorkerOwnsEpollState, kernelWorkerOwnsPipeState } from "../kernel-share.js";
import {
  drainProcessOutput,
  flushStdin,
} from "../stdio.js";
import { adoptKernelState } from "../sync.js";
import type { ProcessHandle } from "../types.js";
import type { ThreadWorkerOutbound } from "../../messages.js";
import type { WorkerMessageHandlerDeps } from "./types.js";

function formatDebugError(error: unknown): string {
  return error instanceof Error ? (error.stack ?? error.message) : String(error);
}

function safeThreadErrorDebug(
  handle: ProcessHandle,
  deps: WorkerMessageHandlerDeps,
  vmPtr: number,
): {
  threadCount: string;
  liveThreads: string[];
  threadSlots: string[];
} {
  try {
    const threadCount = handle.kernel.thread_process_thread_count(vmPtr);
    const liveThreads: string[] = [];
    for (let slot = 0; slot < threadCount; slot += 1) {
      const threadVmPtr = handle.kernel.process_thread_vm_ptr(vmPtr, slot);
      if (threadVmPtr === 0) {
        continue;
      }
      liveThreads.push(`0x${threadVmPtr.toString(16)}`);
    }
    const threadSlots = Array.from({ length: Math.min(handle.kernel.abi_max_threads(), 8) }, (_value, slot) => {
      const candidateVmPtr = handle.kernel.process_thread_vm_ptr(vmPtr, slot);
      return candidateVmPtr === 0 ? `${slot}=0x0` : `${slot}=0x${candidateVmPtr.toString(16)}`;
    });
    return {
      threadCount: String(threadCount),
      liveThreads,
      threadSlots,
    };
  } catch (error) {
    const detail = formatDebugError(error);
    return {
      threadCount: `unavailable:${detail}`,
      liveThreads: [],
      threadSlots: [`debug-error=${JSON.stringify(detail)}`],
    };
  }
}

export function createWorkerControlMessageHandler(deps: WorkerMessageHandlerDeps) {
  return function handleWorkerControlMessage(
    handle: ProcessHandle,
    worker: Worker,
    msg: ThreadWorkerOutbound,
  ): boolean {
    if (msg.type === "ready") {
      const thread = deps.threadWorkerStates.get(worker);
      if (!thread) {
        return true;
      }
      handle.kernelState = adoptKernelState(handle.kernelState, msg.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState);
      thread.initialized = true;
      thread.waitingInWorkerStatus = null;
      deps.clearBlockedWorker(handle, worker);
      const wroteToPipe = flushStdin(handle);
      const drainedPipe = kernelWorkerOwnsPipeState(handle) ? false : drainProcessOutput(handle);
      const readySyncs: Promise<void>[] = [];
      if (wroteToPipe || drainedPipe) {
        readySyncs.push(deps.requestPipeStateSync(handle, true));
      }
      if (
        kernelWorkerOwnsEpollState(handle)
        && collectHeldEpollOfdIds(handle).length > 0
      ) {
        readySyncs.push(deps.requestEpollStateSync(handle, false));
      }
      if (readySyncs.length > 0) {
        void Promise.all(readySyncs)
          .then(() => {
            deps.enqueueWorker(handle, worker);
          })
          .catch((error) => {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return true;
      }
      deps.enqueueWorker(handle, worker);
      return true;
    }

    if (msg.type === "status") {
      return false;
    }

    if (msg.type === "error") {
      handle.lastDebugMessage = msg.message;
      if (handle.runningWorker === worker) {
        handle.runningWorker = null;
      }
      deps.clearBlockedWorker(handle, worker);
      const {
        threadCount,
        liveThreads,
        threadSlots,
      } = safeThreadErrorDebug(handle, deps, msg.vmPtr);
      const attachedWorkers = [...handle.workers]
        .map((candidate) => deps.threadWorkerStates.get(candidate)?.vmPtr ?? 0)
        .filter((candidateVmPtr) => candidateVmPtr !== 0)
        .map((candidateVmPtr) => `0x${candidateVmPtr.toString(16)}`);
      deps.failProcess(
        handle,
        new Error(
          `pid=${handle.pid} thread vm=0x${msg.vmPtr.toString(16)} thread_count=${threadCount}`
            + ` live_threads=[${liveThreads.join(", ")}]`
            + ` attached_workers=[${attachedWorkers.join(", ")}]`
            + ` thread_slots=[${threadSlots.join(", ")}]`
            + ` ${msg.message}`,
        ),
      );
    }

    return true;
  };
}
