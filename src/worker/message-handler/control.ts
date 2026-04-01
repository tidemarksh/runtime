import {
  drainProcessOutput,
  flushStdin,
} from "../stdio.js";
import type { ProcessHandle } from "../types.js";
import type { ThreadWorkerOutbound } from "../../messages.js";
import type { WorkerMessageHandlerDeps } from "./types.js";

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
      thread.initialized = true;
      thread.waitingInWorkerStatus = null;
      deps.clearBlockedWorker(handle, worker);
      const wroteToPipe = flushStdin(handle);
      const drainedPipe = drainProcessOutput(handle);
      if (wroteToPipe || drainedPipe) {
        void deps.requestPipeStateSync(handle, true).catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
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
      const threadCount = handle.kernel.thread_process_thread_count(msg.vmPtr);
      const liveThreads: string[] = [];
      for (let slot = 0; slot < threadCount; slot += 1) {
        const vmPtr = handle.kernel.process_thread_vm_ptr(msg.vmPtr, slot);
        if (vmPtr === 0) {
          continue;
        }
        liveThreads.push(`0x${vmPtr.toString(16)}`);
      }
      const attachedWorkers = [...handle.workers]
        .map((candidate) => deps.threadWorkerStates.get(candidate)?.vmPtr ?? 0)
        .filter((candidateVmPtr) => candidateVmPtr !== 0)
        .map((candidateVmPtr) => `0x${candidateVmPtr.toString(16)}`);
      const threadSlots = Array.from({ length: Math.min(handle.kernel.abi_max_threads(), 8) }, (_value, slot) => {
        const candidateVmPtr = handle.kernel.process_thread_vm_ptr(msg.vmPtr, slot);
        return candidateVmPtr === 0 ? `${slot}=0x0` : `${slot}=0x${candidateVmPtr.toString(16)}`;
      });
      deps.failProcess(
        handle,
        new Error(
          `pid=${handle.pid} thread vm=0x${msg.vmPtr.toString(16)} thread_count=${threadCount}`
            + ` live_threads=[${liveThreads.join(", ")}]`
            + ` attached_workers=[${attachedWorkers.join(", ")}]`
            + ` thread_slots=[${threadSlots.join(", ")}] ${msg.message}`,
        ),
      );
    }

    return true;
  };
}
