import { setWorkerMessageHandler } from "../../bridge/compat.js";
import type { ProcessHandle, ThreadWorkerState } from "../types.js";
import type { ThreadWorkerOutbound } from "../../messages.js";
import type {
  ThreadWorkerInbound,
  WorkerSchedulerQueueHelpers,
} from "./types.js";

interface WorkerSchedulerQueueDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  handleWorkerMessage(handle: ProcessHandle, worker: Worker, msg: ThreadWorkerOutbound): void;
}

export function createWorkerSchedulerQueueHelpers(
  deps: WorkerSchedulerQueueDeps,
): WorkerSchedulerQueueHelpers {
  function hasExclusiveVforkWorker(handle: ProcessHandle): boolean {
    return handle.vforkSnapshot !== null && handle.vforkWorker !== null;
  }

  function attachThreadWorker(handle: ProcessHandle, worker: Worker, vmPtr: number): void {
    handle.workers.add(worker);
    deps.appendWorkerDebugLog(handle, `attach:0x${vmPtr.toString(16)}`);
    deps.threadWorkerStates.set(worker, { vmPtr, initialized: false, waitingInWorkerStatus: null });
    setWorkerMessageHandler(worker, (event: MessageEvent<ThreadWorkerOutbound>) => {
      deps.handleWorkerMessage(handle, worker, event.data);
    });

    worker.postMessage({
      type: "init",
      wasmModule: handle.wasmModule,
      memory: handle.memory,
      vmPtr,
      guestBase: handle.guestBase,
      kernelState: handle.kernelState,
      sabPageCache: handle.sabPageCache,
      runtimeNotify: handle.runtimeBridge.notifySab,
      runtimeState: handle.runtimeBridge.stateSab,
    } satisfies ThreadWorkerInbound);
  }

  function enqueueWorker(handle: ProcessHandle, worker: Worker): void {
    if (!handle.readyWorkers.includes(worker)) {
      const vmPtr = deps.threadWorkerStates.get(worker)?.vmPtr;
      if (vmPtr !== undefined) {
        deps.appendWorkerDebugLog(handle, `ready:0x${vmPtr.toString(16)}`);
      }
      handle.readyWorkers.push(worker);
    }
    pumpProcess(handle);
  }

  function pumpProcess(handle: ProcessHandle): void {
    if (handle.runningWorker !== null) {
      return;
    }
    let next: Worker | undefined;
    if (hasExclusiveVforkWorker(handle)) {
      const index = handle.readyWorkers.indexOf(handle.vforkWorker!);
      if (index === -1) {
        return;
      }
      next = handle.readyWorkers.splice(index, 1)[0];
    } else {
      next = handle.readyWorkers.shift();
    }
    if (!next) {
      return;
    }
    handle.runningWorker = next;
    const vmPtr = deps.threadWorkerStates.get(next)?.vmPtr;
    if (vmPtr !== undefined) {
      deps.appendWorkerDebugLog(handle, `step:0x${vmPtr.toString(16)}`);
    }
    next.postMessage({
      type: "step",
      budget: 20_000,
      kernelState: handle.kernelState,
    } satisfies ThreadWorkerInbound);
  }

  return {
    attachThreadWorker,
    enqueueWorker,
    pumpProcess,
  };
}
