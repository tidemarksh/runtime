import { createWorkerControlMessageHandler } from "./message-handler/control.js";
import { createWorkerStatusMessageHandler } from "./message-handler/status.js";
import type { WorkerMessageHandlerDeps } from "./message-handler/types.js";
import type { ProcessHandle } from "./types.js";
import type { ThreadWorkerOutbound } from "../messages.js";

export function createWorkerMessageHandler(deps: WorkerMessageHandlerDeps) {
  const handleWorkerControlMessage = createWorkerControlMessageHandler(deps);
  const handleWorkerStatusMessage = createWorkerStatusMessageHandler(deps);

  return function handleWorkerMessage(
    handle: ProcessHandle,
    worker: Worker,
    msg: ThreadWorkerOutbound,
  ): void {
    if (handle.pid > 0 && deps.processHandles.get(handle.pid) !== handle) {
      return;
    }

    if (handleWorkerControlMessage(handle, worker, msg)) {
      return;
    }
    handleWorkerStatusMessage(handle, worker, msg as Extract<ThreadWorkerOutbound, { type: "status" }>);
  };
}
