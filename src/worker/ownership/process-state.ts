import type { ProcessHandle, ThreadWorkerState } from "../types.js";

export function createWorkerOwnershipGuard(
  handle: ProcessHandle,
  worker: Worker,
  thread: ThreadWorkerState,
  threadWorkerStates: Pick<WeakMap<Worker, ThreadWorkerState>, "get">,
): () => boolean {
  const threadGeneration = thread.generation;
  return () => {
    const current = threadWorkerStates.get(worker);
    return current?.generation === threadGeneration
      && current.ownerPid === handle.pid
      && handle.workers.has(worker);
  };
}
