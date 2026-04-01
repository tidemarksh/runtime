import { readProcessSpawnHandoff } from "../abi.js";
import { handlePlainFork } from "./fork/plain-fork.js";
import { handleVforkExecve } from "./fork/vfork-execve.js";
import type { WorkerForkDeps, ProcessHandle, ThreadWorkerState } from "./fork/types.js";

export function createWorkerForkHandlers(deps: WorkerForkDeps) {
  async function handleExecveSpawn(
    handle: ProcessHandle,
    worker: Worker,
    thread: ThreadWorkerState,
  ): Promise<void> {
    const handoff = readProcessSpawnHandoff(handle.kernel, thread.vmPtr);
    const spawnKindExecve = handle.kernel.abi_spawn_kind_execve();
    const spawnKindFork = handle.kernel.abi_spawn_kind_fork();
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `spawn-handoff:${handle.pid}:${handoff.kind}:${handoff.childPid}`,
    );

    if (handoff.kind === spawnKindExecve) {
      await handleVforkExecve(deps, handle, worker, thread, handoff.childPid);
      return;
    }

    if (handoff.kind === spawnKindFork) {
      await handlePlainFork(deps, handle, worker, thread, handoff.childPid);
      return;
    }

    handle.kernel.thread_clear_spawn_handoff(thread.vmPtr);
    deps.enqueueWorker(handle, worker);
  }

  return {
    handleExecveSpawn,
    handlePlainFork: (
      handle: ProcessHandle,
      parentWorker: Worker,
      parentThread: ThreadWorkerState,
      childPid: number,
    ) => handlePlainFork(deps, handle, parentWorker, parentThread, childPid),
    handleVforkExecve: (
      handle: ProcessHandle,
      parentWorker: Worker,
      parentThread: ThreadWorkerState,
      childPid: number,
    ) => handleVforkExecve(deps, handle, parentWorker, parentThread, childPid),
  };
}
