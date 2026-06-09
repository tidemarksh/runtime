import { readProcessSpawnHandoff } from "../abi.js";
import { handlePlainFork } from "./fork/plain.js";
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
    deps.appendWorkerDebugLog(
      handle,
      `execve-spawn:vm=0x${thread.vmPtr.toString(16)}:kind=${handoff.kind}:child=${handoff.childPid}`,
    );
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

    if (handoff.kind === 0 && handoff.childPid === 0 && handle.pendingChildSpawnRecords > 0) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `spawn-handoff-stale:${handle.pid}:0x${thread.vmPtr.toString(16)}:${handle.pendingChildSpawnRecords}`,
      );
      deps.appendWorkerDebugLog(
        handle,
        `execve-spawn-stale:vm=0x${thread.vmPtr.toString(16)}:pending=${handle.pendingChildSpawnRecords}`,
      );
      deps.kickBlockedWorkers(handle);
      return;
    }

    throw new Error(
      `unexpected spawn handoff kind pid=${handle.pid} vm=0x${thread.vmPtr.toString(16)}`
        + ` kind=${handoff.kind} child=${handoff.childPid}`
        + ` execve=${spawnKindExecve} fork=${spawnKindFork}`,
    );
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
