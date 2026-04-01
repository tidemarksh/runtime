import { readKernelRuntimeState } from "../abi.js";
import { publishKernelRuntimeState } from "../bridge/runtime-state.js";
import {
  kernelShareOtherMembers,
  kernelWorkerOwnsEpollState,
  kernelWorkerOwnsPipeState,
  kernelWorkerOwnsSocketState,
} from "./kernel-share.js";
import { collectProcessHeldOfds } from "./fork-shared.js";
import { releaseSharedHostStdioLocal } from "./stdio.js";
import type { ProcessHandle } from "./types.js";

interface WorkerLifecycleDeps {
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  drainProcessOutput(handle: ProcessHandle): boolean;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  kickBlockedWorkers(handle: ProcessHandle): void;
  recordChildExitViaKernelWorker(parent: ProcessHandle, childPid: number, waitStatus: number): Promise<void>;
  releaseSharedHostStdio(handle: ProcessHandle): Promise<void>;
  requestEpollStateSync(handle: ProcessHandle, retryWaiters?: boolean): Promise<void>;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  requestSocketStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  resumeVforkParentViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    waitStatus: number,
  ): Promise<void>;
  syncForkSharedStateViaKernelWorker(handle: ProcessHandle, targets: ProcessHandle[]): Promise<void>;
  workerExitCompleteProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number): Promise<void>;
  workerExitFailProcess(handle: ProcessHandle, error: Error): void;
}

function encodeWaitStatus(exitCode: number, termSignal: number): number {
  if (termSignal !== 0) {
    return termSignal & 0x7f;
  }
  return (exitCode & 0xff) << 8;
}

export function createWorkerLifecycleHelpers(deps: WorkerLifecycleDeps) {
  async function completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number): Promise<void> {
    if (kernelWorkerOwnsPipeState(handle)) {
      await deps.requestPipeStateSync(handle, true);
    }
    deps.drainProcessOutput(handle);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `exit-begin:${handle.pid}:held=[${[...collectProcessHeldOfds(handle)].sort((left, right) => left - right).join(",")}]:shared=[${[...handle.kernelShare.sharedOfdIds].sort((left, right) => left - right).join(",")}]`,
    );
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      await deps.syncForkSharedStateViaKernelWorker(handle, kernelShareOtherMembers(handle));
    }
    if (kernelWorkerOwnsSocketState(handle)) {
      await deps.requestSocketStateSync(handle, false);
    }
    if (kernelWorkerOwnsEpollState(handle)) {
      await deps.requestEpollStateSync(handle, false);
    }
    if (kernelWorkerOwnsPipeState(handle)) {
      await deps.releaseSharedHostStdio(handle);
    } else {
      releaseSharedHostStdioLocal(handle);
    }
    await deps.workerExitCompleteProcessExit(handle, exitCode, termSignal);
  }

  function failProcess(handle: ProcessHandle, error: Error): void {
    deps.drainProcessOutput(handle);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `fail:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${JSON.stringify(error.message)}`,
    );
    deps.workerExitFailProcess(handle, error);
  }

  function resumeVforkParentAfterChildExit(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    exitCode: number,
    termSignal: number,
  ): boolean {
    const snapshot = handle.vforkSnapshot;
    if (snapshot === null || snapshot.vmPtr !== vmPtr) {
      return false;
    }

    const waitStatus = encodeWaitStatus(exitCode, termSignal);

    if (handle.kernelWorkerRequest && handle.pid > 0) {
      void deps.resumeVforkParentViaKernelWorker(handle, worker, vmPtr, waitStatus);
      return true;
    }

    const childPid = handle.kernel.thread_vfork_child_pid();
    if (childPid <= 0) {
      return false;
    }

    void deps.recordChildExitViaKernelWorker(handle, childPid, waitStatus);
    handle.vforkSnapshot = null;
    handle.vforkWorker = null;
    handle.kernel.thread_resume_vfork_parent(vmPtr, childPid);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    deps.kickBlockedWorkers(handle);
    deps.enqueueWorker(handle, worker);
    return true;
  }

  function statusDefersOwnedStateSync(handle: ProcessHandle, status: number): boolean {
    return status === handle.kernel.abi_status_vfork_wait()
      || status === handle.kernel.abi_status_execve_spawn()
      || status === handle.kernel.abi_status_clone_spawn();
  }

  return {
    completeProcessExit,
    failProcess,
    resumeVforkParentAfterChildExit,
    statusDefersOwnedStateSync,
  };
}
