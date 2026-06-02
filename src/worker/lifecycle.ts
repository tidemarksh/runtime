import { readKernelRuntimeState } from "../abi.js";
import { publishKernelRuntimeState } from "../bridge/runtime-state.js";
import {
  kernelShareOtherMembers,
  reconcileKernelSharePipeControl,
} from "./kernel-share.js";
import type { KernelRpcResumeOutcome } from "./kernel-rpc/outcomes.js";
import { collectProcessHeldOfds } from "./ownership/ofd-table.js";
import { kernelWorkerOwnsEpollState } from "./ownership/epoll-control.js";
import { kernelWorkerOwnsPipeState } from "./ownership/pipe-control.js";
import { kernelWorkerOwnsSocketState } from "./ownership/socket-control.js";
import type { ProcessHandle } from "./types.js";
import type { SyncCause } from "./contracts/sync.js";

interface WorkerLifecycleDeps {
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  drainProcessOutput(handle: ProcessHandle): boolean;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  getProcessByPid(pid: number): ProcessHandle | undefined;
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
  ): Promise<KernelRpcResumeOutcome>;
  syncForkSharedStateViaKernelWorker(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options: {
      cause: SyncCause;
      includeMemfs?: boolean;
      trackHeldOfds?: boolean;
      deltaOfdIds?: boolean;
      preserveSourceRefcounts?: boolean;
      preservePublishedRegularFileData?: boolean;
      sourceVmPtr?: number;
    },
  ): Promise<void>;
  workerExitCompleteProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number, exitVmPtr?: number): Promise<void>;
  workerExitFailProcess(handle: ProcessHandle, error: Error): void;
  workerExitPrepublishParentWaitStatus?(handle: ProcessHandle, exitCode: number, termSignal: number): boolean;
}

function encodeWaitStatus(exitCode: number, termSignal: number): number {
  if (termSignal !== 0) {
    return termSignal & 0x7f;
  }
  return (exitCode & 0xff) << 8;
}

export function createWorkerLifecycleHelpers(deps: WorkerLifecycleDeps) {
  function beginProcessTeardown(handle: ProcessHandle): boolean {
    if (handle.exitInProgress) {
      return false;
    }
    handle.exitInProgress = true;
    if (handle.stdioOwner === null && !kernelWorkerOwnsPipeState(handle)) {
      deps.drainProcessOutput(handle);
    }
    return true;
  }

  async function syncOwnedStateBeforeExit(handle: ProcessHandle): Promise<void> {
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      await deps.syncForkSharedStateViaKernelWorker(handle, kernelShareOtherMembers(handle), {
        cause: { kind: "syscall", nr: -1n, name: "process-exit-owned-state" },
        includeMemfs: false,
        trackHeldOfds: true,
        deltaOfdIds: true,
        sourceVmPtr: handle.vmPtr,
      });
    }
    if (kernelWorkerOwnsPipeState(handle)) {
      await deps.requestPipeStateSync(handle, true);
    }
    if (kernelWorkerOwnsSocketState(handle)) {
      await deps.requestSocketStateSync(handle, false);
    }
    if (kernelWorkerOwnsEpollState(handle)) {
      await deps.requestEpollStateSync(handle, false);
    }
  }

  async function completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number, exitVmPtr?: number): Promise<void> {
    if (!beginProcessTeardown(handle)) {
      return;
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `exit-begin:${handle.pid}:held=[${[...collectProcessHeldOfds(handle)].sort((left, right) => left - right).join(",")}]:shared=[${[...handle.kernelShare.sharedOfdIds].sort((left, right) => left - right).join(",")}]`
        + `:mem=${handle.kernel.debug_memory_brk_base?.() ?? "n/a"}`
        + `:${handle.kernel.debug_memory_brk_current?.() ?? "n/a"}`
        + `:${handle.kernel.debug_memory_mmap_next?.() ?? "n/a"}`,
    );
    deps.workerExitPrepublishParentWaitStatus?.(handle, exitCode, termSignal);
    await syncOwnedStateBeforeExit(handle);
    if (kernelWorkerOwnsPipeState(handle)) {
      reconcileKernelSharePipeControl(handle);
    }
    await deps.workerExitCompleteProcessExit(handle, exitCode, termSignal, exitVmPtr);
  }

  function failProcess(handle: ProcessHandle, error: Error): void {
    if (!beginProcessTeardown(handle)) {
      return;
    }
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

    handle.vforkSnapshot = null;
    handle.vforkWorker = null;
    handle.kernel.thread_resume_vfork_parent(vmPtr, childPid);
    void deps.recordChildExitViaKernelWorker(handle, childPid, waitStatus);
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
