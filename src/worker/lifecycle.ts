import { hostExecutionState } from "./host-execution-state.js";
import {
  kernelShareOtherMembers,
  reconcileKernelSharePipeControl,
} from "./kernel-share.js";
import type { KernelRpcResumeOutcome } from "./kernel-rpc/outcomes.js";
import { collectProcessHeldOfds } from "./ownership/ofd-table.js";
import {
  supervisorProcessFdFlagsForFd,
  supervisorProcessOfdForFd,
} from "./ownership/fd-table.js";
import {
  supervisorOfdFdType,
  supervisorOfdHostFd,
  supervisorOfdIno,
} from "./ownership/ofd-metadata.js";
import { kernelWorkerOwnsEpollState } from "./ownership/epoll-control.js";
import { kernelWorkerOwnsPipeState } from "./ownership/pipe-control.js";
import { kernelWorkerOwnsSocketState } from "./ownership/socket-control.js";
import { beginProcessExit } from "./process-lifecycle-state.js";
import { supervisorProcessStatusIsLifecycleHandoff } from "./supervisor-status.js";
import type { ProcessHandle } from "./types.js";
import type { SyncCause } from "./contracts/sync.js";

interface WorkerLifecycleDeps {
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  drainProcessOutput(handle: ProcessHandle): boolean;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  getProcessByPid(pid: number): ProcessHandle | undefined;
  notifyLifecycleProgress(handle: ProcessHandle): void;
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
      sourceVmPtr?: number;
    },
  ): Promise<void>;
  workerExitCompleteProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number, exitVmPtr?: number): Promise<void>;
  workerExitFailProcess(handle: ProcessHandle, error: Error): void;
}

function encodeWaitStatus(exitCode: number, termSignal: number): number {
  if (termSignal !== 0) {
    return termSignal & 0x7f;
  }
  return (exitCode & 0xff) << 8;
}

export function createWorkerLifecycleHelpers(deps: WorkerLifecycleDeps) {
  function formatExitOwnedStateFdDebug(handle: ProcessHandle): string {
    const parts: string[] = [];
    const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
    for (const fd of [0, 1, 2]) {
      if (fd >= maxFds) {
        parts.push(`${fd}->out`);
        continue;
      }
      try {
        const ofdId = supervisorProcessOfdForFd(handle, handle.vmPtr, fd);
        const fdFlags = supervisorProcessFdFlagsForFd(handle, handle.vmPtr, fd);
        if (ofdId <= 0 || ofdId >= maxFds) {
          parts.push(`${fd}->${ofdId}/${fdFlags}`);
          continue;
        }
        parts.push([
          `${fd}->${ofdId}`,
          `flags=${fdFlags}`,
          `type=${supervisorOfdFdType(handle, ofdId)}`,
          `ino=${supervisorOfdIno(handle, ofdId)}`,
          `host=${supervisorOfdHostFd(handle, ofdId)}`,
        ].join("/"));
      } catch (error) {
        parts.push(`${fd}->error:${JSON.stringify(error instanceof Error ? error.message : String(error))}`);
      }
    }
    return [
      `vm=0x${handle.vmPtr.toString(16)}`,
      `stdioOwner=${handle.stdioOwner?.pid ?? 0}`,
      `host=${handle.hostStdio.stdinPipe}.${handle.hostStdio.stdoutPipe}.${handle.hostStdio.stderrPipe}`,
      `local=${handle.stdinPipe}.${handle.stdoutPipe}.${handle.stderrPipe}`,
      `fds=${parts.join(",")}`,
    ].join(":");
  }

  function beginProcessTeardown(handle: ProcessHandle): boolean {
    if (!beginProcessExit(handle)) {
      return false;
    }
    if (handle.stdioOwner === null && !kernelWorkerOwnsPipeState(handle)) {
      deps.drainProcessOutput(handle);
    }
    return true;
  }

  async function syncOwnedStateBeforeExit(handle: ProcessHandle): Promise<void> {
    if (handle.pid > 0) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `exit-owned-state-sync-before:${handle.pid}:${formatExitOwnedStateFdDebug(handle)}`,
      );
      await deps.syncForkSharedStateViaKernelWorker(handle, kernelShareOtherMembers(handle), {
        cause: { kind: "syscall", nr: -1n, name: "process-exit-owned-state" },
        includeMemfs: false,
        trackHeldOfds: true,
        deltaOfdIds: true,
        sourceVmPtr: handle.vmPtr,
      });
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `exit-owned-state-sync-after:${handle.pid}:${formatExitOwnedStateFdDebug(handle)}`,
      );
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `exit-owned-state-before-shared-sync:${handle.pid}:${formatExitOwnedStateFdDebug(handle)}`,
    );
    if (kernelWorkerOwnsPipeState(handle)) {
      await deps.requestPipeStateSync(handle, true);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `exit-owned-state-after-pipe-sync:${handle.pid}:${formatExitOwnedStateFdDebug(handle)}`,
      );
    }
    if (kernelWorkerOwnsSocketState(handle)) {
      await deps.requestSocketStateSync(handle, false);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `exit-owned-state-after-socket-sync:${handle.pid}:${formatExitOwnedStateFdDebug(handle)}`,
      );
    }
    if (kernelWorkerOwnsEpollState(handle)) {
      await deps.requestEpollStateSync(handle, false);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `exit-owned-state-after-epoll-sync:${handle.pid}:${formatExitOwnedStateFdDebug(handle)}`,
      );
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
    const snapshot = hostExecutionState(handle).vforkSnapshot;
    if (snapshot === null || snapshot.vmPtr !== vmPtr) {
      return false;
    }

    const waitStatus = encodeWaitStatus(exitCode, termSignal);
    const childPid = handle.kernel.thread_vfork_child_pid();
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `resume-vfork-parent-after-exit:start:${handle.pid}:vm=0x${vmPtr.toString(16)}`
        + `:child=${childPid}`
        + `:snapshot_child=${snapshot.childPid ?? "none"}`
        + `:exit=${exitCode}:${termSignal}:wait=${waitStatus}`,
    );

    void deps.resumeVforkParentViaKernelWorker(handle, worker, vmPtr, waitStatus).catch((error) => {
      deps.workerExitFailProcess(handle, error instanceof Error ? error : new Error(String(error)));
    });
    return true;
  }

  function statusDefersOwnedStateSync(handle: ProcessHandle, status: number): boolean {
    return supervisorProcessStatusIsLifecycleHandoff(handle.kernel, status);
  }

  return {
    completeProcessExit,
    failProcess,
    resumeVforkParentAfterChildExit,
    statusDefersOwnedStateSync,
  };
}
