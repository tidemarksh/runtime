import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import type { SyncCause } from "../contracts/sync.js";
import { adoptKernelStateForHandle } from "../kernel-state.js";
import type {
  CanonicalizedChildLifecycle,
  KernelWorkerMemfsSeedMode,
  KernelWorkerPipeSeedMode,
  ProcessHandle,
} from "../types.js";
import type { WorkerKernelRpcLifecycleDeps } from "./lifecycle/types.js";
import { completedKernelRpcOutcome, type KernelRpcResumeEffect, type KernelRpcResumeOutcome } from "./outcomes.js";
import { expectKernelWorkerResponse } from "./transport.js";

export function createWorkerKernelRpcProcessLifecycleHelpers(
  deps: WorkerKernelRpcLifecycleDeps,
) {
  async function registerProcessWithKernelWorker(
    handle: ProcessHandle,
    options: { seedMemfsMode?: KernelWorkerMemfsSeedMode; pipeSeedMode?: KernelWorkerPipeSeedMode } = {},
  ): Promise<void> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return;
    }
    const response = expectKernelWorkerResponse(await handle.kernelWorkerRequest({
      type: "register-process",
      pid: handle.pid,
      vmPtr: handle.vmPtr,
      memory: handle.memory,
      kernelState: handle.kernelState,
      seedMemfsMode: options.seedMemfsMode ?? "full",
      pipeSeedMode: options.pipeSeedMode ?? "canonical",
    }), "process-registered", "register-process");
    handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
  }

  async function initializeProcessLifecycleViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
    ppid: number,
    nextPid: number,
    pgid: number,
    sid: number,
    didExec: boolean,
  ): Promise<void> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return;
    }

    const response = expectKernelWorkerResponse(await handle.kernelWorkerRequest({
      type: "initialize-process-lifecycle",
      pid: handle.pid,
      vmPtr,
      ppid,
      nextPid,
      pgid,
      sid,
      didExec,
      kernelState: handle.kernelState,
    }), "process-lifecycle-initialized", "initialize-process-lifecycle");
    handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    handle.kernel.process_reset_lifecycle(
      vmPtr,
      handle.pid,
      ppid,
      nextPid,
      pgid,
      sid,
      didExec ? 1 : 0,
    );
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
  }

  async function canonicalizeChildLifecycleViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
    provisionalChildPid: number,
  ): Promise<CanonicalizedChildLifecycle> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return {
        childPid: provisionalChildPid,
        nextPid: provisionalChildPid + 1,
        pgid: handle.kernel.process_current_pgid(vmPtr),
        sid: handle.kernel.process_current_sid(vmPtr),
      };
    }

    const response = expectKernelWorkerResponse(await handle.kernelWorkerRequest({
      type: "canonicalize-child-pid",
      pid: handle.pid,
      vmPtr,
      provisionalChildPid,
      kernelState: handle.kernelState,
    }), "child-pid-canonicalized", "canonicalize-child-pid");
    handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    handle.kernel.process_rewrite_child_pid(vmPtr, provisionalChildPid, response.childPid, response.nextPid);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    return {
      childPid: response.childPid,
      nextPid: response.nextPid,
      pgid: response.pgid,
      sid: response.sid,
    };
  }

  async function unregisterProcessWithKernelWorker(handle: ProcessHandle): Promise<void> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return;
    }
    expectKernelWorkerResponse(await handle.kernelWorkerRequest({
      type: "unregister-process",
      pid: handle.pid,
    }), "process-unregistered", "unregister-process");
  }

  async function recordChildExitViaKernelWorker(
    parent: ProcessHandle,
    childPid: number,
    waitStatus: number,
  ): Promise<KernelRpcResumeOutcome> {
    if (!parent.kernelWorkerRequest || parent.pid <= 0) {
      deps.drainProcessOutput(parent);
      if (parent.kernel.process_record_child_exit_for_parent) {
        parent.kernel.process_record_child_exit_for_parent(parent.pid, childPid, waitStatus);
      } else {
        parent.kernel.process_record_child_exit(childPid, waitStatus);
      }
      parent.kernelState = readKernelRuntimeState(parent.kernel);
      return completedKernelRpcOutcome();
    }

    try {
      deps.drainProcessOutput(parent);

      const response = expectKernelWorkerResponse(await parent.kernelWorkerRequest({
        type: "record-child-exit",
        pid: parent.pid,
        childPid,
        waitStatus,
        kernelState: parent.kernelState,
      }), "child-exit-recorded", "record-child-exit");
      if (parent.pid <= 0 || deps.processHandles.get(parent.pid) !== parent) {
        return { kind: "process-gone" };
      }
      parent.kernelState = adoptKernelStateForHandle(parent, response.kernelState);
      attachKernelRuntimeState(parent.kernel, parent.kernelState);
      if (parent.kernel.process_record_child_exit_for_parent) {
        parent.kernel.process_record_child_exit_for_parent(parent.pid, childPid, waitStatus);
      } else {
        parent.kernel.process_record_child_exit(childPid, waitStatus);
      }
      parent.kernelState = readKernelRuntimeState(parent.kernel);
      attachKernelRuntimeState(parent.kernel, parent.kernelState);
      deps.drainProcessOutput(parent);
      const effects: KernelRpcResumeEffect[] = [
        { kind: "request-pipe-state-sync", handle: parent, wakeWaiters: true },
        { kind: "kick-blocked-workers", handle: parent },
      ];
      return completedKernelRpcOutcome(undefined, effects);
    } catch (error) {
      if (parent.pid > 0 && deps.processHandles.get(parent.pid) === parent) {
        deps.failProcess(parent, error instanceof Error ? error : new Error(String(error)));
      }
      return { kind: "process-gone" };
    }
  }

  async function syncProcessLifecycleViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
  ): Promise<void> {
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      return;
    }

    const pidBeforeSync = handle.kernel.process_current_pid(vmPtr);
    const response = expectKernelWorkerResponse(await handle.kernelWorkerRequest({
      type: "sync-process-lifecycle",
      pid: handle.pid,
      vmPtr,
      kernelState: handle.kernelState,
    }), "process-lifecycle-synced", "sync-process-lifecycle");
    handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    const pidAfterSync = handle.kernel.process_current_pid(vmPtr);
    if (pidBeforeSync === handle.pid && pidAfterSync > 0 && pidAfterSync !== handle.pid) {
      handle.kernel.process_rewrite_child_pid(vmPtr, pidAfterSync, handle.pid, handle.pid + 1);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
    }
  }

  async function syncProcessMemfsViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<void> {
    await deps.syncProcessMemfsViaKernelWorker(handle, cause);
  }

  return {
    canonicalizeChildLifecycleViaKernelWorker,
    initializeProcessLifecycleViaKernelWorker,
    recordChildExitViaKernelWorker,
    registerProcessWithKernelWorker,
    syncProcessMemfsViaKernelWorker,
    syncProcessLifecycleViaKernelWorker,
    unregisterProcessWithKernelWorker,
  };
}
