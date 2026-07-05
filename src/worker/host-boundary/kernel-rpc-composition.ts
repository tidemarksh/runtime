import { hostExecutionState } from "../host-execution-state.js";
import { createWorkerKernelRpcLifecycleHelpers } from "../kernel-rpc/lifecycle.js";
import { createWorkerKernelRpcBlockedRpcHelpers } from "./blocked-rpc-resume.js";
import { createWorkerKernelRpcFsHelpers } from "./fs-host-ops.js";
import { expectKernelWorkerResponse } from "../kernel-rpc/transport.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { kernelShareOtherMembers } from "../kernel-share.js";
import { drainProcessOutput, settleVisibleProcessOutput } from "../stdio.js";
import { syncSocketStatePreservingNetworkBridge } from "../sync/socket.js";
import {
  type KernelWorkerRequestFn,
  type ProcessHandle,
  type ThreadWorkerState,
} from "../types.js";
import type { KernelWorkerPidBytesSnapshot } from "../../messages.js";
import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerSocketStateSnapshot,
  SyncCause,
} from "../contracts/index.js";

interface WorkerKernelRpcDeps {
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, import("../types.js").ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleExecveSpawn(handle: ProcessHandle, worker: Worker, thread: ThreadWorkerState): Promise<void>;
  notifyHostEffectProgress(handle: ProcessHandle): void;
  clearHostBlockedRetryTimer(handle: ProcessHandle, worker: Worker): void;
  requestThreadWorkerBlockedResume(handle: ProcessHandle, worker: Worker): void;
  markRuntimeFsDirty(handle: ProcessHandle): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  syncProcessMemfsViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<void>;
  syncBlockedForkSharedStateViaKernelWorker(handle: ProcessHandle, sourceVmPtr: number | undefined, cause: SyncCause): Promise<void>;
  syncForkSharedStateViaKernelWorker(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options: {
      cause: SyncCause;
      includeMemfs?: boolean;
      trackHeldOfds?: boolean;
      deltaOfdIds?: boolean;
      memfsOnly?: boolean;
      preserveSourceRefcounts?: boolean;
      sourceVmPtr?: number;
      ofdIds?: number[];
    },
  ): Promise<void>;
  rehydrateForkSharedStateViaKernelWorker(
    targets: ProcessHandle[],
    ofdIds: number[],
    options: { cause: SyncCause; procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean },
  ): Promise<void>;
  syncSocketStateViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<{ socketUsed: number; netBufUsed: bigint }>;
  snapshotProcessFdOfdStateForKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
  ): {
    fdEntries: KernelWorkerFdEntrySnapshot[];
    ofdSlots: KernelWorkerOfdSlotSnapshot[];
  };
  applySocketStateFromKernelWorker(handle: ProcessHandle, snapshot: KernelWorkerSocketStateSnapshot): void;
}

export function clearVforkSnapshot(handle: ProcessHandle): void {
  hostExecutionState(handle).vforkSnapshot = null;
  hostExecutionState(handle).vforkWorker = null;
}

export async function resolveProcessCwdInoViaKernelWorker(
  kernelWorkerRequest: KernelWorkerRequestFn,
  cwd: string | undefined,
): Promise<number> {
  if (!cwd) {
    return 1;
  }
  const response = await kernelWorkerRequest({
    type: "stat",
    path: cwd,
  });
  const statResponse = expectKernelWorkerResponse(response, "stat-result", "stat");
  if ((statResponse.stat.mode & 0o170000) !== 0o040000) {
    throw new Error(`cwd is not a directory: ${cwd}`);
  }
  return statResponse.stat.ino;
}

export function createWorkerKernelRpcHelpers(deps: WorkerKernelRpcDeps) {
  function kernelRpcSyncCause(name: string): SyncCause {
    return { kind: "syscall", nr: -1n, name };
  }

  async function syncSocketStateForKernelRpc(handle: ProcessHandle, cause: SyncCause): Promise<{
    socketUsed: number;
    netBufUsed: bigint;
  }> {
    return syncSocketStatePreservingNetworkBridge(handle, cause, deps.syncSocketStateViaKernelWorker);
  }

  const kernelRpcLifecycleHelpers = createWorkerKernelRpcLifecycleHelpers({
    appendWorkerDebugLog: deps.appendWorkerDebugLog,
    processHandles: deps.processHandles,
    threadWorkerStates: deps.threadWorkerStates,
    drainProcessOutput,
    enqueueWorker: deps.enqueueWorker,
    failProcess: deps.failProcess,
    notifyHostEffectProgress: deps.notifyHostEffectProgress,
    requestPipeStateSync: deps.requestPipeStateSync,
    settleVisibleProcessOutput,
    syncProcessMemfsViaKernelWorker: deps.syncProcessMemfsViaKernelWorker,
    syncSocketStateViaKernelWorker: syncSocketStateForKernelRpc,
    snapshotProcessFdOfdStateForKernelWorker: deps.snapshotProcessFdOfdStateForKernelWorker,
    applySocketStateFromKernelWorker: deps.applySocketStateFromKernelWorker,
    clearVforkSnapshot,
  });

  const {
    canonicalizeActiveVforkChildViaKernelWorker,
    canonicalizeChildLifecycleViaKernelWorker,
    initializeProcessLifecycleViaKernelWorker,
    recordChildExitViaKernelWorker,
    registerProcessWithKernelWorker,
    rollbackVforkExecveHandoffViaKernelWorker,
    resumeConnectViaKernelWorker,
    resumeVforkExecveParentViaKernelWorker,
    resumeVforkParentViaKernelWorker,
    resumeWait4ViaKernelWorker,
    syncProcessMemfsViaKernelWorker,
    syncProcessLifecycleViaKernelWorker,
    unregisterProcessWithKernelWorker,
  } = kernelRpcLifecycleHelpers;
  const kernelRpcFsHelpers = createWorkerKernelRpcFsHelpers({
    threadWorkerStates: deps.threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    appendWorkerDebugLog: deps.appendWorkerDebugLog,
    assertProcessStandardFdIntegrity: deps.assertProcessStandardFdIntegrity,
    enqueueWorker: deps.enqueueWorker,
    failProcess: deps.failProcess,
    handleExecveSpawn: deps.handleExecveSpawn,
    markRuntimeFsDirty: deps.markRuntimeFsDirty,
    notifyHostEffectProgress: deps.notifyHostEffectProgress,
    kernelShareOtherMembers,
    syncDirtyMemfsPeerViaKernelWorker: deps.syncForkSharedStateViaKernelWorker,
    syncSharedOfdStateAfterFs: (handle, targets, options) => (
      deps.syncForkSharedStateViaKernelWorker(
        handle,
        targets,
        {
          cause: kernelRpcSyncCause("fs-shared-ofd-sync"),
          includeMemfs: false,
          trackHeldOfds: false,
          deltaOfdIds: true,
          sourceVmPtr: options.sourceVmPtr,
        },
      )
    ),
    rehydrateForkSharedStateViaKernelWorker: deps.rehydrateForkSharedStateViaKernelWorker,
    publishRuntimeKernelState: (handle) => publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true),
  });
  const kernelRpcBlockedRpcHelpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: deps.processHandles,
    threadWorkerStates: deps.threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    appendWorkerDebugLog: deps.appendWorkerDebugLog,
    failProcess: deps.failProcess,
    clearHostBlockedRetryTimer: deps.clearHostBlockedRetryTimer,
    requestThreadWorkerBlockedResume: deps.requestThreadWorkerBlockedResume,
    requestPipeStateSync: deps.requestPipeStateSync,
    syncBlockedForkSharedStateViaKernelWorker: deps.syncBlockedForkSharedStateViaKernelWorker,
    syncSocketStateViaKernelWorker: syncSocketStateForKernelRpc,
  });

  return {
    canonicalizeActiveVforkChildViaKernelWorker,
    canonicalizeChildLifecycleViaKernelWorker,
    initializeProcessLifecycleViaKernelWorker,
    recordChildExitViaKernelWorker,
    registerProcessWithKernelWorker,
    executeHostBlockedSyscallResumeOutcome: kernelRpcBlockedRpcHelpers.executeHostBlockedSyscallResumeOutcome,
    resumeConnectViaKernelWorker,
    resumeFsViaKernelWorkerOutcome: kernelRpcFsHelpers.resumeFsViaKernelWorkerOutcome,
    resumeFsViaKernelWorker: kernelRpcFsHelpers.resumeFsViaKernelWorker,
    rollbackVforkExecveHandoffViaKernelWorker,
    resumeVforkExecveParentViaKernelWorker,
    resumeVforkParentViaKernelWorker,
    resumeWait4ViaKernelWorker,
    syncProcessMemfsViaKernelWorker,
    syncProcessLifecycleViaKernelWorker,
    unregisterProcessWithKernelWorker,
  };
}
