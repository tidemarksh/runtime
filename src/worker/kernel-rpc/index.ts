import { createWorkerKernelRpcLifecycleHelpers } from "./lifecycle.js";
import { createWorkerKernelRpcBlockedRpcHelpers } from "./blocked-rpc.js";
import { createWorkerKernelRpcFsHelpers } from "./fs.js";
import { expectKernelWorkerResponse } from "./transport.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { kernelShareOtherMembers } from "../kernel-share.js";
import { drainProcessOutput } from "../stdio.js";
import { readKernelRuntimeState } from "../../abi.js";
import {
  type KernelWorkerRequestFn,
  type ProcessHandle,
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
  blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleBlockedRetryResult(handle: ProcessHandle, worker: Worker, vmPtr: number, status: number): void;
  kickBlockedWorkers(handle: ProcessHandle): void;
  markRuntimeFsDirty(handle: ProcessHandle): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  syncProcessMemfsViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<void>;
  syncBlockedForkSharedStateViaKernelWorker(handle: ProcessHandle, sourceVmPtr: number | undefined, cause: SyncCause): Promise<void>;
  syncForkSharedStateLocally(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options: { cause: SyncCause; includeMemfs?: boolean; trackHeldOfds?: boolean; ofdIds?: number[] },
  ): void;
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
  syncPipeStateViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<bigint>;
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
  handle.vforkSnapshot = null;
  handle.vforkWorker = null;
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

  async function syncSocketStatePreservingNetworkBridge(handle: ProcessHandle, cause: SyncCause): Promise<{
    socketUsed: number;
    netBufUsed: bigint;
  }> {
    if (handle.networkConnections.size > 0) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      return {
        socketUsed: handle.kernelState.socketUsed,
        netBufUsed: handle.kernelState.netBufUsed,
      };
    }
    return deps.syncSocketStateViaKernelWorker(handle, cause);
  }

  const kernelRpcLifecycleHelpers = createWorkerKernelRpcLifecycleHelpers({
    appendWorkerDebugLog: deps.appendWorkerDebugLog,
    processHandles: deps.processHandles,
    threadWorkerStates: deps.threadWorkerStates,
    drainProcessOutput,
    enqueueWorker: deps.enqueueWorker,
    failProcess: deps.failProcess,
    kickBlockedWorkers: deps.kickBlockedWorkers,
    requestPipeStateSync: deps.requestPipeStateSync,
    syncProcessMemfsViaKernelWorker: deps.syncProcessMemfsViaKernelWorker,
    syncSocketStateViaKernelWorker: syncSocketStatePreservingNetworkBridge,
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
    releaseHostStdioViaKernelWorker,
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
    blockWorker: deps.blockWorker,
    enqueueWorker: deps.enqueueWorker,
    failProcess: deps.failProcess,
    kickBlockedWorkers: deps.kickBlockedWorkers,
    markRuntimeFsDirty: deps.markRuntimeFsDirty,
    kernelShareOtherMembers,
    syncForkSharedStateLocallyAfterFs: deps.syncForkSharedStateLocally,
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
    publishMemfsAfterFs: async (handle, targets, options) => {
      await deps.syncForkSharedStateViaKernelWorker(
        handle,
        targets,
        {
          cause: kernelRpcSyncCause("fs-memfs-publication"),
          includeMemfs: true,
          trackHeldOfds: false,
          memfsOnly: true,
          sourceVmPtr: options.sourceVmPtr,
        },
      );
      await deps.syncPipeStateViaKernelWorker(handle, kernelRpcSyncCause("fs-memfs-pipe-refresh"));
    },
    rehydrateForkSharedStateViaKernelWorker: deps.rehydrateForkSharedStateViaKernelWorker,
    publishRuntimeKernelState: (handle) => publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true),
  });
  const kernelRpcBlockedRpcHelpers = createWorkerKernelRpcBlockedRpcHelpers({
    processHandles: deps.processHandles,
    threadWorkerStates: deps.threadWorkerStates,
    appendRuntimeLifecycleEvent: deps.appendRuntimeLifecycleEvent,
    appendWorkerDebugLog: deps.appendWorkerDebugLog,
    enqueueWorker: deps.enqueueWorker,
    failProcess: deps.failProcess,
    handleBlockedRetryResult: deps.handleBlockedRetryResult,
    requestPipeStateSync: deps.requestPipeStateSync,
    syncBlockedForkSharedStateViaKernelWorker: deps.syncBlockedForkSharedStateViaKernelWorker,
    syncSocketStateViaKernelWorker: syncSocketStatePreservingNetworkBridge,
  });

  return {
    canonicalizeActiveVforkChildViaKernelWorker,
    canonicalizeChildLifecycleViaKernelWorker,
    initializeProcessLifecycleViaKernelWorker,
    recordChildExitViaKernelWorker,
    registerProcessWithKernelWorker,
    releaseHostStdioViaKernelWorker,
    resumeBlockedSyscallOutcome: kernelRpcBlockedRpcHelpers.resumeBlockedSyscallOutcome,
    resumeBlockedSyscallLocally: kernelRpcBlockedRpcHelpers.resumeBlockedSyscallLocally,
    resumeConnectViaKernelWorker,
    resumeFsViaKernelWorkerOutcome: kernelRpcFsHelpers.resumeFsViaKernelWorkerOutcome,
    resumeFsViaKernelWorker: kernelRpcFsHelpers.resumeFsViaKernelWorker,
    resumeVforkExecveParentViaKernelWorker,
    resumeVforkParentViaKernelWorker,
    resumeWait4ViaKernelWorker,
    syncProcessMemfsViaKernelWorker,
    syncProcessLifecycleViaKernelWorker,
    unregisterProcessWithKernelWorker,
  };
}
