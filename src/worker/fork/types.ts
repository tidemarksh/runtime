import type { AttachThreadWorkerRequest, CanonicalizedChildLifecycle, KernelWorkerMemfsSeedMode, KernelWorkerPipeSeedMode, ProcessHandle, ThreadWorkerState } from "../types.js";
import type { KernelWorkerPidBytesSnapshot } from "../../messages.js";
import type { KernelRpcResumeOutcome } from "../kernel-rpc/outcomes.js";
import type { SyncCause } from "../contracts/sync.js";

export interface WorkerForkDeps {
  processHandles: Map<number, ProcessHandle>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void;
  attachThreadWorker(handle: ProcessHandle, request: AttachThreadWorkerRequest): void;
  clearVforkSnapshot(handle: ProcessHandle): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  initializeProcessLifecycleViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
    ppid: number,
    nextPid: number,
    pgid: number,
    sid: number,
    flushSharedMappings: boolean,
  ): Promise<void>;
  notifyForkProgress(handle: ProcessHandle): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  rehydrateForkSharedStateViaKernelWorker(
    targets: ProcessHandle[],
    ofdIds: number[],
    options: { cause: SyncCause; procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean },
  ): Promise<void>;
  registerProcessWithKernelWorker(
    handle: ProcessHandle,
    options?: { seedMemfsMode?: KernelWorkerMemfsSeedMode; pipeSeedMode?: KernelWorkerPipeSeedMode },
  ): Promise<void>;
  resumeVforkExecveParentViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
    childPid: number,
  ): Promise<KernelRpcResumeOutcome>;
  rollbackVforkExecveHandoffViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
    childPid: number,
  ): Promise<KernelRpcResumeOutcome>;
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
  syncProcessMemfsViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<void>;
  canonicalizeChildLifecycleViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
    childPid: number,
  ): Promise<CanonicalizedChildLifecycle>;
  bootStrapMemoryBytes: number;
  wasmPageSize: number;
  wasmMaxPages: number;
}

export type { ProcessHandle, ThreadWorkerState };
