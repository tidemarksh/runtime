import type { CanonicalizedChildLifecycle, KernelWorkerMemfsSeedMode, KernelWorkerPipeSeedMode, ProcessHandle, ThreadWorkerState } from "../types.js";
import type { KernelWorkerPidBytesSnapshot } from "../../messages.js";

export interface WorkerForkDeps {
  processHandles: Map<number, ProcessHandle>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void;
  attachThreadWorker(handle: ProcessHandle, worker: Worker, vmPtr: number): void;
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
  kickBlockedWorkers(handle: ProcessHandle): void;
  recordChildExitViaKernelWorker(
    parent: ProcessHandle,
    childPid: number,
    waitStatus: number,
  ): Promise<void>;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  rehydrateForkSharedStateViaKernelWorker(
    targets: ProcessHandle[],
    ofdIds: number[],
    options?: { procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean },
  ): Promise<void>;
  registerProcessWithKernelWorker(
    handle: ProcessHandle,
    options?: { seedMemfsMode?: KernelWorkerMemfsSeedMode; pipeSeedMode?: KernelWorkerPipeSeedMode },
  ): Promise<void>;
  resumeVforkExecveParentViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
    childPid: number,
  ): Promise<void>;
  syncForkSharedStateLocally(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options?: { includeMemfs?: boolean; trackHeldOfds?: boolean },
  ): void;
  syncForkSharedStateViaKernelWorker(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options?: { includeMemfs?: boolean; trackHeldOfds?: boolean; preserveSourceRefcounts?: boolean },
  ): Promise<void>;
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
