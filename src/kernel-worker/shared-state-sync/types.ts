import type { KernelExports, KernelRuntimeState } from "../../abi.js";
import type {
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPidStateSnapshot,
} from "../../messages.js";

export interface RecomputedPipeControlState {
  kernelState: KernelRuntimeState;
  readOpen: Int32Array;
  writeOpen: Int32Array;
  managedPipeSlots: number[];
}

export interface KernelWorkerSharedStateSyncDeps {
  getCanonicalExports(): KernelExports;
  getCanonicalProcessVmPtr(pid: number): number;
  processKernels: Map<number, KernelExports>;
  lastProcessPipeSlotModes: Map<number, Map<number, number>>;
  requireProcessKernel(pid: number): KernelExports;
  applySelectedOfdSnapshots(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    snapshots: KernelWorkerOfdSlotSnapshot[],
  ): void;
  collectProcessHeldOfdRefcounts(processExports: KernelExports, vmPtr: number): Int32Array;
  collectProcessHeldEpollOfdIds(processExports: KernelExports, vmPtr: number): number[];
  collectProcessHeldPipeSlots(processExports: KernelExports, vmPtr: number): Set<number>;
  collectProcessPipeSlotModes(processExports: KernelExports, vmPtr: number): Map<number, number>;
  collectProcessPrivatePipeSlots(processExports: KernelExports, vmPtr: number): Set<number>;
  collectCanonicalManagedPipeSlots(kernelState: KernelRuntimeState): number[];
  copySelectedOfdState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    ofdIds: Iterable<number>,
  ): void;
  copySelectedEpollState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    epollOfdIds: Iterable<number>,
  ): void;
  clearSelectedOfdState(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    ofdIds: Iterable<number>,
  ): void;
  copyMemfsState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    options?: { includeData?: boolean },
  ): void;
  clearMemfsState(targetExports: KernelExports, targetState: KernelRuntimeState): void;
  reconcileCanonicalOfdRefcounts(
    memberPids: ReadonlySet<number>,
    memberVmPtrs: ReadonlyMap<number, number>,
    ofdIds: Iterable<number>,
  ): void;
  recomputeCanonicalPipeControlState(): RecomputedPipeControlState;
  collectPipeSlotsForOfdIds(targetExports: KernelExports, ofdIds: Iterable<number>): number[];
  copySelectedPipeSlots(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    pipeSlots: Iterable<number>,
  ): void;
  clearSelectedPipeSlots(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    pipeSlots: Iterable<number>,
  ): void;
  mergePipeSlotsFromProcess(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    sourceModes: ReadonlyMap<number, number>,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    pipeSlots: Iterable<number>,
  ): void;
  mergePipeSlotsFromCanonical(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    targetVmPtr: number,
    pipeSlots: Iterable<number>,
  ): void;
  applyPipeControlState(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    readOpen: Int32Array,
    writeOpen: Int32Array,
    pipeUsed: bigint,
    managedPipeSlots: Iterable<number>,
  ): void;
  composeProcessPipeUsed(
    processExports: KernelExports,
    vmPtr: number,
    sharedPipeUsed: bigint,
  ): bigint;
  copyCanonicalMemfsToProcess(
    canonicalState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    options?: { includeData?: boolean },
  ): void;
  snapshotProcSelfExe(targetExports: KernelExports): Uint8Array | null;
  restoreProcSelfExe(targetExports: KernelExports, target: Uint8Array | null): void;
  copyEpollState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void;
  copySocketState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void;
}

export type {
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPidStateSnapshot,
  KernelRuntimeState,
};
