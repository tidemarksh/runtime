import { attachKernelRuntimeState, readKernelRuntimeState } from "../../abi.js";
import type { KernelWorkerOfdSlotSnapshot } from "../../messages.js";
import { clearMemoryRange } from "../common.js";
import type { KernelWorkerSharedStateSyncDeps, KernelWorkerPidBytesSnapshot, KernelWorkerPidStateSnapshot, KernelRuntimeState } from "./types.js";

export function createKernelWorkerForkSharedStateSync(deps: KernelWorkerSharedStateSyncDeps) {
  function syncForkSharedState(
    pid: number,
    vmPtr: number,
    targetPids: number[],
    targetKernelStates: KernelWorkerPidStateSnapshot[],
    ofdIds: number[],
    ofdSlots: KernelWorkerOfdSlotSnapshot[],
    includeMemfs: boolean,
    preserveSourceRefcounts: boolean,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState {
    const canonicalExports = deps.getCanonicalExports();
    const processExports = deps.requireProcessKernel(pid);
    attachKernelRuntimeState(processExports, kernelState);

    const sourceState = readKernelRuntimeState(processExports);
    const snappedOfdIds = new Set<number>();
    if (ofdSlots.length > 0) {
      deps.applySelectedOfdSnapshots(processExports, sourceState, ofdSlots);
      const canonicalState = readKernelRuntimeState(canonicalExports);
      deps.applySelectedOfdSnapshots(canonicalExports, canonicalState, ofdSlots);
      for (const snapshot of ofdSlots) {
        snappedOfdIds.add(snapshot.ofdId);
      }
    }
    const sourceHeldRefcounts = vmPtr !== 0
      ? deps.collectProcessHeldOfdRefcounts(processExports, vmPtr)
      : new Int32Array(processExports.abi_max_fds());
    const sourceHeldOfdIds = ofdIds.filter((ofdId) => {
      if (ofdId <= 0 || ofdId >= processExports.abi_max_fds()) {
        return false;
      }
      return (sourceHeldRefcounts[ofdId] ?? 0) > 0;
    });
    const canonicalState = readKernelRuntimeState(canonicalExports);
    deps.copySelectedOfdState(
      processExports,
      sourceState,
      canonicalExports,
      canonicalState,
      sourceHeldOfdIds.filter((ofdId) => !snappedOfdIds.has(ofdId)),
    );
    if (includeMemfs) {
      deps.copyMemfsState(processExports, sourceState, canonicalExports, canonicalState, {
        includeData: true,
      });
    }

    const memberVmPtrs = new Map<number, number>([[pid, vmPtr]]);
    const memberPids = new Set<number>([pid]);
    for (const target of targetKernelStates) {
      memberVmPtrs.set(target.pid, target.vmPtr);
    }
    for (const targetPid of targetPids) {
      memberPids.add(targetPid);
    }
    if (!preserveSourceRefcounts) {
      deps.reconcileCanonicalOfdRefcounts(memberPids, memberVmPtrs, ofdIds);
    }
    const {
      kernelState: reconciledCanonicalState,
      readOpen,
      writeOpen,
    } = deps.recomputeCanonicalPipeControlState();
    const sharedPipeSlots = deps.collectPipeSlotsForOfdIds(canonicalExports, ofdIds);
    attachKernelRuntimeState(processExports, sourceState);
    deps.copySelectedOfdState(canonicalExports, reconciledCanonicalState, processExports, sourceState, ofdIds);
    deps.copySelectedPipeSlots(canonicalExports, reconciledCanonicalState, processExports, sourceState, sharedPipeSlots);
    deps.applyPipeControlState(
      processExports,
      sourceState,
      readOpen,
      writeOpen,
      deps.composeProcessPipeUsed(processExports, vmPtr, reconciledCanonicalState.pipeUsed),
      sharedPipeSlots,
    );

    const targetStateByPid = new Map<number, KernelRuntimeState>();
    const targetVmPtrByPid = new Map<number, number>();
    for (const target of targetKernelStates) {
      targetStateByPid.set(target.pid, target.kernelState);
      targetVmPtrByPid.set(target.pid, target.vmPtr);
    }
    const uniqueTargets = new Set(targetPids);
    uniqueTargets.delete(pid);
    for (const targetPid of uniqueTargets) {
      const targetExports = deps.requireProcessKernel(targetPid);
      const targetKernelState = targetStateByPid.get(targetPid);
      if (targetKernelState) {
        attachKernelRuntimeState(targetExports, targetKernelState);
      }
      const targetState = readKernelRuntimeState(targetExports);
      attachKernelRuntimeState(targetExports, targetState);
      deps.copySelectedOfdState(canonicalExports, reconciledCanonicalState, targetExports, targetState, ofdIds);
      deps.copySelectedPipeSlots(canonicalExports, reconciledCanonicalState, targetExports, targetState, sharedPipeSlots);
      deps.applyPipeControlState(
        targetExports,
        targetState,
        readOpen,
        writeOpen,
        deps.composeProcessPipeUsed(
          targetExports,
          targetVmPtrByPid.get(targetPid) ?? 0,
          reconciledCanonicalState.pipeUsed,
        ),
        sharedPipeSlots,
      );
      if (includeMemfs) {
        deps.copyCanonicalMemfsToProcess(reconciledCanonicalState, targetExports, targetState, {
          includeData: true,
        });
      }
    }

    return readKernelRuntimeState(processExports);
  }

  function rehydrateForkSharedState(
    targetPids: number[],
    targetKernelStates: KernelWorkerPidStateSnapshot[],
    ofdIds: number[],
    procSelfExeTargets: KernelWorkerPidBytesSnapshot[] = [],
    includeMemfs = true,
  ): void {
    const canonicalExports = deps.getCanonicalExports();
    const {
      kernelState: canonicalState,
      readOpen,
      writeOpen,
    } = deps.recomputeCanonicalPipeControlState();
    const targetStateByPid = new Map<number, KernelRuntimeState>();
    const targetVmPtrByPid = new Map<number, number>();
    for (const target of targetKernelStates) {
      targetStateByPid.set(target.pid, target.kernelState);
      targetVmPtrByPid.set(target.pid, target.vmPtr);
    }
    const procSelfExeTargetByPid = new Map<number, Uint8Array>();
    for (const target of procSelfExeTargets) {
      procSelfExeTargetByPid.set(target.pid, target.bytes);
    }
    const sharedPipeSlots = deps.collectPipeSlotsForOfdIds(canonicalExports, ofdIds);
    const uniqueTargets = new Set(targetPids);
    for (const targetPid of uniqueTargets) {
      const targetExports = deps.requireProcessKernel(targetPid);
      const targetKernelState = targetStateByPid.get(targetPid);
      if (targetKernelState) {
        attachKernelRuntimeState(targetExports, targetKernelState);
      }
      const targetState = readKernelRuntimeState(targetExports);
      const procSelfExe = procSelfExeTargetByPid.get(targetPid)
        ?? (includeMemfs ? deps.snapshotProcSelfExe(targetExports) : null);
      deps.clearSelectedOfdState(targetExports, targetState, ofdIds);
      if (includeMemfs) {
        deps.clearMemfsState(targetExports, targetState);
      }
      deps.clearSelectedPipeSlots(targetExports, targetState, sharedPipeSlots);
      clearMemoryRange(
        targetExports.memory,
        targetState.socketTablePtr,
        targetExports.kernel_socket_table_bytes(),
      );
      clearMemoryRange(
        targetExports.memory,
        targetState.netBufBase,
        targetExports.kernel_net_buf_pool_bytes(),
      );
      deps.copySelectedOfdState(canonicalExports, canonicalState, targetExports, targetState, ofdIds);
      if (includeMemfs) {
        deps.copyMemfsState(canonicalExports, canonicalState, targetExports, targetState, {
          includeData: true,
        });
      }
      if (procSelfExe !== null) {
        deps.restoreProcSelfExe(targetExports, procSelfExe);
      }
      deps.copySelectedPipeSlots(canonicalExports, canonicalState, targetExports, targetState, sharedPipeSlots);
      deps.applyPipeControlState(
        targetExports,
        targetState,
        readOpen,
        writeOpen,
        deps.composeProcessPipeUsed(
          targetExports,
          targetVmPtrByPid.get(targetPid) ?? 0,
          canonicalState.pipeUsed,
        ),
        sharedPipeSlots,
      );
      deps.copySelectedEpollState(canonicalExports, canonicalState, targetExports, targetState, ofdIds);
      deps.copySocketState(canonicalExports, canonicalState, targetExports, targetState);
    }
  }

  return {
    rehydrateForkSharedState,
    syncForkSharedState,
  };
}
