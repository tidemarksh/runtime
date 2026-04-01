import { attachKernelRuntimeState, readKernelRuntimeState } from "../../abi.js";
import type { KernelWorkerSharedStateSyncDeps, KernelRuntimeState } from "./types.js";

export function createKernelWorkerLiveStateSync(deps: KernelWorkerSharedStateSyncDeps) {
  function syncPipeState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState {
    const canonicalExports = deps.getCanonicalExports();
    const processExports = deps.processKernels.get(pid);
    if (!processExports) {
      return kernelState;
    }
    attachKernelRuntimeState(processExports, kernelState);

    const sourceState = readKernelRuntimeState(processExports);
    const canonicalState = readKernelRuntimeState(canonicalExports);
    const sourceVmPtr = deps.getCanonicalProcessVmPtr(pid);
    const canonicalManagedPipeSlots = new Set<number>(deps.collectCanonicalManagedPipeSlots(canonicalState));
    if (sourceVmPtr !== 0) {
      for (const pipeSlot of deps.collectProcessHeldPipeSlots(processExports, sourceVmPtr)) {
        canonicalManagedPipeSlots.add(pipeSlot);
      }
    }
    const currentSourcePipeSlotModes = sourceVmPtr !== 0
      ? deps.collectProcessPipeSlotModes(processExports, sourceVmPtr)
      : new Map<number, number>();
    const previousSourcePipeSlotModes = deps.lastProcessPipeSlotModes.get(pid) ?? new Map<number, number>();
    const sourcePipeSlotModes = new Map(currentSourcePipeSlotModes);
    const sourceSyncPipeSlots = new Set<number>(sourcePipeSlotModes.keys());
    if (sourceVmPtr !== 0) {
      const privatePipeSlots = deps.collectProcessPrivatePipeSlots(processExports, sourceVmPtr);
      for (const pipeSlot of deps.collectProcessHeldPipeSlots(processExports, sourceVmPtr)) {
        sourceSyncPipeSlots.add(pipeSlot);
      }
      for (let pipeSlot = 1; pipeSlot < processExports.abi_max_pipes(); pipeSlot += 1) {
        if (privatePipeSlots.has(pipeSlot) || sourcePipeSlotModes.has(pipeSlot)) {
          continue;
        }
        const previousMode = previousSourcePipeSlotModes.get(pipeSlot);
        if (previousMode !== undefined) {
          if (
            (sourceState.pipeUsed & (1n << BigInt(pipeSlot))) === 0n
            && !canonicalManagedPipeSlots.has(pipeSlot)
          ) {
            continue;
          }
          sourcePipeSlotModes.set(pipeSlot, previousMode);
          sourceSyncPipeSlots.add(pipeSlot);
        }
      }
    }
    deps.mergePipeSlotsFromProcess(
      processExports,
      sourceState,
      sourcePipeSlotModes,
      canonicalExports,
      canonicalState,
      canonicalManagedPipeSlots,
    );
    const {
      kernelState: reconciledCanonicalState,
      readOpen,
      writeOpen,
      managedPipeSlots,
    } = deps.recomputeCanonicalPipeControlState();

    for (const [memberPid, memberExports] of deps.processKernels) {
      const memberState = readKernelRuntimeState(memberExports);
      const memberVmPtr = deps.getCanonicalProcessVmPtr(memberPid);
      const memberPipeUsed = memberVmPtr !== 0
        ? deps.composeProcessPipeUsed(memberExports, memberVmPtr, reconciledCanonicalState.pipeUsed)
        : reconciledCanonicalState.pipeUsed;
      attachKernelRuntimeState(memberExports, {
        ...memberState,
        pipeUsed: memberPipeUsed,
      });
      deps.mergePipeSlotsFromCanonical(
        canonicalExports,
        reconciledCanonicalState,
        memberExports,
        memberState,
        memberVmPtr,
        [...sourceSyncPipeSlots].sort((left, right) => left - right),
      );
      deps.applyPipeControlState(
        memberExports,
        memberState,
        readOpen,
        writeOpen,
        memberPipeUsed,
        managedPipeSlots,
      );
    }

    if (currentSourcePipeSlotModes.size === 0) {
      deps.lastProcessPipeSlotModes.delete(pid);
    } else {
      deps.lastProcessPipeSlotModes.set(pid, currentSourcePipeSlotModes);
    }

    return readKernelRuntimeState(processExports);
  }

  function syncEpollState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState {
    const canonicalExports = deps.getCanonicalExports();
    const processExports = deps.processKernels.get(pid);
    if (!processExports) {
      return kernelState;
    }
    attachKernelRuntimeState(processExports, kernelState);

    const sourceState = readKernelRuntimeState(processExports);
    const canonicalState = readKernelRuntimeState(canonicalExports);
    deps.copyEpollState(processExports, sourceState, canonicalExports, canonicalState);

    for (const memberExports of deps.processKernels.values()) {
      const memberState = readKernelRuntimeState(memberExports);
      deps.copyEpollState(canonicalExports, canonicalState, memberExports, memberState);
    }

    return readKernelRuntimeState(processExports);
  }

  function syncOfdState(pid: number, ofdIds: number[], kernelState: KernelRuntimeState): KernelRuntimeState {
    const canonicalExports = deps.getCanonicalExports();
    const processExports = deps.processKernels.get(pid);
    if (!processExports || ofdIds.length === 0) {
      return kernelState;
    }
    attachKernelRuntimeState(processExports, kernelState);

    const sourceState = readKernelRuntimeState(processExports);
    const canonicalState = readKernelRuntimeState(canonicalExports);
    deps.copySelectedOfdState(processExports, sourceState, canonicalExports, canonicalState, ofdIds);

    for (const memberExports of deps.processKernels.values()) {
      const memberState = readKernelRuntimeState(memberExports);
      deps.copySelectedOfdState(canonicalExports, canonicalState, memberExports, memberState, ofdIds);
    }

    return readKernelRuntimeState(processExports);
  }

  function syncSocketState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState {
    const canonicalExports = deps.getCanonicalExports();
    const processExports = deps.processKernels.get(pid);
    if (!processExports) {
      return kernelState;
    }
    attachKernelRuntimeState(processExports, kernelState);

    const sourceState = readKernelRuntimeState(processExports);
    const canonicalState = readKernelRuntimeState(canonicalExports);
    deps.copySocketState(processExports, sourceState, canonicalExports, canonicalState);

    for (const memberExports of deps.processKernels.values()) {
      const memberState = readKernelRuntimeState(memberExports);
      deps.copySocketState(canonicalExports, canonicalState, memberExports, memberState);
    }

    return readKernelRuntimeState(processExports);
  }

  function releaseHostStdio(
    pid: number,
    vmPtr: number,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState {
    const processExports = deps.processKernels.get(pid);
    if (!processExports) {
      return kernelState;
    }
    attachKernelRuntimeState(processExports, kernelState);
    processExports.process_release_host_stdio(vmPtr);
    return syncPipeState(pid, readKernelRuntimeState(processExports));
  }

  return {
    releaseHostStdio,
    syncEpollState,
    syncOfdState,
    syncPipeState,
    syncSocketState,
  };
}
