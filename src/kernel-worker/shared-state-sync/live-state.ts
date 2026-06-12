import { attachKernelRuntimeState, readKernelRuntimeState, type KernelExports } from "../../abi.js";
import { copyMemoryRange } from "../common.js";
import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPipeSlotSnapshot,
} from "../../messages.js";
import {
  PIPE_BUFFER_BYTES,
  PIPE_SLOT_BYTES,
  PIPE_SLOT_MODE_CONTROL,
  PIPE_SLOT_MODE_READ,
  PIPE_SLOT_MODE_WRITE,
} from "../../layout/pipe.js";
import type { KernelWorkerSharedStateSyncDeps, KernelRuntimeState } from "./types.js";

const PIPE_BUF_SIZE = PIPE_BUFFER_BYTES;

export function createKernelWorkerLiveStateSync(deps: KernelWorkerSharedStateSyncDeps) {
  function copyTimeState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void {
    const byteLength = sourceExports.kernel_time_state_bytes?.() ?? 0;
    if (byteLength <= 0) {
      return;
    }
    copyMemoryRange(
      sourceExports.memory,
      sourceState.timeStatePtr,
      targetExports.memory,
      targetState.timeStatePtr,
      byteLength,
    );
  }

  function ofdIdsIncludeTimerfd(sourceExports: KernelExports, ofdIds: Iterable<number>): boolean {
    const timerfdType = sourceExports.abi_fd_type_timerfd?.();
    if (timerfdType === undefined) {
      return false;
    }
    const maxFds = sourceExports.abi_max_fds();
    for (const ofdId of ofdIds) {
      if (
        ofdId > 0
        && ofdId < maxFds
        && sourceExports.kernel_ofd_fd_type(ofdId) === timerfdType
      ) {
        return true;
      }
    }
    return false;
  }

  function snapshotCanonicalPipeSlots(
    kernelState: KernelRuntimeState,
    pipeSlots: Iterable<number>,
    pipeSlotModes?: ReadonlyMap<number, number>,
  ): KernelWorkerPipeSlotSnapshot[] {
    const canonicalExports = deps.getCanonicalExports();
    const memory = new Uint8Array(canonicalExports.memory.buffer);
    const snapshots: KernelWorkerPipeSlotSnapshot[] = [];
    for (const pipeSlot of pipeSlots) {
      if (pipeSlot <= 0 || pipeSlot >= canonicalExports.abi_max_pipes()) {
        continue;
      }
      const offset = kernelState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
      const slot = memory.slice(offset, offset + PIPE_SLOT_BYTES);
      const mode = pipeSlotModes?.get(pipeSlot);
      snapshots.push({
        pipeSlot,
        slot,
        mode: mode === undefined ? undefined : mode | PIPE_SLOT_MODE_CONTROL,
      });
    }
    return snapshots;
  }

  function syncPipeState(
    pid: number,
    kernelState: KernelRuntimeState,
    fdEntries?: KernelWorkerFdEntrySnapshot[],
    ofdSlots?: KernelWorkerOfdSlotSnapshot[],
    pipeSlots?: KernelWorkerPipeSlotSnapshot[],
    vmPtrOverride?: number,
    applyFdEntries = false,
  ): { kernelState: KernelRuntimeState; pipeSlots: KernelWorkerPipeSlotSnapshot[] } {
    const canonicalExports = deps.getCanonicalExports();
    const processExports = deps.processKernels.get(pid);
    if (!processExports) {
      return { kernelState, pipeSlots: [] };
    }
    attachKernelRuntimeState(processExports, kernelState);

    const sourceState = readKernelRuntimeState(processExports);
    const canonicalState = readKernelRuntimeState(canonicalExports);
    const sourceVmPtr = vmPtrOverride ?? deps.getCanonicalProcessVmPtr(pid);
    if (ofdSlots && ofdSlots.length > 0) {
      deps.applySelectedOfdSnapshots(processExports, sourceState, ofdSlots);
      deps.applySelectedOfdSnapshots(canonicalExports, canonicalState, ofdSlots);
    }
    if (applyFdEntries && fdEntries && fdEntries.length > 0 && sourceVmPtr !== 0) {
      const maxFds = processExports.abi_max_fds();
      for (const entry of fdEntries) {
        if (entry.fd < 0 || entry.fd >= maxFds || entry.ofdId < 0 || entry.ofdId >= maxFds) {
          continue;
        }
        processExports.process_set_fd_entry(sourceVmPtr, entry.fd, entry.ofdId, entry.fdFlags);
      }
    }
    // `processExports` is backed by the live process memory. Applying an older
    // fd snapshot here can race with dup/close/pipe2 in the running worker and
    // roll the live fd table backward. The fd entries are still used by the
    // caller to provide OFD/pipe snapshots; the live process table remains the
    // source of truth for the source process itself.
    if (pipeSlots && pipeSlots.length > 0) {
      deps.applySelectedPipeSlotSnapshots(processExports, sourceState, pipeSlots);
      deps.applySelectedPipeSlotSnapshots(canonicalExports, canonicalState, pipeSlots);
    }
    const canonicalManagedPipeSlots = new Set<number>(deps.collectCanonicalManagedPipeSlots(canonicalState));
    if (sourceVmPtr !== 0) {
      const privatePipeSlotModes = deps.collectProcessPrivatePipeSourceModes(processExports, sourceVmPtr, pid);
      for (const pipeSlot of privatePipeSlotModes.keys()) {
        canonicalManagedPipeSlots.add(pipeSlot);
      }
      for (const pipeSlot of deps.collectProcessHeldPipeSlots(processExports, sourceVmPtr)) {
        canonicalManagedPipeSlots.add(pipeSlot);
      }
    }
    const currentSourcePipeSlotModes = sourceVmPtr !== 0
      ? deps.collectProcessPipeSlotModes(processExports, sourceVmPtr)
      : new Map<number, number>();
    const snapshotPipeSlotModes = new Map<number, number>();
    if (pipeSlots && pipeSlots.length > 0) {
      for (const snapshot of pipeSlots) {
        const mode = snapshot.mode === undefined
          ? PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE
          : snapshot.mode & (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE);
        if (mode !== 0) {
          snapshotPipeSlotModes.set(snapshot.pipeSlot, mode);
        }
      }
    }
    const previousSourcePipeSlotModes = deps.lastProcessPipeSlotModes.get(pid) ?? new Map<number, number>();
    const sourcePipeSlotModes = new Map(currentSourcePipeSlotModes);
    const sourceSyncPipeSlots = new Set<number>(sourcePipeSlotModes.keys());
    const sourcePipeView = new DataView(processExports.memory.buffer);
    const canonicalPipeView = new DataView(canonicalExports.memory.buffer);

    function counterAhead(sourceValue: number, targetValue: number): boolean {
      return sourceValue > targetValue
        || (sourceValue < targetValue && targetValue > PIPE_BUF_SIZE / 2 && sourceValue < PIPE_BUF_SIZE / 2);
    }

    if (sourceVmPtr !== 0) {
      const privatePipeSlotModes = deps.collectProcessPrivatePipeSourceModes(processExports, sourceVmPtr, pid);
      const privatePipeSlots = new Set<number>(privatePipeSlotModes.keys());
      for (const [pipeSlot, mode] of privatePipeSlotModes) {
        sourcePipeSlotModes.set(pipeSlot, (sourcePipeSlotModes.get(pipeSlot) ?? 0) | mode);
        sourceSyncPipeSlots.add(pipeSlot);
      }
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
    for (const [pipeSlot, mode] of snapshotPipeSlotModes) {
      sourcePipeSlotModes.set(pipeSlot, mode);
      sourceSyncPipeSlots.add(pipeSlot);
      canonicalManagedPipeSlots.add(pipeSlot);
    }
    const maxPipeSlots = Math.min(processExports.abi_max_pipes(), canonicalExports.abi_max_pipes());
    for (let pipeSlot = 1; pipeSlot < maxPipeSlots; pipeSlot += 1) {
      const sourceOffset = sourceState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
      const canonicalOffset = canonicalState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
      const sourceHead = sourcePipeView.getUint32(sourceOffset, true);
      const sourceTail = sourcePipeView.getUint32(sourceOffset + 4, true);
      const canonicalHead = canonicalPipeView.getUint32(canonicalOffset, true);
      const canonicalTail = canonicalPipeView.getUint32(canonicalOffset + 4, true);
      let mode = sourcePipeSlotModes.get(pipeSlot) ?? 0;
      if ((mode & PIPE_SLOT_MODE_READ) !== 0 && counterAhead(sourceHead, canonicalHead)) {
        mode |= PIPE_SLOT_MODE_READ;
      }
      if ((mode & PIPE_SLOT_MODE_WRITE) !== 0 && counterAhead(sourceTail, canonicalTail)) {
        mode |= PIPE_SLOT_MODE_WRITE;
      }
      if (mode !== 0) {
        canonicalManagedPipeSlots.add(pipeSlot);
        sourcePipeSlotModes.set(pipeSlot, mode);
        sourceSyncPipeSlots.add(pipeSlot);
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

    return {
      kernelState: readKernelRuntimeState(processExports),
      pipeSlots: snapshotCanonicalPipeSlots(
        reconciledCanonicalState,
        [...sourceSyncPipeSlots].sort((left, right) => left - right),
        sourcePipeSlotModes,
      ),
    };
  }

  function syncEpollState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState {
    const canonicalExports = deps.getCanonicalExports();
    const processExports = deps.processKernels.get(pid);
    if (!processExports) {
      return kernelState;
    }
    attachKernelRuntimeState(processExports, kernelState);

    const sourceVmPtr = deps.getCanonicalProcessVmPtr(pid);
    if (sourceVmPtr === 0) {
      return readKernelRuntimeState(processExports);
    }

    const sourceHeldEpollOfdIds = deps.collectProcessHeldEpollOfdIds(processExports, sourceVmPtr);
    if (sourceHeldEpollOfdIds.length === 0) {
      return readKernelRuntimeState(processExports);
    }

    const sourceState = readKernelRuntimeState(processExports);
    const canonicalState = readKernelRuntimeState(canonicalExports);
    deps.copySelectedOfdState(
      processExports,
      sourceState,
      canonicalExports,
      canonicalState,
      sourceHeldEpollOfdIds,
    );
    deps.copySelectedEpollState(
      processExports,
      sourceState,
      canonicalExports,
      canonicalState,
      sourceHeldEpollOfdIds,
    );

    for (const [memberPid, memberExports] of deps.processKernels) {
      const memberVmPtr = deps.getCanonicalProcessVmPtr(memberPid);
      if (memberVmPtr === 0) {
        continue;
      }
      const memberState = readKernelRuntimeState(memberExports);
      const memberHeldEpollOfdIds = new Set(
        deps.collectProcessHeldEpollOfdIds(memberExports, memberVmPtr),
      );
      const sharedEpollOfdIds = sourceHeldEpollOfdIds.filter((ofdId) => memberHeldEpollOfdIds.has(ofdId));
      if (sharedEpollOfdIds.length === 0) {
        continue;
      }
      deps.copySelectedOfdState(
        canonicalExports,
        canonicalState,
        memberExports,
        memberState,
        sharedEpollOfdIds,
      );
      deps.copySelectedEpollState(
        canonicalExports,
        canonicalState,
        memberExports,
        memberState,
        sharedEpollOfdIds,
      );
    }

    return readKernelRuntimeState(processExports);
  }

  function syncOfdState(
    pid: number,
    ofdIds: number[],
    kernelState: KernelRuntimeState,
    mode: "push" | "pull" = "push",
  ): KernelRuntimeState {
    const canonicalExports = deps.getCanonicalExports();
    const processExports = deps.processKernels.get(pid);
    if (!processExports || ofdIds.length === 0) {
      return kernelState;
    }
    attachKernelRuntimeState(processExports, kernelState);

    const sourceState = readKernelRuntimeState(processExports);
    const canonicalState = readKernelRuntimeState(canonicalExports);
    const syncTimerfdState = ofdIdsIncludeTimerfd(processExports, ofdIds);
    if (mode !== "pull") {
      deps.copySelectedOfdState(processExports, sourceState, canonicalExports, canonicalState, ofdIds);
    }
    if (syncTimerfdState && mode !== "pull") {
      copyTimeState(processExports, sourceState, canonicalExports, canonicalState);
    }

    for (const [memberPid, memberExports] of deps.processKernels) {
      const memberVmPtr = deps.getCanonicalProcessVmPtr(memberPid);
      if (memberVmPtr === 0) {
        continue;
      }
      const memberHeldOfdRefcounts = deps.collectProcessHeldOfdRefcounts(memberExports, memberVmPtr);
      const memberOfdIds = ofdIds.filter((ofdId) => (memberHeldOfdRefcounts[ofdId] ?? 0) > 0);
      if (memberOfdIds.length === 0) {
        continue;
      }
      const memberState = readKernelRuntimeState(memberExports);
      deps.copySelectedOfdState(canonicalExports, canonicalState, memberExports, memberState, memberOfdIds);
      if (syncTimerfdState) {
        copyTimeState(canonicalExports, canonicalState, memberExports, memberState);
      }
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
    const syncedCanonicalState = readKernelRuntimeState(canonicalExports);

    for (const [memberPid, memberExports] of deps.processKernels) {
      const memberState = readKernelRuntimeState(memberExports);
      if (
        memberPid !== pid
        && (
          memberState.socketUsed !== syncedCanonicalState.socketUsed
          || memberState.netBufUsed !== syncedCanonicalState.netBufUsed
        )
      ) {
        continue;
      }
      deps.copySocketState(canonicalExports, syncedCanonicalState, memberExports, memberState);
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
    const releasedPrivatePipeSlots = deps.collectProcessPrivatePipeSlots(processExports, vmPtr, pid);
    processExports.process_release_host_stdio(vmPtr);
    if (releasedPrivatePipeSlots.size > 0) {
      const processState = readKernelRuntimeState(processExports);
      const canonicalExports = deps.getCanonicalExports();
      const canonicalState = readKernelRuntimeState(canonicalExports);
      deps.clearSelectedPipeSlots(processExports, processState, releasedPrivatePipeSlots);
      deps.clearSelectedPipeSlots(canonicalExports, canonicalState, releasedPrivatePipeSlots);
    }
    return syncPipeState(pid, readKernelRuntimeState(processExports)).kernelState;
  }

  return {
    releaseHostStdio,
    syncEpollState,
    syncOfdState,
    syncPipeState,
    syncSocketState,
  };
}
