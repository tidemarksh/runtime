import { attachKernelRuntimeState, readKernelRuntimeState } from "../../abi.js";
import { MAX_INODES } from "../../fs/sab-page-cache.js";
import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
} from "../../messages.js";
import {
  clearMemoryRange,
  markMaterializedMemfsFile,
  MEMFS_S_IFMT,
  MEMFS_S_IFREG,
  writeScratchFor,
} from "../common.js";
import type { KernelWorkerSharedStateSyncDeps, KernelWorkerPidBytesSnapshot, KernelWorkerPidStateSnapshot, KernelRuntimeState } from "./types.js";

export function createKernelWorkerForkSharedStateSync(deps: KernelWorkerSharedStateSyncDeps) {
  interface RegularFileDataSnapshot {
    ino: number;
    data: Uint8Array;
  }

  function readRegularFileData(targetExports: ReturnType<KernelWorkerSharedStateSyncDeps["requireProcessKernel"]>, ino: number): Uint8Array | null {
    if ((targetExports.memfs_inode_mode(ino) & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
      return null;
    }
    const size = Number(targetExports.memfs_file_size(ino));
    if (size <= 0) {
      return null;
    }
    const ptr = writeScratchFor(targetExports, new Uint8Array(size));
    const read = Number(targetExports.memfs_read_file_to(ino, 0n, ptr, size));
    if (read < 0) {
      return null;
    }
    return new Uint8Array(targetExports.memory.buffer.slice(ptr, ptr + read));
  }

  function snapshotRegularFileData(targetExports: ReturnType<KernelWorkerSharedStateSyncDeps["requireProcessKernel"]>): RegularFileDataSnapshot[] {
    const snapshots: RegularFileDataSnapshot[] = [];
    for (let ino = 1; ino < MAX_INODES; ino += 1) {
      const data = readRegularFileData(targetExports, ino);
      if (data !== null) {
        snapshots.push({ ino, data });
      }
    }
    return snapshots;
  }

  function restoreRegularFileData(
    targetExports: ReturnType<KernelWorkerSharedStateSyncDeps["requireProcessKernel"]>,
    snapshots: RegularFileDataSnapshot[],
  ): void {
    for (const snapshot of snapshots) {
      if ((targetExports.memfs_inode_mode(snapshot.ino) & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
        continue;
      }
      if (Number(targetExports.memfs_file_size(snapshot.ino)) !== snapshot.data.length) {
        continue;
      }
      const truncateResult = Number(targetExports.memfs_truncate_file(snapshot.ino, BigInt(snapshot.data.length)));
      if (truncateResult < 0) {
        continue;
      }
      if (snapshot.data.length > 0) {
        const ptr = writeScratchFor(targetExports, snapshot.data);
        const written = Number(targetExports.memfs_write_file_at(snapshot.ino, 0n, ptr, snapshot.data.length));
        if (written !== snapshot.data.length) {
          continue;
        }
      }
      markMaterializedMemfsFile(targetExports, snapshot.ino);
    }
  }

  function restoreRegularFileDataToCanonicalAndSource(
    sourceExports: ReturnType<KernelWorkerSharedStateSyncDeps["requireProcessKernel"]>,
    snapshots: RegularFileDataSnapshot[],
  ): void {
    if (snapshots.length === 0) {
      return;
    }
    restoreRegularFileData(deps.getCanonicalExports(), snapshots);
    restoreRegularFileData(sourceExports, snapshots);
  }

  function applyFdEntrySnapshot(
    processExports: ReturnType<KernelWorkerSharedStateSyncDeps["requireProcessKernel"]>,
    vmPtr: number,
    fdEntries: KernelWorkerFdEntrySnapshot[],
    ofdIds: Iterable<number>,
  ): void {
    if (vmPtr === 0 || fdEntries.length === 0) {
      return;
    }
    const maxFds = processExports.abi_max_fds();
    const trackedOfdIds = new Set(
      [...ofdIds].filter((ofdId) => ofdId > 0 && ofdId < maxFds),
    );
    for (const entry of fdEntries) {
      if (entry.fd < 0 || entry.fd >= maxFds || entry.ofdId < 0 || entry.ofdId >= maxFds) {
        continue;
      }
      const previousOfdId = processExports.process_fd_ofd_id(vmPtr, entry.fd);
      if (entry.ofdId !== 0 && trackedOfdIds.has(entry.ofdId)) {
        processExports.process_set_fd_entry(vmPtr, entry.fd, entry.ofdId, entry.fdFlags);
      } else if (previousOfdId !== 0 && trackedOfdIds.has(previousOfdId)) {
        processExports.process_set_fd_entry(vmPtr, entry.fd, 0, 0);
      }
    }
  }

  function syncForkSharedState(
    pid: number,
    vmPtr: number,
    targetPids: number[],
    targetKernelStates: KernelWorkerPidStateSnapshot[],
    ofdIds: number[],
    ofdSlots: KernelWorkerOfdSlotSnapshot[],
    fdEntries: KernelWorkerFdEntrySnapshot[],
    includeMemfs: boolean,
    preserveSourceRefcounts: boolean,
    preservePublishedRegularFileData: boolean,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState {
    const canonicalExports = deps.getCanonicalExports();
    const processExports = deps.processKernels.get(pid);
    if (!processExports) {
      return kernelState;
    }
    attachKernelRuntimeState(processExports, kernelState);
    applyFdEntrySnapshot(processExports, vmPtr, fdEntries, ofdIds);
    const preservedRegularFileData = includeMemfs && preservePublishedRegularFileData
      ? snapshotRegularFileData(canonicalExports)
      : [];

    const sourceState = readKernelRuntimeState(processExports);
    const snappedOfdIds = new Set<number>();
    if (ofdSlots.length > 0) {
      const sourceOfdSlots = ofdSlots.filter((snapshot) => {
        const view = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
        return view.getInt32(0, true) !== processExports.abi_fd_type_file();
      });
      deps.applySelectedOfdSnapshots(processExports, sourceState, sourceOfdSlots);
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
    const sharedPipeSlots = deps.collectPipeSlotsForOfdIds(processExports, sourceHeldOfdIds);
    if (vmPtr !== 0 && sharedPipeSlots.length > 0) {
      deps.mergePipeSlotsFromProcess(
        processExports,
        sourceState,
        deps.collectProcessPipeSlotModes(processExports, vmPtr),
        canonicalExports,
        canonicalState,
        sharedPipeSlots,
      );
    }
    if (includeMemfs) {
      const canonicalProcSelfExe = deps.snapshotProcSelfExe(canonicalExports);
      deps.copyMemfsState(processExports, sourceState, canonicalExports, canonicalState, {
        includeData: true,
      });
      deps.restoreProcSelfExe(canonicalExports, canonicalProcSelfExe);
      if (preservePublishedRegularFileData) {
        deps.restorePublishedRegularFileDataFromPageCache?.();
        restoreRegularFileDataToCanonicalAndSource(processExports, preservedRegularFileData);
      }
    }

    const memberVmPtrs = new Map<number, number>([[pid, vmPtr]]);
    const memberPids = new Set<number>([pid]);
    for (const target of targetKernelStates) {
      const targetExports = deps.processKernels.get(target.pid);
      if (targetExports) {
        attachKernelRuntimeState(targetExports, target.kernelState);
        memberVmPtrs.set(target.pid, target.vmPtr);
      }
    }
    for (const targetPid of targetPids) {
      if (deps.processKernels.has(targetPid)) {
        memberPids.add(targetPid);
      }
    }
    if (!preserveSourceRefcounts) {
      deps.reconcileCanonicalOfdRefcounts(memberPids, memberVmPtrs, ofdIds);
    }
    const {
      kernelState: reconciledCanonicalState,
      readOpen,
      writeOpen,
      managedPipeSlots,
    } = deps.recomputeCanonicalPipeControlState();
    attachKernelRuntimeState(processExports, sourceState);
    const sourceRehydrateOfdIds = ofdIds.filter(
      (ofdId) => processExports.kernel_ofd_fd_type(ofdId) !== processExports.abi_fd_type_file(),
    );
    deps.copySelectedOfdState(canonicalExports, reconciledCanonicalState, processExports, sourceState, sourceRehydrateOfdIds);
    deps.applyPipeControlState(
      processExports,
      sourceState,
      readOpen,
      writeOpen,
      deps.composeProcessPipeUsed(processExports, vmPtr, reconciledCanonicalState.pipeUsed),
      managedPipeSlots,
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
      const targetExports = deps.processKernels.get(targetPid);
      if (!targetExports) {
        continue;
      }
      const targetKernelState = targetStateByPid.get(targetPid);
      if (targetKernelState) {
        attachKernelRuntimeState(targetExports, targetKernelState);
      }
      const targetState = readKernelRuntimeState(targetExports);
      attachKernelRuntimeState(targetExports, targetState);
      deps.copySelectedOfdState(canonicalExports, reconciledCanonicalState, targetExports, targetState, ofdIds);
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
        managedPipeSlots,
      );
      if (includeMemfs) {
        const targetProcSelfExe = deps.snapshotProcSelfExe(targetExports);
        deps.copyCanonicalMemfsToProcess(reconciledCanonicalState, targetExports, targetState, {
          includeData: true,
        });
        deps.restoreProcSelfExe(targetExports, targetProcSelfExe);
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
      managedPipeSlots,
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
        managedPipeSlots,
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
