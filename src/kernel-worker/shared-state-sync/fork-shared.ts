import { attachKernelRuntimeState, readKernelRuntimeState } from "../../abi.js";
import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
} from "../../messages.js";
import {
  clearMemoryRange,
  MEMFS_S_IFMT,
  MEMFS_S_IFREG,
  writeScratchFor,
} from "../common.js";
import type { KernelWorkerSharedStateSyncDeps, KernelWorkerPidBytesSnapshot, KernelWorkerPidStateSnapshot, KernelRuntimeState } from "./types.js";

const FORK_SHARED_MEMFS_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_PUBLICATION_DEBUG ?? "") === "1";
const LOADER_PATH_BYTES = new TextEncoder().encode("/lib/ld-linux-riscv64-lp64d.so.1");

export function createKernelWorkerForkSharedStateSync(deps: KernelWorkerSharedStateSyncDeps) {
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

  function describeLoaderHead(
    label: string,
    targetExports: ReturnType<KernelWorkerSharedStateSyncDeps["requireProcessKernel"]>,
  ): string {
    const pathPtr = writeScratchFor(targetExports, LOADER_PATH_BYTES);
    const ino = targetExports.memfs_resolve_path(pathPtr, LOADER_PATH_BYTES.length);
    if (ino === 0) {
      return `${label}=missing`;
    }
    const mode = targetExports.memfs_inode_mode(ino);
    const size = Number(targetExports.memfs_file_size(ino));
    let head = "n/a";
    if ((mode & MEMFS_S_IFMT) === MEMFS_S_IFREG && size > 0) {
      const readLen = Math.min(4, size);
      const readPtr = writeScratchFor(targetExports, new Uint8Array(readLen));
      const read = Number(targetExports.memfs_read_file_to(ino, 0n, readPtr, readLen));
      if (read > 0) {
        head = [...new Uint8Array(targetExports.memory.buffer, readPtr, read)]
          .map((byte) => byte.toString(16).padStart(2, "0"))
          .join("");
      }
    }
    return `${label}=ino${ino}/mode${mode.toString(8)}/size${size}/head${head}`;
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
  ): { kernelState: KernelRuntimeState; forkSharedDebug?: string } {
    const canonicalExports = deps.getCanonicalExports();
    const processExports = deps.processKernels.get(pid);
    if (!processExports) {
      return { kernelState };
    }
    const debug: string[] = [];
    const pushMemfsDebug = (phase: string): void => {
      if (!FORK_SHARED_MEMFS_DEBUG_ENABLED || !includeMemfs) {
        return;
      }
      debug.push([
        `phase=${phase}`,
        `pid=${pid}`,
        `targets=${targetPids.join(".")}`,
        `preserve=${preservePublishedRegularFileData ? 1 : 0}`,
        describeLoaderHead("source", processExports),
        describeLoaderHead("canonical", canonicalExports),
      ].join(":"));
    };
    attachKernelRuntimeState(processExports, kernelState);
    applyFdEntrySnapshot(processExports, vmPtr, fdEntries, ofdIds);
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
      pushMemfsDebug("before-source-to-canonical");
      const canonicalProcSelfExe = deps.snapshotProcSelfExe(canonicalExports);
      deps.copyMemfsState(processExports, sourceState, canonicalExports, canonicalState, {
        includeData: true,
      });
      deps.restoreProcSelfExe(canonicalExports, canonicalProcSelfExe);
      if (preservePublishedRegularFileData) {
        deps.restorePublishedRegularFileDataFromPageCache?.();
      }
      pushMemfsDebug("after-source-to-canonical");
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
        pushMemfsDebug(`before-canonical-to-target-${targetPid}`);
        deps.copyCanonicalMemfsToProcess(reconciledCanonicalState, targetExports, targetState, {
          includeData: true,
        });
        if (preservePublishedRegularFileData) {
          deps.restorePublishedRegularFileDataFromPageCache?.(targetExports);
        }
        if (FORK_SHARED_MEMFS_DEBUG_ENABLED) {
          debug.push([
            `phase=after-canonical-to-target-${targetPid}`,
            `pid=${pid}`,
            `targets=${targetPids.join(".")}`,
            `preserve=${preservePublishedRegularFileData ? 1 : 0}`,
            describeLoaderHead("target", targetExports),
            describeLoaderHead("canonical", canonicalExports),
          ].join(":"));
        }
      }
    }

    return {
      kernelState: readKernelRuntimeState(processExports),
      forkSharedDebug: debug.length > 0 ? debug.join(";") : undefined,
    };
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
