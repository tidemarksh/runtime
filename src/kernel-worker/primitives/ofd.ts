import {
  kernelOfdStateBytes,
  kernelOfdStatePtr,
  type KernelExports,
  type KernelRuntimeState,
} from "../../abi.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_REFCOUNT_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../layout/ofd.js";
import type { KernelWorkerOfdSlotSnapshot } from "../../messages.js";
import { copyMemoryRange } from "../common.js";

interface KernelWorkerPrimitivesDeps {
  getCanonicalExports(): KernelExports;
  processKernels: Map<number, KernelExports>;
}

function ofdSlotsCanBeShared(
  sourceExports: KernelExports,
  targetExports: KernelExports,
  ofdId: number,
): boolean {
  const sourceType = sourceExports.kernel_ofd_fd_type(ofdId);
  const targetType = targetExports.kernel_ofd_fd_type(ofdId);
  if (sourceType === 0) {
    return targetType === 0;
  }
  if (targetType === 0) {
    return true;
  }
  if (sourceType !== targetType) {
    return false;
  }
  if (
    sourceExports.kernel_ofd_ino(ofdId) !== targetExports.kernel_ofd_ino(ofdId)
    || sourceExports.kernel_ofd_host_fd(ofdId) !== targetExports.kernel_ofd_host_fd(ofdId)
  ) {
    return false;
  }
  if (
    sourceType === sourceExports.abi_fd_type_file()
    && targetExports.kernel_ofd_refcount(ofdId) > 0
    && sourceExports.kernel_ofd_offset(ofdId) < targetExports.kernel_ofd_offset(ofdId)
  ) {
    return false;
  }
  return true;
}

function ofdSnapshotCanApply(
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  snapshot: KernelWorkerOfdSlotSnapshot,
  slotBytes: number,
): boolean {
  const targetType = targetExports.kernel_ofd_fd_type(snapshot.ofdId);
  if (targetType === 0) {
    return true;
  }
  const view = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
  const sourceType = view.getInt32(OFD_TYPE_OFFSET, true);
  if (sourceType === 0) {
    return targetType === 0;
  }
  if (sourceType !== targetType) {
    return false;
  }
  const sourceHostFd = view.getInt32(OFD_HOST_FD_OFFSET, true);
  const sourceIno = view.getUint32(OFD_INO_OFFSET, true);
  const targetOffset = targetState.ofdTablePtr + snapshot.ofdId * slotBytes;
  const targetView = new DataView(targetExports.memory.buffer, targetOffset, slotBytes);
  const targetHostFd = targetView.getInt32(OFD_HOST_FD_OFFSET, true);
  const targetIno = targetView.getUint32(OFD_INO_OFFSET, true);
  if (sourceIno !== targetIno || sourceHostFd !== targetHostFd) {
    return false;
  }
  const fileType = targetExports.abi_fd_type_file?.();
  if (fileType !== undefined) {
    if (
      sourceType === fileType
      && targetView.getInt32(OFD_REFCOUNT_OFFSET, true) > 0
      && view.getBigInt64(OFD_OFFSET_OFFSET, true) < targetView.getBigInt64(OFD_OFFSET_OFFSET, true)
    ) {
      return false;
    }
  }
  return true;
}

function requireSupervisorProcessHeldOfdRefcount(
  processExports: KernelExports,
): (vmPtr: number, ofdId: number) => number {
  const supervisorHeldOfdRefcount = processExports.supervisor_process_held_ofd_refcount;
  if (!supervisorHeldOfdRefcount) {
    throw new Error("kernel supervisor ABI missing supervisor_process_held_ofd_refcount");
  }
  return supervisorHeldOfdRefcount;
}

export function collectProcessHeldOfdRefcounts(processExports: KernelExports, vmPtr: number): Int32Array {
  const maxFds = processExports.abi_max_fds();
  const heldOfdRefcounts = new Int32Array(maxFds);
  const supervisorHeldOfdRefcount = requireSupervisorProcessHeldOfdRefcount(processExports);
  for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
    const refcount = supervisorHeldOfdRefcount(vmPtr, ofdId);
    if (refcount > 0) {
      heldOfdRefcounts[ofdId] = refcount;
    }
  }
  return heldOfdRefcounts;
}

export function createKernelWorkerOfdPrimitives(deps: KernelWorkerPrimitivesDeps) {
  function copyOfdState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void {
    const tableBytes = sourceExports.kernel_ofd_table_bytes?.() ?? 0;
    const stateBytes = kernelOfdStateBytes(sourceExports, sourceState);
    const hasFullState = Boolean(
      sourceExports.kernel_ofd_state_ptr
        || sourceExports.kernel_ofd_state_bytes
        || (sourceState.ofdStatePtr !== undefined && sourceState.ofdStatePtr > 0 && sourceState.ofdStatePtr !== sourceState.ofdTablePtr)
        || stateBytes > tableBytes
        || (sourceState.ofdOffsetFloorBytes ?? 0) > 0,
    );
    copyMemoryRange(
      sourceExports.memory,
      hasFullState ? kernelOfdStatePtr(sourceState) : sourceState.ofdTablePtr,
      targetExports.memory,
      hasFullState ? kernelOfdStatePtr(targetState) : targetState.ofdTablePtr,
      hasFullState ? stateBytes : tableBytes,
    );
  }

  function copySelectedOfdState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    ofdIds: Iterable<number>,
  ): void {
    const maxFds = sourceExports.abi_max_fds();
    const tableBytes = sourceExports.kernel_ofd_table_bytes();
    const slotBytes = tableBytes / maxFds;
    if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
      throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
    }

    for (const ofdId of ofdIds) {
      if (ofdId <= 0 || ofdId >= maxFds) {
        continue;
      }
      if (!ofdSlotsCanBeShared(sourceExports, targetExports, ofdId)) {
        continue;
      }
      copyMemoryRange(
        sourceExports.memory,
        sourceState.ofdTablePtr + ofdId * slotBytes,
        targetExports.memory,
        targetState.ofdTablePtr + ofdId * slotBytes,
        slotBytes,
      );
    }
  }

  function clearSelectedOfdState(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    ofdIds: Iterable<number>,
  ): void {
    const maxFds = targetExports.abi_max_fds();
    const tableBytes = targetExports.kernel_ofd_table_bytes();
    const slotBytes = tableBytes / maxFds;
    if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
      throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
    }

    const bytes = new Uint8Array(targetExports.memory.buffer);
    for (const ofdId of ofdIds) {
      if (ofdId <= 0 || ofdId >= maxFds) {
        continue;
      }
      bytes.fill(0, targetState.ofdTablePtr + ofdId * slotBytes, targetState.ofdTablePtr + (ofdId + 1) * slotBytes);
    }
  }

  function applySelectedOfdSnapshots(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    snapshots: KernelWorkerOfdSlotSnapshot[],
  ): void {
    const maxFds = targetExports.abi_max_fds();
    const tableBytes = targetExports.kernel_ofd_table_bytes();
    const slotBytes = tableBytes / maxFds;
    if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
      throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
    }

    const bytes = new Uint8Array(targetExports.memory.buffer);
    for (const snapshot of snapshots) {
      if (snapshot.ofdId <= 0 || snapshot.ofdId >= maxFds || snapshot.slot.length !== slotBytes) {
        continue;
      }
      if (!ofdSnapshotCanApply(targetExports, targetState, snapshot, slotBytes)) {
        continue;
      }
      bytes.set(snapshot.slot, targetState.ofdTablePtr + snapshot.ofdId * slotBytes);
    }
  }

  function reconcileCanonicalOfdRefcounts(
    memberPids: Iterable<number>,
    memberVmPtrs: ReadonlyMap<number, number>,
    ofdIds: Iterable<number>,
  ): void {
    const canonicalExports = deps.getCanonicalExports();
    const maxFds = canonicalExports.abi_max_fds();
    const trackedOfdIds = [...new Set(ofdIds)].filter((ofdId) => ofdId > 0 && ofdId < maxFds);
    if (trackedOfdIds.length === 0) {
      return;
    }

    const expectedRefcounts = new Int32Array(maxFds);
    const refcountPids = new Set([...memberPids, ...deps.processKernels.keys()]);
    for (const pid of refcountPids) {
      const vmPtr = memberVmPtrs.get(pid) ?? 0;
      if (vmPtr === 0) {
        continue;
      }
      const processExports = deps.processKernels.get(pid);
      if (!processExports) {
        continue;
      }
      const heldRefcounts = collectProcessHeldOfdRefcounts(processExports, vmPtr);
      for (const ofdId of trackedOfdIds) {
        expectedRefcounts[ofdId] += heldRefcounts[ofdId] ?? 0;
      }
    }

    for (const ofdId of trackedOfdIds) {
      let currentRefcount = canonicalExports.kernel_ofd_refcount(ofdId);
      const expectedRefcount = expectedRefcounts[ofdId] ?? 0;
      while (currentRefcount < expectedRefcount) {
        canonicalExports.kernel_retain_ofd(ofdId);
        currentRefcount += 1;
      }
      while (currentRefcount > expectedRefcount) {
        canonicalExports.kernel_release_ofd(ofdId);
        currentRefcount -= 1;
      }
    }
  }

  return {
    applySelectedOfdSnapshots,
    clearSelectedOfdState,
    collectProcessHeldOfdRefcounts,
    copyOfdState,
    copySelectedOfdState,
    reconcileCanonicalOfdRefcounts,
  };
}
