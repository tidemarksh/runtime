import {
  kernelOfdStateBytes,
  kernelOfdStatePtr,
  supervisorOfdReconcileRefcount,
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
  getCanonicalProcessVmPtr?(pid: number): number;
  processKernels: Map<number, KernelExports>;
}

function requireSupervisorOfdSlotWriteAction(
  targetExports: KernelExports,
): NonNullable<KernelExports["supervisor_ofd_slot_write_action"]> {
  const decide = targetExports.supervisor_ofd_slot_write_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_ofd_slot_write_action");
  }
  return decide.bind(targetExports);
}

function requireSupervisorRestoreActionSkip(targetExports: KernelExports): number {
  const skip = targetExports.supervisor_fd_entry_restore_action_skip;
  if (!skip) {
    throw new Error("kernel supervisor ABI missing supervisor_fd_entry_restore_action_skip");
  }
  return skip.call(targetExports);
}

function ofdSlotWriteShouldBeSkipped(
  ofdId: number,
  candidate: DataView,
  decide: ReturnType<typeof requireSupervisorOfdSlotWriteAction>,
  skipAction: number,
): boolean {
  return decide(
    ofdId,
    candidate.getInt32(OFD_TYPE_OFFSET, true),
    candidate.getInt32(OFD_HOST_FD_OFFSET, true),
    candidate.getUint32(OFD_INO_OFFSET, true),
    candidate.getBigInt64(OFD_OFFSET_OFFSET, true),
  ) === skipAction;
}

function preserveLiveTargetRefcount(target: DataView, candidate: Uint8Array): Uint8Array {
  const candidateView = new DataView(candidate.buffer, candidate.byteOffset, candidate.byteLength);
  if (
    target.getInt32(OFD_TYPE_OFFSET, true) === 0
    || target.getInt32(OFD_TYPE_OFFSET, true) !== candidateView.getInt32(OFD_TYPE_OFFSET, true)
  ) {
    return candidate;
  }
  const currentRefcount = target.getInt32(OFD_REFCOUNT_OFFSET, true);
  const candidateRefcount = candidateView.getInt32(OFD_REFCOUNT_OFFSET, true);
  if (currentRefcount <= candidateRefcount) {
    return candidate;
  }
  const preserved = candidate.slice();
  new DataView(preserved.buffer, preserved.byteOffset, preserved.byteLength)
    .setInt32(OFD_REFCOUNT_OFFSET, currentRefcount, true);
  return preserved;
}

function requireSupervisorProcessHeldOfdRefcount(
  processExports: KernelExports,
): (vmPtr: number, ofdId: number) => number {
  const readSupervisorHeldOfdRefcount = processExports.supervisor_process_held_ofd_refcount;
  if (!readSupervisorHeldOfdRefcount) {
    throw new Error("kernel supervisor ABI missing supervisor_process_held_ofd_refcount");
  }
  return readSupervisorHeldOfdRefcount.bind(processExports);
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
    const decideWrite = requireSupervisorOfdSlotWriteAction(targetExports);
    const skipAction = requireSupervisorRestoreActionSkip(targetExports);
    const sourceBytes = new Uint8Array(sourceExports.memory.buffer);
    const targetBytes = new Uint8Array(targetExports.memory.buffer);

    for (const ofdId of ofdIds) {
      if (ofdId <= 0 || ofdId >= maxFds) {
        continue;
      }
      const sourceOffset = sourceState.ofdTablePtr + ofdId * slotBytes;
      const targetOffset = targetState.ofdTablePtr + ofdId * slotBytes;
      const candidate = sourceBytes.slice(sourceOffset, sourceOffset + slotBytes);
      const candidateView = new DataView(candidate.buffer, candidate.byteOffset, candidate.byteLength);
      if (ofdSlotWriteShouldBeSkipped(ofdId, candidateView, decideWrite, skipAction)) {
        continue;
      }
      const targetView = new DataView(targetExports.memory.buffer, targetOffset, slotBytes);
      targetBytes.set(
        preserveLiveTargetRefcount(targetView, candidate),
        targetOffset,
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
    const decideWrite = requireSupervisorOfdSlotWriteAction(targetExports);
    const skipAction = requireSupervisorRestoreActionSkip(targetExports);

    const bytes = new Uint8Array(targetExports.memory.buffer);
    for (const snapshot of snapshots) {
      if (snapshot.ofdId <= 0 || snapshot.ofdId >= maxFds || snapshot.slot.length !== slotBytes) {
        continue;
      }
      const candidate = new DataView(
        snapshot.slot.buffer,
        snapshot.slot.byteOffset,
        snapshot.slot.byteLength,
      );
      if (ofdSlotWriteShouldBeSkipped(snapshot.ofdId, candidate, decideWrite, skipAction)) {
        continue;
      }
      const targetOffset = targetState.ofdTablePtr + snapshot.ofdId * slotBytes;
      const targetView = new DataView(targetExports.memory.buffer, targetOffset, slotBytes);
      bytes.set(preserveLiveTargetRefcount(targetView, snapshot.slot), targetOffset);
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
      const vmPtr = memberVmPtrs.get(pid) ?? deps.getCanonicalProcessVmPtr?.(pid) ?? 0;
      if (vmPtr === 0) {
        continue;
      }
      const processExports = deps.processKernels.get(pid);
      if (!processExports) {
        continue;
      }
      const authorityVmPtr = processExports.supervisor_process_fd_authority_vm_ptr?.(vmPtr, pid) ?? vmPtr;
      if (authorityVmPtr === 0) {
        continue;
      }
      const heldRefcounts = collectProcessHeldOfdRefcounts(processExports, authorityVmPtr);
      for (const ofdId of trackedOfdIds) {
        expectedRefcounts[ofdId] += heldRefcounts[ofdId] ?? 0;
      }
    }

    for (const ofdId of trackedOfdIds) {
      const expectedRefcount = expectedRefcounts[ofdId] ?? 0;
      supervisorOfdReconcileRefcount(canonicalExports, ofdId, expectedRefcount);
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
