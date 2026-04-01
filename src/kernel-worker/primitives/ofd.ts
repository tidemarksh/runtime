import type { KernelExports, KernelRuntimeState } from "../../abi.js";
import type { KernelWorkerOfdSlotSnapshot } from "../../messages.js";
import { copyMemoryRange } from "../common.js";

interface KernelWorkerPrimitivesDeps {
  getCanonicalExports(): KernelExports;
  processKernels: Map<number, KernelExports>;
}

export function collectProcessHeldOfdRefcounts(processExports: KernelExports, vmPtr: number): Int32Array {
  const maxFds = processExports.abi_max_fds();
  const heldOfdRefcounts = new Int32Array(maxFds);
  const processHeldOfds = new Set<number>();

  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = processExports.process_fd_ofd_id(vmPtr, fd);
    if (ofdId !== 0 && ofdId < maxFds) {
      processHeldOfds.add(ofdId);
    }
  }

  for (const ofdId of processHeldOfds) {
    heldOfdRefcounts[ofdId] += 1;
  }

  for (let slot = 0; slot < processExports.abi_max_threads(); slot += 1) {
    const threadVmPtr = processExports.process_thread_vm_ptr(vmPtr, slot);
    if (threadVmPtr === 0) {
      continue;
    }
    const blockedOfdId = processExports.thread_blocked_ofd_id(threadVmPtr);
    if (blockedOfdId !== 0 && blockedOfdId < maxFds) {
      heldOfdRefcounts[blockedOfdId] += 1;
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
    copyMemoryRange(
      sourceExports.memory,
      sourceState.ofdTablePtr,
      targetExports.memory,
      targetState.ofdTablePtr,
      sourceExports.kernel_ofd_table_bytes(),
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
    for (const pid of memberPids) {
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
