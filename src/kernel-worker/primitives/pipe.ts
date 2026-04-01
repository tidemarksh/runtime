import { attachKernelRuntimeState, readKernelRuntimeState, type KernelExports, type KernelRuntimeState } from "../../abi.js";
import { collectProcessHeldOfdRefcounts } from "./ofd.js";

const FD_TYPE_PIPE = 6;
const PIPE_SLOT_BYTES = 32 + 4096;
const PIPE_SLOT_MODE_READ = 1 << 0;
const PIPE_SLOT_MODE_WRITE = 1 << 1;
const PIPE_OFF_HEAD = 0;
const PIPE_OFF_TAIL = 4;
const PIPE_OFF_READ_OPEN = 8;
const PIPE_OFF_WRITE_OPEN = 12;
const PIPE_OFF_DATA = 32;

interface KernelWorkerPrimitivesDeps {
  getCanonicalExports(): KernelExports;
  processKernels: Map<number, KernelExports>;
  getCanonicalProcessVmPtr(pid: number): number;
}

export function createKernelWorkerPipePrimitives(deps: KernelWorkerPrimitivesDeps) {
  function collectProcessPrivatePipeSlots(processExports: KernelExports, vmPtr: number): Set<number> {
    const maxPipes = processExports.abi_max_pipes();
    const privatePipeSlots = new Set<number>();
    const candidatePipeSlots = [
      processExports.process_stdin_pipe(vmPtr),
      processExports.process_stdout_pipe(vmPtr),
      processExports.process_stderr_pipe(vmPtr),
    ];

    for (const pipeSlot of candidatePipeSlots) {
      if (pipeSlot > 0 && pipeSlot < maxPipes) {
        privatePipeSlots.add(pipeSlot);
      }
    }

    return privatePipeSlots;
  }

  function collectProcessHeldPipeSlots(processExports: KernelExports, vmPtr: number): Set<number> {
    const maxFds = processExports.abi_max_fds();
    const maxPipes = processExports.abi_max_pipes();
    const heldRefcounts = collectProcessHeldOfdRefcounts(processExports, vmPtr);
    const privatePipeSlots = collectProcessPrivatePipeSlots(processExports, vmPtr);
    const pipeSlots = new Set<number>();

    for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
      if (heldRefcounts[ofdId] <= 0 || processExports.kernel_ofd_fd_type(ofdId) !== FD_TYPE_PIPE) {
        continue;
      }
      const pipeIdx = processExports.kernel_ofd_ino(ofdId);
      if (pipeIdx > 0 && pipeIdx < maxPipes && !privatePipeSlots.has(pipeIdx)) {
        pipeSlots.add(pipeIdx);
      }
    }

    return pipeSlots;
  }

  function composeProcessPipeUsed(
    processExports: KernelExports,
    vmPtr: number,
    sharedPipeUsed: bigint,
  ): bigint {
    let pipeUsed = sharedPipeUsed | 1n;
    for (const pipeSlot of collectProcessPrivatePipeSlots(processExports, vmPtr)) {
      pipeUsed |= 1n << BigInt(pipeSlot);
    }
    return pipeUsed;
  }

  function collectProcessPipeSlotModes(processExports: KernelExports, vmPtr: number): Map<number, number> {
    const maxFds = processExports.abi_max_fds();
    const maxPipes = processExports.abi_max_pipes();
    const heldRefcounts = collectProcessHeldOfdRefcounts(processExports, vmPtr);
    const privatePipeSlots = collectProcessPrivatePipeSlots(processExports, vmPtr);
    const pipeSlotModes = new Map<number, number>();

    for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
      if (heldRefcounts[ofdId] <= 0 || processExports.kernel_ofd_fd_type(ofdId) !== FD_TYPE_PIPE) {
        continue;
      }
      const pipeIdx = processExports.kernel_ofd_ino(ofdId);
      if (pipeIdx <= 0 || pipeIdx >= maxPipes || privatePipeSlots.has(pipeIdx)) {
        continue;
      }
      const currentMode = pipeSlotModes.get(pipeIdx) ?? 0;
      const nextMode = processExports.kernel_ofd_host_fd(ofdId) >= 0
        ? currentMode | PIPE_SLOT_MODE_READ
        : currentMode | PIPE_SLOT_MODE_WRITE;
      pipeSlotModes.set(pipeIdx, nextMode);
    }

    return pipeSlotModes;
  }

  function recomputeCanonicalPipeControlState(): {
    kernelState: KernelRuntimeState;
    readOpen: Int32Array;
    writeOpen: Int32Array;
    managedPipeSlots: number[];
  } {
    const canonicalExports = deps.getCanonicalExports();
    const maxFds = canonicalExports.abi_max_fds();
    const maxPipes = canonicalExports.abi_max_pipes();
    const readOpen = new Int32Array(maxPipes);
    const writeOpen = new Int32Array(maxPipes);
    const managedPipeSlots = new Set<number>();
    const priorPipeUsed = readKernelRuntimeState(canonicalExports).pipeUsed;

    for (let pipeIdx = 1; pipeIdx < maxPipes; pipeIdx += 1) {
      if ((priorPipeUsed & (1n << BigInt(pipeIdx))) !== 0n) {
        managedPipeSlots.add(pipeIdx);
      }
    }

    for (const [pid, processExports] of deps.processKernels) {
      const vmPtr = deps.getCanonicalProcessVmPtr(pid);
      if (vmPtr === 0) {
        continue;
      }

      const heldRefcounts = collectProcessHeldOfdRefcounts(processExports, vmPtr);
      for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
        const refcount = heldRefcounts[ofdId] ?? 0;
        if (refcount <= 0 || processExports.kernel_ofd_fd_type(ofdId) !== FD_TYPE_PIPE) {
          continue;
        }
        const pipeIdx = processExports.kernel_ofd_ino(ofdId);
        if (pipeIdx <= 0 || pipeIdx >= maxPipes) {
          continue;
        }
        managedPipeSlots.add(pipeIdx);
        if (processExports.kernel_ofd_host_fd(ofdId) >= 0) {
          readOpen[pipeIdx] += refcount;
        } else {
          writeOpen[pipeIdx] += refcount;
        }
      }
    }

    const bytes = new DataView(canonicalExports.memory.buffer);
    let pipeUsed = 1n;
    for (const pipeIdx of managedPipeSlots) {
      const slotBase = canonicalExports.kernel_pipe_base() + pipeIdx * PIPE_SLOT_BYTES;
      bytes.setInt32(slotBase + PIPE_OFF_READ_OPEN, readOpen[pipeIdx] ?? 0, true);
      bytes.setInt32(slotBase + PIPE_OFF_WRITE_OPEN, writeOpen[pipeIdx] ?? 0, true);
      if ((readOpen[pipeIdx] ?? 0) > 0 || (writeOpen[pipeIdx] ?? 0) > 0) {
        pipeUsed |= 1n << BigInt(pipeIdx);
      }
    }

    const kernelState = {
      ...readKernelRuntimeState(canonicalExports),
      pipeUsed,
    };
    attachKernelRuntimeState(canonicalExports, kernelState);
    return {
      kernelState,
      readOpen,
      writeOpen,
      managedPipeSlots: [...managedPipeSlots].sort((left, right) => left - right),
    };
  }

  function applyPipeControlState(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    readOpen: Int32Array,
    writeOpen: Int32Array,
    pipeUsed: bigint,
    managedPipeSlots: Iterable<number>,
  ): void {
    attachKernelRuntimeState(targetExports, {
      ...targetState,
      pipeUsed,
    });
    const bytes = new DataView(targetExports.memory.buffer);
    for (const pipeIdx of managedPipeSlots) {
      const slotBase = targetState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
      bytes.setInt32(slotBase + PIPE_OFF_READ_OPEN, readOpen[pipeIdx] ?? 0, true);
      bytes.setInt32(slotBase + PIPE_OFF_WRITE_OPEN, writeOpen[pipeIdx] ?? 0, true);
    }
  }

  function collectPipeSlotsForOfdIds(targetExports: KernelExports, ofdIds: Iterable<number>): number[] {
    const maxPipes = targetExports.abi_max_pipes();
    const pipeSlots = new Set<number>();
    for (const ofdId of ofdIds) {
      if (targetExports.kernel_ofd_fd_type(ofdId) !== FD_TYPE_PIPE) {
        continue;
      }
      const pipeSlot = targetExports.kernel_ofd_ino(ofdId);
      if (pipeSlot > 0 && pipeSlot < maxPipes) {
        pipeSlots.add(pipeSlot);
      }
    }
    return [...pipeSlots].sort((left, right) => left - right);
  }

  function copySelectedPipeSlots(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    pipeSlots: Iterable<number>,
  ): void {
    const sourceBytes = new Uint8Array(sourceExports.memory.buffer);
    const targetBytes = new Uint8Array(targetExports.memory.buffer);
    for (const pipeSlot of pipeSlots) {
      if (pipeSlot <= 0 || pipeSlot >= sourceExports.abi_max_pipes()) {
        continue;
      }
      const sourceOffset = sourceState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
      const targetOffset = targetState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
      targetBytes.set(sourceBytes.subarray(sourceOffset, sourceOffset + PIPE_SLOT_BYTES), targetOffset);
    }
  }

  function clearSelectedPipeSlots(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    pipeSlots: Iterable<number>,
  ): void {
    const targetBytes = new Uint8Array(targetExports.memory.buffer);
    for (const pipeSlot of pipeSlots) {
      if (pipeSlot <= 0 || pipeSlot >= targetExports.abi_max_pipes()) {
        continue;
      }
      const targetOffset = targetState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
      targetBytes.fill(0, targetOffset, targetOffset + PIPE_SLOT_BYTES);
    }
  }

  function mergePipeSlotsFromProcess(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    sourceModes: ReadonlyMap<number, number>,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    pipeSlots: Iterable<number>,
  ): void {
    const sourceBytes = new Uint8Array(sourceExports.memory.buffer);
    const targetBytes = new Uint8Array(targetExports.memory.buffer);

    for (const pipeIdx of pipeSlots) {
      if (pipeIdx <= 0 || pipeIdx >= sourceExports.abi_max_pipes()) {
        continue;
      }
      const sourceOffset = sourceState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
      const targetOffset = targetState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
      const mode = sourceModes.get(pipeIdx) ?? 0;
      if ((mode & PIPE_SLOT_MODE_READ) !== 0 && (mode & PIPE_SLOT_MODE_WRITE) !== 0) {
        targetBytes.set(sourceBytes.subarray(sourceOffset, sourceOffset + PIPE_SLOT_BYTES), targetOffset);
        continue;
      }
      if ((mode & PIPE_SLOT_MODE_READ) !== 0) {
        targetBytes.set(
          sourceBytes.subarray(sourceOffset + PIPE_OFF_HEAD, sourceOffset + PIPE_OFF_TAIL),
          targetOffset + PIPE_OFF_HEAD,
        );
      }
      if ((mode & PIPE_SLOT_MODE_WRITE) !== 0) {
        targetBytes.set(
          sourceBytes.subarray(sourceOffset + PIPE_OFF_TAIL, sourceOffset + PIPE_OFF_READ_OPEN),
          targetOffset + PIPE_OFF_TAIL,
        );
        targetBytes.set(
          sourceBytes.subarray(sourceOffset + PIPE_OFF_DATA, sourceOffset + PIPE_SLOT_BYTES),
          targetOffset + PIPE_OFF_DATA,
        );
      }
    }
  }

  function mergePipeSlotsFromCanonical(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    targetVmPtr: number,
    pipeSlots: Iterable<number>,
  ): void {
    const sourceBytes = new Uint8Array(sourceExports.memory.buffer);
    const targetBytes = new Uint8Array(targetExports.memory.buffer);
    const targetModes = targetVmPtr !== 0
      ? collectProcessPipeSlotModes(targetExports, targetVmPtr)
      : new Map<number, number>();

    for (const pipeIdx of pipeSlots) {
      if (pipeIdx <= 0 || pipeIdx >= sourceExports.abi_max_pipes()) {
        continue;
      }
      const sourceOffset = sourceState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
      const targetOffset = targetState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
      const mode = targetModes.get(pipeIdx) ?? 0;
      if ((mode & PIPE_SLOT_MODE_READ) !== 0 && (mode & PIPE_SLOT_MODE_WRITE) !== 0) {
        targetBytes.set(sourceBytes.subarray(sourceOffset, sourceOffset + PIPE_SLOT_BYTES), targetOffset);
        continue;
      }
      if ((mode & PIPE_SLOT_MODE_READ) !== 0) {
        targetBytes.set(
          sourceBytes.subarray(sourceOffset + PIPE_OFF_TAIL, sourceOffset + PIPE_OFF_READ_OPEN),
          targetOffset + PIPE_OFF_TAIL,
        );
        targetBytes.set(
          sourceBytes.subarray(sourceOffset + PIPE_OFF_DATA, sourceOffset + PIPE_SLOT_BYTES),
          targetOffset + PIPE_OFF_DATA,
        );
      }
      if ((mode & PIPE_SLOT_MODE_WRITE) !== 0) {
        targetBytes.set(
          sourceBytes.subarray(sourceOffset + PIPE_OFF_HEAD, sourceOffset + PIPE_OFF_TAIL),
          targetOffset + PIPE_OFF_HEAD,
        );
      }
    }
  }

  function collectCanonicalManagedPipeSlots(kernelState: KernelRuntimeState): number[] {
    const canonicalExports = deps.getCanonicalExports();
    const managedPipeSlots = new Set<number>();
    const maxPipes = canonicalExports.abi_max_pipes();

    for (let pipeIdx = 1; pipeIdx < maxPipes; pipeIdx += 1) {
      if ((kernelState.pipeUsed & (1n << BigInt(pipeIdx))) !== 0n) {
        managedPipeSlots.add(pipeIdx);
      }
    }

    for (let ofdId = 1; ofdId < canonicalExports.abi_max_fds(); ofdId += 1) {
      if (canonicalExports.kernel_ofd_fd_type(ofdId) !== FD_TYPE_PIPE || canonicalExports.kernel_ofd_refcount(ofdId) <= 0) {
        continue;
      }
      const pipeIdx = canonicalExports.kernel_ofd_ino(ofdId);
      if (pipeIdx > 0 && pipeIdx < maxPipes) {
        managedPipeSlots.add(pipeIdx);
      }
    }

    return [...managedPipeSlots].sort((left, right) => left - right);
  }

  return {
    applyPipeControlState,
    clearSelectedPipeSlots,
    collectCanonicalManagedPipeSlots,
    collectPipeSlotsForOfdIds,
    collectProcessHeldPipeSlots,
    collectProcessPipeSlotModes,
    collectProcessPrivatePipeSlots,
    composeProcessPipeUsed,
    copySelectedPipeSlots,
    mergePipeSlotsFromCanonical,
    mergePipeSlotsFromProcess,
    recomputeCanonicalPipeControlState,
  };
}
