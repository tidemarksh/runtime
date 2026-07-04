import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  supervisorOfdFdType,
  supervisorOfdHostFd,
  supervisorOfdIno,
  supervisorOfdRefcount,
  supervisorOfdStatusFlags,
  type KernelExports,
  type KernelRuntimeState,
} from "../../abi.js";
import {
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../layout/ofd.js";
import {
  PIPE_BUFFER_BYTES,
  PIPE_DATA_OFFSET,
  PIPE_GENERATION_OFFSET,
  PIPE_HEAD_OFFSET,
  PIPE_READ_OPEN_OFFSET,
  PIPE_SLOT_BYTES,
  PIPE_SLOT_MODE_CONTROL,
  PIPE_SLOT_MODE_READ,
  PIPE_SLOT_MODE_WRITE,
  PIPE_TAIL_OFFSET,
  PIPE_WRITE_OPEN_OFFSET,
} from "../../layout/pipe.js";
import type { KernelWorkerPipeSlotSnapshot } from "../../messages.js";

const PIPE_OFF_HEAD = PIPE_HEAD_OFFSET;
const PIPE_OFF_TAIL = PIPE_TAIL_OFFSET;
const PIPE_OFF_READ_OPEN = PIPE_READ_OPEN_OFFSET;
const PIPE_OFF_WRITE_OPEN = PIPE_WRITE_OPEN_OFFSET;
const PIPE_OFF_GENERATION = PIPE_GENERATION_OFFSET;
const PIPE_OFF_DATA = PIPE_DATA_OFFSET;
const PIPE_BUF_SIZE = PIPE_BUFFER_BYTES;

function copyPipeDataTailDelta(
  sourceBytes: Uint8Array,
  sourceDataOffset: number,
  targetBytes: Uint8Array,
  targetDataOffset: number,
  previousTail: number,
  nextTail: number,
): void {
  const copySpan = (start: number, end: number): void => {
    if (end <= start) {
      return;
    }
    targetBytes.set(
      sourceBytes.subarray(sourceDataOffset + start, sourceDataOffset + end),
      targetDataOffset + start,
    );
  };
  if (previousTail < nextTail) {
    copySpan(previousTail, nextTail);
  } else if (previousTail > nextTail) {
    copySpan(previousTail, PIPE_BUF_SIZE);
    copySpan(0, nextTail);
  }
}

interface KernelWorkerPrimitivesDeps {
  getCanonicalExports(): KernelExports;
  processKernels: Map<number, KernelExports>;
  canonicalProcesses: Map<number, { ppid: number }>;
  getCanonicalProcessVmPtr(pid: number): number;
}

function supervisorMaskToBigInt(mask: bigint | number): bigint {
  return typeof mask === "bigint" ? mask : BigInt(mask);
}

function pipeSlotsFromMask(mask: bigint, maxPipes: number): Set<number> {
  const pipeSlots = new Set<number>();
  for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
    if ((mask & (1n << BigInt(pipeSlot))) !== 0n) {
      pipeSlots.add(pipeSlot);
    }
  }
  return pipeSlots;
}

function pipeModesFromSupervisor(
  maxPipes: number,
  modeForSlot: (pipeSlot: number) => number,
): Map<number, number> {
  const modes = new Map<number, number>();
  for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
    const mode = modeForSlot(pipeSlot);
    if (mode !== 0) {
      modes.set(pipeSlot, mode);
    }
  }
  return modes;
}

function requireSupervisorPipeMask(
  processExports: KernelExports,
  exportName:
    | "supervisor_process_private_pipe_slot_mask"
    | "supervisor_process_held_pipe_slot_mask"
    | "supervisor_process_pipe_used_mask",
): (vmPtr: number) => bigint | number {
  const readMask = processExports[exportName];
  if (!readMask) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  return readMask;
}

function requireSupervisorPipeMode(
  processExports: KernelExports,
  exportName:
    | "supervisor_process_private_pipe_source_mode"
    | "supervisor_process_private_pipe_target_mode"
    | "supervisor_process_pipe_slot_mode",
): (vmPtr: number, pipeSlot: number) => number {
  const readMode = processExports[exportName];
  if (!readMode) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  return readMode;
}

function requireSupervisorPipeCount(
  processExports: KernelExports,
  exportName:
    | "supervisor_process_pipe_read_open_count"
    | "supervisor_process_pipe_write_open_count",
): (vmPtr: number, pipeSlot: number) => number {
  const readCount = processExports[exportName];
  if (!readCount) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  return readCount;
}

export function createKernelWorkerPipePrimitives(deps: KernelWorkerPrimitivesDeps) {
  function syncPipeOfdGenerations(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    pipeSlot: number,
  ): void {
    if (pipeSlot <= 0 || pipeSlot >= targetExports.abi_max_pipes()) {
      return;
    }
    const maxFds = targetExports.abi_max_fds();
    const ofdTableBytes = targetExports.kernel_ofd_table_bytes();
    const ofdSlotBytes = ofdTableBytes / maxFds;
    if (!Number.isInteger(ofdSlotBytes) || ofdSlotBytes <= OFD_INO_OFFSET + 4) {
      return;
    }
    const view = new DataView(targetExports.memory.buffer);
    const pipeGeneration = view.getUint32(
      targetState.pipeBase + pipeSlot * PIPE_SLOT_BYTES + PIPE_OFF_GENERATION,
      true,
    );
    if (pipeGeneration === 0) {
      return;
    }
    for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
      const ofdOffset = targetState.ofdTablePtr + ofdId * ofdSlotBytes;
      if (
        view.getInt32(ofdOffset + OFD_TYPE_OFFSET, true) === targetExports.abi_fd_type_pipe()
        && view.getUint32(ofdOffset + OFD_INO_OFFSET, true) === pipeSlot
      ) {
        view.setBigInt64(ofdOffset + OFD_OFFSET_OFFSET, BigInt(pipeGeneration), true);
      }
    }
  }

  function collectOfdPipeModes(
    processExports: KernelExports,
    ofdId: number,
  ): Array<{ pipeSlot: number; mode: number }> {
    const fdType = supervisorOfdFdType(processExports, ofdId);
    const statusFlags = supervisorOfdStatusFlags(processExports, ofdId);
    const oRdonly = processExports.abi_o_rdonly();
    const oWronly = processExports.abi_o_wronly();
    const accessModeMask = oRdonly | oWronly | processExports.abi_o_rdwr();
    const accessMode = statusFlags & accessModeMask;
    if (fdType === processExports.abi_fd_type_pipe()) {
      const pipeSlot = supervisorOfdIno(processExports, ofdId);
      if (pipeSlot <= 0 || pipeSlot >= processExports.abi_max_pipes()) {
        return [];
      }
      return [{
        pipeSlot,
        mode: supervisorOfdHostFd(processExports, ofdId) >= 0
          ? PIPE_SLOT_MODE_READ
          : PIPE_SLOT_MODE_WRITE,
      }];
    }
    const ptySlaveType = processExports.abi_fd_type_pty_slave?.();
    const ptyMasterType = processExports.abi_fd_type_pty_master?.();
    if (fdType !== ptySlaveType && fdType !== ptyMasterType) {
      return [];
    }
    const ptyIdx = supervisorOfdIno(processExports, ofdId);
    const inputPipe = processExports.kernel_pty_input_pipe?.(ptyIdx) ?? 0;
    const outputPipe = processExports.kernel_pty_output_pipe?.(ptyIdx) ?? 0;
    const modes: Array<{ pipeSlot: number; mode: number }> = [];
    if (fdType === ptySlaveType) {
      if (accessMode !== oWronly && inputPipe > 0) {
        modes.push({ pipeSlot: inputPipe, mode: PIPE_SLOT_MODE_READ });
      }
      if (accessMode !== oRdonly && outputPipe > 0) {
        modes.push({ pipeSlot: outputPipe, mode: PIPE_SLOT_MODE_WRITE });
      }
      return modes;
    }
    if (accessMode !== oWronly && outputPipe > 0) {
      modes.push({ pipeSlot: outputPipe, mode: PIPE_SLOT_MODE_READ });
    }
    if (accessMode !== oRdonly && inputPipe > 0) {
      modes.push({ pipeSlot: inputPipe, mode: PIPE_SLOT_MODE_WRITE });
    }
    return modes;
  }

  function collectProcessPrivatePipeSlots(
    processExports: KernelExports,
    vmPtr: number,
  ): Set<number> {
    const maxPipes = processExports.abi_max_pipes();
    const supervisorMask = requireSupervisorPipeMask(
      processExports,
      "supervisor_process_private_pipe_slot_mask",
    )(vmPtr);
    return pipeSlotsFromMask(supervisorMaskToBigInt(supervisorMask), maxPipes);
  }

  function collectProcessPrivatePipeSourceModes(
    processExports: KernelExports,
    vmPtr: number,
  ): Map<number, number> {
    const maxPipes = processExports.abi_max_pipes();
    const supervisorSourceMode = requireSupervisorPipeMode(
      processExports,
      "supervisor_process_private_pipe_source_mode",
    );
    return pipeModesFromSupervisor(maxPipes, (pipeSlot) => supervisorSourceMode(vmPtr, pipeSlot));
  }

  function collectProcessPrivatePipeTargetModes(
    processExports: KernelExports,
    vmPtr: number,
  ): Map<number, number> {
    const maxPipes = processExports.abi_max_pipes();
    const supervisorTargetMode = requireSupervisorPipeMode(
      processExports,
      "supervisor_process_private_pipe_target_mode",
    );
    return pipeModesFromSupervisor(maxPipes, (pipeSlot) => supervisorTargetMode(vmPtr, pipeSlot));
  }

  function collectProcessHeldPipeSlots(processExports: KernelExports, vmPtr: number): Set<number> {
    const maxPipes = processExports.abi_max_pipes();
    const supervisorMask = requireSupervisorPipeMask(
      processExports,
      "supervisor_process_held_pipe_slot_mask",
    )(vmPtr);
    return pipeSlotsFromMask(supervisorMaskToBigInt(supervisorMask), maxPipes);
  }

  function composeProcessPipeUsed(
    processExports: KernelExports,
    vmPtr: number,
    sharedPipeUsed: bigint,
  ): bigint {
    const supervisorMask = requireSupervisorPipeMask(
      processExports,
      "supervisor_process_pipe_used_mask",
    )(vmPtr);
    return sharedPipeUsed | supervisorMaskToBigInt(supervisorMask);
  }

  function collectProcessPipeSlotModes(processExports: KernelExports, vmPtr: number): Map<number, number> {
    const maxPipes = processExports.abi_max_pipes();
    const supervisorSlotMode = requireSupervisorPipeMode(
      processExports,
      "supervisor_process_pipe_slot_mode",
    );
    return pipeModesFromSupervisor(maxPipes, (pipeSlot) => supervisorSlotMode(vmPtr, pipeSlot));
  }

  function recomputeCanonicalPipeControlState(): {
    kernelState: KernelRuntimeState;
    readOpen: Int32Array;
    writeOpen: Int32Array;
    managedPipeSlots: number[];
  } {
    const canonicalExports = deps.getCanonicalExports();
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

    for (const [processPid, processExports] of deps.processKernels) {
      const vmPtr = deps.getCanonicalProcessVmPtr(processPid);
      if (vmPtr === 0) {
        continue;
      }

      const supervisorReadOpen = requireSupervisorPipeCount(
        processExports,
        "supervisor_process_pipe_read_open_count",
      );
      const supervisorWriteOpen = requireSupervisorPipeCount(
        processExports,
        "supervisor_process_pipe_write_open_count",
      );

      const supervisorUsedMask = requireSupervisorPipeMask(
        processExports,
        "supervisor_process_pipe_used_mask",
      )(vmPtr);
      for (const pipeSlot of pipeSlotsFromMask(supervisorMaskToBigInt(supervisorUsedMask), maxPipes)) {
        managedPipeSlots.add(pipeSlot);
      }
      for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
        const processReadOpen = supervisorReadOpen(vmPtr, pipeSlot);
        const processWriteOpen = supervisorWriteOpen(vmPtr, pipeSlot);
        if (processReadOpen > 0 || processWriteOpen > 0) {
          managedPipeSlots.add(pipeSlot);
        }
        if (processReadOpen > 0) {
          readOpen[pipeSlot] += processReadOpen;
        }
        if (processWriteOpen > 0) {
          writeOpen[pipeSlot] += processWriteOpen;
        }
      }
    }

    const bytes = new DataView(canonicalExports.memory.buffer);
    const rawBytes = new Uint8Array(canonicalExports.memory.buffer);
    let pipeUsed = 1n;
    for (const pipeIdx of managedPipeSlots) {
      const slotBase = canonicalExports.kernel_pipe_base() + pipeIdx * PIPE_SLOT_BYTES;
      if ((readOpen[pipeIdx] ?? 0) <= 0 && (writeOpen[pipeIdx] ?? 0) <= 0) {
        rawBytes.fill(0, slotBase, slotBase + PIPE_SLOT_BYTES);
        continue;
      }
      bytes.setInt32(slotBase + PIPE_OFF_READ_OPEN, readOpen[pipeIdx] ?? 0, true);
      bytes.setInt32(slotBase + PIPE_OFF_WRITE_OPEN, writeOpen[pipeIdx] ?? 0, true);
      pipeUsed |= 1n << BigInt(pipeIdx);
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
      for (const { pipeSlot } of collectOfdPipeModes(targetExports, ofdId)) {
        if (pipeSlot > 0 && pipeSlot < maxPipes) {
          pipeSlots.add(pipeSlot);
        }
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
      syncPipeOfdGenerations(targetExports, targetState, pipeSlot);
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

  function applySelectedPipeSlotSnapshots(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    snapshots: KernelWorkerPipeSlotSnapshot[],
  ): void {
    const targetBytes = new Uint8Array(targetExports.memory.buffer);
    const targetView = new DataView(targetExports.memory.buffer);
    function counterAhead(sourceValue: number, targetValue: number): boolean {
      return sourceValue > targetValue
        || (sourceValue < targetValue && targetValue > PIPE_BUF_SIZE / 2 && sourceValue < PIPE_BUF_SIZE / 2);
    }
    function generationAhead(sourceValue: number, targetValue: number): boolean {
      return targetValue === 0
        || sourceValue > targetValue
        || (sourceValue < targetValue && targetValue > 0x80000000 && sourceValue < 0x80000000);
    }
    for (const snapshot of snapshots) {
      if (
        snapshot.pipeSlot <= 0
        || snapshot.pipeSlot >= targetExports.abi_max_pipes()
        || snapshot.slot.length !== PIPE_SLOT_BYTES
      ) {
        continue;
      }
      const targetOffset = targetState.pipeBase + snapshot.pipeSlot * PIPE_SLOT_BYTES;
      const sourceView = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
      const sourceHead = sourceView.getUint32(PIPE_OFF_HEAD, true);
      const sourceTail = sourceView.getUint32(PIPE_OFF_TAIL, true);
      const sourceReadOpen = sourceView.getInt32(PIPE_OFF_READ_OPEN, true);
      const sourceWriteOpen = sourceView.getInt32(PIPE_OFF_WRITE_OPEN, true);
      let targetHead = targetView.getUint32(targetOffset + PIPE_OFF_HEAD, true);
      const targetTail = targetView.getUint32(targetOffset + PIPE_OFF_TAIL, true);
      const targetReadOpen = targetView.getInt32(targetOffset + PIPE_OFF_READ_OPEN, true);
      const targetWriteOpen = targetView.getInt32(targetOffset + PIPE_OFF_WRITE_OPEN, true);
      const mode = snapshot.mode ?? (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE | PIPE_SLOT_MODE_CONTROL);
      const sourceGeneration = sourceView.getUint32(PIPE_OFF_GENERATION, true);
      const targetGeneration = targetView.getUint32(targetOffset + PIPE_OFF_GENERATION, true);
      if (
        sourceGeneration !== 0
        && sourceGeneration !== targetGeneration
      ) {
        if (
          sourceHead === 0
          && sourceTail === 0
          && targetHead !== targetTail
          && snapshot.fresh !== true
          && !generationAhead(sourceGeneration, targetGeneration)
        ) {
          continue;
        }
        if (
          snapshot.fresh === true
          || generationAhead(sourceGeneration, targetGeneration)
          || (targetReadOpen <= 0 && targetWriteOpen <= 0)
        ) {
          targetBytes.set(snapshot.slot, targetOffset);
        }
        syncPipeOfdGenerations(targetExports, targetState, snapshot.pipeSlot);
        continue;
      }
    if (
      snapshot.fresh === true
      && sourceHead === 0
      && sourceTail === 0
        && targetHead === targetTail
        && sourceReadOpen > 0
        && sourceWriteOpen > 0
        && (mode & (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE)) === (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE)
      ) {
        targetBytes.set(snapshot.slot, targetOffset);
        syncPipeOfdGenerations(targetExports, targetState, snapshot.pipeSlot);
        continue;
      }
      if (
        sourceHead === 0
        && sourceTail === 0
        && (targetHead !== 0 || targetTail !== 0)
        && targetReadOpen <= 0
        && targetWriteOpen <= 0
      ) {
        targetBytes.set(snapshot.slot, targetOffset);
        syncPipeOfdGenerations(targetExports, targetState, snapshot.pipeSlot);
        continue;
      }
      if (
        targetReadOpen <= 0
        && targetWriteOpen <= 0
        && targetHead === targetTail
        && (sourceReadOpen > 0 || sourceWriteOpen > 0)
        && ((mode & (PIPE_SLOT_MODE_WRITE | PIPE_SLOT_MODE_CONTROL)) !== 0)
      ) {
        targetBytes.set(snapshot.slot, targetOffset);
        syncPipeOfdGenerations(targetExports, targetState, snapshot.pipeSlot);
        continue;
      }
      if ((mode & PIPE_SLOT_MODE_CONTROL) !== 0) {
        targetView.setInt32(targetOffset + PIPE_OFF_READ_OPEN, sourceReadOpen, true);
        targetView.setInt32(targetOffset + PIPE_OFF_WRITE_OPEN, sourceWriteOpen, true);
      }
      if (
        (mode & PIPE_SLOT_MODE_READ) !== 0
        && (mode & PIPE_SLOT_MODE_WRITE) === 0
        && sourceHead !== 0
        && sourceHead === sourceTail
        && targetTail !== sourceTail
        && counterAhead(sourceTail, targetTail)
      ) {
        targetView.setUint32(targetOffset + PIPE_OFF_HEAD, sourceHead, true);
        targetView.setUint32(targetOffset + PIPE_OFF_TAIL, sourceTail, true);
        targetHead = sourceHead;
      }
      if (
        (mode & PIPE_SLOT_MODE_WRITE) !== 0
        && (mode & PIPE_SLOT_MODE_READ) === 0
        && sourceHead === 0
        && sourceTail !== 0
        && targetHead !== 0
        && (targetHead === targetTail || targetTail === 0)
        && counterAhead(sourceTail, targetTail)
      ) {
        targetView.setUint32(targetOffset + PIPE_OFF_HEAD, 0, true);
        targetHead = 0;
      }
      if (
        (mode & PIPE_SLOT_MODE_READ) !== 0
        && counterAhead(sourceHead, targetHead)
        && (sourceHead !== sourceTail || targetTail === sourceTail)
      ) {
        targetView.setUint32(targetOffset + PIPE_OFF_HEAD, sourceHead, true);
      }
      if ((mode & PIPE_SLOT_MODE_WRITE) !== 0 && counterAhead(sourceTail, targetTail)) {
        targetBytes.set(
          snapshot.slot.subarray(PIPE_OFF_TAIL, PIPE_OFF_READ_OPEN),
          targetOffset + PIPE_OFF_TAIL,
        );
        copyPipeDataTailDelta(
          snapshot.slot,
          PIPE_OFF_DATA,
          targetBytes,
          targetOffset + PIPE_OFF_DATA,
          targetTail,
          sourceTail,
        );
      } else if (
        (mode & PIPE_SLOT_MODE_WRITE) !== 0
        && sourceTail === targetTail
        && sourceHead !== sourceTail
        && !((mode & PIPE_SLOT_MODE_READ) !== 0 && counterAhead(targetHead, sourceHead))
      ) {
        targetBytes.set(
          snapshot.slot.subarray(PIPE_OFF_DATA, PIPE_SLOT_BYTES),
          targetOffset + PIPE_OFF_DATA,
        );
      }
      syncPipeOfdGenerations(targetExports, targetState, snapshot.pipeSlot);
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
    const sourceView = new DataView(sourceExports.memory.buffer);
    const targetView = new DataView(targetExports.memory.buffer);

    function counterAhead(sourceValue: number, targetValue: number): boolean {
      return sourceValue > targetValue
        || (sourceValue < targetValue && targetValue > PIPE_BUF_SIZE / 2 && sourceValue < PIPE_BUF_SIZE / 2);
    }
    function generationAhead(sourceValue: number, targetValue: number): boolean {
      return targetValue === 0
        || sourceValue > targetValue
        || (sourceValue < targetValue && targetValue > 0x80000000 && sourceValue < 0x80000000);
    }

    for (const pipeIdx of pipeSlots) {
      if (pipeIdx <= 0 || pipeIdx >= sourceExports.abi_max_pipes()) {
        continue;
      }
      const sourceOffset = sourceState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
      const targetOffset = targetState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
      const mode = sourceModes.get(pipeIdx) ?? 0;
      if (mode === 0) {
        continue;
      }
      const sourceHead = sourceView.getUint32(sourceOffset + PIPE_OFF_HEAD, true);
      const sourceTail = sourceView.getUint32(sourceOffset + PIPE_OFF_TAIL, true);
      const sourceWasZeroEmpty = sourceHead === 0 && sourceTail === 0;
      const sourceReadOpen = sourceView.getInt32(sourceOffset + PIPE_OFF_READ_OPEN, true);
      const sourceWriteOpen = sourceView.getInt32(sourceOffset + PIPE_OFF_WRITE_OPEN, true);
      let targetHead = targetView.getUint32(targetOffset + PIPE_OFF_HEAD, true);
      const targetTail = targetView.getUint32(targetOffset + PIPE_OFF_TAIL, true);
      const targetReadOpen = targetView.getInt32(targetOffset + PIPE_OFF_READ_OPEN, true);
      const targetWriteOpen = targetView.getInt32(targetOffset + PIPE_OFF_WRITE_OPEN, true);
      const sourceGeneration = sourceView.getUint32(sourceOffset + PIPE_OFF_GENERATION, true);
      const targetGeneration = targetView.getUint32(targetOffset + PIPE_OFF_GENERATION, true);
      if (
        sourceGeneration !== 0
        && sourceGeneration !== targetGeneration
      ) {
        if (
          sourceWasZeroEmpty
          && targetHead !== targetTail
          && !generationAhead(sourceGeneration, targetGeneration)
        ) {
          continue;
        }
        if (generationAhead(sourceGeneration, targetGeneration) || (targetReadOpen <= 0 && targetWriteOpen <= 0)) {
          targetBytes.set(
            sourceBytes.subarray(sourceOffset, sourceOffset + PIPE_SLOT_BYTES),
            targetOffset,
          );
        }
        syncPipeOfdGenerations(targetExports, targetState, pipeIdx);
        continue;
      }
      if (
        sourceHead === 0
        && sourceTail === 0
        && sourceReadOpen > 0
        && sourceWriteOpen > 0
        && targetReadOpen <= 0
        && targetWriteOpen <= 0
        && targetHead === targetTail
        && (mode & (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE)) === (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE)
      ) {
        targetBytes.set(
          sourceBytes.subarray(sourceOffset, sourceOffset + PIPE_SLOT_BYTES),
          targetOffset,
        );
        syncPipeOfdGenerations(targetExports, targetState, pipeIdx);
        continue;
      }
      if (
        sourceHead === 0
        && sourceTail === 0
        && (targetHead !== 0 || targetTail !== 0)
      ) {
        const targetReadOpen = targetView.getInt32(targetOffset + PIPE_OFF_READ_OPEN, true);
        const targetWriteOpen = targetView.getInt32(targetOffset + PIPE_OFF_WRITE_OPEN, true);
        if (targetReadOpen <= 0 && targetWriteOpen <= 0) {
          targetBytes.set(
            sourceBytes.subarray(sourceOffset, sourceOffset + PIPE_SLOT_BYTES),
            targetOffset,
          );
        }
        syncPipeOfdGenerations(targetExports, targetState, pipeIdx);
        continue;
      }
      if (
        (mode & PIPE_SLOT_MODE_WRITE) !== 0
        && (mode & PIPE_SLOT_MODE_READ) === 0
        && sourceHead === 0
        && sourceTail !== 0
        && targetHead !== 0
        && (targetHead === targetTail || targetTail === 0)
        && counterAhead(sourceTail, targetTail)
      ) {
        targetView.setUint32(targetOffset + PIPE_OFF_HEAD, 0, true);
        targetHead = 0;
      }
      if (
        (mode & PIPE_SLOT_MODE_READ) !== 0
        && sourceHead !== 0
        && sourceHead === sourceTail
        && targetTail !== sourceTail
        && counterAhead(sourceTail, targetTail)
      ) {
        targetView.setUint32(targetOffset + PIPE_OFF_HEAD, sourceHead, true);
        targetView.setUint32(targetOffset + PIPE_OFF_TAIL, sourceTail, true);
        targetHead = sourceHead;
      }
      if ((mode & PIPE_SLOT_MODE_READ) !== 0) {
        if (
          counterAhead(sourceHead, targetHead)
          && (sourceHead !== sourceTail || targetTail === sourceTail)
        ) {
          targetView.setUint32(targetOffset + PIPE_OFF_HEAD, sourceHead, true);
        }
      }
      if ((mode & PIPE_SLOT_MODE_WRITE) !== 0) {
        if (counterAhead(sourceTail, targetTail)) {
          targetBytes.set(
            sourceBytes.subarray(sourceOffset + PIPE_OFF_TAIL, sourceOffset + PIPE_OFF_READ_OPEN),
            targetOffset + PIPE_OFF_TAIL,
          );
          targetBytes.set(
            sourceBytes.subarray(sourceOffset + PIPE_OFF_DATA, sourceOffset + PIPE_SLOT_BYTES),
            targetOffset + PIPE_OFF_DATA,
          );
        } else if (sourceTail === targetTail && sourceHead !== sourceTail) {
          targetBytes.set(
            sourceBytes.subarray(sourceOffset + PIPE_OFF_DATA, sourceOffset + PIPE_SLOT_BYTES),
            targetOffset + PIPE_OFF_DATA,
          );
        }
      }
      syncPipeOfdGenerations(targetExports, targetState, pipeIdx);
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
    const sourceView = new DataView(sourceExports.memory.buffer);
    const targetView = new DataView(targetExports.memory.buffer);
    const targetModes = targetVmPtr !== 0
      ? collectProcessPipeSlotModes(targetExports, targetVmPtr)
      : new Map<number, number>();
    if (targetVmPtr !== 0) {
      for (const [pipeSlot, mode] of collectProcessPrivatePipeTargetModes(targetExports, targetVmPtr)) {
        targetModes.set(pipeSlot, mode);
      }
    }

    function counterAhead(sourceValue: number, targetValue: number): boolean {
      return sourceValue > targetValue
        || (sourceValue < targetValue && targetValue > PIPE_BUF_SIZE / 2 && sourceValue < PIPE_BUF_SIZE / 2);
    }
    function generationAhead(sourceValue: number, targetValue: number): boolean {
      return targetValue === 0
        || sourceValue > targetValue
        || (sourceValue < targetValue && targetValue > 0x80000000 && sourceValue < 0x80000000);
    }

    for (const pipeIdx of pipeSlots) {
      if (pipeIdx <= 0 || pipeIdx >= sourceExports.abi_max_pipes()) {
        continue;
      }
      const sourceOffset = sourceState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
      const targetOffset = targetState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
      const mode = targetModes.get(pipeIdx) ?? 0;
      const sourceHead = sourceView.getUint32(sourceOffset + PIPE_OFF_HEAD, true);
      const sourceTail = sourceView.getUint32(sourceOffset + PIPE_OFF_TAIL, true);
      const sourceWasZeroEmpty = sourceHead === 0 && sourceTail === 0;
      const sourceReadOpen = sourceView.getInt32(sourceOffset + PIPE_OFF_READ_OPEN, true);
      const sourceWriteOpen = sourceView.getInt32(sourceOffset + PIPE_OFF_WRITE_OPEN, true);
      const sourceGeneration = sourceView.getUint32(sourceOffset + PIPE_OFF_GENERATION, true);
      const targetGeneration = targetView.getUint32(targetOffset + PIPE_OFF_GENERATION, true);
      const targetReadOpen = targetView.getInt32(targetOffset + PIPE_OFF_READ_OPEN, true);
      const targetWriteOpen = targetView.getInt32(targetOffset + PIPE_OFF_WRITE_OPEN, true);
      if (
        sourceGeneration !== 0
        && sourceGeneration !== targetGeneration
      ) {
        if (
          sourceWasZeroEmpty
          && targetReadOpen > 0
          && targetWriteOpen > 0
          && !generationAhead(sourceGeneration, targetGeneration)
        ) {
          const targetHead = targetView.getUint32(targetOffset + PIPE_OFF_HEAD, true);
          const targetTail = targetView.getUint32(targetOffset + PIPE_OFF_TAIL, true);
          if (targetHead !== targetTail) {
            continue;
          }
        }
        if (generationAhead(sourceGeneration, targetGeneration) || (targetReadOpen <= 0 && targetWriteOpen <= 0)) {
          targetBytes.set(
            sourceBytes.subarray(sourceOffset, sourceOffset + PIPE_SLOT_BYTES),
            targetOffset,
          );
        }
        syncPipeOfdGenerations(targetExports, targetState, pipeIdx);
        continue;
      }
      const initialTargetHead = targetView.getUint32(targetOffset + PIPE_OFF_HEAD, true);
      const initialTargetTail = targetView.getUint32(targetOffset + PIPE_OFF_TAIL, true);
      if (
        sourceHead === 0
        && sourceTail === 0
        && sourceReadOpen > 0
        && sourceWriteOpen > 0
        && targetReadOpen <= 0
        && targetWriteOpen <= 0
        && initialTargetHead === initialTargetTail
        && (mode & (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE)) === (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE)
      ) {
        targetBytes.set(
          sourceBytes.subarray(sourceOffset, sourceOffset + PIPE_SLOT_BYTES),
          targetOffset,
        );
        syncPipeOfdGenerations(targetExports, targetState, pipeIdx);
        continue;
      }
      if ((mode & PIPE_SLOT_MODE_READ) !== 0) {
        let targetHead = targetView.getUint32(targetOffset + PIPE_OFF_HEAD, true);
        const targetTail = targetView.getUint32(targetOffset + PIPE_OFF_TAIL, true);
        if (
          (mode & PIPE_SLOT_MODE_WRITE) === 0
          && sourceHead !== 0
          && sourceHead === sourceTail
          && targetTail !== sourceTail
          && counterAhead(sourceTail, targetTail)
        ) {
          targetView.setUint32(targetOffset + PIPE_OFF_HEAD, sourceHead, true);
          targetView.setUint32(targetOffset + PIPE_OFF_TAIL, sourceTail, true);
          targetHead = sourceHead;
        }
        if (
          counterAhead(sourceHead, targetHead)
          && (sourceHead !== sourceTail || targetTail === sourceTail)
        ) {
          targetView.setUint32(targetOffset + PIPE_OFF_HEAD, sourceHead, true);
          targetHead = sourceHead;
        }
        if (counterAhead(sourceTail, targetTail)) {
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
      if ((mode & PIPE_SLOT_MODE_WRITE) !== 0) {
        let targetHead = targetView.getUint32(targetOffset + PIPE_OFF_HEAD, true);
        const targetTail = targetView.getUint32(targetOffset + PIPE_OFF_TAIL, true);
        let repairedHead = false;
        if (
          counterAhead(sourceHead, targetHead)
          && (sourceHead !== sourceTail || targetTail === sourceTail)
        ) {
          targetView.setUint32(targetOffset + PIPE_OFF_HEAD, sourceHead, true);
          targetHead = sourceHead;
          repairedHead = true;
        }
        if (counterAhead(sourceTail, targetTail) || (repairedHead && sourceHead !== sourceTail)) {
          targetBytes.set(
            sourceBytes.subarray(sourceOffset + PIPE_OFF_TAIL, sourceOffset + PIPE_OFF_READ_OPEN),
            targetOffset + PIPE_OFF_TAIL,
          );
          targetBytes.set(
            sourceBytes.subarray(sourceOffset + PIPE_OFF_DATA, sourceOffset + PIPE_SLOT_BYTES),
            targetOffset + PIPE_OFF_DATA,
          );
        } else if (sourceTail === targetTail && sourceHead !== sourceTail) {
          targetBytes.set(
            sourceBytes.subarray(sourceOffset + PIPE_OFF_DATA, sourceOffset + PIPE_SLOT_BYTES),
            targetOffset + PIPE_OFF_DATA,
          );
        }
      }
      syncPipeOfdGenerations(targetExports, targetState, pipeIdx);
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
      if (supervisorOfdRefcount(canonicalExports, ofdId) <= 0) {
        continue;
      }
      for (const { pipeSlot } of collectOfdPipeModes(canonicalExports, ofdId)) {
        if (pipeSlot > 0 && pipeSlot < maxPipes) {
          managedPipeSlots.add(pipeSlot);
        }
      }
    }

    return [...managedPipeSlots].sort((left, right) => left - right);
  }

  return {
    applySelectedPipeSlotSnapshots,
    applyPipeControlState,
    clearSelectedPipeSlots,
    collectCanonicalManagedPipeSlots,
    collectPipeSlotsForOfdIds,
    collectProcessHeldPipeSlots,
    collectProcessPrivatePipeSourceModes,
    collectProcessPipeSlotModes,
    collectProcessPrivatePipeSlots,
    composeProcessPipeUsed,
    copySelectedPipeSlots,
    mergePipeSlotsFromCanonical,
    mergePipeSlotsFromProcess,
    recomputeCanonicalPipeControlState,
  };
}
