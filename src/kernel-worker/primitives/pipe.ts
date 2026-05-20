import { attachKernelRuntimeState, readKernelRuntimeState, type KernelExports, type KernelRuntimeState } from "../../abi.js";
import type { KernelWorkerPipeSlotSnapshot } from "../../messages.js";
import { collectProcessHeldOfdRefcounts } from "./ofd.js";

const FD_TYPE_PIPE = 6;
const FD_TYPE_PTY_MASTER = 11;
const FD_TYPE_PTY_SLAVE = 12;
const OPEN_ACCESS_MODE_MASK = 0x3;
const O_RDONLY = 0;
const O_WRONLY = 1;
const PIPE_SLOT_BYTES = 32 + 65_536;
const PIPE_SLOT_MODE_READ = 1 << 0;
const PIPE_SLOT_MODE_WRITE = 1 << 1;
const PIPE_SLOT_MODE_CONTROL = 1 << 2;
const PIPE_OFF_HEAD = 0;
const PIPE_OFF_TAIL = 4;
const PIPE_OFF_READ_OPEN = 8;
const PIPE_OFF_WRITE_OPEN = 12;
const PIPE_OFF_GENERATION = 20;
const PIPE_OFF_DATA = 32;
const PIPE_BUF_SIZE = PIPE_SLOT_BYTES - PIPE_OFF_DATA;
const OFD_TYPE_OFFSET = 0;
const OFD_OFFSET_OFFSET = 16;
const OFD_INO_OFFSET = 24;

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
        view.getInt32(ofdOffset + OFD_TYPE_OFFSET, true) === FD_TYPE_PIPE
        && view.getUint32(ofdOffset + OFD_INO_OFFSET, true) === pipeSlot
      ) {
        view.setBigInt64(ofdOffset + OFD_OFFSET_OFFSET, BigInt(pipeGeneration), true);
      }
    }
  }

  function collectActiveHostStdioPipeSlots(processExports: KernelExports, vmPtr: number): {
    stdin: number;
    stdout: number;
    stderr: number;
  } {
    const validPipeSlot = (pipeSlot: number): number => (
      pipeSlot > 0 && pipeSlot < processExports.abi_max_pipes() ? pipeSlot : 0
    );
    const stdinPipe = processExports.process_stdin_pipe(vmPtr);
    const stdoutPipe = processExports.process_stdout_pipe(vmPtr);
    const stderrPipe = processExports.process_stderr_pipe(vmPtr);
    return {
      stdin: validPipeSlot(stdinPipe),
      stdout: validPipeSlot(stdoutPipe),
      stderr: validPipeSlot(stderrPipe),
    };
  }

  function collectOfdPipeModes(
    processExports: KernelExports,
    ofdId: number,
  ): Array<{ pipeSlot: number; mode: number }> {
    const fdType = processExports.kernel_ofd_fd_type(ofdId);
    const statusFlags = processExports.kernel_ofd_status_flags(ofdId);
    const accessMode = statusFlags & OPEN_ACCESS_MODE_MASK;
    if (fdType === FD_TYPE_PIPE) {
      const pipeSlot = processExports.kernel_ofd_ino(ofdId);
      if (pipeSlot <= 0 || pipeSlot >= processExports.abi_max_pipes()) {
        return [];
      }
      return [{
        pipeSlot,
        mode: processExports.kernel_ofd_host_fd(ofdId) >= 0
          ? PIPE_SLOT_MODE_READ
          : PIPE_SLOT_MODE_WRITE,
      }];
    }
    if (fdType !== FD_TYPE_PTY_SLAVE && fdType !== FD_TYPE_PTY_MASTER) {
      return [];
    }
    const ptyIdx = processExports.kernel_ofd_ino(ofdId);
    const inputPipe = processExports.kernel_pty_input_pipe?.(ptyIdx) ?? 0;
    const outputPipe = processExports.kernel_pty_output_pipe?.(ptyIdx) ?? 0;
    const modes: Array<{ pipeSlot: number; mode: number }> = [];
    if (fdType === FD_TYPE_PTY_SLAVE) {
      if (accessMode !== O_WRONLY && inputPipe > 0) {
        modes.push({ pipeSlot: inputPipe, mode: PIPE_SLOT_MODE_READ });
      }
      if (accessMode !== O_RDONLY && outputPipe > 0) {
        modes.push({ pipeSlot: outputPipe, mode: PIPE_SLOT_MODE_WRITE });
      }
      return modes;
    }
    if (accessMode !== O_WRONLY && outputPipe > 0) {
      modes.push({ pipeSlot: outputPipe, mode: PIPE_SLOT_MODE_READ });
    }
    if (accessMode !== O_RDONLY && inputPipe > 0) {
      modes.push({ pipeSlot: inputPipe, mode: PIPE_SLOT_MODE_WRITE });
    }
    return modes;
  }

  function collectBlockedOfdPipeModes(
    processExports: KernelExports,
    vmPtr: number,
  ): Array<{ ofdId: number; pipeSlot: number; mode: number }> {
    const ofdId = processExports.thread_blocked_ofd_id?.(vmPtr) ?? 0;
    if (ofdId <= 0 || ofdId >= processExports.abi_max_fds()) {
      return [];
    }
    return collectOfdPipeModes(processExports, ofdId)
      .map(({ pipeSlot, mode }) => ({ ofdId, pipeSlot, mode }));
  }

  function collectProcessPrivatePipeSlots(
    processExports: KernelExports,
    vmPtr: number,
  ): Set<number> {
    const maxPipes = processExports.abi_max_pipes();
    const privatePipeSlots = new Set<number>();
    const activeHostStdio = collectActiveHostStdioPipeSlots(processExports, vmPtr);
    const candidatePipeSlots = [activeHostStdio.stdin, activeHostStdio.stdout, activeHostStdio.stderr];

    for (const pipeSlot of candidatePipeSlots) {
      if (pipeSlot > 0 && pipeSlot < maxPipes) {
        privatePipeSlots.add(pipeSlot);
      }
    }

    return privatePipeSlots;
  }

  function collectProcessPrivatePipeSourceModes(
    processExports: KernelExports,
    vmPtr: number,
  ): Map<number, number> {
    const maxPipes = processExports.abi_max_pipes();
    const privatePipeSlotModes = new Map<number, number>();
    const activeHostStdio = collectActiveHostStdioPipeSlots(processExports, vmPtr);
    const privatePipeCandidates = [
      {
        pipeSlot: activeHostStdio.stdin,
        mode: PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE,
      },
      { pipeSlot: activeHostStdio.stdout, mode: PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE },
      { pipeSlot: activeHostStdio.stderr, mode: PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE },
    ];

    for (const { pipeSlot, mode } of privatePipeCandidates) {
      if (pipeSlot <= 0 || pipeSlot >= maxPipes) {
        continue;
      }
      privatePipeSlotModes.set(pipeSlot, (privatePipeSlotModes.get(pipeSlot) ?? 0) | mode);
    }

    return privatePipeSlotModes;
  }

  function collectProcessPrivatePipeTargetModes(
    processExports: KernelExports,
    vmPtr: number,
  ): Map<number, number> {
    const maxPipes = processExports.abi_max_pipes();
    const privatePipeSlotModes = new Map<number, number>();
    const activeHostStdio = collectActiveHostStdioPipeSlots(processExports, vmPtr);
    const privatePipeCandidates = [
      {
        pipeSlot: activeHostStdio.stdin,
        mode: PIPE_SLOT_MODE_READ,
      },
      { pipeSlot: activeHostStdio.stdout, mode: PIPE_SLOT_MODE_READ },
      { pipeSlot: activeHostStdio.stderr, mode: PIPE_SLOT_MODE_READ },
    ];

    for (const { pipeSlot, mode } of privatePipeCandidates) {
      if (pipeSlot <= 0 || pipeSlot >= maxPipes) {
        continue;
      }
      privatePipeSlotModes.set(pipeSlot, (privatePipeSlotModes.get(pipeSlot) ?? 0) | mode);
    }

    return privatePipeSlotModes;
  }

  function collectProcessHeldPipeSlots(processExports: KernelExports, vmPtr: number): Set<number> {
    const maxFds = processExports.abi_max_fds();
    const maxPipes = processExports.abi_max_pipes();
    const heldRefcounts = collectProcessHeldOfdRefcounts(processExports, vmPtr);
    const privatePipeSlots = collectProcessPrivatePipeSlots(processExports, vmPtr);
    const pipeSlots = new Set<number>();

    for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
      if (heldRefcounts[ofdId] <= 0) {
        continue;
      }
      for (const { pipeSlot } of collectOfdPipeModes(processExports, ofdId)) {
        if (pipeSlot > 0 && pipeSlot < maxPipes && !privatePipeSlots.has(pipeSlot)) {
          pipeSlots.add(pipeSlot);
        }
      }
    }
    for (const { pipeSlot } of collectBlockedOfdPipeModes(processExports, vmPtr)) {
      if (pipeSlot > 0 && pipeSlot < maxPipes && !privatePipeSlots.has(pipeSlot)) {
        pipeSlots.add(pipeSlot);
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
      if (heldRefcounts[ofdId] <= 0) {
        continue;
      }
      for (const { pipeSlot, mode } of collectOfdPipeModes(processExports, ofdId)) {
        if (pipeSlot <= 0 || pipeSlot >= maxPipes || privatePipeSlots.has(pipeSlot)) {
          continue;
        }
        const currentMode = pipeSlotModes.get(pipeSlot) ?? 0;
        pipeSlotModes.set(pipeSlot, currentMode | mode);
      }
    }
    for (const { pipeSlot, mode } of collectBlockedOfdPipeModes(processExports, vmPtr)) {
      if (pipeSlot <= 0 || pipeSlot >= maxPipes || privatePipeSlots.has(pipeSlot)) {
        continue;
      }
      const currentMode = pipeSlotModes.get(pipeSlot) ?? 0;
      pipeSlotModes.set(pipeSlot, currentMode | mode);
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
    const hostReadPipeSlots = new Set<number>();
    const hostWritePipeSlots = new Set<number>();
    const readEndpointKeys = new Set<string>();
    const writeEndpointKeys = new Set<string>();
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

      const activeHostStdio = collectActiveHostStdioPipeSlots(processExports, vmPtr);
      const stdinPipeSlot = activeHostStdio.stdin;
      if (stdinPipeSlot > 0 && stdinPipeSlot < maxPipes) {
        managedPipeSlots.add(stdinPipeSlot);
        hostWritePipeSlots.add(stdinPipeSlot);
      }

      const stdoutPipeSlot = activeHostStdio.stdout;
      if (stdoutPipeSlot > 0 && stdoutPipeSlot < maxPipes) {
        managedPipeSlots.add(stdoutPipeSlot);
        hostReadPipeSlots.add(stdoutPipeSlot);
      }

      const stderrPipeSlot = activeHostStdio.stderr;
      if (stderrPipeSlot > 0 && stderrPipeSlot < maxPipes) {
        managedPipeSlots.add(stderrPipeSlot);
        hostReadPipeSlots.add(stderrPipeSlot);
      }

      const heldRefcounts = collectProcessHeldOfdRefcounts(processExports, vmPtr);
      for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
        const refcount = heldRefcounts[ofdId] ?? 0;
        if (refcount <= 0) {
          continue;
        }
        for (const { pipeSlot, mode } of collectOfdPipeModes(processExports, ofdId)) {
          if (pipeSlot <= 0 || pipeSlot >= maxPipes) {
            continue;
          }
          managedPipeSlots.add(pipeSlot);
          if ((mode & PIPE_SLOT_MODE_READ) !== 0) {
            readEndpointKeys.add(`${pid}:${pipeSlot}:${ofdId}`);
          }
          if ((mode & PIPE_SLOT_MODE_WRITE) !== 0) {
            writeEndpointKeys.add(`${pid}:${pipeSlot}:${ofdId}`);
          }
        }
      }
      for (const { ofdId, pipeSlot, mode } of collectBlockedOfdPipeModes(processExports, vmPtr)) {
        if (pipeSlot <= 0 || pipeSlot >= maxPipes) {
          continue;
        }
        managedPipeSlots.add(pipeSlot);
        if ((mode & PIPE_SLOT_MODE_READ) !== 0) {
          readEndpointKeys.add(`${pid}:${pipeSlot}:${ofdId}`);
        }
        if ((mode & PIPE_SLOT_MODE_WRITE) !== 0) {
          writeEndpointKeys.add(`${pid}:${pipeSlot}:${ofdId}`);
        }
      }
    }

    for (const key of readEndpointKeys) {
      const [, pipeSlot] = key.split(":");
      readOpen[Number(pipeSlot)] += 1;
    }
    for (const key of writeEndpointKeys) {
      const [, pipeSlot] = key.split(":");
      writeOpen[Number(pipeSlot)] += 1;
    }
    for (const pipeSlot of hostReadPipeSlots) {
      readOpen[pipeSlot] += 1;
    }
    for (const pipeSlot of hostWritePipeSlots) {
      writeOpen[pipeSlot] += 1;
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
      if (canonicalExports.kernel_ofd_refcount(ofdId) <= 0) {
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
