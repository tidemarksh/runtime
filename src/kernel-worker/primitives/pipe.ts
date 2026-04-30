import { attachKernelRuntimeState, readKernelRuntimeState, type KernelExports, type KernelRuntimeState } from "../../abi.js";
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

  function collectProcessPrivatePipeSourceModes(
    processExports: KernelExports,
    vmPtr: number,
  ): Map<number, number> {
    const maxPipes = processExports.abi_max_pipes();
    const privatePipeSlotModes = new Map<number, number>();
    const privatePipeCandidates = [
      {
        pipeSlot: processExports.process_stdin_pipe(vmPtr),
        mode: PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE,
      },
      { pipeSlot: processExports.process_stdout_pipe(vmPtr), mode: PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE },
      { pipeSlot: processExports.process_stderr_pipe(vmPtr), mode: PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE },
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
    const privatePipeCandidates = [
      {
        pipeSlot: processExports.process_stdin_pipe(vmPtr),
        mode: PIPE_SLOT_MODE_READ,
      },
      { pipeSlot: processExports.process_stdout_pipe(vmPtr), mode: PIPE_SLOT_MODE_READ },
      { pipeSlot: processExports.process_stderr_pipe(vmPtr), mode: PIPE_SLOT_MODE_READ },
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

      const stdinPipeSlot = processExports.process_stdin_pipe(vmPtr);
      if (stdinPipeSlot > 0 && stdinPipeSlot < maxPipes) {
        managedPipeSlots.add(stdinPipeSlot);
        writeOpen[stdinPipeSlot] += 1;
      }

      const hostReadPipeSlots = new Set<number>();
      const stdoutPipeSlot = processExports.process_stdout_pipe(vmPtr);
      if (stdoutPipeSlot > 0 && stdoutPipeSlot < maxPipes) {
        managedPipeSlots.add(stdoutPipeSlot);
        hostReadPipeSlots.add(stdoutPipeSlot);
      }

      const stderrPipeSlot = processExports.process_stderr_pipe(vmPtr);
      if (stderrPipeSlot > 0 && stderrPipeSlot < maxPipes) {
        managedPipeSlots.add(stderrPipeSlot);
        hostReadPipeSlots.add(stderrPipeSlot);
      }
      for (const pipeSlot of hostReadPipeSlots) {
        readOpen[pipeSlot] += 1;
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
    }

    for (const key of readEndpointKeys) {
      const [, pipeSlot] = key.split(":");
      readOpen[Number(pipeSlot)] += 1;
    }
    for (const key of writeEndpointKeys) {
      const [, pipeSlot] = key.split(":");
      writeOpen[Number(pipeSlot)] += 1;
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
    const sourceView = new DataView(sourceExports.memory.buffer);
    const targetView = new DataView(targetExports.memory.buffer);

    function counterAhead(sourceValue: number, targetValue: number): boolean {
      const delta = (sourceValue - targetValue) >>> 0;
      return delta !== 0 && delta < 0x80000000;
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
      const targetHead = targetView.getUint32(targetOffset + PIPE_OFF_HEAD, true);
      const targetTail = targetView.getUint32(targetOffset + PIPE_OFF_TAIL, true);
      if (
        sourceHead === 0
        && sourceTail === 0
        && (targetHead !== 0 || targetTail !== 0)
      ) {
        targetBytes.set(
          sourceBytes.subarray(sourceOffset, sourceOffset + PIPE_SLOT_BYTES),
          targetOffset,
        );
        continue;
      }
      if ((mode & PIPE_SLOT_MODE_READ) !== 0) {
        if (counterAhead(sourceHead, targetHead)) {
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
        }
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
      const delta = (sourceValue - targetValue) >>> 0;
      return delta !== 0 && delta < 0x80000000;
    }

    for (const pipeIdx of pipeSlots) {
      if (pipeIdx <= 0 || pipeIdx >= sourceExports.abi_max_pipes()) {
        continue;
      }
      const sourceOffset = sourceState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
      const targetOffset = targetState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
      const mode = targetModes.get(pipeIdx) ?? 0;
      if ((mode & PIPE_SLOT_MODE_READ) !== 0) {
        const sourceHead = sourceView.getUint32(sourceOffset + PIPE_OFF_HEAD, true);
        const targetHead = targetView.getUint32(targetOffset + PIPE_OFF_HEAD, true);
        if (counterAhead(sourceHead, targetHead)) {
          targetView.setUint32(targetOffset + PIPE_OFF_HEAD, sourceHead, true);
        }
        const sourceTail = sourceView.getUint32(sourceOffset + PIPE_OFF_TAIL, true);
        const targetTail = targetView.getUint32(targetOffset + PIPE_OFF_TAIL, true);
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
        const sourceHead = sourceView.getUint32(sourceOffset + PIPE_OFF_HEAD, true);
        const targetHead = targetView.getUint32(targetOffset + PIPE_OFF_HEAD, true);
        if (counterAhead(sourceHead, targetHead)) {
          targetView.setUint32(targetOffset + PIPE_OFF_HEAD, sourceHead, true);
        }
        const sourceTail = sourceView.getUint32(sourceOffset + PIPE_OFF_TAIL, true);
        const targetTail = targetView.getUint32(targetOffset + PIPE_OFF_TAIL, true);
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
