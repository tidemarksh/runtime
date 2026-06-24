import { readPipe, writePipe } from "../bridge/pipe.js";
import { writeKernelScratch } from "./kernel-scratch.js";
import {
  PIPE_HEAD_OFFSET,
  PIPE_HEADER_BYTES,
  PIPE_READ_OPEN_OFFSET,
  PIPE_SLOT_BYTES,
  PIPE_SLOT_MODE_READ,
  PIPE_TAIL_OFFSET,
  PIPE_WRITE_OPEN_OFFSET,
} from "../layout/pipe.js";
import type { KernelWorkerPipeSlotSnapshot } from "../messages.js";
import type { ProcessHandle, SharedHostStdio } from "./types.js";

function flushOutputBacklog(handle: ProcessHandle): void {
  const onStdout = handle.onStdout;
  if (!onStdout || handle.outputBacklog.length === 0) {
    return;
  }
  while (handle.outputBacklog.length > 0) {
    const chunk = handle.outputBacklog.shift();
    if (!chunk) {
      continue;
    }
    onStdout(chunk);
  }
}

function enqueueOutput(handle: ProcessHandle, data: Uint8Array): void {
  handle.outputBacklog.push(new Uint8Array(data));
  flushOutputBacklog(handle);
}

function outputTarget(handle: ProcessHandle): ProcessHandle {
  return handle.stdioOwner ?? handle;
}

function pipeStateSignature(handle: ProcessHandle): string {
  const pipeBase = handle.kernelState.pipeBase;
  const bytes = new DataView(handle.memory.buffer);
  const slots = [handle.hostStdio.stdoutPipe, handle.hostStdio.stderrPipe]
    .filter((slot, index, array) => slot > 0 && array.indexOf(slot) === index);
  return slots
    .map((pipeSlot) => {
      const base = pipeBase + pipeSlot * PIPE_SLOT_BYTES;
      const head = bytes.getUint32(base + PIPE_HEAD_OFFSET, true);
      const tail = bytes.getUint32(base + PIPE_TAIL_OFFSET, true);
      const readOpen = bytes.getInt32(base + PIPE_READ_OPEN_OFFSET, true);
      const writeOpen = bytes.getInt32(base + PIPE_WRITE_OPEN_OFFSET, true);
      return `${pipeSlot}:${head}:${tail}:${readOpen}:${writeOpen}`;
    })
    .join("|");
}

function readPipeCounters(handle: ProcessHandle, pipeSlot: number): {
  head: number;
  tail: number;
} {
  const base = handle.kernelState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
  const bytes = new DataView(handle.memory.buffer);
  return {
    head: bytes.getUint32(base + PIPE_HEAD_OFFSET, true),
    tail: bytes.getUint32(base + PIPE_TAIL_OFFSET, true),
  };
}

function snapshotPipeSlotReadProgress(
  handle: ProcessHandle,
  pipeSlot: number,
): KernelWorkerPipeSlotSnapshot | null {
  const maxPipes = handle.kernel.abi_max_pipes?.() ?? 0;
  const pipeRegionBytes = handle.kernel.kernel_pipe_region_bytes?.() ?? 0;
  const slotBytes = maxPipes > 0 ? pipeRegionBytes / maxPipes : 0;
  if (
    pipeSlot <= 0
    || pipeSlot >= maxPipes
    || !Number.isInteger(slotBytes)
    || slotBytes <= PIPE_HEADER_BYTES
  ) {
    return null;
  }
  const offset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
  return {
    pipeSlot,
    slot: new Uint8Array(handle.memory.buffer).slice(offset, offset + slotBytes),
    mode: PIPE_SLOT_MODE_READ,
  };
}

function queueDrainedPipeReadProgress(handle: ProcessHandle, pipeSlot: number): void {
  const snapshot = snapshotPipeSlotReadProgress(handle, pipeSlot);
  if (snapshot === null) {
    return;
  }
  handle.pendingPipeSlotSnapshots ??= [];
  const existingIndex = handle.pendingPipeSlotSnapshots.findIndex((pending) => (
    pending.pipeSlot === pipeSlot
  ));
  if (existingIndex >= 0) {
    handle.pendingPipeSlotSnapshots.splice(existingIndex, 1);
  }
  handle.pendingPipeSlotSnapshots.push(snapshot);
}

export function stdioPipeSlotForFd(handle: ProcessHandle, fd: number): number {
  const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
  if (ofdId === 0) {
    return 0;
  }
  const fdType = handle.kernel.kernel_ofd_fd_type(ofdId);
  if (fdType === handle.kernel.abi_fd_type_pipe()) {
    return handle.kernel.kernel_ofd_ino(ofdId);
  }

  const ptySlaveType = handle.kernel.abi_fd_type_pty_slave?.();
  const ptyMasterType = handle.kernel.abi_fd_type_pty_master?.();
  if (fdType !== ptySlaveType && fdType !== ptyMasterType) {
    return 0;
  }

  const ptyIdx = handle.kernel.kernel_ofd_ino(ofdId);
  const statusFlags = handle.kernel.kernel_ofd_status_flags?.(ofdId) ?? 0;
  const accessMode = statusFlags & (
    handle.kernel.abi_o_rdonly()
    | handle.kernel.abi_o_wronly()
    | handle.kernel.abi_o_rdwr()
  );
  const fdWritesToStdioOutput = fd === 1 || fd === 2
    || accessMode === handle.kernel.abi_o_wronly()
    || accessMode === handle.kernel.abi_o_rdwr();

  if (fdType === ptySlaveType) {
    return fdWritesToStdioOutput
      ? (handle.kernel.kernel_pty_output_pipe?.(ptyIdx) ?? 0)
      : (handle.kernel.kernel_pty_input_pipe?.(ptyIdx) ?? 0);
  }

  return fdWritesToStdioOutput
    ? (handle.kernel.kernel_pty_input_pipe?.(ptyIdx) ?? 0)
    : (handle.kernel.kernel_pty_output_pipe?.(ptyIdx) ?? 0);
}

function fdPipeOfdId(handle: ProcessHandle, fd: number): number {
  const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
  if (
    ofdId === 0
    || stdioPipeSlotForFd(handle, fd) === 0
  ) {
    return 0;
  }
  return ofdId;
}

function stdioFdUsesPipeSlot(handle: ProcessHandle, fd: number, pipeSlot: number): boolean {
  return pipeSlot > 0 && stdioPipeSlotForFd(handle, fd) === pipeSlot;
}

function stdioFdIsDetached(handle: ProcessHandle, fd: number): boolean {
  return handle.kernel.process_fd_ofd_id(handle.vmPtr, fd) === 0;
}

function inheritedOutputPipeSlots(parent: ProcessHandle): Set<number> {
  return new Set(
    [
      parent.hostStdio.stdoutPipe,
      parent.hostStdio.stderrPipe,
      stdioPipeSlotForFd(parent, 1),
      stdioPipeSlotForFd(parent, 2),
    ].filter((slot) => slot > 0),
  );
}

export function writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void {
  if (data.length === 0) {
    return;
  }
  handle.stdinQueue.push(new Uint8Array(data));
}

export function flushStdin(handle: ProcessHandle): boolean {
  if (handle.stdinPipe === 0 && handle.ttyPtySlot === 0) {
    return false;
  }

  let wrote = false;
  while (handle.stdinQueue.length > 0) {
    const chunk = handle.stdinQueue[0];
    if (!chunk) {
      handle.stdinQueue.shift();
      continue;
    }
    if (handle.ttyPtySlot !== 0 && handle.kernel.process_host_write_stdin) {
      const srcPtr = writeKernelScratch(handle.kernel, chunk);
      const written = handle.kernel.process_host_write_stdin(handle.vmPtr, srcPtr, chunk.length);
      if (written < 0) {
        return wrote;
      }
      if (written === 0) {
        break;
      }
      wrote = true;
      if (written >= chunk.length) {
        handle.stdinQueue.shift();
        continue;
      }
      handle.stdinQueue[0] = chunk.slice(written);
      break;
    } else {
      const result = writePipe(handle.memory, handle.kernelState.pipeBase, handle.stdinPipe, chunk);
      if (result.broken) {
        handle.stdinQueue.length = 0;
        return wrote;
      }
      if (result.written <= 0) {
        break;
      }
      wrote = true;
      if (result.written >= chunk.length) {
        handle.stdinQueue.shift();
        continue;
      }
      handle.stdinQueue[0] = chunk.slice(result.written);
      break;
    }
  }

  return wrote;
}

export function drainPipeOutput(
  source: ProcessHandle,
  target: ProcessHandle,
  pipeSlot: number,
): boolean {
  let drained = false;
  for (;;) {
    const result = readPipe(source.memory, source.kernelState.pipeBase, pipeSlot);
    if (result.data.length > 0) {
      drained = true;
      enqueueOutput(target, result.data);
    }
    if (result.eof || result.data.length === 0) {
      if (drained) {
        queueDrainedPipeReadProgress(source, pipeSlot);
      }
      return drained;
    }
  }
}

export function drainProcessOutput(handle: ProcessHandle): boolean {
  if (handle.stdioOwner !== null) {
    return false;
  }
  const target = outputTarget(handle);
  const source = handle;
  const allowHostPipeFallback = handle.parentPid === null;
  const stdoutPipe = stdioFdUsesPipeSlot(handle, 1, handle.hostStdio.stdoutPipe)
    ? handle.hostStdio.stdoutPipe
    : allowHostPipeFallback && stdioFdIsDetached(handle, 1)
      ? handle.hostStdio.stdoutPipe
      : 0;
  const stderrPipe = stdioFdUsesPipeSlot(handle, 2, handle.hostStdio.stderrPipe)
    ? handle.hostStdio.stderrPipe
    : allowHostPipeFallback && stdioFdIsDetached(handle, 2)
      ? handle.hostStdio.stderrPipe
      : 0;
  const stdoutDrained = stdoutPipe !== 0
    ? drainPipeOutput(source, target, stdoutPipe)
    : false;
  const stderrDrained = stderrPipe !== 0 && stderrPipe !== stdoutPipe
    ? drainPipeOutput(source, target, stderrPipe)
    : false;
  return stdoutDrained || stderrDrained;
}

export function drainHiddenProcessOutput(handle: ProcessHandle): boolean {
  if (handle.stdioOwner === null) {
    return false;
  }
  const inheritedSlots = inheritedOutputPipeSlots(handle.stdioOwner);
  const target = outputTarget(handle);
  const source = handle;
  const stdoutSlot = stdioPipeSlotForFd(handle, 1);
  const stderrSlot = stdioPipeSlotForFd(handle, 2);
  const ownerStdoutOfd = handle.stdioOwner ? fdPipeOfdId(handle.stdioOwner, 1) : 0;
  const ownerStderrOfd = handle.stdioOwner ? fdPipeOfdId(handle.stdioOwner, 2) : 0;
  const stdoutPipe = inheritedSlots.has(stdoutSlot) && fdPipeOfdId(handle, 1) === ownerStdoutOfd
    ? stdoutSlot
    : stdoutSlot > 0 && stdoutSlot === handle.stdoutPipe
      ? stdoutSlot
      : stdioFdIsDetached(handle, 1) && inheritedSlots.has(handle.hostStdio.stdoutPipe)
        ? handle.hostStdio.stdoutPipe
        : 0;
  const stderrPipe = inheritedSlots.has(stderrSlot) && fdPipeOfdId(handle, 2) === ownerStderrOfd
    ? stderrSlot
    : stderrSlot > 0 && stderrSlot === handle.stderrPipe
      ? stderrSlot
      : stdioFdIsDetached(handle, 2) && inheritedSlots.has(handle.hostStdio.stderrPipe)
        ? handle.hostStdio.stderrPipe
        : 0;
  const stdoutDrained = stdoutPipe !== 0
    ? drainPipeOutput(source, target, stdoutPipe)
    : false;
  const stderrDrained = stderrPipe !== 0 && stderrPipe !== stdoutPipe
    ? drainPipeOutput(source, target, stderrPipe)
    : false;
  return stdoutDrained || stderrDrained;
}

export async function settleVisibleProcessOutput(
  handle: ProcessHandle,
  requestPipeStateSync: (handle: ProcessHandle, wakeWaiters?: boolean) => Promise<void>,
  maxPasses = 4,
): Promise<boolean> {
  const visible = outputTarget(handle);
  let drainedAny = false;
  let stablePasses = 0;

  for (let pass = 0; pass < maxPasses; pass += 1) {
    const preSyncDrained = drainProcessOutput(visible);
    if (preSyncDrained && visible.kernelWorkerRequest && visible.pid > 0) {
      await requestPipeStateSync(visible, false);
    }
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      await requestPipeStateSync(handle, false);
    }
    if (visible !== handle && visible.kernelWorkerRequest && visible.pid > 0) {
      await requestPipeStateSync(visible, false);
    }
    const before = pipeStateSignature(visible);
    const drained = drainProcessOutput(visible);
    if (drained && visible.kernelWorkerRequest && visible.pid > 0) {
      await requestPipeStateSync(visible, false);
    }
    const after = pipeStateSignature(visible);
    drainedAny ||= preSyncDrained || drained;
    if (!preSyncDrained && !drained && before === after) {
      stablePasses += 1;
      if (stablePasses >= 2) {
        break;
      }
      continue;
    }
    stablePasses = 0;
  }

  return drainedAny;
}

export async function settleInheritedChildOutput(
  child: ProcessHandle,
  requestPipeStateSync: (handle: ProcessHandle, wakeWaiters?: boolean) => Promise<void>,
  maxPasses = 6,
): Promise<boolean> {
  const parent = child.stdioOwner;
  if (!parent) {
    return false;
  }
  const inheritedSlots = inheritedOutputPipeSlots(parent);
  const slots = [stdioPipeSlotForFd(child, 1), stdioPipeSlotForFd(child, 2)]
    .filter((slot) => inheritedSlots.has(slot))
    .filter((slot, index, array) => slot > 0 && array.indexOf(slot) === index);
  if (slots.length === 0) {
    for (const slot of [child.hostStdio.stdoutPipe, child.hostStdio.stderrPipe]) {
      if (slot > 0 && inheritedSlots.has(slot) && !slots.includes(slot)) {
        slots.push(slot);
      }
    }
  }
  if (slots.length === 0) {
    return false;
  }
  let stablePasses = 0;

  for (let pass = 0; pass < maxPasses; pass += 1) {
    await requestPipeStateSync(child, false);
    const drainedParent = drainProcessOutput(parent);
    if (drainedParent && parent.kernelWorkerRequest && parent.pid > 0) {
      await requestPipeStateSync(parent, false);
    }
    const drainedChild = drainedParent ? false : drainHiddenProcessOutput(child);
    if (drainedChild && child.kernelWorkerRequest && child.pid > 0) {
      await requestPipeStateSync(child, false);
    }
    const settled = slots.every((pipeSlot) => {
      const childCounters = readPipeCounters(child, pipeSlot);
      const parentCounters = readPipeCounters(parent, pipeSlot);
      if (parentCounters.tail === childCounters.tail && parentCounters.head === parentCounters.tail) {
        return true;
      }
      return drainedChild && childCounters.head === childCounters.tail;
    });
    if (settled) {
      stablePasses += 1;
      if (stablePasses >= 2) {
        break;
      }
      continue;
    }
    stablePasses = 0;
  }

  return true;
}

export function createInheritedErrorSink(
  parent: ProcessHandle,
  childPid: number,
  getProcessByPid: (pid: number) => ProcessHandle | undefined,
  failProcess: (handle: ProcessHandle, error: Error) => void,
): (error: Error) => void {
  return (error: Error) => {
    if (parent.pid <= 0 || getProcessByPid(parent.pid) !== parent) {
      return;
    }
    failProcess(parent, new Error(`child pid=${childPid}: ${error.message}`));
  };
}

export function retainSharedHostStdio(shared: SharedHostStdio): SharedHostStdio {
  shared.refCount += 1;
  return shared;
}

export function releaseSharedHostStdioLocal(handle: ProcessHandle): void {
  const hadLocalHostStdio = handle.stdinPipe !== 0 || handle.stdoutPipe !== 0 || handle.stderrPipe !== 0;
  if (!hadLocalHostStdio) {
    return;
  }
  const shared = handle.hostStdio;
  if (shared.refCount > 0) {
    shared.refCount -= 1;
  }
  handle.stdinPipe = 0;
  handle.stdoutPipe = 0;
  handle.stderrPipe = 0;
  if (shared.refCount > 0) {
    return;
  }
  if (handle.stdioOwner && !handle.stdioOwner.exitInProgress) {
    return;
  }
  try {
    handle.kernel.process_release_host_stdio(handle.vmPtr);
  } catch {
    // Worker-error teardown can race with corrupted or already-released process state.
  }
}
