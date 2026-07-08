import { readPipe, writePipe } from "../../bridge/pipe.js";
import {
  processHostStdio,
  processStderrPipe,
  processStdinPipe,
  processStdioOwner,
  processStdoutPipe,
  processTtyPtySlot,
} from "../process-stdio-state.js";
import { writeKernelScratch } from "../kernel-scratch.js";
import {
  PIPE_HEAD_OFFSET,
  PIPE_HEADER_BYTES,
  PIPE_READ_OPEN_OFFSET,
  PIPE_SLOT_BYTES,
  PIPE_SLOT_MODE_READ,
  PIPE_TAIL_OFFSET,
  PIPE_WRITE_OPEN_OFFSET,
} from "../../layout/pipe.js";
import type { KernelWorkerPipeSlotSnapshot } from "../../messages.js";
import {
  supervisorProcessPipeSlotForFd,
} from "../ownership/pipe-slot.js";
import {
  supervisorProcessFdIsOpen,
  supervisorProcessOfdForFd,
} from "../ownership/fd-table.js";
import { supervisorOfdFdType } from "../ownership/ofd-metadata.js";
import { replacePendingPipeSlotSnapshotForSlot } from "../pending-pipe-slot-state.js";
import { processOnStdout } from "../process-callback-state.js";
import { outputBacklog, stdinQueue } from "../stdio-buffer-state.js";
import type { ProcessHandle, SharedHostStdio } from "../types.js";

function flushOutputBacklog(handle: ProcessHandle): void {
  const onStdout = processOnStdout(handle);
  const backlog = outputBacklog(handle);
  if (!onStdout || backlog.length === 0) {
    return;
  }
  while (backlog.length > 0) {
    const chunk = backlog.shift();
    if (!chunk) {
      continue;
    }
    onStdout(chunk);
  }
}

function enqueueOutput(handle: ProcessHandle, data: Uint8Array): void {
  outputBacklog(handle).push(new Uint8Array(data));
  flushOutputBacklog(handle);
}

function outputTarget(handle: ProcessHandle): ProcessHandle {
  return processStdioOwner(handle) ?? handle;
}

type SupervisorStdioOutputSettleKind =
  | "visible"
  | "inherited-child";

function supervisorStdioOutputSettleKindVisible(handle: ProcessHandle): number {
  const read = handle.kernel.supervisor_stdio_output_settle_kind_visible;
  if (read === undefined) {
    throw new Error("kernel supervisor ABI missing stdio visible output settle kind export");
  }
  return read.call(handle.kernel);
}

function supervisorStdioOutputSettleKindInheritedChild(handle: ProcessHandle): number {
  const read = handle.kernel.supervisor_stdio_output_settle_kind_inherited_child;
  if (read === undefined) {
    throw new Error("kernel supervisor ABI missing stdio inherited-child output settle kind export");
  }
  return read.call(handle.kernel);
}

function supervisorStdioOutputSettleKindValue(
  handle: ProcessHandle,
  kind: SupervisorStdioOutputSettleKind,
): number {
  switch (kind) {
    case "visible":
      return supervisorStdioOutputSettleKindVisible(handle);
    case "inherited-child":
      return supervisorStdioOutputSettleKindInheritedChild(handle);
    default: {
      const exhaustive: never = kind;
      return exhaustive;
    }
  }
}

function supervisorStdioOutputSettlePlan(
  handle: ProcessHandle,
  kind: SupervisorStdioOutputSettleKind,
): { maxPasses: number; stablePasses: number } {
  const readMaxPasses = handle.kernel.supervisor_stdio_output_settle_max_passes;
  const readStablePasses = handle.kernel.supervisor_stdio_output_settle_stable_passes;
  if (readMaxPasses === undefined || readStablePasses === undefined) {
    throw new Error("kernel supervisor ABI missing stdio output settle plan exports");
  }
  const kernelKind = supervisorStdioOutputSettleKindValue(handle, kind);
  const maxPasses = readMaxPasses.call(handle.kernel, kernelKind);
  const stablePasses = readStablePasses.call(handle.kernel, kernelKind);
  if (maxPasses < 0 || stablePasses < 0) {
    throw new Error(`kernel supervisor stdio output settle plan failed: ${maxPasses}/${stablePasses}`);
  }
  if (maxPasses === 0 || stablePasses === 0) {
    throw new Error(`kernel supervisor stdio output settle plan is empty: ${maxPasses}/${stablePasses}`);
  }
  return { maxPasses, stablePasses };
}

function supervisorStdioPipeReadProgressActionValue(
  handle: ProcessHandle,
  action: number,
): "none" | "snapshot" {
  const readNone = handle.kernel.supervisor_stdio_pipe_read_progress_action_none;
  const readSnapshot = handle.kernel.supervisor_stdio_pipe_read_progress_action_snapshot;
  if (readNone === undefined || readSnapshot === undefined) {
    throw new Error("kernel supervisor ABI missing stdio pipe read-progress action kind exports");
  }
  const none = readNone.call(handle.kernel);
  const snapshot = readSnapshot.call(handle.kernel);
  if (action === none) {
    return "none";
  }
  if (action === snapshot) {
    return "snapshot";
  }
  throw new Error(`kernel supervisor stdio pipe read-progress action is unknown: ${action}`);
}

function supervisorShouldSnapshotDrainedPipeReadProgress(
  handle: ProcessHandle,
  pipeSlot: number,
  drained: boolean,
): boolean {
  const decide = handle.kernel.supervisor_stdio_pipe_read_progress_action;
  if (decide === undefined) {
    throw new Error("kernel supervisor ABI missing stdio pipe read-progress action export");
  }
  const action = decide.call(handle.kernel, handle.vmPtr, pipeSlot, drained ? 1 : 0);
  if (action < 0) {
    throw new Error(`kernel supervisor stdio pipe read-progress action failed: ${action}`);
  }
  return supervisorStdioPipeReadProgressActionValue(handle, action) === "snapshot";
}

function pipeStateSignature(handle: ProcessHandle): string {
  const pipeBase = handle.kernelState.pipeBase;
  const bytes = new DataView(handle.memory.buffer);
  const slots = [processHostStdio(handle).stdoutPipe, processHostStdio(handle).stderrPipe]
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

function writePipeHead(handle: ProcessHandle, pipeSlot: number, head: number): void {
  const base = handle.kernelState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
  const u32 = new Uint32Array(handle.memory.buffer);
  Atomics.store(u32, (base + PIPE_HEAD_OFFSET) >>> 2, head);
}

function mirrorDrainedPipeReadProgress(
  source: ProcessHandle,
  target: ProcessHandle,
  pipeSlot: number,
): boolean {
  if (source === target || pipeSlot <= 0) {
    return false;
  }
  const sourceCounters = readPipeCounters(source, pipeSlot);
  if (sourceCounters.head !== sourceCounters.tail) {
    return false;
  }
  const targetCounters = readPipeCounters(target, pipeSlot);
  if (
    targetCounters.head === targetCounters.tail
    || targetCounters.tail !== sourceCounters.tail
  ) {
    return false;
  }
  writePipeHead(target, pipeSlot, sourceCounters.head);
  queueDrainedPipeReadProgress(target, pipeSlot);
  return true;
}

function drainHiddenPipeOutput(
  source: ProcessHandle,
  target: ProcessHandle,
  pipeSlot: number,
): boolean {
  const drained = drainPipeOutput(source, target, pipeSlot);
  const mirrored = mirrorDrainedPipeReadProgress(source, target, pipeSlot);
  return drained || mirrored;
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
  if (!supervisorShouldSnapshotDrainedPipeReadProgress(handle, pipeSlot, true)) {
    return;
  }
  const snapshot = snapshotPipeSlotReadProgress(handle, pipeSlot);
  if (snapshot === null) {
    return;
  }
  replacePendingPipeSlotSnapshotForSlot(handle, snapshot);
}

export function stdioPipeSlotForFd(handle: ProcessHandle, fd: number): number {
  return supervisorProcessPipeSlotForFd(handle, handle.vmPtr, fd);
}

function fdOfdId(handle: ProcessHandle, fd: number): number {
  return supervisorProcessOfdForFd(handle, handle.vmPtr, fd);
}

function stdioFdUsesPipeSlot(handle: ProcessHandle, fd: number, pipeSlot: number): boolean {
  return pipeSlot > 0 && stdioPipeSlotForFd(handle, fd) === pipeSlot;
}

function stdioFdIsDetached(handle: ProcessHandle, fd: number): boolean {
  return !supervisorProcessFdIsOpen(handle, handle.vmPtr, fd);
}

function fdTypeCarriesOutput(handle: ProcessHandle, ofdId: number): boolean {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return false;
  }
  const fdType = supervisorOfdFdType(handle, ofdId);
  const ptySlaveType = handle.kernel.abi_fd_type_pty_slave?.();
  const ptyMasterType = handle.kernel.abi_fd_type_pty_master?.();
  return fdType === handle.kernel.abi_fd_type_pipe()
    || (ptySlaveType !== undefined && fdType === ptySlaveType)
    || (ptyMasterType !== undefined && fdType === ptyMasterType);
}

function outputPipeForFd(handle: ProcessHandle, fd: 1 | 2): number {
  return fd === 1 ? processStdoutPipe(handle) : processStderrPipe(handle);
}

function hostOutputPipeForFd(handle: ProcessHandle, fd: 1 | 2): number {
  return fd === 1 ? processHostStdio(handle).stdoutPipe : processHostStdio(handle).stderrPipe;
}

function inheritedHostOutputFallbackSlot(
  handle: ProcessHandle,
  fd: 1 | 2,
  inheritedSlots: Set<number>,
  ownerOfdId: number,
): number {
  const hostPipeSlot = hostOutputPipeForFd(handle, fd);
  if (hostPipeSlot <= 0 || !inheritedSlots.has(hostPipeSlot)) {
    return 0;
  }
  if (stdioFdIsDetached(handle, fd)) {
    return hostPipeSlot;
  }
  const childOfdId = fdOfdId(handle, fd);
  return childOfdId === ownerOfdId && fdTypeCarriesOutput(handle, childOfdId)
    ? hostPipeSlot
    : 0;
}

function hiddenOutputPipeForFd(
  handle: ProcessHandle,
  owner: ProcessHandle,
  fd: 1 | 2,
  inheritedSlots: Set<number>,
): number {
  const fdSlot = stdioPipeSlotForFd(handle, fd);
  const ownerOfdId = fdOfdId(owner, fd);
  if (inheritedSlots.has(fdSlot) && fdOfdId(handle, fd) === ownerOfdId) {
    return fdSlot;
  }
  if (fdSlot > 0 && fdSlot === outputPipeForFd(handle, fd)) {
    return fdSlot;
  }
  return inheritedHostOutputFallbackSlot(handle, fd, inheritedSlots, ownerOfdId);
}

function inheritedOutputPipeSlots(parent: ProcessHandle): Set<number> {
  return new Set(
    [
      processHostStdio(parent).stdoutPipe,
      processHostStdio(parent).stderrPipe,
      stdioPipeSlotForFd(parent, 1),
      stdioPipeSlotForFd(parent, 2),
    ].filter((slot) => slot > 0),
  );
}

export function writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void {
  if (data.length === 0) {
    return;
  }
  stdinQueue(handle).push(new Uint8Array(data));
}

export function flushStdin(handle: ProcessHandle): boolean {
  if (processStdinPipe(handle) === 0 && processTtyPtySlot(handle) === 0) {
    return false;
  }

  let wrote = false;
  const queue = stdinQueue(handle);
  while (queue.length > 0) {
    const chunk = queue[0];
    if (!chunk) {
      queue.shift();
      continue;
    }
    if (processTtyPtySlot(handle) !== 0 && handle.kernel.process_host_write_stdin) {
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
        queue.shift();
        continue;
      }
      queue[0] = chunk.slice(written);
      break;
    } else {
      const result = writePipe(handle.memory, handle.kernelState.pipeBase, processStdinPipe(handle), chunk);
      if (result.broken) {
        queue.length = 0;
        return wrote;
      }
      if (result.written <= 0) {
        break;
      }
      wrote = true;
      if (result.written >= chunk.length) {
        queue.shift();
        continue;
      }
      queue[0] = chunk.slice(result.written);
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
  if (processStdioOwner(handle) !== null) {
    return false;
  }
  const target = outputTarget(handle);
  const source = handle;
  const allowHostPipeFallback = handle.parentPid === null;
  const stdoutPipe = stdioFdUsesPipeSlot(handle, 1, processHostStdio(handle).stdoutPipe)
    ? processHostStdio(handle).stdoutPipe
    : allowHostPipeFallback && stdioFdIsDetached(handle, 1)
      ? processHostStdio(handle).stdoutPipe
      : 0;
  const stderrPipe = stdioFdUsesPipeSlot(handle, 2, processHostStdio(handle).stderrPipe)
    ? processHostStdio(handle).stderrPipe
    : allowHostPipeFallback && stdioFdIsDetached(handle, 2)
      ? processHostStdio(handle).stderrPipe
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
  const owner = processStdioOwner(handle);
  if (owner === null) {
    return false;
  }
  const inheritedSlots = inheritedOutputPipeSlots(owner);
  const target = outputTarget(handle);
  const source = handle;
  const stdoutPipe = hiddenOutputPipeForFd(handle, owner, 1, inheritedSlots);
  const stderrPipe = hiddenOutputPipeForFd(handle, owner, 2, inheritedSlots);
  const stdoutDrained = stdoutPipe !== 0
    ? drainHiddenPipeOutput(source, target, stdoutPipe)
    : false;
  const stderrDrained = stderrPipe !== 0 && stderrPipe !== stdoutPipe
    ? drainHiddenPipeOutput(source, target, stderrPipe)
    : false;
  return stdoutDrained || stderrDrained;
}

export async function settleVisibleProcessOutput(
  handle: ProcessHandle,
  requestPipeStateSync: (handle: ProcessHandle, wakeWaiters?: boolean) => Promise<void>,
): Promise<boolean> {
  const settlePlan = supervisorStdioOutputSettlePlan(
    handle,
    "visible",
  );
  const visible = outputTarget(handle);
  let drainedAny = false;
  let stablePasses = 0;

  for (let pass = 0; pass < settlePlan.maxPasses; pass += 1) {
    const preSyncDrained = drainProcessOutput(visible);
    if (preSyncDrained) {
      await requestPipeStateSync(visible, false);
    }
    await requestPipeStateSync(handle, false);
    if (visible !== handle) {
      await requestPipeStateSync(visible, false);
    }
    const before = pipeStateSignature(visible);
    const drained = drainProcessOutput(visible);
    if (drained) {
      await requestPipeStateSync(visible, false);
    }
    const after = pipeStateSignature(visible);
    drainedAny ||= preSyncDrained || drained;
    if (!preSyncDrained && !drained && before === after) {
      stablePasses += 1;
      if (stablePasses >= settlePlan.stablePasses) {
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
): Promise<boolean> {
  const parent = processStdioOwner(child);
  if (!parent) {
    return false;
  }
  const settlePlan = supervisorStdioOutputSettlePlan(
    child,
    "inherited-child",
  );
  const inheritedSlots = inheritedOutputPipeSlots(parent);
  const slots = [
    hiddenOutputPipeForFd(child, parent, 1, inheritedSlots),
    hiddenOutputPipeForFd(child, parent, 2, inheritedSlots),
  ]
    .filter((slot, index, array) => slot > 0 && array.indexOf(slot) === index);
  if (slots.length === 0) {
    return false;
  }
  let stablePasses = 0;

  for (let pass = 0; pass < settlePlan.maxPasses; pass += 1) {
    const preSyncDrainedChild = drainHiddenProcessOutput(child);
    if (preSyncDrainedChild) {
      await requestPipeStateSync(child, false);
    }
    await requestPipeStateSync(child, false);
    const drainedParent = drainProcessOutput(parent);
    if (drainedParent) {
      await requestPipeStateSync(parent, false);
    }
    const drainedChild = drainedParent ? false : drainHiddenProcessOutput(child);
    if (drainedChild) {
      await requestPipeStateSync(child, false);
    }
    const settled = slots.every((pipeSlot) => {
      const childCounters = readPipeCounters(child, pipeSlot);
      const parentCounters = readPipeCounters(parent, pipeSlot);
      if (parentCounters.tail === childCounters.tail && parentCounters.head === parentCounters.tail) {
        return true;
      }
      return (preSyncDrainedChild || drainedChild) && childCounters.head === childCounters.tail;
    });
    if (settled) {
      stablePasses += 1;
      if (stablePasses >= settlePlan.stablePasses) {
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
