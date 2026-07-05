import { supervisorPipeReconcileControl } from "../../abi.js";
import type { KernelWorkerPipeSlotSnapshot } from "../../messages.js";
import {
  PIPE_BUFFER_BYTES,
  PIPE_DATA_OFFSET,
  PIPE_GENERATION_OFFSET,
  PIPE_HEAD_OFFSET,
  PIPE_HEADER_BYTES,
  PIPE_READ_OPEN_OFFSET,
  PIPE_SLOT_MODE_CONTROL,
  PIPE_SLOT_MODE_READ,
  PIPE_SLOT_MODE_WRITE,
  PIPE_SLOT_BYTES,
  PIPE_TAIL_OFFSET,
  PIPE_WRITE_OPEN_OFFSET,
} from "../../layout/pipe.js";
import type { ProcessHandle } from "../types.js";
import { kernelShareMemberList } from "../kernel-share.js";
import { appendPendingPipeSlotSnapshots } from "../pending-pipe-slot-state.js";
import {
  collectProcessHeldOfds,
  syncPipeOfdGenerationsForSlot,
} from "./ofd-table.js";
import {
  supervisorOfdPipeSlotMask,
  supervisorOfdPipeSlots,
  supervisorProcessPipeOfdForFd,
} from "./pipe-slot.js";

const PIPE_CONTROL_END_OFFSET = PIPE_WRITE_OPEN_OFFSET + 4;

function supervisorMaskToBigInt(mask: bigint | number): bigint {
  return typeof mask === "bigint" ? mask : BigInt(mask);
}

function requireSupervisorPipeMask(
  handle: ProcessHandle,
  exportName:
    | "supervisor_process_private_pipe_slot_mask"
    | "supervisor_process_pipe_used_mask",
): (vmPtr: number) => bigint | number {
  const readMask = handle.kernel[exportName];
  if (!readMask) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  return readMask.bind(handle.kernel);
}

function requireSupervisorPipeCount(
  handle: ProcessHandle,
  exportName:
    | "supervisor_process_pipe_read_open_count"
    | "supervisor_process_pipe_write_open_count",
): (vmPtr: number, pipeSlot: number) => number {
  const readCount = handle.kernel[exportName];
  if (!readCount) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  return readCount.bind(handle.kernel);
}

function applySupervisorPipeControl(
  handle: ProcessHandle,
  pipeSlot: number,
  readOpen: number,
  writeOpen: number,
): void {
  const applied = supervisorPipeReconcileControl(handle.kernel, pipeSlot, readOpen, writeOpen);
  if (applied === 0) {
    throw new Error(`kernel supervisor rejected pipe control reconciliation for slot ${pipeSlot}`);
  }
}

function copyPipeSlotDataPlane(
  memory: Uint8Array,
  sourceSlot: Uint8Array,
  targetOffset: number,
  slotBytes: number,
): void {
  memory.set(sourceSlot.subarray(0, PIPE_READ_OPEN_OFFSET), targetOffset);
  memory.set(
    sourceSlot.subarray(PIPE_CONTROL_END_OFFSET, slotBytes),
    targetOffset + PIPE_CONTROL_END_OFFSET,
  );
}

function applyPipeSlotSnapshot(
  handle: ProcessHandle,
  memory: Uint8Array,
  sourceSlot: Uint8Array,
  targetOffset: number,
  slotBytes: number,
  pipeSlot: number,
  readOpen: number,
  writeOpen: number,
): void {
  copyPipeSlotDataPlane(memory, sourceSlot, targetOffset, slotBytes);
  applySupervisorPipeControl(handle, pipeSlot, readOpen, writeOpen);
}

function processPrivatePipeSlotMask(handle: ProcessHandle, vmPtr: number): bigint {
  if (vmPtr === 0) {
    return 0n;
  }
  return supervisorMaskToBigInt(
    requireSupervisorPipeMask(handle, "supervisor_process_private_pipe_slot_mask")(vmPtr),
  );
}

export function isProcessHostStdioOfd(handle: ProcessHandle, vmPtr: number, ofdId: number): boolean {
  const privatePipeMask = processPrivatePipeSlotMask(handle, vmPtr);
  if (privatePipeMask === 0n) {
    return false;
  }
  const ofdPipeMask = supervisorOfdPipeSlotMask(handle, ofdId);
  if ((ofdPipeMask & privatePipeMask) === 0n) {
    return false;
  }
  for (const fd of [0, 1, 2] as const) {
    const fdOfdId = supervisorProcessPipeOfdForFd(handle, vmPtr, fd);
    if (fdOfdId === ofdId) {
      return true;
    }
  }
  return false;
}

export function isProcessHostStdioPipeOfd(handle: ProcessHandle, ofdId: number): boolean {
  return (supervisorOfdPipeSlotMask(handle, ofdId) & processPrivatePipeSlotMask(handle, handle.vmPtr)) !== 0n;
}

export function expandPipePeerOfdIds(handle: ProcessHandle, ofdIds: Iterable<number>): number[] {
  let pipeSlotMask = 0n;
  for (const ofdId of ofdIds) {
    pipeSlotMask |= supervisorOfdPipeSlotMask(handle, ofdId);
  }
  if (pipeSlotMask === 0n) {
    return [];
  }

  const peers = new Set<number>();
  for (const member of kernelShareMemberList(handle)) {
    for (const ofdId of collectProcessHeldOfds(member)) {
      if ((supervisorOfdPipeSlotMask(member, ofdId) & pipeSlotMask) !== 0n) {
        peers.add(ofdId);
      }
    }
  }
  return [...peers];
}

export function pipeSlotForOfd(handle: ProcessHandle, ofdId: number): number {
  return supervisorOfdPipeSlots(handle, ofdId)[0] ?? 0;
}

export function hasPipePeerInOtherKernelShareMember(handle: ProcessHandle, ofdId: number): boolean {
  const pipeSlotMask = supervisorOfdPipeSlotMask(handle, ofdId);
  if (pipeSlotMask === 0n) {
    return false;
  }
  for (const member of kernelShareMemberList(handle)) {
    if (member === handle) {
      continue;
    }
    for (const peerOfdId of collectProcessHeldOfds(member)) {
      if ((supervisorOfdPipeSlotMask(member, peerOfdId) & pipeSlotMask) !== 0n) {
        return true;
      }
    }
  }
  return false;
}

export function liveProcessPipeUsedBits(handle: ProcessHandle): bigint {
  if (handle.vmPtr === 0) {
    return 0n;
  }
  const supervisorPipeUsedMask = requireSupervisorPipeMask(handle, "supervisor_process_pipe_used_mask");
  const mask = supervisorPipeUsedMask(handle.vmPtr);
  return supervisorMaskToBigInt(mask);
}

export function composeHandlePipeUsed(handle: ProcessHandle, sharedPipeUsed: bigint): bigint {
  if (handle.vmPtr === 0) {
    return sharedPipeUsed | 1n;
  }
  const supervisorPipeUsedMask = requireSupervisorPipeMask(handle, "supervisor_process_pipe_used_mask");
  const mask = supervisorPipeUsedMask(handle.vmPtr);
  return sharedPipeUsed | 1n | supervisorMaskToBigInt(mask);
}

export const PIPE_WAIT_GUEST_MEMORY_WRITE_BYTES = PIPE_BUFFER_BYTES;

function pipeDebugPreviewEnabled(): boolean {
  return ((globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_PIPE_DEBUG_PREVIEW ?? "") === "1";
}

function formatPipeDebugAscii(bytes: Uint8Array, maxLen = 96): string {
  let out = "";
  for (const byte of bytes.subarray(0, maxLen)) {
    if (byte === 0x0a) {
      out += "\\n";
    } else if (byte === 0x0d) {
      out += "\\r";
    } else if (byte === 0x09) {
      out += "\\t";
    } else if (byte >= 0x20 && byte <= 0x7e && byte !== 0x5c && byte !== 0x22) {
      out += String.fromCharCode(byte);
    } else {
      out += `\\x${byte.toString(16).padStart(2, "0")}`;
    }
  }
  if (bytes.length > maxLen) {
    out += "...";
  }
  return out;
}

function pipeSlotBase(handle: ProcessHandle, pipeSlot: number): number | null {
  if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
    return null;
  }
  return handle.kernelState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
}

function readPipeSlotCounters(
  handle: ProcessHandle,
  pipeSlot: number,
): { base: number; head: number; tail: number; readOpen: number; writeOpen: number } | null {
  const base = pipeSlotBase(handle, pipeSlot);
  if (base === null || base < 0 || base + PIPE_HEADER_BYTES > handle.memory.buffer.byteLength) {
    return null;
  }
  const view = new DataView(handle.memory.buffer);
  return {
    base,
    head: view.getUint32(base + PIPE_HEAD_OFFSET, true),
    tail: view.getUint32(base + PIPE_TAIL_OFFSET, true),
    readOpen: view.getInt32(base + PIPE_READ_OPEN_OFFSET, true),
    writeOpen: view.getInt32(base + PIPE_WRITE_OPEN_OFFSET, true),
  };
}

export function pipeSlotHasBufferedData(handle: ProcessHandle, pipeSlot: number): boolean {
  const counters = readPipeSlotCounters(handle, pipeSlot);
  return counters !== null && counters.head !== counters.tail;
}

export function pipeSlotWriteOpen(handle: ProcessHandle, pipeSlot: number): number | null {
  return readPipeSlotCounters(handle, pipeSlot)?.writeOpen ?? null;
}

function formatPipeWaitDataDebug(handle: ProcessHandle, base: number, head: number, tail: number): string {
  if (!pipeDebugPreviewEnabled()) {
    return "";
  }
  const len = tail >= head
    ? tail - head
    : PIPE_BUFFER_BYTES - head + tail;
  if (len <= 0 || len > PIPE_BUFFER_BYTES) {
    return `:data_len=${len}`;
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  const dataBase = base + PIPE_DATA_OFFSET;
  const previewLen = Math.min(len, 96);
  const preview = new Uint8Array(previewLen);
  for (let i = 0; i < previewLen; i += 1) {
    preview[i] = bytes[dataBase + ((head + i) % PIPE_BUFFER_BYTES)];
  }
  return `:data_len=${len}:data_ascii=${JSON.stringify(formatPipeDebugAscii(preview))}`;
}

export function formatPipeSlotControlDebug(handle: ProcessHandle, pipeSlot: number): string {
  const counters = readPipeSlotCounters(handle, pipeSlot);
  if (counters === null) {
    return `slot${pipeSlot}/invalid`;
  }
  return `slot${pipeSlot}/h${counters.head}/t${counters.tail}`
    + `/ro${counters.readOpen}/wo${counters.writeOpen}`
    + formatPipeWaitDataDebug(handle, counters.base, counters.head, counters.tail);
}

export function applyThreadPipeSlots(
  handle: ProcessHandle,
  pipeSlots: readonly KernelWorkerPipeSlotSnapshot[] | undefined,
  appendRuntimeLifecycleEvent?: (runtimeId: number, event: string) => void,
): void {
  if (!pipeSlots || pipeSlots.length === 0) {
    return;
  }
  const pipeRegionBytes = handle.kernel.kernel_pipe_region_bytes();
  const maxPipes = handle.kernel.abi_max_pipes();
  const slotBytes = pipeRegionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
  }
  const pipeBufferBytes = slotBytes - PIPE_HEADER_BYTES;
  const counterAhead = (sourceValue: number, targetValue: number): boolean =>
    sourceValue > targetValue
    || (
      sourceValue < targetValue
      && targetValue > pipeBufferBytes / 2
      && sourceValue < pipeBufferBytes / 2
    );
  const generationAhead = (sourceValue: number, targetValue: number): boolean =>
    targetValue === 0
    || sourceValue > targetValue
    || (
      sourceValue < targetValue
      && targetValue > 0x80000000
      && sourceValue < 0x80000000
    );
  const memory = new Uint8Array(handle.memory.buffer);
  const view = new DataView(handle.memory.buffer);
  const appliedSnapshots: KernelWorkerPipeSlotSnapshot[] = [];
  const copyPipeDataTailDelta = (
    sourceBytes: Uint8Array,
    targetDataOffset: number,
    previousTail: number,
    nextTail: number,
  ): void => {
    const copySpan = (start: number, end: number): void => {
      if (end <= start) {
        return;
      }
      memory.set(
        sourceBytes.subarray(PIPE_HEADER_BYTES + start, PIPE_HEADER_BYTES + end),
        targetDataOffset + start,
      );
    };
    if (previousTail < nextTail) {
      copySpan(previousTail, nextTail);
    } else if (previousTail > nextTail) {
      copySpan(previousTail, pipeBufferBytes);
      copySpan(0, nextTail);
    }
  };
  for (const snapshot of pipeSlots) {
    if (
      snapshot.pipeSlot <= 0
      || snapshot.pipeSlot >= maxPipes
      || snapshot.slot.length !== slotBytes
    ) {
      continue;
    }
    const targetOffset = handle.kernelState.pipeBase + snapshot.pipeSlot * slotBytes;
    const snapshotView = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
    const sourceHead = snapshotView.getUint32(PIPE_HEAD_OFFSET, true);
    const sourceTail = snapshotView.getUint32(PIPE_TAIL_OFFSET, true);
    const sourceReadOpen = snapshotView.getInt32(PIPE_READ_OPEN_OFFSET, true);
    const sourceWriteOpen = snapshotView.getInt32(PIPE_WRITE_OPEN_OFFSET, true);
    let targetHead = view.getUint32(targetOffset + PIPE_HEAD_OFFSET, true);
    let targetTail = view.getUint32(targetOffset + PIPE_TAIL_OFFSET, true);
    const targetReadOpen = view.getInt32(targetOffset + PIPE_READ_OPEN_OFFSET, true);
    const targetWriteOpen = view.getInt32(targetOffset + PIPE_WRITE_OPEN_OFFSET, true);
    const mode = snapshot.mode ?? (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE | PIPE_SLOT_MODE_CONTROL);
    const sourceGeneration = snapshotView.getUint32(PIPE_GENERATION_OFFSET, true);
    const targetGeneration = view.getUint32(targetOffset + PIPE_GENERATION_OFFSET, true);
    if (appendRuntimeLifecycleEvent) {
      const data = snapshot.slot.subarray(PIPE_HEADER_BYTES, PIPE_HEADER_BYTES + 24);
      const ascii = [...data]
        .map((byte) => byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : ".")
        .join("");
      appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `pipe-slot-status:${handle.pid}:slot=${snapshot.pipeSlot}:mode=${mode}`
          + `:src=${sourceHead}/${sourceTail}/${sourceReadOpen}/${sourceWriteOpen}`
          + `:dst=${targetHead}/${targetTail}/${targetReadOpen}/${targetWriteOpen}`
          + `:gen=${sourceGeneration}/${targetGeneration}:fresh=${snapshot.fresh ? 1 : 0}`
          + `:data=${JSON.stringify(ascii)}`,
      );
    }
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
        applyPipeSlotSnapshot(
          handle,
          memory,
          snapshot.slot,
          targetOffset,
          slotBytes,
          snapshot.pipeSlot,
          sourceReadOpen,
          sourceWriteOpen,
        );
        appliedSnapshots.push({
          pipeSlot: snapshot.pipeSlot,
          slot: memory.slice(targetOffset, targetOffset + slotBytes),
          mode,
          fresh: snapshot.fresh,
        });
        syncPipeOfdGenerationsForSlot(handle, snapshot.pipeSlot);
      }
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
      applyPipeSlotSnapshot(
        handle,
        memory,
        snapshot.slot,
        targetOffset,
        slotBytes,
        snapshot.pipeSlot,
        sourceReadOpen,
        sourceWriteOpen,
      );
      appliedSnapshots.push({
        pipeSlot: snapshot.pipeSlot,
        slot: memory.slice(targetOffset, targetOffset + slotBytes),
        mode,
        fresh: snapshot.fresh,
      });
      syncPipeOfdGenerationsForSlot(handle, snapshot.pipeSlot);
      continue;
    }
    if (
      targetReadOpen <= 0
      && targetWriteOpen <= 0
      && targetHead === targetTail
      && (sourceReadOpen > 0 || sourceWriteOpen > 0)
      && ((mode & (PIPE_SLOT_MODE_WRITE | PIPE_SLOT_MODE_CONTROL)) !== 0)
    ) {
      applyPipeSlotSnapshot(
        handle,
        memory,
        snapshot.slot,
        targetOffset,
        slotBytes,
        snapshot.pipeSlot,
        sourceReadOpen,
        sourceWriteOpen,
      );
      appliedSnapshots.push({
        pipeSlot: snapshot.pipeSlot,
        slot: memory.slice(targetOffset, targetOffset + slotBytes),
        mode,
        fresh: snapshot.fresh,
      });
      syncPipeOfdGenerationsForSlot(handle, snapshot.pipeSlot);
      continue;
    }
    if ((mode & PIPE_SLOT_MODE_CONTROL) !== 0) {
      applySupervisorPipeControl(handle, snapshot.pipeSlot, sourceReadOpen, sourceWriteOpen);
    }
    if (
      (mode & PIPE_SLOT_MODE_READ) !== 0
      && (mode & PIPE_SLOT_MODE_WRITE) === 0
      && sourceHead !== 0
      && sourceHead === sourceTail
      && targetTail !== sourceTail
      && counterAhead(sourceTail, targetTail)
    ) {
      view.setUint32(targetOffset + PIPE_HEAD_OFFSET, sourceHead, true);
      view.setUint32(targetOffset + PIPE_TAIL_OFFSET, sourceTail, true);
      targetHead = sourceHead;
      targetTail = sourceTail;
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
      view.setUint32(targetOffset + PIPE_HEAD_OFFSET, 0, true);
      targetHead = 0;
    }
    if (
      (mode & PIPE_SLOT_MODE_READ) !== 0
      && counterAhead(sourceHead, targetHead)
      && (sourceHead !== sourceTail || targetTail === sourceTail)
    ) {
      view.setUint32(targetOffset + PIPE_HEAD_OFFSET, sourceHead, true);
    }
    if ((mode & PIPE_SLOT_MODE_WRITE) !== 0 && counterAhead(sourceTail, targetTail)) {
      view.setUint32(targetOffset + PIPE_TAIL_OFFSET, sourceTail, true);
      copyPipeDataTailDelta(snapshot.slot, targetOffset + PIPE_HEADER_BYTES, targetTail, sourceTail);
    } else if (
      (mode & PIPE_SLOT_MODE_WRITE) !== 0
      && sourceTail === targetTail
      && sourceHead !== sourceTail
      && !((mode & PIPE_SLOT_MODE_READ) !== 0 && counterAhead(targetHead, sourceHead))
    ) {
      memory.set(
        snapshot.slot.subarray(PIPE_HEADER_BYTES, slotBytes),
        targetOffset + PIPE_HEADER_BYTES,
      );
    }
    appliedSnapshots.push({
      pipeSlot: snapshot.pipeSlot,
      slot: memory.slice(targetOffset, targetOffset + slotBytes),
      mode,
      fresh: snapshot.fresh,
    });
    syncPipeOfdGenerationsForSlot(handle, snapshot.pipeSlot);
  }
  appendPendingPipeSlotSnapshots(handle, appliedSnapshots);
}

export function snapshotProcessPipeSlotsForThreadStep(
  handle: ProcessHandle,
  vmPtr: number,
): KernelWorkerPipeSlotSnapshot[] {
  if (vmPtr === 0) {
    return [];
  }
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  const maxPipes = handle.kernel.abi_max_pipes?.() ?? 0;
  const slotBytes = maxPipes > 0 ? handle.kernel.kernel_pipe_region_bytes() / maxPipes : 0;
  if (
    maxFds <= 0
    || maxPipes <= 0
    || !Number.isInteger(slotBytes)
    || slotBytes <= 0
  ) {
    return [];
  }
  const memory = new Uint8Array(handle.memory.buffer);
  const snapshots = new Map<number, KernelWorkerPipeSlotSnapshot>();
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = supervisorProcessPipeOfdForFd(handle, vmPtr, fd);
    for (const pipeSlot of supervisorOfdPipeSlots(handle, ofdId)) {
      if (pipeSlot <= 0 || pipeSlot >= maxPipes || snapshots.has(pipeSlot)) {
        continue;
      }
      const offset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
      if (offset < 0 || offset + slotBytes > memory.byteLength) {
        continue;
      }
      snapshots.set(pipeSlot, {
        pipeSlot,
        slot: memory.slice(offset, offset + slotBytes),
      });
    }
  }
  return [...snapshots.values()];
}

export function refreshPipeControlFromKernelShare(handle: ProcessHandle): void {
  const maxPipes = handle.kernel.abi_max_pipes?.() ?? 0;
  const slotBytes = maxPipes > 0 ? handle.kernel.kernel_pipe_region_bytes() / maxPipes : 0;
  if (
    maxPipes <= 0
    || !Number.isInteger(slotBytes)
    || slotBytes <= 0
  ) {
    return;
  }
  const readOpen = new Int32Array(maxPipes);
  const writeOpen = new Int32Array(maxPipes);
  for (const member of kernelShareMemberList(handle)) {
    if (member.vmPtr === 0) {
      continue;
    }
    const supervisorReadOpen = requireSupervisorPipeCount(
      member,
      "supervisor_process_pipe_read_open_count",
    );
    const supervisorWriteOpen = requireSupervisorPipeCount(
      member,
      "supervisor_process_pipe_write_open_count",
    );
    for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
      const readCount = supervisorReadOpen(member.vmPtr, pipeSlot);
      const writeCount = supervisorWriteOpen(member.vmPtr, pipeSlot);
      if (readCount > 0) {
        readOpen[pipeSlot] += readCount;
      }
      if (writeCount > 0) {
        writeOpen[pipeSlot] += writeCount;
      }
    }
  }
  for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
    if (readOpen[pipeSlot] <= 0 && writeOpen[pipeSlot] <= 0) {
      continue;
    }
    const offset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
    if (offset < 0 || offset + 16 > handle.memory.buffer.byteLength) {
      continue;
    }
    applySupervisorPipeControl(handle, pipeSlot, readOpen[pipeSlot], writeOpen[pipeSlot]);
  }
}
