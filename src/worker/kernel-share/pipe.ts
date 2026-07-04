import { attachKernelRuntimeState, supervisorPipeReconcileControl } from "../../abi.js";
import {
  PIPE_BUFFER_BYTES,
  PIPE_DATA_OFFSET,
  PIPE_READ_OPEN_OFFSET,
  PIPE_SLOT_BYTES,
  PIPE_TAIL_OFFSET,
  PIPE_WRITE_OPEN_OFFSET,
} from "../../layout/pipe.js";
import type { KernelSyncHandle, ProcessHandle } from "../types.js";
import {
  supervisorOfdPipeSlotModeMap,
  supervisorOfdPipeSlots,
} from "../ownership/pipe-slot.js";
import {
  supervisorProcessFdFlagsForFd,
  supervisorProcessOfdForFd,
} from "../ownership/fd-table.js";
import {
  supervisorOfdFdType,
  supervisorOfdHostFd,
  supervisorOfdIno,
} from "../ownership/ofd-metadata.js";

const PIPE_CONTROL_START = PIPE_READ_OPEN_OFFSET;
const PIPE_DATA_START = PIPE_DATA_OFFSET;
const PIPE_OFF_READ_OPEN = PIPE_READ_OPEN_OFFSET;
const PIPE_OFF_TAIL = PIPE_TAIL_OFFSET;
const PIPE_OFF_WRITE_OPEN = PIPE_WRITE_OPEN_OFFSET;
const PIPE_CONTROL_END = PIPE_OFF_WRITE_OPEN + 4;
const PIPE_BUF_SIZE = PIPE_BUFFER_BYTES;

function pipeDebugPreviewEnabled(): boolean {
  const globalWithProcess = globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  };
  return globalWithProcess.process?.env?.TIDEMARK_PIPE_DEBUG_PREVIEW === "1";
}

function pipeDebugDumpEnabled(): boolean {
  const globalWithProcess = globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  };
  return globalWithProcess.process?.env?.TIDEMARK_PIPE_DEBUG_DUMP === "1";
}

function fnv1a32(bytes: Uint8Array): string {
  let hash = 0x811c9dc5;
  for (const byte of bytes) {
    hash ^= byte;
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return `0x${hash.toString(16).padStart(8, "0")}`;
}

function formatAsciiPreview(bytes: Uint8Array, maxLen = 96): string {
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

function formatHex(bytes: Uint8Array): string {
  let out = "";
  for (const byte of bytes) {
    out += byte.toString(16).padStart(2, "0");
  }
  return out;
}

function pipeDebugPreview(handle: ProcessHandle, base: number, head: number, tail: number): string[] {
  if (!pipeDebugPreviewEnabled() || tail === 0) {
    return [];
  }
  const dataBase = base + PIPE_DATA_START;
  const bytes = new Uint8Array(handle.memory.buffer);
  const len = head === tail
    ? tail
    : tail > head
      ? tail - head
      : PIPE_BUF_SIZE - head + tail;
  if (len <= 0 || len > PIPE_BUF_SIZE) {
    return [];
  }
  const contiguousStart = head === tail ? 0 : head;
  const previewLen = Math.min(len, PIPE_BUF_SIZE - contiguousStart);
  const data = bytes.slice(dataBase + contiguousStart, dataBase + contiguousStart + previewLen);
  const tailStart = Math.max(0, data.length - 96);
  const parts = [
    `data_len=${len}`,
    `data_hash=${fnv1a32(data)}`,
    `data_ascii=${JSON.stringify(formatAsciiPreview(data))}`,
    `data_tail=${JSON.stringify(formatAsciiPreview(data.subarray(tailStart), 96))}`,
  ];
  if (pipeDebugDumpEnabled()) {
    parts.push(`data_hex=${formatHex(data)}`);
  }
  return parts;
}

function collectLocalPipeSlotsForOfd(handle: ProcessHandle, ofdId: number): number[] {
  return supervisorOfdPipeSlots(handle, ofdId);
}

function copyPipeSlotWithoutOpenCounts(bytes: Uint8Array, slot: Uint8Array, slotOffset: number): void {
  bytes.set(slot.subarray(0, PIPE_CONTROL_START), slotOffset);
  bytes.set(slot.subarray(PIPE_CONTROL_END), slotOffset + PIPE_CONTROL_END);
}

export function snapshotLocalPipeSlots(
  handle: ProcessHandle,
  ofdIds: Iterable<number>,
): Map<number, Uint8Array> {
  const maxPipes = handle.kernel.abi_max_pipes();
  const regionBytes = handle.kernel.kernel_pipe_region_bytes();
  const slotBytes = regionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid pipe region layout: bytes=${regionBytes} maxPipes=${maxPipes}`);
  }

  const snapshots = new Map<number, Uint8Array>();
  for (const ofdId of ofdIds) {
    for (const pipeSlot of collectLocalPipeSlotsForOfd(handle, ofdId)) {
      if (pipeSlot < 0 || pipeSlot >= maxPipes || snapshots.has(pipeSlot)) {
        continue;
      }
      const slotOffset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
      snapshots.set(pipeSlot, new Uint8Array(handle.memory.buffer, slotOffset, slotBytes).slice());
    }
  }
  return snapshots;
}

export function restoreLocalPipeSlots(
  handle: KernelSyncHandle,
  preservedPipeSlots: ReadonlyMap<number, Uint8Array> | null,
  options: { includeControl?: boolean } = {},
): void {
  if (!preservedPipeSlots || preservedPipeSlots.size === 0) {
    return;
  }
  const maxPipes = handle.kernel.abi_max_pipes();
  const regionBytes = handle.kernel.kernel_pipe_region_bytes();
  const slotBytes = regionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid pipe region layout: bytes=${regionBytes} maxPipes=${maxPipes}`);
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  for (const [pipeSlot, slot] of preservedPipeSlots) {
    const slotOffset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
    if (options.includeControl === true) {
      const slotView = new DataView(slot.buffer, slot.byteOffset, slot.byteLength);
      const readOpen = slotView.getInt32(PIPE_OFF_READ_OPEN, true);
      const writeOpen = slotView.getInt32(PIPE_OFF_WRITE_OPEN, true);
      copyPipeSlotWithoutOpenCounts(bytes, slot, slotOffset);
      const applied = supervisorPipeReconcileControl(handle.kernel, pipeSlot, readOpen, writeOpen);
      if (applied === 0) {
        throw new Error(`kernel supervisor rejected pipe control reconciliation for slot ${pipeSlot}`);
      }
      continue;
    }
    bytes.set(slot.subarray(0, PIPE_CONTROL_START), slotOffset);
    bytes.set(slot.subarray(PIPE_DATA_START), slotOffset + PIPE_DATA_START);
  }
}

function supervisorMaskToBigInt(mask: bigint | number): bigint {
  return typeof mask === "bigint" ? mask : BigInt(mask);
}

function addSupervisorPipeControlForMember(
  member: ProcessHandle,
  readOpen: Int32Array,
  writeOpen: Int32Array,
  maxPipes: number,
): bigint {
  const usedMask = member.kernel.supervisor_process_pipe_used_mask;
  const readOpenCount = member.kernel.supervisor_process_pipe_read_open_count;
  const writeOpenCount = member.kernel.supervisor_process_pipe_write_open_count;
  if (!usedMask || !readOpenCount || !writeOpenCount) {
    throw new Error("kernel supervisor ABI missing process pipe control exports");
  }

  for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
    const readCount = readOpenCount.call(member.kernel, member.vmPtr, pipeSlot);
    const writeCount = writeOpenCount.call(member.kernel, member.vmPtr, pipeSlot);
    if (readCount > 0) {
      readOpen[pipeSlot] += readCount;
    }
    if (writeCount > 0) {
      writeOpen[pipeSlot] += writeCount;
    }
  }
  return supervisorMaskToBigInt(usedMask.call(member.kernel, member.vmPtr));
}

export function reconcileKernelSharePipeControl(handle: ProcessHandle): void {
  const members = [...handle.kernelShare.members].filter((member) => !member.exitInProgress);
  if (members.length === 0) {
    return;
  }
  const maxPipes = handle.kernel.abi_max_pipes();
  const readOpen = new Int32Array(maxPipes);
  const writeOpen = new Int32Array(maxPipes);
  let pipeUsed = 1n;

  for (const member of members) {
    pipeUsed |= addSupervisorPipeControlForMember(member, readOpen, writeOpen, maxPipes);
  }

  for (const member of members) {
    for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
      if ((pipeUsed & (1n << BigInt(pipeSlot))) === 0n) {
        continue;
      }
      const applied = supervisorPipeReconcileControl(
        member.kernel,
        pipeSlot,
        readOpen[pipeSlot] ?? 0,
        writeOpen[pipeSlot] ?? 0,
      );
      if (applied === 0) {
        throw new Error(`kernel supervisor rejected pipe control reconciliation for slot ${pipeSlot}`);
      }
    }
    member.kernelState = {
      ...member.kernelState,
      pipeUsed,
    };
    attachKernelRuntimeState(member.kernel, member.kernelState);
  }
}

export function formatPipeCounterDebug(handle: ProcessHandle, fd: number): string {
  const ofdId = supervisorProcessOfdForFd(handle, handle.vmPtr, fd);
  if (ofdId === 0) {
    return `${fd}:ofd=0`;
  }
  const fdType = supervisorOfdFdType(handle, ofdId);
  const firstPipeSlotMode = supervisorOfdPipeSlotModeMap(handle, ofdId).entries().next().value;
  const pipeSlot = firstPipeSlotMode?.[0] ?? 0;
  const mode = firstPipeSlotMode?.[1] ?? 0;
  if (pipeSlot === 0) {
    return `${fd}:ofd=${ofdId}:type=${fdType}:slot=0`;
  }
  const base = handle.kernelState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
  const i32 = new Int32Array(handle.memory.buffer);
  const u32 = new Uint32Array(handle.memory.buffer);
  const head = Atomics.load(u32, base >>> 2);
  const tail = Atomics.load(u32, (base + PIPE_OFF_TAIL) >>> 2);
  return [
    `${fd}:ofd=${ofdId}`,
    `type=${fdType}`,
    `slot=${pipeSlot}`,
    `mode=${mode}`,
    `ro=${Atomics.load(i32, (base + PIPE_OFF_READ_OPEN) >>> 2)}`,
    `wo=${Atomics.load(i32, (base + PIPE_OFF_WRITE_OPEN) >>> 2)}`,
    `head=${head}`,
    `tail=${tail}`,
    ...pipeDebugPreview(handle, base, head, tail),
  ].join(":");
}

export function formatOpenFdSummary(handle: ProcessHandle, limit = 16): string {
  return Array.from({ length: limit }, (_value, fd) => {
    const ofdId = supervisorProcessOfdForFd(handle, handle.vmPtr, fd);
    if (ofdId === 0) {
      return null;
    }
    return `${fd}->${ofdId}/${supervisorProcessFdFlagsForFd(handle, handle.vmPtr, fd)}/${supervisorOfdFdType(handle, ofdId)}/${supervisorOfdIno(handle, ofdId)}/${supervisorOfdHostFd(handle, ofdId)}`;
  }).filter(Boolean).join(" ");
}
