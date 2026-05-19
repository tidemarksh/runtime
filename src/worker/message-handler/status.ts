import {
  attachKernelRuntimeState,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import {
  drainProcessOutput,
  flushStdin,
} from "../stdio.js";
import {
  kernelShareOtherMembers,
  kernelWorkerOwnsPipeState,
  restoreProcSelfExe,
  snapshotProcSelfExe,
} from "../kernel-share.js";
import { processHasLiveThreads } from "../live-threads.js";
import { adoptKernelStateForHandle } from "../sync.js";
import {
  forceNetworkClosedFds,
  networkSocketSlotRecvBufferIndex,
} from "../network-socket-close.js";
import type { ProcessHandle } from "../types.js";
import { createWorkerOwnedStateSyncHandler } from "./owned-state-sync.js";
import { createWorkerStatusTransitionHandler } from "./transitions.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "./types.js";
import { formatCompactDebugSuffix, readProcessPidField, WRITE_DEBUG_KEYS } from "./debug-summary.js";
import { createWorkerWaitInWorkerStatusHandler } from "./wait-in-worker.js";

const STATUS_SOCKET_DEBUG_LOG_ENABLED = ((globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.TIDEMARK_WORKER_DEBUG_LOG ?? "") === "1"
  || ((globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.TIDEMARK_WORKER_DEBUG_NETWORK ?? "") === "1";
const SOCK_STATE_SHUTDOWN = 5;
const maxRegularFileOffsetByHandle = new WeakMap<ProcessHandle, Map<number, bigint>>();
const NET_BUFFER_HEADER_BYTES = 16;
const NET_BUFFER_HEAD_OFFSET = 0;
const NET_BUFFER_TAIL_OFFSET = 4;
const PIPE_HEADER_BYTES = 32;
const PIPE_HEAD_OFFSET = 0;
const PIPE_TAIL_OFFSET = 4;
const PIPE_READ_OPEN_OFFSET = 8;
const PIPE_WRITE_OPEN_OFFSET = 12;
const PIPE_GENERATION_OFFSET = 20;
const PIPE_SLOT_MODE_READ = 1 << 0;
const PIPE_SLOT_MODE_WRITE = 1 << 1;
const PIPE_SLOT_MODE_CONTROL = 1 << 2;
const FS_RING_BUFFER_BYTES = 32 + 64 * 64 + 64 * 16;
const OFD_TYPE_OFFSET = 0;
const OFD_HOST_FD_OFFSET = 4;
const OFD_REFCOUNT_OFFSET = 12;
const OFD_OFFSET_OFFSET = 16;
const OFD_INO_OFFSET = 24;
const FD_ENTRY_BYTES = 8;
const FD_ENTRY_OFD_ID_OFFSET = 0;

interface KernelMemoryWriteOptions {
  skipFsRingWrites?: boolean;
}

interface ThreadRegisterSnapshot {
  vmPtr: number;
  pc: bigint;
  sp: bigint;
  s11: bigint;
}

interface PreservedNetworkRecvBuffer {
  offset: number;
  head: number;
  tail: number;
  data: Uint8Array;
  capacity: number;
  preserveProducer: boolean;
  authoritativeData: boolean;
  forceHead?: boolean;
  forceTail?: boolean;
}

interface PreservedNetBufferHead {
  offset: number;
  head: number;
  tail?: number;
  data?: Uint8Array;
  restoreData?: boolean;
  capacity: number;
  forceHead?: boolean;
  forceTail?: boolean;
}

interface PreservedNetworkSocketSlot {
  offset: number;
  data: Uint8Array;
  socketMask: number;
}

function appendStatusSocketDebug(handle: ProcessHandle, event: string): void {
  if (!STATUS_SOCKET_DEBUG_LOG_ENABLED) {
    return;
  }
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 4096) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 4096);
  }
}

const statusSocketMetaDebug = new WeakMap<ProcessHandle, Map<number, string>>();

function recordStatusSocketMetaTransition(
  handle: ProcessHandle,
  source: string,
  fd: number,
  socketIdx: number,
  state: number,
  type: number,
  ext: number,
  buf: number,
  recv: number,
): void {
  if (!STATUS_SOCKET_DEBUG_LOG_ENABLED) {
    return;
  }
  let byFd = statusSocketMetaDebug.get(handle);
  if (byFd === undefined) {
    byFd = new Map();
    statusSocketMetaDebug.set(handle, byFd);
  }
  const next = `${socketIdx}/${state}/${type}/${ext}/${buf}/${recv}`;
  const previous = byFd.get(fd);
  if (previous === next) {
    return;
  }
  byFd.set(fd, next);
  appendStatusSocketDebug(handle, `socket-meta-transition:${handle.pid}:source=${source}:fd=${fd}:prev=${previous ?? "none"}:next=${next}`);
}

function socketTablePtrs(handle: ProcessHandle): number[] {
  const ptrs = [handle.kernelState.socketTablePtr];
  const activePtr = handle.kernel.kernel_socket_table_ptr?.();
  if (activePtr !== undefined && !ptrs.includes(activePtr)) {
    ptrs.push(activePtr);
  }
  return ptrs;
}

function activeSocketTablePtr(handle: ProcessHandle): number {
  return handle.kernel.kernel_socket_table_ptr?.() ?? handle.kernelState.socketTablePtr;
}

function readNetworkRecvBufferDebug(
  handle: ProcessHandle,
  socketIdx: number,
  netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes(),
): string {
  if (socketIdx < 0) {
    return "-1/-1";
  }
  const recvBufIdx = handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1;
  if (recvBufIdx < 0) {
    return "-1/-1";
  }
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return "-1/-1";
  }
  const absolute = handle.kernelState.netBufBase + recvBufIdx * netBufferBytes;
  const view = new DataView(handle.memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
  return `${view.getUint32(NET_BUFFER_HEAD_OFFSET, true)}/${view.getUint32(NET_BUFFER_TAIL_OFFSET, true)}`;
}

function applyThreadFdEntries(handle: ProcessHandle, msg: ThreadWorkerStatusMessage): void {
  if (!msg.fdEntries || msg.vmPtr === 0) {
    return;
  }
  restoreNetworkOfdSlotWatermarks(handle);
  const maxFds = handle.kernel.abi_max_fds();
  const networkOfds = networkConnectionOfds(handle);
  for (const entry of msg.fdEntries) {
    if (entry.fd < 0 || entry.fd >= maxFds) {
      continue;
    }
    const expectedNetworkOfd = networkOfds.get(entry.fd);
    if (
      expectedNetworkOfd !== undefined
      && entry.ofdId !== 0
      && entry.ofdId !== expectedNetworkOfd
      && !handle.networkClosedFds.has(entry.fd)
    ) {
      continue;
    }
    handle.kernel.process_set_fd_entry(msg.vmPtr, entry.fd, entry.ofdId, entry.fdFlags);
  }
}

function applyThreadGuestMemoryWrites(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  appendRuntimeLifecycleEvent?: WorkerMessageHandlerDeps["appendRuntimeLifecycleEvent"],
): void {
  if (!msg.guestMemoryWrites || msg.guestMemoryWrites.length === 0) {
    return;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  for (const write of msg.guestMemoryWrites) {
    const start = handle.guestBase + write.guestPtr;
    if (
      !Number.isFinite(start)
      || start < handle.guestBase
      || write.bytes.length === 0
      || start + write.bytes.length > memory.byteLength
    ) {
      continue;
    }
    const writeBytes = maskRegularFileFdEntryRollbackGuestWrite(handle, msg, write.guestPtr, write.bytes);
    memory.set(writeBytes, start);
    if (pipeSlotLifecycleDebugEnabled()) {
      appendRuntimeLifecycleEvent?.(
        handle.runtimeId,
        `guest-write-status:${handle.pid}:ptr=0x${write.guestPtr.toString(16)}`
          + `:len=${write.bytes.length}:data=${JSON.stringify(formatBytesAscii(write.bytes))}`,
      );
    }
  }
}

function maskRegularFileFdEntryRollbackGuestWrite(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  guestPtr: number,
  bytes: Uint8Array,
): Uint8Array {
  const fileType = handle.kernel.abi_fd_type_file?.();
  const processPtr = msg.vmPtr === 0 ? 0 : handle.kernel.thread_process_ptr?.(msg.vmPtr);
  if (fileType === undefined || processPtr === undefined || processPtr <= 0) {
    return bytes;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const fdTableStart = processPtr;
  const fdTableEnd = fdTableStart + maxFds * FD_ENTRY_BYTES;
  const writeStart = guestPtr;
  const writeEnd = guestPtr + bytes.length;
  if (writeEnd <= fdTableStart || writeStart >= fdTableEnd) {
    return bytes;
  }

  let masked: Uint8Array | null = null;
  const firstFd = Math.max(0, Math.floor((Math.max(writeStart, fdTableStart) - fdTableStart) / FD_ENTRY_BYTES));
  const lastFd = Math.min(
    maxFds - 1,
    Math.floor((Math.min(writeEnd - 1, fdTableEnd - 1) - fdTableStart) / FD_ENTRY_BYTES),
  );
  const memory = new Uint8Array(handle.memory.buffer);
  for (let fd = firstFd; fd <= lastFd; fd += 1) {
    const entryStart = fdTableStart + fd * FD_ENTRY_BYTES;
    const entryEnd = entryStart + FD_ENTRY_BYTES;
    const overlapStart = Math.max(writeStart, entryStart);
    const overlapEnd = Math.min(writeEnd, entryEnd);
    if (overlapStart >= overlapEnd) {
      continue;
    }
    const currentEntry = memory.subarray(
      handle.guestBase + entryStart,
      handle.guestBase + entryEnd,
    ).slice();
    const candidateEntry = currentEntry.slice();
    candidateEntry.set(
      bytes.subarray(overlapStart - writeStart, overlapEnd - writeStart),
      overlapStart - entryStart,
    );
    const currentOfdId = new DataView(
      currentEntry.buffer,
      currentEntry.byteOffset,
      currentEntry.byteLength,
    ).getUint32(FD_ENTRY_OFD_ID_OFFSET, true);
    const candidateOfdId = new DataView(
      candidateEntry.buffer,
      candidateEntry.byteOffset,
      candidateEntry.byteLength,
    ).getUint32(FD_ENTRY_OFD_ID_OFFSET, true);
    const explicitFdEntry = msg.fdEntries?.find((entry) => entry.fd === fd);
    if (
      !regularFileFdEntryWouldRollBack(handle, fileType, currentOfdId, candidateOfdId)
      && !explicitPipeFdEntryWouldRollBack(handle, explicitFdEntry?.ofdId, currentOfdId, candidateOfdId)
    ) {
      continue;
    }
    if (masked === null) {
      masked = bytes.slice();
    }
    masked.set(
      currentEntry.subarray(overlapStart - entryStart, overlapEnd - entryStart),
      overlapStart - writeStart,
    );
  }
  return masked ?? bytes;
}

function explicitPipeFdEntryWouldRollBack(
  handle: ProcessHandle,
  explicitOfdId: number | undefined,
  currentOfdId: number,
  candidateOfdId: number,
): boolean {
  const maxFds = handle.kernel.abi_max_fds();
  if (
    explicitOfdId === undefined
    || currentOfdId <= 0
    || currentOfdId >= maxFds
    || candidateOfdId === currentOfdId
    || explicitOfdId !== currentOfdId
  ) {
    return false;
  }
  const currentType = handle.kernel.kernel_ofd_fd_type(currentOfdId);
  return currentType === handle.kernel.abi_fd_type_pipe()
    || currentType === handle.kernel.abi_fd_type_pty_master?.()
    || currentType === handle.kernel.abi_fd_type_pty_slave?.();
}

function regularFileFdEntryWouldRollBack(
  handle: ProcessHandle,
  fileType: number,
  currentOfdId: number,
  candidateOfdId: number,
): boolean {
  const maxFds = handle.kernel.abi_max_fds();
  if (
    currentOfdId <= 0
    || candidateOfdId <= 0
    || currentOfdId >= maxFds
    || candidateOfdId >= maxFds
    || currentOfdId === candidateOfdId
  ) {
    return false;
  }
  return handle.kernel.kernel_ofd_fd_type(currentOfdId) === fileType
    && handle.kernel.kernel_ofd_fd_type(candidateOfdId) === fileType
    && handle.kernel.kernel_ofd_host_fd(currentOfdId) === handle.kernel.kernel_ofd_host_fd(candidateOfdId)
    && handle.kernel.kernel_ofd_ino(currentOfdId) === handle.kernel.kernel_ofd_ino(candidateOfdId)
    && handle.kernel.kernel_ofd_refcount(currentOfdId) > 0
    && handle.kernel.kernel_ofd_offset(candidateOfdId) < handle.kernel.kernel_ofd_offset(currentOfdId);
}

function applyThreadKernelMemoryWrites(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  appendRuntimeLifecycleEvent?: WorkerMessageHandlerDeps["appendRuntimeLifecycleEvent"],
  options?: KernelMemoryWriteOptions,
): void {
  if (!msg.kernelMemoryWrites || msg.kernelMemoryWrites.length === 0) {
    return;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  for (const write of msg.kernelMemoryWrites) {
    if (
      !Number.isFinite(write.ptr)
      || write.ptr < 0
      || write.bytes.length === 0
      || write.ptr + write.bytes.length > memory.byteLength
    ) {
      continue;
    }
    if (options?.skipFsRingWrites === true && isFsRingKernelMemoryWrite(handle, msg, write.ptr, write.bytes.length)) {
      if (pipeSlotLifecycleDebugEnabled()) {
        appendRuntimeLifecycleEvent?.(
          handle.runtimeId,
          `kernel-write-status-skip-fs-ring:${handle.pid}:ptr=0x${write.ptr.toString(16)}:len=${write.bytes.length}`,
        );
      }
      continue;
    }
    const restoreNetworkRecv = preserveNetworkRecvForKernelMemoryWrite(handle, msg, write.ptr, write.bytes.length);
    const writeBytes = maskIncompatibleOfdSlotKernelWrite(handle, write.ptr, write.bytes);
    memory.set(writeBytes, write.ptr);
    restoreNetworkRecv?.();
    applyThreadMemfsKernelWriteToForkShareMembers(handle, write.ptr, write.bytes);
    if (pipeSlotLifecycleDebugEnabled()) {
      appendRuntimeLifecycleEvent?.(
        handle.runtimeId,
        `kernel-write-status:${handle.pid}:ptr=0x${write.ptr.toString(16)}:len=${write.bytes.length}`,
      );
    }
  }
}

function maskIncompatibleOfdSlotKernelWrite(
  handle: ProcessHandle,
  ptr: number,
  bytes: Uint8Array,
): Uint8Array {
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return bytes;
  }
  const tableStart = handle.kernelState.ofdTablePtr;
  const tableEnd = tableStart + tableBytes;
  const writeStart = ptr;
  const writeEnd = ptr + bytes.length;
  if (writeEnd <= tableStart || writeStart >= tableEnd) {
    return bytes;
  }

  let masked: Uint8Array | null = null;
  const firstOfdId = Math.max(1, Math.floor((Math.max(writeStart, tableStart) - tableStart) / slotBytes));
  const lastOfdId = Math.min(
    maxFds - 1,
    Math.floor((Math.min(writeEnd - 1, tableEnd - 1) - tableStart) / slotBytes),
  );
  for (let ofdId = firstOfdId; ofdId <= lastOfdId; ofdId += 1) {
    const slotStart = tableStart + ofdId * slotBytes;
    const slotEnd = slotStart + slotBytes;
    const overlapStart = Math.max(writeStart, slotStart);
    const overlapEnd = Math.min(writeEnd, slotEnd);
    if (overlapStart >= overlapEnd) {
      continue;
    }
    const currentSlot = new Uint8Array(handle.memory.buffer, slotStart, slotBytes).slice();
    const candidateSlot = currentSlot.slice();
    candidateSlot.set(
      bytes.subarray(overlapStart - writeStart, overlapEnd - writeStart),
      overlapStart - slotStart,
    );
    if (!ofdSlotWriteWouldClobberIdentityOrRollBack(handle, currentSlot, candidateSlot)) {
      continue;
    }
    if (masked === null) {
      masked = bytes.slice();
    }
    masked.set(
      currentSlot.subarray(overlapStart - slotStart, overlapEnd - slotStart),
      overlapStart - writeStart,
    );
  }
  return masked ?? bytes;
}

function ofdSlotWriteWouldClobberIdentityOrRollBack(
  handle: ProcessHandle,
  currentSlot: Uint8Array,
  candidateSlot: Uint8Array,
): boolean {
  const currentView = new DataView(currentSlot.buffer, currentSlot.byteOffset, currentSlot.byteLength);
  const candidateView = new DataView(candidateSlot.buffer, candidateSlot.byteOffset, candidateSlot.byteLength);
  const currentType = currentView.getInt32(OFD_TYPE_OFFSET, true);
  const candidateType = candidateView.getInt32(OFD_TYPE_OFFSET, true);
  if (currentType === 0) {
    return false;
  }
  if (currentView.getInt32(OFD_REFCOUNT_OFFSET, true) <= 0) {
    return false;
  }
  if (candidateType === 0 || candidateType !== currentType) {
    return true;
  }
  if (
    currentView.getInt32(OFD_HOST_FD_OFFSET, true) !== candidateView.getInt32(OFD_HOST_FD_OFFSET, true)
    || currentView.getUint32(OFD_INO_OFFSET, true) !== candidateView.getUint32(OFD_INO_OFFSET, true)
  ) {
    return true;
  }
  return currentType === handle.kernel.abi_fd_type_file?.()
    && candidateView.getBigInt64(OFD_OFFSET_OFFSET, true) < currentView.getBigInt64(OFD_OFFSET_OFFSET, true);
}

function isFsRingKernelMemoryWrite(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  ptr: number,
  length: number,
): boolean {
  if (msg.status !== handle.kernel.abi_status_fs() || handle.kernel.thread_fs_ring_base === undefined) {
    return false;
  }
  const ringBase = handle.kernel.thread_fs_ring_base(msg.vmPtr);
  if (ringBase <= 0) {
    return false;
  }
  const ringEnd = ringBase + FS_RING_BUFFER_BYTES;
  return ptr < ringEnd && ptr + length > ringBase;
}

function preserveNetworkRecvForKernelMemoryWrite(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  ptr: number,
  length: number,
): (() => void) | null {
  if (networkAuthoritativeFds(handle).size === 0) {
    return null;
  }
  const netBufBase = handle.kernelState.netBufBase;
  const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes?.() ?? 0;
  const netBufEnd = netBufBase + netBufPoolBytes;
  if (netBufBase <= 0 || netBufPoolBytes <= 0 || ptr >= netBufEnd || ptr + length <= netBufBase) {
    return null;
  }
  const currentNetBufUsed = handle.kernelState.netBufUsed
    | handle.kernel.kernel_net_buf_used()
    | deriveNetBufUsedFromSocketSlots(handle, handle.kernelState.socketUsed | handle.kernel.kernel_socket_used());
  const preservedRecvBuffers = preserveNetworkRecvBuffers(handle, msg.vmPtr, netBufPoolBytes);
  const preservedNetBufferHeads = preserveUsedNetBufferHeads(
    handle,
    netBufPoolBytes,
    currentNetBufUsed,
  ).concat(preserveClosedNetworkRecvBuffers(handle, netBufPoolBytes));
  if (preservedRecvBuffers.length === 0 && preservedNetBufferHeads.length === 0) {
    return null;
  }
  return () => {
    const memory = new Uint8Array(handle.memory.buffer);
    restoreNetworkRecvBufferProducers(memory, handle.kernelState.netBufBase, preservedRecvBuffers);
    restoreMergedNetBufferHeads(memory, handle.kernelState.netBufBase, preservedNetBufferHeads);
  };
}

function applyThreadMemfsKernelWriteToForkShareMembers(
  handle: ProcessHandle,
  ptr: number,
  bytes: Uint8Array,
): void {
  const targetOffset = memfsKernelWriteOffset(handle, ptr);
  if (targetOffset === null) {
    return;
  }
  for (const member of kernelShareOtherMembers(handle)) {
    const targetPtr = memfsKernelWriteTargetPtr(member, targetOffset);
    if (targetPtr === null) {
      continue;
    }
    const targetMemory = new Uint8Array(member.memory.buffer);
    if (targetPtr < 0 || targetPtr + bytes.length > targetMemory.byteLength) {
      continue;
    }
    const procSelfExe = snapshotProcSelfExe(member);
    targetMemory.set(bytes, targetPtr);
    restoreProcSelfExe(member, procSelfExe);
  }
}

function memfsKernelWriteOffset(
  handle: ProcessHandle,
  ptr: number,
): { region: "header" | "inodes" | "firstChild" | "dirEntries" | "data"; offset: number } | null {
  const regions: Array<ReturnType<typeof memfsKernelWriteOffset> & { start: number; length: number }> = [
    { region: "header", start: handle.kernelState.memfsPtr, length: handle.kernelState.memfsHeaderBytes, offset: 0 },
    { region: "inodes", start: handle.kernel.kernel_memfs_inodes_ptr?.() ?? 0, length: handle.kernelState.memfsInodesBytes, offset: 0 },
    { region: "firstChild", start: handle.kernel.kernel_memfs_first_child_ptr?.() ?? 0, length: handle.kernelState.memfsFirstChildBytes, offset: 0 },
    { region: "dirEntries", start: handle.kernel.kernel_memfs_dir_entries_ptr?.() ?? 0, length: handle.kernelState.memfsDirEntriesBytes, offset: 0 },
    { region: "data", start: handle.kernel.kernel_memfs_data_ptr?.() ?? 0, length: handle.kernelState.memfsDataBytes, offset: 0 },
  ];
  for (const region of regions) {
    if (region.start > 0 && region.length > 0 && ptr >= region.start && ptr < region.start + region.length) {
      return { region: region.region, offset: ptr - region.start };
    }
  }
  return null;
}

function memfsKernelWriteTargetPtr(
  handle: ProcessHandle,
  sourceOffset: NonNullable<ReturnType<typeof memfsKernelWriteOffset>>,
): number | null {
  switch (sourceOffset.region) {
    case "header":
      return handle.kernelState.memfsPtr + sourceOffset.offset;
    case "inodes":
      return handle.kernel.kernel_memfs_inodes_ptr === undefined
        ? null
        : handle.kernel.kernel_memfs_inodes_ptr() + sourceOffset.offset;
    case "firstChild":
      return handle.kernel.kernel_memfs_first_child_ptr === undefined
        ? null
        : handle.kernel.kernel_memfs_first_child_ptr() + sourceOffset.offset;
    case "dirEntries":
      return handle.kernel.kernel_memfs_dir_entries_ptr === undefined
        ? null
        : handle.kernel.kernel_memfs_dir_entries_ptr() + sourceOffset.offset;
    case "data":
      return handle.kernel.kernel_memfs_data_ptr === undefined
        ? null
        : handle.kernel.kernel_memfs_data_ptr() + sourceOffset.offset;
  }
}

function networkConnectionOfds(handle: ProcessHandle): Map<number, number> {
  handle.networkConnectionOfds ??= new Map();
  return handle.networkConnectionOfds;
}

function restoreNetworkOfdSlotWatermarks(handle: ProcessHandle): void {
  const watermarks = handle.networkOfdSlotWatermarks;
  if (!watermarks || watermarks.size === 0) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  for (const ofdId of networkConnectionOfds(handle).values()) {
    const slot = watermarks.get(ofdId);
    if (!slot || slot.length !== slotBytes || ofdId <= 0 || ofdId >= maxFds) {
      continue;
    }
    if (handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_socket()) {
      continue;
    }
    memory.set(slot, handle.kernelState.ofdTablePtr + ofdId * slotBytes);
    appendStatusSocketDebug(handle, `net-status-ofd-restore:${handle.pid}:ofd=${ofdId}`);
  }
}

function clearNetworkRecvWatermarks(handle: ProcessHandle, fd: number): void {
  if (handle.networkRecvPayloadWatermarks?.has(fd)) {
    appendStatusSocketDebug(handle, `net-status-watermark-clear:${handle.pid}:fd=${fd}`);
  }
  handle.networkRecvHeadWatermarks.delete(fd);
  handle.networkRecvTailWatermarks.delete(fd);
  handle.networkRecvPayloadWatermarks ??= new Map();
  handle.networkRecvPayloadWatermarks.delete(fd);
  handle.networkSocketSlotWatermarks ??= new Map();
  handle.networkSocketSlotWatermarks.delete(fd);
}

function moveNetworkRecvWatermarks(handle: ProcessHandle, fromFd: number, toFd: number): void {
  if (fromFd === toFd) {
    return;
  }
  const head = handle.networkRecvHeadWatermarks.get(fromFd);
  const tail = handle.networkRecvTailWatermarks.get(fromFd);
  handle.networkRecvPayloadWatermarks ??= new Map();
  const payload = handle.networkRecvPayloadWatermarks.get(fromFd);
  handle.networkSocketSlotWatermarks ??= new Map();
  const socketSlot = handle.networkSocketSlotWatermarks.get(fromFd);
  clearNetworkRecvWatermarks(handle, fromFd);
  if (head !== undefined) {
    handle.networkRecvHeadWatermarks.set(toFd, head);
  }
  if (tail !== undefined) {
    handle.networkRecvTailWatermarks.set(toFd, tail);
  }
  if (payload !== undefined) {
    handle.networkRecvPayloadWatermarks.set(toFd, payload);
  }
  if (socketSlot !== undefined) {
    handle.networkSocketSlotWatermarks.set(toFd, socketSlot);
  }
}

function pruneStaleNetworkConnectionsFromFdEntries(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  deps: WorkerMessageHandlerDeps,
): void {
  if (!msg.fdEntries || msg.vmPtr === 0 || handle.networkConnections.size === 0) {
    return;
  }
  const ofds = networkConnectionOfds(handle);
  const maxFds = handle.kernel.abi_max_fds();
  const findFdForOfd = (ofdId: number, preferredFd: number): number | null => {
    if (ofdId <= 0) {
      return null;
    }
    if (handle.kernel.process_fd_ofd_id(msg.vmPtr, preferredFd) === ofdId) {
      return preferredFd;
    }
    for (let fd = 0; fd < maxFds; fd += 1) {
      if (fd !== preferredFd && handle.kernel.process_fd_ofd_id(msg.vmPtr, fd) === ofdId) {
        return fd;
      }
    }
    return null;
  };
  for (const [fd, connection] of [...handle.networkConnections]) {
    const fdEntry = msg.fdEntries.find((entry) => entry.fd === fd);
    const expectedOfd = ofds.get(fd);
    const currentOfd = handle.kernel.process_fd_ofd_id(msg.vmPtr, fd);
    if (expectedOfd === undefined) {
      if (currentOfd > 0) {
        ofds.set(fd, currentOfd);
        continue;
      }
    } else if (currentOfd === expectedOfd) {
      continue;
    } else {
      const replacementFd = findFdForOfd(expectedOfd, fd);
      if (replacementFd !== null) {
        if (replacementFd !== fd) {
          handle.networkConnections.delete(fd);
          handle.networkConnections.set(replacementFd, connection);
          ofds.delete(fd);
          ofds.set(replacementFd, expectedOfd);
          moveNetworkRecvWatermarks(handle, fd, replacementFd);
          deps.appendWorkerDebugLog(handle, `net-status-rekey:${handle.pid}:fd=${fd}->${replacementFd}:ofd=${expectedOfd}`);
        }
        continue;
      }
      if (
        currentOfd > 0
        && handle.kernel.kernel_ofd_fd_type(currentOfd) === handle.kernel.abi_fd_type_socket()
        && !handle.networkPendingConnectFds.has(fd)
      ) {
        ofds.set(fd, currentOfd);
        deps.appendWorkerDebugLog(handle, `net-status-rekey:${handle.pid}:fd=${fd}:ofd=${expectedOfd}->${currentOfd}`);
        continue;
      }
    }
    if (
      !handle.networkClosedFds.has(fd)
      && !(fdEntry?.ofdId === 0 && currentOfd === 0)
    ) {
      deps.appendWorkerDebugLog(handle, `net-status-keep:${handle.pid}:fd=${fd}:ofd=${expectedOfd ?? "unknown"}->${currentOfd}`);
      continue;
    }
    handle.networkConnections.delete(fd);
    ofds.delete(fd);
    handle.networkPendingConnectFds.delete(fd);
    clearNetworkRecvWatermarks(handle, fd);
    deps.appendWorkerDebugLog(handle, `net-status-close:${handle.pid}:fd=${fd}:ofd=${expectedOfd ?? "unknown"}->${currentOfd}`);
    void Promise.resolve(connection.close?.()).catch((error) => {
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    });
  }
}

function formatStatusFdEntries(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  limit = 8,
): string {
  if (!msg.fdEntries || msg.fdEntries.length === 0) {
    return "none";
  }
  const maxFds = handle.kernel.abi_max_fds();
  return msg.fdEntries
    .filter((entry) => entry.fd >= 0 && entry.fd < limit && entry.ofdId !== 0)
    .map((entry) => {
      const hasValidOfd = entry.ofdId > 0 && entry.ofdId < maxFds;
      const type = hasValidOfd ? handle.kernel.kernel_ofd_fd_type(entry.ofdId) : -1;
      const ino = hasValidOfd ? handle.kernel.kernel_ofd_ino(entry.ofdId) : -1;
      const hostFd = hasValidOfd ? handle.kernel.kernel_ofd_host_fd(entry.ofdId) : -1;
      const offset = hasValidOfd && handle.kernel.kernel_ofd_offset
        ? handle.kernel.kernel_ofd_offset(entry.ofdId)
        : -1n;
      return `${entry.fd}->${entry.ofdId}/${entry.fdFlags}/${type}/${ino}/${hostFd}/${offset.toString()}`;
    })
    .join(" ");
}

function shouldRecordStatusFdLifecycle(
  handle: ProcessHandle,
  status: number,
  msg: ThreadWorkerStatusMessage,
): boolean {
  if (!msg.fdEntries || msg.fdEntries.length === 0) {
    return false;
  }
  if (status === handle.kernel.abi_status_ok()) {
    return false;
  }
  if (
    status === handle.kernel.abi_status_pipe_wait()
    || status === handle.kernel.abi_status_wait4()
    || handle.stdioOwner !== null
  ) {
    return true;
  }
  return msg.fdEntries.some((entry) => entry.fd >= 4 && entry.ofdId !== 0);
}

function formatLifecycleFdTrace(trace: string, maxLength = 360): string {
  return trace.length <= maxLength
    ? trace
    : `${trace.slice(0, maxLength)}...`;
}

function ofdSnapshotWouldRollBackRegularFileOffset(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  snapshot: NonNullable<ThreadWorkerStatusMessage["ofdSlots"]>[number],
  slotBytes: number,
): boolean {
  const fileType = handle.kernel.abi_fd_type_file?.();
  if (fileType === undefined) {
    return false;
  }
  const sourceView = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
  const sourceType = sourceView.getInt32(0, true);
  if (sourceType !== fileType) {
    return false;
  }
  const targetType = handle.kernel.kernel_ofd_fd_type(snapshot.ofdId);
  if (targetType !== fileType) {
    return false;
  }
  const targetOffset = handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes;
  const targetView = new DataView(handle.memory.buffer, targetOffset, slotBytes);
  const targetRefcount = targetView.getInt32(12, true);
  if (targetRefcount <= 0) {
    return false;
  }
  const sourceHostFd = sourceView.getInt32(4, true);
  const targetHostFd = targetView.getInt32(4, true);
  const sourceIno = sourceView.getUint32(24, true);
  const targetIno = targetView.getUint32(24, true);
  if (sourceHostFd !== targetHostFd || sourceIno !== targetIno) {
    return false;
  }
  const sourceOffset = sourceView.getBigInt64(16, true);
  const targetCurrentOffset = targetView.getBigInt64(16, true);
  let maxOffsets = maxRegularFileOffsetByHandle.get(handle);
  if (maxOffsets === undefined) {
    maxOffsets = new Map();
    maxRegularFileOffsetByHandle.set(handle, maxOffsets);
  }
  const maxSeenOffset = maxOffsets.get(snapshot.ofdId) ?? targetCurrentOffset;
  const nextMaxOffset = sourceOffset > targetCurrentOffset ? sourceOffset : targetCurrentOffset;
  if (nextMaxOffset > maxSeenOffset) {
    maxOffsets.set(snapshot.ofdId, nextMaxOffset);
  }
  if (sourceOffset < maxSeenOffset) {
    return true;
  }
  if (sourceOffset >= targetCurrentOffset) {
    return false;
  }
  const syscallNr = msg.syscallNr;
  const lseekNr = handle.kernel.abi_sys_lseek?.();
  return syscallNr === undefined || syscallNr !== BigInt(lseekNr ?? 62);
}

function ofdSnapshotWouldClobberExistingSlot(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  snapshot: NonNullable<ThreadWorkerStatusMessage["ofdSlots"]>[number],
  slotBytes: number,
): boolean {
  const targetOffset = handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes;
  if (targetOffset < 0 || targetOffset + slotBytes > handle.memory.buffer.byteLength) {
    return true;
  }
  if ((msg.fdEntries ?? []).some((entry) => entry.ofdId === snapshot.ofdId)) {
    return false;
  }
  const targetView = new DataView(handle.memory.buffer, targetOffset, slotBytes);
  const targetType = targetView.getInt32(OFD_TYPE_OFFSET, true);
  if (targetType === 0 || targetView.getInt32(OFD_REFCOUNT_OFFSET, true) <= 0) {
    return false;
  }
  const sourceView = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
  const sourceType = sourceView.getInt32(OFD_TYPE_OFFSET, true);
  if (sourceType === 0 || sourceType !== targetType) {
    return true;
  }
  return sourceView.getInt32(OFD_HOST_FD_OFFSET, true) !== targetView.getInt32(OFD_HOST_FD_OFFSET, true)
    || sourceView.getUint32(OFD_INO_OFFSET, true) !== targetView.getUint32(OFD_INO_OFFSET, true);
}

function applyThreadOfdSlots(handle: ProcessHandle, msg: ThreadWorkerStatusMessage): void {
  restoreNetworkOfdSlotWatermarks(handle);
  if (!msg.ofdSlots || msg.ofdSlots.length === 0) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const protectedNetworkOfds = new Set(networkConnectionOfds(handle).values());
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
  }
  const fdByOfd = new Map<number, number[]>();
  for (const entry of msg.fdEntries ?? []) {
    if (entry.ofdId <= 0) {
      continue;
    }
    const fds = fdByOfd.get(entry.ofdId) ?? [];
    fds.push(entry.fd);
    fdByOfd.set(entry.ofdId, fds);
  }
  const memory = new Uint8Array(handle.memory.buffer);
  for (const snapshot of msg.ofdSlots) {
    if (
      snapshot.ofdId <= 0
      || snapshot.ofdId >= maxFds
      || snapshot.slot.length !== slotBytes
    ) {
      continue;
    }
    if (protectedNetworkOfds.has(snapshot.ofdId)) {
      continue;
    }
    if (ofdSnapshotWouldClobberExistingSlot(handle, msg, snapshot, slotBytes)) {
      continue;
    }
    if (ofdSnapshotWouldRollBackRegularFileOffset(handle, msg, snapshot, slotBytes)) {
      continue;
    }
    memory.set(snapshot.slot, handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes);
  }
  restoreNetworkOfdSlotWatermarks(handle);
  recordRegularFileOfdOffsets(handle, msg, slotBytes);
}

function recordRegularFileOfdOffsets(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  slotBytes: number,
): void {
  const fileType = handle.kernel.abi_fd_type_file?.();
  if (fileType === undefined || !msg.ofdSlots || msg.ofdSlots.length === 0) {
    return;
  }
  for (const snapshot of msg.ofdSlots) {
    if (
      snapshot.ofdId <= 0
      || snapshot.ofdId >= handle.kernel.abi_max_fds()
      || snapshot.slot.length !== slotBytes
    ) {
      continue;
    }
    const view = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
    if (view.getInt32(OFD_TYPE_OFFSET, true) !== fileType) {
      continue;
    }
    const offsets = handle.regularFileOfdOffsets ?? new Map<number, { ino: number; hostFd: number; offset: bigint }>();
    offsets.set(snapshot.ofdId, {
      ino: view.getUint32(OFD_INO_OFFSET, true),
      hostFd: view.getInt32(OFD_HOST_FD_OFFSET, true),
      offset: view.getBigInt64(OFD_OFFSET_OFFSET, true),
    });
    handle.regularFileOfdOffsets = offsets;
  }
}

function pipeSlotLifecycleDebugEnabled(): boolean {
  return ((globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_PIPE_SLOT_DEBUG ?? "") === "1";
}

function formatBytesAscii(bytes: Uint8Array, limit = 32): string {
  return [...bytes.subarray(0, limit)]
    .map((byte) => byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : ".")
    .join("");
}

function applyThreadPipeSlots(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  appendRuntimeLifecycleEvent?: (runtimeId: number, event: string) => void,
): void {
  if (!msg.pipeSlots || msg.pipeSlots.length === 0) {
    return;
  }
  const pipeRegionBytes = handle.kernel.kernel_pipe_region_bytes();
  const maxPipes = handle.kernel.abi_max_pipes();
  const slotBytes = pipeRegionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
  }
  const pipeBufferBytes = slotBytes - PIPE_HEADER_BYTES;
  const syncPipeOfdGenerationsForSlot = (pipeSlot: number): void => {
    const pipeGeneration = new DataView(handle.memory.buffer).getUint32(
      handle.kernelState.pipeBase + pipeSlot * slotBytes + PIPE_GENERATION_OFFSET,
      true,
    );
    if (pipeGeneration === 0) {
      return;
    }
    const maxFds = handle.kernel.abi_max_fds();
    const tableBytes = handle.kernel.kernel_ofd_table_bytes();
    const ofdSlotBytes = tableBytes / maxFds;
    if (!Number.isInteger(ofdSlotBytes) || ofdSlotBytes <= OFD_INO_OFFSET + 4) {
      return;
    }
    const view = new DataView(handle.memory.buffer);
    for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
      const ofdOffset = handle.kernelState.ofdTablePtr + ofdId * ofdSlotBytes;
      if (
        view.getInt32(ofdOffset + OFD_TYPE_OFFSET, true) === handle.kernel.abi_fd_type_pipe()
        && view.getUint32(ofdOffset + OFD_INO_OFFSET, true) === pipeSlot
      ) {
        view.setBigInt64(ofdOffset + OFD_OFFSET_OFFSET, BigInt(pipeGeneration), true);
      }
    }
  };
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
  const appliedSnapshots: NonNullable<ThreadWorkerStatusMessage["pipeSlots"]> = [];
  for (const snapshot of msg.pipeSlots) {
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
    if (pipeSlotLifecycleDebugEnabled()) {
      const data = snapshot.slot.subarray(PIPE_HEADER_BYTES, PIPE_HEADER_BYTES + 24);
      const ascii = [...data]
        .map((byte) => byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : ".")
        .join("");
      appendRuntimeLifecycleEvent?.(
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
        memory.set(snapshot.slot, targetOffset);
        appliedSnapshots.push({
          pipeSlot: snapshot.pipeSlot,
          slot: memory.slice(targetOffset, targetOffset + slotBytes),
          mode,
          fresh: snapshot.fresh,
        });
        syncPipeOfdGenerationsForSlot(snapshot.pipeSlot);
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
      memory.set(snapshot.slot, targetOffset);
      appliedSnapshots.push({
        pipeSlot: snapshot.pipeSlot,
        slot: memory.slice(targetOffset, targetOffset + slotBytes),
        mode,
        fresh: snapshot.fresh,
      });
      syncPipeOfdGenerationsForSlot(snapshot.pipeSlot);
      continue;
    }
    if (
      targetReadOpen <= 0
      && targetWriteOpen <= 0
      && targetHead === targetTail
      && (sourceReadOpen > 0 || sourceWriteOpen > 0)
      && ((mode & (PIPE_SLOT_MODE_WRITE | PIPE_SLOT_MODE_CONTROL)) !== 0)
    ) {
      memory.set(snapshot.slot, targetOffset);
      appliedSnapshots.push({
        pipeSlot: snapshot.pipeSlot,
        slot: memory.slice(targetOffset, targetOffset + slotBytes),
        mode,
        fresh: snapshot.fresh,
      });
      syncPipeOfdGenerationsForSlot(snapshot.pipeSlot);
      continue;
    }
    if ((mode & PIPE_SLOT_MODE_CONTROL) !== 0) {
      view.setInt32(targetOffset + PIPE_READ_OPEN_OFFSET, sourceReadOpen, true);
      view.setInt32(targetOffset + PIPE_WRITE_OPEN_OFFSET, sourceWriteOpen, true);
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
      memory.set(
        snapshot.slot.subarray(PIPE_HEADER_BYTES, slotBytes),
        targetOffset + PIPE_HEADER_BYTES,
      );
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
    syncPipeOfdGenerationsForSlot(snapshot.pipeSlot);
  }
  handle.pendingPipeSlotSnapshots.push(...appliedSnapshots);
}

function applyThreadSocketState(handle: ProcessHandle, msg: ThreadWorkerStatusMessage): void {
  if (!msg.socketState) {
    return;
  }
  const currentSocketUsed = handle.kernelState.socketUsed | handle.kernel.kernel_socket_used();
  const currentNetBufUsed = handle.kernelState.netBufUsed
    | handle.kernel.kernel_net_buf_used()
    | deriveNetBufUsedFromSocketSlots(handle, currentSocketUsed);
  const debugFds = networkAuthoritativeFds(handle);
  const socketDebugEntry = (fd: number, source: string) => {
    const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(msg.vmPtr, fd) ?? -1;
    const state = socketIdx >= 0 ? handle.kernel.thread_socket_state?.(socketIdx) ?? -1 : -1;
    const type = socketIdx >= 0 ? handle.kernel.thread_socket_type?.(socketIdx) ?? -1 : -1;
    const ext = socketIdx >= 0 ? handle.kernel.thread_socket_external_peer?.(socketIdx) ?? -1 : -1;
    const buf = socketIdx >= 0 ? handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1 : -1;
    const recv = socketIdx >= 0 ? handle.kernel.thread_socket_recv_available?.(socketIdx) ?? -1 : -1;
    recordStatusSocketMetaTransition(handle, source, fd, socketIdx, state, type, ext, buf, recv);
    return {
      fd,
      sock: socketIdx,
      state,
      type,
      ext,
      buf,
      recv,
      ht: readNetworkRecvBufferDebug(handle, socketIdx),
      avail: handle.kernel.thread_socket_outbound_available?.(msg.vmPtr, fd) ?? -1,
    };
  };
  const before = STATUS_SOCKET_DEBUG_LOG_ENABLED && debugFds.size > 0
    ? [...debugFds].map((fd) => socketDebugEntry(fd, "status-before"))
    : [];
  const pendingOutboundFds = [...debugFds].filter((fd) =>
    (handle.kernel.thread_socket_outbound_available?.(msg.vmPtr, fd) ?? 0) > 0
  );
  if (pendingOutboundFds.length > 0) {
    appendStatusSocketDebug(
      handle,
      `socket-state-skip:${handle.pid}:reason=pending-outbound:fds=${pendingOutboundFds.join(".")}:before=${JSON.stringify(before)}`,
    );
    return;
  }
  const socketTableBytes = handle.kernel.kernel_socket_table_bytes();
  const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes();
  if (
    msg.socketState.socketTable.length !== socketTableBytes
    || msg.socketState.netBufPool.length !== netBufPoolBytes
  ) {
    if (debugFds.size > 0) {
      appendStatusSocketDebug(handle, `socket-state-skip:${handle.pid}:reason=size:socket=${msg.socketState.socketTable.length}/${socketTableBytes}:net=${msg.socketState.netBufPool.length}/${netBufPoolBytes}`);
    }
    return;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  const preservedSocketSlots = preserveNetworkSocketSlots(handle, msg.vmPtr, socketTableBytes);
  const preservedSocketUsed = preservedSocketSlots.reduce((mask, slot) => mask | slot.socketMask, 0);
  const preservedSocketRecvBuffers = [
    ...preservedSocketRecvBufferIndices(preservedSocketSlots),
    ...closedSocketRecvBufferIndices(handle),
  ];
  const preservedSocketNetBufUsed = netBufUsedMaskFromBufferIndices(preservedSocketRecvBuffers);
  const preservedRecvBuffers = preserveNetworkRecvBuffers(handle, msg.vmPtr, netBufPoolBytes);
  const preservedNetBufferHeads = preserveUsedNetBufferHeads(
    handle,
    netBufPoolBytes,
    currentNetBufUsed | msg.socketState.netBufUsed | preservedSocketNetBufUsed,
    preservedSocketRecvBuffers,
  ).concat(preserveClosedNetworkRecvBuffers(handle, netBufPoolBytes));
  memory.set(msg.socketState.socketTable, handle.kernelState.socketTablePtr);
  memory.set(msg.socketState.netBufPool, handle.kernelState.netBufBase);
  restoreNetworkSocketSlots(memory, socketTablePtrs(handle), preservedSocketSlots);
  const mergedSocketUsed = msg.socketState.socketUsed | currentSocketUsed | preservedSocketUsed;
  const derivedNetBufUsed = deriveNetBufUsedFromSocketSlots(handle, mergedSocketUsed);
  restoreNetworkRecvBufferProducers(memory, handle.kernelState.netBufBase, preservedRecvBuffers);
  restoreMergedNetBufferHeads(memory, handle.kernelState.netBufBase, preservedNetBufferHeads);
  closeNetworkClosedFdPeers(handle, msg.vmPtr);
  handle.kernelState = {
    ...handle.kernelState,
    socketUsed: mergedSocketUsed,
    netBufUsed: msg.socketState.netBufUsed | currentNetBufUsed | derivedNetBufUsed | preservedSocketNetBufUsed,
  };
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  if (debugFds.size > 0) {
    const after = [...debugFds].map((fd) => socketDebugEntry(fd, "status-after"));
    appendStatusSocketDebug(handle, `socket-state-apply:${handle.pid}:socketUsed=0x${msg.socketState.socketUsed.toString(16)}:netBufUsed=0x${msg.socketState.netBufUsed.toString(16)}:derivedNetBufUsed=0x${derivedNetBufUsed.toString(16)}:before=${JSON.stringify(before)}:after=${JSON.stringify(after)}`);
  }
}

function deriveNetBufUsedFromSocketSlots(handle: ProcessHandle, socketUsed: number): bigint {
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  let netBufUsed = 0n;
  for (let socketIdx = 0; socketIdx < maxSockets; socketIdx += 1) {
    if ((socketUsed & (1 << socketIdx)) === 0) {
      continue;
    }
    const recvBufIdx = handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1;
    if (recvBufIdx >= 0 && recvBufIdx < netBufferCount) {
      netBufUsed |= 1n << BigInt(recvBufIdx);
    }
  }
  return netBufUsed;
}

function networkAuthoritativeFds(handle: ProcessHandle): Set<number> {
  return new Set([
    ...(handle.networkConnections?.keys() ?? []),
    ...(handle.networkPendingConnectFds ?? []),
    ...(handle.networkClosedFds ?? []),
    ...(handle.networkClosedSocketSlots?.keys() ?? []),
    ...(handle.networkSocketSlotWatermarks?.keys() ?? []),
    ...(handle.networkRecvPayloadWatermarks?.keys() ?? []),
  ]);
}

function closeNetworkClosedFdPeers(handle: ProcessHandle, vmPtr: number): void {
  forceNetworkClosedFds(handle, vmPtr);
}

function counterAhead(sourceValue: number, targetValue: number, capacity: number): boolean {
  if (sourceValue === targetValue) {
    return false;
  }
  if (!Number.isFinite(capacity) || capacity <= 0) {
    return sourceValue > targetValue;
  }
  return sourceValue > targetValue
    || (
      sourceValue < targetValue
      && targetValue > capacity / 2
      && sourceValue < capacity / 2
    );
}

function rememberNetworkRecvHead(handle: ProcessHandle, fd: number, head: number, capacity: number): number {
  const previous = handle.networkRecvHeadWatermarks.get(fd) ?? 0;
  const next = counterAhead(head, previous, capacity) ? head : previous;
  handle.networkRecvHeadWatermarks.set(fd, next);
  return next;
}

function rememberNetworkRecvTail(handle: ProcessHandle, fd: number, tail: number, capacity: number): number {
  const previous = handle.networkRecvTailWatermarks.get(fd) ?? 0;
  const next = counterAhead(tail, previous, capacity) ? tail : previous;
  handle.networkRecvTailWatermarks.set(fd, next);
  return next;
}

function socketReadHeadFloor(
  handle: ProcessHandle,
  socketIdx: number,
  tail: number,
  capacity?: number,
): number {
  const readBytes = handle.kernel.thread_socket_read_bytes?.(socketIdx);
  if (readBytes === undefined) {
    return 0;
  }
  const numeric = typeof readBytes === "bigint" ? Number(readBytes) : readBytes;
  if (!Number.isSafeInteger(numeric) || numeric <= 0) {
    return 0;
  }
  if (capacity !== undefined && Number.isFinite(capacity) && capacity > 0) {
    return numeric % capacity;
  }
  return Math.min(numeric, tail);
}

function networkRecvPayloadWatermark(
  handle: ProcessHandle,
  fd: number,
  socketIdx: number,
  currentTail: number,
  capacity: number,
): { head: number; tail: number; data: Uint8Array } | null {
  handle.networkRecvPayloadWatermarks ??= new Map();
  const payload = handle.networkRecvPayloadWatermarks.get(fd);
  if (payload === undefined || payload.capacity !== capacity) {
    return null;
  }
  if (payload.readBytes !== undefined && payload.availableBytes !== undefined && socketIdx >= 0) {
    const readBytesRaw = handle.kernel.thread_socket_read_bytes?.(socketIdx);
    const readBytes = typeof readBytesRaw === "bigint" ? Number(readBytesRaw) : readBytesRaw;
    const availableBytes = handle.kernel.thread_socket_recv_available?.(socketIdx);
    if (
      Number.isSafeInteger(readBytes)
      && Number.isSafeInteger(availableBytes)
      && availableBytes !== undefined
      && availableBytes >= 0
    ) {
      const payloadProgress = payload.deliveredBytes ?? (payload.readBytes + payload.availableBytes);
      const currentProgress = (readBytes as number) + availableBytes;
      const readProgress = (readBytes as number) - payload.readBytes;
      const deliveredRemaining = payload.deliveredBytes === undefined
        ? undefined
        : payload.deliveredBytes - (readBytes as number);
      const payloadCoversReadProgress = deliveredRemaining === undefined
        ? readProgress >= 0 && readProgress < payload.availableBytes
        : readProgress >= 0
          && readProgress <= payload.availableBytes
          && deliveredRemaining >= 0
          && deliveredRemaining <= payload.availableBytes - readProgress;
      if (payloadCoversReadProgress) {
        const restoredHead = readProgress > 0
          ? (payload.head + readProgress) % payload.capacity
          : payload.head;
        const restoredTail = deliveredRemaining === undefined
          ? payload.tail
          : (restoredHead + deliveredRemaining) % payload.capacity;
        appendStatusSocketDebug(
          handle,
          `net-payload-watermark-use-progress:${handle.pid}:fd=${fd}:source=status:payload=${payloadProgress}:current=${currentProgress}:phead=${payload.head}:rhead=${restoredHead}:ptail=${payload.tail}:rtail=${restoredTail}:ctail=${currentTail}`,
        );
        return {
          head: restoredHead,
          tail: restoredTail,
          data: payload.data,
        };
      }
      if (currentProgress > payloadProgress) {
        appendStatusSocketDebug(
          handle,
          `net-payload-watermark-skip-progress:${handle.pid}:fd=${fd}:source=status:payload=${payloadProgress}:current=${currentProgress}:ptail=${payload.tail}:ctail=${currentTail}`,
        );
        return null;
      }
      return null;
    }
  }
  const payloadTailAhead = (() => {
    if (payload.tail === currentTail) {
      return true;
    }
    if (!Number.isFinite(capacity) || capacity <= 0) {
      return payload.tail > currentTail;
    }
    const distance = (payload.tail - currentTail + capacity) % capacity;
    return distance > 0 && distance < capacity / 2;
  })();
  if (payloadTailAhead) {
    appendStatusSocketDebug(
      handle,
      `net-payload-watermark-use:${handle.pid}:fd=${fd}:source=status:ptail=${payload.tail}:ctail=${currentTail}:cap=${capacity}`,
    );
    return {
      head: payload.head,
      tail: payload.tail,
      data: payload.data,
    };
  }
  return null;
}

function mergeSocketReadHeadFloor(
  handle: ProcessHandle,
  socketIdx: number,
  head: number,
  tail: number,
  capacity: number,
): number {
  const available = handle.kernel.thread_socket_recv_available?.(socketIdx);
  if (available === 0 && head === tail) {
    return head;
  }
  const floor = socketReadHeadFloor(handle, socketIdx, tail, capacity);
  return counterAhead(floor, head, capacity) ? floor : head;
}

function preserveNetworkSocketSlots(
  handle: ProcessHandle,
  vmPtr: number,
  socketTableBytes: number,
): PreservedNetworkSocketSlot[] {
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const socketSlotBytes = maxSockets > 0 ? socketTableBytes / maxSockets : 0;
  if (!Number.isInteger(socketSlotBytes) || socketSlotBytes <= 0) {
    return [];
  }
  const preserved: PreservedNetworkSocketSlot[] = [];
  const preservedOffsets = new Set<number>();
  const memory = new Uint8Array(handle.memory.buffer);
  const preserveSocketIdx = (socketIdx: number): void => {
    if (socketIdx < 0 || socketIdx >= maxSockets) {
      return;
    }
    const offset = socketIdx * socketSlotBytes;
    if (preservedOffsets.has(offset)) {
      return;
    }
    const absolute = activeSocketTablePtr(handle) + offset;
    preservedOffsets.add(offset);
    preserved.push({
      offset,
      data: memory.slice(absolute, absolute + socketSlotBytes),
      socketMask: 1 << socketIdx,
    });
  };
  for (const fd of networkAuthoritativeFds(handle)) {
    const closedSlot = handle.networkClosedSocketSlots.get(fd);
    if (closedSlot) {
      if (!preservedOffsets.has(closedSlot.offset)) {
        preservedOffsets.add(closedSlot.offset);
        preserved.push({
          offset: closedSlot.offset,
          data: closedSlot.data,
          socketMask: 1 << Math.floor(closedSlot.offset / socketSlotBytes),
        });
      }
      continue;
    }
    handle.networkSocketSlotWatermarks ??= new Map();
    const watermarkedSlot = handle.networkSocketSlotWatermarks.get(fd);
    if (watermarkedSlot && !preservedOffsets.has(watermarkedSlot.offset)) {
      preservedOffsets.add(watermarkedSlot.offset);
      preserved.push({
        offset: watermarkedSlot.offset,
        data: watermarkedSlot.data,
        socketMask: 1 << Math.floor(watermarkedSlot.offset / socketSlotBytes),
      });
    }
    const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd);
    if (socketIdx === undefined || socketIdx < 0 || socketIdx >= maxSockets) {
      handle.networkClosedFds.delete(fd);
      continue;
    }
    preserveSocketIdx(socketIdx);
    preserveSocketIdx(handle.kernel.thread_socket_peer_idx?.(socketIdx) ?? -1);
  }
  return preserved;
}

function restoreNetworkSocketSlots(
  memory: Uint8Array,
  socketTablePtrs: number[],
  preserved: PreservedNetworkSocketSlot[],
): void {
  for (const slot of preserved) {
    for (const socketTablePtr of socketTablePtrs) {
      memory.set(slot.data, socketTablePtr + slot.offset);
    }
  }
}

function preserveNetworkRecvBuffers(
  handle: ProcessHandle,
  vmPtr: number,
  netBufPoolBytes: number,
): PreservedNetworkRecvBuffer[] {
  const authoritativeFds = networkAuthoritativeFds(handle);
  if (authoritativeFds.size === 0) {
    return [];
  }
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return [];
  }
  const preserved: PreservedNetworkRecvBuffer[] = [];
  const preservedOffsets = new Set<number>();
  const memory = new Uint8Array(handle.memory.buffer);
  const preserveWatermarkedRecvBuffer = (fd: number): boolean => {
    handle.networkRecvPayloadWatermarks ??= new Map();
    const payload = handle.networkRecvPayloadWatermarks.get(fd);
    if (payload === undefined || payload.recvBufIdx < 0 || payload.recvBufIdx >= netBufferCount) {
      return false;
    }
    const offset = payload.recvBufIdx * netBufferBytes;
    if (preservedOffsets.has(offset)) {
      return true;
    }
    const absolute = handle.kernelState.netBufBase + offset;
    const view = new DataView(memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
    const currentHead = view.getUint32(NET_BUFFER_HEAD_OFFSET, true);
    const currentTail = view.getUint32(NET_BUFFER_TAIL_OFFSET, true);
    handle.networkRecvHeadWatermarks.set(fd, payload.head);
    const watermarkHead = payload.head;
    handle.networkRecvTailWatermarks.set(fd, payload.tail);
    const watermarkTail = payload.tail;
    preservedOffsets.add(offset);
    preserved.push({
      offset,
      head: watermarkHead,
      tail: watermarkTail,
      data: payload.data,
      capacity: payload.capacity,
      preserveProducer: true,
      authoritativeData: true,
      forceHead: true,
      forceTail: true,
    });
    return true;
  };
  const preserveSocketRecvBuffer = (socketIdx: number, fd: number | null): void => {
    const recvBufIdx = handle.kernel.thread_socket_recv_buf_idx?.(socketIdx);
    if (recvBufIdx === undefined || recvBufIdx < 0 || recvBufIdx >= netBufferCount) {
      if (fd !== null) {
        preserveWatermarkedRecvBuffer(fd);
      }
      return;
    }
    const offset = recvBufIdx * netBufferBytes;
    if (preservedOffsets.has(offset)) {
      return;
    }
    const absolute = handle.kernelState.netBufBase + offset;
    const bytes = memory.slice(absolute, absolute + netBufferBytes);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const currentHead = view.getUint32(NET_BUFFER_HEAD_OFFSET, true);
    const currentTail = view.getUint32(NET_BUFFER_TAIL_OFFSET, true);
    const capacity = netBufferBytes - NET_BUFFER_HEADER_BYTES;
    const payload = fd === null ? null : networkRecvPayloadWatermark(handle, fd, socketIdx, currentTail, capacity);
    const terminalRecv = fd !== null
      && handle.kernel.thread_socket_state?.(socketIdx) === SOCK_STATE_SHUTDOWN
      && (handle.kernel.thread_socket_external_peer?.(socketIdx) ?? 0) === 0;
    const preservedTail = payload?.tail ?? currentTail;
    const mergedHead = terminalRecv
      ? currentHead
      : payload?.head ?? mergeSocketReadHeadFloor(
        handle,
        socketIdx,
        currentHead,
        currentTail,
        capacity,
      );
    const watermarkHead = fd === null ? mergedHead : mergedHead;
    if (fd !== null) {
      handle.networkRecvHeadWatermarks.set(fd, watermarkHead);
    }
    const watermarkTail = (() => {
      if (fd === null) {
        return currentTail;
      }
      if (payload !== null) {
        handle.networkRecvTailWatermarks.set(fd, preservedTail);
        return preservedTail;
      }
      if (terminalRecv) {
        handle.networkRecvTailWatermarks.set(fd, watermarkHead);
        return watermarkHead;
      }
      return rememberNetworkRecvTail(handle, fd, preservedTail, capacity);
    })();
    preservedOffsets.add(offset);
    preserved.push({
      offset,
      head: watermarkHead,
      tail: watermarkTail,
      data: payload?.data ?? bytes.slice(NET_BUFFER_HEADER_BYTES),
      capacity,
      preserveProducer: fd !== null,
      authoritativeData: payload !== null,
      forceHead: fd !== null,
      forceTail: payload !== null || terminalRecv,
    });
  };
  for (const fd of authoritativeFds) {
    const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, fd);
    if (socketIdx === undefined || socketIdx < 0) {
      if (!preserveWatermarkedRecvBuffer(fd)) {
        handle.networkClosedFds.delete(fd);
      }
      continue;
    }
    preserveSocketRecvBuffer(socketIdx, fd);
    const peerIdx = handle.kernel.thread_socket_peer_idx?.(socketIdx) ?? -1;
    if (peerIdx >= 0) {
      preserveSocketRecvBuffer(peerIdx, null);
    }
  }
  return preserved;
}

function preserveUsedNetBufferHeads(
  handle: ProcessHandle,
  netBufPoolBytes: number,
  netBufUsed: bigint,
  extraBufferIndices: Iterable<number> = [],
): PreservedNetBufferHead[] {
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return [];
  }
  const preserved: PreservedNetBufferHead[] = [];
  const memory = new Uint8Array(handle.memory.buffer);
  const bufferIndices = new Set<number>();
  const socketReadFloors = socketReadHeadFloorsByBuffer(handle, netBufPoolBytes);
  for (let bufferIndex = 0; bufferIndex < netBufferCount; bufferIndex += 1) {
    if ((netBufUsed & (1n << BigInt(bufferIndex))) === 0n) {
      continue;
    }
    bufferIndices.add(bufferIndex);
  }
  for (const bufferIndex of extraBufferIndices) {
    if (bufferIndex >= 0 && bufferIndex < netBufferCount) {
      bufferIndices.add(bufferIndex);
    }
  }
  for (const bufferIndex of bufferIndices) {
    const offset = bufferIndex * netBufferBytes;
    const absolute = handle.kernelState.netBufBase + offset;
    const view = new DataView(memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
    const currentHead = view.getUint32(NET_BUFFER_HEAD_OFFSET, true);
    const currentTail = view.getUint32(NET_BUFFER_TAIL_OFFSET, true);
    const socketReadFloor = socketReadFloors.get(bufferIndex) ?? 0;
    preserved.push({
      offset,
      head: counterAhead(socketReadFloor, currentHead, netBufferBytes - NET_BUFFER_HEADER_BYTES)
        ? socketReadFloor
        : currentHead,
      tail: currentTail,
      data: memory.slice(absolute + NET_BUFFER_HEADER_BYTES, absolute + netBufferBytes),
      capacity: netBufferBytes - NET_BUFFER_HEADER_BYTES,
    });
  }
  return preserved;
}

function socketReadHeadFloorsByBuffer(
  handle: ProcessHandle,
  netBufPoolBytes: number,
): Map<number, number> {
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  const floors = new Map<number, number>();
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return floors;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  for (let socketIdx = 0; socketIdx < maxSockets; socketIdx += 1) {
    const recvBufIdx = handle.kernel.thread_socket_recv_buf_idx?.(socketIdx) ?? -1;
    if (recvBufIdx < 0 || recvBufIdx >= netBufferCount) {
      continue;
    }
    const absolute = handle.kernelState.netBufBase + recvBufIdx * netBufferBytes;
    const view = new DataView(memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
    const tail = view.getUint32(NET_BUFFER_TAIL_OFFSET, true);
    const floor = socketReadHeadFloor(handle, socketIdx, tail, netBufferBytes - NET_BUFFER_HEADER_BYTES);
    const previous = floors.get(recvBufIdx) ?? 0;
    if (counterAhead(floor, previous, netBufferBytes - NET_BUFFER_HEADER_BYTES)) {
      floors.set(recvBufIdx, floor);
    }
  }
  return floors;
}

function preservedSocketRecvBufferIndices(preserved: PreservedNetworkSocketSlot[]): number[] {
  const indices: number[] = [];
  for (const slot of preserved) {
    const recvBufIdx = networkSocketSlotRecvBufferIndex(slot);
    if (recvBufIdx >= 0) {
      indices.push(recvBufIdx);
    }
  }
  return indices;
}

function closedSocketRecvBufferIndices(handle: ProcessHandle): number[] {
  const indices: number[] = [];
  for (const slot of handle.networkClosedSocketSlots.values()) {
    const recvBufIdx = networkSocketSlotRecvBufferIndex(slot);
    if (recvBufIdx >= 0) {
      indices.push(recvBufIdx);
    }
  }
  return indices;
}

function preserveClosedNetworkRecvBuffers(
  handle: ProcessHandle,
  netBufPoolBytes: number,
): PreservedNetBufferHead[] {
  const maxSockets = handle.kernel.abi_max_sockets?.() ?? 32;
  const netBufferCount = maxSockets * 2;
  const netBufferBytes = netBufferCount > 0 ? netBufPoolBytes / netBufferCount : 0;
  if (!Number.isInteger(netBufferBytes) || netBufferBytes <= NET_BUFFER_HEADER_BYTES) {
    return [];
  }
  const preserved: PreservedNetBufferHead[] = [];
  const memory = new Uint8Array(handle.memory.buffer);
  for (const recvBufIdx of closedSocketRecvBufferIndices(handle)) {
    if (recvBufIdx < 0 || recvBufIdx >= netBufferCount) {
      continue;
    }
    const offset = recvBufIdx * netBufferBytes;
    const absolute = handle.kernelState.netBufBase + offset;
    const view = new DataView(memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
    preserved.push({
      offset,
      head: view.getUint32(NET_BUFFER_HEAD_OFFSET, true),
      tail: view.getUint32(NET_BUFFER_TAIL_OFFSET, true),
      data: memory.slice(absolute + NET_BUFFER_HEADER_BYTES, absolute + netBufferBytes),
      capacity: netBufferBytes - NET_BUFFER_HEADER_BYTES,
      forceHead: true,
      forceTail: true,
    });
  }
  return preserved;
}

function netBufUsedMaskFromBufferIndices(indices: Iterable<number>): bigint {
  let mask = 0n;
  for (const index of indices) {
    if (index >= 0) {
      mask |= 1n << BigInt(index);
    }
  }
  return mask;
}

function restoreNetworkRecvBufferProducers(
  memory: Uint8Array,
  netBufBase: number,
  preserved: PreservedNetworkRecvBuffer[],
): void {
  for (const buffer of preserved) {
    const absolute = netBufBase + buffer.offset;
    const view = new DataView(memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
    const incomingHead = view.getUint32(NET_BUFFER_HEAD_OFFSET, true);
    const incomingTail = view.getUint32(NET_BUFFER_TAIL_OFFSET, true);
    const mergedHead = buffer.forceHead === true
      ? buffer.head
      : counterAhead(incomingHead, buffer.head, buffer.capacity)
      ? incomingHead
      : buffer.head;
    const tailDistanceFromHead = (tail: number): number => (
      tail - mergedHead + buffer.capacity
    ) % buffer.capacity;
    const preservedTailAhead = buffer.forceTail === true
      ? true
      : buffer.authoritativeData
      ? tailDistanceFromHead(buffer.tail) > tailDistanceFromHead(incomingTail)
      : counterAhead(buffer.tail, incomingTail, buffer.capacity);
    if (
      preservedTailAhead
      || (buffer.authoritativeData && buffer.tail === incomingTail)
      || (buffer.forceHead === true && incomingHead !== buffer.head)
    ) {
      memory.set(buffer.data, absolute + NET_BUFFER_HEADER_BYTES);
    }
    view.setUint32(NET_BUFFER_HEAD_OFFSET, mergedHead, true);
    view.setUint32(
      NET_BUFFER_TAIL_OFFSET,
      buffer.forceTail === true || preservedTailAhead ? buffer.tail : incomingTail,
      true,
    );
  }
}

function restoreMergedNetBufferHeads(
  memory: Uint8Array,
  netBufBase: number,
  preserved: PreservedNetBufferHead[],
): void {
  for (const buffer of preserved) {
    const absolute = netBufBase + buffer.offset;
    const view = new DataView(memory.buffer, absolute, NET_BUFFER_HEADER_BYTES);
    const incomingHead = view.getUint32(NET_BUFFER_HEAD_OFFSET, true);
    if (buffer.forceHead === true) {
      if (incomingHead !== buffer.head && buffer.data !== undefined) {
        memory.set(buffer.data, absolute + NET_BUFFER_HEADER_BYTES);
      }
      view.setUint32(NET_BUFFER_HEAD_OFFSET, buffer.head, true);
    } else if (counterAhead(buffer.head, incomingHead, buffer.capacity)) {
      view.setUint32(NET_BUFFER_HEAD_OFFSET, buffer.head, true);
    }
    if (buffer.tail !== undefined) {
      const incomingTail = view.getUint32(NET_BUFFER_TAIL_OFFSET, true);
      const incomingTailPlausible = incomingTail < buffer.capacity
        && buffer.head < buffer.capacity;
      if (!incomingTailPlausible) {
        if (buffer.data !== undefined) {
          memory.set(buffer.data, absolute + NET_BUFFER_HEADER_BYTES);
        }
        view.setUint32(NET_BUFFER_HEAD_OFFSET, buffer.head, true);
        view.setUint32(NET_BUFFER_TAIL_OFFSET, buffer.tail, true);
        continue;
      }
      const tailDistanceFromHead = (tail: number): number => (
        tail - buffer.head + buffer.capacity
      ) % buffer.capacity;
      const preservedTailAhead = buffer.forceTail === true
        ? true
        : buffer.restoreData
        ? tailDistanceFromHead(buffer.tail) > tailDistanceFromHead(incomingTail)
        : counterAhead(buffer.tail, incomingTail, buffer.capacity);
      if (buffer.forceTail === true || preservedTailAhead) {
        if (buffer.restoreData && buffer.data !== undefined) {
          memory.set(buffer.data, absolute + NET_BUFFER_HEADER_BYTES);
        }
        view.setUint32(NET_BUFFER_TAIL_OFFSET, buffer.tail, true);
      }
    }
  }
}

function incrementBoundedCounter(
  counters: Record<string, number>,
  key: string,
  maxEntries = 32,
): void {
  const existing = counters[key];
  if (existing !== undefined) {
    counters[key] = existing + 1;
    return;
  }
  const entries = Object.keys(counters);
  if (entries.length < maxEntries) {
    counters[key] = 1;
    return;
  }
  for (const entry of entries) {
    const next = (counters[entry] ?? 0) - 1;
    if (next <= 0) {
      delete counters[entry];
      continue;
    }
    counters[entry] = next;
  }
}

function hasSuspiciousLowStateDebug(debug: string | undefined): boolean {
  if (!debug) {
    return false;
  }
  return /(?:^|\s)pc=0x[0-9a-f]{1,3}(?:\s|$)/i.test(debug)
    || /(?:^|\s)sp=0x0(?:\s|$)/.test(debug)
    || /(?:^|\s)s11=0x0(?:\s|$)/.test(debug);
}

function readLocalProcessPid(handle: ProcessHandle, vmPtr: number): number | null {
  if (vmPtr === 0) {
    return null;
  }
  try {
    const pid = handle.kernel.process_current_pid(vmPtr);
    return Number.isFinite(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

export function createWorkerStatusMessageHandler(deps: WorkerMessageHandlerDeps) {
  const handleWaitInWorkerStatus = createWorkerWaitInWorkerStatusHandler(deps);
  const maybeHandleOwnedStateSync = createWorkerOwnedStateSyncHandler(deps);
  const handleStatusTransition = createWorkerStatusTransitionHandler(deps);

  return function handleWorkerStatusMessage(
    handle: ProcessHandle,
    worker: Worker,
    msg: ThreadWorkerStatusMessage,
  ): void {
    const thread = deps.threadWorkerStates.get(worker);
    if (!thread) {
      return;
    }
    if (msg.generation !== undefined && msg.generation !== thread.generation) {
      if (handle.runningWorker === worker && thread.runningGeneration === msg.generation) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
      }
      deps.appendWorkerDebugLog(
        handle,
        `status-stale-generation:0x${thread.vmPtr.toString(16)}`
          + `:msg=${msg.generation}:current=${thread.generation}:status=${msg.status}`,
      );
      return;
    }
    const status = msg.status;
    const processPidField = readProcessPidField(msg.debug);
    const localProcessPid = readLocalProcessPid(handle, msg.vmPtr);
    const transientProcessOwnerMismatch = processPidField !== null
      ? processPidField !== handle.pid
      : localProcessPid !== null && localProcessPid !== handle.pid;
    const activeVforkWorker = handle.vforkSnapshot !== null && handle.vforkWorker === worker;
    const statusBelongsToHandle = !transientProcessOwnerMismatch || activeVforkWorker;
    if ((thread.ownerAsyncStatus ?? null) !== null) {
      if (handle.runningWorker === worker && thread.runningGeneration === msg.generation) {
        handle.runningWorker = null;
        thread.runningGeneration = null;
      }
      if (statusBelongsToHandle) {
        applyThreadKernelMemoryWrites(
          handle,
          msg,
          deps.appendRuntimeLifecycleEvent,
          { skipFsRingWrites: msg.status === handle.kernel.abi_status_fs() },
        );
        applyThreadGuestMemoryWrites(handle, msg, deps.appendRuntimeLifecycleEvent);
        applyThreadOfdSlots(handle, msg);
        applyThreadFdEntries(handle, msg);
        pruneStaleNetworkConnectionsFromFdEntries(handle, msg, deps);
      }
      deps.appendWorkerDebugLog(
        handle,
        `status-owner-async:0x${thread.vmPtr.toString(16)}`
          + `:owner=${thread.ownerAsyncStatus}:status=${msg.status}`
          + `:ownerMatch=${statusBelongsToHandle ? 1 : 0}`,
      );
      return;
    }
    const previousWaitingStatus = thread.waitingInWorkerStatus;
    thread.waitingInWorkerStatus = null;
    if (status === handle.kernel.abi_status_execve_spawn()) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `status-execve-spawn-seen:${handle.pid}:0x${msg.vmPtr.toString(16)}`
          + `:owner=${thread.ownerPid}`
          + `:field=${processPidField ?? "n/a"}`
          + `:local=${localProcessPid ?? "n/a"}`
          + `:kind=${handle.kernel.thread_spawn_kind?.(msg.vmPtr) ?? -1}`
          + `:child=${handle.kernel.thread_spawn_child_pid?.(msg.vmPtr) ?? -1}`,
      );
    }
    if (statusBelongsToHandle) {
      applyThreadOfdSlots(handle, msg);
      applyThreadFdEntries(handle, msg);
      pruneStaleNetworkConnectionsFromFdEntries(handle, msg, deps);
    }
    if (STATUS_SOCKET_DEBUG_LOG_ENABLED && msg.fdEntries) {
      appendStatusSocketDebug(
        handle,
        `status-fds:${handle.pid}:0x${msg.vmPtr.toString(16)}:status=${status}:${formatStatusFdEntries(handle, msg)}`,
      );
    }
    if (shouldRecordStatusFdLifecycle(handle, status, msg)) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `status-fds:${handle.pid}:0x${msg.vmPtr.toString(16)}:status=${status}:${formatStatusFdEntries(handle, msg)}`,
      );
    }
    thread.statusCount += 1;
    thread.statusCounts[String(status)] = (thread.statusCounts[String(status)] ?? 0) + 1;
    thread.lastStatus = status;
    thread.lastStatusAtMs = Date.now();
    thread.lastStatusPc = msg.pc;
    thread.lastStatusSp = msg.sp ?? null;
    thread.lastStatusS11 = msg.s11 ?? null;
    if (msg.forkStackSnapshot) {
      thread.forkStackSnapshot = msg.forkStackSnapshot;
    }
    if (msg.processState) {
      thread.execveProcessState = msg.processState;
    }
    incrementBoundedCounter(thread.hotStatusPcs, `0x${msg.pc.toString(16)}`);
    const faultStatus = handle.kernel.abi_status_fault?.();
    const suspiciousLowStateDebug = hasSuspiciousLowStateDebug(msg.debug);
    deps.appendWorkerDebugLog(
      handle,
      `status-route:0x${thread.vmPtr.toString(16)}`
        + `:owner_pid=${thread.ownerPid}`
        + `:handle_pid=${handle.pid}`
        + `:generation=${msg.generation}`
        + `:status=${status}`,
    );
    if (!(status === faultStatus && suspiciousLowStateDebug && handle.lastDebugMessage)) {
      handle.lastDebugMessage = msg.debug;
    }
    deps.appendWorkerDebugLog(
      handle,
      `status:0x${thread.vmPtr.toString(16)}@${msg.status}${formatCompactDebugSuffix(msg.debug)}`,
    );
    if (
      status === handle.kernel.abi_status_ok()
      && (
        msg.pc < 0x1000n
        || (msg.sp ?? 0n) === 0n
        || (msg.s11 ?? 0n) === 0n
      )
    ) {
      deps.appendWorkerDebugLog(
        handle,
        `status_message_break:0x${thread.vmPtr.toString(16)}`
          + `:pc=0x${msg.pc.toString(16)}`
          + `:ra=0x${(msg.ra ?? 0n).toString(16)}`
          + `:sp=0x${(msg.sp ?? 0n).toString(16)}`
          + `:s11=0x${(msg.s11 ?? 0n).toString(16)}`,
      );
    }
    if (msg.debug && suspiciousLowStateDebug) {
      deps.appendWorkerDebugLog(
        handle,
        `status_debug:0x${thread.vmPtr.toString(16)}:${msg.debug}`,
      );
    }
    if (processPidField !== null && processPidField !== handle.pid) {
      deps.appendWorkerDebugLog(
        handle,
        `status_debug:0x${thread.vmPtr.toString(16)}:handle_pid=${handle.pid}:process_pid_field=${processPidField}`,
      );
    }
    const writeDebugSuffix = formatCompactDebugSuffix(msg.debug, WRITE_DEBUG_KEYS);
    if (writeDebugSuffix !== "") {
      deps.appendWorkerDebugLog(handle, `write:0x${thread.vmPtr.toString(16)}${writeDebugSuffix}`);
    }
    if (msg.fdTraceDebug) {
      deps.appendWorkerDebugLog(handle, `fdtrace:0x${thread.vmPtr.toString(16)}:${msg.fdTraceDebug}`);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `fdtrace:${handle.pid}:0x${thread.vmPtr.toString(16)}:${formatLifecycleFdTrace(msg.fdTraceDebug)}`,
      );
    }

    const isRunningWorker = handle.runningWorker === worker;
    const deferLocalPipeDrain = kernelWorkerOwnsPipeState(handle);
    const adoptPreSnapshots: ThreadRegisterSnapshot[] = [];
    for (const candidate of handle.workers) {
      const candidateThread = deps.threadWorkerStates.get(candidate);
      if (!candidateThread) {
        continue;
      }
      adoptPreSnapshots.push({
        vmPtr: candidateThread.vmPtr,
        pc: handle.kernel.thread_pc(candidateThread.vmPtr),
        sp: handle.kernel.thread_sp(candidateThread.vmPtr),
        s11: handle.kernel.thread_s11?.(candidateThread.vmPtr) ?? 0n,
      });
    }
    const shouldAdoptThreadKernelState = (
      status !== handle.kernel.abi_status_clone_spawn()
      && statusBelongsToHandle
    );
    if (shouldAdoptThreadKernelState) {
      handle.kernelState = adoptKernelStateForHandle(handle, msg.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      applyThreadKernelMemoryWrites(handle, msg, deps.appendRuntimeLifecycleEvent);
      applyThreadGuestMemoryWrites(handle, msg, deps.appendRuntimeLifecycleEvent);
      applyThreadPipeSlots(handle, msg, deps.appendRuntimeLifecycleEvent);
      applyThreadOfdSlots(handle, msg);
      applyThreadFdEntries(handle, msg);
      applyThreadSocketState(handle, msg);
      for (const pre of adoptPreSnapshots) {
        const postPc = handle.kernel.thread_pc(pre.vmPtr);
        const postSp = handle.kernel.thread_sp(pre.vmPtr);
        const postS11 = handle.kernel.thread_s11?.(pre.vmPtr) ?? 0n;
        const becameBroken = (
          pre.pc >= 0x1000n
          && pre.sp !== 0n
          && pre.s11 !== 0n
          && (postPc < 0x1000n || postSp === 0n || postS11 === 0n)
        );
        if (!becameBroken) {
          continue;
        }
        deps.appendWorkerDebugLog(
          handle,
          `status_adopt_break:src=0x${thread.vmPtr.toString(16)} target=0x${pre.vmPtr.toString(16)} pre_pc=0x${pre.pc.toString(16)} pre_sp=0x${pre.sp.toString(16)} pre_s11=0x${pre.s11.toString(16)} post_pc=0x${postPc.toString(16)} post_sp=0x${postSp.toString(16)} post_s11=0x${postS11.toString(16)} incoming_sig_slots=0x${msg.kernelState.sigSlotsPtr.toString(16)} incoming_process_pending=0x${msg.kernelState.processPendingPtr.toString(16)} incoming_process_runtime=0x${msg.kernelState.processRuntimeStatePtr.toString(16)}`,
        );
      }
    } else if (status === handle.kernel.abi_status_fs()) {
      applyThreadKernelMemoryWrites(handle, msg, deps.appendRuntimeLifecycleEvent);
      applyThreadGuestMemoryWrites(handle, msg, deps.appendRuntimeLifecycleEvent);
    }
    if (msg.waitInWorker) {
      handleWaitInWorkerStatus(handle, worker, thread, msg, isRunningWorker);
      return;
    }
    if (previousWaitingStatus === handle.kernel.abi_status_futex_wait()) {
      deps.appendWorkerDebugLog(
        handle,
        `resume:0x${thread.vmPtr.toString(16)}@${previousWaitingStatus}->${status}${formatCompactDebugSuffix(msg.debug)}`,
      );
    }
    deps.clearBlockedWorker(handle, worker);
    const wroteToPipe = flushStdin(handle);
    const drainedPipe = deferLocalPipeDrain || kernelWorkerOwnsPipeState(handle)
      ? false
      : drainProcessOutput(handle);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState);
    if (isRunningWorker) {
      handle.runningWorker = null;
      thread.runningGeneration = null;
    }

    let transitionStatus = status;
    let transitionMsg = msg;
    if (status === faultStatus && handle.kernel.thread_status) {
      const currentStatus = handle.kernel.thread_status(thread.vmPtr);
      const currentStatusHasLiveThread = processHasLiveThreads(handle.kernel, thread.vmPtr);
      if (
        currentStatus !== status
        && currentStatus !== handle.kernel.abi_status_fault()
        && currentStatusHasLiveThread
        && !activeVforkWorker
      ) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `status-stale-fault-current:${handle.pid}:0x${thread.vmPtr.toString(16)}:${status}->${currentStatus}`,
        );
        deps.appendWorkerDebugLog(
          handle,
          `status-stale-fault-current:0x${thread.vmPtr.toString(16)}:${status}->${currentStatus}`,
        );
        transitionStatus = currentStatus;
        transitionMsg = {
          ...msg,
          status: currentStatus,
        };
      }
    }

    const completedSyncEffects = (
      transitionStatus === handle.kernel.abi_status_ok()
      && transitionMsg.syncEffects === undefined
    )
      ? deps.statusOkCompletedSyncEffects
      : (transitionMsg.syncEffects ?? 0);
    const continueAfterOwnedStateSync = (): void => {
      handleStatusTransition({
        handle,
        worker,
        thread,
        msg: transitionMsg,
        status: transitionStatus,
        wroteToPipe,
        drainedPipe,
        completedSyncEffects,
      });
    };
    if (maybeHandleOwnedStateSync({
      handle,
      worker,
      thread,
      status: transitionStatus,
      syscallNr: transitionMsg.syscallNr ?? null,
      rawSyncEffects: transitionMsg.syncEffects,
      completedSyncEffects,
      latestFdTrace: transitionMsg.fdTraceSyscallNr === undefined
        ? null
        : {
            nr: transitionMsg.fdTraceSyscallNr,
            arg0OfdId: transitionMsg.fdTraceArg0OfdId ?? 0,
            arg0OfdType: transitionMsg.fdTraceArg0OfdType ?? 0,
            arg1: transitionMsg.fdTraceArg1 ?? null,
            arg2OfdId: transitionMsg.fdTraceArg2OfdId ?? 0,
            arg2OfdType: transitionMsg.fdTraceArg2OfdType ?? 0,
            result: transitionMsg.fdTraceResult ?? null,
          },
      isRunningWorker,
      wroteToPipe,
      drainedPipe,
      continueAfterOwnedStateSync,
    })) {
      return;
    }
    continueAfterOwnedStateSync();
  };
}
