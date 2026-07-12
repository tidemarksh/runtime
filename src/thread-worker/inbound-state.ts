import type { KernelExports, KernelRuntimeState } from "../abi.js";
import { attachKernelRuntimeState } from "../abi.js";
import { kernelNetBufferCount } from "../kernel-socket-layout.js";
import {
  readRuntimeFdAuthorityEpoch,
  type RuntimeStateBridge,
} from "../bridge/runtime-state.js";
import type {
  GuestMemoryWriteSnapshot,
  GuestWriteWatchRegion,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPipeSlotSnapshot,
  KernelWorkerSocketStateSnapshot,
  ThreadWorkerProcessIdentity,
} from "../messages.js";

export interface ThreadWorkerInboundStateContext {
  kernelExports: KernelExports;
  guestBase: number;
  runtimeBridge?: RuntimeStateBridge | null;
}

export interface ThreadWorkerInboundStatePayload {
  kernelState: KernelRuntimeState;
  targetVmPtr: number;
  processIdentity?: ThreadWorkerProcessIdentity;
  ofdSlots?: KernelWorkerOfdSlotSnapshot[];
  fdEntries?: KernelWorkerFdEntrySnapshot[];
  socketState?: KernelWorkerSocketStateSnapshot;
  pipeSlots?: KernelWorkerPipeSlotSnapshot[];
  guestMemoryWrites?: GuestMemoryWriteSnapshot[];
  guestWriteWatchRegions?: GuestWriteWatchRegion[];
}

let activeGuestWriteWatchRegions: readonly GuestWriteWatchRegion[] = [];
const inboundGuestWriteDebug: string[] = [];
const SUPERVISOR_FD_ENTRY_RESTORE_SYSCALL_NONE = 0xffffffffffffffffn;

function applyProcessIdentity(
  kernelExports: KernelExports,
  targetVmPtr: number,
  identity?: ThreadWorkerProcessIdentity,
): void {
  if (
    identity === undefined
    || targetVmPtr === 0
    || !Number.isInteger(identity.processPtr)
    || identity.processPtr <= 0
    || !Number.isInteger(identity.pid)
    || identity.pid <= 0
    || !Number.isInteger(identity.ppid)
    || identity.ppid < 0
    || !Number.isInteger(identity.nextPid)
    || identity.nextPid <= 0
  ) {
    return;
  }
  if (kernelExports.thread_process_ptr(targetVmPtr) !== identity.processPtr) {
    return;
  }
  const currentPid = kernelExports.process_current_pid(targetVmPtr);
  const currentPpid = kernelExports.process_current_ppid(targetVmPtr);
  if (
    currentPid === identity.pid
    && currentPpid === identity.ppid
  ) {
    return;
  }
  kernelExports.process_restore_current_identity(
    targetVmPtr,
    identity.pid,
    identity.ppid,
    identity.nextPid,
  );
}

function toKernelU64(value: number): bigint {
  return BigInt(Math.trunc(value));
}

function formatGuestWriteOverlap(
  source: string,
  regions: readonly GuestWriteWatchRegion[] | undefined,
  guestPtr: number,
  bytes: Uint8Array,
): string | null {
  if (!regions || regions.length === 0 || bytes.length === 0 || !Number.isFinite(guestPtr)) {
    return null;
  }
  const writeEnd = guestPtr + bytes.length;
  for (const region of regions) {
    const overlapStart = Math.max(guestPtr, region.start);
    const overlapEnd = Math.min(writeEnd, region.end);
    if (overlapStart >= overlapEnd) {
      continue;
    }
    const sampleStart = Math.max(0, overlapStart - guestPtr);
    const sampleEnd = Math.min(bytes.length, sampleStart + 16, overlapEnd - guestPtr);
    const sample = Array.from(bytes.subarray(sampleStart, sampleEnd))
      .map((value) => value.toString(16).padStart(2, "0"))
      .join("");
    return `${source}:slot=${region.slot}:ptr=0x${guestPtr.toString(16)}`
      + `:len=${bytes.length}:overlap=0x${overlapStart.toString(16)}`
      + `..0x${overlapEnd.toString(16)}:sample=${sample}`;
  }
  return null;
}

function recordInboundGuestWriteDebug(entry: string): void {
  inboundGuestWriteDebug.push(entry);
  if (inboundGuestWriteDebug.length > 64) {
    inboundGuestWriteDebug.splice(0, inboundGuestWriteDebug.length - 64);
  }
}

export function consumeThreadWorkerInboundGuestWriteDebug(): string[] {
  const entries = inboundGuestWriteDebug.slice();
  inboundGuestWriteDebug.length = 0;
  return entries;
}

function applyGuestWriteWatchRegions(
  kernelExports: KernelExports,
  regions?: GuestWriteWatchRegion[],
): void {
  activeGuestWriteWatchRegions = regions ?? [];
  kernelExports.debug_clear_guest_write_watch_trace?.();
  if (!regions || regions.length === 0) {
    return;
  }
  for (const region of regions) {
    kernelExports.debug_configure_guest_write_watch?.(
      region.slot,
      toKernelU64(region.start),
      toKernelU64(region.end),
    );
  }
}

function applyInboundPipeSlots(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
  snapshots?: KernelWorkerPipeSlotSnapshot[],
): void {
  if (!snapshots || snapshots.length === 0) {
    return;
  }
  const maxPipes = kernelExports.abi_max_pipes();
  const slotBytes = kernelExports.kernel_pipe_region_bytes() / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
  }
  const memory = new Uint8Array(kernelExports.memory.buffer);
  for (const snapshot of snapshots) {
    if (
      snapshot.pipeSlot <= 0
      || snapshot.pipeSlot >= maxPipes
      || snapshot.slot.length !== slotBytes
    ) {
      continue;
    }
    memory.set(snapshot.slot, kernelState.pipeBase + snapshot.pipeSlot * slotBytes);
  }
}

export function applyThreadWorkerGuestMemoryWrites(
  context: ThreadWorkerInboundStateContext,
  writes?: GuestMemoryWriteSnapshot[],
): void {
  if (!writes || writes.length === 0) {
    return;
  }
  const memory = new Uint8Array(context.kernelExports.memory.buffer);
  for (const write of writes) {
    const start = context.guestBase + write.guestPtr;
    if (
      !Number.isFinite(start)
      || start < 0
      || start + write.bytes.length > memory.byteLength
    ) {
      continue;
    }
    const overlap = formatGuestWriteOverlap(
      "thread-inbound-guest-write",
      activeGuestWriteWatchRegions,
      write.guestPtr,
      write.bytes,
    );
    if (overlap !== null) {
      recordInboundGuestWriteDebug(overlap);
    }
    memory.set(write.bytes, start);
  }
}

function applySocketState(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
  socketState?: KernelWorkerSocketStateSnapshot,
): void {
  if (!socketState) {
    return;
  }
  const memory = new Uint8Array(kernelExports.memory.buffer);
  const socketTableBytes = kernelExports.kernel_socket_table_bytes();
  const netBufPoolBytes = kernelExports.kernel_net_buf_pool_bytes();
  const maxBuffers = kernelNetBufferCount(kernelExports);
  const netBufSlotBytes = Math.floor(netBufPoolBytes / maxBuffers);
  if (
    socketState.socketTable.length !== socketTableBytes
    || (socketState.netBufPool !== undefined && socketState.netBufPool.length !== netBufPoolBytes)
  ) {
    return;
  }
  memory.set(socketState.socketTable, kernelState.socketTablePtr);
  if (socketState.netBufPool !== undefined) {
    memory.set(socketState.netBufPool, kernelState.netBufBase);
  } else if (netBufSlotBytes > 0) {
    for (const slot of socketState.netBufSlots ?? []) {
      if (slot.bufferIndex < 0 || slot.bufferIndex >= maxBuffers || slot.bytes.length !== netBufSlotBytes) {
        continue;
      }
      memory.set(slot.bytes, kernelState.netBufBase + slot.bufferIndex * netBufSlotBytes);
    }
  }
  attachKernelRuntimeState(kernelExports, {
    ...kernelState,
    socketUsed: kernelState.socketUsed | socketState.socketUsed,
    netBufUsed: kernelState.netBufUsed | socketState.netBufUsed,
  });
}

function applyOfdSlots(
  kernelExports: KernelExports,
  kernelState: KernelRuntimeState,
  protectedOfdId: number,
  ofdSlots?: KernelWorkerOfdSlotSnapshot[],
): void {
  if (!ofdSlots || ofdSlots.length === 0) {
    return;
  }
  const maxFds = kernelExports.abi_max_fds();
  const tableBytes = kernelExports.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
  }
  const memory = new Uint8Array(kernelExports.memory.buffer);
  for (const snapshot of ofdSlots) {
    if (
      snapshot.ofdId <= 0
      || snapshot.ofdId >= maxFds
      || (protectedOfdId > 0 && snapshot.ofdId === protectedOfdId)
      || snapshot.slot.length !== slotBytes
    ) {
      continue;
    }
    memory.set(snapshot.slot, kernelState.ofdTablePtr + snapshot.ofdId * slotBytes);
  }
}

function ofdIsPipeLike(kernelExports: KernelExports, ofdId: number): boolean {
  if (ofdId <= 0 || ofdId >= kernelExports.abi_max_fds()) {
    return false;
  }
  const pipeSyncCandidate = kernelExports.supervisor_pipe_sync_ofd_candidate?.(ofdId);
  if (pipeSyncCandidate !== undefined) {
    return pipeSyncCandidate === 1;
  }
  const fdType = kernelExports.supervisor_ofd_fd_type?.(ofdId)
    ?? kernelExports.kernel_ofd_fd_type?.(ofdId)
    ?? 0;
  const pipeType = kernelExports.abi_fd_type_pipe?.();
  if (pipeType !== undefined && fdType === pipeType) {
    return true;
  }
  const ptyMaster = kernelExports.abi_fd_type_pty_master?.();
  const ptySlave = kernelExports.abi_fd_type_pty_slave?.();
  return (ptyMaster !== undefined && fdType === ptyMaster)
    || (ptySlave !== undefined && fdType === ptySlave);
}

function applyFdEntries(
  kernelExports: KernelExports,
  targetVmPtr: number,
  protectedOfdId: number,
  runtimeBridge: RuntimeStateBridge | null | undefined,
  fdEntries?: KernelWorkerFdEntrySnapshot[],
): void {
  if (!fdEntries || fdEntries.length === 0 || targetVmPtr === 0) {
    return;
  }
  const maxFds = kernelExports.abi_max_fds();
  for (const entry of fdEntries) {
    if (
      entry.fd < 0
      || entry.fd >= maxFds
      || entry.ofdId < 0
      || entry.ofdId >= maxFds
      || (protectedOfdId > 0 && entry.ofdId === protectedOfdId)
      || (
        entry.authorityEpoch !== undefined
        && runtimeBridge !== null
        && runtimeBridge !== undefined
        && readRuntimeFdAuthorityEpoch(runtimeBridge, entry.fd) > entry.authorityEpoch
      )
    ) {
      continue;
    }
    const currentOfdId = kernelExports.supervisor_process_ofd_for_fd?.(targetVmPtr, entry.fd)
      ?? kernelExports.process_fd_ofd_id(targetVmPtr, entry.fd);
    const currentFdFlags = kernelExports.supervisor_process_fd_flags_for_fd?.(targetVmPtr, entry.fd)
      ?? kernelExports.process_fd_flags(targetVmPtr, entry.fd);
    if (currentOfdId === entry.ofdId && currentFdFlags === entry.fdFlags) {
      continue;
    }
    const currentGeneration = kernelExports.process_fd_generation?.(targetVmPtr, entry.fd);
    if (
      entry.generation !== undefined
      && currentGeneration !== undefined
      && currentGeneration > entry.generation
    ) {
      if (entry.monotonicGeneration === true) {
        continue;
      }
      if (ofdIsPipeLike(kernelExports, currentOfdId)) {
        continue;
      }
      const restoreAction = kernelExports.supervisor_fd_entry_restore_action;
      const restoreSkip = kernelExports.supervisor_fd_entry_restore_action_skip?.();
      const regularFileFlag = kernelExports.supervisor_fd_entry_restore_flag_preserve_live_regular_file?.() ?? 0;
      const epollDependentFlag = kernelExports.supervisor_fd_entry_restore_flag_preserve_live_epoll_dependent?.() ?? 0;
      const restoreFlags = regularFileFlag | epollDependentFlag;
      if (
        restoreAction
        && restoreSkip !== undefined
        && restoreFlags !== 0
        && restoreAction(
          targetVmPtr,
          entry.fd,
          entry.ofdId,
          SUPERVISOR_FD_ENTRY_RESTORE_SYSCALL_NONE,
          restoreFlags,
        ) === restoreSkip
      ) {
        continue;
      }
    }
    kernelExports.process_set_fd_entry(targetVmPtr, entry.fd, entry.ofdId, entry.fdFlags);
  }
}

function fdEntriesTargetCurrentProcess(
  kernelExports: KernelExports,
  targetVmPtr: number,
  identity?: ThreadWorkerProcessIdentity,
): boolean {
  return identity === undefined
    || (targetVmPtr !== 0 && kernelExports.thread_process_ptr(targetVmPtr) === identity.processPtr);
}

export function applyThreadWorkerInboundState(
  context: ThreadWorkerInboundStateContext,
  payload: ThreadWorkerInboundStatePayload,
): void {
  const { kernelExports } = context;
  const protectedOfdId = payload.targetVmPtr === 0
    ? 0
    : (kernelExports.thread_blocked_ofd_id?.(payload.targetVmPtr) ?? 0);
  if (payload.guestWriteWatchRegions !== undefined) {
    activeGuestWriteWatchRegions = payload.guestWriteWatchRegions;
  }
  applyProcessIdentity(kernelExports, payload.targetVmPtr, payload.processIdentity);
  applyOfdSlots(kernelExports, payload.kernelState, protectedOfdId, payload.ofdSlots);
  if (fdEntriesTargetCurrentProcess(kernelExports, payload.targetVmPtr, payload.processIdentity)) {
    applyFdEntries(
      kernelExports,
      payload.targetVmPtr,
      protectedOfdId,
      context.runtimeBridge,
      payload.fdEntries,
    );
  }
  applySocketState(kernelExports, payload.kernelState, payload.socketState);
  applyInboundPipeSlots(kernelExports, payload.kernelState, payload.pipeSlots);
  applyThreadWorkerGuestMemoryWrites(context, payload.guestMemoryWrites);
  if (payload.guestWriteWatchRegions !== undefined) {
    applyGuestWriteWatchRegions(kernelExports, payload.guestWriteWatchRegions);
  }
}
