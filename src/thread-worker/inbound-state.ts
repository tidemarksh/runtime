import type { KernelExports, KernelRuntimeState } from "../abi.js";
import {
  attachKernelRuntimeState,
  kernelSocketStateBytes,
  kernelSocketStatePtr,
} from "../abi.js";
import { kernelNetBufferCount } from "../kernel-socket-layout.js";
import {
  readRuntimeFdAuthorityEpoch,
  type RuntimeStateBridge,
} from "../bridge/runtime-state.js";
import type {
  GuestMemoryWriteSnapshot,
  GuestWriteWatchRegion,
  KernelMemoryWriteSnapshot,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPipeSlotSnapshot,
  KernelWorkerSocketStateSnapshot,
  ThreadWorkerProcessIdentity,
  ThreadWorkerExitedThreadPublication,
} from "../messages.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_REFCOUNT_OFFSET,
  OFD_TYPE_OFFSET,
} from "../layout/ofd.js";
import {
  PIPE_BUFFER_BYTES,
  PIPE_GENERATION_OFFSET,
  PIPE_HEAD_OFFSET,
  PIPE_HEADER_BYTES,
  PIPE_READ_OPEN_OFFSET,
  PIPE_SLOT_MODE_CONTROL,
  PIPE_SLOT_MODE_READ,
  PIPE_SLOT_MODE_WRITE,
  PIPE_TAIL_OFFSET,
  PIPE_WRITE_OPEN_OFFSET,
} from "../layout/pipe.js";
import {
  pipeSlotSnapshotActionHas,
  pipeSlotSnapshotActionSetForKernel,
  pipeSlotSnapshotMergeActionForKernel,
} from "../supervisor-pipe-snapshot.js";

export interface ThreadWorkerInboundStateContext {
  kernelExports: KernelExports;
  guestBase: number;
  runtimeBridge?: RuntimeStateBridge | null;
}

export interface ThreadWorkerInboundStatePayload {
  kernelState: KernelRuntimeState;
  targetVmPtr: number;
  processIdentity?: ThreadWorkerProcessIdentity;
  exitedThreads?: ThreadWorkerExitedThreadPublication[];
  ofdSlots?: KernelWorkerOfdSlotSnapshot[];
  fdEntries?: KernelWorkerFdEntrySnapshot[];
  socketState?: KernelWorkerSocketStateSnapshot;
  pipeSlots?: KernelWorkerPipeSlotSnapshot[];
  guestMemoryWrites?: GuestMemoryWriteSnapshot[];
  kernelMemoryWrites?: KernelMemoryWriteSnapshot[];
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

function applyExitedThreads(
  kernelExports: KernelExports,
  targetVmPtr: number,
  publications?: readonly ThreadWorkerExitedThreadPublication[],
): void {
  if (publications === undefined || publications.length === 0) {
    return;
  }
  const apply = kernelExports.supervisor_apply_exited_thread_publication;
  if (apply === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_apply_exited_thread_publication");
  }
  for (const publication of publications) {
    const result = apply.call(kernelExports, targetVmPtr, publication.vmPtr, publication.tid);
    if (result < 0) {
      throw new Error(`kernel supervisor exited thread publication failed: ${result}`);
    }
  }
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
  const memoryView = new DataView(kernelExports.memory.buffer);
  const actionSet = slotBytes >= PIPE_GENERATION_OFFSET + 4
    ? pipeSlotSnapshotActionSetForKernel(kernelExports)
    : null;
  for (const snapshot of snapshots) {
    if (
      snapshot.pipeSlot <= 0
      || snapshot.pipeSlot >= maxPipes
      || snapshot.slot.length !== slotBytes
    ) {
      continue;
    }
    const targetOffset = kernelState.pipeBase + snapshot.pipeSlot * slotBytes;
    if (slotBytes >= PIPE_GENERATION_OFFSET + 4) {
      const snapshotView = new DataView(
        snapshot.slot.buffer,
        snapshot.slot.byteOffset,
        snapshot.slot.byteLength,
      );
      const sourceGeneration = snapshotView.getUint32(PIPE_GENERATION_OFFSET, true);
      const targetGeneration = memoryView.getUint32(targetOffset + PIPE_GENERATION_OFFSET, true);
      const sourceHead = snapshotView.getUint32(PIPE_HEAD_OFFSET, true);
      const sourceTail = snapshotView.getUint32(PIPE_TAIL_OFFSET, true);
      const sourceReadOpen = snapshotView.getInt32(PIPE_READ_OPEN_OFFSET, true);
      const sourceWriteOpen = snapshotView.getInt32(PIPE_WRITE_OPEN_OFFSET, true);
      const targetHead = memoryView.getUint32(targetOffset + PIPE_HEAD_OFFSET, true);
      const targetTail = memoryView.getUint32(targetOffset + PIPE_TAIL_OFFSET, true);
      const mode = snapshot.mode
        ?? (PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE | PIPE_SLOT_MODE_CONTROL);
      const action = pipeSlotSnapshotMergeActionForKernel(kernelExports, {
        context: actionSet!.threadStepContext,
        mode,
        fresh: snapshot.fresh === true,
        sourceHead,
        sourceTail,
        sourceReadOpen,
        sourceWriteOpen,
        sourceGeneration,
        targetHead,
        targetTail,
        targetReadOpen: memoryView.getInt32(targetOffset + PIPE_READ_OPEN_OFFSET, true),
        targetWriteOpen: memoryView.getInt32(targetOffset + PIPE_WRITE_OPEN_OFFSET, true),
        targetGeneration,
      });
      if (pipeSlotSnapshotActionHas(action, actionSet!.skip)) {
        continue;
      }
      if (pipeSlotSnapshotActionHas(action, actionSet!.applySlot)) {
        memory.set(snapshot.slot.subarray(PIPE_READ_OPEN_OFFSET), targetOffset + PIPE_READ_OPEN_OFFSET);
        const counters = new Uint32Array(memory.buffer);
        Atomics.store(counters, (targetOffset + PIPE_HEAD_OFFSET) >>> 2, sourceHead);
        Atomics.store(counters, (targetOffset + PIPE_TAIL_OFFSET) >>> 2, sourceTail);
        continue;
      }
      if (pipeSlotSnapshotActionHas(action, actionSet!.applyControl)) {
        memoryView.setInt32(targetOffset + PIPE_READ_OPEN_OFFSET, sourceReadOpen, true);
        memoryView.setInt32(targetOffset + PIPE_WRITE_OPEN_OFFSET, sourceWriteOpen, true);
      }
      if (pipeSlotSnapshotActionHas(action, actionSet!.setHeadToSource)) {
        memoryView.setUint32(targetOffset + PIPE_HEAD_OFFSET, sourceHead, true);
      }
      if (pipeSlotSnapshotActionHas(action, actionSet!.setHeadZero)) {
        memoryView.setUint32(targetOffset + PIPE_HEAD_OFFSET, 0, true);
      }
      if (pipeSlotSnapshotActionHas(action, actionSet!.copyTailDelta)) {
        const copySpan = (start: number, end: number): void => {
          if (end > start) {
            memory.set(
              snapshot.slot.subarray(PIPE_HEADER_BYTES + start, PIPE_HEADER_BYTES + end),
              targetOffset + PIPE_HEADER_BYTES + start,
            );
          }
        };
        if (targetTail < sourceTail) {
          copySpan(targetTail, sourceTail);
        } else if (targetTail > sourceTail) {
          copySpan(targetTail, PIPE_BUFFER_BYTES);
          copySpan(0, sourceTail);
        }
      }
      if (pipeSlotSnapshotActionHas(action, actionSet!.copyFull)) {
        memory.set(snapshot.slot.subarray(PIPE_HEADER_BYTES), targetOffset + PIPE_HEADER_BYTES);
      }
      if (pipeSlotSnapshotActionHas(action, actionSet!.setTailToSource)) {
        Atomics.store(new Uint32Array(memory.buffer), (targetOffset + PIPE_TAIL_OFFSET) >>> 2, sourceTail);
      }
      continue;
    }
    memory.set(snapshot.slot, targetOffset);
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

function applyThreadWorkerKernelMemoryWrites(
  kernelExports: KernelExports,
  writes?: readonly KernelMemoryWriteSnapshot[],
): void {
  if (writes === undefined || writes.length === 0) {
    return;
  }
  const memory = new Uint8Array(kernelExports.memory.buffer);
  for (const write of writes) {
    if (
      !Number.isFinite(write.ptr)
      || write.ptr < 0
      || write.bytes.length === 0
      || write.ptr + write.bytes.length > memory.length
    ) {
      continue;
    }
    memory.set(write.bytes, write.ptr);
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
  const socketStateBytes = kernelSocketStateBytes(kernelExports, kernelState);
  const netBufPoolBytes = kernelExports.kernel_net_buf_pool_bytes();
  const maxBuffers = kernelNetBufferCount(kernelExports);
  const netBufSlotBytes = Math.floor(netBufPoolBytes / maxBuffers);
  if (
    socketState.socketTable.length !== socketTableBytes
    || (
      socketState.socketState !== undefined
      && socketState.socketState.length !== socketStateBytes
    )
    || (socketState.netBufPool !== undefined && socketState.netBufPool.length !== netBufPoolBytes)
  ) {
    return;
  }
  if (socketState.socketState !== undefined) {
    memory.set(socketState.socketState, kernelSocketStatePtr(kernelState));
  } else {
    memory.set(socketState.socketTable, kernelState.socketTablePtr);
  }
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
  const decideWrite = kernelExports.supervisor_ofd_slot_write_action;
  const skipWrite = kernelExports.supervisor_fd_entry_restore_action_skip?.();
  for (const snapshot of ofdSlots) {
    if (
      snapshot.ofdId <= 0
      || snapshot.ofdId >= maxFds
      || snapshot.slot.length !== slotBytes
    ) {
      continue;
    }
    const currentType = kernelExports.supervisor_ofd_fd_type?.(snapshot.ofdId)
      ?? kernelExports.kernel_ofd_fd_type?.(snapshot.ofdId)
      ?? 0;
    const candidate = snapshot.slot.byteLength >= OFD_OFFSET_OFFSET + BigInt64Array.BYTES_PER_ELEMENT
      ? new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength)
      : null;
    const candidateType = candidate?.getInt32(OFD_TYPE_OFFSET, true) ?? 0;
    let writeAction: number | undefined;
    if (
      decideWrite !== undefined
      && skipWrite !== undefined
      && candidate !== null
    ) {
      writeAction = decideWrite(
        snapshot.ofdId,
        candidateType,
        candidate.getInt32(OFD_HOST_FD_OFFSET, true),
        candidate.getUint32(OFD_INO_OFFSET, true),
        candidate.getBigInt64(OFD_OFFSET_OFFSET, true),
      );
      if (writeAction === skipWrite) {
        if (currentType !== candidateType) {
          recordInboundGuestWriteDebug(
            `thread-inbound-ofd:skip:id=${snapshot.ofdId}:current=${currentType}:candidate=${candidateType}`,
          );
        }
        continue;
      }
    }
    let appliedSlot = snapshot.slot;
    const targetOffset = kernelState.ofdTablePtr + snapshot.ofdId * slotBytes;
    if (currentType !== 0 && currentType === candidateType) {
      const currentRefcount = new DataView(memory.buffer, targetOffset, slotBytes)
        .getInt32(OFD_REFCOUNT_OFFSET, true);
      const candidateRefcount = candidate?.getInt32(OFD_REFCOUNT_OFFSET, true) ?? 0;
      if (currentRefcount > candidateRefcount) {
        appliedSlot = snapshot.slot.slice();
        new DataView(appliedSlot.buffer, appliedSlot.byteOffset, appliedSlot.byteLength)
          .setInt32(OFD_REFCOUNT_OFFSET, currentRefcount, true);
      }
    }
    memory.set(appliedSlot, targetOffset);
    if (currentType !== candidateType) {
      const afterType = kernelExports.supervisor_ofd_fd_type?.(snapshot.ofdId)
        ?? kernelExports.kernel_ofd_fd_type?.(snapshot.ofdId)
        ?? 0;
      recordInboundGuestWriteDebug(
        `thread-inbound-ofd:apply:id=${snapshot.ofdId}:current=${currentType}`
          + `:candidate=${candidateType}:action=${writeAction ?? "none"}:after=${afterType}`,
      );
    }
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
    const restoreAction = kernelExports.supervisor_fd_entry_restore_action;
    const restoreSkip = kernelExports.supervisor_fd_entry_restore_action_skip?.();
    const authoritativeRefresh = entry.generation === undefined;
    const regularFileFlag = kernelExports.supervisor_fd_entry_restore_flag_preserve_live_regular_file?.() ?? 0;
    const epollDependentFlag = kernelExports.supervisor_fd_entry_restore_flag_preserve_live_epoll_dependent?.() ?? 0;
    const restoreFlags = authoritativeRefresh ? 0 : regularFileFlag | epollDependentFlag;
    let restoreDecision: number | undefined;
    if (
      restoreAction
      && restoreSkip !== undefined
      && restoreFlags !== 0
      && (restoreDecision = restoreAction(
        targetVmPtr,
        entry.fd,
        entry.ofdId,
        SUPERVISOR_FD_ENTRY_RESTORE_SYSCALL_NONE,
        restoreFlags,
      )) === restoreSkip
    ) {
      recordInboundGuestWriteDebug(
        `thread-inbound-fd:skip:fd=${entry.fd}:current=${currentOfdId}:candidate=${entry.ofdId}`,
      );
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
    }
    kernelExports.process_set_fd_entry(targetVmPtr, entry.fd, entry.ofdId, entry.fdFlags);
    recordInboundGuestWriteDebug(
      `thread-inbound-fd:apply:fd=${entry.fd}:current=${currentOfdId}`
        + `:candidate=${entry.ofdId}:action=${restoreDecision ?? "none"}`,
    );
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
  if (payload.guestWriteWatchRegions !== undefined) {
    activeGuestWriteWatchRegions = payload.guestWriteWatchRegions;
  }
  applyProcessIdentity(kernelExports, payload.targetVmPtr, payload.processIdentity);
  applyThreadWorkerKernelMemoryWrites(kernelExports, payload.kernelMemoryWrites);
  applyExitedThreads(kernelExports, payload.targetVmPtr, payload.exitedThreads);
  applyOfdSlots(kernelExports, payload.kernelState, payload.ofdSlots);
  if (fdEntriesTargetCurrentProcess(kernelExports, payload.targetVmPtr, payload.processIdentity)) {
    applyFdEntries(
      kernelExports,
      payload.targetVmPtr,
      context.runtimeBridge,
      payload.fdEntries,
    );
  }
  applySocketState(kernelExports, payload.kernelState, payload.socketState);
  applyInboundPipeSlots(
    kernelExports,
    payload.kernelState,
    payload.pipeSlots,
  );
  applyThreadWorkerGuestMemoryWrites(context, payload.guestMemoryWrites);
  if (payload.guestWriteWatchRegions !== undefined) {
    applyGuestWriteWatchRegions(kernelExports, payload.guestWriteWatchRegions);
  }
}
