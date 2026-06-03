import type { KernelExports, KernelRuntimeState } from "../abi.js";
import { attachKernelRuntimeState } from "../abi.js";
import type {
  GuestMemoryWriteSnapshot,
  GuestWriteWatchRegion,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPipeSlotSnapshot,
  KernelWorkerSocketStateSnapshot,
} from "../messages.js";

export interface ThreadWorkerInboundStateContext {
  kernelExports: KernelExports;
  guestBase: number;
}

export interface ThreadWorkerInboundStatePayload {
  kernelState: KernelRuntimeState;
  targetVmPtr: number;
  ofdSlots?: KernelWorkerOfdSlotSnapshot[];
  fdEntries?: KernelWorkerFdEntrySnapshot[];
  socketState?: KernelWorkerSocketStateSnapshot;
  childExitRecords?: Array<{ childPid: number; waitStatus: number; parentPid?: number }>;
  pipeSlots?: KernelWorkerPipeSlotSnapshot[];
  guestMemoryWrites?: GuestMemoryWriteSnapshot[];
  guestWriteWatchRegions?: GuestWriteWatchRegion[];
}

function toKernelU64(value: number): bigint {
  return BigInt(Math.trunc(value));
}

function applyChildExitRecords(
  kernelExports: KernelExports,
  records?: Array<{ childPid: number; waitStatus: number; parentPid?: number }>,
): void {
  if (!records || records.length === 0) {
    return;
  }
  for (const record of records) {
    if (
      record.parentPid !== undefined
      && record.parentPid > 0
      && kernelExports.process_record_child_exit_for_parent
    ) {
      kernelExports.process_record_child_exit_for_parent(
        record.parentPid,
        record.childPid,
        record.waitStatus,
      );
    } else {
      kernelExports.process_record_child_exit(record.childPid, record.waitStatus);
    }
  }
}

function applyGuestWriteWatchRegions(
  kernelExports: KernelExports,
  regions?: GuestWriteWatchRegion[],
): void {
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
  const maxBuffers = Math.max(1, Number(kernelExports.abi_max_sockets?.() ?? 32) * 2);
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
      || snapshot.slot.length !== slotBytes
    ) {
      continue;
    }
    memory.set(snapshot.slot, kernelState.ofdTablePtr + snapshot.ofdId * slotBytes);
  }
}

function applyFdEntries(
  kernelExports: KernelExports,
  targetVmPtr: number,
  fdEntries?: KernelWorkerFdEntrySnapshot[],
): void {
  if (!fdEntries || fdEntries.length === 0 || targetVmPtr === 0) {
    return;
  }
  const maxFds = kernelExports.abi_max_fds();
  for (const entry of fdEntries) {
    if (entry.fd < 0 || entry.fd >= maxFds || entry.ofdId < 0 || entry.ofdId >= maxFds) {
      continue;
    }
    kernelExports.process_set_fd_entry(targetVmPtr, entry.fd, entry.ofdId, entry.fdFlags);
  }
}

export function applyThreadWorkerInboundState(
  context: ThreadWorkerInboundStateContext,
  payload: ThreadWorkerInboundStatePayload,
): void {
  const { kernelExports } = context;
  applyOfdSlots(kernelExports, payload.kernelState, payload.ofdSlots);
  applyFdEntries(kernelExports, payload.targetVmPtr, payload.fdEntries);
  applySocketState(kernelExports, payload.kernelState, payload.socketState);
  applyChildExitRecords(kernelExports, payload.childExitRecords);
  applyInboundPipeSlots(kernelExports, payload.kernelState, payload.pipeSlots);
  applyThreadWorkerGuestMemoryWrites(context, payload.guestMemoryWrites);
  if (payload.guestWriteWatchRegions !== undefined) {
    applyGuestWriteWatchRegions(kernelExports, payload.guestWriteWatchRegions);
  }
}
