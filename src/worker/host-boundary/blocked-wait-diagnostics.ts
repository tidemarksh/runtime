import type {
  HostBlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import {
  supervisorProcessFdFlagsForFd,
  supervisorProcessOfdForFd,
} from "../ownership/fd-table.js";
import {
  supervisorOfdFdType,
  supervisorOfdHostFd,
  supervisorOfdIno,
  supervisorOfdOffset,
  supervisorOfdRefcount,
} from "../ownership/ofd-metadata.js";
import {
  getThreadOwnerAsyncStatus,
  getThreadWaitingInWorkerStatus,
  isThreadInitialized,
} from "../thread-host-state.js";
import { maybeThreadActivityState } from "../thread-activity-state.js";
import { pendingGuestMemoryWritesForNextStepCount } from "../thread-step-payload.js";
import { pendingChildSpawnRecordCount } from "../pending-child-spawn-state.js";
import {
  getLastPipeRetryLifecycleEvent,
  getLastWait4RetryLifecycleEvent,
  setLastPipeRetryLifecycleEvent,
  setLastWait4RetryLifecycleEvent,
} from "../host-blocked-state.js";

export interface BlockedWaitDiagnosticsDeps {
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  activeChildDebugFlag(handle: ProcessHandle, vmPtr: number): number;
  formatBlockedPollFdSet(handle: ProcessHandle, vmPtr: number): string;
  formatFirstPipeWaitDebug(handle: ProcessHandle, vmPtr: number): string;
}

export interface BlockedWaitDiagnostics {
  appendBlockedStateLifecycle(handle: ProcessHandle, blocked: HostBlockedWorkerState): void;
}

export function formatProcessThreadSlots(handle: ProcessHandle, vmPtr: number): string {
  const slots: string[] = [];
  for (let slot = 0; slot < handle.kernel.abi_max_threads(); slot += 1) {
    const tid = handle.kernel.process_thread_tid(vmPtr, slot);
    const state = handle.kernel.process_thread_state(vmPtr, slot);
    const threadVmPtr = handle.kernel.process_thread_vm_ptr(vmPtr, slot);
    if (tid === 0 && state === 0 && threadVmPtr === 0) {
      continue;
    }
    slots.push(`${slot}:${tid}/${state}/0x${threadVmPtr.toString(16)}`);
  }
  return slots.join(",");
}

export function formatFaultThreadSummary(handle: ProcessHandle, label: string, vmPtr: number): string {
  return `${label}=0x${vmPtr.toString(16)}`
    + `/status=${handle.kernel.thread_status?.(vmPtr) ?? -1}`
    + `/exit=${handle.kernel.thread_exit_code?.(vmPtr) ?? 0}:${handle.kernel.thread_term_signal?.(vmPtr) ?? 0}`
    + `/sig=${handle.kernel.thread_fault_signal?.(vmPtr) ?? 0}`
    + `/kind=${handle.kernel.thread_fault_kind?.(vmPtr) ?? 0}`
    + `/pc=0x${(handle.kernel.thread_fault_pc?.(vmPtr) ?? 0n).toString(16)}`
    + `/addr=0x${(handle.kernel.thread_fault_addr?.(vmPtr) ?? 0n).toString(16)}`;
}

function formatOfdType(handle: ProcessHandle, ofdId: number): string {
  try {
    const fdType = supervisorOfdFdType(handle, ofdId);
    if (fdType === handle.kernel.abi_fd_type_file?.()) {
      return "file";
    }
    if (fdType === handle.kernel.abi_fd_type_pipe?.()) {
      return "pipe";
    }
    if (fdType === handle.kernel.abi_fd_type_epoll?.()) {
      return "epoll";
    }
    if (fdType === handle.kernel.abi_fd_type_eventfd?.()) {
      return "eventfd";
    }
    if (fdType === handle.kernel.abi_fd_type_socket?.()) {
      return "socket";
    }
    return String(fdType);
  } catch {
    return "err";
  }
}

export function formatThreadFdTableSummary(handle: ProcessHandle, vmPtr: number): string {
  if (vmPtr === 0) {
    return "fds=none";
  }
  const focusFds = new Set([9, 10, 11, 12]);
  const entries: string[] = [];
  const maxFds = handle.kernel.abi_max_fds();
  for (let fd = 0; fd < maxFds; fd += 1) {
    let ofdId = 0;
    try {
      ofdId = supervisorProcessOfdForFd(handle, vmPtr, fd);
    } catch {
      entries.push(`${fd}=err`);
      continue;
    }
    if (ofdId === 0 && !focusFds.has(fd)) {
      continue;
    }
    const flags = supervisorProcessFdFlagsForFd(handle, vmPtr, fd);
    if (ofdId === 0) {
      entries.push(`${fd}=0/fl${flags}`);
      continue;
    }
    let ino = "err";
    let refcount = "err";
    try {
      ino = String(supervisorOfdIno(handle, ofdId));
    } catch {
      // keep diagnostic compact when an inherited harness lacks an export
    }
    try {
      refcount = String(supervisorOfdRefcount(handle, ofdId));
    } catch {
      // keep diagnostic compact when an inherited harness lacks an export
    }
    entries.push(`${fd}=${ofdId}/${formatOfdType(handle, ofdId)}/ino${ino}/ref${refcount}/fl${flags}`);
    if (entries.length >= 24) {
      entries.push("truncated");
      break;
    }
  }
  return `fds=${entries.join(",") || "empty"}`;
}

export function formatRunningRetryBlocker(
  handle: ProcessHandle,
  runningThread: ThreadWorkerState | undefined,
): string {
  const runningVmPtr = runningThread?.vmPtr ?? 0;
  const runningStatus = runningVmPtr === 0 ? -1 : handle.kernel.thread_status(runningVmPtr);
  const activeStepVmPtr = handle.kernel.debug_thread_step_vm_ptr?.() ?? 0;
  const now = Date.now();
  const activity = maybeThreadActivityState(runningThread);
  const lastStepAge = activity?.lastStepAtMs == null ? "null" : String(now - activity.lastStepAtMs);
  const lastStatusAge = activity?.lastStatusAtMs == null ? "null" : String(now - activity.lastStatusAtMs);
  const processPtr = runningVmPtr === 0 ? 0 : handle.kernel.thread_process_ptr(runningVmPtr);
  const waitingInWorkerStatus = getThreadWaitingInWorkerStatus(runningThread);
  const ownerAsyncStatus = getThreadOwnerAsyncStatus(runningThread);
  return `runningVm=0x${runningVmPtr.toString(16)}:runningStatus=${runningStatus}`
    + `:runningLast=${activity?.lastStatus ?? "null"}:runningWait=${waitingInWorkerStatus ?? "null"}`
    + `:runningSteps=${runningThread === undefined ? "null" : `${activity?.stepCount ?? 0}/${activity?.statusCount ?? 0}`}`
    + `:pendingWrites=${runningThread === undefined ? "null" : pendingGuestMemoryWritesForNextStepCount(runningThread)}`
    + `:lastStepAge=${lastStepAge}:lastStatusAge=${lastStatusAge}`
    + `:proc=0x${processPtr.toString(16)}`
    + `:initialized=${runningThread === undefined ? "null" : isThreadInitialized(runningThread) ? 1 : 0}`
    + `:ownerAsync=${ownerAsyncStatus ?? "null"}`
    + `:ready=${handle.vforkReadyWorkers.length}`
    + `:active=0x${activeStepVmPtr.toString(16)}`;
}

function formatBlockedFdEntries(handle: ProcessHandle, vmPtr: number, limit = 32): string {
  const entries: string[] = [];
  const maxFds = handle.kernel.abi_max_fds();
  for (let fd = 0; fd < Math.min(limit, maxFds); fd += 1) {
    const ofdId = supervisorProcessOfdForFd(handle, vmPtr, fd);
    if (ofdId === 0) {
      continue;
    }
    const hasValidOfd = ofdId > 0 && ofdId < maxFds;
    const type = hasValidOfd ? supervisorOfdFdType(handle, ofdId) : -1;
    const ino = hasValidOfd ? supervisorOfdIno(handle, ofdId) : -1;
    const hostFd = hasValidOfd ? supervisorOfdHostFd(handle, ofdId) : -1;
    const offset = hasValidOfd ? supervisorOfdOffset(handle, ofdId) : -1n;
    entries.push(`${fd}->${ofdId}/${type}/${ino}/${hostFd}/${offset.toString()}`);
  }
  return entries.join(" ") || "none";
}

function formatPipeSlotSummary(handle: ProcessHandle, pipeSlot: number): string {
  const maxPipes = handle.kernel.abi_max_pipes();
  const pipeRegionBytes = handle.kernel.kernel_pipe_region_bytes();
  const slotBytes = maxPipes > 0 ? Math.floor(pipeRegionBytes / maxPipes) : 0;
  const dataOffset = 32;
  if (
    pipeSlot <= 0
    || pipeSlot >= maxPipes
    || slotBytes <= dataOffset
    || handle.kernelState.pipeBase < 0
  ) {
    return "pipe-state=invalid";
  }
  const slotBase = handle.kernelState.pipeBase + pipeSlot * slotBytes;
  const view = new DataView(handle.memory.buffer);
  if (slotBase < 0 || slotBase + slotBytes > view.byteLength) {
    return "pipe-state=oob";
  }
  const head = view.getUint32(slotBase, true);
  const tail = view.getUint32(slotBase + 4, true);
  const readOpen = view.getInt32(slotBase + 8, true);
  const writeOpen = view.getInt32(slotBase + 12, true);
  const generation = view.getUint32(slotBase + 20, true);
  const capacity = slotBytes - dataOffset;
  const unread = tail >= head ? tail - head : capacity - head + tail;
  return `pipe-state=h${head}:t${tail}:ro${readOpen}:wo${writeOpen}:u${unread}:gen${generation}`;
}

export function formatBlockedEpollEntries(handle: ProcessHandle, vmPtr: number, limit = 8): string {
  const maxFds = handle.kernel.abi_max_fds();
  const epollFd = handle.kernel.thread_blocked_fd?.(vmPtr)
    ?? Number(handle.kernel.thread_a0?.(vmPtr) ?? -1n);
  const epollOfdId = (handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0) > 0
    ? handle.kernel.thread_blocked_ofd_id(vmPtr)
    : supervisorProcessOfdForFd(handle, vmPtr, epollFd);
  if (
    epollFd < 0
    || epollOfdId <= 0
    || epollOfdId >= maxFds
    || supervisorOfdFdType(handle, epollOfdId) !== handle.kernel.abi_fd_type_epoll()
  ) {
    return "none";
  }

  const tableBytes = handle.kernel.kernel_epoll_table_bytes();
  const instanceBytes = handle.kernel.kernel_epoll_instance_bytes();
  const epollIdx = supervisorOfdIno(handle, epollOfdId);
  const tablePointerBytes = tableBytes % 8 === 0
    && Math.floor((instanceBytes - 4) / (tableBytes >>> 3)) >= 28
    ? 8
    : 4;
  const tableEntries = Math.floor(tableBytes / tablePointerBytes);
  const entryStride = tableEntries > 0 ? Math.floor((instanceBytes - 4) / tableEntries) : 0;
  const countOffset = entryStride * tableEntries;
  const tablePtr = handle.kernelState.epollTablePtr;
  const view = new DataView(handle.memory.buffer);
  if (
    tableEntries <= 0
    || entryStride < 28
    || epollIdx < 0
    || epollIdx >= tableEntries
    || tablePtr < 0
    || tablePtr + tableBytes > view.byteLength
  ) {
    return `invalid:fd=${epollFd}:ofd=${epollOfdId}:idx=${epollIdx}:stride=${entryStride}`;
  }

  const instancePtr = view.getUint32(tablePtr + epollIdx * tablePointerBytes, true);
  if (instancePtr === 0 || instancePtr + instanceBytes > view.byteLength) {
    return `empty:fd=${epollFd}:ofd=${epollOfdId}:idx=${epollIdx}:ptr=0x${instancePtr.toString(16)}`;
  }
  const rawCount = view.getUint32(instancePtr + countOffset, true);
  const count = Math.min(rawCount, tableEntries, limit);
  const entries: string[] = [];
  const eventfdType = handle.kernel.abi_fd_type_eventfd?.();
  const pipeType = handle.kernel.abi_fd_type_pipe();
  for (let i = 0; i < count; i += 1) {
    const offset = instancePtr + i * entryStride;
    const fd = view.getUint32(offset, true);
    const ofdId = view.getUint32(offset + 4, true);
    const events = view.getUint32(offset + 8, true);
    const data = view.getBigUint64(offset + 16, true);
    const last = view.getUint32(offset + 24, true);
    const hasValidOfd = ofdId > 0 && ofdId < maxFds;
    const type = hasValidOfd ? supervisorOfdFdType(handle, ofdId) : -1;
    const ino = hasValidOfd ? supervisorOfdIno(handle, ofdId) : -1;
    const ofdOffset = hasValidOfd ? supervisorOfdOffset(handle, ofdId) : -1n;
    const readiness = type === eventfdType
      ? `eventfd=${ofdOffset.toString()}`
      : type === pipeType
        ? `pipe=${ino}:${formatPipeSlotSummary(handle, ino)}`
        : "obj=other";
    entries.push(
      `${i}:fd=${fd}:ofd=${ofdId}:ty=${type}:ino=${ino}`
        + `:ev=0x${events.toString(16)}:last=0x${last.toString(16)}`
        + `:data=0x${data.toString(16)}:${readiness}`,
    );
  }
  return `fd=${epollFd}:ofd=${epollOfdId}:idx=${epollIdx}:count=${rawCount}:entries=${entries.join("|") || "none"}`;
}

export function createBlockedWaitDiagnostics(
  deps: BlockedWaitDiagnosticsDeps,
): BlockedWaitDiagnostics {
  function appendBlockedStateLifecycle(handle: ProcessHandle, blocked: HostBlockedWorkerState): void {
    const status = blocked.status;
    if (status !== handle.kernel.abi_status_pipe_wait() && status !== handle.kernel.abi_status_wait4()) {
      return;
    }
    if (handle.pid === 1) {
      return;
    }
    const parts = [
      `blocked-state:${handle.pid}:0x${blocked.vmPtr.toString(16)}:status=${status}`,
      `fds=${formatBlockedFdEntries(handle, blocked.vmPtr)}`,
      "records=supervisor",
      `pending=0/${pendingChildSpawnRecordCount(handle)}`,
      `active=${deps.activeChildDebugFlag(handle, blocked.vmPtr)}`,
    ];
    if (status === handle.kernel.abi_status_pipe_wait()) {
      parts.push(
        `ofd=${handle.kernel.thread_blocked_ofd_id(blocked.vmPtr)}`,
        `ready=${handle.kernel.thread_poll_wait_ready_count?.(blocked.vmPtr) ?? -1}`,
        `pipeRevents=${handle.kernel.thread_poll_wait_first_pipe_revents?.(blocked.vmPtr) ?? -1}`,
        deps.formatBlockedPollFdSet(handle, blocked.vmPtr),
        `eofPending=${handle.kernel.thread_pipe_read_eof_sync_pending?.(blocked.vmPtr) ?? -1}`,
        `hupPending=${handle.kernel.thread_poll_hup_sync_pending?.(blocked.vmPtr) ?? -1}`,
        `buf=${handle.kernel.thread_blocked_pipe_buffered_len?.(blocked.vmPtr) ?? -1}`,
        `writeOpen=${handle.kernel.thread_blocked_pipe_write_open?.(blocked.vmPtr) ?? -1}`,
        `readReady=${handle.kernel.thread_blocked_pipe_read_ready?.(blocked.vmPtr) ?? -1}`,
      );
    }
    const event = parts.join(":");
    if (status === handle.kernel.abi_status_pipe_wait()) {
      if (getLastPipeRetryLifecycleEvent(blocked) === event) {
        return;
      }
      setLastPipeRetryLifecycleEvent(blocked, event);
    } else {
      if (getLastWait4RetryLifecycleEvent(blocked) === event) {
        return;
      }
      setLastWait4RetryLifecycleEvent(blocked, event);
    }
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
  }

  return {
    appendBlockedStateLifecycle,
  };
}
