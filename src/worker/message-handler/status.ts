import {
  attachKernelRuntimeState,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import {
  drainProcessOutput,
  flushStdin,
} from "../stdio.js";
import { kernelWorkerOwnsPipeState } from "../kernel-share.js";
import { adoptKernelStateForHandle } from "../sync.js";
import type { ProcessHandle } from "../types.js";
import { createWorkerOwnedStateSyncHandler } from "./owned-state-sync.js";
import { createWorkerStatusTransitionHandler } from "./transitions.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "./types.js";
import { formatCompactDebugSuffix, readProcessPidField, WRITE_DEBUG_KEYS } from "./debug-summary.js";
import { createWorkerWaitInWorkerStatusHandler } from "./wait-in-worker.js";

const STATUS_SOCKET_DEBUG_LOG_ENABLED = ((globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.TIDEMARK_WORKER_DEBUG_LOG ?? "") === "1";

interface ThreadRegisterSnapshot {
  vmPtr: number;
  pc: bigint;
  sp: bigint;
  s11: bigint;
}

function appendStatusSocketDebug(handle: ProcessHandle, event: string): void {
  if (!STATUS_SOCKET_DEBUG_LOG_ENABLED) {
    return;
  }
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 256) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 256);
  }
}

function applyThreadFdEntries(handle: ProcessHandle, msg: ThreadWorkerStatusMessage): void {
  if (!msg.fdEntries || msg.vmPtr === 0) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds();
  for (const entry of msg.fdEntries) {
    if (entry.fd < 0 || entry.fd >= maxFds) {
      continue;
    }
    handle.kernel.process_set_fd_entry(msg.vmPtr, entry.fd, entry.ofdId, entry.fdFlags);
  }
}

function applyThreadOfdSlots(handle: ProcessHandle, msg: ThreadWorkerStatusMessage): void {
  if (!msg.ofdSlots || msg.ofdSlots.length === 0) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
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
    memory.set(snapshot.slot, handle.kernelState.ofdTablePtr + snapshot.ofdId * slotBytes);
  }
}

function applyThreadSocketState(handle: ProcessHandle, msg: ThreadWorkerStatusMessage): void {
  if (!msg.socketState) {
    return;
  }
  const before = STATUS_SOCKET_DEBUG_LOG_ENABLED && handle.networkConnections.size > 0
    ? [...handle.networkConnections.keys()].map((fd) => ({
      fd,
      sock: handle.kernel.thread_socket_idx_for_fd?.(msg.vmPtr, fd) ?? -1,
      avail: handle.kernel.thread_socket_outbound_available?.(msg.vmPtr, fd) ?? -1,
    }))
    : [];
  const socketTableBytes = handle.kernel.kernel_socket_table_bytes();
  const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes();
  if (
    msg.socketState.socketTable.length !== socketTableBytes
    || msg.socketState.netBufPool.length !== netBufPoolBytes
  ) {
    if (handle.networkConnections.size > 0) {
      appendStatusSocketDebug(handle, `socket-state-skip:${handle.pid}:reason=size:socket=${msg.socketState.socketTable.length}/${socketTableBytes}:net=${msg.socketState.netBufPool.length}/${netBufPoolBytes}`);
    }
    return;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  memory.set(msg.socketState.socketTable, handle.kernelState.socketTablePtr);
  memory.set(msg.socketState.netBufPool, handle.kernelState.netBufBase);
  handle.kernelState = {
    ...handle.kernelState,
    socketUsed: msg.socketState.socketUsed,
    netBufUsed: msg.socketState.netBufUsed,
  };
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  if (handle.networkConnections.size > 0) {
    const after = [...handle.networkConnections.keys()].map((fd) => ({
      fd,
      sock: handle.kernel.thread_socket_idx_for_fd?.(msg.vmPtr, fd) ?? -1,
      avail: handle.kernel.thread_socket_outbound_available?.(msg.vmPtr, fd) ?? -1,
    }));
    appendStatusSocketDebug(handle, `socket-state-apply:${handle.pid}:socketUsed=0x${msg.socketState.socketUsed.toString(16)}:netBufUsed=0x${msg.socketState.netBufUsed.toString(16)}:before=${JSON.stringify(before)}:after=${JSON.stringify(after)}`);
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
    const previousWaitingStatus = thread.waitingInWorkerStatus;
    thread.waitingInWorkerStatus = null;
    const status = msg.status;
    applyThreadOfdSlots(handle, msg);
    applyThreadFdEntries(handle, msg);
    thread.statusCount += 1;
    thread.statusCounts[String(status)] = (thread.statusCounts[String(status)] ?? 0) + 1;
    thread.lastStatus = status;
    thread.lastStatusAtMs = Date.now();
    thread.lastStatusPc = msg.pc;
    thread.lastStatusSp = msg.sp ?? null;
    thread.lastStatusS11 = msg.s11 ?? null;
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
    const processPidField = readProcessPidField(msg.debug);
    const transientProcessOwnerMismatch = processPidField !== null && processPidField !== handle.pid;
    const activeVforkWorker = handle.vforkSnapshot !== null && handle.vforkWorker === worker;
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
    if (
      status !== handle.kernel.abi_status_clone_spawn()
      && (!transientProcessOwnerMismatch || activeVforkWorker)
    ) {
      handle.kernelState = adoptKernelStateForHandle(handle, msg.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
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
    }

    const completedSyncEffects = (
      status === handle.kernel.abi_status_ok()
      && msg.syncEffects === undefined
    )
      ? deps.statusOkCompletedSyncEffects
      : (msg.syncEffects ?? 0);
    const continueAfterOwnedStateSync = (): void => {
      handleStatusTransition({
        handle,
        worker,
        thread,
        msg,
        status,
        wroteToPipe,
        drainedPipe,
        completedSyncEffects,
      });
    };
    if (maybeHandleOwnedStateSync({
      handle,
      worker,
      thread,
      status,
      rawSyncEffects: msg.syncEffects,
      completedSyncEffects,
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
