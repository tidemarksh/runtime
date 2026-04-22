import {
  attachKernelRuntimeState,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import {
  drainProcessOutput,
  flushStdin,
} from "../stdio.js";
import { kernelWorkerOwnsPipeState } from "../kernel-share.js";
import { adoptKernelState } from "../sync.js";
import type { ProcessHandle } from "../types.js";
import { createWorkerOwnedStateSyncHandler } from "./owned-state-sync.js";
import { createWorkerStatusTransitionHandler } from "./transitions.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "./types.js";
import { formatCompactDebugSuffix, readProcessPidField, WRITE_DEBUG_KEYS } from "./debug-summary.js";
import { createWorkerWaitInWorkerStatusHandler } from "./wait-in-worker.js";

interface ThreadRegisterSnapshot {
  vmPtr: number;
  pc: bigint;
  sp: bigint;
  s11: bigint;
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
      handle.kernelState = adoptKernelState(handle.kernelState, msg.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
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
    const drainedPipe = deferLocalPipeDrain ? false : drainProcessOutput(handle);
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
