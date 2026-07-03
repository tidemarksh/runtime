import type { KernelExports, StatusCodes } from "../abi.js";
import {
  publishRuntimeBridgeDebugStage,
  type RuntimeStateBridge,
} from "../bridge/runtime-state.js";
import { executeInterpreterStep } from "../execution/interpreter.js";
import { clearSyscallTrace } from "./debug.js";
import {
  handleFutexWait,
} from "./blocking.js";
import {
  createThreadWorkerStatusOutcome,
  type ThreadWorkerOutcomePublisher,
} from "./outcomes.js";
import { readCompletedSyncEffects, type SyscallNumbers } from "./sync-effects.js";

const STAGE_DEBUG_ENABLED = ((globalThis as typeof globalThis & {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_THREAD_WORKER_STAGE_DEBUG ?? "") === "1";

interface ThreadWorkerExecContext {
  exports: KernelExports;
  statusCodes: StatusCodes;
  vmPtr: number;
  guestBase: number;
  threadStepBudget: number;
  eintrErrno: number;
  futexWaitSliceMs: number;
  syscallNumbers: SyscallNumbers | null;
  runtimeBridge: RuntimeStateBridge | null;
  postStatus: ThreadWorkerOutcomePublisher;
  reattachPublishedKernelRuntimeState(): void;
  recordFutexWaitDebug?(debug: string): void;
}

function publishStage(ctx: ThreadWorkerExecContext, stage: number, value = 0): void {
  if (STAGE_DEBUG_ENABLED && ctx.runtimeBridge !== null) {
    publishRuntimeBridgeDebugStage(ctx.runtimeBridge, stage, value);
  }
}

function readRuntimeNotifyCounter(bridge: RuntimeStateBridge | null): number {
  return bridge === null ? 0 : Atomics.load(new Int32Array(bridge.notifySab), 0);
}

export function stepThread(ctx: ThreadWorkerExecContext, budget: number): void {
  let remainingBudget = budget;
  let observedNotifyCounter = readRuntimeNotifyCounter(ctx.runtimeBridge);
  for (;;) {
    const requestedBudget = (
      ctx.runtimeBridge !== null
      && ctx.threadStepBudget > 0
      && remainingBudget > ctx.threadStepBudget
    )
      ? ctx.threadStepBudget
      : remainingBudget;
    publishStage(ctx, 1, requestedBudget);
    clearSyscallTrace(ctx.exports);
    const effectiveBudget = requestedBudget;
    publishStage(ctx, 2, effectiveBudget);
    const execution = executeInterpreterStep({
      exports: ctx.exports,
      statusCodes: ctx.statusCodes,
      vmPtr: ctx.vmPtr,
      budget: effectiveBudget,
    });
    const status = execution.status;
    publishStage(ctx, 3, status);
    remainingBudget = Math.max(0, remainingBudget - effectiveBudget);

    if (status === ctx.statusCodes.FUTEX_WAIT) {
      // Keep futex waits inside the thread-worker so wakeups come from
      // shared-memory Atomics.notify instead of owner-side 1ms retry polling.
      if (handleFutexWait({
        ...ctx,
        step: (nextBudget: number) => stepThread(ctx, nextBudget),
      }, status)) {
        stepThread(ctx, ctx.threadStepBudget);
      }
      return;
    }

    publishStage(ctx, 5, status);
    const completedSyncEffects = status === ctx.statusCodes.OK
      ? readCompletedSyncEffects(ctx.exports, ctx.statusCodes, status, ctx.syscallNumbers, ctx.vmPtr)
      : 0;
    if (
      status === ctx.statusCodes.OK
      && completedSyncEffects !== 0
    ) {
      ctx.postStatus(createThreadWorkerStatusOutcome(status, false, completedSyncEffects));
      publishStage(ctx, 6, status);
      return;
    }
    if (
      status === ctx.statusCodes.OK
      && remainingBudget > 0
      && ctx.runtimeBridge !== null
    ) {
      const currentNotifyCounter = readRuntimeNotifyCounter(ctx.runtimeBridge);
      if (currentNotifyCounter === observedNotifyCounter) {
        continue;
      }
      observedNotifyCounter = currentNotifyCounter;
    }
    ctx.postStatus(createThreadWorkerStatusOutcome(status));
    publishStage(ctx, 6, status);
    return;
  }
}
