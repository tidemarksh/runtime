import type { KernelExports, StatusCodes } from "../abi.js";
import {
  publishRuntimeBridgeDebugStage,
  type RuntimeStateBridge,
} from "../bridge/runtime-state.js";
import { clearSyscallTrace } from "./debug.js";
import {
  handleFutexWait,
  handleRuntimeBridgeBlockedStatus,
  isRuntimeBridgeBlockedStatus,
} from "./blocking.js";
import { readCompletedSyncEffects, type SyscallNumbers } from "./sync-effects.js";

interface DebugBudgetClamp {
  start: bigint;
  end: bigint;
  budget: number;
}

function parseDebugBudgetClampEntry(value: string): DebugBudgetClamp | null {
  const match = value.match(/^0x([0-9a-f]+):0x([0-9a-f]+):([0-9]+)$/i);
  if (!match) {
    return null;
  }
  const budget = Number.parseInt(match[3]!, 10);
  if (!Number.isFinite(budget) || budget <= 0) {
    return null;
  }
  return {
    start: BigInt(`0x${match[1]}`),
    end: BigInt(`0x${match[2]}`),
    budget,
  };
}

function parseDebugBudgetClamp(value: string | undefined): DebugBudgetClamp[] {
  if (!value) {
    return [];
  }
  return value
    .split(",")
    .map((entry) => parseDebugBudgetClampEntry(entry.trim()))
    .filter((entry): entry is DebugBudgetClamp => entry !== null);
}

function readDebugBudgetClampEnv(): string | undefined {
  const maybeProcess = (globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  }).process;
  return maybeProcess?.env?.TIDEMARK_DEBUG_STEP_PC_RANGE;
}

const DEBUG_BUDGET_CLAMP = parseDebugBudgetClamp(readDebugBudgetClampEnv());
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
  postStatus(status: number, waitInWorker?: boolean, syncEffects?: number): void;
  reattachPublishedKernelRuntimeState(): void;
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
    let effectiveBudget = requestedBudget;
    if (DEBUG_BUDGET_CLAMP.length > 0) {
      const pc = ctx.exports.thread_pc(ctx.vmPtr);
      for (const clamp of DEBUG_BUDGET_CLAMP) {
        if (pc >= clamp.start && pc < clamp.end) {
          effectiveBudget = Math.min(effectiveBudget, clamp.budget);
        }
      }
    }
    publishStage(ctx, 2, effectiveBudget);
    ctx.exports.thread_step(ctx.vmPtr, effectiveBudget);
    const status = ctx.exports.thread_status(ctx.vmPtr);
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

    if (isRuntimeBridgeBlockedStatus(ctx.exports, ctx.vmPtr, status, ctx.statusCodes)) {
      publishStage(ctx, 4, status);
      handleRuntimeBridgeBlockedStatus({
        ...ctx,
        runtimeBridge: ctx.runtimeBridge,
        step: (nextBudget: number) => stepThread(ctx, nextBudget),
      }, status);
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
      ctx.postStatus(status, false, completedSyncEffects);
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
    ctx.postStatus(status);
    publishStage(ctx, 6, status);
    return;
  }
}
