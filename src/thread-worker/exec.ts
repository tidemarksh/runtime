import type { KernelExports, StatusCodes } from "../abi.js";
import { clearSyscallTrace } from "./debug.js";
import {
  handleFutexWait,
  handleRuntimeBridgeBlockedStatus,
  isRuntimeBridgeBlockedStatus,
} from "./blocking.js";

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

const DEBUG_BUDGET_CLAMP = parseDebugBudgetClamp(process.env.TIDEMARK_DEBUG_STEP_PC_RANGE);

interface ThreadWorkerExecContext {
  exports: KernelExports;
  statusCodes: StatusCodes;
  vmPtr: number;
  guestBase: number;
  threadStepBudget: number;
  eintrErrno: number;
  futexWaitSliceMs: number;
  runtimeBridge: ReturnType<() => import("../bridge/runtime-state.js").RuntimeStateBridge | null>;
  postStatus(status: number, waitInWorker?: boolean): void;
  reattachPublishedKernelRuntimeState(): void;
}

export function stepThread(ctx: ThreadWorkerExecContext, budget: number): void {
  for (;;) {
    clearSyscallTrace(ctx.exports);
    let effectiveBudget = budget;
    if (DEBUG_BUDGET_CLAMP.length > 0) {
      const pc = ctx.exports.thread_pc(ctx.vmPtr);
      for (const clamp of DEBUG_BUDGET_CLAMP) {
        if (pc >= clamp.start && pc < clamp.end) {
          effectiveBudget = Math.min(effectiveBudget, clamp.budget);
        }
      }
    }
    ctx.exports.thread_step(ctx.vmPtr, effectiveBudget);
    const status = ctx.exports.thread_status(ctx.vmPtr);

    if (status === ctx.statusCodes.FUTEX_WAIT) {
      if (handleFutexWait({
        ...ctx,
        runtimeBridge: ctx.runtimeBridge,
        step: (nextBudget: number) => stepThread(ctx, nextBudget),
      }, status)) {
        budget = ctx.threadStepBudget;
        continue;
      }
      return;
    }

    if (isRuntimeBridgeBlockedStatus(ctx.exports, ctx.vmPtr, status, ctx.statusCodes)) {
      handleRuntimeBridgeBlockedStatus({
        ...ctx,
        runtimeBridge: ctx.runtimeBridge,
        step: (nextBudget: number) => stepThread(ctx, nextBudget),
      }, status);
      return;
    }

    ctx.postStatus(status);
    return;
  }
}
