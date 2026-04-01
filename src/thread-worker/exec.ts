import type { KernelExports, StatusCodes } from "../abi.js";
import { clearSyscallTrace } from "./debug.js";
import {
  handleFutexWait,
  handleRuntimeBridgeBlockedStatus,
  isRuntimeBridgeBlockedStatus,
} from "./blocking.js";

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
    ctx.exports.thread_step(ctx.vmPtr, budget);
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

    if (isRuntimeBridgeBlockedStatus(status, ctx.statusCodes)) {
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
