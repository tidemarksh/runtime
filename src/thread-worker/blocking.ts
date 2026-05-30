import type { KernelExports, StatusCodes } from "../abi.js";
import {
  waitForRuntimeBridgeWake,
  type RuntimeStateBridge,
} from "../bridge/runtime-state.js";
import { clearSyscallTrace } from "./debug.js";
import type { ThreadWorkerStatusPublisher } from "./outcomes.js";
import { checkPendingSignalsAtBoundary } from "./signals.js";

const BLOCKED_ADDR_NOT_FOUND = 0xffff_ffff;

interface ThreadWorkerBlockingContext {
  exports: KernelExports;
  statusCodes: StatusCodes;
  vmPtr: number;
  guestBase: number;
  runtimeBridge: RuntimeStateBridge | null;
  threadStepBudget: number;
  eintrErrno: number;
  futexWaitSliceMs: number;
  postStatus: ThreadWorkerStatusPublisher;
  reattachPublishedKernelRuntimeState(): void;
  step(budget: number): void;
}

function epollWaitShouldStayInWorker(
  exports: KernelExports,
  vmPtr: number,
  status: number,
  statusCodes: StatusCodes,
): boolean {
  void exports;
  void vmPtr;
  return status === statusCodes.EPOLL_WAIT && false;
}

export function isRuntimeBridgeBlockedStatus(
  exports: KernelExports,
  vmPtr: number,
  status: number,
  statusCodes: StatusCodes,
): boolean {
  // Keep blocked waits owner-side so the runtime scheduler remains the single
  // authority over process progress.
  return epollWaitShouldStayInWorker(exports, vmPtr, status, statusCodes);
}

export function handleRuntimeBridgeBlockedStatus(
  ctx: ThreadWorkerBlockingContext,
  status: number,
): void {
  if (ctx.runtimeBridge === null) {
    ctx.postStatus(status);
    return;
  }

  let blockedStatus = status;
  for (;;) {
    ctx.postStatus(blockedStatus, true);

    const timeoutMs = Number(ctx.exports.thread_epoll_wait_timeout(ctx.vmPtr));
    const waitResult = timeoutMs < 0
      ? waitForRuntimeBridgeWake(ctx.runtimeBridge)
      : waitForRuntimeBridgeWake(ctx.runtimeBridge, Math.max(0, timeoutMs));

    ctx.reattachPublishedKernelRuntimeState();
    const boundary = checkPendingSignalsAtBoundary(ctx.exports, ctx.vmPtr, ctx.statusCodes);
    if (boundary.delivered) {
      if (blockedStatus === ctx.statusCodes.EPOLL_WAIT) {
        const interrupted = ctx.exports.thread_interrupt_epoll_wait?.(ctx.vmPtr, -ctx.eintrErrno) ?? 0;
        if (interrupted === 0) {
          blockedStatus = ctx.exports.thread_status(ctx.vmPtr);
          if (!isRuntimeBridgeBlockedStatus(ctx.exports, ctx.vmPtr, blockedStatus, ctx.statusCodes)) {
            ctx.postStatus(blockedStatus);
            return;
          }
          continue;
        }
      }
      ctx.postStatus(ctx.statusCodes.OK);
      return;
    }
    if (
      blockedStatus === ctx.statusCodes.EPOLL_WAIT
      && waitResult === "timed-out"
      && boundary.status === ctx.statusCodes.EPOLL_WAIT
    ) {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_epoll_wait_timeout(ctx.vmPtr);
      blockedStatus = ctx.exports.thread_status(ctx.vmPtr);
      if (blockedStatus === ctx.statusCodes.FUTEX_WAIT) {
        if (handleFutexWait(ctx, blockedStatus)) {
          ctx.step(ctx.threadStepBudget);
        }
        return;
      }
      if (!isRuntimeBridgeBlockedStatus(ctx.exports, ctx.vmPtr, blockedStatus, ctx.statusCodes)) {
        ctx.postStatus(blockedStatus);
        return;
      }
      continue;
    }
    if (boundary.status !== ctx.statusCodes.OK) {
      blockedStatus = boundary.status;
      if (blockedStatus === ctx.statusCodes.FUTEX_WAIT) {
        if (handleFutexWait(ctx, blockedStatus)) {
          ctx.step(ctx.threadStepBudget);
        }
        return;
      }
      if (!isRuntimeBridgeBlockedStatus(ctx.exports, ctx.vmPtr, blockedStatus, ctx.statusCodes)) {
        ctx.postStatus(blockedStatus);
        return;
      }
      continue;
    }
    if (blockedStatus === ctx.statusCodes.EPOLL_WAIT && waitResult === "timed-out") {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_epoll_wait_timeout(ctx.vmPtr);
    } else {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_blocked_syscall(ctx.vmPtr);
    }

    blockedStatus = ctx.exports.thread_status(ctx.vmPtr);
    if (blockedStatus === ctx.statusCodes.FUTEX_WAIT) {
      if (handleFutexWait(ctx, blockedStatus)) {
        ctx.step(ctx.threadStepBudget);
      }
      return;
    }
    if (!isRuntimeBridgeBlockedStatus(ctx.exports, ctx.vmPtr, blockedStatus, ctx.statusCodes)) {
      ctx.postStatus(blockedStatus);
      return;
    }
  }
}

export function handleFutexWait(
  ctx: ThreadWorkerBlockingContext,
  status: number,
): boolean {
  const i32 = new Int32Array(ctx.exports.memory.buffer);
  let guestAddr = Number(ctx.exports.thread_futex_wait_addr(ctx.vmPtr));
  const expected = ctx.exports.thread_futex_wait_expected(ctx.vmPtr);

  if (ctx.exports.thread_record_blocked_addr(ctx.vmPtr) === 0) {
    ctx.exports.thread_resume_futex_wait(ctx.vmPtr);
    return true;
  }

  ctx.postStatus(status, true);

  const timeoutPtr = Number(ctx.exports.thread_futex_wait_timeout(ctx.vmPtr));
  const timeoutMs = readTimespecMillis(ctx.exports, ctx.guestBase, timeoutPtr);
  const deadlineMs = timeoutMs === undefined ? undefined : Date.now() + timeoutMs;

  for (;;) {
    const wordIndex = (ctx.guestBase + guestAddr) >>> 2;
    const waitMs = deadlineMs === undefined
      ? ctx.futexWaitSliceMs
      : Math.max(0, Math.min(ctx.futexWaitSliceMs, deadlineMs - Date.now()));
    const result = Atomics.wait(i32, wordIndex, expected, waitMs);

    ctx.reattachPublishedKernelRuntimeState();
    const currentStatus = ctx.exports.thread_status(ctx.vmPtr);
    if (currentStatus !== ctx.statusCodes.FUTEX_WAIT) {
      ctx.postStatus(currentStatus);
      return false;
    }
    const stillBlockedAddr = ctx.exports.thread_record_blocked_addr(ctx.vmPtr);
    const hasPendingSignal = ctx.exports.thread_pending_signal_interrupts_blocked_syscall?.(ctx.vmPtr) === 1
      || ctx.exports.thread_sig_pending(ctx.vmPtr) !== 0n;
    if (stillBlockedAddr === BLOCKED_ADDR_NOT_FOUND) {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_futex_wait_result(ctx.vmPtr, BigInt(ctx.exports.abi_eagain()));
      break;
    }
    if (stillBlockedAddr !== 0 && stillBlockedAddr !== guestAddr) {
      guestAddr = stillBlockedAddr;
      continue;
    }
    if (stillBlockedAddr !== 0 && hasPendingSignal) {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_futex_wait_result(
        ctx.vmPtr,
        BigInt(ctx.exports.abi_eintr?.() ?? -ctx.eintrErrno),
      );
      break;
    }
    if (stillBlockedAddr === 0) {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_futex_wait(ctx.vmPtr);
      break;
    }
    if (result === "not-equal") {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_futex_wait_result(ctx.vmPtr, BigInt(ctx.exports.abi_eagain()));
      break;
    }
    if (deadlineMs !== undefined && Date.now() >= deadlineMs) {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_futex_wait_result(ctx.vmPtr, BigInt(-110));
      break;
    }
  }

  const resumedStatus = ctx.exports.thread_status(ctx.vmPtr);
  ctx.postStatus(resumedStatus);
  return false;
}

function readTimespecMillis(
  exports: KernelExports,
  guestBase: number,
  timespecPtr: number,
): number | undefined {
  if (timespecPtr === 0) {
    return undefined;
  }
  const view = new DataView(exports.memory.buffer);
  const absolute = guestBase + timespecPtr;
  const sec = Number(view.getBigUint64(absolute, true));
  const nsec = Number(view.getBigUint64(absolute + 8, true));
  return Math.max(0, sec * 1000 + Math.floor(nsec / 1_000_000));
}
