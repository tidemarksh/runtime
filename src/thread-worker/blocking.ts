import type { KernelExports, StatusCodes } from "../abi.js";
import {
  waitForRuntimeBridgeWake,
  type RuntimeStateBridge,
} from "../bridge/runtime-state.js";
import { clearSyscallTrace } from "./debug.js";
import {
  createThreadWorkerStatusOutcome,
  type ThreadWorkerBoundaryOutcome,
  type ThreadWorkerOutcomePublisher,
} from "./outcomes.js";
import { checkPendingSignalsAtBoundary } from "./signals.js";
import { formatThreadWorkerGuestBufferPreview } from "./status.js";

const BLOCKED_ADDR_NOT_FOUND = 0xffff_ffff_ffff_ffffn;

function futexWordIndex(guestBase: number, guestAddr: bigint, words: Int32Array): number | null {
  if (guestBase < 0 || !Number.isSafeInteger(guestBase)) {
    return null;
  }
  const absolute = BigInt(guestBase) + guestAddr;
  if (absolute < 0n || (absolute & 3n) !== 0n) {
    return null;
  }
  const index = absolute >> 2n;
  if (index > BigInt(Number.MAX_SAFE_INTEGER)) {
    return null;
  }
  const numericIndex = Number(index);
  return numericIndex >= 0 && numericIndex < words.length ? numericIndex : null;
}

interface ThreadWorkerBlockingContext {
  exports: KernelExports;
  statusCodes: StatusCodes;
  vmPtr: number;
  guestBase: number;
  runtimeBridge: RuntimeStateBridge | null;
  threadStepBudget: number;
  eintrErrno: number;
  futexWaitSliceMs: number;
  postStatus: ThreadWorkerOutcomePublisher;
  reattachPublishedKernelRuntimeState(): void;
  recordFutexWaitDebug?(debug: string): void;
  step(budget: number): void;
}

export interface ThreadWorkerResumeBlockedSyscallContext {
  exports: KernelExports;
  vmPtr: number;
  guestBase: number;
  resumeBlockedSyscall: boolean;
  pipeResumeDebugEnabled: boolean;
  applyGuestMemoryWrites(): void;
  recordResumeBlockedDebug(debug: string): void;
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
    ctx.postStatus(createThreadWorkerStatusOutcome(status));
    return;
  }

  let blockedStatus = status;
  for (;;) {
    ctx.postStatus(createThreadWorkerStatusOutcome(blockedStatus, true));

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
            ctx.postStatus(createThreadWorkerStatusOutcome(blockedStatus));
            return;
          }
          continue;
        }
      }
      ctx.postStatus(createThreadWorkerStatusOutcome(ctx.statusCodes.OK));
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
        ctx.postStatus(createThreadWorkerStatusOutcome(blockedStatus));
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
        ctx.postStatus(createThreadWorkerStatusOutcome(blockedStatus));
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
      ctx.postStatus(createThreadWorkerStatusOutcome(blockedStatus));
      return;
    }
  }
}

export function resumeBlockedSyscallStep(
  ctx: ThreadWorkerResumeBlockedSyscallContext,
): ThreadWorkerBoundaryOutcome | undefined {
  if (!ctx.resumeBlockedSyscall) {
    return undefined;
  }

  ctx.applyGuestMemoryWrites();
  const resumeFd = ctx.exports.thread_a0(ctx.vmPtr);
  const resumeBuf = ctx.exports.thread_a1(ctx.vmPtr);
  const resumeCount = ctx.exports.thread_reg?.(ctx.vmPtr, 12) ?? 0n;
  const blockedSyscallNr = ctx.exports.thread_blocked_syscall_nr?.(ctx.vmPtr) ?? 0n;
  const blockedArg0 = ctx.exports.thread_blocked_syscall_arg?.(ctx.vmPtr, 0) ?? 0n;
  const blockedArg1 = ctx.exports.thread_blocked_syscall_arg?.(ctx.vmPtr, 1) ?? 0n;
  const blockedArg2 = ctx.exports.thread_blocked_syscall_arg?.(ctx.vmPtr, 2) ?? 0n;
  const resumeResult = ctx.exports.thread_resume_blocked_syscall(ctx.vmPtr);
  if (ctx.pipeResumeDebugEnabled) {
    ctx.recordResumeBlockedDebug([
      "thread-resume-blocked",
      `fd=${resumeFd.toString()}`,
      `buf=0x${resumeBuf.toString(16)}`,
      `count=${resumeCount.toString()}`,
      `saved=${blockedSyscallNr.toString()}/${blockedArg0.toString()}/0x${blockedArg1.toString(16)}/${blockedArg2.toString()}`,
      `ret=${resumeResult.toString()}`,
      `status=${ctx.exports.thread_status(ctx.vmPtr)}`,
      `data="${formatThreadWorkerGuestBufferPreview(
        ctx.exports,
        ctx.guestBase,
        resumeBuf,
        resumeResult > 0n ? resumeResult : resumeCount,
      )}"`,
    ].join(":"));
  }
  return createThreadWorkerStatusOutcome(ctx.exports.thread_status(ctx.vmPtr));
}

export function handleFutexWait(
  ctx: ThreadWorkerBlockingContext,
  status: number,
): boolean {
  const i32 = new Int32Array(ctx.exports.memory.buffer);
  let guestAddr = ctx.exports.thread_futex_wait_addr(ctx.vmPtr);
  const expected = ctx.exports.thread_futex_wait_expected(ctx.vmPtr);
  const initialBlockedAddr = ctx.exports.thread_record_blocked_addr(ctx.vmPtr);
  recordFutexWaitDebug(ctx, i32, guestAddr, expected, "entry", null, initialBlockedAddr);

  if (initialBlockedAddr === 0n) {
    ctx.exports.thread_resume_futex_wait(ctx.vmPtr);
    return true;
  }

  ctx.postStatus(createThreadWorkerStatusOutcome(status, true));

  const timeoutPtr = Number(ctx.exports.thread_futex_wait_timeout(ctx.vmPtr));
  const timeoutMs = readTimespecMillis(ctx.exports, ctx.guestBase, timeoutPtr);
  const deadlineMs = timeoutMs === undefined ? undefined : Date.now() + timeoutMs;

  for (;;) {
    const wordIndex = futexWordIndex(ctx.guestBase, guestAddr, i32);
    if (wordIndex === null) {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_futex_wait_result(ctx.vmPtr, BigInt(ctx.exports.abi_eagain()));
      break;
    }
    const waitMs = deadlineMs === undefined
      ? ctx.futexWaitSliceMs
      : Math.max(0, Math.min(ctx.futexWaitSliceMs, deadlineMs - Date.now()));
    const result = Atomics.wait(i32, wordIndex, expected, waitMs);

    ctx.reattachPublishedKernelRuntimeState();
    const currentStatus = ctx.exports.thread_status(ctx.vmPtr);
    const stillBlockedAddr = ctx.exports.thread_record_blocked_addr(ctx.vmPtr);
    recordFutexWaitDebug(ctx, i32, guestAddr, expected, "wait", result, stillBlockedAddr, currentStatus);
    if (currentStatus !== ctx.statusCodes.FUTEX_WAIT) {
      ctx.postStatus(createThreadWorkerStatusOutcome(currentStatus));
      return false;
    }
    const hasPendingSignal = ctx.exports.thread_pending_signal_interrupts_blocked_syscall?.(ctx.vmPtr) === 1
      || ctx.exports.thread_sig_pending(ctx.vmPtr) !== 0n;
    if (stillBlockedAddr === BLOCKED_ADDR_NOT_FOUND) {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_futex_wait_result(ctx.vmPtr, BigInt(ctx.exports.abi_eagain()));
      break;
    }
    if (stillBlockedAddr !== 0n && stillBlockedAddr !== guestAddr) {
      guestAddr = stillBlockedAddr;
      continue;
    }
    if (stillBlockedAddr !== 0n && hasPendingSignal) {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_futex_wait_result(
        ctx.vmPtr,
        BigInt(ctx.exports.abi_eintr?.() ?? -ctx.eintrErrno),
      );
      break;
    }
    if (stillBlockedAddr === 0n) {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_futex_wait(ctx.vmPtr);
      break;
    }
    if (result === "not-equal") {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_futex_wait(ctx.vmPtr);
      break;
    }
    if (deadlineMs !== undefined && Date.now() >= deadlineMs) {
      clearSyscallTrace(ctx.exports);
      ctx.exports.thread_resume_futex_wait_result(ctx.vmPtr, BigInt(-110));
      break;
    }
  }

  const resumedStatus = ctx.exports.thread_status(ctx.vmPtr);
  ctx.postStatus(createThreadWorkerStatusOutcome(resumedStatus));
  return false;
}

function recordFutexWaitDebug(
  ctx: ThreadWorkerBlockingContext,
  i32: Int32Array,
  guestAddr: bigint,
  expected: number,
  stage: string,
  result: "ok" | "not-equal" | "timed-out" | null,
  blockedAddr: bigint,
  status?: number,
): void {
  if (ctx.recordFutexWaitDebug === undefined) {
    return;
  }
  const wordIndex = futexWordIndex(ctx.guestBase, guestAddr, i32);
  const word = wordIndex === null ? "n/a" : Atomics.load(i32, wordIndex).toString();
  ctx.recordFutexWaitDebug([
    "thread-futex-wait",
    `stage=${stage}`,
    `vm=0x${ctx.vmPtr.toString(16)}`,
    `addr=0x${guestAddr.toString(16)}`,
    `index=${wordIndex ?? "n/a"}`,
    `expected=${expected}`,
    `word=${word}`,
    `result=${result ?? "n/a"}`,
    `blocked=0x${blockedAddr.toString(16)}`,
    status === undefined ? "" : `status=${status}`,
  ].filter((part) => part.length > 0).join(":"));
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
