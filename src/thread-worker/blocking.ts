import type { KernelExports, StatusCodes } from "../abi.js";
import {
  type RuntimeStateBridge,
} from "../bridge/runtime-state.js";
import { clearSyscallTrace } from "./debug.js";
import {
  createThreadWorkerStatusOutcome,
  type ThreadWorkerBoundaryOutcome,
  type ThreadWorkerOutcomePublisher,
} from "./outcomes.js";
import {
  hasPendingSignalForBlockedWait,
} from "./signals.js";
import { formatThreadWorkerGuestBufferPreview } from "./status.js";
import { resumeBlockedSyscallBySupervisor } from "../supervisor-blocked-resume.js";

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

export function resumeBlockedSyscallStep(
  ctx: ThreadWorkerResumeBlockedSyscallContext,
): ThreadWorkerBoundaryOutcome | undefined {
  if (!ctx.resumeBlockedSyscall) {
    return undefined;
  }

  ctx.applyGuestMemoryWrites();
  if (ctx.exports.thread_status(ctx.vmPtr) === ctx.exports.abi_status_futex_wait()) {
    completeSupervisorFutexWaitByExports(ctx.exports, ctx.vmPtr, "woken");
    return createThreadWorkerStatusOutcome(ctx.exports.thread_status(ctx.vmPtr));
  }

  const resumeFd = ctx.exports.thread_a0(ctx.vmPtr);
  const resumeBuf = ctx.exports.thread_a1(ctx.vmPtr);
  const resumeCount = ctx.exports.thread_reg?.(ctx.vmPtr, 12) ?? 0n;
  const blockedSyscallNr = ctx.exports.thread_blocked_syscall_nr?.(ctx.vmPtr) ?? 0n;
  const blockedArg0 = ctx.exports.thread_blocked_syscall_arg?.(ctx.vmPtr, 0) ?? 0n;
  const blockedArg1 = ctx.exports.thread_blocked_syscall_arg?.(ctx.vmPtr, 1) ?? 0n;
  const blockedArg2 = ctx.exports.thread_blocked_syscall_arg?.(ctx.vmPtr, 2) ?? 0n;
  const resumeResult = resumeBlockedSyscallBySupervisor(ctx.exports, ctx.vmPtr);
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
    completeSupervisorFutexWait(ctx, "woken");
    ctx.postStatus(createThreadWorkerStatusOutcome(ctx.exports.thread_status(ctx.vmPtr)));
    return false;
  }

  ctx.postStatus(createThreadWorkerStatusOutcome(status, true));

  for (;;) {
    const wordIndex = futexWordIndex(ctx.guestBase, guestAddr, i32);
    if (wordIndex === null) {
      clearSyscallTrace(ctx.exports);
      completeSupervisorFutexWait(ctx, "invalid-address");
      break;
    }
    const result = Atomics.wait(i32, wordIndex, expected, ctx.futexWaitSliceMs);

    ctx.reattachPublishedKernelRuntimeState();
    const currentStatus = ctx.exports.thread_status(ctx.vmPtr);
    const stillBlockedAddr = ctx.exports.thread_record_blocked_addr(ctx.vmPtr);
    recordFutexWaitDebug(ctx, i32, guestAddr, expected, "wait", result, stillBlockedAddr, currentStatus);
    if (currentStatus !== ctx.statusCodes.FUTEX_WAIT) {
      ctx.postStatus(createThreadWorkerStatusOutcome(currentStatus));
      return false;
    }
    const hasPendingSignal = hasPendingSignalForBlockedWait(ctx.exports, ctx.vmPtr);
    if (stillBlockedAddr === BLOCKED_ADDR_NOT_FOUND) {
      clearSyscallTrace(ctx.exports);
      completeSupervisorFutexWait(ctx, "invalid-address");
      break;
    }
    if (stillBlockedAddr !== 0n && stillBlockedAddr !== guestAddr) {
      guestAddr = stillBlockedAddr;
      continue;
    }
    if (stillBlockedAddr !== 0n && hasPendingSignal) {
      clearSyscallTrace(ctx.exports);
      completeSupervisorFutexWait(ctx, "signal-check");
      break;
    }
    if (stillBlockedAddr === 0n) {
      clearSyscallTrace(ctx.exports);
      completeSupervisorFutexWait(ctx, "woken");
      break;
    }
    if (result === "ok") {
      clearSyscallTrace(ctx.exports);
      completeSupervisorFutexWait(ctx, "woken");
      break;
    }
    if (result === "not-equal") {
      clearSyscallTrace(ctx.exports);
      completeSupervisorFutexWait(ctx, "not-equal");
      break;
    }
  }

  const resumedStatus = ctx.exports.thread_status(ctx.vmPtr);
  ctx.postStatus(createThreadWorkerStatusOutcome(resumedStatus));
  return false;
}

type SupervisorFutexWaitCompletion =
  | "woken"
  | "not-equal"
  | "invalid-address"
  | "signal-check";

function completeSupervisorFutexWait(
  ctx: ThreadWorkerBlockingContext,
  completion: SupervisorFutexWaitCompletion,
): void {
  completeSupervisorFutexWaitByExports(ctx.exports, ctx.vmPtr, completion);
}

function completeSupervisorFutexWaitByExports(
  exports: KernelExports,
  vmPtr: number,
  completion: SupervisorFutexWaitCompletion,
): void {
  const complete = exports.supervisor_complete_futex_wait;
  if (complete === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_complete_futex_wait");
  }

  complete.call(
    exports,
    vmPtr,
    supervisorFutexWaitCompletionCode(exports, completion),
  );
}

function requireSupervisorCompletionCode(
  exports: KernelExports,
  name: keyof Pick<
    KernelExports,
    | "supervisor_futex_wait_completion_woken"
    | "supervisor_futex_wait_completion_not_equal"
    | "supervisor_futex_wait_completion_invalid_address"
    | "supervisor_futex_wait_completion_signal_check"
  >,
): number {
  const readCode = exports[name];
  if (readCode === undefined) {
    throw new Error(`kernel supervisor ABI missing ${name}`);
  }
  return readCode();
}

function supervisorFutexWaitCompletionCode(
  exports: KernelExports,
  completion: SupervisorFutexWaitCompletion,
): number {
  switch (completion) {
    case "woken":
      return requireSupervisorCompletionCode(exports, "supervisor_futex_wait_completion_woken");
    case "not-equal":
      return requireSupervisorCompletionCode(exports, "supervisor_futex_wait_completion_not_equal");
    case "invalid-address":
      return requireSupervisorCompletionCode(exports, "supervisor_futex_wait_completion_invalid_address");
    case "signal-check":
      return requireSupervisorCompletionCode(exports, "supervisor_futex_wait_completion_signal_check");
    default: {
      const exhaustive: never = completion;
      return exhaustive;
    }
  }
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
