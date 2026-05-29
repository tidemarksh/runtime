import type {
  BlockedWorkerState,
  ProcessHandle,
} from "../types.js";

const GUEST_TICKS_PER_SEC = 100_000_000n;
const GUEST_NSEC_PER_SEC = 1_000_000_000n;

export interface SleepSchedulerDeps {
  clearBlockedWorker(handle: ProcessHandle, worker: Worker): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
}

export interface SleepScheduler {
  monotonicTicksCurrent(handle: ProcessHandle): bigint;
  sleepDurationToTicks(sec: bigint, nsec: bigint): bigint;
  sleepRetryDelayMs(handle: ProcessHandle, deadlineTicks: bigint): number;
  advanceGuestTimeByMs(handle: ProcessHandle, timeoutMs: number): void;
  sleepDeadlineTicks(handle: ProcessHandle, vmPtr: number): bigint;
  wakeBlockedSleepWorker(handle: ProcessHandle, worker: Worker, blocked: { vmPtr: number }): void;
  wakeDueSleepWorkers(handle: ProcessHandle): boolean;
  maybeAdvanceGuestTimeToNextSleep(handle: ProcessHandle): boolean;
}

export function createSleepScheduler(deps: SleepSchedulerDeps): SleepScheduler {
  function monotonicTicksCurrent(handle: ProcessHandle): bigint {
    const ticks = handle.kernel.monotonic_ticks_current?.();
    return ticks === undefined ? 0n : BigInt(ticks);
  }

  function sleepDurationToTicks(sec: bigint, nsec: bigint): bigint {
    const base = sec * GUEST_TICKS_PER_SEC;
    const sub = nsec * GUEST_TICKS_PER_SEC;
    const rounded = sub === 0n ? 0n : (sub + (GUEST_NSEC_PER_SEC - 1n)) / GUEST_NSEC_PER_SEC;
    return base + rounded;
  }

  function sleepRetryDelayMs(handle: ProcessHandle, deadlineTicks: bigint): number {
    const now = monotonicTicksCurrent(handle);
    if (deadlineTicks <= now) {
      return 0;
    }
    const remainingTicks = deadlineTicks - now;
    const delayMs = Number((remainingTicks * 1000n + (GUEST_TICKS_PER_SEC - 1n)) / GUEST_TICKS_PER_SEC);
    if (!Number.isFinite(delayMs)) {
      return 2_147_483_647;
    }
    return Math.max(0, Math.min(delayMs, 2_147_483_647));
  }

  function advanceGuestTimeByMs(handle: ProcessHandle, timeoutMs: number): void {
    if (timeoutMs <= 0 || !Number.isFinite(timeoutMs)) {
      return;
    }
    const deltaTicks = BigInt(Math.ceil(timeoutMs)) * GUEST_TICKS_PER_SEC / 1000n;
    handle.kernel.sync_monotonic_ticks_floor?.(monotonicTicksCurrent(handle) + deltaTicks);
  }

  function sleepDeadlineTicks(handle: ProcessHandle, vmPtr: number): bigint {
    const exportedDeadline = handle.kernel.thread_sleep_deadline_ticks?.(vmPtr);
    if (exportedDeadline !== undefined) {
      return BigInt(exportedDeadline);
    }
    const sec = handle.kernel.thread_sleep_seconds(vmPtr);
    const nsec = handle.kernel.thread_sleep_nanoseconds(vmPtr);
    return monotonicTicksCurrent(handle) + sleepDurationToTicks(sec, nsec);
  }

  function wakeBlockedSleepWorker(
    handle: ProcessHandle,
    worker: Worker,
    blocked: Pick<BlockedWorkerState, "vmPtr">,
  ): void {
    handle.kernel.thread_resume_sleep?.(blocked.vmPtr);
    deps.clearBlockedWorker(handle, worker);
    deps.enqueueWorker(handle, worker);
  }

  function wakeDueSleepWorkers(handle: ProcessHandle): boolean {
    const now = monotonicTicksCurrent(handle);
    let wokeAny = false;
    for (const [worker, blocked] of [...handle.blockedWorkers.entries()]) {
      if (
        blocked.status !== handle.kernel.abi_status_sleep()
        || blocked.sleepDeadlineTicks === null
        || blocked.sleepDeadlineTicks > now
      ) {
        continue;
      }
      wakeBlockedSleepWorker(handle, worker, blocked);
      wokeAny = true;
    }
    return wokeAny;
  }

  function maybeAdvanceGuestTimeToNextSleep(handle: ProcessHandle): boolean {
    if (handle.runningWorker !== null || handle.readyWorkers.length > 0) {
      return false;
    }
    let nextDeadline: bigint | null = null;
    for (const blocked of handle.blockedWorkers.values()) {
      if (blocked.status !== handle.kernel.abi_status_sleep() || blocked.sleepDeadlineTicks === null) {
        continue;
      }
      if (nextDeadline === null || blocked.sleepDeadlineTicks < nextDeadline) {
        nextDeadline = blocked.sleepDeadlineTicks;
      }
    }
    if (nextDeadline === null) {
      return false;
    }
    if (nextDeadline > monotonicTicksCurrent(handle)) {
      handle.kernel.sync_monotonic_ticks_floor?.(nextDeadline);
    }
    return wakeDueSleepWorkers(handle);
  }

  return {
    monotonicTicksCurrent,
    sleepDurationToTicks,
    sleepRetryDelayMs,
    advanceGuestTimeByMs,
    sleepDeadlineTicks,
    wakeBlockedSleepWorker,
    wakeDueSleepWorkers,
    maybeAdvanceGuestTimeToNextSleep,
  };
}
