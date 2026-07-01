import type { ProcessHandle } from "../types.js";

const GUEST_TICKS_PER_SEC = 100_000_000n;

export interface SleepScheduler {
  monotonicTicksCurrent(handle: ProcessHandle): bigint;
  sleepRetryDelayMs(handle: ProcessHandle, deadlineTicks: bigint): number;
  advanceGuestTimeByMs(handle: ProcessHandle, timeoutMs: number): void;
}

export function createSleepScheduler(): SleepScheduler {
  function monotonicTicksCurrent(handle: ProcessHandle): bigint {
    const ticks = handle.kernel.monotonic_ticks_current?.();
    return ticks === undefined ? 0n : BigInt(ticks);
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

  return {
    monotonicTicksCurrent,
    sleepRetryDelayMs,
    advanceGuestTimeByMs,
  };
}
