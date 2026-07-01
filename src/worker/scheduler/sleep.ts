import type { ProcessHandle } from "../types.js";

const GUEST_TICKS_PER_SEC = 100_000_000n;

export interface SleepScheduler {
  monotonicTicksCurrent(handle: ProcessHandle): bigint;
  advanceGuestTimeByMs(handle: ProcessHandle, timeoutMs: number): void;
}

export function createSleepScheduler(): SleepScheduler {
  function monotonicTicksCurrent(handle: ProcessHandle): bigint {
    const ticks = handle.kernel.monotonic_ticks_current?.();
    return ticks === undefined ? 0n : BigInt(ticks);
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
    advanceGuestTimeByMs,
  };
}
