import type { ProcessHandle } from "../types.js";

const BLOCKED_ADDR_NOT_FOUND = 0xffff_ffff_ffff_ffffn;

function threadRecordBlockedAddr(
  handle: ProcessHandle,
  vmPtr: number,
): bigint {
  return handle.kernel.thread_record_blocked_addr?.(vmPtr) ?? 0n;
}

function isKnownThreadRecordBlock(blockedAddr: bigint): boolean {
  return blockedAddr !== 0n && blockedAddr !== BLOCKED_ADDR_NOT_FOUND;
}

export function threadHasCanonicalFutexBlock(
  handle: ProcessHandle,
  vmPtr: number,
): boolean {
  const blockedAddr = threadRecordBlockedAddr(handle, vmPtr);
  if (!isKnownThreadRecordBlock(blockedAddr)) {
    return false;
  }
  if (handle.kernel.thread_status(vmPtr) === handle.kernel.abi_status_futex_wait()) {
    return true;
  }
  return (handle.kernel.thread_futex_wait_addr?.(vmPtr) ?? 0n) !== 0n;
}
