import type { KernelExports, StatusCodes } from "../abi.js";

export interface SyscallNumbers {
  read: bigint | null;
  pipe2: bigint | null;
}

function readOptionalU64(value: bigint | number | undefined): bigint | null {
  if (value === undefined) {
    return null;
  }
  return typeof value === "bigint" ? value : BigInt(value);
}

export function loadSyscallNumbers(exports: KernelExports): SyscallNumbers {
  return {
    read: readOptionalU64(exports.abi_sys_read?.()),
    pipe2: readOptionalU64(exports.abi_sys_pipe2?.()),
  };
}

export function readCompletedSyncEffects(
  exports: KernelExports,
  statusCodes: StatusCodes,
  status: number,
  syscallNumbers: SyscallNumbers | null,
  vmPtr = 0,
): number {
  void statusCodes;
  void syscallNumbers;
  const takeCompletedSyncEffects = exports.supervisor_thread_take_completed_sync_effects;
  if (typeof takeCompletedSyncEffects !== "function") {
    throw new Error("kernel supervisor ABI missing supervisor_thread_take_completed_sync_effects");
  }
  return takeCompletedSyncEffects(vmPtr, status);
}
