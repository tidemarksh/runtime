import type { KernelExports, StatusCodes } from "../abi.js";

export function readCompletedSyncEffects(
  exports: KernelExports,
  statusCodes: StatusCodes,
  status: number,
  vmPtr = 0,
): number {
  void statusCodes;
  const takeCompletedSyncEffects = exports.supervisor_thread_take_completed_sync_effects;
  if (typeof takeCompletedSyncEffects !== "function") {
    throw new Error("kernel supervisor ABI missing supervisor_thread_take_completed_sync_effects");
  }
  return takeCompletedSyncEffects(vmPtr, status);
}
