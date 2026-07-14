import type { ProcessHandle } from "../types.js";
import type { KernelMemoryWriteSnapshot } from "../contracts/snapshots.js";
import { kernelShareOtherMembers } from "./common.js";

/** Transport Kernel-owned process-pending signal bits to their target process views. */
export function transportProcessPendingSignals(source: ProcessHandle): ProcessHandle[] {
  const targets: ProcessHandle[] = [];
  const take = source.kernel.supervisor_signal_transport_take;
  if (take === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_signal_transport_take");
  }
  for (const target of kernelShareOtherMembers(source)) {
    const pending = take.call(source.kernel, target.pid);
    if (pending === 0n) {
      continue;
    }
    const merge = target.kernel.supervisor_signal_transport_merge;
    if (merge === undefined) {
      throw new Error("kernel supervisor ABI missing supervisor_signal_transport_merge");
    }
    const result = merge.call(target.kernel, target.pid, pending);
    if (result < 0) {
      throw new Error(`kernel supervisor signal transport failed: ${result}`);
    }
    targets.push(target);
  }
  return targets;
}

export function snapshotProcessPendingSignalState(
  handle: ProcessHandle,
): KernelMemoryWriteSnapshot[] {
  const ptr = handle.kernelState.processPendingPtr;
  const bytes = handle.kernelState.processPendingBytes ?? 0;
  if (ptr <= 0 || bytes <= 0 || ptr + bytes > handle.memory.buffer.byteLength) {
    return [];
  }
  return [{ ptr, bytes: new Uint8Array(handle.memory.buffer, ptr, bytes).slice() }];
}
