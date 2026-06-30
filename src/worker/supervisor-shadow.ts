import type { KernelExports } from "../abi.js";

export const SUPERVISOR_SHADOW_THREAD_EVENT_MAGIC = 0x5645_5354;
export const SUPERVISOR_SHADOW_THREAD_EVENT_VERSION = 1;
export const SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES = 64;

export interface SupervisorShadowKernelExports extends KernelExports {
  supervisor_abi_version(): number;
  supervisor_submit_thread_event(eventPtr: number, eventLen: number): number;
}

export interface SupervisorShadowThreadEvent {
  sequence?: bigint | number;
  vmPtr: bigint | number;
  pid: number;
  tid: number;
  status: number;
  syncEffects?: number;
  blockedReason?: number;
  pc?: bigint | number;
  sp?: bigint | number;
}

export function hasSupervisorShadowProtocol(
  kernelExports: KernelExports,
): kernelExports is SupervisorShadowKernelExports {
  return typeof kernelExports.supervisor_abi_version === "function"
    && typeof kernelExports.supervisor_submit_thread_event === "function";
}

export function encodeSupervisorShadowThreadEvent(
  view: DataView,
  offset: number,
  event: SupervisorShadowThreadEvent,
): void {
  if (offset < 0 || offset + SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES > view.byteLength) {
    throw new RangeError("supervisor shadow thread event record does not fit in the target view");
  }

  view.setUint32(offset, SUPERVISOR_SHADOW_THREAD_EVENT_MAGIC, true);
  view.setUint32(offset + 4, SUPERVISOR_SHADOW_THREAD_EVENT_VERSION, true);
  view.setUint32(offset + 8, SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES, true);
  view.setUint32(offset + 12, event.status, true);
  view.setBigUint64(offset + 16, BigInt(event.sequence ?? 0), true);
  view.setBigUint64(offset + 24, BigInt(event.vmPtr), true);
  view.setUint32(offset + 32, event.pid, true);
  view.setUint32(offset + 36, event.tid, true);
  view.setUint32(offset + 40, event.syncEffects ?? 0, true);
  view.setUint32(offset + 44, event.blockedReason ?? 0, true);
  view.setBigUint64(offset + 48, BigInt(event.pc ?? 0), true);
  view.setBigUint64(offset + 56, BigInt(event.sp ?? 0), true);
}

export function submitSupervisorShadowThreadEvent(
  kernelExports: KernelExports,
  recordPtr: number,
  event: SupervisorShadowThreadEvent,
): number | undefined {
  if (!hasSupervisorShadowProtocol(kernelExports)) {
    return undefined;
  }

  encodeSupervisorShadowThreadEvent(new DataView(kernelExports.memory.buffer), recordPtr, event);
  return kernelExports.supervisor_submit_thread_event(
    recordPtr,
    SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES,
  );
}
