import {
  supervisorOfdFdType as supervisorOfdFdTypeForExports,
  supervisorOfdHostFd as supervisorOfdHostFdForExports,
  supervisorOfdIno as supervisorOfdInoForExports,
  supervisorOfdOffset as supervisorOfdOffsetForExports,
  supervisorOfdReconcileRefcount as supervisorOfdReconcileRefcountForExports,
  supervisorOfdRefcount as supervisorOfdRefcountForExports,
  supervisorOfdStatusFlags as supervisorOfdStatusFlagsForExports,
} from "../../abi.js";
import type { ProcessHandle } from "../types.js";

type KernelHandle = Pick<ProcessHandle, "kernel">;

export function supervisorOfdFdType(handle: KernelHandle, ofdId: number): number {
  return supervisorOfdFdTypeForExports(handle.kernel, ofdId);
}

export function supervisorOfdHostFd(handle: KernelHandle, ofdId: number): number {
  return supervisorOfdHostFdForExports(handle.kernel, ofdId);
}

export function supervisorOfdStatusFlags(handle: KernelHandle, ofdId: number): number {
  return supervisorOfdStatusFlagsForExports(handle.kernel, ofdId);
}

export function supervisorOfdIno(handle: KernelHandle, ofdId: number): number {
  return supervisorOfdInoForExports(handle.kernel, ofdId);
}

export function supervisorOfdRefcount(handle: KernelHandle, ofdId: number): number {
  return supervisorOfdRefcountForExports(handle.kernel, ofdId);
}

export function supervisorOfdReconcileRefcount(
  handle: KernelHandle,
  ofdId: number,
  expectedRefcount: number,
): number {
  return supervisorOfdReconcileRefcountForExports(handle.kernel, ofdId, expectedRefcount);
}

export function supervisorOfdOffset(handle: KernelHandle, ofdId: number): bigint {
  return supervisorOfdOffsetForExports(handle.kernel, ofdId);
}
