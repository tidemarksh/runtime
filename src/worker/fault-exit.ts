import type { ProcessHandle } from "./types.js";
import { processHasLiveThreads } from "./live-threads.js";

export interface FaultExitPayload {
  exitCode: number;
  termSignal: number;
  syscallNr?: bigint;
  vmPtr: number;
}

function optionalBigInt(value: bigint | number | undefined): bigint | undefined {
  return value === undefined ? undefined : BigInt(value);
}

function matchesSyscall(
  syscallNr: bigint | undefined,
  candidate: (() => bigint | number) | undefined,
): boolean {
  return syscallNr !== undefined
    && candidate !== undefined
    && syscallNr === BigInt(candidate());
}

export function faultPayloadCanCompleteProcessExit(
  handle: ProcessHandle,
  payload: FaultExitPayload,
): boolean {
  const kernelExitCode = handle.kernel.thread_exit_code(payload.vmPtr);
  const kernelTermSignal = handle.kernel.thread_term_signal(payload.vmPtr);
  const terminationReason = handle.kernel.thread_termination_reason?.(payload.vmPtr) ?? 0;
  if (kernelExitCode !== 0 || kernelTermSignal !== 0 || terminationReason !== 0) {
    return true;
  }
  if (payload.exitCode !== 0 || payload.termSignal !== 0) {
    return false;
  }
  const syscallNr = payload.syscallNr ?? optionalBigInt(handle.kernel.thread_a7?.(payload.vmPtr));
  return matchesSyscall(syscallNr, handle.kernel.abi_sys_exit)
    || matchesSyscall(syscallNr, handle.kernel.abi_sys_exit_group);
}

export function rememberFaultExitPayload(
  handle: ProcessHandle,
  payload: FaultExitPayload,
): void {
  if (payload.exitCode === 0 && payload.termSignal === 0) {
    return;
  }
  handle.pendingFaultExitPayload = {
    exitCode: payload.exitCode,
    termSignal: payload.termSignal,
    vmPtr: payload.vmPtr,
  };
}

export function consumeFaultExitPayloadForDeadProcess(
  handle: ProcessHandle,
  vmPtr: number,
): { exitCode: number; termSignal: number; vmPtr: number } | null {
  const pending = handle.pendingFaultExitPayload;
  if (!pending) {
    return null;
  }
  if (handle.kernel.thread_process_thread_count(vmPtr) !== 0) {
    return null;
  }
  if (processHasLiveThreads(handle.kernel, vmPtr)) {
    return null;
  }
  handle.pendingFaultExitPayload = undefined;
  return pending;
}

export function formatAmbiguousFaultExitPayload(
  handle: ProcessHandle,
  payload: FaultExitPayload,
): string {
  const syscallNr = payload.syscallNr ?? optionalBigInt(handle.kernel.thread_a7?.(payload.vmPtr));
  const currentPid = handle.kernel.process_current_pid(payload.vmPtr);
  const threadCount = handle.kernel.thread_process_thread_count(payload.vmPtr);
  return `ambiguous-fault-exit:${handle.pid}:vm=0x${payload.vmPtr.toString(16)}`
    + `:exit=${payload.exitCode}:${payload.termSignal}`
    + `:kernel_exit=${handle.kernel.thread_exit_code(payload.vmPtr)}`
    + `:${handle.kernel.thread_term_signal(payload.vmPtr)}`
    + `:reason=${handle.kernel.thread_termination_reason?.(payload.vmPtr) ?? 0}`
    + `:a7=${syscallNr?.toString() ?? "n/a"}`
    + `:fault=${handle.kernel.thread_fault_kind?.(payload.vmPtr) ?? 0}`
    + `:${handle.kernel.thread_fault_signal?.(payload.vmPtr) ?? 0}`
    + `:pidField=${currentPid}`
    + `:threads=${threadCount}`
    + `:live=${processHasLiveThreads(handle.kernel, payload.vmPtr) ? 1 : 0}`
    + `:liveExcludingSelf=${processHasLiveThreads(handle.kernel, payload.vmPtr, payload.vmPtr) ? 1 : 0}`;
}
