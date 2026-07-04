import type { ProcessHandle } from "../types.js";
import { supervisorProcessOwnerMatches } from "../process-identity.js";
import {
  supervisorProcessFdFlagsForFd,
  supervisorProcessOfdForFd,
} from "./fd-table.js";

type StandardFdIntegrityCheck = (handle: ProcessHandle, context: string) => void;

export interface OwnershipInvariantDeps {
  assertProcessStandardFdIntegrity: StandardFdIntegrityCheck;
}

function assertThreadBelongsToHandle(
  handle: ProcessHandle,
  vmPtr: number,
  context: string,
): void {
  if (vmPtr === 0) {
    throw new Error(`${context}: missing vmPtr for pid=${handle.pid}`);
  }
  if (!supervisorProcessOwnerMatches(handle.kernel, vmPtr, handle.pid)) {
    const currentPid = handle.kernel.process_current_pid(vmPtr);
    throw new Error(
      `${context}: thread owner pid drift expected=${handle.pid} actual=${currentPid}`
        + ` vm=0x${vmPtr.toString(16)}`,
    );
  }
  const processPtr = handle.kernel.thread_process_ptr(vmPtr);
  if (processPtr === 0) {
    throw new Error(`${context}: missing process pointer pid=${handle.pid} vm=0x${vmPtr.toString(16)}`);
  }
}

function assertOpenFdOfdsAreValid(
  handle: ProcessHandle,
  vmPtr: number,
  context: string,
): void {
  const maxFds = handle.kernel.abi_max_fds();
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = supervisorProcessOfdForFd(handle, vmPtr, fd);
    if (ofdId === 0) {
      continue;
    }
    if (ofdId < 0 || ofdId >= maxFds) {
      throw new Error(`${context}: fd=${fd} invalid ofd=${ofdId} max=${maxFds}`);
    }
    const type = handle.kernel.kernel_ofd_fd_type(ofdId);
    if (type <= 0 || type > 64) {
      throw new Error(`${context}: fd=${fd} ofd=${ofdId} invalid type=${type}`);
    }
  }
}

function assertNoCloexecDescriptors(
  handle: ProcessHandle,
  vmPtr: number,
  context: string,
): void {
  const cloexecFlag = handle.kernel.abi_o_cloexec?.() ?? 0;
  if (cloexecFlag === 0) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds();
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = supervisorProcessOfdForFd(handle, vmPtr, fd);
    if (ofdId === 0) {
      continue;
    }
    const fdFlags = supervisorProcessFdFlagsForFd(handle, vmPtr, fd);
    if ((fdFlags & cloexecFlag) !== 0) {
      throw new Error(`${context}: exec child retained CLOEXEC fd=${fd} ofd=${ofdId}`);
    }
  }
}

export function assertForkChildReadyOwnership(
  deps: OwnershipInvariantDeps,
  handle: ProcessHandle,
  vmPtr: number,
  context: string,
): void {
  assertThreadBelongsToHandle(handle, vmPtr, context);
  assertOpenFdOfdsAreValid(handle, vmPtr, context);
  deps.assertProcessStandardFdIntegrity(handle, context);
}

export function assertExecveChildReadyOwnership(
  deps: OwnershipInvariantDeps,
  handle: ProcessHandle,
  vmPtr: number,
  context: string,
): void {
  assertThreadBelongsToHandle(handle, vmPtr, context);
  assertOpenFdOfdsAreValid(handle, vmPtr, context);
  assertNoCloexecDescriptors(handle, vmPtr, context);
  deps.assertProcessStandardFdIntegrity(handle, context);
}

export function assertVforkParentResumeOwnership(
  deps: OwnershipInvariantDeps,
  handle: ProcessHandle,
  vmPtr: number,
  context: string,
): void {
  assertThreadBelongsToHandle(handle, vmPtr, context);
  deps.assertProcessStandardFdIntegrity(handle, context);
}
