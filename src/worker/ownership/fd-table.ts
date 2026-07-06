import type { KernelWorkerFdEntrySnapshot } from "../../messages.js";
import type { ProcessHandle } from "../types.js";
import { networkClosedFds } from "../network-socket-state.js";
import {
  networkConnectionOfds,
  restoreNetworkOfdSlotWatermarks,
} from "./socket-control.js";

interface ApplyThreadFdEntriesOptions {
  appendStatusSocketDebug?: (handle: ProcessHandle, event: string) => void;
  preserveLiveEpollDependentDescriptors?: boolean;
  preserveLiveRegularFileDescriptors?: boolean;
  syscallNr?: bigint;
}

const SUPERVISOR_FD_ENTRY_RESTORE_SYSCALL_NONE = 0xffffffffffffffffn;

function requireSupervisorFdEntryRestoreAction(
  handle: ProcessHandle,
): (vmPtr: number, fd: number, snapshotOfdId: number, syscallNr: bigint | number, flags: number) => number {
  const restoreAction = handle.kernel.supervisor_fd_entry_restore_action;
  if (!restoreAction) {
    throw new Error("kernel supervisor ABI missing supervisor_fd_entry_restore_action");
  }
  return restoreAction.bind(handle.kernel);
}

function requireSupervisorFdEntryRestoreActionSkip(handle: ProcessHandle): number {
  const restoreActionSkip = handle.kernel.supervisor_fd_entry_restore_action_skip;
  if (!restoreActionSkip) {
    throw new Error("kernel supervisor ABI missing supervisor_fd_entry_restore_action_skip");
  }
  return restoreActionSkip.call(handle.kernel);
}

function requireSupervisorProcessFdIsOpen(handle: ProcessHandle): (vmPtr: number, fd: number) => number {
  const processFdIsOpen = handle.kernel.supervisor_process_fd_is_open;
  if (!processFdIsOpen) {
    throw new Error("kernel supervisor ABI missing supervisor_process_fd_is_open");
  }
  return processFdIsOpen.bind(handle.kernel);
}

function requireSupervisorProcessOfdForFd(handle: ProcessHandle): (vmPtr: number, fd: number) => number {
  const processOfdForFd = handle.kernel.supervisor_process_ofd_for_fd;
  if (!processOfdForFd) {
    throw new Error("kernel supervisor ABI missing supervisor_process_ofd_for_fd");
  }
  return processOfdForFd.bind(handle.kernel);
}

function requireSupervisorProcessFdFlagsForFd(handle: ProcessHandle): (vmPtr: number, fd: number) => number {
  const processFdFlagsForFd = handle.kernel.supervisor_process_fd_flags_for_fd;
  if (!processFdFlagsForFd) {
    throw new Error("kernel supervisor ABI missing supervisor_process_fd_flags_for_fd");
  }
  return processFdFlagsForFd.bind(handle.kernel);
}

export function supervisorProcessOfdForFd(handle: ProcessHandle, vmPtr: number, fd: number): number {
  if (vmPtr === 0 || fd < 0 || fd >= handle.kernel.abi_max_fds()) {
    return 0;
  }
  const ofdId = requireSupervisorProcessOfdForFd(handle)(vmPtr, fd);
  return ofdId > 0 && ofdId < handle.kernel.abi_max_fds() ? ofdId : 0;
}

export function supervisorProcessFdFlagsForFd(handle: ProcessHandle, vmPtr: number, fd: number): number {
  if (vmPtr === 0 || fd < 0 || fd >= handle.kernel.abi_max_fds()) {
    return 0;
  }
  return requireSupervisorProcessFdFlagsForFd(handle)(vmPtr, fd);
}

export function supervisorProcessFdIsOpen(handle: ProcessHandle, vmPtr: number, fd: number): boolean {
  if (vmPtr === 0 || fd < 0 || fd >= handle.kernel.abi_max_fds()) {
    return false;
  }
  return requireSupervisorProcessFdIsOpen(handle)(vmPtr, fd) === 1;
}

function supervisorFdEntryRestoreFlags(handle: ProcessHandle, options: ApplyThreadFdEntriesOptions): number {
  let flags = 0;
  if (options.preserveLiveRegularFileDescriptors === true) {
    const flag = handle.kernel.supervisor_fd_entry_restore_flag_preserve_live_regular_file;
    if (!flag) {
      throw new Error("kernel supervisor ABI missing supervisor_fd_entry_restore_flag_preserve_live_regular_file");
    }
    flags |= flag.call(handle.kernel);
  }
  if (options.preserveLiveEpollDependentDescriptors === true) {
    const flag = handle.kernel.supervisor_fd_entry_restore_flag_preserve_live_epoll_dependent;
    if (!flag) {
      throw new Error("kernel supervisor ABI missing supervisor_fd_entry_restore_flag_preserve_live_epoll_dependent");
    }
    flags |= flag.call(handle.kernel);
  }
  return flags;
}

function processPidForVm(handle: ProcessHandle, vmPtr: number): number | null {
  if (vmPtr === 0) {
    return null;
  }
  try {
    const pid = handle.kernel.process_current_pid?.(vmPtr);
    return typeof pid === "number" && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

function vmBelongsToHandleProcess(handle: ProcessHandle, vmPtr: number): boolean {
  if (vmPtr === handle.vmPtr) {
    return true;
  }
  if (vmPtr === 0 || handle.vmPtr === 0) {
    return false;
  }
  const targetPid = processPidForVm(handle, vmPtr);
  const ownerPid = processPidForVm(handle, handle.vmPtr) ?? (handle.pid > 0 ? handle.pid : null);
  return targetPid !== null && ownerPid !== null && targetPid === ownerPid;
}

export function canonicalFdTableVmPtrForThread(handle: ProcessHandle, vmPtr: number): number {
  return vmBelongsToHandleProcess(handle, vmPtr) ? handle.vmPtr : vmPtr;
}

export function snapshotProcessFdEntries(
  handle: ProcessHandle,
  vmPtr: number,
): KernelWorkerFdEntrySnapshot[] {
  const fdEntries: KernelWorkerFdEntrySnapshot[] = [];
  for (let fd = 0; fd < handle.kernel.abi_max_fds(); fd += 1) {
    fdEntries.push({
      fd,
      ofdId: supervisorProcessOfdForFd(handle, vmPtr, fd),
      fdFlags: supervisorProcessFdFlagsForFd(handle, vmPtr, fd),
      generation: handle.kernel.process_fd_generation?.(vmPtr, fd),
    });
  }
  return fdEntries;
}

export function snapshotProcessFdEntriesForThread(
  handle: ProcessHandle,
  vmPtr: number,
): KernelWorkerFdEntrySnapshot[] | undefined {
  if (vmPtr === 0) {
    return undefined;
  }
  const sourceVmPtr = canonicalFdTableVmPtrForThread(handle, vmPtr);
  const maxFds = handle.kernel.abi_max_fds();
  const fdEntries: KernelWorkerFdEntrySnapshot[] = [];
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = supervisorProcessOfdForFd(handle, sourceVmPtr, fd);
    fdEntries.push({
      fd,
      ofdId,
      fdFlags: supervisorProcessFdFlagsForFd(handle, sourceVmPtr, fd),
      generation: handle.kernel.process_fd_generation?.(sourceVmPtr, fd),
    });
  }
  return fdEntries.length > 0 ? fdEntries : undefined;
}

export function restoreProcessFdEntries(
  handle: ProcessHandle,
  vmPtr: number,
  entries: readonly KernelWorkerFdEntrySnapshot[],
): void {
  if (vmPtr === 0 || entries.length === 0 || !handle.kernel.process_set_fd_entry) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds();
  for (const entry of entries) {
    if (entry.fd < 0 || entry.fd >= maxFds || entry.ofdId < 0 || entry.ofdId >= maxFds) {
      continue;
    }
    handle.kernel.process_set_fd_entry(vmPtr, entry.fd, entry.ofdId, entry.fdFlags);
  }
}

export function applyThreadFdEntries(
  handle: ProcessHandle,
  vmPtr: number,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  options: ApplyThreadFdEntriesOptions = {},
): void {
  if (!fdEntries || vmPtr === 0) {
    return;
  }
  restoreNetworkOfdSlotWatermarks(handle, options.appendStatusSocketDebug);
  const maxFds = handle.kernel.abi_max_fds();
  const networkOfds = networkConnectionOfds(handle);
  const restoreFlags = supervisorFdEntryRestoreFlags(handle, options);
  const restoreAction = restoreFlags !== 0 ? requireSupervisorFdEntryRestoreAction(handle) : undefined;
  const restoreActionSkip = restoreFlags !== 0 ? requireSupervisorFdEntryRestoreActionSkip(handle) : 0;
  const restoreSyscallNr = options.syscallNr ?? SUPERVISOR_FD_ENTRY_RESTORE_SYSCALL_NONE;
  for (const entry of fdEntries) {
    if (entry.fd < 0 || entry.fd >= maxFds) {
      continue;
    }
    const expectedNetworkOfd = networkOfds.get(entry.fd);
    if (
      expectedNetworkOfd !== undefined
      && entry.ofdId !== 0
      && entry.ofdId !== expectedNetworkOfd
      && !networkClosedFds(handle).has(entry.fd)
    ) {
      continue;
    }
    if (
      restoreAction
      && restoreAction(vmPtr, entry.fd, entry.ofdId, restoreSyscallNr, restoreFlags) === restoreActionSkip
    ) {
      continue;
    }
    handle.kernel.process_set_fd_entry(vmPtr, entry.fd, entry.ofdId, entry.fdFlags);
  }
}

export function ofdIdsFromFdEntries(
  handle: ProcessHandle,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[],
): number[] {
  const maxFds = handle.kernel.abi_max_fds();
  return [...new Set(
    fdEntries
      .map((entry) => entry.ofdId)
      .filter((ofdId) => ofdId > 0 && ofdId < maxFds),
  )];
}
