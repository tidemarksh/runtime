import type { KernelWorkerFdEntrySnapshot } from "../../messages.js";
import type { ProcessHandle } from "../types.js";
import { collectHeldEpollDependentOfdIdsByType } from "./ofd-table.js";
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
      ofdId: handle.kernel.process_fd_ofd_id(vmPtr, fd),
      fdFlags: handle.kernel.process_fd_flags(vmPtr, fd),
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
    const ofdId = handle.kernel.process_fd_ofd_id(sourceVmPtr, fd);
    fdEntries.push({
      fd,
      ofdId,
      fdFlags: handle.kernel.process_fd_flags?.(sourceVmPtr, fd) ?? 0,
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

function matchesOptionalSyscall(syscallNr: bigint | undefined, candidate: (() => bigint | number) | undefined): boolean {
  return syscallNr !== undefined && candidate !== undefined && syscallNr === BigInt(candidate());
}

function syscallMayClearFdEntries(handle: ProcessHandle, syscallNr: bigint | undefined): boolean {
  return matchesOptionalSyscall(syscallNr, handle.kernel.abi_sys_close)
    || matchesOptionalSyscall(syscallNr, handle.kernel.abi_sys_close_range)
    || matchesOptionalSyscall(syscallNr, handle.kernel.abi_sys_execve);
}

function fdEntryWouldClobberLiveRegularFile(
  handle: ProcessHandle,
  vmPtr: number,
  entry: KernelWorkerFdEntrySnapshot,
  syscallNr: bigint | undefined,
): boolean {
  if (syscallMayClearFdEntries(handle, syscallNr)) {
    return false;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const currentOfdId = handle.kernel.process_fd_ofd_id?.(vmPtr, entry.fd) ?? 0;
  if (currentOfdId <= 0 || currentOfdId >= maxFds || currentOfdId === entry.ofdId) {
    return false;
  }
  return handle.kernel.kernel_ofd_fd_type(currentOfdId) === handle.kernel.abi_fd_type_file?.()
    && handle.kernel.kernel_ofd_refcount(currentOfdId) > 0;
}

function fdEntryWouldClobberEpollDependentOfd(
  handle: ProcessHandle,
  vmPtr: number,
  entry: KernelWorkerFdEntrySnapshot,
  syscallNr: bigint | undefined,
): boolean {
  if (syscallMayClearFdEntries(handle, syscallNr)) {
    return false;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const currentOfdId = handle.kernel.process_fd_ofd_id?.(vmPtr, entry.fd) ?? 0;
  if (currentOfdId <= 0 || currentOfdId >= maxFds || currentOfdId === entry.ofdId) {
    return false;
  }
  let currentType = 0;
  try {
    currentType = handle.kernel.kernel_ofd_fd_type(currentOfdId);
  } catch {
    return false;
  }
  if (currentType === 0) {
    return false;
  }
  try {
    return collectHeldEpollDependentOfdIdsByType(handle, [currentType]).includes(currentOfdId);
  } catch {
    return false;
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
  for (const entry of fdEntries) {
    if (entry.fd < 0 || entry.fd >= maxFds) {
      continue;
    }
    const expectedNetworkOfd = networkOfds.get(entry.fd);
    if (
      expectedNetworkOfd !== undefined
      && entry.ofdId !== 0
      && entry.ofdId !== expectedNetworkOfd
      && !handle.networkClosedFds.has(entry.fd)
    ) {
      continue;
    }
    if (
      options.preserveLiveRegularFileDescriptors === true
      && fdEntryWouldClobberLiveRegularFile(handle, vmPtr, entry, options.syscallNr)
    ) {
      continue;
    }
    if (
      options.preserveLiveEpollDependentDescriptors === true
      && fdEntryWouldClobberEpollDependentOfd(handle, vmPtr, entry, options.syscallNr)
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
