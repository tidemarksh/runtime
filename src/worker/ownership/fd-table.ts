import type { KernelWorkerFdEntrySnapshot } from "../../messages.js";
import type { ProcessHandle } from "../types.js";
import {
  networkConnectionOfds,
  restoreNetworkOfdSlotWatermarks,
} from "./socket-control.js";

interface ApplyThreadFdEntriesOptions {
  appendStatusSocketDebug?: (handle: ProcessHandle, event: string) => void;
  preserveLiveDescriptors?: boolean;
  syscallNr?: bigint;
  fdTraceArg0?: bigint;
  fdTraceArg1?: bigint;
  fdTraceResult?: bigint;
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
  const maxFds = handle.kernel.abi_max_fds();
  const fdEntries: KernelWorkerFdEntrySnapshot[] = [];
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    fdEntries.push({
      fd,
      ofdId,
      fdFlags: handle.kernel.process_fd_flags?.(vmPtr, fd) ?? 0,
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

export function syscallMayClearFdEntries(handle: ProcessHandle, syscallNr: bigint | undefined): boolean {
  return matchesOptionalSyscall(syscallNr, handle.kernel.abi_sys_close)
    || matchesOptionalSyscall(syscallNr, handle.kernel.abi_sys_close_range)
    || matchesOptionalSyscall(syscallNr, handle.kernel.abi_sys_execve);
}

export function syscallMayClearFdEntry(
  handle: ProcessHandle,
  fd: number,
  syscallNr: bigint | undefined,
  fdTraceArg0: bigint | undefined,
  fdTraceArg1: bigint | undefined,
  fdTraceResult: bigint | undefined,
): boolean {
  if (syscallNr === undefined) {
    return false;
  }
  if (fdTraceResult !== undefined && BigInt.asIntN(64, fdTraceResult) < 0n) {
    return false;
  }
  if (matchesOptionalSyscall(syscallNr, handle.kernel.abi_sys_execve)) {
    return true;
  }
  if (matchesOptionalSyscall(syscallNr, handle.kernel.abi_sys_close)) {
    return fdTraceArg0 !== undefined && BigInt(fd) === fdTraceArg0;
  }
  if (matchesOptionalSyscall(syscallNr, handle.kernel.abi_sys_close_range)) {
    if (fdTraceArg0 === undefined || fdTraceArg1 === undefined) {
      return false;
    }
    const first = fdTraceArg0 <= fdTraceArg1 ? fdTraceArg0 : fdTraceArg1;
    const last = fdTraceArg0 <= fdTraceArg1 ? fdTraceArg1 : fdTraceArg0;
    const candidate = BigInt(fd);
    return candidate >= first && candidate <= last;
  }
  return false;
}

function fdEntryWouldClearLiveDescriptor(
  handle: ProcessHandle,
  vmPtr: number,
  entry: KernelWorkerFdEntrySnapshot,
  options: ApplyThreadFdEntriesOptions,
): boolean {
  if (
    entry.ofdId !== 0
    || syscallMayClearFdEntry(
      handle,
      entry.fd,
      options.syscallNr,
      options.fdTraceArg0,
      options.fdTraceArg1,
      options.fdTraceResult,
    )
  ) {
    return false;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const currentOfdId = handle.kernel.process_fd_ofd_id?.(vmPtr, entry.fd) ?? 0;
  if (currentOfdId <= 0 || currentOfdId >= maxFds) {
    return false;
  }
  const currentType = handle.kernel.kernel_ofd_fd_type(currentOfdId);
  return currentType > 0 && currentType <= 64;
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
    if (
      entry.fd < 0
      || entry.fd >= maxFds
      || entry.ofdId < 0
      || entry.ofdId >= maxFds
    ) {
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
      options.preserveLiveDescriptors === true
      && fdEntryWouldClearLiveDescriptor(handle, vmPtr, entry, options)
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
