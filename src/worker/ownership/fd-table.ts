import type { KernelWorkerFdEntrySnapshot } from "../../messages.js";
import type { ProcessHandle } from "../types.js";
import {
  networkConnectionOfds,
  restoreNetworkOfdSlotWatermarks,
} from "./socket-control.js";

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

export function applyThreadFdEntries(
  handle: ProcessHandle,
  vmPtr: number,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  appendStatusSocketDebug?: (handle: ProcessHandle, event: string) => void,
): void {
  if (!fdEntries || vmPtr === 0) {
    return;
  }
  restoreNetworkOfdSlotWatermarks(handle, appendStatusSocketDebug);
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
