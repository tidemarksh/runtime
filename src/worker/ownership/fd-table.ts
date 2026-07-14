import type { KernelWorkerFdEntrySnapshot } from "../../messages.js";
import { readRuntimeFdAuthorityEpoch } from "../../bridge/runtime-state.js";
import type { ProcessHandle } from "../types.js";
import { networkClosedFds } from "../network-socket-state.js";
import {
  networkConnectionOfds,
  restoreNetworkOfdSlotWatermarks,
} from "./socket-control.js";

interface ApplyThreadFdEntriesOptions {
  appendStatusSocketDebug?: (handle: ProcessHandle, event: string) => void;
  ignoreSnapshotGeneration?: boolean;
  preserveClosedDescriptorsFromStaleSnapshots?: boolean;
  preserveLiveDescriptorsFromStaleCloseSnapshots?: boolean;
  preserveLiveEpollDependentDescriptors?: boolean;
  preserveLiveRegularFileDescriptors?: boolean;
  syscallNr?: bigint;
}

const SUPERVISOR_FD_ENTRY_RESTORE_SYSCALL_NONE = 0xffffffffffffffffn;
const FD_APPLY_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FD_APPLY_DEBUG ?? "") === "1";
const FD_APPLY_DEBUG_FDS = new Set(
  (((globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_FD_APPLY_DEBUG_FDS ?? "3,4,5")
    .split(",")
    .map((value) => Number.parseInt(value, 10))
    .filter(Number.isSafeInteger)),
);

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

function processFdGeneration(handle: ProcessHandle, vmPtr: number, fd: number): number | undefined {
  const generation = handle.kernel.process_fd_generation?.(vmPtr, fd);
  return typeof generation === "number" && Number.isSafeInteger(generation)
    ? generation
    : undefined;
}

function fdEntryMatchesState(
  entry: KernelWorkerFdEntrySnapshot,
  currentOfdId: number,
  currentFdFlags: number,
): boolean {
  return currentOfdId === entry.ofdId && currentFdFlags === entry.fdFlags;
}

function fdEntrySnapshotIsOlderThanGeneration(
  entry: KernelWorkerFdEntrySnapshot,
  currentGeneration: number | undefined,
): boolean {
  if (entry.generation === undefined) {
    return false;
  }
  return currentGeneration !== undefined && currentGeneration > entry.generation;
}

function syscallMayCreateFdEntries(handle: ProcessHandle, syscallNr: bigint): boolean {
  const maybeFdCreatingSyscalls = [
    handle.kernel.abi_sys_openat?.(),
    handle.kernel.abi_sys_pipe2?.(),
    handle.kernel.abi_sys_epoll_create1?.(),
    handle.kernel.abi_sys_dup?.(),
    handle.kernel.abi_sys_dup3?.(),
    handle.kernel.abi_sys_fcntl?.(),
    handle.kernel.abi_sys_socket?.(),
    handle.kernel.abi_sys_socketpair?.(),
    handle.kernel.abi_sys_accept4?.(),
    handle.kernel.abi_sys_eventfd2?.(),
  ];
  return maybeFdCreatingSyscalls.some((candidate) => {
    if (candidate === undefined) {
      return false;
    }
    return (typeof candidate === "bigint" ? candidate : BigInt(candidate)) === syscallNr;
  });
}

function ofdIsKnownDescriptor(handle: ProcessHandle, ofdId: number): boolean {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return false;
  }
  return handle.kernel.kernel_ofd_fd_type(ofdId) > 0;
}

function staleSnapshotWouldReopenClosedFd(
  handle: ProcessHandle,
  entry: KernelWorkerFdEntrySnapshot,
  currentOfdId: number,
  currentGeneration: number | undefined,
  restoreSyscallNr: bigint,
): boolean {
  return fdEntrySnapshotIsOlderThanGeneration(entry, currentGeneration)
    && currentOfdId === 0
    && entry.ofdId !== 0
    && ofdIsKnownDescriptor(handle, entry.ofdId)
    && !syscallMayCreateFdEntries(handle, restoreSyscallNr);
}

function staleSnapshotWouldCloseLiveFd(
  entry: KernelWorkerFdEntrySnapshot,
  currentOfdId: number,
  currentGeneration: number | undefined,
): boolean {
  return fdEntrySnapshotIsOlderThanGeneration(entry, currentGeneration)
    && currentOfdId !== 0
    && entry.ofdId === 0;
}

function debugFdEntryApply(
  reason: string,
  vmPtr: number,
  entry: KernelWorkerFdEntrySnapshot,
  currentOfdId: number,
  currentFdFlags: number,
  currentGeneration: number | undefined,
  restoreSyscallNr: bigint,
): void {
  if (!FD_APPLY_DEBUG_ENABLED || !FD_APPLY_DEBUG_FDS.has(entry.fd)) {
    return;
  }
  console.error(
    "[tidemark-fd-apply] "
      + `reason=${reason}:vm=0x${vmPtr.toString(16)}:fd=${entry.fd}`
      + `:snapshot_ofd=${entry.ofdId}:snapshot_flags=${entry.fdFlags}`
      + `:snapshot_gen=${entry.generation ?? "n/a"}`
      + `:current_ofd=${currentOfdId}:current_flags=${currentFdFlags}`
      + `:current_gen=${currentGeneration ?? "n/a"}`
      + `:syscall=${restoreSyscallNr.toString()}`,
  );
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

export function supervisorProcessFdTableIsCoherent(handle: ProcessHandle, vmPtr: number): boolean {
  const processFdTableIsCoherent = handle.kernel.supervisor_process_fd_table_is_coherent;
  if (!processFdTableIsCoherent) {
    throw new Error("kernel supervisor ABI missing supervisor_process_fd_table_is_coherent");
  }
  return vmPtr !== 0 && processFdTableIsCoherent.call(handle.kernel, vmPtr) === 1;
}

export function describeSupervisorProcessFdTableIncoherence(
  handle: ProcessHandle,
  vmPtr: number,
): string {
  if (vmPtr === 0) {
    return "vm=0";
  }
  const maxFds = handle.kernel.abi_max_fds();
  for (let fd = 0; fd < maxFds; fd += 1) {
    const rawOfdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (rawOfdId === 0) {
      continue;
    }
    const refcount = rawOfdId < maxFds
      ? handle.kernel.kernel_ofd_refcount(rawOfdId)
      : 0;
    if (rawOfdId >= maxFds || refcount <= 0) {
      return `fd=${fd}:raw-ofd=${rawOfdId}:refcount=${refcount}:max=${maxFds}`;
    }
  }
  return "process-state-unavailable";
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
  const sourceProcessPtr = handle.kernel.thread_process_ptr?.(sourceVmPtr) ?? 0;
  const targetProcessPtr = handle.kernel.thread_process_ptr?.(vmPtr) ?? 0;
  const monotonicGeneration = sourceProcessPtr !== 0 && sourceProcessPtr === targetProcessPtr;
  const maxFds = handle.kernel.abi_max_fds();
  const fdEntries: KernelWorkerFdEntrySnapshot[] = [];
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = supervisorProcessOfdForFd(handle, sourceVmPtr, fd);
    fdEntries.push({
      fd,
      ofdId,
      fdFlags: supervisorProcessFdFlagsForFd(handle, sourceVmPtr, fd),
      authorityEpoch: readRuntimeFdAuthorityEpoch(handle.runtimeBridge, fd),
      generation: handle.kernel.process_fd_generation?.(sourceVmPtr, fd),
      ...(monotonicGeneration ? { monotonicGeneration: true } : {}),
    });
  }
  return fdEntries.length > 0 ? fdEntries : undefined;
}

export function snapshotAuthoritativeProcessFdEntriesForThread(
  handle: ProcessHandle,
  vmPtr: number,
): KernelWorkerFdEntrySnapshot[] | undefined {
  return snapshotProcessFdEntriesForThread(handle, vmPtr)?.map((entry) => ({
    fd: entry.fd,
    ofdId: entry.ofdId,
    fdFlags: entry.fdFlags,
    authorityEpoch: entry.authorityEpoch,
  }));
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
    const currentOfdId = supervisorProcessOfdForFd(handle, vmPtr, entry.fd);
    const currentFdFlags = supervisorProcessFdFlagsForFd(handle, vmPtr, entry.fd);
    const currentGeneration = processFdGeneration(handle, vmPtr, entry.fd);
    if (fdEntryMatchesState(entry, currentOfdId, currentFdFlags)) {
      debugFdEntryApply("skip-current", vmPtr, entry, currentOfdId, currentFdFlags, currentGeneration, restoreSyscallNr);
      continue;
    }
    if (
      options.ignoreSnapshotGeneration !== true
      && fdEntrySnapshotIsOlderThanGeneration(entry, currentGeneration)
    ) {
      debugFdEntryApply("skip-stale-generation", vmPtr, entry, currentOfdId, currentFdFlags, currentGeneration, restoreSyscallNr);
      continue;
    }
    if (
      options.preserveClosedDescriptorsFromStaleSnapshots === true
      && staleSnapshotWouldReopenClosedFd(
        handle,
        entry,
        currentOfdId,
        currentGeneration,
        restoreSyscallNr,
      )
    ) {
      debugFdEntryApply("skip-stale-closed-fd", vmPtr, entry, currentOfdId, currentFdFlags, currentGeneration, restoreSyscallNr);
      continue;
    }
    if (
      options.preserveLiveDescriptorsFromStaleCloseSnapshots === true
      && staleSnapshotWouldCloseLiveFd(entry, currentOfdId, currentGeneration)
    ) {
      debugFdEntryApply("skip-stale-live-fd", vmPtr, entry, currentOfdId, currentFdFlags, currentGeneration, restoreSyscallNr);
      continue;
    }
    if (
      restoreAction
      && restoreAction(vmPtr, entry.fd, entry.ofdId, restoreSyscallNr, restoreFlags) === restoreActionSkip
    ) {
      debugFdEntryApply("skip-restore-action", vmPtr, entry, currentOfdId, currentFdFlags, currentGeneration, restoreSyscallNr);
      continue;
    }
    debugFdEntryApply("apply", vmPtr, entry, currentOfdId, currentFdFlags, currentGeneration, restoreSyscallNr);
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
