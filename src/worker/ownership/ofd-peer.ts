import type { ProcessHandle } from "../types.js";
import {
  collectProcessHeldOfds,
  trackForkSharedOfds,
} from "./ofd-table.js";
import {
  hasPipePeerInOtherKernelShareMember,
  isProcessHostStdioOfd,
  isProcessHostStdioPipeOfd,
} from "./pipe-control.js";

export interface LatestFdSyscallTrace {
  nr: bigint;
  arg0OfdId: number;
  arg0OfdType: number;
  arg1: bigint | null;
  arg2OfdId: number;
  arg2OfdType: number;
  result: bigint | null;
}

function forkSharedOfdIdentityKey(handle: ProcessHandle, ofdId: number): string | null {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return null;
  }
  const type = handle.kernel.kernel_ofd_fd_type(ofdId);
  if (type <= 0) {
    return null;
  }
  const ino = handle.kernel.kernel_ofd_ino(ofdId);
  const hostFd = handle.kernel.kernel_ofd_host_fd(ofdId);
  if (type === handle.kernel.abi_fd_type_pipe()) {
    const endpoint = hostFd >= 0 ? "read" : "write";
    return `${type}:${ino}:${endpoint}`;
  }
  if (type === handle.kernel.abi_fd_type_socket?.()) {
    return `${type}:${ino}:${hostFd}`;
  }
  return `${type}:${ino}`;
}

export function sourceForkSharedOfdPeerState(
  handle: ProcessHandle,
  ofdId: number,
): { compatiblePeer: boolean; incompatiblePeer: boolean } {
  const sourceKey = forkSharedOfdIdentityKey(handle, ofdId);
  if (sourceKey === null) {
    return { compatiblePeer: false, incompatiblePeer: true };
  }
  let compatiblePeer = false;
  let incompatiblePeer = false;
  for (const member of handle.kernelShare.members) {
    if (member === handle || !collectProcessHeldOfds(member).has(ofdId)) {
      continue;
    }
    const peerKey = forkSharedOfdIdentityKey(member, ofdId);
    if (peerKey === sourceKey) {
      compatiblePeer = true;
    } else {
      incompatiblePeer = true;
    }
  }
  return { compatiblePeer, incompatiblePeer };
}

export function sourceOfdCanPublishForkSharedState(handle: ProcessHandle, ofdId: number): boolean {
  if (!handle.kernelShare.sharedOfdIds.has(ofdId)) {
    return false;
  }
  const { compatiblePeer, incompatiblePeer } = sourceForkSharedOfdPeerState(handle, ofdId);
  if (incompatiblePeer) {
    return false;
  }
  const fdType = handle.kernel.kernel_ofd_fd_type(ofdId);
  const fileType = handle.kernel.abi_fd_type_file?.();
  if (fileType !== undefined && fdType === fileType) {
    return compatiblePeer;
  }
  if (fdType === handle.kernel.abi_fd_type_pipe()) {
    return compatiblePeer || hasPipePeerInOtherKernelShareMember(handle, ofdId);
  }
  return true;
}

export function targetHasIncompatibleForkSharedOfd(
  source: ProcessHandle,
  target: ProcessHandle,
  ofdIds: Iterable<number>,
): boolean {
  if (target.vmPtr === 0) {
    return false;
  }
  const trackedOfdIds = new Set(ofdIds);
  if (trackedOfdIds.size === 0) {
    return false;
  }
  const maxFds = target.kernel.abi_max_fds();
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = target.kernel.process_fd_ofd_id(target.vmPtr, fd);
    if (
      ofdId <= 0
      || ofdId >= maxFds
      || !trackedOfdIds.has(ofdId)
      || !source.kernelShare.sharedOfdIds.has(ofdId)
    ) {
      continue;
    }
    const sourceKey = forkSharedOfdIdentityKey(source, ofdId);
    const targetKey = forkSharedOfdIdentityKey(target, ofdId);
    if (sourceKey !== null && targetKey !== null && sourceKey !== targetKey) {
      return true;
    }
  }
  return false;
}

export function targetHoldsSyncedRegularFileOfd(target: ProcessHandle, ofdIds: ReadonlySet<number>): boolean {
  if (target.vmPtr === 0 || ofdIds.size === 0) {
    return false;
  }
  const fileType = target.kernel.abi_fd_type_file();
  for (const ofdId of collectProcessHeldOfds(target)) {
    if (
      ofdIds.has(ofdId)
      && target.kernel.kernel_ofd_fd_type(ofdId) === fileType
    ) {
      return true;
    }
  }
  return false;
}

export function sourceHoldsSharedRegularFileOfd(handle: ProcessHandle, vmPtr: number): boolean {
  if (vmPtr === 0) {
    return false;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const fileType = (handle.kernel as { abi_fd_type_file?: () => number }).abi_fd_type_file?.();
  if (fileType === undefined) {
    return false;
  }
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId > 0
      && ofdId < maxFds
      && handle.kernelShare.sharedOfdIds.has(ofdId)
      && handle.kernel.kernel_ofd_fd_type(ofdId) === fileType
    ) {
      return true;
    }
  }
  return false;
}

function matchesOptionalSyscall(nr: bigint | null, candidate: bigint | number | undefined): boolean {
  return nr !== null && candidate !== undefined && nr === BigInt(candidate);
}

function readThreadSyscallNr(handle: ProcessHandle, vmPtr: number, syscallNr: bigint | null): bigint | null {
  if (syscallNr !== null) {
    return syscallNr;
  }
  const nr = handle.kernel.thread_a7?.(vmPtr);
  return nr === undefined ? null : nr;
}

function readOptionalTraceU64(value: bigint | number | undefined): bigint | null {
  if (value === undefined) {
    return null;
  }
  return typeof value === "bigint" ? value : BigInt(value);
}

function threadSyscallReturnedError(handle: ProcessHandle, vmPtr: number): boolean {
  const result = handle.kernel.thread_a0?.(vmPtr);
  return result !== undefined && BigInt.asIntN(64, BigInt(result)) < 0n;
}

function fcntlCommandMayChangeForkSharedOfdTopology(cmd: bigint | null): boolean {
  if (cmd === null) {
    return true;
  }
  switch (cmd) {
    case 4n: // F_SETFL
      return true;
    default:
      return false;
  }
}

function ioctlRequestMayChangeForkSharedOfdTopology(request: bigint | null): boolean {
  if (request === null) {
    return true;
  }
  return request === 0x5421n; // FIONBIO mutates O_NONBLOCK on the open file description.
}

function fcntlMayChangeForkSharedOfdTopology(handle: ProcessHandle, vmPtr: number): boolean {
  return fcntlCommandMayChangeForkSharedOfdTopology(
    readOptionalTraceU64(handle.kernel.thread_a1?.(vmPtr)),
  );
}

function ioctlMayChangeForkSharedOfdTopology(handle: ProcessHandle, vmPtr: number): boolean {
  return ioctlRequestMayChangeForkSharedOfdTopology(
    readOptionalTraceU64(handle.kernel.thread_a1?.(vmPtr)),
  );
}

export function syscallMayChangeForkSharedOfdTopology(
  handle: ProcessHandle,
  vmPtr: number,
  syscallNr: bigint | null,
): boolean {
  if (threadSyscallReturnedError(handle, vmPtr)) {
    return false;
  }
  const nr = readThreadSyscallNr(handle, vmPtr, syscallNr);
  if (matchesOptionalSyscall(nr, handle.kernel.abi_sys_fcntl?.())) {
    return fcntlMayChangeForkSharedOfdTopology(handle, vmPtr);
  }
  if (matchesOptionalSyscall(nr, handle.kernel.abi_sys_ioctl?.())) {
    return ioctlMayChangeForkSharedOfdTopology(handle, vmPtr);
  }
  return matchesOptionalSyscall(nr, handle.kernel.abi_sys_openat?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_close?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_close_range?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_pipe2?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_socket?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_socketpair?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_accept4?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_eventfd2?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_epoll_create1?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_dup?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_dup3?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_execve?.());
}

export function explicitForkSharedEffectMayRequireSync(
  handle: ProcessHandle,
  vmPtr: number,
  syscallNr: bigint | null,
): boolean {
  if (threadSyscallReturnedError(handle, vmPtr)) {
    return false;
  }
  const nr = readThreadSyscallNr(handle, vmPtr, syscallNr);
  if (matchesOptionalSyscall(nr, handle.kernel.abi_sys_fcntl?.())) {
    return fcntlMayChangeForkSharedOfdTopology(handle, vmPtr);
  }
  if (matchesOptionalSyscall(nr, handle.kernel.abi_sys_ioctl?.())) {
    return ioctlMayChangeForkSharedOfdTopology(handle, vmPtr);
  }
  return true;
}

export function latestFdTraceRulesOutForkSharedTopologyChange(
  handle: ProcessHandle,
  vmPtr: number,
  latestFdTrace: LatestFdSyscallTrace | null,
): boolean {
  const trace = latestFdTrace;
  if (trace === null) {
    return false;
  }
  if (trace.result !== null && BigInt.asIntN(64, trace.result) < 0n) {
    return true;
  }
  if (matchesOptionalSyscall(trace.nr, handle.kernel.abi_sys_dup3?.())) {
    const newFd = trace.arg1 === null ? -1 : Number(trace.arg1);
    if (Number.isInteger(newFd) && newFd >= 0 && newFd < handle.kernel.abi_max_fds()) {
      const newOfdId = handle.kernel.process_fd_ofd_id(vmPtr, newFd);
      if (
        newOfdId > 0
        && newOfdId < handle.kernel.abi_max_fds()
        && handle.kernel.kernel_ofd_fd_type(newOfdId) === handle.kernel.abi_fd_type_file?.()
      ) {
        return false;
      }
    }
  }
  if (
    matchesOptionalSyscall(trace.nr, handle.kernel.abi_sys_close?.())
    || matchesOptionalSyscall(trace.nr, handle.kernel.abi_sys_dup?.())
    || matchesOptionalSyscall(trace.nr, handle.kernel.abi_sys_pipe2?.())
    || matchesOptionalSyscall(trace.nr, handle.kernel.abi_sys_socket?.())
    || matchesOptionalSyscall(trace.nr, handle.kernel.abi_sys_socketpair?.())
    || matchesOptionalSyscall(trace.nr, handle.kernel.abi_sys_accept4?.())
    || matchesOptionalSyscall(trace.nr, handle.kernel.abi_sys_eventfd2?.())
    || matchesOptionalSyscall(trace.nr, handle.kernel.abi_sys_epoll_create1?.())
  ) {
    return !fdTraceTouchesForkSharedPeer(handle, trace);
  }
  if (matchesOptionalSyscall(trace.nr, handle.kernel.abi_sys_fcntl?.())) {
    return fcntlCommandMayChangeForkSharedOfdTopology(trace.arg1)
      ? !fdTraceTouchesForkSharedPeer(handle, trace)
      : true;
  }
  if (matchesOptionalSyscall(trace.nr, handle.kernel.abi_sys_ioctl?.())) {
    return ioctlRequestMayChangeForkSharedOfdTopology(trace.arg1)
      ? !fdTraceTouchesForkSharedPeer(handle, trace)
      : true;
  }
  return false;
}

function fdTraceTouchesForkSharedPeer(
  handle: ProcessHandle,
  trace: LatestFdSyscallTrace,
): boolean {
  const touchedOfdIds = fdTraceTouchedOfdIds(handle, trace);
  if (touchedOfdIds.length === 0) {
    return true;
  }
  for (const member of handle.kernelShare.members) {
    if (member === handle || member.vmPtr === 0) {
      continue;
    }
    const maxFds = member.kernel.abi_max_fds();
    for (let fd = 0; fd < maxFds; fd += 1) {
      const memberOfdId = member.kernel.process_fd_ofd_id(member.vmPtr, fd);
      if (touchedOfdIds.includes(memberOfdId)) {
        return true;
      }
    }
  }
  return false;
}

function fdTraceTouchedOfdIds(handle: ProcessHandle, trace: LatestFdSyscallTrace): number[] {
  return [...new Set([trace.arg0OfdId, trace.arg2OfdId])]
    .filter((ofdId) => ofdId > 0 && ofdId < handle.kernel.abi_max_fds());
}

export function syscallOnlyNeedsRegularFileOffsetSyncWhenPeerShared(
  handle: ProcessHandle,
  vmPtr: number,
  syscallNr: bigint | null,
): boolean {
  const nr = readThreadSyscallNr(handle, vmPtr, syscallNr);
  if (
    !matchesOptionalSyscall(nr, handle.kernel.abi_sys_read?.())
    && !matchesOptionalSyscall(nr, handle.kernel.abi_sys_readv?.())
    && !matchesOptionalSyscall(nr, handle.kernel.abi_sys_write?.())
    && !matchesOptionalSyscall(nr, handle.kernel.abi_sys_writev?.())
    && !matchesOptionalSyscall(nr, handle.kernel.abi_sys_pwrite64?.())
    && !matchesOptionalSyscall(nr, handle.kernel.abi_sys_lseek?.())
  ) {
    return false;
  }
  const fd = Number(handle.kernel.thread_a0?.(vmPtr) ?? -1n);
  if (!Number.isFinite(fd) || fd < 0 || fd >= handle.kernel.abi_max_fds()) {
    return false;
  }
  const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
  const fileType = (handle.kernel as { abi_fd_type_file?: () => number }).abi_fd_type_file?.();
  return fileType !== undefined
    && ofdId > 0
    && ofdId < handle.kernel.abi_max_fds()
    && handle.kernel.kernel_ofd_fd_type(ofdId) === fileType;
}

export function sourceSharesRegularFileOfdWithPeer(handle: ProcessHandle, vmPtr: number): boolean {
  if (vmPtr === 0) {
    return false;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const fileType = (handle.kernel as { abi_fd_type_file?: () => number }).abi_fd_type_file?.();
  if (fileType === undefined) {
    return false;
  }
  const sourceRegularOfds = new Set<number>();
  const sourceRegularInoByOfd = new Map<number, number>();
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId > 0
      && ofdId < maxFds
      && handle.kernelShare.sharedOfdIds.has(ofdId)
      && handle.kernel.kernel_ofd_fd_type(ofdId) === fileType
    ) {
      sourceRegularOfds.add(ofdId);
      sourceRegularInoByOfd.set(ofdId, handle.kernel.kernel_ofd_ino(ofdId));
    }
  }
  if (sourceRegularOfds.size === 0) {
    return false;
  }
  for (const peer of handle.kernelShare.members) {
    if (peer === handle || peer.vmPtr === 0) {
      continue;
    }
    for (let fd = 0; fd < maxFds; fd += 1) {
      const ofdId = peer.kernel.process_fd_ofd_id(peer.vmPtr, fd);
      if (
        sourceRegularOfds.has(ofdId)
        && peer.kernel.kernel_ofd_fd_type(ofdId) === fileType
        && peer.kernel.kernel_ofd_ino(ofdId) === sourceRegularInoByOfd.get(ofdId)
      ) {
        return true;
      }
    }
  }
  return false;
}

export function collectTrackableForkSharedOfds(handle: ProcessHandle): Set<number> {
  const stdioOfdIds = new Set<number>();
  const stdioFdOfdIds = new Set<number>();
  for (const [fd, stdioPipeSlot] of [
    [0, handle.stdinPipe],
    [1, handle.stdoutPipe],
    [2, handle.stderrPipe],
  ] as const) {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId !== 0) {
      stdioFdOfdIds.add(ofdId);
    }
    if (
      ofdId !== 0
      && stdioPipeSlot > 0
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
      && handle.kernel.kernel_ofd_ino(ofdId) === stdioPipeSlot
    ) {
      stdioOfdIds.add(ofdId);
    }
  }
  const tracked = new Set<number>();
  for (const ofdId of handle.kernelShare.sharedOfdIds) {
    if (
      ofdId > 0
      && ofdId < handle.kernel.abi_max_fds()
    ) {
      tracked.add(ofdId);
    }
  }
  for (const ofdId of collectProcessHeldOfds(handle)) {
    if (stdioOfdIds.has(ofdId)) {
      continue;
    }
    if (
      !handle.kernelShare.sharedOfdIds.has(ofdId)
      && !stdioFdOfdIds.has(ofdId)
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_file()
    ) {
      continue;
    }
    tracked.add(ofdId);
  }
  return tracked;
}

export function collectTracedForkSharedOfdIds(
  handle: ProcessHandle,
  sourceVmPtr: number,
): number[] {
  const len = handle.kernel.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return [];
  }
  const traced = new Set<number>();
  const sampleToPid = (sample: bigint | number | undefined): number => {
    if (sample === undefined) {
      return 0;
    }
    return Number(BigInt.asIntN(32, BigInt(sample) & 0xffffffffn));
  };
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
    if (traceVmPtr !== 0 && sourceVmPtr !== 0 && traceVmPtr !== sourceVmPtr) {
      continue;
    }
    const tracePid = sampleToPid(handle.kernel.debug_fd_syscall_trace_pid_sample0?.(index));
    if (tracePid > 0 && tracePid !== handle.pid) {
      continue;
    }
    const arg0OfdId = handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0;
    const arg2OfdId = handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? 0;
    if (arg0OfdId > 0) {
      traced.add(arg0OfdId);
    }
    if (arg2OfdId > 0) {
      traced.add(arg2OfdId);
    }
  }
  return [...traced];
}

export function trackHeldForkSharedOfds(handle: ProcessHandle, vmPtr: number): void {
  trackForkSharedOfds(handle, vmPtr, false);
  for (const ofdId of collectProcessHeldOfds(handle)) {
    if (isProcessHostStdioOfd(handle, vmPtr, ofdId) || isProcessHostStdioPipeOfd(handle, ofdId)) {
      continue;
    }
    if (
      !handle.kernelShare.sharedOfdIds.has(ofdId)
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_file()
    ) {
      continue;
    }
    handle.kernelShare.sharedOfdIds.add(ofdId);
  }
}

export function isRegularFileOfd(handle: ProcessHandle, ofdId: number): boolean {
  return ofdId > 0
    && ofdId < handle.kernel.abi_max_fds()
    && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_file();
}
