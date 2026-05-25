import { collectHeldEpollOfdIds } from "../fork-shared.js";
import {
  describeForkSharedSyncPromise,
  registerForkSharedSyncPromise,
} from "../fork-shared-sync-debug.js";
import {
  kernelShareOtherMembers,
  kernelWorkerOwnsEpollState,
  kernelWorkerOwnsSocketState,
  reconcileKernelSharePipeControl,
} from "../kernel-share.js";
import type { ProcessHandle, ThreadWorkerState } from "../types.js";
import type { WorkerMessageHandlerDeps } from "./types.js";
import { createWorkerOwnershipGuard } from "./worker-ownership.js";

interface OwnedStateSyncArgs {
  handle: ProcessHandle;
  worker: Worker;
  thread: ThreadWorkerState;
  status: number;
  syscallNr?: bigint | null;
  rawSyncEffects: number | undefined;
  completedSyncEffects: number;
  latestFdTrace: LatestFdSyscallTrace | null;
  isRunningWorker: boolean;
  wroteToPipe: boolean;
  drainedPipe: boolean;
  continueAfterOwnedStateSync(): void;
}

interface LatestFdSyscallTrace {
  nr: bigint;
  arg0OfdId: number;
  arg0OfdType: number;
  arg1: bigint | null;
  arg2OfdId: number;
  arg2OfdType: number;
  result: bigint | null;
}

function sourceHoldsSharedRegularFileOfd(handle: ProcessHandle, vmPtr: number): boolean {
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

function readThreadSyscallNr(handle: ProcessHandle, vmPtr: number, syscallNr: bigint | null): bigint | null {
  if (syscallNr !== null) {
    return syscallNr;
  }
  const nr = handle.kernel.thread_a7?.(vmPtr);
  return nr === undefined ? null : nr;
}

function matchesOptionalSyscall(nr: bigint | null, candidate: bigint | number | undefined): boolean {
  return nr !== null && candidate !== undefined && nr === BigInt(candidate);
}

function readOptionalTraceU64(value: bigint | number | undefined): bigint | null {
  if (value === undefined) {
    return null;
  }
  return typeof value === "bigint" ? value : BigInt(value);
}

function readLatestFdSyscallTrace(handle: ProcessHandle, vmPtr: number): LatestFdSyscallTrace | null {
  const len = handle.kernel.debug_fd_syscall_trace_len?.() ?? 0;
  for (let index = len - 1; index >= 0; index -= 1) {
    const entryVmPtr = handle.kernel.debug_fd_syscall_trace_vm_ptr?.(index);
    if (entryVmPtr !== undefined && entryVmPtr !== vmPtr) {
      continue;
    }
    const nr = readOptionalTraceU64(handle.kernel.debug_fd_syscall_trace_nr?.(index));
    if (nr === null) {
      continue;
    }
    return {
      nr,
      arg0OfdId: handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0,
      arg0OfdType: handle.kernel.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0,
      arg1: readOptionalTraceU64(handle.kernel.debug_fd_syscall_trace_arg1?.(index)),
      arg2OfdId: handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? 0,
      arg2OfdType: handle.kernel.debug_fd_syscall_trace_arg2_ofd_type?.(index) ?? 0,
      result: readOptionalTraceU64(handle.kernel.debug_fd_syscall_trace_result?.(index)),
    };
  }
  return null;
}

function threadSyscallReturnedError(handle: ProcessHandle, vmPtr: number): boolean {
  const result = handle.kernel.thread_a0?.(vmPtr);
  return result !== undefined && BigInt.asIntN(64, BigInt(result)) < 0n;
}

function regularFileWriteMutatedMemfs(handle: ProcessHandle, trace: LatestFdSyscallTrace | null | undefined): boolean {
  if (trace == null || trace.nr !== 64n || trace.result === null || BigInt.asIntN(64, trace.result) <= 0n) {
    return false;
  }
  const fileType = (handle.kernel as { abi_fd_type_file?: () => number }).abi_fd_type_file?.();
  return fileType !== undefined && trace.arg0OfdType === fileType;
}

function syscallIsFdDataPlaneIo(handle: ProcessHandle, nr: bigint): boolean {
  return matchesOptionalSyscall(nr, handle.kernel.abi_sys_read?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_readv?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_write?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_writev?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_recvfrom?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_recvmsg?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_sendto?.())
    || matchesOptionalSyscall(nr, handle.kernel.abi_sys_sendmsg?.());
}

function fdTraceDataPlaneIoUsesSocketOrPipe(
  handle: ProcessHandle,
  trace: LatestFdSyscallTrace | null | undefined,
): boolean {
  if (
    trace == null
    || trace.result === null
    || BigInt.asIntN(64, trace.result) <= 0n
    || !syscallIsFdDataPlaneIo(handle, trace.nr)
  ) {
    return false;
  }
  let fdType = trace.arg0OfdType;
  if (fdType <= 0 && trace.arg0OfdId > 0 && trace.arg0OfdId < handle.kernel.abi_max_fds()) {
    fdType = handle.kernel.kernel_ofd_fd_type(trace.arg0OfdId);
  }
  return fdType === handle.kernel.abi_fd_type_socket()
    || fdType === handle.kernel.abi_fd_type_pipe();
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

function syscallMayChangeForkSharedOfdTopology(
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

function explicitForkSharedEffectMayRequireSync(
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

function latestFdTraceRulesOutForkSharedTopologyChange(
  handle: ProcessHandle,
  vmPtr: number,
  latestFdTrace: LatestFdSyscallTrace | null,
): boolean {
  const trace = latestFdTrace ?? readLatestFdSyscallTrace(handle, vmPtr);
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
  for (const member of kernelShareOtherMembers(handle)) {
    if (member.vmPtr === 0) {
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

function syscallOnlyNeedsRegularFileOffsetSyncWhenPeerShared(
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

function sourceSharesRegularFileOfdWithPeer(handle: ProcessHandle, vmPtr: number): boolean {
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
  for (const peer of kernelShareOtherMembers(handle)) {
    if (peer.vmPtr === 0) {
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

function ofdIdentityKey(handle: ProcessHandle, ofdId: number): string | null {
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
    return `${type}:${ino}:${hostFd >= 0 ? "read" : "write"}`;
  }
  return `${type}:${ino}:${hostFd}`;
}

function targetHasIncompatibleForkSharedOfd(source: ProcessHandle, target: ProcessHandle): boolean {
  if (target.vmPtr === 0) {
    return false;
  }
  const sharedOfdIds = source.kernelShare.sharedOfdIds;
  if (sharedOfdIds === undefined) {
    return false;
  }
  const maxFds = target.kernel.abi_max_fds();
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = target.kernel.process_fd_ofd_id(target.vmPtr, fd);
    if (
      ofdId <= 0
      || ofdId >= maxFds
      || !sharedOfdIds.has(ofdId)
    ) {
      continue;
    }
    const sourceKey = ofdIdentityKey(source, ofdId);
    const targetKey = ofdIdentityKey(target, ofdId);
    if (sourceKey !== null && targetKey !== null && sourceKey !== targetKey) {
      return true;
    }
  }
  return false;
}

export function createWorkerOwnedStateSyncHandler(deps: WorkerMessageHandlerDeps) {
  function requestForkSharedStateSync(
    handle: ProcessHandle,
    thread: ThreadWorkerState,
    latestFdTrace: LatestFdSyscallTrace | null,
  ): Promise<void> {
    const otherMembers = kernelShareOtherMembers(handle);
    const activeSync = handle.kernelShare.forkSharedSyncPromise ?? handle.forkSharedSyncPromise;
    const syncTargetIsUsable = (member: ProcessHandle): boolean => {
      if (!Number.isFinite(member.pid) || member.pid <= 0) {
        return false;
      }
      const registered = deps.processHandles?.get(member.pid);
      if (
        member.runningWorker !== null
        || member.exitInProgress
        || registered?.exitInProgress
        || targetHasIncompatibleForkSharedOfd(handle, member)
      ) {
        return false;
      }
      return registered === undefined || registered === member;
    };
    const syncTargetCanBecomeUsable = (member: ProcessHandle): boolean => {
      if (!Number.isFinite(member.pid) || member.pid <= 0) {
        return false;
      }
      const registered = deps.processHandles?.get(member.pid);
      if (
        member.exitInProgress
        || registered?.exitInProgress
        || targetHasIncompatibleForkSharedOfd(handle, member)
      ) {
        return false;
      }
      return registered === undefined || registered === member;
    };
    const hasSyncTargetCandidate = otherMembers.some(syncTargetCanBecomeUsable);
    const startForkSharedStateSync = (targets = otherMembers.filter(syncTargetIsUsable)): Promise<void> => {
      if (targets.length === 0) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `fork-shared-sync-local:${handle.pid}:src=0x${thread.vmPtr.toString(16)}:targets=none`,
        );
        return deps.syncForkSharedStateViaKernelWorker(
          handle,
          targets,
          {
            deltaOfdIds: true,
            includeMemfs: false,
            trackHeldOfds: true,
            sourceVmPtr: thread.vmPtr,
          },
        );
      }
      const promise = deps.syncForkSharedStateViaKernelWorker(
        handle,
        targets,
        {
          deltaOfdIds: true,
          includeMemfs: false,
          trackHeldOfds: true,
          sourceVmPtr: thread.vmPtr,
        },
      ).finally(() => {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `fork-shared-sync-settled:${describeForkSharedSyncPromise(promise)}`,
        );
        if (handle.forkSharedSyncPromise === promise) {
          handle.forkSharedSyncPromise = null;
        }
        if (handle.kernelShare.forkSharedSyncPromise === promise) {
          handle.kernelShare.forkSharedSyncPromise = null;
        }
      });
      const syncId = registerForkSharedSyncPromise(promise, {
        ownerPid: handle.pid,
        sourceVmPtr: thread.vmPtr,
        reason: "owned-ofd",
        targetPids: targets.map((target) => target.pid),
      });
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `fork-shared-sync-start:id=${syncId}:owner=${handle.pid}:src=0x${thread.vmPtr.toString(16)}`
          + `:targets=${targets.map((target) => target.pid).join(".") || "none"}`,
      );
      handle.forkSharedSyncPromise = promise;
      handle.kernelShare.forkSharedSyncPromise = promise;
      return promise;
    };

    if (activeSync !== null) {
      if (!hasSyncTargetCandidate) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `fork-shared-sync-local-queue:${describeForkSharedSyncPromise(activeSync)}:owner=${handle.pid}:src=0x${thread.vmPtr.toString(16)}`,
        );
        return activeSync.then(() => startForkSharedStateSync([]));
      }
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `fork-shared-sync-queue:${describeForkSharedSyncPromise(activeSync)}:owner=${handle.pid}:src=0x${thread.vmPtr.toString(16)}`,
      );
      const queuedSync = activeSync
        .then(() => startForkSharedStateSync())
        .finally(() => {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `fork-shared-sync-queue-settled:${describeForkSharedSyncPromise(queuedSync)}`,
          );
          if (handle.forkSharedSyncPromise === queuedSync) {
            handle.forkSharedSyncPromise = null;
          }
          if (handle.kernelShare.forkSharedSyncPromise === queuedSync) {
            handle.kernelShare.forkSharedSyncPromise = null;
          }
        });
      registerForkSharedSyncPromise(queuedSync, {
        ownerPid: handle.pid,
        sourceVmPtr: thread.vmPtr,
        reason: "queued-owned-ofd",
        targetPids: otherMembers.map((target) => target.pid),
      });
      handle.forkSharedSyncPromise = queuedSync;
      handle.kernelShare.forkSharedSyncPromise = queuedSync;
      return queuedSync;
    }

    return startForkSharedStateSync();
  }

  return function maybeHandleOwnedStateSync({
    handle,
    worker,
    thread,
    status,
    syscallNr = null,
    rawSyncEffects,
    completedSyncEffects,
    latestFdTrace = null,
    isRunningWorker,
    wroteToPipe,
    drainedPipe,
    continueAfterOwnedStateSync,
  }: OwnedStateSyncArgs): boolean {
    const workerStillOwnedByHandle = createWorkerOwnershipGuard(
      handle,
      worker,
      thread,
      deps.threadWorkerStates,
    );
    const workerOwnsAuthoritativeState = isRunningWorker || workerStillOwnedByHandle();
    const shouldWakeSocketWaiters = status === handle.kernel.abi_status_ok()
      && (completedSyncEffects & deps.syncEffectSocketState) !== 0;
    const hasExplicitForkSharedEffect = rawSyncEffects !== undefined
      && (rawSyncEffects & deps.syncEffectForkSharedOfdState) !== 0;
    const fdTraceRulesOutForkSharedTopologyChange =
      latestFdTraceRulesOutForkSharedTopologyChange(handle, thread.vmPtr, latestFdTrace);
    const explicitForkSharedEffectMaySync = explicitForkSharedEffectMayRequireSync(
      handle,
      thread.vmPtr,
      syscallNr,
    ) && !fdTraceRulesOutForkSharedTopologyChange;
    const forkSharedDataPlaneIoUsesSocketOrPipe = fdTraceDataPlaneIoUsesSocketOrPipe(handle, latestFdTrace);
    const forkSharedTopologyMayChange = syscallMayChangeForkSharedOfdTopology(handle, thread.vmPtr, syscallNr)
      && !fdTraceRulesOutForkSharedTopologyChange;
    const forkSharedOffsetOnlyRegularFile = syscallOnlyNeedsRegularFileOffsetSyncWhenPeerShared(
      handle,
      thread.vmPtr,
      syscallNr,
    );
    const forkSharedSourceHoldsRegularFile = sourceHoldsSharedRegularFileOfd(handle, thread.vmPtr);
    const forkSharedRegularFileHasPeer = sourceSharesRegularFileOfdWithPeer(handle, thread.vmPtr);
    const forkSharedHasTrackedState = (handle.kernelShare.sharedOfdIds?.size ?? 0) > 0
      || kernelShareOtherMembers(handle).length > 0;
    const shouldRequestForkSharedForEffect = forkSharedTopologyMayChange
      || (
        hasExplicitForkSharedEffect
        && explicitForkSharedEffectMaySync
        && !forkSharedDataPlaneIoUsesSocketOrPipe
        && forkSharedHasTrackedState
        && !forkSharedOffsetOnlyRegularFile
        && (
          !forkSharedSourceHoldsRegularFile
          || (
            forkSharedSourceHoldsRegularFile
            && forkSharedRegularFileHasPeer
          )
        )
      );
    const hasProcessMemfsEffect = (completedSyncEffects & deps.syncEffectProcessMemfs) !== 0
      || regularFileWriteMutatedMemfs(handle, latestFdTrace);
    if (isRunningWorker && hasProcessMemfsEffect) {
      handle.processMemfsDirty = true;
    }
    const activeVforkChildOwnerMismatch = handle.vforkSnapshot !== null
      && handle.vforkWorker === worker
      && handle.pid > 0
      && typeof handle.kernel.process_current_pid === "function"
      && handle.kernel.process_current_pid(thread.vmPtr) !== handle.pid;
    const shouldSyncForkSharedState = workerOwnsAuthoritativeState
      && handle.kernelWorkerRequest !== null
      && handle.pid > 0
      && shouldRequestForkSharedForEffect
      && status === handle.kernel.abi_status_ok()
      && !activeVforkChildOwnerMismatch
      && !deps.statusDefersOwnedStateSync(handle, status);
    const shouldSyncEpollDependentOfds = isRunningWorker
      && kernelWorkerOwnsEpollState(handle)
      && (completedSyncEffects & deps.syncEffectEpollDependentOfdState) !== 0
      && !activeVforkChildOwnerMismatch
      && !deps.statusDefersOwnedStateSync(handle, status);
    const shouldSyncSocketState = shouldSyncForkSharedState
      && isRunningWorker
      && kernelWorkerOwnsSocketState(handle)
      && (completedSyncEffects & deps.syncEffectSocketState) !== 0
      && !activeVforkChildOwnerMismatch
      && !deps.statusDefersOwnedStateSync(handle, status);
    const shouldPublishDirtyMemfs = status !== handle.kernel.abi_status_ok()
      || (completedSyncEffects & deps.syncEffectPipeState) !== 0;
    const shouldSyncProcessMemfs = isRunningWorker
      && handle.kernelWorkerRequest !== null
      && handle.pid > 0
      && handle.processMemfsDirty
      && shouldPublishDirtyMemfs
      && !activeVforkChildOwnerMismatch
      && !deps.statusDefersOwnedStateSync(handle, status);
    const shouldSyncEpollState = isRunningWorker
      && kernelWorkerOwnsEpollState(handle)
      && collectHeldEpollOfdIds(handle).length > 0
      && (completedSyncEffects & deps.syncEffectEpollState) !== 0
      && !activeVforkChildOwnerMismatch
      && !deps.statusDefersOwnedStateSync(handle, status);
    if (
      !shouldSyncForkSharedState
      && !shouldSyncEpollDependentOfds
      && !shouldSyncSocketState
      && !shouldSyncProcessMemfs
      && !shouldSyncEpollState
    ) {
      return false;
    }

    const syncForkSharedState = (): Promise<void> => shouldSyncForkSharedState
      ? requestForkSharedStateSync(handle, thread, latestFdTrace).then(() => {
          reconcileKernelSharePipeControl(handle);
        })
      : Promise.resolve();
    const syncOfdState = (): Promise<void> => shouldSyncEpollDependentOfds
      ? deps.requestEpollDependentOfdStateSync(handle)
      : Promise.resolve();
    const syncSocketState = (): Promise<void> => shouldSyncSocketState
      ? deps.requestSocketStateSync(handle, shouldWakeSocketWaiters)
      : Promise.resolve();
    const syncProcessMemfs = (): Promise<void> => shouldSyncProcessMemfs
      ? deps.syncForkSharedStateViaKernelWorker(
          handle,
          kernelShareOtherMembers(handle),
          {
            includeMemfs: true,
            memfsOnly: true,
            trackHeldOfds: false,
            preservePublishedRegularFileData: false,
            sourceVmPtr: thread.vmPtr,
          },
        ).then(() => {
          deps.markRuntimeFsDirty(handle);
          for (const member of kernelShareOtherMembers(handle)) {
            deps.markRuntimeFsDirty(member);
          }
          handle.processMemfsDirty = false;
        })
      : Promise.resolve();
    void syncSocketState()
      .then(() => syncForkSharedState())
      .then(() => syncOfdState())
      .then(() => syncProcessMemfs())
      .then(() => (shouldSyncEpollState
        ? deps.requestEpollStateSync(handle, !(wroteToPipe || drainedPipe))
        : Promise.resolve()))
      .then(() => {
        if (!workerStillOwnedByHandle()) {
          return;
        }
        continueAfterOwnedStateSync();
        for (const member of handle.kernelShare.members) {
          deps.pumpProcess(member);
        }
      })
      .catch((error) => {
        if (!workerStillOwnedByHandle()) {
          return;
        }
        const failure = error instanceof Error ? error : new Error(String(error));
        if (typeof deps.failProcess === "function") {
          deps.failProcess(handle, failure);
          return;
        }
        throw failure;
      });
    return true;
  };
}
