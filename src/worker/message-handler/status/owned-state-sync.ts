import { collectHeldEpollOfdIds } from "../../ownership/ofd-table.js";
import {
  describeForkSharedSyncPromise,
  registerForkSharedSyncPromise,
} from "../../fork-shared-sync-debug.js";
import {
  kernelShareOtherMembers,
  reconcileKernelSharePipeControl,
} from "../../kernel-share.js";
import { kernelWorkerOwnsEpollState } from "../../ownership/epoll-control.js";
import {
  explicitForkSharedEffectMayRequireSync,
  latestFdTraceRulesOutForkSharedTopologyChange,
  sourceHoldsSharedRegularFileOfd,
  sourceSharesRegularFileOfdWithPeer,
  syscallMayChangeForkSharedOfdTopology,
  syscallOnlyNeedsRegularFileOffsetSyncWhenPeerShared,
  targetHasIncompatibleForkSharedOfd as ownershipTargetHasIncompatibleForkSharedOfd,
} from "../../ownership/ofd-peer.js";
import type { LatestFdSyscallTrace } from "../../ownership/ofd-peer.js";
import { kernelWorkerOwnsSocketState } from "../../ownership/socket-control.js";
import type { ProcessHandle, ThreadWorkerState } from "../../types.js";
import type { WorkerMessageHandlerDeps } from "../types.js";
import { createWorkerOwnershipGuard } from "../../ownership/process-state.js";
import { resolveWorkerMessageHandlerSyncEffects } from "../sync-effects.js";

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

export function createWorkerOwnedStateSyncHandler(deps: WorkerMessageHandlerDeps) {
  const syncEffects = resolveWorkerMessageHandlerSyncEffects(deps);
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
        || ownershipTargetHasIncompatibleForkSharedOfd(handle, member, handle.kernelShare.sharedOfdIds)
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
        || ownershipTargetHasIncompatibleForkSharedOfd(handle, member, handle.kernelShare.sharedOfdIds)
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
      && (completedSyncEffects & syncEffects.syncEffectSocketState) !== 0;
    const hasExplicitForkSharedEffect = rawSyncEffects !== undefined
      && (rawSyncEffects & syncEffects.syncEffectForkSharedOfdState) !== 0;
    const latestFdTraceForTopology = latestFdTrace ?? readLatestFdSyscallTrace(handle, thread.vmPtr);
    const fdTraceRulesOutForkSharedTopologyChange =
      latestFdTraceRulesOutForkSharedTopologyChange(handle, thread.vmPtr, latestFdTraceForTopology);
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
    const hasProcessMemfsEffect = (completedSyncEffects & syncEffects.syncEffectProcessMemfs) !== 0
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
      && (completedSyncEffects & syncEffects.syncEffectEpollDependentOfdState) !== 0
      && !activeVforkChildOwnerMismatch
      && !deps.statusDefersOwnedStateSync(handle, status);
    const shouldSyncSocketState = shouldSyncForkSharedState
      && isRunningWorker
      && kernelWorkerOwnsSocketState(handle)
      && (completedSyncEffects & syncEffects.syncEffectSocketState) !== 0
      && !activeVforkChildOwnerMismatch
      && !deps.statusDefersOwnedStateSync(handle, status);
    const shouldPublishDirtyMemfs = status !== handle.kernel.abi_status_ok()
      || (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0;
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
      && (completedSyncEffects & syncEffects.syncEffectEpollState) !== 0
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
    thread.ownerAsyncStatus = status;
    void syncSocketState()
      .then(() => syncForkSharedState())
      .then(() => syncOfdState())
      .then(() => syncProcessMemfs())
      .then(() => (shouldSyncEpollState
        ? deps.requestEpollStateSync(handle, !(wroteToPipe || drainedPipe))
        : Promise.resolve()))
      .then(() => {
        thread.ownerAsyncStatus = null;
        if (!workerStillOwnedByHandle()) {
          return;
        }
        continueAfterOwnedStateSync();
        for (const member of handle.kernelShare.members) {
          deps.pumpProcess(member);
        }
      })
      .catch((error) => {
        thread.ownerAsyncStatus = null;
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
