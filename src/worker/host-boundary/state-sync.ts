import { hostExecutionState } from "../host-execution-state.js";
import {
  processHostStdio,
  processStdioOwner,
} from "../process-stdio-state.js";
import { processWorkerList } from "../process-worker-state.js";
import {
  publishKernelRuntimeState,
} from "../../bridge/runtime-state.js";
import {
  supervisorBlockedEpollWaitTimeoutMs,
  supervisorBlockedPipeSyncBeforeRetryClear,
  supervisorBlockedWaitWakeAction,
  supervisorBlockedWaitStatusMatchesWakeSource,
  supervisorStateSyncClear,
  supervisorStateSyncComplete,
  supervisorStateSyncNextRun,
  supervisorStateSyncRequest,
  supervisorStateSyncStartAction,
  type SupervisorBlockedWaitWakeSource,
} from "../../supervisor-blocked-resume.js";
import {
  processExitInProgress,
} from "../process-lifecycle-state.js";
import {
  collectProcessHeldOfds,
  collectHeldEpollDependentSyncOfdIds,
  collectHeldEpollOfdIds,
  collectProcessSyncDependentOfdIds,
} from "../ownership/ofd-table.js";
import { kernelWorkerOwnsEpollState } from "../ownership/epoll-control.js";
import { processHasKernelWorkerOwnership } from "../kernel-worker-ownership.js";
import {
  composeHandlePipeUsed,
  pipeSlotHasBufferedData,
} from "../ownership/pipe-control.js";
import { supervisorProcessPipeSlotForFd } from "../ownership/pipe-slot.js";
import {
  assertHandleWorkerProcessIdentity,
  supervisorProcessOwnerMismatch,
  supervisorVforkChildOwnerMismatch,
} from "../process-identity.js";
import {
  clearRuntimeStateSyncPromise,
  getRuntimeStateSyncPromise,
  setRuntimeStateSyncPromise,
} from "../state-sync-inflight.js";
import {
  enqueueOfdSyncIds,
  getOfdSyncPromise,
  queuedOfdSyncIdCount,
  setOfdSyncPromise,
  takeQueuedOfdSyncIds,
} from "../ofd-sync-state.js";
import {
  isEpollPassiveUntilWake,
  isEpollWakeRetryPending,
  isBlockedRpcPending,
  markBlockedRpcDeferredRetry,
  setBlockedPassiveUntilWake,
  setEpollPassiveUntilWake,
  setEpollWakeRetryPending,
} from "../host-blocked-state.js";
import {
  clearThreadRunningGeneration,
  getThreadWaitingInWorkerStatus,
} from "../thread-host-state.js";
import { threadLatestStepAnswered } from "../thread-activity-state.js";
import {
  syncEpollStateViaKernelWorker,
  syncOfdStateViaKernelWorker,
} from "./fd-ofd-state-sync.js";
import {
  syncPipeStateViaKernelWorker,
} from "./pipe-state-sync.js";
import {
  syncSocketStateViaKernelWorker,
  applySocketStateFromKernelWorker,
} from "./socket-state-sync.js";
import { hasRuntimeNetworkSocketState } from "../ownership/socket-control.js";
import { kernelShareMemberList } from "../kernel-share.js";
import type { SyncCause } from "../contracts/sync.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import { transferDebugStats } from "../worker-debug-stats.js";
import type { WorkerSchedulerStateSyncHelpers } from "./worker-scheduler-types.js";

interface WorkerSchedulerStateSyncDeps {
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  drainProcessOutput(handle: ProcessHandle): boolean;
  drainHiddenProcessOutput(handle: ProcessHandle): boolean;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  pumpProcess?(handle: ProcessHandle): void;
  disarmSupervisorTimerForWake?(blocked: { status: number; vmPtr: number }): void;
  retryHostBlockedWorker(handle: ProcessHandle, worker: Worker): void;
}

type StateSyncKind = "pipe" | "epoll" | "socket";

export function createWorkerSchedulerStateSyncHelpers(
  deps: WorkerSchedulerStateSyncDeps,
): WorkerSchedulerStateSyncHelpers {
  function assertProcessIdentity(handle: ProcessHandle, context: string): void {
    assertHandleWorkerProcessIdentity(handle, deps.threadWorkerStates, context);
  }

  function visibleOutputHandle(handle: ProcessHandle): ProcessHandle {
    return processStdioOwner(handle) ?? handle;
  }

  function hasActiveVforkChildOwnerMismatch(handle: ProcessHandle): boolean {
    const executionState = hostExecutionState(handle);
    if (executionState.vforkWorker == null || handle.pid <= 0) {
      return false;
    }
    const thread = deps.threadWorkerStates.get(executionState.vforkWorker);
    if (!thread || thread.vmPtr === 0) {
      return false;
    }
    return supervisorVforkChildOwnerMismatch(handle.kernel, thread.vmPtr, handle.pid);
  }

  function hasAttachedWorkerOwnerMismatch(handle: ProcessHandle): boolean {
    const seenVmPtrs = new Set<number>();
    const checkVmPtr = (vmPtr: number): boolean => {
      if (vmPtr === 0 || seenVmPtrs.has(vmPtr)) {
        return false;
      }
      seenVmPtrs.add(vmPtr);
      return supervisorProcessOwnerMismatch(handle.kernel, vmPtr, handle.pid);
    };

    if (checkVmPtr(handle.vmPtr)) {
      return true;
    }
    return processWorkerList(handle).some((worker) => {
      const thread = deps.threadWorkerStates.get(worker);
      return thread !== undefined && checkVmPtr(thread.vmPtr);
    });
  }

  function hasStateSyncOwnerMismatch(handle: ProcessHandle): boolean {
    return hasActiveVforkChildOwnerMismatch(handle) || hasAttachedWorkerOwnerMismatch(handle);
  }

  function deferStateSyncForOwnerMismatch(
    handle: ProcessHandle,
    kind: StateSyncKind,
    phase: string,
  ): boolean {
    if (!hasStateSyncOwnerMismatch(handle)) {
      return false;
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `${kind}-sync-owner-mismatch-defer:${handle.pid}:${phase}`,
    );
    supervisorStateSyncClear(handle.kernel, handle.vmPtr, kind);
    return true;
  }

  function assertProcessIdentityOrDeferStateSync(
    handle: ProcessHandle,
    kind: StateSyncKind,
    context: string,
    phase: string,
  ): boolean {
    try {
      assertProcessIdentity(handle, context);
      return false;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (!message.startsWith(`${context} process identity drift `)) {
        throw error;
      }
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `${kind}-sync-owner-mismatch-defer:${handle.pid}:${phase}`,
      );
      supervisorStateSyncClear(handle.kernel, handle.vmPtr, kind);
      return true;
    }
  }

  function shouldDrainHiddenInheritedOutput(
    handle: ProcessHandle,
    visibleHandle: ProcessHandle,
  ): boolean {
    if (visibleHandle === handle) {
      return false;
    }
    const visibleSlots = new Set(
      [
        processHostStdio(visibleHandle).stdoutPipe,
        processHostStdio(visibleHandle).stderrPipe,
      ].filter((slot) => slot > 0),
    );
    const fdPipeSlot = (fd: number): number => supervisorProcessPipeSlotForFd(handle, handle.vmPtr, fd);
    const hostOutputSlots = [
      processHostStdio(handle).stdoutPipe,
      processHostStdio(handle).stderrPipe,
    ].filter((slot) => visibleSlots.has(slot));
    const hiddenSlots = [
      fdPipeSlot(1),
      fdPipeSlot(2),
      ...hostOutputSlots,
    ]
      .filter((slot, index, slots) => slot > 0 && slots.indexOf(slot) === index);
    if (hiddenSlots.length === 0) {
      return false;
    }
    return hiddenSlots.some((slot) => visibleSlots.has(slot));
  }

  function wakeBlockedWaitersInKernelShare(
    handle: ProcessHandle,
    wakeSource: SupervisorBlockedWaitWakeSource,
    pipeUsed?: bigint,
  ): void {
    function isIndefiniteEpollWait(
      member: ProcessHandle,
      blocked: { status: number; vmPtr: number },
    ): boolean {
      return blocked.status === member.kernel.abi_status_epoll_wait()
        && supervisorBlockedEpollWaitTimeoutMs(member.kernel, blocked.vmPtr) < 0;
    }

    function clearAnsweredRunningWorker(member: ProcessHandle): void {
      const runningWorker = hostExecutionState(member).runningWorker;
      if (runningWorker === null) {
        return;
      }
      const runningThread = deps.threadWorkerStates.get(runningWorker);
      if (runningThread === undefined || !threadLatestStepAnswered(runningThread)) {
        return;
      }
      hostExecutionState(member).runningWorker = null;
      clearThreadRunningGeneration(runningThread);
      deps.appendRuntimeLifecycleEvent(
        member.runtimeId,
        `running-worker-cleared:answered-before-wake:${member.pid}:vm=0x${runningThread.vmPtr.toString(16)}`,
      );
    }

    for (const member of kernelShareMemberList(handle)) {
      if (pipeUsed !== undefined) {
        member.kernelState = {
          ...member.kernelState,
          pipeUsed: composeHandlePipeUsed(member, pipeUsed),
        };
      }
      const shouldWake = processWorkerList(member).some((candidate) => {
        const state = deps.threadWorkerStates.get(candidate);
        const waitingInWorkerStatus = getThreadWaitingInWorkerStatus(state);
        return state !== undefined
          && waitingInWorkerStatus !== null
          && supervisorBlockedWaitStatusMatchesWakeSource(
            member.kernel,
            waitingInWorkerStatus,
            wakeSource,
          );
      });
      if (shouldWake) {
        publishKernelRuntimeState(member.runtimeBridge, member.kernelState, true);
      }
      for (const [blockedWorker, blocked] of hostExecutionState(member).hostBlockedWorkers) {
        const action = supervisorBlockedWaitWakeAction(
          member.kernel,
          blocked.status,
          wakeSource,
          { rpcPending: isBlockedRpcPending(blocked) },
        );
        if (
          action !== "skip"
          && isBlockedRpcPending(blocked)
          && isIndefiniteEpollWait(member, blocked)
        ) {
          setEpollWakeRetryPending(blocked, true);
          setEpollPassiveUntilWake(blocked, false);
          deps.appendRuntimeLifecycleEvent(
            member.runtimeId,
            `epoll-wake-defer-rpc:${member.pid}:vm=0x${blocked.vmPtr.toString(16)}:status=${blocked.status}`,
          );
          continue;
        }
        if (action !== "skip" && isIndefiniteEpollWait(member, blocked)) {
          clearAnsweredRunningWorker(member);
        }
        if (
          action !== "skip"
          && isIndefiniteEpollWait(member, blocked)
          && hostExecutionState(member).runningWorker !== null
        ) {
          const wasPending = isEpollWakeRetryPending(blocked);
          const wasPassive = isEpollPassiveUntilWake(blocked);
          setEpollWakeRetryPending(blocked, true);
          setEpollPassiveUntilWake(blocked, false);
          if (!wasPending || wasPassive) {
            deps.appendRuntimeLifecycleEvent(
              member.runtimeId,
              `epoll-wake-defer-running:${member.pid}:vm=0x${blocked.vmPtr.toString(16)}:status=${blocked.status}`,
            );
          }
          continue;
        }
        switch (action) {
          case "skip": {
            if (
              isBlockedRpcPending(blocked)
              && blocked.status === member.kernel.abi_status_pipe_wait()
              && supervisorBlockedWaitStatusMatchesWakeSource(
                member.kernel,
                blocked.status,
                wakeSource,
              )
            ) {
              markBlockedRpcDeferredRetry(blocked);
              deps.appendRuntimeLifecycleEvent(
                member.runtimeId,
                `pipe-wake-defer-rpc:${member.pid}:vm=0x${blocked.vmPtr.toString(16)}:status=${blocked.status}`,
              );
            }
            continue;
          }
          case "retry":
            break;
          case "clear-pipe-sync-and-retry":
            supervisorBlockedPipeSyncBeforeRetryClear(member.kernel, blocked.vmPtr);
            break;
          case "log-epoll-and-retry": {
            const vmPtr = typeof blocked.vmPtr === "number"
              ? `0x${blocked.vmPtr.toString(16)}`
              : "n/a";
            deps.appendRuntimeLifecycleEvent(
              member.runtimeId,
              `epoll-wake-retry:${member.pid}:vm=${vmPtr}:status=${blocked.status}`,
            );
            break;
          }
          default: {
            const exhaustive: never = action;
            throw new Error(`unknown supervisor blocked wait wake action: ${exhaustive}`);
          }
        }
        setBlockedPassiveUntilWake(blocked, false);
        if (isIndefiniteEpollWait(member, blocked)) {
          setEpollWakeRetryPending(blocked, true);
        }
        deps.disarmSupervisorTimerForWake?.(blocked);
        setEpollPassiveUntilWake(blocked, false);
        const runningBeforePump = hostExecutionState(member).runningWorker;
        deps.pumpProcess?.(member);
        if (!hostExecutionState(member).hostBlockedWorkers.has(blockedWorker)) {
          continue;
        }
        if (runningBeforePump === null && hostExecutionState(member).runningWorker !== null) {
          if (isIndefiniteEpollWait(member, blocked)) {
            setEpollPassiveUntilWake(blocked, false);
          }
          continue;
        }
        deps.retryHostBlockedWorker(member, blockedWorker);
      }
    }
  }

  function wakePipeAndEpollWaitersInKernelShare(handle: ProcessHandle, pipeUsed: bigint): void {
    wakeBlockedWaitersInKernelShare(
      handle,
      "pipe-and-epoll-sync",
      pipeUsed,
    );
  }

  function wakePipeWaitersInKernelShare(handle: ProcessHandle, pipeUsed: bigint): void {
    wakeBlockedWaitersInKernelShare(
      handle,
      "pipe-sync",
      pipeUsed,
    );
  }

  function retryEpollWaitersInKernelShare(handle: ProcessHandle): void {
    wakeBlockedWaitersInKernelShare(
      handle,
      "epoll-sync",
    );
  }

  function syscallSyncCause(name: string): SyncCause {
    return { kind: "syscall", nr: -1n, name };
  }

  function blockedRpcSyncCause(status: number): SyncCause {
    return { kind: "blocked-rpc", status };
  }

  function wakeSocketWaitersInKernelShare(
    handle: ProcessHandle,
    socketUsed: number,
    netBufUsed: bigint,
  ): void {
    for (const member of kernelShareMemberList(handle)) {
      member.kernelState = {
        ...member.kernelState,
        socketUsed,
        netBufUsed,
      };
    }
    wakeBlockedWaitersInKernelShare(
      handle,
      "socket-sync",
    );
  }

  function requestEpollStateSync(handle: ProcessHandle, retryWaiters = false): Promise<void> {
    const heldEpollOfd = collectHeldEpollOfdIds(handle).length > 0;
    const startAction = supervisorStateSyncStartAction(handle.kernel, "epoll", {
      activeVforkChildOwnerMismatch: hasStateSyncOwnerMismatch(handle),
      kernelWorkerAvailable: false,
      epollStateKernelOwns: kernelWorkerOwnsEpollState(handle),
      heldEpollOfd,
      wakeWaiters: retryWaiters,
    });
    switch (startAction) {
      case "skip":
        return Promise.resolve();
      case "retry-waiters":
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `epoll-sync-skip-no-ofd:${handle.pid}:retry=1`);
        retryEpollWaitersInKernelShare(handle);
        return Promise.resolve();
      case "request-sync":
        break;
      default: {
        const exhaustive: never = startAction;
        throw new Error(`unknown supervisor state-sync start action: ${exhaustive}`);
      }
    }

    supervisorStateSyncRequest(
      handle.kernel,
      handle.vmPtr,
      "epoll",
      retryWaiters,
    );
    const activeSync = getRuntimeStateSyncPromise(handle, "epoll");
    if (activeSync !== null) {
      return retryWaiters
        ? activeSync.then(() => requestEpollStateSync(handle, retryWaiters))
        : activeSync;
    }
    const syncPromise = (async () => {
      try {
        for (;;) {
          const run = supervisorStateSyncNextRun(
            handle.kernel,
            handle.vmPtr,
            "epoll",
          );
          if (!run.run) {
            break;
          }
          const shouldRetry = run.wakeWaiters;
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `epoll-sync-start:${handle.pid}:retry=${shouldRetry ? 1 : 0}`);
          if (deferStateSyncForOwnerMismatch(handle, "epoll", "before-run")) {
            break;
          }
          await syncEpollStateViaKernelWorker(
            handle,
            shouldRetry
              ? blockedRpcSyncCause(handle.kernel.abi_status_epoll_wait())
              : syscallSyncCause("epoll-state-sync"),
          );
          if (deferStateSyncForOwnerMismatch(handle, "epoll", "after-run")) {
            break;
          }
          if (
            assertProcessIdentityOrDeferStateSync(
              handle,
              "epoll",
              "requestEpollStateSync",
              "after-run",
            )
          ) {
            break;
          }
          if (shouldRetry) {
            retryEpollWaitersInKernelShare(handle);
          }
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `epoll-sync-done:${handle.pid}:retry=${shouldRetry ? 1 : 0}`);
          if (!supervisorStateSyncComplete(
            handle.kernel,
            handle.vmPtr,
            "epoll",
          )) {
            break;
          }
        }
      } catch (error) {
        supervisorStateSyncClear(handle.kernel, handle.vmPtr, "epoll");
        throw error;
      }
    })().finally(() => {
      clearRuntimeStateSyncPromise(handle, "epoll");
    });
    setRuntimeStateSyncPromise(handle, "epoll", syncPromise);

    return syncPromise;
  }

  function requestEpollDependentOfdStateSync(
    handle: ProcessHandle,
    additionalOfdIds: readonly number[] = [],
    options: { mode?: "push" | "pull" } = {},
  ): Promise<void> {
    if (hasActiveVforkChildOwnerMismatch(handle)) {
      return Promise.resolve();
    }
    if (handle.pid <= 0) {
      return Promise.resolve();
    }

    const dependentOfdIds = new Set([
      ...collectProcessSyncDependentOfdIds(handle),
      ...collectHeldEpollDependentSyncOfdIds(handle),
      ...additionalOfdIds,
    ]);
    if (options.mode === "pull") {
      const ofdIds = [...dependentOfdIds];
      if (ofdIds.length === 0) {
        return Promise.resolve();
      }
      return (async () => {
        const activeOfdSync = getOfdSyncPromise(handle);
        if (activeOfdSync !== null) {
          await activeOfdSync;
        }
        await syncOfdStateViaKernelWorker(
          handle,
          ofdIds,
          syscallSyncCause("epoll-dependent-ofd-sync"),
          { mode: "pull" },
        );
        assertProcessIdentity(handle, "requestEpollDependentOfdStateSync");
      })();
    }
    enqueueOfdSyncIds(handle, dependentOfdIds);
    if (queuedOfdSyncIdCount(handle) === 0) {
      return Promise.resolve();
    }
    const activeOfdSync = getOfdSyncPromise(handle);
    if (activeOfdSync !== null) {
      return activeOfdSync;
    }

    const ofdSyncPromise = (async () => {
      while (queuedOfdSyncIdCount(handle) > 0) {
        const ofdIds = takeQueuedOfdSyncIds(handle);
        await syncOfdStateViaKernelWorker(handle, ofdIds, syscallSyncCause("epoll-dependent-ofd-sync"));
        assertProcessIdentity(handle, "requestEpollDependentOfdStateSync");
      }
    })().finally(() => {
      setOfdSyncPromise(handle, null);
    });
    setOfdSyncPromise(handle, ofdSyncPromise);

    return ofdSyncPromise;
  }

  function kernelShareMembersIncludingSelf(handle: ProcessHandle): ProcessHandle[] {
    const members = kernelShareMemberList(handle);
    return members.includes(handle) ? members : [handle, ...members];
  }

  function hasPipeSyncWaiterInKernelShare(handle: ProcessHandle): boolean {
    for (const member of kernelShareMembersIncludingSelf(handle)) {
      const waitingWorker = processWorkerList(member).some((candidate) => {
        const state = deps.threadWorkerStates.get(candidate);
        const waitingInWorkerStatus = getThreadWaitingInWorkerStatus(state);
        return state !== undefined
          && waitingInWorkerStatus !== null
          && supervisorBlockedWaitStatusMatchesWakeSource(
            member.kernel,
            waitingInWorkerStatus,
            "pipe-sync",
          );
      });
      if (waitingWorker) {
        return true;
      }
      if (hostExecutionState(member).hostBlockedWorkers.size > 0) {
        return true;
      }
    }
    return false;
  }

  function hasBufferedHostOutput(handle: ProcessHandle): boolean {
    const stdio = processHostStdio(handle);
    const slots = [stdio.stdoutPipe, stdio.stderrPipe]
      .filter((slot, index, array) => slot > 0 && array.indexOf(slot) === index);
    return slots.some((slot) => pipeSlotHasBufferedData(handle, slot));
  }

  function shouldSkipEmptyExitPipeStateSync(handle: ProcessHandle): boolean {
    if (!processExitInProgress(handle) || handle.vmPtr === 0) {
      return false;
    }
    if (processStdioOwner(handle) !== null) {
      return false;
    }
    if (kernelShareMemberList(handle).some((member) => member !== handle)) {
      return false;
    }
    const executionState = hostExecutionState(handle);
    if (executionState.runningWorker !== null || executionState.hostBlockedWorkers.size > 0) {
      return false;
    }
    if (collectProcessHeldOfds(handle).size > 0) {
      return false;
    }
    if ([0, 1, 2].some((fd) => supervisorProcessPipeSlotForFd(handle, handle.vmPtr, fd) !== 0)) {
      return false;
    }
    if (hasBufferedHostOutput(handle)) {
      return false;
    }
    return !hasPipeSyncWaiterInKernelShare(handle);
  }

  function stateValueIsNonZero(value: bigint | number | undefined): boolean {
    return value !== undefined && value !== 0 && value !== 0n;
  }

  function hasSocketState(handle: ProcessHandle): boolean {
    return handle.kernelState.socketUsed !== 0
      || handle.kernelState.netBufUsed !== 0n
      || stateValueIsNonZero(handle.kernel.kernel_socket_used?.())
      || stateValueIsNonZero(handle.kernel.kernel_net_buf_used?.())
      || hasRuntimeNetworkSocketState(handle);
  }

  function hasSocketSyncWaiterInKernelShare(handle: ProcessHandle): boolean {
    for (const member of kernelShareMembersIncludingSelf(handle)) {
      const waitingWorker = processWorkerList(member).some((candidate) => {
        const state = deps.threadWorkerStates.get(candidate);
        const waitingInWorkerStatus = getThreadWaitingInWorkerStatus(state);
        return state !== undefined
          && waitingInWorkerStatus !== null
          && supervisorBlockedWaitStatusMatchesWakeSource(
            member.kernel,
            waitingInWorkerStatus,
            "socket-sync",
          );
      });
      if (waitingWorker) {
        return true;
      }
      if (hostExecutionState(member).hostBlockedWorkers.size > 0) {
        return true;
      }
    }
    return false;
  }

  function shouldSkipEmptyExitSocketStateSync(handle: ProcessHandle): boolean {
    if (!processExitInProgress(handle) || handle.vmPtr === 0) {
      return false;
    }
    if (kernelShareMemberList(handle).some((member) => member !== handle)) {
      return false;
    }
    const executionState = hostExecutionState(handle);
    if (executionState.runningWorker !== null || executionState.hostBlockedWorkers.size > 0) {
      return false;
    }
    if (collectProcessHeldOfds(handle).size > 0) {
      return false;
    }
    if (hasSocketState(handle)) {
      return false;
    }
    return !hasSocketSyncWaiterInKernelShare(handle);
  }

  function requestPipeStateSync(handle: ProcessHandle, wakeWaiters = false): Promise<void> {
    transferDebugStats(handle).pipeSyncRequests += 1;
    const startAction = supervisorStateSyncStartAction(handle.kernel, "pipe", {
      activeVforkChildOwnerMismatch: hasStateSyncOwnerMismatch(handle),
      kernelWorkerAvailable: processHasKernelWorkerOwnership(handle),
      epollStateKernelOwns: false,
      heldEpollOfd: false,
      wakeWaiters,
    });
    switch (startAction) {
      case "skip":
        return Promise.resolve();
      case "request-sync":
        break;
      case "retry-waiters":
        throw new Error("kernel supervisor returned epoll retry action for pipe state sync");
      default: {
        const exhaustive: never = startAction;
        throw new Error(`unknown supervisor state-sync start action: ${exhaustive}`);
      }
    }

    if (shouldSkipEmptyExitPipeStateSync(handle)) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `pipe-sync-skip-empty-exit:${handle.pid}:wake=${wakeWaiters ? 1 : 0}`,
      );
      return Promise.resolve();
    }

    const shouldStart = supervisorStateSyncRequest(
      handle.kernel,
      handle.vmPtr,
      "pipe",
      wakeWaiters,
    );
    const activeSync = getRuntimeStateSyncPromise(handle, "pipe");
    if (activeSync !== null) {
      return activeSync;
    }
    if (!shouldStart) {
      return Promise.resolve();
    }

    const syncPromise = (async () => {
      try {
        for (;;) {
          const run = supervisorStateSyncNextRun(
            handle.kernel,
            handle.vmPtr,
            "pipe",
          );
          if (!run.run) {
            break;
          }
          const shouldWake = run.wakeWaiters;
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-start:${handle.pid}:wake=${shouldWake ? 1 : 0}`);
          if (deferStateSyncForOwnerMismatch(handle, "pipe", "before-run")) {
            break;
          }
          transferDebugStats(handle).pipeSyncKernelCalls += 1;
          const pipeUsed = await syncPipeStateViaKernelWorker(
            handle,
            shouldWake
              ? blockedRpcSyncCause(handle.kernel.abi_status_pipe_wait())
              : syscallSyncCause("pipe-state-sync"),
          );
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-canonical:${handle.pid}:used=${pipeUsed.toString()}`);
          if (deferStateSyncForOwnerMismatch(handle, "pipe", "after-run")) {
            break;
          }
          assertProcessIdentity(handle, "requestPipeStateSync");
          const visibleHandle = visibleOutputHandle(handle);
          const drainedHiddenOutput = shouldDrainHiddenInheritedOutput(handle, visibleHandle)
            ? deps.drainHiddenProcessOutput(handle)
            : false;
          const drainedVisibleOutput = drainedHiddenOutput
            ? false
            : deps.drainProcessOutput(visibleHandle);
          if (drainedHiddenOutput) {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-hidden-drain:${handle.pid}`);
            transferDebugStats(handle).pipeSyncKernelCalls += 1;
            const refreshedPipeUsed = await syncPipeStateViaKernelWorker(
              handle,
              syscallSyncCause("pipe-hidden-output-drain"),
            );
            assertProcessIdentity(handle, "requestPipeStateSync:after-hidden-drain");
            handle.kernelState = {
              ...handle.kernelState,
              pipeUsed: refreshedPipeUsed,
            };
          }
          if (
            drainedVisibleOutput
            && processHasKernelWorkerOwnership(visibleHandle)
          ) {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-visible-drain:${visibleHandle.pid}`);
            transferDebugStats(handle).pipeSyncVisibleDrainCalls += 1;
            transferDebugStats(visibleHandle).pipeSyncKernelCalls += 1;
            const visiblePipeUsed = await syncPipeStateViaKernelWorker(
              visibleHandle,
              syscallSyncCause("pipe-visible-output-drain"),
            );
            assertProcessIdentity(visibleHandle, "requestPipeStateSync:after-visible-drain");
            visibleHandle.kernelState = {
              ...visibleHandle.kernelState,
              pipeUsed: visiblePipeUsed,
            };
          }
          if (shouldWake) {
            wakePipeWaitersInKernelShare(handle, pipeUsed);
            if (kernelWorkerOwnsEpollState(handle)) {
              await requestEpollDependentOfdStateSync(handle);
              await requestEpollStateSync(handle, true);
            } else {
              wakeBlockedWaitersInKernelShare(
                handle,
                "epoll-sync",
                pipeUsed,
              );
            }
          }
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-sync-done:${handle.pid}:wake=${shouldWake ? 1 : 0}`);
          if (!supervisorStateSyncComplete(
            handle.kernel,
            handle.vmPtr,
            "pipe",
          )) {
            break;
          }
        }
      } catch (error) {
        supervisorStateSyncClear(handle.kernel, handle.vmPtr, "pipe");
        throw error;
      }
    })().finally(() => {
      clearRuntimeStateSyncPromise(handle, "pipe");
    });
    setRuntimeStateSyncPromise(handle, "pipe", syncPromise);

    return syncPromise;
  }

  function requestSocketStateSync(handle: ProcessHandle, wakeWaiters = false): Promise<void> {
    transferDebugStats(handle).socketSyncRequests += 1;
    const startAction = supervisorStateSyncStartAction(handle.kernel, "socket", {
      activeVforkChildOwnerMismatch: hasStateSyncOwnerMismatch(handle),
      kernelWorkerAvailable: processHasKernelWorkerOwnership(handle),
      epollStateKernelOwns: false,
      heldEpollOfd: false,
      wakeWaiters,
    });
    switch (startAction) {
      case "skip":
        return Promise.resolve();
      case "request-sync":
        break;
      case "retry-waiters":
        throw new Error("kernel supervisor returned epoll retry action for socket state sync");
      default: {
        const exhaustive: never = startAction;
        throw new Error(`unknown supervisor state-sync start action: ${exhaustive}`);
      }
    }

    if (shouldSkipEmptyExitSocketStateSync(handle)) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `socket-sync-skip-empty-exit:${handle.pid}:wake=${wakeWaiters ? 1 : 0}`,
      );
      return Promise.resolve();
    }

    const shouldStart = supervisorStateSyncRequest(
      handle.kernel,
      handle.vmPtr,
      "socket",
      wakeWaiters,
    );
    const activeSync = getRuntimeStateSyncPromise(handle, "socket");
    if (activeSync !== null) {
      return activeSync;
    }
    if (!shouldStart) {
      return Promise.resolve();
    }

    const syncPromise = (async () => {
      try {
        for (;;) {
          const run = supervisorStateSyncNextRun(
            handle.kernel,
            handle.vmPtr,
            "socket",
          );
          if (!run.run) {
            break;
          }
          const shouldWake = run.wakeWaiters;
          transferDebugStats(handle).socketSyncKernelCalls += 1;
          if (deferStateSyncForOwnerMismatch(handle, "socket", "before-run")) {
            break;
          }
          const { socketUsed, netBufUsed, socketState } = await syncSocketStateViaKernelWorker(
            handle,
            shouldWake
              ? blockedRpcSyncCause(handle.kernel.abi_status_socket_recv())
              : syscallSyncCause("socket-state-sync"),
          );
          if (deferStateSyncForOwnerMismatch(handle, "socket", "after-run")) {
            break;
          }
          assertProcessIdentity(handle, "requestSocketStateSync");
          if (shouldWake) {
            if (socketState) {
              for (const member of kernelShareMemberList(handle)) {
                if (member !== handle && processHasKernelWorkerOwnership(member)) {
                  applySocketStateFromKernelWorker(member, socketState);
                }
              }
            }
            wakeSocketWaitersInKernelShare(handle, socketUsed, netBufUsed);
          }
          if (!supervisorStateSyncComplete(
            handle.kernel,
            handle.vmPtr,
            "socket",
          )) {
            break;
          }
        }
      } catch (error) {
        supervisorStateSyncClear(handle.kernel, handle.vmPtr, "socket");
        throw error;
      }
    })().finally(() => {
      clearRuntimeStateSyncPromise(handle, "socket");
    });
    setRuntimeStateSyncPromise(handle, "socket", syncPromise);

    return syncPromise;
  }

  return {
    requestEpollDependentOfdStateSync,
    requestEpollStateSync,
    requestPipeStateSync,
    requestSocketStateSync,
    wakePipeAndEpollWaitersInKernelShare,
  };
}
