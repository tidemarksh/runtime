import { hostExecutionState } from "../host-execution-state.js";
import { processHasWorker } from "../process-worker-state.js";
import {
  wakeRuntimeBridge,
} from "../../bridge/runtime-state.js";
import {
  supervisorBlockedEpollWaitTimeoutMs,
  supervisorBlockedRetryAction,
  type SupervisorBlockedRetryAction,
  type SupervisorBlockedRetryActionFlag,
} from "../../supervisor-blocked-resume.js";
import {
  getEpollDeferredRetryWakeGeneration,
  getEpollDeferredRetrySignature,
  getEpollWakeRetryGeneration,
  isBlockedRpcPending,
  isEpollWakeRetryPending,
  setEpollDeferredRetrySignature,
  setEpollPassiveUntilWake,
  setEpollWakeRetryPending,
} from "../host-blocked-state.js";
import type {
  HostBlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";

interface BlockedEpollRetryDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  beginBlockedRpc(blocked: HostBlockedWorkerState): number;
  finishBlockedRpc(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    rpcGeneration: number,
  ): HostBlockedWorkerState | null;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  requestEpollStateSync(handle: ProcessHandle, retryWaiters?: boolean): Promise<void>;
  requestEpollDependentOfdStateSync(
    handle: ProcessHandle,
    additionalOfdIds?: readonly number[],
    options?: { mode?: "push" | "pull" },
  ): Promise<void>;
  armBlockedSupervisorTimerForBlocked(handle: ProcessHandle, blocked: HostBlockedWorkerState): boolean;
  clearStaleRunningWorker(handle: ProcessHandle, worker: Worker): void;
  scheduleSupervisorBlockedRetry(handle: ProcessHandle, worker: Worker): void;
  resumeEpollWait(
    handle: ProcessHandle,
    worker: Worker,
    blocked: HostBlockedWorkerState,
    consumedWakeGeneration?: number,
  ): void;
  formatBlockedEpollEntries(handle: ProcessHandle, vmPtr: number): string;
  formatRunningRetryBlocker(handle: ProcessHandle, runningThread: ThreadWorkerState | undefined): string;
}

function blockedEpollWatchesPipe(handle: ProcessHandle, vmPtr: number): boolean {
  const supervisorWatchesPipe = handle.kernel.supervisor_blocked_epoll_watches_pipe;
  if (supervisorWatchesPipe === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_epoll_watches_pipe");
  }
  return supervisorWatchesPipe.call(handle.kernel, vmPtr) !== 0;
}

function collectBlockedEpollDependentSyncOfdIds(
  handle: ProcessHandle,
  vmPtr: number,
): number[] {
  const supervisorDependentOfd = handle.kernel.supervisor_blocked_epoll_dependent_sync_ofd;
  if (supervisorDependentOfd === undefined) {
    throw new Error("kernel supervisor ABI missing supervisor_blocked_epoll_dependent_sync_ofd");
  }

  const maxFds = handle.kernel.abi_max_fds();
  const ofdIds: number[] = [];
  for (let index = 0; index < maxFds; index += 1) {
    const ofdId = supervisorDependentOfd.call(handle.kernel, vmPtr, index);
    if (ofdId === 0) {
      break;
    }
    if (
      ofdId > 0
      && ofdId < maxFds
      && !ofdIds.includes(ofdId)
    ) {
      ofdIds.push(ofdId);
    }
  }
  return ofdIds;
}

function postSyncRetryActionFlags(
  handle: ProcessHandle,
): readonly SupervisorBlockedRetryActionFlag[] {
  const flags: SupervisorBlockedRetryActionFlag[] = ["wait-timer-already-checked"];
  if (hostExecutionState(handle).runningWorker !== null) {
    flags.push("running-worker");
  }
  return flags;
}

function readPostSyncRetryAction(
  deps: BlockedEpollRetryDeps,
  handle: ProcessHandle,
  latestBlocked: HostBlockedWorkerState,
): SupervisorBlockedRetryAction | null {
  try {
    return supervisorBlockedRetryAction(
      handle.kernel,
      latestBlocked.vmPtr,
      postSyncRetryActionFlags(handle),
    );
  } catch (error) {
    deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    return null;
  }
}

function deferPostSyncRetry(
  deps: BlockedEpollRetryDeps,
  handle: ProcessHandle,
  worker: Worker,
  blocked: HostBlockedWorkerState,
  reason: string,
): void {
  const runningWorker = hostExecutionState(handle).runningWorker;
  if (runningWorker !== null) {
    wakeRuntimeBridge(handle.runtimeBridge);
  }
  const runningThread = runningWorker === null
    ? undefined
    : deps.threadWorkerStates.get(runningWorker);
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `epoll-retry-skip:${handle.pid}:running=${runningWorker !== null ? 1 : 0}:reason=${reason}`
      + `:${deps.formatRunningRetryBlocker(handle, runningThread)}`,
  );
  if (
    reason === "running-worker"
    && supervisorBlockedEpollWaitTimeoutMs(handle.kernel, blocked.vmPtr) < 0
  ) {
    setEpollDeferredRetrySignature(blocked, deps.formatBlockedEpollEntries(handle, blocked.vmPtr));
    if (isEpollWakeRetryPending(blocked)) {
      setEpollPassiveUntilWake(blocked, false);
      return;
    }
    setEpollPassiveUntilWake(blocked, true);
    return;
  }
  deps.scheduleSupervisorBlockedRetry(
    handle,
    worker,
  );
}

function deferRetryEntryForRunningWorker(
  deps: BlockedEpollRetryDeps,
  handle: ProcessHandle,
  blocked: HostBlockedWorkerState,
): boolean {
  const runningWorker = hostExecutionState(handle).runningWorker;
  if (
    runningWorker === null
    || supervisorBlockedEpollWaitTimeoutMs(handle.kernel, blocked.vmPtr) >= 0
  ) {
    return false;
  }
  wakeRuntimeBridge(handle.runtimeBridge);
  const runningThread = deps.threadWorkerStates.get(runningWorker);
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `epoll-retry-entry-defer-running:${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}`
      + `:pending=${isEpollWakeRetryPending(blocked) ? 1 : 0}`
      + `:${deps.formatRunningRetryBlocker(handle, runningThread)}`,
  );
  if (isEpollWakeRetryPending(blocked)) {
    setEpollPassiveUntilWake(blocked, false);
    return true;
  }
  setEpollPassiveUntilWake(blocked, true);
  return true;
}

export function executeKernelWorkerBlockedEpollRetry(
  deps: BlockedEpollRetryDeps,
  handle: ProcessHandle,
  worker: Worker,
  blocked: HostBlockedWorkerState,
): void {
  const epollRetryTimeoutMs = supervisorBlockedEpollWaitTimeoutMs(handle.kernel, blocked.vmPtr);
  const wakeGenerationAtRetryStart = getEpollWakeRetryGeneration(blocked);
  const epollRetrySignature = epollRetryTimeoutMs < 0
    ? deps.formatBlockedEpollEntries(handle, blocked.vmPtr)
    : null;
  if (
    epollRetrySignature !== null
    && !isEpollWakeRetryPending(blocked)
    && getEpollDeferredRetrySignature(blocked) === epollRetrySignature
    && getEpollDeferredRetryWakeGeneration(blocked) === getEpollWakeRetryGeneration(blocked)
  ) {
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `epoll-retry-skip-unchanged:${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}`,
    );
    if (isEpollWakeRetryPending(blocked)) {
      setEpollWakeRetryPending(blocked, false);
    }
    setEpollPassiveUntilWake(blocked, true);
    return;
  }
  if (getEpollDeferredRetrySignature(blocked) !== null) {
    setEpollDeferredRetrySignature(blocked, null);
  }
  if (deferRetryEntryForRunningWorker(deps, handle, blocked)) {
    return;
  }
  setEpollPassiveUntilWake(blocked, false);
  const rpcGeneration = deps.beginBlockedRpc(blocked);
  const dependentOfdIds = collectBlockedEpollDependentSyncOfdIds(handle, blocked.vmPtr);
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `epoll-retry-rpc:${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}`
      + `:timeout=${epollRetryTimeoutMs}`
      + `:depOfds=${dependentOfdIds.join(",") || "none"}`
      + `:epoll=${deps.formatBlockedEpollEntries(handle, blocked.vmPtr)}`,
  );
  void (blockedEpollWatchesPipe(handle, blocked.vmPtr)
    ? deps.requestPipeStateSync(handle, false)
    : Promise.resolve())
    .then(() => deps.requestEpollStateSync(handle, false))
    .then(() => deps.requestEpollDependentOfdStateSync(
      handle,
      dependentOfdIds,
    ))
    .then(() => {
      const latestBlocked = deps.finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
      if (!latestBlocked) {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `epoll-retry-stale:${handle.pid}`);
        const currentBlocked = hostExecutionState(handle).hostBlockedWorkers.get(worker);
        if (
          currentBlocked?.vmPtr === blocked.vmPtr
          && isEpollWakeRetryPending(currentBlocked)
          && !isBlockedRpcPending(currentBlocked)
        ) {
          setEpollPassiveUntilWake(currentBlocked, false);
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `epoll-retry-stale-wake-retry:${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}`,
          );
          deps.scheduleSupervisorBlockedRetry(handle, worker);
        }
        return;
      }
      const latestEpollRetryTimeoutMs = supervisorBlockedEpollWaitTimeoutMs(handle.kernel, latestBlocked.vmPtr);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `epoll-retry-post-sync:${handle.pid}:vm=0x${latestBlocked.vmPtr.toString(16)}`
          + `:timeout=${latestEpollRetryTimeoutMs}`
          + `:epoll=${deps.formatBlockedEpollEntries(handle, latestBlocked.vmPtr)}`,
      );
      if (deps.armBlockedSupervisorTimerForBlocked(handle, latestBlocked)) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `epoll-retry-timer:${handle.pid}:vm=0x${latestBlocked.vmPtr.toString(16)}`,
        );
        return;
      }
      if (latestEpollRetryTimeoutMs < 0) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `epoll-retry-no-deadline:${handle.pid}:vm=0x${latestBlocked.vmPtr.toString(16)}`
            + `:timeout=${latestEpollRetryTimeoutMs}`,
        );
      }
      deps.clearStaleRunningWorker(handle, worker);
      const action = readPostSyncRetryAction(deps, handle, latestBlocked);
      switch (action) {
        case null:
          return;
        case "defer-running-worker":
          deferPostSyncRetry(deps, handle, worker, latestBlocked, "running-worker");
          return;
        case "defer-process-gate":
          deferPostSyncRetry(deps, handle, worker, latestBlocked, "process-gate");
          return;
        case "arm-wait-timer":
          if (deps.armBlockedSupervisorTimerForBlocked(handle, latestBlocked)) {
            deps.appendRuntimeLifecycleEvent(
              handle.runtimeId,
              `epoll-retry-timer:${handle.pid}:vm=0x${latestBlocked.vmPtr.toString(16)}`,
            );
            return;
          }
          break;
        case "resume-epoll-direct":
        case "resume-direct":
          break;
        case "defer-passive-wait":
          deps.scheduleSupervisorBlockedRetry(handle, worker);
          return;
        default: {
          deps.failProcess(
            handle,
            new Error(`kernel supervisor returned invalid post-sync epoll retry action: ${action}`),
          );
          return;
        }
      }
      deps.resumeEpollWait(handle, worker, latestBlocked, wakeGenerationAtRetryStart);
    })
    .catch((error) => {
      deps.finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
      if (processHasWorker(handle, worker) && deps.threadWorkerStates.has(worker)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    });
}
