import {
  wakeRuntimeBridge,
} from "../../bridge/runtime-state.js";
import {
  supervisorBlockedEpollWaitTimeoutMs,
  supervisorBlockedRetryAction,
  type SupervisorBlockedRetryAction,
  type SupervisorBlockedRetryActionFlag,
} from "../../supervisor-blocked-resume.js";
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
  resumeEpollWait(handle: ProcessHandle, worker: Worker, blocked: HostBlockedWorkerState): void;
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
  if (handle.runningWorker !== null) {
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
  reason: string,
): void {
  if (handle.runningWorker !== null) {
    wakeRuntimeBridge(handle.runtimeBridge);
  }
  const runningThread = handle.runningWorker === null
    ? undefined
    : deps.threadWorkerStates.get(handle.runningWorker);
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `epoll-retry-skip:${handle.pid}:running=${handle.runningWorker !== null ? 1 : 0}:reason=${reason}`
      + `:${deps.formatRunningRetryBlocker(handle, runningThread)}`,
  );
  deps.scheduleSupervisorBlockedRetry(
    handle,
    worker,
  );
}

export function executeKernelWorkerBlockedEpollRetry(
  deps: BlockedEpollRetryDeps,
  handle: ProcessHandle,
  worker: Worker,
  blocked: HostBlockedWorkerState,
): void {
  const rpcGeneration = deps.beginBlockedRpc(blocked);
  const epollRetryTimeoutMs = supervisorBlockedEpollWaitTimeoutMs(handle.kernel, blocked.vmPtr);
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `epoll-retry-rpc:${handle.pid}:vm=0x${blocked.vmPtr.toString(16)}`
      + `:timeout=${epollRetryTimeoutMs}`
      + `:epoll=${deps.formatBlockedEpollEntries(handle, blocked.vmPtr)}`,
  );
  void (blockedEpollWatchesPipe(handle, blocked.vmPtr)
    ? deps.requestPipeStateSync(handle, false)
    : Promise.resolve())
    .then(() => deps.requestEpollStateSync(handle, false))
    .then(() => deps.requestEpollDependentOfdStateSync(
      handle,
      collectBlockedEpollDependentSyncOfdIds(handle, blocked.vmPtr),
      { mode: "pull" },
    ))
    .then(() => {
      const latestBlocked = deps.finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
      if (!latestBlocked) {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `epoll-retry-stale:${handle.pid}`);
        return;
      }
      const latestEpollRetryTimeoutMs = supervisorBlockedEpollWaitTimeoutMs(handle.kernel, latestBlocked.vmPtr);
      if (latestEpollRetryTimeoutMs < 0) {
        if (deps.armBlockedSupervisorTimerForBlocked(handle, latestBlocked)) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `epoll-retry-timer:${handle.pid}:vm=0x${latestBlocked.vmPtr.toString(16)}`,
          );
          return;
        }
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
          deferPostSyncRetry(deps, handle, worker, "running-worker");
          return;
        case "defer-process-gate":
          deferPostSyncRetry(deps, handle, worker, "process-gate");
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
      deps.resumeEpollWait(handle, worker, latestBlocked);
    })
    .catch((error) => {
      deps.finishBlockedRpc(handle, worker, blocked.vmPtr, rpcGeneration);
      if (handle.workers.has(worker) && deps.threadWorkerStates.has(worker)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    });
}
