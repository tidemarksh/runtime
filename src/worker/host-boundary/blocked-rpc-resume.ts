import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import {
  resumeBlockedSyscallBySupervisor,
  supervisorBlockedPollWaitHasSocket,
  supervisorBlockedResumeWritebacks,
  supervisorBlockedPipeResumeAction,
  supervisorBlockedStatusIsEpollWait,
  supervisorBlockedStatusIsPipeWait,
  supervisorBlockedStatusSyncsSocketAfterResume,
} from "../../supervisor-blocked-resume.js";
import { forceNetworkClosedFds } from "../network-socket-close.js";
import { supervisorProcessOwnerMismatch } from "../process-identity.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import {
  completedKernelRpcOutcome,
  type KernelRpcResumeOutcome,
} from "../kernel-rpc/outcomes.js";
import type { SyncCause } from "../contracts/sync.js";
import { appendPendingGuestMemoryWrites } from "../guest-memory-writes.js";

export interface WorkerKernelRpcBlockedRpcDeps {
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  clearHostBlockedRetryTimer(handle: ProcessHandle, worker: Worker): void;
  requestThreadWorkerBlockedResume(handle: ProcessHandle, worker: Worker): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  syncBlockedForkSharedStateViaKernelWorker(handle: ProcessHandle, sourceVmPtr: number | undefined, cause: SyncCause): Promise<void>;
  syncSocketStateViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<{ socketUsed: number; netBufUsed: bigint }>;
}

type HostBlockedRetryResultHandler = (
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  status: number,
) => void;

interface HostBlockedSyscallResumeMode {
  emitRetryResult(status: number): void;
}

type ExecuteHostBlockedSyscallResumeForTest = (
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  blockedStatus: number,
  handleHostBlockedRetryResult: HostBlockedRetryResultHandler,
) => Promise<void>;

interface WorkerKernelRpcBlockedRpcInternals {
  executeHostBlockedSyscallResumeOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    blockedStatus: number,
  ): Promise<KernelRpcResumeOutcome>;
  executeHostBlockedSyscallResumeForTest: ExecuteHostBlockedSyscallResumeForTest;
}

function createWorkerKernelRpcBlockedRpcHelperInternals(deps: WorkerKernelRpcBlockedRpcDeps): WorkerKernelRpcBlockedRpcInternals {
function blockedRpcSyncCause(status: number): SyncCause {
  return { kind: "blocked-rpc", status };
}

function queueGuestBufferWrite(
  handle: ProcessHandle,
  worker: Worker,
  ptr: bigint | number,
  length: bigint | number,
): void {
  const start = typeof ptr === "bigint" ? Number(ptr) : ptr;
  const requested = typeof length === "bigint" ? Number(length) : length;
  if (!Number.isFinite(start) || !Number.isFinite(requested) || start <= 0 || requested <= 0) {
    return;
  }
  const absolute = handle.guestBase + start;
  const writeLen = requested;
  const memory = new Uint8Array(handle.memory.buffer);
  if (absolute < 0 || absolute + writeLen > memory.byteLength) {
    return;
  }
  const thread = deps.threadWorkerStates.get(worker);
  if (!thread) {
    return;
  }
  appendPendingGuestMemoryWrites(thread, [{
    guestPtr: start,
    bytes: memory.slice(absolute, absolute + writeLen),
  }]);
}

function blockedPipeWaitFd(handle: ProcessHandle, vmPtr: number): number {
  const blockedFd = handle.kernel.thread_blocked_fd?.(vmPtr) ?? -1;
  if (blockedFd >= 0) {
    return blockedFd;
  }
  const blockedArg0 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0);
  if (blockedArg0 !== undefined) {
    return Number(blockedArg0);
  }
  return Number(handle.kernel.thread_a0(vmPtr));
}

async function syncSocketStatePreservingNetworkBridge(handle: ProcessHandle, status: number): Promise<{
  socketUsed: number;
  netBufUsed: bigint;
}> {
  if (handle.networkConnections.size > 0) {
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    return {
      socketUsed: handle.kernelState.socketUsed,
      netBufUsed: handle.kernelState.netBufUsed,
    };
  }
  return deps.syncSocketStateViaKernelWorker(handle, blockedRpcSyncCause(status));
}



// Executes host-side synchronization and guest-memory writeback around the
// supervisor-owned blocked-syscall resume decision.
async function executeHostBlockedSyscallResume(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  blockedStatus: number,
  mode: HostBlockedSyscallResumeMode,
): Promise<void> {
  let rpcGeneration: number | null = null;
  let canonicalEofReadyAtStart = false;
  let preserveCanonicalEofReadyOnFinish = false;
  const preserveCanonicalEofReadyForDeferredRetry = (): void => {
    if (canonicalEofReadyAtStart && supervisorBlockedStatusIsPipeWait(handle.kernel, blockedStatus)) {
      preserveCanonicalEofReadyOnFinish = true;
    }
  };
  const emitBlockedRpcRetryResult = (status: number): void => {
    mode.emitRetryResult(status);
  };
  const finishBlockedRpc = (): void => {
    const blocked = handle.hostBlockedWorkers.get(worker);
    if (
      blocked
      && blocked.vmPtr === vmPtr
      && (rpcGeneration === null || blocked.rpcGeneration === rpcGeneration)
    ) {
      blocked.rpcPending = false;
      if (!preserveCanonicalEofReadyOnFinish) {
        blocked.pipeCanonicalEofReady = false;
      }
    }
  };
  try {
    deps.appendWorkerDebugLog(handle, `blocked-rpc-start:${blockedStatus}:0x${vmPtr.toString(16)}`);
    deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:start:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-start:${handle.pid}:${blockedStatus}`);
    const blockedAtStart = handle.hostBlockedWorkers.get(worker);
    if (blockedAtStart && blockedAtStart.vmPtr === vmPtr) {
      blockedAtStart.rpcPending = true;
      blockedAtStart.rpcGeneration = (blockedAtStart.rpcGeneration ?? 0) + 1;
      rpcGeneration = blockedAtStart.rpcGeneration;
    } else {
      return;
    }
    canonicalEofReadyAtStart = blockedAtStart.pipeCanonicalEofReady === true;
    const isCurrentBlockedRpc = (): boolean => {
      const blocked = handle.hostBlockedWorkers.get(worker);
      return blocked !== undefined
        && blocked.vmPtr === vmPtr
        && rpcGeneration !== null
        && blocked.rpcGeneration === rpcGeneration;
    };
    const abortIfStaleBlockedRpc = (): boolean => {
      if (isCurrentBlockedRpc()) {
        return false;
      }
      deps.appendWorkerDebugLog(handle, `blocked-rpc-stale:${blockedStatus}:0x${vmPtr.toString(16)}`);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-stale:${handle.pid}:${blockedStatus}`);
      return true;
    };
    forceNetworkClosedFds(handle, vmPtr);
    const resumeDirectlyBySupervisor = async (options: {
      stage: string;
      syncAfterResumeStage: string;
      emitResumeLifecycleEvent: boolean;
      includeStageInDoneEvent: boolean;
      requireBlockedBeforeDone: boolean;
      syncSocketAfterResume: boolean;
    }): Promise<void> => {
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      forceNetworkClosedFds(handle, vmPtr);
      const resumeResult = resumeBlockedSyscallBySupervisor(handle.kernel, vmPtr);
      for (const writeback of supervisorBlockedResumeWritebacks(handle.kernel, vmPtr, blockedStatus, resumeResult)) {
        queueGuestBufferWrite(handle, worker, writeback.guestPtr, writeback.length);
      }
      if (options.emitResumeLifecycleEvent) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `${options.stage}:${handle.pid}:ret=${resumeResult.toString()}:status=${handle.kernel.thread_status(vmPtr)}`,
        );
      }
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      const transientProcessOwnerMismatch = supervisorProcessOwnerMismatch(handle.kernel, vmPtr, handle.pid);
      if (
        !transientProcessOwnerMismatch
        && !!handle.kernelWorkerRequest
        && handle.pid > 0
        && !supervisorBlockedStatusIsEpollWait(handle.kernel, blockedStatus)
      ) {
        deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:${options.syncAfterResumeStage}:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
        await deps.syncBlockedForkSharedStateViaKernelWorker(handle, vmPtr, blockedRpcSyncCause(blockedStatus));
        if (abortIfStaleBlockedRpc()) {
          return;
        }
      }
      if (
        !transientProcessOwnerMismatch
        && options.syncSocketAfterResume
      ) {
        await syncSocketStatePreservingNetworkBridge(handle, blockedStatus);
        if (abortIfStaleBlockedRpc()) {
          return;
        }
      }
      if (options.requireBlockedBeforeDone) {
        const blocked = handle.hostBlockedWorkers.get(worker);
        if (!blocked || blocked.vmPtr !== vmPtr) {
          return;
        }
      }
      finishBlockedRpc();
      const doneStatus = handle.kernel.thread_status(vmPtr);
      const doneSuffix = options.includeStageInDoneEvent ? `:${options.stage}` : "";
      deps.appendWorkerDebugLog(handle, `blocked-rpc-done:${doneStatus}:0x${vmPtr.toString(16)}${doneSuffix}`);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-done:${handle.pid}:${doneStatus}${doneSuffix}`);
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }
      emitBlockedRpcRetryResult(doneStatus);
    };
    const applySupervisorPipeAction = async (): Promise<boolean> => {
      if (!supervisorBlockedStatusIsPipeWait(handle.kernel, blockedStatus)) {
        return false;
      }
      if (supervisorBlockedPollWaitHasSocket(handle.kernel, vmPtr)) {
        deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-socket-supervisor-pipe:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
        await syncSocketStatePreservingNetworkBridge(handle, blockedStatus);
        if (abortIfStaleBlockedRpc()) {
          return true;
        }
      }
      const supervisorPipeAction = supervisorBlockedPipeResumeAction(handle.kernel, vmPtr);
      switch (supervisorPipeAction) {
        case "defer":
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          preserveCanonicalEofReadyForDeferredRetry();
          finishBlockedRpc();
          deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-pipe-supervisor:0x${vmPtr.toString(16)}`);
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-defer-pipe-supervisor:${handle.pid}`);
          emitBlockedRpcRetryResult(blockedStatus);
          return true;
        case "direct":
          await resumeDirectlyBySupervisor({
            stage: "blocked-rpc-supervisor-pipe-resume",
            syncAfterResumeStage: "sync-after-supervisor-resume",
            emitResumeLifecycleEvent: true,
            includeStageInDoneEvent: true,
            requireBlockedBeforeDone: false,
            syncSocketAfterResume: false,
          });
          return true;
        default: {
          const exhaustive: never = supervisorPipeAction;
          throw new Error(`unhandled supervisor blocked pipe resume action: ${exhaustive}`);
        }
      }
    };
    if (await applySupervisorPipeAction()) {
      return;
    }

    await resumeDirectlyBySupervisor({
      stage: "blocked-rpc-resume",
      syncAfterResumeStage: "sync-after-resume",
      emitResumeLifecycleEvent: false,
      includeStageInDoneEvent: false,
      requireBlockedBeforeDone: true,
      syncSocketAfterResume: supervisorBlockedStatusSyncsSocketAfterResume(handle.kernel, blockedStatus),
    });
  } catch (error) {
    finishBlockedRpc();
    if (handle.workers.has(worker) && deps.threadWorkerStates.has(worker)) {
      const fd = blockedPipeWaitFd(handle, vmPtr);
      const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
      const buf = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 1) ?? handle.kernel.thread_a1(vmPtr);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      const debug = [
        `vm=0x${vmPtr.toString(16)}`,
        `fd=${fd}`,
        `buf=${buf.toString()}`,
        `process_ptr=0x${handle.kernel.thread_process_ptr(vmPtr).toString(16)}`,
        `ofd_table_ptr=0x${handle.kernel.kernel_ofd_table_ptr().toString(16)}`,
        `ofd_id=${ofdId}`,
        `ofd_type=${handle.kernel.kernel_ofd_fd_type(ofdId)}`,
        `ofd_host_fd=${handle.kernel.kernel_ofd_host_fd(ofdId)}`,
        `ofd_flags=${handle.kernel.kernel_ofd_status_flags(ofdId)}`,
        `ofd_ino=${handle.kernel.kernel_ofd_ino(ofdId)}`,
        `ofd_offset=${handle.kernel.kernel_ofd_offset(ofdId).toString()}`,
        `blocked_fd=${handle.kernel.thread_blocked_fd(vmPtr)}`,
        `blocked_ofd_id=${handle.kernel.thread_blocked_ofd_id(vmPtr)}`,
      ].join(" ");
      deps.failProcess(handle, new Error(`${detail}
${debug}`));
    }
  } finally {
    finishBlockedRpc();
  }
}


  async function executeHostBlockedSyscallResumeOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    blockedStatus: number,
  ): Promise<KernelRpcResumeOutcome> {
    const collectedRetryResult: { status?: number } = {};
    await executeHostBlockedSyscallResume(
      handle,
      worker,
      vmPtr,
      blockedStatus,
      {
        emitRetryResult(status) {
          collectedRetryResult.status = status;
        },
      },
    );
    if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
      return { kind: "process-gone" };
    }
    if (!handle.hostBlockedWorkers.has(worker)) {
      return { kind: "queued-in-worker" };
    }
    if (collectedRetryResult.status !== undefined) {
      return completedKernelRpcOutcome(collectedRetryResult.status);
    }
    const status = handle.kernel.thread_status?.(vmPtr);
    if (status === undefined) {
      return { kind: "stale" };
    }
    return { kind: "blocked", status };
  }

  async function executeHostBlockedSyscallResumeForTest(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    blockedStatus: number,
    handleHostBlockedRetryResult: HostBlockedRetryResultHandler,
  ): Promise<void> {
    await executeHostBlockedSyscallResume(
      handle,
      worker,
      vmPtr,
      blockedStatus,
      {
        emitRetryResult(status) {
          handleHostBlockedRetryResult(handle, worker, vmPtr, status);
        },
      },
    );
  }

  return {
    executeHostBlockedSyscallResumeForTest,
    executeHostBlockedSyscallResumeOutcome,
  };
}

export function createWorkerKernelRpcBlockedRpcHelpers(deps: WorkerKernelRpcBlockedRpcDeps) {
  const { executeHostBlockedSyscallResumeOutcome } = createWorkerKernelRpcBlockedRpcHelperInternals(deps);
  return { executeHostBlockedSyscallResumeOutcome };
}

export function createWorkerKernelRpcBlockedRpcTestHelpers(
  deps: Omit<WorkerKernelRpcBlockedRpcDeps, "clearHostBlockedRetryTimer" | "requestThreadWorkerBlockedResume">
    & Partial<Pick<WorkerKernelRpcBlockedRpcDeps, "clearHostBlockedRetryTimer" | "requestThreadWorkerBlockedResume">>
    & { handleHostBlockedRetryResult?: HostBlockedRetryResultHandler },
) {
  const internals = createWorkerKernelRpcBlockedRpcHelperInternals({
    ...deps,
    clearHostBlockedRetryTimer: deps.clearHostBlockedRetryTimer ?? (() => {}),
    requestThreadWorkerBlockedResume: deps.requestThreadWorkerBlockedResume ?? (() => {}),
  });
  return {
    executeHostBlockedSyscallResume(
      handle: ProcessHandle,
      worker: Worker,
      vmPtr: number,
      blockedStatus: number,
    ): Promise<void> {
      if (deps.handleHostBlockedRetryResult === undefined) {
        throw new Error("blocked-rpc retry callback is only available through test helper deps");
      }
      return internals.executeHostBlockedSyscallResumeForTest(
        handle,
        worker,
        vmPtr,
        blockedStatus,
        deps.handleHostBlockedRetryResult,
      );
    },
    executeHostBlockedSyscallResumeOutcome: internals.executeHostBlockedSyscallResumeOutcome,
  };
}
