import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import {
  resumeBlockedSyscallBySupervisor,
  supervisorBlockedPipeResumeAction,
  SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DEFER,
  SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DIRECT,
} from "../../supervisor-blocked-resume.js";
import { forceNetworkClosedFds } from "../network-socket-close.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import { POLLFD_BYTES } from "../../layout/poll.js";
import {
  completedKernelRpcOutcome,
  type KernelRpcResumeEffect,
  type KernelRpcResumeOutcome,
} from "./outcomes.js";
import type { SyncCause } from "../contracts/sync.js";
import { appendPendingGuestMemoryWrites } from "../guest-memory-writes.js";

export interface WorkerKernelRpcBlockedRpcDeps {
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
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
  applyPeerHostEffects: boolean;
  emitEffect(effect: KernelRpcResumeEffect): void;
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
  const pendingOutcomeEffects = new WeakMap<Worker, KernelRpcResumeEffect[]>();

  function pushOutcomeEffect(worker: Worker, effect: KernelRpcResumeEffect): void {
    const effects = pendingOutcomeEffects.get(worker);
    if (effects) {
      effects.push(effect);
      return;
    }
    pendingOutcomeEffects.set(worker, [effect]);
  }

  function takeOutcomeEffects(worker: Worker): KernelRpcResumeEffect[] | undefined {
    const effects = pendingOutcomeEffects.get(worker);
    pendingOutcomeEffects.delete(worker);
    return effects;
  }

function blockedRpcSyncCause(status: number): SyncCause {
  return { kind: "blocked-rpc", status };
}

function blockedStatusIsEpollWait(handle: ProcessHandle, status: number): boolean {
  const epollWaitStatus = handle.kernel.abi_status_epoll_wait?.();
  return epollWaitStatus !== undefined && status === epollWaitStatus;
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

function syscallMatches(
  syscallNr: bigint,
  abiSyscall: (() => bigint | number) | undefined,
): boolean {
  const value = abiSyscall?.();
  return value !== undefined && syscallNr === BigInt(value);
}

function queueBlockedPollWaitGuestMemoryWrites(
  handle: ProcessHandle,
  worker: Worker,
  syscallNr: bigint,
  arg0: bigint,
  arg1: bigint,
  arg2: bigint,
  arg3: bigint,
): boolean {
  const maxFds = Math.max(0, handle.kernel.abi_max_fds?.() ?? 1024);
  if (syscallMatches(syscallNr, handle.kernel.abi_sys_ppoll)) {
    const nfds = Math.min(Number(arg1), maxFds);
    if (Number.isFinite(nfds) && nfds > 0) {
      queueGuestBufferWrite(handle, worker, arg0, nfds * POLLFD_BYTES);
    }
    return true;
  }
  if (syscallMatches(syscallNr, handle.kernel.abi_sys_pselect6)) {
    const nfds = Math.min(Number(arg0), maxFds);
    if (!Number.isFinite(nfds) || nfds <= 0) {
      return true;
    }
    const setBytes = Math.floor((nfds + 63) / 64) * 8;
    queueGuestBufferWrite(handle, worker, arg1, setBytes);
    queueGuestBufferWrite(handle, worker, arg2, setBytes);
    queueGuestBufferWrite(handle, worker, arg3, setBytes);
    return true;
  }
  return false;
}

function blockedPipeResumeWritesGuestBuffer(handle: ProcessHandle, syscallNr: bigint): boolean {
  return syscallMatches(syscallNr, handle.kernel.abi_sys_read);
}

function blockedSocketResumeWritesGuestBuffer(handle: ProcessHandle, syscallNr: bigint): boolean {
  return syscallMatches(syscallNr, handle.kernel.abi_sys_read)
    || syscallMatches(syscallNr, handle.kernel.abi_sys_recvfrom);
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
    if (canonicalEofReadyAtStart && blockedStatus === handle.kernel.abi_status_pipe_wait()) {
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
    const resumeDirectlyBySupervisor = async (stage: string): Promise<void> => {
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      const blockedSyscallNr = handle.kernel.thread_blocked_syscall_nr?.(vmPtr) ?? 0n;
      const blockedArg0 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0) ?? 0n;
      const blockedArg1 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 1) ?? 0n;
      const blockedArg2 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 2) ?? 0n;
      const blockedArg3 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 3) ?? 0n;
      forceNetworkClosedFds(handle, vmPtr);
      const resumeResult = resumeBlockedSyscallBySupervisor(handle.kernel, vmPtr);
      if (blockedStatus === handle.kernel.abi_status_pipe_wait() && resumeResult > 0n) {
        const queuedPollWrites = queueBlockedPollWaitGuestMemoryWrites(
          handle,
          worker,
          blockedSyscallNr,
          blockedArg0,
          blockedArg1,
          blockedArg2,
          blockedArg3,
        );
        if (!queuedPollWrites && blockedPipeResumeWritesGuestBuffer(handle, blockedSyscallNr)) {
          queueGuestBufferWrite(handle, worker, blockedArg1, resumeResult);
        }
      }
      if (
        blockedStatus === handle.kernel.abi_status_socket_recv()
        && resumeResult > 0n
        && blockedSocketResumeWritesGuestBuffer(handle, blockedSyscallNr)
      ) {
        queueGuestBufferWrite(handle, worker, blockedArg1, resumeResult);
      }
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `${stage}:${handle.pid}:ret=${resumeResult.toString()}:status=${handle.kernel.thread_status(vmPtr)}`,
      );
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      const currentPid = handle.kernel.process_current_pid?.(vmPtr) ?? handle.pid;
      const transientProcessOwnerMismatch = currentPid > 0 && currentPid !== handle.pid;
      if (
        !transientProcessOwnerMismatch
        && !blockedStatusIsEpollWait(handle, blockedStatus)
      ) {
        deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-after-supervisor-resume:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
        await deps.syncBlockedForkSharedStateViaKernelWorker(handle, vmPtr, blockedRpcSyncCause(blockedStatus));
        if (abortIfStaleBlockedRpc()) {
          return;
        }
      }
      finishBlockedRpc();
      deps.appendWorkerDebugLog(handle, `blocked-rpc-done:${handle.kernel.thread_status(vmPtr)}:0x${vmPtr.toString(16)}:${stage}`);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-done:${handle.pid}:${handle.kernel.thread_status(vmPtr)}:${stage}`);
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }
      emitBlockedRpcRetryResult(handle.kernel.thread_status(vmPtr));
    };
    const applySupervisorPipeAction = async (): Promise<boolean> => {
      if (blockedStatus !== handle.kernel.abi_status_pipe_wait()) {
        return false;
      }
      if (handle.kernel.thread_poll_wait_has_socket?.(vmPtr) === 1) {
        deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-socket-supervisor-pipe:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
        await syncSocketStatePreservingNetworkBridge(handle, blockedStatus);
        if (abortIfStaleBlockedRpc()) {
          return true;
        }
      }
      const supervisorPipeAction = supervisorBlockedPipeResumeAction(handle.kernel, vmPtr);
      switch (supervisorPipeAction) {
        case SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DEFER:
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          preserveCanonicalEofReadyForDeferredRetry();
          finishBlockedRpc();
          deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-pipe-supervisor:0x${vmPtr.toString(16)}`);
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-defer-pipe-supervisor:${handle.pid}`);
          emitBlockedRpcRetryResult(blockedStatus);
          return true;
        case SUPERVISOR_BLOCKED_PIPE_RESUME_ACTION_DIRECT:
          await resumeDirectlyBySupervisor("blocked-rpc-supervisor-pipe-resume");
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

    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    const blockedSyscallNr = handle.kernel.thread_blocked_syscall_nr?.(vmPtr) ?? 0n;
    const blockedArg0 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0) ?? 0n;
    const blockedArg1 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 1) ?? 0n;
    const blockedArg2 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 2) ?? 0n;
    const blockedArg3 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 3) ?? 0n;
    forceNetworkClosedFds(handle, vmPtr);
    const resumeResult = resumeBlockedSyscallBySupervisor(handle.kernel, vmPtr);
    if (blockedStatus === handle.kernel.abi_status_pipe_wait() && resumeResult > 0n) {
      const queuedPollWrites = queueBlockedPollWaitGuestMemoryWrites(
        handle,
        worker,
        blockedSyscallNr,
        blockedArg0,
        blockedArg1,
        blockedArg2,
        blockedArg3,
      );
      if (!queuedPollWrites && blockedPipeResumeWritesGuestBuffer(handle, blockedSyscallNr)) {
        queueGuestBufferWrite(handle, worker, blockedArg1, resumeResult);
      }
    }
    if (
      blockedStatus === handle.kernel.abi_status_socket_recv()
      && resumeResult > 0n
      && blockedSocketResumeWritesGuestBuffer(handle, blockedSyscallNr)
    ) {
      queueGuestBufferWrite(handle, worker, blockedArg1, resumeResult);
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    const currentPid = handle.kernel.process_current_pid?.(vmPtr) ?? handle.pid;
    const transientProcessOwnerMismatch = currentPid > 0 && currentPid !== handle.pid;
    if (
      !transientProcessOwnerMismatch
      && !blockedStatusIsEpollWait(handle, blockedStatus)
    ) {
      deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-after-resume:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
      await deps.syncBlockedForkSharedStateViaKernelWorker(handle, vmPtr, blockedRpcSyncCause(blockedStatus));
      if (abortIfStaleBlockedRpc()) {
        return;
      }
    }

    if (
      !transientProcessOwnerMismatch
      && (
        blockedStatus === handle.kernel.abi_status_connect()
        || blockedStatus === handle.kernel.abi_status_socket_recv()
        || blockedStatus === handle.kernel.abi_status_socket_send()
      )
    ) {
      await syncSocketStatePreservingNetworkBridge(handle, blockedStatus);
      if (abortIfStaleBlockedRpc()) {
        return;
      }
    }

    const blocked = handle.hostBlockedWorkers.get(worker);
    if (!blocked || blocked.vmPtr !== vmPtr) {
      return;
    }
    finishBlockedRpc();
    deps.appendWorkerDebugLog(handle, `blocked-rpc-done:${handle.kernel.thread_status(vmPtr)}:0x${vmPtr.toString(16)}`);
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-done:${handle.pid}:${handle.kernel.thread_status(vmPtr)}`);

    if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
      return;
    }

    emitBlockedRpcRetryResult(handle.kernel.thread_status(vmPtr));
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
    pendingOutcomeEffects.delete(worker);
    const collectedRetryResult: { status?: number } = {};
    await executeHostBlockedSyscallResume(
      handle,
      worker,
      vmPtr,
      blockedStatus,
      {
        applyPeerHostEffects: false,
        emitEffect(effect) {
          pushOutcomeEffect(worker, effect);
        },
        emitRetryResult(status) {
          collectedRetryResult.status = status;
        },
      },
    );
    const effects = takeOutcomeEffects(worker);
    if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
      return { kind: "process-gone", effects };
    }
    if (!handle.hostBlockedWorkers.has(worker)) {
      return { kind: "queued-in-worker", effects };
    }
    if (collectedRetryResult.status !== undefined) {
      return completedKernelRpcOutcome(collectedRetryResult.status, effects);
    }
    const status = handle.kernel.thread_status?.(vmPtr);
    if (status === undefined) {
      return { kind: "stale", effects };
    }
    return { kind: "blocked", status, effects };
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
        applyPeerHostEffects: true,
        emitEffect(effect) {
          switch (effect.kind) {
            case "enqueue-worker":
              deps.enqueueWorker(handle, worker);
              return;
            default:
              throw new Error(`blocked-rpc test helper cannot apply effect: ${effect.kind}`);
          }
        },
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
