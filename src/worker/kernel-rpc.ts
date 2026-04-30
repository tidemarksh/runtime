import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../abi.js";
import { publishKernelRuntimeState } from "../bridge/runtime-state.js";
import { activeKernelShareOfdIds, collectProcessHeldOfds, hasPlausibleOfdState } from "./fork-shared.js";
import {
  kernelShareOtherMembers,
  reconcileKernelSharePipeControl,
} from "./kernel-share.js";
import { createWorkerKernelRpcLifecycleHelpers } from "./kernel-rpc/lifecycle.js";
import { drainProcessOutput } from "./stdio.js";
import type {
  KernelWorkerRequestFn,
  ProcessHandle,
  ThreadWorkerState,
} from "./types.js";
import type { KernelWorkerPidBytesSnapshot } from "../messages.js";

const FS_DEBUG_PATH_BUF_LEN = 4096;
const GUEST_TICKS_PER_SEC = 100_000_000n;

interface PendingFsContext {
  opcode: number;
  effects: number;
  targetIno: number;
  path: string | null;
  secondaryPath: string | null;
}

interface WorkerKernelRpcDeps {
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleBlockedRetryResult(handle: ProcessHandle, worker: Worker, vmPtr: number, status: number): void;
  kickBlockedWorkers(handle: ProcessHandle): void;
  markRuntimeFsDirty(handle: ProcessHandle): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  syncBlockedForkSharedStateViaKernelWorker(handle: ProcessHandle, sourceVmPtr?: number): Promise<void>;
  syncForkSharedStateLocally(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options?: { includeMemfs?: boolean; trackHeldOfds?: boolean },
  ): void;
  syncForkSharedStateViaKernelWorker(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options?: {
      includeMemfs?: boolean;
      trackHeldOfds?: boolean;
      deltaOfdIds?: boolean;
      preserveSourceRefcounts?: boolean;
      sourceVmPtr?: number;
    },
  ): Promise<void>;
  rehydrateForkSharedStateViaKernelWorker(
    targets: ProcessHandle[],
    ofdIds: number[],
    options?: { procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean },
  ): Promise<void>;
  syncPipeStateViaKernelWorker(handle: ProcessHandle): Promise<bigint>;
  syncSocketStateViaKernelWorker(handle: ProcessHandle): Promise<{ socketUsed: number; netBufUsed: bigint }>;
  collectTrackableForkSharedOfds(handle: ProcessHandle): Set<number>;
}

function hasActiveChildProcess(deps: WorkerKernelRpcDeps, handle: ProcessHandle): boolean {
  for (const candidate of deps.processHandles.values()) {
    if (
      candidate.parentPid === handle.pid
      && candidate.workers.size > 0
      && !candidate.exitInProgress
    ) {
      return true;
    }
  }
  return false;
}

export function clearVforkSnapshot(handle: ProcessHandle): void {
  handle.vforkSnapshot = null;
  handle.vforkWorker = null;
}

export async function resolveProcessCwdInoViaKernelWorker(
  kernelWorkerRequest: KernelWorkerRequestFn,
  cwd: string | undefined,
): Promise<number> {
  if (!cwd) {
    return 1;
  }
  const response = await kernelWorkerRequest({
    type: "stat",
    path: cwd,
  });
  if (response.type !== "stat-result") {
    throw new Error(`unexpected kernel-worker response for stat: ${response.type}`);
  }
  if ((response.stat.mode & 0o170000) !== 0o040000) {
    throw new Error(`cwd is not a directory: ${cwd}`);
  }
  return response.stat.ino;
}

export function createWorkerKernelRpcHelpers(deps: WorkerKernelRpcDeps) {
  function monotonicTicksCurrent(handle: ProcessHandle): bigint {
    const ticks = handle.kernel.monotonic_ticks_current?.();
    return ticks === undefined ? 0n : BigInt(ticks);
  }

  function advanceGuestTimeByMs(handle: ProcessHandle, timeoutMs: number): void {
    if (timeoutMs <= 0 || !Number.isFinite(timeoutMs)) {
      return;
    }
    const deltaTicks = BigInt(Math.ceil(timeoutMs)) * GUEST_TICKS_PER_SEC / 1000n;
    handle.kernel.sync_monotonic_ticks_floor?.(monotonicTicksCurrent(handle) + deltaTicks);
  }

  const decoder = new TextDecoder();

  async function syncSocketStatePreservingNetworkBridge(handle: ProcessHandle): Promise<{
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
    return deps.syncSocketStateViaKernelWorker(handle);
  }

  const kernelRpcLifecycleHelpers = createWorkerKernelRpcLifecycleHelpers({
    appendWorkerDebugLog: deps.appendWorkerDebugLog,
    processHandles: deps.processHandles,
    threadWorkerStates: deps.threadWorkerStates,
    drainProcessOutput,
    enqueueWorker: deps.enqueueWorker,
    failProcess: deps.failProcess,
    handleBlockedRetryResult: deps.handleBlockedRetryResult,
    kickBlockedWorkers: deps.kickBlockedWorkers,
    requestPipeStateSync: deps.requestPipeStateSync,
    syncSocketStateViaKernelWorker: syncSocketStatePreservingNetworkBridge,
    clearVforkSnapshot,
  });

  const {
    canonicalizeActiveVforkChildViaKernelWorker,
    canonicalizeChildLifecycleViaKernelWorker,
    initializeProcessLifecycleViaKernelWorker,
    recordChildExitViaKernelWorker,
    registerProcessWithKernelWorker,
    releaseHostStdioViaKernelWorker,
    resumeConnectViaKernelWorker,
    resumeVforkExecveParentViaKernelWorker,
    resumeVforkParentViaKernelWorker,
    resumeWait4ViaKernelWorker,
    syncProcessMemfsViaKernelWorker,
    syncProcessLifecycleViaKernelWorker,
    unregisterProcessWithKernelWorker,
  } = kernelRpcLifecycleHelpers;

  function decodeKernelBytes(memory: WebAssembly.Memory, ptr: number, length: number): string {
    const bytes = new Uint8Array(length);
    bytes.set(new Uint8Array(memory.buffer, ptr, length));
    return decoder.decode(bytes);
  }

  function readPendingFsPath(handle: ProcessHandle, vmPtr: number, secondary = false): string | null {
    const absolutePath = secondary
      ? handle.kernel.thread_pending_fs_secondary_absolute_path
      : handle.kernel.thread_pending_fs_absolute_path;
    if (!absolutePath) {
      return null;
    }
    const bufPtr = handle.kernel.kernel_heap_ptr() + 0x8000 + (secondary ? FS_DEBUG_PATH_BUF_LEN : 0);
    const length = Number(absolutePath(vmPtr, bufPtr, FS_DEBUG_PATH_BUF_LEN) ?? 0n);
    if (length <= 0 || bufPtr + length > handle.kernel.memory.buffer.byteLength) {
      return null;
    }
    return decodeKernelBytes(handle.kernel.memory, bufPtr, length);
  }

  function readPendingFsContext(handle: ProcessHandle, vmPtr: number): PendingFsContext {
    return {
      opcode: handle.kernel.thread_pending_fs_opcode(vmPtr),
      effects: handle.kernel.thread_pending_fs_effects(vmPtr),
      targetIno: handle.kernel.thread_pending_fs_target_ino(vmPtr),
      path: readPendingFsPath(handle, vmPtr),
      secondaryPath: readPendingFsPath(handle, vmPtr, true),
    };
  }

  function formatPendingFsDebugSuffix(context: PendingFsContext): string {
    const pathSuffix = context.path ? `:path=${JSON.stringify(context.path)}` : "";
    const secondaryPathSuffix = context.secondaryPath
      ? `:path2=${JSON.stringify(context.secondaryPath)}`
      : "";
    return `:op=${context.opcode}:fx=${context.effects}:ino=${context.targetIno}${pathSuffix}${secondaryPathSuffix}`;
  }

  async function resumeFsViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    fsEffects: { memfsMutation: number; memfsFileData: number; sharedOfdState: number },
  ): Promise<void> {
    const pendingFs = readPendingFsContext(handle, vmPtr);
    const fsDebugSuffix = formatPendingFsDebugSuffix(pendingFs);
    const memfsMutated =
      (pendingFs.effects & (fsEffects.memfsMutation | fsEffects.memfsFileData)) !== 0;
    const ofdStateChanged = (pendingFs.effects & fsEffects.sharedOfdState) !== 0;
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      deps.appendWorkerDebugLog(handle, `fs-start:0x${vmPtr.toString(16)}:local${fsDebugSuffix}`);
      handle.kernel.thread_process_fs(vmPtr);
      handle.kernel.thread_resume_fs(vmPtr);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      const fsResult = handle.kernel.thread_a0?.(vmPtr) ?? 0n;
      if (memfsMutated) {
        deps.syncForkSharedStateLocally(handle, kernelShareOtherMembers(handle));
        deps.markRuntimeFsDirty(handle);
      } else if (ofdStateChanged) {
        deps.syncForkSharedStateLocally(
          handle,
          kernelShareOtherMembers(handle),
          { includeMemfs: false },
        );
      }
      deps.kickBlockedWorkers(handle);
      deps.appendWorkerDebugLog(handle, `fs-done:0x${vmPtr.toString(16)}:local:rv=${fsResult.toString()}`);
      deps.enqueueWorker(handle, worker);
      return;
    }

    try {
      deps.appendWorkerDebugLog(handle, `fs-start:0x${vmPtr.toString(16)}${fsDebugSuffix}`);
      const heldOfdIds = collectProcessHeldOfds(handle);
      const missingSharedOfdIds = activeKernelShareOfdIds(handle)
        .filter((ofdId) => !heldOfdIds.has(ofdId) || !hasPlausibleOfdState(handle, ofdId));
      if (missingSharedOfdIds.length > 0) {
        await deps.rehydrateForkSharedStateViaKernelWorker([handle], missingSharedOfdIds, {
          includeMemfs: false,
        });
      }
      deps.assertProcessStandardFdIntegrity(handle, "resume-fs-before");
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      handle.kernel.thread_process_fs(vmPtr);
      handle.kernel.thread_resume_fs(vmPtr);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      const currentPid = handle.kernel.process_current_pid?.(vmPtr) ?? handle.pid;
      const transientProcessOwnerMismatch = currentPid > 0 && currentPid !== handle.pid;
      const fsResult = handle.kernel.thread_a0?.(vmPtr) ?? 0n;
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }
      deps.assertProcessStandardFdIntegrity(handle, "resume-fs");
      const syncTargets = kernelShareOtherMembers(handle)
        .filter((member) => member.runningWorker === null);
      if (ofdStateChanged && !transientProcessOwnerMismatch) {
        await deps.syncForkSharedStateViaKernelWorker(
          handle,
          syncTargets,
          {
            includeMemfs: false,
            trackHeldOfds: false,
            deltaOfdIds: true,
            sourceVmPtr: vmPtr,
          },
        );
      }
      if (memfsMutated) {
        if (!transientProcessOwnerMismatch && syncTargets.length > 0) {
          deps.syncForkSharedStateLocally(handle, syncTargets);
          await deps.syncForkSharedStateViaKernelWorker(
            handle,
            syncTargets,
            {
              includeMemfs: true,
              trackHeldOfds: false,
              sourceVmPtr: vmPtr,
            },
          );
        }
        deps.markRuntimeFsDirty(handle);
      }
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      deps.kickBlockedWorkers(handle);
      deps.appendWorkerDebugLog(handle, `fs-done:0x${vmPtr.toString(16)}:rv=${fsResult.toString()}`);
      deps.enqueueWorker(handle, worker);
    } catch (error) {
      deps.appendWorkerDebugLog(handle, `fs-err:0x${vmPtr.toString(16)}`);
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    }
  }

  async function resumeBlockedSyscallLocally(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    blockedStatus: number,
    timeoutExpired = false,
  ): Promise<void> {
    try {
      deps.appendWorkerDebugLog(handle, `blocked-rpc-start:${blockedStatus}:0x${vmPtr.toString(16)}`);
      deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:start:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-start:${handle.pid}:${blockedStatus}`);
      let forkSharedSyncedBeforeResume = false;
      const resumePollTimeoutFn = timeoutExpired
        && blockedStatus === handle.kernel.abi_status_pipe_wait()
        ? handle.kernel.thread_resume_poll_wait_timeout
        : undefined;
      const resumePollTimeout = resumePollTimeoutFn !== undefined;
      if (blockedStatus === handle.kernel.abi_status_pipe_wait() && !resumePollTimeout) {
        if (handle.kernelShare.forkSharedSyncPromise !== null) {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:await-fork-sync:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          await handle.kernelShare.forkSharedSyncPromise;
        }
        for (const member of [...kernelShareOtherMembers(handle), handle]) {
          if (member !== handle && member.runningWorker !== null) {
            deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe-skip-running:${member.pid}:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
            continue;
          }
          if (member.kernelWorkerRequest && member.pid > 0) {
            deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe:${member.pid}:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-start:${handle.pid}->${member.pid}`);
            await deps.requestPipeStateSync(member, false);
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-done:${handle.pid}->${member.pid}`);
          }
        }
        if (handle.kernelWorkerRequest && handle.pid > 0) {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-blocked:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-blocked-start:${handle.pid}`);
          await deps.syncBlockedForkSharedStateViaKernelWorker(handle, vmPtr);
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-blocked-done:${handle.pid}`);
          forkSharedSyncedBeforeResume = true;
        }
        deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:reconcile:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
        reconcileKernelSharePipeControl(handle);
        if (handle.kernel.thread_poll_wait_has_socket?.(vmPtr) === 1) {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-socket:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          await syncSocketStatePreservingNetworkBridge(handle);
        }
      }
      if (
        blockedStatus === handle.kernel.abi_status_pipe_wait()
        && !resumePollTimeout
        && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) === 1
        && (handle.kernel.thread_blocked_pipe_buffered_len?.(vmPtr) ?? 0) === 0
      ) {
        if (
          (handle.kernel.thread_blocked_pipe_write_open?.(vmPtr) ?? 0) > 0
          && handle.kernel.thread_resume_pipe_read_open_empty?.(vmPtr) === 1
        ) {
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          const blocked = handle.blockedWorkers.get(worker);
          if (blocked && blocked.vmPtr === vmPtr) {
            blocked.rpcPending = false;
          }
          deps.appendWorkerDebugLog(handle, `blocked-rpc-pipe-open-empty:0x${vmPtr.toString(16)}`);
          deps.handleBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr));
          return;
        }
        if (hasActiveChildProcess(deps, handle)) {
          const blocked = handle.blockedWorkers.get(worker);
          if (blocked && blocked.vmPtr === vmPtr) {
            blocked.rpcPending = false;
          }
          deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-pipe-eof:0x${vmPtr.toString(16)}`);
          deps.handleBlockedRetryResult(handle, worker, vmPtr, blockedStatus);
          return;
        }
      }
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      if (resumePollTimeout) {
        advanceGuestTimeByMs(handle, Number(handle.kernel.thread_poll_wait_timeout?.(vmPtr) ?? 0));
        resumePollTimeoutFn(vmPtr);
      } else if (
        timeoutExpired
        && (
          blockedStatus === handle.kernel.abi_status_connect()
          || blockedStatus === handle.kernel.abi_status_socket_recv()
          || blockedStatus === handle.kernel.abi_status_socket_send()
        )
        && handle.kernel.thread_resume_socket_wait_timeout
      ) {
        handle.kernel.thread_resume_socket_wait_timeout(vmPtr);
      } else {
        handle.kernel.thread_resume_blocked_syscall(vmPtr);
      }
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      const currentPid = handle.kernel.process_current_pid?.(vmPtr) ?? handle.pid;
      const transientProcessOwnerMismatch = currentPid > 0 && currentPid !== handle.pid;
      if (!transientProcessOwnerMismatch && !forkSharedSyncedBeforeResume && !resumePollTimeout) {
        deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-after-resume:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
        await deps.syncBlockedForkSharedStateViaKernelWorker(handle, vmPtr);
      }

      if (
        !transientProcessOwnerMismatch
        && blockedStatus === handle.kernel.abi_status_pipe_wait()
        && !resumePollTimeout
      ) {
        deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe-after:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-after-start:${handle.pid}`);
        await deps.requestPipeStateSync(handle, true);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-after-done:${handle.pid}`);
      } else if (
        !transientProcessOwnerMismatch
        && (
          blockedStatus === handle.kernel.abi_status_connect()
          || blockedStatus === handle.kernel.abi_status_socket_recv()
          || blockedStatus === handle.kernel.abi_status_socket_send()
        )
      ) {
        await syncSocketStatePreservingNetworkBridge(handle);
      }

      const blocked = handle.blockedWorkers.get(worker);
      if (!blocked || blocked.vmPtr !== vmPtr) {
        return;
      }
      blocked.rpcPending = false;
      deps.appendWorkerDebugLog(handle, `blocked-rpc-done:${handle.kernel.thread_status(vmPtr)}:0x${vmPtr.toString(16)}`);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-done:${handle.pid}:${handle.kernel.thread_status(vmPtr)}`);

      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }

      deps.handleBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr));
    } catch (error) {
      const blocked = handle.blockedWorkers.get(worker);
      if (blocked && blocked.vmPtr === vmPtr) {
        blocked.rpcPending = false;
      }
      if (handle.workers.has(worker) && deps.threadWorkerStates.has(worker)) {
        const fd = Number(handle.kernel.thread_a0(vmPtr));
        const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        const debug = [
          `vm=0x${vmPtr.toString(16)}`,
          `fd=${fd}`,
          `buf=${handle.kernel.thread_a1(vmPtr).toString()}`,
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
        deps.failProcess(handle, new Error(`${detail}\n${debug}`));
      }
    }
  }

  return {
    canonicalizeActiveVforkChildViaKernelWorker,
    canonicalizeChildLifecycleViaKernelWorker,
    initializeProcessLifecycleViaKernelWorker,
    recordChildExitViaKernelWorker,
    registerProcessWithKernelWorker,
    releaseHostStdioViaKernelWorker,
    resumeBlockedSyscallLocally,
    resumeConnectViaKernelWorker,
    resumeFsViaKernelWorker,
    resumeVforkExecveParentViaKernelWorker,
    resumeVforkParentViaKernelWorker,
    resumeWait4ViaKernelWorker,
    syncProcessMemfsViaKernelWorker,
    syncProcessLifecycleViaKernelWorker,
    unregisterProcessWithKernelWorker,
  };
}
