import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../abi.js";
import { publishKernelRuntimeState } from "../bridge/runtime-state.js";
import { kernelShareOtherMembers } from "./kernel-share.js";
import { createWorkerKernelRpcLifecycleHelpers } from "./kernel-rpc/lifecycle.js";
import { drainProcessOutput } from "./stdio.js";
import type {
  KernelWorkerRequestFn,
  ProcessHandle,
  ThreadWorkerState,
} from "./types.js";
import type { KernelWorkerPidBytesSnapshot } from "../messages.js";

const FS_DEBUG_PATH_BUF_LEN = 4096;

interface WorkerKernelRpcDeps {
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleBlockedRetryResult(handle: ProcessHandle, worker: Worker, vmPtr: number, status: number): void;
  kickBlockedWorkers(handle: ProcessHandle): void;
  markRuntimeFsDirty(handle: ProcessHandle): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  syncBlockedForkSharedStateViaKernelWorker(handle: ProcessHandle): Promise<void>;
  syncForkSharedStateLocally(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options?: { includeMemfs?: boolean; trackHeldOfds?: boolean },
  ): void;
  syncForkSharedStateViaKernelWorker(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options?: { includeMemfs?: boolean; trackHeldOfds?: boolean; preserveSourceRefcounts?: boolean },
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
  const decoder = new TextDecoder();
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
    syncProcessLifecycleViaKernelWorker,
    unregisterProcessWithKernelWorker,
  } = kernelRpcLifecycleHelpers;

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
    return decoder.decode(new Uint8Array(handle.kernel.memory.buffer, bufPtr, length));
  }

  async function resumeFsViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    fsEffects: { memfsMutation: number; memfsFileData: number; sharedOfdState: number },
  ): Promise<void> {
    const pendingOpcode = handle.kernel.thread_pending_fs_opcode(vmPtr);
    const pendingEffects = handle.kernel.thread_pending_fs_effects(vmPtr);
    const pendingTargetIno = handle.kernel.thread_pending_fs_target_ino(vmPtr);
    const pendingPath = readPendingFsPath(handle, vmPtr);
    const pendingSecondaryPath = readPendingFsPath(handle, vmPtr, true);
    const pathSuffix = pendingPath ? `:path=${JSON.stringify(pendingPath)}` : "";
    const secondaryPathSuffix = pendingSecondaryPath
      ? `:path2=${JSON.stringify(pendingSecondaryPath)}`
      : "";
    const fsDebugSuffix =
      `:op=${pendingOpcode}:fx=${pendingEffects}:ino=${pendingTargetIno}${pathSuffix}${secondaryPathSuffix}`;
    const memfsMutated =
      (pendingEffects & (fsEffects.memfsMutation | fsEffects.memfsFileData)) !== 0;
    const ofdStateChanged = (pendingEffects & fsEffects.sharedOfdState) !== 0;
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
      deps.assertProcessStandardFdIntegrity(handle, "resume-fs-before");
      handle.kernel.thread_process_fs(vmPtr);
      handle.kernel.thread_resume_fs(vmPtr);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      const fsResult = handle.kernel.thread_a0?.(vmPtr) ?? 0n;
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }
      deps.assertProcessStandardFdIntegrity(handle, "resume-fs");
      if (memfsMutated || ofdStateChanged) {
        await deps.syncForkSharedStateViaKernelWorker(
          handle,
          kernelShareOtherMembers(handle),
          {
            includeMemfs: memfsMutated,
            trackHeldOfds: ofdStateChanged,
          },
        );
      }
      if (memfsMutated) {
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
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      if (
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
      await deps.syncBlockedForkSharedStateViaKernelWorker(handle);

      if (blockedStatus === handle.kernel.abi_status_pipe_wait()) {
        await deps.syncPipeStateViaKernelWorker(handle);
      } else if (
        blockedStatus === handle.kernel.abi_status_connect()
        || blockedStatus === handle.kernel.abi_status_socket_recv()
        || blockedStatus === handle.kernel.abi_status_socket_send()
      ) {
        await deps.syncSocketStateViaKernelWorker(handle);
      }

      const blocked = handle.blockedWorkers.get(worker);
      if (!blocked || blocked.vmPtr !== vmPtr) {
        return;
      }
      blocked.rpcPending = false;

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
    syncProcessLifecycleViaKernelWorker,
    unregisterProcessWithKernelWorker,
  };
}
