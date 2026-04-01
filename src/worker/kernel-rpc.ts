import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../abi.js";
import { publishKernelRuntimeState } from "../bridge/runtime-state.js";
import { kernelShareOtherMembers } from "./kernel-share.js";
import { createWorkerKernelRpcLifecycleHelpers } from "./kernel-rpc/lifecycle.js";
import type {
  KernelWorkerRequestFn,
  ProcessHandle,
  ThreadWorkerState,
} from "./types.js";
import type { KernelWorkerPidBytesSnapshot } from "../messages.js";
import { adoptKernelState } from "./sync.js";

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
  const kernelRpcLifecycleHelpers = createWorkerKernelRpcLifecycleHelpers({
    processHandles: deps.processHandles,
    threadWorkerStates: deps.threadWorkerStates,
    enqueueWorker: deps.enqueueWorker,
    failProcess: deps.failProcess,
    handleBlockedRetryResult: deps.handleBlockedRetryResult,
    kickBlockedWorkers: deps.kickBlockedWorkers,
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

  async function resumeFsViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    fsEffects: { memfsMutation: number; memfsFileData: number; sharedOfdState: number },
  ): Promise<void> {
    const pendingOpcode = handle.kernel.thread_pending_fs_opcode(vmPtr);
    const pendingEffects = handle.kernel.thread_pending_fs_effects(vmPtr);
    const pendingTargetIno = handle.kernel.thread_pending_fs_target_ino(vmPtr);
    const fsDebugSuffix = `:op=${pendingOpcode}:fx=${pendingEffects}:ino=${pendingTargetIno}`;
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      deps.appendWorkerDebugLog(handle, `fs-start:0x${vmPtr.toString(16)}:local${fsDebugSuffix}`);
      const effects = handle.kernel.thread_pending_fs_effects(vmPtr);
      const memfsMutated = (effects & (fsEffects.memfsMutation | fsEffects.memfsFileData)) !== 0;
      const ofdStateChanged = (effects & fsEffects.sharedOfdState) !== 0;
      handle.kernel.thread_process_fs(vmPtr);
      handle.kernel.thread_resume_fs(vmPtr);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
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
      deps.appendWorkerDebugLog(handle, `fs-done:0x${vmPtr.toString(16)}:local`);
      deps.enqueueWorker(handle, worker);
      return;
    }

    try {
      deps.appendWorkerDebugLog(handle, `fs-start:0x${vmPtr.toString(16)}${fsDebugSuffix}`);
      deps.assertProcessStandardFdIntegrity(handle, "resume-fs-before");
      const response = await handle.kernelWorkerRequest({
        type: "process-fs",
        pid: handle.pid,
        vmPtr,
        kernelState: handle.kernelState,
      });
      if (response.type !== "fs-processed") {
        throw new Error(`unexpected kernel-worker response for process-fs: ${response.type}`);
      }
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }
      handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      deps.assertProcessStandardFdIntegrity(handle, "resume-fs");
      if (response.ofdStateChanged) {
        await deps.syncForkSharedStateViaKernelWorker(
          handle,
          kernelShareOtherMembers(handle),
          { includeMemfs: false },
        );
      }
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      deps.kickBlockedWorkers(handle);
      deps.appendWorkerDebugLog(handle, `fs-done:0x${vmPtr.toString(16)}`);
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
  ): Promise<void> {
    try {
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      handle.kernel.thread_resume_blocked_syscall(vmPtr);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      await deps.syncBlockedForkSharedStateViaKernelWorker(handle);

      if (blockedStatus === handle.kernel.abi_status_pipe_wait()) {
        await deps.syncPipeStateViaKernelWorker(handle);
      } else if (
        blockedStatus === handle.kernel.abi_status_socket_recv()
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
