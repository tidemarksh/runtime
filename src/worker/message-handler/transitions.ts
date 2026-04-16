import { readKernelRuntimeState } from "../../abi.js";
import { publishKernelRuntimeState, wakeRuntimeBridge } from "../../bridge/runtime-state.js";
import {
  acquireWorker,
  maybeUnrefTimer,
  releaseWorker,
} from "../pool.js";
import { kernelShareOtherMembers } from "../kernel-share.js";
import { processHasLiveThreads } from "../live-threads.js";
import { blockedStatusUsesForkSharedStateSync } from "../sync.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "./types.js";

interface StatusTransitionArgs {
  handle: ProcessHandle;
  worker: Worker;
  thread: ThreadWorkerState;
  msg: ThreadWorkerStatusMessage;
  status: number;
  wroteToPipe: boolean;
  drainedPipe: boolean;
  completedSyncEffects: number;
}

export function createWorkerStatusTransitionHandler(deps: WorkerMessageHandlerDeps) {
  return function handleStatusTransition({
    handle,
    worker,
    thread,
    msg,
    status,
    wroteToPipe,
    drainedPipe,
    completedSyncEffects,
  }: StatusTransitionArgs): void {
    if (status === handle.kernel.abi_status_ok()) {
      const shouldSyncPipeState = wroteToPipe
        || drainedPipe
        || (completedSyncEffects & deps.syncEffectPipeState) !== 0;
      const shouldSyncLifecycle = (completedSyncEffects & deps.syncEffectProcessLifecycle) !== 0;
      const shouldWakeSignalWaiters = (completedSyncEffects & deps.syncEffectSignalWaiters) !== 0;
      if (handle.kernelWorkerRequest && handle.pid > 0 && (shouldSyncPipeState || shouldSyncLifecycle)) {
        deps.appendWorkerDebugLog(handle, `ok-sync-start:0x${thread.vmPtr.toString(16)}`);
        deps.pumpProcess(handle);
        const syncPipeState = shouldSyncPipeState
          ? deps.requestPipeStateSync(handle, true)
          : Promise.resolve();
        void syncPipeState
          .then(() => (shouldSyncLifecycle
            ? deps.syncProcessLifecycleViaKernelWorker(handle, thread.vmPtr)
            : Promise.resolve()))
          .then(() => {
            deps.appendWorkerDebugLog(handle, `ok-sync-done:0x${thread.vmPtr.toString(16)}`);
            if (shouldWakeSignalWaiters) {
              wakeRuntimeBridge(handle.runtimeBridge);
            }
            deps.kickBlockedWorkers(handle);
            deps.enqueueWorker(handle, worker);
          })
          .catch((error) => {
            deps.appendWorkerDebugLog(handle, `ok-sync-err:0x${thread.vmPtr.toString(16)}`);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (shouldWakeSignalWaiters) {
        wakeRuntimeBridge(handle.runtimeBridge);
      }
      deps.kickBlockedWorkers(handle);
      deps.enqueueWorker(handle, worker);
      return;
    }

    if (status === handle.kernel.abi_status_fault()) {
      if (deps.resumeVforkParentAfterChildExit(handle, worker, thread.vmPtr, msg.exitCode, msg.termSignal)) {
        return;
      }
      if (msg.debug) {
        deps.appendWorkerDebugLog(handle, `fault_debug:0x${thread.vmPtr.toString(16)}:${JSON.stringify(msg.debug)}`);
      }
      const remainingThreads = handle.kernel.thread_process_thread_count(thread.vmPtr);
      const hasLiveThreads = remainingThreads !== 0 && processHasLiveThreads(handle.kernel, thread.vmPtr);
      const shouldExitProcess = remainingThreads === 0 || !hasLiveThreads;
      deps.appendWorkerDebugLog(handle, `fault:0x${thread.vmPtr.toString(16)} remaining=${remainingThreads}`);
      if (shouldExitProcess) {
        void deps.completeProcessExit(handle, msg.exitCode, msg.termSignal).catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      } else {
        handle.workers.delete(worker);
        deps.threadWorkerStates.delete(worker);
        releaseWorker(worker);
        if (wroteToPipe || drainedPipe) {
          void deps.requestPipeStateSync(handle, true)
            .then(() => {
              deps.kickBlockedWorkers(handle);
              deps.pumpProcess(handle);
            })
            .catch((error) => {
              deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
            });
          return;
        }
        deps.kickBlockedWorkers(handle);
        deps.pumpProcess(handle);
      }
      return;
    }

    if (wroteToPipe || drainedPipe) {
      void deps.requestPipeStateSync(handle, true).catch((error) => {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    }

    if (status === handle.kernel.abi_status_sleep()) {
      const sec = Number(handle.kernel.thread_sleep_seconds(thread.vmPtr));
      const nsec = Number(handle.kernel.thread_sleep_nanoseconds(thread.vmPtr));
      const ms = sec * 1000 + Math.floor(nsec / 1_000_000);
      if (ms <= 0) {
        deps.appendWorkerDebugLog(handle, `yield:0x${thread.vmPtr.toString(16)}`);
        deps.enqueueWorker(handle, worker);
        return;
      }
      deps.appendWorkerDebugLog(handle, `sleep:0x${thread.vmPtr.toString(16)}@${ms}`);
      const sleepTimer = setTimeout(() => {
        if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
          return;
        }
        deps.enqueueWorker(handle, worker);
      }, ms);
      maybeUnrefTimer(sleepTimer);
      deps.pumpProcess(handle);
      return;
    }

    if (status === handle.kernel.abi_status_fs()) {
      deps.appendWorkerDebugLog(handle, `fs:0x${thread.vmPtr.toString(16)}`);
      void deps.resumeFsViaKernelWorker(handle, worker, thread.vmPtr);
      deps.kickBlockedWorkers(handle);
      deps.pumpProcess(handle);
      return;
    }

    if (status === handle.kernel.abi_status_connect()) {
      const timeout = handle.kernel.thread_socket_wait_timeout?.(thread.vmPtr);
      if (timeout !== undefined && Number(timeout) >= 0) {
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        deps.pumpProcess(handle);
        return;
      }
      void deps.resumeConnectViaKernelWorker(
        handle,
        worker,
        thread.vmPtr,
        BigInt(-handle.kernel.abi_econnrefused()),
      );
      deps.kickBlockedWorkers(handle);
      deps.pumpProcess(handle);
      return;
    }

    if (status === handle.kernel.abi_status_futex_wait()) {
      deps.kickBlockedWorkers(handle);
      deps.pumpProcess(handle);
      return;
    }

    if (status === handle.kernel.abi_status_clone_spawn()) {
      if (msg.debug) {
        deps.appendWorkerDebugLog(
          handle,
          `clone_spawn_debug:0x${thread.vmPtr.toString(16)}:${JSON.stringify(msg.debug)}`,
        );
      }
      const childVmPtr = handle.kernel.thread_spawn_from_clone(thread.vmPtr);
      if (childVmPtr === 0) {
        throw new Error(`thread_spawn_from_clone failed for pid=${handle.pid}`);
      }
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      deps.assertProcessStandardFdIntegrity(handle, "clone-spawn");
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      const childWorker = acquireWorker();
      deps.attachThreadWorker(handle, childWorker, childVmPtr);
      const childThread = deps.threadWorkerStates.get(childWorker);
      if (!childThread) {
        throw new Error(`child thread state missing for pid=${handle.pid} vm=0x${childVmPtr.toString(16)}`);
      }
      childThread.resumeWorkersOnReady.push(worker);
      deps.kickBlockedWorkers(handle);
      return;
    }

    if (status === handle.kernel.abi_status_execve_spawn()) {
      void deps.handleExecveSpawn(handle, worker, thread).catch((error) => {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
      return;
    }

    if (status === handle.kernel.abi_status_vfork_wait()) {
      if (handle.vforkSnapshot === null) {
        handle.vforkSnapshot = deps.captureVforkSnapshot(handle, thread.vmPtr);
        handle.vforkWorker = worker;
      }
      if (
        handle.kernelWorkerRequest
        && handle.pid > 0
        && handle.vforkSnapshot.childPid === null
      ) {
        deps.pumpProcess(handle);
        void deps.syncForkSharedStateViaKernelWorker(
          handle,
          kernelShareOtherMembers(handle),
          { includeMemfs: false, preserveSourceRefcounts: true },
        )
          .then(() => {
            if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
              return;
            }
            if (!handle.vforkSnapshot || handle.vforkSnapshot.vmPtr !== thread.vmPtr) {
              return;
            }
            if (handle.vforkSnapshot.childPid === null) {
              const provisionalChildPid = handle.kernel.thread_vfork_child_pid();
              if (provisionalChildPid > 0) {
                void deps.canonicalizeActiveVforkChildViaKernelWorker(
                  handle,
                  worker,
                  thread.vmPtr,
                  provisionalChildPid,
                );
                return;
              }
            }
            deps.enqueueWorker(handle, worker);
          })
          .catch((error) => {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (
        handle.kernelWorkerRequest
        && handle.pid > 0
        && handle.vforkSnapshot.childPid === null
      ) {
        const provisionalChildPid = handle.kernel.thread_vfork_child_pid();
        if (provisionalChildPid > 0) {
          void deps.canonicalizeActiveVforkChildViaKernelWorker(handle, worker, thread.vmPtr, provisionalChildPid);
          return;
        }
      }
      deps.enqueueWorker(handle, worker);
      return;
    }

    if (
      status === handle.kernel.abi_status_wait4()
      || status === handle.kernel.abi_status_pipe_wait()
      || status === handle.kernel.abi_status_epoll_wait()
      || status === handle.kernel.abi_status_socket_recv()
      || status === handle.kernel.abi_status_socket_send()
      || status === handle.kernel.abi_status_flock_wait()
      || status === handle.kernel.abi_status_connect()
    ) {
      if (blockedStatusUsesForkSharedStateSync(handle, status) && handle.kernelWorkerRequest && handle.pid > 0) {
        void deps.syncBlockedForkSharedStateViaKernelWorker(handle)
          .then(() => {
            if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
              return;
            }
            deps.blockWorker(handle, worker, status, thread.vmPtr);
            deps.pumpProcess(handle);
          })
          .catch((error) => {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      deps.blockWorker(handle, worker, status, thread.vmPtr);
      deps.pumpProcess(handle);
      return;
    }

    deps.enqueueWorker(handle, worker);
  };
}
