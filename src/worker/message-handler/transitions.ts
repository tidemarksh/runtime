import { readKernelRuntimeState } from "../../abi.js";
import { publishKernelRuntimeState, wakeRuntimeBridge } from "../../bridge/runtime-state.js";
import {
  acquireWorker,
  releaseWorker,
} from "../pool.js";
import { kernelShareOtherMembers } from "../kernel-share.js";
import { processHasLiveThreads } from "../live-threads.js";
import { assertHandleWorkerProcessIdentity } from "../process-identity.js";
import { blockedStatusUsesForkSharedStateSync } from "../sync.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "./types.js";
import { createWorkerOwnershipGuard } from "./worker-ownership.js";

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
    const vforkWaitStatus = handle.kernel.abi_status_vfork_wait?.();
    const execveSpawnStatus = handle.kernel.abi_status_execve_spawn?.();
    const assertHandleProcessIdentity = (context: string): void => {
      assertHandleWorkerProcessIdentity(handle, deps.threadWorkerStates, context, {
        status,
        threadVmPtr: thread.vmPtr,
        workerLogTail: handle.workerDebugLog.slice(-16),
        skipVmPtrs: [handle.vforkSnapshot?.vmPtr ?? 0].filter((vmPtr) => vmPtr !== 0),
      });
    };
    const workerStillOwnedByHandle = createWorkerOwnershipGuard(
      handle,
      worker,
      thread,
      deps.threadWorkerStates,
    );
    if (
      status !== vforkWaitStatus
      && status !== execveSpawnStatus
    ) {
      assertHandleProcessIdentity("handleStatusTransition:start");
    }

    if (status === handle.kernel.abi_status_ok()) {
      const shouldSyncPipeState = wroteToPipe
        || drainedPipe
        || (completedSyncEffects & deps.syncEffectPipeState) !== 0;
      const shouldSyncLifecycle = (completedSyncEffects & deps.syncEffectProcessLifecycle) !== 0;
      const shouldWakeSignalWaiters = (completedSyncEffects & deps.syncEffectSignalWaiters) !== 0;
      if (handle.kernelWorkerRequest && handle.pid > 0 && (shouldSyncPipeState || shouldSyncLifecycle)) {
        deps.appendWorkerDebugLog(handle, `ok-sync-start:0x${thread.vmPtr.toString(16)}`);
        const syncPipeState = shouldSyncPipeState
          ? deps.requestPipeStateSync(handle, true)
          : Promise.resolve();
        void syncPipeState
          .then(() => {
            assertHandleProcessIdentity("handleStatusTransition:after-pipe-sync");
          })
          .then(() => (shouldSyncLifecycle
            ? deps.syncProcessLifecycleViaKernelWorker(handle, thread.vmPtr)
            : Promise.resolve()))
          .then(() => {
            assertHandleProcessIdentity("handleStatusTransition:after-lifecycle-sync");
            if (!workerStillOwnedByHandle()) {
              return;
            }
            if (shouldSyncLifecycle) {
              deps.pruneDetachedProcessWorkers(handle, thread.vmPtr, worker);
              assertHandleProcessIdentity("handleStatusTransition:after-prune-detached");
            }
            deps.appendWorkerDebugLog(handle, `ok-sync-done:0x${thread.vmPtr.toString(16)}`);
            return handle.networkConnections.size > 0
              ? deps.pumpNetworkConnections(handle, thread.vmPtr)
              : Promise.resolve(false);
          })
          .then((networkChanged) => {
            if (!workerStillOwnedByHandle()) {
              return;
            }
            if (networkChanged) {
              void deps.requestSocketStateSync(handle, true);
            }
            if (shouldWakeSignalWaiters) {
              wakeRuntimeBridge(handle.runtimeBridge);
            }
            deps.kickBlockedWorkers(handle);
            if (shouldSyncPipeState) {
              for (const member of kernelShareOtherMembers(handle)) {
                deps.kickBlockedWorkers(member);
                deps.pumpProcess(member);
              }
            }
            assertHandleProcessIdentity("handleStatusTransition:before-enqueue");
            deps.enqueueWorker(handle, worker);
          })
          .catch((error) => {
            if (!workerStillOwnedByHandle()) {
              return;
            }
            deps.appendWorkerDebugLog(handle, `ok-sync-err:0x${thread.vmPtr.toString(16)}`);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (shouldSyncLifecycle) {
        deps.pruneDetachedProcessWorkers(handle, thread.vmPtr, worker);
      }
      if (shouldWakeSignalWaiters) {
        wakeRuntimeBridge(handle.runtimeBridge);
      }
      if (handle.networkConnections.size > 0) {
        void deps.pumpNetworkConnections(handle, thread.vmPtr)
          .then((networkChanged) => {
            if (networkChanged) {
              void deps.requestSocketStateSync(handle, true);
            }
            deps.kickBlockedWorkers(handle);
            deps.enqueueWorker(handle, worker);
          })
          .catch((error) => {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
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
        if (handle.runningWorker === worker) {
          handle.runningWorker = null;
        }
        handle.workers.delete(worker);
        deps.threadWorkerStates.delete(worker);
        releaseWorker(worker);
        if (wroteToPipe || drainedPipe || (completedSyncEffects & deps.syncEffectPipeState) !== 0) {
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

    if (wroteToPipe || drainedPipe || (completedSyncEffects & deps.syncEffectPipeState) !== 0) {
      void deps.requestPipeStateSync(handle, true).catch((error) => {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    }
    if (handle.networkConnections.size > 0) {
      void deps.pumpNetworkConnections(handle, thread.vmPtr)
        .then((changed) => {
          if (changed) {
            void deps.requestSocketStateSync(handle, true);
          }
        })
        .catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
    }

    if (status === handle.kernel.abi_status_sleep()) {
      const sec = handle.kernel.thread_sleep_seconds(thread.vmPtr);
      const nsec = handle.kernel.thread_sleep_nanoseconds(thread.vmPtr);
      if (sec === 0n && nsec === 0n) {
        deps.appendWorkerDebugLog(handle, `yield:0x${thread.vmPtr.toString(16)}`);
        const shouldSyncPipeState = (completedSyncEffects & deps.syncEffectPipeState) !== 0;
        if (handle.kernelWorkerRequest && handle.pid > 0 && shouldSyncPipeState) {
          void deps.requestPipeStateSync(handle, false)
            .then(() => {
              if (!workerStillOwnedByHandle()) {
                return;
              }
              deps.kickBlockedWorkers(handle);
              deps.enqueueWorker(handle, worker);
            })
            .catch((error) => {
              if (!workerStillOwnedByHandle()) {
                return;
              }
              deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
            });
          return;
        }
        deps.kickBlockedWorkers(handle);
        deps.enqueueWorker(handle, worker);
        return;
      }
      deps.appendWorkerDebugLog(handle, `sleep:0x${thread.vmPtr.toString(16)}@${sec.toString()}s+${nsec.toString()}ns`);
      deps.blockWorker(handle, worker, status, thread.vmPtr);
      deps.kickBlockedWorkers(handle);
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
      if (handle.networkBridge) {
        void deps.resolveNetworkConnect(handle, thread.vmPtr)
          .then((result) => deps.resumeConnectViaKernelWorker(handle, worker, thread.vmPtr, result))
          .then(() => deps.commitNetworkConnect(handle, thread.vmPtr))
          .then(() => {
            deps.kickBlockedWorkers(handle);
            deps.enqueueWorker(handle, worker);
          })
          .catch((error) => {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        deps.pumpProcess(handle);
        return;
      }
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
      )
        .then(() => {
          deps.kickBlockedWorkers(handle);
          deps.enqueueWorker(handle, worker);
        })
        .catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
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
      const finishCloneSpawn = (): void => {
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        deps.assertProcessStandardFdIntegrity(handle, "clone-spawn");
        publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
        const childWorker = acquireWorker();
        deps.attachThreadWorker(handle, { worker: childWorker, vmPtr: childVmPtr });
        const childThread = deps.threadWorkerStates.get(childWorker);
        if (!childThread) {
          throw new Error(`child thread state missing for pid=${handle.pid} vm=0x${childVmPtr.toString(16)}`);
        }
        childThread.resumeWorkersOnReady.push(worker);
        deps.kickBlockedWorkers(handle);
      };

      if (handle.kernelWorkerRequest && handle.pid > 0) {
        void deps.syncProcessLifecycleViaKernelWorker(handle, thread.vmPtr)
          .then(() => {
            if (!workerStillOwnedByHandle()) {
              return;
            }
            finishCloneSpawn();
          })
          .catch((error) => {
            if (!workerStillOwnedByHandle()) {
              return;
            }
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }

      finishCloneSpawn();
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
        handle.vforkSnapshot = deps.captureVforkSnapshot(handle, thread);
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
          {
            includeMemfs: false,
            preserveSourceRefcounts: true,
            sourceVmPtr: thread.vmPtr,
          },
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
      status === handle.kernel.abi_status_futex_wait()
      || status === handle.kernel.abi_status_wait4()
      || status === handle.kernel.abi_status_pipe_wait()
      || status === handle.kernel.abi_status_epoll_wait()
      || status === handle.kernel.abi_status_socket_recv()
      || status === handle.kernel.abi_status_socket_send()
      || status === handle.kernel.abi_status_flock_wait()
      || status === handle.kernel.abi_status_connect()
    ) {
      const finitePollTimeout = status === handle.kernel.abi_status_pipe_wait()
        && handle.kernel.thread_poll_wait_timeout !== undefined
        && Number(handle.kernel.thread_poll_wait_timeout(thread.vmPtr)) >= 0;
      const pipeWaitHasSocket = status === handle.kernel.abi_status_pipe_wait()
        && handle.networkConnections.size > 0
        && handle.kernel.thread_poll_wait_has_socket?.(thread.vmPtr) === 1;
      if (
        pipeWaitHasSocket
        && handle.kernelWorkerRequest
        && handle.pid > 0
      ) {
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        const blocked = handle.blockedWorkers.get(worker);
        if (blocked) {
          blocked.rpcPending = true;
        }
        void deps.requestSocketStateSync(handle, true)
          .then(() => {
            const latestBlocked = handle.blockedWorkers.get(worker);
            if (latestBlocked && latestBlocked.vmPtr === thread.vmPtr) {
              latestBlocked.rpcPending = false;
            }
            deps.kickBlockedWorkers(handle);
            deps.pumpProcess(handle);
          })
          .catch((error) => {
            const latestBlocked = handle.blockedWorkers.get(worker);
            if (latestBlocked && latestBlocked.vmPtr === thread.vmPtr) {
              latestBlocked.rpcPending = false;
            }
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (
        status === handle.kernel.abi_status_wait4()
        && handle.kernelWorkerRequest
        && handle.pid > 0
      ) {
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        const blocked = handle.blockedWorkers.get(worker);
        if (blocked) {
          blocked.rpcPending = true;
        }
        void deps.requestPipeStateSync(handle, true)
          .then(() => {
            const latestBlocked = handle.blockedWorkers.get(worker);
            if (latestBlocked && latestBlocked.vmPtr === thread.vmPtr) {
              latestBlocked.rpcPending = false;
            }
            deps.kickBlockedWorkers(handle);
            deps.pumpProcess(handle);
          })
          .catch((error) => {
            const latestBlocked = handle.blockedWorkers.get(worker);
            if (latestBlocked && latestBlocked.vmPtr === thread.vmPtr) {
              latestBlocked.rpcPending = false;
            }
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (
        blockedStatusUsesForkSharedStateSync(handle, status)
        && !pipeWaitHasSocket
        && !finitePollTimeout
        && handle.kernelWorkerRequest
        && handle.pid > 0
      ) {
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        const blocked = handle.blockedWorkers.get(worker);
        if (blocked) {
          blocked.rpcPending = true;
        }
        const syncPipeState = status === handle.kernel.abi_status_pipe_wait()
          ? deps.requestPipeStateSync(handle, true)
          : Promise.resolve();
        void syncPipeState
          .then(() => {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-start:${handle.pid}:status=${status}`);
            return deps.syncBlockedForkSharedStateViaKernelWorker(handle, thread.vmPtr);
          })
          .then(() => {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-done:${handle.pid}:status=${status}`);
            const latestBlocked = handle.blockedWorkers.get(worker);
            if (latestBlocked && latestBlocked.vmPtr === thread.vmPtr) {
              latestBlocked.rpcPending = false;
            }
            if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
              handle.blockedWorkers.delete(worker);
              return;
            }
            deps.kickBlockedWorkers(handle);
            deps.pumpProcess(handle);
          })
          .catch((error) => {
            const latestBlocked = handle.blockedWorkers.get(worker);
            if (latestBlocked && latestBlocked.vmPtr === thread.vmPtr) {
              latestBlocked.rpcPending = false;
            }
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
