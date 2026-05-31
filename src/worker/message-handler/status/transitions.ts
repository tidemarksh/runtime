import { readKernelRuntimeState } from "../../../abi.js";
import { publishKernelRuntimeState, wakeRuntimeBridge } from "../../../bridge/runtime-state.js";
import {
  acquireWorker,
  releaseWorker,
} from "../../pool.js";
import { kernelShareOtherMembers } from "../../kernel-share.js";
import { processHasLiveThreads } from "../../live-threads.js";
import { assertHandleWorkerProcessIdentity } from "../../process-identity.js";
import { blockedStatusUsesForkSharedStateSync } from "../../sync.js";
import type {
  BlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../../types.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "../types.js";
import { resolveWorkerMessageHandlerSyncEffects } from "../sync-effects.js";
import type { KernelRpcResumeOutcome } from "../../kernel-rpc/outcomes.js";
import { createWorkerOwnershipGuard } from "../../ownership/process-state.js";
import {
  refreshPipeControlFromKernelShare,
  shouldRefreshPipeStateBeforeFsResume,
  snapshotProcessPipeSlotsForThreadStep,
} from "../../ownership/pipe-control.js";
import { restoreRuntimeNetworkSidecarForHandle } from "../../ownership/socket-control.js";

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

function beginStatusTransitionRpc(blocked: BlockedWorkerState): number {
  blocked.rpcPending = true;
  blocked.rpcGeneration += 1;
  return blocked.rpcGeneration;
}

function finishStatusTransitionRpc(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  rpcGeneration: number,
): BlockedWorkerState | null {
  const latestBlocked = handle.blockedWorkers.get(worker);
  if (!latestBlocked || latestBlocked.vmPtr !== vmPtr) {
    return null;
  }
  if (latestBlocked.rpcGeneration === rpcGeneration) {
    latestBlocked.rpcPending = false;
  }
  return latestBlocked;
}

export function createWorkerStatusTransitionHandler(deps: WorkerMessageHandlerDeps) {
  const syncEffects = resolveWorkerMessageHandlerSyncEffects(deps);
  function consumeKernelRpcResumeOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    outcome: KernelRpcResumeOutcome,
  ): void {
    switch (outcome.kind) {
      case "completed":
      case "queued-in-worker":
      case "process-gone":
      case "stale":
        return;
      case "blocked": {
        const blocked = handle.blockedWorkers.get(worker);
        if (blocked && blocked.vmPtr === vmPtr) {
          blocked.status = outcome.status;
        }
        return;
      }
      default: {
        const exhaustive: never = outcome;
        throw new Error(`unhandled kernel-rpc outcome: ${JSON.stringify(exhaustive)}`);
      }
    }
  }

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

    const enqueueWorkerAfterOk = (): void => {
      deps.enqueueWorker(handle, worker);
    };

    const syncSocketAfterNetworkPump = (networkChanged: boolean): Promise<void> => {
      if (!networkChanged) {
        return Promise.resolve();
      }
      return deps.requestSocketStateSync(handle, true);
    };

    if (status === handle.kernel.abi_status_ok()) {
      const shouldSyncPipeState = wroteToPipe
        || drainedPipe
        || (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0;
      const shouldSyncSocketState = (completedSyncEffects & syncEffects.syncEffectSocketState) !== 0;
      const shouldSyncLifecycle = (completedSyncEffects & syncEffects.syncEffectProcessLifecycle) !== 0;
      const shouldWakeSignalWaiters = (completedSyncEffects & syncEffects.syncEffectSignalWaiters) !== 0;
      if (
        handle.kernelWorkerRequest
        && handle.pid > 0
        && (shouldSyncPipeState || shouldSyncSocketState || shouldSyncLifecycle)
      ) {
        deps.appendWorkerDebugLog(handle, `ok-sync-start:0x${thread.vmPtr.toString(16)}`);
        thread.ownerAsyncStatus = status;
        const syncPipeState = shouldSyncPipeState
          ? deps.requestPipeStateSync(handle, true)
          : Promise.resolve();
        const syncSocketState = shouldSyncSocketState
          ? deps.requestSocketStateSync(handle, true)
          : Promise.resolve();
        void syncPipeState
          .then(() => {
            assertHandleProcessIdentity("handleStatusTransition:after-pipe-sync");
          })
          .then(() => syncSocketState)
          .then(() => {
            assertHandleProcessIdentity("handleStatusTransition:after-socket-sync");
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
          .then((networkChanged) => syncSocketAfterNetworkPump(networkChanged ?? false))
          .then(() => {
            thread.ownerAsyncStatus = null;
            if (!workerStillOwnedByHandle()) {
              deps.pumpProcess(handle);
              return;
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
            enqueueWorkerAfterOk();
          })
          .catch((error) => {
            thread.ownerAsyncStatus = null;
            if (!workerStillOwnedByHandle()) {
              deps.pumpProcess(handle);
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
          .then((networkChanged) => syncSocketAfterNetworkPump(networkChanged))
          .then(() => {
            if (!workerStillOwnedByHandle()) {
              return;
            }
            deps.kickBlockedWorkers(handle);
            enqueueWorkerAfterOk();
          })
          .catch((error) => {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      deps.kickBlockedWorkers(handle);
      enqueueWorkerAfterOk();
      return;
    }

    if (status === handle.kernel.abi_status_fault()) {
      if (deps.resumeVforkParentAfterChildExit(handle, worker, thread.vmPtr, msg.exitCode, msg.termSignal)) {
        return;
      }
      if (msg.debug) {
        deps.appendWorkerDebugLog(handle, `fault_debug:0x${thread.vmPtr.toString(16)}:${JSON.stringify(msg.debug)}`);
      }
      const currentPid = handle.kernel.process_current_pid(thread.vmPtr);
      const faultStateLooksStale = currentPid === 0 || currentPid > 0xffff;
      const remainingThreads = faultStateLooksStale ? 0 : handle.kernel.thread_process_thread_count(thread.vmPtr);
      const hasLiveThreads = !faultStateLooksStale
        && remainingThreads !== 0
        && processHasLiveThreads(handle.kernel, thread.vmPtr, thread.vmPtr);
      const shouldExitProcess = remainingThreads <= 1 || !hasLiveThreads;
      deps.appendWorkerDebugLog(handle, `fault:0x${thread.vmPtr.toString(16)} remaining=${remainingThreads}`);
      if (shouldExitProcess) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `complete-exit-request:transitions-fault:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:status=${status}:msg=${msg.status}:exit=${msg.exitCode}:${msg.termSignal}`
            + `:current=${handle.kernel.thread_status?.(thread.vmPtr) ?? -1}:remaining=${remainingThreads}:live=${hasLiveThreads ? 1 : 0}`,
        );
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
        if (wroteToPipe || drainedPipe || (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0) {
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

    const shouldSyncPipeState = wroteToPipe
      || drainedPipe
      || (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0;
    const pendingHostResume = status === handle.kernel.abi_status_fs();
    if (shouldSyncPipeState && !pendingHostResume) {
      void deps.requestPipeStateSync(handle, true).catch((error) => {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    }
    const networkRelevantStatus = status === handle.kernel.abi_status_connect()
      || status === handle.kernel.abi_status_socket_recv()
      || status === handle.kernel.abi_status_socket_send()
      || status === handle.kernel.abi_status_pipe_wait()
      || status === handle.kernel.abi_status_epoll_wait();
    if (networkRelevantStatus) {
      restoreRuntimeNetworkSidecarForHandle(handle, deps.processHandles, thread.vmPtr);
    }

    if (
      handle.networkConnections.size > 0
      && status !== handle.kernel.abi_status_connect()
    ) {
      void deps.pumpNetworkConnections(handle, thread.vmPtr)
        .then(async (changed) => {
          if (!changed) {
            return;
          }
          await deps.requestSocketStateSync(handle, true);
          if (!workerStillOwnedByHandle()) {
            return;
          }
          deps.kickBlockedWorkers(handle);
          deps.pumpProcess(handle);
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
        const resumeZeroDurationSleep = (): void => {
          handle.kernel.thread_resume_sleep?.(thread.vmPtr);
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
        };
        const enqueueAfterYield = (): void => {
          if (handle.readyWorkers.length > 0) {
            deps.pumpProcess(handle);
            setTimeout(() => {
              if (workerStillOwnedByHandle()) {
                resumeZeroDurationSleep();
                deps.enqueueWorker(handle, worker);
              }
            }, 0);
            return;
          }
          deps.enqueueWorker(handle, worker);
        };
        const shouldSyncPipeState = (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0;
        if (handle.kernelWorkerRequest && handle.pid > 0 && shouldSyncPipeState) {
          void deps.requestPipeStateSync(handle, false)
            .then(() => {
              if (!workerStillOwnedByHandle()) {
                return;
              }
              resumeZeroDurationSleep();
              deps.kickBlockedWorkers(handle);
              enqueueAfterYield();
            })
            .catch((error) => {
              if (!workerStillOwnedByHandle()) {
                return;
              }
              deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
            });
          return;
        }
        resumeZeroDurationSleep();
        deps.kickBlockedWorkers(handle);
        enqueueAfterYield();
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
      if (shouldRefreshPipeStateBeforeFsResume(handle, thread.vmPtr)) {
        thread.ownerAsyncStatus = status;
        void deps.requestPipeStateSync(handle, false)
          .then(() => {
            refreshPipeControlFromKernelShare(handle);
            const pipeSlotsForNextStep = snapshotProcessPipeSlotsForThreadStep(handle, thread.vmPtr);
            if (pipeSlotsForNextStep.length > 0) {
              thread.pendingPipeSlotsForNextStep.push(...pipeSlotsForNextStep);
            }
            if (!workerStillOwnedByHandle()) {
              thread.ownerAsyncStatus = null;
              return;
            }
            const resumeFs = deps.resumeFsViaKernelWorkerOutcome ?? deps.resumeFsViaKernelWorker;
            return resumeFs(handle, worker, thread.vmPtr)
              .then((outcome) => {
                if (outcome !== undefined) {
                  consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
                }
              })
              .then(() => undefined)
              .finally(() => {
                thread.ownerAsyncStatus = null;
                deps.pumpProcess(handle);
              });
          })
          .catch((error) => {
            thread.ownerAsyncStatus = null;
            if (!workerStillOwnedByHandle()) {
              return;
            }
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (!handle.kernelWorkerRequest || handle.pid <= 0) {
        const resumeFs = deps.resumeFsViaKernelWorkerOutcome ?? deps.resumeFsViaKernelWorker;
        void resumeFs(handle, worker, thread.vmPtr)
          .then((outcome) => {
            if (outcome !== undefined) {
              consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
            }
          })
          .catch((error) => {
            if (!workerStillOwnedByHandle()) {
              return;
            }
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      thread.ownerAsyncStatus = status;
      const resumeFs = deps.resumeFsViaKernelWorkerOutcome ?? deps.resumeFsViaKernelWorker;
      void resumeFs(handle, worker, thread.vmPtr)
        .then((outcome) => {
          if (outcome !== undefined) {
            consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
          }
        })
        .finally(() => {
          thread.ownerAsyncStatus = null;
          deps.pumpProcess(handle);
        })
        .catch((error) => {
          if (!workerStillOwnedByHandle()) {
            return;
          }
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }

    if (status === handle.kernel.abi_status_connect()) {
      const socketTimeoutMs = handle.kernel.thread_socket_wait_timeout === undefined
        ? -1
        : Number(handle.kernel.thread_socket_wait_timeout(thread.vmPtr));
      if (socketTimeoutMs >= 0) {
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        deps.pumpProcess(handle);
        return;
      }
      if (restoreRuntimeNetworkSidecarForHandle(handle, deps.processHandles, thread.vmPtr)) {
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
        const connectResult = deps.resolveNetworkConnect(handle, thread.vmPtr);
        void connectResult
          .then(async (result) => {
            await deps.requestSocketStateSync(handle, false);
            const outcome = await deps.resumeConnectViaKernelWorker(handle, worker, thread.vmPtr, result);
            if (outcome !== undefined) {
              consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
            }
            await deps.commitNetworkConnect(handle, thread.vmPtr, result);
          })
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
      if (!handle.kernelWorkerRequest || handle.pid <= 0) {
        void deps.resumeConnectViaKernelWorker(
          handle,
          worker,
          thread.vmPtr,
          BigInt(-handle.kernel.abi_econnrefused()),
        ).then((outcome) => {
          if (outcome !== undefined) {
            consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
          }
        }).catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
        deps.kickBlockedWorkers(handle);
        deps.enqueueWorker(handle, worker);
        return;
      }
      void deps.requestSocketStateSync(handle, false)
        .then(() => deps.resumeConnectViaKernelWorker(
          handle,
          worker,
          thread.vmPtr,
          BigInt(-handle.kernel.abi_econnrefused()),
        ))
        .then((outcome) => {
          if (outcome !== undefined) {
            consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
          }
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
        thread.ownerAsyncStatus = status;
        void deps.syncProcessLifecycleViaKernelWorker(handle, thread.vmPtr)
          .then(() => {
            if (!workerStillOwnedByHandle()) {
              return;
            }
            thread.ownerAsyncStatus = null;
            finishCloneSpawn();
          })
          .catch((error) => {
            thread.ownerAsyncStatus = null;
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
      thread.ownerAsyncStatus = status;
      void deps.handleExecveSpawn(handle, worker, thread)
        .finally(() => {
          if (workerStillOwnedByHandle()) {
            thread.ownerAsyncStatus = null;
          }
        })
        .catch((error) => {
          if (!workerStillOwnedByHandle()) {
            return;
          }
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
            cause: { kind: "syscall", nr: -1n, name: "vfork-wait-preserve-source" },
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
                ).then((outcome) => {
                  if (outcome !== undefined) {
                    consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
                  }
                });
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
          void deps.canonicalizeActiveVforkChildViaKernelWorker(
            handle,
            worker,
            thread.vmPtr,
            provisionalChildPid,
          ).then((outcome) => {
            if (outcome !== undefined) {
              consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
            }
          });
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
        const rpcGeneration = blocked ? beginStatusTransitionRpc(blocked) : -1;
        void deps.requestSocketStateSync(handle, true)
          .then(() => {
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            deps.kickBlockedWorkers(handle);
            deps.pumpProcess(handle);
          })
          .catch((error) => {
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (
        status === handle.kernel.abi_status_wait4()
        && handle.kernelWorkerRequest
        && handle.pid > 0
      ) {
        if (handle.runningWorker === worker) {
          handle.runningWorker = null;
        }
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        if (handle.runningWorker === worker) {
          handle.runningWorker = null;
        }
        const blocked = handle.blockedWorkers.get(worker);
        const rpcGeneration = blocked ? beginStatusTransitionRpc(blocked) : -1;
        void deps.requestPipeStateSync(handle, true)
          .then(() => {
            const latestBlocked = finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            if (!latestBlocked) {
              return;
            }
            deps.kickBlockedWorkers(handle);
            deps.pumpProcess(handle);
          })
          .catch((error) => {
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
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
        const rpcGeneration = blocked ? beginStatusTransitionRpc(blocked) : -1;
        const syncPipeState = status === handle.kernel.abi_status_pipe_wait()
          ? deps.requestPipeStateSync(handle, false)
          : Promise.resolve();
        void syncPipeState
          .then(() => {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-start:${handle.pid}:status=${status}`);
            return deps.syncBlockedForkSharedStateViaKernelWorker(
              handle,
              thread.vmPtr,
              { kind: "blocked-rpc", status },
            );
          })
          .then(() => {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-done:${handle.pid}:status=${status}`);
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
              handle.blockedWorkers.delete(worker);
              return;
            }
            if (
              status === handle.kernel.abi_status_pipe_wait()
              && (handle.kernel.thread_poll_wait_ready_count?.(thread.vmPtr) ?? 0) <= 0
            ) {
              return;
            }
            deps.kickBlockedWorkers(handle);
            deps.pumpProcess(handle);
          })
          .catch((error) => {
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
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
