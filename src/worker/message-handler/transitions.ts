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
  BlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type { RuntimeNetworkBridge } from "../../network.js";
import type { KernelWorkerPipeSlotSnapshot } from "../../messages.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "./types.js";
import { createWorkerOwnershipGuard } from "../ownership/process-state.js";

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

function socketOfdForFd(handle: ProcessHandle, vmPtr: number, fd: number): number {
  return handle.kernel.process_fd_ofd_id?.(vmPtr, fd) ?? 0;
}

function networkConnectionOfds(handle: ProcessHandle): Map<number, number> {
  handle.networkConnectionOfds ??= new Map();
  return handle.networkConnectionOfds;
}

function isSocketOfd(handle: ProcessHandle, ofdId: number): boolean {
  if (ofdId <= 0) {
    return false;
  }
  const fdType = handle.kernel.kernel_ofd_fd_type?.(ofdId);
  const socketType = handle.kernel.abi_fd_type_socket?.();
  return fdType === undefined || socketType === undefined || fdType === socketType;
}

function findSocketFdForOfd(
  handle: ProcessHandle,
  vmPtr: number,
  ofdId: number,
  preferredFd: number,
): number | null {
  if (isSocketOfd(handle, ofdId) && socketOfdForFd(handle, vmPtr, preferredFd) === ofdId) {
    return preferredFd;
  }
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  for (let fd = 0; fd < maxFds; fd += 1) {
    if (fd !== preferredFd && socketOfdForFd(handle, vmPtr, fd) === ofdId && isSocketOfd(handle, ofdId)) {
      return fd;
    }
  }
  return null;
}

function processHasPipeFd(handle: ProcessHandle, vmPtr: number): boolean {
  if (vmPtr === 0) {
    return false;
  }
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  const pipeType = handle.kernel.abi_fd_type_pipe?.();
  if (maxFds <= 0 || pipeType === undefined) {
    return false;
  }
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = handle.kernel.process_fd_ofd_id?.(vmPtr, fd) ?? 0;
    if (ofdId <= 0) {
      continue;
    }
    if (handle.kernel.kernel_ofd_fd_type?.(ofdId) === pipeType) {
      return true;
    }
  }
  return false;
}

function snapshotProcessPipeSlotsForThreadStep(
  handle: ProcessHandle,
  vmPtr: number,
): KernelWorkerPipeSlotSnapshot[] {
  if (vmPtr === 0) {
    return [];
  }
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  const maxPipes = handle.kernel.abi_max_pipes?.() ?? 0;
  const pipeType = handle.kernel.abi_fd_type_pipe?.();
  const slotBytes = maxPipes > 0 ? handle.kernel.kernel_pipe_region_bytes() / maxPipes : 0;
  if (
    maxFds <= 0
    || maxPipes <= 0
    || pipeType === undefined
    || !Number.isInteger(slotBytes)
    || slotBytes <= 0
  ) {
    return [];
  }
  const memory = new Uint8Array(handle.memory.buffer);
  const snapshots = new Map<number, KernelWorkerPipeSlotSnapshot>();
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = handle.kernel.process_fd_ofd_id?.(vmPtr, fd) ?? 0;
    if (
      ofdId <= 0
      || ofdId >= maxFds
      || handle.kernel.kernel_ofd_fd_type?.(ofdId) !== pipeType
    ) {
      continue;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino?.(ofdId) ?? 0;
    if (pipeSlot <= 0 || pipeSlot >= maxPipes || snapshots.has(pipeSlot)) {
      continue;
    }
    const offset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
    if (offset < 0 || offset + slotBytes > memory.byteLength) {
      continue;
    }
    snapshots.set(pipeSlot, {
      pipeSlot,
      slot: memory.slice(offset, offset + slotBytes),
    });
  }
  return [...snapshots.values()];
}

function refreshPipeControlFromKernelShare(handle: ProcessHandle): void {
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  const maxPipes = handle.kernel.abi_max_pipes?.() ?? 0;
  const pipeType = handle.kernel.abi_fd_type_pipe?.();
  const slotBytes = maxPipes > 0 ? handle.kernel.kernel_pipe_region_bytes() / maxPipes : 0;
  if (
    maxFds <= 0
    || maxPipes <= 0
    || pipeType === undefined
    || !Number.isInteger(slotBytes)
    || slotBytes <= 0
  ) {
    return;
  }
  const readOpen = new Int32Array(maxPipes);
  const writeOpen = new Int32Array(maxPipes);
  for (const member of handle.kernelShare.members) {
    if (member.vmPtr === 0) {
      continue;
    }
    for (let fd = 0; fd < maxFds; fd += 1) {
      const ofdId = member.kernel.process_fd_ofd_id?.(member.vmPtr, fd) ?? 0;
      if (
        ofdId <= 0
        || ofdId >= maxFds
        || member.kernel.kernel_ofd_fd_type?.(ofdId) !== pipeType
      ) {
        continue;
      }
      const pipeSlot = member.kernel.kernel_ofd_ino?.(ofdId) ?? 0;
      if (pipeSlot <= 0 || pipeSlot >= maxPipes) {
        continue;
      }
      if ((member.kernel.kernel_ofd_host_fd?.(ofdId) ?? -1) >= 0) {
        readOpen[pipeSlot] += 1;
      } else {
        writeOpen[pipeSlot] += 1;
      }
    }
  }
  const view = new DataView(handle.memory.buffer);
  for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
    if (readOpen[pipeSlot] <= 0 && writeOpen[pipeSlot] <= 0) {
      continue;
    }
    const offset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
    if (offset < 0 || offset + 16 > handle.memory.buffer.byteLength) {
      continue;
    }
    view.setInt32(offset + 8, readOpen[pipeSlot], true);
    view.setInt32(offset + 12, writeOpen[pipeSlot], true);
  }
}

function shouldRefreshPipeStateBeforeFsResume(handle: ProcessHandle, vmPtr: number): boolean {
  return Boolean(handle.kernelWorkerRequest)
    && handle.pid > 0
    && kernelShareOtherMembers(handle).length > 0
    && processHasPipeFd(handle, vmPtr);
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

function restoreRuntimeNetworkSidecarForHandle(
  handle: ProcessHandle,
  processHandles: Map<number, ProcessHandle>,
  vmPtr: number,
): RuntimeNetworkBridge | undefined {
  const relatedHandles = [
    ...handle.kernelShare.members,
    ...processHandles.values(),
  ].filter((member) => member !== handle && member.runtimeId === handle.runtimeId);

  if (handle.networkBridge) {
    // Keep the existing bridge, but still restore fd sidecars below.
  } else {
    for (const member of relatedHandles) {
      if (member.networkBridge) {
        handle.networkBridge = member.networkBridge;
        break;
      }
    }
  }

  if (handle.networkBridge) {
    for (const member of relatedHandles) {
      for (const [sourceFd, connection] of member.networkConnections) {
        const sourceOfd = socketOfdForFd(member, member.vmPtr, sourceFd);
        if (!isSocketOfd(member, sourceOfd)) {
          continue;
        }
        const targetFd = findSocketFdForOfd(handle, vmPtr, sourceOfd, sourceFd);
        if (
          targetFd === null
          || handle.networkConnections.has(targetFd)
          || handle.networkClosedFds.has(targetFd)
        ) {
          continue;
        }
        handle.networkConnections.set(targetFd, connection);
        networkConnectionOfds(handle).set(targetFd, sourceOfd);
        handle.networkPendingConnectFds.delete(targetFd);
        handle.networkClosedSocketSlots.delete(targetFd);
      }
    }
  }
  return handle.networkBridge;
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
        || (completedSyncEffects & deps.syncEffectPipeState) !== 0;
      const shouldSyncSocketState = (completedSyncEffects & deps.syncEffectSocketState) !== 0;
      const shouldSyncLifecycle = (completedSyncEffects & deps.syncEffectProcessLifecycle) !== 0;
      const shouldWakeSignalWaiters = (completedSyncEffects & deps.syncEffectSignalWaiters) !== 0;
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

    const shouldSyncPipeState = wroteToPipe
      || drainedPipe
      || (completedSyncEffects & deps.syncEffectPipeState) !== 0;
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
        const shouldSyncPipeState = (completedSyncEffects & deps.syncEffectPipeState) !== 0;
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
            return deps.resumeFsViaKernelWorker(handle, worker, thread.vmPtr)
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
        void deps.resumeFsViaKernelWorker(handle, worker, thread.vmPtr)
          .catch((error) => {
            if (!workerStillOwnedByHandle()) {
              return;
            }
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      thread.ownerAsyncStatus = status;
      void deps.resumeFsViaKernelWorker(handle, worker, thread.vmPtr)
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
            await deps.resumeConnectViaKernelWorker(handle, worker, thread.vmPtr, result);
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
        ).catch((error) => {
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
            return deps.syncBlockedForkSharedStateViaKernelWorker(handle, thread.vmPtr);
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
