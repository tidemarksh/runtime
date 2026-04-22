import { attachKernelRuntimeState, readKernelRuntimeState, type KernelRuntimeState } from "../abi.js";
import type {
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPidStateSnapshot,
} from "../messages.js";
import { copyLegacySharedKernelObjectsLocal, kernelShareOtherMembers } from "./kernel-share.js";
import {
  activeKernelShareOfdIds,
  collectHeldEpollOfdIds,
  collectProcessHeldOfds,
  mergeKernelShareOfdStateInto,
  pruneStaleKernelShareOfdIds,
  reconcileForkSharedOfdRefcounts,
  snapshotKernelShareOfdSlots,
  staleKernelShareOfdIds,
} from "./fork-shared.js";
import type { ProcessHandle } from "./types.js";

const lastForkSharedActiveOfdIdsByHandle = new WeakMap<ProcessHandle, Set<number>>();

function collectTracedForkSharedOfdIds(
  handle: ProcessHandle,
  sourceVmPtr: number,
): number[] {
  const len = handle.kernel.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return [];
  }
  const traced = new Set<number>();
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
    if (traceVmPtr !== 0 && sourceVmPtr !== 0 && traceVmPtr !== sourceVmPtr) {
      continue;
    }
    const arg0OfdId = handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0;
    const arg2OfdId = handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? 0;
    if (arg0OfdId > 0) {
      traced.add(arg0OfdId);
    }
    if (arg2OfdId > 0) {
      traced.add(arg2OfdId);
    }
  }
  return [...traced];
}

export function adoptKernelState(
  current: KernelRuntimeState,
  incoming: KernelRuntimeState,
): KernelRuntimeState {
  return {
    heapBase: incoming.heapBase,
    heapPtr: Math.max(current.heapPtr, incoming.heapPtr),
    ofdTablePtr: current.ofdTablePtr,
    memfsPtr: current.memfsPtr,
    memfsHeaderBytes: current.memfsHeaderBytes > 0 ? current.memfsHeaderBytes : incoming.memfsHeaderBytes,
    memfsInodesBytes: current.memfsInodesBytes > 0 ? current.memfsInodesBytes : incoming.memfsInodesBytes,
    memfsFirstChildBytes: current.memfsFirstChildBytes > 0
      ? current.memfsFirstChildBytes
      : incoming.memfsFirstChildBytes,
    memfsDirEntriesBytes: current.memfsDirEntriesBytes > 0
      ? current.memfsDirEntriesBytes
      : incoming.memfsDirEntriesBytes,
    memfsDataBytes: current.memfsDataBytes > 0 ? current.memfsDataBytes : incoming.memfsDataBytes,
    memStatePtr: current.memStatePtr,
    epollTablePtr: current.epollTablePtr,
    socketTablePtr: current.socketTablePtr,
    socketUsed: incoming.socketUsed,
    pipeBase: current.pipeBase,
    pipeUsed: incoming.pipeUsed,
    netBufBase: current.netBufBase,
    netBufUsed: incoming.netBufUsed,
    sigSlotsPtr: current.sigSlotsPtr,
    processPendingPtr: current.processPendingPtr,
    processRuntimeStatePtr: current.processRuntimeStatePtr,
    timeStatePtr: current.timeStatePtr,
  };
}

export function composeHandlePipeUsed(handle: ProcessHandle, sharedPipeUsed: bigint): bigint {
  let pipeUsed = sharedPipeUsed | 1n;
  for (const pipeSlot of [handle.stdinPipe, handle.stdoutPipe, handle.stderrPipe]) {
    if (pipeSlot > 0) {
      pipeUsed |= 1n << BigInt(pipeSlot);
    }
  }
  return pipeUsed;
}

export function blockedStatusUsesForkSharedStateSync(handle: ProcessHandle, status: number): boolean {
  return status === handle.kernel.abi_status_pipe_wait()
    || status === handle.kernel.abi_status_socket_recv()
    || status === handle.kernel.abi_status_socket_send()
    || status === handle.kernel.abi_status_flock_wait();
}

export function syncForkSharedStateLocally(
  handle: ProcessHandle,
  targets: ProcessHandle[],
  options: { includeMemfs?: boolean; trackHeldOfds?: boolean } = {},
  collectTrackableForkSharedOfds: (handle: ProcessHandle) => Set<number>,
): void {
  if (options.trackHeldOfds === true) {
    for (const ofdId of collectTrackableForkSharedOfds(handle)) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
    }
  }
  const activeOfdIds = activeKernelShareOfdIds(handle);
  const staleOfdIds = staleKernelShareOfdIds(handle);
  const syncOfdIds = [...new Set([...activeOfdIds, ...staleOfdIds])].sort((left, right) => left - right);
  mergeKernelShareOfdStateInto(handle, activeOfdIds);
  reconcileForkSharedOfdRefcounts(handle, syncOfdIds);
  for (const target of targets) {
    copyLegacySharedKernelObjectsLocal(handle, target, syncOfdIds, options);
    attachKernelRuntimeState(target.kernel, target.kernelState);
  }
  pruneStaleKernelShareOfdIds(handle, staleOfdIds);
}

export async function syncForkSharedStateViaKernelWorker(
  handle: ProcessHandle,
  targets: ProcessHandle[],
  options: {
    includeMemfs?: boolean;
    trackHeldOfds?: boolean;
    deltaOfdIds?: boolean;
    preserveSourceRefcounts?: boolean;
    sourceVmPtr?: number;
  } = {},
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    throw new Error("syncForkSharedStateViaKernelWorker requires kernel-worker ownership");
  }

  const sourceHeldOfdIds = collectProcessHeldOfds(handle);
  const activeOfdIds = activeKernelShareOfdIds(handle);
  const sourceActiveOfdIds = activeOfdIds
    .filter((ofdId) => sourceHeldOfdIds.has(ofdId));
  const staleOfdIds = staleKernelShareOfdIds(handle);
  const previousActiveOfdIds = lastForkSharedActiveOfdIdsByHandle.get(handle) ?? new Set<number>();
  let syncOfdIds = [...new Set([...sourceActiveOfdIds, ...staleOfdIds])]
    .sort((left, right) => left - right);
  if (options.deltaOfdIds) {
    const sourceVmPtr = options.sourceVmPtr ?? handle.vmPtr;
    const tracedOfdIds = collectTracedForkSharedOfdIds(handle, sourceVmPtr);
    const newlyActiveOfdIds = sourceActiveOfdIds.filter((ofdId) => !previousActiveOfdIds.has(ofdId));
    const deltaOfdIds = [...new Set([...tracedOfdIds, ...newlyActiveOfdIds, ...staleOfdIds])]
      .sort((left, right) => left - right);
    if (deltaOfdIds.length === 0) {
      lastForkSharedActiveOfdIdsByHandle.set(handle, new Set(sourceActiveOfdIds));
      return;
    }
    syncOfdIds = deltaOfdIds;
  }
  mergeKernelShareOfdStateInto(handle, sourceActiveOfdIds);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  const ofdSlots: KernelWorkerOfdSlotSnapshot[] = [...snapshotKernelShareOfdSlots(handle, sourceActiveOfdIds)]
    .map(([ofdId, slot]) => ({ ofdId, slot }));
  const targetPids = targets.map((target) => target.pid).filter((pid) => pid > 0);
  const targetKernelStates: KernelWorkerPidStateSnapshot[] = targets
    .filter((target) => target.pid > 0)
    .map((target) => ({
      pid: target.pid,
      vmPtr: target.vmPtr,
      kernelState: target.kernelState,
    }));
  const response = await handle.kernelWorkerRequest({
    type: "sync-fork-shared-state",
    pid: handle.pid,
    vmPtr: options.sourceVmPtr ?? handle.vmPtr,
    targetPids,
    targetKernelStates,
    ofdIds: syncOfdIds,
    ofdSlots,
    includeMemfs: options.includeMemfs ?? true,
    preserveSourceRefcounts: options.preserveSourceRefcounts,
    kernelState: handle.kernelState,
  });
  if (response.type !== "fork-shared-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-fork-shared-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  lastForkSharedActiveOfdIdsByHandle.set(handle, new Set(sourceActiveOfdIds));
  pruneStaleKernelShareOfdIds(handle, staleOfdIds);
}

export async function rehydrateForkSharedStateViaKernelWorker(
  targets: ProcessHandle[],
  ofdIds: number[],
  options: { procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean } = {},
): Promise<void> {
  if (targets.length === 0) {
    return;
  }
  const owner = targets.find((target) => target.kernelWorkerRequest && target.pid > 0);
  if (!owner?.kernelWorkerRequest) {
    return;
  }
  const targetPids = [...new Set(targets.map((target) => target.pid).filter((pid) => pid > 0))];
  const targetKernelStates: KernelWorkerPidStateSnapshot[] = targets
    .filter((target) => target.pid > 0)
    .map((target) => {
      target.kernelState = readKernelRuntimeState(target.kernel);
      attachKernelRuntimeState(target.kernel, target.kernelState);
      return {
        pid: target.pid,
        vmPtr: target.vmPtr,
        kernelState: target.kernelState,
      };
    });
  if (targetPids.length === 0) {
    return;
  }
  const response = await owner.kernelWorkerRequest({
    type: "rehydrate-fork-shared-state",
    targetPids,
    targetKernelStates,
    ofdIds,
    procSelfExeTargets: options.procSelfExeTargets,
    includeMemfs: options.includeMemfs,
  });
  if (response.type !== "fork-shared-state-rehydrated") {
    throw new Error(
      `unexpected kernel-worker response for rehydrate-fork-shared-state: ${response.type}`,
    );
  }
}

export async function syncBlockedForkSharedStateViaKernelWorker(
  handle: ProcessHandle,
  sourceVmPtr = handle.vmPtr,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }
  await syncForkSharedStateViaKernelWorker(
    handle,
    kernelShareOtherMembers(handle),
    {
      includeMemfs: false,
      deltaOfdIds: true,
      sourceVmPtr,
    },
  );
}

export async function syncPipeStateViaKernelWorker(handle: ProcessHandle): Promise<bigint> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return handle.kernelState.pipeUsed;
  }

  const response = await handle.kernelWorkerRequest({
    type: "sync-pipe-state",
    pid: handle.pid,
    kernelState: handle.kernelState,
  });
  if (response.type !== "pipe-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-pipe-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  return handle.kernelState.pipeUsed;
}

export async function syncEpollStateViaKernelWorker(handle: ProcessHandle): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }
  if (collectHeldEpollOfdIds(handle).length === 0) {
    return;
  }

  const response = await handle.kernelWorkerRequest({
    type: "sync-epoll-state",
    pid: handle.pid,
    kernelState: handle.kernelState,
  });
  if (response.type !== "epoll-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-epoll-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

export async function syncOfdStateViaKernelWorker(handle: ProcessHandle, ofdIds: number[]): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0 || ofdIds.length === 0) {
    return;
  }

  const response = await handle.kernelWorkerRequest({
    type: "sync-ofd-state",
    pid: handle.pid,
    ofdIds,
    kernelState: handle.kernelState,
  });
  if (response.type !== "ofd-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-ofd-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

export async function syncSocketStateViaKernelWorker(handle: ProcessHandle): Promise<{
  socketUsed: number;
  netBufUsed: bigint;
}> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return {
      socketUsed: handle.kernelState.socketUsed,
      netBufUsed: handle.kernelState.netBufUsed,
    };
  }

  const response = await handle.kernelWorkerRequest({
    type: "sync-socket-state",
    pid: handle.pid,
    kernelState: handle.kernelState,
  });
  if (response.type !== "socket-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-socket-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  return {
    socketUsed: handle.kernelState.socketUsed,
    netBufUsed: handle.kernelState.netBufUsed,
  };
}

export async function releaseHostStdioViaKernelWorker(handle: ProcessHandle): Promise<bigint> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    handle.kernel.process_release_host_stdio(handle.vmPtr);
    return handle.kernelState.pipeUsed;
  }

  const response = await handle.kernelWorkerRequest({
    type: "release-host-stdio",
    pid: handle.pid,
    vmPtr: handle.vmPtr,
    kernelState: handle.kernelState,
  });
  if (response.type !== "host-stdio-released") {
    throw new Error(`unexpected kernel-worker response for release-host-stdio: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  return handle.kernelState.pipeUsed;
}
