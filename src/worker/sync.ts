import { attachKernelRuntimeState, readKernelRuntimeState, type KernelRuntimeState } from "../abi.js";
import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPidStateSnapshot,
  KernelWorkerSocketStateSnapshot,
} from "../messages.js";
import {
  copyLegacySharedKernelObjectsLocal,
  kernelShareOtherMembers,
  restoreLocalOfdSlots,
  trackForkSharedOfds,
} from "./kernel-share.js";
import {
  activeKernelShareOfdIds,
  collectHeldEpollOfdIds,
  collectProcessHeldOfds,
  mergeKernelShareOfdStateInto,
  pruneStaleKernelShareOfdIds,
  reconcileForkSharedOfdRefcounts,
  snapshotKernelShareOfdSlots,
  staleKernelShareOfdIds,
  syncKernelShareOfdReservations,
} from "./fork-shared.js";
import type { ProcessHandle } from "./types.js";

const lastForkSharedActiveOfdIdsByHandle = new WeakMap<ProcessHandle, Set<number>>();

interface NetworkBridgeSocketStateSnapshot {
  handle: ProcessHandle;
  connectionFds: number[];
  socketTable: Uint8Array;
  netBufPool: Uint8Array;
  ofdSlots: Array<{
    ofdId: number;
    bytes: Uint8Array;
  }>;
  socketUsed: number;
  netBufUsed: bigint;
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) {
    return false;
  }
  for (let index = 0; index < left.byteLength; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }
  return true;
}

function snapshotNetworkBridgeSocketState(
  handles: Iterable<ProcessHandle>,
): NetworkBridgeSocketStateSnapshot[] {
  const snapshots: NetworkBridgeSocketStateSnapshot[] = [];
  for (const handle of handles) {
    if (handle.networkConnections.size === 0) {
      continue;
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    const memory = new Uint8Array(handle.memory.buffer);
    const socketTableBytes = handle.kernel.kernel_socket_table_bytes();
    const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes();
    const ofdSlotBytes = Math.floor(handle.kernel.kernel_ofd_table_bytes() / handle.kernel.abi_max_fds());
    const ofdSlots: NetworkBridgeSocketStateSnapshot["ofdSlots"] = [];
    const connectionFds = [...handle.networkConnections.keys()];
    for (let ofdId = 1; ofdId < handle.kernel.abi_max_fds(); ofdId += 1) {
      if (
        ofdSlotBytes <= 0
        || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_socket()
      ) {
        continue;
      }
      const offset = handle.kernelState.ofdTablePtr + ofdId * ofdSlotBytes;
      ofdSlots.push({
        ofdId,
        bytes: memory.slice(offset, offset + ofdSlotBytes),
      });
    }
    snapshots.push({
      handle,
      connectionFds,
      socketTable: memory.slice(
        handle.kernelState.socketTablePtr,
        handle.kernelState.socketTablePtr + socketTableBytes,
      ),
      netBufPool: memory.slice(
        handle.kernelState.netBufBase,
        handle.kernelState.netBufBase + netBufPoolBytes,
      ),
      ofdSlots,
      socketUsed: handle.kernelState.socketUsed,
      netBufUsed: handle.kernelState.netBufUsed,
    });
  }
  return snapshots;
}

function snapshotSocketStateForKernelWorker(handle: ProcessHandle): KernelWorkerSocketStateSnapshot {
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  const memory = new Uint8Array(handle.memory.buffer);
  const socketTableBytes = handle.kernel.kernel_socket_table_bytes();
  const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes();
  return {
    socketTable: memory.slice(
      handle.kernelState.socketTablePtr,
      handle.kernelState.socketTablePtr + socketTableBytes,
    ),
    netBufPool: memory.slice(
      handle.kernelState.netBufBase,
      handle.kernelState.netBufBase + netBufPoolBytes,
    ),
    socketUsed: handle.kernelState.socketUsed,
    netBufUsed: handle.kernelState.netBufUsed,
  };
}

function applySocketStateFromKernelWorker(
  handle: ProcessHandle,
  snapshot: KernelWorkerSocketStateSnapshot,
): void {
  const memory = new Uint8Array(handle.memory.buffer);
  memory.set(snapshot.socketTable, handle.kernelState.socketTablePtr);
  memory.set(snapshot.netBufPool, handle.kernelState.netBufBase);
  handle.kernelState = {
    ...handle.kernelState,
    socketUsed: snapshot.socketUsed,
    netBufUsed: snapshot.netBufUsed,
  };
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

function restoreNetworkBridgeSocketState(
  snapshots: NetworkBridgeSocketStateSnapshot[],
): void {
  for (const snapshot of snapshots) {
    const { handle } = snapshot;
    if (snapshot.connectionFds.some((fd) => !handle.networkConnections.has(fd))) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      continue;
    }
    const memory = new Uint8Array(handle.memory.buffer);
    const currentNetBufPool = new Uint8Array(
      handle.memory.buffer,
      handle.kernelState.netBufBase,
      snapshot.netBufPool.byteLength,
    );
    const netBufferChanged = !bytesEqual(currentNetBufPool, snapshot.netBufPool);
    const ofdSlotBytes = Math.floor(handle.kernel.kernel_ofd_table_bytes() / handle.kernel.abi_max_fds());
    if (netBufferChanged) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      continue;
    }
    memory.set(snapshot.socketTable, handle.kernelState.socketTablePtr);
    memory.set(snapshot.netBufPool, handle.kernelState.netBufBase);
    for (const ofdSlot of snapshot.ofdSlots) {
      if (ofdSlot.ofdId <= 0 || ofdSlotBytes <= 0) {
        continue;
      }
      memory.set(ofdSlot.bytes, handle.kernelState.ofdTablePtr + ofdSlot.ofdId * ofdSlotBytes);
    }
    handle.kernelState = {
      ...handle.kernelState,
      socketUsed: snapshot.socketUsed,
      netBufUsed: snapshot.netBufUsed,
    };
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
  }
}

function snapshotProcessFdEntries(handle: ProcessHandle, vmPtr: number): KernelWorkerFdEntrySnapshot[] {
  const fdEntries: KernelWorkerFdEntrySnapshot[] = [];
  for (let fd = 0; fd < handle.kernel.abi_max_fds(); fd += 1) {
    fdEntries.push({
      fd,
      ofdId: handle.kernel.process_fd_ofd_id(vmPtr, fd),
      fdFlags: handle.kernel.process_fd_flags(vmPtr, fd),
    });
  }
  return fdEntries;
}

function ofdIdsFromFdEntries(
  handle: ProcessHandle,
  fdEntries: KernelWorkerFdEntrySnapshot[],
): number[] {
  const maxFds = handle.kernel.abi_max_fds();
  return [...new Set(
    fdEntries
      .map((entry) => entry.ofdId)
      .filter((ofdId) => ofdId > 0 && ofdId < maxFds),
  )];
}

function isProcessHostStdioOfd(handle: ProcessHandle, ofdId: number): boolean {
  for (const [fd, stdioPipeSlot] of [
    [0, handle.stdinPipe],
    [1, handle.stdoutPipe],
    [2, handle.stderrPipe],
  ] as const) {
    const fdOfdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (
      fdOfdId === ofdId
      && stdioPipeSlot > 0
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
      && handle.kernel.kernel_ofd_ino(ofdId) === stdioPipeSlot
    ) {
      return true;
    }
  }
  return false;
}

function trackHeldForkSharedOfds(handle: ProcessHandle, vmPtr: number): void {
  trackForkSharedOfds(handle, vmPtr, false);
  for (const ofdId of collectProcessHeldOfds(handle)) {
    if (!isProcessHostStdioOfd(handle, ofdId)) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
    }
  }
}

function collectTracedForkSharedOfdIds(
  handle: ProcessHandle,
  sourceVmPtr: number,
): number[] {
  const len = handle.kernel.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return [];
  }
  const traced = new Set<number>();
  const sampleToPid = (sample: bigint | number | undefined): number => {
    if (sample === undefined) {
      return 0;
    }
    return Number(BigInt.asIntN(32, BigInt(sample) & 0xffffffffn));
  };
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
    if (traceVmPtr !== 0 && sourceVmPtr !== 0 && traceVmPtr !== sourceVmPtr) {
      continue;
    }
    const tracePid = sampleToPid(handle.kernel.debug_fd_syscall_trace_pid_sample0?.(index));
    if (tracePid > 0 && tracePid !== handle.pid) {
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

function expandPipePeerOfdIds(handle: ProcessHandle, ofdIds: Iterable<number>): number[] {
  const pipeSlots = new Set<number>();
  for (const ofdId of ofdIds) {
    if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
      continue;
    }
    if (handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()) {
      const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
      if (pipeSlot > 0) {
        pipeSlots.add(pipeSlot);
      }
    }
  }
  if (pipeSlots.size === 0) {
    return [];
  }

  const peers = new Set<number>();
  for (const member of handle.kernelShare.members) {
    for (let ofdId = 1; ofdId < member.kernel.abi_max_fds(); ofdId += 1) {
      if (
        member.kernel.kernel_ofd_fd_type(ofdId) === member.kernel.abi_fd_type_pipe()
        && pipeSlots.has(member.kernel.kernel_ofd_ino(ofdId))
      ) {
        peers.add(ofdId);
      }
    }
  }
  return [...peers];
}

function pipeSlotForOfd(handle: ProcessHandle, ofdId: number): number {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return 0;
  }
  if (handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()) {
    return 0;
  }
  return handle.kernel.kernel_ofd_ino(ofdId);
}

function hasPipePeerInOtherKernelShareMember(handle: ProcessHandle, ofdId: number): boolean {
  const pipeSlot = pipeSlotForOfd(handle, ofdId);
  if (pipeSlot <= 0) {
    return false;
  }
  for (const member of handle.kernelShare.members) {
    if (member === handle) {
      continue;
    }
    for (const peerOfdId of collectProcessHeldOfds(member)) {
      if (
        member.kernel.kernel_ofd_fd_type(peerOfdId) === member.kernel.abi_fd_type_pipe()
        && member.kernel.kernel_ofd_ino(peerOfdId) === pipeSlot
      ) {
        return true;
      }
    }
  }
  return false;
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

export function adoptKernelStateForHandle(
  handle: ProcessHandle,
  incoming: KernelRuntimeState,
): KernelRuntimeState {
  const current = handle.kernelState;
  const adopted = adoptKernelState(current, incoming);
  if (handle.networkConnections.size === 0) {
    return adopted;
  }
  return {
    ...adopted,
    socketUsed: current.socketUsed | incoming.socketUsed,
    netBufUsed: current.netBufUsed | incoming.netBufUsed,
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
  syncKernelShareOfdReservations(handle);
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

  if (options.trackHeldOfds === true && options.deltaOfdIds !== true) {
    trackHeldForkSharedOfds(handle, options.sourceVmPtr ?? handle.vmPtr);
  }
  const sourceVmPtr = options.sourceVmPtr ?? handle.vmPtr;
  const sourceFdEntries = snapshotProcessFdEntries(handle, sourceVmPtr);
  const sourceFdOfdIds = ofdIdsFromFdEntries(handle, sourceFdEntries);
  const preservedSourceFdOfdSlots = snapshotKernelShareOfdSlots(handle, sourceFdOfdIds);
  const sourceHeldOfdIds = collectProcessHeldOfds(handle);
  const activeOfdIds = activeKernelShareOfdIds(handle);
  const sourceActiveOfdIds = activeOfdIds
    .filter((ofdId) => sourceHeldOfdIds.has(ofdId) || sourceFdOfdIds.includes(ofdId));
  const staleOfdIds = staleKernelShareOfdIds(handle);
  syncKernelShareOfdReservations(handle);
  let syncOfdIds = [...new Set([...sourceHeldOfdIds, ...sourceFdOfdIds, ...activeOfdIds, ...staleOfdIds])]
    .sort((left, right) => left - right);
  if (options.deltaOfdIds) {
    const tracedOfdIds = collectTracedForkSharedOfdIds(handle, sourceVmPtr)
      .filter((ofdId) => {
        if (handle.kernelShare.sharedOfdIds.has(ofdId)) {
          return true;
        }
        if (sourceHeldOfdIds.has(ofdId) && hasPipePeerInOtherKernelShareMember(handle, ofdId)) {
          handle.kernelShare.sharedOfdIds.add(ofdId);
          return true;
        }
        return false;
      });
    syncKernelShareOfdReservations(handle);
    const deltaOfdIdSet = new Set([
      ...sourceActiveOfdIds,
      ...sourceFdOfdIds,
      ...activeOfdIds,
      ...tracedOfdIds,
      ...staleOfdIds,
    ]);
    for (const ofdId of expandPipePeerOfdIds(handle, deltaOfdIdSet)) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
      deltaOfdIdSet.add(ofdId);
    }
    const deltaOfdIds = [...deltaOfdIdSet].sort((left, right) => left - right);
    deltaOfdIds.sort((left, right) => left - right);
    if (deltaOfdIds.length === 0) {
      lastForkSharedActiveOfdIdsByHandle.set(handle, new Set(sourceActiveOfdIds));
      return;
    }
    syncOfdIds = deltaOfdIds;
  }
  mergeKernelShareOfdStateInto(handle, sourceActiveOfdIds);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  const snapshotOfdIds = options.deltaOfdIds
    ? [...new Set([...sourceActiveOfdIds, ...sourceFdOfdIds, ...syncOfdIds])]
    : [...new Set([...sourceHeldOfdIds, ...sourceFdOfdIds, ...sourceActiveOfdIds, ...syncOfdIds])];
  const ofdSlots: KernelWorkerOfdSlotSnapshot[] = [...snapshotKernelShareOfdSlots(handle, snapshotOfdIds)]
    .map(([ofdId, slot]) => ({ ofdId, slot }));
  const targetPids = targets.map((target) => target.pid).filter((pid) => pid > 0);
  const targetKernelStates: KernelWorkerPidStateSnapshot[] = targets
    .filter((target) => target.pid > 0)
    .map((target) => ({
      pid: target.pid,
      vmPtr: target.vmPtr,
      kernelState: target.kernelState,
    }));
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle, ...targets]);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "sync-fork-shared-state",
      pid: handle.pid,
      vmPtr: options.sourceVmPtr ?? handle.vmPtr,
      targetPids,
      targetKernelStates,
      ofdIds: syncOfdIds,
      ofdSlots,
      fdEntries: sourceFdEntries,
      includeMemfs: options.includeMemfs ?? true,
      preserveSourceRefcounts: options.preserveSourceRefcounts,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "fork-shared-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-fork-shared-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  restoreLocalOfdSlots(handle, preservedSourceFdOfdSlots);
  lastForkSharedActiveOfdIdsByHandle.set(handle, new Set(sourceActiveOfdIds));
  pruneStaleKernelShareOfdIds(handle, staleOfdIds);
  syncKernelShareOfdReservations(handle);
  const heldAfterSync = collectProcessHeldOfds(handle);
  const unheldActiveOfdIds = activeKernelShareOfdIds(handle)
    .filter((ofdId) => !heldAfterSync.has(ofdId));
  if (unheldActiveOfdIds.length > 0) {
    await rehydrateForkSharedStateViaKernelWorker([handle], unheldActiveOfdIds, {
      includeMemfs: false,
    });
  }
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
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState(targets);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await owner.kernelWorkerRequest({
      type: "rehydrate-fork-shared-state",
      targetPids,
      targetKernelStates,
      ofdIds,
      procSelfExeTargets: options.procSelfExeTargets,
      includeMemfs: options.includeMemfs,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "fork-shared-state-rehydrated") {
    throw new Error(
      `unexpected kernel-worker response for rehydrate-fork-shared-state: ${response.type}`,
    );
  }
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
}

export async function syncBlockedForkSharedStateViaKernelWorker(
  handle: ProcessHandle,
  sourceVmPtr = handle.vmPtr,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }
  const syncTargets = kernelShareOtherMembers(handle)
    .filter((member) => member.runningWorker === null);
  await syncForkSharedStateViaKernelWorker(
    handle,
    syncTargets,
    {
      includeMemfs: false,
      deltaOfdIds: true,
      trackHeldOfds: true,
      sourceVmPtr,
    },
  );
}

export async function syncPipeStateViaKernelWorker(handle: ProcessHandle): Promise<bigint> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return handle.kernelState.pipeUsed;
  }

  const fdEntries = snapshotProcessFdEntries(handle, handle.vmPtr);
  const fdEntryOfdIds = ofdIdsFromFdEntries(handle, fdEntries);
  const preservedFdOfdSlots = snapshotKernelShareOfdSlots(handle, fdEntryOfdIds);
  const fdEntryOfdIdSet = new Set(fdEntryOfdIds);
  const pipeOfdIds = [...new Set([
    ...collectProcessHeldOfds(handle),
    ...fdEntryOfdIds,
    ...activeKernelShareOfdIds(handle),
  ])]
    .filter((ofdId) => {
      if (fdEntryOfdIdSet.has(ofdId)) {
        return true;
      }
      const fdType = handle.kernel.kernel_ofd_fd_type(ofdId);
      return fdType === handle.kernel.abi_fd_type_pipe()
        || fdType === handle.kernel.abi_fd_type_pty_master?.()
        || fdType === handle.kernel.abi_fd_type_pty_slave?.();
    });
  const ofdSlots = [...snapshotKernelShareOfdSlots(handle, pipeOfdIds)]
    .map(([ofdId, slot]) => ({ ofdId, slot }));
  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "sync-pipe-state",
      pid: handle.pid,
      vmPtr: handle.vmPtr,
      fdEntries,
      ofdSlots,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "pipe-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-pipe-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  restoreLocalOfdSlots(handle, preservedFdOfdSlots);
  return handle.kernelState.pipeUsed;
}

export async function syncEpollStateViaKernelWorker(handle: ProcessHandle): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }
  if (collectHeldEpollOfdIds(handle).length === 0) {
    return;
  }

  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "sync-epoll-state",
      pid: handle.pid,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "epoll-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-epoll-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

export async function syncOfdStateViaKernelWorker(handle: ProcessHandle, ofdIds: number[]): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0 || ofdIds.length === 0) {
    return;
  }

  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "sync-ofd-state",
      pid: handle.pid,
      ofdIds,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "ofd-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-ofd-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
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

  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "sync-socket-state",
      pid: handle.pid,
      kernelState: handle.kernelState,
      socketState: snapshotSocketStateForKernelWorker(handle),
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "socket-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-socket-state: ${response.type}`);
  }
  handle.kernelState = response.socketState
    ? adoptKernelState(handle.kernelState, response.kernelState)
    : adoptKernelStateForHandle(handle, response.kernelState);
  if (response.socketState) {
    applySocketStateFromKernelWorker(handle, response.socketState);
  }
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
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

  const networkSocketSnapshots = snapshotNetworkBridgeSocketState([handle]);
  let response: Awaited<ReturnType<NonNullable<ProcessHandle["kernelWorkerRequest"]>>>;
  try {
    response = await handle.kernelWorkerRequest({
      type: "release-host-stdio",
      pid: handle.pid,
      vmPtr: handle.vmPtr,
      kernelState: handle.kernelState,
    });
  } finally {
    restoreNetworkBridgeSocketState(networkSocketSnapshots);
  }
  if (response.type !== "host-stdio-released") {
    throw new Error(`unexpected kernel-worker response for release-host-stdio: ${response.type}`);
  }
  handle.kernelState = adoptKernelStateForHandle(handle, response.kernelState);
  restoreNetworkBridgeSocketState(networkSocketSnapshots);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  return handle.kernelState.pipeUsed;
}
