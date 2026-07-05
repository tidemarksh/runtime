import { processKernelWorkerRequest, setProcessKernelWorkerRequest } from "../../../../src/worker/process-kernel-worker-request-state.js";
import { addKernelShareMember, addKernelShareSharedOfdId, kernelShareHasSharedOfdId, kernelShareMembers, kernelShareSharedOfdIdList, processKernelShareGroup, setProcessKernelShareGroup } from "../../../../src/worker/kernel-share.js";
import { expect, test, vi } from "vite-plus/test";
import { registerForkSharedSyncPromise } from "../../../../src/worker/fork-shared-sync-debug.js";
import {
  getHandleForkSharedSync,
  getProcessKernelShareForkSharedSync,
  setActiveForkSharedSync,
} from "../../../../src/worker/fork-shared-sync-state.js";
import {
  adoptKernelState,
  adoptKernelStateForHandle,
  syncBlockedForkSharedStateViaKernelWorker,
  syncForkSharedStateViaKernelWorker,
} from "../../../../src/worker/sync.js";
import { reconcileKernelSharePipeControl } from "../../../../src/worker/kernel-share/pipe.js";
import {
  collectRuntimeNetworkSocketOfdIds,
  hasKernelSocketFdState,
} from "../../../../src/worker/ownership/socket-control.js";
import {
  networkConnectionOfds,
  networkConnections,
} from "../../../../src/worker/network-socket-state.js";
import { setProcessDebugCommand } from "../../../../src/worker/process-debug-state.js";
import type { KernelRuntimeState } from "../../../../src/abi.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

const FD_TYPE_NONE = 0;
const FD_TYPE_FILE = 1;
const FD_TYPE_PIPE = 6;
const FD_TYPE_EPOLL = 7;
const FD_TYPE_SOCKET = 8;
const PIPE_SLOT_MODE_READ = 1;
const PIPE_SLOT_MODE_WRITE = 2;
const PIPE_SLOT_BYTES = 32 + 65_536;
const OFD_SLOT_BYTES = 32;
const PROCESS_FD_ENTRY_BYTES = 8;

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0x20,
    memfsInodesBytes: 0x4000,
    memfsFirstChildBytes: 0x800,
    memfsDirEntriesBytes: 0x2000,
    memfsDataBytes: 0x400000,
    memStatePtr: 0x240000,
    epollTablePtr: 0x250000,
    socketTablePtr: 0x260000,
    socketUsed: 0,
    pipeBase: 0x270000,
    pipeUsed: 0n,
    netBufBase: 0x280000,
    netBufUsed: 0n,
    sigSlotsPtr: 0xc00000,
    processPendingPtr: 0xc10000,
    processRuntimeStatePtr: 0xc20000,
    timeStatePtr: 0xc30000,
  };
}

function seedOfd(
  memory: WebAssembly.Memory,
  state: KernelRuntimeState,
  ofdId: number,
  type: number,
  ino: number,
  hostFd: number,
): void {
  const view = new DataView(memory.buffer);
  const base = state.ofdTablePtr + ofdId * OFD_SLOT_BYTES;
  view.setInt32(base, type, true);
  view.setInt32(base + 4, hostFd, true);
  view.setUint32(base + 8, hostFd >= 0 ? 0 : 1, true);
  view.setUint32(base + 12, 1, true);
  view.setBigInt64(base + 16, 0n, true);
  view.setUint32(base + 24, ino, true);
}

function seedPipeSlot(
  memory: WebAssembly.Memory,
  state: KernelRuntimeState,
  pipeSlot: number,
  readOpen: number,
  writeOpen: number,
): void {
  const view = new DataView(memory.buffer);
  const base = state.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
  view.setInt32(base + 8, readOpen, true);
  view.setInt32(base + 12, writeOpen, true);
  state.pipeUsed |= 1n << BigInt(pipeSlot);
}

function makeKernel(
  memory: WebAssembly.Memory,
  state: KernelRuntimeState,
  fdMap: Map<number, number>,
) {
  const fdEntryBase = state.memStatePtr;
  const readOfdInt32 = (ofdId: number, offset: number): number =>
    new DataView(memory.buffer).getInt32(state.ofdTablePtr + ofdId * OFD_SLOT_BYTES + offset, true);
  const readOfdUint32 = (ofdId: number, offset: number): number =>
    new DataView(memory.buffer).getUint32(state.ofdTablePtr + ofdId * OFD_SLOT_BYTES + offset, true);
  const readOfdBigInt64 = (ofdId: number, offset: number): bigint =>
    new DataView(memory.buffer).getBigInt64(state.ofdTablePtr + ofdId * OFD_SLOT_BYTES + offset, true);
  const heldOfdRefcount = (ofdId: number): number => {
    let refcount = 0;
    for (const heldOfdId of fdMap.values()) {
      if (heldOfdId === ofdId) {
        refcount += 1;
      }
    }
    return refcount;
  };
  const pipeModeForOfd = (ofdId: number): { pipeSlot: number; mode: number } | null => {
    if (ofdId <= 0 || ofdId >= 32 || readOfdInt32(ofdId, 0) !== FD_TYPE_PIPE) {
      return null;
    }
    const pipeSlot = readOfdUint32(ofdId, 24);
    if (pipeSlot <= 0 || pipeSlot >= 32) {
      return null;
    }
    return {
      pipeSlot,
      mode: readOfdInt32(ofdId, 4) >= 0 ? PIPE_SLOT_MODE_READ : PIPE_SLOT_MODE_WRITE,
    };
  };
  const heldPipeSlotMask = (): bigint => {
    let mask = 0n;
    for (const ofdId of new Set(fdMap.values())) {
      const pipeMode = pipeModeForOfd(ofdId);
      if (pipeMode) {
        mask |= 1n << BigInt(pipeMode.pipeSlot);
      }
    }
    return mask;
  };
  const pipeSlotMode = (pipeSlot: number): number => {
    let mode = 0;
    for (const ofdId of new Set(fdMap.values())) {
      const pipeMode = pipeModeForOfd(ofdId);
      if (pipeMode?.pipeSlot === pipeSlot) {
        mode |= pipeMode.mode;
      }
    }
    return mode;
  };
  const pipeOpenCount = (pipeSlot: number, requestedMode: number): number => {
    let count = 0;
    for (const ofdId of new Set(fdMap.values())) {
      const pipeMode = pipeModeForOfd(ofdId);
      if (pipeMode?.pipeSlot === pipeSlot && (pipeMode.mode & requestedMode) !== 0) {
        count += 1;
      }
    }
    return count;
  };
  const forkSharedPeerFlagCompatible = 1;
  const forkSharedPeerFlagIncompatible = 1 << 1;
  const forkSharedPeerFlagPipePeer = 1 << 2;
  const forkSharedHeldOfdTrackFlagShared = 1;
  const forkSharedHeldOfdTrackFlagHostStdioPipe = 1 << 1;
  const forkSharedHeldOfdTrackFlagStdioFd = 1 << 2;
  const forkSharedHeldOfdTrackFlagFdEntrySource = 1 << 3;
  const forkSharedSyncTargetActionReject = 0;
  const forkSharedSyncTargetActionDefer = 1;
  const forkSharedSyncTargetActionUse = 2;
  const forkSharedOfdActionInclude = 1;
  const forkSharedOfdActionAddShared = 1 << 1;
  const forkSharedOfdActionRehydrate = 1 << 2;
  const forkSharedOfdIdentityKey = (ofdId: number): bigint => {
    if (ofdId <= 0 || ofdId >= 32) {
      return 0n;
    }
    const type = readOfdInt32(ofdId, 0);
    if (type <= 0) {
      return 0n;
    }
    const ino = readOfdUint32(ofdId, 24);
    const hostFd = readOfdInt32(ofdId, 4);
    const endpointToken = type === FD_TYPE_PIPE
      ? (hostFd >= 0 ? 1n : 2n)
      : type === FD_TYPE_SOCKET
        ? BigInt.asUintN(64, BigInt(hostFd))
        : 0n;
    let hash = 0xcbf29ce484222325n;
    for (const value of [BigInt(type), BigInt(ino), endpointToken]) {
      hash ^= BigInt.asUintN(64, value);
      hash = BigInt.asUintN(64, hash * 0x100000001b3n);
    }
    return hash === 0n ? 0xcbf29ce484222325n : hash;
  };
  const forkSharedOfdPublishAction = (ofdId: number, peerFlags: number): number => {
    if (ofdId <= 0 || ofdId >= 32) {
      return 0;
    }
    const type = readOfdInt32(ofdId, 0);
    if (type <= 0 || (peerFlags & forkSharedPeerFlagIncompatible) !== 0) {
      return 0;
    }
    const hasCompatiblePeer = (peerFlags & forkSharedPeerFlagCompatible) !== 0;
    const hasPipePeer = (peerFlags & forkSharedPeerFlagPipePeer) !== 0;
    if (type === FD_TYPE_FILE) {
      return hasCompatiblePeer ? 1 : 0;
    }
    if (type === FD_TYPE_PIPE) {
      return hasCompatiblePeer || hasPipePeer ? 1 : 0;
    }
    return 1;
  };
  const forkSharedEffectSyncAction = (
    hasEffect: number,
    hasTrackedState: number,
    sourceHoldsRegularFile: number,
    regularFileHasPeer: number,
  ): number => {
    if (hasEffect === 0 || hasTrackedState === 0) {
      return 0;
    }
    return sourceHoldsRegularFile !== 0 && regularFileHasPeer === 0 ? 0 : 1;
  };
  const forkSharedSyncTargetAction = (
    invalidPid: number,
    runningWorker: number,
    vforkReadyWorker: number,
    hasHostBlockedWorker: number,
    rpcPending: number,
    processExit: number,
    registeredProcessExit: number,
    registeredMismatch: number,
    incompatibleOfd: number,
    requireHostBlocked: number,
    includeMemfs: number,
    runtimeNetworkState: number,
  ): number => {
    if (
      invalidPid !== 0
      || processExit !== 0
      || registeredProcessExit !== 0
      || registeredMismatch !== 0
      || incompatibleOfd !== 0
    ) {
      return forkSharedSyncTargetActionReject;
    }
    if (includeMemfs !== 0 && runtimeNetworkState !== 0) {
      return forkSharedSyncTargetActionReject;
    }
    if (runningWorker !== 0 || vforkReadyWorker !== 0 || rpcPending !== 0) {
      return forkSharedSyncTargetActionDefer;
    }
    if (requireHostBlocked !== 0 && hasHostBlockedWorker === 0) {
      return forkSharedSyncTargetActionDefer;
    }
    return forkSharedSyncTargetActionUse;
  };
  const forkSharedSyncableOfdAction = (
    syncable: number,
    sourceConflicting: number,
    allowed: number,
    regularFile: number,
    sourcePublished: number,
    runtimeNetworkSocket: number,
  ): number => {
    if (syncable === 0 || sourceConflicting !== 0 || allowed === 0 || runtimeNetworkSocket !== 0) {
      return 0;
    }
    return regularFile !== 0 && sourcePublished === 0 ? 0 : forkSharedOfdActionInclude;
  };
  const forkSharedDeltaSourceOfdAction = (
    allowed: number,
    sourcePublished: number,
    sourceHeld: number,
    pipePeer: number,
  ): number => {
    if (allowed === 0) {
      return 0;
    }
    if (sourcePublished !== 0) {
      return forkSharedOfdActionInclude;
    }
    return sourceHeld !== 0 && pipePeer !== 0
      ? forkSharedOfdActionInclude | forkSharedOfdActionAddShared
      : 0;
  };
  const forkSharedTrackedSourceOfdAction = (
    kernelShared: number,
    sourceFdEntry: number,
    syncable: number,
    typeNone: number,
    incompatiblePeer: number,
  ): number => {
    if (syncable === 0 || typeNone !== 0 || (kernelShared === 0 && sourceFdEntry === 0)) {
      return 0;
    }
    return sourceFdEntry === 0 && incompatiblePeer !== 0 ? 0 : forkSharedOfdActionInclude;
  };
  const forkSharedPipePeerOfdAction = (stale: number, syncable: number): number => (
    stale !== 0 || syncable === 0 ? 0 : forkSharedOfdActionInclude | forkSharedOfdActionAddShared
  );
  const forkSharedRehydrateOfdAction = (active: number, sourceHeld: number, allowed: number): number => (
    active !== 0 && sourceHeld === 0 && allowed !== 0 ? forkSharedOfdActionRehydrate : 0
  );

  return {
    memory,
    abi_max_fds: () => 32,
    abi_max_threads: () => 4,
    abi_max_pipes: () => 32,
    abi_max_sockets: () => 32,
    abi_fd_type_none: () => FD_TYPE_NONE,
    abi_fd_type_file: () => FD_TYPE_FILE,
    abi_fd_type_pipe: () => FD_TYPE_PIPE,
    abi_fd_type_socket: () => FD_TYPE_SOCKET,
    supervisor_ofd_is_regular_file: (ofdId: number) => (
      ofdId > 0 && ofdId < 32 && readOfdInt32(ofdId, 0) === FD_TYPE_FILE ? 1 : 0
    ),
    supervisor_pipe_sync_ofd_candidate: (ofdId: number) => (
      ofdId > 0 && ofdId < 32 && readOfdInt32(ofdId, 0) === FD_TYPE_PIPE ? 1 : 0
    ),
    supervisor_ofd_pipe_generation_slot: (ofdId: number) => {
      const pipeMode = pipeModeForOfd(ofdId);
      return pipeMode ? pipeMode.pipeSlot : 0;
    },
    supervisor_ofd_pipe_slot_mask: (ofdId: number) => {
      const pipeMode = pipeModeForOfd(ofdId);
      return pipeMode ? 1n << BigInt(pipeMode.pipeSlot) : 0n;
    },
    supervisor_ofd_pipe_slot_mode: (ofdId: number, pipeSlot: number) => {
      const pipeMode = pipeModeForOfd(ofdId);
      return pipeMode?.pipeSlot === pipeSlot ? pipeMode.mode : 0;
    },
    supervisor_socket_ofd_is_socket: (ofdId: number) => (
      ofdId > 0 && ofdId < 32 && readOfdInt32(ofdId, 0) === FD_TYPE_SOCKET ? 1 : 0
    ),
    supervisor_process_socket_ofd_for_fd: (_vmPtr: number, fd: number) => {
      const ofdId = fdMap.get(fd) ?? 0;
      return ofdId > 0 && readOfdInt32(ofdId, 0) === FD_TYPE_SOCKET ? ofdId : 0;
    },
    supervisor_process_socket_drain_fd: (_vmPtr: number, connectedFd: number, index: number) => {
      const connectedOfd = fdMap.get(connectedFd) ?? 0;
      if (connectedOfd <= 0 || readOfdInt32(connectedOfd, 0) !== FD_TYPE_SOCKET) {
        return -1;
      }
      const candidates = [connectedFd];
      for (const [fd, ofdId] of fdMap) {
        if (fd !== connectedFd && ofdId === connectedOfd) {
          candidates.push(fd);
        }
      }
      return candidates[index] ?? -1;
    },
    kernel_heap_base: () => state.heapBase,
    kernel_heap_ptr: () => state.heapPtr,
    kernel_ofd_table_ptr: () => state.ofdTablePtr,
    kernel_ofd_table_bytes: () => 32 * OFD_SLOT_BYTES,
    kernel_memfs_ptr: () => state.memfsPtr,
    kernel_memfs_header_bytes: () => state.memfsHeaderBytes,
    kernel_memfs_inodes_bytes: () => state.memfsInodesBytes,
    kernel_memfs_first_child_bytes: () => state.memfsFirstChildBytes,
    kernel_memfs_dir_entries_bytes: () => state.memfsDirEntriesBytes,
    kernel_memfs_data_bytes: () => state.memfsDataBytes,
    kernel_mem_state_ptr: () => state.memStatePtr,
    kernel_epoll_table_ptr: () => state.epollTablePtr,
    kernel_socket_table_ptr: () => state.socketTablePtr,
    kernel_socket_table_bytes: () => 32 * 128,
    kernel_socket_used: () => state.socketUsed,
    kernel_pipe_base: () => state.pipeBase,
    kernel_pipe_used: () => state.pipeUsed,
    kernel_pipe_region_bytes: () => 32 * PIPE_SLOT_BYTES,
    kernel_net_buf_base: () => state.netBufBase,
    kernel_net_buf_pool_bytes: () => 32 * (16 + 8192),
    kernel_net_buf_used: () => state.netBufUsed,
    thread_socket_idx_for_fd: () => -1,
    thread_socket_peer_idx: () => -1,
    thread_socket_state: () => 3,
    thread_socket_external_peer: () => 1,
    thread_socket_recv_buf_idx: () => -1,
    thread_socket_recv_available: () => 0,
    thread_socket_read_bytes: () => 0n,
    thread_socket_outbound_available: () => 0,
    kernel_sig_slots_ptr: () => state.sigSlotsPtr,
    kernel_process_pending_ptr: () => state.processPendingPtr,
    kernel_process_runtime_state_ptr: () => state.processRuntimeStatePtr,
    kernel_time_state_ptr: () => state.timeStatePtr,
    kernel_attach_runtime_state: (
      heapBase: number,
      heapPtr: number,
      ofdStatePtr: number,
      memfsPtr: number,
      memStatePtr: number,
      epollTablePtr: number,
      socketTablePtr: number,
      socketUsed: number,
      pipeBase: number,
      pipeUsed: bigint,
      netBufBase: number,
      netBufUsed: bigint,
      sigSlotsPtr: number,
      processPendingPtr: number,
      processRuntimeStatePtr: number,
      timeStatePtr: number,
      signalfdStatePtr: number,
    ) => {
      Object.assign(state, {
        heapBase,
        heapPtr,
        ofdTablePtr: ofdStatePtr,
        memfsPtr,
        memStatePtr,
        epollTablePtr,
        socketTablePtr,
        socketUsed,
        pipeBase,
        pipeUsed,
        netBufBase,
        netBufUsed,
        sigSlotsPtr,
        processPendingPtr,
        processRuntimeStatePtr,
        timeStatePtr,
        signalfdStatePtr,
      });
    },
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
    process_fd_flags: () => 0,
    supervisor_process_ofd_for_fd: (_vmPtr: number, fd: number) => {
      const ofdId = fdMap.get(fd) ?? 0;
      return ofdId > 0 && ofdId < 32 ? ofdId : 0;
    },
    supervisor_process_fd_flags_for_fd: () => 0,
    supervisor_process_pipe_slot_for_fd: (_vmPtr: number, fd: number) => {
      const pipeMode = pipeModeForOfd(fdMap.get(fd) ?? 0);
      return pipeMode?.pipeSlot ?? 0;
    },
    supervisor_process_pipe_ofd_for_fd: (_vmPtr: number, fd: number) => {
      const ofdId = fdMap.get(fd) ?? 0;
      return pipeModeForOfd(ofdId) ? ofdId : 0;
    },
    supervisor_process_pipe_host_fd_for_fd: (_vmPtr: number, fd: number) => {
      const ofdId = fdMap.get(fd) ?? 0;
      return pipeModeForOfd(ofdId) ? readOfdInt32(ofdId, 4) : -1;
    },
    process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
      if (ofdId > 0) {
        fdMap.set(fd, ofdId);
      } else {
        fdMap.delete(fd);
      }
    },
    process_fd_table_ptr: () => fdEntryBase,
    process_fd_entry_bytes: () => PROCESS_FD_ENTRY_BYTES,
    process_thread_vm_ptr: (_vmPtr: number, slot: number) => (slot === 0 ? 0x1234 : 0),
    thread_process_ptr: () => 0x1000,
    process_state_bytes: () => 256,
    process_stdin_pipe: () => 0,
    process_stdout_pipe: () => 0,
    process_stderr_pipe: () => 0,
    thread_blocked_ofd_id: () => 0,
    supervisor_process_held_ofd_refcount: (_vmPtr: number, ofdId: number) => heldOfdRefcount(ofdId),
    supervisor_process_holds_regular_file_ofd: (_vmPtr: number, ofdId: number) => (
      heldOfdRefcount(ofdId) > 0 && readOfdInt32(ofdId, 0) === FD_TYPE_FILE ? 1 : 0
    ),
    supervisor_fd_entry_restore_action_apply: () => 1,
    supervisor_fd_entry_restore_action_skip: () => 2,
    supervisor_ofd_slot_write_action: (
      ofdId: number,
      candidateType: number,
      candidateHostFd: number,
      candidateIno: number,
      candidateOffset: bigint | number,
    ) => {
      if (ofdId <= 0 || ofdId >= 32) {
        return 2;
      }
      const currentType = readOfdInt32(ofdId, 0);
      if (currentType === 0 || readOfdUint32(ofdId, 12) <= 0) {
        return 1;
      }
      if (candidateType === 0 || candidateType !== currentType) {
        return 2;
      }
      if (
        candidateHostFd !== readOfdInt32(ofdId, 4)
        || candidateIno !== readOfdUint32(ofdId, 24)
      ) {
        return 2;
      }
      const candidate = typeof candidateOffset === "bigint" ? candidateOffset : BigInt(Math.trunc(candidateOffset));
      if (currentType === FD_TYPE_FILE && candidate < readOfdBigInt64(ofdId, 16)) {
        return 2;
      }
      return 1;
    },
    supervisor_fork_shared_peer_flag_compatible: () => forkSharedPeerFlagCompatible,
    supervisor_fork_shared_peer_flag_incompatible: () => forkSharedPeerFlagIncompatible,
    supervisor_fork_shared_peer_flag_pipe_peer: () => forkSharedPeerFlagPipePeer,
    supervisor_fork_shared_held_ofd_track_flag_shared: () => forkSharedHeldOfdTrackFlagShared,
    supervisor_fork_shared_held_ofd_track_flag_host_stdio_pipe: () => (
      forkSharedHeldOfdTrackFlagHostStdioPipe
    ),
    supervisor_fork_shared_held_ofd_track_flag_stdio_fd: () => forkSharedHeldOfdTrackFlagStdioFd,
    supervisor_fork_shared_held_ofd_track_flag_fd_entry_source: () => (
      forkSharedHeldOfdTrackFlagFdEntrySource
    ),
    supervisor_fork_shared_sync_target_action_reject: () => forkSharedSyncTargetActionReject,
    supervisor_fork_shared_sync_target_action_defer: () => forkSharedSyncTargetActionDefer,
    supervisor_fork_shared_sync_target_action_use: () => forkSharedSyncTargetActionUse,
    supervisor_fork_shared_ofd_action_include: () => forkSharedOfdActionInclude,
    supervisor_fork_shared_ofd_action_add_shared: () => forkSharedOfdActionAddShared,
    supervisor_fork_shared_ofd_action_rehydrate: () => forkSharedOfdActionRehydrate,
    supervisor_fork_shared_held_ofd_track_action: (ofdId: number, flags: number) => {
      if (ofdId <= 0 || ofdId >= 32) {
        return 0;
      }
      if ((flags & forkSharedHeldOfdTrackFlagHostStdioPipe) !== 0) {
        return 0;
      }
      if (
        (flags
          & (forkSharedHeldOfdTrackFlagShared
            | forkSharedHeldOfdTrackFlagStdioFd
            | forkSharedHeldOfdTrackFlagFdEntrySource)) !== 0
      ) {
        return 1;
      }
      return readOfdInt32(ofdId, 0) === FD_TYPE_FILE ? 0 : 1;
    },
    supervisor_fork_shared_ofd_identity_key: (ofdId: number) => forkSharedOfdIdentityKey(ofdId),
    supervisor_fork_shared_ofd_publish_action: (ofdId: number, peerFlags: number) => (
      forkSharedOfdPublishAction(ofdId, peerFlags)
    ),
    supervisor_fork_shared_effect_sync_action: (
      hasEffect: number,
      hasTrackedState: number,
      sourceHoldsRegularFile: number,
      regularFileHasPeer: number,
    ) => forkSharedEffectSyncAction(
      hasEffect,
      hasTrackedState,
      sourceHoldsRegularFile,
      regularFileHasPeer,
    ),
    supervisor_fork_shared_sync_target_action: (
      invalidPid: number,
      runningWorker: number,
      vforkReadyWorker: number,
      hasHostBlockedWorker: number,
      rpcPending: number,
      processExit: number,
      registeredProcessExit: number,
      registeredMismatch: number,
      incompatibleOfd: number,
      requireHostBlocked: number,
      includeMemfs: number,
      runtimeNetworkState: number,
    ) => forkSharedSyncTargetAction(
      invalidPid,
      runningWorker,
      vforkReadyWorker,
      hasHostBlockedWorker,
      rpcPending,
      processExit,
      registeredProcessExit,
      registeredMismatch,
      incompatibleOfd,
      requireHostBlocked,
      includeMemfs,
      runtimeNetworkState,
    ),
    supervisor_fork_shared_syncable_ofd_action: (
      syncable: number,
      sourceConflicting: number,
      allowed: number,
      regularFile: number,
      sourcePublished: number,
      runtimeNetworkSocket: number,
    ) => forkSharedSyncableOfdAction(
      syncable,
      sourceConflicting,
      allowed,
      regularFile,
      sourcePublished,
      runtimeNetworkSocket,
    ),
    supervisor_fork_shared_delta_source_ofd_action: (
      allowed: number,
      sourcePublished: number,
      sourceHeld: number,
      pipePeer: number,
    ) => forkSharedDeltaSourceOfdAction(allowed, sourcePublished, sourceHeld, pipePeer),
    supervisor_fork_shared_tracked_source_ofd_action: (
      kernelShared: number,
      sourceFdEntry: number,
      syncable: number,
      typeNone: number,
      incompatiblePeer: number,
    ) => forkSharedTrackedSourceOfdAction(
      kernelShared,
      sourceFdEntry,
      syncable,
      typeNone,
      incompatiblePeer,
    ),
    supervisor_fork_shared_pipe_peer_ofd_action: (stale: number, syncable: number) => (
      forkSharedPipePeerOfdAction(stale, syncable)
    ),
    supervisor_fork_shared_rehydrate_ofd_action: (
      active: number,
      sourceHeld: number,
      allowed: number,
    ) => forkSharedRehydrateOfdAction(active, sourceHeld, allowed),
    supervisor_process_private_pipe_slot_mask: () => 0n,
    supervisor_process_private_pipe_source_mode: () => 0,
    supervisor_process_private_pipe_target_mode: () => 0,
    supervisor_process_held_pipe_slot_mask: () => heldPipeSlotMask(),
    supervisor_process_pipe_used_mask: () => 1n | heldPipeSlotMask(),
    supervisor_process_pipe_slot_mode: (_vmPtr: number, pipeSlot: number) => pipeSlotMode(pipeSlot),
    supervisor_process_pipe_read_open_count: (_vmPtr: number, pipeSlot: number) => (
      pipeOpenCount(pipeSlot, PIPE_SLOT_MODE_READ)
    ),
    supervisor_process_pipe_write_open_count: (_vmPtr: number, pipeSlot: number) => (
      pipeOpenCount(pipeSlot, PIPE_SLOT_MODE_WRITE)
    ),
    supervisor_pipe_reconcile_control: (pipeSlot: number, readOpen: number, writeOpen: number) => {
      if (pipeSlot <= 0 || pipeSlot >= 32 || readOpen < 0 || writeOpen < 0) {
        return 0;
      }
      const slotBase = state.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
      const view = new DataView(memory.buffer);
      view.setInt32(slotBase + 8, Math.trunc(readOpen), true);
      view.setInt32(slotBase + 12, Math.trunc(writeOpen), true);
      return 1;
    },
    supervisor_ofd_fd_type(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_fd_type(ofdId) : 0;
    },
    supervisor_ofd_host_fd(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_host_fd(ofdId) : -1;
    },
    supervisor_ofd_status_flags(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_status_flags(ofdId) : 0;
    },
    supervisor_ofd_refcount(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_refcount(ofdId) : 0;
    },
    supervisor_ofd_offset(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_offset(ofdId) : 0n;
    },
    supervisor_ofd_ino(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_ino(ofdId) : 0;
    },
    kernel_ofd_fd_type: (ofdId: number) => readOfdInt32(ofdId, 0),
    kernel_ofd_host_fd: (ofdId: number) => readOfdInt32(ofdId, 4),
    kernel_ofd_status_flags: (ofdId: number) => readOfdUint32(ofdId, 8),
    kernel_ofd_refcount: (ofdId: number) => readOfdUint32(ofdId, 12),
    kernel_ofd_offset: (ofdId: number) => readOfdBigInt64(ofdId, 16),
    kernel_ofd_ino: (ofdId: number) => readOfdUint32(ofdId, 24),
  };
}

function makeHandle(pid: number, fdMap: Map<number, number>): ProcessHandle {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernelState = makeKernelState();
  const handle = {
    runtimeId: 1,
    pid,
    parentPid: null,
    vmPtr: 0x1234,
    guestMemoryPolicyLimitBytes: 0,
    maxMemoryPages: 64,
    forkMaxMemoryPages: 64,
    guestBase: 0,
    worker: {} as Worker,
    memory,
    wasmModule: {} as WebAssembly.Module,
    kernel: makeKernel(memory, kernelState, fdMap),
    kernelState,
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    ttyPtySlot: 0,
    hostStdio: { stdinPipe: 0, stdoutPipe: 0, stderrPipe: 0, refCount: 1 },
    stdioOwner: null,
    kernelShare: {
      members: new Set<ProcessHandle>(),
      sharedOfdIds: new Set<number>(),
      ofdSlotSnapshots: new Map<number, Uint8Array>(),
    },
    stdinQueue: [],
    stdinFlushPromise: null,
    outputBacklog: [],
    runtimeBridge: {} as never,
    sabPageCache: null,
    kernelWorkerRequest: vi.fn(async (msg) => {
      if (msg.type === "rehydrate-fork-shared-state") {
        return {
          type: "fork-shared-state-rehydrated",
          requestId: msg.requestId,
        };
      }
      return {
        type: "fork-shared-state-synced",
        requestId: msg.requestId,
        kernelState: msg.kernelState,
      };
    }),
    workers: new Set(),
    vforkReadyWorkers: [],
    runningWorker: null,
    hostBlockedWorkers: new Map([[{} as Worker, {
      vmPtr: 0x1234,
      status: 0,
    }]]),
    vforkSnapshot: null,
    vforkWorker: null,
    exitInProgress: false,
    workerDebugLog: [],
    guestWriteWatchRegions: [],
  } as ProcessHandle;
  setProcessDebugCommand(handle, "");
  addKernelShareMember(handle);
  return handle;
}

test("adoptKernelState preserves canonical pointer fields while keeping heapPtr monotonic", () => {
  const current = {
    heapBase: 0x200000,
    heapPtr: 0x210080,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0x20,
    memfsInodesBytes: 0x4000,
    memfsFirstChildBytes: 0x800,
    memfsDirEntriesBytes: 0x2000,
    memfsDataBytes: 0x400000,
    memStatePtr: 0x240000,
    epollTablePtr: 0x250000,
    socketTablePtr: 0x260000,
    socketUsed: 1,
    pipeBase: 0x270000,
    pipeUsed: 0n,
    netBufBase: 0x280000,
    netBufUsed: 0n,
    sigSlotsPtr: 0xc00000,
    processPendingPtr: 0xc10000,
    processRuntimeStatePtr: 0xc20000,
    timeStatePtr: 0xc30000,
  };
  const incoming = {
    ...current,
    heapBase: 0x300000,
    heapPtr: 0x210040,
    ofdTablePtr: 0x320000,
    memfsPtr: 0x330000,
    memfsHeaderBytes: 0x40,
    memfsInodesBytes: 0x8000,
    memfsFirstChildBytes: 0x1000,
    memfsDirEntriesBytes: 0x4000,
    memfsDataBytes: 0x800000,
    memStatePtr: 0x340000,
    epollTablePtr: 0x350000,
    socketTablePtr: 0x360000,
    socketUsed: 3,
    pipeBase: 0x370000,
    pipeUsed: 5n,
    netBufBase: 0x380000,
    netBufUsed: 7n,
    sigSlotsPtr: 0xd00000,
    processPendingPtr: 0xd10000,
    processRuntimeStatePtr: 0xd20000,
    timeStatePtr: 0xd30000,
  };

  expect(adoptKernelState(current, incoming)).toEqual({
    ...current,
    heapBase: incoming.heapBase,
    heapPtr: current.heapPtr,
    socketUsed: incoming.socketUsed,
    pipeUsed: incoming.pipeUsed,
    netBufUsed: incoming.netBufUsed,
  });
});

test("adoptKernelStateForHandle preserves local epoll table pointer", () => {
  const handle = makeHandle(7, new Map([[4, 10], [11, 12]]));
  const incoming = {
    ...handle.kernelState,
    heapBase: 0x300000,
    heapPtr: handle.kernelState.heapPtr + 0x80,
    epollTablePtr: 0x350000,
    pipeUsed: 4n,
  };

  const adopted = adoptKernelStateForHandle(handle, incoming);

  expect(adopted.heapBase).toBe(incoming.heapBase);
  expect(adopted.heapPtr).toBe(incoming.heapPtr);
  expect(adopted.epollTablePtr).toBe(handle.kernelState.epollTablePtr);
  expect(adopted.pipeUsed).toBe(incoming.pipeUsed | 1n);
});

test("adoptKernelStateForHandle uses supervisor pipe-used authority when available", () => {
  const handle = makeHandle(7, new Map([[4, 10]]));
  seedOfd(handle.memory, handle.kernelState, 10, FD_TYPE_PIPE, 6, 3);
  handle.kernel.supervisor_process_pipe_used_mask = () => 1n | (1n << 9n);
  const incoming = {
    ...handle.kernelState,
    pipeUsed: 1n,
  };

  const adopted = adoptKernelStateForHandle(handle, incoming);

  expect(adopted.pipeUsed & (1n << 9n)).not.toBe(0n);
  expect(adopted.pipeUsed & (1n << 6n)).toBe(0n);
});

test("fork-shared sync keeps a pipe reader target that also owns runtime network state", async () => {
  const child = makeHandle(7, new Map([[5, 8]]));
  const parent = makeHandle(3, new Map([[4, 7], [9, 19]]));
  addKernelShareMember(child, parent);
  setProcessKernelShareGroup(parent, processKernelShareGroup(child));
  addKernelShareSharedOfdId(child, 8);

  seedOfd(child.memory, child.kernelState, 8, FD_TYPE_PIPE, 6, -11);
  seedOfd(parent.memory, parent.kernelState, 7, FD_TYPE_PIPE, 6, 10);
  seedOfd(parent.memory, parent.kernelState, 19, FD_TYPE_SOCKET, 19, 99);
  seedPipeSlot(child.memory, child.kernelState, 6, 1, 1);
  seedPipeSlot(parent.memory, parent.kernelState, 6, 1, 1);
  networkConnections(parent).set(9, {
    close: vi.fn(async () => {}),
    read: vi.fn(),
    write: vi.fn(),
  } as never);

  await syncForkSharedStateViaKernelWorker(child, [parent], {
    cause: { kind: "syscall", nr: -1n, name: "test-fork-shared-sync" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: true,
    sourceVmPtr: child.vmPtr,
  });

  expect(processKernelWorkerRequest(child)).toHaveBeenCalledWith(expect.objectContaining({
    type: "sync-fork-shared-state",
    targetPids: [3],
  }));
});

test("fork-shared sync preserves epoll watched fd ownership and local epoll state", async () => {
  const child = makeHandle(7, new Map([[4, 10], [11, 12]]));
  const parent = makeHandle(3, new Map([[13, 12]]));
  addKernelShareMember(child, parent);
  setProcessKernelShareGroup(parent, processKernelShareGroup(child));
  addKernelShareSharedOfdId(child, 12);
  const originalEpollTablePtr = child.kernelState.epollTablePtr;

  seedOfd(child.memory, child.kernelState, 10, FD_TYPE_EPOLL, 0, -1);
  seedOfd(child.memory, child.kernelState, 12, FD_TYPE_PIPE, 6, -11);
  seedOfd(parent.memory, parent.kernelState, 12, FD_TYPE_PIPE, 6, 10);
  seedPipeSlot(child.memory, child.kernelState, 6, 1, 1);
  seedPipeSlot(parent.memory, parent.kernelState, 6, 1, 1);
  setProcessKernelWorkerRequest(child, vi.fn(async (msg) => ({
    type: "fork-shared-state-synced",
    requestId: msg.requestId,
    kernelState: {
      ...msg.kernelState,
      epollTablePtr: 0x350000,
      pipeUsed: msg.kernelState.pipeUsed | 2n,
    },
  })));

  await syncForkSharedStateViaKernelWorker(child, [parent], {
    cause: { kind: "syscall", nr: -1n, name: "test-fork-shared-epoll-watched-fd" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: true,
    sourceVmPtr: child.vmPtr,
  });

  expect(processKernelWorkerRequest(child)).toHaveBeenCalledWith(expect.objectContaining({
    type: "sync-fork-shared-state",
  }));
  expect(child.kernel.process_fd_ofd_id(child.vmPtr, 4)).toBe(10);
  expect(child.kernel.process_fd_ofd_id(child.vmPtr, 11)).toBe(12);
  expect(child.kernelState.epollTablePtr).toBe(originalEpollTablePtr);
});

test("pipe-control reconciliation preserves live writer when slot counters are stale closed", () => {
  const handle = makeHandle(1, new Map([[12, 12], [13, 13]]));
  addKernelShareMember(handle);
  seedOfd(handle.memory, handle.kernelState, 12, FD_TYPE_PIPE, 6, 10);
  seedOfd(handle.memory, handle.kernelState, 13, FD_TYPE_PIPE, 6, -11);
  seedPipeSlot(handle.memory, handle.kernelState, 6, 0, 0);

  reconcileKernelSharePipeControl(handle);

  const view = new DataView(handle.memory.buffer);
  const slotBase = handle.kernelState.pipeBase + 6 * PIPE_SLOT_BYTES;
  expect(view.getInt32(slotBase + 8, true)).toBe(1);
  expect(view.getInt32(slotBase + 12, true)).toBe(1);
  expect(handle.kernelState.pipeUsed & (1n << 6n)).not.toBe(0n);
});

test("runtime network OFD collection excludes unrelated local socket OFDs", () => {
  const handle = makeHandle(3, new Map([[3, 11], [16, 17]]));
  seedOfd(handle.memory, handle.kernelState, 11, FD_TYPE_SOCKET, 11, 30);
  seedOfd(handle.memory, handle.kernelState, 17, FD_TYPE_SOCKET, 17, 31);
  networkConnections(handle).set(3, {
    close: vi.fn(async () => {}),
    read: vi.fn(),
    write: vi.fn(),
  } as never);
  networkConnectionOfds(handle).set(3, 11);

  expect([...collectRuntimeNetworkSocketOfdIds([handle])].sort((left, right) => left - right))
    .toEqual([11]);
});

test("kernel socket fd state detection uses supervisor socket-held authority", () => {
  const handle = makeHandle(3, new Map([[16, 17]]));
  seedOfd(handle.memory, handle.kernelState, 17, FD_TYPE_SOCKET, 17, 31);
  handle.kernel.supervisor_process_socket_held_ofd = (_vmPtr, index) => (index === 0 ? 17 : 0);

  expect(hasKernelSocketFdState(handle)).toBe(true);
});

test("fork-shared delta skips regular file OFDs held only by a peer", async () => {
  const child = makeHandle(7, new Map());
  const parent = makeHandle(3, new Map([[4, 12]]));
  addKernelShareMember(child, parent);
  setProcessKernelShareGroup(parent, processKernelShareGroup(child));
  addKernelShareSharedOfdId(child, 12);

  seedOfd(child.memory, child.kernelState, 12, FD_TYPE_FILE, 44, 21);
  seedOfd(parent.memory, parent.kernelState, 12, FD_TYPE_FILE, 44, 21);

  await syncForkSharedStateViaKernelWorker(child, [parent], {
    cause: { kind: "syscall", nr: -1n, name: "test-fork-shared-regular-file-delta" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: false,
    sourceVmPtr: child.vmPtr,
  });

  expect(processKernelWorkerRequest(child)).not.toHaveBeenCalled();
});

test("fork-shared delta prunes stale shared OFDs when no sync is needed", async () => {
  const child = makeHandle(7, new Map());
  const parent = makeHandle(3, new Map());
  addKernelShareMember(child, parent);
  setProcessKernelShareGroup(parent, processKernelShareGroup(child));
  addKernelShareSharedOfdId(child, 12);

  seedOfd(child.memory, child.kernelState, 12, FD_TYPE_PIPE, 4, -1);
  seedOfd(parent.memory, parent.kernelState, 12, FD_TYPE_PIPE, 4, -1);

  await syncForkSharedStateViaKernelWorker(child, [parent], {
    cause: { kind: "syscall", nr: -1n, name: "test-fork-shared-stale-prune" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: true,
    sourceVmPtr: child.vmPtr,
  });

  expect(kernelShareHasSharedOfdId(child, 12)).toBe(false);
  expect(processKernelWorkerRequest(child)).not.toHaveBeenCalled();
});

test("fork-shared tracking prunes regular file OFDs without a peer", async () => {
  const child = makeHandle(7, new Map([[3, 12]]));
  const parent = makeHandle(3, new Map());
  addKernelShareMember(child, parent);
  setProcessKernelShareGroup(parent, processKernelShareGroup(child));
  addKernelShareSharedOfdId(child, 12);

  seedOfd(child.memory, child.kernelState, 12, FD_TYPE_FILE, 44, 21);

  await syncForkSharedStateViaKernelWorker(child, [parent], {
    cause: { kind: "syscall", nr: -1n, name: "test-fork-shared-unpeered-regular-file-prune" },
    deltaOfdIds: true,
    includeMemfs: false,
    trackHeldOfds: true,
    sourceVmPtr: child.vmPtr,
  });

  expect(kernelShareHasSharedOfdId(child, 12)).toBe(false);
  expect(processKernelWorkerRequest(child)).not.toHaveBeenCalled();
});

test("blocked fork-shared sync clears a stale active sync before waiting", async () => {
  const handle = makeHandle(7, new Map());
  const staleSync = new Promise<void>(() => {});
  registerForkSharedSyncPromise(staleSync, {
    ownerPid: handle.pid,
    sourceVmPtr: 0xdead,
    reason: "unit-stale-sync",
    targetPids: [],
  });
  handle.kernel.thread_process_ptr = vi.fn((vmPtr: number) => (vmPtr === 0xdead ? 0 : 0x1000));
  setActiveForkSharedSync(handle, staleSync);

  const result = await Promise.race([
    syncBlockedForkSharedStateViaKernelWorker(
      handle,
      handle.vmPtr,
      { kind: "blocked-rpc", status: 21 },
    ).then(() => "done"),
    new Promise<"timed-out">((resolve) => setTimeout(() => resolve("timed-out"), 25)),
  ]);

  expect(result).toBe("done");
  expect(getHandleForkSharedSync(handle)).toBeNull();
  expect(getProcessKernelShareForkSharedSync(handle as never)).toBeNull();
});

test("blocked fork-shared sync registers its active gate for stale-source clearing", async () => {
  const handle = makeHandle(7, new Map([[3, 12]]));
  const peer = makeHandle(3, new Map([[4, 13]]));
  addKernelShareMember(handle, peer);
  setProcessKernelShareGroup(peer, processKernelShareGroup(handle));
  seedOfd(handle.memory, handle.kernelState, 12, FD_TYPE_PIPE, 4, 10);
  seedOfd(peer.memory, peer.kernelState, 13, FD_TYPE_PIPE, 4, -11);
  addKernelShareSharedOfdId(handle, 12);
  addKernelShareSharedOfdId(handle, 13);
  let syncRequests = 0;
  setProcessKernelWorkerRequest(handle, vi.fn(async (msg) => {
    if (msg.type !== "sync-fork-shared-state") {
      return {
        type: "fork-shared-state-rehydrated",
        requestId: msg.requestId,
      };
    }
    syncRequests += 1;
    if (syncRequests === 1) {
      return new Promise<never>(() => {});
    }
    return {
      type: "fork-shared-state-synced",
      requestId: msg.requestId,
      kernelState: msg.kernelState,
    };
  }));

  void syncBlockedForkSharedStateViaKernelWorker(
    handle,
    handle.vmPtr,
    { kind: "blocked-rpc", status: 21 },
  );
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(syncRequests).toBe(1);

  handle.kernel.thread_process_ptr = vi.fn((vmPtr: number) => (vmPtr === handle.vmPtr ? 0 : 0x1000));
  const result = await Promise.race([
    syncBlockedForkSharedStateViaKernelWorker(
      handle,
      handle.vmPtr,
      { kind: "blocked-rpc", status: 21 },
    ).then(() => "done"),
    new Promise<"timed-out">((resolve) => setTimeout(() => resolve("timed-out"), 25)),
  ]);

  expect(result).toBe("done");
  expect(syncRequests).toBe(2);
});
