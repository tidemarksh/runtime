/**
 * Dedicated kernel-worker RPC endpoint.
 *
 * Kernel-worker-owned FS state is canonical here, and registered process
 * memories are rehydrated from this worker for MemFS/control-plane ownership
 * and fork-shared OFD/MemFS fanout. The current kernel still runs per-process
 * instances, so this remains a staged ownership boundary rather than a single
 * in-worker kernel instance.
 */

import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  type KernelExports,
  type KernelRuntimeState,
} from "./abi.js";
import type { FsSnapshotEntry } from "./fs/types.js";
import {
  OFF_DATA_USED,
  OFF_DIR_EPOCH,
  OFF_INODE_COUNT,
  readInodeRecord,
} from "./fs/sab-page-cache.js";
import type {
  KernelWorkerInbound,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerOutbound,
  KernelWorkerPipeSlotSnapshot,
  KernelWorkerSocketStateSnapshot,
} from "./messages.js";
import { bindWorkerMessageHandler, workerPostMessage } from "./bridge/scope.js";
import { createKernelWorkerFsState } from "./kernel-worker/fs-state.js";
import { createKernelWorkerFsUtils } from "./kernel-worker/fs-utils.js";
import { createKernelWorkerLifecycle } from "./kernel-worker/lifecycle.js";
import { createKernelWorkerMessageHandler } from "./kernel-worker/message-handler.js";
import { createKernelWorkerProcessControl } from "./kernel-worker/process-control.js";
import { createKernelWorkerPrimitives } from "./kernel-worker/primitives.js";
import { createKernelWorkerSharedStateSync } from "./kernel-worker/shared-state-sync.js";

let exports: KernelExports;
let wasmModule: WebAssembly.Module | null = null;
let sabPageCache: SharedArrayBuffer;
const processKernels = new Map<number, KernelExports>();
const lastProcessPipeSlotModes = new Map<number, Map<number, number>>();
let canonicalNextPid = 3;
const canonicalProcesses = new Map<number, {
  pid: number;
  ppid: number;
  pgid: number;
  sid: number;
  didExec: boolean;
  vmPtr: number;
}>();

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const seedEntries = new Map<string, FsSnapshotEntry>();
const FS_EFFECT_MEMFS_MUTATION = 1 << 0;
const FS_EFFECT_SHARED_OFD_STATE = 1 << 1;
const FS_EFFECT_MEMFS_FILE_DATA = 1 << 2;
const FS_OP_OPENAT = 3;
const FS_OP_MKDIR = 6;
const FS_OP_UNLINK = 7;

function postResponse(msg: KernelWorkerOutbound): void {
  workerPostMessage(msg);
}

const kernelWorkerPrimitives = createKernelWorkerPrimitives({
  getCanonicalExports: () => exports,
  processKernels,
  canonicalProcesses,
  getCanonicalProcessVmPtr: (pid) => canonicalProcesses.get(pid)?.vmPtr ?? 0,
});

const {
  applyPipeControlState,
  applySelectedOfdSnapshots,
  applySelectedPipeSlotSnapshots,
  clearMemfsState,
  clearSelectedOfdState,
  clearSelectedPipeSlots,
  collectCanonicalManagedPipeSlots,
  collectProcessHeldEpollOfdIds,
  collectPipeSlotsForOfdIds,
  collectProcessHeldOfdRefcounts,
  collectProcessHeldPipeSlots,
  collectProcessPrivatePipeSourceModes,
  collectProcessPipeSlotModes,
  collectProcessPrivatePipeSlots,
  composeProcessPipeUsed,
  copyEpollState,
  copySelectedEpollState,
  copyMemfsState,
  copyOfdState,
  copySelectedOfdState,
  copySelectedPipeSlots,
  copySocketState,
  mergePipeSlotsFromCanonical,
  mergePipeSlotsFromProcess,
  recomputeCanonicalPipeControlState,
  reconcileCanonicalOfdRefcounts,
} = kernelWorkerPrimitives;

const kernelWorkerFsUtils = createKernelWorkerFsUtils({
  getCanonicalExports: () => exports,
  getSabPageCache: () => sabPageCache,
  processKernels,
  seedEntries,
  encoder,
  decoder,
  copyMemfsState,
});

const {
  copyCanonicalMemfsToProcess,
  fanoutCanonicalMemfs,
  formatProcessDebug,
  publishRootInode,
  resolvePathIn,
  restoreProcSelfExe,
  snapshotProcSelfExe,
} = kernelWorkerFsUtils;

const kernelWorkerFsState = createKernelWorkerFsState({
  getCanonicalExports: () => exports,
  getSabPageCache: () => sabPageCache,
  processKernels,
  seedEntries,
  encoder,
  decoder,
  fanoutCanonicalMemfs,
  publishRootInode,
  resolvePathIn: kernelWorkerFsUtils.resolvePathIn,
  upsertDirSeed: kernelWorkerFsUtils.upsertDirSeed,
  upsertFileSeed: kernelWorkerFsUtils.upsertFileSeed,
  upsertSymlinkSeed: kernelWorkerFsUtils.upsertSymlinkSeed,
});

const {
  loadFsSnapshot,
  mkdir,
  readFile,
  readlink,
  readdir,
  rebuildPageCacheFromExports,
  rebuildPageCacheFromCanonical,
  restorePublishedRegularFileDataFromPageCache,
  snapshotCanonicalFs,
  statPath,
  syncFileDataMutation,
  syncFileDataMutationByPath,
  syncMetadataMutation,
  symlink,
  writeFile,
} = kernelWorkerFsState;

let requireProcessKernelImpl: (pid: number) => KernelExports;
function requireProcessKernel(pid: number): KernelExports {
  return requireProcessKernelImpl(pid);
}

const kernelWorkerLifecycle = createKernelWorkerLifecycle({
  processKernels,
  canonicalProcesses,
  getCanonicalNextPid: () => canonicalNextPid,
  setCanonicalNextPid: (nextPid) => {
    canonicalNextPid = nextPid;
  },
  requireProcessKernel,
  formatProcessDebug,
});

const {
  canonicalizeChildPid,
  initializeProcessLifecycle,
  recordChildExit,
  resumeBlockedSyscall,
  resumeConnect,
  resumeVforkExecveParent,
  resumeVforkParent,
  resumeWait4,
  syncProcessLifecycle,
} = kernelWorkerLifecycle;

let syncPipeStateImpl: (
  pid: number,
  kernelState: KernelRuntimeState,
  fdEntries?: KernelWorkerFdEntrySnapshot[],
  ofdSlots?: KernelWorkerOfdSlotSnapshot[],
  pipeSlots?: KernelWorkerPipeSlotSnapshot[],
  vmPtrOverride?: number,
) => { kernelState: KernelRuntimeState; pipeSlots: KernelWorkerPipeSlotSnapshot[] };
let syncEpollStateImpl: (pid: number, kernelState: KernelRuntimeState) => KernelRuntimeState;
let syncSocketStateImpl: (pid: number, kernelState: KernelRuntimeState) => KernelRuntimeState;

function syncPipeState(
  pid: number,
  kernelState: KernelRuntimeState,
  fdEntries?: KernelWorkerFdEntrySnapshot[],
  ofdSlots?: KernelWorkerOfdSlotSnapshot[],
  pipeSlots?: KernelWorkerPipeSlotSnapshot[],
  vmPtrOverride?: number,
): { kernelState: KernelRuntimeState; pipeSlots: KernelWorkerPipeSlotSnapshot[] } {
  return syncPipeStateImpl(pid, kernelState, fdEntries, ofdSlots, pipeSlots, vmPtrOverride);
}

function syncEpollState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState {
  return syncEpollStateImpl(pid, kernelState);
}

function syncSocketState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState {
  return syncSocketStateImpl(pid, kernelState);
}

function applySocketStateSnapshot(
  pid: number,
  kernelState: KernelRuntimeState,
  snapshot: KernelWorkerSocketStateSnapshot,
): KernelRuntimeState {
  const processExports = processKernels.get(pid);
  if (!processExports) {
    return kernelState;
  }
  const state = readKernelRuntimeState(processExports);
  const memory = new Uint8Array(processExports.memory.buffer);
  const netBufPoolBytes = processExports.kernel_net_buf_pool_bytes();
  const maxBuffers = Math.max(1, Number(processExports.abi_max_sockets?.() ?? 32) * 2);
  const netBufSlotBytes = Math.floor(netBufPoolBytes / maxBuffers);
  memory.set(snapshot.socketTable, state.socketTablePtr);
  if (snapshot.netBufPool !== undefined) {
    memory.set(snapshot.netBufPool, state.netBufBase);
  } else if (netBufSlotBytes > 0) {
    for (const slot of snapshot.netBufSlots ?? []) {
      if (slot.bufferIndex < 0 || slot.bufferIndex >= maxBuffers || slot.bytes.length !== netBufSlotBytes) {
        continue;
      }
      memory.set(slot.bytes, state.netBufBase + slot.bufferIndex * netBufSlotBytes);
    }
  }
  attachKernelRuntimeState(processExports, {
    ...state,
    socketUsed: snapshot.socketUsed,
    netBufUsed: snapshot.netBufUsed,
  });
  return readKernelRuntimeState(processExports);
}

function snapshotSocketState(
  pid: number,
  kernelState: KernelRuntimeState,
): KernelWorkerSocketStateSnapshot | undefined {
  const processExports = processKernels.get(pid);
  if (!processExports) {
    return undefined;
  }
  attachKernelRuntimeState(processExports, kernelState);
  const state = readKernelRuntimeState(processExports);
  const memory = new Uint8Array(processExports.memory.buffer);
  const netBufPoolBytes = processExports.kernel_net_buf_pool_bytes();
  const maxBuffers = Math.max(1, Number(processExports.abi_max_sockets?.() ?? 32) * 2);
  const netBufSlotBytes = Math.floor(netBufPoolBytes / maxBuffers);
  const netBufSlots: KernelWorkerSocketStateSnapshot["netBufSlots"] = [];
  if (netBufSlotBytes > 0) {
    for (let bufferIndex = 0; bufferIndex < maxBuffers; bufferIndex += 1) {
      if ((state.netBufUsed & (1n << BigInt(bufferIndex))) === 0n) {
        continue;
      }
      const offset = state.netBufBase + bufferIndex * netBufSlotBytes;
      netBufSlots.push({
        bufferIndex,
        bytes: memory.slice(offset, offset + netBufSlotBytes),
      });
    }
  }
  return {
    socketTable: memory.slice(
      state.socketTablePtr,
      state.socketTablePtr + processExports.kernel_socket_table_bytes(),
    ),
    netBufSlots,
    socketUsed: state.socketUsed,
    netBufUsed: state.netBufUsed,
  };
}

function syncProcessMemfs(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState {
  const canonicalExports = exports;
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);
  const canonicalState = readKernelRuntimeState(canonicalExports);
  const processState = readKernelRuntimeState(processExports);
  copyCanonicalMemfsToProcess(canonicalState, processExports, processState, {
    includeData: true,
  });
  restorePublishedRegularFileDataFromPageCache(processExports);
  return readKernelRuntimeState(processExports);
}

const kernelWorkerSharedStateSync = createKernelWorkerSharedStateSync({
  getCanonicalExports: () => exports,
  getCanonicalProcessVmPtr: (pid) => canonicalProcesses.get(pid)?.vmPtr ?? 0,
  processKernels,
  lastProcessPipeSlotModes,
  requireProcessKernel,
  applySelectedOfdSnapshots,
  collectProcessHeldOfdRefcounts,
  collectProcessHeldEpollOfdIds,
  collectProcessHeldPipeSlots,
  collectProcessPrivatePipeSourceModes,
  collectProcessPipeSlotModes,
  collectProcessPrivatePipeSlots,
  collectCanonicalManagedPipeSlots,
  copySelectedOfdState,
  copySelectedEpollState,
  clearSelectedOfdState,
  copyMemfsState,
  restorePublishedRegularFileDataFromPageCache,
  clearMemfsState,
  reconcileCanonicalOfdRefcounts,
  recomputeCanonicalPipeControlState,
  collectPipeSlotsForOfdIds,
  copySelectedPipeSlots,
  clearSelectedPipeSlots,
  applySelectedPipeSlotSnapshots,
  mergePipeSlotsFromProcess,
  mergePipeSlotsFromCanonical,
  applyPipeControlState,
  composeProcessPipeUsed,
  copyCanonicalMemfsToProcess,
  snapshotProcSelfExe,
  restoreProcSelfExe,
  copyEpollState,
  copySocketState,
});

const {
  rehydrateForkSharedState,
  releaseHostStdio,
  syncEpollState: nextSyncEpollState,
  syncForkSharedState,
  syncOfdState,
  syncPipeState: nextSyncPipeState,
  syncSocketState: nextSyncSocketState,
} = kernelWorkerSharedStateSync;

syncPipeStateImpl = nextSyncPipeState;
syncEpollStateImpl = nextSyncEpollState;
syncSocketStateImpl = nextSyncSocketState;

const kernelWorkerProcessControl = createKernelWorkerProcessControl({
  getCanonicalExports: () => exports,
  setCanonicalExports: (nextExports) => {
    exports = nextExports;
  },
  getWasmModule: () => wasmModule,
  setWasmModule: (module) => {
    wasmModule = module;
  },
  getSabPageCache: () => sabPageCache,
  setSabPageCache: (pageCache) => {
    sabPageCache = pageCache;
  },
  processKernels,
  lastProcessPipeSlotModes,
  canonicalProcesses,
  seedEntries,
  encoder,
  getCanonicalNextPid: () => canonicalNextPid,
  setCanonicalNextPid: (nextPid) => {
    canonicalNextPid = nextPid;
  },
  publishRootInode,
  copyOfdState,
  copyMemfsState,
  rebuildPageCacheFromExports,
  rebuildPageCacheFromCanonical,
  restorePublishedRegularFileDataFromPageCache,
  syncPipeState,
  syncEpollState,
  syncSocketState,
  copyCanonicalMemfsToProcess,
  snapshotProcSelfExe,
  restoreProcSelfExe,
  composeProcessPipeUsed,
  collectProcessHeldEpollOfdIds,
  collectProcessHeldPipeSlots,
  mergePipeSlotsFromCanonical,
  recomputeCanonicalPipeControlState,
  applyPipeControlState,
  copyEpollState,
  copySelectedEpollState,
  copySocketState,
  syncFileDataMutation,
  syncFileDataMutationByPath,
  syncMetadataMutation,
  fanoutCanonicalMemfs,
  fsEffectMemfsMutation: FS_EFFECT_MEMFS_MUTATION,
  fsEffectSharedOfdState: FS_EFFECT_SHARED_OFD_STATE,
  fsEffectMemfsFileData: FS_EFFECT_MEMFS_FILE_DATA,
  fsOpOpenAt: FS_OP_OPENAT,
  fsOpMkdir: FS_OP_MKDIR,
  fsOpUnlink: FS_OP_UNLINK,
});

const {
  debugProcesses,
  initKernel,
  processFs,
  registerProcess,
  requireProcessKernel: nextRequireProcessKernel,
  unregisterProcess,
} = kernelWorkerProcessControl;

requireProcessKernelImpl = nextRequireProcessKernel;

const handleKernelWorkerMessage = createKernelWorkerMessageHandler({
  postResponse,
  initKernel,
  writeFile,
  writeFiles: (entries) => {
    for (const entry of entries) {
      writeFile(entry.path, entry.data, entry.mode, false);
    }
    fanoutCanonicalMemfs();
  },
  applyFileLayer: (entries) => {
    for (const entry of entries) {
      if (entry.kind === "dir") {
        mkdir(entry.path, entry.mode ?? 0o755, false);
        continue;
      }
      if (entry.kind === "file") {
        writeFile(entry.path, entry.data, entry.mode, false);
        continue;
      }
      symlink(entry.path, entry.target, entry.mode, false);
    }
    fanoutCanonicalMemfs();
  },
  symlink,
  readFile,
  readlink,
  mkdir,
  statPath,
  readdir,
  snapshotCanonicalFs,
  loadFsSnapshot,
  registerProcess,
  unregisterProcess,
  processFs,
  recordChildExit,
  resumeConnect,
  resumeWait4,
  resumeBlockedSyscall,
  resumeVforkParent,
  canonicalizeChildPid,
  initializeProcessLifecycle,
  resumeVforkExecveParent,
  syncProcessLifecycle,
  syncProcessMemfs,
  syncPipeState,
  syncEpollState,
  syncOfdState,
  syncSocketState,
  applySocketStateSnapshot,
  snapshotSocketState,
  syncForkSharedState,
  rehydrateForkSharedState,
  debugProcesses,
  debugPageCachePath: (path) => {
    const ino = resolvePathIn(exports, path);
    if (ino === 0) {
      return null;
    }
    const record = readInodeRecord(sabPageCache, ino);
    if (!record) {
      return {
        path,
        ino,
        dataOff: 0,
        size: Number(exports.memfs_file_size(ino)),
        mode: exports.memfs_inode_mode(ino),
      };
    }
    return {
      path,
      ino,
      dataOff: record.dataOff,
      size: record.size,
      mode: record.mode,
    };
  },
  debugPageCacheStats: () => {
    const u32 = new Uint32Array(sabPageCache);
    return {
      byteLength: sabPageCache.byteLength,
      dataUsed: Atomics.load(u32, OFF_DATA_USED >>> 2),
      inodeCount: Atomics.load(u32, OFF_INODE_COUNT >>> 2),
      dirEpoch: Atomics.load(u32, OFF_DIR_EPOCH >>> 2),
    };
  },
  releaseHostStdio,
});

await bindWorkerMessageHandler<KernelWorkerInbound>(async (event) => {
  await handleKernelWorkerMessage(event.data);
});
