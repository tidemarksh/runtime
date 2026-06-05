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
  type KernelExports,
  type KernelRuntimeState,
} from "./abi.js";
import type { FsSnapshotEntry } from "./fs/types.js";
import {
  debugResidentPageCountForInode,
  evictResidentPage,
  OFF_DATA_USED,
  OFF_DIR_EPOCH,
  OFF_INODE_COUNT,
  readInodeRecord,
} from "./fs/sab-page-cache.js";
import {
  FS_EFFECT_MEMFS_FILE_DATA,
  FS_EFFECT_MEMFS_MUTATION,
  FS_EFFECT_SHARED_OFD_STATE,
} from "./layout/fs-effects.js";
import {
  FS_OP_MKDIR,
  FS_OP_OPENAT,
  FS_OP_UNLINK,
} from "./layout/fs-ring.js";
import type {
  KernelWorkerInbound,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerOutbound,
  KernelWorkerPipeSlotSnapshot,
} from "./messages.js";
import { bindWorkerMessageHandler, workerPostMessage } from "./bridge/scope.js";
import { createKernelWorkerFsState } from "./kernel-worker/fs-state.js";
import { createKernelWorkerFsUtils } from "./kernel-worker/fs-utils.js";
import { createKernelWorkerLifecycle } from "./kernel-worker/lifecycle.js";
import { createKernelWorkerMessageHandler } from "./kernel-worker/message-handler.js";
import { createKernelWorkerProcessControl } from "./kernel-worker/process-control.js";
import { createKernelWorkerPrimitives } from "./kernel-worker/primitives.js";
import { createKernelWorkerSharedStateSync } from "./kernel-worker/shared-state-sync.js";
import { createKernelWorkerProcessMemfsSync } from "./kernel-worker/fs-state/process-memfs.js";
import { createKernelWorkerSocketStateBridge } from "./kernel-worker/shared-state-sync/socket-state.js";
import { createSha256VerifiedBackingSource } from "./kernel-worker/backing-source.js";
import { createNodeHostFileBackingSource } from "./kernel-worker/host-file-backing-source.js";

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
  readFileAsync,
  readlink,
  readdir,
  registerBackingSource,
  registerBackingSourceObject,
  registerBackingSourceRange,
  registerRemoteBackingSource,
  registerOpfsBackingSource,
  rebuildPageCacheFromExports,
  rebuildPageCacheFromCanonical,
  readBackingFileByInode,
  readBackingFileRangeByInode,
  truncateDirtyResidentFileByInode,
  writeDirtyResidentFileByInode,
  ensureBackingSourceRangeForInode,
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

const syncProcessMemfs = createKernelWorkerProcessMemfsSync({
  getCanonicalExports: () => exports,
  requireProcessKernel,
  copyCanonicalMemfsToProcess,
  restorePublishedRegularFileDataFromPageCache,
});

const {
  applySocketStateSnapshot,
  snapshotSocketState,
} = createKernelWorkerSocketStateBridge({
  processKernels,
});

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
  readBackingFileRangeByInode,
  truncateDirtyResidentFileByInode,
  writeDirtyResidentFileByInode,
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
  ensureBackingSourceRangeForInode,
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
  readFile: readFileAsync,
  readlink,
  mkdir,
  statPath,
  readdir,
  snapshotCanonicalFs,
  loadFsSnapshot,
  registerBackingSource,
  registerBackingSourceRange,
  registerRemoteBackingSource,
  registerOpfsBackingSource,
  registerHostFileBackingSource: async (sourceIdentity, hostPath, byteLength) => {
    registerBackingSourceObject(
      sourceIdentity,
      await createNodeHostFileBackingSource(hostPath, byteLength),
    );
  },
  registerContentAddressedBackingSource: async (sourceIdentity, digest, data) => {
    registerBackingSourceObject(
      sourceIdentity,
      await createSha256VerifiedBackingSource(data, digest),
    );
  },
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
    const backingBytes = readBackingFileByInode(ino)?.length;
    const record = readInodeRecord(sabPageCache, ino);
    if (!record) {
      return {
        path,
        ino,
        dataOff: 0,
        size: Number(exports.memfs_file_size(ino)),
        mode: exports.memfs_inode_mode(ino),
        backingBytes,
        residentPages: debugResidentPageCountForInode(sabPageCache, ino),
      };
    }
    return {
      path,
      ino,
      dataOff: record.dataOff,
      size: record.size,
      mode: record.mode,
      backingBytes,
      residentPages: debugResidentPageCountForInode(sabPageCache, ino),
    };
  },
  debugEvictPageCachePath: (path, pageIndex) => {
    const ino = resolvePathIn(exports, path);
    if (ino === 0) {
      return null;
    }
    const status = evictResidentPage(sabPageCache, ino, pageIndex);
    return {
      path,
      ino,
      pageIndex,
      status,
      residentPages: debugResidentPageCountForInode(sabPageCache, ino),
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
