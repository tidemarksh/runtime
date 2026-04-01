/**
 * Dedicated kernel-worker RPC endpoint.
 *
 * Kernel-worker-owned FS state is canonical here, and registered process
 * memories are rehydrated from this worker for MemFS/control-plane ownership
 * and fork-shared OFD/MemFS fanout. The current kernel still runs per-process
 * instances, so this remains a staged ownership boundary rather than a single
 * in-worker kernel instance.
 */

import type { KernelExports, KernelRuntimeState } from "./abi.js";
import type { FsSnapshotEntry } from "./fs/types.js";
import type {
  KernelWorkerInbound,
  KernelWorkerOutbound,
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

function postResponse(msg: KernelWorkerOutbound): void {
  workerPostMessage(msg);
}

const kernelWorkerPrimitives = createKernelWorkerPrimitives({
  getCanonicalExports: () => exports,
  processKernels,
  getCanonicalProcessVmPtr: (pid) => canonicalProcesses.get(pid)?.vmPtr ?? 0,
});

const {
  applyPipeControlState,
  applySelectedOfdSnapshots,
  clearMemfsState,
  clearSelectedOfdState,
  clearSelectedPipeSlots,
  collectCanonicalManagedPipeSlots,
  collectProcessHeldEpollOfdIds,
  collectPipeSlotsForOfdIds,
  collectProcessHeldOfdRefcounts,
  collectProcessHeldPipeSlots,
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
});

const {
  loadFsSnapshot,
  mkdir,
  readFile,
  readdir,
  rebuildPageCacheFromCanonical,
  snapshotCanonicalFs,
  statPath,
  syncFileDataMutation,
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

let syncPipeStateImpl: (pid: number, kernelState: KernelRuntimeState) => KernelRuntimeState;
let syncEpollStateImpl: (pid: number, kernelState: KernelRuntimeState) => KernelRuntimeState;
let syncSocketStateImpl: (pid: number, kernelState: KernelRuntimeState) => KernelRuntimeState;

function syncPipeState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState {
  return syncPipeStateImpl(pid, kernelState);
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
  collectProcessPipeSlotModes,
  collectProcessPrivatePipeSlots,
  collectCanonicalManagedPipeSlots,
  copySelectedOfdState,
  copySelectedEpollState,
  clearSelectedOfdState,
  copyMemfsState,
  clearMemfsState,
  reconcileCanonicalOfdRefcounts,
  recomputeCanonicalPipeControlState,
  collectPipeSlotsForOfdIds,
  copySelectedPipeSlots,
  clearSelectedPipeSlots,
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
  getCanonicalNextPid: () => canonicalNextPid,
  setCanonicalNextPid: (nextPid) => {
    canonicalNextPid = nextPid;
  },
  publishRootInode,
  copyOfdState,
  copyMemfsState,
  rebuildPageCacheFromCanonical,
  syncPipeState,
  syncEpollState,
  syncSocketState,
  copyCanonicalMemfsToProcess,
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
  fanoutCanonicalMemfs,
  fsEffectMemfsMutation: FS_EFFECT_MEMFS_MUTATION,
  fsEffectSharedOfdState: FS_EFFECT_SHARED_OFD_STATE,
  fsEffectMemfsFileData: FS_EFFECT_MEMFS_FILE_DATA,
  fsOpOpenAt: FS_OP_OPENAT,
  fsOpMkdir: FS_OP_MKDIR,
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
  readFile,
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
  syncPipeState,
  syncEpollState,
  syncOfdState,
  syncSocketState,
  syncForkSharedState,
  rehydrateForkSharedState,
  debugProcesses,
  releaseHostStdio,
});

await bindWorkerMessageHandler<KernelWorkerInbound>(async (event) => {
  await handleKernelWorkerMessage(event.data);
});
