import { attachKernelRuntimeState, readKernelRuntimeState, type KernelExports, type KernelRuntimeState } from "../abi.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPipeSlotSnapshot,
} from "../messages.js";
import { resetPageCache } from "../fs/sab-page-cache.js";
import {
  restoreThreadWorkerPrepareMemory,
  snapshotThreadWorkerPrepareMemory,
} from "../thread-worker/prepare-memory.js";
import { ensureMemoryCapacityFor, scratchBaseFor } from "./common.js";
import { createKernelEnv } from "../wasm/env.js";
import {
  configureKernelInstanceStack,
  KERNEL_WORKER_STACK_SLOT,
} from "../wasm/stack.js";

interface CanonicalProcessEntry {
  pid: number;
  ppid: number;
  pgid: number;
  sid: number;
  didExec: boolean;
  vmPtr: number;
}

interface ProcessFsResult {
  kernelState: KernelRuntimeState;
  result: bigint;
  memfsMutated: boolean;
  ofdStateChanged: boolean;
}

interface PendingFsMutationContext {
  effects: number;
  pendingPath: string | null;
  pendingSecondaryPath: string | null;
  fileDataTargetIno: number;
}

interface AppliedFsMutationResult {
  memfsMutated: boolean;
  ofdStateChanged: boolean;
  needsFullSync: boolean;
  preservePublishedRegularFileData: boolean;
}

interface KernelWorkerProcessControlDeps {
  getCanonicalExports(): KernelExports;
  setCanonicalExports(nextExports: KernelExports): void;
  getWasmModule(): WebAssembly.Module | null;
  setWasmModule(module: WebAssembly.Module | null): void;
  getSabPageCache(): SharedArrayBuffer;
  setSabPageCache(pageCache: SharedArrayBuffer): void;
  processKernels: Map<number, KernelExports>;
  lastProcessPipeSlotModes: Map<number, Map<number, number>>;
  canonicalProcesses: Map<number, CanonicalProcessEntry>;
  seedEntries: Map<string, FsSnapshotEntry>;
  getCanonicalNextPid(): number;
  setCanonicalNextPid(nextPid: number): void;
  publishRootInode(mode?: number): void;
  copyOfdState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void;
  copyMemfsState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    options?: { includeData?: boolean },
  ): void;
  rebuildPageCacheFromExports(sourceExports: KernelExports): void;
  rebuildPageCacheFromCanonical(): void;
  restorePublishedRegularFileDataFromPageCache?(targetExports?: KernelExports): void;
  syncPipeState(
    pid: number,
    kernelState: KernelRuntimeState,
    fdEntries?: KernelWorkerFdEntrySnapshot[],
    ofdSlots?: KernelWorkerOfdSlotSnapshot[],
    pipeSlots?: KernelWorkerPipeSlotSnapshot[],
    vmPtrOverride?: number,
  ): { kernelState: KernelRuntimeState; pipeSlots: KernelWorkerPipeSlotSnapshot[] };
  syncEpollState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState;
  syncSocketState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState;
  copyCanonicalMemfsToProcess(
    canonicalState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    options?: { includeData?: boolean },
  ): void;
  snapshotProcSelfExe(targetExports: KernelExports): Uint8Array | null;
  restoreProcSelfExe(targetExports: KernelExports, target: Uint8Array | null): void;
  composeProcessPipeUsed(processExports: KernelExports, vmPtr: number, sharedPipeUsed: bigint): bigint;
  collectProcessHeldEpollOfdIds(processExports: KernelExports, vmPtr: number): number[];
  collectProcessHeldPipeSlots(processExports: KernelExports, vmPtr: number): Set<number>;
  mergePipeSlotsFromCanonical(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    targetVmPtr: number,
    pipeSlots: Iterable<number>,
  ): void;
  recomputeCanonicalPipeControlState(): {
    kernelState: KernelRuntimeState;
    readOpen: Int32Array;
    writeOpen: Int32Array;
    managedPipeSlots?: number[];
  };
  applyPipeControlState(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    readOpen: Int32Array,
    writeOpen: Int32Array,
    pipeUsed: bigint,
    managedPipeSlots: Iterable<number>,
  ): void;
  copyEpollState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void;
  copySelectedEpollState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    epollOfdIds: Iterable<number>,
  ): void;
  copySocketState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void;
  syncFileDataMutation(sourceExports: KernelExports, ino: number): boolean;
  syncFileDataMutationByPath(sourceExports: KernelExports, path: string): boolean;
  syncMetadataMutation(sourceExports: KernelExports, path: string): boolean;
  fanoutCanonicalMemfs(includeData?: boolean): void;
  fsEffectMemfsMutation: number;
  fsEffectSharedOfdState: number;
  fsEffectMemfsFileData: number;
  fsOpOpenAt: number;
  fsOpMkdir: number;
  fsOpUnlink: number;
}

export function createKernelWorkerProcessControl(deps: KernelWorkerProcessControlDeps) {
  const decoder = new TextDecoder();

  function decodeKernelBytes(memory: WebAssembly.Memory, ptr: number, length: number): string {
    const bytes = new Uint8Array(length);
    bytes.set(new Uint8Array(memory.buffer, ptr, length));
    return decoder.decode(bytes);
  }

  function readPendingFsPath(processExports: KernelExports, vmPtr: number, secondary = false): string | null {
    const absPathPtr = scratchBaseFor(processExports) + 0x8000 + (secondary ? 4096 : 0);
    ensureMemoryCapacityFor(processExports.memory, absPathPtr + 4096);
    const absPathLen = Number(
      (secondary
        ? processExports.thread_pending_fs_secondary_absolute_path?.(vmPtr, absPathPtr, 4096)
        : processExports.thread_pending_fs_absolute_path?.(vmPtr, absPathPtr, 4096)) ?? 0n,
    );
    if (absPathLen > 0 && absPathPtr + absPathLen <= processExports.memory.buffer.byteLength) {
      return decodeKernelBytes(processExports.memory, absPathPtr, absPathLen);
    }
    if (secondary) {
      return null;
    }
    const pathPtr = processExports.thread_pending_fs_path_ptr?.(vmPtr) ?? 0;
    const pathLen = processExports.thread_pending_fs_path_len?.(vmPtr) ?? 0;
    const guestBase = processExports.thread_guest_base?.(vmPtr) ?? 0;
    if (pathPtr === 0 || pathLen === 0 || guestBase === 0) {
      return null;
    }
    const linearPtr = guestBase + pathPtr;
    if (linearPtr + pathLen > processExports.memory.buffer.byteLength) {
      return null;
    }
    return decodeKernelBytes(processExports.memory, linearPtr, pathLen);
  }

  function fallbackToFullMemfsSync(
    processExports: KernelExports,
    canonicalExports: KernelExports,
    preservePublishedRegularFileData = false,
  ): void {
    const sourceState = readKernelRuntimeState(processExports);
    const canonicalState = readKernelRuntimeState(canonicalExports);
    const canonicalProcSelfExe = deps.snapshotProcSelfExe(canonicalExports);
    deps.copyMemfsState(processExports, sourceState, canonicalExports, canonicalState, {
      // A fallback full sync must preserve regular-file bodies. Tree-only
      // metadata copies can reconstruct directory shape, but they also turn
      // unrelated executables into zero-filled stubs once canonical state is
      // rebuilt and fanned out.
      includeData: true,
    });
    deps.restoreProcSelfExe(canonicalExports, canonicalProcSelfExe);
    if (preservePublishedRegularFileData) {
      deps.restorePublishedRegularFileDataFromPageCache?.();
    }
    deps.rebuildPageCacheFromCanonical();
    deps.fanoutCanonicalMemfs();
  }

  function readPendingFsMutationContext(
    processExports: KernelExports,
    vmPtr: number,
  ): PendingFsMutationContext {
    const effects = processExports.thread_pending_fs_effects(vmPtr);
    return {
      effects,
      pendingPath: (effects & (deps.fsEffectMemfsMutation | deps.fsEffectMemfsFileData)) !== 0
        ? readPendingFsPath(processExports, vmPtr)
        : null,
      pendingSecondaryPath: (effects & deps.fsEffectMemfsMutation) !== 0
        ? readPendingFsPath(processExports, vmPtr, true)
        : null,
      fileDataTargetIno: (effects & deps.fsEffectMemfsFileData) !== 0
        ? processExports.thread_pending_fs_target_ino(vmPtr)
        : 0,
    };
  }

  function applyPendingFsMutation(
    processExports: KernelExports,
    context: PendingFsMutationContext,
    fsSucceeded: boolean,
  ): AppliedFsMutationResult {
    const treeMutated = fsSucceeded && (context.effects & deps.fsEffectMemfsMutation) !== 0;
    const fileDataMutated = fsSucceeded && (context.effects & deps.fsEffectMemfsFileData) !== 0;
    let handledFileDataMutation = false;
    let handledMetadataMutation = false;

    if (fileDataMutated && context.fileDataTargetIno !== 0) {
      handledFileDataMutation = deps.syncFileDataMutation(processExports, context.fileDataTargetIno);
    } else if (fileDataMutated && context.pendingPath) {
      handledFileDataMutation = deps.syncFileDataMutationByPath(processExports, context.pendingPath);
    }

    if (treeMutated && context.pendingPath) {
      handledMetadataMutation = deps.syncMetadataMutation(processExports, context.pendingPath);
      if (
        handledMetadataMutation
        && context.pendingSecondaryPath
        && context.pendingSecondaryPath !== context.pendingPath
      ) {
        handledMetadataMutation = deps.syncMetadataMutation(processExports, context.pendingSecondaryPath);
      }
    }

    const needsMetadataFullSync = treeMutated && !handledMetadataMutation;
    const needsFileDataFullSync = fileDataMutated && !handledFileDataMutation;

    return {
      memfsMutated: treeMutated || fileDataMutated,
      ofdStateChanged: fsSucceeded && (context.effects & deps.fsEffectSharedOfdState) !== 0,
      needsFullSync: needsMetadataFullSync || needsFileDataFullSync,
      preservePublishedRegularFileData: needsMetadataFullSync && !needsFileDataFullSync,
    };
  }

  async function initKernel(
    module: WebAssembly.Module,
    pageCache: SharedArrayBuffer,
    maxMemoryPages = 32768,
  ): Promise<KernelRuntimeState> {
    deps.setWasmModule(module);
    deps.setSabPageCache(pageCache);

    const boundedMaxMemoryPages = Math.max(maxMemoryPages, 1024);
    const privateMemory = new WebAssembly.Memory({
      initial: 1024,
      maximum: boundedMaxMemoryPages,
      shared: true,
    });
    const instance = await WebAssembly.instantiate(module, {
      env: createKernelEnv(privateMemory, { sabPageCache: pageCache }),
    });
    const nextExports = instance.exports as unknown as KernelExports;
    configureKernelInstanceStack(nextExports, KERNEL_WORKER_STACK_SLOT);
    deps.setCanonicalExports(nextExports);

    const wasmHeapBase = nextExports.__heap_base?.value ?? 0x200000;
    nextExports.kernel_init(wasmHeapBase);

    const now = Date.now();
    nextExports.set_base_time(
      BigInt(Math.floor(now / 1000)),
      BigInt((now % 1000) * 1_000_000),
    );

    deps.seedEntries.clear();
    deps.seedEntries.set("/", { kind: "dir", path: "/", mode: 0o755 });
    resetPageCache(pageCache);
    deps.publishRootInode();
    deps.setCanonicalNextPid(3);
    deps.canonicalProcesses.clear();
    deps.canonicalProcesses.set(1, {
      pid: 1,
      ppid: 0,
      pgid: 1,
      sid: 1,
      didExec: false,
      vmPtr: 0,
    });

    return readKernelRuntimeState(nextExports);
  }

  async function registerProcess(
    pid: number,
    vmPtr: number,
    memory: WebAssembly.Memory,
    kernelState: KernelRuntimeState,
    seedMemfsMode: "full" | "metadata" | "none" = "full",
    pipeSeedMode: "canonical" | "process" = "canonical",
  ): Promise<KernelRuntimeState> {
    const wasmModule = deps.getWasmModule();
    if (wasmModule === null) {
      throw new Error("kernel worker is not initialized");
    }
    const canonicalExports = deps.getCanonicalExports();
    const memorySnapshot = snapshotThreadWorkerPrepareMemory(
      memory,
      null,
      kernelState,
      kernelState.heapBase,
      { includeMemfsData: seedMemfsMode === "none" },
    );
    const instance = await WebAssembly.instantiate(wasmModule, {
      env: createKernelEnv(memory, { sabPageCache: deps.getSabPageCache() }),
    });
    // Registering a process must not mutate the caller-owned shared memory.
    // Browsers can replay data segments on instantiation. Restore the live
    // thread/process regions that sit outside the static kernel image instead
    // of snapshotting the entire shared linear memory.
    restoreThreadWorkerPrepareMemory(memory, memorySnapshot);
    const processExports = instance.exports as unknown as KernelExports;
    configureKernelInstanceStack(processExports, KERNEL_WORKER_STACK_SLOT);
    attachKernelRuntimeState(processExports, kernelState);
    deps.processKernels.set(pid, processExports);
    const existingProcess = deps.canonicalProcesses.get(pid);
    deps.canonicalProcesses.set(pid, {
      pid,
      ppid: existingProcess?.ppid ?? 0,
      pgid: existingProcess?.pgid ?? pid,
      sid: existingProcess?.sid ?? pid,
      didExec: existingProcess?.didExec ?? false,
      vmPtr,
    });
    deps.setCanonicalNextPid(Math.max(deps.getCanonicalNextPid(), pid + 1));
    if (deps.processKernels.size === 1) {
      const sourceState = readKernelRuntimeState(processExports);
      const canonicalState = readKernelRuntimeState(canonicalExports);
      const canonicalHasSeedSnapshot = deps.seedEntries.size > 1;
      deps.copyOfdState(processExports, sourceState, canonicalExports, canonicalState);
      if (seedMemfsMode === "full" && !canonicalHasSeedSnapshot) {
        deps.copyMemfsState(processExports, sourceState, canonicalExports, canonicalState, {
          includeData: false,
        });
        deps.rebuildPageCacheFromExports(processExports);
      }
      const pipeState = deps.syncPipeState(pid, readKernelRuntimeState(processExports)).kernelState;
      const epollState = deps.syncEpollState(pid, pipeState);
      const socketState = deps.syncSocketState(pid, epollState);
      if (seedMemfsMode !== "none") {
        const memberState = readKernelRuntimeState(processExports);
        const updatedCanonicalState = readKernelRuntimeState(canonicalExports);
        deps.copyCanonicalMemfsToProcess(updatedCanonicalState, processExports, memberState, {
          includeData: seedMemfsMode === "full",
        });
      }
      return readKernelRuntimeState(processExports) ?? socketState;
    }

    const canonicalState = readKernelRuntimeState(canonicalExports);
    const processState = readKernelRuntimeState(processExports);
    if (pipeSeedMode === "process") {
      deps.syncPipeState(pid, processState);
    } else {
      attachKernelRuntimeState(processExports, {
        ...processState,
        pipeUsed: deps.composeProcessPipeUsed(processExports, vmPtr, canonicalState.pipeUsed),
        socketUsed: canonicalState.socketUsed,
        netBufUsed: canonicalState.netBufUsed,
      });
      const registeredProcessState = readKernelRuntimeState(processExports);
      const sharedPipeSlots = deps.collectProcessHeldPipeSlots(processExports, vmPtr);
      deps.mergePipeSlotsFromCanonical(
        canonicalExports,
        canonicalState,
        processExports,
        registeredProcessState,
        vmPtr,
        sharedPipeSlots,
      );
      const { readOpen, writeOpen } = deps.recomputeCanonicalPipeControlState();
      deps.applyPipeControlState(
        processExports,
        registeredProcessState,
        readOpen,
        writeOpen,
        deps.composeProcessPipeUsed(processExports, vmPtr, readKernelRuntimeState(canonicalExports).pipeUsed),
        sharedPipeSlots,
      );
    }
    const registeredCanonicalState = readKernelRuntimeState(canonicalExports);
    const registeredProcessState = readKernelRuntimeState(processExports);
    if (seedMemfsMode !== "none") {
      deps.copyCanonicalMemfsToProcess(registeredCanonicalState, processExports, registeredProcessState, {
        includeData: seedMemfsMode === "full",
      });
    }
    deps.copySelectedEpollState(
      canonicalExports,
      registeredCanonicalState,
      processExports,
      registeredProcessState,
      deps.collectProcessHeldEpollOfdIds(processExports, vmPtr),
    );
    deps.copySocketState(canonicalExports, registeredCanonicalState, processExports, registeredProcessState);
    return readKernelRuntimeState(processExports);
  }

  function unregisterProcess(pid: number): void {
    deps.processKernels.delete(pid);
    deps.lastProcessPipeSlotModes.delete(pid);
    deps.canonicalProcesses.delete(pid);
    for (const processExports of deps.processKernels.values()) {
      processExports.process_remove_process_entry(pid);
    }
    const {
      kernelState: canonicalState,
      readOpen,
      writeOpen,
      managedPipeSlots = [],
    } = deps.recomputeCanonicalPipeControlState();
    for (const [memberPid, memberExports] of deps.processKernels) {
      const memberProcess = deps.canonicalProcesses.get(memberPid);
      const memberState = readKernelRuntimeState(memberExports);
      const memberPipeUsed = memberProcess?.vmPtr
        ? deps.composeProcessPipeUsed(memberExports, memberProcess.vmPtr, canonicalState.pipeUsed)
        : canonicalState.pipeUsed;
      deps.applyPipeControlState(
        memberExports,
        memberState,
        readOpen,
        writeOpen,
        memberPipeUsed,
        managedPipeSlots,
      );
    }
  }

  function debugProcesses(): { pids: number[]; canonicalPids: number[] } {
    return {
      pids: [...deps.processKernels.keys()].sort((left, right) => left - right),
      canonicalPids: [...deps.canonicalProcesses.keys()].sort((left, right) => left - right),
    };
  }

  function requireProcessKernel(pid: number): KernelExports {
    const processExports = deps.processKernels.get(pid);
    if (!processExports) {
      throw new Error(`process ${pid} is not registered`);
    }
    return processExports;
  }

  function processFs(
    pid: number,
    vmPtr: number,
    kernelState: KernelRuntimeState,
  ): ProcessFsResult {
    const canonicalExports = deps.getCanonicalExports();
    const processExports = requireProcessKernel(pid);
    attachKernelRuntimeState(processExports, kernelState);
    const pendingContext = readPendingFsMutationContext(processExports, vmPtr);
    processExports.thread_process_fs(vmPtr);
    const result = BigInt(processExports.thread_resume_fs(vmPtr));
    const fsSucceeded = Number(BigInt.asIntN(64, result)) >= 0;
    const mutation = applyPendingFsMutation(processExports, pendingContext, fsSucceeded);
    if (mutation.needsFullSync) {
      fallbackToFullMemfsSync(
        processExports,
        canonicalExports,
        mutation.preservePublishedRegularFileData,
      );
    }
    return {
      kernelState: readKernelRuntimeState(processExports),
      result,
      memfsMutated: mutation.memfsMutated,
      ofdStateChanged: mutation.ofdStateChanged,
    };
  }

  return {
    debugProcesses,
    initKernel,
    processFs,
    registerProcess,
    requireProcessKernel,
    unregisterProcess,
  };
}
