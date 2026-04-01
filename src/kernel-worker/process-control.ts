import { attachKernelRuntimeState, readKernelRuntimeState, type KernelExports, type KernelRuntimeState } from "../abi.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import { bumpDirEpoch, resetPageCache } from "../fs/sab-page-cache.js";
import { createKernelEnv } from "../wasm/env.js";

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
  memfsMutated: boolean;
  ofdStateChanged: boolean;
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
  rebuildPageCacheFromCanonical(): void;
  syncPipeState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState;
  syncEpollState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState;
  syncSocketState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState;
  copyCanonicalMemfsToProcess(
    canonicalState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    options?: { includeData?: boolean },
  ): void;
  composeProcessPipeUsed(processExports: KernelExports, vmPtr: number, sharedPipeUsed: bigint): bigint;
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
  copySocketState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void;
  syncFileDataMutation(sourceExports: KernelExports, ino: number): boolean;
  fanoutCanonicalMemfs(includeData?: boolean): void;
  fsEffectMemfsMutation: number;
  fsEffectSharedOfdState: number;
  fsEffectMemfsFileData: number;
  fsOpOpenAt: number;
  fsOpMkdir: number;
}

export function createKernelWorkerProcessControl(deps: KernelWorkerProcessControlDeps) {
  async function initKernel(
    module: WebAssembly.Module,
    pageCache: SharedArrayBuffer,
  ): Promise<KernelRuntimeState> {
    deps.setWasmModule(module);
    deps.setSabPageCache(pageCache);

    const privateMemory = new WebAssembly.Memory({
      initial: 1024,
      maximum: 32768,
      shared: true,
    });
    const instance = await WebAssembly.instantiate(module, {
      env: createKernelEnv(privateMemory, { sabPageCache: pageCache }),
    });
    const nextExports = instance.exports as unknown as KernelExports;
    deps.setCanonicalExports(nextExports);

    const wasmHeapBase = (nextExports as { __heap_base?: { value: number } }).__heap_base?.value ?? 0x200000;
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
    const instance = await WebAssembly.instantiate(wasmModule, {
      env: createKernelEnv(memory, { sabPageCache: deps.getSabPageCache() }),
    });
    const processExports = instance.exports as unknown as KernelExports;
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
      deps.copyOfdState(processExports, sourceState, canonicalExports, canonicalState);
      if (seedMemfsMode === "full") {
        deps.copyMemfsState(processExports, sourceState, canonicalExports, canonicalState);
        deps.rebuildPageCacheFromCanonical();
      }
      const pipeState = deps.syncPipeState(pid, readKernelRuntimeState(processExports));
      const epollState = deps.syncEpollState(pid, pipeState);
      const socketState = deps.syncSocketState(pid, epollState);
      if (seedMemfsMode === "metadata") {
        const memberState = readKernelRuntimeState(processExports);
        const updatedCanonicalState = readKernelRuntimeState(canonicalExports);
        deps.copyCanonicalMemfsToProcess(updatedCanonicalState, processExports, memberState, {
          includeData: false,
        });
      }
      return socketState;
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
    deps.copyEpollState(canonicalExports, registeredCanonicalState, processExports, registeredProcessState);
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
    const opcode = processExports.thread_pending_fs_opcode(vmPtr);
    const effects = processExports.thread_pending_fs_effects(vmPtr);
    const fileDataTargetIno = (effects & deps.fsEffectMemfsFileData) !== 0
      ? processExports.thread_pending_fs_target_ino(vmPtr)
      : 0;
    processExports.thread_process_fs(vmPtr);
    const result = Number(processExports.thread_resume_fs(vmPtr));
    const fsSucceeded = result >= 0;
    const treeMutated = fsSucceeded && (effects & deps.fsEffectMemfsMutation) !== 0;
    const fileDataMutated = fsSucceeded && (effects & deps.fsEffectMemfsFileData) !== 0;
    let handledFileDataMutation = false;
    if (!treeMutated && fileDataMutated && fileDataTargetIno !== 0) {
      handledFileDataMutation = deps.syncFileDataMutation(processExports, fileDataTargetIno);
    }
    const needsFullSync = treeMutated || (fileDataMutated && !handledFileDataMutation);
    if (needsFullSync) {
      const sourceState = readKernelRuntimeState(processExports);
      const canonicalState = readKernelRuntimeState(canonicalExports);
      const metadataOnlyTreeSync = treeMutated
        && !fileDataMutated
        && (opcode === deps.fsOpMkdir || opcode === deps.fsOpOpenAt);
      deps.copyMemfsState(processExports, sourceState, canonicalExports, canonicalState, {
        includeData: !metadataOnlyTreeSync,
      });
      if (metadataOnlyTreeSync) {
        bumpDirEpoch(deps.getSabPageCache());
      } else {
        deps.rebuildPageCacheFromCanonical();
      }
      deps.fanoutCanonicalMemfs();
    }
    return {
      kernelState: readKernelRuntimeState(processExports),
      memfsMutated: treeMutated || fileDataMutated,
      ofdStateChanged: fsSucceeded && (effects & deps.fsEffectSharedOfdState) !== 0,
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
