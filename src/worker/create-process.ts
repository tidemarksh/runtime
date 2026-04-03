import {
  readKernelRuntimeState,
  type KernelExports,
} from "../abi.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import { createRuntimeStateBridge, publishKernelRuntimeState } from "../bridge/runtime-state.js";
import { createKernelEnv } from "../wasm/env.js";
import {
  preloadSnapshotEntries,
  resolveProcessCwdIno,
} from "./process-fs.js";
import { createKernelShareGroup } from "./kernel-share.js";
import {
  alignUp,
  detachedKernelScratchBase,
  writeDetachedKernelScratch,
} from "./kernel-scratch.js";
import {
  acquireWorker,
  ensureEvictionTimer,
} from "./pool.js";
import type {
  CreateProcessOptions,
  KernelWorkerMemfsSeedMode,
  ProcessHandle,
} from "./types.js";

const encoder = new TextEncoder();

interface WorkerCreateProcessDeps {
  processHandles: Map<number, ProcessHandle>;
  attachThreadWorker(handle: ProcessHandle, worker: Worker, vmPtr: number): void;
  assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void;
  ensureCompiledModule(wasmBytes: BufferSource): Promise<WebAssembly.Module>;
  getRuntimeFsEntries(runtimeId: number): FsSnapshotEntry[];
  initializeProcessLifecycleViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
    ppid: number,
    nextPid: number,
    pgid: number,
    sid: number,
    flushSharedMappings: boolean,
  ): Promise<void>;
  registerProcessWithKernelWorker(
    handle: ProcessHandle,
    options?: { seedMemfsMode?: KernelWorkerMemfsSeedMode },
  ): Promise<void>;
  resolveProcessCwdInoViaKernelWorker(
    kernelWorkerRequest: NonNullable<CreateProcessOptions["kernelWorkerRequest"]>,
    cwd: string | undefined,
  ): Promise<number>;
  bootstrapMemoryBytes: number;
  wasmPageSize: number;
  wasmMaxPages: number;
}

export function createWorkerProcessFactory(deps: WorkerCreateProcessDeps) {
  function appendMemoryStateDebug(handle: ProcessHandle, stage: string): void {
    const brkBase = handle.kernel.debug_memory_brk_base?.();
    const brkCurrent = handle.kernel.debug_memory_brk_current?.();
    const mmapNext = handle.kernel.debug_memory_mmap_next?.();
    if (brkBase === undefined || brkCurrent === undefined || mmapNext === undefined) {
      return;
    }
    const heapPtr = handle.kernel.kernel_heap_ptr?.() ?? handle.kernelState.heapPtr;
    const entry = `${stage}:heap=0x${heapPtr.toString(16)}:mem=0x${handle.kernelState.memStatePtr.toString(16)}:${brkBase.toString()}:${brkCurrent.toString()}:${mmapNext.toString()}`;
    handle.workerDebugLog.push(entry);
    if (handle.workerDebugLog.length > 48) {
      handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 48);
    }
    handle.lastDebugMessage = handle.lastDebugMessage
      ? `${handle.lastDebugMessage} ${entry}`
      : entry;
  }

  async function instantiateProcessKernel(
    wasmBytes: BufferSource,
    ramSize: number,
    sabPageCache: SharedArrayBuffer | null,
  ): Promise<{
    wasmModule: WebAssembly.Module;
    memory: WebAssembly.Memory;
    exports: KernelExports;
  }> {
    const memory = new WebAssembly.Memory({
      initial: Math.ceil(deps.bootstrapMemoryBytes / deps.wasmPageSize),
      maximum: deps.wasmMaxPages,
      shared: true,
    });
    const wasmModule = await deps.ensureCompiledModule(wasmBytes);
    const mainInstance = await WebAssembly.instantiate(wasmModule, {
      env: createKernelEnv(memory, { sabPageCache }),
    });
    const exports = mainInstance.exports as unknown as KernelExports;
    const wasmHeapBase = (exports as { __heap_base?: { value: number } }).__heap_base?.value
      ?? (ramSize - 4 * 1024 * 1024);
    exports.kernel_init(wasmHeapBase);

    const now = Date.now();
    exports.set_base_time(
      BigInt(Math.floor(now / 1000)),
      BigInt((now % 1000) * 1_000_000),
    );

    return { wasmModule, memory, exports };
  }

  function stageProcessCreation(
    exports: KernelExports,
    guestBase: number,
    elfData: Uint8Array,
    ramSize: number,
    options: CreateProcessOptions,
  ): number {
    const inspectScratchBase = detachedKernelScratchBase(exports.memory, guestBase, 0);
    const inspectElfOffset = writeDetachedKernelScratch(exports, inspectScratchBase, elfData);
    const elfBrk = Number(exports.process_inspect_elf_brk(inspectElfOffset, elfData.length, ramSize));
    if (elfBrk === 0) {
      throw new Error("ELF inspection failed");
    }

    const runtimeScratchBase = detachedKernelScratchBase(exports.memory, guestBase, elfBrk);
    let scratchOffset = 0;
    const elfOffset = writeDetachedKernelScratch(exports, runtimeScratchBase, elfData, scratchOffset);
    scratchOffset = alignUp(scratchOffset + elfData.length, 16);

    exports.process_clear_args();
    const argBytes = (options.argv ?? []).map((arg) => encoder.encode(`${arg}\0`));
    const envBytes = (options.env ?? []).map((env) => encoder.encode(`${env}\0`));
    for (const bytes of argBytes) {
      const ptr = writeDetachedKernelScratch(exports, runtimeScratchBase, bytes, scratchOffset);
      exports.process_push_arg(ptr);
      scratchOffset = alignUp(scratchOffset + bytes.length, 16);
    }
    for (const bytes of envBytes) {
      const ptr = writeDetachedKernelScratch(exports, runtimeScratchBase, bytes, scratchOffset);
      exports.process_push_env(ptr);
      scratchOffset = alignUp(scratchOffset + bytes.length, 16);
    }

    return elfOffset;
  }

  function createProcessHandle(
    exports: KernelExports,
    wasmModule: WebAssembly.Module,
    memory: WebAssembly.Memory,
    runtimeId: number,
    vmPtr: number,
    ramSize: number,
    guestBase: number,
    options: CreateProcessOptions,
    worker: Worker,
  ): ProcessHandle {
    const stdinPipe = exports.process_stdin_pipe(vmPtr);
    const stdoutPipe = exports.process_stdout_pipe(vmPtr);
    const stderrPipe = exports.process_stderr_pipe(vmPtr);

    return {
      runtimeId,
      pid: options.pid ?? 0,
      parentPid: options.parentPid ?? null,
      vmPtr,
      ramSize,
      guestBase,
      worker,
      memory,
      wasmModule,
      kernel: exports,
      kernelState: readKernelRuntimeState(exports),
      stdinPipe,
      stdoutPipe,
      stderrPipe,
      hostStdio: {
        stdinPipe,
        stdoutPipe,
        stderrPipe,
        refCount: 1,
      },
      kernelShare: createKernelShareGroup(),
      stdinQueue: [],
      outputBacklog: [],
      debugCommand: options.argv?.[0] ?? "<entry>",
      lastDebugMessage: undefined,
      runtimeBridge: createRuntimeStateBridge(),
      sabPageCache: options.sabPageCache ?? null,
      kernelWorkerRequest: options.kernelWorkerRequest,
      workers: new Set(),
      readyWorkers: [],
      runningWorker: null,
      blockedWorkers: new Map(),
      pipeSyncPromise: null,
      pipeSyncQueued: false,
      pipeWaiterWakeQueued: false,
      epollSyncPromise: null,
      epollSyncQueued: false,
      epollWaiterWakeQueued: false,
      ofdSyncPromise: null,
      ofdSyncQueuedIds: new Set(),
      socketSyncPromise: null,
      socketSyncQueued: false,
      socketWaiterWakeQueued: false,
      vforkSnapshot: null,
      vforkWorker: null,
      onExit: options.onExit,
      onError: options.onError,
      onStdout: options.onStdout,
      workerDebugLog: [],
    };
  }

  async function createProcess(
    wasmBytes: BufferSource,
    elfData: Uint8Array,
    options: CreateProcessOptions = {},
  ): Promise<ProcessHandle> {
    const ramSize = Math.max(options.ramSize ?? 16 * 1024 * 1024, 64 * 1024 * 1024);
    const stackSize = options.stackSize ?? 1024 * 1024;
    const { wasmModule, memory, exports } = await instantiateProcessKernel(
      wasmBytes,
      ramSize,
      options.sabPageCache ?? null,
    );

    const runtimeId = options.runtimeId ?? 0;
    const delegateFsHydrationToKernelWorker = !!(
      options.hydrateFsFromKernelWorker
      && options.kernelWorkerRequest
      && (options.pid ?? 0) > 0
      && options.preloadEntries === undefined
    );
    const bootstrapEntries = options.preloadEntries
      ?? (delegateFsHydrationToKernelWorker ? [] : deps.getRuntimeFsEntries(runtimeId));
    preloadSnapshotEntries(exports, bootstrapEntries);

    const cwdIno = delegateFsHydrationToKernelWorker
      ? await deps.resolveProcessCwdInoViaKernelWorker(options.kernelWorkerRequest!, options.cwd)
      : resolveProcessCwdIno(exports, options.cwd);
    const guestBase = exports.process_guest_base(ramSize);
    if (guestBase === 0) {
      throw new Error("process_guest_base failed");
    }

    const elfOffset = stageProcessCreation(exports, guestBase, elfData, ramSize, options);
    const vmPtr = exports.process_create(elfOffset, elfData.length, ramSize, stackSize);
    if (vmPtr === 0) {
      throw new Error("process_create failed");
    }
    if (!delegateFsHydrationToKernelWorker) {
      exports.process_set_cwd_ino(vmPtr, cwdIno);
    }

    ensureEvictionTimer();
    const worker = acquireWorker();
    const handle = createProcessHandle(
      exports,
      wasmModule,
      memory,
      runtimeId,
      vmPtr,
      ramSize,
      guestBase,
      options,
      worker,
    );
    appendMemoryStateDebug(handle, "create");

    const memfsSeedMode: KernelWorkerMemfsSeedMode = delegateFsHydrationToKernelWorker
      ? "metadata"
      : "full";
    await deps.registerProcessWithKernelWorker(handle, { seedMemfsMode: memfsSeedMode });
    appendMemoryStateDebug(handle, "register");
    if (delegateFsHydrationToKernelWorker) {
      handle.kernel.process_set_cwd_ino(vmPtr, cwdIno);
    }
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      const parent = handle.parentPid !== null ? deps.processHandles.get(handle.parentPid) : undefined;
      const inheritedPgid = parent ? parent.kernel.process_current_pgid(parent.vmPtr) : handle.pid;
      const inheritedSid = parent ? parent.kernel.process_current_sid(parent.vmPtr) : handle.pid;
      await deps.initializeProcessLifecycleViaKernelWorker(
        handle,
        vmPtr,
        handle.parentPid ?? 0,
        Math.max(3, handle.pid + 1),
        inheritedPgid,
        inheritedSid,
        false,
      );
      appendMemoryStateDebug(handle, "lifecycle");
    }
    deps.assertProcessStandardFdIntegrity(handle, "create-process");
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState);
    handle.kernelShare.members.add(handle);

    if (handle.pid > 0) {
      deps.processHandles.set(handle.pid, handle);
    }

    deps.attachThreadWorker(handle, worker, vmPtr);
    return handle;
  }

  return {
    createProcess,
  };
}
