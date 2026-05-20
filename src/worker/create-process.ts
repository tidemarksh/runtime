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
import { computeGuestRamLimitBytes } from "./fork/common.js";
import { createTransferDebugStats } from "./types.js";
import type {
  AttachThreadWorkerRequest,
  CreateProcessOptions,
  KernelWorkerMemfsSeedMode,
  ProcessHandle,
} from "./types.js";
import {
  configureKernelInstanceStack,
  MAIN_THREAD_KERNEL_STACK_SLOT,
} from "../wasm/stack.js";

const encoder = new TextEncoder();
const WORKER_DEBUG_LOG_ENABLED = false;
const MIN_PROCESS_RAM_SIZE_BYTES = 16 * 1024 * 1024;

function readElfInterpPath(elfData: Uint8Array): string | null {
  if (elfData.length < 64) {
    return null;
  }
  if (elfData[0] !== 0x7f || elfData[1] !== 0x45 || elfData[2] !== 0x4c || elfData[3] !== 0x46) {
    return null;
  }
  if (elfData[4] !== 2 || elfData[5] !== 1) {
    return null;
  }

  const view = new DataView(elfData.buffer, elfData.byteOffset, elfData.byteLength);
  const phOff = Number(view.getBigUint64(32, true));
  const phEntSize = view.getUint16(54, true);
  const phNum = view.getUint16(56, true);
  const phEnd = phOff + phEntSize * phNum;
  if (phEntSize < 56 || phEnd > elfData.length) {
    return null;
  }

  for (let index = 0; index < phNum; index += 1) {
    const start = phOff + index * phEntSize;
    if (view.getUint32(start, true) !== 3) {
      continue;
    }
    const offset = Number(view.getBigUint64(start + 8, true));
    const fileSize = Number(view.getBigUint64(start + 32, true));
    const end = offset + fileSize;
    if (fileSize === 0 || end > elfData.length) {
      return null;
    }
    const bytes = elfData.subarray(offset, end);
    const nul = bytes.indexOf(0);
    const interp = decodeBytes(nul >= 0 ? bytes.subarray(0, nul) : bytes);
    return interp.length > 0 ? interp : null;
  }

  return null;
}

function decodeBytes(bytes: Uint8Array): string {
  const decoder = new TextDecoder();
  if (typeof SharedArrayBuffer !== "undefined" && bytes.buffer instanceof SharedArrayBuffer) {
    return decoder.decode(new Uint8Array(bytes));
  }
  return decoder.decode(bytes);
}

function snapshotHasPath(entries: readonly FsSnapshotEntry[], path: string): boolean {
  return entries.some((entry) => entry.path === path);
}

function normalizeGuestPath(path: string): string {
  const parts: string[] = [];
  for (const part of path.split("/")) {
    if (!part || part === ".") {
      continue;
    }
    if (part === "..") {
      parts.pop();
      continue;
    }
    parts.push(part);
  }
  return `/${parts.join("/")}`;
}

function snapshotEntryMap(entries: readonly FsSnapshotEntry[]): Map<string, FsSnapshotEntry> {
  return new Map(entries.map((entry) => [normalizeGuestPath(entry.path), entry]));
}

function resolveSnapshotPathFollowingSymlinks(
  entries: readonly FsSnapshotEntry[],
  path: string,
): string | null {
  const entriesByPath = snapshotEntryMap(entries);
  let resolvedParts: string[] = [];
  let remainingParts = normalizeGuestPath(path).split("/").filter(Boolean);
  let follows = 0;

  while (remainingParts.length > 0) {
    const part = remainingParts.shift()!;
    const candidate = normalizeGuestPath(`/${[...resolvedParts, part].join("/")}`);
    const entry = entriesByPath.get(candidate);
    if (!entry) {
      return null;
    }
    if (entry.kind !== "symlink") {
      resolvedParts.push(part);
      continue;
    }
    follows += 1;
    if (follows > 40 || !entry.target) {
      return null;
    }

    const target = decodeBytes(entry.target);
    const targetPath = target.startsWith("/")
      ? normalizeGuestPath(target)
      : normalizeGuestPath(`/${[...resolvedParts, target].join("/")}`);
    remainingParts = [
      ...targetPath.split("/").filter(Boolean),
      ...remainingParts,
    ];
    resolvedParts = [];
  }

  return normalizeGuestPath(`/${resolvedParts.join("/")}`);
}

async function materializeInterpEntry(
  entries: FsSnapshotEntry[],
  interpPath: string | null,
  kernelWorkerRequest: NonNullable<CreateProcessOptions["kernelWorkerRequest"]> | undefined,
): Promise<FsSnapshotEntry[]> {
  if (!interpPath || !kernelWorkerRequest) {
    return entries;
  }
  const materializedPath = resolveSnapshotPathFollowingSymlinks(entries, interpPath) ?? normalizeGuestPath(interpPath);
  const existing = entries.find((entry) => normalizeGuestPath(entry.path) === materializedPath);
  if (existing?.kind === "file" && existing.data !== undefined) {
    return entries;
  }

  const response = await kernelWorkerRequest({
    type: "read-file",
    path: interpPath,
  });
  if (response.type !== "file-read") {
    throw new Error(`unexpected kernel-worker read-file response: ${response.type}`);
  }

  let replaced = false;
  const nextEntries = entries.map((entry) => {
    if (normalizeGuestPath(entry.path) !== materializedPath || entry.kind !== "file") {
      return entry;
    }
    replaced = true;
    return { ...entry, data: response.data, size: response.data.length };
  });
  return replaced
    ? nextEntries
    : [
        ...nextEntries,
        { kind: "file", path: materializedPath, data: response.data, size: response.data.length, mode: 0o755 },
      ];
}

interface WorkerCreateProcessDeps {
  processHandles: Map<number, ProcessHandle>;
  attachThreadWorker(handle: ProcessHandle, request: AttachThreadWorkerRequest): void;
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
  function entriesRequireMaterializedData(entries: FsSnapshotEntry[]): boolean {
    return entries.some((entry) => entry.kind === "file" && entry.data === undefined && (entry.size ?? 0) > 0);
  }

  async function resolveBootstrapEntries(
    runtimeId: number,
    options: CreateProcessOptions,
    delegateFsHydrationToKernelWorker: boolean,
    interpPath: string | null,
  ): Promise<FsSnapshotEntry[]> {
    const bootstrapEntries = options.preloadEntries
      ?? deps.getRuntimeFsEntries(runtimeId);
    if (
      !delegateFsHydrationToKernelWorker
      && options.kernelWorkerRequest
      && entriesRequireMaterializedData(bootstrapEntries)
    ) {
      const response = await options.kernelWorkerRequest({
        type: "snapshot-fs",
        // Guest-side execve must be able to read executable bytes for child
        // processes launched by shells/package managers. Metadata-only dynamic
        // snapshots make nested execve fail with ENOEXEC.
        includeData: true,
      });
      if (response.type !== "snapshot-fs-result") {
        throw new Error(`unexpected kernel-worker snapshot response: ${response.type}`);
      }
      return response.entries;
    }
    if (
      !delegateFsHydrationToKernelWorker
      || !options.kernelWorkerRequest
      || !entriesRequireMaterializedData(bootstrapEntries)
    ) {
      return bootstrapEntries;
    }
    const response = await options.kernelWorkerRequest({
      type: "snapshot-fs",
      includeData: false,
    });
    if (response.type !== "snapshot-fs-result") {
      throw new Error(`unexpected kernel-worker snapshot response: ${response.type}`);
    }
    return response.entries;
  }

  function appendMemoryStateDebug(handle: ProcessHandle, stage: string): void {
    if (!WORKER_DEBUG_LOG_ENABLED) {
      return;
    }
    const brkBase = handle.kernel.debug_memory_brk_base?.();
    const brkCurrent = handle.kernel.debug_memory_brk_current?.();
    const mmapNext = handle.kernel.debug_memory_mmap_next?.();
    if (brkBase === undefined || brkCurrent === undefined || mmapNext === undefined) {
      return;
    }
    const heapPtr = handle.kernel.kernel_heap_ptr?.() ?? handle.kernelState.heapPtr;
    const entry = `${stage}:heap=0x${heapPtr.toString(16)}:mem=0x${handle.kernelState.memStatePtr.toString(16)}:${brkBase.toString()}:${brkCurrent.toString()}:${mmapNext.toString()}`;
    handle.workerDebugLog.push(entry);
    if (handle.workerDebugLog.length > 256) {
      handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 256);
    }
    handle.lastDebugMessage = handle.lastDebugMessage
      ? `${handle.lastDebugMessage} ${entry}`
      : entry;
  }

  async function instantiateProcessKernel(
    wasmBytes: BufferSource,
    ramSize: number,
    sabPageCache: SharedArrayBuffer | null,
    maxMemoryPages: number,
  ): Promise<{
    wasmModule: WebAssembly.Module;
    memory: WebAssembly.Memory;
    exports: KernelExports;
  }> {
    const memory = new WebAssembly.Memory({
      initial: Math.ceil(deps.bootstrapMemoryBytes / deps.wasmPageSize),
      maximum: maxMemoryPages,
      shared: true,
    });
    const wasmModule = await deps.ensureCompiledModule(wasmBytes);
    const mainInstance = await WebAssembly.instantiate(wasmModule, {
      env: createKernelEnv(memory, { sabPageCache }),
    });
    const exports = mainInstance.exports as unknown as KernelExports;
    configureKernelInstanceStack(exports, MAIN_THREAD_KERNEL_STACK_SLOT);
    const wasmHeapBase = exports.__heap_base?.value
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
    elfBrk = inspectProcessElfBrk(exports, guestBase, elfData, ramSize),
  ): number {
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

  function inspectProcessElfBrk(
    exports: KernelExports,
    guestBase: number,
    elfData: Uint8Array,
    ramSize: number,
  ): number {
    const inspectScratchBase = detachedKernelScratchBase(exports.memory, guestBase, 0);
    const inspectElfOffset = writeDetachedKernelScratch(exports, inspectScratchBase, elfData);
    const elfBrk = Number(exports.process_inspect_elf_brk(inspectElfOffset, elfData.length, ramSize));
    return elfBrk;
  }

  function createProcessHandle(
    exports: KernelExports,
    wasmModule: WebAssembly.Module,
    memory: WebAssembly.Memory,
    runtimeId: number,
    vmPtr: number,
    ramSize: number,
    guestBase: number,
    maxMemoryPages: number,
    forkMaxMemoryPages: number,
    options: CreateProcessOptions,
    worker: Worker,
    runtimeBridge: ReturnType<typeof createRuntimeStateBridge>,
  ): ProcessHandle {
    const stdinPipe = exports.process_stdin_pipe(vmPtr);
    const stdoutPipe = exports.process_stdout_pipe(vmPtr);
    const stderrPipe = exports.process_stderr_pipe(vmPtr);
    const ttyPtySlot = exports.process_tty_pty_slot?.(vmPtr) ?? 0;

    return {
      runtimeId,
      pid: options.pid ?? 0,
      parentPid: options.parentPid ?? null,
      vmPtr,
      ramSize,
      maxMemoryPages,
      forkMaxMemoryPages,
      guestBase,
      worker,
      memory,
      wasmModule,
      kernel: exports,
      kernelState: readKernelRuntimeState(exports),
      stdinPipe,
      stdoutPipe,
      stderrPipe,
      ttyPtySlot,
      hostStdio: {
        stdinPipe,
        stdoutPipe,
        stderrPipe,
        ttyPtySlot,
        refCount: 1,
      },
      stdioOwner: null,
      kernelShare: createKernelShareGroup(),
      stdinQueue: [],
      stdinFlushPromise: null,
      outputBacklog: [],
      debugCommand: options.argv?.[0] ?? "<entry>",
      lastDebugMessage: undefined,
      runtimeBridge,
      sabPageCache: options.sabPageCache ?? null,
      kernelWorkerRequest: options.kernelWorkerRequest,
      networkBridge: options.networkBridge,
      networkConnections: new Map(),
      networkConnectionOfds: new Map(),
      networkOfdSlotWatermarks: new Map(),
      networkPendingConnectFds: new Set(),
      networkClosedFds: new Set(),
      networkClosedSocketSlots: new Map(),
      networkSocketSlotWatermarks: new Map(),
      networkRecvHeadWatermarks: new Map(),
      networkRecvTailWatermarks: new Map(),
      networkRecvPayloadWatermarks: new Map(),
      workers: new Set(),
      readyWorkers: [],
      runningWorker: null,
      blockedWorkers: new Map(),
      pendingChildExitRecords: 0,
      pendingChildSpawnRecords: 0,
      childExitSyncRecords: new Map(),
      pipeSyncPromise: null,
      pipeSyncQueued: false,
      pipeWaiterWakeQueued: false,
      pendingPipeSlotSnapshots: [],
      epollSyncPromise: null,
      epollSyncQueued: false,
      epollWaiterWakeQueued: false,
      forkSharedSyncPromise: null,
      processMemfsDirty: false,
      ofdSyncPromise: null,
      ofdSyncQueuedIds: new Set(),
      socketSyncPromise: null,
      socketSyncQueued: false,
      socketWaiterWakeQueued: false,
      vforkSnapshot: null,
      vforkWorker: null,
      exitInProgress: false,
      onExit: options.onExit,
      onError: options.onError,
      onStdout: options.onStdout,
      workerDebugLog: [],
      transferDebugStats: createTransferDebugStats(),
      guestWriteWatchRegions: [],
    };
  }

  async function createProcess(
    wasmBytes: BufferSource,
    elfData: Uint8Array,
    options: CreateProcessOptions = {},
  ): Promise<ProcessHandle> {
    const ramSize = Math.max(options.ramSize ?? MIN_PROCESS_RAM_SIZE_BYTES, MIN_PROCESS_RAM_SIZE_BYTES);
    const minMemoryPages = Math.ceil(deps.bootstrapMemoryBytes / deps.wasmPageSize);
    const requestedMaxMemoryPages = Math.max(options.maxMemoryPages ?? deps.wasmMaxPages, 1);
    const maxMemoryPages = Math.min(requestedMaxMemoryPages, deps.wasmMaxPages);
    const boundedMaxMemoryPages = Math.max(maxMemoryPages, minMemoryPages);
    const forkMaxMemoryPages = Math.max(options.forkMaxMemoryPages ?? boundedMaxMemoryPages, boundedMaxMemoryPages);
    const stackSize = options.stackSize ?? 1024 * 1024;
    const { wasmModule, memory, exports } = await instantiateProcessKernel(
      wasmBytes,
      ramSize,
      options.sabPageCache ?? null,
      boundedMaxMemoryPages,
    );

    const runtimeId = options.runtimeId ?? 0;
    const interpPath = readElfInterpPath(elfData);
    const delegateFsHydrationToKernelWorker = !!(
      options.hydrateFsFromKernelWorker
      && options.kernelWorkerRequest
      && (options.pid ?? 0) > 0
      && options.preloadEntries === undefined
      && !interpPath
    );
    const preloadScratchBase = detachedKernelScratchBase(exports.memory, 0, 0);
    const bootstrapEntries = await materializeInterpEntry(await resolveBootstrapEntries(
      runtimeId,
      options,
      delegateFsHydrationToKernelWorker,
      interpPath,
    ), interpPath, options.kernelWorkerRequest);
    const bootstrapEntriesRequireMaterializedData = entriesRequireMaterializedData(bootstrapEntries);
    preloadSnapshotEntries(exports, bootstrapEntries, preloadScratchBase);
    const guestBase = exports.process_guest_base(ramSize);
    if (guestBase === 0) {
      throw new Error("process_guest_base failed");
    }
    const elfBrk = inspectProcessElfBrk(exports, guestBase, elfData, ramSize);
    if (elfBrk === 0) {
      throw new Error("ELF inspection failed");
    }

    const cwdIno = delegateFsHydrationToKernelWorker
      ? await deps.resolveProcessCwdInoViaKernelWorker(options.kernelWorkerRequest!, options.cwd)
      : resolveProcessCwdIno(exports, options.cwd);

    ensureEvictionTimer();
    const worker = acquireWorker();
    const runtimeBridge = createRuntimeStateBridge();

    const elfOffset = stageProcessCreation(exports, guestBase, elfData, ramSize, options, elfBrk);
    const vmPtr = exports.process_create(elfOffset, elfData.length, ramSize, stackSize);
    if (vmPtr === 0) {
      const interpInSnapshot = interpPath ? snapshotHasPath(bootstrapEntries, interpPath) : null;
      const processCreateLastError = exports.process_create_last_error?.() ?? null;
      throw new Error([
        "process_create failed",
        `processCreateLastError=${processCreateLastError === null ? "<unavailable>" : processCreateLastError}`,
        `elfBrk=${elfBrk}`,
        `ramSize=${ramSize}`,
        `stackSize=${stackSize}`,
        `guestBase=0x${guestBase.toString(16)}`,
        `interp=${interpPath ?? "<none>"}`,
        `interpInSnapshot=${interpInSnapshot === null ? "<n/a>" : String(interpInSnapshot)}`,
        `bootstrapEntryCount=${bootstrapEntries.length}`,
        `argvCount=${options.argv?.length ?? 0}`,
        `envCount=${options.env?.length ?? 0}`,
      ].join(" "));
    }
    if ((options.stdio ?? "pipe") === "pipe" && exports.process_use_pipe_stdio) {
      const stdioRet = exports.process_use_pipe_stdio(vmPtr);
      if (stdioRet !== 0) {
        throw new Error("process_use_pipe_stdio failed");
      }
      if ((options.stdin ?? "pipe") === "closed") {
        exports.process_close_host_stdin?.(vmPtr);
      }
    }
    if (!delegateFsHydrationToKernelWorker) {
      exports.process_set_cwd_ino(vmPtr, cwdIno);
    }
    const handle = createProcessHandle(
      exports,
      wasmModule,
      memory,
      runtimeId,
      vmPtr,
      ramSize,
      guestBase,
      boundedMaxMemoryPages,
      forkMaxMemoryPages,
      options,
      worker,
      runtimeBridge,
    );
    handle.kernel.thread_set_guest_ram_limit(
      vmPtr,
      computeGuestRamLimitBytes(guestBase, ramSize, boundedMaxMemoryPages),
    );
    appendMemoryStateDebug(handle, "create");

    const memfsSeedMode: KernelWorkerMemfsSeedMode = delegateFsHydrationToKernelWorker
      ? bootstrapEntriesRequireMaterializedData ? "metadata" : "none"
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

    deps.attachThreadWorker(handle, { worker, vmPtr });
    return handle;
  }

  return {
    createProcess,
  };
}
