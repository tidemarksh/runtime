import {
  assertSupervisorRuntimeAbi,
  guestAddressProfileForProcessCreation,
  processCreateWithMemoryPolicy,
  processGuestBaseForAddressProfile,
  processInspectElfBrkWithMemoryPolicy,
  readKernelRuntimeState,
  setGuestMemoryPolicyLimit,
  type KernelExports,
} from "../abi.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import { createRuntimeStateBridge, publishKernelRuntimeState } from "../bridge/runtime-state.js";
import { instantiateKernelWithSharedMemory } from "../wasm/kernel-memory.js";
import {
  DEFAULT_RESIDENT_PAGE_SIZE,
  publishResidentPage,
  readResidentFileData,
} from "../fs/sab-page-cache.js";
import { supervisorResidentPageTableAuthority } from "../fs/supervisor-page-table.js";
import {
  preloadSnapshotEntries,
  resolveProcessCwdIno,
} from "./process-fs.js";
import type { KernelWorkerFileLayerEntry } from "../messages.js";
import type { GuestWriteWatchRegion } from "../messages.js";
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
import { normalizeGuestMemoryPolicyLimitBytes } from "./fork/common.js";
import {
  MIN_GUEST_MEMORY_POLICY_LIMIT_BYTES,
  normalizeGuestMemoryPolicyLimitBytesWithMinimum,
} from "./memory-policy.js";
import { createTransferDebugStats, createWorkerMessageDebugStats } from "./types.js";
import type {
  AttachThreadWorkerRequest,
  CreateProcessOptions,
  KernelWorkerMemfsSeedMode,
  ProcessHandle,
  RuntimeMemoryOwnership,
} from "./types.js";
import {
  configureKernelInstanceStack,
  kernelHeapBase,
  MAIN_THREAD_KERNEL_STACK_SLOT,
} from "../wasm/stack.js";
import { FS_RING_TOTAL_BYTES } from "../layout/fs-ring.js";

const DEFAULT_PROCESS_STACK_SIZE_BYTES = 8 * 1024 * 1024;
const KERNEL_HEAP_BASE_FALLBACK_BYTES = 12 * 1024 * 1024;
const DEBUG_GUEST_WRITE_WATCH_ENV = "TIDEMARK_DEBUG_GUEST_WRITE_WATCH";

const encoder = new TextEncoder();
const WORKER_DEBUG_LOG_ENABLED = false;

function parseDebugAddress(value: string): number | null {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return null;
  }
  return Math.trunc(parsed);
}

function parseDebugGuestWriteWatchRegions(raw: string | undefined): GuestWriteWatchRegion[] {
  if (!raw) {
    return [];
  }
  const regions: GuestWriteWatchRegion[] = [];
  for (const token of raw.split(/[,\s]+/u)) {
    if (regions.length >= 2) {
      break;
    }
    const [startRaw, endRaw] = token.split(":");
    if (startRaw === undefined || endRaw === undefined) {
      continue;
    }
    const start = parseDebugAddress(startRaw);
    const end = parseDebugAddress(endRaw);
    if (start === null || end === null || end <= start) {
      continue;
    }
    regions.push({ slot: regions.length, start, end });
  }
  return regions;
}

function readDebugGuestWriteWatchRegions(): GuestWriteWatchRegion[] {
  const raw = (globalThis as { process?: { env?: Record<string, string | undefined> } })
    .process?.env?.[DEBUG_GUEST_WRITE_WATCH_ENV];
  return parseDebugGuestWriteWatchRegions(raw);
}

function configureInitialGuestWriteWatch(
  exports: KernelExports,
  handle: ProcessHandle,
  regions: readonly GuestWriteWatchRegion[],
): void {
  if (regions.length === 0) {
    return;
  }
  handle.guestWriteWatchRegions = regions.map((region) => ({ ...region }));
  exports.debug_clear_guest_write_watch_trace?.();
  for (const region of handle.guestWriteWatchRegions) {
    exports.debug_configure_guest_write_watch?.(
      region.slot,
      BigInt(Math.trunc(region.start)),
      BigInt(Math.trunc(region.end)),
    );
  }
}

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

function snapshotMaterializedDataBytes(entries: readonly FsSnapshotEntry[]): number {
  let total = 0;
  for (const entry of entries) {
    if (entry.kind === "file" && entry.data !== undefined) {
      total += entry.data.byteLength;
    }
    if (entry.kind === "symlink" && entry.target !== undefined) {
      total += entry.target.byteLength;
    }
  }
  return total;
}

function entriesContainInlineFileData(entries: readonly FsSnapshotEntry[]): boolean {
  return entries.some((entry) => entry.kind === "file" && entry.data !== undefined);
}

function explicitPreloadPathSet(entries: readonly FsSnapshotEntry[]): Set<string> {
  const paths = new Set<string>(["/"]);
  for (const entry of entries) {
    const normalized = normalizeGuestPath(entry.path);
    paths.add(normalized);
    const parts = normalized.split("/").filter(Boolean);
    for (let index = 1; index < parts.length; index += 1) {
      paths.add(`/${parts.slice(0, index).join("/")}`);
    }
  }
  return paths;
}

function kernelWorkerLayerEntriesForExplicitPreload(
  entries: readonly FsSnapshotEntry[],
): KernelWorkerFileLayerEntry[] {
  const layerEntries: KernelWorkerFileLayerEntry[] = [];
  for (const entry of entries) {
    if (entry.kind === "dir") {
      layerEntries.push({ kind: "dir", path: entry.path, mode: entry.mode });
      continue;
    }
    if (entry.kind === "symlink" && entry.target !== undefined) {
      layerEntries.push({ kind: "symlink", path: entry.path, target: entry.target, mode: entry.mode });
      continue;
    }
    if (entry.kind === "file" && entry.data !== undefined) {
      layerEntries.push({ kind: "file", path: entry.path, data: entry.data, mode: entry.mode });
    }
  }
  return layerEntries;
}

async function routeExplicitPreloadDataThroughKernelWorker(
  entries: FsSnapshotEntry[],
  kernelWorkerRequest: NonNullable<CreateProcessOptions["kernelWorkerRequest"]> | undefined,
): Promise<FsSnapshotEntry[]> {
  if (!entriesContainInlineFileData(entries)) {
    return entries;
  }
  if (!kernelWorkerRequest) {
    throw new Error("explicit preload file data requires kernel-worker backing ownership");
  }
  const layerEntries = kernelWorkerLayerEntriesForExplicitPreload(entries);
  if (layerEntries.length === 0) {
    return entries;
  }
  const layerResponse = await kernelWorkerRequest({
    type: "apply-file-layer",
    entries: layerEntries,
  });
  if (layerResponse.type !== "file-layer-applied") {
    throw new Error(`unexpected kernel-worker apply-file-layer response: ${layerResponse.type}`);
  }
  const snapshotResponse = await kernelWorkerRequest({
    type: "snapshot-fs",
    includeData: false,
  });
  if (snapshotResponse.type !== "snapshot-fs-result") {
    throw new Error(`unexpected kernel-worker snapshot response: ${snapshotResponse.type}`);
  }
  const selectedPaths = explicitPreloadPathSet(entries);
  const filtered = snapshotResponse.entries
    .filter((entry) => selectedPaths.has(normalizeGuestPath(entry.path)));
  for (const entry of entries) {
    if (entry.kind !== "file" || entry.data === undefined) {
      continue;
    }
    const normalized = normalizeGuestPath(entry.path);
    const published = filtered.find((candidate) => (
      candidate.kind === "file" && normalizeGuestPath(candidate.path) === normalized
    ));
    if (!published) {
      throw new Error(`kernel-worker snapshot missing explicit preload file: ${normalized}`);
    }
  }
  return filtered;
}

function assertArtifactMaterializationBudget(
  materializedBytes: number,
  budgetBytes: number | undefined,
): void {
  if (budgetBytes === undefined) {
    return;
  }
  const normalizedBudget = Math.max(Math.trunc(budgetBytes), 0);
  if (materializedBytes <= normalizedBudget) {
    return;
  }
  throw new Error(
    `artifact materialization budget exceeded: stagedBytes=${materializedBytes} budgetBytes=${normalizedBudget}`,
  );
}

function normalizeFsRpcRingCapacityBytes(capacityBytes: number | undefined): number {
  if (capacityBytes === undefined) {
    return FS_RING_TOTAL_BYTES;
  }
  const normalized = Math.max(Math.trunc(capacityBytes), 0);
  if (normalized !== FS_RING_TOTAL_BYTES) {
    throw new Error(
      `fsRpcRingCapacityBytes is fixed by kernel ABI at ${FS_RING_TOTAL_BYTES} bytes; got ${normalized}`,
    );
  }
  return normalized;
}

function publishedMetadataCapacityBytes(state: ReturnType<typeof readKernelRuntimeState>): number {
  return state.memfsHeaderBytes
    + state.memfsInodesBytes
    + state.memfsFirstChildBytes
    + state.memfsDirEntriesBytes;
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

async function publishInterpResidentPages(
  entries: FsSnapshotEntry[],
  interpPath: string | null,
  sabPageCache: SharedArrayBuffer | null | undefined,
  kernelWorkerRequest: NonNullable<CreateProcessOptions["kernelWorkerRequest"]> | undefined,
  kernelExports: KernelExports | undefined,
): Promise<void> {
  if (!interpPath || !sabPageCache || !kernelWorkerRequest || !kernelExports) {
    return;
  }
  const resolvedPath = resolveSnapshotPathFollowingSymlinks(entries, interpPath) ?? normalizeGuestPath(interpPath);
  const entry = entries.find((candidate) => normalizeGuestPath(candidate.path) === resolvedPath);
  if (!entry || entry.kind !== "file" || entry.data !== undefined) {
    return;
  }
  const ino = entry.ino;
  const expectedSize = entry.size ?? 0;
  if (!Number.isInteger(ino) || ino === undefined || ino <= 0 || expectedSize <= 0) {
    throw new Error(`cannot publish PT_INTERP resident pages without inode metadata: ${resolvedPath}`);
  }

  for (let offset = 0; offset < expectedSize; offset += DEFAULT_RESIDENT_PAGE_SIZE) {
    const length = Math.min(DEFAULT_RESIDENT_PAGE_SIZE, expectedSize - offset);
    if (readResidentFileData(sabPageCache, ino, offset, length)) {
      continue;
    }
    const response = await kernelWorkerRequest({
      type: "read-file-range-by-inode",
      ino,
      offset,
      length,
      expectedSize,
    });
    if (response.type !== "file-range-read") {
      throw new Error(`unexpected kernel-worker file-range response: ${response.type}`);
    }
    if (response.data.length !== length) {
      throw new Error(
        `short PT_INTERP backing range: path=${resolvedPath} offset=${offset} length=${length} actual=${response.data.length}`,
      );
    }
    if (!publishResidentPage(sabPageCache, ino, offset / DEFAULT_RESIDENT_PAGE_SIZE, response.data, {
      authority: supervisorResidentPageTableAuthority(kernelExports),
    })) {
      throw new Error(`failed to publish PT_INTERP resident page: path=${resolvedPath} offset=${offset}`);
    }
  }
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
        includeData: false,
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
    sabPageCache: SharedArrayBuffer | null,
    maxMemoryPages: number,
  ): Promise<{
    wasmModule: WebAssembly.Module;
    memory: WebAssembly.Memory;
    exports: KernelExports;
  }> {
    const wasmModule = await deps.ensureCompiledModule(wasmBytes);
    const { instance: mainInstance, memory } = await instantiateKernelWithSharedMemory(wasmModule, {
      initialPages: Math.ceil(deps.bootstrapMemoryBytes / deps.wasmPageSize),
      maximumPages: maxMemoryPages,
    }, {
      sabPageCache,
    });
    const exports = mainInstance.exports as unknown as KernelExports;
    configureKernelInstanceStack(exports, MAIN_THREAD_KERNEL_STACK_SLOT);
    const wasmHeapBase = kernelHeapBase(exports, KERNEL_HEAP_BASE_FALLBACK_BYTES);
    exports.kernel_init(wasmHeapBase);
    assertSupervisorRuntimeAbi(exports);

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
    guestMemoryPolicyLimitBytes: bigint,
    options: CreateProcessOptions,
    elfBrk = inspectProcessElfBrk(exports, guestBase, elfData, guestMemoryPolicyLimitBytes),
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
    guestMemoryPolicyLimitBytes: bigint,
    guestAddressProfile = guestAddressProfileForProcessCreation(exports),
  ): number {
    const inspectScratchBase = detachedKernelScratchBase(exports.memory, guestBase, 0);
    const inspectElfOffset = writeDetachedKernelScratch(exports, inspectScratchBase, elfData);
    const elfBrk = Number(processInspectElfBrkWithMemoryPolicy(
      exports,
      inspectElfOffset,
      elfData.length,
      guestAddressProfile,
      guestMemoryPolicyLimitBytes,
    ));
    return elfBrk;
  }

  function createProcessHandle(
    exports: KernelExports,
    wasmModule: WebAssembly.Module,
    memory: WebAssembly.Memory,
    runtimeId: number,
    vmPtr: number,
    guestMemoryPolicyLimitBytes: bigint,
    guestBase: number,
    guestAddressProfile: number,
    maxMemoryPages: number,
    forkMaxMemoryPages: number,
    artifactMaterializedBytes: number,
    fsRpcRingCapacityBytes: number,
    options: CreateProcessOptions,
    worker: Worker,
    runtimeBridge: ReturnType<typeof createRuntimeStateBridge>,
  ): ProcessHandle {
    const stdinPipe = exports.process_stdin_pipe(vmPtr);
    const stdoutPipe = exports.process_stdout_pipe(vmPtr);
    const stderrPipe = exports.process_stderr_pipe(vmPtr);
    const ttyPtySlot = exports.process_tty_pty_slot?.(vmPtr) ?? 0;

    const kernelState = readKernelRuntimeState(exports);
    const memoryOwnership: RuntimeMemoryOwnership = {
      guestAddressProfile: exports.thread_guest_address_profile?.(vmPtr) ?? guestAddressProfile,
      guestAddressLimitBytes: exports.thread_guest_address_limit?.(vmPtr)?.toString() ?? null,
      guestMemoryPolicyLimitBytes,
      processMaxMemoryBytes: maxMemoryPages * 65536,
      forkProcessMaxMemoryBytes: forkMaxMemoryPages * 65536,
      pageCacheDataCapacityBytes: options.sabPageCache?.byteLength ?? 0,
      publishedMetadataCapacityBytes: publishedMetadataCapacityBytes(kernelState),
      fsRpcRingCapacityBytes,
      kernelWorkerPrivateMemoryBytes: options.kernelWorkerPrivateMemoryBytes ?? null,
      artifactMaterializationBudgetBytes: options.artifactMaterializationBudgetBytes ?? null,
      artifactMaterializedBytes,
    };

    return {
      runtimeId,
      pid: options.pid ?? 0,
      parentPid: options.parentPid ?? null,
      vmPtr,
      guestMemoryPolicyLimitBytes,
      maxMemoryPages,
      forkMaxMemoryPages,
      guestBase,
      worker,
      memory,
      wasmModule,
      kernel: exports,
      kernelState,
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
      stdinClosed: (options.stdin ?? "pipe") === "closed",
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
      vforkReadyWorkers: [],
      runningWorker: null,
      hostBlockedWorkers: new Map(),
      pendingChildSpawnRecords: 0,
      pipeSyncPromise: null,
      pipeSyncQueued: false,
      pipeWaiterWakeQueued: false,
      pendingPipeSlotSnapshots: [],
      epollSyncPromise: null,
      epollSyncQueued: false,
      epollWaiterWakeQueued: false,
      forkSharedSyncPromise: null,
      processMemfsDirty: false,
      processMemfsPublishPending: false,
      processMemfsDeletedPaths: new Set(),
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
      workerMessageDebugStats: createWorkerMessageDebugStats(),
      transferDebugStats: createTransferDebugStats(),
      guestWriteWatchRegions: [],
      memoryOwnership,
    };
  }

  async function createProcess(
    wasmBytes: BufferSource,
    elfData: Uint8Array,
    options: CreateProcessOptions = {},
  ): Promise<ProcessHandle> {
    const guestMemoryPolicyLimitBytes = normalizeGuestMemoryPolicyLimitBytesWithMinimum(
      options.guestMemoryPolicyLimitBytes,
      MIN_GUEST_MEMORY_POLICY_LIMIT_BYTES,
    );
    const minMemoryPages = Math.ceil(deps.bootstrapMemoryBytes / deps.wasmPageSize);
    const requestedMaxMemoryPages = Math.max(options.maxMemoryPages ?? deps.wasmMaxPages, 1);
    const maxMemoryPages = Math.min(requestedMaxMemoryPages, deps.wasmMaxPages);
    const boundedMaxMemoryPages = Math.max(maxMemoryPages, minMemoryPages);
    const forkMaxMemoryPages = Math.max(options.forkMaxMemoryPages ?? boundedMaxMemoryPages, boundedMaxMemoryPages);
    const fsRpcRingCapacityBytes = normalizeFsRpcRingCapacityBytes(options.fsRpcRingCapacityBytes);
    const stackSize = options.stackSize ?? DEFAULT_PROCESS_STACK_SIZE_BYTES;
    const { wasmModule, memory, exports } = await instantiateProcessKernel(
      wasmBytes,
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
    );
    const preloadScratchBase = detachedKernelScratchBase(exports.memory, 0, 0);
    const resolvedBootstrapEntries = await resolveBootstrapEntries(
      runtimeId,
      options,
      delegateFsHydrationToKernelWorker,
    );
    const artifactMaterializedBytes = snapshotMaterializedDataBytes(resolvedBootstrapEntries);
    assertArtifactMaterializationBudget(
      artifactMaterializedBytes,
      options.artifactMaterializationBudgetBytes,
    );
    const bootstrapEntries = options.preloadEntries
      ? await routeExplicitPreloadDataThroughKernelWorker(
        resolvedBootstrapEntries,
        options.kernelWorkerRequest,
      )
      : resolvedBootstrapEntries;
    if (delegateFsHydrationToKernelWorker) {
      await publishInterpResidentPages(
        bootstrapEntries,
        interpPath,
        options.sabPageCache,
        options.kernelWorkerRequest,
        exports,
      );
    }
    const bootstrapEntriesRequireMaterializedData = entriesRequireMaterializedData(bootstrapEntries);
    preloadSnapshotEntries(exports, bootstrapEntries, preloadScratchBase);
    const guestAddressProfile = guestAddressProfileForProcessCreation(exports);
    const guestBase = processGuestBaseForAddressProfile(exports, guestAddressProfile);
    if (guestBase === 0) {
      throw new Error("process_guest_base failed");
    }
    const elfBrk = inspectProcessElfBrk(
      exports,
      guestBase,
      elfData,
      guestMemoryPolicyLimitBytes,
      guestAddressProfile,
    );
    if (elfBrk === 0) {
      throw new Error("ELF inspection failed");
    }

    const cwdIno = delegateFsHydrationToKernelWorker
      ? await deps.resolveProcessCwdInoViaKernelWorker(options.kernelWorkerRequest!, options.cwd)
      : resolveProcessCwdIno(exports, options.cwd);

    ensureEvictionTimer();
    const worker = acquireWorker();
    const runtimeBridge = createRuntimeStateBridge();

    const elfOffset = stageProcessCreation(
      exports,
      guestBase,
      elfData,
      guestMemoryPolicyLimitBytes,
      options,
      elfBrk,
    );
    const vmPtr = processCreateWithMemoryPolicy(
      exports,
      elfOffset,
      elfData.length,
      guestAddressProfile,
      guestMemoryPolicyLimitBytes,
      boundedMaxMemoryPages,
      stackSize,
    );
    if (vmPtr === 0) {
      const interpInSnapshot = interpPath ? snapshotHasPath(bootstrapEntries, interpPath) : null;
      const processCreateLastError = exports.process_create_last_error?.() ?? null;
      throw new Error([
        "process_create failed",
        `processCreateLastError=${processCreateLastError === null ? "<unavailable>" : processCreateLastError}`,
        `elfBrk=${elfBrk}`,
        `guestMemoryPolicyLimitBytes=${guestMemoryPolicyLimitBytes}`,
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
      guestMemoryPolicyLimitBytes,
      guestBase,
      guestAddressProfile,
      boundedMaxMemoryPages,
      forkMaxMemoryPages,
      artifactMaterializedBytes,
      fsRpcRingCapacityBytes,
      options,
      worker,
      runtimeBridge,
    );
    configureInitialGuestWriteWatch(exports, handle, readDebugGuestWriteWatchRegions());
    setGuestMemoryPolicyLimit(
      handle.kernel,
      vmPtr,
      normalizeGuestMemoryPolicyLimitBytes(guestMemoryPolicyLimitBytes),
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
