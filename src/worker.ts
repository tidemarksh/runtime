/**
 * Process and thread-worker orchestration.
 */

import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  readProcessSpawnHandoff,
  type KernelExports,
  type KernelRuntimeState,
} from "./abi.js";
import type { FsSnapshotEntry } from "./fs-types.js";
import { readPipe, writePipe } from "./pipe-bridge.js";
import type {
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerOutbound,
  KernelWorkerPidStateSnapshot,
  KernelWorkerRequest,
  ThreadWorkerInbound,
  ThreadWorkerOutbound,
} from "./messages.js";
import {
  createRuntimeStateBridge,
  publishKernelRuntimeState,
  type RuntimeStateBridge,
  wakeRuntimeBridge,
} from "./runtime-state-bridge.js";
import { createKernelEnv } from "./wasm-env.js";
import { setWorkerMessageHandler } from "./worker-compat.js";

let cachedModule: WebAssembly.Module | null = null;

export async function precompileModule(wasmBytes: BufferSource): Promise<WebAssembly.Module> {
  cachedModule = await WebAssembly.compile(wasmBytes);
  return cachedModule;
}

export function clearModuleCache(): void {
  cachedModule = null;
}

interface WorkerPoolEntry {
  worker: Worker;
  lastUsed: number;
  busy: boolean;
}

interface SharedHostStdio {
  stdinPipe: number;
  stdoutPipe: number;
  stderrPipe: number;
  refCount: number;
}

interface KernelShareGroup {
  members: Set<ProcessHandle>;
  sharedOfdIds: Set<number>;
}

type KernelSyncHandle = Pick<
  ProcessHandle,
  "memory" | "kernel" | "kernelState" | "pid" | "kernelWorkerRequest"
>;
type KernelOwnershipHandle = Pick<ProcessHandle, "pid" | "kernelWorkerRequest">;
type KernelWorkerRequestFn = (msg: KernelWorkerRequest) => Promise<KernelWorkerOutbound>;

const workerPool: WorkerPoolEntry[] = [];
const workerEntryMap = new WeakMap<Worker, WorkerPoolEntry>();
const MAX_WORKERS = 4;
const IDLE_TIMEOUT_MS = 60_000;
const EVICTION_INTERVAL_MS = 10_000;

let evictionTimer: ReturnType<typeof setInterval> | null = null;

function maybeUnrefWorker(worker: Worker): void {
  const nodeWorker = worker as Worker & { unref?: () => void };
  nodeWorker.unref?.();
}

function ensureEvictionTimer(): void {
  if (evictionTimer !== null) {
    return;
  }
  evictionTimer = setInterval(evictIdleWorkers, EVICTION_INTERVAL_MS);
  if (typeof evictionTimer === "object" && "unref" in evictionTimer) {
    evictionTimer.unref();
  }
}

function evictIdleWorkers(): void {
  const now = Date.now();
  for (let i = workerPool.length - 1; i >= 0; i--) {
    const entry = workerPool[i];
    if (entry.busy || now - entry.lastUsed <= IDLE_TIMEOUT_MS) {
      continue;
    }
    entry.worker.terminate();
    workerEntryMap.delete(entry.worker);
    workerPool.splice(i, 1);
  }
}

function acquireWorker(): Worker {
  for (const entry of workerPool) {
    if (entry.busy) {
      continue;
    }
    entry.busy = true;
    entry.lastUsed = Date.now();
    return entry.worker;
  }

  const workerUrl = new URL("./thread-worker.mjs", import.meta.url);
  if (workerPool.length < MAX_WORKERS) {
    const worker = new Worker(workerUrl, { type: "module" });
    maybeUnrefWorker(worker);
    const entry: WorkerPoolEntry = {
      worker,
      lastUsed: Date.now(),
      busy: true,
    };
    workerPool.push(entry);
    workerEntryMap.set(worker, entry);
    return worker;
  }

  let lruIndex = -1;
  let lruTime = Number.POSITIVE_INFINITY;
  for (let i = 0; i < workerPool.length; i++) {
    const entry = workerPool[i];
    if (!entry.busy && entry.lastUsed < lruTime) {
      lruIndex = i;
      lruTime = entry.lastUsed;
    }
  }

  if (lruIndex === -1) {
    const worker = new Worker(workerUrl, { type: "module" });
    maybeUnrefWorker(worker);
    return worker;
  }

  const evicted = workerPool[lruIndex];
  evicted.worker.terminate();
  workerEntryMap.delete(evicted.worker);

  const worker = new Worker(workerUrl, { type: "module" });
  maybeUnrefWorker(worker);
  const entry: WorkerPoolEntry = {
    worker,
    lastUsed: Date.now(),
    busy: true,
  };
  workerPool[lruIndex] = entry;
  workerEntryMap.set(worker, entry);
  return worker;
}

function releaseWorker(worker: Worker): void {
  const entry = workerEntryMap.get(worker);
  if (!entry) {
    worker.terminate();
    return;
  }
  entry.busy = false;
  entry.lastUsed = Date.now();
}

export function destroyWorkerPool(): void {
  if (evictionTimer !== null) {
    clearInterval(evictionTimer);
    evictionTimer = null;
  }
  for (const entry of workerPool) {
    entry.worker.terminate();
    workerEntryMap.delete(entry.worker);
  }
  workerPool.length = 0;
}

function maybeUnrefTimer(timer: ReturnType<typeof setTimeout> | ReturnType<typeof setInterval>): void {
  if (typeof timer === "object" && timer !== null && "unref" in timer) {
    timer.unref();
  }
}

export interface ProcessHandle {
  runtimeId: number;
  pid: number;
  parentPid: number | null;
  vmPtr: number;
  ramSize: number;
  guestBase: number;
  worker: Worker;
  memory: WebAssembly.Memory;
  wasmModule: WebAssembly.Module;
  kernel: KernelExports;
  kernelState: KernelRuntimeState;
  stdinPipe: number;
  stdoutPipe: number;
  stderrPipe: number;
  hostStdio: SharedHostStdio;
  kernelShare: KernelShareGroup;
  stdinQueue: Uint8Array[];
  outputBacklog: Uint8Array[];
  lastDebugMessage?: string;
  runtimeBridge: RuntimeStateBridge;
  kernelWorkerRequest?: KernelWorkerRequestFn;
  workers: Set<Worker>;
  readyWorkers: Worker[];
  runningWorker: Worker | null;
  blockedWorkers: Map<Worker, BlockedWorkerState>;
  pipeSyncPromise: Promise<void> | null;
  pipeSyncQueued: boolean;
  pipeWaiterWakeQueued: boolean;
  epollSyncPromise: Promise<void> | null;
  epollSyncQueued: boolean;
  epollWaiterWakeQueued: boolean;
  socketSyncPromise: Promise<void> | null;
  socketSyncQueued: boolean;
  socketWaiterWakeQueued: boolean;
  vforkSnapshot: VforkSnapshot | null;
  vforkWorker: Worker | null;
  onExit?: (code: number) => void;
  onError?: (error: Error) => void;
  onStdout?: (data: Uint8Array) => void;
  workerDebugLog: string[];
}

interface VforkSnapshot {
  vmPtr: number;
  childPid: number | null;
  nextPid: number | null;
  pgid: number | null;
  sid: number | null;
  inheritedSharedOfdIds: number[];
}

interface ThreadWorkerState {
  vmPtr: number;
  initialized: boolean;
  waitingInWorkerStatus: number | null;
}

interface BlockedWorkerState {
  vmPtr: number;
  status: number;
  retryTimer: ReturnType<typeof setTimeout> | null;
  timeoutExpired: boolean;
  rpcPending: boolean;
}

interface RuntimeFsState {
  version: number;
  entries: FsSnapshotEntry[];
}

interface CanonicalizedChildLifecycle {
  childPid: number;
  nextPid: number;
  pgid: number;
  sid: number;
}

const processHandles = new Map<number, ProcessHandle>();
const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
const runtimeFsStates = new Map<number, RuntimeFsState>();
const runtimeFsDirtyHandles = new Map<number, Set<ProcessHandle>>();

interface CreateProcessOptions {
  runtimeId?: number;
  pid?: number;
  parentPid?: number | null;
  ramSize?: number;
  stackSize?: number;
  argv?: string[];
  env?: string[];
  cwd?: string;
  preloadEntries?: FsSnapshotEntry[];
  kernelWorkerRequest?: KernelWorkerRequestFn;
  onExit?: (code: number) => void;
  onError?: (error: Error) => void;
  onStdout?: (data: Uint8Array) => void;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const WASM_PAGE_SIZE = 65536;
const WASM_MAX_PAGES = 65536;
const GUEST_PAGE_SIZE = 4096;
const THREAD_STEP_BUDGET = 20_000;
const BOOTSTRAP_MEMORY_BYTES = 64 * 1024 * 1024;

function normalizePath(path: string): string {
  const normalized = `/${path.split("/").filter(Boolean).join("/")}`;
  return normalized === "//" ? "/" : normalized || "/";
}

function cloneSnapshotEntry(entry: FsSnapshotEntry): FsSnapshotEntry {
  return entry.kind === "file"
    ? {
        kind: "file",
        path: entry.path,
        data: new Uint8Array(entry.data ?? new Uint8Array()),
        mode: entry.mode,
      }
    : {
        kind: "dir",
        path: entry.path,
        mode: entry.mode,
      };
}

function cloneSnapshotEntries(entries: FsSnapshotEntry[]): FsSnapshotEntry[] {
  return entries.map(cloneSnapshotEntry);
}

function writeRuntimeFsState(runtimeId: number, entries: FsSnapshotEntry[], bumpVersion: boolean): number {
  if (runtimeId <= 0) {
    return 0;
  }
  const existing = runtimeFsStates.get(runtimeId);
  const version = bumpVersion ? (existing?.version ?? 0) + 1 : (existing?.version ?? 1);
  runtimeFsStates.set(runtimeId, {
    version,
    entries: cloneSnapshotEntries(entries),
  });
  return version;
}

export function primeRuntimeFsState(runtimeId: number, entries: FsSnapshotEntry[]): number {
  return writeRuntimeFsState(runtimeId, entries, false);
}

export function publishRuntimeFsState(runtimeId: number, entries: FsSnapshotEntry[]): number {
  return writeRuntimeFsState(runtimeId, entries, true);
}

export function getRuntimeFsState(
  runtimeId: number,
): { version: number; entries: FsSnapshotEntry[] } | null {
  const state = runtimeFsStates.get(runtimeId);
  if (!state) {
    return null;
  }
  return {
    version: state.version,
    entries: cloneSnapshotEntries(state.entries),
  };
}

export function flushRuntimeFsStateFromDirtyHandle(
  runtimeId: number,
): { version: number; entries: FsSnapshotEntry[] } | null {
  const dirtyHandles = runtimeFsDirtyHandles.get(runtimeId);
  if (!dirtyHandles || dirtyHandles.size === 0) {
    return null;
  }

  const liveRepresentatives = new Map<KernelShareGroup, ProcessHandle>();
  for (const handle of dirtyHandles) {
    if (handle.pid > 0 && processHandles.get(handle.pid) !== handle) {
      continue;
    }
    const representative = kernelShareRepresentative(handle) ?? handle;
    liveRepresentatives.set(representative.kernelShare, representative);
  }

  runtimeFsDirtyHandles.delete(runtimeId);
  if (liveRepresentatives.size === 0) {
    return null;
  }

  let entries: FsSnapshotEntry[] | null = null;
  for (const representative of liveRepresentatives.values()) {
    const snapshot = snapshotProcessFs(representative);
    if (entries === null) {
      entries = snapshot;
      continue;
    }
    if (!snapshotEntriesEqual(entries, snapshot)) {
      throw new Error(`runtime ${runtimeId} has divergent process-local fs state`);
    }
  }

  if (entries === null) {
    runtimeFsDirtyHandles.delete(runtimeId);
    return null;
  }

  const version = publishRuntimeFsState(runtimeId, entries);
  return { version, entries };
}

export function clearRuntimeFsState(runtimeId: number): void {
  runtimeFsStates.delete(runtimeId);
  runtimeFsDirtyHandles.delete(runtimeId);
}

function bufferSourceByteLength(value: BufferSource): number {
  return ArrayBuffer.isView(value) ? value.byteLength : value.byteLength;
}

function alignUp(value: number, align: number): number {
  return Math.ceil(value / align) * align;
}

function alignDown(value: number, align: number): number {
  return Math.floor(value / align) * align;
}

function ensureMemoryCapacity(memory: WebAssembly.Memory, endOffset: number): void {
  const currentBytes = memory.buffer.byteLength;
  if (endOffset <= currentBytes) {
    return;
  }
  const additionalPages = Math.ceil((endOffset - currentBytes) / WASM_PAGE_SIZE);
  memory.grow(additionalPages);
}

function copyMemoryRange(
  sourceMemory: WebAssembly.Memory,
  sourceOffset: number,
  targetMemory: WebAssembly.Memory,
  targetOffset: number,
  byteLength: number,
): void {
  if (byteLength <= 0) {
    return;
  }
  ensureMemoryCapacity(targetMemory, targetOffset + byteLength);
  const source = new Uint8Array(sourceMemory.buffer, sourceOffset, byteLength);
  new Uint8Array(targetMemory.buffer).set(source, targetOffset);
}

function createKernelShareGroup(): KernelShareGroup {
  return {
    members: new Set(),
    sharedOfdIds: new Set(),
  };
}

function kernelShareRepresentative(handle: ProcessHandle): ProcessHandle | null {
  const representative = handle.kernelShare.members.values().next().value as ProcessHandle | undefined;
  return representative ?? null;
}

function kernelShareOtherMembers(handle: ProcessHandle): ProcessHandle[] {
  return [...handle.kernelShare.members].filter((member) => member !== handle);
}

function copyKernelRegion(
  source: KernelSyncHandle,
  sourceOffset: number,
  target: KernelSyncHandle,
  targetOffset: number,
  byteLength: number,
): void {
  copyMemoryRange(source.memory, sourceOffset, target.memory, targetOffset, byteLength);
}

function clearKernelRegion(
  target: KernelSyncHandle,
  targetOffset: number,
  byteLength: number,
): void {
  new Uint8Array(target.memory.buffer, targetOffset, byteLength).fill(0);
}

function clearMemfsState(target: KernelSyncHandle): void {
  clearKernelRegion(
    target,
    target.kernel.kernel_memfs_inodes_ptr(),
    target.kernel.kernel_memfs_inodes_bytes(),
  );
  clearKernelRegion(
    target,
    target.kernel.kernel_memfs_first_child_ptr(),
    target.kernel.kernel_memfs_first_child_bytes(),
  );
  clearKernelRegion(
    target,
    target.kernel.kernel_memfs_dir_entries_ptr(),
    target.kernel.kernel_memfs_dir_entries_bytes(),
  );
  clearKernelRegion(
    target,
    target.kernel.kernel_memfs_data_ptr(),
    target.kernel.kernel_memfs_data_bytes(),
  );
  clearKernelRegion(
    target,
    target.kernelState.memfsPtr,
    target.kernel.kernel_memfs_header_bytes(),
  );
}

function syncMemfsState(source: KernelSyncHandle, target: KernelSyncHandle): void {
  copyKernelRegion(
    source,
    source.kernel.kernel_memfs_inodes_ptr(),
    target,
    target.kernel.kernel_memfs_inodes_ptr(),
    source.kernel.kernel_memfs_inodes_bytes(),
  );
  copyKernelRegion(
    source,
    source.kernel.kernel_memfs_first_child_ptr(),
    target,
    target.kernel.kernel_memfs_first_child_ptr(),
    source.kernel.kernel_memfs_first_child_bytes(),
  );
  copyKernelRegion(
    source,
    source.kernel.kernel_memfs_dir_entries_ptr(),
    target,
    target.kernel.kernel_memfs_dir_entries_ptr(),
    source.kernel.kernel_memfs_dir_entries_bytes(),
  );
  copyKernelRegion(
    source,
    source.kernel.kernel_memfs_data_ptr(),
    target,
    target.kernel.kernel_memfs_data_ptr(),
    source.kernel.kernel_memfs_data_bytes(),
  );
  copyKernelRegion(
    source,
    source.kernelState.memfsPtr,
    target,
    target.kernelState.memfsPtr,
    source.kernel.kernel_memfs_header_bytes(),
  );
}

function copySelectedOfdState(
  source: KernelSyncHandle,
  target: KernelSyncHandle,
  ofdIds: Iterable<number>,
): void {
  const maxFds = source.kernel.abi_max_fds();
  const tableBytes = source.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }

  for (const ofdId of ofdIds) {
    if (ofdId <= 0 || ofdId >= maxFds) {
      continue;
    }
    const slotOffset = source.kernelState.ofdTablePtr + ofdId * slotBytes;
    copyKernelRegion(source, slotOffset, target, target.kernelState.ofdTablePtr + ofdId * slotBytes, slotBytes);
  }
}

function clearEpollState(target: KernelSyncHandle): void {
  const tableBytes = target.kernel.kernel_epoll_table_bytes();
  const instanceBytes = target.kernel.kernel_epoll_instance_bytes();
  const tableEntries = tableBytes >>> 2;
  const table = new Uint32Array(target.memory.buffer, target.kernelState.epollTablePtr, tableEntries);
  const instancePtrs = [...table].filter((ptr) => ptr !== 0);

  clearKernelRegion(target, target.kernelState.epollTablePtr, tableBytes);
  for (const ptr of instancePtrs) {
    clearKernelRegion(target, ptr, instanceBytes);
  }
}

function syncEpollState(source: KernelSyncHandle, target: KernelSyncHandle): void {
  const tableBytes = source.kernel.kernel_epoll_table_bytes();
  const instanceBytes = source.kernel.kernel_epoll_instance_bytes();

  copyKernelRegion(source, source.kernelState.epollTablePtr, target, target.kernelState.epollTablePtr, tableBytes);

  const tableEntries = tableBytes >>> 2;
  const sourceTable = new Uint32Array(source.memory.buffer, source.kernelState.epollTablePtr, tableEntries);
  for (let i = 0; i < tableEntries; i++) {
    const ptr = sourceTable[i];
    if (ptr === 0) {
      continue;
    }
    copyKernelRegion(source, ptr, target, ptr, instanceBytes);
  }
}

function copySocketState(source: KernelSyncHandle, target: KernelSyncHandle): void {
  copyKernelRegion(
    source,
    source.kernelState.socketTablePtr,
    target,
    target.kernelState.socketTablePtr,
    source.kernel.kernel_socket_table_bytes(),
  );
  copyKernelRegion(
    source,
    source.kernelState.netBufBase,
    target,
    target.kernelState.netBufBase,
    source.kernel.kernel_net_buf_pool_bytes(),
  );
}

function kernelWorkerOwnsPipeState(handle: KernelOwnershipHandle): boolean {
  return !!handle.kernelWorkerRequest && handle.pid > 0;
}

function kernelWorkerOwnsEpollState(handle: KernelOwnershipHandle): boolean {
  return !!handle.kernelWorkerRequest && handle.pid > 0;
}

function kernelWorkerOwnsSocketState(handle: KernelOwnershipHandle): boolean {
  return !!handle.kernelWorkerRequest && handle.pid > 0;
}

function discardCopiedSharedKernelObjects(
  handle: KernelSyncHandle,
  preservedOfdSlots: ReadonlyMap<number, Uint8Array> | null = null,
): void {
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  clearKernelRegion(handle, handle.kernelState.ofdTablePtr, tableBytes);
  if (preservedOfdSlots && preservedOfdSlots.size > 0 && Number.isInteger(slotBytes) && slotBytes > 0) {
    const bytes = new Uint8Array(handle.memory.buffer);
    for (const [ofdId, slot] of preservedOfdSlots) {
      bytes.set(slot, handle.kernelState.ofdTablePtr + ofdId * slotBytes);
    }
  }
  clearMemfsState(handle);
  clearKernelRegion(
    handle,
    handle.kernelState.pipeBase,
    handle.kernel.kernel_pipe_region_bytes(),
  );
  clearEpollState(handle);
  clearKernelRegion(
    handle,
    handle.kernelState.socketTablePtr,
    handle.kernel.kernel_socket_table_bytes(),
  );
  clearKernelRegion(
    handle,
    handle.kernelState.netBufBase,
    handle.kernel.kernel_net_buf_pool_bytes(),
  );
}

function restoreLocalOfdSlots(
  handle: KernelSyncHandle,
  preservedOfdSlots: ReadonlyMap<number, Uint8Array> | null,
): void {
  if (!preservedOfdSlots || preservedOfdSlots.size === 0) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  for (const [ofdId, slot] of preservedOfdSlots) {
    bytes.set(slot, handle.kernelState.ofdTablePtr + ofdId * slotBytes);
  }
}

function copyLegacySharedKernelObjectsLocal(
  source: KernelSyncHandle,
  target: KernelSyncHandle,
  sharedOfdIds: Iterable<number>,
): void {
  if (source === target) {
    return;
  }
  const includePipe = !kernelWorkerOwnsPipeState(source) && !kernelWorkerOwnsPipeState(target);
  const includeEpoll = !kernelWorkerOwnsEpollState(source) && !kernelWorkerOwnsEpollState(target);
  const includeSocket = !kernelWorkerOwnsSocketState(source)
    && !kernelWorkerOwnsSocketState(target);

  // Shared backend state that survives fork/vfork across processes.
  // Per-process signal disposition/pending state must not be synchronized here.
  copySelectedOfdState(source, target, sharedOfdIds);
  syncMemfsState(source, target);
  if (includePipe) {
    copyKernelRegion(
      source,
      source.kernelState.pipeBase,
      target,
      target.kernelState.pipeBase,
      source.kernel.kernel_pipe_region_bytes(),
    );
  }
  if (includeSocket) {
    copySocketState(source, target);
  }
  // Process lifecycle state is owned per-process and updated through
  // explicit kernel-worker lifecycle RPCs rather than parent/child byte-copy.
  if (includeEpoll) {
    syncEpollState(source, target);
  }
  target.kernelState = {
    ...target.kernelState,
    socketUsed: includeSocket ? source.kernelState.socketUsed : target.kernelState.socketUsed,
    pipeUsed: includePipe ? source.kernelState.pipeUsed : target.kernelState.pipeUsed,
    netBufUsed: includeSocket ? source.kernelState.netBufUsed : target.kernelState.netBufUsed,
  };
}

function adoptKernelShare(parent: ProcessHandle, child: ProcessHandle): void {
  child.kernelShare = parent.kernelShare;
  child.kernelShare.members.add(child);
}

function detachKernelShare(handle: ProcessHandle): void {
  handle.kernelShare.members.delete(handle);
}

function trackForkSharedOfds(
  handle: ProcessHandle,
  vmPtr: number,
  retainRefcounts: boolean,
): number[] {
  const retained = new Set<number>();
  for (let fd = 0; fd < handle.kernel.abi_max_fds(); fd++) {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (ofdId === 0 || retained.has(ofdId)) {
      continue;
    }
    retained.add(ofdId);
    handle.kernelShare.sharedOfdIds.add(ofdId);
    if (retainRefcounts) {
      handle.kernel.kernel_retain_ofd(ofdId);
    }
  }
  return [...retained].sort((left, right) => left - right);
}

function retainForkSharedOfds(handle: ProcessHandle): void {
  trackForkSharedOfds(handle, handle.vmPtr, true);
}

function kernelScratchBase(exports: KernelExports): number {
  return exports.kernel_heap_ptr() + 0x10000;
}

function ensureKernelMemoryCapacity(exports: KernelExports, endOffset: number): void {
  ensureMemoryCapacity(exports.memory, endOffset);
}

function writeKernelScratch(exports: KernelExports, bytes: Uint8Array, offset = 0): number {
  const scratch = kernelScratchBase(exports) + offset;
  ensureKernelMemoryCapacity(exports, scratch + bytes.length);
  new Uint8Array(exports.memory.buffer).set(bytes, scratch);
  return scratch;
}

export async function createProcess(
  wasmBytes: BufferSource,
  elfData: Uint8Array,
  options: CreateProcessOptions = {},
): Promise<ProcessHandle> {
  const ramSize = Math.max(options.ramSize ?? 16 * 1024 * 1024, 64 * 1024 * 1024);
  const stackSize = options.stackSize ?? 1024 * 1024;

  const memory = new WebAssembly.Memory({
    initial: Math.ceil(BOOTSTRAP_MEMORY_BYTES / WASM_PAGE_SIZE),
    maximum: WASM_MAX_PAGES,
    shared: true,
  });

  if (cachedModule === null) {
    if (bufferSourceByteLength(wasmBytes) === 0) {
      throw new Error("WASM module is not precompiled");
    }
    cachedModule = await WebAssembly.compile(wasmBytes);
  }
  const wasmModule = cachedModule;

  const mainInstance = await WebAssembly.instantiate(wasmModule, {
    env: createKernelEnv(memory),
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

  const runtimeId = options.runtimeId ?? 0;
  const bootstrapEntries = options.preloadEntries
    ?? getRuntimeFsState(runtimeId)?.entries
    ?? [];
  // New processes seed local MemFS from the runtime mirror; kernel-worker stays
  // authoritative once live process ownership takes over.
  preloadSnapshotEntries(exports, bootstrapEntries);
  const cwdIno = (() => {
    if (!options.cwd) {
      return 1;
    }
    const ino = resolveMemfsPath(exports, options.cwd);
    if (ino === 0) {
      throw new Error(`cwd does not exist: ${options.cwd}`);
    }
    const mode = exports.memfs_inode_mode(ino);
    if ((mode & 0o170000) !== 0o040000) {
      throw new Error(`cwd is not a directory: ${options.cwd}`);
    }
    return ino;
  })();
  const guestBase = exports.process_guest_base(ramSize);
  if (guestBase === 0) {
    throw new Error("process_guest_base failed");
  }

  const guestLimit = guestBase + ramSize;
  const elfSlack = 256 * 1024;
  const elfOffset = alignUp(guestLimit + GUEST_PAGE_SIZE, GUEST_PAGE_SIZE);
  ensureMemoryCapacity(memory, elfOffset + elfData.length + elfSlack);
  new Uint8Array(memory.buffer).set(elfData, elfOffset);

  const elfBrk = Number(exports.process_inspect_elf_brk(elfOffset, elfData.length, ramSize));
  if (elfBrk === 0) {
    throw new Error("ELF inspection failed");
  }

  exports.process_clear_args();
  const argBytes = (options.argv ?? []).map((arg) => encoder.encode(`${arg}\0`));
  const envBytes = (options.env ?? []).map((env) => encoder.encode(`${env}\0`));
  const stagedStringBytes = [...argBytes, ...envBytes]
    .reduce((total, bytes) => total + bytes.length, 0);
  const stackReserve = alignUp(stackSize, GUEST_PAGE_SIZE) + GUEST_PAGE_SIZE;
  const stringsReserve = alignUp(Math.max(stagedStringBytes, GUEST_PAGE_SIZE), GUEST_PAGE_SIZE);
  const stringsStart = alignDown(ramSize - stackReserve - stringsReserve, 16);
  if (stringsStart <= elfBrk) {
    throw new Error("argv/env do not fit in process memory");
  }
  ensureMemoryCapacity(memory, guestBase + stringsStart + stagedStringBytes);

  const memView = new Uint8Array(memory.buffer);
  let strOffset = stringsStart;
  for (const bytes of argBytes) {
    memView.set(bytes, guestBase + strOffset);
    exports.process_push_arg(strOffset);
    strOffset += bytes.length;
  }
  for (const bytes of envBytes) {
    memView.set(bytes, guestBase + strOffset);
    exports.process_push_env(strOffset);
    strOffset += bytes.length;
  }

  const vmPtr = exports.process_create(elfOffset, elfData.length, ramSize, stackSize);
  if (vmPtr === 0) {
    throw new Error("process_create failed");
  }
  exports.process_set_cwd_ino(vmPtr, cwdIno);

  const stdinPipe = exports.process_stdin_pipe(vmPtr);
  const stdoutPipe = exports.process_stdout_pipe(vmPtr);
  const stderrPipe = exports.process_stderr_pipe(vmPtr);

  ensureEvictionTimer();
  const worker = acquireWorker();

  const handle: ProcessHandle = {
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
    lastDebugMessage: undefined,
    runtimeBridge: createRuntimeStateBridge(),
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

  await registerProcessWithKernelWorker(handle);
  if (handle.kernelWorkerRequest && handle.pid > 0) {
    const parent = handle.parentPid !== null ? processHandles.get(handle.parentPid) : undefined;
    const inheritedPgid = parent ? parent.kernel.process_current_pgid(parent.vmPtr) : handle.pid;
    const inheritedSid = parent ? parent.kernel.process_current_sid(parent.vmPtr) : handle.pid;
    await initializeProcessLifecycleViaKernelWorker(
      handle,
      vmPtr,
      handle.parentPid ?? 0,
      Math.max(3, handle.pid + 1),
      inheritedPgid,
      inheritedSid,
      false,
    );
  }
  assertProcessStandardFdIntegrity(handle, "create-process");
  publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState);
  handle.kernelShare.members.add(handle);

  if (handle.pid > 0) {
    processHandles.set(handle.pid, handle);
  }

  attachThreadWorker(handle, worker, vmPtr);
  return handle;
}

export function writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void {
  if (data.length === 0) {
    return;
  }
  handle.stdinQueue.push(new Uint8Array(data));
  const changed = flushStdin(handle);
  if (!changed) {
    return;
  }
  void requestPipeStateSync(handle, true).catch((error) => {
    if (handle.pid > 0 && processHandles.get(handle.pid) === handle) {
      failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    }
  });
}

export async function injectHttpRequest(
  handle: ProcessHandle,
  port: number,
  request: string,
): Promise<string> {
  const requestBytes = encoder.encode(request);
  const scratch = handle.kernel.kernel_heap_ptr() + 0x20000;
  const chunkSize = 16 * 1024;
  const responseChunks: Uint8Array[] = [];

  new Uint8Array(handle.memory.buffer).set(requestBytes, scratch);
  const connId = handle.kernel.process_inject_http(
    handle.vmPtr,
    port,
    scratch,
    requestBytes.length,
  );
  if (connId < 0) {
    throw new Error(`no listening process on port ${port}`);
  }
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);

  const deadline = Date.now() + 10_000;
  try {
    for (;;) {
      kickBlockedWorkers(handle);
      drainProcessOutput(handle);

      const n = handle.kernel.process_read_http_response(
        handle.vmPtr,
        connId,
        scratch,
        chunkSize,
      );
      if (n < 0) {
        break;
      }
      if (n > 0) {
        responseChunks.push(new Uint8Array(handle.memory.buffer.slice(scratch, scratch + n)));
        continue;
      }
      if (Date.now() >= deadline) {
        throw new Error(`timed out waiting for HTTP response on port ${port}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  } finally {
    handle.kernel.process_close_http_connection(handle.vmPtr, connId);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
  }

  const total = responseChunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of responseChunks) {
    merged.set(chunk, offset);
    offset += chunk.length;
  }
  return decoder.decode(merged);
}

function preloadSnapshotEntries(exports: KernelExports, entries: FsSnapshotEntry[]): void {
  if (entries.length === 0) {
    return;
  }

  const ordered = [...entries].sort((left, right) => {
    if (left.kind !== right.kind) {
      return left.kind === "dir" ? -1 : 1;
    }
    return left.path.localeCompare(right.path);
  });

  for (const entry of ordered) {
    const parts = entry.path.split("/").filter(Boolean);
    if (parts.length === 0) {
      continue;
    }

    const name = parts.pop()!;
    let parentIno = 1;
    for (const dir of parts) {
      const dirBytes = encoder.encode(dir);
      const dirPtr = writeKernelScratch(exports, dirBytes);
      parentIno = exports.memfs_ensure_dir(parentIno, dirPtr, dirBytes.length, 0o755);
      if (parentIno === 0) {
        break;
      }
    }
    if (parentIno === 0) {
      continue;
    }

    const existingIno = resolveMemfsPath(exports, entry.path);
    const nameBytes = encoder.encode(name);
    const namePtr = writeKernelScratch(exports, nameBytes);
    if (entry.kind === "dir") {
      exports.memfs_ensure_dir(parentIno, namePtr, nameBytes.length, entry.mode ?? 0o755);
      continue;
    }

    const data = entry.data ?? new Uint8Array();
    const mode = entry.mode ?? 0o644;
    const ino = existingIno !== 0
      ? existingIno
      : exports.memfs_create_file(parentIno, namePtr, nameBytes.length);
    if (ino === 0) {
      continue;
    }

    const truncateResult = Number(exports.memfs_truncate_file(ino, BigInt(data.length)));
    if (truncateResult < 0) {
      continue;
    }

    if (data.length > 0) {
      const dataPtr = writeKernelScratch(exports, data, nameBytes.length + 64);
      const written = Number(exports.memfs_write_file_at(ino, 0n, dataPtr, data.length));
      if (written < 0 || written !== data.length) {
        continue;
      }
    }
    exports.memfs_set_mode(ino, mode);
  }
}

function resolveMemfsPath(exports: KernelExports, path: string): number {
  const bytes = encoder.encode(normalizePath(path));
  const scratch = writeKernelScratch(exports, bytes);
  return exports.memfs_resolve_path(scratch, bytes.length);
}

function readMemfsDirectoryNames(exports: KernelExports, dirIno: number): string[] {
  const bufPtr = kernelScratchBase(exports) + 0x20000;
  const bufLen = 64 * 1024;
  ensureKernelMemoryCapacity(exports, bufPtr + bufLen);
  const byteLen = Number(exports.memfs_readdir_names(dirIno, bufPtr, bufLen));
  if (byteLen < 0) {
    throw new Error(`memfs_readdir_names failed for ino=${dirIno}`);
  }

  const bytes = new Uint8Array(exports.memory.buffer, bufPtr, byteLen);
  const names: string[] = [];
  let start = 0;
  for (let i = 0; i < bytes.length; i++) {
    if (bytes[i] !== 0) {
      continue;
    }
    if (i > start) {
      names.push(decoder.decode(bytes.subarray(start, i)));
    }
    start = i + 1;
  }
  return names.sort();
}

function readMemfsFile(exports: KernelExports, ino: number): Uint8Array {
  const size = Number(exports.memfs_file_size(ino));
  if (size <= 0) {
    return new Uint8Array();
  }

  const bufPtr = kernelScratchBase(exports) + 0x30000;
  ensureKernelMemoryCapacity(exports, bufPtr + size);
  const read = Number(exports.memfs_read_file_to(ino, 0n, bufPtr, size));
  if (read < 0) {
    throw new Error(`memfs_read_file_to failed for ino=${ino}`);
  }
  return new Uint8Array(exports.memory.buffer.slice(bufPtr, bufPtr + read));
}

function snapshotMemfsTree(
  exports: KernelExports,
  ino: number,
  path: string,
  out: FsSnapshotEntry[],
): void {
  const normalized = normalizePath(path);
  const mode = exports.memfs_inode_mode(ino);
  const perms = mode & 0o7777;
  const fileType = mode & 0o170000;

  if (fileType === 0o040000) {
    out.push({ kind: "dir", path: normalized, mode: perms });
    for (const name of readMemfsDirectoryNames(exports, ino)) {
      const childPath = normalized === "/" ? `/${name}` : `${normalized}/${name}`;
      const childIno = resolveMemfsPath(exports, childPath);
      if (childIno === 0) {
        continue;
      }
      snapshotMemfsTree(exports, childIno, childPath, out);
    }
    return;
  }

  if (fileType !== 0o100000) {
    return;
  }

  out.push({
    kind: "file",
    path: normalized,
    data: readMemfsFile(exports, ino),
    mode: perms,
  });
}

export function snapshotProcessFs(handle: ProcessHandle): FsSnapshotEntry[] {
  const entries: FsSnapshotEntry[] = [];
  snapshotMemfsTree(handle.kernel, 1, "/", entries);
  return entries;
}

export function debugRuntimeProcessHandles(runtimeId: number): string {
  const lines = [...processHandles.values()]
    .filter((handle) => handle.runtimeId === runtimeId)
    .sort((left, right) => left.pid - right.pid)
    .map((handle) => {
      const processPtr = handle.kernel.thread_process_ptr(handle.vmPtr);
      const blocked = [...handle.blockedWorkers.values()]
        .map((state) => `${state.status}@0x${state.vmPtr.toString(16)}`)
        .join(",");
      const attachedWorkerVmPtrs = [...handle.workers]
        .map((worker) => threadWorkerStates.get(worker)?.vmPtr ?? 0)
        .filter((vmPtr) => vmPtr !== 0)
        .map((vmPtr) => `0x${vmPtr.toString(16)}`)
        .join(",");
      const liveThreadVmPtrs = Array.from({ length: handle.kernel.abi_max_threads() }, (_value, slot) =>
        handle.kernel.process_thread_vm_ptr(handle.vmPtr, slot))
        .filter((vmPtr) => vmPtr !== 0)
        .map((vmPtr) => `0x${vmPtr.toString(16)}`)
        .join(",");
      const fdSummary = Array.from({ length: 16 }, (_, fd) => {
        const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
        if (ofdId === 0) {
          return null;
        }
        return `${fd}->${ofdId}/${handle.kernel.kernel_ofd_fd_type(ofdId)}/${handle.kernel.kernel_ofd_ino(ofdId)}/${handle.kernel.kernel_ofd_host_fd(ofdId)}`;
      }).filter(Boolean).join(" ");
      const ofdSummary = Array.from({ length: 16 }, (_value, ofdId) => {
        if (ofdId === 0) {
          return null;
        }
        const type = handle.kernel.kernel_ofd_fd_type(ofdId);
        if (type === 0) {
          return null;
        }
        return `${ofdId}:${type}/${handle.kernel.kernel_ofd_ino(ofdId)}/${handle.kernel.kernel_ofd_host_fd(ofdId)}/rc=${handle.kernel.kernel_ofd_refcount(ofdId)}`;
      }).filter(Boolean).join(" ");
      return [
        `pid=${handle.pid}`,
        `ppid=${handle.parentPid ?? 0}`,
        `vm=0x${handle.vmPtr.toString(16)}`,
        `process=0x${processPtr.toString(16)}`,
        `workers=${handle.workers.size}`,
        `worker_vms=[${attachedWorkerVmPtrs}]`,
        `thread_vms=[${liveThreadVmPtrs}]`,
        `ready=${handle.readyWorkers.length}`,
        `running=${handle.runningWorker ? "1" : "0"}`,
        `blocked=[${blocked}]`,
        `vfork=${handle.vforkSnapshot ? "1" : "0"}`,
        `ofd_table=0x${handle.kernelState.ofdTablePtr.toString(16)}`,
        `memfs=0x${handle.kernelState.memfsPtr.toString(16)}`,
        `pipe=0x${handle.kernelState.pipeBase.toString(16)}`,
        `process_runtime=0x${handle.kernelState.processRuntimeStatePtr.toString(16)}`,
        `worker_log=[${handle.workerDebugLog.join(", ")}]`,
        `fds=[${fdSummary}]`,
        `ofds=[${ofdSummary}]`,
        `debug=${handle.lastDebugMessage ?? "n/a"}`,
      ].join(" ");
    });
  return lines.join("\n");
}

function assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void {
  if (handle.pid <= 0) {
    return;
  }
  for (const fd of [0, 1, 2]) {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId === 0) {
      continue;
    }
    const type = handle.kernel.kernel_ofd_fd_type(ofdId);
    if (type > 0 && type <= 64) {
      continue;
    }
    throw new Error(
      `${context}: pid=${handle.pid} fd=${fd} ofd=${ofdId} type=${type}\n`
      + debugRuntimeProcessHandles(handle.runtimeId),
    );
  }
}

function snapshotEntriesEqual(left: FsSnapshotEntry[], right: FsSnapshotEntry[]): boolean {
  if (left.length !== right.length) {
    return false;
  }

  for (let i = 0; i < left.length; i++) {
    const a = left[i];
    const b = right[i];
    if (!a || !b || a.kind !== b.kind || a.path !== b.path || a.mode !== b.mode) {
      return false;
    }
    if (a.kind === "file" && b.kind === "file") {
      const aData = a.data ?? new Uint8Array();
      const bData = b.data ?? new Uint8Array();
      if (aData.length !== bData.length) {
        return false;
      }
      for (let j = 0; j < aData.length; j++) {
        if (aData[j] !== bData[j]) {
          return false;
        }
      }
    }
  }

  return true;
}

function applyFsEntryToHandle(handle: ProcessHandle, entry: FsSnapshotEntry): void {
  const exports = handle.kernel;
  const parts = normalizePath(entry.path).split("/").filter(Boolean);
  if (parts.length === 0) {
    return;
  }

  const name = parts.pop()!;
  let parentIno = 1;
  for (const dir of parts) {
    const dirBytes = encoder.encode(dir);
    const dirPtr = writeKernelScratch(exports, dirBytes);
    parentIno = exports.memfs_ensure_dir(parentIno, dirPtr, dirBytes.length, 0o755);
    if (parentIno === 0) {
      return;
    }
  }

  const nameBytes = encoder.encode(name);
  const namePtr = writeKernelScratch(exports, nameBytes);
  if (entry.kind === "dir") {
    exports.memfs_ensure_dir(parentIno, namePtr, nameBytes.length, entry.mode ?? 0o755);
    return;
  }

  const data = entry.data ?? new Uint8Array();
  const mode = entry.mode ?? 0o644;
  const existingIno = resolveMemfsPath(exports, entry.path);
  const ino = existingIno !== 0
    ? existingIno
    : exports.memfs_create_file(parentIno, namePtr, nameBytes.length);
  if (ino === 0) {
    return;
  }

  const truncateResult = Number(exports.memfs_truncate_file(ino, BigInt(data.length)));
  if (truncateResult < 0) {
    return;
  }

  if (data.length > 0) {
    const dataPtr = writeKernelScratch(exports, data, nameBytes.length + 64);
    const written = Number(exports.memfs_write_file_at(ino, 0n, dataPtr, data.length));
    if (written < 0 || written !== data.length) {
      return;
    }
  }
  exports.memfs_set_mode(ino, mode);
}

function forEachRuntimeKernelShare(
  runtimeId: number,
  fn: (representative: ProcessHandle) => void,
): void {
  if (runtimeId <= 0) {
    return;
  }

  const visited = new Set<KernelShareGroup>();
  for (const handle of processHandles.values()) {
    if (handle.runtimeId !== runtimeId) {
      continue;
    }
    const representative = kernelShareRepresentative(handle) ?? handle;
    const group = representative.kernelShare;
    if (visited.has(group)) {
      continue;
    }
    visited.add(group);
    fn(representative);
  }
}

export function mirrorRuntimeFsEntry(runtimeId: number, entry: FsSnapshotEntry): void {
  forEachRuntimeKernelShare(runtimeId, (representative) => {
    applyFsEntryToHandle(representative, entry);
    for (const member of kernelShareOtherMembers(representative)) {
      syncMemfsState(representative, member);
      attachKernelRuntimeState(member.kernel, member.kernelState);
    }
  });
}

export function replaceRuntimeFsSnapshot(runtimeId: number, entries: FsSnapshotEntry[]): void {
  runtimeFsDirtyHandles.delete(runtimeId);
  forEachRuntimeKernelShare(runtimeId, (representative) => {
    representative.kernel.kernel_reset_memfs();
    const rootEntry = entries.find((entry) => entry.kind === "dir" && normalizePath(entry.path) === "/");
    if (rootEntry) {
      representative.kernel.memfs_set_mode(1, rootEntry.mode ?? 0o755);
    }
    preloadSnapshotEntries(representative.kernel, entries);
    for (const member of kernelShareOtherMembers(representative)) {
      syncMemfsState(representative, member);
      attachKernelRuntimeState(member.kernel, member.kernelState);
    }
  });
}

function flushStdin(handle: ProcessHandle): boolean {
  if (handle.stdinPipe === 0) {
    return false;
  }

  let wrote = false;
  while (handle.stdinQueue.length > 0) {
    const chunk = handle.stdinQueue[0];
    const result = writePipe(handle.memory, handle.kernelState.pipeBase, handle.stdinPipe, chunk);
    if (result.broken) {
      handle.stdinQueue.length = 0;
      return wrote;
    }
    if (result.written === 0) {
      return wrote;
    }
    wrote = true;
    if (result.written >= chunk.length) {
      handle.stdinQueue.shift();
    } else {
      handle.stdinQueue[0] = chunk.subarray(result.written);
      return wrote;
    }
  }
  return wrote;
}

function markRuntimeFsDirty(handle: ProcessHandle): void {
  if (handle.runtimeId <= 0) {
    return;
  }
  let dirtyHandles = runtimeFsDirtyHandles.get(handle.runtimeId);
  if (!dirtyHandles) {
    dirtyHandles = new Set();
    runtimeFsDirtyHandles.set(handle.runtimeId, dirtyHandles);
  }
  dirtyHandles.add(handle);
}

async function registerProcessWithKernelWorker(
  handle: ProcessHandle,
  options: { seedMemfsFromCanonical?: boolean } = {},
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }
  const response = await handle.kernelWorkerRequest({
    type: "register-process",
    pid: handle.pid,
    memory: handle.memory,
    kernelState: handle.kernelState,
    seedMemfsFromCanonical: options.seedMemfsFromCanonical ?? true,
  });
  if (response.type !== "process-registered") {
    throw new Error(`unexpected kernel-worker response for register-process: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

async function initializeProcessLifecycleViaKernelWorker(
  handle: ProcessHandle,
  vmPtr: number,
  ppid: number,
  nextPid: number,
  pgid: number,
  sid: number,
  didExec: boolean,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }

  const response = await handle.kernelWorkerRequest({
    type: "initialize-process-lifecycle",
    pid: handle.pid,
    vmPtr,
    ppid,
    nextPid,
    pgid,
    sid,
    didExec,
    kernelState: handle.kernelState,
  });
  if (response.type !== "process-lifecycle-initialized") {
    throw new Error(`unexpected kernel-worker response for initialize-process-lifecycle: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  handle.kernel.process_reset_lifecycle(
    vmPtr,
    handle.pid,
    ppid,
    nextPid,
    pgid,
    sid,
    didExec ? 1 : 0,
  );
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

async function canonicalizeChildLifecycleViaKernelWorker(
  handle: ProcessHandle,
  vmPtr: number,
  provisionalChildPid: number,
): Promise<CanonicalizedChildLifecycle> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return {
      childPid: provisionalChildPid,
      nextPid: provisionalChildPid + 1,
      pgid: handle.kernel.process_current_pgid(vmPtr),
      sid: handle.kernel.process_current_sid(vmPtr),
    };
  }

  const response = await handle.kernelWorkerRequest({
    type: "canonicalize-child-pid",
    pid: handle.pid,
    vmPtr,
    provisionalChildPid,
    kernelState: handle.kernelState,
  });
  if (response.type !== "child-pid-canonicalized") {
    throw new Error(`unexpected kernel-worker response for canonicalize-child-pid: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  return {
    childPid: response.childPid,
    nextPid: response.nextPid,
    pgid: response.pgid,
    sid: response.sid,
  };
}

function unregisterProcessWithKernelWorker(handle: ProcessHandle): void {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }
  void handle.kernelWorkerRequest({
    type: "unregister-process",
    pid: handle.pid,
  }).catch(() => {});
}

async function resumeFsViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    handle.kernel.thread_process_fs(vmPtr);
    handle.kernel.thread_resume_fs(vmPtr);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    syncForkSharedStateLocally(handle, kernelShareOtherMembers(handle));
    markRuntimeFsDirty(handle);
    kickBlockedWorkers(handle);
    enqueueWorker(handle, worker);
    return;
  }

  try {
    assertProcessStandardFdIntegrity(handle, "resume-fs-before");
    const response = await handle.kernelWorkerRequest({
      type: "process-fs",
      pid: handle.pid,
      vmPtr,
      kernelState: handle.kernelState,
    });
    if (response.type !== "fs-processed") {
      throw new Error(`unexpected kernel-worker response for process-fs: ${response.type}`);
    }
    if (!handle.workers.has(worker) || !threadWorkerStates.has(worker)) {
      return;
    }
    handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    assertProcessStandardFdIntegrity(handle, "resume-fs");
    markRuntimeFsDirty(handle);
    await syncForkSharedStateViaKernelWorker(handle, kernelShareOtherMembers(handle));
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    kickBlockedWorkers(handle);
    enqueueWorker(handle, worker);
  } catch (error) {
    if (!handle.workers.has(worker) || !threadWorkerStates.has(worker)) {
      return;
    }
    failProcess(handle, error instanceof Error ? error : new Error(String(error)));
  }
}

async function recordChildExitViaKernelWorker(
  parent: ProcessHandle,
  childPid: number,
  waitStatus: number,
): Promise<void> {
  if (!parent.kernelWorkerRequest || parent.pid <= 0) {
    parent.kernel.process_record_child_exit(childPid, waitStatus);
    parent.kernelState = readKernelRuntimeState(parent.kernel);
    publishKernelRuntimeState(parent.runtimeBridge, parent.kernelState, true);
    kickBlockedWorkers(parent);
    return;
  }

  try {
    const response = await parent.kernelWorkerRequest({
      type: "record-child-exit",
      pid: parent.pid,
      childPid,
      waitStatus,
      kernelState: parent.kernelState,
    });
    if (response.type !== "child-exit-recorded") {
      throw new Error(`unexpected kernel-worker response for record-child-exit: ${response.type}`);
    }
    if (parent.pid <= 0 || processHandles.get(parent.pid) !== parent) {
      return;
    }
    parent.kernelState = adoptKernelState(parent.kernelState, response.kernelState);
    attachKernelRuntimeState(parent.kernel, parent.kernelState);
    publishKernelRuntimeState(parent.runtimeBridge, parent.kernelState, true);
    kickBlockedWorkers(parent);
  } catch (error) {
    if (parent.pid > 0 && processHandles.get(parent.pid) === parent) {
      failProcess(parent, error instanceof Error ? error : new Error(String(error)));
    }
  }
}

async function resumeConnectViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  result: bigint,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    handle.kernel.thread_resume_connect_result(vmPtr, result);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    kickBlockedWorkers(handle);
    enqueueWorker(handle, worker);
    return;
  }

  try {
    const response = await handle.kernelWorkerRequest({
      type: "resume-connect",
      pid: handle.pid,
      vmPtr,
      result,
      kernelState: handle.kernelState,
    });
    if (response.type !== "connect-resumed") {
      throw new Error(`unexpected kernel-worker response for resume-connect: ${response.type}`);
    }
    if (!handle.workers.has(worker) || !threadWorkerStates.has(worker)) {
      return;
    }
    handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    kickBlockedWorkers(handle);
    enqueueWorker(handle, worker);
  } catch (error) {
    if (handle.workers.has(worker) && threadWorkerStates.has(worker)) {
      failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    }
  }
}

async function resumeWait4ViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }

  try {
    const response = await handle.kernelWorkerRequest({
      type: "resume-wait4",
      pid: handle.pid,
      vmPtr,
      kernelState: handle.kernelState,
    });
    if (response.type !== "wait4-resumed") {
      throw new Error(`unexpected kernel-worker response for resume-wait4: ${response.type}`);
    }

    const blocked = handle.blockedWorkers.get(worker);
    if (!blocked || blocked.vmPtr !== vmPtr) {
      return;
    }
    blocked.rpcPending = false;

    if (!handle.workers.has(worker) || !threadWorkerStates.has(worker)) {
      return;
    }

    handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);

    handleBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr));
  } catch (error) {
    const blocked = handle.blockedWorkers.get(worker);
    if (blocked && blocked.vmPtr === vmPtr) {
      blocked.rpcPending = false;
    }
    if (handle.workers.has(worker) && threadWorkerStates.has(worker)) {
      failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    }
  }
}

async function resumeEpollWaitViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  timeoutExpired: boolean,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }

  try {
    const response = await handle.kernelWorkerRequest({
      type: "resume-epoll-wait",
      pid: handle.pid,
      vmPtr,
      timeoutExpired,
      kernelState: handle.kernelState,
    });
    if (response.type !== "epoll-wait-resumed") {
      throw new Error(`unexpected kernel-worker response for resume-epoll-wait: ${response.type}`);
    }

    const blocked = handle.blockedWorkers.get(worker);
    if (!blocked || blocked.vmPtr !== vmPtr) {
      return;
    }
    blocked.rpcPending = false;
    blocked.timeoutExpired = false;

    if (!handle.workers.has(worker) || !threadWorkerStates.has(worker)) {
      return;
    }

    handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);

    handleBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr));
  } catch (error) {
    const blocked = handle.blockedWorkers.get(worker);
    if (blocked && blocked.vmPtr === vmPtr) {
      blocked.rpcPending = false;
      blocked.timeoutExpired = false;
    }
    if (handle.workers.has(worker) && threadWorkerStates.has(worker)) {
      failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    }
  }
}

async function resumeBlockedSyscallLocally(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  blockedStatus: number,
): Promise<void> {
  try {
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    handle.kernel.thread_resume_blocked_syscall(vmPtr);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    await syncBlockedForkSharedStateViaKernelWorker(handle);

    if (blockedStatus === handle.kernel.abi_status_pipe_wait()) {
      await syncPipeStateViaKernelWorker(handle);
    } else if (
      blockedStatus === handle.kernel.abi_status_socket_recv()
      || blockedStatus === handle.kernel.abi_status_socket_send()
    ) {
      await syncSocketStateViaKernelWorker(handle);
    }

    const blocked = handle.blockedWorkers.get(worker);
    if (!blocked || blocked.vmPtr !== vmPtr) {
      return;
    }
    blocked.rpcPending = false;

    if (!handle.workers.has(worker) || !threadWorkerStates.has(worker)) {
      return;
    }

    handleBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr));
  } catch (error) {
    const blocked = handle.blockedWorkers.get(worker);
    if (blocked && blocked.vmPtr === vmPtr) {
      blocked.rpcPending = false;
    }
    if (handle.workers.has(worker) && threadWorkerStates.has(worker)) {
      const fd = Number(handle.kernel.thread_a0(vmPtr));
      const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      const debug = [
        `vm=0x${vmPtr.toString(16)}`,
        `fd=${fd}`,
        `buf=${handle.kernel.thread_a1(vmPtr).toString()}`,
        `process_ptr=0x${handle.kernel.thread_process_ptr(vmPtr).toString(16)}`,
        `ofd_table_ptr=0x${handle.kernel.kernel_ofd_table_ptr().toString(16)}`,
        `ofd_id=${ofdId}`,
        `ofd_type=${handle.kernel.kernel_ofd_fd_type(ofdId)}`,
        `ofd_host_fd=${handle.kernel.kernel_ofd_host_fd(ofdId)}`,
        `ofd_flags=${handle.kernel.kernel_ofd_status_flags(ofdId)}`,
        `ofd_ino=${handle.kernel.kernel_ofd_ino(ofdId)}`,
        `ofd_offset=${handle.kernel.kernel_ofd_offset(ofdId).toString()}`,
        `blocked_fd=${handle.kernel.thread_blocked_fd(vmPtr)}`,
        `blocked_ofd_id=${handle.kernel.thread_blocked_ofd_id(vmPtr)}`,
      ].join(" ");
      failProcess(handle, new Error(`${detail}\n${debug}`));
    }
  }
}

async function resumeVforkParentViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  waitStatus: number,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }

  try {
    const response = await handle.kernelWorkerRequest({
      type: "resume-vfork-parent",
      pid: handle.pid,
      vmPtr,
      waitStatus,
      kernelState: handle.kernelState,
    });
    if (response.type !== "vfork-parent-resumed") {
      throw new Error(`unexpected kernel-worker response for resume-vfork-parent: ${response.type}`);
    }
    if (response.childPid <= 0) {
      throw new Error(`kernel-worker did not report an active vfork child for pid=${handle.pid}`);
    }
    if (!handle.workers.has(worker) || !threadWorkerStates.has(worker)) {
      return;
    }

    handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    handle.kernel.thread_resume_vfork_parent(vmPtr, response.childPid);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    clearVforkSnapshot(handle);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    kickBlockedWorkers(handle);
    enqueueWorker(handle, worker);
  } catch (error) {
    if (handle.workers.has(worker) && threadWorkerStates.has(worker)) {
      failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    }
  }
}

async function canonicalizeActiveVforkChildViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  provisionalChildPid: number,
): Promise<void> {
  const snapshot = handle.vforkSnapshot;
  if (!snapshot || snapshot.vmPtr !== vmPtr) {
    return;
  }

  try {
    const lifecycle = await canonicalizeChildLifecycleViaKernelWorker(handle, vmPtr, provisionalChildPid);
    if (!handle.workers.has(worker) || !threadWorkerStates.has(worker)) {
      return;
    }

    snapshot.childPid = lifecycle.childPid;
    snapshot.nextPid = lifecycle.nextPid;
    snapshot.pgid = lifecycle.pgid;
    snapshot.sid = lifecycle.sid;
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    enqueueWorker(handle, worker);
  } catch (error) {
    if (handle.workers.has(worker) && threadWorkerStates.has(worker)) {
      failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    }
  }
}

async function resumeVforkExecveParentViaKernelWorker(
  handle: ProcessHandle,
  vmPtr: number,
  childPid: number,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }

  const response = await handle.kernelWorkerRequest({
    type: "resume-vfork-execve-parent",
    pid: handle.pid,
    vmPtr,
    childPid,
    kernelState: handle.kernelState,
  });
  if (response.type !== "vfork-execve-parent-resumed") {
    throw new Error(`unexpected kernel-worker response for resume-vfork-execve-parent: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  handle.kernel.thread_clear_spawn_handoff(vmPtr);
  handle.kernel.thread_resume_vfork_parent(vmPtr, childPid);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

async function syncProcessLifecycleViaKernelWorker(
  handle: ProcessHandle,
  vmPtr: number,
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }

  const response = await handle.kernelWorkerRequest({
    type: "sync-process-lifecycle",
    pid: handle.pid,
    vmPtr,
    kernelState: handle.kernelState,
  });
  if (response.type !== "process-lifecycle-synced") {
    throw new Error(`unexpected kernel-worker response for sync-process-lifecycle: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

function syncForkSharedStateLocally(
  handle: ProcessHandle,
  targets: ProcessHandle[],
): void {
  const sharedOfdIds = activeKernelShareOfdIds(handle);
  mergeKernelShareOfdStateInto(handle, sharedOfdIds);
  reconcileForkSharedOfdRefcounts(handle, sharedOfdIds);
  for (const target of targets) {
    copyLegacySharedKernelObjectsLocal(handle, target, sharedOfdIds);
    attachKernelRuntimeState(target.kernel, target.kernelState);
  }
}

function collectProcessHeldOfds(handle: ProcessHandle): Set<number> {
  const maxFds = handle.kernel.abi_max_fds();
  const heldOfds = new Set<number>();

  for (let fd = 0; fd < maxFds; fd++) {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId !== 0 && ofdId < maxFds) {
      heldOfds.add(ofdId);
    }
  }

  for (let slot = 0; slot < handle.kernel.abi_max_threads(); slot++) {
    const threadVmPtr = handle.kernel.process_thread_vm_ptr(handle.vmPtr, slot);
    if (threadVmPtr === 0) {
      continue;
    }
    const blockedOfdId = handle.kernel.thread_blocked_ofd_id?.(threadVmPtr) ?? 0;
    if (blockedOfdId !== 0 && blockedOfdId < maxFds) {
      heldOfds.add(blockedOfdId);
    }
  }

  return heldOfds;
}

function hasPlausibleOfdState(handle: KernelSyncHandle, ofdId: number): boolean {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return false;
  }
  const type = handle.kernel.kernel_ofd_fd_type(ofdId);
  return type > 0 && type <= 64;
}

function snapshotKernelShareOfdSlots(
  handle: ProcessHandle,
  ofdIds: Iterable<number>,
): Map<number, Uint8Array> {
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }

  const snapshots = new Map<number, Uint8Array>();
  const members = [handle, ...[...handle.kernelShare.members].filter((member) => member !== handle)];
  for (const ofdId of ofdIds) {
    if (ofdId <= 0 || ofdId >= maxFds || snapshots.has(ofdId)) {
      continue;
    }
    for (const member of members) {
      if (!hasPlausibleOfdState(member, ofdId)) {
        continue;
      }
      const slotOffset = member.kernelState.ofdTablePtr + ofdId * slotBytes;
      snapshots.set(ofdId, new Uint8Array(member.memory.buffer, slotOffset, slotBytes).slice());
      break;
    }
  }
  return snapshots;
}

function activeKernelShareOfdIds(handle: ProcessHandle): number[] {
  const heldByAnyMember = new Set<number>();
  for (const member of handle.kernelShare.members) {
    for (const ofdId of collectProcessHeldOfds(member)) {
      heldByAnyMember.add(ofdId);
    }
  }

  const active: number[] = [];
  for (const ofdId of handle.kernelShare.sharedOfdIds) {
    if (heldByAnyMember.has(ofdId)) {
      active.push(ofdId);
      continue;
    }
    handle.kernelShare.sharedOfdIds.delete(ofdId);
  }
  return active.sort((left, right) => left - right);
}

function mergeKernelShareOfdStateInto(handle: ProcessHandle, sharedOfdIds: Iterable<number>): void {
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }

  const slotCopies = new Map<number, Uint8Array>();
  const sourceFirstMembers = [
    handle,
    ...[...handle.kernelShare.members].filter((member) => member !== handle),
  ];
  for (const ofdId of sharedOfdIds) {
    for (const member of sourceFirstMembers) {
      if (
        slotCopies.has(ofdId)
        || !collectProcessHeldOfds(member).has(ofdId)
        || !hasPlausibleOfdState(member, ofdId)
      ) {
        continue;
      }
      const slotOffset = member.kernelState.ofdTablePtr + ofdId * slotBytes;
      slotCopies.set(ofdId, new Uint8Array(member.memory.buffer, slotOffset, slotBytes).slice());
      break;
    }
  }

  const targetTable = new Uint8Array(handle.memory.buffer);
  for (const [ofdId, slotBytesView] of slotCopies) {
    const slotOffset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
    targetTable.set(slotBytesView, slotOffset);
  }
}

function reconcileForkSharedOfdRefcounts(handle: ProcessHandle, sharedOfdIds: Iterable<number>): void {
  const maxFds = handle.kernel.abi_max_fds();
  const sharedOfdIdSet = new Set(sharedOfdIds);
  const expectedRefcounts = new Int32Array(maxFds);

  for (const member of handle.kernelShare.members) {
    for (const ofdId of collectProcessHeldOfds(member)) {
      if (!sharedOfdIdSet.has(ofdId)) {
        continue;
      }
      expectedRefcounts[ofdId] += 1;
    }
  }

  for (const ofdId of sharedOfdIdSet) {
    if (ofdId <= 0 || ofdId >= maxFds) {
      continue;
    }
    let currentRefcount = handle.kernel.kernel_ofd_refcount(ofdId);
    const expectedRefcount = expectedRefcounts[ofdId] ?? 0;
    while (currentRefcount < expectedRefcount) {
      handle.kernel.kernel_retain_ofd(ofdId);
      currentRefcount += 1;
    }
    while (currentRefcount > expectedRefcount) {
      handle.kernel.kernel_release_ofd(ofdId);
      currentRefcount -= 1;
    }
  }
}

async function syncForkSharedStateViaKernelWorker(
  handle: ProcessHandle,
  targets: ProcessHandle[],
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    syncForkSharedStateLocally(handle, targets);
    return;
  }

  const sharedOfdIds = activeKernelShareOfdIds(handle);
  mergeKernelShareOfdStateInto(handle, sharedOfdIds);
  reconcileForkSharedOfdRefcounts(handle, sharedOfdIds);
  const ofdSlots: KernelWorkerOfdSlotSnapshot[] = [...snapshotKernelShareOfdSlots(handle, sharedOfdIds)]
    .map(([ofdId, slot]) => ({ ofdId, slot }));
  const targetPids = targets.map((target) => target.pid).filter((pid) => pid > 0);
  const targetKernelStates: KernelWorkerPidStateSnapshot[] = targets
    .filter((target) => target.pid > 0)
    .map((target) => ({
      pid: target.pid,
      kernelState: target.kernelState,
    }));
  const response = await handle.kernelWorkerRequest({
    type: "sync-fork-shared-state",
    pid: handle.pid,
    targetPids,
    targetKernelStates,
    ofdIds: sharedOfdIds,
    ofdSlots,
    kernelState: handle.kernelState,
  });
  if (response.type !== "fork-shared-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-fork-shared-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

async function rehydrateForkSharedStateViaKernelWorker(
  targets: ProcessHandle[],
  ofdIds: number[],
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
    .map((target) => ({
      pid: target.pid,
      kernelState: target.kernelState,
    }));
  if (targetPids.length === 0) {
    return;
  }
  const response = await owner.kernelWorkerRequest({
    type: "rehydrate-fork-shared-state",
    targetPids,
    targetKernelStates,
    ofdIds,
  });
  if (response.type !== "fork-shared-state-rehydrated") {
    throw new Error(
      `unexpected kernel-worker response for rehydrate-fork-shared-state: ${response.type}`,
    );
  }
}

function blockedStatusUsesForkSharedStateSync(handle: ProcessHandle, status: number): boolean {
  return status === handle.kernel.abi_status_pipe_wait()
    || status === handle.kernel.abi_status_socket_recv()
    || status === handle.kernel.abi_status_socket_send()
    || status === handle.kernel.abi_status_flock_wait();
}

async function syncBlockedForkSharedStateViaKernelWorker(handle: ProcessHandle): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }
  await syncForkSharedStateViaKernelWorker(handle, kernelShareOtherMembers(handle));
}

async function syncPipeStateViaKernelWorker(handle: ProcessHandle): Promise<bigint> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return handle.kernelState.pipeUsed;
  }

  const response = await handle.kernelWorkerRequest({
    type: "sync-pipe-state",
    pid: handle.pid,
    kernelState: handle.kernelState,
  });
  if (response.type !== "pipe-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-pipe-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  return handle.kernelState.pipeUsed;
}

async function syncEpollStateViaKernelWorker(handle: ProcessHandle): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }

  const response = await handle.kernelWorkerRequest({
    type: "sync-epoll-state",
    pid: handle.pid,
    kernelState: handle.kernelState,
  });
  if (response.type !== "epoll-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-epoll-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

async function syncSocketStateViaKernelWorker(handle: ProcessHandle): Promise<{
  socketUsed: number;
  netBufUsed: bigint;
}> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return {
      socketUsed: handle.kernelState.socketUsed,
      netBufUsed: handle.kernelState.netBufUsed,
    };
  }

  const response = await handle.kernelWorkerRequest({
    type: "sync-socket-state",
    pid: handle.pid,
    kernelState: handle.kernelState,
  });
  if (response.type !== "socket-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-socket-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  return {
    socketUsed: handle.kernelState.socketUsed,
    netBufUsed: handle.kernelState.netBufUsed,
  };
}

async function releaseHostStdioViaKernelWorker(handle: ProcessHandle): Promise<bigint> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    handle.kernel.process_release_host_stdio(handle.vmPtr);
    return handle.kernelState.pipeUsed;
  }

  const response = await handle.kernelWorkerRequest({
    type: "release-host-stdio",
    pid: handle.pid,
    vmPtr: handle.vmPtr,
    kernelState: handle.kernelState,
  });
  if (response.type !== "host-stdio-released") {
    throw new Error(`unexpected kernel-worker response for release-host-stdio: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  return handle.kernelState.pipeUsed;
}

function drainPipeOutput(
  handle: ProcessHandle,
  pipeSlot: number,
): boolean {
  if (pipeSlot === 0) {
    return false;
  }

  let drained = false;
  for (;;) {
    const { data } = readPipe(handle.memory, handle.kernelState.pipeBase, pipeSlot);
    if (data.length === 0) {
      return drained;
    }
    drained = true;
    if (handle.onStdout) {
      handle.onStdout(data);
    } else {
      handle.outputBacklog.push(new Uint8Array(data));
    }
  }
}

function drainProcessOutput(handle: ProcessHandle): boolean {
  const stdoutDrained = drainPipeOutput(handle, handle.stdoutPipe);
  const stderrDrained = drainPipeOutput(handle, handle.stderrPipe);
  return stdoutDrained || stderrDrained;
}

function createInheritedStdoutSink(parent: ProcessHandle): (data: Uint8Array) => void {
  return (data: Uint8Array) => {
    if (parent.onStdout) {
      parent.onStdout(data);
      return;
    }
    parent.outputBacklog.push(new Uint8Array(data));
  };
}

function createInheritedErrorSink(parent: ProcessHandle, childPid: number): (error: Error) => void {
  return (error: Error) => {
    if (parent.pid <= 0 || processHandles.get(parent.pid) !== parent) {
      return;
    }
    failProcess(parent, new Error(`child pid=${childPid}: ${error.message}`));
  };
}

function captureVforkSnapshot(handle: ProcessHandle, vmPtr: number): VforkSnapshot {
  const ofdIds = trackForkSharedOfds(handle, vmPtr, false);
  return {
    vmPtr,
    childPid: null,
    nextPid: null,
    pgid: null,
    sid: null,
    inheritedSharedOfdIds: ofdIds,
  };
}

function clearVforkSnapshot(handle: ProcessHandle): void {
  handle.vforkSnapshot = null;
  handle.vforkWorker = null;
}

function retainSharedHostStdio(shared: SharedHostStdio): SharedHostStdio {
  shared.refCount += 1;
  return shared;
}

function releaseSharedHostStdioLocal(handle: ProcessHandle): void {
  const shared = handle.hostStdio;
  if (shared.refCount > 0) {
    shared.refCount -= 1;
  }
  if (shared.refCount !== 0) {
    return;
  }
  handle.kernel.process_release_host_stdio(handle.vmPtr);
  handle.stdinPipe = 0;
  handle.stdoutPipe = 0;
  handle.stderrPipe = 0;
}

async function releaseSharedHostStdio(handle: ProcessHandle): Promise<void> {
  const shared = handle.hostStdio;
  if (shared.refCount > 0) {
    shared.refCount -= 1;
  }
  if (shared.refCount !== 0) {
    return;
  }
  const pipeUsed = await releaseHostStdioViaKernelWorker(handle);
  handle.stdinPipe = 0;
  handle.stdoutPipe = 0;
  handle.stderrPipe = 0;
  if (kernelWorkerOwnsPipeState(handle)) {
    wakePipeAndEpollWaitersInKernelShare(handle, pipeUsed);
  }
}

function encodeWaitStatus(exitCode: number, termSignal: number): number {
  if (termSignal !== 0) {
    return termSignal & 0x7f;
  }
  return (exitCode & 0xff) << 8;
}

function adoptKernelState(
  current: KernelRuntimeState,
  incoming: KernelRuntimeState,
): KernelRuntimeState {
  return {
    ...current,
    heapPtr: Math.max(current.heapPtr, incoming.heapPtr),
    socketUsed: incoming.socketUsed,
    pipeUsed: incoming.pipeUsed,
    netBufUsed: incoming.netBufUsed,
  };
}

function resumeVforkParentAfterChildExit(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  exitCode: number,
  termSignal: number,
): boolean {
  const snapshot = handle.vforkSnapshot;
  if (snapshot === null || snapshot.vmPtr !== vmPtr) {
    return false;
  }

  const waitStatus = encodeWaitStatus(exitCode, termSignal);

  if (handle.kernelWorkerRequest && handle.pid > 0) {
    void resumeVforkParentViaKernelWorker(handle, worker, vmPtr, waitStatus);
    return true;
  }

  const childPid = handle.kernel.thread_vfork_child_pid();
  if (childPid <= 0) {
    return false;
  }

  void recordChildExitViaKernelWorker(handle, childPid, waitStatus);
  clearVforkSnapshot(handle);
  handle.kernel.thread_resume_vfork_parent(vmPtr, childPid);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
  kickBlockedWorkers(handle);
  enqueueWorker(handle, worker);
  return true;
}

function hasExclusiveVforkWorker(handle: ProcessHandle): boolean {
  return handle.vforkSnapshot !== null && handle.vforkWorker !== null;
}

function canScheduleWorker(handle: ProcessHandle, worker: Worker): boolean {
  return !hasExclusiveVforkWorker(handle) || handle.vforkWorker === worker;
}

function clearBlockedWorker(handle: ProcessHandle, worker: Worker): void {
  const blocked = handle.blockedWorkers.get(worker);
  if (!blocked) {
    return;
  }
  if (blocked.retryTimer !== null) {
    clearTimeout(blocked.retryTimer);
  }
  handle.blockedWorkers.delete(worker);
}

function collectLiveThreadVmPtrs(handle: ProcessHandle, vmPtr: number): Set<number> {
  const liveVmPtrs = new Set<number>();
  for (let slot = 0; slot < handle.kernel.abi_max_threads(); slot++) {
    const liveVmPtr = handle.kernel.process_thread_vm_ptr(vmPtr, slot);
    if (liveVmPtr !== 0) {
      liveVmPtrs.add(liveVmPtr);
    }
  }
  return liveVmPtrs;
}

function selectProcessAnchorVmPtr(handle: ProcessHandle, excludedWorker: Worker | null = null): number | null {
  const candidates: Worker[] = [];
  if (handle.runningWorker !== null) {
    candidates.push(handle.runningWorker);
  }
  candidates.push(...handle.readyWorkers, ...handle.workers);
  const seen = new Set<Worker>();
  for (const candidate of candidates) {
    if (candidate === excludedWorker) {
      continue;
    }
    if (seen.has(candidate)) {
      continue;
    }
    seen.add(candidate);
    const vmPtr = threadWorkerStates.get(candidate)?.vmPtr;
    if (vmPtr !== undefined && vmPtr !== 0) {
      return vmPtr;
    }
  }
  return null;
}

function removeReadyWorker(handle: ProcessHandle, worker: Worker): void {
  for (let index = handle.readyWorkers.length - 1; index >= 0; index--) {
    if (handle.readyWorkers[index] === worker) {
      handle.readyWorkers.splice(index, 1);
    }
  }
}

function appendWorkerDebugLog(handle: ProcessHandle, event: string): void {
  handle.workerDebugLog.push(event);
  if (handle.workerDebugLog.length > 16) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 16);
  }
}

function pruneStaleProcessWorkers(
  handle: ProcessHandle,
  liveVmPtrs: Set<number>,
  currentWorker: Worker | null,
): void {
  if (liveVmPtrs.size === 0) {
    return;
  }

  for (const worker of [...handle.workers]) {
    if (worker === currentWorker) {
      continue;
    }
    const thread = threadWorkerStates.get(worker);
    if (!thread || liveVmPtrs.has(thread.vmPtr)) {
      continue;
    }
    clearBlockedWorker(handle, worker);
    removeReadyWorker(handle, worker);
    if (handle.vforkWorker === worker) {
      handle.vforkWorker = null;
    }
    appendWorkerDebugLog(handle, `prune:0x${thread.vmPtr.toString(16)}`);
    handle.workers.delete(worker);
    threadWorkerStates.delete(worker);
    releaseWorker(worker);
  }
}

function wakeBlockedStatusesInKernelShare(
  handle: ProcessHandle,
  statuses: number[],
  pipeUsed?: bigint,
): void {
  const blockedStatuses = new Set(statuses);
  for (const member of handle.kernelShare.members) {
    if (pipeUsed !== undefined) {
      member.kernelState = {
        ...member.kernelState,
        pipeUsed,
      };
    }
    const shouldWake = [...member.workers].some((candidate) => {
      const state = threadWorkerStates.get(candidate);
      return state !== undefined
        && state.waitingInWorkerStatus !== null
        && blockedStatuses.has(state.waitingInWorkerStatus);
    });
    if (shouldWake) {
      publishKernelRuntimeState(member.runtimeBridge, member.kernelState, true);
    }
    for (const [blockedWorker, blocked] of member.blockedWorkers) {
      if (blockedStatuses.has(blocked.status)) {
        retryBlockedWorker(member, blockedWorker);
      }
    }
  }
}

function wakePipeAndEpollWaitersInKernelShare(handle: ProcessHandle, pipeUsed: bigint): void {
  wakeBlockedStatusesInKernelShare(
    handle,
    [
      handle.kernel.abi_status_pipe_wait(),
      handle.kernel.abi_status_epoll_wait(),
    ],
    pipeUsed,
  );
}

function retryEpollWaitersInKernelShare(handle: ProcessHandle): void {
  wakeBlockedStatusesInKernelShare(
    handle,
    [handle.kernel.abi_status_epoll_wait()],
  );
}

function wakeSocketWaitersInKernelShare(
  handle: ProcessHandle,
  socketUsed: number,
  netBufUsed: bigint,
): void {
  for (const member of handle.kernelShare.members) {
    member.kernelState = {
      ...member.kernelState,
      socketUsed,
      netBufUsed,
    };
  }
  wakeBlockedStatusesInKernelShare(
    handle,
    [
      handle.kernel.abi_status_socket_recv(),
      handle.kernel.abi_status_socket_send(),
    ],
  );
}

function statusDefersOwnedStateSync(handle: ProcessHandle, status: number): boolean {
  return status === handle.kernel.abi_status_vfork_wait()
    || status === handle.kernel.abi_status_execve_spawn()
    || status === handle.kernel.abi_status_clone_spawn();
}

function requestEpollStateSync(handle: ProcessHandle, retryWaiters = false): Promise<void> {
  if (!kernelWorkerOwnsEpollState(handle)) {
    if (retryWaiters) {
      retryEpollWaitersInKernelShare(handle);
    }
    return Promise.resolve();
  }

  handle.epollSyncQueued = true;
  if (retryWaiters) {
    handle.epollWaiterWakeQueued = true;
  }
  if (handle.epollSyncPromise !== null) {
    return handle.epollSyncPromise;
  }

  handle.epollSyncPromise = (async () => {
    while (handle.epollSyncQueued) {
      handle.epollSyncQueued = false;
      const shouldRetry = handle.epollWaiterWakeQueued;
      handle.epollWaiterWakeQueued = false;
      await syncEpollStateViaKernelWorker(handle);
      if (shouldRetry) {
        retryEpollWaitersInKernelShare(handle);
      }
    }
  })().finally(() => {
    handle.epollSyncPromise = null;
  });

  return handle.epollSyncPromise;
}

function requestPipeStateSync(handle: ProcessHandle, wakeWaiters = false): Promise<void> {
  if (!kernelWorkerOwnsPipeState(handle)) {
    if (wakeWaiters) {
      wakeRuntimeBridge(handle.runtimeBridge);
      kickBlockedWorkers(handle);
    }
    return Promise.resolve();
  }

  handle.pipeSyncQueued = true;
  if (wakeWaiters) {
    handle.pipeWaiterWakeQueued = true;
  }
  if (handle.pipeSyncPromise !== null) {
    return handle.pipeSyncPromise;
  }

  handle.pipeSyncPromise = (async () => {
    while (handle.pipeSyncQueued) {
      handle.pipeSyncQueued = false;
      const shouldWake = handle.pipeWaiterWakeQueued;
      handle.pipeWaiterWakeQueued = false;
      const pipeUsed = await syncPipeStateViaKernelWorker(handle);
      if (shouldWake) {
        wakePipeAndEpollWaitersInKernelShare(handle, pipeUsed);
      }
    }
  })().finally(() => {
    handle.pipeSyncPromise = null;
  });

  return handle.pipeSyncPromise;
}

function requestSocketStateSync(handle: ProcessHandle, wakeWaiters = false): Promise<void> {
  if (!kernelWorkerOwnsSocketState(handle)) {
    if (wakeWaiters) {
      wakeSocketWaitersInKernelShare(handle, handle.kernelState.socketUsed, handle.kernelState.netBufUsed);
    }
    return Promise.resolve();
  }

  handle.socketSyncQueued = true;
  if (wakeWaiters) {
    handle.socketWaiterWakeQueued = true;
  }
  if (handle.socketSyncPromise !== null) {
    return handle.socketSyncPromise;
  }

  handle.socketSyncPromise = (async () => {
    while (handle.socketSyncQueued) {
      handle.socketSyncQueued = false;
      const shouldWake = handle.socketWaiterWakeQueued;
      handle.socketWaiterWakeQueued = false;
      const { socketUsed, netBufUsed } = await syncSocketStateViaKernelWorker(handle);
      if (shouldWake) {
        wakeSocketWaitersInKernelShare(handle, socketUsed, netBufUsed);
      }
    }
  })().finally(() => {
    handle.socketSyncPromise = null;
  });

  return handle.socketSyncPromise;
}

function scheduleBlockedRetry(
  handle: ProcessHandle,
  worker: Worker,
  delayMs = 10,
  timeoutExpired = false,
): void {
  const blocked = handle.blockedWorkers.get(worker);
  if (!blocked || blocked.retryTimer !== null) {
    return;
  }
  blocked.retryTimer = setTimeout(() => {
    blocked.retryTimer = null;
    blocked.timeoutExpired = timeoutExpired;
    retryBlockedWorker(handle, worker);
  }, delayMs);
  maybeUnrefTimer(blocked.retryTimer);
}

function blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void {
  clearBlockedWorker(handle, worker);
  handle.blockedWorkers.set(worker, {
    vmPtr,
    status,
    retryTimer: null,
    timeoutExpired: false,
    rpcPending: false,
  });
  if (status === handle.kernel.abi_status_epoll_wait()) {
    const timeoutMs = Number(handle.kernel.thread_epoll_wait_timeout(vmPtr));
    if (timeoutMs >= 0) {
      scheduleBlockedRetry(handle, worker, Math.max(1, timeoutMs), true);
    }
    return;
  }
  scheduleBlockedRetry(handle, worker, status === handle.kernel.abi_status_wait4() ? 10 : 1);
}

function handleBlockedRetryResult(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  status: number,
): void {
  const blocked = handle.blockedWorkers.get(worker);
  if (!blocked || blocked.vmPtr !== vmPtr) {
    return;
  }

  if (status === handle.kernel.abi_status_ok()) {
    clearBlockedWorker(handle, worker);
    enqueueWorker(handle, worker);
    return;
  }
  if (status === handle.kernel.abi_status_fault()) {
    clearBlockedWorker(handle, worker);
    const exitCode = handle.kernel.thread_exit_code(vmPtr);
    const termSignal = handle.kernel.thread_term_signal(vmPtr);
    if (resumeVforkParentAfterChildExit(handle, worker, vmPtr, exitCode, termSignal)) {
      return;
    }
    const remainingThreads = handle.kernel.thread_process_thread_count(vmPtr);
    appendWorkerDebugLog(handle, `fault:0x${vmPtr.toString(16)} remaining=${remainingThreads}`);
    handle.workers.delete(worker);
    threadWorkerStates.delete(worker);
    releaseWorker(worker);
    if (remainingThreads === 0) {
      void completeProcessExit(handle, exitCode, termSignal).catch((error) => {
        failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    } else {
      pumpProcess(handle);
    }
    return;
  }

  blocked.status = status;
  if (status === handle.kernel.abi_status_epoll_wait()) {
    const timeoutMs = Number(handle.kernel.thread_epoll_wait_timeout(vmPtr));
    if (timeoutMs >= 0) {
      scheduleBlockedRetry(handle, worker, Math.max(1, timeoutMs), true);
    }
    return;
  }
  scheduleBlockedRetry(handle, worker, status === handle.kernel.abi_status_wait4() ? 10 : 1);
}

function retryBlockedWorker(handle: ProcessHandle, worker: Worker): void {
  const blocked = handle.blockedWorkers.get(worker);
  if (!blocked || blocked.rpcPending || handle.runningWorker !== null || !canScheduleWorker(handle, worker)) {
    return;
  }

  const anchorVmPtr = selectProcessAnchorVmPtr(handle, worker);
  if (anchorVmPtr !== null) {
    try {
      const liveVmPtrs = collectLiveThreadVmPtrs(handle, anchorVmPtr);
      pruneStaleProcessWorkers(handle, liveVmPtrs, null);
      if (!liveVmPtrs.has(blocked.vmPtr)) {
        clearBlockedWorker(handle, worker);
        return;
      }
    } catch {
      // If the candidate anchor is stale, fall back to the normal resume path.
    }
  }

  flushStdin(handle);
  drainProcessOutput(handle);

  if (blocked.status === handle.kernel.abi_status_wait4() && handle.kernelWorkerRequest && handle.pid > 0) {
    blocked.rpcPending = true;
    void resumeWait4ViaKernelWorker(handle, worker, blocked.vmPtr);
    return;
  }

  if (blocked.status === handle.kernel.abi_status_epoll_wait() && kernelWorkerOwnsEpollState(handle)) {
    blocked.rpcPending = true;
    void resumeEpollWaitViaKernelWorker(handle, worker, blocked.vmPtr, blocked.timeoutExpired);
    return;
  }

  if (
    handle.kernelWorkerRequest
    && handle.pid > 0
    && (
      blocked.status === handle.kernel.abi_status_pipe_wait()
      || blocked.status === handle.kernel.abi_status_socket_recv()
      || blocked.status === handle.kernel.abi_status_socket_send()
      || blocked.status === handle.kernel.abi_status_flock_wait()
    )
  ) {
    blocked.rpcPending = true;
    void resumeBlockedSyscallLocally(handle, worker, blocked.vmPtr, blocked.status);
    return;
  }

  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  if (blocked.status === handle.kernel.abi_status_epoll_wait() && blocked.timeoutExpired) {
    handle.kernel.thread_resume_epoll_wait_timeout(blocked.vmPtr);
    blocked.timeoutExpired = false;
  } else if (blocked.status === handle.kernel.abi_status_wait4()) {
    handle.kernel.thread_resume_wait4(blocked.vmPtr);
  } else {
    handle.kernel.thread_resume_blocked_syscall(blocked.vmPtr);
  }
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  handleBlockedRetryResult(handle, worker, blocked.vmPtr, handle.kernel.thread_status(blocked.vmPtr));
}

function kickBlockedWorkers(handle: ProcessHandle): void {
  for (const worker of handle.blockedWorkers.keys()) {
    retryBlockedWorker(handle, worker);
  }
}

function attachThreadWorker(handle: ProcessHandle, worker: Worker, vmPtr: number): void {
  handle.workers.add(worker);
  appendWorkerDebugLog(handle, `attach:0x${vmPtr.toString(16)}`);
  threadWorkerStates.set(worker, { vmPtr, initialized: false, waitingInWorkerStatus: null });
  setWorkerMessageHandler(worker, (event: MessageEvent<ThreadWorkerOutbound>) => {
    handleWorkerMessage(handle, worker, event.data);
  });

  worker.postMessage({
    type: "init",
    wasmModule: handle.wasmModule,
    memory: handle.memory,
    vmPtr,
    guestBase: handle.guestBase,
    kernelState: handle.kernelState,
    runtimeNotify: handle.runtimeBridge.notifySab,
    runtimeState: handle.runtimeBridge.stateSab,
  } satisfies ThreadWorkerInbound);
}

function enqueueWorker(handle: ProcessHandle, worker: Worker): void {
  if (!handle.readyWorkers.includes(worker)) {
    handle.readyWorkers.push(worker);
  }
  pumpProcess(handle);
}

function pumpProcess(handle: ProcessHandle): void {
  if (handle.runningWorker !== null) {
    return;
  }
  let next: Worker | undefined;
  if (hasExclusiveVforkWorker(handle)) {
    const index = handle.readyWorkers.indexOf(handle.vforkWorker!);
    if (index === -1) {
      return;
    }
    next = handle.readyWorkers.splice(index, 1)[0];
  } else {
    next = handle.readyWorkers.shift();
  }
  if (!next) {
    return;
  }
  handle.runningWorker = next;
  next.postMessage({
    type: "step",
    budget: THREAD_STEP_BUDGET,
    kernelState: handle.kernelState,
  } satisfies ThreadWorkerInbound);
}

function handleWorkerMessage(
  handle: ProcessHandle,
  worker: Worker,
  msg: ThreadWorkerOutbound,
): void {
  if (msg.type === "ready") {
    const thread = threadWorkerStates.get(worker);
    if (!thread) {
      return;
    }
    thread.initialized = true;
    thread.waitingInWorkerStatus = null;
    clearBlockedWorker(handle, worker);
    const wroteToPipe = flushStdin(handle);
    const drainedPipe = drainProcessOutput(handle);
    if (wroteToPipe || drainedPipe) {
      void requestPipeStateSync(handle, true).catch((error) => {
        failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    }
    enqueueWorker(handle, worker);
    return;
  }

  if (msg.type !== "status") {
    if (msg.type === "error") {
      handle.lastDebugMessage = msg.message;
      if (handle.runningWorker === worker) {
        handle.runningWorker = null;
      }
      clearBlockedWorker(handle, worker);
      const threadCount = handle.kernel.thread_process_thread_count(msg.vmPtr);
      const liveThreads: string[] = [];
      for (let slot = 0; slot < threadCount; slot++) {
        const vmPtr = handle.kernel.process_thread_vm_ptr(msg.vmPtr, slot);
        if (vmPtr === 0) {
          continue;
        }
        liveThreads.push(`0x${vmPtr.toString(16)}`);
      }
      const attachedWorkers = [...handle.workers]
        .map((candidate) => threadWorkerStates.get(candidate)?.vmPtr ?? 0)
        .filter((candidateVmPtr) => candidateVmPtr !== 0)
        .map((candidateVmPtr) => `0x${candidateVmPtr.toString(16)}`);
      const threadSlots = Array.from({ length: Math.min(handle.kernel.abi_max_threads(), 8) }, (_value, slot) => {
        const candidateVmPtr = handle.kernel.process_thread_vm_ptr(msg.vmPtr, slot);
        return candidateVmPtr === 0 ? `${slot}=0x0` : `${slot}=0x${candidateVmPtr.toString(16)}`;
      });
      failProcess(
        handle,
        new Error(
          `pid=${handle.pid} thread vm=0x${msg.vmPtr.toString(16)} thread_count=${threadCount}`
            + ` live_threads=[${liveThreads.join(", ")}]`
            + ` attached_workers=[${attachedWorkers.join(", ")}]`
            + ` thread_slots=[${threadSlots.join(", ")}] ${msg.message}`,
        ),
      );
    }
    return;
  }

  const thread = threadWorkerStates.get(worker);
  if (!thread) {
    return;
  }
  thread.waitingInWorkerStatus = null;
  handle.lastDebugMessage = msg.debug;

  const isRunningWorker = handle.runningWorker === worker;
  if (isRunningWorker) {
    handle.kernelState = adoptKernelState(handle.kernelState, msg.kernelState);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
  }
  if (msg.waitInWorker) {
    thread.waitingInWorkerStatus = msg.status;
    clearBlockedWorker(handle, worker);
    const drainedPipe = drainProcessOutput(handle);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState);
    const continueWaitInWorker = (): void => {
      if (drainedPipe) {
        void requestPipeStateSync(handle, true).catch((error) => {
          failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      }
      if (isRunningWorker) {
        handle.runningWorker = null;
      }
      pumpProcess(handle);
    };
    if (isRunningWorker && kernelWorkerOwnsEpollState(handle)) {
      const shouldSyncSocket = kernelWorkerOwnsSocketState(handle)
        && (
          msg.status === handle.kernel.abi_status_socket_recv()
          || msg.status === handle.kernel.abi_status_socket_send()
        );
      const syncSocketState = shouldSyncSocket
        ? requestSocketStateSync(handle, false)
        : Promise.resolve();
      void syncSocketState
        .then(() => requestEpollStateSync(handle, !drainedPipe))
        .then(() => {
          continueWaitInWorker();
        })
        .catch((error) => {
          failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }
    if (
      isRunningWorker
      && kernelWorkerOwnsSocketState(handle)
      && (
        msg.status === handle.kernel.abi_status_socket_recv()
        || msg.status === handle.kernel.abi_status_socket_send()
      )
    ) {
      void requestSocketStateSync(handle, false)
        .then(() => {
          continueWaitInWorker();
        })
        .catch((error) => {
          failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }
    continueWaitInWorker();
    return;
  }
  clearBlockedWorker(handle, worker);
  const wroteToPipe = flushStdin(handle);
  const drainedPipe = drainProcessOutput(handle);
  publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
  if (isRunningWorker) {
    handle.runningWorker = null;
  }

  const status = msg.status;
  const continueAfterOwnedStateSync = (): void => {
    if (status === handle.kernel.abi_status_ok()) {
      if (handle.kernelWorkerRequest && handle.pid > 0) {
        void requestPipeStateSync(handle, true)
          .then(() => syncProcessLifecycleViaKernelWorker(handle, thread.vmPtr))
          .then(() => {
            kickBlockedWorkers(handle);
            enqueueWorker(handle, worker);
          })
          .catch((error) => {
            failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      kickBlockedWorkers(handle);
      enqueueWorker(handle, worker);
      return;
    }

    if (status === handle.kernel.abi_status_fault()) {
      if (resumeVforkParentAfterChildExit(handle, worker, thread.vmPtr, msg.exitCode, msg.termSignal)) {
        return;
      }
      const remainingThreads = handle.kernel.thread_process_thread_count(thread.vmPtr);
      appendWorkerDebugLog(handle, `fault:0x${thread.vmPtr.toString(16)} remaining=${remainingThreads}`);
      handle.workers.delete(worker);
      threadWorkerStates.delete(worker);
      releaseWorker(worker);
      if (remainingThreads === 0) {
        void completeProcessExit(handle, msg.exitCode, msg.termSignal).catch((error) => {
          failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      } else {
        if (wroteToPipe || drainedPipe) {
          void requestPipeStateSync(handle, true)
            .then(() => {
              kickBlockedWorkers(handle);
              pumpProcess(handle);
            })
            .catch((error) => {
              failProcess(handle, error instanceof Error ? error : new Error(String(error)));
            });
          return;
        }
        kickBlockedWorkers(handle);
        pumpProcess(handle);
      }
      return;
    }

    if (wroteToPipe || drainedPipe) {
      void requestPipeStateSync(handle, true).catch((error) => {
        failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    }

    if (status === handle.kernel.abi_status_sleep()) {
      const sec = Number(handle.kernel.thread_sleep_seconds(thread.vmPtr));
      const nsec = Number(handle.kernel.thread_sleep_nanoseconds(thread.vmPtr));
      const ms = Math.max(1, sec * 1000 + Math.floor(nsec / 1_000_000));
      const sleepTimer = setTimeout(() => {
        if (!handle.workers.has(worker) || !threadWorkerStates.has(worker)) {
          return;
        }
        enqueueWorker(handle, worker);
      }, ms);
      maybeUnrefTimer(sleepTimer);
      pumpProcess(handle);
      return;
    }

    if (status === handle.kernel.abi_status_fs()) {
      void resumeFsViaKernelWorker(handle, worker, thread.vmPtr);
      return;
    }

    if (status === handle.kernel.abi_status_connect()) {
      void resumeConnectViaKernelWorker(
        handle,
        worker,
        thread.vmPtr,
        BigInt(-handle.kernel.abi_econnrefused()),
      );
      return;
    }

    if (status === handle.kernel.abi_status_futex_wait()) {
      kickBlockedWorkers(handle);
      pumpProcess(handle);
      return;
    }

    if (status === handle.kernel.abi_status_clone_spawn()) {
      const childVmPtr = handle.kernel.thread_spawn_from_clone(thread.vmPtr);
      if (childVmPtr === 0) {
        throw new Error(`thread_spawn_from_clone failed for pid=${handle.pid}`);
      }
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      assertProcessStandardFdIntegrity(handle, "clone-spawn");
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      const childWorker = acquireWorker();
      attachThreadWorker(handle, childWorker, childVmPtr);
      kickBlockedWorkers(handle);
      enqueueWorker(handle, worker);
      return;
    }

    if (status === handle.kernel.abi_status_execve_spawn()) {
      void handleExecveSpawn(handle, worker, thread).catch((error) => {
        failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
      return;
    }

    if (status === handle.kernel.abi_status_vfork_wait()) {
      if (handle.vforkSnapshot === null) {
        handle.vforkSnapshot = captureVforkSnapshot(handle, thread.vmPtr);
        handle.vforkWorker = worker;
      }
      if (
        handle.kernelWorkerRequest
        && handle.pid > 0
        && handle.vforkSnapshot.childPid === null
      ) {
        const provisionalChildPid = handle.kernel.thread_vfork_child_pid();
        if (provisionalChildPid > 0) {
          void canonicalizeActiveVforkChildViaKernelWorker(handle, worker, thread.vmPtr, provisionalChildPid);
          return;
        }
      }
      enqueueWorker(handle, worker);
      return;
    }

    if (
      status === handle.kernel.abi_status_wait4()
      || status === handle.kernel.abi_status_pipe_wait()
      || status === handle.kernel.abi_status_epoll_wait()
      || status === handle.kernel.abi_status_socket_recv()
      || status === handle.kernel.abi_status_socket_send()
      || status === handle.kernel.abi_status_flock_wait()
      || status === handle.kernel.abi_status_connect()
    ) {
      if (blockedStatusUsesForkSharedStateSync(handle, status) && handle.kernelWorkerRequest && handle.pid > 0) {
        void syncBlockedForkSharedStateViaKernelWorker(handle)
          .then(() => {
            if (!handle.workers.has(worker) || !threadWorkerStates.has(worker)) {
              return;
            }
            blockWorker(handle, worker, status, thread.vmPtr);
          })
          .catch((error) => {
            failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      blockWorker(handle, worker, status, thread.vmPtr);
      return;
    }

    enqueueWorker(handle, worker);
  };

  const shouldWakeSocketWaiters = status === handle.kernel.abi_status_ok();
  const shouldSyncSocketState = isRunningWorker
    && kernelWorkerOwnsSocketState(handle)
    && shouldWakeSocketWaiters
    && !statusDefersOwnedStateSync(handle, status);
  const shouldSyncEpollState = isRunningWorker
    && kernelWorkerOwnsEpollState(handle)
    && !statusDefersOwnedStateSync(handle, status);
  if (shouldSyncSocketState || shouldSyncEpollState) {
    const syncSocketState = shouldSyncSocketState
      ? requestSocketStateSync(handle, shouldWakeSocketWaiters)
      : Promise.resolve();
    void syncSocketState
      .then(() => (shouldSyncEpollState
        ? requestEpollStateSync(handle, !(wroteToPipe || drainedPipe))
        : Promise.resolve()))
      .then(() => {
        continueAfterOwnedStateSync();
      })
      .catch((error) => {
        failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    return;
  }

  continueAfterOwnedStateSync();
}

async function completeProcessExit(handle: ProcessHandle, exitCode: number, termSignal: number): Promise<void> {
  drainProcessOutput(handle);
  if (handle.kernelWorkerRequest && handle.pid > 0) {
    await syncForkSharedStateViaKernelWorker(handle, kernelShareOtherMembers(handle));
  }
  if (kernelWorkerOwnsSocketState(handle)) {
    await requestSocketStateSync(handle, false);
  }
  if (kernelWorkerOwnsEpollState(handle)) {
    await requestEpollStateSync(handle, false);
  }
  if (kernelWorkerOwnsPipeState(handle)) {
    await requestPipeStateSync(handle, true);
    await releaseSharedHostStdio(handle);
  } else {
    releaseSharedHostStdioLocal(handle);
  }
  for (const worker of [...handle.blockedWorkers.keys()]) {
    clearBlockedWorker(handle, worker);
  }
  unregisterProcessWithKernelWorker(handle);
  detachKernelShare(handle);
  if (handle.pid > 0 && processHandles.get(handle.pid) === handle) {
    processHandles.delete(handle.pid);
  }
  recordExitOnParent(handle, exitCode, termSignal);
  handle.onExit?.(exitCode);
}

function failProcess(handle: ProcessHandle, error: Error): void {
  drainProcessOutput(handle);
  destroyProcessHandle(handle, 15, false);
  handle.onError?.(error);
}

function recordExitOnParent(handle: ProcessHandle, exitCode: number, termSignal: number): void {
  if (handle.parentPid === null || handle.pid <= 0) {
    return;
  }

  const parent = processHandles.get(handle.parentPid);
  if (!parent) {
    return;
  }

  const waitStatus = encodeWaitStatus(exitCode, termSignal);
  void recordChildExitViaKernelWorker(parent, handle.pid, waitStatus);
}

async function handleExecveSpawn(
  handle: ProcessHandle,
  worker: Worker,
  thread: ThreadWorkerState,
): Promise<void> {
  const handoff = readProcessSpawnHandoff(handle.kernel, thread.vmPtr);
  const spawnKindExecve = handle.kernel.abi_spawn_kind_execve();
  const spawnKindFork = handle.kernel.abi_spawn_kind_fork();

  if (handoff.kind === spawnKindExecve) {
    await handleVforkExecve(handle, worker, thread, handoff.childPid);
    return;
  }

  if (handoff.kind === spawnKindFork) {
    await handlePlainFork(handle, worker, thread, handoff.childPid);
    return;
  }

  handle.kernel.thread_clear_spawn_handoff(thread.vmPtr);
  enqueueWorker(handle, worker);
}

async function handleVforkExecve(
  handle: ProcessHandle,
  parentWorker: Worker,
  parentThread: ThreadWorkerState,
  childPid: number,
): Promise<void> {
  const snapshot = handle.vforkSnapshot;
  const inheritedSharedOfdIds = new Set(snapshot?.inheritedSharedOfdIds ?? []);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  const parentMem = new Uint8Array(handle.memory.buffer);
  const childMemory = new WebAssembly.Memory({
    initial: Math.ceil(handle.memory.buffer.byteLength / WASM_PAGE_SIZE),
    maximum: WASM_MAX_PAGES,
    shared: true,
  });
  new Uint8Array(childMemory.buffer).set(parentMem);

  const childInstance = await WebAssembly.instantiate(handle.wasmModule, {
    env: createKernelEnv(childMemory),
  });
  const childKernel = childInstance.exports as unknown as KernelExports;
  attachKernelRuntimeState(childKernel, handle.kernelState);

  const childVmPtr = parentThread.vmPtr;
  const execRet = Number(childKernel.thread_complete_execve_handoff(childVmPtr));
  if (execRet < 0) {
    const debugStage = childKernel.debug_execve_stage?.();
    const debugValue0 = childKernel.debug_execve_value0?.();
    const debugValue1 = childKernel.debug_execve_value1?.();
    throw new Error(
      `thread_complete_execve_handoff failed for pid=${childPid}: ${execRet}`
        + ` (stage=${debugStage ?? "n/a"}, value0=${debugValue0 ?? "n/a"}, value1=${debugValue1 ?? "n/a"})`,
    );
  }

  ensureEvictionTimer();
  const childWorker = acquireWorker();
  const childHandle: ProcessHandle = {
    runtimeId: handle.runtimeId,
    pid: childPid,
    parentPid: handle.pid,
    vmPtr: childVmPtr,
    ramSize: handle.ramSize,
    guestBase: handle.guestBase,
    worker: childWorker,
    memory: childMemory,
    wasmModule: handle.wasmModule,
    kernel: childKernel,
    kernelState: readKernelRuntimeState(childKernel),
    stdinPipe: childKernel.process_stdin_pipe(childVmPtr),
    stdoutPipe: childKernel.process_stdout_pipe(childVmPtr),
    stderrPipe: childKernel.process_stderr_pipe(childVmPtr),
    hostStdio: retainSharedHostStdio(handle.hostStdio),
    kernelShare: handle.kernelShare,
    stdinQueue: [],
    outputBacklog: [],
    lastDebugMessage: undefined,
    runtimeBridge: createRuntimeStateBridge(),
    kernelWorkerRequest: handle.kernelWorkerRequest,
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
    socketSyncPromise: null,
    socketSyncQueued: false,
    socketWaiterWakeQueued: false,
    vforkSnapshot: null,
    vforkWorker: null,
    onStdout: createInheritedStdoutSink(handle),
    onError: createInheritedErrorSink(handle, childPid),
    workerDebugLog: ["spawn:vfork-execve"],
  };
  assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-execve");
  if (handle.kernelWorkerRequest && handle.pid > 0) {
    await resumeVforkExecveParentViaKernelWorker(handle, parentThread.vmPtr, childPid);
    assertProcessStandardFdIntegrity(handle, "vfork-parent-restored");
  } else {
    handle.kernel.thread_clear_spawn_handoff(parentThread.vmPtr);
    handle.kernel.thread_resume_vfork_parent(parentThread.vmPtr, childPid);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
  }
  publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);

  adoptKernelShare(handle, childHandle);
  if (handle.kernelWorkerRequest && handle.pid > 0) {
    for (const ofdId of inheritedSharedOfdIds) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
    }
    const sharedOfdIds = activeKernelShareOfdIds(handle);
    const childHeldOfdIds = collectProcessHeldOfds(childHandle);
    const childHeldOfdSlots = snapshotKernelShareOfdSlots(childHandle, childHeldOfdIds);
    const preservedOfdSlots = childHeldOfdSlots;
    const rehydratedSharedOfdIds = sharedOfdIds
      .filter((ofdId) => !childHeldOfdIds.has(ofdId));
    await registerProcessWithKernelWorker(childHandle, { seedMemfsFromCanonical: false });
    assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-register");
    reconcileForkSharedOfdRefcounts(childHandle, sharedOfdIds);
    await syncForkSharedStateViaKernelWorker(childHandle, kernelShareOtherMembers(childHandle));
    assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-sync-fork-shared-state");
    discardCopiedSharedKernelObjects(childHandle, preservedOfdSlots);
    assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-discard");
    await rehydrateForkSharedStateViaKernelWorker([childHandle], rehydratedSharedOfdIds);
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-rehydrate");
  } else {
    syncForkSharedStateLocally(childHandle, kernelShareOtherMembers(childHandle));
    discardCopiedSharedKernelObjects(childHandle);
    syncForkSharedStateLocally(handle, [childHandle]);
  }
  await initializeProcessLifecycleViaKernelWorker(
    childHandle,
    childVmPtr,
    handle.pid,
    snapshot?.nextPid ?? Math.max(3, childPid + 1),
    snapshot?.pgid ?? childKernel.process_current_pgid(childVmPtr),
    snapshot?.sid ?? childKernel.process_current_sid(childVmPtr),
    true,
  );
  const childThreadCount = childHandle.kernel.thread_process_thread_count(childVmPtr);
  if (childThreadCount !== 1) {
    const threadSlots = Array.from({ length: Math.min(childHandle.kernel.abi_max_threads(), 8) }, (_value, slot) => {
      const candidateVmPtr = childHandle.kernel.process_thread_vm_ptr(childVmPtr, slot);
      return candidateVmPtr === 0 ? `${slot}=0x0` : `${slot}=0x${candidateVmPtr.toString(16)}`;
    });
    throw new Error(
      `vfork-child-post-lifecycle-init thread_count=${childThreadCount}`
        + ` thread_slots=[${threadSlots.join(", ")}]`,
    );
  }
  assertProcessStandardFdIntegrity(
    childHandle,
    `vfork-child-ready shared=[${activeKernelShareOfdIds(childHandle).join(",")}]`
      + ` private=[${[...collectProcessHeldOfds(childHandle)]
        .filter((ofdId) => !inheritedSharedOfdIds.has(ofdId))
        .join(",")}]`,
  );
  publishKernelRuntimeState(childHandle.runtimeBridge, childHandle.kernelState, true);
  processHandles.set(childPid, childHandle);
  attachThreadWorker(childHandle, childWorker, childVmPtr);

  clearVforkSnapshot(handle);
  kickBlockedWorkers(handle);
  enqueueWorker(handle, parentWorker);
}

async function handlePlainFork(
  handle: ProcessHandle,
  parentWorker: Worker,
  parentThread: ThreadWorkerState,
  childPid: number,
): Promise<void> {
  const cloneFlags = Number(handle.kernel.thread_clone_flags(parentThread.vmPtr));
  const childPpid = (cloneFlags & Number(handle.kernel.abi_clone_parent())) !== 0
    ? (handle.parentPid ?? handle.pid)
    : handle.pid;
  const lifecycle = handle.kernelWorkerRequest && handle.pid > 0
    ? await canonicalizeChildLifecycleViaKernelWorker(handle, parentThread.vmPtr, childPid)
    : {
        childPid,
        nextPid: childPid + 1,
        pgid: handle.kernel.process_current_pgid(parentThread.vmPtr),
        sid: handle.kernel.process_current_sid(parentThread.vmPtr),
      };
  childPid = lifecycle.childPid;

  retainForkSharedOfds(handle);
  handle.kernelState = readKernelRuntimeState(handle.kernel);

  const parentMem = new Uint8Array(handle.memory.buffer);
  const childMemory = new WebAssembly.Memory({
    initial: Math.ceil(handle.memory.buffer.byteLength / 65536),
    maximum: WASM_MAX_PAGES,
    shared: true,
  });
  new Uint8Array(childMemory.buffer).set(parentMem);

  const childInstance = await WebAssembly.instantiate(handle.wasmModule, {
    env: createKernelEnv(childMemory),
  });
  const childKernel = childInstance.exports as unknown as KernelExports;
  attachKernelRuntimeState(childKernel, handle.kernelState);

  ensureEvictionTimer();
  const childWorker = acquireWorker();
  const childHandle: ProcessHandle = {
    runtimeId: handle.runtimeId,
    pid: childPid,
    parentPid: childPpid,
    vmPtr: handle.vmPtr,
    ramSize: handle.ramSize,
    guestBase: handle.guestBase,
    worker: childWorker,
    memory: childMemory,
    wasmModule: handle.wasmModule,
    kernel: childKernel,
    kernelState: readKernelRuntimeState(childKernel),
    stdinPipe: childKernel.process_stdin_pipe(handle.vmPtr),
    stdoutPipe: childKernel.process_stdout_pipe(handle.vmPtr),
    stderrPipe: childKernel.process_stderr_pipe(handle.vmPtr),
    hostStdio: retainSharedHostStdio(handle.hostStdio),
    kernelShare: handle.kernelShare,
    stdinQueue: [],
    outputBacklog: [],
    lastDebugMessage: undefined,
    runtimeBridge: createRuntimeStateBridge(),
    kernelWorkerRequest: handle.kernelWorkerRequest,
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
    socketSyncPromise: null,
    socketSyncQueued: false,
    socketWaiterWakeQueued: false,
    vforkSnapshot: null,
    vforkWorker: null,
    onStdout: createInheritedStdoutSink(handle),
    onError: createInheritedErrorSink(handle, childPid),
    workerDebugLog: ["spawn:plain-fork"],
  };
  adoptKernelShare(handle, childHandle);
  discardCopiedSharedKernelObjects(childHandle);
  await registerProcessWithKernelWorker(childHandle);
  await syncForkSharedStateViaKernelWorker(handle, [childHandle]);
  await initializeProcessLifecycleViaKernelWorker(
    childHandle,
    childHandle.vmPtr,
    childPpid,
    lifecycle.nextPid,
    lifecycle.pgid,
    lifecycle.sid,
    false,
  );
  assertProcessStandardFdIntegrity(childHandle, "plain-fork-child-ready");
  publishKernelRuntimeState(childHandle.runtimeBridge, childHandle.kernelState);
  processHandles.set(childPid, childHandle);
  attachThreadWorker(childHandle, childWorker, childHandle.vmPtr);

  handle.kernel.thread_clear_spawn_handoff(parentThread.vmPtr);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  enqueueWorker(handle, parentWorker);
}

function terminateTrackedWorker(worker: Worker): void {
  const entry = workerEntryMap.get(worker);
  if (entry) {
    const index = workerPool.indexOf(entry);
    if (index >= 0) {
      workerPool.splice(index, 1);
    }
    workerEntryMap.delete(worker);
  }
  threadWorkerStates.delete(worker);
  worker.terminate();
}

function destroyProcessHandle(handle: ProcessHandle, signal: number, notifyExit: boolean): void {
  for (const worker of [...handle.blockedWorkers.keys()]) {
    clearBlockedWorker(handle, worker);
  }
  handle.readyWorkers.length = 0;
  handle.runningWorker = null;
  clearVforkSnapshot(handle);

  const workers = new Set(handle.workers);
  workers.add(handle.worker);
  for (const worker of workers) {
    terminateTrackedWorker(worker);
  }
  handle.workers.clear();

  unregisterProcessWithKernelWorker(handle);
  releaseSharedHostStdioLocal(handle);
  detachKernelShare(handle);
  if (handle.pid > 0 && processHandles.get(handle.pid) === handle) {
    processHandles.delete(handle.pid);
  }
  const dirtyHandles = runtimeFsDirtyHandles.get(handle.runtimeId);
  dirtyHandles?.delete(handle);
  if (dirtyHandles && dirtyHandles.size === 0) {
    runtimeFsDirtyHandles.delete(handle.runtimeId);
  }

  if (notifyExit) {
    handle.onExit?.(128 + signal);
  }
}

export function destroyRuntimeProcesses(runtimeId: number, signal = 15): void {
  for (const handle of [...processHandles.values()]) {
    if (handle.runtimeId !== runtimeId) {
      continue;
    }
    destroyProcessHandle(handle, signal, true);
  }
}

export function destroyProcessTree(rootPid: number, runtimeId: number, signal = 15): void {
  const pending = [rootPid];
  const seen = new Set<number>();
  while (pending.length > 0) {
    const pid = pending.pop()!;
    if (seen.has(pid)) {
      continue;
    }
    seen.add(pid);
    for (const handle of processHandles.values()) {
      if (handle.runtimeId !== runtimeId) {
        continue;
      }
      if (handle.parentPid === pid) {
        pending.push(handle.pid);
      }
    }
  }

  for (const pid of [...seen].reverse()) {
    const handle = processHandles.get(pid);
    if (!handle || handle.runtimeId !== runtimeId) {
      continue;
    }
    destroyProcessHandle(handle, signal, true);
  }
}
