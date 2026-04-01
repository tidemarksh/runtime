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
import type {
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPidStateSnapshot,
  ThreadWorkerInbound,
  ThreadWorkerOutbound,
} from "./messages.js";
import {
  createRuntimeStateBridge,
  publishKernelRuntimeState,
  wakeRuntimeBridge,
} from "./runtime-state-bridge.js";
import { createKernelEnv } from "./wasm-env.js";
import { clearWorkerMessageHandler, setWorkerMessageHandler } from "./worker-compat.js";
import {
  acquireWorker,
  ensureEvictionTimer,
  maybeUnrefTimer,
  removeWorkerFromPool,
  releaseWorker,
} from "./worker-pool.js";
import {
  applyFsEntryToHandle,
  cloneSnapshotEntries,
  normalizePath,
  preloadSnapshotEntries,
  resolveProcessCwdIno,
  snapshotEntriesEqual,
  snapshotProcessFs,
} from "./worker-process-fs.js";
import {
  captureExecveGuestHandoff,
  decodeExecveBytes,
  stageExecveGuestHandoff,
} from "./worker-execve-handoff.js";
import {
  alignUp,
  copyKernelPrivateRegion,
  detachedKernelScratchBase,
  ensureMemoryCapacity,
  writeDetachedKernelScratch,
} from "./worker-kernel-scratch.js";
import {
  createInheritedErrorSink,
  createInheritedStdoutSink,
  drainProcessOutput,
  flushStdin,
  releaseSharedHostStdioLocal,
  retainSharedHostStdio,
  writeProcessStdin as bufferProcessStdin,
} from "./worker-stdio.js";
import type {
  CanonicalizedChildLifecycle,
  CreateProcessOptions,
  KernelOwnershipHandle,
  KernelShareGroup,
  KernelSyncHandle,
  KernelWorkerMemfsSeedMode,
  KernelWorkerPipeSeedMode,
  KernelWorkerRequestFn,
  ProcessHandle,
  RuntimeFsState,
  ThreadWorkerState,
  VforkSnapshot,
} from "./worker-types.js";

export type { CreateProcessOptions, ProcessHandle } from "./worker-types.js";
export { destroyWorkerPool } from "./worker-pool.js";

let cachedModule: WebAssembly.Module | null = null;

export async function precompileModule(wasmBytes: BufferSource): Promise<WebAssembly.Module> {
  cachedModule = await WebAssembly.compile(wasmBytes);
  return cachedModule;
}

export function clearModuleCache(): void {
  cachedModule = null;
}

const FD_TYPE_EVENTFD = 9;
const FD_TYPE_TIMERFD = 10;

const processHandles = new Map<number, ProcessHandle>();
const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
const runtimeFsStates = new Map<number, RuntimeFsState>();
const runtimeFsDirtyHandles = new Map<number, Set<ProcessHandle>>();
const runtimeLifecycleEvents = new Map<number, string[]>();

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const WASM_PAGE_SIZE = 65536;
const WASM_MAX_PAGES = 65536;
const THREAD_STEP_BUDGET = 20_000;
const BOOTSTRAP_MEMORY_BYTES = 64 * 1024 * 1024;
const FS_EFFECT_MEMFS_MUTATION = 1 << 0;
const FS_EFFECT_SHARED_OFD_STATE = 1 << 1;
const FS_EFFECT_MEMFS_FILE_DATA = 1 << 2;
const SYNC_EFFECT_PIPE_STATE = 1 << 0;
const SYNC_EFFECT_SOCKET_STATE = 1 << 1;
const SYNC_EFFECT_EPOLL_STATE = 1 << 2;
const SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE = 1 << 3;
const SYNC_EFFECT_PROCESS_LIFECYCLE = 1 << 4;
const SYNC_EFFECT_SIGNAL_WAITERS = 1 << 5;
const SYNC_EFFECT_FORK_SHARED_OFD_STATE = 1 << 6;
const LEGACY_OK_SYNC_EFFECTS = SYNC_EFFECT_PIPE_STATE
  | SYNC_EFFECT_SOCKET_STATE
  | SYNC_EFFECT_EPOLL_STATE
  | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
  | SYNC_EFFECT_PROCESS_LIFECYCLE
  | SYNC_EFFECT_FORK_SHARED_OFD_STATE;

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
  if (byteLength <= 0 || targetOffset < 0) {
    return;
  }
  const available = target.memory.buffer.byteLength - targetOffset;
  if (available <= 0) {
    return;
  }
  new Uint8Array(target.memory.buffer, targetOffset, Math.min(byteLength, available)).fill(0);
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
  const tableEntries = tableBytes >>> 2;
  const sourceTable = new Uint32Array(source.memory.buffer, source.kernelState.epollTablePtr, tableEntries);
  const targetTable = new Uint32Array(target.memory.buffer, target.kernelState.epollTablePtr, tableEntries);
  const targetPtrs = [...targetTable];
  for (let i = 0; i < tableEntries; i += 1) {
    const sourcePtr = sourceTable[i] ?? 0;
    const targetPtr = targetPtrs[i] ?? 0;
    if (sourcePtr === 0) {
      targetTable[i] = 0;
      if (targetPtr !== 0) {
        clearKernelRegion(target, targetPtr, instanceBytes);
      }
      continue;
    }
    const instancePtr = targetPtr !== 0 ? targetPtr : sourcePtr;
    targetTable[i] = instancePtr;
    copyKernelRegion(source, sourcePtr, target, instancePtr, instanceBytes);
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

function snapshotLocalPipeSlots(
  handle: ProcessHandle,
  ofdIds: Iterable<number>,
): Map<number, Uint8Array> {
  const maxPipes = handle.kernel.abi_max_pipes();
  const regionBytes = handle.kernel.kernel_pipe_region_bytes();
  const slotBytes = regionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid pipe region layout: bytes=${regionBytes} maxPipes=${maxPipes}`);
  }

  const snapshots = new Map<number, Uint8Array>();
  for (const ofdId of ofdIds) {
    if (handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()) {
      continue;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    if (pipeSlot < 0 || pipeSlot >= maxPipes || snapshots.has(pipeSlot)) {
      continue;
    }
    const slotOffset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
    snapshots.set(pipeSlot, new Uint8Array(handle.memory.buffer, slotOffset, slotBytes).slice());
  }
  return snapshots;
}

function restoreLocalPipeSlots(
  handle: KernelSyncHandle,
  preservedPipeSlots: ReadonlyMap<number, Uint8Array> | null,
): void {
  if (!preservedPipeSlots || preservedPipeSlots.size === 0) {
    return;
  }
  const maxPipes = handle.kernel.abi_max_pipes();
  const regionBytes = handle.kernel.kernel_pipe_region_bytes();
  const slotBytes = regionBytes / maxPipes;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid pipe region layout: bytes=${regionBytes} maxPipes=${maxPipes}`);
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  for (const [pipeSlot, slot] of preservedPipeSlots) {
    bytes.set(slot, handle.kernelState.pipeBase + pipeSlot * slotBytes);
  }
}

function formatPipeCounterDebug(handle: ProcessHandle, fd: number): string {
  const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
  if (ofdId === 0 || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()) {
    return `${fd}:ofd=0`;
  }
  const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
  const base = handle.kernelState.pipeBase + pipeSlot * (32 + 4096);
  const i32 = new Int32Array(handle.memory.buffer);
  const u32 = new Uint32Array(handle.memory.buffer);
  return [
    `${fd}:ofd=${ofdId}`,
    `slot=${pipeSlot}`,
    `ro=${Atomics.load(i32, (base + 8) >>> 2)}`,
    `wo=${Atomics.load(i32, (base + 12) >>> 2)}`,
    `head=${Atomics.load(u32, base >>> 2)}`,
    `tail=${Atomics.load(u32, (base + 4) >>> 2)}`,
  ].join(":");
}

function formatOpenFdSummary(handle: ProcessHandle, limit = 16): string {
  return Array.from({ length: limit }, (_value, fd) => {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId === 0) {
      return null;
    }
    return `${fd}->${ofdId}/${handle.kernel.process_fd_flags(handle.vmPtr, fd)}/${handle.kernel.kernel_ofd_fd_type(ofdId)}/${handle.kernel.kernel_ofd_ino(ofdId)}/${handle.kernel.kernel_ofd_host_fd(ofdId)}`;
  }).filter(Boolean).join(" ");
}

function copyLegacySharedKernelObjectsLocal(
  source: KernelSyncHandle,
  target: KernelSyncHandle,
  sharedOfdIds: Iterable<number>,
  options: { includeMemfs?: boolean } = {},
): void {
  if (source === target) {
    return;
  }
  const includeMemfs = options.includeMemfs ?? true;
  const includePipe = !kernelWorkerOwnsPipeState(source) && !kernelWorkerOwnsPipeState(target);
  const includeEpoll = !kernelWorkerOwnsEpollState(source) && !kernelWorkerOwnsEpollState(target);
  const includeSocket = !kernelWorkerOwnsSocketState(source)
    && !kernelWorkerOwnsSocketState(target);

  // Shared backend state that survives fork/vfork across processes.
  // Per-process signal disposition/pending state must not be synchronized here.
  copySelectedOfdState(source, target, sharedOfdIds);
  if (includeMemfs) {
    syncMemfsState(source, target);
  }
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
  const stdioPipeSlots = new Set<number>([handle.stdinPipe, handle.stdoutPipe, handle.stderrPipe]
    .filter((pipeSlot) => pipeSlot > 0));
  const retained = new Set<number>();
  for (let fd = 0; fd < handle.kernel.abi_max_fds(); fd++) {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (ofdId === 0 || retained.has(ofdId)) {
      continue;
    }
    if (
      handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
      && stdioPipeSlots.has(handle.kernel.kernel_ofd_ino(ofdId))
    ) {
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

function collectTrackableForkSharedOfds(handle: ProcessHandle): Set<number> {
  const stdioPipeSlots = new Set<number>([handle.stdinPipe, handle.stdoutPipe, handle.stderrPipe]
    .filter((pipeSlot) => pipeSlot > 0));
  const tracked = new Set<number>();
  for (const ofdId of collectProcessHeldOfds(handle)) {
    if (
      handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
      && stdioPipeSlots.has(handle.kernel.kernel_ofd_ino(ofdId))
    ) {
      continue;
    }
    tracked.add(ofdId);
  }
  return tracked;
}

function reconcileInheritedSharedOfdsAfterVforkExecve(
  handle: ProcessHandle,
  inheritedSharedOfdIds: Iterable<number>,
): void {
  const heldByExistingMembers = new Set<number>();
  for (const member of handle.kernelShare.members) {
    for (const ofdId of collectProcessHeldOfds(member)) {
      heldByExistingMembers.add(ofdId);
    }
  }
  for (const ofdId of inheritedSharedOfdIds) {
    if (heldByExistingMembers.has(ofdId)) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
    } else {
      handle.kernelShare.sharedOfdIds.delete(ofdId);
    }
  }
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
    env: createKernelEnv(memory, { sabPageCache: options.sabPageCache }),
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
  const delegateFsHydrationToKernelWorker = !!(
    options.hydrateFsFromKernelWorker
    && options.kernelWorkerRequest
    && (options.pid ?? 0) > 0
    && options.preloadEntries === undefined
  );
  const bootstrapEntries = options.preloadEntries
    ?? (delegateFsHydrationToKernelWorker ? [] : (getRuntimeFsState(runtimeId)?.entries ?? []));
  // Kernel-worker-owned runtimes hydrate new processes during register-process
  // using canonical MemFS metadata. Local-only runtimes still use the runtime snapshot path.
  preloadSnapshotEntries(exports, bootstrapEntries);
  const cwdIno = delegateFsHydrationToKernelWorker
    ? await resolveProcessCwdInoViaKernelWorker(options.kernelWorkerRequest!, options.cwd)
    : resolveProcessCwdIno(exports, options.cwd);
  const guestBase = exports.process_guest_base(ramSize);
  if (guestBase === 0) {
    throw new Error("process_guest_base failed");
  }

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

  const vmPtr = exports.process_create(elfOffset, elfData.length, ramSize, stackSize);
  if (vmPtr === 0) {
    throw new Error("process_create failed");
  }
  if (!delegateFsHydrationToKernelWorker) {
    exports.process_set_cwd_ino(vmPtr, cwdIno);
  }

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

  const memfsSeedMode: KernelWorkerMemfsSeedMode = delegateFsHydrationToKernelWorker
    ? "metadata"
    : "full";
  await registerProcessWithKernelWorker(handle, { seedMemfsMode: memfsSeedMode });
  if (delegateFsHydrationToKernelWorker) {
    handle.kernel.process_set_cwd_ino(vmPtr, cwdIno);
  }
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
  bufferProcessStdin(handle, data);
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

async function resolveProcessCwdInoViaKernelWorker(
  kernelWorkerRequest: KernelWorkerRequestFn,
  cwd: string | undefined,
): Promise<number> {
  if (!cwd) {
    return 1;
  }
  const response = await kernelWorkerRequest({
    type: "stat",
    path: cwd,
  });
  if (response.type !== "stat-result") {
    throw new Error(`unexpected kernel-worker response for stat: ${response.type}`);
  }
  if ((response.stat.mode & 0o170000) !== 0o040000) {
    throw new Error(`cwd is not a directory: ${cwd}`);
  }
  return response.stat.ino;
}

function formatThreadDebug(handle: ProcessHandle, vmPtr: number): string {
  const tid = handle.kernel.thread_tid?.(vmPtr) ?? 0;
  const pc = handle.kernel.thread_pc?.(vmPtr) ?? 0n;
  const ra = handle.kernel.thread_ra?.(vmPtr) ?? 0n;
  const sp = handle.kernel.thread_sp?.(vmPtr) ?? 0n;
  const a0 = handle.kernel.thread_a0?.(vmPtr) ?? 0n;
  const a1 = handle.kernel.thread_a1?.(vmPtr) ?? 0n;
  const sigPending = handle.kernel.thread_sig_pending?.(vmPtr) ?? 0n;
  const parts = [
    `tid=${tid}`,
    `pc=0x${pc.toString(16)}`,
    `ra=0x${ra.toString(16)}`,
    `sp=0x${sp.toString(16)}`,
    `a0=${a0.toString()}`,
    `a1=${a1.toString()}`,
    `sig_pending=0x${sigPending.toString(16)}`,
  ];
  const syscallNr = handle.kernel.debug_syscall_trace_nr?.();
  if (syscallNr !== undefined && syscallNr !== 0n) {
    parts.push(`syscall=${syscallNr.toString()}`);
    parts.push(`status=${handle.kernel.debug_syscall_trace_status?.() ?? "n/a"}`);
    parts.push(`result=${handle.kernel.debug_syscall_trace_result?.().toString() ?? "n/a"}`);
  }
  const signalDeliverySig = handle.kernel.debug_signal_delivery_sig?.();
  if (signalDeliverySig !== undefined && signalDeliverySig !== 0) {
    parts.push(`sig_deliver=${signalDeliverySig}`);
    parts.push(`sig_pc=0x${(handle.kernel.debug_signal_delivery_pc?.() ?? 0n).toString(16)}`);
  }
  return parts.join(" ");
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
      const workerStates = [...handle.workers]
        .map((worker) => {
          const state = threadWorkerStates.get(worker);
          if (!state) {
            return null;
          }
          let ownerState = "idle";
          if (handle.runningWorker === worker) {
            ownerState = "running";
          } else if (handle.readyWorkers.includes(worker)) {
            ownerState = "ready";
          } else if (handle.blockedWorkers.has(worker)) {
            ownerState = "blocked";
          } else if (state.waitingInWorkerStatus !== null) {
            ownerState = `wait:${state.waitingInWorkerStatus}`;
          } else if (!state.initialized) {
            ownerState = "init";
          }
          return `0x${state.vmPtr.toString(16)}:${ownerState}`;
        })
        .filter(Boolean)
        .join(",");
      const liveThreadVmPtrs = Array.from({ length: handle.kernel.abi_max_threads() }, (_value, slot) =>
        handle.kernel.process_thread_vm_ptr(handle.vmPtr, slot))
        .filter((vmPtr) => vmPtr !== 0)
        .map((vmPtr) => `0x${vmPtr.toString(16)}`)
        .join(",");
      const threadStates = Array.from({ length: handle.kernel.abi_max_threads() }, (_value, slot) => {
        const threadVmPtr = handle.kernel.process_thread_vm_ptr(handle.vmPtr, slot);
        if (threadVmPtr === 0) {
          return null;
        }
        return `${slot}:${handle.kernel.process_thread_tid(handle.vmPtr, slot)}/${handle.kernel.process_thread_state(handle.vmPtr, slot)}/0x${threadVmPtr.toString(16)}`;
      }).filter(Boolean).join(",");
      const fdSummary = Array.from({ length: 16 }, (_, fd) => {
        const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
        if (ofdId === 0) {
          return null;
        }
        return `${fd}->${ofdId}/${handle.kernel.process_fd_flags(handle.vmPtr, fd)}/${handle.kernel.kernel_ofd_fd_type(ofdId)}/${handle.kernel.kernel_ofd_ino(ofdId)}/${handle.kernel.kernel_ofd_host_fd(ofdId)}`;
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
      let procSelfExe = "unavailable";
      try {
        const pathBytes = encoder.encode("/proc/self/exe");
        const scratchBase = handle.kernel.kernel_scratch_base?.();
        if (scratchBase === undefined) {
          throw new Error("kernel_scratch_base unavailable");
        }
        const pathPtr = scratchBase;
        const bufPtr = scratchBase + 0x200;
        new Uint8Array(handle.kernel.memory.buffer, pathPtr, pathBytes.length).set(pathBytes);
        const ino = handle.kernel.memfs_resolve_path(pathPtr, pathBytes.length);
        if (ino === 0 || (handle.kernel.memfs_inode_mode(ino) & 0o170000) !== 0o120000) {
          procSelfExe = "missing";
        } else {
          const len = Number(handle.kernel.memfs_read_link(ino, bufPtr, 1024));
          if (len <= 0) {
            procSelfExe = "empty";
          } else {
            procSelfExe = JSON.stringify(
              decoder.decode(new Uint8Array(handle.kernel.memory.buffer.slice(bufPtr, bufPtr + len))),
            );
          }
        }
      } catch {
        procSelfExe = "unavailable";
      }
      const runningState = handle.runningWorker ? threadWorkerStates.get(handle.runningWorker) : undefined;
      const runningVmPtr = runningState?.vmPtr ?? 0;
      const runningDebug = runningVmPtr !== 0 ? formatThreadDebug(handle, runningVmPtr) : "n/a";
      const allThreadDebug = Array.from({ length: handle.kernel.abi_max_threads() }, (_value, slot) => {
        const threadVmPtr = handle.kernel.process_thread_vm_ptr(handle.vmPtr, slot);
        if (threadVmPtr === 0) {
          return null;
        }
        const tid = handle.kernel.process_thread_tid(handle.vmPtr, slot);
        return `${tid}:${JSON.stringify(formatThreadDebug(handle, threadVmPtr))}`;
      }).filter(Boolean).join(" ");
      return [
        `pid=${handle.pid}`,
        `cmd=${JSON.stringify(handle.debugCommand)}`,
        `ppid=${handle.parentPid ?? 0}`,
        `vm=0x${handle.vmPtr.toString(16)}`,
        `process=0x${processPtr.toString(16)}`,
        `workers=${handle.workers.size}`,
        `worker_vms=[${attachedWorkerVmPtrs}]`,
        `worker_states=[${workerStates}]`,
        `thread_vms=[${liveThreadVmPtrs}]`,
        `thread_states=[${threadStates}]`,
        `ready=${handle.readyWorkers.length}`,
        `running=${handle.runningWorker ? "1" : "0"}`,
        `blocked=[${blocked}]`,
        `vfork=${handle.vforkSnapshot ? "1" : "0"}`,
        `stdio_pipes=${handle.stdinPipe}/${handle.stdoutPipe}/${handle.stderrPipe}`,
        `shared_ofds=[${[...handle.kernelShare.sharedOfdIds].sort((left, right) => left - right).join(",")}]`,
        `ofd_table=0x${handle.kernelState.ofdTablePtr.toString(16)}`,
        `memfs=0x${handle.kernelState.memfsPtr.toString(16)}`,
        `pipe=0x${handle.kernelState.pipeBase.toString(16)}`,
        `process_runtime=0x${handle.kernelState.processRuntimeStatePtr.toString(16)}`,
        `worker_log=[${handle.workerDebugLog.join(", ")}]`,
        `fds=[${fdSummary}]`,
        `ofds=[${ofdSummary}]`,
        `proc_self_exe=${procSelfExe}`,
        `running_vm=${runningVmPtr === 0 ? "0x0" : `0x${runningVmPtr.toString(16)}`}`,
        `running_debug=${JSON.stringify(runningDebug)}`,
        `thread_debugs=[${allThreadDebug}]`,
        `debug=${handle.lastDebugMessage ?? "n/a"}`,
      ].join(" ");
    });
  const lifecycle = runtimeLifecycleEvents.get(runtimeId);
  if (lifecycle && lifecycle.length > 0) {
    lines.push(`lifecycle=[${lifecycle.join(", ")}]`);
  }
  return lines.join("\n");
}

function appendRuntimeLifecycleEvent(runtimeId: number, event: string): void {
  const events = runtimeLifecycleEvents.get(runtimeId) ?? [];
  events.push(event);
  if (events.length > 64) {
    events.splice(0, events.length - 64);
  }
  runtimeLifecycleEvents.set(runtimeId, events);
}

function formatAllLiveThreadDebug(handle: ProcessHandle): string {
  const vmPtrs = new Set<number>();
  if (handle.vmPtr !== 0) {
    vmPtrs.add(handle.vmPtr);
  }
  vmPtrs.add(threadWorkerStates.get(handle.worker)?.vmPtr ?? 0);
  for (const worker of handle.workers) {
    vmPtrs.add(threadWorkerStates.get(worker)?.vmPtr ?? 0);
  }
  return [...vmPtrs]
    .filter((vmPtr) => vmPtr !== 0)
    .map((vmPtr) => {
      const tid = handle.kernel.thread_tid?.(vmPtr) ?? 0;
      return `${tid}:${JSON.stringify(formatThreadDebug(handle, vmPtr))}`;
    })
    .join(" ");
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
    if (type <= 0 || type > 64) {
      throw new Error(
        `${context}: pid=${handle.pid} fd=${fd} ofd=${ofdId} type=${type}\n`
        + debugRuntimeProcessHandles(handle.runtimeId),
      );
    }
    if (type !== handle.kernel.abi_fd_type_pipe()) {
      continue;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    const base = handle.kernelState.pipeBase + pipeSlot * (32 + 4096);
    const i32 = new Int32Array(handle.memory.buffer);
    const readOpen = Atomics.load(i32, (base + 8) >>> 2);
    const writeOpen = Atomics.load(i32, (base + 12) >>> 2);
    if (readOpen >= 0 && writeOpen >= 0) {
      continue;
    }
    throw new Error(
      `${context}: pid=${handle.pid} fd=${fd} ofd=${ofdId} pipe=${pipeSlot} ro=${readOpen} wo=${writeOpen}\n`
      + `${formatPipeCounterDebug(handle, fd)}\n`
      + debugRuntimeProcessHandles(handle.runtimeId),
    );
  }
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
  options: { seedMemfsMode?: KernelWorkerMemfsSeedMode; pipeSeedMode?: KernelWorkerPipeSeedMode } = {},
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return;
  }
  const response = await handle.kernelWorkerRequest({
    type: "register-process",
    pid: handle.pid,
    vmPtr: handle.vmPtr,
    memory: handle.memory,
    kernelState: handle.kernelState,
    seedMemfsMode: options.seedMemfsMode ?? "full",
    pipeSeedMode: options.pipeSeedMode ?? "canonical",
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
  const pendingOpcode = handle.kernel.thread_pending_fs_opcode(vmPtr);
  const pendingEffects = handle.kernel.thread_pending_fs_effects(vmPtr);
  const pendingTargetIno = handle.kernel.thread_pending_fs_target_ino(vmPtr);
  const fsDebugSuffix = `:op=${pendingOpcode}:fx=${pendingEffects}:ino=${pendingTargetIno}`;
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    appendWorkerDebugLog(handle, `fs-start:0x${vmPtr.toString(16)}:local${fsDebugSuffix}`);
    const effects = handle.kernel.thread_pending_fs_effects(vmPtr);
    const memfsMutated = (effects & (FS_EFFECT_MEMFS_MUTATION | FS_EFFECT_MEMFS_FILE_DATA)) !== 0;
    const ofdStateChanged = (effects & FS_EFFECT_SHARED_OFD_STATE) !== 0;
    handle.kernel.thread_process_fs(vmPtr);
    handle.kernel.thread_resume_fs(vmPtr);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    if (memfsMutated) {
      syncForkSharedStateLocally(handle, kernelShareOtherMembers(handle));
      markRuntimeFsDirty(handle);
    } else if (ofdStateChanged) {
      syncForkSharedStateLocally(
        handle,
        kernelShareOtherMembers(handle),
        { includeMemfs: false },
      );
    }
    kickBlockedWorkers(handle);
    appendWorkerDebugLog(handle, `fs-done:0x${vmPtr.toString(16)}:local`);
    enqueueWorker(handle, worker);
    return;
  }

  try {
    appendWorkerDebugLog(handle, `fs-start:0x${vmPtr.toString(16)}${fsDebugSuffix}`);
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
    if (response.ofdStateChanged) {
      await syncForkSharedStateViaKernelWorker(
        handle,
        kernelShareOtherMembers(handle),
        { includeMemfs: false },
      );
    }
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    kickBlockedWorkers(handle);
    appendWorkerDebugLog(handle, `fs-done:0x${vmPtr.toString(16)}`);
    enqueueWorker(handle, worker);
  } catch (error) {
    appendWorkerDebugLog(handle, `fs-err:0x${vmPtr.toString(16)}`);
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
  if (handle.kernel.thread_restore_vfork_parent_without_cleanup) {
    handle.kernel.thread_restore_vfork_parent_without_cleanup(vmPtr, childPid);
  } else {
    handle.kernel.thread_resume_vfork_parent(vmPtr, childPid);
  }
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
  options: { includeMemfs?: boolean; trackHeldOfds?: boolean } = {},
): void {
  if (options.trackHeldOfds === true) {
    for (const ofdId of collectTrackableForkSharedOfds(handle)) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
    }
  }
  const activeOfdIds = activeKernelShareOfdIds(handle);
  const staleOfdIds = staleKernelShareOfdIds(handle);
  const syncOfdIds = [...new Set([...activeOfdIds, ...staleOfdIds])].sort((left, right) => left - right);
  mergeKernelShareOfdStateInto(handle, activeOfdIds);
  reconcileForkSharedOfdRefcounts(handle, syncOfdIds);
  for (const target of targets) {
    copyLegacySharedKernelObjectsLocal(handle, target, syncOfdIds, options);
    attachKernelRuntimeState(target.kernel, target.kernelState);
  }
  pruneStaleKernelShareOfdIds(handle, staleOfdIds);
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

function collectEpollDependentOfdIds(handle: ProcessHandle): number[] {
  const held = collectProcessHeldOfds(handle);
  const ofdIds: number[] = [];
  for (const ofdId of held) {
    const type = handle.kernel.kernel_ofd_fd_type(ofdId);
    if (type === FD_TYPE_EVENTFD || type === FD_TYPE_TIMERFD) {
      ofdIds.push(ofdId);
    }
  }
  return ofdIds;
}

function collectHeldEpollOfdIds(handle: ProcessHandle): number[] {
  const held = collectProcessHeldOfds(handle);
  const epollOfdIds: number[] = [];
  for (const ofdId of held) {
    if (handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_epoll()) {
      epollOfdIds.push(ofdId);
    }
  }
  return epollOfdIds;
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
    }
  }
  return active.sort((left, right) => left - right);
}

function staleKernelShareOfdIds(handle: ProcessHandle): number[] {
  const heldByAnyMember = new Set<number>();
  for (const member of handle.kernelShare.members) {
    for (const ofdId of collectProcessHeldOfds(member)) {
      heldByAnyMember.add(ofdId);
    }
  }

  const stale: number[] = [];
  for (const ofdId of handle.kernelShare.sharedOfdIds) {
    if (!heldByAnyMember.has(ofdId)) {
      stale.push(ofdId);
    }
  }
  return stale.sort((left, right) => left - right);
}

function pruneStaleKernelShareOfdIds(handle: ProcessHandle, staleOfdIds: Iterable<number>): void {
  for (const ofdId of staleOfdIds) {
    handle.kernelShare.sharedOfdIds.delete(ofdId);
  }
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
  options: {
    includeMemfs?: boolean;
    trackHeldOfds?: boolean;
    preserveSourceRefcounts?: boolean;
  } = {},
): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    syncForkSharedStateLocally(handle, targets, options);
    return;
  }

  if (options.trackHeldOfds === true) {
    for (const ofdId of collectTrackableForkSharedOfds(handle)) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
    }
  }
  const sourceHeldOfdIds = collectProcessHeldOfds(handle);
  const activeOfdIds = activeKernelShareOfdIds(handle);
  const sourceActiveOfdIds = activeOfdIds
    .filter((ofdId) => sourceHeldOfdIds.has(ofdId));
  const staleOfdIds = staleKernelShareOfdIds(handle);
  const syncOfdIds = [...new Set([...sourceActiveOfdIds, ...staleOfdIds])]
    .sort((left, right) => left - right);
  mergeKernelShareOfdStateInto(handle, sourceActiveOfdIds);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  const ofdSlots: KernelWorkerOfdSlotSnapshot[] = [...snapshotKernelShareOfdSlots(handle, sourceActiveOfdIds)]
    .map(([ofdId, slot]) => ({ ofdId, slot }));
  const targetPids = targets.map((target) => target.pid).filter((pid) => pid > 0);
  const targetKernelStates: KernelWorkerPidStateSnapshot[] = targets
    .filter((target) => target.pid > 0)
    .map((target) => ({
      pid: target.pid,
      vmPtr: target.vmPtr,
      kernelState: target.kernelState,
    }));
  const response = await handle.kernelWorkerRequest({
    type: "sync-fork-shared-state",
    pid: handle.pid,
    vmPtr: handle.vmPtr,
    targetPids,
    targetKernelStates,
    ofdIds: syncOfdIds,
    ofdSlots,
    includeMemfs: options.includeMemfs ?? true,
    preserveSourceRefcounts: options.preserveSourceRefcounts,
    kernelState: handle.kernelState,
  });
  if (response.type !== "fork-shared-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-fork-shared-state: ${response.type}`);
  }
  handle.kernelState = adoptKernelState(handle.kernelState, response.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  pruneStaleKernelShareOfdIds(handle, staleOfdIds);
}

async function rehydrateForkSharedStateViaKernelWorker(
  targets: ProcessHandle[],
  ofdIds: number[],
  options: { procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean } = {},
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
      vmPtr: target.vmPtr,
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
    procSelfExeTargets: options.procSelfExeTargets,
    includeMemfs: options.includeMemfs,
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
  await syncForkSharedStateViaKernelWorker(
    handle,
    kernelShareOtherMembers(handle),
    { includeMemfs: false },
  );
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
  if (collectHeldEpollOfdIds(handle).length === 0) {
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

async function syncOfdStateViaKernelWorker(handle: ProcessHandle, ofdIds: number[]): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0 || ofdIds.length === 0) {
    return;
  }

  const response = await handle.kernelWorkerRequest({
    type: "sync-ofd-state",
    pid: handle.pid,
    ofdIds,
    kernelState: handle.kernelState,
  });
  if (response.type !== "ofd-state-synced") {
    throw new Error(`unexpected kernel-worker response for sync-ofd-state: ${response.type}`);
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

function captureVforkSnapshot(handle: ProcessHandle, vmPtr: number): VforkSnapshot {
  // vfork also duplicates the caller's FD table into a distinct child process.
  // The parent is suspended, but both processes still hold references to the
  // shared OFDs until the child execs or exits.
  const ofdIds = trackForkSharedOfds(handle, vmPtr, true);
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

function composeHandlePipeUsed(handle: ProcessHandle, sharedPipeUsed: bigint): bigint {
  let pipeUsed = sharedPipeUsed | 1n;
  for (const pipeSlot of [handle.stdinPipe, handle.stdoutPipe, handle.stderrPipe]) {
    if (pipeSlot > 0) {
      pipeUsed |= 1n << BigInt(pipeSlot);
    }
  }
  return pipeUsed;
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
  if (handle.workerDebugLog.length > 48) {
    handle.workerDebugLog.splice(0, handle.workerDebugLog.length - 48);
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
        pipeUsed: composeHandlePipeUsed(member, pipeUsed),
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

function wakePipeWaitersInKernelShare(handle: ProcessHandle, pipeUsed: bigint): void {
  wakeBlockedStatusesInKernelShare(
    handle,
    [handle.kernel.abi_status_pipe_wait()],
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
  if (collectHeldEpollOfdIds(handle).length === 0) {
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

function requestEpollDependentOfdStateSync(handle: ProcessHandle): Promise<void> {
  if (!handle.kernelWorkerRequest || handle.pid <= 0) {
    return Promise.resolve();
  }

  for (const ofdId of collectEpollDependentOfdIds(handle)) {
    handle.ofdSyncQueuedIds.add(ofdId);
  }
  if (handle.ofdSyncQueuedIds.size === 0) {
    return Promise.resolve();
  }
  if (handle.ofdSyncPromise !== null) {
    return handle.ofdSyncPromise;
  }

  handle.ofdSyncPromise = (async () => {
    while (handle.ofdSyncQueuedIds.size > 0) {
      const ofdIds = [...handle.ofdSyncQueuedIds];
      handle.ofdSyncQueuedIds.clear();
      await syncOfdStateViaKernelWorker(handle, ofdIds);
    }
  })().finally(() => {
    handle.ofdSyncPromise = null;
  });

  return handle.ofdSyncPromise;
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
        wakePipeWaitersInKernelShare(handle, pipeUsed);
        if (kernelWorkerOwnsEpollState(handle)) {
          await requestEpollStateSync(handle, true);
        } else {
          wakeBlockedStatusesInKernelShare(
            handle,
            [handle.kernel.abi_status_epoll_wait()],
            pipeUsed,
          );
        }
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
    sabPageCache: handle.sabPageCache,
    runtimeNotify: handle.runtimeBridge.notifySab,
    runtimeState: handle.runtimeBridge.stateSab,
  } satisfies ThreadWorkerInbound);
}

function enqueueWorker(handle: ProcessHandle, worker: Worker): void {
  if (!handle.readyWorkers.includes(worker)) {
    const vmPtr = threadWorkerStates.get(worker)?.vmPtr;
    if (vmPtr !== undefined) {
      appendWorkerDebugLog(handle, `ready:0x${vmPtr.toString(16)}`);
    }
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
  const vmPtr = threadWorkerStates.get(next)?.vmPtr;
  if (vmPtr !== undefined) {
    appendWorkerDebugLog(handle, `step:0x${vmPtr.toString(16)}`);
  }
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
  if (handle.pid > 0 && processHandles.get(handle.pid) !== handle) {
    return;
  }

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
  handle.kernelState = adoptKernelState(handle.kernelState, msg.kernelState);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  if (msg.waitInWorker) {
    appendWorkerDebugLog(handle, `wait:0x${thread.vmPtr.toString(16)}@${msg.status}`);
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
    const waitInWorkerNeedsEpollSync = msg.status === handle.kernel.abi_status_epoll_wait()
      || (
        kernelWorkerOwnsSocketState(handle)
        && (
          msg.status === handle.kernel.abi_status_socket_recv()
          || msg.status === handle.kernel.abi_status_socket_send()
        )
      );
    if (isRunningWorker && kernelWorkerOwnsEpollState(handle) && waitInWorkerNeedsEpollSync) {
      const shouldSyncSocket = kernelWorkerOwnsSocketState(handle)
        && (
          msg.status === handle.kernel.abi_status_socket_recv()
          || msg.status === handle.kernel.abi_status_socket_send()
        );
      const syncSocketState = shouldSyncSocket
        ? requestSocketStateSync(handle, false)
        : Promise.resolve();
      void syncSocketState
        .then(() => requestEpollStateSync(handle, false))
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
  const completedSyncEffects = msg.syncEffects ?? (
    status === handle.kernel.abi_status_ok() ? LEGACY_OK_SYNC_EFFECTS : 0
  );
  const continueAfterOwnedStateSync = (): void => {
    if (status === handle.kernel.abi_status_ok()) {
      const shouldSyncPipeState = wroteToPipe
        || drainedPipe
        || (completedSyncEffects & SYNC_EFFECT_PIPE_STATE) !== 0;
      const shouldSyncLifecycle = (completedSyncEffects & SYNC_EFFECT_PROCESS_LIFECYCLE) !== 0;
      const shouldWakeSignalWaiters = (completedSyncEffects & SYNC_EFFECT_SIGNAL_WAITERS) !== 0;
      if (handle.kernelWorkerRequest && handle.pid > 0 && (shouldSyncPipeState || shouldSyncLifecycle)) {
        appendWorkerDebugLog(handle, `ok-sync-start:0x${thread.vmPtr.toString(16)}`);
        pumpProcess(handle);
        const syncPipeState = shouldSyncPipeState
          ? requestPipeStateSync(handle, true)
          : Promise.resolve();
        void syncPipeState
          .then(() => (shouldSyncLifecycle
            ? syncProcessLifecycleViaKernelWorker(handle, thread.vmPtr)
            : Promise.resolve()))
          .then(() => {
            appendWorkerDebugLog(handle, `ok-sync-done:0x${thread.vmPtr.toString(16)}`);
            if (shouldWakeSignalWaiters) {
              wakeRuntimeBridge(handle.runtimeBridge);
            }
            kickBlockedWorkers(handle);
            enqueueWorker(handle, worker);
          })
          .catch((error) => {
            appendWorkerDebugLog(handle, `ok-sync-err:0x${thread.vmPtr.toString(16)}`);
            failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (shouldWakeSignalWaiters) {
        wakeRuntimeBridge(handle.runtimeBridge);
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
      const ms = sec * 1000 + Math.floor(nsec / 1_000_000);
      if (ms <= 0) {
        appendWorkerDebugLog(handle, `yield:0x${thread.vmPtr.toString(16)}`);
        enqueueWorker(handle, worker);
        return;
      }
      appendWorkerDebugLog(handle, `sleep:0x${thread.vmPtr.toString(16)}@${ms}`);
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
      appendWorkerDebugLog(handle, `fs:0x${thread.vmPtr.toString(16)}`);
      void resumeFsViaKernelWorker(handle, worker, thread.vmPtr);
      kickBlockedWorkers(handle);
      pumpProcess(handle);
      return;
    }

    if (status === handle.kernel.abi_status_connect()) {
      void resumeConnectViaKernelWorker(
        handle,
        worker,
        thread.vmPtr,
        BigInt(-handle.kernel.abi_econnrefused()),
      );
      kickBlockedWorkers(handle);
      pumpProcess(handle);
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
        pumpProcess(handle);
        void syncForkSharedStateViaKernelWorker(
          handle,
          kernelShareOtherMembers(handle),
          { includeMemfs: false, preserveSourceRefcounts: true },
        )
          .then(() => {
            if (!handle.workers.has(worker) || !threadWorkerStates.has(worker)) {
              return;
            }
            if (!handle.vforkSnapshot || handle.vforkSnapshot.vmPtr !== thread.vmPtr) {
              return;
            }
            if (handle.vforkSnapshot.childPid === null) {
              const provisionalChildPid = handle.kernel.thread_vfork_child_pid();
              if (provisionalChildPid > 0) {
                void canonicalizeActiveVforkChildViaKernelWorker(
                  handle,
                  worker,
                  thread.vmPtr,
                  provisionalChildPid,
                );
                return;
              }
            }
            enqueueWorker(handle, worker);
          })
          .catch((error) => {
            failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
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

  const shouldWakeSocketWaiters = status === handle.kernel.abi_status_ok()
    && (completedSyncEffects & SYNC_EFFECT_SOCKET_STATE) !== 0;
  const shouldSyncForkSharedState = isRunningWorker
    && handle.kernelWorkerRequest !== null
    && handle.pid > 0
    && handle.kernelShare.sharedOfdIds.size > 0
    && (completedSyncEffects & SYNC_EFFECT_FORK_SHARED_OFD_STATE) !== 0
    && !statusDefersOwnedStateSync(handle, status);
  const shouldSyncEpollDependentOfds = isRunningWorker
    && kernelWorkerOwnsEpollState(handle)
    && (completedSyncEffects & SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE) !== 0
    && !statusDefersOwnedStateSync(handle, status);
  const shouldSyncSocketState = isRunningWorker
    && kernelWorkerOwnsSocketState(handle)
    && (completedSyncEffects & SYNC_EFFECT_SOCKET_STATE) !== 0
    && !statusDefersOwnedStateSync(handle, status);
  const shouldSyncEpollState = isRunningWorker
    && kernelWorkerOwnsEpollState(handle)
    && collectHeldEpollOfdIds(handle).length > 0
    && (completedSyncEffects & SYNC_EFFECT_EPOLL_STATE) !== 0
    && !statusDefersOwnedStateSync(handle, status);
  if (shouldSyncForkSharedState || shouldSyncEpollDependentOfds || shouldSyncSocketState || shouldSyncEpollState) {
    const syncForkSharedState = shouldSyncForkSharedState
      ? syncForkSharedStateViaKernelWorker(
        handle,
        kernelShareOtherMembers(handle),
        { includeMemfs: false },
      )
      : Promise.resolve();
    const syncOfdState = shouldSyncEpollDependentOfds
      ? requestEpollDependentOfdStateSync(handle)
      : Promise.resolve();
    const syncSocketState = shouldSyncSocketState
      ? requestSocketStateSync(handle, shouldWakeSocketWaiters)
      : Promise.resolve();
    pumpProcess(handle);
    void syncForkSharedState
      .then(() => syncOfdState)
      .then(() => syncSocketState)
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
  appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `exit-begin:${handle.pid}:held=[${[...collectProcessHeldOfds(handle)].sort((left, right) => left - right).join(",")}]:shared=[${[...handle.kernelShare.sharedOfdIds].sort((left, right) => left - right).join(",")}]`,
  );
  if (kernelWorkerOwnsPipeState(handle)) {
    await requestPipeStateSync(handle, true);
  }
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
    await releaseSharedHostStdio(handle);
  } else {
    releaseSharedHostStdioLocal(handle);
  }
  for (const worker of [...handle.blockedWorkers.keys()]) {
    clearBlockedWorker(handle, worker);
  }
  const liveThreadDebug = formatAllLiveThreadDebug(handle);
  unregisterProcessWithKernelWorker(handle);
  detachKernelShare(handle);
  handle.lastDebugMessage = [
    `exit=${exitCode}`,
    `signal=${termSignal}`,
    `cmd=${JSON.stringify(handle.debugCommand)}`,
    `worker_log=[${handle.workerDebugLog.join(", ")}]`,
    `thread_debugs=[${liveThreadDebug}]`,
    handle.lastDebugMessage ?? "debug=n/a",
  ].join(" ");
  if (handle.pid > 0 && processHandles.get(handle.pid) === handle) {
    processHandles.delete(handle.pid);
  }
  appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
  );
  appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `exit-debug:${handle.pid}:${handle.lastDebugMessage ?? "debug=n/a"}`,
  );
  recordExitOnParent(handle, exitCode, termSignal);
  handle.onExit?.(exitCode);
}

function failProcess(handle: ProcessHandle, error: Error): void {
  drainProcessOutput(handle);
  appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `fail:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${JSON.stringify(error.message)}`,
  );
  handle.lastDebugMessage = [
    `cmd=${JSON.stringify(handle.debugCommand)}`,
    `worker_log=[${handle.workerDebugLog.join(", ")}]`,
    `thread_debugs=[${formatAllLiveThreadDebug(handle)}]`,
    handle.lastDebugMessage ?? "debug=n/a",
  ].join(" ");
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

  appendWorkerDebugLog(
    parent,
    `child-exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
  );
  if (exitCode !== 0 || termSignal !== 0) {
    appendWorkerDebugLog(
      parent,
      `child-debug:${handle.pid}:${handle.lastDebugMessage ?? "debug=n/a"}`,
    );
  }
  parent.lastDebugMessage = [
    `child-exit:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${exitCode}:${termSignal}`,
    ...(exitCode !== 0 || termSignal !== 0
      ? [`child-debug:${handle.pid}:${handle.lastDebugMessage ?? "debug=n/a"}`]
      : []),
    parent.lastDebugMessage ?? "debug=n/a",
  ].join(" ");

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
  appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `spawn-handoff:${handle.pid}:${handoff.kind}:${handoff.childPid}`,
  );

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
  appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:start:${childPid}`);
  appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `vfork-parent-fds:${childPid}:${formatOpenFdSummary(handle)}`,
  );
  appendWorkerDebugLog(handle, `vfork-parent-fds:${childPid}:${formatOpenFdSummary(handle)}`);
  handle.lastDebugMessage = `vfork-execve:start child=${childPid}`;
  const snapshot = handle.vforkSnapshot;
  const inheritedSharedOfdIds = new Set(snapshot?.inheritedSharedOfdIds ?? []);
  const handoff = readProcessSpawnHandoff(handle.kernel, parentThread.vmPtr);
  const execveGuestPayload = captureExecveGuestHandoff(
    handle,
    parentThread.vmPtr,
    Number(handoff.path),
    Number(handoff.argv),
    Number(handoff.envp),
  );
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  const childMemory = new WebAssembly.Memory({
    initial: Math.ceil(Math.max(BOOTSTRAP_MEMORY_BYTES, handle.guestBase + 0x10000) / WASM_PAGE_SIZE),
    maximum: WASM_MAX_PAGES,
    shared: true,
  });

  const childInstance = await WebAssembly.instantiate(handle.wasmModule, {
    env: createKernelEnv(childMemory, { sabPageCache: handle.sabPageCache }),
  });
  const childKernel = childInstance.exports as unknown as KernelExports;
  copyKernelPrivateRegion(handle.memory, childMemory, handle.guestBase);
  attachKernelRuntimeState(childKernel, handle.kernelState);

  const childVmPtr = parentThread.vmPtr;
  const stagedExecveHandoff = stageExecveGuestHandoff(
    childKernel,
    handle.guestBase,
    execveGuestPayload,
  );
  childKernel.thread_set_execve_handoff(
    childVmPtr,
    stagedExecveHandoff.processPtr,
    stagedExecveHandoff.pathPtr,
    stagedExecveHandoff.argvPtr,
    stagedExecveHandoff.envpPtr,
  );
  let execRet: number;
  try {
    execRet = Number(childKernel.thread_complete_execve_handoff(childVmPtr));
  } catch (error) {
    const debugStage = childKernel.debug_execve_stage?.();
    const debugValue0 = childKernel.debug_execve_value0?.();
    const debugValue1 = childKernel.debug_execve_value1?.();
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    throw new Error(
      `thread_complete_execve_handoff trapped for pid=${childPid}: ${detail}`
        + ` (stage=${debugStage ?? "n/a"}, value0=${debugValue0 ?? "n/a"}, value1=${debugValue1 ?? "n/a"})`,
    );
  }
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
    debugCommand: execveGuestPayload.argv[0]
      ? decodeExecveBytes(execveGuestPayload.argv[0])
      : decodeExecveBytes(execveGuestPayload.path),
    lastDebugMessage: undefined,
    runtimeBridge: createRuntimeStateBridge(),
    sabPageCache: handle.sabPageCache,
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
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    socketSyncPromise: null,
    socketSyncQueued: false,
    socketWaiterWakeQueued: false,
    vforkSnapshot: null,
    vforkWorker: null,
    onStdout: createInheritedStdoutSink(handle),
    onError: createInheritedErrorSink(
      handle,
      childPid,
      (pid) => processHandles.get(pid),
      failProcess,
    ),
    workerDebugLog: ["spawn:vfork-execve"],
  };
  assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-execve");
  appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-kernel-ready:${childPid}`);
  handle.lastDebugMessage = `vfork-execve:child-kernel-ready child=${childPid}`;
  if (handle.kernelWorkerRequest && handle.pid > 0) {
    await resumeVforkExecveParentViaKernelWorker(handle, parentThread.vmPtr, childPid);
    const parentRehydrateOfdIds = [...inheritedSharedOfdIds].sort((left, right) => left - right);
    if (parentRehydrateOfdIds.length > 0) {
      await rehydrateForkSharedStateViaKernelWorker([handle], parentRehydrateOfdIds, {
        includeMemfs: false,
      });
    }
    appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-parent-restored-fds:${childPid}:${formatOpenFdSummary(handle)}`,
    );
    appendWorkerDebugLog(handle, `vfork-parent-restored-fds:${childPid}:${formatOpenFdSummary(handle)}`);
    assertProcessStandardFdIntegrity(handle, "vfork-parent-restored");
  } else {
    handle.kernel.thread_clear_spawn_handoff(parentThread.vmPtr);
    handle.kernel.thread_resume_vfork_parent(parentThread.vmPtr, childPid);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
  }
  publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);

  reconcileInheritedSharedOfdsAfterVforkExecve(handle, inheritedSharedOfdIds);
  adoptKernelShare(handle, childHandle);
  if (handle.kernelWorkerRequest && handle.pid > 0) {
    const sharedOfdIds = activeKernelShareOfdIds(handle);
    const childHeldOfdIds = collectProcessHeldOfds(childHandle);
    const rehydratedSharedOfdIds = sharedOfdIds
      .filter((ofdId) => !childHeldOfdIds.has(ofdId));
    const procSelfExeTarget = execveGuestPayload.path.length > 0
      && execveGuestPayload.path[execveGuestPayload.path.length - 1] === 0
      ? execveGuestPayload.path.slice(0, -1)
      : execveGuestPayload.path;
    await registerProcessWithKernelWorker(childHandle, {
      seedMemfsMode: "none",
      pipeSeedMode: "process",
    });
    appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-registered:${childPid}`);
    handle.lastDebugMessage = `vfork-execve:child-registered child=${childPid}`;
    assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-register");
    await syncForkSharedStateViaKernelWorker(childHandle, kernelShareOtherMembers(childHandle), {
      includeMemfs: false,
      trackHeldOfds: false,
    });
    appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-synced:${childPid}`);
    handle.lastDebugMessage = `vfork-execve:child-synced child=${childPid}`;
    assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-sync-fork-shared-state");
    const preservedOfdSlots = snapshotKernelShareOfdSlots(childHandle, childHeldOfdIds);
    const preservedPipeSlots = snapshotLocalPipeSlots(childHandle, childHeldOfdIds);
    discardCopiedSharedKernelObjects(childHandle, preservedOfdSlots);
    assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-discard");
    await rehydrateForkSharedStateViaKernelWorker([childHandle], rehydratedSharedOfdIds, {
      procSelfExeTargets: procSelfExeTarget.length > 0
        ? [{ pid: childHandle.pid, bytes: procSelfExeTarget }]
        : undefined,
    });
    appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-rehydrated:${childPid}`);
    appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:child-fds:${childPid}:${formatPipeCounterDebug(childHandle, 1)}:${formatPipeCounterDebug(childHandle, 2)}`,
    );
    appendWorkerDebugLog(childHandle, `vfork-child-fds:${childPid}:${formatOpenFdSummary(childHandle)}`);
    handle.lastDebugMessage = `vfork-execve:child-rehydrated child=${childPid}`;
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreLocalPipeSlots(childHandle, preservedPipeSlots);
    assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-rehydrate");
  } else {
    const childHeldOfdIds = collectProcessHeldOfds(childHandle);
    const preservedOfdSlots = snapshotKernelShareOfdSlots(childHandle, childHeldOfdIds);
    const preservedPipeSlots = snapshotLocalPipeSlots(childHandle, childHeldOfdIds);
    syncForkSharedStateLocally(childHandle, kernelShareOtherMembers(childHandle), {
      includeMemfs: false,
      trackHeldOfds: false,
    });
    discardCopiedSharedKernelObjects(childHandle);
    syncForkSharedStateLocally(handle, [childHandle]);
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreLocalPipeSlots(childHandle, preservedPipeSlots);
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
  handle.lastDebugMessage = `vfork-execve:child-visible child=${childPid}`;
  appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `spawn:${childPid}:${JSON.stringify(childHandle.debugCommand)}`,
  );
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
  appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:start:${childPid}`);
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
    env: createKernelEnv(childMemory, { sabPageCache: handle.sabPageCache }),
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
    debugCommand: handle.debugCommand,
    lastDebugMessage: undefined,
    runtimeBridge: createRuntimeStateBridge(),
    sabPageCache: handle.sabPageCache,
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
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    socketSyncPromise: null,
    socketSyncQueued: false,
    socketWaiterWakeQueued: false,
    vforkSnapshot: null,
    vforkWorker: null,
    onStdout: createInheritedStdoutSink(handle),
    onError: createInheritedErrorSink(
      handle,
      childPid,
      (pid) => processHandles.get(pid),
      failProcess,
    ),
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
  removeWorkerFromPool(worker);
  threadWorkerStates.delete(worker);
  clearWorkerMessageHandler(worker);
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
  appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `destroy:${handle.pid}:${JSON.stringify(handle.debugCommand)}:${signal}:${notifyExit ? 1 : 0}`,
  );
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
