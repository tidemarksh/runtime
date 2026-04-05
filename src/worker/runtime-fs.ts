import { attachKernelRuntimeState } from "../abi.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import { detachedKernelScratchBase } from "./kernel-scratch.js";
import { normalizePath, preloadSnapshotEntries } from "./process-fs.js";
import type {
  KernelShareGroup,
  KernelSyncHandle,
  ProcessHandle,
  RuntimeFsState,
} from "./types.js";

export interface RuntimeFsContext {
  processHandles: Map<number, ProcessHandle>;
  runtimeFsStates: Map<number, RuntimeFsState>;
  runtimeFsDirtyHandles: Map<number, Set<ProcessHandle>>;
  kernelShareRepresentative(handle: ProcessHandle): ProcessHandle | null;
  kernelShareOtherMembers(handle: ProcessHandle): ProcessHandle[];
  cloneSnapshotEntries(entries: FsSnapshotEntry[]): FsSnapshotEntry[];
  snapshotProcessFs(handle: ProcessHandle): FsSnapshotEntry[];
  snapshotEntriesEqual(left: FsSnapshotEntry[], right: FsSnapshotEntry[]): boolean;
  applyFsEntryToHandle(handle: ProcessHandle, entry: FsSnapshotEntry): void;
  syncMemfsState(source: KernelSyncHandle, target: KernelSyncHandle): void;
}

function writeRuntimeFsState(
  runtimeId: number,
  entries: FsSnapshotEntry[],
  bumpVersion: boolean,
  runtimeFsStates: Map<number, RuntimeFsState>,
  cloneSnapshotEntries: RuntimeFsContext["cloneSnapshotEntries"],
): number {
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

export function primeRuntimeFsState(
  runtimeId: number,
  entries: FsSnapshotEntry[],
  context: RuntimeFsContext,
): number {
  return writeRuntimeFsState(runtimeId, entries, false, context.runtimeFsStates, context.cloneSnapshotEntries);
}

export function publishRuntimeFsState(
  runtimeId: number,
  entries: FsSnapshotEntry[],
  context: RuntimeFsContext,
): number {
  return writeRuntimeFsState(runtimeId, entries, true, context.runtimeFsStates, context.cloneSnapshotEntries);
}

export function getRuntimeFsState(
  runtimeId: number,
  context: RuntimeFsContext,
): { version: number; entries: FsSnapshotEntry[] } | null {
  const state = context.runtimeFsStates.get(runtimeId);
  if (!state) {
    return null;
  }
  return {
    version: state.version,
    entries: context.cloneSnapshotEntries(state.entries),
  };
}

export function flushRuntimeFsStateFromDirtyHandle(
  runtimeId: number,
  context: RuntimeFsContext,
): { version: number; entries: FsSnapshotEntry[] } | null {
  const dirtyHandles = context.runtimeFsDirtyHandles.get(runtimeId);
  if (!dirtyHandles || dirtyHandles.size === 0) {
    return null;
  }

  const liveRepresentatives = new Map<KernelShareGroup, ProcessHandle>();
  for (const handle of dirtyHandles) {
    if (handle.pid > 0 && context.processHandles.get(handle.pid) !== handle) {
      continue;
    }
    const representative = context.kernelShareRepresentative(handle) ?? handle;
    liveRepresentatives.set(representative.kernelShare, representative);
  }

  context.runtimeFsDirtyHandles.delete(runtimeId);
  if (liveRepresentatives.size === 0) {
    return null;
  }

  let entries: FsSnapshotEntry[] | null = null;
  for (const representative of liveRepresentatives.values()) {
    const snapshot = context.snapshotProcessFs(representative);
    if (entries === null) {
      entries = snapshot;
      continue;
    }
    if (!context.snapshotEntriesEqual(entries, snapshot)) {
      throw new Error(`runtime ${runtimeId} has divergent process-local fs state`);
    }
  }

  if (entries === null) {
    context.runtimeFsDirtyHandles.delete(runtimeId);
    return null;
  }

  const version = publishRuntimeFsState(runtimeId, entries, context);
  return { version, entries };
}

export function clearRuntimeFsState(runtimeId: number, context: RuntimeFsContext): void {
  context.runtimeFsStates.delete(runtimeId);
  context.runtimeFsDirtyHandles.delete(runtimeId);
}

export function forEachRuntimeKernelShare(
  runtimeId: number,
  context: RuntimeFsContext,
  fn: (representative: ProcessHandle) => void,
): void {
  if (runtimeId <= 0) {
    return;
  }

  const visited = new Set<KernelShareGroup>();
  for (const handle of context.processHandles.values()) {
    if (handle.runtimeId !== runtimeId) {
      continue;
    }
    const representative = context.kernelShareRepresentative(handle) ?? handle;
    const group = representative.kernelShare;
    if (visited.has(group)) {
      continue;
    }
    visited.add(group);
    fn(representative);
  }
}

export function mirrorRuntimeFsEntry(
  runtimeId: number,
  entry: FsSnapshotEntry,
  context: RuntimeFsContext,
): void {
  forEachRuntimeKernelShare(runtimeId, context, (representative) => {
    context.applyFsEntryToHandle(representative, entry);
    for (const member of context.kernelShareOtherMembers(representative)) {
      context.syncMemfsState(representative, member);
      attachKernelRuntimeState(member.kernel, member.kernelState);
    }
  });
}

export function replaceRuntimeFsSnapshot(
  runtimeId: number,
  entries: FsSnapshotEntry[],
  context: RuntimeFsContext,
): void {
  context.runtimeFsDirtyHandles.delete(runtimeId);
  forEachRuntimeKernelShare(runtimeId, context, (representative) => {
    representative.kernel.kernel_reset_memfs();
    const rootEntry = entries.find((entry) => entry.kind === "dir" && normalizePath(entry.path) === "/");
    if (rootEntry) {
      representative.kernel.memfs_set_mode(1, rootEntry.mode ?? 0o755);
    }
    const preloadScratchBase = detachedKernelScratchBase(representative.memory, representative.guestBase, 0);
    preloadSnapshotEntries(representative.kernel, entries, preloadScratchBase);
    for (const member of context.kernelShareOtherMembers(representative)) {
      context.syncMemfsState(representative, member);
      attachKernelRuntimeState(member.kernel, member.kernelState);
    }
  });
}

export function markRuntimeFsDirty(handle: ProcessHandle, context: RuntimeFsContext): void {
  if (handle.runtimeId <= 0) {
    return;
  }
  let dirtyHandles = context.runtimeFsDirtyHandles.get(handle.runtimeId);
  if (!dirtyHandles) {
    dirtyHandles = new Set();
    context.runtimeFsDirtyHandles.set(handle.runtimeId, dirtyHandles);
  }
  dirtyHandles.add(handle);
}
