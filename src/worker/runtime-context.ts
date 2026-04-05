import type { FsSnapshotEntry } from "../fs/types.js";
import {
  appendRuntimeLifecycleEvent as appendRuntimeLifecycleEventImpl,
  assertProcessStandardFdIntegrity as assertProcessStandardFdIntegrityImpl,
  debugRuntimeProcessHandles as debugRuntimeProcessHandlesImpl,
  formatAllLiveThreadDebug as formatAllLiveThreadDebugImpl,
} from "./debug.js";
import {
  applyFsEntryToHandle,
  cloneSnapshotEntries,
  snapshotEntriesEqual,
  snapshotProcessFs,
} from "./process-fs.js";
import {
  kernelShareOtherMembers,
  kernelShareRepresentative,
  syncMemfsState,
} from "./kernel-share.js";
import {
  clearRuntimeFsState as clearRuntimeFsStateImpl,
  flushRuntimeFsStateFromDirtyHandle as flushRuntimeFsStateFromDirtyHandleImpl,
  getRuntimeFsState as getRuntimeFsStateImpl,
  markRuntimeFsDirty as markRuntimeFsDirtyImpl,
  mirrorRuntimeFsEntry as mirrorRuntimeFsEntryImpl,
  primeRuntimeFsState as primeRuntimeFsStateImpl,
  publishRuntimeFsState as publishRuntimeFsStateImpl,
  replaceRuntimeFsSnapshot as replaceRuntimeFsSnapshotImpl,
  type RuntimeFsContext,
} from "./runtime-fs.js";
import type {
  ProcessHandle,
  RuntimeFsState,
  ThreadWorkerState,
} from "./types.js";

interface WorkerRuntimeContextDeps {
  formatPipeCounterDebug(handle: ProcessHandle, fd: number): string;
}

export function createWorkerRuntimeContext(deps: WorkerRuntimeContextDeps) {
  const processHandles = new Map<number, ProcessHandle>();
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  const runtimeFsStates = new Map<number, RuntimeFsState>();
  const runtimeFsDirtyHandles = new Map<number, Set<ProcessHandle>>();
  const runtimeLifecycleEvents = new Map<number, string[]>();
  const runtimeFsContext: RuntimeFsContext = {
    processHandles,
    runtimeFsStates,
    runtimeFsDirtyHandles,
    kernelShareRepresentative,
    kernelShareOtherMembers,
    cloneSnapshotEntries,
    snapshotProcessFs,
    snapshotEntriesEqual,
    applyFsEntryToHandle,
    syncMemfsState,
  };

  function primeRuntimeFsState(runtimeId: number, entries: FsSnapshotEntry[]): number {
    return primeRuntimeFsStateImpl(runtimeId, entries, runtimeFsContext);
  }

  function publishRuntimeFsState(runtimeId: number, entries: FsSnapshotEntry[]): number {
    return publishRuntimeFsStateImpl(runtimeId, entries, runtimeFsContext);
  }

  function getRuntimeFsState(
    runtimeId: number,
  ): { version: number; entries: FsSnapshotEntry[] } | null {
    return getRuntimeFsStateImpl(runtimeId, runtimeFsContext);
  }

  function flushRuntimeFsStateFromDirtyHandle(
    runtimeId: number,
  ): { version: number; entries: FsSnapshotEntry[] } | null {
    return flushRuntimeFsStateFromDirtyHandleImpl(runtimeId, runtimeFsContext);
  }

  function clearRuntimeFsState(runtimeId: number): void {
    clearRuntimeFsStateImpl(runtimeId, runtimeFsContext);
  }

  function debugRuntimeProcessHandles(runtimeId: number): string {
    try {
      return debugRuntimeProcessHandlesImpl(
        runtimeId,
        processHandles,
        threadWorkerStates,
        runtimeLifecycleEvents,
      );
    } catch (error) {
      return `unavailable:${error instanceof Error ? error.message : String(error)}`;
    }
  }

  function appendRuntimeLifecycleEvent(runtimeId: number, event: string): void {
    appendRuntimeLifecycleEventImpl(runtimeId, event, runtimeLifecycleEvents);
  }

  function formatAllLiveThreadDebug(handle: ProcessHandle): string {
    try {
      return formatAllLiveThreadDebugImpl(handle, threadWorkerStates);
    } catch (error) {
      return `unavailable:${error instanceof Error ? error.message : String(error)}`;
    }
  }

  function assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void {
    assertProcessStandardFdIntegrityImpl(
      handle,
      context,
      debugRuntimeProcessHandles(handle.runtimeId),
      deps.formatPipeCounterDebug,
    );
  }

  function mirrorRuntimeFsEntry(runtimeId: number, entry: FsSnapshotEntry): void {
    mirrorRuntimeFsEntryImpl(runtimeId, entry, runtimeFsContext);
  }

  function replaceRuntimeFsSnapshot(runtimeId: number, entries: FsSnapshotEntry[]): void {
    replaceRuntimeFsSnapshotImpl(runtimeId, entries, runtimeFsContext);
  }

  function markRuntimeFsDirty(handle: ProcessHandle): void {
    markRuntimeFsDirtyImpl(handle, runtimeFsContext);
  }

  return {
    appendRuntimeLifecycleEvent,
    assertProcessStandardFdIntegrity,
    clearRuntimeFsState,
    debugRuntimeProcessHandles,
    flushRuntimeFsStateFromDirtyHandle,
    formatAllLiveThreadDebug,
    getRuntimeFsState,
    markRuntimeFsDirty,
    mirrorRuntimeFsEntry,
    primeRuntimeFsState,
    processHandles,
    publishRuntimeFsState,
    replaceRuntimeFsSnapshot,
    runtimeFsDirtyHandles,
    runtimeFsStates,
    threadWorkerStates,
  };
}
