import type { FsSnapshotEntry } from "../fs/types.js";
import {
  assertProcessStandardFdIntegrity as assertProcessStandardFdIntegrityImpl,
  debugRuntimeExecutionStats as debugRuntimeExecutionStatsImpl,
  debugRuntimeProcessHandles as debugRuntimeProcessHandlesImpl,
  formatAllLiveThreadDebug as formatAllLiveThreadDebugImpl,
} from "./diagnostics/snapshot.js";
import {
  debugRuntimeWorkerLogFocus as debugRuntimeWorkerLogFocusImpl,
  summarizeProcessWorkerLogFocus as summarizeProcessWorkerLogFocusImpl,
  type RuntimeProcessWorkerLogFocus,
} from "./diagnostics/worker-focus.js";
import {
  retainRuntimeLifecycleEvents,
} from "./diagnostics/lifecycle-log.js";
import {
  applyFsEntryToHandle,
  cloneSnapshotEntries,
  snapshotEntriesEqual,
  snapshotProcessFs,
} from "./process-fs.js";
import { processMemoryOwnership } from "./memory-ownership-state.js";
import {
  processDebugCommand,
  processLastDebugMessage,
} from "./process-debug-state.js";
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

export interface RuntimeFailureSnapshot {
  pid: number;
  cmd: string;
  exitCode: number;
  termSignal: number;
  debug: string;
  memory: {
    memoryBytes: number;
    guestBase: number;
    processCommittedBytes: number;
    guestMemoryPolicyLimitBytes: string;
    guestAddressProfile: number | null;
    guestAddressLimitBytes: string | null;
    processMaxMemoryBytes: number;
    forkProcessMaxMemoryBytes: number;
    pageCacheDataCapacityBytes: number | null;
    publishedMetadataCapacityBytes: number | null;
    fsRpcRingCapacityBytes: number | null;
    kernelWorkerPrivateMemoryBytes: number | null;
    artifactMaterializationBudgetBytes: number | null;
    artifactMaterializedBytes: number | null;
  };
  memorySyscalls: string[];
  workerFocus: RuntimeProcessWorkerLogFocus[];
}

export function createWorkerRuntimeContext(deps: WorkerRuntimeContextDeps) {
  const processHandles = new Map<number, ProcessHandle>();
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  const runtimeFsStates = new Map<number, RuntimeFsState>();
  const runtimeFsDirtyHandles = new Map<number, Set<ProcessHandle>>();
  const runtimeLifecycleEvents = new Map<number, string[]>();
  const runtimeFailureSnapshots = new Map<number, RuntimeFailureSnapshot>();
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

  function captureFailureMemorySnapshot(handle: ProcessHandle): RuntimeFailureSnapshot["memory"] {
    const ownership = processMemoryOwnership(handle);
    const memoryBytes = handle.memory.buffer.byteLength;
    return {
      memoryBytes,
      guestBase: handle.guestBase,
      processCommittedBytes: Math.max(0, memoryBytes - handle.guestBase),
      guestMemoryPolicyLimitBytes: String(
        ownership?.guestMemoryPolicyLimitBytes ?? handle.guestMemoryPolicyLimitBytes,
      ),
      guestAddressProfile: ownership?.guestAddressProfile ?? null,
      guestAddressLimitBytes: ownership?.guestAddressLimitBytes ?? null,
      processMaxMemoryBytes: ownership?.processMaxMemoryBytes ?? handle.maxMemoryPages * 65536,
      forkProcessMaxMemoryBytes: ownership?.forkProcessMaxMemoryBytes ?? handle.forkMaxMemoryPages * 65536,
      pageCacheDataCapacityBytes: ownership?.pageCacheDataCapacityBytes ?? handle.sabPageCache?.byteLength ?? null,
      publishedMetadataCapacityBytes: ownership?.publishedMetadataCapacityBytes ?? null,
      fsRpcRingCapacityBytes: ownership?.fsRpcRingCapacityBytes ?? null,
      kernelWorkerPrivateMemoryBytes: ownership?.kernelWorkerPrivateMemoryBytes ?? null,
      artifactMaterializationBudgetBytes: ownership?.artifactMaterializationBudgetBytes ?? null,
      artifactMaterializedBytes: ownership?.artifactMaterializedBytes ?? null,
    };
  }

  function captureRuntimeFailureSnapshot(
    handle: ProcessHandle,
    exitCode: number,
    termSignal: number,
  ): void {
    const memorySyscalls = captureMemorySyscallTrace(handle);
    const lastDebugMessage = processLastDebugMessage(handle);
    runtimeFailureSnapshots.set(handle.runtimeId, {
      pid: handle.pid,
      cmd: processDebugCommand(handle),
      exitCode,
      termSignal,
      debug: lastDebugMessage ?? debugRuntimeProcessHandles(handle.runtimeId),
      memory: captureFailureMemorySnapshot(handle),
      memorySyscalls,
      workerFocus: [{
        pid: handle.pid,
        cmd: processDebugCommand(handle),
        workers: summarizeProcessWorkerLogFocusImpl(handle, threadWorkerStates),
      }],
    });
  }

  function debugRuntimeLastFailureSnapshot(runtimeId: number): RuntimeFailureSnapshot | null {
    return runtimeFailureSnapshots.get(runtimeId) ?? null;
  }

  function clearRuntimeFailureSnapshot(runtimeId: number): void {
    runtimeFailureSnapshots.delete(runtimeId);
  }

  function extractDebugFieldValues(debug: string | undefined, key: string): string[] {
    if (!debug) {
      return [];
    }
    const escapedKey = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const matches = debug.matchAll(new RegExp(`(?:^|\\s)${escapedKey}=([^\\s]+)`, "g"));
    const values: string[] = [];
    for (const match of matches) {
      if (match[1]) {
        values.push(match[1]);
      }
    }
    return values;
  }

  function captureMemorySyscallTrace(handle: ProcessHandle): string[] {
    const debugValues = extractDebugFieldValues(processLastDebugMessage(handle), "memory_syscalls");
    if (debugValues.length > 0) {
      return debugValues.at(-1)?.split(",").filter((entry) => entry.length > 0) ?? [];
    }

    const processPtr = handle.kernel.thread_process_ptr?.(handle.vmPtr) ?? 0;
    const len = handle.kernel.debug_memory_syscall_trace_len?.() ?? 0;
    if (len <= 0) {
      return [];
    }

    const byProcessPtr: string[] = [];
    const byPid: string[] = [];
    for (let index = 0; index < len; index += 1) {
      const entryProcessPtr = handle.kernel.debug_memory_syscall_trace_entry_process_ptr?.(index) ?? 0;
      const entryPid = handle.kernel.debug_memory_syscall_trace_entry_pid?.(index) ?? 0;
      const line = [
        `idx=${index}`,
        `nr=${handle.kernel.debug_memory_syscall_trace_entry_nr?.(index)?.toString() ?? "n/a"}`,
        `a0=${handle.kernel.debug_memory_syscall_trace_entry_arg0?.(index)?.toString() ?? "n/a"}`,
        `a1=${handle.kernel.debug_memory_syscall_trace_entry_arg1?.(index)?.toString() ?? "n/a"}`,
        `a2=${handle.kernel.debug_memory_syscall_trace_entry_arg2?.(index)?.toString() ?? "n/a"}`,
        `a3=${handle.kernel.debug_memory_syscall_trace_entry_arg3?.(index)?.toString() ?? "n/a"}`,
        `a4=${handle.kernel.debug_memory_syscall_trace_entry_arg4?.(index)?.toString() ?? "n/a"}`,
        `a5=${handle.kernel.debug_memory_syscall_trace_entry_arg5?.(index)?.toString() ?? "n/a"}`,
        `ret=${handle.kernel.debug_memory_syscall_trace_entry_result?.(index)?.toString() ?? "n/a"}`,
        `pid=${entryPid}`,
        `process=0x${entryProcessPtr.toString(16)}`,
      ].join(" ");

      if (processPtr !== 0 && entryProcessPtr === processPtr) {
        byProcessPtr.push(line);
      }
      if (entryPid === handle.pid) {
        byPid.push(line);
      }
    }

    const selected = byProcessPtr.length > 0 ? byProcessPtr : byPid;
    return selected.slice(-32);
  }

  function debugRuntimeProcessHandles(runtimeId: number): string {
    try {
      return debugRuntimeProcessHandlesImpl(
        runtimeId,
        processHandles,
        threadWorkerStates,
        runtimeLifecycleEvents,
        deps.formatPipeCounterDebug,
      );
    } catch (error) {
      return `unavailable:${error instanceof Error ? error.message : String(error)}`;
    }
  }

  function appendRuntimeLifecycleEvent(runtimeId: number, event: string): void {
    const events = runtimeLifecycleEvents.get(runtimeId) ?? [];
    runtimeLifecycleEvents.set(runtimeId, retainRuntimeLifecycleEvents([...events, event]));
  }

  function debugRuntimeWorkerLogFocus(runtimeId: number) {
    try {
      return debugRuntimeWorkerLogFocusImpl(runtimeId, processHandles, threadWorkerStates);
    } catch (error) {
      return [{
        pid: -1,
        cmd: `unavailable:${error instanceof Error ? error.message : String(error)}`,
        workers: [],
      }];
    }
  }

  function debugRuntimeExecutionStats(runtimeId: number) {
    try {
      return debugRuntimeExecutionStatsImpl(runtimeId, processHandles, threadWorkerStates);
    } catch (error) {
      return [{
        pid: -1,
        cmd: `unavailable:${error instanceof Error ? error.message : String(error)}`,
        running: false,
        vforkReadyWorkers: 0,
        hostBlockedWorkers: 0,
        workers: [],
      }];
    }
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
    captureRuntimeFailureSnapshot,
    clearRuntimeFsState,
    clearRuntimeFailureSnapshot,
    debugRuntimeLastFailureSnapshot,
    debugRuntimeExecutionStats,
    debugRuntimeProcessHandles,
    debugRuntimeWorkerLogFocus,
    flushRuntimeFsStateFromDirtyHandle,
    formatAllLiveThreadDebug,
    getRuntimeFsState,
    markRuntimeFsDirty,
    mirrorRuntimeFsEntry,
    primeRuntimeFsState,
    processHandles,
    publishRuntimeFsState,
    replaceRuntimeFsSnapshot,
    runtimeFailureSnapshots,
    runtimeFsDirtyHandles,
    runtimeFsStates,
    snapshotProcessFs,
    threadWorkerStates,
  };
}
