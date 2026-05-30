import type { FsSnapshotEntry } from "../../fs/types.js";
import type {
  CreateProcessOptions,
  ProcessHandle,
} from "../types.js";
import type { createWorkerRuntimeContext } from "../runtime-context.js";

type WorkerRuntimeContext = ReturnType<typeof createWorkerRuntimeContext>;

export interface WorkerOrchestratorDeps {
  ensureCompiledModule(wasmBytes: BufferSource): Promise<WebAssembly.Module>;
}

export interface WorkerOrchestrator {
  clearRuntimeFsState(runtimeId: number): void;
  clearRuntimeFailureSnapshot(runtimeId: number): void;
  createProcess(
    wasmBytes: BufferSource,
    elfData: Uint8Array,
    options?: CreateProcessOptions,
  ): Promise<ProcessHandle>;
  debugRuntimeExecutionStats(runtimeId: number): ReturnType<WorkerRuntimeContext["debugRuntimeExecutionStats"]>;
  debugRuntimeLastFailureSnapshot(runtimeId: number): ReturnType<WorkerRuntimeContext["debugRuntimeLastFailureSnapshot"]>;
  debugRuntimeProcessHandles(runtimeId: number): string;
  debugRuntimeWorkerLogFocus(runtimeId: number): ReturnType<WorkerRuntimeContext["debugRuntimeWorkerLogFocus"]>;
  destroyProcessTree(rootPid: number, runtimeId: number, signal?: number): void;
  destroyRuntimeProcesses(runtimeId: number, signal?: number): void;
  flushRuntimeFsStateFromDirtyHandle(runtimeId: number): { version: number; entries: FsSnapshotEntry[] } | null;
  getRuntimeFsState(runtimeId: number): { version: number; entries: FsSnapshotEntry[] } | null;
  injectHttpRequest(handle: ProcessHandle, port: number, request: string): Promise<string>;
  mirrorRuntimeFsEntry(runtimeId: number, entry: FsSnapshotEntry): void;
  primeRuntimeFsState(runtimeId: number, entries: FsSnapshotEntry[]): number;
  publishRuntimeFsState(runtimeId: number, entries: FsSnapshotEntry[]): number;
  replaceRuntimeFsSnapshot(runtimeId: number, entries: FsSnapshotEntry[]): void;
  writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void;
}

export type WorkerOrchestratorFacadePorts = WorkerOrchestrator;
