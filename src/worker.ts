/**
 * Process worker entrypoint and public surface.
 */

import type { FsSnapshotEntry } from "./fs/types.js";
import { createWorkerOrchestrator } from "./worker/orchestrator.js";
import type {
  CreateProcessOptions,
  ProcessHandle,
} from "./worker/types.js";

export type { CreateProcessOptions, ProcessHandle } from "./worker/types.js";
export { destroyWorkerPool } from "./worker/pool.js";

let cachedModule: WebAssembly.Module | null = null;

function bufferSourceByteLength(value: BufferSource): number {
  return ArrayBuffer.isView(value) ? value.byteLength : value.byteLength;
}

async function ensureCompiledModule(wasmBytes: BufferSource): Promise<WebAssembly.Module> {
  if (cachedModule === null) {
    if (bufferSourceByteLength(wasmBytes) === 0) {
      throw new Error("WASM module is not precompiled");
    }
    cachedModule = await WebAssembly.compile(wasmBytes);
  }
  return cachedModule;
}

export async function precompileModule(wasmBytes: BufferSource): Promise<WebAssembly.Module> {
  cachedModule = await WebAssembly.compile(wasmBytes);
  return cachedModule;
}

export function clearModuleCache(): void {
  cachedModule = null;
}

const workerOrchestrator = createWorkerOrchestrator({
  ensureCompiledModule,
});

export function primeRuntimeFsState(runtimeId: number, entries: FsSnapshotEntry[]): number {
  return workerOrchestrator.primeRuntimeFsState(runtimeId, entries);
}

export function publishRuntimeFsState(runtimeId: number, entries: FsSnapshotEntry[]): number {
  return workerOrchestrator.publishRuntimeFsState(runtimeId, entries);
}

export function getRuntimeFsState(
  runtimeId: number,
): { version: number; entries: FsSnapshotEntry[] } | null {
  return workerOrchestrator.getRuntimeFsState(runtimeId);
}

export function flushRuntimeFsStateFromDirtyHandle(
  runtimeId: number,
): { version: number; entries: FsSnapshotEntry[] } | null {
  return workerOrchestrator.flushRuntimeFsStateFromDirtyHandle(runtimeId);
}

export function clearRuntimeFsState(runtimeId: number): void {
  workerOrchestrator.clearRuntimeFsState(runtimeId);
}

export async function createProcess(
  wasmBytes: BufferSource,
  elfData: Uint8Array,
  options: CreateProcessOptions = {},
): Promise<ProcessHandle> {
  return workerOrchestrator.createProcess(wasmBytes, elfData, options);
}

export function writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void {
  workerOrchestrator.writeProcessStdin(handle, data);
}

export async function injectHttpRequest(
  handle: ProcessHandle,
  port: number,
  request: string,
): Promise<string> {
  return workerOrchestrator.injectHttpRequest(handle, port, request);
}

export function debugRuntimeProcessHandles(runtimeId: number): string {
  return workerOrchestrator.debugRuntimeProcessHandles(runtimeId);
}

export function mirrorRuntimeFsEntry(runtimeId: number, entry: FsSnapshotEntry): void {
  workerOrchestrator.mirrorRuntimeFsEntry(runtimeId, entry);
}

export function replaceRuntimeFsSnapshot(runtimeId: number, entries: FsSnapshotEntry[]): void {
  workerOrchestrator.replaceRuntimeFsSnapshot(runtimeId, entries);
}

export function destroyProcessTree(rootPid: number, runtimeId: number, signal = 15): void {
  workerOrchestrator.destroyProcessTree(rootPid, runtimeId, signal);
}

export function destroyRuntimeProcesses(runtimeId: number, signal = 15): void {
  workerOrchestrator.destroyRuntimeProcesses(runtimeId, signal);
}
