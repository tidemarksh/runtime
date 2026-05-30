import type {
  WorkerOrchestrator,
  WorkerOrchestratorFacadePorts,
} from "./types.js";

export function createWorkerOrchestratorFacade(
  ports: WorkerOrchestratorFacadePorts,
): WorkerOrchestrator {
  return {
    clearRuntimeFailureSnapshot: (runtimeId) => ports.clearRuntimeFailureSnapshot(runtimeId),
    clearRuntimeFsState: (runtimeId) => ports.clearRuntimeFsState(runtimeId),
    createProcess: (wasmBytes, elfData, options) => ports.createProcess(wasmBytes, elfData, options),
    debugRuntimeExecutionStats: (runtimeId) => ports.debugRuntimeExecutionStats(runtimeId),
    debugRuntimeLastFailureSnapshot: (runtimeId) => ports.debugRuntimeLastFailureSnapshot(runtimeId),
    debugRuntimeProcessHandles: (runtimeId) => ports.debugRuntimeProcessHandles(runtimeId),
    debugRuntimeWorkerLogFocus: (runtimeId) => ports.debugRuntimeWorkerLogFocus(runtimeId),
    destroyProcessTree: (rootPid, runtimeId, signal) => ports.destroyProcessTree(rootPid, runtimeId, signal),
    destroyRuntimeProcesses: (runtimeId, signal) => ports.destroyRuntimeProcesses(runtimeId, signal),
    flushRuntimeFsStateFromDirtyHandle: (runtimeId) => ports.flushRuntimeFsStateFromDirtyHandle(runtimeId),
    getRuntimeFsState: (runtimeId) => ports.getRuntimeFsState(runtimeId),
    injectHttpRequest: (handle, port, request) => ports.injectHttpRequest(handle, port, request),
    mirrorRuntimeFsEntry: (runtimeId, entry) => ports.mirrorRuntimeFsEntry(runtimeId, entry),
    primeRuntimeFsState: (runtimeId, entries) => ports.primeRuntimeFsState(runtimeId, entries),
    publishRuntimeFsState: (runtimeId, entries) => ports.publishRuntimeFsState(runtimeId, entries),
    replaceRuntimeFsSnapshot: (runtimeId, entries) => ports.replaceRuntimeFsSnapshot(runtimeId, entries),
    writeProcessStdin: (handle, data) => ports.writeProcessStdin(handle, data),
  };
}
