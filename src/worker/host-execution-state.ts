import type { HostBlockedWorkerState, ProcessHandle, VforkSnapshot } from "./types.js";

export interface HostExecutionState {
  vforkReadyWorkers: Worker[];
  runningWorker: Worker | null;
  hostBlockedWorkers: Map<Worker, HostBlockedWorkerState>;
  vforkSnapshot: VforkSnapshot | null;
  vforkWorker: Worker | null;
}

const hostExecutionStates = new WeakMap<ProcessHandle, HostExecutionState>();

export function hostExecutionState(handle: ProcessHandle): HostExecutionState {
  let state = hostExecutionStates.get(handle);
  if (!state) {
    const legacy = handle as unknown as Partial<HostExecutionState>;
    if (
      "vforkReadyWorkers" in legacy
      || "runningWorker" in legacy
      || "hostBlockedWorkers" in legacy
      || "vforkSnapshot" in legacy
      || "vforkWorker" in legacy
    ) {
      legacy.vforkReadyWorkers ??= [];
      legacy.runningWorker ??= null;
      legacy.hostBlockedWorkers ??= new Map();
      legacy.vforkSnapshot ??= null;
      legacy.vforkWorker ??= null;
      state = legacy as HostExecutionState;
    } else {
      state = {
        vforkReadyWorkers: [],
        runningWorker: null,
        hostBlockedWorkers: new Map(),
        vforkSnapshot: null,
        vforkWorker: null,
      };
    }
    hostExecutionStates.set(handle, state);
  }
  return state;
}
