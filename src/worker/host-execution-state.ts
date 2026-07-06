import type { HostBlockedWorkerState, ProcessHandle, VforkSnapshot } from "./types.js";

export interface HostExecutionState {
  vforkReadyWorkers: Worker[];
  runningWorker: Worker | null;
  hostBlockedWorkers: Map<Worker, HostBlockedWorkerState>;
  vforkSnapshot: VforkSnapshot | null;
  vforkWorker: Worker | null;
}

const hostExecutionStates = new WeakMap<ProcessHandle, HostExecutionState>();
const HOST_EXECUTION_STATE = Symbol.for("tidemark.worker.hostExecutionState");

type HostExecutionStateCarrier = ProcessHandle & {
  [HOST_EXECUTION_STATE]?: HostExecutionState;
};

function storeHostExecutionState(handle: ProcessHandle, state: HostExecutionState): HostExecutionState {
  hostExecutionStates.set(handle, state);
  Object.defineProperty(handle, HOST_EXECUTION_STATE, {
    configurable: true,
    enumerable: false,
    value: state,
    writable: true,
  });
  return state;
}

export function hostExecutionState(handle: ProcessHandle): HostExecutionState {
  let state = hostExecutionStates.get(handle);
  if (!state) {
    const carrier = handle as HostExecutionStateCarrier;
    if (Object.prototype.hasOwnProperty.call(carrier, HOST_EXECUTION_STATE)) {
      const sharedState = carrier[HOST_EXECUTION_STATE];
      if (sharedState !== undefined) {
        return storeHostExecutionState(handle, sharedState);
      }
    }
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
    storeHostExecutionState(handle, state);
  }
  return state;
}
