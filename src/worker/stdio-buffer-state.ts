import type { ProcessHandle } from "./types.js";

interface StdioBufferState {
  stdinQueue: Uint8Array[];
  outputBacklog: Uint8Array[];
}

const stdioBufferStates = new WeakMap<ProcessHandle, StdioBufferState>();

type LegacyStdioBufferSeed = {
  stdinQueue?: Uint8Array[];
  outputBacklog?: Uint8Array[];
};

function stdioBufferState(handle: ProcessHandle): StdioBufferState {
  let state = stdioBufferStates.get(handle);
  if (!state) {
    const legacy = handle as LegacyStdioBufferSeed;
    state = {
      stdinQueue: legacy.stdinQueue ?? [],
      outputBacklog: legacy.outputBacklog ?? [],
    };
    stdioBufferStates.set(handle, state);
    if (legacy.stdinQueue !== undefined) {
      delete legacy.stdinQueue;
    }
    if (legacy.outputBacklog !== undefined) {
      delete legacy.outputBacklog;
    }
  }
  return state;
}

export function stdinQueue(handle: ProcessHandle): Uint8Array[] {
  return stdioBufferState(handle).stdinQueue;
}

export function outputBacklog(handle: ProcessHandle): Uint8Array[] {
  return stdioBufferState(handle).outputBacklog;
}
