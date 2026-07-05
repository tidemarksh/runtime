import type { ProcessHandle } from "./types.js";

interface StdioBufferState {
  stdinQueue: Uint8Array[];
  outputBacklog: Uint8Array[];
}

const stdioBufferStates = new WeakMap<ProcessHandle, StdioBufferState>();

function stdioBufferState(handle: ProcessHandle): StdioBufferState {
  let state = stdioBufferStates.get(handle);
  if (!state) {
    state = {
      stdinQueue: [],
      outputBacklog: [],
    };
    stdioBufferStates.set(handle, state);
  }
  return state;
}

export function stdinQueue(handle: ProcessHandle): Uint8Array[] {
  return stdioBufferState(handle).stdinQueue;
}

export function outputBacklog(handle: ProcessHandle): Uint8Array[] {
  return stdioBufferState(handle).outputBacklog;
}
