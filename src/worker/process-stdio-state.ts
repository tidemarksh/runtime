import type { ProcessHandle, SharedHostStdio } from "./types.js";

export interface ProcessStdioState {
  stdinPipe: number;
  stdoutPipe: number;
  stderrPipe: number;
  ttyPtySlot: number;
  hostStdio: SharedHostStdio;
  stdinClosed: boolean;
  stdioOwner: ProcessHandle | null;
  inheritedParentOutput: boolean | null;
}

const processStdioStates = new WeakMap<ProcessHandle, ProcessStdioState>();
const PROCESS_STDIO_STATE = Symbol.for("tidemark.worker.processStdioState");

type ProcessStdioStateCarrier = ProcessHandle & {
  [PROCESS_STDIO_STATE]?: ProcessStdioState;
};

export function processStdioState(handle: ProcessHandle): ProcessStdioState {
  const state = processStdioStates.get(handle);
  if (state !== undefined) {
    return state;
  }
  const carrier = handle as ProcessStdioStateCarrier;
  if (Object.prototype.hasOwnProperty.call(carrier, PROCESS_STDIO_STATE)) {
    const sharedState = carrier[PROCESS_STDIO_STATE];
    if (sharedState !== undefined) {
      processStdioStates.set(handle, sharedState);
      return sharedState;
    }
  }
  const created: ProcessStdioState = {
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    ttyPtySlot: 0,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    stdinClosed: false,
    stdioOwner: null,
    inheritedParentOutput: null,
  };
  setProcessStdioState(handle, created);
  return created;
}

export function setProcessStdioState(handle: ProcessHandle, state: ProcessStdioState): void {
  processStdioStates.set(handle, state);
  Object.defineProperty(handle, PROCESS_STDIO_STATE, {
    configurable: true,
    enumerable: false,
    value: state,
    writable: true,
  });
}

export function processStdinPipe(handle: ProcessHandle): number {
  return processStdioState(handle).stdinPipe;
}

export function processStdoutPipe(handle: ProcessHandle): number {
  return processStdioState(handle).stdoutPipe;
}

export function processStderrPipe(handle: ProcessHandle): number {
  return processStdioState(handle).stderrPipe;
}

export function processTtyPtySlot(handle: ProcessHandle): number {
  return processStdioState(handle).ttyPtySlot;
}

export function processHostStdio(handle: ProcessHandle): SharedHostStdio {
  return processStdioState(handle).hostStdio;
}

export function processStdinClosed(handle: ProcessHandle): boolean {
  return processStdioState(handle).stdinClosed;
}

export function processStdioOwner(handle: ProcessHandle): ProcessHandle | null {
  return processStdioState(handle).stdioOwner;
}

export function processInheritedParentOutput(handle: ProcessHandle): boolean | null {
  return processStdioState(handle).inheritedParentOutput ?? null;
}

export function setProcessInheritedParentOutput(
  handle: ProcessHandle,
  inheritedParentOutput: boolean,
): void {
  processStdioState(handle).inheritedParentOutput = inheritedParentOutput;
}

export function setProcessStdinPipe(handle: ProcessHandle, stdinPipe: number): void {
  processStdioState(handle).stdinPipe = stdinPipe;
}

export function setProcessStdoutPipe(handle: ProcessHandle, stdoutPipe: number): void {
  processStdioState(handle).stdoutPipe = stdoutPipe;
}

export function setProcessStderrPipe(handle: ProcessHandle, stderrPipe: number): void {
  processStdioState(handle).stderrPipe = stderrPipe;
}

export function setProcessHostStdio(handle: ProcessHandle, hostStdio: SharedHostStdio): void {
  processStdioState(handle).hostStdio = hostStdio;
}

export function clearProcessLocalStdioPipes(handle: ProcessHandle): void {
  const state = processStdioState(handle);
  state.stdinPipe = 0;
  state.stdoutPipe = 0;
  state.stderrPipe = 0;
}

export function processHasLocalHostStdio(handle: ProcessHandle): boolean {
  const state = processStdioState(handle);
  return state.stdinPipe !== 0 || state.stdoutPipe !== 0 || state.stderrPipe !== 0;
}
