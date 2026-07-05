import type { ProcessHandle, SharedHostStdio } from "./types.js";

export interface ProcessStdioState {
  stdinPipe: number;
  stdoutPipe: number;
  stderrPipe: number;
  ttyPtySlot: number;
  hostStdio: SharedHostStdio;
  stdinClosed: boolean;
  stdioOwner: ProcessHandle | null;
}

const processStdioStates = new WeakMap<ProcessHandle, ProcessStdioState>();

type LegacyProcessStdioSeed = {
  stdinPipe?: number;
  stdoutPipe?: number;
  stderrPipe?: number;
  ttyPtySlot?: number;
  hostStdio?: SharedHostStdio;
  stdinClosed?: boolean;
  stdioOwner?: ProcessHandle | null;
};

function consumeLegacyProcessStdioState(handle: ProcessHandle): ProcessStdioState | null {
  const legacy = handle as LegacyProcessStdioSeed;
  if (
    legacy.hostStdio === undefined
    && legacy.stdinPipe === undefined
    && legacy.stdoutPipe === undefined
    && legacy.stderrPipe === undefined
    && legacy.ttyPtySlot === undefined
    && legacy.stdinClosed === undefined
    && legacy.stdioOwner === undefined
  ) {
    return null;
  }
  const state: ProcessStdioState = {
    stdinPipe: legacy.stdinPipe ?? 0,
    stdoutPipe: legacy.stdoutPipe ?? 0,
    stderrPipe: legacy.stderrPipe ?? 0,
    ttyPtySlot: legacy.ttyPtySlot ?? 0,
    hostStdio: legacy.hostStdio ?? {
      stdinPipe: legacy.stdinPipe ?? 0,
      stdoutPipe: legacy.stdoutPipe ?? 0,
      stderrPipe: legacy.stderrPipe ?? 0,
      ttyPtySlot: legacy.ttyPtySlot,
      refCount: 1,
    },
    stdinClosed: legacy.stdinClosed ?? false,
    stdioOwner: legacy.stdioOwner ?? null,
  };
  delete legacy.stdinPipe;
  delete legacy.stdoutPipe;
  delete legacy.stderrPipe;
  delete legacy.ttyPtySlot;
  delete legacy.hostStdio;
  delete legacy.stdinClosed;
  delete legacy.stdioOwner;
  return state;
}

export function processStdioState(handle: ProcessHandle): ProcessStdioState {
  const state = processStdioStates.get(handle);
  if (state !== undefined) {
    return state;
  }
  const legacy = consumeLegacyProcessStdioState(handle);
  if (legacy !== null) {
    processStdioStates.set(handle, legacy);
    return legacy;
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
  };
  processStdioStates.set(handle, created);
  return created;
}

export function setProcessStdioState(handle: ProcessHandle, state: ProcessStdioState): void {
  consumeLegacyProcessStdioState(handle);
  processStdioStates.set(handle, state);
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
