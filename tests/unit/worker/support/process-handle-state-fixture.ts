import {
  processKernelShareGroup,
  setProcessKernelShareGroup,
} from "../../../../src/worker/kernel-share.js";
import {
  getKernelShareForkSharedSync,
  setKernelShareForkSharedSync,
} from "../../../../src/worker/fork-shared-sync-state.js";
import {
  processOnError,
  processOnExit,
  processOnStdout,
  setProcessOnError,
  setProcessOnExit,
  setProcessOnStdout,
} from "../../../../src/worker/process-callback-state.js";
import {
  setProcessDebugCommand,
  setProcessLastDebugMessage,
} from "../../../../src/worker/process-debug-state.js";
import { setProcessStdioState } from "../../../../src/worker/process-stdio-state.js";
import {
  processWorkers,
  setProcessWorkers,
} from "../../../../src/worker/process-worker-state.js";
import type {
  KernelShareGroup,
  ProcessHandle,
  SharedHostStdio,
} from "../../../../src/worker/types.js";
import { workerDebugLog } from "../../../../src/worker/worker-debug-log.js";
import { seedProcessKernelWorkerRequestFixture } from "./kernel-worker-request-fixture.js";

type OnExitCallback = (code: number) => void;
type OnErrorCallback = (error: Error) => void;
type OnStdoutCallback = (data: Uint8Array) => void;
type KernelShareFixture = Partial<KernelShareGroup> & {
  members?: Set<ProcessHandle>;
  sharedOfdIds?: Set<number>;
  ofdSlotSnapshots?: Map<number, Uint8Array>;
};

type ProcessHandleStateFixture = ProcessHandle & {
  workers?: Iterable<Worker>;
  kernelShare?: KernelShareFixture;
  stdinPipe?: number;
  stdoutPipe?: number;
  stderrPipe?: number;
  ttyPtySlot?: number;
  hostStdio?: SharedHostStdio;
  stdinClosed?: boolean;
  stdioOwner?: ProcessHandle | null;
  onExit?: OnExitCallback | null;
  onError?: OnErrorCallback | null;
  onStdout?: OnStdoutCallback | null;
  debugCommand?: string;
  lastDebugMessage?: string | null;
  workerDebugLog?: string[];
};

function hasOwn(handle: ProcessHandleStateFixture, key: keyof ProcessHandleStateFixture): boolean {
  return Object.prototype.hasOwnProperty.call(handle, key);
}

function hasStdioFixture(handle: ProcessHandleStateFixture): boolean {
  return hasOwn(handle, "stdinPipe")
    || hasOwn(handle, "stdoutPipe")
    || hasOwn(handle, "stderrPipe")
    || hasOwn(handle, "ttyPtySlot")
    || hasOwn(handle, "hostStdio")
    || hasOwn(handle, "stdinClosed")
    || hasOwn(handle, "stdioOwner");
}

function installCallbackFixtureAccessors(handle: ProcessHandleStateFixture): void {
  Object.defineProperties(handle, {
    onExit: {
      configurable: true,
      enumerable: true,
      get: () => processOnExit(handle),
      set: (callback: OnExitCallback | null | undefined) => setProcessOnExit(handle, callback),
    },
    onError: {
      configurable: true,
      enumerable: true,
      get: () => processOnError(handle),
      set: (callback: OnErrorCallback | null | undefined) => setProcessOnError(handle, callback),
    },
    onStdout: {
      configurable: true,
      enumerable: true,
      get: () => processOnStdout(handle),
      set: (callback: OnStdoutCallback | null | undefined) => setProcessOnStdout(handle, callback),
    },
  });
}

export function seedProcessHandleStateFixture(handle: unknown): void {
  const fixture = handle as ProcessHandleStateFixture;

  seedProcessKernelWorkerRequestFixture(fixture);

  if (hasOwn(fixture, "kernelShare") && fixture.kernelShare !== undefined) {
    fixture.kernelShare.members ??= new Set();
    fixture.kernelShare.sharedOfdIds ??= new Set();
    fixture.kernelShare.ofdSlotSnapshots ??= new Map();
    const existingGroup = processKernelShareGroup(fixture);
    const existingForkSharedSync = getKernelShareForkSharedSync(existingGroup);
    for (const member of existingGroup.members) {
      fixture.kernelShare.members.add(member);
    }
    for (const ofdId of existingGroup.sharedOfdIds) {
      fixture.kernelShare.sharedOfdIds.add(ofdId);
    }
    for (const [ofdId, snapshot] of existingGroup.ofdSlotSnapshots ?? []) {
      fixture.kernelShare.ofdSlotSnapshots.set(ofdId, snapshot);
    }
    setProcessKernelShareGroup(fixture, fixture.kernelShare as KernelShareGroup);
    if (existingForkSharedSync !== null) {
      setKernelShareForkSharedSync(fixture.kernelShare as KernelShareGroup, existingForkSharedSync);
    }
  }

  if (hasOwn(fixture, "workers")) {
    const existingWorkers = processWorkers(fixture);
    const workers = new Set<Worker>(existingWorkers);
    for (const worker of fixture.workers ?? []) {
      workers.add(worker);
    }
    setProcessWorkers(fixture, workers);
  }

  if (hasStdioFixture(fixture)) {
    const stdinPipe = fixture.stdinPipe ?? 0;
    const stdoutPipe = fixture.stdoutPipe ?? 0;
    const stderrPipe = fixture.stderrPipe ?? 0;
    setProcessStdioState(fixture, {
      stdinPipe,
      stdoutPipe,
      stderrPipe,
      ttyPtySlot: fixture.ttyPtySlot ?? fixture.hostStdio?.ttyPtySlot ?? 0,
      hostStdio: fixture.hostStdio ?? {
        stdinPipe,
        stdoutPipe,
        stderrPipe,
        ttyPtySlot: fixture.ttyPtySlot,
        refCount: 1,
      },
      stdinClosed: fixture.stdinClosed ?? false,
      stdioOwner: fixture.stdioOwner ?? null,
    });
  }

  if (hasOwn(fixture, "onExit")) {
    setProcessOnExit(fixture, fixture.onExit);
  }
  if (hasOwn(fixture, "onError")) {
    setProcessOnError(fixture, fixture.onError);
  }
  if (hasOwn(fixture, "onStdout")) {
    setProcessOnStdout(fixture, fixture.onStdout);
  }
  installCallbackFixtureAccessors(fixture);

  if (fixture.debugCommand !== undefined) {
    setProcessDebugCommand(fixture, fixture.debugCommand);
  }
  if (fixture.lastDebugMessage !== undefined) {
    setProcessLastDebugMessage(fixture, fixture.lastDebugMessage);
  }
  if (hasOwn(fixture, "workerDebugLog")) {
    const log = workerDebugLog(fixture);
    log.splice(0, log.length, ...(fixture.workerDebugLog ?? []));
  }
}
