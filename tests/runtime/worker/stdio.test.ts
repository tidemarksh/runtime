import { expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { readPipe, writePipe } from "../../../src/bridge/pipe.js";
import {
  drainProcessOutput,
  flushStdin,
  releaseSharedHostStdioLocal,
} from "../../../src/worker/stdio.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memStatePtr: 0x240000,
    epollTablePtr: 0x250000,
    socketTablePtr: 0x260000,
    socketUsed: 0,
    pipeBase: 0x1000,
    pipeUsed: 0n,
    netBufBase: 0x280000,
    netBufUsed: 0n,
    sigSlotsPtr: 0xc00000,
    processPendingPtr: 0xc10000,
    processRuntimeStatePtr: 0xc20000,
    timeStatePtr: 0xc30000,
  };
}

function setPipeEnds(
  memory: WebAssembly.Memory,
  kernelState: KernelRuntimeState,
  pipeSlot: number,
  readOpen: number,
  writeOpen: number,
): void {
  const view = new DataView(memory.buffer);
  const base = kernelState.pipeBase + pipeSlot * (32 + 4096);
  view.setInt32(base + 8, readOpen, true);
  view.setInt32(base + 12, writeOpen, true);
}

function seedPipe(
  memory: WebAssembly.Memory,
  kernelState: KernelRuntimeState,
  pipeSlot: number,
  text: string,
): void {
  setPipeEnds(memory, kernelState, pipeSlot, 1, 1);
  const result = writePipe(memory, kernelState.pipeBase, pipeSlot, new TextEncoder().encode(text));
  if (result.written !== text.length || result.broken) {
    throw new Error(`failed to seed pipe slot ${pipeSlot}`);
  }
  setPipeEnds(memory, kernelState, pipeSlot, 1, 0);
}

function makeHandle(
  overrides: Partial<ProcessHandle> = {},
): { handle: ProcessHandle; releaseHostStdio: ReturnType<typeof vi.fn> } {
  const memory = new WebAssembly.Memory({ initial: 2, maximum: 2, shared: true });
  const kernelState = makeKernelState();
  const releaseHostStdio = vi.fn();
  const kernel = {
    process_release_host_stdio: releaseHostStdio,
  } as unknown as KernelExports;
  const handle = {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0x1234,
    ramSize: 0,
    guestBase: 0,
    worker: {} as Worker,
    memory,
    wasmModule: {} as WebAssembly.Module,
    kernel,
    kernelState,
    stdinPipe: 0,
    stdoutPipe: 4,
    stderrPipe: 5,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 4,
      stderrPipe: 5,
      refCount: 1,
    },
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
    stdinQueue: [],
    outputBacklog: [],
    debugCommand: "test",
    runtimeBridge: {} as ProcessHandle["runtimeBridge"],
    sabPageCache: null,
    workers: new Set(),
    readyWorkers: [],
    runningWorker: null,
    blockedWorkers: new Map(),
    pipeSyncPromise: null,
    pipeSyncQueued: false,
    pipeWaiterWakeQueued: false,
    epollSyncPromise: null,
    epollSyncQueued: false,
    epollWaiterWakeQueued: false,
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    socketSyncPromise: null,
    socketSyncQueued: false,
    socketWaiterWakeQueued: false,
    vforkSnapshot: null,
    vforkWorker: null,
    workerDebugLog: [],
    ...overrides,
  } as unknown as ProcessHandle;
  handle.kernelShare.members.add(handle);
  return { handle, releaseHostStdio };
}

test("drainProcessOutput drains final stdout and stderr bytes from hidden host stdio slots", () => {
  const { handle } = makeHandle();
  const drainedChunks: string[] = [];
  handle.onStdout = (data) => {
    drainedChunks.push(new TextDecoder().decode(data));
  };
  seedPipe(handle.memory, handle.kernelState, handle.stdoutPipe, "stdout-tail\n");
  seedPipe(handle.memory, handle.kernelState, handle.stderrPipe, "stderr-tail\n");

  expect(drainProcessOutput(handle)).toBe(true);
  expect(drainedChunks).toEqual(["stdout-tail\n", "stderr-tail\n"]);
  expect(drainProcessOutput(handle)).toBe(false);
});

test("drainProcessOutput queues merged output when no stdout callback is installed", () => {
  const { handle } = makeHandle();
  seedPipe(handle.memory, handle.kernelState, handle.stdoutPipe, "first\n");
  seedPipe(handle.memory, handle.kernelState, handle.stderrPipe, "second\n");

  expect(drainProcessOutput(handle)).toBe(true);
  expect(handle.outputBacklog.map((chunk) => new TextDecoder().decode(chunk))).toEqual([
    "first\n",
    "second\n",
  ]);
});

test("drainProcessOutput preserves bytes across repeated capture phases on the same hidden host pipes", () => {
  const { handle } = makeHandle();
  const drainedChunks: string[] = [];
  handle.onStdout = (data) => {
    drainedChunks.push(new TextDecoder().decode(data));
  };

  seedPipe(handle.memory, handle.kernelState, handle.stdoutPipe, "stdout-1\n");
  seedPipe(handle.memory, handle.kernelState, handle.stderrPipe, "stderr-1\n");
  expect(drainProcessOutput(handle)).toBe(true);

  seedPipe(handle.memory, handle.kernelState, handle.stdoutPipe, "stdout-2\n");
  seedPipe(handle.memory, handle.kernelState, handle.stderrPipe, "stderr-2\n");
  expect(drainProcessOutput(handle)).toBe(true);

  expect(drainedChunks).toEqual([
    "stdout-1\n",
    "stderr-1\n",
    "stdout-2\n",
    "stderr-2\n",
  ]);
});

test("drainProcessOutput preserves separate stdout/stderr phases across sequential child-style captures", () => {
  const { handle } = makeHandle();
  const drainedChunks: string[] = [];
  handle.onStdout = (data) => {
    drainedChunks.push(new TextDecoder().decode(data));
  };

  seedPipe(handle.memory, handle.kernelState, handle.stdoutPipe, "stdout-1\n");
  expect(drainProcessOutput(handle)).toBe(true);
  expect(drainProcessOutput(handle)).toBe(false);

  seedPipe(handle.memory, handle.kernelState, handle.stderrPipe, "stderr-2\n");
  expect(drainProcessOutput(handle)).toBe(true);
  expect(drainProcessOutput(handle)).toBe(false);

  seedPipe(handle.memory, handle.kernelState, handle.stdoutPipe, "stdout-3\n");
  seedPipe(handle.memory, handle.kernelState, handle.stderrPipe, "stderr-3\n");
  expect(drainProcessOutput(handle)).toBe(true);

  expect(drainedChunks).toEqual([
    "stdout-1\n",
    "stderr-2\n",
    "stdout-3\n",
    "stderr-3\n",
  ]);
});

test("drainProcessOutput drains hidden stdout bytes after stdout was remapped away from the host pipe", () => {
  const { handle } = makeHandle({
    stdoutPipe: 9,
  });
  const drainedChunks: string[] = [];
  handle.onStdout = (data) => {
    drainedChunks.push(new TextDecoder().decode(data));
  };
  seedPipe(handle.memory, handle.kernelState, handle.hostStdio.stdoutPipe, "stdout-tail\n");
  seedPipe(handle.memory, handle.kernelState, handle.stderrPipe, "stderr-tail\n");

  expect(drainProcessOutput(handle)).toBe(true);
  expect(drainedChunks).toEqual(["stdout-tail\n", "stderr-tail\n"]);
});

test("drainProcessOutput drains hidden stderr bytes after stderr was remapped away from the host pipe", () => {
  const { handle } = makeHandle({
    stderrPipe: 9,
  });
  const drainedChunks: string[] = [];
  handle.onStdout = (data) => {
    drainedChunks.push(new TextDecoder().decode(data));
  };
  seedPipe(handle.memory, handle.kernelState, handle.stdoutPipe, "stdout-tail\n");
  seedPipe(handle.memory, handle.kernelState, handle.hostStdio.stderrPipe, "stderr-tail\n");

  expect(drainProcessOutput(handle)).toBe(true);
  expect(drainedChunks).toEqual(["stdout-tail\n", "stderr-tail\n"]);
});

test("flushStdin writes inherited stdin without breaking the next output drain", () => {
  const { handle } = makeHandle({
    stdinPipe: 3,
    hostStdio: {
      stdinPipe: 3,
      stdoutPipe: 4,
      stderrPipe: 5,
      refCount: 1,
    },
  });
  const decoder = new TextDecoder();
  const stdinText = "stdin-seed\n";
  const drainedChunks: string[] = [];
  handle.onStdout = (data) => {
    drainedChunks.push(decoder.decode(data));
  };

  setPipeEnds(handle.memory, handle.kernelState, handle.stdinPipe, 1, 1);
  handle.stdinQueue.push(new TextEncoder().encode(stdinText));
  expect(flushStdin(handle)).toBe(true);
  expect(decoder.decode(readPipe(handle.memory, handle.kernelState.pipeBase, handle.stdinPipe).data)).toBe(stdinText);

  seedPipe(handle.memory, handle.kernelState, handle.stdoutPipe, "stdout-tail\n");
  seedPipe(handle.memory, handle.kernelState, handle.stderrPipe, "stderr-tail\n");
  expect(drainProcessOutput(handle)).toBe(true);
  expect(drainedChunks).toEqual(["stdout-tail\n", "stderr-tail\n"]);
});

test("flushStdin supports a second child capture phase after the first inherited stdin run completes", () => {
  const { handle } = makeHandle({
    stdinPipe: 3,
    hostStdio: {
      stdinPipe: 3,
      stdoutPipe: 4,
      stderrPipe: 5,
      refCount: 1,
    },
  });
  const decoder = new TextDecoder();
  const drainedChunks: string[] = [];
  handle.onStdout = (data) => {
    drainedChunks.push(decoder.decode(data));
  };

  setPipeEnds(handle.memory, handle.kernelState, handle.stdinPipe, 1, 1);
  handle.stdinQueue.push(new TextEncoder().encode("stdin-phase-1\n"));
  expect(flushStdin(handle)).toBe(true);
  expect(decoder.decode(readPipe(handle.memory, handle.kernelState.pipeBase, handle.stdinPipe).data)).toBe(
    "stdin-phase-1\n",
  );

  seedPipe(handle.memory, handle.kernelState, handle.stdoutPipe, "stdout-1\n");
  seedPipe(handle.memory, handle.kernelState, handle.stderrPipe, "stderr-1\n");
  expect(drainProcessOutput(handle)).toBe(true);

  setPipeEnds(handle.memory, handle.kernelState, handle.stdinPipe, 1, 1);
  handle.stdinQueue.push(new TextEncoder().encode("stdin-phase-2\n"));
  expect(flushStdin(handle)).toBe(true);
  expect(decoder.decode(readPipe(handle.memory, handle.kernelState.pipeBase, handle.stdinPipe).data)).toBe(
    "stdin-phase-2\n",
  );

  seedPipe(handle.memory, handle.kernelState, handle.stdoutPipe, "stdout-2\n");
  seedPipe(handle.memory, handle.kernelState, handle.stderrPipe, "stderr-2\n");
  expect(drainProcessOutput(handle)).toBe(true);

  expect(drainedChunks).toEqual([
    "stdout-1\n",
    "stderr-1\n",
    "stdout-2\n",
    "stderr-2\n",
  ]);
});

test("releaseSharedHostStdioLocal clears local pipe slots without releasing the shared backend while other references remain", () => {
  const { handle, releaseHostStdio } = makeHandle({
    hostStdio: {
      stdinPipe: 3,
      stdoutPipe: 4,
      stderrPipe: 5,
      refCount: 2,
    },
    stdinPipe: 3,
  });

  releaseSharedHostStdioLocal(handle);

  expect(handle.stdinPipe).toBe(0);
  expect(handle.stdoutPipe).toBe(0);
  expect(handle.stderrPipe).toBe(0);
  expect(handle.hostStdio.refCount).toBe(1);
  expect(releaseHostStdio).not.toHaveBeenCalled();
});

test("releaseSharedHostStdioLocal is idempotent and releases the kernel host stdio once on the last reference", () => {
  const { handle, releaseHostStdio } = makeHandle({
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 4,
      stderrPipe: 5,
      refCount: 1,
    },
  });

  releaseSharedHostStdioLocal(handle);
  releaseSharedHostStdioLocal(handle);

  expect(handle.stdoutPipe).toBe(0);
  expect(handle.stderrPipe).toBe(0);
  expect(handle.hostStdio.refCount).toBe(0);
  expect(releaseHostStdio).toHaveBeenCalledTimes(1);
});
