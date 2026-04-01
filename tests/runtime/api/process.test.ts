import { expect, test } from "vite-plus/test";
import { Process } from "../../../src/index.js";
import { createInheritedStdoutSink } from "../../../src/worker/stdio.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

function makeHandle(): ProcessHandle {
  const handle = {
    runtimeId: 1,
    pid: 11,
    parentPid: null,
    vmPtr: 0x1234,
    ramSize: 0,
    guestBase: 0,
    worker: {} as Worker,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel: {
      process_release_host_stdio: () => {},
    } as unknown as ProcessHandle["kernel"],
    kernelState: {
      heapBase: 0x200000,
      heapPtr: 0x210000,
      ofdTablePtr: 0x220000,
      memfsPtr: 0x230000,
      memStatePtr: 0x240000,
      epollTablePtr: 0x250000,
      socketTablePtr: 0x260000,
      socketUsed: 0,
      pipeBase: 0x270000,
      pipeUsed: 0n,
      netBufBase: 0x280000,
      netBufUsed: 0n,
      sigSlotsPtr: 0xc00000,
      processPendingPtr: 0xc10000,
      processRuntimeStatePtr: 0xc20000,
      timeStatePtr: 0xc30000,
    },
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
    stdinQueue: [],
    outputBacklog: [],
    debugCommand: "process-api",
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
  } as unknown as ProcessHandle;
  handle.kernelShare.members.add(handle);
  return handle;
}

test("Process.onStdout flushes queued backlog before delivering live inherited output", () => {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const handle = makeHandle();
  handle.outputBacklog.push(encoder.encode("stdout-1\n"));
  handle.outputBacklog.push(encoder.encode("stderr-1\n"));
  const proc = new Process(handle.pid, handle);
  const sink = createInheritedStdoutSink(handle);
  const seen: string[] = [];

  proc.onStdout((data) => {
    seen.push(decoder.decode(data));
  });
  sink(encoder.encode("stdout-2\n"));

  expect(seen).toEqual([
    "stdout-1\n",
    "stderr-1\n",
    "stdout-2\n",
  ]);
  expect(handle.outputBacklog).toEqual([]);
});

test("Process.onStdout can resume after output temporarily falls back to backlog accumulation", () => {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const handle = makeHandle();
  const proc = new Process(handle.pid, handle);
  const sink = createInheritedStdoutSink(handle);
  const seen: string[] = [];

  proc.onStdout((data) => {
    seen.push(decoder.decode(data));
  });
  sink(encoder.encode("phase-1\n"));

  handle.onStdout = undefined;
  sink(encoder.encode("phase-2a\n"));
  sink(encoder.encode("phase-2b\n"));
  expect(handle.outputBacklog.map((chunk) => decoder.decode(chunk))).toEqual([
    "phase-2a\n",
    "phase-2b\n",
  ]);

  proc.onStdout((data) => {
    seen.push(decoder.decode(data));
  });
  sink(encoder.encode("phase-3\n"));

  expect(seen).toEqual([
    "phase-1\n",
    "phase-2a\n",
    "phase-2b\n",
    "phase-3\n",
  ]);
  expect(handle.outputBacklog).toEqual([]);
});
