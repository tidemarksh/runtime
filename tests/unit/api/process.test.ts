import { expect, test, vi } from "vite-plus/test";
import { Process } from "../../../src/index.js";
import {
  guestWriteWatchRegions,
  setGuestWriteWatchRegions,
} from "../../../src/worker/guest-write-watch-state.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

function makeHandle(): ProcessHandle {
  const handle = {
    runtimeId: 1,
    pid: 11,
    parentPid: null,
    vmPtr: 0x1234,
    guestMemoryPolicyLimitBytes: 0,
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
    stdioOwner: null,
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
    vforkReadyWorkers: [],
    runningWorker: null,
    hostBlockedWorkers: new Map(),
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    vforkSnapshot: null,
    vforkWorker: null,
    workerDebugLog: [],
  } as unknown as ProcessHandle;
  handle.kernelShare.members.add(handle);
  return handle;
}

test("Process.onStdout flushes queued backlog before delivering later bytes", () => {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const handle = makeHandle();
  handle.outputBacklog.push(encoder.encode("stdout-1\n"));
  handle.outputBacklog.push(encoder.encode("stderr-1\n"));
  const proc = new Process(handle.pid, handle);
  const seen: string[] = [];

  proc.onStdout((data) => {
    seen.push(decoder.decode(data));
  });
  handle.onStdout?.(encoder.encode("stdout-2\n"));

  expect(seen).toEqual([
    "stdout-1\n",
    "stderr-1\n",
    "stdout-2\n",
  ]);
  expect(handle.outputBacklog).toEqual([]);
});

test("Process guest write watch coerces numeric regions to kernel u64 arguments", () => {
  const handle = makeHandle();
  const clearTrace = (handle.kernel.debug_clear_guest_write_watch_trace = vi.fn());
  const configureTrace = (handle.kernel.debug_configure_guest_write_watch = vi.fn());
  const proc = new Process(handle.pid, handle);

  proc.configureGuestWriteWatch([
    {
      slot: 1,
      start: 0x3008d0,
      end: 0x300908,
    },
  ]);

  expect(guestWriteWatchRegions(handle)).toEqual([
    {
      slot: 1,
      start: 0x3008d0,
      end: 0x300908,
    },
  ]);
  expect(clearTrace).toHaveBeenCalledTimes(1);
  expect(configureTrace).toHaveBeenCalledWith(1, 0x3008d0n, 0x300908n);
});

test("Process clearGuestWriteWatch clears stored regions and kernel trace state", () => {
  const handle = makeHandle();
  const clearTrace = (handle.kernel.debug_clear_guest_write_watch_trace = vi.fn());
  const proc = new Process(handle.pid, handle);
  setGuestWriteWatchRegions(handle, [{ slot: 0, start: 0x1000, end: 0x1010 }]);

  proc.clearGuestWriteWatch();

  expect(guestWriteWatchRegions(handle)).toEqual([]);
  expect(clearTrace).toHaveBeenCalledTimes(1);
});
