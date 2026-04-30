import { createRuntimeStateBridge } from "../../bridge/runtime-state.js";
import { createInheritedErrorSink, retainSharedHostStdio } from "../stdio.js";
import type { KernelExports } from "../../abi.js";
import type { ProcessHandle } from "../types.js";
import type { WorkerForkDeps } from "./types.js";

const WASM_PAGE_SIZE_BYTES = 64 * 1024;
const WASM32_LINEAR_MEMORY_LIMIT_BYTES = 0x1_0000_0000;

interface CreateForkChildHandleArgs {
  deps: WorkerForkDeps;
  parent: ProcessHandle;
  childPid: number;
  childPpid: number;
  vmPtr: number;
  worker: Worker;
  memory: WebAssembly.Memory;
  kernel: KernelExports;
  kernelState: ProcessHandle["kernelState"];
  debugCommand: string;
  workerDebugLabel: string;
}

export function createForkChildHandle({
  deps,
  parent,
  childPid,
  childPpid,
  vmPtr,
  worker,
  memory,
  kernel,
  kernelState,
  debugCommand,
  workerDebugLabel,
}: CreateForkChildHandleArgs): ProcessHandle {
  return {
    runtimeId: parent.runtimeId,
    pid: childPid,
    parentPid: childPpid,
    vmPtr,
    ramSize: parent.ramSize,
    maxMemoryPages: parent.maxMemoryPages,
    forkMaxMemoryPages: parent.forkMaxMemoryPages,
    guestBase: parent.guestBase,
    worker,
    memory,
    wasmModule: parent.wasmModule,
    kernel,
    kernelState,
    stdinPipe: kernel.process_stdin_pipe(vmPtr),
    stdoutPipe: kernel.process_stdout_pipe(vmPtr),
    stderrPipe: kernel.process_stderr_pipe(vmPtr),
    ttyPtySlot: kernel.process_tty_pty_slot?.(vmPtr) ?? parent.ttyPtySlot,
    hostStdio: retainSharedHostStdio(parent.hostStdio),
    stdioOwner: parent,
    kernelShare: parent.kernelShare,
    stdinQueue: [],
    stdinFlushPromise: null,
    outputBacklog: [],
    debugCommand,
    lastDebugMessage: undefined,
    runtimeBridge: createRuntimeStateBridge(),
    sabPageCache: parent.sabPageCache,
    kernelWorkerRequest: parent.kernelWorkerRequest,
    networkBridge: parent.networkBridge,
    networkConnections: new Map(parent.networkConnections),
    workers: new Set(),
    readyWorkers: [],
    runningWorker: null,
    blockedWorkers: new Map(),
    pendingChildExitRecords: 0,
    pendingChildSpawnRecords: 0,
    childExitSyncRecords: new Map(),
    pipeSyncPromise: null,
    pipeSyncQueued: false,
    pipeWaiterWakeQueued: false,
    epollSyncPromise: null,
    epollSyncQueued: false,
    epollWaiterWakeQueued: false,
    forkSharedSyncPromise: null,
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    socketSyncPromise: null,
    socketSyncQueued: false,
    socketWaiterWakeQueued: false,
    vforkSnapshot: null,
    vforkWorker: null,
    exitInProgress: false,
    onError: createInheritedErrorSink(
      parent,
      childPid,
      (pid) => deps.processHandles.get(pid),
      deps.failProcess,
    ),
    workerDebugLog: [],
    guestWriteWatchRegions: parent.guestWriteWatchRegions.map((region) => ({ ...region })),
  };
}

export function computeGuestRamLimitBytes(
  guestBase: number,
  minRamSize: number,
  maxMemoryPages: number,
): number {
  const totalLinearBytes = Math.min(
    maxMemoryPages * WASM_PAGE_SIZE_BYTES,
    WASM32_LINEAR_MEMORY_LIMIT_BYTES,
  );
  const guestWindowBytes = Math.max(0, totalLinearBytes - guestBase);
  return Math.max(
    minRamSize,
    guestWindowBytes,
  );
}

export function detachInheritedHostStdioFromChild(child: ProcessHandle): void {
  if (child.stdioOwner === null) {
    return;
  }
  child.kernel.process_set_host_stdio(child.vmPtr, 0, 0, 0);
}
