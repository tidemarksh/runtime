import { createRuntimeStateBridge } from "../../bridge/runtime-state.js";
import { createInheritedErrorSink, createInheritedStdoutSink, retainSharedHostStdio } from "../stdio.js";
import type { KernelExports } from "../../abi.js";
import type { ProcessHandle } from "../types.js";
import type { WorkerForkDeps } from "./types.js";

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
    guestBase: parent.guestBase,
    worker,
    memory,
    wasmModule: parent.wasmModule,
    kernel,
    kernelState,
    stdinPipe: kernel.process_stdin_pipe(vmPtr),
    stdoutPipe: kernel.process_stdout_pipe(vmPtr),
    stderrPipe: kernel.process_stderr_pipe(vmPtr),
    hostStdio: retainSharedHostStdio(parent.hostStdio),
    kernelShare: parent.kernelShare,
    stdinQueue: [],
    outputBacklog: [],
    debugCommand,
    lastDebugMessage: undefined,
    runtimeBridge: createRuntimeStateBridge(),
    sabPageCache: parent.sabPageCache,
    kernelWorkerRequest: parent.kernelWorkerRequest,
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
    onStdout: createInheritedStdoutSink(parent),
    onError: createInheritedErrorSink(
      parent,
      childPid,
      (pid) => deps.processHandles.get(pid),
      deps.failProcess,
    ),
    workerDebugLog: [],
  };
}
